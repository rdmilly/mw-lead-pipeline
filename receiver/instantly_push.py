"""Instantly.ai Integration — Push qualified leads to email campaigns.

Uses Instantly API V2 to add leads to campaigns.
API: POST https://api.instantly.ai/api/v2/leads
"""

import os
import json
import httpx
from datetime import datetime
import db

INSTANTLY_API_KEY = os.environ.get('INSTANTLY_API_KEY', '')
INSTANTLY_API_URL = 'https://api.instantly.ai/api/v2'


def _headers():
    return {
        'Authorization': f'Bearer {INSTANTLY_API_KEY}',
        'Content-Type': 'application/json'
    }


def list_campaigns(limit=10):
    """List available Instantly campaigns."""
    if not INSTANTLY_API_KEY:
        return {'error': 'INSTANTLY_API_KEY not configured'}
    try:
        r = httpx.get(f'{INSTANTLY_API_URL}/campaigns', headers=_headers(),
                      params={'limit': limit}, timeout=15)
        return r.json()
    except Exception as e:
        return {'error': str(e)}


def push_leads(campaign_id: str, min_score: float = 50, limit: int = 100) -> dict:
    """Push qualified leads to an Instantly campaign.
    
    Selects leads that:
    - Have a primary_email
    - Score >= min_score
    - Haven't been exported yet
    
    Returns push results.
    """
    if not INSTANTLY_API_KEY:
        return {'error': 'INSTANTLY_API_KEY not configured'}
    
    # Get qualifying leads from DB
    leads = db.export_leads_csv(min_score=min_score, limit=limit)
    
    if not leads:
        return {'pushed': 0, 'message': 'No qualifying leads found'}
    
    # Filter to leads with email that haven't been exported
    to_push = [l for l in leads if l.get('primary_email')]
    
    if not to_push:
        return {'pushed': 0, 'message': 'No leads with email found above score threshold'}
    
    # Build Instantly lead objects
    instantly_leads = []
    lead_ids = []
    
    for lead in to_push:
        # Split owner name into first/last
        owner = (lead.get('owner_name') or '').strip()
        parts = owner.split(' ', 1) if owner else ['', '']
        first_name = parts[0] if parts else ''
        last_name = parts[1] if len(parts) > 1 else ''
        
        instantly_lead = {
            'email': lead['primary_email'],
            'first_name': first_name,
            'last_name': last_name,
            'company_name': lead.get('business_name', ''),
            'phone': lead.get('primary_phone', ''),
            'website': lead.get('source_url', ''),
            'custom_variables': {
                'companyName': lead.get('business_name') or '',
                'firstName': first_name,
                'category': lead.get('category') or '',
                'google_rating': str(lead.get('google_rating') or ''),
                'review_count': str(lead.get('google_review_count') or ''),
                'lead_score': str(lead.get('lead_score') or ''),
                'quality_tier': lead.get('quality_tier') or '',
                'city': lead.get('city') or '',
                'state': lead.get('state') or '',
                'domain': lead.get('domain') or '',
            }
        }
        instantly_leads.append(instantly_lead)
        
        # Look up the DB id for this domain
        db_lead = db.get_lead_by_domain(lead.get('domain', ''))
        if db_lead:
            lead_ids.append(db_lead['id'])
    
    # Push to Instantly in batches of 100
    total_created = 0
    total_skipped = 0
    errors = []
    
    for i in range(0, len(instantly_leads), 100):
        batch = instantly_leads[i:i+100]
        batch_ids = lead_ids[i:i+100]
        
        try:
            payload = {
                'campaign_id': campaign_id,
                'leads': batch,
                'skip_if_in_workspace': True
            }
            r = httpx.post(f'{INSTANTLY_API_URL}/leads/add', headers=_headers(),
                          json=payload, timeout=30)
            result = r.json()
            
            if r.status_code == 200:
                created = result.get('leads_uploaded', 0)
                total_created += created
                total_skipped += result.get('skipped_count', 0)
                
                # Mark as exported in our DB
                if batch_ids:
                    db.mark_exported(batch_ids, campaign_id)
            else:
                errors.append(f'Batch {i//100}: HTTP {r.status_code} - {result}')
        except Exception as e:
            errors.append(f'Batch {i//100}: {str(e)}')
    
    return {
        'pushed': total_created,
        'skipped': total_skipped,
        'total_attempted': len(instantly_leads),
        'campaign_id': campaign_id,
        'errors': errors if errors else None
    }


def get_status() -> dict:
    """Check Instantly connection and campaign status."""
    if not INSTANTLY_API_KEY:
        return {'connected': False, 'reason': 'INSTANTLY_API_KEY not set'}
    
    campaigns = list_campaigns(limit=5)
    if campaigns.get('error'):
        return {'connected': False, 'reason': campaigns['error']}
    
    return {
        'connected': True,
        'campaigns': campaigns.get('items', campaigns.get('data', [])),
        'db_stats': {
            'ready_to_export': len(db.export_leads_csv(min_score=50)),
            'already_exported': db.get_stats().get('exported', 0)
        }
    }
