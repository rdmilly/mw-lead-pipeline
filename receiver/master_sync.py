"""Master Leads JSON <-> PostgreSQL sync bridge.

Keeps the enrichment modules working with their existing JSON interface
while PostgreSQL is the source of truth.

Usage:
    import master_sync
    master_sync.dump_to_json()   # PG -> JSON (before enrichment)
    master_sync.sync_from_json() # JSON -> PG (after enrichment)
"""

import json
from datetime import datetime
from pathlib import Path
import db

MASTER_FILE = Path('/data/leads/master_leads.json')


def dump_to_json():
    """Export all PostgreSQL leads to master_leads.json format.
    Call BEFORE running enrichment stages."""
    compat = db.load_master_compat()
    with open(MASTER_FILE, 'w') as f:
        json.dump(compat, f, indent=2, default=str)
    count = len(compat.get('leads', {}))
    print(f'[SYNC] Dumped {count} leads from PostgreSQL -> master_leads.json')
    return count


def sync_from_json():
    """Read master_leads.json and sync enrichment changes back to PostgreSQL.
    Call AFTER running enrichment stages."""
    if not MASTER_FILE.exists():
        print('[SYNC] No master_leads.json found')
        return 0

    with open(MASTER_FILE) as f:
        data = json.load(f)

    leads = data.get('leads', {})
    updated = 0
    errors = 0

    for domain, lead in leads.items():
        try:
            # Find existing lead in DB
            existing = db.get_lead_by_domain(domain)
            if not existing:
                # New lead added during enrichment — upsert it
                db.upsert_lead({
                    'source_url': lead.get('website', ''),
                    'business_name': lead.get('business_name', ''),
                    'emails': lead.get('emails', []),
                    'phones': lead.get('phones', []),
                    'owner_info': {'name': lead.get('owner_name', ''), 'source': ''},
                    'meta': {},
                    'social_links': {},
                    'source': lead.get('source', 'enrichment'),
                })
                existing = db.get_lead_by_domain(domain)
                if not existing:
                    continue

            bid = existing['id']
            updates = {}

            # Map enrichment fields back to DB columns
            field_map = {
                'business_name': 'business_name',
                'category': 'category',
                'address': 'address',
                'city': 'city',
                'state': 'state',
                'zip_code': 'zip_code',
                'owner_name': 'owner_name',
                'notes': 'notes',
                'license_number': 'license_number',
                'license_status': 'license_status',
            }

            for json_key, db_key in field_map.items():
                val = lead.get(json_key)
                if val and val != existing.get(db_key):
                    updates[db_key] = val

            # Numeric fields
            if lead.get('rating') and lead['rating'] != existing.get('google_rating'):
                try:
                    updates['google_rating'] = float(lead['rating'])
                except (ValueError, TypeError):
                    pass
            if lead.get('reviews') and lead['reviews'] != existing.get('google_review_count'):
                try:
                    updates['google_review_count'] = int(lead['reviews'])
                except (ValueError, TypeError):
                    pass
            if lead.get('employee_count'):
                try:
                    updates['employee_count'] = int(lead['employee_count'])
                except (ValueError, TypeError):
                    pass
            if lead.get('years_in_business'):
                try:
                    updates['years_in_business'] = int(lead['years_in_business'])
                except (ValueError, TypeError):
                    pass
            if lead.get('revenue_estimate'):
                updates['revenue_estimate'] = str(lead['revenue_estimate'])

            # BBB
            if lead.get('bbb_accredited') is not None:
                updates['bbb_accredited'] = bool(lead.get('bbb_accredited'))
            if lead.get('bbb_rating'):
                updates['bbb_rating'] = lead['bbb_rating']

            # Yelp
            if lead.get('yelp_rating'):
                try:
                    updates['yelp_rating'] = float(lead['yelp_rating'])
                except (ValueError, TypeError):
                    pass
            if lead.get('yelp_review_count'):
                try:
                    updates['yelp_review_count'] = int(lead['yelp_review_count'])
                except (ValueError, TypeError):
                    pass

            # Email — pick best email
            best_email = lead.get('best_email') or lead.get('verified_email')
            if not best_email:
                emails = lead.get('emails', [])
                if emails:
                    best_email = emails[0]
            if best_email and best_email != existing.get('primary_email'):
                updates['primary_email'] = best_email

            # Emails array
            json_emails = lead.get('emails', [])
            if json_emails and set(json_emails) != set(existing.get('emails') or []):
                merged = list(set((existing.get('emails') or []) + json_emails))
                # Update via direct SQL since update_lead doesn't handle arrays
                with db.get_conn() as conn:
                    cur = conn.cursor()
                    cur.execute('UPDATE businesses SET emails = %s WHERE id = %s', (merged, bid))

            # Phones array
            json_phones = lead.get('phones', [])
            if json_phones and set(json_phones) != set(existing.get('phones') or []):
                merged = list(set((existing.get('phones') or []) + json_phones))
                with db.get_conn() as conn:
                    cur = conn.cursor()
                    cur.execute('UPDATE businesses SET phones = %s, primary_phone = COALESCE(primary_phone, %s) WHERE id = %s',
                                (merged, merged[0] if merged else None, bid))

            # Enrichment status
            if lead.get('enriched') and existing.get('enrichment_status') != 'complete':
                updates['enrichment_status'] = 'complete'

            # Score if present
            if lead.get('score') and lead['score'] != existing.get('lead_score'):
                try:
                    updates['lead_score'] = float(lead['score'])
                except (ValueError, TypeError):
                    pass

            if updates:
                db.update_lead(bid, updates)
                updated += 1

        except Exception as e:
            print(f'[SYNC] Error syncing {domain}: {e}')
            errors += 1

    print(f'[SYNC] Synced master_leads.json -> PostgreSQL: {updated} updated, {errors} errors')
    return updated
