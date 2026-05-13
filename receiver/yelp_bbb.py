"""Yelp + BBB Enrichment Module
Looks up businesses on Yelp Fusion API and BBB website.
"""
import httpx
import os
import re
import logging
import time
from typing import Dict, Optional

import db

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('yelp_bbb')

YELP_API_KEY = os.environ.get('YELP_API_KEY', '')
YELP_URL = 'https://api.yelp.com/v3'


def search_yelp(name: str, city: str, state: str = 'OR') -> Dict:
    """Search Yelp for a business by name and location."""
    if not YELP_API_KEY:
        return {}
    try:
        r = httpx.get(f'{YELP_URL}/businesses/search', headers={
            'Authorization': f'Bearer {YELP_API_KEY}'
        }, params={
            'term': name,
            'location': f'{city}, {state}',
            'limit': 1,
            'sort_by': 'best_match'
        }, timeout=10)
        data = r.json()
        businesses = data.get('businesses', [])
        if businesses:
            biz = businesses[0]
            # Confidence check: name similarity
            yelp_name = biz.get('name', '').lower()
            our_name = name.lower()
            # Simple word overlap check
            our_words = set(our_name.split())
            yelp_words = set(yelp_name.split())
            overlap = len(our_words & yelp_words)
            if overlap >= 1 or len(our_words) <= 2:
                return {
                    'yelp_rating': biz.get('rating'),
                    'yelp_review_count': biz.get('review_count'),
                    'yelp_url': biz.get('url', ''),
                    'yelp_categories': [c['title'] for c in biz.get('categories', [])],
                }
    except Exception as e:
        log.error(f'Yelp search error: {e}')
    return {}


def search_bbb(name: str, city: str, state: str = 'OR') -> Dict:
    """Search BBB for a business. Uses BBB's search page scraping."""
    try:
        search_url = f'https://www.bbb.org/search?find_text={name}&find_loc={city}%2C+{state}'
        r = httpx.get(search_url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }, timeout=10, follow_redirects=True)
        html = r.text.lower()
        
        # Check if accredited
        accredited = 'bbb accredited' in html and name.lower().split()[0] in html
        
        # Try to find rating
        rating_match = re.search(r'rating[:\s]*([A-F][+-]?)', r.text)
        rating = rating_match.group(1) if rating_match else None
        
        if accredited or rating:
            return {
                'bbb_accredited': accredited,
                'bbb_rating': rating,
            }
    except Exception as e:
        log.error(f'BBB search error: {e}')
    return {}


def enrich_leads(limit: int = 50) -> Dict:
    """Enrich leads that haven't been checked on Yelp/BBB yet."""
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, business_name, city, state FROM businesses
            WHERE (yelp_rating IS NULL OR bbb_rating IS NULL)
            AND business_name IS NOT NULL AND business_name != ''
            AND city IS NOT NULL AND city != '' AND city != 'None'
            ORDER BY lead_score DESC NULLS LAST
            LIMIT %s
        """, (limit,))
        leads = cur.fetchall()
    
    log.info(f'Enriching {len(leads)} leads with Yelp/BBB')
    yelp_found = 0
    bbb_found = 0
    
    for lead_id, name, city, state in leads:
        updates = {}
        
        # Yelp
        if YELP_API_KEY:
            yelp = search_yelp(name, city or 'Oregon', state or 'OR')
            if yelp.get('yelp_rating'):
                updates['yelp_rating'] = yelp['yelp_rating']
                updates['yelp_review_count'] = yelp.get('yelp_review_count', 0)
                yelp_found += 1
                db.log_enrichment(lead_id, 'yelp', 'success', yelp)
            time.sleep(0.5)  # Yelp rate limit
        
        # BBB
        bbb = search_bbb(name, city or 'Oregon', state or 'OR')
        if bbb.get('bbb_accredited') or bbb.get('bbb_rating'):
            if bbb.get('bbb_accredited') is not None:
                updates['bbb_accredited'] = bbb['bbb_accredited']
            if bbb.get('bbb_rating'):
                updates['bbb_rating'] = bbb['bbb_rating']
            bbb_found += 1
            db.log_enrichment(lead_id, 'bbb', 'success', bbb)
        time.sleep(0.3)
        
        if updates:
            db.update_lead(lead_id, updates)
    
    result = {'total': len(leads), 'yelp_found': yelp_found, 'bbb_found': bbb_found}
    log.info(f'Done: {result}')
    return result


if __name__ == '__main__':
    import sys
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    enrich_leads(limit)
