"""Google Places Website Finder v2
Searches Google Places for leads without websites.
Includes confidence filter (fuzzy name match) and clean domain merge.
"""
import httpx, os, re, sys, time, logging
from urllib.parse import urlparse
from difflib import SequenceMatcher
import db

logging.basicConfig(level=logging.INFO, format='%(asctime)s [PLACES] %(message)s')
log = logging.getLogger('places')

PLACES_KEY = os.environ.get('GOOGLE_PLACES_KEY', 'AIzaSyARIS8p7XpdzHxnkaS9EyxraH3LKnk1sxE')
PLACES_URL = 'https://maps.googleapis.com/maps/api/place'
MIN_CONFIDENCE = 0.45  # Minimum name similarity


def name_similarity(a: str, b: str) -> float:
    """Fuzzy name match. Handles LLC/Inc suffixes and word reordering."""
    def clean(s):
        s = s.lower()
        for junk in ['llc', 'inc', 'corp', 'co', 'ltd', 'the', 'of', '&', 'and']:
            s = s.replace(junk, '')
        return ' '.join(s.split())
    ca, cb = clean(a), clean(b)
    # Direct ratio
    ratio = SequenceMatcher(None, ca, cb).ratio()
    # Also check word overlap
    wa, wb = set(ca.split()), set(cb.split())
    if wa and wb:
        overlap = len(wa & wb) / max(len(wa), len(wb))
        ratio = max(ratio, overlap)
    return ratio


def find_place(name: str, city: str, state: str = 'OR') -> dict:
    query = f"{name} {city} {state}"
    try:
        r = httpx.get(f"{PLACES_URL}/findplacefromtext/json", params={
            'input': query, 'inputtype': 'textquery',
            'fields': 'place_id,name,formatted_address,geometry',
            'key': PLACES_KEY
        }, timeout=10)
        data = r.json()
        candidates = data.get('candidates', [])
        if candidates:
            candidate = candidates[0]
            # Confidence check
            places_name = candidate.get('name', '')
            sim = name_similarity(name, places_name)
            if sim < MIN_CONFIDENCE:
                log.info(f'  SKIP (low confidence {sim:.2f}): "{name}" vs "{places_name}"')
                return {}
            candidate['_confidence'] = round(sim, 2)
            return candidate
    except Exception as e:
        log.error(f'Find place error for "{name}": {e}')
    return {}


def get_place_details(place_id: str) -> dict:
    try:
        r = httpx.get(f"{PLACES_URL}/details/json", params={
            'place_id': place_id,
            'fields': 'website,formatted_phone_number,rating,user_ratings_total,url,name,types',
            'key': PLACES_KEY
        }, timeout=10)
        return r.json().get('result', {})
    except Exception as e:
        log.error(f'Place details error: {e}')
    return {}


def process_leads(limit: int = 50):
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, business_name, city, state, domain
            FROM businesses
            WHERE (source_url IS NULL OR source_url = '' OR source_url = 'None')
            AND business_name IS NOT NULL AND business_name != ''
            ORDER BY lead_score DESC NULLS LAST, created_at DESC
            LIMIT %s
        """, (limit,))
        leads = cur.fetchall()

    log.info(f'Processing {len(leads)} leads without websites')
    found = 0
    skipped = 0
    not_found = 0
    errors = 0

    for lead_id, name, city, state, domain in leads:
        try:
            place = find_place(name, city or 'Oregon', state or 'OR')
            if not place or not place.get('place_id'):
                not_found += 1
                continue

            pid = place['place_id']
            details = get_place_details(pid)
            if not details:
                not_found += 1
                continue

            website = details.get('website', '')
            rating = details.get('rating')
            reviews = details.get('user_ratings_total')

            updates = {'place_id': pid}
            if rating:
                updates['google_rating'] = float(rating)
            if reviews:
                updates['google_review_count'] = int(reviews)

            if website:
                updates['source_url'] = website
                # Domain merge: update .ccb.local to real domain
                parsed = urlparse(website)
                real_domain = parsed.netloc.lower().replace('www.', '')
                if real_domain and domain and domain.endswith('.ccb.local'):
                    # Check if real domain already exists
                    existing = db.get_lead_by_domain(real_domain)
                    if not existing:
                        with db.get_conn() as conn:
                            cur = conn.cursor()
                            cur.execute('UPDATE businesses SET domain = %s WHERE id = %s', (real_domain, lead_id))
                    else:
                        # Merge: transfer CCB data to existing real-domain lead
                        merge_updates = {}
                        ccb_lead = db.get_lead_by_id(lead_id)
                        if ccb_lead:
                            if ccb_lead.get('license_number') and not existing.get('license_number'):
                                merge_updates['license_number'] = ccb_lead['license_number']
                                merge_updates['license_status'] = 'active'
                            if ccb_lead.get('owner_name') and not existing.get('owner_name'):
                                merge_updates['owner_name'] = ccb_lead['owner_name']
                            if ccb_lead.get('years_in_business') and not existing.get('years_in_business'):
                                merge_updates['years_in_business'] = ccb_lead['years_in_business']
                            if ccb_lead.get('category') and not existing.get('category'):
                                merge_updates['category'] = ccb_lead['category']
                            if merge_updates:
                                db.update_lead(existing['id'], merge_updates)
                            # Delete the CCB placeholder
                            db.delete_lead(lead_id)
                            log.info(f'  MERGED {name} into existing {real_domain}')
                            found += 1
                            time.sleep(0.15)
                            continue

                found += 1
                log.info(f'  FOUND: {name} -> {website} (conf={place.get("_confidence","?")})')
            else:
                skipped += 1

            db.update_lead(lead_id, updates)
            db.log_enrichment(lead_id, 'google_places', 'success', {
                'website': website, 'rating': rating, 'reviews': reviews,
                'confidence': place.get('_confidence')
            })
            time.sleep(0.15)

        except Exception as e:
            errors += 1
            log.error(f'Error processing {name}: {e}')

    result = {'found': found, 'skipped_no_website': skipped, 'not_found': not_found, 'errors': errors}
    log.info(f'Done: {result}')
    return result


if __name__ == '__main__':
    limit = int(sys.argv[sys.argv.index('--limit') + 1]) if '--limit' in sys.argv else 50
    process_leads(limit)
