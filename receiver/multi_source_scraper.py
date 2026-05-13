#!/usr/bin/env python3
"""Multi-Source Lead Scraper v1.0
Aggregates leads from 4 free sources:
1. Serper Maps (Google Maps via API) - 2,500/mo free
2. Foursquare Places - 100K/mo free
3. Bing Local Search - 50K/year free
4. Geoapify Places - 90K/mo free

Deduplicates by domain/phone before inserting into PostgreSQL.
"""
import httpx
import os
import json
import time
import logging
from datetime import datetime
from urllib.parse import urlparse, quote_plus
from pathlib import Path

import db

logging.basicConfig(level=logging.INFO, format='%(asctime)s [MULTI] %(message)s')
log = logging.getLogger('multi_source')

# API Keys
SERPER_KEY = os.environ.get('SERPER_API_KEY', '')
FOURSQUARE_KEY = os.environ.get('FOURSQUARE_API_KEY', '')
BING_KEY = os.environ.get('BING_MAPS_KEY', '')
GEOAPIFY_KEY = os.environ.get('GEOAPIFY_KEY', '')

# Categories for service businesses
CATEGORIES = [
    'plumber', 'HVAC contractor', 'electrician', 'roofer',
    'landscaper', 'painter', 'handyman', 'pest control',
    'carpet cleaner', 'garage door repair', 'fence contractor',
    'flooring contractor', 'window installer', 'concrete contractor',
    'tree service', 'cleaning service', 'general contractor',
    'home remodeling', 'pressure washing', 'gutter cleaning',
    'appliance repair', 'deck builder', 'siding contractor',
]

CITIES_OR = [
    ('Portland', 'OR', 45.5152, -122.6784),
    ('Salem', 'OR', 44.9429, -123.0351),
    ('Eugene', 'OR', 44.0521, -123.0868),
    ('Gresham', 'OR', 45.4983, -122.4310),
    ('Hillsboro', 'OR', 45.5229, -122.9898),
    ('Beaverton', 'OR', 45.4871, -122.8038),
    ('Bend', 'OR', 44.0582, -121.3153),
    ('Medford', 'OR', 42.3265, -122.8756),
    ('Tigard', 'OR', 45.4312, -122.7715),
    ('Lake Oswego', 'OR', 45.4207, -122.6706),
    ('Oregon City', 'OR', 45.3573, -122.6068),
    ('Tualatin', 'OR', 45.3840, -122.7637),
    ('Newberg', 'OR', 45.3001, -122.9732),
    ('McMinnville', 'OR', 45.2101, -123.1968),
    ('Sherwood', 'OR', 45.3565, -122.8401),
    ('Wilsonville', 'OR', 45.2998, -122.7735),
    ('West Linn', 'OR', 45.3654, -122.6121),
    ('Milwaukie', 'OR', 45.4443, -122.6393),
    ('Canby', 'OR', 45.2629, -122.6926),
    ('Albany', 'OR', 44.6365, -123.1059),
    ('Corvallis', 'OR', 44.5646, -123.2620),
    ('Redmond', 'OR', 44.2726, -121.1740),
    ('Springfield', 'OR', 44.0462, -123.0220),
    ('Grants Pass', 'OR', 42.4390, -123.3284),
    ('Vancouver', 'WA', 45.6387, -122.6615),
]

SEARCH_LOG_FILE = Path('/data/multi_source_log.json')


def load_log():
    if SEARCH_LOG_FILE.exists():
        with open(SEARCH_LOG_FILE) as f:
            return json.load(f)
    return {'serper': [], 'foursquare': [], 'bing': [], 'geoapify': [], 'stats': {}}


def save_log(data):
    SEARCH_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(SEARCH_LOG_FILE, 'w') as f:
        json.dump(data, f, indent=2)


def import_lead(name, website, phone, address, city, state, category, rating, reviews, source) -> bool:
    """Import a lead, deduplicating by domain."""
    if not website or not name:
        return False
    parsed = urlparse(website if website.startswith('http') else 'https://' + website)
    domain = parsed.netloc.lower().replace('www.', '')
    if not domain:
        return False
    existing = db.get_lead_by_domain(domain)
    if existing:
        return False
    try:
        with db.get_conn() as conn:
            cur = conn.cursor()
            cur.execute('''INSERT INTO businesses
                (domain, business_name, source_url, primary_phone, phones,
                 address, city, state, google_rating, google_review_count,
                 category, source, pipeline_status, enrichment_status,
                 created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                ON CONFLICT (domain) DO NOTHING
            ''', (
                domain, name, website, phone, [phone] if phone else [],
                address, city, state,
                float(rating) if rating else None,
                int(reviews) if reviews else None,
                category, source, 'new', 'pending'
            ))
            return cur.rowcount > 0
    except Exception as e:
        log.error(f'Import error: {e}')
        return False


# ============================================================
# SOURCE 1: SERPER MAPS (Google Maps via API)
# ============================================================
def search_serper_maps(category, city, state='OR', limit=20):
    """Search Google Maps via Serper.dev Maps endpoint."""
    if not SERPER_KEY:
        return []
    query = f'{category} in {city} {state}'
    try:
        r = httpx.post('https://google.serper.dev/maps',
            headers={'X-API-KEY': SERPER_KEY, 'Content-Type': 'application/json'},
            json={'q': query, 'num': limit}, timeout=10)
        if r.status_code != 200:
            return []
        data = r.json()
        results = []
        for place in data.get('places', []):
            results.append({
                'name': place.get('title', ''),
                'website': place.get('website', ''),
                'phone': place.get('phoneNumber', ''),
                'address': place.get('address', ''),
                'rating': place.get('rating'),
                'reviews': place.get('ratingCount'),
            })
        return results
    except Exception as e:
        log.error(f'Serper Maps error: {e}')
        return []


# ============================================================
# SOURCE 2: FOURSQUARE PLACES
# ============================================================
def search_foursquare(category, city, state='OR', lat=None, lng=None, limit=20):
    """Search Foursquare Places API."""
    if not FOURSQUARE_KEY:
        return []
    query = f'{category}'
    near = f'{city}, {state}'
    try:
        params = {
            'query': query,
            'near': near,
            'limit': limit,
            'fields': 'name,website,tel,location,rating,stats',
        }
        if lat and lng:
            params['ll'] = f'{lat},{lng}'
            params['radius'] = 25000  # 25km
            del params['near']

        r = httpx.get('https://api.foursquare.com/v3/places/search',
            headers={'Authorization': FOURSQUARE_KEY, 'Accept': 'application/json'},
            params=params, timeout=10)
        if r.status_code != 200:
            log.warning(f'Foursquare {r.status_code}: {r.text[:100]}')
            return []
        data = r.json()
        results = []
        for place in data.get('results', []):
            loc = place.get('location', {})
            results.append({
                'name': place.get('name', ''),
                'website': place.get('website', ''),
                'phone': place.get('tel', ''),
                'address': loc.get('formatted_address', ''),
                'city': loc.get('locality', city),
                'state': loc.get('region', state),
                'rating': place.get('rating'),
                'reviews': None,
            })
        return results
    except Exception as e:
        log.error(f'Foursquare error: {e}')
        return []


# ============================================================
# SOURCE 3: BING LOCAL SEARCH
# ============================================================
def search_bing(category, city, state='OR', lat=None, lng=None, limit=20):
    """Search Azure Maps Fuzzy Search API (replaces deprecated Bing Maps)."""
    if not BING_KEY:
        return []
    query = f'{category} {city} {state}'
    try:
        params = {
            'api-version': '1.0',
            'query': query,
            'limit': min(limit, 100),
            'subscription-key': BING_KEY,
            'countrySet': 'US',
            'categorySet': '9361,9362,9363',
        }
        if lat and lng:
            params['lat'] = lat
            params['lon'] = lng
            params['radius'] = 25000

        r = httpx.get('https://atlas.microsoft.com/search/fuzzy/json',
            params=params, timeout=15)
        if r.status_code != 200:
            log.warning(f'Azure Maps {r.status_code}')
            return []
        data = r.json()
        results = []
        for res in data.get('results', []):
            poi = res.get('poi', {})
            addr = res.get('address', {})
            results.append({
                'name': poi.get('name', ''),
                'website': poi.get('url', ''),
                'phone': poi.get('phone', ''),
                'address': addr.get('freeformAddress', ''),
                'rating': None,
                'reviews': None,
            })
        return results
    except Exception as e:
        log.error(f'Azure Maps error: {e}')
        return []


# ============================================================
# SOURCE 4: GEOAPIFY PLACES
# ============================================================
# Geoapify category mapping for service businesses
GEOAPIFY_CATEGORIES = {
    'plumber': 'building',
    'electrician': 'building',
    'HVAC contractor': 'building',
    'roofer': 'building',
    'landscaper': 'service',
    'painter': 'building',
    'handyman': 'building',
    'cleaning service': 'service',
    'pest control': 'service',
    'general contractor': 'building',
    'home remodeling': 'building',
    'flooring contractor': 'building',
    'concrete contractor': 'building',
    'tree service': 'service',
    'pressure washing': 'service',
    'appliance repair': 'service',
}

def search_geoapify(category, city, state='OR', lat=None, lng=None, limit=20):
    """Search Geoapify Places API (OpenStreetMap data)."""
    if not GEOAPIFY_KEY:
        return []
    geo_cat = GEOAPIFY_CATEGORIES.get(category, 'building')
    try:
        params = {
            'categories': geo_cat,
            'conditions': f'named',
            'filter': f'circle:{lng},{lat},25000' if lat and lng else f'circle:-122.6784,45.5152,50000',
            'limit': limit,
            'apiKey': GEOAPIFY_KEY,
        }

        r = httpx.get('https://api.geoapify.com/v2/places',
            params=params, timeout=20)
        if r.status_code != 200:
            log.warning(f'Geoapify {r.status_code}: {r.text[:100]}')
            return []
        data = r.json()
        results = []
        for feat in data.get('features', []):
            props = feat.get('properties', {})
            results.append({
                'name': props.get('name', ''),
                'website': props.get('website', ''),
                'phone': props.get('contact', {}).get('phone', '') if isinstance(props.get('contact'), dict) else '',
                'address': props.get('formatted', ''),
                'rating': None,
                'reviews': None,
            })
        return results
    except Exception as e:
        log.error(f'Geoapify error: {e}')
        return []


# ============================================================
# ORCHESTRATOR: Run all sources for a category/city
# ============================================================
def scrape_all_sources(category, city, state='OR', lat=None, lng=None):
    """Query all available sources for a category+city and import results."""
    total_new = 0
    sources_used = []

    # Source 1: Serper Maps
    if SERPER_KEY:
        results = search_serper_maps(category, city, state)
        new = 0
        for r in results:
            if r.get('website'):
                if import_lead(r['name'], r['website'], r.get('phone',''),
                              r.get('address',''), city, state, category,
                              r.get('rating'), r.get('reviews'), 'serper_maps'):
                    new += 1
        total_new += new
        if results:
            sources_used.append(f'serper({len(results)}/{new}new)')
        time.sleep(0.3)

    # Source 2: Foursquare
    if False and FOURSQUARE_KEY:  # Disabled - API in migration
        results = search_foursquare(category, city, state, lat, lng)
        new = 0
        for r in results:
            if r.get('website'):
                if import_lead(r['name'], r['website'], r.get('phone',''),
                              r.get('address',''), r.get('city', city),
                              r.get('state', state), category,
                              r.get('rating'), r.get('reviews'), 'foursquare'):
                    new += 1
        total_new += new
        if results:
            sources_used.append(f'fsq({len(results)}/{new}new)')
        time.sleep(0.3)

    # Source 3: Bing
    if BING_KEY:
        results = search_bing(category, city, state, lat, lng)
        new = 0
        for r in results:
            if r.get('website'):
                if import_lead(r['name'], r['website'], r.get('phone',''),
                              r.get('address',''), city, state, category,
                              r.get('rating'), r.get('reviews'), 'bing_local'):
                    new += 1
        total_new += new
        if results:
            sources_used.append(f'bing({len(results)}/{new}new)')
        time.sleep(0.3)

    # Source 4: Geoapify
    if GEOAPIFY_KEY:
        results = search_geoapify(category, city, state, lat, lng)
        new = 0
        for r in results:
            if r.get('website') and r.get('name'):
                if import_lead(r['name'], r['website'], r.get('phone',''),
                              r.get('address',''), city, state, category,
                              r.get('rating'), r.get('reviews'), 'geoapify'):
                    new += 1
        total_new += new
        if results:
            sources_used.append(f'geo({len(results)}/{new}new)')

    return {'new': total_new, 'sources': sources_used, 'category': category, 'city': city}


def run_multi_source(max_combos=10):
    """Run multi-source scraping across categories and cities."""
    log_data = load_log()
    searched = set(log_data.get('searched_combos', []))
    total_new = 0
    combos_run = 0

    for cat in CATEGORIES:
        for city, state, lat, lng in CITIES_OR:
            key = f'{cat}|{city}'
            if key in searched:
                continue
            if combos_run >= max_combos:
                save_log(log_data)
                return {'new_leads': total_new, 'combos': combos_run}

            result = scrape_all_sources(cat, city, state, lat, lng)
            total_new += result['new']
            combos_run += 1

            if result['new'] > 0:
                log.info(f'{cat} in {city}: +{result["new"]} new [{" ".join(result["sources"])}]')

            searched.add(key)
            log_data['searched_combos'] = list(searched)
            log_data['stats']['total_new'] = log_data['stats'].get('total_new', 0) + result['new']
            log_data['stats']['last_run'] = datetime.utcnow().isoformat()

            if combos_run % 5 == 0:
                save_log(log_data)

    save_log(log_data)
    return {'new_leads': total_new, 'combos': combos_run}


if __name__ == '__main__':
    import sys
    max_c = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    print(f'Sources: Serper={bool(SERPER_KEY)} Foursquare={bool(FOURSQUARE_KEY)} Bing={bool(BING_KEY)} Geoapify={bool(GEOAPIFY_KEY)}')
    result = run_multi_source(max_combos=max_c)
    print(json.dumps(result, indent=2))
