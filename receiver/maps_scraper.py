#!/usr/bin/env python3
"""Google Maps Category Scraper v1.0
Searches Google Maps/Places by trade category + city.
Returns businesses that ALREADY HAVE websites.
This is the highest-value lead source — pre-enriched with web presence.

Usage:
  python3 maps_scraper.py --once          # Run all categories/cities once
  python3 maps_scraper.py --category plumber --city Portland
  python3 maps_scraper.py --continuous     # Run continuously with hourly cycles
"""
import httpx
import os
import sys
import json
import time
import logging
from datetime import datetime
from urllib.parse import urlparse
from difflib import SequenceMatcher
from pathlib import Path

import db

logging.basicConfig(level=logging.INFO, format='%(asctime)s [MAPS] %(message)s')
log = logging.getLogger('maps_scraper')

PLACES_KEY = os.environ.get('GOOGLE_PLACES_KEY', 'AIzaSyARIS8p7XpdzHxnkaS9EyxraH3LKnk1sxE')
PLACES_URL = 'https://maps.googleapis.com/maps/api/place'

# High-value service business categories for MW Development
CATEGORIES = [
    'plumber', 'HVAC contractor', 'electrician', 'roofer', 'roofing contractor',
    'landscaper', 'landscaping company', 'painter', 'painting contractor',
    'handyman', 'pest control', 'carpet cleaner', 'carpet cleaning',
    'garage door repair', 'fence contractor', 'fencing company',
    'flooring contractor', 'window installer', 'window replacement',
    'concrete contractor', 'drywall contractor', 'insulation contractor',
    'siding contractor', 'solar installer', 'tree service',
    'cleaning service', 'house cleaner', 'janitorial service',
    'general contractor', 'home remodeling', 'kitchen remodeling',
    'bathroom remodeling', 'deck builder', 'pressure washing',
    'gutter cleaning', 'appliance repair',
]

# Oregon + SW Washington cities, ordered by population
CITIES = [
    ('Portland', 'OR'), ('Salem', 'OR'), ('Eugene', 'OR'), ('Gresham', 'OR'),
    ('Hillsboro', 'OR'), ('Beaverton', 'OR'), ('Bend', 'OR'), ('Medford', 'OR'),
    ('Springfield', 'OR'), ('Corvallis', 'OR'), ('Albany', 'OR'), ('Tigard', 'OR'),
    ('Lake Oswego', 'OR'), ('Oregon City', 'OR'), ('Tualatin', 'OR'),
    ('West Linn', 'OR'), ('Milwaukie', 'OR'), ('Sherwood', 'OR'),
    ('Wilsonville', 'OR'), ('Newberg', 'OR'), ('McMinnville', 'OR'),
    ('Canby', 'OR'), ('Clackamas', 'OR'), ('Troutdale', 'OR'),
    ('Redmond', 'OR'), ('Grants Pass', 'OR'), ('Roseburg', 'OR'),
    ('Vancouver', 'WA'), ('Camas', 'WA'), ('Battle Ground', 'WA'),
]

# Track what we've already searched to avoid duplicate API calls
SEARCH_LOG = Path('/data/maps_search_log.json')


def load_search_log():
    if SEARCH_LOG.exists():
        with open(SEARCH_LOG) as f:
            return json.load(f)
    return {'searched': [], 'total_queries': 0, 'total_found': 0}


def save_search_log(log_data):
    SEARCH_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(SEARCH_LOG, 'w') as f:
        json.dump(log_data, f, indent=2)


def text_search(query: str, page_token: str = None) -> dict:
    """Search Google Maps via Text Search API. Returns up to 20 results."""
    params = {
        'query': query,
        'key': PLACES_KEY,
        'type': 'establishment',
    }
    if page_token:
        params['pagetoken'] = page_token

    try:
        r = httpx.get(f'{PLACES_URL}/textsearch/json', params=params, timeout=15)
        return r.json()
    except Exception as e:
        log.error(f'Text search error: {e}')
        return {}


def get_details(place_id: str) -> dict:
    """Get website, phone, rating for a place."""
    try:
        r = httpx.get(f'{PLACES_URL}/details/json', params={
            'place_id': place_id,
            'fields': 'website,formatted_phone_number,rating,user_ratings_total,name,formatted_address,types,url',
            'key': PLACES_KEY
        }, timeout=10)
        return r.json().get('result', {})
    except Exception as e:
        log.error(f'Details error for {place_id}: {e}')
        return {}


def import_maps_result(details: dict, category: str, city: str, state: str) -> bool:
    """Import a Google Maps result into the leads database."""
    website = details.get('website', '')
    name = details.get('name', '')
    phone = details.get('formatted_phone_number', '')
    address = details.get('formatted_address', '')
    rating = details.get('rating')
    reviews = details.get('user_ratings_total')

    if not website or not name:
        return False

    # Extract domain
    parsed = urlparse(website)
    domain = parsed.netloc.lower().replace('www.', '')
    if not domain:
        return False

    # Check if already exists
    existing = db.get_lead_by_domain(domain)
    if existing:
        # Update with Maps data if we have better info
        updates = {}
        if rating and not existing.get('google_rating'):
            updates['google_rating'] = float(rating)
        if reviews and not existing.get('google_review_count'):
            updates['google_review_count'] = int(reviews)
        if category and not existing.get('category'):
            updates['category'] = category
        if updates:
            db.update_lead(existing['id'], updates)
        return False  # Not a new lead

    # Parse address components
    addr_parts = address.split(', ') if address else []
    street = addr_parts[0] if len(addr_parts) > 0 else ''
    addr_city = addr_parts[1] if len(addr_parts) > 1 else city
    state_zip = addr_parts[2] if len(addr_parts) > 2 else state
    addr_state = state_zip.split()[0] if state_zip else state
    addr_zip = state_zip.split()[1] if len(state_zip.split()) > 1 else ''

    # Insert
    try:
        with db.get_conn() as conn:
            cur = conn.cursor()
            cur.execute('''INSERT INTO businesses
                (domain, business_name, source_url, primary_phone, phones,
                 address, city, state, zip_code,
                 google_rating, google_review_count, category,
                 place_id, source, pipeline_status, enrichment_status,
                 created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                ON CONFLICT (domain) DO NOTHING
            ''', (
                domain, name, website, phone, [phone] if phone else [],
                street, addr_city, addr_state, addr_zip,
                float(rating) if rating else None,
                int(reviews) if reviews else None,
                category,
                details.get('place_id', ''),
                'google_maps', 'new', 'pending'
            ))
            return cur.rowcount > 0
    except Exception as e:
        log.error(f'Import error for {name}: {e}')
        return False


def scrape_category_city(category: str, city: str, state: str = 'OR') -> dict:
    """Scrape all results for one category + city combination."""
    query = f'{category} in {city} {state}'
    log.info(f'Searching: {query}')

    new_leads = 0
    total_results = 0
    page_token = None

    # Google returns up to 60 results across 3 pages of 20
    for page in range(3):
        data = text_search(query, page_token)
        results = data.get('results', [])
        if not results:
            break

        total_results += len(results)

        for result in results:
            place_id = result.get('place_id')
            if not place_id:
                continue

            details = get_details(place_id)
            if details.get('website'):
                if import_maps_result(details, category, city, state):
                    new_leads += 1
                    log.info(f'  NEW: {details.get("name")} -> {details.get("website")}')

            time.sleep(0.1)  # Rate limit

        page_token = data.get('next_page_token')
        if not page_token:
            break
        time.sleep(2)  # Google requires 2s delay before next_page_token works

    return {'query': query, 'results': total_results, 'new_leads': new_leads}


def run_full_scrape(categories=None, cities=None, max_queries=100):
    """Run a full scrape across categories and cities."""
    cats = categories or CATEGORIES
    cits = cities or CITIES
    search_log = load_search_log()
    searched = set(search_log.get('searched', []))

    total_new = 0
    queries_run = 0

    for cat in cats:
        for city, state in cits:
            key = f'{cat}|{city}|{state}'
            if key in searched:
                continue
            if queries_run >= max_queries:
                log.info(f'Hit query limit ({max_queries}). Stopping.')
                save_search_log(search_log)
                return {'new_leads': total_new, 'queries': queries_run}

            result = scrape_category_city(cat, city, state)
            total_new += result['new_leads']
            queries_run += 1

            searched.add(key)
            search_log['searched'] = list(searched)
            search_log['total_queries'] = search_log.get('total_queries', 0) + 1
            search_log['total_found'] = search_log.get('total_found', 0) + result['new_leads']
            search_log['last_run'] = datetime.utcnow().isoformat()

            # Save progress periodically
            if queries_run % 10 == 0:
                save_search_log(search_log)
                log.info(f'Progress: {queries_run} queries, {total_new} new leads')

    save_search_log(search_log)
    log.info(f'Full scrape done: {queries_run} queries, {total_new} new leads')
    return {'new_leads': total_new, 'queries': queries_run}


if __name__ == '__main__':
    if '--category' in sys.argv and '--city' in sys.argv:
        cat_idx = sys.argv.index('--category')
        city_idx = sys.argv.index('--city')
        cat = sys.argv[cat_idx + 1]
        city = sys.argv[city_idx + 1]
        result = scrape_category_city(cat, city)
        print(json.dumps(result, indent=2))
    elif '--continuous' in sys.argv:
        while True:
            result = run_full_scrape(max_queries=50)
            log.info(f'Cycle: {result}')
            if result['queries'] == 0:
                log.info('All category/city combos searched. Sleeping 24h.')
                time.sleep(86400)
            else:
                log.info('Sleeping 1h before next batch.')
                time.sleep(3600)
    else:
        max_q = 50
        if '--max' in sys.argv:
            max_q = int(sys.argv[sys.argv.index('--max') + 1])
        result = run_full_scrape(max_queries=max_q)
        print(json.dumps(result, indent=2))
