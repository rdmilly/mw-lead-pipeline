"""LinkedIn Owner Profile Enricher
import serper_tracker
Finds LinkedIn personal profile URLs for business owners.

Phase 1: Google search (no LinkedIn login needed)
  - Searches: site:linkedin.com/in/ "owner name" "business name" city
  - Extracts profile URL from search results
  - Stores linkedin_url in businesses table

Phase 2 (future): Direct LinkedIn scraping with Camoufox
  - Requires LinkedIn session cookies
  - Gets: title, company page, employee count, connections

Run standalone: python3 linkedin_enricher.py [--limit 20]
Or called from pipeline runner.
"""
import asyncio
import logging
import os
import re
import time
import httpx
from urllib.parse import quote_plus, urlparse
from typing import Dict, Optional, List

log = logging.getLogger('linkedin')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [LINKEDIN] %(message)s')

RECEIVER_URL = os.environ.get('RECEIVER_URL', 'https://leads.millyweb.com')
API_KEY = os.environ.get('API_KEY', 'milly-dev-key-change-me')
HEADERS = {'X-API-Key': API_KEY, 'Content-Type': 'application/json'}

# Rate limiting
SEARCH_DELAY = 5  # seconds between Google searches to avoid captcha
MAX_PER_RUN = 20  # max profiles per enrichment run

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
]


def extract_linkedin_url(html: str, owner_name: str) -> Optional[str]:
    """Extract LinkedIn profile URL from Google search results."""
    # Match linkedin.com/in/ URLs
    pattern = re.compile(r'https?://(?:www\.)?linkedin\.com/in/[\w\-]+/?', re.I)
    matches = pattern.findall(html)

    if not matches:
        return None

    # Deduplicate
    seen = set()
    unique = []
    for m in matches:
        clean = m.rstrip('/').lower()
        if clean not in seen:
            seen.add(clean)
            unique.append(m)

    # Return first match (Google ranks by relevance)
    return unique[0] if unique else None


async def search_google_for_linkedin(owner_name: str, business_name: str, city: str = '', state: str = 'OR') -> Optional[str]:
    """Search for a LinkedIn profile via Serper.dev API (2,500 free/month)."""
    serper_key = os.environ.get('SERPER_API_KEY', '')

    if serper_key and not serper_tracker.has_budget():
        log.warning("Serper quota exhausted - skipping LinkedIn enrichment")
        return None
    if not owner_name or len(owner_name.strip()) < 3:
        return None

    query = f'site:linkedin.com/in/ "{owner_name}" "{business_name}"'
    if city:
        query += f' {city}'

    if serper_key:
        # Use Serper.dev API
        try:
            async with httpx.AsyncClient() as client:
                r = await client.post('https://google.serper.dev/search',
                    headers={'X-API-KEY': serper_key, 'Content-Type': 'application/json'},
                    json={'q': query, 'num': 5}, timeout=10)
                serper_tracker.track_query(query)
                if r.status_code == 200:
                    data = r.json()
                    for result in data.get('organic', []):
                        link = result.get('link', '')
                        if 'linkedin.com/in/' in link:
                            return link
                else:
                    log.warning(f'Serper returned {r.status_code}')
        except Exception as e:
            log.error(f'Serper search error: {e}')
        return None
    else:
        # Fallback to direct Google (may get CAPTCHA from datacenter IPs)
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(f'https://www.google.com/search?q={quote_plus(query)}&num=5',
                    headers={'User-Agent': USER_AGENTS[0]}, timeout=10, follow_redirects=True)
                if r.status_code == 200:
                    return extract_linkedin_url(r.text, owner_name)
        except Exception as e:
            log.error(f'Google search error: {e}')
        return None


async def search_with_camoufox(owner_name: str, business_name: str, city: str = '') -> Optional[str]:
    """Fallback: use Camoufox for Google search if httpx gets blocked."""
    try:
        from browser_fallback import scrape_with_browser
        query = f'site:linkedin.com/in/ "{owner_name}" "{business_name}" {city}'.strip()
        search_url = f'https://www.google.com/search?q={quote_plus(query)}&num=5'
        html = await scrape_with_browser(search_url, timeout=30)
        if html:
            return extract_linkedin_url(html, owner_name)
    except Exception as e:
        log.error(f'Camoufox search error: {e}')
    return None


async def enrich_leads(limit: int = MAX_PER_RUN) -> Dict:
    """Find LinkedIn profiles for leads that have owner names but no LinkedIn URL."""
    # Get leads needing LinkedIn enrichment from receiver
    try:
        r = httpx.get(f'{RECEIVER_URL}/api/v1/leads/master',
                      headers=HEADERS,
                      params={'limit': 500, 'sort_by': 'lead_score', 'order': 'DESC'},
                      timeout=30)
        data = r.json()
        leads = data.get('leads', [])
    except Exception as e:
        log.error(f'Failed to fetch leads: {e}')
        return {'error': str(e)}

    # Filter: has owner_name, no linkedin_url
    candidates = []
    for l in leads:
        owner = l.get('owner_name', '').strip()
        linkedin = (l.get('linkedin_url') or '').strip()
        if owner and len(owner) > 3 and ' ' in owner and not linkedin:
            candidates.append(l)

    candidates = candidates[:limit]
    log.info(f'Enriching {len(candidates)} leads with LinkedIn profiles')

    found = 0
    not_found = 0
    errors = 0
    rate_limited = False

    for lead in candidates:
        if rate_limited:
            break

        owner = lead['owner_name']
        biz = lead.get('business_name', '')
        city = lead.get('city', '')
        lead_id = lead.get('id')

        # Try Google search first
        linkedin_url = await search_google_for_linkedin(owner, biz, city)

        # Fallback to Camoufox if Google blocked us
        if linkedin_url is None:
            linkedin_url = await search_with_camoufox(owner, biz, city)

        if linkedin_url:
            # Update lead via receiver API
            try:
                r = httpx.put(f'{RECEIVER_URL}/api/v1/leads/master/{lead_id}',
                              headers=HEADERS,
                              json={'linkedin_url': linkedin_url},
                              timeout=10)
                if r.status_code == 200:
                    found += 1
                    log.info(f'  FOUND: {owner} ({biz}) -> {linkedin_url}')
                else:
                    log.warning(f'  Failed to update lead {lead_id}: {r.status_code}')
            except Exception as e:
                errors += 1
                log.error(f'  Update error: {e}')
        else:
            not_found += 1
            log.info(f'  NOT FOUND: {owner} ({biz})')

        # Rate limit delay
        await asyncio.sleep(SEARCH_DELAY)

    result = {
        'total_checked': len(candidates),
        'found': found,
        'not_found': not_found,
        'errors': errors,
        'rate_limited': rate_limited
    }
    log.info(f'LinkedIn enrichment done: {result}')
    return result


if __name__ == '__main__':
    import sys
    limit = MAX_PER_RUN
    if '--limit' in sys.argv:
        idx = sys.argv.index('--limit')
        limit = int(sys.argv[idx + 1])
    asyncio.run(enrich_leads(limit))
