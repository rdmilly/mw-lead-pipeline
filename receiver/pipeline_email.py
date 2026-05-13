#!/usr/bin/env python3
"""Pipeline Email + Intelligence Step
Replaces the old JSON sync bridge email discovery with:
- Deep email extraction (JSON-LD, mailto, deep crawl, MX guess)
- Website intelligence (social links, tech stack, team page, booking systems)
- Direct PostgreSQL updates (no JSON file intermediary)
"""
import asyncio
import logging
import httpx
from typing import List, Dict
from deep_email_extractor import deep_extract
import website_intelligence
import db

log = logging.getLogger('pipeline_email')

# Social media URL patterns
SOCIAL_PATTERNS = {
    'facebook': ['facebook.com/', 'fb.com/'],
    'instagram': ['instagram.com/'],
    'twitter': ['twitter.com/', 'x.com/'],
    'linkedin': ['linkedin.com/company/', 'linkedin.com/in/'],
    'youtube': ['youtube.com/'],
    'tiktok': ['tiktok.com/@'],
    'nextdoor': ['nextdoor.com/'],
    'yelp': ['yelp.com/biz/'],
}

# Booking/scheduling platforms (indicates established business)
BOOKING_PLATFORMS = [
    'calendly.com', 'acuityscheduling.com', 'squareup.com/appointments',
    'setmore.com', 'booksy.com', 'vagaro.com', 'housecallpro.com',
    'jobber.com', 'servicetitan.com', 'fieldedge.com',
]

# Tech stack indicators
TECH_INDICATORS = {
    'wordpress': ['wp-content', 'wp-includes', 'wordpress'],
    'wix': ['wixsite.com', '_wix_browser_sess', 'wix.com'],
    'squarespace': ['squarespace.com', 'sqsp.net', 'squarespace-cdn'],
    'godaddy': ['godaddysites.com', 'secureserver.net'],
    'weebly': ['weebly.com'],
    'shopify': ['shopify.com', 'cdn.shopify.com'],
}


def detect_social_links(html: str) -> Dict[str, str]:
    """Extract social media profile URLs from HTML."""
    social = {}
    html_lower = html.lower()
    import re
    for platform, patterns in SOCIAL_PATTERNS.items():
        for pattern in patterns:
            if pattern in html_lower:
                # Extract the full URL
                regex = re.compile(r'https?://(?:www\.)?' + re.escape(pattern) + r'[\w\-./]+', re.I)
                matches = regex.findall(html)
                if matches:
                    url = matches[0].rstrip('/')
                    # Filter out share/intent URLs
                    if 'share' not in url.lower() and 'intent' not in url.lower():
                        social[platform] = url
                        break
    return social


def detect_tech_stack(html: str) -> str:
    """Detect which website builder/CMS is used."""
    html_lower = html.lower()
    for tech, indicators in TECH_INDICATORS.items():
        for indicator in indicators:
            if indicator in html_lower:
                return tech
    return 'custom'


def detect_booking(html: str) -> bool:
    """Check if the business uses a booking/scheduling platform."""
    html_lower = html.lower()
    return any(platform in html_lower for platform in BOOKING_PLATFORMS)


def estimate_employees(html: str, reviews: int = 0) -> int:
    """Rough employee estimate from web signals."""
    import re
    # Check for team page indicators
    team_keywords = ['our team', 'meet the team', 'our staff', 'our crew',
                     'team members', 'about us', 'our people']
    has_team_page = any(kw in html.lower() for kw in team_keywords)

    # Count team member indicators (headshots, bios)
    headshot_count = len(re.findall(r'<img[^>]*(?:team|staff|employee|headshot|portrait)', html, re.I))

    if headshot_count > 10:
        return headshot_count
    elif headshot_count > 0:
        return max(headshot_count, 3)
    elif has_team_page:
        return 5  # Has team page but no individual photos
    elif reviews and reviews > 100:
        return 8  # Lots of reviews = established business
    elif reviews and reviews > 30:
        return 4
    else:
        return 2  # Default: small operation


async def process_lead(client: httpx.AsyncClient, lead: Dict) -> Dict:
    """Run deep email extraction + intelligence on a single lead."""
    website = lead.get('source_url', '')
    owner_name = lead.get('owner_name', '')
    owner_first = owner_name.split()[0] if owner_name and ' ' in owner_name else ''
    reviews = lead.get('google_review_count', 0) or 0

    result = {
        'id': lead['id'],
        'emails': [],
        'social_links': {},
        'tech_stack': None,
        'has_booking': False,
        'estimated_employees': None,
    }

    if not website or not website.startswith('http'):
        return result

    # Deep email extraction
    email_result = await deep_extract(client, website, owner_first)
    result['emails'] = email_result.get('emails', [])
    result['email_source'] = email_result.get('source', 'not_found')
    result['pages_checked'] = email_result.get('pages_checked', 0)

    # Website intelligence (grab homepage HTML for analysis)
    try:
        r = await client.get(website, follow_redirects=True, timeout=8)
        if r.status_code == 200 and len(r.text) > 500:
            html = r.text
            result['social_links'] = detect_social_links(html)
            result['tech_stack'] = detect_tech_stack(html)
            result['has_booking'] = detect_booking(html)
            result['estimated_employees'] = estimate_employees(html, reviews)

            # Run all intelligence layers
            intel = await website_intelligence.analyze(client, html, website, {
                'google_rating': lead.get('google_rating'),
                'google_review_count': reviews,
                'source_url': website,
                'primary_phone': lead.get('primary_phone'),
                'category': lead.get('category'),
                'social_links': {},
            })
            result['intelligence'] = intel
    except Exception as e:
        log.warning(f"Intelligence error for {website}: {e}")
        pass

    return result


async def run_deep_enrichment(limit: int = 50) -> Dict:
    """Run deep email + intelligence enrichment on leads with websites but no email."""
    # Get leads needing enrichment
    try:
        with db.get_conn() as conn:
            cur = conn.cursor()
            cur.execute('''
                SELECT id, domain, business_name, source_url, owner_name,
                       google_review_count, primary_email
                FROM businesses
                WHERE source_url IS NOT NULL
                  AND source_url LIKE 'http%%'
                  AND (primary_email IS NULL OR primary_email = '')
                  AND enrichment_status = 'pending'
                ORDER BY google_review_count DESC NULLS LAST,
                         lead_score DESC NULLS LAST
                LIMIT %s
            ''', (limit,))
            columns = [desc[0] for desc in cur.description]
            leads = [dict(zip(columns, row)) for row in cur.fetchall()]
    except Exception as e:
        log.error(f'Failed to fetch leads: {e}')
        return {'error': str(e)}

    if not leads:
        log.info('No leads needing deep enrichment')
        return {'processed': 0, 'found_emails': 0}

    log.info(f'Deep enriching {len(leads)} leads')

    found_emails = 0
    found_social = 0
    errors = 0

    async with httpx.AsyncClient(
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
        verify=False,
        follow_redirects=True,
        timeout=10
    ) as client:
        # Process in batches of 5 concurrent
        for i in range(0, len(leads), 5):
            batch = leads[i:i+5]
            tasks = [process_lead(client, lead) for lead in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    errors += 1
                    continue

                lead_id = result['id']
                updates = {'enrichment_status': 'complete'}

                # Update emails
                if result['emails']:
                    updates['primary_email'] = result['emails'][0]
                    updates['emails'] = result['emails']
                    found_emails += 1
                    log.info(f'  EMAIL: {result["emails"][0]} (via {result.get("email_source", "?")})')

                # Update social links
                if result['social_links']:
                    updates['social_links'] = result['social_links']
                    found_social += 1

                # Update intelligence
                if result['tech_stack']:
                    updates['tech_stack'] = result['tech_stack']
                if result['has_booking']:
                    updates['has_booking_system'] = True
                if result.get('intelligence'):
                    intel = result['intelligence']
                    for field in ('website_quality_score', 'is_mobile_friendly', 'has_ssl',
                                  'domain_age_years', 'domain_created', 'email_provider',
                                  'gbp_completeness', 'website_stale', 'days_since_archived'):
                        if intel.get(field) is not None:
                            updates[field] = intel[field]
                    for arr_field in ('website_issues', 'gbp_missing', 'pain_points'):
                        if intel.get(arr_field):
                            updates[arr_field] = intel[arr_field]
                if result['estimated_employees']:
                    updates['estimated_employees'] = result['estimated_employees']

                # Write to DB
                try:
                    db.update_lead(lead_id, updates)
                except Exception as e:
                    log.error(f'  Update error for {lead_id}: {e}')
                    errors += 1

    summary = {
        'processed': len(leads),
        'found_emails': found_emails,
        'found_social': found_social,
        'errors': errors,
        'email_rate': f'{(found_emails/len(leads)*100):.1f}%' if leads else '0%'
    }
    log.info(f'Deep enrichment done: {summary}')
    return summary


def run_sync(limit: int = 50) -> Dict:
    """Synchronous wrapper for pipeline_auto.py to call."""
    return asyncio.run(run_deep_enrichment(limit))
