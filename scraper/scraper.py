"""Lead Scraper Worker v1.0
from browser_fallback import scrape_with_fallback
Crawls websites for contact info: phones, emails, owner names, social links.
Runs on Contabo, pushes results to VPS1 receiver.
"""
import httpx
import re
import logging
import asyncio
import time
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, unquote
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s')
log = logging.getLogger('scraper')

# Patterns
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
PHONE_RE = re.compile(r'(?:\+?1[-.]?)?\(?\d{3}\)?[-.]?\d{3}[-.]?\d{4}')
SOCIAL_PATTERNS = {
    'facebook': re.compile(r'https?://(?:www\.)?facebook\.com/[\w.\-/]+', re.I),
    'instagram': re.compile(r'https?://(?:www\.)?instagram\.com/[\w.\-/]+', re.I),
    'linkedin': re.compile(r'https?://(?:www\.)?linkedin\.com/(?:company|in)/[\w.\-/]+', re.I),
    'twitter': re.compile(r'https?://(?:www\.)?(?:twitter|x)\.com/[\w.\-/]+', re.I),
    'youtube': re.compile(r'https?://(?:www\.)?youtube\.com/[\w.\-/@]+', re.I),
}

JUNK_EMAILS = {'email@example.com', 'info@example.com', 'user@email.com', 'admin@wordpress.com',
               'noreply@', 'no-reply@', 'webmaster@', 'postmaster@', 'hostmaster@',
               'user@domain.com', 'your@email.com', 'marie.curie@', 'contact@beachsideresort.com',
               'mymail@mailservice.com', 'myemail@gmail.com', 'name@domain.com', 'you@yours.com'}

JUNK_DOMAINS = {'sentry.io', 'sentry-next.wixpress.com', 'wixpress.com', 'example.com',
                'domain.com', 'email.com', 'mailservice.com', 'test.com', 'sample.com',
                'placeholder.com', 'yoursite.com', 'website.com'}

CONTACT_PATHS = ['/contact', '/contact-us', '/about', '/about-us', '/our-team', '/team', '/staff']

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}


def clean_email(email: str) -> Optional[str]:
    email = email.strip().lower()
    if not email or '@' not in email:
        return None
    if any(j in email for j in JUNK_EMAILS):
        return None
    email_domain = email.split('@')[-1]
    if email_domain in JUNK_DOMAINS:
        return None
    if email.endswith(('.png', '.jpg', '.gif', '.svg', '.webp', '.avif', '.css', '.js')):
        return None
    return email


def clean_phone(raw: str) -> str:
    decoded = unquote(str(raw))
    digits = re.sub(r'[^\d]', '', decoded)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return decoded.strip()


def dedup_phones(phones: List[str]) -> List[str]:
    seen = set()
    result = []
    for p in phones:
        digits = re.sub(r'[^\d]', '', p)
        if len(digits) >= 7 and digits[-10:] not in seen:
            seen.add(digits[-10:])
            result.append(clean_phone(p))
    return result


def dedup_emails(emails: List[str]) -> List[str]:
    seen = set()
    result = []
    for e in emails:
        cleaned = clean_email(e)
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            result.append(cleaned)
    return result


def extract_meta(soup: BeautifulSoup) -> Dict:
    title = ''
    desc = ''
    if soup.title:
        title = soup.title.string or ''
    meta_desc = soup.find('meta', attrs={'name': 'description'})
    if meta_desc:
        desc = meta_desc.get('content', '')
    return {'title': title.strip(), 'description': desc.strip()}


def extract_owner_name(soup: BeautifulSoup, html: str) -> Dict:
    """Try to find owner/founder name from about page."""
    patterns = [
        re.compile(r'(?:owner|founder|ceo|president|principal)[:\s]*([A-Z][a-z]+ [A-Z][a-z]+)', re.I),
        re.compile(r'([A-Z][a-z]+ [A-Z][a-z]+)(?:[,\s]*(?:owner|founder|ceo|president|principal))', re.I),
    ]
    for pat in patterns:
        m = pat.search(html)
        if m:
            name = m.group(1).strip()
            if len(name) < 40 and ' ' in name:
                return {'name': name, 'source': 'website'}
    return {}


async def scrape_url(client: httpx.AsyncClient, url: str) -> Dict:
    """Scrape a single URL and its contact pages."""
    result = {
        'source_url': url,
        'final_url': '',
        'business_name': '',
        'phones': [],
        'emails': [],
        'social_links': {},
        'owner_info': {},
        'meta': {},
        'subpages_scraped': [],
        'scraped_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'website_quality': {},
    }

    all_phones = []
    all_emails = []
    all_social = {}
    owner_info = {}

    try:
        # Fetch main page
        r = await client.get(url, follow_redirects=True, timeout=15)
        result['final_url'] = str(r.url)
        html = r.text
        soup = BeautifulSoup(html, 'html.parser')

        result['meta'] = extract_meta(soup)
        result['business_name'] = result['meta'].get('title', '').split('|')[0].split('-')[0].strip()

        # Extract from main page
        all_emails.extend(EMAIL_RE.findall(html))
        all_phones.extend(PHONE_RE.findall(html))
        for platform, pattern in SOCIAL_PATTERNS.items():
            matches = pattern.findall(html)
            if matches:
                all_social[platform] = matches[0]

        result['subpages_scraped'].append(str(r.url))

        # Website quality signals
        quality = {}
        quality['https'] = str(r.url).startswith('https://')
        quality['mobile_friendly'] = bool(soup.find('meta', attrs={'name': 'viewport'}))
        quality['has_schema'] = 'application/ld+json' in html

        # Booking/scheduling detection
        booking_terms = ['book', 'schedul', 'appointment', 'reserv', 'quote', 'estimate',
                         'calendly', 'acuity', 'housecall', 'jobber', 'servicetitan']
        quality['has_booking'] = any(t in html.lower() for t in booking_terms)

        # Basic tech detection
        quality['uses_wordpress'] = 'wp-content' in html or 'wordpress' in html.lower()
        quality['uses_wix'] = 'wix.com' in html.lower()
        quality['uses_squarespace'] = 'squarespace' in html.lower()

        result['website_quality'] = quality

        # Scrape contact/about pages
        base = f"{r.url.scheme}://{r.url.host}"
        for path in CONTACT_PATHS:
            try:
                sub_url = urljoin(base, path)
                sr = await client.get(sub_url, follow_redirects=True, timeout=10)
                if sr.status_code == 200 and len(sr.text) > 500:
                    sub_html = sr.text
                    all_emails.extend(EMAIL_RE.findall(sub_html))
                    all_phones.extend(PHONE_RE.findall(sub_html))
                    for platform, pattern in SOCIAL_PATTERNS.items():
                        if platform not in all_social:
                            matches = pattern.findall(sub_html)
                            if matches:
                                all_social[platform] = matches[0]
                    if not owner_info:
                        owner_info = extract_owner_name(BeautifulSoup(sub_html, 'html.parser'), sub_html)
                    result['subpages_scraped'].append(sub_url)
                await asyncio.sleep(0.5)  # Polite delay
            except Exception:
                continue

        # Try owner from main page if not found
        if not owner_info:
            owner_info = extract_owner_name(soup, html)

    except httpx.TimeoutException:
        result['error'] = 'timeout'
    except httpx.ConnectError:
        result['error'] = 'connection_failed'
    except Exception as e:
        result['error'] = str(e)[:200]

    # Browser fallback for blocked sites
    if result.get('error') and result['error'] not in ('timeout',):
        try:
            fallback = await scrape_with_fallback(url, httpx_status=0, httpx_html='')
            if fallback.get('html') and len(fallback['html']) > 500:
                fb_soup = BeautifulSoup(fallback['html'], 'html.parser')
                fb_html = fallback['html']
                all_emails.extend(EMAIL_RE.findall(fb_html))
                all_phones.extend(PHONE_RE.findall(fb_html))
                for platform, pattern in SOCIAL_PATTERNS.items():
                    if platform not in all_social:
                        matches = pattern.findall(fb_html)
                        if matches:
                            all_social[platform] = matches[0]
                if not owner_info:
                    owner_info = extract_owner_name(fb_soup, fb_html)
                result['error'] = None  # Clear error since fallback worked
                result['used_browser'] = True
                log.info(f'Browser fallback success for {url}')
        except Exception as e:
            log.warning(f'Browser fallback error for {url}: {e}')

    result['phones'] = dedup_phones(all_phones)
    result['emails'] = dedup_emails(all_emails)
    result['social_links'] = all_social
    result['owner_info'] = owner_info

    return result


async def scrape_batch(urls: List[str], concurrency: int = 4) -> List[Dict]:
    """Scrape a batch of URLs with controlled concurrency."""
    sem = asyncio.Semaphore(concurrency)
    results = []

    async def _scrape(url):
        async with sem:
            async with httpx.AsyncClient(headers=HEADERS, verify=False) as client:
                return await scrape_url(client, url)

    tasks = [_scrape(url) for url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    cleaned = []
    for r in results:
        if isinstance(r, Exception):
            cleaned.append({'error': str(r)[:200]})
        else:
            cleaned.append(r)

    return cleaned
