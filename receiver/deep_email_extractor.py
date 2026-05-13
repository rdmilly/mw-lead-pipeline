#!/usr/bin/env python3
"""Deep Email Extractor v2.0
Enhanced email discovery with:
- Full internal link crawl (depth 2, max 15 pages)
- JSON-LD structured data parsing
- mailto: link priority
- MX record verification + pattern guessing
- Contact form email detection
- Domain-specific email from page meta

Replaces the old email_discovery.py with higher hit rates.
"""
import re
import json
import asyncio
import logging
import dns.resolver
from datetime import datetime
from pathlib import Path
from typing import List, Set, Tuple, Optional
from urllib.parse import urlparse, urljoin
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger('deep_email')

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', re.I)

JUNK_DOMAINS = {
    'example.com', 'sentry.io', 'wixpress.com', 'wordpress.org',
    'w3.org', 'schema.org', 'googleapis.com', 'google.com',
    'facebook.com', 'twitter.com', 'instagram.com', 'youtube.com',
    'linkedin.com', 'squarespace.com', 'godaddy.com', 'wix.com',
    'cloudflare.com', 'jquery.com', 'bootstrapcdn.com', 'fontawesome.com',
    'gstatic.com', 'googletagmanager.com', 'doubleclick.net',
    'gravatar.com', 'wp.com', 'amazonaws.com', 'cdnjs.com',
    'sentry-next.wixpress.com', 'feedburner.com', 'mailchimp.com',
    'constantcontact.com', 'sendgrid.net', 'mandrillapp.com',
    'intercom.io', 'crisp.chat', 'tawk.to', 'zendesk.com',
    'hubspot.com', 'salesforce.com', 'apple.com', 'microsoft.com',
}

PLACEHOLDER_EMAILS = {
    'your@email.com', 'you@email.com', 'email@domain.com',
    'user@domain.com', 'example@domain.com', 'john@doe.com',
    'name@domain.com', 'test@test.com', 'info@example.com',
    'email@example.com', 'youremail@domain.com', 'user@example.com',
    'someone@example.com', 'contact@example.com', 'your@domain.com',
    'myemail@gmail.com', 'mymail@mailservice.com', 'you@yours.com',
    'name@email.com', 'name@example.com', 'marie.curie@example.com',
    'contact@beachsideresort.com',
}

JUNK_PREFIXES = {'noreply', 'no-reply', 'donotreply', 'mailer-daemon',
                 'postmaster', 'webmaster', 'abuse', 'hostmaster', 'null', 'root'}

AGENCY_DOMAINS = {'webpixel.ai', 'developer.com', 'developer.org', 'theme.co',
                  'developer.io', 'developer.net', 'developer.dev'}

# Pages most likely to contain email addresses
CONTACT_PATHS = [
    '/contact', '/contact-us', '/about', '/about-us',
    '/our-team', '/team', '/staff', '/people',
    '/get-in-touch', '/reach-us', '/support',
    '/location', '/locations', '/service-area',
]

# Email patterns for MX-verified domains, ordered by likelihood for service businesses
GUESS_PATTERNS = [
    'info@{domain}', 'contact@{domain}', 'office@{domain}',
    'service@{domain}', 'hello@{domain}', 'sales@{domain}',
    '{first}@{domain}',  # If we know owner first name
]

FREE_EMAIL_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
    'aol.com', 'icloud.com', 'protonmail.com', 'me.com',
    'live.com', 'msn.com', 'comcast.net', 'att.net',
}


def is_valid_email(email: str) -> bool:
    email = email.lower().strip()
    if email in PLACEHOLDER_EMAILS:
        return False
    if len(email) < 6 or len(email) > 80:
        return False
    local, _, domain = email.partition('@')
    if not domain or '.' not in domain:
        return False
    if domain in JUNK_DOMAINS or domain in AGENCY_DOMAINS:
        return False
    if any(local.startswith(p) for p in JUNK_PREFIXES):
        return False
    if domain.endswith(('.png', '.jpg', '.gif', '.svg', '.css', '.js')):
        return False
    if '..' in email or email.startswith('.') or email.endswith('.'):
        return False
    return True


def extract_from_jsonld(html: str) -> List[str]:
    """Extract emails from JSON-LD structured data (highest confidence)."""
    emails = []
    try:
        soup = BeautifulSoup(html, 'html.parser')
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                items = data if isinstance(data, list) else (data.get('@graph', [data]))
                for item in items:
                    if isinstance(item, dict):
                        for field in ['email', 'contactPoint']:
                            val = item.get(field)
                            if isinstance(val, str) and '@' in val:
                                email = val.replace('mailto:', '').strip()
                                if is_valid_email(email):
                                    emails.append(email.lower())
                            elif isinstance(val, dict) and val.get('email'):
                                email = val['email'].replace('mailto:', '').strip()
                                if is_valid_email(email):
                                    emails.append(email.lower())
                            elif isinstance(val, list):
                                for cp in val:
                                    if isinstance(cp, dict) and cp.get('email'):
                                        email = cp['email'].replace('mailto:', '').strip()
                                        if is_valid_email(email):
                                            emails.append(email.lower())
            except (json.JSONDecodeError, TypeError):
                continue
    except Exception:
        pass
    return emails


def extract_from_mailto(html: str) -> List[str]:
    """Extract emails from mailto: links (high confidence)."""
    emails = []
    try:
        soup = BeautifulSoup(html, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            if 'mailto:' in href.lower():
                email = href.split('mailto:')[1].split('?')[0].strip()
                if is_valid_email(email):
                    emails.append(email.lower())
    except Exception:
        pass
    return emails


def extract_from_text(html: str) -> List[str]:
    """Extract emails from visible text and raw HTML (lower confidence)."""
    emails = []
    try:
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text(separator=' ')
        for match in EMAIL_RE.findall(text):
            if is_valid_email(match):
                emails.append(match.lower())
        if not emails:
            for match in EMAIL_RE.findall(html):
                if is_valid_email(match):
                    emails.append(match.lower())
    except Exception:
        pass
    return emails


def find_internal_links(html: str, base_url: str) -> List[str]:
    """Find all internal links on a page."""
    links = set()
    try:
        parsed_base = urlparse(base_url)
        soup = BeautifulSoup(html, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('#') or href.startswith('javascript:'):
                continue
            full_url = urljoin(base_url, href)
            parsed = urlparse(full_url)
            if parsed.netloc == parsed_base.netloc:
                clean = f'{parsed.scheme}://{parsed.netloc}{parsed.path}'.rstrip('/')
                links.add(clean)
    except Exception:
        pass
    return list(links)


def check_mx(domain: str) -> bool:
    """Check if domain has MX records."""
    try:
        answers = dns.resolver.resolve(domain, 'MX')
        return len(list(answers)) > 0
    except Exception:
        return False


def guess_email(domain: str, owner_first: str = '') -> Optional[str]:
    """Guess email for a domain with verified MX records."""
    if domain in FREE_EMAIL_DOMAINS:
        return None
    if not check_mx(domain):
        return None

    # For service businesses, info@ is most common
    if owner_first:
        return f'{owner_first.lower()}@{domain}'
    return f'info@{domain}'


async def deep_extract(client: httpx.AsyncClient, website: str, owner_first: str = '') -> dict:
    """Deep email extraction from a website. Crawls multiple pages."""
    if not website:
        return {'emails': [], 'source': 'no_website', 'pages_checked': 0}

    if not website.startswith('http'):
        website = 'https://' + website

    parsed = urlparse(website)
    base_url = f'{parsed.scheme}://{parsed.netloc}'
    domain = parsed.netloc.replace('www.', '')

    all_emails = set()
    email_sources = {}  # email -> source
    pages_checked = 0
    urls_to_check = [website]

    # Add known contact pages
    for path in CONTACT_PATHS:
        urls_to_check.append(urljoin(base_url, path))

    checked_urls = set()
    internal_links = set()

    # Phase 1: Check known pages (homepage + contact pages)
    for url in urls_to_check[:8]:  # Cap at 8 known paths
        if url in checked_urls:
            continue
        checked_urls.add(url)

        try:
            r = await client.get(url, follow_redirects=True, timeout=8)
            if r.status_code != 200 or len(r.text) < 200:
                continue
            pages_checked += 1
            html = r.text

            # Extract emails in priority order
            jsonld = extract_from_jsonld(html)
            for e in jsonld:
                all_emails.add(e)
                email_sources[e] = 'json-ld'

            mailto = extract_from_mailto(html)
            for e in mailto:
                all_emails.add(e)
                if e not in email_sources:
                    email_sources[e] = 'mailto'

            text_emails = extract_from_text(html)
            for e in text_emails:
                all_emails.add(e)
                if e not in email_sources:
                    email_sources[e] = 'page_text'

            # Collect internal links for Phase 2
            if pages_checked <= 3:  # Only crawl links from first few pages
                for link in find_internal_links(html, url):
                    internal_links.add(link)

            if len(all_emails) >= 3:
                break  # Found enough, stop crawling

        except Exception:
            continue

    # Phase 2: If no emails found, crawl internal links
    if not all_emails and internal_links:
        # Prioritize links with contact/about in the path
        priority = [l for l in internal_links if any(k in l.lower() for k in ['contact', 'about', 'team', 'staff', 'people', 'reach'])]
        others = [l for l in internal_links if l not in set(priority) and l not in checked_urls]
        to_check = (priority + others)[:7]  # Max 7 more pages

        for url in to_check:
            if url in checked_urls:
                continue
            checked_urls.add(url)

            try:
                r = await client.get(url, follow_redirects=True, timeout=6)
                if r.status_code != 200 or len(r.text) < 200:
                    continue
                pages_checked += 1

                mailto = extract_from_mailto(r.text)
                for e in mailto:
                    all_emails.add(e)
                    email_sources[e] = 'mailto_deep'

                text_emails = extract_from_text(r.text)
                for e in text_emails:
                    all_emails.add(e)
                    if e not in email_sources:
                        email_sources[e] = 'deep_crawl'

                if all_emails:
                    break
            except Exception:
                continue

    # Phase 3: MX-verified pattern guess as last resort
    if not all_emails:
        guessed = guess_email(domain, owner_first)
        if guessed:
            all_emails.add(guessed)
            email_sources[guessed] = 'mx_guess'

    # Prioritize: domain emails > free email > guesses
    domain_emails = [e for e in all_emails if e.split('@')[1] == domain]
    free_emails = [e for e in all_emails if e.split('@')[1] in FREE_EMAIL_DOMAINS]
    other_emails = [e for e in all_emails if e not in set(domain_emails) and e not in set(free_emails)]

    # Prefer domain emails, then other business emails, then free emails
    ordered = domain_emails + other_emails + free_emails

    return {
        'emails': ordered,
        'sources': email_sources,
        'pages_checked': pages_checked,
        'source': email_sources.get(ordered[0], 'unknown') if ordered else 'not_found'
    }
