# MillyExt - Email Discovery Engine v2
# Fast concurrent website scraping for email extraction

import re
import json
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urlparse, urljoin
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger("email_discovery")

DATA_DIR = Path("/data")
LEADS_DIR = DATA_DIR / "leads"
MASTER_FILE = LEADS_DIR / "master_leads.json"
DISCOVERY_LOG = DATA_DIR / "logs" / "email_discovery.jsonl"

EMAIL_RE = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

JUNK_DOMAINS = {
    'example.com', 'sentry.io', 'wixpress.com', 'wordpress.org',
    'w3.org', 'schema.org', 'googleapis.com', 'google.com',
    'facebook.com', 'twitter.com', 'instagram.com', 'youtube.com',
    'linkedin.com', 'squarespace.com', 'godaddy.com', 'wix.com',
    'cloudflare.com', 'jquery.com', 'bootstrapcdn.com', 'fontawesome.com',
    'gstatic.com', 'googletagmanager.com', 'doubleclick.net',
    'gravatar.com', 'wp.com', 'amazonaws.com', 'cdnjs.com',
}

PLACEHOLDER_EMAILS = {
    'your@email.com', 'you@email.com', 'email@domain.com',
    'user@domain.com', 'example@domain.com', 'john@doe.com',
    'jane@doe.com', 'name@domain.com', 'test@test.com',
    'info@example.com', 'contact@example.com', 'your@domain.com',
    'email@example.com', 'name@email.com', 'name@example.com',
    'someone@example.com', 'youremail@domain.com', 'user@example.com',
}

AGENCY_DOMAINS = {
    'webpixel.ai', 'developer.com', 'developer.org', 'theme.co',
    'developer.io', 'developer.net', 'developer.dev',
}

JUNK_PREFIXES = {
    'noreply', 'no-reply', 'donotreply', 'mailer-daemon',
    'postmaster', 'webmaster', 'abuse', 'hostmaster',
    'null', 'root',
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
    if domain in JUNK_DOMAINS:
        return False
    if any(local.startswith(p) for p in JUNK_PREFIXES):
        return False
    if domain.endswith(('.png', '.jpg', '.gif', '.svg', '.css', '.js')):
        return False
    if '..' in email or email.startswith('.') or email.endswith('.'):
        return False
    if domain in AGENCY_DOMAINS:
        return False
    return True


def extract_emails_from_html(html: str) -> List[str]:
    found = set()
    # 1. mailto: links (highest confidence)
    soup = BeautifulSoup(html, 'html.parser')
    for a in soup.find_all('a', href=True):
        href = a['href']
        if 'mailto:' in href.lower():
            email = href.split('mailto:')[1].split('?')[0].strip()
            if is_valid_email(email):
                found.add(email.lower())
    if found:
        return list(found)  # mailto is best signal, return early
    # 2. Regex on visible text + raw HTML
    text = soup.get_text(separator=' ')
    for match in EMAIL_RE.findall(text):
        if is_valid_email(match):
            found.add(match.lower())
    if not found:
        for match in EMAIL_RE.findall(html):
            if is_valid_email(match):
                found.add(match.lower())
    return list(found)


async def discover_one(client: httpx.AsyncClient, lead: dict) -> Tuple[List[str], str]:
    """Discover emails for one lead. Fast: homepage then /contact only."""
    website = lead.get('website', '')
    if not website:
        return [], 'no_website'
    if not website.startswith('http'):
        website = 'https://' + website

    parsed = urlparse(website)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    all_emails = set()

    # Try homepage first, then /contact only
    for url in [website, urljoin(base_url, '/contact'), urljoin(base_url, '/contact-us')]:
        try:
            resp = await client.get(url, follow_redirects=True, timeout=8.0)
            if resp.status_code == 200:
                emails = extract_emails_from_html(resp.text)
                all_emails.update(emails)
                if all_emails:
                    break  # Found emails, stop crawling
        except Exception:
            continue

    return list(all_emails), 'found' if all_emails else 'not_found'


async def run_email_discovery(limit: int = 50):
    """Run email discovery concurrently on leads missing emails."""
    with open(MASTER_FILE) as f:
        master = json.load(f)

    leads = master.get('leads', {})
    targets = []
    for domain, lead in leads.items():
        if not lead.get('emails') and lead.get('website'):
            if not lead.get('_email_discovery_at'):
                targets.append((domain, lead))
    targets = targets[:limit]
    logger.info(f"Email discovery: {len(targets)} leads to process")

    found_count = 0
    error_count = 0
    total_emails = 0

    async with httpx.AsyncClient(
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'},
        verify=False,
        limits=httpx.Limits(max_connections=10),
        follow_redirects=True
    ) as client:
        # Process in batches of 5 concurrently
        batch_size = 5
        for i in range(0, len(targets), batch_size):
            batch = targets[i:i+batch_size]
            tasks = [discover_one(client, lead) for _, lead in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for (domain, lead), result in zip(batch, results):
                if isinstance(result, Exception):
                    leads[domain]['_email_discovery_at'] = datetime.utcnow().isoformat()
                    leads[domain]['_email_discovery_status'] = f'error: {str(result)[:80]}'
                    error_count += 1
                else:
                    emails, status = result
                    if emails:
                        leads[domain]['emails'] = emails
                        leads[domain]['_email_source'] = 'website_discovery'
                        found_count += 1
                        total_emails += len(emails)
                        logger.info(f"FOUND {len(emails)} emails for {lead.get('business_name', domain)}: {emails}")
                    leads[domain]['_email_discovery_at'] = datetime.utcnow().isoformat()
                    leads[domain]['_email_discovery_status'] = status

            # Brief pause between batches
            await asyncio.sleep(0.5)

            # Progress log every 25 leads
            processed = min(i + batch_size, len(targets))
            if processed % 25 == 0 or processed == len(targets):
                logger.info(f"Discovery progress: {processed}/{len(targets)} processed, {found_count} found")

    master['updated_at'] = datetime.utcnow().isoformat()
    with open(MASTER_FILE, 'w') as f:
        json.dump(master, f, indent=2)

    result = {
        'status': 'completed',
        'timestamp': datetime.utcnow().isoformat(),
        'targets': len(targets),
        'found_emails': found_count,
        'total_emails_discovered': total_emails,
        'not_found': len(targets) - found_count - error_count,
        'errors': error_count,
    }

    DISCOVERY_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(DISCOVERY_LOG, 'a') as f:
        f.write(json.dumps(result) + '\n')

    logger.info(f"Email discovery complete: {json.dumps(result)}")
    return result


if __name__ == '__main__':
    import sys
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    result = asyncio.run(run_email_discovery(limit))
    print(json.dumps(result, indent=2))
