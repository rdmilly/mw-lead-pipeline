# MillyExt Receiver - Background Scraper Worker
# Processes pending lead batches by scraping URLs for contact info
# Auto-enriches with Oregon SOS registry data

import json, re, asyncio, logging, os, sqlite3, urllib.request
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, urljoin, unquote
from typing import Optional
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger("scraper_worker")
logging.basicConfig(level=logging.INFO)

DATA_DIR = Path("/data")
LEADS_DIR = DATA_DIR / "leads"
BATCHES_DIR = LEADS_DIR / "batches"
MASTER_FILE = LEADS_DIR / "master_leads.json"
SOS_DB = DATA_DIR / "oregon_registry.db"

POLL_INTERVAL = 30  # seconds between checks
_worker_running = False
_worker_status = {"last_run": None, "batches_processed": 0, "leads_scraped": 0, "errors": 0}

# ===== EXTRACTION HELPERS =====

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
PHONE_RE = re.compile(r'(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}')

SOCIAL_DOMAINS = {
    'facebook.com': 'facebook', 'fb.com': 'facebook',
    'instagram.com': 'instagram',
    'twitter.com': 'twitter', 'x.com': 'twitter',
    'linkedin.com': 'linkedin',
    'tiktok.com': 'tiktok',
    'youtube.com': 'youtube',
    'yelp.com': 'yelp',
    'nextdoor.com': 'nextdoor',
}

JUNK_EMAIL_PATTERNS = [
    'example.com', 'mysite.com', 'domain.com', 'email.com',
    'sentry.io', 'wixpress.com', 'squarespace.com', 'wordpress.com',
    'googleapis.com', 'cloudflare.com', 'schema.org', 'w3.org',
    'gravatar.com', 'placeholder', 'your@', 'info@example',
    'noreply', 'no-reply', 'donotreply',
    'stratam.app', 'mailchimp.com', 'constantcontact.com', 'hubspot.com',
]

ABOUT_PATHS = [
    '/about', '/about-us', '/about-me', '/our-story', '/our-team',
    '/team', '/meet-the-team', '/staff', '/who-we-are',
    '/contact', '/contact-us',
]

NAME_PATTERNS = [
    r'(?:owner|founder|president|ceo|proprietor|principal)\s*[:\-\u2013]\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})',
    r'(?:owned|founded|operated|started|run)\s+(?:and\s+operated\s+)?by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})',
    r"(?:my name is|i'm|i am)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})",
    r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\s*[,\-\u2013]\s*(?:owner|founder|president|ceo|proprietor|principal)',
]

EXCLUDE_NAMES = {
    'home', 'about', 'contact', 'services', 'service', 'reviews', 'blog',
    'our team', 'our story', 'about us', 'read more', 'learn more',
    'click here', 'get started', 'free estimate', 'free quote',
    'google', 'facebook', 'instagram', 'twitter', 'yelp',
    'privacy policy', 'terms', 'powered by', 'all rights',
    'united states', 'north america',
}


def clean_phone(raw):
    decoded = unquote(str(raw))
    digits = re.sub(r'[^\d]', '', decoded)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return decoded.strip()


def is_junk_phone(phone):
    digits = re.sub(r'[^\d]', '', phone)
    if len(digits) < 10:
        return True
    d10 = digits[-10:]
    area = d10[:3]
    exchange = d10[3:6]
    # NANP: area code and exchange first digit must be 2-9
    if area[0] in ('0', '1'):
        return True
    if exchange[0] in ('0', '1'):
        return True
    # Known fake/toll-free
    if area in ('555', '000', '999', '800', '888', '877', '866', '855', '844', '833'):
        return True
    # Repeated digits
    if len(set(area)) == 1:
        return True
    if len(set(d10[3:])) == 1:
        return True
    if d10 == '1234567890' or d10 == '0987654321':
        return True
    return False


def is_junk_email(email):
    email_lower = email.lower()
    if any(pat in email_lower for pat in JUNK_EMAIL_PATTERNS):
        return True
    if email_lower.startswith(('unsubscribe@', 'noreply@', 'no-reply@', 'donotreply@', 'mailer@')):
        return True
    # Image filenames parsed as emails
    if any(email_lower.endswith(ext) for ext in ['.webp', '.png', '.jpg', '.gif', '.svg', '.pdf']):
        return True
    # Placeholder emails
    if 'doe.com' in email_lower or 'jane@' in email_lower or 'john@doe' in email_lower:
        return True
    # Pixel dimensions in email (e.g. logo@300ppi)
    if '@300' in email_lower or 'ppi' in email_lower:
        return True
    return False


def is_valid_name(name):
    if not name or len(name) < 3:
        return False
    if name.lower().strip() in EXCLUDE_NAMES:
        return False
    parts = name.strip().split()
    if len(parts) > 4 or any(len(p) < 2 for p in parts):
        return False
    if name[0].islower():
        return False
    low = name.lower()
    bad_phrases = ['happy with', 'interested in', 'us and', 'occupied', 'open to',
                   'consider', 'very ', 'always', 'their ', 'your ', 'our ']
    if any(p in low for p in bad_phrases):
        return False
    return True


def extract_emails(html):
    raw = EMAIL_RE.findall(html)
    seen = set()
    result = []
    for e in raw:
        e = e.strip().lower()
        if e not in seen and '@' in e and not is_junk_email(e):
            seen.add(e)
            result.append(e)
    return result[:5]


def extract_phones(html):
    phones = set()
    soup = BeautifulSoup(html, 'html.parser')
    for a in soup.find_all('a', href=True):
        if a['href'].startswith('tel:'):
            digits = re.sub(r'[^\d]', '', a['href'])
            if len(digits) >= 10:
                phones.add(clean_phone(digits))
    for match in PHONE_RE.findall(html):
        digits = re.sub(r'[^\d]', '', match)
        if len(digits) >= 10:
            phones.add(clean_phone(digits))
    return [p for p in list(phones) if not is_junk_phone(p)][:5]


def extract_social_links(html):
    soup = BeautifulSoup(html, 'html.parser')
    socials = {}
    for a in soup.find_all('a', href=True):
        href = a['href'].lower()
        for domain, platform in SOCIAL_DOMAINS.items():
            if domain in href and platform not in socials:
                socials[platform] = a['href']
                break
    return socials


def extract_owner_name(html):
    for pattern in NAME_PATTERNS:
        matches = re.findall(pattern, html, re.IGNORECASE)
        for m in matches:
            name = m.strip()
            if is_valid_name(name):
                return name
    return ""


def extract_business_name(html, url):
    soup = BeautifulSoup(html, 'html.parser')
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
        for sep in [' | ', ' - ', ' \u2013 ', ' \u2014 ', ' :: ']:
            if sep in title:
                title = title.split(sep)[0].strip()
        if len(title) > 3 and len(title) < 80:
            return title
    og = soup.find('meta', property='og:site_name')
    if og and og.get('content'):
        return og['content'].strip()
    domain = urlparse(url).netloc.replace('www.', '')
    return domain


# ===== OREGON SOS REGISTRY LOOKUP =====

def normalize_biz_name(name):
    n = name.upper().strip()
    for suffix in [' LLC', ' L.L.C.', ' INC', ' INC.', ' INCORPORATED', ' CORP', ' CORP.',
                   ' CORPORATION', ' CO', ' CO.', ' COMPANY', ' LTD', ' LTD.',
                   ' DBA', ' D.B.A.', ' SERVICES', ' SERVICE', ' ENTERPRISES',
                   ' SOLUTIONS', ' GROUP', ' & ASSOCIATES', ' ASSOCIATES']:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    n = re.sub(r'[^A-Z0-9\s]', '', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def lookup_sos_owner(business_name):
    """Look up owner in Oregon SOS registry. Returns (name, confidence, match_type) or (None, 0, None)."""
    if not SOS_DB.exists():
        return None, 0, None

    normalized = normalize_biz_name(business_name)
    if not normalized or len(normalized) < 3:
        return None, 0, None

    try:
        db = sqlite3.connect(str(SOS_DB))
        c = db.cursor()

        # Exact match - prefer AUTHORIZED REPRESENTATIVE
        c.execute("""
            SELECT first_name, last_name, name_type, city
            FROM businesses WHERE business_name_normalized = ?
            ORDER BY CASE name_type WHEN 'AUTHORIZED REPRESENTATIVE' THEN 1 ELSE 2 END
            LIMIT 1
        """, (normalized,))
        row = c.fetchone()
        db.close()

        if row:
            fname = row[0].strip().title()
            lname = row[1].strip().title()
            if len(fname) >= 2 and len(lname) >= 2:
                conf = 0.95 if row[2] == 'AUTHORIZED REPRESENTATIVE' else 0.7
                return f"{fname} {lname}", conf, "exact"
            elif len(lname) >= 2:
                # Single initial first name (e.g., "C Fuchs")
                return f"{fname.upper()}. {lname}", 0.6, "exact"
    except Exception as e:
        logger.error(f"SOS lookup error: {e}")

    return None, 0, None


# ===== CCB LICENSE DB LOOKUP =====

CCB_DB = DATA_DIR / "ccb_active.db"

def parse_rmi_name(rmi):
    parts = rmi.strip().split()
    suffixes = {'JR', 'SR', 'II', 'III', 'IV'}
    while parts and parts[-1].upper() in suffixes:
        parts.pop()
    if len(parts) >= 2:
        return f"{parts[0].title()} {parts[-1].title()}"
    return ""

def lookup_ccb_owner(business_name):
    """Look up owner in Oregon CCB active licenses database."""
    if not CCB_DB.exists():
        return None, 0, None
    normalized = normalize_biz_name(business_name)
    if not normalized or len(normalized) < 3:
        return None, 0, None
    try:
        db = sqlite3.connect(str(CCB_DB))
        c = db.cursor()
        c.execute("SELECT rmi_name FROM contractors WHERE full_name_normalized = ? LIMIT 1", (normalized,))
        row = c.fetchone()
        if not row:
            words = normalized.split()
            if words and len(words[0]) >= 4:
                c.execute("SELECT rmi_name, full_name_normalized FROM contractors WHERE full_name_normalized LIKE ? LIMIT 5", (f"%{words[0]}%",))
                for r in c.fetchall():
                    if all(w in r[1] for w in words[:2] if len(w) >= 3):
                        row = (r[0],)
                        break
        db.close()
        if row and row[0]:
            name = parse_rmi_name(row[0])
            if name and is_valid_name(name):
                return name, 0.98, "ccb_rmi"
    except Exception as e:
        logger.error(f"CCB lookup error: {e}")
    return None, 0, None


# ===== PAGE FETCHING =====

async def fetch_page(client, url):
    try:
        resp = await client.get(url, follow_redirects=True, timeout=15)
        if resp.status_code == 200 and 'text/html' in resp.headers.get('content-type', ''):
            return resp.text
    except Exception as e:
        logger.debug(f"Failed to fetch {url}: {e}")
    return None


async def find_about_page(client, base_url, homepage_html):
    if not homepage_html:
        return None
    soup = BeautifulSoup(homepage_html, 'html.parser')
    about_links = []
    for a in soup.find_all('a', href=True):
        href = a['href'].lower()
        text = a.get_text(strip=True).lower()
        if any(kw in href or kw in text for kw in ['about', 'team', 'story', 'staff', 'who-we']):
            full_url = urljoin(base_url, a['href'])
            if urlparse(full_url).netloc == urlparse(base_url).netloc:
                about_links.append(full_url)
    for link in about_links[:3]:
        page = await fetch_page(client, link)
        if page and len(page) > 500:
            return page
    for path in ABOUT_PATHS[:4]:
        url = urljoin(base_url, path)
        page = await fetch_page(client, url)
        if page and len(page) > 500:
            return page
    return None


# ===== SCRAPE A SINGLE URL =====

async def scrape_url(client, url_entry):
    """Scrape a single URL and return lead data."""
    url = url_entry.get("url", "")
    name_hint = url_entry.get("name", "")
    place_id = url_entry.get("place_id", "")

    if not url:
        return {"error": "no url", "source_url": url}

    if not url.startswith("http"):
        url = "https://" + url

    logger.info(f"Scraping: {url}")

    homepage = await fetch_page(client, url)
    if not homepage:
        return {"error": "failed to fetch", "source_url": url}

    emails = extract_emails(homepage)
    phones = extract_phones(homepage)
    socials = extract_social_links(homepage)
    biz_name = name_hint or extract_business_name(homepage, url)
    owner = extract_owner_name(homepage)

    # Try about page
    about_html = await find_about_page(client, url, homepage)
    if about_html and about_html != homepage:
        for e in extract_emails(about_html):
            if e not in emails:
                emails.append(e)
        for p in extract_phones(about_html):
            if p not in phones:
                phones.append(p)
        socials.update(extract_social_links(about_html))
        if not owner:
            owner = extract_owner_name(about_html)

    # Try contact page
    contact_html = await fetch_page(client, urljoin(url, '/contact'))
    if not contact_html:
        contact_html = await fetch_page(client, urljoin(url, '/contact-us'))
    if contact_html:
        for e in extract_emails(contact_html):
            if e not in emails:
                emails.append(e)
        for p in extract_phones(contact_html):
            if p not in phones:
                phones.append(p)

    # SOS Registry enrichment for owner name
    name_source = "website" if owner else "not_found"
    name_confidence = 0.5 if owner else 0
    sos_owner, sos_conf, sos_match_type = lookup_sos_owner(biz_name)

    if sos_owner:
        if not owner or sos_conf > name_confidence:
            owner = sos_owner
            name_source = f"oregon_sos_{sos_match_type}"
            name_confidence = sos_conf
            logger.info(f"  SOS match: {biz_name} -> {sos_owner} ({sos_conf})")

    # CCB License DB enrichment (highest authority for contractors)
    ccb_owner, ccb_conf, ccb_source = lookup_ccb_owner(biz_name)
    if ccb_owner and ccb_conf > name_confidence:
        owner = ccb_owner
        name_source = ccb_source
        name_confidence = ccb_conf
        logger.info(f"  CCB match: {biz_name} -> {ccb_owner} ({ccb_conf})")

    return {
        "business_name": biz_name,
        "source_url": url,
        "emails": emails[:5],
        "phones": phones[:5],
        "owner_info": {"name": owner, "title": ""} if owner else None,
        "social_links": socials,
        "place_id": place_id,
        "name_source": name_source,
        "name_confidence": name_confidence,
        "scraped_at": datetime.utcnow().isoformat(),
    }


# ===== BATCH PROCESSING =====

def load_master():
    if MASTER_FILE.exists():
        with open(MASTER_FILE) as f:
            return json.load(f)
    return {"leads": {}, "updated_at": None}


def save_master(data):
    data["updated_at"] = datetime.utcnow().isoformat()
    with open(MASTER_FILE, "w") as f:
        json.dump(data, f, indent=2)


def normalize_domain(url):
    try:
        parsed = urlparse(url if '://' in url else f'https://{url}')
        return parsed.netloc.lower().replace('www.', '')
    except:
        return url.lower()


def dedup_emails(emails):
    seen = set()
    result = []
    for e in emails:
        e = e.strip().lower()
        if e and '@' in e and e not in seen and not is_junk_email(e):
            seen.add(e)
            result.append(e)
    return result


def dedup_phones(phones):
    seen = set()
    result = []
    for p in phones:
        digits = re.sub(r'[^\d]', '', str(p))
        if len(digits) >= 10 and digits[-10:] not in seen:
            seen.add(digits[-10:])
            result.append(clean_phone(p))
    return result


def merge_result_to_master(result, master, search_query=""):
    """Merge a scraped result into the master store."""
    url = result.get("source_url", "")
    domain = normalize_domain(url)
    if not domain or result.get("error"):
        return None, False

    is_new = domain not in master["leads"]

    category = ""
    area = ""
    if search_query:
        parts = search_query.split(" near ")
        if len(parts) == 2:
            category = parts[0].strip()
            area = parts[1].strip()

    if is_new:
        owner_name = ""
        if result.get("owner_info") and result["owner_info"].get("name"):
            owner_name = result["owner_info"]["name"]

        master["leads"][domain] = {
            "domain": domain,
            "business_name": result.get("business_name", ""),
            "website": url,
            "emails": dedup_emails(result.get("emails", [])),
            "phones": dedup_phones(result.get("phones", [])),
            "owner_name": owner_name,
            "owner_title": result.get("owner_info", {}).get("title", "") if result.get("owner_info") else "",
            "social_links": ", ".join(result.get("social_links", {}).keys()) if isinstance(result.get("social_links"), dict) else "",
            "address": "",
            "category": category,
            "area": area,
            "rating": None,
            "reviews": None,
            "score": None,
            "place_id": result.get("place_id", ""),
            "gmaps_url": "",
            "source": "scraper-worker",
            "name_source": result.get("name_source", ""),
            "name_confidence": result.get("name_confidence", 0),
            "first_seen": datetime.utcnow().isoformat(),
            "last_seen": datetime.utcnow().isoformat(),
            "enriched": False,
            "contacted": False,
            "notes": "",
        }
    else:
        e = master["leads"][domain]
        new_emails = dedup_emails(result.get("emails", []))
        new_phones = dedup_phones(result.get("phones", []))
        e["emails"] = list(set(e.get("emails", []) + new_emails))
        e["phones"] = list(set(e.get("phones", []) + new_phones))
        if not e.get("owner_name") and result.get("owner_info", {}).get("name"):
            e["owner_name"] = result["owner_info"]["name"]
            e["name_source"] = result.get("name_source", "")
            e["name_confidence"] = result.get("name_confidence", 0)
        if result.get("social_links") and not e.get("social_links"):
            e["social_links"] = ", ".join(result["social_links"].keys())
        if not e.get("category") and category:
            e["category"] = category
        if not e.get("area") and area:
            e["area"] = area
        e["last_seen"] = datetime.utcnow().isoformat()

    return domain, is_new


async def process_batch(batch_file):
    """Process a single pending batch."""
    global _worker_status

    with open(batch_file) as f:
        batch = json.load(f)

    batch_id = batch["batch_id"]
    urls = batch.get("urls", [])
    search_query = batch.get("search_query", "")

    logger.info(f"Processing batch {batch_id}: {len(urls)} URLs from '{search_query}'")

    batch["status"] = "processing"
    batch["started_at"] = datetime.utcnow().isoformat()
    with open(batch_file, "w") as f:
        json.dump(batch, f, indent=2)

    results = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    }

    async with httpx.AsyncClient(headers=headers, verify=False) as client:
        for url_entry in urls:
            try:
                result = await scrape_url(client, url_entry)
                results.append(result)
                _worker_status["leads_scraped"] += 1
            except Exception as e:
                logger.error(f"Error scraping {url_entry}: {e}")
                results.append({"error": str(e), "source_url": url_entry.get("url", "")})
                _worker_status["errors"] += 1
            await asyncio.sleep(1.5)

    # Merge results into master
    master = load_master()
    new_count = 0
    updated_count = 0
    for r in results:
        domain, is_new = merge_result_to_master(r, master, search_query)
        if domain:
            if is_new:
                new_count += 1
            else:
                updated_count += 1
    save_master(master)

    # Save raw results
    results_dir = LEADS_DIR / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    result_file = results_dir / f"{batch_id}.json"
    with open(result_file, "w") as f:
        json.dump({
            "batch_id": batch_id,
            "results": results,
            "total": len(results),
            "successful": len([r for r in results if not r.get("error")]),
            "failed": len([r for r in results if r.get("error")]),
            "received_at": datetime.utcnow().isoformat(),
        }, f, indent=2)

    # Update batch status
    batch["status"] = "completed"
    batch["completed_at"] = datetime.utcnow().isoformat()
    batch["results_count"] = len(results)
    batch["successful"] = len([r for r in results if not r.get("error")])
    batch["failed"] = len([r for r in results if r.get("error")])
    batch["new_leads"] = new_count
    batch["updated_leads"] = updated_count
    with open(batch_file, "w") as f:
        json.dump(batch, f, indent=2)

    _worker_status["batches_processed"] += 1
    logger.info(f"Batch {batch_id} complete: {new_count} new, {updated_count} updated, {batch['failed']} failed")

    return {"batch_id": batch_id, "new": new_count, "updated": updated_count, "failed": batch["failed"]}


def send_telegram(msg):
    try:
        tg_url = "https://api.telegram.org/bot8460486926:AAEqOp5aunfeJomhTflAK6gQU0gVc90Gep0/sendMessage"
        tg_data = json.dumps({"chat_id": "8438279461", "text": msg}).encode()
        tg_req = urllib.request.Request(tg_url, data=tg_data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(tg_req)
    except Exception as e:
        logger.debug(f"Telegram notify failed: {e}")


# ===== BACKGROUND WORKER LOOP =====

async def worker_loop():
    """Main worker loop - checks for pending batches and processes them."""
    global _worker_running, _worker_status
    _worker_running = True
    logger.info("Scraper worker started (with SOS enrichment)")

    while True:
        try:
            pending = []
            if BATCHES_DIR.exists():
                for f in sorted(BATCHES_DIR.glob("*.json")):
                    try:
                        with open(f) as fh:
                            batch = json.load(fh)
                        if batch.get("status") == "pending":
                            pending.append(f)
                    except:
                        pass

            if pending:
                logger.info(f"Found {len(pending)} pending batches")
                total_new = 0
                total_updated = 0
                for batch_file in pending:
                    try:
                        result = await process_batch(batch_file)
                        total_new += result.get("new", 0)
                        total_updated += result.get("updated", 0)
                    except Exception as e:
                        logger.error(f"Error processing batch {batch_file.name}: {e}")
                        _worker_status["errors"] += 1

                _worker_status["last_run"] = datetime.utcnow().isoformat()

                if total_new > 0 or total_updated > 0:
                    master = load_master()
                    owners = len([l for l in master['leads'].values() if l.get('owner_name')])
                    msg = (f"\U0001f50d Lead scraper complete\n"
                           f"Batches: {len(pending)}\n"
                           f"New leads: {total_new}\n"
                           f"Updated: {total_updated}\n"
                           f"Total in pipeline: {len(master['leads'])}\n"
                           f"With owner name: {owners}")
                    send_telegram(msg)

        except Exception as e:
            logger.error(f"Worker loop error: {e}")

        await asyncio.sleep(POLL_INTERVAL)


def get_worker_status():
    return {**_worker_status, "running": _worker_running}
