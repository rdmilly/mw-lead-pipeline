# MillyExt Receiver - Lead Enrichment Routes
# Enriches raw leads with owner names, verified emails, and campaign-ready formatting

import json, re, asyncio, logging
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, urljoin
from typing import Optional
import httpx
import dns.resolver
from bs4 import BeautifulSoup
from fastapi import APIRouter, Request, HTTPException, BackgroundTasks

router = APIRouter(prefix="/api/v1/enrich", tags=["enrichment"])

DATA_DIR = Path("/data")
LEADS_DIR = DATA_DIR / "leads"
MASTER_FILE = LEADS_DIR / "master_leads.json"
ENRICH_LOG = DATA_DIR / "logs" / "enrichment.jsonl"

logger = logging.getLogger("enrichment")

# ===== MASTER STORE HELPERS =====
def load_master():
    if MASTER_FILE.exists():
        with open(MASTER_FILE) as f:
            return json.load(f)
    return {"leads": {}, "updated_at": None}

def save_master(data):
    data["updated_at"] = datetime.utcnow().isoformat()
    with open(MASTER_FILE, "w") as f:
        json.dump(data, f, indent=2)

def check_auth(request: Request):
    import os
    key = request.headers.get("X-API-Key", "")
    expected = os.environ.get("MILLYEXT_API_KEY", "milly-dev-key-change-me")
    if key != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")


# ===== NAME EXTRACTION =====

# Common owner/founder patterns on service business websites
NAME_PATTERNS = [
    # "Owner: John Smith" or "Owner - John Smith"
    r'(?:owner|founder|president|ceo|proprietor|principal)\s*[:\-–]\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})',
    # "owned and operated by John Smith"
    r'(?:owned|founded|operated|started|run)\s+(?:and\s+operated\s+)?by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})',
    # "My name is John Smith" or "I'm John Smith"
    r"(?:my name is|i'm|i am)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})",
    # "John Smith, Owner" or "John Smith - Owner"
    r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\s*[,\-–]\s*(?:owner|founder|president|ceo|proprietor|principal)',
    # "About John Smith" as heading
    r'(?:about|meet)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})',
    # "Hi, I'm John" in casual about sections
    r"(?:hi,?\s+i'm|hello,?\s+i'm|hey,?\s+i'm)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})",
]

# Pages most likely to have owner info
ABOUT_PATHS = [
    '/about', '/about-us', '/about-me', '/our-story', '/our-team',
    '/team', '/meet-the-team', '/staff', '/who-we-are',
    '/contact', '/contact-us',
]

# Names to filter out (common false positives)
EXCLUDE_NAMES = {
    'home', 'about', 'contact', 'services', 'service', 'reviews', 'blog',
    'our team', 'our story', 'about us', 'read more', 'learn more',
    'click here', 'get started', 'free estimate', 'free quote',
    'google', 'facebook', 'instagram', 'twitter', 'yelp', 'bbb',
    'privacy policy', 'terms', 'powered by', 'all rights',
    'us home', 'home about', 'us home about', 'menu close',
    'next page', 'previous page', 'read our', 'see our',
    'call us', 'call today', 'call now', 'get quote',
    'your home', 'your business', 'your property', 'our services',
    'us contact us', 'us contact', 'contact us today', 'about us contact',
    'our work', 'our mission', 'our values', 'trusted by',
    'serving the', 'we serve', 'we provide', 'we offer',
    'view all', 'see all', 'show more', 'load more',
    'united states', 'north america', 'south america',
}


def is_valid_name(name: str) -> bool:
    """Filter out false positive name matches."""
    if not name or len(name) < 3:
        return False
    lower = name.lower().strip()
    if lower in EXCLUDE_NAMES:
        return False
    # Must have at least one space (first + last) for full names
    # Single names are OK for first-name-only extraction
    parts = name.strip().split()
    if len(parts) > 4:  # Too many words, probably not a name
        return False
    # Each part should be 2+ chars
    if any(len(p) < 2 for p in parts):
        return False
    # Should not contain numbers or special chars
    if re.search(r'[0-9@#$%^&*(){}[\]]', name):
        return False
    return True


def extract_names_from_html(html: str) -> list[dict]:
    """Extract potential owner/founder names from HTML content."""
    soup = BeautifulSoup(html, 'html.parser')
    
    # Remove script, style, nav, footer elements
    for tag in soup.find_all(['script', 'style', 'nav', 'footer', 'header']):
        tag.decompose()
    
    text = soup.get_text(separator=' ', strip=True)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    
    found = []
    
    # Try each pattern
    for pattern in NAME_PATTERNS:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for m in matches:
            name = m.group(1).strip()
            # Capitalize properly
            name = ' '.join(w.capitalize() for w in name.split())
            if is_valid_name(name):
                found.append({
                    'name': name,
                    'source': 'pattern',
                    'confidence': 0.8
                })
    
    # Also check meta tags
    for meta in soup.find_all('meta'):
        content = meta.get('content', '')
        name_attr = meta.get('name', '').lower()
        if name_attr in ('author', 'owner'):
            name = content.strip()
            name = ' '.join(w.capitalize() for w in name.split())
            if is_valid_name(name):
                found.append({
                    'name': name,
                    'source': 'meta',
                    'confidence': 0.9
                })
    
    # Check structured data (JSON-LD)
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            ld = json.loads(script.string)
            if isinstance(ld, list):
                ld = ld[0] if ld else {}
            # Check for founder/owner in schema
            for key in ['founder', 'author', 'employee', 'member']:
                val = ld.get(key)
                if isinstance(val, dict):
                    name = val.get('name', '')
                    if is_valid_name(name):
                        found.append({'name': name, 'source': 'schema', 'confidence': 0.95})
                elif isinstance(val, str) and is_valid_name(val):
                    found.append({'name': val, 'source': 'schema', 'confidence': 0.9})
        except (json.JSONDecodeError, TypeError):
            pass
    
    # Deduplicate by name
    seen = set()
    unique = []
    for f in found:
        if f['name'].lower() not in seen:
            seen.add(f['name'].lower())
            unique.append(f)
    
    # Sort by confidence
    unique.sort(key=lambda x: x['confidence'], reverse=True)
    return unique


def parse_name(full_name: str) -> dict:
    """Split full name into first and last."""
    parts = full_name.strip().split()
    if len(parts) >= 2:
        return {'first_name': parts[0], 'last_name': ' '.join(parts[1:])}
    elif len(parts) == 1:
        return {'first_name': parts[0], 'last_name': ''}
    return {'first_name': '', 'last_name': ''}


# ===== EMAIL VERIFICATION =====

async def verify_email_mx(email: str) -> dict:
    """Check if email domain has valid MX records."""
    try:
        domain = email.split('@')[1]
        # Check MX records
        try:
            mx_records = dns.resolver.resolve(domain, 'MX')
            mx_hosts = [str(r.exchange).rstrip('.') for r in mx_records]
            return {
                'email': email,
                'valid_mx': True,
                'mx_hosts': mx_hosts[:3],
                'status': 'deliverable'
            }
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
            # Try A record as fallback
            try:
                dns.resolver.resolve(domain, 'A')
                return {
                    'email': email,
                    'valid_mx': False,
                    'mx_hosts': [],
                    'status': 'risky'  # Has A record but no MX
                }
            except:
                return {
                    'email': email,
                    'valid_mx': False,
                    'mx_hosts': [],
                    'status': 'undeliverable'
                }
        except dns.resolver.NoNameservers:
            return {'email': email, 'valid_mx': False, 'mx_hosts': [], 'status': 'undeliverable'}
    except Exception as e:
        return {'email': email, 'valid_mx': False, 'mx_hosts': [], 'status': 'error', 'error': str(e)}


# Catch-all detection patterns
CATCHALL_DOMAINS = set()  # Cache for known catch-all domains

def is_generic_email(email: str) -> bool:
    """Check if email is likely a generic/role address vs personal."""
    local = email.split('@')[0].lower()
    generic_prefixes = [
        'info', 'contact', 'hello', 'support', 'admin', 'sales',
        'office', 'help', 'service', 'billing', 'team', 'mail',
        'general', 'inquiry', 'enquiry', 'request', 'noreply',
        'no-reply', 'notifications', 'marketing', 'press'
    ]
    return local in generic_prefixes


# ===== WEBSITE FETCHING =====

async def fetch_page(client: httpx.AsyncClient, url: str) -> Optional[str]:
    """Fetch a web page with error handling."""
    try:
        resp = await client.get(url, follow_redirects=True, timeout=15)
        if resp.status_code == 200 and 'text/html' in resp.headers.get('content-type', ''):
            return resp.text
    except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError, Exception) as e:
        logger.debug(f"Failed to fetch {url}: {e}")
    return None


async def find_about_page(client: httpx.AsyncClient, base_url: str) -> Optional[str]:
    """Find and fetch the About/Team page from a website."""
    # First fetch homepage and look for about links
    homepage = await fetch_page(client, base_url)
    if not homepage:
        return None
    
    soup = BeautifulSoup(homepage, 'html.parser')
    
    # Look for about/team links in the page
    about_links = []
    for a in soup.find_all('a', href=True):
        href = a['href'].lower()
        text = a.get_text(strip=True).lower()
        if any(kw in href or kw in text for kw in ['about', 'team', 'story', 'staff', 'who-we']):
            full_url = urljoin(base_url, a['href'])
            if urlparse(full_url).netloc == urlparse(base_url).netloc:
                about_links.append(full_url)
    
    # Try found links first
    for link in about_links[:3]:
        page = await fetch_page(client, link)
        if page and len(page) > 500:
            return page
    
    # Try common paths as fallback
    for path in ABOUT_PATHS[:6]:
        url = urljoin(base_url, path)
        page = await fetch_page(client, url)
        if page and len(page) > 500:
            return page
    
    # Return homepage as last resort
    return homepage


# ===== CATEGORY MAPPING =====

CATEGORY_MAP = {
    'plumber': 'Plumbing', 'plumbing': 'Plumbing',
    'electric': 'Electrical', 'electrician': 'Electrical', 'electrical': 'Electrical',
    'hvac': 'HVAC', 'heating': 'HVAC', 'cooling': 'HVAC', 'air conditioning': 'HVAC',
    'roofing': 'Roofing', 'roofer': 'Roofing', 'roof': 'Roofing',
    'landscap': 'Landscaping', 'lawn': 'Landscaping', 'yard': 'Landscaping',
    'painting': 'Painting', 'painter': 'Painting',
    'cleaning': 'Cleaning', 'janitorial': 'Cleaning', 'maid': 'Cleaning',
    'pest': 'Pest Control', 'exterminator': 'Pest Control',
    'moving': 'Moving', 'mover': 'Moving',
    'contractor': 'General Contractor', 'remodel': 'General Contractor', 'renovation': 'General Contractor',
    'garage door': 'Garage Door', 'overhead door': 'Garage Door',
    'window': 'Windows', 'glass': 'Windows',
    'fencing': 'Fencing', 'fence': 'Fencing',
    'tree': 'Tree Service', 'arborist': 'Tree Service',
    'concrete': 'Concrete', 'masonry': 'Concrete',
    'carpet': 'Flooring', 'flooring': 'Flooring', 'floor': 'Flooring',
    'locksmith': 'Locksmith',
    'handyman': 'Handyman', 'handy': 'Handyman',
    'septic': 'Septic', 'drain': 'Plumbing',
    'solar': 'Solar', 'insulation': 'Insulation',
    'siding': 'Siding', 'gutter': 'Gutters',
    'pressure wash': 'Pressure Washing', 'power wash': 'Pressure Washing',
    'chimney': 'Chimney', 'fireplace': 'Chimney',
    'pool': 'Pool Service', 'spa': 'Pool Service',
    'appliance': 'Appliance Repair',
    'christmas light': 'Holiday Lighting', 'holiday light': 'Holiday Lighting',
}

def infer_category(lead: dict) -> str:
    """Infer business category from available data."""
    # Check existing category first
    if lead.get('category'):
        return lead['category']
    
    # Check business name
    biz_name = (lead.get('business_name') or '').lower()
    website = (lead.get('website') or '').lower()
    combined = f"{biz_name} {website}"
    
    for keyword, category in CATEGORY_MAP.items():
        if keyword in combined:
            return category
    
    return 'Service Business'  # Generic fallback




# ===== OREGON SOS REGISTRY LOOKUP =====
import sqlite3

SOS_DB = Path("/data/oregon_registry.db")

def normalize_biz_name(name):
    """Normalize business name for matching against SOS registry."""
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


def lookup_sos_registry(business_name: str) -> list[dict]:
    """Look up business owner in Oregon SOS registry.
    Returns list of matches sorted by confidence.
    Priority: AUTHORIZED REPRESENTATIVE > REGISTERED AGENT"""
    if not SOS_DB.exists():
        return []
    
    normalized = normalize_biz_name(business_name)
    if not normalized or len(normalized) < 3:
        return []
    
    results = []
    
    try:
        db = sqlite3.connect(str(SOS_DB))
        c = db.cursor()
        
        # Try exact normalized match first
        c.execute("""
            SELECT business_name, first_name, last_name, name_type, entity_type, city, state
            FROM businesses
            WHERE business_name_normalized = ?
            ORDER BY CASE name_type 
                WHEN 'AUTHORIZED REPRESENTATIVE' THEN 1 
                WHEN 'REGISTERED AGENT' THEN 2 
                ELSE 3 END
        """, (normalized,))
        
        for row in c.fetchall():
            results.append({
                'business_name_match': row[0],
                'first_name': row[1].title(),
                'last_name': row[2].title(),
                'name_type': row[3],
                'entity_type': row[4],
                'city': row[5],
                'state': row[6],
                'match_type': 'exact',
                'confidence': 0.95 if row[3] == 'AUTHORIZED REPRESENTATIVE' else 0.7
            })
        
        # If no exact match, try LIKE match
        if not results:
            # Try with % wildcards on both sides
            like_pattern = f"%{normalized}%"
            c.execute("""
                SELECT business_name, first_name, last_name, name_type, entity_type, city, state,
                       business_name_normalized
                FROM businesses
                WHERE business_name_normalized LIKE ?
                AND name_type = 'AUTHORIZED REPRESENTATIVE'
                LIMIT 10
            """, (like_pattern,))
            
            for row in c.fetchall():
                # Calculate a simple similarity score
                match_norm = row[7]
                # Prefer shorter matches (more specific)
                len_ratio = min(len(normalized), len(match_norm)) / max(len(normalized), len(match_norm))
                conf = 0.6 * len_ratio  # Scale confidence by length similarity
                
                results.append({
                    'business_name_match': row[0],
                    'first_name': row[1].title(),
                    'last_name': row[2].title(),
                    'name_type': row[3],
                    'entity_type': row[4],
                    'city': row[5],
                    'state': row[6],
                    'match_type': 'fuzzy',
                    'confidence': round(conf, 2)
                })
        
        db.close()
        
    except Exception as e:
        logger.error(f"SOS registry lookup error: {e}")
    
    # Sort by confidence desc
    results.sort(key=lambda x: x['confidence'], reverse=True)
    return results[:5]




# ===== LINKEDIN ENRICHMENT (TIER 3) =====
LINKEDIN_SERVICE_URL = "http://127.0.0.1:8098"
LINKEDIN_API_KEY = "milly-li-enricher-key"

async def enqueue_linkedin_lookup(domain: str, business_name: str) -> dict | None:
    """Queue a LinkedIn lookup for leads where SOS/CCB didn't find an owner.
    Returns cached result if available, otherwise queues for async processing."""
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(
                f"{LINKEDIN_SERVICE_URL}/api/v1/linkedin/enqueue",
                params={"domain": domain, "business_name": business_name, "priority": 5},
                headers={"X-API-Key": LINKEDIN_API_KEY}
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "cached" and data.get("result", {}).get("first_name"):
                    return data["result"]
                logger.info(f"LinkedIn lookup queued for {business_name}")
            return None
    except Exception as e:
        logger.debug(f"LinkedIn service unavailable: {e}")
        return None


# ===== CORE ENRICHMENT ENGINE =====

_enrichment_running = False
_enrichment_status = {
    'running': False,
    'last_run': None,
    'processed': 0,
    'enriched': 0,
    'errors': 0
}

async def enrich_lead(client: httpx.AsyncClient, lead: dict) -> dict:
    """Enrich a single lead with owner name, email verification, and category."""
    updates = {
        'enrichment_status': 'enriched',
        'enriched_at': datetime.utcnow().isoformat(),
    }
    errors = []
    
    # 1. Category inference
    category = infer_category(lead)
    if category:
        updates['category'] = category
    
    # 2. Owner name extraction - TIERED APPROACH
    if not lead.get('owner_name') or lead['owner_name'].strip() == '':
        # Tier 1: Oregon SOS Registry (highest confidence, free, legal)
        biz_name = lead.get('business_name', '')
        if biz_name:
            sos_results = lookup_sos_registry(biz_name)
            if sos_results:
                best = sos_results[0]
                if best['confidence'] >= 0.5:
                    updates['owner_name'] = f"{best['first_name']} {best['last_name']}"
                    updates['first_name'] = best['first_name']
                    updates['last_name'] = best['last_name']
                    updates['name_source'] = f"oregon_sos_{best['match_type']}"
                    updates['name_confidence'] = best['confidence']
                    updates['sos_match'] = best['business_name_match']
                    updates['sos_entity_type'] = best['entity_type']
        
        # Tier 2: Website scraping (fallback if SOS didn't find it)
        if not updates.get('first_name'):
            website = lead.get('website', '')
            if website:
                if not website.startswith('http'):
                    website = f'https://{website}'
                try:
                    about_html = await find_about_page(client, website)
                    if about_html:
                        names = extract_names_from_html(about_html)
                        if names:
                            best = names[0]
                            updates['owner_name'] = best['name']
                            parsed = parse_name(best['name'])
                            updates['first_name'] = parsed['first_name']
                            updates['last_name'] = parsed['last_name']
                            updates['name_source'] = f"website_{best['source']}"
                            updates['name_confidence'] = best['confidence']
                        else:
                            updates['first_name'] = ''
                            updates['last_name'] = ''
                            updates['name_source'] = 'not_found'
                except Exception as e:
                    errors.append(f"name_extraction: {str(e)}")
                    updates['name_source'] = 'error'
        
        # Tier 3: LinkedIn (async queue - only if SOS and web scraping both missed)
        if not updates.get('first_name') and updates.get('name_source') != 'error':
            domain = lead.get('domain', '')
            biz_name = lead.get('business_name', '')
            if domain and biz_name:
                li_result = await enqueue_linkedin_lookup(domain, biz_name)
                if li_result and li_result.get('first_name'):
                    updates['owner_name'] = li_result.get('full_name', f"{li_result['first_name']} {li_result.get('last_name', '')}")
                    updates['first_name'] = li_result['first_name']
                    updates['last_name'] = li_result.get('last_name', '')
                    updates['name_source'] = 'linkedin'
                    updates['name_confidence'] = li_result.get('confidence', 0.6)
                elif not li_result:
                    updates['linkedin_queued'] = True
    else:
        # Parse existing owner name
        parsed = parse_name(lead['owner_name'])
        updates['first_name'] = parsed['first_name']
        updates['last_name'] = parsed['last_name']
    
    # 2b. LinkedIn enricher fallback (if website scraping found nothing)
    if not updates.get('first_name') and updates.get('name_source') in ('not_found', 'error', None):
        try:
            biz_name = lead.get('business_name', '')
            area = lead.get('area', '') or lead.get('address', '')
            if biz_name:
                async with httpx.AsyncClient() as li_client:
                    li_resp = await li_client.post(
                        'http://127.0.0.1:8098/api/v1/lookup',
                        headers={'X-API-Key': 'milly-dev-key-change-me', 'Content-Type': 'application/json'},
                        json={'company': biz_name, 'location': area, 'use_linkedin': True},
                        timeout=120
                    )
                    if li_resp.status_code == 200:
                        li_data = li_resp.json()
                        if li_data.get('status') == 'found' and li_data.get('name'):
                            parsed = parse_name(li_data['name'])
                            updates['owner_name'] = li_data['name']
                            updates['first_name'] = parsed['first_name']
                            updates['last_name'] = parsed['last_name']
                            updates['name_source'] = li_data.get('source', 'linkedin')
                            updates['name_confidence'] = li_data.get('confidence', 0.8)
                            if li_data.get('linkedin_url'):
                                updates['linkedin_url'] = li_data['linkedin_url']
        except Exception as e:
            logger.debug(f"LinkedIn enricher fallback failed: {e}")

    # 3. Email verification
    emails = lead.get('emails', [])
    verified_emails = []
    personal_emails = []
    
    for email in emails:
        result = await verify_email_mx(email)
        if result['status'] in ('deliverable', 'risky'):
            verified_emails.append({
                'email': email,
                'status': result['status'],
                'is_generic': is_generic_email(email)
            })
            if not is_generic_email(email):
                personal_emails.append(email)
    
    updates['verified_emails'] = verified_emails
    updates['has_verified_email'] = len(verified_emails) > 0
    updates['has_personal_email'] = len(personal_emails) > 0
    updates['best_email'] = personal_emails[0] if personal_emails else (
        verified_emails[0]['email'] if verified_emails else ''
    )
    
    # 4. Record any errors
    if errors:
        updates['enrichment_errors'] = errors
        updates['enrichment_status'] = 'partial'
    
    return updates


async def run_enrichment(limit: int = 20, force: bool = False):
    """Run enrichment on un-enriched leads."""
    global _enrichment_running, _enrichment_status
    
    if _enrichment_running:
        return {'status': 'already_running'}
    
    _enrichment_running = True
    _enrichment_status['running'] = True
    
    master = load_master()
    leads = master['leads']
    
    # Find leads needing enrichment
    to_enrich = []
    for domain, lead in leads.items():
        status = lead.get('enrichment_status', '')
        if force or status not in ('enriched', 'skipped'):
            # Must have at least one email
            if lead.get('emails'):
                to_enrich.append((domain, lead))
    
    to_enrich = to_enrich[:limit]
    
    processed = 0
    enriched = 0
    errors = 0
    
    async with httpx.AsyncClient(
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
        verify=False,
        limits=httpx.Limits(max_connections=5)
    ) as client:
        for domain, lead in to_enrich:
            try:
                updates = await enrich_lead(client, lead)
                # Apply updates to master
                for k, v in updates.items():
                    leads[domain][k] = v
                processed += 1
                if updates.get('enrichment_status') in ('enriched', 'partial'):
                    enriched += 1
                
                # Rate limit - don't hammer websites
                await asyncio.sleep(2)
                
            except Exception as e:
                leads[domain]['enrichment_status'] = 'error'
                leads[domain]['enrichment_error'] = str(e)
                errors += 1
                logger.error(f"Enrichment error for {domain}: {e}")
    
    save_master(master)
    
    result = {
        'status': 'completed',
        'timestamp': datetime.utcnow().isoformat(),
        'processed': processed,
        'enriched': enriched,
        'errors': errors,
        'remaining': len([d for d, l in leads.items() 
                         if l.get('enrichment_status', '') not in ('enriched', 'skipped') 
                         and l.get('emails')])
    }
    
    # Log result
    ENRICH_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(ENRICH_LOG, 'a') as f:
        f.write(json.dumps(result) + '\n')
    
    _enrichment_status = {
        'running': False,
        'last_run': result['timestamp'],
        'processed': processed,
        'enriched': enriched,
        'errors': errors
    }
    _enrichment_running = False
    
    return result


# ===== API ENDPOINTS =====

@router.get("/pending")
async def get_pending(request: Request):
    """Get leads that need enrichment."""
    check_auth(request)
    master = load_master()
    pending = []
    for domain, lead in master['leads'].items():
        status = lead.get('enrichment_status', '')
        if status not in ('enriched', 'skipped'):
            if lead.get('emails'):
                pending.append({
                    'domain': domain,
                    'business_name': lead.get('business_name', ''),
                    'emails': lead.get('emails', []),
                    'owner_name': lead.get('owner_name', ''),
                    'category': lead.get('category', ''),
                    'status': status or 'new'
                })
    return {'total': len(pending), 'leads': pending}


@router.post("/run")
async def trigger_enrichment(request: Request, background_tasks: BackgroundTasks):
    """Trigger enrichment processing."""
    check_auth(request)
    global _enrichment_running
    if _enrichment_running:
        return {'status': 'already_running'}
    
    body = await request.json() if await request.body() else {}
    limit = body.get('limit', 20)
    force = body.get('force', False)
    
    background_tasks.add_task(run_enrichment, limit=limit, force=force)
    return {'status': 'started', 'limit': limit, 'force': force}


@router.get("/status")
async def enrichment_status():
    """Get enrichment processing status."""
    master = load_master()
    leads = list(master['leads'].values())
    
    return {
        **_enrichment_status,
        'totals': {
            'total_leads': len(leads),
            'with_email': len([l for l in leads if l.get('emails')]),
            'enriched': len([l for l in leads if l.get('enrichment_status') == 'enriched']),
            'partial': len([l for l in leads if l.get('enrichment_status') == 'partial']),
            'pending': len([l for l in leads if l.get('enrichment_status', '') not in ('enriched', 'skipped', 'partial') and l.get('emails')]),
            'with_owner': len([l for l in leads if l.get('first_name')]),
            'with_verified_email': len([l for l in leads if l.get('has_verified_email')]),
        }
    }


@router.get("/export/instantly")
async def export_for_instantly(request: Request, enriched_only: bool = True):
    """Export leads in Instantly-compatible CSV format."""
    check_auth(request)
    master = load_master()
    
    rows = []
    for domain, lead in master['leads'].items():
        # Filter
        if enriched_only and lead.get('enrichment_status') not in ('enriched', 'partial'):
            continue
        
        email = lead.get('best_email', '')
        if not email:
            # Fallback to first email
            emails = lead.get('emails', [])
            email = emails[0] if emails else ''
        
        if not email:
            continue
        
        first_name = lead.get('first_name', '')
        # If no first name, use a generic but warm greeting fallback
        # Instantly will use companyName variable in the template
        
        rows.append({
            'email': email,
            'firstName': first_name,
            'lastName': lead.get('last_name', ''),
            'companyName': lead.get('business_name', ''),
            'category': lead.get('category', '') or infer_category(lead),
            'website': lead.get('website', ''),
            'phone': lead.get('phones', [''])[0] if lead.get('phones') else '',
            'domain': domain,
        })
    
    return {
        'total': len(rows),
        'leads': rows,
        'format': 'instantly_csv',
        'fields': ['email', 'firstName', 'lastName', 'companyName', 'category', 'website', 'phone']
    }


@router.get("/export/instantly/csv")
async def export_instantly_csv(request: Request, enriched_only: bool = True):
    """Download Instantly-compatible CSV file."""
    import csv, io
    from fastapi.responses import StreamingResponse
    
    check_auth(request)
    result = await export_for_instantly(request, enriched_only=enriched_only)
    
    output = io.StringIO()
    fields = result['fields']
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    for row in result['leads']:
        writer.writerow({k: row.get(k, '') for k in fields})
    
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=instantly_leads_{datetime.utcnow().strftime('%Y%m%d')}.csv"}
    )


@router.post("/lead/{domain}")
async def enrich_single(domain: str, request: Request):
    """Manually trigger enrichment for a single lead."""
    check_auth(request)
    master = load_master()
    
    if domain not in master['leads']:
        raise HTTPException(404, f"Lead {domain} not found")
    
    lead = master['leads'][domain]
    
    async with httpx.AsyncClient(
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
        verify=False
    ) as client:
        updates = await enrich_lead(client, lead)
    
    for k, v in updates.items():
        master['leads'][domain][k] = v
    save_master(master)
    
    return {
        'domain': domain,
        'updates': updates,
        'lead': master['leads'][domain]
    }


@router.get("/sos-lookup/{business_name}")
async def sos_lookup(business_name: str):
    """Direct lookup against Oregon SOS registry."""
    results = lookup_sos_registry(business_name)
    return {
        'query': business_name,
        'normalized': normalize_biz_name(business_name),
        'matches': len(results),
        'results': results
    }
