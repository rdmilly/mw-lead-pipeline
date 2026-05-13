# MillyExt - Advanced Owner Enrichment Engine
# Tier 1: Fuzzy SOS matching (broader search strategies)
# Tier 2: Oregon CCB lookup (via workaround - name search in SOS DB for CCB-registered names)
# Tier 3: Google Maps reviews (extract owner names from review text)
# Tier 4: AI-assisted website parsing (LLM reads about/team pages)

import json, re, sqlite3, asyncio, logging, os, urllib.request, urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Optional
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger("enricher")
logging.basicConfig(level=logging.INFO)

DATA_DIR = Path("/data")
LEADS_DIR = DATA_DIR / "leads"
MASTER_FILE = LEADS_DIR / "master_leads.json"
SOS_DB = DATA_DIR / "oregon_registry.db"
ENRICH_LOG = DATA_DIR / "logs" / "enrichment_v2.jsonl"

PLACES_KEY = os.environ.get("GOOGLE_PLACES_KEY", "AIzaSyARIS8p7XpdzHxnkaS9EyxraH3LKnk1sxE")
OPENROUTER_KEY = os.environ.get("OPENROUTER_API_KEY", "")

# ===== HELPERS =====

def load_master():
    if MASTER_FILE.exists():
        with open(MASTER_FILE) as f:
            return json.load(f)
    return {"leads": {}, "updated_at": None}

def save_master(data):
    data["updated_at"] = datetime.utcnow().isoformat()
    with open(MASTER_FILE, "w") as f:
        json.dump(data, f, indent=2)

def normalize_biz_name(name):
    n = name.upper().strip()
    for suffix in [' LLC', ' L.L.C.', ' INC', ' INC.', ' INCORPORATED', ' CORP', ' CORP.',
                   ' CORPORATION', ' CO', ' CO.', ' COMPANY', ' LTD', ' LTD.',
                   ' DBA', ' D.B.A.', ' SERVICES', ' SERVICE', ' ENTERPRISES',
                   ' SOLUTIONS', ' GROUP', ' & ASSOCIATES', ' ASSOCIATES',
                   ' OF OREGON', ' OR', ' NW', ' NORTHWEST', ' PACIFIC',
                   ' HEATING AND COOLING', ' HEATING & COOLING',
                   ' HEATING AND AIR', ' HEATING & AIR',
                   ' HEATING & AIR CONDITIONING', ' HEATING AND AIR CONDITIONING',
                   ' PLUMBING AND HEATING', ' ELECTRIC SERVICE',
                   ' ELECTRICAL CONTRACTORS', ' ELECTRIC SUPPLY']:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    n = re.sub(r'[^A-Z0-9\s]', '', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n

def is_valid_owner_name(name):
    if not name or len(name.strip()) < 3:
        return False
    parts = name.strip().split()
    if len(parts) < 2 or len(parts) > 4:
        return False
    if parts[0][0].islower():
        return False
    bad = ['happy with', 'interested in', 'us and', 'occupied', 'open to',
           'very ', 'always', 'their ', 'your ', 'our ', 'read more',
           'click here', 'contact us', 'learn more', 'free estimate']
    if any(p in name.lower() for p in bad):
        return False
    return True


# ===== TIER 1: FUZZY SOS MATCHING =====

def tier1_fuzzy_sos(business_name):
    """Broader SOS search with multiple strategies."""
    if not SOS_DB.exists():
        return None, 0, "sos_unavailable"

    strategies = []

    # Strategy A: Exact normalized (already tried, but with expanded suffix stripping)
    norm = normalize_biz_name(business_name)
    if norm:
        strategies.append(("exact_expanded", norm))

    # Strategy B: First word only (e.g., "RENHARD" from "Renhard Heating and Cooling")
    words = norm.split() if norm else []
    if words and len(words[0]) >= 4:
        strategies.append(("first_word", words[0]))

    # Strategy C: First two words
    if len(words) >= 2:
        strategies.append(("first_two", f"{words[0]} {words[1]}"))

    # Strategy D: Try without common trade words
    trade_words = {'HEATING', 'COOLING', 'PLUMBING', 'ELECTRIC', 'ELECTRICAL', 'HVAC',
                   'AIR', 'CONDITIONING', 'MECHANICAL', 'CONTRACTING', 'CONSTRUCTION',
                   'ROOFING', 'PAINTING', 'LANDSCAPING', 'CLEANING'}
    core_words = [w for w in words if w not in trade_words]
    if core_words and len(core_words) < len(words):
        strategies.append(("core_name", " ".join(core_words)))

    # Strategy E: If name has "&" or "and" pattern like "D & R" -> "D R"
    if 'AND' in words or '&' in business_name.upper():
        pass  # Already handled by normalization

    try:
        db = sqlite3.connect(str(SOS_DB))
        c = db.cursor()

        for strategy, search_term in strategies:
            if not search_term or len(search_term) < 3:
                continue

            # Exact match
            c.execute("""
                SELECT business_name, first_name, last_name, name_type, city, state, business_name_normalized
                FROM businesses WHERE business_name_normalized = ?
                ORDER BY CASE name_type WHEN 'AUTHORIZED REPRESENTATIVE' THEN 1 ELSE 2 END
                LIMIT 3
            """, (search_term,))
            rows = c.fetchall()

            if not rows:
                # LIKE match with the search term
                c.execute("""
                    SELECT business_name, first_name, last_name, name_type, city, state, business_name_normalized
                    FROM businesses WHERE business_name_normalized LIKE ?
                    ORDER BY CASE name_type WHEN 'AUTHORIZED REPRESENTATIVE' THEN 1 ELSE 2 END
                    LIMIT 10
                """, (f"%{search_term}%",))
                rows = c.fetchall()

            for row in rows:
                fname = row[1].strip().title()
                lname = row[2].strip().title()
                if len(fname) < 2 or len(lname) < 2:
                    continue

                full_name = f"{fname} {lname}"
                if not is_valid_owner_name(full_name):
                    continue

                # Score based on match quality
                match_norm = row[6]
                is_auth_rep = row[3] == 'AUTHORIZED REPRESENTATIVE'

                if match_norm == norm:
                    conf = 0.95 if is_auth_rep else 0.75
                elif search_term in match_norm and len(search_term) >= len(match_norm) * 0.5:
                    conf = 0.7 if is_auth_rep else 0.55
                else:
                    conf = 0.5 if is_auth_rep else 0.35

                db.close()
                return full_name, conf, f"sos_fuzzy_{strategy}"

        db.close()
    except Exception as e:
        logger.error(f"Tier 1 SOS error: {e}")

    return None, 0, "sos_no_match"


# ===== TIER 2: CCB LICENSE DATABASE (Oregon Open Data - 55K+ active licenses) =====

CCB_DB = DATA_DIR / "ccb_active.db"

def parse_rmi_name(rmi):
    """Parse RMI name handling Jr/Sr/II/III suffixes."""
    parts = rmi.strip().split()
    suffixes = {'JR', 'SR', 'II', 'III', 'IV'}
    # Remove suffixes from end
    while parts and parts[-1].upper() in suffixes:
        parts.pop()
    if len(parts) >= 2:
        return parts[0].title(), parts[-1].title(), " ".join(parts).title()
    elif len(parts) == 1:
        return parts[0].title(), "", parts[0].title()
    return "", "", ""

def tier2_ccb_crossref(business_name):
    """Look up contractor in Oregon CCB active licenses database (55K+ records)."""
    if not CCB_DB.exists():
        return None, 0, "ccb_db_unavailable"

    norm = normalize_biz_name(business_name)
    if not norm or len(norm) < 3:
        return None, 0, "ccb_skip"

    try:
        db = sqlite3.connect(str(CCB_DB))
        c = db.cursor()

        # Exact match
        c.execute("""SELECT full_name, rmi_name, city, phone_number, license_number
                     FROM contractors WHERE full_name_normalized = ? LIMIT 1""", (norm,))
        row = c.fetchone()

        # Fuzzy: first two words
        if not row:
            words = norm.split()
            if len(words) >= 2:
                pattern = f"%{words[0]} {words[1]}%"
                c.execute("""SELECT full_name, rmi_name, city, phone_number, license_number
                             FROM contractors WHERE full_name_normalized LIKE ? LIMIT 5""", (pattern,))
                rows = c.fetchall()
                for r in rows:
                    if all(w in r[0] for w in words[:2] if len(w) >= 3):
                        row = r
                        break

        # Fuzzy: first word only
        if not row:
            words = norm.split()
            if words and len(words[0]) >= 5:
                c.execute("""SELECT full_name, rmi_name, city, phone_number, license_number
                             FROM contractors WHERE full_name_normalized LIKE ? LIMIT 5""", (f"%{words[0]}%",))
                rows = c.fetchall()
                for r in rows:
                    if words[0] in r[0]:
                        row = r
                        break

        db.close()

        if row and row[1]:
            first, last, full = parse_rmi_name(row[1])
            if first and last and is_valid_owner_name(f"{first} {last}"):
                return f"{first} {last}", 0.98, "ccb_rmi"

    except Exception as e:
        logger.error(f"Tier 2 CCB error: {e}")

    return None, 0, "ccb_no_match"


# ===== TIER 3: GOOGLE MAPS REVIEWS =====

def tier3_google_reviews(business_name, place_id=None, area=""):
    """Extract owner names from Google Maps reviews."""
    if not PLACES_KEY:
        return None, 0, "gmaps_no_key"

    try:
        # Get place_id if not provided
        if not place_id:
            query = urllib.parse.quote(f"{business_name} {area}")
            url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={query}&key={PLACES_KEY}"
            resp = urllib.request.urlopen(url, timeout=10)
            data = json.loads(resp.read())
            results = data.get("results", [])
            if not results:
                return None, 0, "gmaps_not_found"
            place_id = results[0]["place_id"]

        # Get reviews
        url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&key={PLACES_KEY}&fields=name,reviews"
        resp = urllib.request.urlopen(url, timeout=10)
        data = json.loads(resp.read())
        reviews = data.get("result", {}).get("reviews", [])

        if not reviews:
            return None, 0, "gmaps_no_reviews"

        # Strategy A: Look for owner names mentioned IN review text
        name_patterns = [
            r'(?:owner|founder|president|boss)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:the owner|the founder|is the owner|owns)',
            r'(?:thank(?:s| you)\s+)([A-Z][a-z]+)',
            r'([A-Z][a-z]+)\s+(?:came out|fixed|installed|was great|was very|did a|did an)',
        ]

        # Check the business name for a surname hint
        biz_words = set(business_name.upper().split())

        name_candidates = {}
        for review in reviews:
            text = review.get("text", "")
            for pattern in name_patterns:
                matches = re.findall(pattern, text)
                for m in matches:
                    m = m.strip()
                    if len(m) < 3 or not m[0].isupper():
                        continue
                    # Boost if name appears in business name
                    if m.upper().split()[0] in biz_words:
                        name_candidates[m] = name_candidates.get(m, 0) + 3
                    else:
                        name_candidates[m] = name_candidates.get(m, 0) + 1

        # Strategy B: Look for owner responses (author_name of reviews that are from the business)
        # Unfortunately Places API doesn't return owner responses separately
        # But we can check if any review author name matches business name patterns
        for review in reviews:
            author = review.get("author_name", "")
            # If author name contains a word from the business name, might be owner responding
            # (This doesn't reliably work with Places API, keeping for future expansion)

        if name_candidates:
            best_name = max(name_candidates, key=name_candidates.get)
            score = name_candidates[best_name]
            if score >= 2 and is_valid_owner_name(best_name):
                conf = min(0.7, 0.4 + score * 0.1)
                return best_name, conf, "gmaps_review_mention"

        return None, 0, "gmaps_no_owner_in_reviews"

    except Exception as e:
        logger.error(f"Tier 3 Google Reviews error: {e}")
        return None, 0, f"gmaps_error"


# ===== TIER 4: AI-ASSISTED WEBSITE PARSING =====

async def tier4_ai_website(business_name, website, client):
    """Use LLM to intelligently extract owner info from website."""
    if not OPENROUTER_KEY or not website:
        return None, 0, "ai_no_key_or_website"

    if not website.startswith("http"):
        website = f"https://{website}"

    try:
        # Fetch about page
        about_html = None
        homepage = None

        try:
            resp = await client.get(website, follow_redirects=True, timeout=15)
            if resp.status_code == 200:
                homepage = resp.text
        except:
            return None, 0, "ai_fetch_failed"

        if not homepage:
            return None, 0, "ai_fetch_failed"

        # Find about page link
        soup = BeautifulSoup(homepage, 'html.parser')
        about_url = None
        for a in soup.find_all('a', href=True):
            href = a['href'].lower()
            text = a.get_text(strip=True).lower()
            if any(kw in href or kw in text for kw in ['about', 'team', 'story', 'staff', 'who-we']):
                from urllib.parse import urljoin, urlparse
                full_url = urljoin(website, a['href'])
                if urlparse(full_url).netloc == urlparse(website).netloc:
                    about_url = full_url
                    break

        if about_url:
            try:
                resp = await client.get(about_url, follow_redirects=True, timeout=15)
                if resp.status_code == 200:
                    about_html = resp.text
            except:
                pass

        # Extract text from the most relevant page
        target_html = about_html or homepage
        soup = BeautifulSoup(target_html, 'html.parser')

        # Remove script/style
        for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'iframe']):
            tag.decompose()

        page_text = soup.get_text(separator='\n', strip=True)
        # Truncate to ~2000 chars to save tokens
        if len(page_text) > 2000:
            page_text = page_text[:2000]

        if len(page_text) < 50:
            return None, 0, "ai_no_content"

        # Call LLM
        prompt = f"""Extract the owner or principal person's name from this business website text.
Business name: {business_name}

Website text:
{page_text}

If you can identify the owner, founder, president, CEO, or main person behind this business, respond with ONLY their full name (first and last).
If you cannot find a specific owner name, respond with ONLY the word "NONE".
Do not explain, just the name or NONE."""

        ai_resp = await client.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "google/gemini-2.0-flash-001",
                "max_tokens": 50,
                "temperature": 0,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )

        if ai_resp.status_code != 200:
            return None, 0, f"ai_api_error_{ai_resp.status_code}"

        ai_data = ai_resp.json()
        answer = ai_data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()

        if answer and answer.upper() != "NONE" and len(answer) < 40:
            # Clean up
            answer = re.sub(r'["\'\.]', '', answer).strip()
            if is_valid_owner_name(answer):
                return answer, 0.65, "ai_website"

        return None, 0, "ai_not_found"

    except Exception as e:
        logger.error(f"Tier 4 AI error for {business_name}: {e}")
        return None, 0, f"ai_error"


# ===== MASTER ENRICHMENT RUNNER =====

async def run_enrichment_v2(limit=50, force=False):
    """Run all 4 enrichment tiers on leads missing owner names."""
    master = load_master()
    leads = master["leads"]

    to_enrich = []
    for domain, lead in leads.items():
        if force or not lead.get("owner_name"):
            to_enrich.append((domain, lead))

    to_enrich = to_enrich[:limit]
    logger.info(f"Enrichment V2: {len(to_enrich)} leads to process")

    results_log = []
    enriched = 0

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    async with httpx.AsyncClient(headers=headers, verify=False, timeout=30) as client:
        for domain, lead in to_enrich:
            biz = lead.get("business_name", "")
            area = lead.get("area", "") or "Oregon"
            website = lead.get("website", "")
            place_id = lead.get("place_id", "")
            entry = {"domain": domain, "business": biz, "tiers_tried": []}

            logger.info(f"Enriching: {biz}")

            # Tier 1: Fuzzy SOS
            name, conf, source = tier1_fuzzy_sos(biz)
            entry["tiers_tried"].append({"tier": 1, "source": source, "name": name, "conf": conf})
            if name and conf >= 0.4:
                lead["owner_name"] = name
                lead["name_source"] = source
                lead["name_confidence"] = conf
                entry["result"] = name
                enriched += 1
                logger.info(f"  T1 SOS: {name} ({conf})")
                results_log.append(entry)
                continue

            # Tier 2: CCB cross-reference
            name, conf, source = tier2_ccb_crossref(biz)
            entry["tiers_tried"].append({"tier": 2, "source": source, "name": name, "conf": conf})
            if name and conf >= 0.4:
                lead["owner_name"] = name
                lead["name_source"] = source
                lead["name_confidence"] = conf
                entry["result"] = name
                enriched += 1
                logger.info(f"  T2 CCB: {name} ({conf})")
                results_log.append(entry)
                continue

            # Tier 3: Google Maps reviews
            name, conf, source = tier3_google_reviews(biz, place_id, area)
            entry["tiers_tried"].append({"tier": 3, "source": source, "name": name, "conf": conf})
            if name and conf >= 0.4:
                lead["owner_name"] = name
                lead["name_source"] = source
                lead["name_confidence"] = conf
                entry["result"] = name
                enriched += 1
                logger.info(f"  T3 GMaps: {name} ({conf})")
                results_log.append(entry)
                continue

            # Tier 4: AI website parsing
            name, conf, source = await tier4_ai_website(biz, website, client)
            entry["tiers_tried"].append({"tier": 4, "source": source, "name": name, "conf": conf})
            if name and conf >= 0.4:
                lead["owner_name"] = name
                lead["name_source"] = source
                lead["name_confidence"] = conf
                entry["result"] = name
                enriched += 1
                logger.info(f"  T4 AI: {name} ({conf})")
                results_log.append(entry)
                continue

            entry["result"] = None
            results_log.append(entry)
            logger.info(f"  No owner found across all tiers")

            # Rate limit
            await asyncio.sleep(1)

    save_master(master)

    # Log results
    ENRICH_LOG.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_processed": len(to_enrich),
        "enriched": enriched,
        "details": results_log,
    }
    with open(ENRICH_LOG, "a") as f:
        f.write(json.dumps(summary) + "\n")

    # Telegram notify
    try:
        master = load_master()
        total_owners = len([l for l in master["leads"].values() if l.get("owner_name")])
        msg = (f"\U0001f50e Enrichment V2 complete\n"
               f"Processed: {len(to_enrich)}\n"
               f"New owners found: {enriched}\n"
               f"Total with owner: {total_owners}/{len(master['leads'])}")
        tg_url = "https://api.telegram.org/bot8460486926:AAEqOp5aunfeJomhTflAK6gQU0gVc90Gep0/sendMessage"
        tg_data = json.dumps({"chat_id": "8438279461", "text": msg}).encode()
        tg_req = urllib.request.Request(tg_url, data=tg_data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(tg_req)
    except:
        pass

    return summary


# CLI runner
if __name__ == "__main__":
    import sys
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    force = "--force" in sys.argv
    result = asyncio.run(run_enrichment_v2(limit=limit, force=force))
    print(f"\nDone: {result['enriched']}/{result['total_processed']} enriched")
