# MillyExt - Full Multi-Source Enrichment Engine
# Pulls ALL available data from every source for every lead
# Sources: Google Places API, Oregon SOS Registry, Website JSON-LD/Meta, Website Scraping
#
# Philosophy: Collect everything, store per-source, merge into unified lead record

import json, re, sqlite3, asyncio, logging, os, urllib.request, urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Optional
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger("enrichment_full")
logging.basicConfig(level=logging.INFO)

DATA_DIR = Path("/data")
LEADS_DIR = DATA_DIR / "leads"
MASTER_FILE = LEADS_DIR / "master_leads.json"
SOS_DB = DATA_DIR / "oregon_registry.db"
ENRICH_LOG = DATA_DIR / "logs" / "enrichment_full.jsonl"

PLACES_KEY = os.environ.get("GOOGLE_PLACES_KEY", "")
OPENROUTER_KEY = os.environ.get("OPENROUTER_API_KEY", "")
TG_BOT = "8460486926:AAEqOp5aunfeJomhTflAK6gQU0gVc90Gep0"
TG_CHAT = "8438279461"

# Google Places field mask - everything useful
PLACES_FIELD_MASK = ",".join([
    "id", "displayName", "formattedAddress", "addressComponents",
    "location", "rating", "userRatingCount", "websiteUri",
    "nationalPhoneNumber", "internationalPhoneNumber",
    "businessStatus", "types", "reviews", "editorialSummary",
    "regularOpeningHours", "primaryType", "shortFormattedAddress",
    "primaryTypeDisplayName", "googleMapsUri",
])


# =====================================================
# HELPERS
# =====================================================

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
                   ' PLUMBING AND HEATING', ' ELECTRIC SERVICE',
                   ' ELECTRICAL CONTRACTORS', ' ELECTRIC SUPPLY']:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    n = re.sub(r'[^A-Z0-9\s]', '', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n

def tg_notify(msg):
    try:
        url = f"https://api.telegram.org/bot{TG_BOT}/sendMessage"
        data = json.dumps({"chat_id": TG_CHAT, "text": msg}).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
    except:
        pass


# =====================================================
# SOURCE 1: GOOGLE PLACES API
# =====================================================

async def enrich_from_google_places(client, place_id):
    """Pull ALL data from Google Places API for a place_id.
    Returns raw structured data dict or None."""
    if not PLACES_KEY or not place_id or place_id.startswith("test_"):
        return None

    url = f"https://places.googleapis.com/v1/places/{place_id}"
    headers = {
        "X-Goog-Api-Key": PLACES_KEY,
        "X-Goog-FieldMask": PLACES_FIELD_MASK,
    }

    try:
        resp = await client.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            # Parse into our format
            result = {
                "source": "google_places",
                "place_id": place_id,
                "business_name": data.get("displayName", {}).get("text", ""),
                "formatted_address": data.get("formattedAddress", ""),
                "short_address": data.get("shortFormattedAddress", ""),
                "address_components": {},
                "lat": data.get("location", {}).get("latitude"),
                "lng": data.get("location", {}).get("longitude"),
                "phone": data.get("nationalPhoneNumber", ""),
                "phone_intl": data.get("internationalPhoneNumber", ""),
                "website": data.get("websiteUri", ""),
                "gmaps_url": data.get("googleMapsUri", ""),
                "rating": data.get("rating"),
                "review_count": data.get("userRatingCount"),
                "business_status": data.get("businessStatus", ""),
                "primary_type": data.get("primaryType", ""),
                "primary_type_display": data.get("primaryTypeDisplayName", {}).get("text", ""),
                "types": data.get("types", []),
                "editorial_summary": data.get("editorialSummary", {}).get("text", "") if data.get("editorialSummary") else "",
                "hours": [],
                "hours_text": [],
                "reviews": [],
                "employees": [],
            }

            # Parse address components
            for comp in data.get("addressComponents", []):
                types = comp.get("types", [])
                text = comp.get("longText", "")
                short = comp.get("shortText", "")
                if "street_number" in types:
                    result["address_components"]["street_number"] = text
                elif "route" in types:
                    result["address_components"]["street"] = text
                elif "locality" in types:
                    result["address_components"]["city"] = text
                elif "administrative_area_level_2" in types:
                    result["address_components"]["county"] = text
                elif "administrative_area_level_1" in types:
                    result["address_components"]["state"] = short
                elif "postal_code" in types:
                    result["address_components"]["zip"] = text
                elif "country" in types:
                    result["address_components"]["country"] = short

            # Parse hours
            hours_data = data.get("regularOpeningHours", {})
            if hours_data.get("weekdayDescriptions"):
                result["hours_text"] = hours_data["weekdayDescriptions"]
            if hours_data.get("periods"):
                result["hours"] = hours_data["periods"]

            # Parse reviews
            for rev in data.get("reviews", []):
                result["reviews"].append({
                    "author": rev.get("authorAttribution", {}).get("displayName", ""),
                    "rating": rev.get("rating"),
                    "text": rev.get("text", {}).get("text", "")[:500] if rev.get("text") else "",
                    "time": rev.get("publishTime", ""),
                    "relative_time": rev.get("relativePublishTimeDescription", ""),
                })

            logger.info(f"  Google Places: {result['business_name']} | {result['rating']}★ ({result['review_count']} reviews) | {result['formatted_address']}")
            return result
        else:
            logger.warning(f"  Google Places API error {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        logger.error(f"  Google Places error: {e}")
        return None


# =====================================================
# SOURCE 2: OREGON SOS REGISTRY (FULL DATA)
# =====================================================

def enrich_from_sos(business_name):
    """Pull ALL data from Oregon SOS for a business.
    Returns list of ALL matching records with full data."""
    if not SOS_DB.exists():
        return None

    normalized = normalize_biz_name(business_name)
    if not normalized or len(normalized) < 3:
        return None

    try:
        db = sqlite3.connect(str(SOS_DB))
        db.row_factory = sqlite3.Row

        # Strategy 1: Exact match
        rows = db.execute("""
            SELECT registry_number, business_name, entity_type, registry_date,
                   name_type, first_name, middle_name, last_name,
                   city, state, zip, address
            FROM businesses WHERE business_name_normalized = ?
            ORDER BY CASE name_type WHEN 'AUTHORIZED REPRESENTATIVE' THEN 1 ELSE 2 END
        """, (normalized,)).fetchall()

        match_type = "exact"

        # Strategy 2: LIKE match if no exact
        if not rows:
            words = normalized.split()
            if len(words) >= 2:
                pattern = f"%{words[0]} {words[1]}%"
            elif words:
                pattern = f"%{words[0]}%"
            else:
                db.close()
                return None

            rows = db.execute("""
                SELECT registry_number, business_name, entity_type, registry_date,
                       name_type, first_name, middle_name, last_name,
                       city, state, zip, address
                FROM businesses WHERE business_name_normalized LIKE ?
                ORDER BY CASE name_type WHEN 'AUTHORIZED REPRESENTATIVE' THEN 1 ELSE 2 END
                LIMIT 10
            """, (pattern,)).fetchall()
            match_type = "fuzzy"

        db.close()

        if not rows:
            return None

        result = {
            "source": "oregon_sos",
            "match_type": match_type,
            "query": normalized,
            "records": [],
        }

        seen_people = set()
        for row in rows:
            fname = row["first_name"].strip().title()
            lname = row["last_name"].strip().title()
            mname = row["middle_name"].strip().title() if row["middle_name"] else ""
            person_key = f"{fname}|{lname}".lower()

            # Calculate years in business from registry date
            years_in_biz = None
            reg_date_str = row["registry_date"] or ""
            if reg_date_str:
                try:
                    # Format: "06/15/2020 09:32:41 AM"
                    dt = datetime.strptime(reg_date_str.split(" ")[0], "%m/%d/%Y")
                    years_in_biz = round((datetime.utcnow() - dt).days / 365.25, 1)
                except:
                    pass

            record = {
                "registry_number": row["registry_number"],
                "business_name_official": row["business_name"],
                "entity_type": row["entity_type"],
                "registry_date": reg_date_str,
                "years_in_business": years_in_biz,
                "role": row["name_type"],
                "first_name": fname,
                "middle_name": mname,
                "last_name": lname,
                "full_name": f"{fname} {lname}".strip(),
                "address": row["address"].strip() if row["address"] else "",
                "city": row["city"].strip().title() if row["city"] else "",
                "state": row["state"].strip() if row["state"] else "",
                "zip": row["zip"].strip() if row["zip"] else "",
                "is_new_person": person_key not in seen_people,
            }
            seen_people.add(person_key)
            result["records"].append(record)

        # Summarize
        auth_reps = [r for r in result["records"] if r["role"] == "AUTHORIZED REPRESENTATIVE"]
        reg_agents = [r for r in result["records"] if r["role"] == "REGISTERED AGENT"]
        result["auth_rep"] = auth_reps[0] if auth_reps else None
        result["reg_agent"] = reg_agents[0] if reg_agents else None
        result["primary_record"] = auth_reps[0] if auth_reps else (reg_agents[0] if reg_agents else result["records"][0])

        logger.info(f"  SOS: {result['primary_record']['business_name_official']} | {result['primary_record']['full_name']} | {result['primary_record']['city']} | {result['primary_record'].get('years_in_business','?')} yrs")
        return result

    except Exception as e:
        logger.error(f"  SOS error: {e}")
        return None


# =====================================================
# SOURCE 3: WEBSITE STRUCTURED DATA (JSON-LD + META)
# =====================================================

async def enrich_from_website(client, website_url):
    """Extract JSON-LD structured data, meta tags, and other structured info from website."""
    if not website_url:
        return None

    if not website_url.startswith("http"):
        website_url = f"https://{website_url}"

    try:
        resp = await client.get(website_url, follow_redirects=True, timeout=15)
        if resp.status_code != 200:
            return None

        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        result = {
            "source": "website_structured",
            "url": website_url,
            "jsonld": [],
            "meta": {},
            "extracted": {
                "description": "",
                "address": "",
                "telephone": "",
                "email": "",
                "employees": [],
                "social_links": {},
                "opening_hours": [],
                "area_served": [],
                "rating": None,
                "review_count": None,
                "price_range": "",
                "founded": "",
                "number_of_employees": "",
            },
        }

        # Parse ALL JSON-LD blocks
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld_data = json.loads(script.string)
                items = ld_data if isinstance(ld_data, list) else [ld_data]
                for item in items:
                    ld_type = item.get("@type", "")
                    if isinstance(ld_type, list):
                        ld_type = ld_type[0] if ld_type else ""

                    # Store raw for reference
                    result["jsonld"].append({
                        "type": ld_type,
                        "data": {k: v for k, v in item.items() if k != "@context"},
                    })

                    # Extract useful fields from LocalBusiness and similar types
                    biz_types = {"LocalBusiness", "HVACBusiness", "Plumber", "Electrician",
                                 "HomeAndConstructionBusiness", "ProfessionalService",
                                 "Organization", "Service", "Store"}

                    if ld_type in biz_types or any(t in str(ld_type) for t in ["Business", "Service", "Plumb", "Electric"]):
                        # Address
                        addr = item.get("address", {})
                        if isinstance(addr, dict):
                            parts = [addr.get("streetAddress", ""), addr.get("addressLocality", ""),
                                     addr.get("addressRegion", ""), addr.get("postalCode", "")]
                            result["extracted"]["address"] = ", ".join(p for p in parts if p)

                        # Phone
                        if item.get("telephone"):
                            result["extracted"]["telephone"] = item["telephone"]

                        # Email
                        if item.get("email"):
                            result["extracted"]["email"] = item["email"]

                        # Employees/people
                        for person_key in ["employee", "founder", "member", "author"]:
                            person = item.get(person_key)
                            if isinstance(person, dict) and person.get("name"):
                                result["extracted"]["employees"].append({
                                    "name": person["name"],
                                    "title": person.get("jobTitle", person_key.title()),
                                    "source": "jsonld",
                                })
                            elif isinstance(person, list):
                                for p in person:
                                    if isinstance(p, dict) and p.get("name"):
                                        result["extracted"]["employees"].append({
                                            "name": p["name"],
                                            "title": p.get("jobTitle", ""),
                                            "source": "jsonld",
                                        })

                        # Aggregate rating
                        agg = item.get("aggregateRating", {})
                        if isinstance(agg, dict):
                            result["extracted"]["rating"] = agg.get("ratingValue")
                            result["extracted"]["review_count"] = agg.get("reviewCount")

                        # Opening hours
                        if item.get("openingHours"):
                            hrs = item["openingHours"]
                            if isinstance(hrs, list):
                                result["extracted"]["opening_hours"] = hrs
                            elif isinstance(hrs, str):
                                result["extracted"]["opening_hours"] = [hrs]

                        # Area served
                        area = item.get("areaServed")
                        if isinstance(area, dict):
                            names = area.get("name", [])
                            if isinstance(names, list):
                                result["extracted"]["area_served"] = names
                            elif isinstance(names, str):
                                result["extracted"]["area_served"] = [names]
                        elif isinstance(area, list):
                            for a in area:
                                if isinstance(a, dict):
                                    result["extracted"]["area_served"].append(a.get("name", ""))
                                elif isinstance(a, str):
                                    result["extracted"]["area_served"].append(a)

                        # Social links via sameAs
                        same_as = item.get("sameAs", [])
                        if isinstance(same_as, str):
                            same_as = [same_as]
                        for link in same_as:
                            for platform in ["facebook", "instagram", "twitter", "linkedin", "youtube", "yelp", "tiktok"]:
                                if platform in link.lower():
                                    result["extracted"]["social_links"][platform] = link

                        # Description
                        if item.get("description") and not result["extracted"]["description"]:
                            result["extracted"]["description"] = str(item["description"])[:500]

                        # Price range
                        if item.get("priceRange"):
                            result["extracted"]["price_range"] = item["priceRange"]

                        # Number of employees
                        if item.get("numberOfEmployees"):
                            noe = item["numberOfEmployees"]
                            if isinstance(noe, dict):
                                result["extracted"]["number_of_employees"] = noe.get("value", str(noe))
                            else:
                                result["extracted"]["number_of_employees"] = str(noe)

            except (json.JSONDecodeError, TypeError):
                continue

        # Parse meta tags
        for meta in soup.find_all("meta"):
            name = meta.get("name", meta.get("property", "")).lower()
            content = meta.get("content", "")
            if name and content:
                if any(k in name for k in ["description", "geo", "address", "author", "business", "og:"]):
                    result["meta"][name] = content[:300]

                # Specific extractions
                if name == "geo.position" and not result["extracted"].get("geo"):
                    result["extracted"]["geo"] = content
                if name in ("author", "og:site_name") and not result["extracted"]["description"]:
                    pass  # Already captured

        employees = result["extracted"]["employees"]
        if employees:
            logger.info(f"  Website JSON-LD: found {len(employees)} people, {len(result['jsonld'])} schema blocks")
        else:
            logger.info(f"  Website JSON-LD: {len(result['jsonld'])} schema blocks, no people found")

        return result

    except Exception as e:
        logger.error(f"  Website structured data error: {e}")
        return None


# =====================================================
# MERGE: Combine all sources into unified lead record
# =====================================================

def merge_enrichment_into_lead(lead, google_data, sos_data, website_data):
    """Merge all source data into the lead record.
    Strategy: store raw per-source data AND merge into top-level fields.
    Google Places is most authoritative for address/rating/hours.
    SOS is most authoritative for owner name/entity type/years in biz.
    Website structured data fills gaps."""

    changes = []

    # Store raw source data for reference
    if google_data:
        lead["_source_google"] = google_data
        changes.append("google_places")
    if sos_data:
        lead["_source_sos"] = sos_data
        changes.append("oregon_sos")
    if website_data:
        lead["_source_website"] = website_data
        changes.append("website_structured")

    # ---- ADDRESS (prefer Google > SOS > Website) ----
    if google_data and google_data.get("formatted_address"):
        lead["address"] = google_data["formatted_address"]
        lead["address_short"] = google_data.get("short_address", "")
        ac = google_data.get("address_components", {})
        lead["city"] = ac.get("city", "")
        lead["county"] = ac.get("county", "")
        lead["state"] = ac.get("state", "")
        lead["zip"] = ac.get("zip", "")
        lead["lat"] = google_data.get("lat")
        lead["lng"] = google_data.get("lng")
    elif sos_data and sos_data.get("primary_record", {}).get("address"):
        pr = sos_data["primary_record"]
        lead["address"] = f"{pr['address']}, {pr['city']} {pr['state']} {pr['zip']}"
        lead["city"] = pr["city"]
        lead["state"] = pr["state"]
        lead["zip"] = pr["zip"]
    elif website_data and website_data.get("extracted", {}).get("address"):
        lead["address"] = website_data["extracted"]["address"]

    # ---- RATING / REVIEWS (prefer Google > Website) ----
    if google_data:
        if google_data.get("rating") is not None:
            lead["rating"] = google_data["rating"]
        if google_data.get("review_count") is not None:
            lead["reviews"] = google_data["review_count"]
        if google_data.get("reviews"):
            lead["review_samples"] = google_data["reviews"][:5]
    elif website_data:
        ext = website_data.get("extracted", {})
        if ext.get("rating") is not None:
            lead["rating"] = ext["rating"]
        if ext.get("review_count") is not None:
            lead["reviews"] = ext["review_count"]

    # ---- GOOGLE MAPS URL ----
    if google_data and google_data.get("gmaps_url"):
        lead["gmaps_url"] = google_data["gmaps_url"]

    # ---- BUSINESS STATUS ----
    if google_data and google_data.get("business_status"):
        lead["business_status"] = google_data["business_status"]

    # ---- CATEGORY / TYPE (prefer Google > existing) ----
    if google_data and google_data.get("primary_type_display"):
        lead["google_category"] = google_data["primary_type_display"]
        if not lead.get("category"):
            lead["category"] = google_data["primary_type_display"]
    if google_data and google_data.get("types"):
        lead["google_types"] = google_data["types"]

    # ---- HOURS ----
    if google_data and google_data.get("hours_text"):
        lead["business_hours"] = google_data["hours_text"]
    elif website_data and website_data.get("extracted", {}).get("opening_hours"):
        lead["business_hours"] = website_data["extracted"]["opening_hours"]

    # ---- PHONE (verify/add Google phone) ----
    if google_data and google_data.get("phone"):
        gphone = google_data["phone"]
        existing = lead.get("phones", [])
        if gphone not in existing:
            lead["phones"] = [gphone] + existing  # Google phone first (most accurate)
        lead["phone_verified"] = gphone

    # ---- OWNER / PEOPLE ----
    # SOS is most authoritative for owner name
    all_people = []
    if sos_data:
        pr = sos_data.get("primary_record", {})
        if pr.get("full_name") and len(pr["full_name"]) > 3:
            # Only update owner if current is empty or SOS has auth rep
            current_owner = lead.get("owner_name", "")
            current_conf = lead.get("name_confidence", 0)
            sos_conf = 0.95 if pr["role"] == "AUTHORIZED REPRESENTATIVE" else 0.7
            if not current_owner or sos_conf > current_conf:
                lead["owner_name"] = pr["full_name"]
                lead["owner_title"] = pr["role"].replace("AUTHORIZED REPRESENTATIVE", "Owner/Principal").replace("REGISTERED AGENT", "Registered Agent")
                lead["name_source"] = f"oregon_sos_{sos_data['match_type']}"
                lead["name_confidence"] = sos_conf

        # Add ALL people from SOS
        for rec in sos_data.get("records", []):
            if rec.get("full_name") and len(rec["full_name"]) > 3:
                all_people.append({
                    "name": rec["full_name"],
                    "title": rec["role"].replace("AUTHORIZED REPRESENTATIVE", "Owner/Principal").replace("REGISTERED AGENT", "Registered Agent"),
                    "source": "oregon_sos",
                })

    # People from website JSON-LD
    if website_data:
        for emp in website_data.get("extracted", {}).get("employees", []):
            # Don't duplicate
            existing_names = {p["name"].lower() for p in all_people}
            if emp["name"].lower() not in existing_names:
                all_people.append(emp)

    # People from Google Reviews (owner responses, mentions)
    if google_data and google_data.get("reviews"):
        pass  # Already stored in review_samples, can mine later

    if all_people:
        lead["people"] = all_people

    # ---- YEARS IN BUSINESS (from SOS registry date) ----
    if sos_data:
        pr = sos_data.get("primary_record", {})
        if pr.get("years_in_business") is not None:
            lead["years_in_business"] = pr["years_in_business"]
            lead["registry_date"] = pr.get("registry_date", "")

    # ---- ENTITY TYPE (from SOS) ----
    if sos_data:
        pr = sos_data.get("primary_record", {})
        if pr.get("entity_type"):
            lead["entity_type"] = pr["entity_type"]
        if pr.get("registry_number"):
            lead["registry_number"] = pr["registry_number"]
        if pr.get("business_name_official"):
            lead["business_name_official"] = pr["business_name_official"]

    # ---- SERVICE AREA (from website JSON-LD) ----
    if website_data and website_data.get("extracted", {}).get("area_served"):
        lead["service_area"] = website_data["extracted"]["area_served"]

    # ---- SOCIAL LINKS (merge website JSON-LD sameAs with existing) ----
    if website_data and website_data.get("extracted", {}).get("social_links"):
        existing_social = lead.get("social_links_detail", {})
        if isinstance(existing_social, str):
            existing_social = {}
        existing_social.update(website_data["extracted"]["social_links"])
        lead["social_links_detail"] = existing_social

    # ---- DESCRIPTION / EDITORIAL ----
    if google_data and google_data.get("editorial_summary"):
        lead["description_google"] = google_data["editorial_summary"]
    if website_data and website_data.get("extracted", {}).get("description"):
        lead["description_website"] = website_data["extracted"]["description"][:500]

    # ---- ENRICHMENT METADATA ----
    lead["enriched"] = True
    lead["enriched_at"] = datetime.utcnow().isoformat()
    lead["enrichment_sources"] = changes

    return lead, changes


# =====================================================
# MAIN ENRICHMENT RUNNER
# =====================================================

_running = False
_status = {"running": False, "last_run": None, "processed": 0, "enriched": 0}

async def run_full_enrichment(limit=50, force=False, domains=None):
    """Run full multi-source enrichment on all leads.
    
    Args:
        limit: Max leads to process
        force: Re-enrich even if already enriched
        domains: Optional list of specific domains to enrich
    """
    global _running, _status
    if _running:
        return {"status": "already_running"}
    
    _running = True
    _status["running"] = True

    master = load_master()
    leads = master["leads"]

    # Select leads to enrich
    to_enrich = []
    for domain, lead in leads.items():
        if domains and domain not in domains:
            continue
        if force or not lead.get("enriched"):
            to_enrich.append((domain, lead))

    to_enrich = to_enrich[:limit]
    logger.info(f"Full enrichment: {len(to_enrich)} leads to process")

    processed = 0
    enriched_count = 0
    errors = []
    stats = {"google": 0, "sos": 0, "website": 0}

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    async with httpx.AsyncClient(headers=headers, verify=False, timeout=30) as client:
        for domain, lead in to_enrich:
            biz = lead.get("business_name", domain)
            logger.info(f"\n{'='*50}\nEnriching: {biz} ({domain})")

            try:
                # Source 1: Google Places
                google_data = None
                place_id = lead.get("place_id", "")
                if place_id and not place_id.startswith("test_"):
                    google_data = await enrich_from_google_places(client, place_id)
                    if google_data:
                        stats["google"] += 1
                    await asyncio.sleep(0.2)  # Rate limit

                # Source 2: Oregon SOS
                sos_data = enrich_from_sos(biz)
                if sos_data:
                    stats["sos"] += 1

                # Source 3: Website structured data
                website_data = None
                website = lead.get("website", "")
                if website:
                    website_data = await enrich_from_website(client, website)
                    if website_data:
                        stats["website"] += 1
                    await asyncio.sleep(0.3)  # Rate limit

                # Merge everything
                lead, changes = merge_enrichment_into_lead(lead, google_data, sos_data, website_data)
                leads[domain] = lead
                processed += 1
                if changes:
                    enriched_count += 1

            except Exception as e:
                logger.error(f"Error enriching {domain}: {e}")
                errors.append({"domain": domain, "error": str(e)})

            # Save periodically (every 5 leads)
            if processed % 5 == 0:
                save_master(master)
                logger.info(f"  Progress: {processed}/{len(to_enrich)}")

    # Final save
    save_master(master)

    # Calculate final stats
    all_leads = list(master["leads"].values())
    final_stats = {
        "total": len(all_leads),
        "has_email": sum(1 for l in all_leads if l.get("emails") and len(l["emails"]) > 0),
        "has_phone": sum(1 for l in all_leads if l.get("phones") and len(l["phones"]) > 0),
        "has_owner": sum(1 for l in all_leads if l.get("owner_name") and l["owner_name"].strip()),
        "has_address": sum(1 for l in all_leads if l.get("address") and l["address"].strip()),
        "has_rating": sum(1 for l in all_leads if l.get("rating") is not None),
        "has_hours": sum(1 for l in all_leads if l.get("business_hours")),
        "has_years": sum(1 for l in all_leads if l.get("years_in_business") is not None),
        "has_people": sum(1 for l in all_leads if l.get("people")),
        "enriched": sum(1 for l in all_leads if l.get("enriched")),
    }

    result = {
        "status": "completed",
        "timestamp": datetime.utcnow().isoformat(),
        "processed": processed,
        "enriched": enriched_count,
        "errors": len(errors),
        "error_details": errors,
        "source_hits": stats,
        "final_stats": final_stats,
    }

    # Log
    ENRICH_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(ENRICH_LOG, "a") as f:
        f.write(json.dumps(result) + "\n")

    # Telegram notify
    msg = (
        f"\U0001f4ca Full Enrichment Complete\n"
        f"Processed: {processed} | Enriched: {enriched_count}\n"
        f"Sources: Google={stats['google']} SOS={stats['sos']} Web={stats['website']}\n\n"
        f"Data Coverage:\n"
        f"  Email: {final_stats['has_email']}/{final_stats['total']}\n"
        f"  Phone: {final_stats['has_phone']}/{final_stats['total']}\n"
        f"  Owner: {final_stats['has_owner']}/{final_stats['total']}\n"
        f"  Address: {final_stats['has_address']}/{final_stats['total']}\n"
        f"  Rating: {final_stats['has_rating']}/{final_stats['total']}\n"
        f"  Hours: {final_stats['has_hours']}/{final_stats['total']}\n"
        f"  Years in Biz: {final_stats['has_years']}/{final_stats['total']}\n"
        f"  People: {final_stats['has_people']}/{final_stats['total']}"
    )
    tg_notify(msg)

    _status = {"running": False, "last_run": result["timestamp"], "processed": processed, "enriched": enriched_count}
    _running = False

    return result


# CLI runner
if __name__ == "__main__":
    import sys
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    force = "--force" in sys.argv
    result = asyncio.run(run_full_enrichment(limit=limit, force=force))
    print(f"\nDone: {result['enriched']}/{result['processed']} enriched")
    print(f"Source hits: {result['source_hits']}")
    print(f"Final stats: {json.dumps(result['final_stats'], indent=2)}")
