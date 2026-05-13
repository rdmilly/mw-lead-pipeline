# MillyExt Receiver - Config Routes
# Manages search queries, pipeline configuration, and manual triggers

import json, os, urllib.request, urllib.parse
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Request, HTTPException, BackgroundTasks

router = APIRouter(prefix="/api/v1/config", tags=["config"])

DATA_DIR = Path("/data")
CONFIG_DIR = DATA_DIR / "config"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)

QUERIES_FILE = CONFIG_DIR / "search_queries.json"
PLACES_KEY = os.environ.get("GOOGLE_PLACES_KEY", "AIzaSyARIS8p7XpdzHxnkaS9EyxraH3LKnk1sxE")

DEFAULT_QUERIES = [
    {"q": "plumber", "area": "Newberg OR"},
    {"q": "plumber", "area": "McMinnville OR"},
    {"q": "electrician", "area": "Newberg OR"},
    {"q": "electrician", "area": "McMinnville OR"},
    {"q": "HVAC contractor", "area": "Sherwood OR"},
    {"q": "HVAC contractor", "area": "Newberg OR"},
    {"q": "handyman", "area": "Newberg OR"},
    {"q": "handyman", "area": "McMinnville OR"},
    {"q": "landscaping", "area": "Newberg OR"},
    {"q": "roofing contractor", "area": "McMinnville OR"},
    {"q": "painting contractor", "area": "Sherwood OR"},
    {"q": "cleaning service", "area": "Newberg OR"}
]


def load_queries():
    if QUERIES_FILE.exists():
        with open(QUERIES_FILE) as f:
            return json.load(f)
    return {"queries": DEFAULT_QUERIES, "updated_at": None}


def save_queries(data):
    data["updated_at"] = datetime.utcnow().isoformat()
    with open(QUERIES_FILE, "w") as f:
        json.dump(data, f, indent=2)


def check_auth(request: Request):
    key = request.headers.get("X-API-Key", "")
    expected = os.environ.get("MILLYEXT_API_KEY", "milly-dev-key-change-me")
    if key != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")


@router.get("/queries")
async def get_queries():
    data = load_queries()
    return data


@router.post("/queries")
async def set_queries(request: Request):
    check_auth(request)
    body = await request.json()
    queries = body.get("queries", [])
    if not queries:
        raise HTTPException(400, "queries list required")
    for q in queries:
        if "q" not in q or "area" not in q:
            raise HTTPException(400, "Each query needs 'q' and 'area' fields")
    data = {"queries": queries}
    save_queries(data)
    return {"status": "updated", "count": len(queries)}


@router.post("/queries/add")
async def add_query(request: Request):
    check_auth(request)
    body = await request.json()
    q = body.get("q")
    area = body.get("area")
    if not q or not area:
        raise HTTPException(400, "'q' and 'area' required")
    data = load_queries()
    for existing in data["queries"]:
        if existing["q"].lower() == q.lower() and existing["area"].lower() == area.lower():
            return {"status": "duplicate", "message": "Query already exists"}
    data["queries"].append({"q": q, "area": area})
    save_queries(data)
    return {"status": "added", "count": len(data["queries"])}


@router.delete("/queries/{index}")
async def delete_query(index: int, request: Request):
    check_auth(request)
    data = load_queries()
    if index < 0 or index >= len(data["queries"]):
        raise HTTPException(400, "Invalid index")
    removed = data["queries"].pop(index)
    save_queries(data)
    return {"status": "removed", "removed": removed, "count": len(data["queries"])}


# ===== MANUAL TRIGGER (server-side Places search) =====

def _places_search(query):
    """Call Google Places API server-side."""
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={urllib.parse.quote(query)}&key={PLACES_KEY}&type=establishment"
    resp = urllib.request.urlopen(url)
    return json.loads(resp.read())


def _place_details(place_id):
    """Get website for a place."""
    url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&key={PLACES_KEY}&fields=website,formatted_phone_number,url"
    resp = urllib.request.urlopen(url)
    return json.loads(resp.read())


def _run_manual_search(queries_batch):
    """Run Places search, dedup, and create batches. Runs in background."""
    from leads_routes import load_master, normalize_domain
    import uuid

    master = load_master()
    existing_domains = set(master["leads"].keys())
    results_log = []

    for q in queries_batch:
        search_query = q["q"] + " near " + q["area"]
        try:
            sd = _places_search(search_query)
            if sd.get("status") != "OK":
                results_log.append({"query": search_query, "error": sd.get("status")})
                continue

            places = []
            for p in sd.get("results", []):
                if p.get("business_status") != "OPERATIONAL":
                    continue
                reviews = p.get("user_ratings_total", 0)
                score = 50
                if reviews < 50: score += 30
                elif reviews < 200: score += 15
                if p.get("rating", 0) >= 4.0: score += 10
                if p.get("rating", 5) < 3.5: score -= 20
                if score >= 40:
                    places.append({"name": p["name"], "place_id": p["place_id"], "score": score})

            places.sort(key=lambda x: x["score"], reverse=True)
            places = places[:10]

            urls = []
            for p in places:
                try:
                    dd = _place_details(p["place_id"])
                    website = dd.get("result", {}).get("website")
                    if website:
                        domain = urllib.parse.urlparse(website).netloc.lower().replace("www.", "")
                        if domain not in existing_domains:
                            urls.append({"url": website, "name": p["name"], "place_id": p["place_id"]})
                            existing_domains.add(domain)
                except:
                    pass

            if urls:
                batch_id = str(uuid.uuid4())[:12]
                batch = {
                    "batch_id": batch_id, "status": "pending", "urls": urls,
                    "total": len(urls), "source": "manual-dashboard",
                    "search_query": search_query,
                    "created_at": datetime.utcnow().isoformat()
                }
                batch_dir = Path("/data/leads/batches")
                batch_dir.mkdir(parents=True, exist_ok=True)
                with open(batch_dir / f"{batch_id}.json", "w") as f:
                    json.dump(batch, f, indent=2)
                results_log.append({"query": search_query, "new_urls": len(urls), "batch_id": batch_id})
            else:
                results_log.append({"query": search_query, "new_urls": 0, "skipped": True})

        except Exception as e:
            results_log.append({"query": search_query, "error": str(e)})

    # Log the run
    log_dir = Path("/data/leads")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "manual_runs.jsonl"
    with open(log_file, "a") as f:
        f.write(json.dumps({"timestamp": datetime.utcnow().isoformat(), "results": results_log}) + "\n")

    # Send Telegram notification
    try:
        total_new = sum(r.get("new_urls", 0) for r in results_log)
        msg = f"Manual run complete\nQueries: {len(queries_batch)}\nNew leads queued: {total_new}"
        tg_url = "https://api.telegram.org/bot8460486926:AAEqOp5aunfeJomhTflAK6gQU0gVc90Gep0/sendMessage"
        tg_data = json.dumps({"chat_id": "8438279461", "text": msg}).encode()
        tg_req = urllib.request.Request(tg_url, data=tg_data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(tg_req)
    except:
        pass


@router.post("/trigger")
async def trigger_manual_run(request: Request, background_tasks: BackgroundTasks):
    """Trigger a manual search run. Picks next 3 queries."""
    check_auth(request)
    body = await request.json() if await request.body() else {}
    count = min(body.get("count", 3), 6)

    data = load_queries()
    queries = data.get("queries", [])
    if not queries:
        raise HTTPException(400, "No queries configured")

    # Pick queries
    state_file = CONFIG_DIR / "trigger_state.json"
    state = {}
    if state_file.exists():
        with open(state_file) as f:
            state = json.load(f)
    idx = state.get("index", 0)
    batch = []
    for i in range(count):
        batch.append(queries[(idx + i) % len(queries)])
    state["index"] = (idx + count) % len(queries)
    with open(state_file, "w") as f:
        json.dump(state, f)

    background_tasks.add_task(_run_manual_search, batch)

    return {
        "status": "started",
        "queries": [q["q"] + " " + q["area"] for q in batch],
        "message": f"Running {len(batch)} queries in background. Extension will pick up batches within 2 minutes."
    }
