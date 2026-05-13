# Database backend
import db

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import json, os, uuid, asyncio
from datetime import datetime
from pathlib import Path

# Background scraper worker
from scraper_worker import worker_loop, get_worker_status
from enricher_v2 import run_enrichment_v2

app = FastAPI(title="MillyExt Receiver", version="0.3.0")

# Lead scraper routes
from leads_routes import router as leads_router
app.include_router(leads_router)

# Config routes
from config_routes import router as config_router

# Enrichment routes
from enrich_routes import router as enrich_router
from enrich_full_routes import router as enrich_full_router
app.include_router(config_router)
app.include_router(enrich_router)
app.include_router(enrich_full_router)
# Ops dashboard routes
from ops_routes import router as ops_router
app.include_router(ops_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    db.init_db()
    import asyncio
    # asyncio.create_task(worker_loop())  # DISABLED: Contabo handles scraping

DATA_DIR = Path("/data")
TRANSCRIPTS_DIR = DATA_DIR / "transcripts"
COMMANDS_DIR = DATA_DIR / "commands"
RESULTS_DIR = DATA_DIR / "results"
LOGS_DIR = DATA_DIR / "logs"
EXTRACTS_DIR = DATA_DIR / "extracts"

for d in [TRANSCRIPTS_DIR, COMMANDS_DIR, RESULTS_DIR, LOGS_DIR, EXTRACTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

API_KEY = os.environ.get("MILLYEXT_API_KEY", "milly-dev-key-change-me")

def check_auth(request: Request):
    key = request.headers.get("X-API-Key", "")
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

# Import extractor
from extractor import run_extraction, get_extraction_status, search_extracts

_extraction_running = False

async def _run_extraction_bg(limit=None, force=False):
    global _extraction_running
    _extraction_running = True
    try:
        result = await run_extraction(limit=limit, force=force)
        log_file = LOGS_DIR / "extraction_runs.jsonl"
        with open(log_file, "a") as f:
            f.write(json.dumps({"timestamp": datetime.utcnow().isoformat(), **result}) + "\n")
    finally:
        _extraction_running = False

# ===== DASHBOARD =====
@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    html_path = Path("/app/dashboard.html")
    if html_path.exists():
        return HTMLResponse(html_path.read_text())
    return HTMLResponse("<h1>Dashboard not found</h1>")

# ===== HEALTH =====
@app.get("/api/v1/health")
async def health():
    return {"status": "ok", "service": "millyext-receiver", "version": "0.3.0",
            "time": datetime.utcnow().isoformat()}

# ===== TRANSCRIPTS =====
@app.post("/api/v1/transcripts/claude")
async def receive_claude_transcript(request: Request):
    data = await request.json()
    conv_id = data.get("conversation_id", str(uuid.uuid4()))
    updated_at = data.get("updated_at", datetime.utcnow().isoformat())
    name = data.get("name", "unknown")
    message_count = data.get("message_count", 0)
    safe_date = updated_at[:10]
    filename = f"{safe_date}_{conv_id}.json"
    filepath = TRANSCRIPTS_DIR / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "conversation_id": conv_id, "name": name,
        "message_count": message_count, "file": str(filepath),
        "size_bytes": filepath.stat().st_size
    }
    log_file = LOGS_DIR / f"transcript_log_{safe_date}.jsonl"
    with open(log_file, "a") as f:
        f.write(json.dumps(log_entry) + "\n")
    return {"status": "saved", "conversation_id": conv_id, "file": filename, "size": filepath.stat().st_size}

# ===== EXTRACTION =====
@app.post("/api/v1/extract/run")
async def trigger_extraction(request: Request, background_tasks: BackgroundTasks):
    check_auth(request)
    global _extraction_running
    if _extraction_running:
        return {"status": "already_running"}
    body = await request.json() if await request.body() else {}
    background_tasks.add_task(_run_extraction_bg, limit=body.get("limit"), force=body.get("force", False))
    return {"status": "started", "limit": body.get("limit"), "force": body.get("force", False)}

@app.get("/api/v1/extract/status")
async def extraction_status():
    status = get_extraction_status()
    status["running"] = _extraction_running
    return status

@app.get("/api/v1/extract/search")
async def extract_search(q: str, limit: int = 10):
    return {"query": q, "results": search_extracts(q, limit=limit)}

@app.get("/api/v1/extract/{conv_id}")
async def get_extract(conv_id: str):
    filepath = EXTRACTS_DIR / f"{conv_id}.json"
    if not filepath.exists():
        raise HTTPException(404, "Extract not found")
    with open(filepath) as f:
        return json.load(f)

@app.get("/api/v1/extracts")
async def list_extracts(offset: int = 0, limit: int = 50):
    files = sorted(EXTRACTS_DIR.glob("*.json"), reverse=True)
    total = len(files)
    page = files[offset:offset + limit]
    items = []
    for f in page:
        with open(f) as fh:
            e = json.load(fh)
        items.append({
            "conversation_id": e.get("conversation_id"), "name": e.get("name"),
            "summary": e.get("summary"), "topics": e.get("topics", []),
            "tags": e.get("tags", []), "conversation_type": e.get("conversation_type"),
            "technical_depth": e.get("technical_depth"),
            "message_count": e.get("message_count"), "updated_at": e.get("updated_at")
        })
    return {"total": total, "offset": offset, "limit": limit, "items": items}

# ===== COMMAND QUEUE =====
@app.get("/api/v1/commands/pending")
async def get_pending_commands(request: Request):
    check_auth(request)
    pending = []
    for f in sorted(COMMANDS_DIR.glob("*.json")):
        with open(f) as fh:
            cmd = json.load(fh)
        if cmd.get("status") == "pending":
            pending.append(cmd)
    return pending

@app.post("/api/v1/commands/create")
async def create_command(request: Request):
    check_auth(request)
    data = await request.json()
    cmd_id = str(uuid.uuid4())[:12]
    cmd = {"id": cmd_id, "type": data["type"], "params": data.get("params", {}),
           "status": "pending", "created_at": datetime.utcnow().isoformat()}
    filepath = COMMANDS_DIR / f"{cmd_id}.json"
    with open(filepath, "w") as f:
        json.dump(cmd, f, indent=2)
    return cmd

@app.post("/api/v1/commands/result")
async def command_result(request: Request):
    data = await request.json()
    cmd_id = data.get("command_id")
    if not cmd_id:
        raise HTTPException(400, "command_id required")
    cmd_file = COMMANDS_DIR / f"{cmd_id}.json"
    if cmd_file.exists():
        with open(cmd_file) as f:
            cmd = json.load(f)
        cmd["status"] = data.get("status", "completed")
        cmd["result"] = data.get("result")
        cmd["error"] = data.get("error")
        cmd["completed_at"] = datetime.utcnow().isoformat()
        with open(cmd_file, "w") as f:
            json.dump(cmd, f, indent=2)
    result_file = RESULTS_DIR / f"{cmd_id}.json"
    with open(result_file, "w") as f:
        json.dump(data, f, indent=2)
    return {"status": "received", "command_id": cmd_id}

# ===== ENRICHMENT V2 =====
@app.post("/api/v1/enrich/v2/run")
async def trigger_enrichment_v2(request: Request, background_tasks: BackgroundTasks):
    check_auth(request)
    body = await request.json() if await request.body() else {}
    limit = body.get("limit", 50)
    force = body.get("force", False)
    background_tasks.add_task(run_enrichment_v2, limit=limit, force=force)
    return {"status": "started", "limit": limit, "force": force}

# ===== WORKER STATUS =====
@app.get("/api/v1/worker/status")
async def scraper_worker_status():
    return get_worker_status()

# ===== STATS =====
@app.get("/api/v1/stats")
async def stats():
    transcript_count = len(list(TRANSCRIPTS_DIR.glob("*.json")))
    total_size = sum(f.stat().st_size for f in TRANSCRIPTS_DIR.glob("*.json"))
    extract_count = len(list(EXTRACTS_DIR.glob("*.json")))
    extract_size = sum(f.stat().st_size for f in EXTRACTS_DIR.glob("*.json"))
    return {
        "transcripts": transcript_count,
        "transcript_size_mb": round(total_size / 1048576, 2),
        "extracts": extract_count,
        "extract_size_kb": round(extract_size / 1024, 1),
        "compression_ratio": f"{round(total_size / max(extract_size, 1))}:1" if extract_size > 0 else "n/a",
        "extraction_running": _extraction_running
    }

# ===== EMAIL DISCOVERY =====
from email_discovery import run_email_discovery

_email_discovery_running = False

async def _run_email_discovery_bg(limit=50):
    global _email_discovery_running
    _email_discovery_running = True
    try:
        result = await run_email_discovery(limit=limit)
        log_file = LOGS_DIR / "email_discovery_runs.jsonl"
        with open(log_file, "a") as f:
            f.write(json.dumps({"timestamp": datetime.utcnow().isoformat(), **result}) + "\n")
    finally:
        _email_discovery_running = False

@app.post("/api/v1/discover/emails")
async def trigger_email_discovery(request: Request, background_tasks: BackgroundTasks):
    """Discover emails for leads that have websites but no email."""
    check_auth(request)
    global _email_discovery_running
    if _email_discovery_running:
        return {"status": "already_running"}
    body = await request.json() if await request.body() else {}
    limit = body.get("limit", 50)
    background_tasks.add_task(_run_email_discovery_bg, limit=limit)
    return {"status": "started", "limit": limit}

@app.get("/api/v1/discover/status")
async def email_discovery_status():
    return {"running": _email_discovery_running}

# ===== EMAIL PATTERN GUESSING =====
from email_guesser import run_email_guessing

@app.post("/api/v1/discover/guess")
async def trigger_email_guess(request: Request, background_tasks: BackgroundTasks):
    """Guess emails using common patterns + MX verification."""
    check_auth(request)
    body = await request.json() if await request.body() else {}
    limit = body.get("limit", 70)
    background_tasks.add_task(run_email_guessing, limit=limit)
    return {"status": "started", "limit": limit}

# ===== LEAD SCORING =====
import scorer

@app.post("/api/v1/leads/score")
async def score_leads(request: Request):
    """Score all leads in the database."""
    check_auth(request)
    result = scorer.score_all_leads()
    return result

@app.get("/api/v1/leads/score/{lead_id}")
async def score_single_lead(lead_id: int):
    """Score a single lead and return breakdown."""
    lead = db.get_lead_by_id(lead_id)
    if not lead:
        raise HTTPException(404, "Lead not found")
    result = scorer.score_lead(lead)
    return {"lead_id": lead_id, "domain": lead.get("domain"), **result}

# ===== INSTANTLY INTEGRATION =====
import instantly_push

@app.get("/api/v1/instantly/status")
async def instantly_status():
    """Check Instantly connection and available campaigns."""
    return instantly_push.get_status()

@app.get("/api/v1/instantly/campaigns")
async def instantly_campaigns():
    """List Instantly campaigns."""
    return instantly_push.list_campaigns()

@app.post("/api/v1/instantly/push")
async def instantly_push_leads(request: Request):
    """Push qualified leads to an Instantly campaign."""
    check_auth(request)
    body = await request.json()
    campaign_id = body.get("campaign_id")
    if not campaign_id:
        raise HTTPException(400, "campaign_id required")
    min_score = body.get("min_score", 50)
    limit = body.get("limit", 100)
    result = instantly_push.push_leads(campaign_id, min_score=min_score, limit=limit)
    return result
