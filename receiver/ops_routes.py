"""Ops API routes - pipeline status, scraper proxy, activity feed."""
import json
import httpx
import logging
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter

router = APIRouter(prefix="/api/v1/ops")
logger = logging.getLogger("ops")

VPS2_SCRAPER = "http://10.0.0.2:8100"
DATA_DIR = Path("/data")


@router.get("/pipeline")
async def pipeline_status():
    """Aggregated pipeline status - everything in one call."""
    result = {"timestamp": datetime.utcnow().isoformat()}

    # 1. Local enrichment status
    try:
        from enrich_routes import _enrichment_running, _enrichment_status
        result["enrichment"] = {
            "running": _enrichment_running,
            **(_enrichment_status or {})
        }
    except:
        result["enrichment"] = {"running": False}

    # 2. Email discovery status
    try:
        from email_discovery import _discovery_running, _discovery_log
        result["discovery"] = {
            "running": _discovery_running,
            "last_run": _discovery_log
        }
    except:
        result["discovery"] = {"running": False}

    # 3. Lead stats
    try:
        master_path = DATA_DIR / "leads" / "master_leads.json"
        if master_path.exists():
            with open(master_path) as f:
                d = json.load(f)
            leads = d.get("leads", {})
            total = len(leads)
            with_email = sum(1 for l in leads.values() if l.get("emails"))
            with_phone = sum(1 for l in leads.values() if l.get("phones"))
            with_owner = sum(1 for l in leads.values() if l.get("owner_name"))
            enriched = sum(1 for l in leads.values() if l.get("enriched") or l.get("enrichment_status"))
            
            # Source breakdown
            sources = {}
            for l in leads.values():
                src = l.get("source") or "direct"
                if src not in sources:
                    sources[src] = {"count": 0, "with_email": 0, "with_phone": 0}
                sources[src]["count"] += 1
                if l.get("emails"): sources[src]["with_email"] += 1
                if l.get("phones"): sources[src]["with_phone"] += 1

            # Category breakdown
            categories = {}
            for l in leads.values():
                cat = l.get("category", l.get("_category", "Unknown"))
                # Clean up concatenated YP categories
                if cat and len(cat) > 30:
                    cat = cat.split("Plumbers")[0] if "Plumbers" in cat else cat[:30]
                if not cat: cat = "Unknown"
                categories[cat] = categories.get(cat, 0) + 1

            # Recent leads (last 24h)
            recent = []
            for dom, l in sorted(leads.items(), key=lambda x: x[1].get("_first_seen", ""), reverse=True)[:20]:
                recent.append({
                    "domain": dom,
                    "name": l.get("business_name", ""),
                    "source": l.get("source", "?"),
                    "has_email": bool(l.get("emails")),
                    "has_phone": bool(l.get("phones")),
                    "has_owner": bool(l.get("owner_name")),
                    "first_seen": l.get("_first_seen", ""),
                    "category": l.get("category", l.get("_category", "")),
                })

            result["leads"] = {
                "total": total, "with_email": with_email,
                "with_phone": with_phone, "with_owner": with_owner,
                "enriched": enriched,
                "sources": sources, "categories": categories,
                "recent": recent,
            }
    except Exception as e:
        result["leads"] = {"error": str(e)}

    # 4. VPS2 scraper status (proxy)
    try:
        async with httpx.AsyncClient(timeout=3) as client:
            resp = await client.get(f"{VPS2_SCRAPER}/stats")
            result["scraper"] = resp.json()
    except:
        result["scraper"] = {"error": "unreachable"}

    # 5. VPS2 scraper jobs
    try:
        async with httpx.AsyncClient(timeout=3) as client:
            resp = await client.get(f"{VPS2_SCRAPER}/jobs")
            jobs_data = resp.json()
            jobs = jobs_data.get("jobs", [])
            # Summarize by source
            source_perf = {}
            for j in jobs:
                src = j.get("source", "?")
                if src not in source_perf:
                    source_perf[src] = {"total": 0, "completed": 0, "failed": 0, "results": 0}
                source_perf[src]["total"] += 1
                if j["status"] == "completed":
                    source_perf[src]["completed"] += 1
                    source_perf[src]["results"] += j.get("results_count", 0)
                elif j["status"] == "failed":
                    source_perf[src]["failed"] += 1

            result["scraper_jobs"] = {
                "total": len(jobs),
                "running": sum(1 for j in jobs if j["status"] == "running"),
                "queued": sum(1 for j in jobs if j["status"] == "queued"),
                "completed": sum(1 for j in jobs if j["status"] == "completed"),
                "failed": sum(1 for j in jobs if j["status"] == "failed"),
                "source_performance": source_perf,
                "recent": [{"source": j["source"], "status": j["status"],
                            "results": j.get("results_count", 0),
                            "created": j.get("created_at", "")[:19],
                            "completed": (j.get("completed_at") or "")[:19]}
                           for j in jobs[:20]],
            }
    except:
        result["scraper_jobs"] = {"error": "unreachable"}

    # 6. Discovery logs
    try:
        log_path = DATA_DIR / "logs" / "email_discovery.jsonl"
        if log_path.exists():
            lines = log_path.read_text().strip().split("\n")
            discovery_runs = [json.loads(l) for l in lines[-10:] if l.strip()]
            result["discovery_history"] = discovery_runs
    except:
        result["discovery_history"] = []

    return result


@router.get("/scraper/sources")
async def scraper_sources():
    """Get available scraper sources from VPS2."""
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{VPS2_SCRAPER}/sources")
            return resp.json()
    except Exception as e:
        return {"error": str(e)}


@router.post("/scraper/trigger")
async def trigger_scraper(sources: list = None, queries: list = None):
    """Trigger scraper jobs on VPS2."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(f"{VPS2_SCRAPER}/scrape/batch",
                json={"sources": sources or [], "urls": queries or [], "priority": 3})
            return resp.json()
    except Exception as e:
        return {"error": str(e)}


@router.get("/leads")
async def all_leads(offset: int = 0, limit: int = 200):
    """All leads for dashboard table."""
    try:
        master_path = DATA_DIR / "leads" / "master_leads.json"
        with open(master_path) as f:
            d = json.load(f)
        leads = d.get("leads", {})
        items = []
        for dom, l in leads.items():
            items.append({
                "domain": dom,
                "name": l.get("business_name", ""),
                "email": (l.get("emails") or [""])[0],
                "phone": (l.get("phones") or [""])[0],
                "owner": l.get("owner_name", ""),
                "source": l.get("source") or "direct",
                "category": l.get("category", l.get("_category", "")),
                "website": l.get("website", ""),
                "has_email": bool(l.get("emails")),
                "has_phone": bool(l.get("phones")),
                "has_owner": bool(l.get("owner_name")),
                "enriched": bool(l.get("enriched") or l.get("enrichment_status")),
                "first_seen": l.get("_first_seen", ""),
            })
        # Sort by has_email desc, then name
        items.sort(key=lambda x: (not x["has_email"], x["name"].lower()))
        return {"total": len(items), "leads": items[offset:offset+limit]}
    except Exception as e:
        return {"error": str(e)}
