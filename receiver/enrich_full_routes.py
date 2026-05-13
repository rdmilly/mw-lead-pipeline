# MillyExt - Full Enrichment API Routes
# Endpoints for triggering and monitoring multi-source enrichment

import json, logging, asyncio
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Request, HTTPException, BackgroundTasks
import os

from enrichment_full import (
    run_full_enrichment, load_master, save_master,
    enrich_from_google_places, enrich_from_sos, enrich_from_website,
    merge_enrichment_into_lead, _status as enrichment_status,
)

router = APIRouter(prefix="/api/v1/enrich-full", tags=["full-enrichment"])
logger = logging.getLogger("enrich_full_routes")


def check_auth(request: Request):
    key = request.headers.get("X-API-Key", "")
    expected = os.environ.get("MILLYEXT_API_KEY", "milly-dev-key-change-me")
    if key != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")


@router.post("/run")
async def trigger_full_enrichment(request: Request, background_tasks: BackgroundTasks):
    """Trigger full multi-source enrichment on all leads."""
    check_auth(request)

    body = await request.json() if await request.body() else {}
    limit = body.get("limit", 50)
    force = body.get("force", False)
    domains = body.get("domains", None)

    background_tasks.add_task(run_full_enrichment, limit=limit, force=force, domains=domains)
    return {"status": "started", "limit": limit, "force": force}


@router.get("/status")
async def get_enrichment_status():
    """Get full enrichment status and data coverage stats."""
    master = load_master()
    leads = list(master["leads"].values())
    n = max(len(leads), 1)

    coverage = {
        "total": len(leads),
        "email": sum(1 for l in leads if l.get("emails") and len(l["emails"]) > 0),
        "phone": sum(1 for l in leads if l.get("phones") and len(l["phones"]) > 0),
        "owner_name": sum(1 for l in leads if l.get("owner_name") and l["owner_name"].strip()),
        "address": sum(1 for l in leads if l.get("address") and l["address"].strip()),
        "rating": sum(1 for l in leads if l.get("rating") is not None),
        "reviews": sum(1 for l in leads if l.get("reviews") is not None),
        "hours": sum(1 for l in leads if l.get("business_hours")),
        "years_in_business": sum(1 for l in leads if l.get("years_in_business") is not None),
        "people": sum(1 for l in leads if l.get("people")),
        "service_area": sum(1 for l in leads if l.get("service_area")),
        "google_enriched": sum(1 for l in leads if l.get("_source_google")),
        "sos_enriched": sum(1 for l in leads if l.get("_source_sos")),
        "website_enriched": sum(1 for l in leads if l.get("_source_website")),
        "fully_enriched": sum(1 for l in leads if l.get("enriched")),
    }

    # Calculate percentages
    pct = {k: f"{v*100//n}%" for k, v in coverage.items() if k != "total"}

    return {
        **enrichment_status,
        "coverage": coverage,
        "coverage_pct": pct,
    }


@router.get("/lead/{domain}")
async def get_enriched_lead(domain: str, request: Request):
    """Get full enriched data for a specific lead."""
    master = load_master()
    if domain not in master["leads"]:
        raise HTTPException(404, f"Lead {domain} not found")
    return master["leads"][domain]


@router.post("/lead/{domain}")
async def enrich_single_lead(domain: str, request: Request):
    """Trigger full enrichment for a single lead."""
    check_auth(request)
    master = load_master()
    if domain not in master["leads"]:
        raise HTTPException(404, f"Lead {domain} not found")

    lead = master["leads"][domain]
    biz = lead.get("business_name", domain)

    import httpx
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    async with httpx.AsyncClient(headers=headers, verify=False, timeout=30) as client:
        # All three sources
        google_data = None
        place_id = lead.get("place_id", "")
        if place_id and not place_id.startswith("test_"):
            google_data = await enrich_from_google_places(client, place_id)

        sos_data = enrich_from_sos(biz)

        website_data = None
        website = lead.get("website", "")
        if website:
            website_data = await enrich_from_website(client, website)

        lead, changes = merge_enrichment_into_lead(lead, google_data, sos_data, website_data)

    master["leads"][domain] = lead
    save_master(master)

    return {
        "domain": domain,
        "sources_used": changes,
        "lead": lead,
    }


@router.get("/gaps")
async def get_data_gaps():
    """Show which leads are missing which data — useful for targeted enrichment."""
    master = load_master()
    gaps = {
        "no_email": [],
        "no_owner": [],
        "no_address": [],
        "no_rating": [],
        "not_enriched": [],
    }

    for domain, lead in master["leads"].items():
        name = lead.get("business_name", domain)
        if not lead.get("emails") or len(lead["emails"]) == 0:
            gaps["no_email"].append({"domain": domain, "name": name, "website": lead.get("website", "")})
        if not lead.get("owner_name") or not lead["owner_name"].strip():
            gaps["no_owner"].append({"domain": domain, "name": name})
        if not lead.get("address") or not lead["address"].strip():
            gaps["no_address"].append({"domain": domain, "name": name})
        if lead.get("rating") is None:
            gaps["no_rating"].append({"domain": domain, "name": name, "place_id": lead.get("place_id", "")})
        if not lead.get("enriched"):
            gaps["not_enriched"].append({"domain": domain, "name": name})

    return {
        "summary": {k: len(v) for k, v in gaps.items()},
        "gaps": gaps,
    }


@router.get("/export/full")
async def export_full_data(request: Request):
    """Export all leads with full enrichment data as JSON."""
    check_auth(request)
    master = load_master()
    leads = []
    for domain, lead in master["leads"].items():
        # Strip raw source data for export (too large)
        export_lead = {k: v for k, v in lead.items() if not k.startswith("_source_")}
        leads.append(export_lead)
    return {"total": len(leads), "leads": leads, "exported_at": datetime.utcnow().isoformat()}
