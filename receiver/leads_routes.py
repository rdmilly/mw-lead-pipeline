from git_sync import sync as git_sync
# MillyExt Receiver - Lead Scraper Routes
# Manages batch URL submissions, result collection, PostgreSQL lead store with dedup
# v2.0 - Migrated from master_leads.json to PostgreSQL

import json, uuid, re, csv, io
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, unquote
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse

# PostgreSQL backend
import db

router = APIRouter(prefix="/api/v1/leads", tags=["leads"])

DATA_DIR = Path("/data")
LEADS_DIR = DATA_DIR / "leads"
BATCHES_DIR = LEADS_DIR / "batches"
LEAD_RESULTS_DIR = LEADS_DIR / "results"

for d in [LEADS_DIR, BATCHES_DIR, LEAD_RESULTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)


def check_auth(request: Request):
    import os
    key = request.headers.get("X-API-Key", "")
    expected = os.environ.get("MILLYEXT_API_KEY", "milly-dev-key-change-me")
    if key != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")


# ===== PHONE CLEANUP =====
def clean_phone(raw):
    """Normalize phone number to consistent format."""
    decoded = unquote(str(raw))
    digits = re.sub(r'[^\d]', '', decoded)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return decoded.strip()


def dedup_phones(phones):
    """Deduplicate phone list by normalized digits."""
    seen = set()
    result = []
    for p in phones:
        digits = re.sub(r'[^\d]', '', unquote(str(p)))
        if len(digits) > 6 and digits[-10:] not in seen:
            seen.add(digits[-10:])
            result.append(clean_phone(p))
    return result


def dedup_emails(emails):
    """Deduplicate and clean email list."""
    seen = set()
    result = []
    for e in emails:
        e = e.strip().lower()
        if e and '@' in e and e not in seen and 'example' not in e and 'mysite' not in e:
            seen.add(e)
            result.append(e)
    return result


# ===== BATCH MANAGEMENT =====
# Batches stay as JSON files — they're ephemeral workflow state

@router.post("/batch/create")
async def create_batch(request: Request):
    check_auth(request)
    data = await request.json()
    urls = data.get("urls", [])
    if not urls:
        raise HTTPException(400, "urls list required")

    batch_id = str(uuid.uuid4())[:12]
    batch = {
        "batch_id": batch_id,
        "status": "pending",
        "urls": urls,
        "total": len(urls),
        "source": data.get("source", "n8n"),
        "search_query": data.get("search_query", ""),
        "created_at": datetime.utcnow().isoformat()
    }
    filepath = BATCHES_DIR / f"{batch_id}.json"
    with open(filepath, "w") as f:
        json.dump(batch, f, indent=2)
    return {"batch_id": batch_id, "total": len(urls), "status": "pending"}


@router.get("/batch/pending")
async def get_pending_batch(request: Request):
    check_auth(request)
    pending = []
    for f in sorted(BATCHES_DIR.glob("*.json")):
        with open(f) as fh:
            batch = json.load(fh)
        if batch.get("status") == "pending":
            pending.append(batch)
    if not pending:
        return {"urls": [], "batch_id": None}

    batch = pending[0]
    batch["status"] = "processing"
    batch["started_at"] = datetime.utcnow().isoformat()
    filepath = BATCHES_DIR / f"{batch['batch_id']}.json"
    with open(filepath, "w") as f:
        json.dump(batch, f, indent=2)
    return batch


@router.get("/batch/{batch_id}")
async def get_batch_status(batch_id: str):
    filepath = BATCHES_DIR / f"{batch_id}.json"
    if not filepath.exists():
        raise HTTPException(404, "Batch not found")
    with open(filepath) as f:
        return json.load(f)


@router.get("/batches")
async def list_batches(status: str = None, limit: int = 20):
    batches = []
    for f in sorted(BATCHES_DIR.glob("*.json"), reverse=True):
        with open(f) as fh:
            b = json.load(fh)
        if status and b.get("status") != status:
            continue
        batches.append({
            "batch_id": b["batch_id"],
            "status": b["status"],
            "total": b["total"],
            "source": b.get("source"),
            "search_query": b.get("search_query"),
            "created_at": b.get("created_at"),
            "completed_at": b.get("completed_at")
        })
        if len(batches) >= limit:
            break
    return {"batches": batches, "count": len(batches)}


# ===== RESULTS — now writes to PostgreSQL =====

@router.post("/results")
async def receive_results(request: Request):
    data = await request.json()
    batch_id = data.get("batch_id")
    results = data.get("results", [])
    if not batch_id:
        raise HTTPException(400, "batch_id required")

    # Save raw results to disk (archive)
    result_file = LEAD_RESULTS_DIR / f"{batch_id}.json"
    result_data = {
        "batch_id": batch_id,
        "results": results,
        "total": len(results),
        "successful": len([r for r in results if not r.get("error")]),
        "failed": len([r for r in results if r.get("error")]),
        "received_at": datetime.utcnow().isoformat()
    }
    with open(result_file, "w") as f:
        json.dump(result_data, f, indent=2)

    # Upsert into PostgreSQL
    new_count = 0
    updated_count = 0
    for r in results:
        if r.get("error"):
            continue
        lead = {
            "business_name": r.get("business_name") or r.get("name", ""),
            "source_url": r.get("source_url", ""),
            "final_url": r.get("final_url", ""),
            "place_id": r.get("place_id", ""),
            "emails": dedup_emails(r.get("emails", [])),
            "phones": dedup_phones(r.get("phones", [])),
            "owner_info": r.get("owner_info", {}),
            "meta": r.get("meta", {}),
            "social_links": r.get("social_links", {}),
            "subpages_scraped": r.get("subpages_scraped", []),
            "scraped_at": r.get("scraped_at", datetime.utcnow().isoformat()),
            "batch_id": batch_id,
            "source": "scraper"
        }
        try:
            bid, is_new = db.upsert_lead(lead)
            if bid:
                if is_new:
                    new_count += 1
                else:
                    updated_count += 1
                db.log_event(bid, 'scraped', {'batch_id': batch_id, 'source_url': lead['source_url']})
        except Exception as e:
            print(f"[LEADS] Error upserting lead: {e}")

    # Update batch status
    batch_file = BATCHES_DIR / f"{batch_id}.json"
    if batch_file.exists():
        with open(batch_file) as f:
            batch = json.load(f)
        batch["status"] = "completed"
        batch["completed_at"] = datetime.utcnow().isoformat()
        batch["results_count"] = len(results)
        batch["successful"] = result_data["successful"]
        batch["failed"] = result_data["failed"]
        batch["new_leads"] = new_count
        batch["updated_leads"] = updated_count
        with open(batch_file, "w") as f:
            json.dump(batch, f, indent=2)

    # Auto-sync to GitHub after every batch
    try:
        git_sync(f"leads: batch {batch_id} +{new_count} new +{updated_count} updated")
    except Exception as e:
        print(f"[GIT_SYNC] non-fatal error: {e}")

    return {
        "status": "received",
        "batch_id": batch_id,
        "total": len(results),
        "successful": result_data["successful"],
        "new_leads": new_count,
        "updated_leads": updated_count
    }


@router.get("/results/{batch_id}")
async def get_results(batch_id: str):
    result_file = LEAD_RESULTS_DIR / f"{batch_id}.json"
    if not result_file.exists():
        batch_file = BATCHES_DIR / f"{batch_id}.json"
        if batch_file.exists():
            with open(batch_file) as f:
                batch = json.load(f)
            return {"batch_id": batch_id, "status": batch["status"], "results": None}
        raise HTTPException(404, "Batch not found")
    with open(result_file) as f:
        return json.load(f)


# ===== MASTER LEADS — now PostgreSQL =====

@router.get("/master")
async def get_master_leads(limit: int = 100, offset: int = 0, category: str = None,
                           has_email: bool = None, contacted: bool = None,
                           status: str = None, tier: str = None,
                           min_score: float = None,
                           sort_by: str = 'created_at', order: str = 'DESC'):
    """Get master lead list with filtering from PostgreSQL."""
    result = db.list_leads(
        limit=limit,
        offset=offset,
        status=status,
        tier=tier,
        min_score=min_score,
        sort_by=sort_by,
        order=order
    )

    leads = result['leads']

    # Additional filters not in DB query
    if category:
        leads = [l for l in leads if category.lower() in (l.get('category') or '').lower()]
    if has_email is True:
        leads = [l for l in leads if l.get('primary_email')]
    if has_email is False:
        leads = [l for l in leads if not l.get('primary_email')]

    # Serialize datetimes
    for lead in leads:
        for k, v in lead.items():
            if isinstance(v, datetime):
                lead[k] = v.isoformat()

    return {
        "total": result['total'],
        "offset": offset,
        "limit": limit,
        "leads": leads
    }


@router.get("/master/check")
async def check_domains(domains: str):
    """Check which domains already exist. For dedup before scraping."""
    domain_list = [d.strip() for d in domains.split(",")]
    existing = []
    new = []
    for d in domain_list:
        nd = db.normalize_domain(d)
        if db.get_lead_by_domain(nd):
            existing.append(nd)
        else:
            new.append(nd)
    return {"existing": existing, "new": new}


@router.get("/master/{lead_id}")
async def get_lead(lead_id: int):
    """Get a single lead by ID."""
    lead = db.get_lead_by_id(lead_id)
    if not lead:
        raise HTTPException(404, "Lead not found")
    for k, v in lead.items():
        if isinstance(v, datetime):
            lead[k] = v.isoformat()
    return lead


@router.put("/master/{lead_id}")
async def update_lead(lead_id: int, request: Request):
    """Update a lead's fields."""
    check_auth(request)
    updates = await request.json()
    result = db.update_lead(lead_id, updates)
    if not result:
        raise HTTPException(404, "Lead not found or no valid fields")
    for k, v in result.items():
        if isinstance(v, datetime):
            result[k] = v.isoformat()
    return result


@router.delete("/master/{lead_id}")
async def delete_lead(lead_id: int, request: Request):
    """Delete a lead."""
    check_auth(request)
    if db.delete_lead(lead_id):
        return {"deleted": True, "id": lead_id}
    raise HTTPException(404, "Lead not found")


@router.post("/master/store")
async def store_leads_to_master(request: Request):
    """n8n posts formatted leads directly to master store."""
    check_auth(request)
    data = await request.json()
    leads = data.get("leads", [])
    if not leads:
        raise HTTPException(400, "leads list required")

    new_count = 0
    updated_count = 0
    for lead in leads:
        # Transform from n8n format to db format
        db_lead = {
            "business_name": lead.get("business_name", ""),
            "source_url": lead.get("website", ""),
            "place_id": lead.get("place_id", ""),
            "emails": dedup_emails(lead.get("emails", [])),
            "phones": dedup_phones(lead.get("phones", [])),
            "owner_info": {"name": lead.get("owner_name", ""), "source": ""},
            "meta": {},
            "social_links": {},
            "source": lead.get("source", "n8n")
        }
        try:
            bid, is_new = db.upsert_lead(db_lead)
            if bid:
                # Apply additional fields
                extra = {}
                if lead.get("category"):
                    extra["category"] = lead["category"]
                if lead.get("address"):
                    extra["address"] = lead["address"]
                if lead.get("rating"):
                    extra["google_rating"] = float(lead["rating"])
                if lead.get("reviews"):
                    extra["google_review_count"] = int(lead["reviews"])
                if extra:
                    db.update_lead(bid, extra)

                if is_new:
                    new_count += 1
                else:
                    updated_count += 1
        except Exception as e:
            print(f"[LEADS] Error storing lead: {e}")

    stats = db.get_stats()
    return {"new": new_count, "updated": updated_count, "total_master": stats['total_leads']}

    return {"existing": existing, "new": new}


@router.get("/export/csv")
async def export_csv(min_score: float = 0):
    """Download leads as CSV."""
    leads = db.export_leads_csv(min_score=min_score)

    output = io.StringIO()
    if leads:
        writer = csv.DictWriter(output, fieldnames=leads[0].keys(), extrasaction='ignore')
        writer.writeheader()
        for lead in leads:
            row = dict(lead)
            for k, v in row.items():
                if isinstance(v, datetime):
                    row[k] = v.isoformat()
            writer.writerow(row)

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=leads_{datetime.utcnow().strftime('%Y%m%d')}.csv"}
    )


# ===== STATS — now PostgreSQL =====

@router.get("/stats")
async def lead_stats():
    batches = list(BATCHES_DIR.glob("*.json"))
    stats = db.get_stats()

    # Count pending batches
    pending_count = 0
    pending_urls = 0
    for bf in batches:
        try:
            with open(bf) as f:
                b = json.load(f)
            if b.get("status") == "pending":
                pending_count += 1
                pending_urls += len(b.get("urls", []))
        except Exception:
            pass

    return {
        "total_batches": len(batches),
        "pending_batches": pending_count,
        "pending_urls": pending_urls,
        "master_leads": stats.get('total_leads', 0),
        "with_phone": stats.get('with_phone', 0),
        "with_email": stats.get('with_email', 0),
        "enriched": stats.get('enriched', 0),
        "scored": stats.get('scored', 0),
        "exported": stats.get('exported', 0),
        "avg_score": round(float(stats.get('avg_score', 0) or 0), 1),
        "pipeline_breakdown": stats.get('pipeline_breakdown', {}),
        "tier_breakdown": stats.get('tier_breakdown', {})
    }
