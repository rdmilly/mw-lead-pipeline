#!/usr/bin/env python3
"""Read-only segmenter for master_leads.json. NEVER mutates data. Canonical copy: /opt/projects/millyweb-skills (vps2).
Usage:
  segment.py --stats
  segment.py --segment hot-verified|enriched|outreach-ready|category=<X> [--limit N] [--format json|csv] [--out PATH]
"""
import json, csv, sys, argparse, collections, io

DATA = "/opt/projects/mw-lead-pipeline/data/master_leads.json"

def load():
    with open(DATA) as f:
        d = json.load(f)
    return d["leads"], d.get("updated_at")

def name_of(l):
    return l.get("first_name") or (l.get("owner_info") or {}).get("name")

def ready(l):
    return bool(l.get("best_email")) and bool(name_of(l))

def match(l, seg):
    if seg == "hot-verified":
        return l.get("quality_tier") in ("hot", "warm") and l.get("has_verified_email") is True
    if seg == "enriched":
        return l.get("enrichment_status") == "enriched"
    if seg == "outreach-ready":
        return ready(l)
    if seg.startswith("category="):
        want = seg.split("=", 1)[1].lower()
        return ready(l) and want in (l.get("category") or "").lower()
    raise SystemExit(f"unknown segment: {seg}")

def stats(leads, updated):
    v = leads.values()
    tiers = collections.Counter(l.get("quality_tier") for l in v)
    cats = collections.Counter(l.get("category") for l in v if l.get("category"))
    print(json.dumps({
        "updated_at": updated, "total": len(leads),
        "phones": sum(1 for l in v if l.get("phones")),
        "enriched": sum(1 for l in v if l.get("enrichment_status") == "enriched"),
        "verified_email": sum(1 for l in v if l.get("has_verified_email") is True),
        "owner_names": sum(1 for l in v if (l.get("owner_info") or {}).get("name")),
        "outreach_ready": sum(1 for l in v if ready(l)),
        "hot_verified": sum(1 for l in v if match(l, "hot-verified")),
        "tiers": dict(tiers), "top_categories": cats.most_common(10),
    }, indent=2))

FIELDS = ["business_name", "best_email", "first_name", "last_name", "owner_name",
          "phones", "website", "category", "quality_tier", "lead_score", "city", "pipeline_status"]

def row(slug, l):
    r = {k: l.get(k) for k in FIELDS}
    r["slug"] = slug
    r["owner_name"] = r["owner_name"] or (l.get("owner_info") or {}).get("name")
    r["phones"] = ";".join(l.get("phones") or [])
    return r

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--stats", action="store_true")
    p.add_argument("--segment")
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--format", choices=["json", "csv"], default="json")
    p.add_argument("--out")
    a = p.parse_args()
    leads, updated = load()
    if a.stats or not a.segment:
        return stats(leads, updated)
    rows = [row(s, l) for s, l in leads.items() if match(l, a.segment)]
    rows.sort(key=lambda r: (r.get("lead_score") or 0), reverse=True)
    if a.limit:
        rows = rows[:a.limit]
    if a.format == "csv":
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=["slug"] + FIELDS)
        w.writeheader(); w.writerows(rows)
        out = buf.getvalue()
    else:
        out = json.dumps({"segment": a.segment, "count": len(rows), "rows": rows}, indent=2, default=str)
    if a.out:
        with open(a.out, "w") as f: f.write(out)
        print(f"wrote {len(rows)} rows -> {a.out}")
    else:
        print(out)

if __name__ == "__main__":
    main()
