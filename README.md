# MW Lead Pipeline

Service business lead scraping pipeline for MW Development.

## Architecture

```
Contabo (single server)
  mw-receiver   — FastAPI, stores leads in /data, auto-pushes to GitHub
  mw-scraper    — Crawls websites, pushes results to receiver
```

## Data

- `data/master_leads.json` — 10,000 enriched leads (auto-committed after each batch)
- `data/ccb_active.db` — 55k Oregon CCB contractors with phone numbers
- `data/bcd_active.db` — 48k Oregon BCD licenses
- `data/oregon_registry_a.db` + `oregon_registry_b.db` — 405k Oregon business registry (split for Git; use SQLite ATTACH to query together)

## Quickstart

```bash
git clone https://github.com/rdmilly/mw-lead-pipeline
cd mw-lead-pipeline
cp .env.example .env  # fill in API keys
docker compose up -d
```

## Querying the split registry

```python
import sqlite3
conn = sqlite3.connect('data/oregon_registry_a.db')
conn.execute("ATTACH 'data/oregon_registry_b.db' AS part_b")
results = conn.execute(
    "SELECT * FROM businesses UNION ALL SELECT * FROM part_b.businesses WHERE city='Portland'"
).fetchall()
```

## Pipeline flow

1. Runner pulls CCB leads → creates batch on receiver
2. Scraper crawls each website, extracts phones/emails/owners
3. Results pushed to receiver → merged into master_leads.json
4. git_sync.py commits + pushes data/ to GitHub automatically
