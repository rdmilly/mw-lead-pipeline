#!/usr/bin/env python3
"""MillyExt Enrichment Pipeline Orchestrator
Runs all enrichment stages in sequence inside the container.
Designed to be called via: docker exec millyext-receiver python3 /app/enrichment_orchestrator.py

v2.0 — Uses PostgreSQL via master_sync bridge
"""
import httpx, json, time, logging, sys
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger('orchestrator')

API = 'http://localhost:8099'
KEY = {'X-API-Key': 'milly-dev-key-change-me', 'Content-Type': 'application/json'}
LOG_FILE = Path('/data/logs/orchestrator.jsonl')
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

# PostgreSQL backend
import db
import master_sync

def get_stats():
    """Get pipeline stats from PostgreSQL."""
    return db.get_stats()

def call_api(method, endpoint, body=None):
    try:
        if method == 'POST':
            r = httpx.post(f'{API}{endpoint}', headers=KEY, json=body or {}, timeout=30)
        else:
            r = httpx.get(f'{API}{endpoint}', headers=KEY, timeout=30)
        return r.json()
    except Exception as e:
        log.error(f'API call failed: {endpoint} - {e}')
        return {'error': str(e)}

def wait_done(status_endpoint, max_wait=900):
    elapsed = 0
    while elapsed < max_wait:
        time.sleep(15)
        elapsed += 15
        status = call_api('GET', status_endpoint)
        running = status.get('running', False)
        if not running:
            log.info(f'  Completed after {elapsed}s')
            return status
        if elapsed % 60 == 0:
            log.info(f'  Still running... ({elapsed}s)')
    log.warning(f'  Timed out after {max_wait}s')
    return {'timeout': True}

def main():
    log.info('=' * 50)
    log.info('ENRICHMENT PIPELINE STARTING')
    log.info('=' * 50)

    before = get_stats()
    log.info(f'BEFORE: {json.dumps(before, default=str)}')

    results = {'timestamp': datetime.utcnow().isoformat(), 'before': before, 'stages': {}}

    # Sync: Dump PostgreSQL -> master_leads.json for enrichment modules
    log.info('--- SYNC: PostgreSQL -> master_leads.json ---')
    master_sync.dump_to_json()

    # Stage 1: Email Discovery
    log.info('--- STAGE 1: Email Discovery (website scraping) ---')
    r = call_api('POST', '/api/v1/discover/emails', {'limit': 200})
    log.info(f'  Triggered: {r}')
    if r.get('status') == 'started':
        s = wait_done('/api/v1/discover/status', max_wait=900)
        results['stages']['email_discovery'] = s
    time.sleep(3)

    # Stage 2: Email Guessing
    log.info('--- STAGE 2: Email Pattern Guessing ---')
    r = call_api('POST', '/api/v1/discover/guess', {'limit': 200})
    log.info(f'  Triggered: {r}')
    time.sleep(90)
    results['stages']['email_guess'] = 'waited_90s'
    time.sleep(3)

    # Stage 3: Full Enrichment (Google Places + SOS + Website)
    log.info('--- STAGE 3: Full Multi-Source Enrichment ---')
    r = call_api('POST', '/api/v1/enrich-full/run', {'limit': 100})
    log.info(f'  Triggered: {r}')
    if r.get('status') == 'started':
        s = wait_done('/api/v1/enrich-full/status', max_wait=1200)
        results['stages']['full_enrichment'] = s
    time.sleep(3)

    # Stage 4: Owner Name + Email Verification
    log.info('--- STAGE 4: Name Extraction & Email Verification ---')
    r = call_api('POST', '/api/v1/enrich/run', {'limit': 100})
    log.info(f'  Triggered: {r}')
    if r.get('status') == 'started':
        s = wait_done('/api/v1/enrich/status', max_wait=900)
        results['stages']['name_email_verify'] = s

    # Sync: master_leads.json -> PostgreSQL (capture enrichment results)
    log.info('--- SYNC: master_leads.json -> PostgreSQL ---')
    master_sync.sync_from_json()

    after = get_stats()
    log.info(f'AFTER: {json.dumps(after, default=str)}')

    # Calculate deltas
    deltas = {}
    for k in before:
        if isinstance(before[k], (int, float)) and isinstance(after.get(k), (int, float)):
            deltas[k] = (after[k] or 0) - (before[k] or 0)
    log.info(f'DELTAS: {json.dumps(deltas, default=str)}')

    results['after'] = after
    results['deltas'] = deltas

    # Persist log
    with open(LOG_FILE, 'a') as f:
        f.write(json.dumps(results, default=str) + '\n')

    log.info('=' * 50)
    log.info('PIPELINE COMPLETE')
    log.info('=' * 50)

if __name__ == '__main__':
    main()
