#!/usr/bin/env python3
"""Scraper Pipeline Runner v1.0
Pulls pending batches from VPS1, scrapes them, pushes results back.
Also supports feeding CCB leads directly.

Usage:
  python3 runner.py             # Pull pending batches from VPS1
  python3 runner.py --ccb 100   # Feed 100 CCB leads (creates batch on VPS1 first)
  python3 runner.py --urls urls.txt  # Scrape URLs from a file
"""
import asyncio
import json
import httpx
import logging
import time
import os
import sys
from datetime import datetime

from scraper import scrape_batch

logging.basicConfig(level=logging.INFO, format='%(asctime)s [RUNNER] %(message)s')
log = logging.getLogger('runner')

RECEIVER_URL = os.environ.get('RECEIVER_URL', 'https://leads.millyweb.com')
API_KEY = os.environ.get('API_KEY', 'milly-dev-key-change-me')
CONCURRENCY = int(os.environ.get('SCRAPE_CONCURRENCY', '6'))
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '25'))
LOOP_INTERVAL = int(os.environ.get('LOOP_INTERVAL', '300'))  # 5 min between cycles

HEADERS = {'X-API-Key': API_KEY, 'Content-Type': 'application/json'}


def api_get(path):
    try:
        r = httpx.get(f'{RECEIVER_URL}{path}', headers=HEADERS, timeout=15)
        return r.json()
    except Exception as e:
        log.error(f'API GET {path}: {e}')
        return None


def api_post(path, data):
    try:
        r = httpx.post(f'{RECEIVER_URL}{path}', headers=HEADERS, json=data, timeout=30)
        return r.json()
    except Exception as e:
        log.error(f'API POST {path}: {e}')
        return None


async def process_batch(batch_id: str, urls: list):
    """Scrape a batch of URLs and push results to VPS1."""
    log.info(f'Processing batch {batch_id}: {len(urls)} URLs')
    start = time.time()

    # Scrape in chunks
    all_results = []
    for i in range(0, len(urls), BATCH_SIZE):
        chunk = urls[i:i+BATCH_SIZE]
        log.info(f'  Scraping chunk {i//BATCH_SIZE + 1}: {len(chunk)} URLs')
        results = await scrape_batch(chunk, concurrency=CONCURRENCY)
        all_results.extend(results)
        log.info(f'  Chunk done: {len([r for r in results if not r.get("error")])} successful')

    elapsed = round(time.time() - start, 1)
    successes = len([r for r in all_results if not r.get('error')])
    log.info(f'Batch {batch_id} done in {elapsed}s: {successes}/{len(all_results)} successful')

    # Push results to VPS1
    push = api_post('/api/v1/leads/results', {
        'batch_id': batch_id,
        'results': all_results
    })
    if push:
        log.info(f'Pushed to VPS1: {push}')
    else:
        log.error('Failed to push results to VPS1')

    return all_results


async def run_pending_loop():
    """Continuously pull and process pending batches from VPS1."""
    while True:
        log.info('Checking for pending batches...')
        batch = api_get('/api/v1/leads/batch/pending')

        if batch and batch.get('batch_id'):
            bid = batch['batch_id']
            urls = batch.get('urls', [])
            log.info(f'Got pending batch {bid}: {len(urls)} URLs')
            await process_batch(bid, urls)
        else:
            log.info('No pending batches')

        log.info(f'Sleeping {LOOP_INTERVAL}s...')
        await asyncio.sleep(LOOP_INTERVAL)


async def run_ccb_feed(count: int):
    """Feed CCB leads from VPS1's database into the scraping pipeline."""
    log.info(f'Requesting {count} CCB leads for scraping...')

    # Get unscraped leads from the receiver
    leads = api_get(f'/api/v1/leads/master?limit={count}&sort_by=created_at&order=ASC')
    if not leads or not leads.get('leads'):
        log.error('No leads returned from VPS1')
        return

    # Extract URLs that have websites
    urls = []
    for lead in leads['leads']:
        url = lead.get('source_url') or lead.get('final_url') or lead.get('website_url')
        if url and url != 'None' and url.startswith('http'):
            urls.append(url)

    if not urls:
        log.error('No scrape-able URLs found')
        return

    log.info(f'Got {len(urls)} URLs to scrape from {len(leads["leads"])} leads')

    # Create a batch on VPS1
    batch = api_post('/api/v1/leads/batch/create', {
        'urls': urls,
        'source': 'contabo-ccb-feed'
    })
    if not batch or not batch.get('batch_id'):
        log.error('Failed to create batch on VPS1')
        return

    bid = batch['batch_id']
    log.info(f'Created batch {bid} with {len(urls)} URLs')
    await process_batch(bid, urls)


async def run_file(filepath: str):
    """Scrape URLs from a text file."""
    with open(filepath) as f:
        urls = [line.strip() for line in f if line.strip() and line.strip().startswith('http')]

    log.info(f'Loaded {len(urls)} URLs from {filepath}')
    bid = f'file-{int(time.time())}'

    # Create batch on VPS1
    batch = api_post('/api/v1/leads/batch/create', {
        'urls': urls,
        'source': f'contabo-file-{filepath}'
    })
    if batch and batch.get('batch_id'):
        bid = batch['batch_id']

    await process_batch(bid, urls)


if __name__ == '__main__':
    if '--ccb' in sys.argv:
        idx = sys.argv.index('--ccb')
        count = int(sys.argv[idx + 1]) if idx + 1 < len(sys.argv) else 100
        asyncio.run(run_ccb_feed(count))
    elif '--urls' in sys.argv:
        idx = sys.argv.index('--urls')
        filepath = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else 'urls.txt'
        asyncio.run(run_file(filepath))
    else:
        asyncio.run(run_pending_loop())
