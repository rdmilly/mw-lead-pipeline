#!/usr/bin/env python3
"""Full Pipeline Automation v2 — All Free Enrichment Layers
Runs the complete lead pipeline on a schedule:
1. Feed CCB leads (with offset tracking + years_in_business)
2. Find websites via Google Places (with confidence filter)
3. Create scrape batches for Contabo
4. Run email discovery
5. BBB enrichment
6. Score all leads (email required for warm)
7. Auto-push warm+ leads to Instantly

Run: python3 /app/pipeline_auto.py [--once] [--step STEP_NAME]
"""
import time, sys, logging, json, sqlite3
from datetime import datetime
from difflib import SequenceMatcher

import db, master_sync, scorer, places_finder, maps_scraper, multi_source_scraper, pipeline_email
import instantly_push, yelp_bbb

logging.basicConfig(level=logging.INFO, format='%(asctime)s [PIPELINE] %(message)s')
log = logging.getLogger('pipeline')

CYCLE_INTERVAL = 3600
CCB_BATCH = 50
PLACES_BATCH = 200
INSTANTLY_CAMPAIGN = 'e7fa0a17-4994-43a1-a847-d9a714cd21f7'
INSTANTLY_MIN_SCORE = 50


def step_feed_ccb(batch_size=CCB_BATCH):
    """Feed new CCB contractors with offset tracking."""
    log.info('=== STEP 1: Feed CCB leads ===')
    try:
        ccb = sqlite3.connect('/data/ccb_active.db')
        metro = ['PORTLAND','NEWBERG','TIGARD','BEAVERTON','HILLSBORO','LAKE OSWEGO',
                 'OREGON CITY','TUALATIN','SHERWOOD','WILSONVILLE','WEST LINN',
                 'MILWAUKIE','CLACKAMAS','GRESHAM','TROUTDALE','CANBY','MCMINNVILLE',
                 'SALEM','ALBANY','CORVALLIS','BEND','EUGENE','MEDFORD','REDMOND',
                 'SPRINGFIELD','GRANTS PASS','VANCOUVER']

        # Get already-imported license numbers
        with db.get_conn() as conn:
            cur = conn.cursor()
            cur.execute('SELECT license_number FROM ccb_imported')
            imported = {r[0] for r in cur.fetchall()}

        placeholders = ','.join(['?' for _ in metro])
        ccb_cur = ccb.cursor()
        ccb_cur.execute(f'''SELECT license_number, full_name, phone_number, address, city, state,
                                zip_code, rmi_first, rmi_last, endorsement_text, orig_regis_date
                         FROM contractors
                         WHERE city IN ({placeholders})
                         AND phone_number IS NOT NULL AND phone_number != ''
                         ORDER BY ROWID''', metro)

        new = 0
        for row in ccb_cur.fetchall():
            lic, name, phone, addr, city, state, zipcode, rmi_first, rmi_last, endorsement, regdate = row
            if lic in imported:
                continue
            if new >= batch_size:
                break

            slug = name.lower().replace(' ', '-').replace('&', 'and').replace(',', '').replace('.', '').replace('/', '-')[:50]
            fake_domain = slug + '.ccb.local'
            owner_name = f'{rmi_first} {rmi_last}'.strip() if rmi_first else ''

            # Calculate years in business
            years = None
            if regdate:
                try:
                    reg = datetime.strptime(regdate, '%m/%d/%Y')
                    years = (datetime.now() - reg).days // 365
                except Exception:
                    pass

            try:
                with db.get_conn() as conn:
                    cur = conn.cursor()
                    cur.execute('''INSERT INTO businesses
                        (domain, business_name, phones, primary_phone, owner_name, owner_source,
                         address, city, state, zip_code, license_number, license_status,
                         category, years_in_business, source, pipeline_status, enrichment_status,
                         created_at, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                        ON CONFLICT (domain) DO NOTHING
                    ''', (
                        fake_domain, name, [phone] if phone else [], phone,
                        owner_name, 'ccb_rmi',
                        (addr or '').title(), (city or '').title(), state or 'OR', zipcode or '',
                        lic or '', 'active',
                        (endorsement or '').split(',')[0].strip()[:50] if endorsement else '',
                        years, 'ccb_database', 'new', 'pending'
                    ))
                    if cur.rowcount > 0:
                        new += 1
                    # Track as imported regardless
                    cur.execute('INSERT INTO ccb_imported (license_number) VALUES (%s) ON CONFLICT DO NOTHING', (lic,))
            except Exception as e:
                if new < 3:
                    log.error(f'CCB insert error: {e}')

        ccb.close()
        log.info(f'Fed {new} new CCB leads')
        return new
    except Exception as e:
        log.error(f'CCB feed error: {e}')
        return 0


def step_find_websites(batch_size=PLACES_BATCH):
    """Find websites via Google Places with confidence filtering."""
    log.info('=== STEP 2: Find websites via Google Places ===')
    try:
        result = places_finder.process_leads(limit=batch_size)
        log.info(f'Places: {result}')
        return result.get('found', 0)
    except Exception as e:
        log.error(f'Places error: {e}')
        return 0


def step_create_scrape_batches():
    """Create batches from newly-discovered URLs for Contabo scraper."""
    log.info('=== STEP 3: Create scrape batches for Contabo ===')
    try:
        import httpx
        with db.get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, source_url FROM businesses
                WHERE source_url IS NOT NULL AND source_url != '' AND source_url != 'None'
                AND source_url LIKE 'http%'
                AND enrichment_status = 'pending'
                AND (primary_email IS NULL OR primary_email = '')
                LIMIT 50
            """)
            rows = cur.fetchall()

        if not rows:
            log.info('No URLs needing scraping')
            return 0

        urls = [r[1] for r in rows]
        log.info(f'Creating batch with {len(urls)} URLs')

        r = httpx.post('http://localhost:8099/api/v1/leads/batch/create',
                       headers={'X-API-Key': 'milly-dev-key-change-me', 'Content-Type': 'application/json'},
                       json={'urls': urls, 'source': 'pipeline-auto-places'}, timeout=10)
        result = r.json()
        log.info(f'Batch created: {result.get("batch_id")}, {len(urls)} URLs')

        # Mark these as enrichment in-progress
        ids = [r[0] for r in rows]
        with db.get_conn() as conn:
            cur = conn.cursor()
            for lid in ids:
                cur.execute("UPDATE businesses SET enrichment_status = 'in_progress' WHERE id = %s", (lid,))

        return len(urls)
    except Exception as e:
        log.error(f'Batch creation error: {e}')
        return 0


def step_deep_enrichment():
    """Run deep email extraction + website intelligence (replaces old JSON sync bridge)."""
    log.info('=== STEP 4: Deep email + intelligence ===')
    try:
        result = pipeline_email.run_sync(limit=150)
        log.info(f'Deep enrichment: {result}')
    except Exception as e:
        log.error(f'Deep enrichment error: {e}')


def step_bbb_enrichment():
    """BBB accreditation check."""
    log.info('=== STEP 5: BBB enrichment ===')
    try:
        result = yelp_bbb.enrich_leads(limit=30)
        log.info(f'BBB: {result}')
    except Exception as e:
        log.error(f'BBB error: {e}')


def step_score():
    """Score all leads."""
    log.info('=== STEP 6: Scoring ===')
    try:
        result = scorer.score_all_leads()
        log.info(f'Scored: {result}')
        return result
    except Exception as e:
        log.error(f'Scoring error: {e}')
        return {}


def step_push_instantly():
    """Push warm+ leads with email to Instantly."""
    log.info('=== STEP 7: Push to Instantly ===')
    try:
        status = instantly_push.get_status()
        if not status.get('connected'):
            log.warning(f'Instantly not connected: {status.get("reason")}')
            return 0
        result = instantly_push.push_leads(INSTANTLY_CAMPAIGN, min_score=INSTANTLY_MIN_SCORE, limit=50)
        log.info(f'Instantly: {result}')
        return result.get('pushed', 0)
    except Exception as e:
        log.error(f'Instantly error: {e}')
        return 0


def run_cycle():
    start = time.time()
    log.info('=' * 60)
    log.info(f'PIPELINE CYCLE at {datetime.utcnow().isoformat()}')
    log.info('=' * 60)

    before = db.get_stats()
    log.info(f'BEFORE: total={before["total_leads"]} email={before["with_email"]} exported={before["exported"]}')


    # Step 0: Google Maps category scraping (pre-enriched leads)
    log.info("=== STEP 0: Google Maps category scrape ===")
    try:
        multi_result = multi_source_scraper.run_multi_source(max_combos=10)
        log.info(f"Multi-source: {multi_result}")
    except Exception as e:
        log.error(f"Maps scraper error: {e}")
    new_ccb = step_feed_ccb()
    websites = step_find_websites()
    if websites > 0:
        step_create_scrape_batches()
    step_deep_enrichment()
    # step_bbb_enrichment()  # BBB returns 403 from datacenter IP
    step_score()
    step_push_instantly()

    after = db.get_stats()
    elapsed = round(time.time() - start, 1)
    log.info(f'AFTER: total={after["total_leads"]} email={after["with_email"]} exported={after["exported"]}')
    log.info(f'DELTA: +{after["total_leads"]-before["total_leads"]} leads, +{after["with_email"]-before["with_email"]} emails, +{after["exported"]-before["exported"]} exported')
    log.info(f'Cycle done in {elapsed}s')
    log.info('=' * 60)


def main():
    once = '--once' in sys.argv
    step_only = None
    if '--step' in sys.argv:
        idx = sys.argv.index('--step')
        step_only = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None

    if step_only:
        steps = {'ccb': step_feed_ccb, 'places': step_find_websites, 'batches': step_create_scrape_batches,
                 'email': step_email_discovery, 'bbb': step_bbb_enrichment, 'score': step_score,
                 'instantly': step_push_instantly}
        fn = steps.get(step_only)
        if fn:
            fn()
        else:
            print(f'Unknown step: {step_only}. Available: {list(steps.keys())}')
    elif once:
        run_cycle()
    else:
        log.info(f'Continuous pipeline (cycle every {CYCLE_INTERVAL}s)')
        while True:
            try:
                run_cycle()
            except Exception as e:
                log.error(f'Cycle failed: {e}')
            time.sleep(CYCLE_INTERVAL)


if __name__ == '__main__':
    main()
