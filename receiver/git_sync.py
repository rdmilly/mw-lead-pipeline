#!/usr/bin/env python3
"""git_sync.py — Auto-commit lead data changes to GitHub.
Called after every batch ingest in leads_routes.py."""
import subprocess
import logging
import os
from datetime import datetime

log = logging.getLogger('git_sync')
REPO_DIR = os.environ.get('REPO_DIR', '/app/repo')

def sync(message: str = None):
    """Stage data/, commit, push. No-op if nothing changed."""
    try:
        result = subprocess.run(
            ['git', '-C', REPO_DIR, 'status', '--porcelain', 'data/'],
            capture_output=True, text=True
        )
        if not result.stdout.strip():
            log.info('git_sync: nothing changed, skip')
            return

        subprocess.run(['git', '-C', REPO_DIR, 'add', 'data/'], check=True)
        msg = message or f'leads: auto-sync {datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}'
        subprocess.run(['git', '-C', REPO_DIR, 'commit', '-m', msg], check=True)
        subprocess.run(['git', '-C', REPO_DIR, 'push', 'origin', 'main'], check=True)
        log.info(f'git_sync: pushed — {msg}')
    except subprocess.CalledProcessError as e:
        log.error(f'git_sync failed: {e}')
