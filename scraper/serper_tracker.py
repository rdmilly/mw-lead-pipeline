"""Serper API Usage Tracker
Tracks queries used, alerts when running low.
Stores count in /data/serper_usage.json
"""
import json, os, logging
from datetime import datetime
from pathlib import Path

log = logging.getLogger('serper_tracker')

USAGE_FILE = Path('/data/serper_usage.json')
QUOTA_LIMIT = 2500
ALERT_THRESHOLD = 200  # Alert when fewer than this remain

def _load():
    if USAGE_FILE.exists():
        with open(USAGE_FILE) as f:
            return json.load(f)
    return {'total_used': 0, 'monthly_used': 0, 'month': datetime.utcnow().strftime('%Y-%m'), 'history': []}

def _save(data):
    USAGE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(USAGE_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def track_query(query: str = ''):
    """Record one Serper query. Returns (remaining, alert_msg or None)."""
    data = _load()
    current_month = datetime.utcnow().strftime('%Y-%m')
    if data.get('month') != current_month:
        data['monthly_used'] = 0
        data['month'] = current_month
    data['total_used'] += 1
    data['monthly_used'] += 1
    data['last_query'] = datetime.utcnow().isoformat()
    _save(data)
    
    remaining = QUOTA_LIMIT - data['total_used']
    alert = None
    if remaining <= 0:
        alert = f'SERPER QUOTA EXHAUSTED! {data["total_used"]}/{QUOTA_LIMIT} used. LinkedIn enrichment will stop.'
        log.error(alert)
    elif remaining <= ALERT_THRESHOLD:
        alert = f'SERPER LOW: {remaining} queries remaining ({data["total_used"]}/{QUOTA_LIMIT} used)'
        log.warning(alert)
    
    return remaining, alert

def get_usage():
    """Get current usage stats."""
    data = _load()
    return {
        'total_used': data.get('total_used', 0),
        'monthly_used': data.get('monthly_used', 0),
        'remaining': QUOTA_LIMIT - data.get('total_used', 0),
        'quota': QUOTA_LIMIT,
        'month': data.get('month'),
        'last_query': data.get('last_query'),
        'alert': data.get('total_used', 0) >= QUOTA_LIMIT - ALERT_THRESHOLD
    }

def has_budget():
    """Check if we have queries remaining."""
    data = _load()
    return data.get('total_used', 0) < QUOTA_LIMIT
