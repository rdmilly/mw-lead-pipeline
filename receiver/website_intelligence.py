#!/usr/bin/env python3
"""Website Intelligence Module v1.0
Extracts business intelligence from websites and public APIs.
All FREE — no paid APIs required.

Layers:
1. Website Quality Score (SSL, mobile, speed indicators)
2. Domain Age via WHOIS
3. Email Provider Detection via MX records
4. Google Business Profile completeness
5. Website Freshness via Wayback Machine
6. Facebook Page signals (if URL found)
7. Review pain point extraction
8. Oregon SOS Registry match
"""
import re
import json
import logging
import socket
from datetime import datetime, timezone
from typing import Dict, Optional, List
from urllib.parse import urlparse

try:
    import dns.resolver
except ImportError:
    dns = None

try:
    import whois as pywhois
except ImportError:
    pywhois = None

import httpx

log = logging.getLogger('web_intel')

# ============================================================
# LAYER 1: Website Quality Score
# ============================================================
def score_website_quality(html: str, url: str, status_code: int = 200, load_time: float = 0) -> Dict:
    """Score website quality 0-100. Low score = needs MW's help."""
    score = 50  # Start neutral
    issues = []
    html_lower = html.lower()

    # SSL check
    if url.startswith('https'):
        score += 10
    else:
        score -= 15
        issues.append('no_ssl')

    # Mobile responsive check
    has_viewport = 'viewport' in html_lower
    has_responsive = any(kw in html_lower for kw in ['@media', 'responsive', 'bootstrap', 'tailwind'])
    if has_viewport:
        score += 10
    else:
        score -= 15
        issues.append('not_mobile_friendly')
    if has_responsive:
        score += 5

    # Modern framework detection
    modern_signals = ['react', 'vue', 'next.js', 'tailwindcss', 'bootstrap-5', 'webpack']
    if any(s in html_lower for s in modern_signals):
        score += 10

    # Outdated signals
    outdated_signals = ['<table', 'bgcolor=', '<font', '<center', '<marquee', 'dreamweaver']
    outdated_count = sum(1 for s in outdated_signals if s in html_lower)
    if outdated_count >= 2:
        score -= 15
        issues.append('outdated_design')
    elif outdated_count == 1:
        score -= 5

    # Page size check
    page_size_kb = len(html) / 1024
    if page_size_kb < 5:
        score -= 10
        issues.append('thin_content')
    elif page_size_kb > 500:
        score -= 5
        issues.append('bloated')

    # Contact info visible
    has_phone_visible = bool(re.search(r'\(\d{3}\)\s*\d{3}[\-.]\d{4}|\d{3}[\-.]\d{3}[\-.]\d{4}', html))
    has_email_visible = bool(re.search(r'mailto:', html_lower))
    if not has_phone_visible and not has_email_visible:
        score -= 10
        issues.append('no_visible_contact')

    # CTA check
    cta_words = ['get a quote', 'free estimate', 'contact us', 'call now', 'book now', 'schedule']
    has_cta = any(cta in html_lower for cta in cta_words)
    if has_cta:
        score += 5
    else:
        issues.append('no_cta')

    # Clamp to 0-100
    score = max(0, min(100, score))

    return {
        'website_quality_score': score,
        'website_issues': issues,
        'has_ssl': url.startswith('https'),
        'is_mobile_friendly': has_viewport,
        'page_size_kb': round(page_size_kb, 1),
    }


# ============================================================
# LAYER 2: Domain Age via WHOIS
# ============================================================
def get_domain_age(domain: str) -> Optional[Dict]:
    """Get domain registration date and age in years."""
    if not pywhois:
        return None
    try:
        w = pywhois.whois(domain)
        creation = w.creation_date
        if isinstance(creation, list):
            creation = creation[0]
        if creation:
            if isinstance(creation, str):
                creation = datetime.strptime(creation[:10], '%Y-%m-%d')
            age_days = (datetime.now() - creation).days
            return {
                'domain_created': creation.strftime('%Y-%m-%d'),
                'domain_age_years': round(age_days / 365.25, 1),
            }
    except Exception:
        pass
    return None


# ============================================================
# LAYER 3: Email Provider Detection via MX
# ============================================================
EMAIL_PROVIDERS = {
    'google': ['google.com', 'googlemail.com', 'smtp.google.com', 'aspmx.l.google.com'],
    'microsoft': ['outlook.com', 'microsoft.com', 'protection.outlook.com'],
    'godaddy': ['secureserver.net', 'mailstore1.secureserver.net'],
    'zoho': ['zoho.com', 'zoho.in'],
    'rackspace': ['emailsrvr.com'],
    'protonmail': ['protonmail.ch'],
    'ionos': ['ionos.com', 'perfora.net', 'kundenserver.de'],
    'namecheap': ['registrar-servers.com', 'privateemail.com'],
    'hostinger': ['hostinger.com', 'titan.email'],
}

def detect_email_provider(domain: str) -> Optional[str]:
    """Detect email provider from MX records."""
    if not dns:
        return None
    try:
        answers = dns.resolver.resolve(domain, 'MX')
        mx_hosts = [str(r.exchange).lower().rstrip('.') for r in answers]
        for provider, patterns in EMAIL_PROVIDERS.items():
            for mx in mx_hosts:
                if any(p in mx for p in patterns):
                    return provider
        return 'other'
    except Exception:
        return None


# ============================================================
# LAYER 4: Google Business Profile Completeness
# ============================================================
def assess_gbp_completeness(lead: Dict) -> Dict:
    """Assess how complete their Google Business Profile is."""
    score = 0
    missing = []

    if lead.get('google_rating'):
        score += 20
    else:
        missing.append('no_rating')

    reviews = lead.get('google_review_count', 0) or 0
    if reviews >= 50:
        score += 30
    elif reviews >= 10:
        score += 20
    elif reviews >= 1:
        score += 10
    else:
        missing.append('no_reviews')

    if lead.get('source_url') and lead['source_url'].startswith('http'):
        score += 20
    else:
        missing.append('no_website')

    if lead.get('primary_phone'):
        score += 15
    else:
        missing.append('no_phone')

    if lead.get('category'):
        score += 15
    else:
        missing.append('no_category')

    return {
        'gbp_completeness': score,
        'gbp_missing': missing,
    }


# ============================================================
# LAYER 5: Website Freshness via Wayback Machine
# ============================================================
async def check_wayback_freshness(client: httpx.AsyncClient, url: str) -> Optional[Dict]:
    """Check last crawl date from Wayback Machine."""
    try:
        domain = urlparse(url).netloc
        r = await client.get(
            f'https://archive.org/wayback/available?url={domain}',
            timeout=8
        )
        if r.status_code == 200:
            data = r.json()
            snapshot = data.get('archived_snapshots', {}).get('closest', {})
            if snapshot.get('timestamp'):
                ts = snapshot['timestamp']
                last_archived = datetime.strptime(ts[:8], '%Y%m%d')
                days_since = (datetime.now() - last_archived).days
                return {
                    'last_archived': last_archived.strftime('%Y-%m-%d'),
                    'days_since_archived': days_since,
                    'website_stale': days_since > 730,  # >2 years = stale
                }
    except Exception:
        pass
    return None


# ============================================================
# LAYER 6: Facebook Page Signals
# ============================================================
async def check_facebook_signals(client: httpx.AsyncClient, fb_url: str) -> Optional[Dict]:
    """Basic Facebook page check — is it active? Has reviews?"""
    if not fb_url:
        return None
    try:
        r = await client.get(fb_url, timeout=8, follow_redirects=True)
        if r.status_code == 200:
            html = r.text
            has_reviews = 'review' in html.lower()
            has_hours = 'hours' in html.lower()
            # Check for recent activity indicators
            recent_post = bool(re.search(r'\d{1,2}[hdm]\s', html))  # "2h", "5d" etc
            return {
                'fb_has_reviews': has_reviews,
                'fb_has_hours': has_hours,
                'fb_recently_active': recent_post,
            }
    except Exception:
        pass
    return None


# ============================================================
# LAYER 7: Review Pain Point Extraction
# ============================================================
PAIN_KEYWORDS = {
    'communication': ['hard to reach', 'never called back', 'no response', "didn't return",
                      'unreachable', 'no communication', "won't answer", 'ghosted'],
    'website': ['website', 'online', 'couldn\'t find', 'no website', 'outdated site',
                'hard to find online', 'no online presence'],
    'scheduling': ['late', 'no-show', 'missed appointment', 'never showed',
                   'scheduling', 'had to wait', 'took forever'],
    'professionalism': ['unprofessional', 'messy', 'rude', 'careless',
                        'wouldn\'t recommend', 'terrible', 'awful'],
    'pricing': ['overpriced', 'hidden fees', 'surprise charge', 'too expensive',
                'bait and switch', 'price gouging'],
}

def extract_pain_points(reviews_text: str) -> Dict:
    """Extract pain point categories from review text."""
    if not reviews_text:
        return {'pain_points': []}
    text_lower = reviews_text.lower()
    found = []
    for category, keywords in PAIN_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            found.append(category)
    return {'pain_points': found}


# ============================================================
# LAYER 8: Oregon SOS Registry Match
# ============================================================
def match_sos_registry(business_name: str, sos_data: Dict = None) -> Optional[Dict]:
    """Match business against Oregon Secretary of State registry.
    sos_data is a pre-loaded dict mapping business names to records.
    If not loaded, returns None."""
    if not sos_data or not business_name:
        return None
    name_upper = business_name.upper().strip()
    # Try exact match first
    if name_upper in sos_data:
        rec = sos_data[name_upper]
        return {
            'sos_entity_type': rec.get('entity_type', ''),
            'sos_status': rec.get('status', ''),
            'sos_reg_date': rec.get('reg_date', ''),
            'sos_state': rec.get('state', 'OR'),
        }
    # Try fuzzy: remove LLC, Inc, etc
    from difflib import SequenceMatcher
    clean_name = re.sub(r'\b(LLC|INC|CORP|CO|LTD|L\.L\.C\.)\b', '', name_upper).strip()
    best_match = None
    best_score = 0
    for key in sos_data:
        clean_key = re.sub(r'\b(LLC|INC|CORP|CO|LTD|L\.L\.C\.)\b', '', key).strip()
        score = SequenceMatcher(None, clean_name, clean_key).ratio()
        if score > best_score and score > 0.85:
            best_score = score
            best_match = key
    if best_match:
        rec = sos_data[best_match]
        return {
            'sos_entity_type': rec.get('entity_type', ''),
            'sos_status': rec.get('status', ''),
            'sos_reg_date': rec.get('reg_date', ''),
            'sos_state': rec.get('state', 'OR'),
            'sos_match_confidence': round(best_score, 2),
        }
    return None


# ============================================================
# MASTER: Run all intelligence layers on a lead
# ============================================================
async def analyze(client: httpx.AsyncClient, html: str, url: str, lead: Dict) -> Dict:
    """Run all intelligence layers and return combined results."""
    results = {}
    domain = urlparse(url).netloc.replace('www.', '') if url else ''

    # Layer 1: Website Quality
    if html:
        quality = score_website_quality(html, url)
        results.update(quality)

    # Layer 2: Domain Age
    if domain:
        age = get_domain_age(domain)
        if age:
            results.update(age)

    # Layer 3: Email Provider
    if domain:
        provider = detect_email_provider(domain)
        if provider:
            results['email_provider'] = provider

    # Layer 4: GBP Completeness
    gbp = assess_gbp_completeness(lead)
    results.update(gbp)

    # Layer 5: Wayback freshness
    if url:
        freshness = await check_wayback_freshness(client, url)
        if freshness:
            results.update(freshness)

    # Layer 6: Facebook signals
    social = lead.get('social_links', {}) or {}
    if isinstance(social, str):
        try:
            social = json.loads(social)
        except Exception:
            social = {}
    fb_url = social.get('facebook', '')
    if fb_url:
        fb = await check_facebook_signals(client, fb_url)
        if fb:
            results.update(fb)

    return results
