"""Lead Scoring Algorithm v2 for MW Development Pipeline.

Scores leads 0-100. Email REQUIRED for warm+ tier.
Includes heuristic business size estimation.

Tiers:
  hot (80-100): Ready for immediate outreach, has email
  warm (50-79): Good prospects with email
  cold (20-49): Missing email or weak signals
  dead (0-19): Not worth pursuing
"""
import db
from datetime import datetime


def estimate_business_size(lead: dict) -> dict:
    """Heuristic estimate of employee count and revenue from available data."""
    signals = []
    years = lead.get('years_in_business') or 0
    reviews = lead.get('google_review_count') or 0
    rating = lead.get('google_rating') or 0
    category = lead.get('category') or ''
    has_multiple_endorsements = ',' in category

    if years > 15:
        signals.append('very_established')
    elif years > 5:
        signals.append('established')
    if reviews > 100:
        signals.append('high_volume')
    elif reviews > 30:
        signals.append('medium_volume')
    if has_multiple_endorsements:
        signals.append('multi_trade')

    # Estimate
    if 'high_volume' in signals and ('very_established' in signals or 'multi_trade' in signals):
        return {'employees': '10-25', 'revenue': '$1M-5M', 'confidence': 0.35, 'signals': signals}
    elif 'high_volume' in signals or ('established' in signals and 'medium_volume' in signals):
        return {'employees': '5-15', 'revenue': '$500K-2M', 'confidence': 0.3, 'signals': signals}
    elif 'established' in signals or 'medium_volume' in signals:
        return {'employees': '2-5', 'revenue': '$150K-500K', 'confidence': 0.25, 'signals': signals}
    else:
        return {'employees': '1-3', 'revenue': '$50K-200K', 'confidence': 0.2, 'signals': signals}


def score_lead(lead: dict) -> dict:
    score = 0
    signals = []
    breakdown = {}
    has_email = bool(lead.get('primary_email'))

    # === CONTACT QUALITY (max 30 pts) ===
    contact = 0
    if has_email:
        contact += 15
        signals.append('has_email')
    elif lead.get('emails'):
        contact += 8
        signals.append('has_email_unverified')
    if lead.get('primary_phone'):
        contact += 10
        signals.append('has_phone')
    if lead.get('owner_name') and lead['owner_name'].strip():
        contact += 5
        signals.append('has_owner_name')
    breakdown['contact'] = contact
    score += contact

    # === BUSINESS SIGNALS (max 25 pts) ===
    business = 0
    rating = lead.get('google_rating')
    reviews = lead.get('google_review_count', 0) or 0
    years = lead.get('years_in_business', 0) or 0

    if rating:
        if rating >= 4.5:
            business += 5
        elif rating >= 4.0:
            business += 3
        elif rating < 3.5:
            business += 7
            signals.append('low_rating_pain')

    if reviews:
        if 10 <= reviews <= 100:
            business += 8
            signals.append('mid_size_business')
        elif reviews > 100:
            business += 4
        elif reviews < 10:
            business += 6
            signals.append('small_business')

    if lead.get('license_number'):
        business += 3
        signals.append('licensed')

    if years:
        if 2 <= years <= 15:
            business += 4
            signals.append('established')
        elif years > 15:
            business += 2
            signals.append('very_established')

    if lead.get('bbb_accredited'):
        business += 3
        signals.append('bbb_accredited')

    breakdown['business'] = business
    score += business

    # === DIGITAL PRESENCE (max 20 pts) ===
    digital = 0
    has_website = bool(lead.get('source_url') or lead.get('final_url'))
    if has_website:
        digital += 5

    social_count = sum(1 for k in ['facebook_url', 'instagram_url', 'linkedin_url', 'twitter_url']
                       if lead.get(k))
    if social_count == 0 and has_website:
        digital += 8
        signals.append('no_social_presence')
    elif social_count == 1:
        digital += 5
        signals.append('minimal_social')
    elif social_count >= 3:
        digital += 2

    # Booking/scheduling detection
    meta_desc = (lead.get('meta_description') or '').lower()
    meta_title = (lead.get('meta_title') or '').lower()
    combined_meta = meta_desc + ' ' + meta_title
    if has_website and not any(w in combined_meta for w in ['book', 'schedul', 'appointment', 'reserv', 'quote online']):
        digital += 5
        signals.append('no_online_booking')

    # Website quality heuristics
    source_url = lead.get('source_url') or ''
    if source_url.startswith('https://'):
        digital += 2
        signals.append('https')

    breakdown['digital'] = digital
    score += digital

    # === CATEGORY FIT (max 15 pts) ===
    category = 0
    cat = (lead.get('category') or '').lower()
    high_value = ['plumb', 'hvac', 'heat', 'cool', 'electric', 'roof',
                  'landscap', 'clean', 'paint', 'remodel', 'handyman',
                  'pest', 'garage', 'fence', 'floor', 'window', 'solar',
                  'concrete', 'drywall', 'insulation', 'siding']
    if any(c in cat for c in high_value):
        category += 15
        signals.append('high_value_category')
    elif cat:
        category += 8

    breakdown['category'] = category
    score += category

    # === ENRICHMENT (max 10 pts) ===
    enrichment = 0
    if lead.get('enrichment_status') == 'complete':
        enrichment += 3
    if lead.get('address'):
        enrichment += 2
        signals.append('has_address')
    if lead.get('google_rating'):
        enrichment += 3
        signals.append('has_google_data')
    if lead.get('yelp_rating'):
        enrichment += 2

    breakdown['enrichment'] = enrichment
    score += enrichment

    # Cap at 100
    score = min(score, 100)

    # === TIER ASSIGNMENT (email required for warm+) ===
    if has_email and score >= 80:
        tier = 'hot'
    elif has_email and score >= 50:
        tier = 'warm'
    elif score >= 20:
        tier = 'cold'
    else:
        tier = 'dead'

    # Business size estimate
    size_est = estimate_business_size(lead)

    return {
        'score': round(score, 1),
        'tier': tier,
        'signals': signals,
        'breakdown': breakdown,
        'size_estimate': size_est
    }


def score_all_leads(limit=2000) -> dict:
    result = db.list_leads(limit=limit, sort_by='created_at', order='ASC')
    scored = 0
    tier_counts = {'hot': 0, 'warm': 0, 'cold': 0, 'dead': 0}

    for lead in result['leads']:
        scoring = score_lead(lead)

        # Build updates
        updates = {
            'lead_score': scoring['score'],
            'quality_tier': scoring['tier'],
        }

        # Store size estimates
        est = scoring.get('size_estimate', {})
        if est.get('employees'):
            updates['revenue_estimate'] = est.get('revenue', '')

        db.update_lead(lead['id'], updates)

        # Update arrays via direct SQL
        with db.get_conn() as conn:
            cur = conn.cursor()
            cur.execute('UPDATE businesses SET pain_signals = %s, scored_at = NOW() WHERE id = %s',
                        (scoring['signals'], lead['id']))

        tier_counts[scoring['tier']] += 1
        scored += 1

    return {'scored': scored, 'tiers': tier_counts, 'total': result['total']}
