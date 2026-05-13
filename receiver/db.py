"""PostgreSQL database layer for MillyExt Lead Pipeline.

Replaces master_leads.json with PostgreSQL backed by helix-postgres.
Connection: helix-postgres:5432/leads
"""

import os
import json
import asyncio
from datetime import datetime
from contextlib import contextmanager
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

# Connection config
DB_HOST = os.environ.get("DB_HOST", "helix-postgres")
DB_PORT = int(os.environ.get("DB_PORT", 5432))
DB_NAME = os.environ.get("DB_NAME", "leads")
DB_USER = os.environ.get("DB_USER", "helix")
DB_PASS = os.environ.get("DB_PASS", "934d69eb7ce6a90710643e93efe36fcc")

# Connection pool
_pool = None

def get_pool():
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
    return _pool


@contextmanager
def get_conn():
    pool = get_pool_safe()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def normalize_domain(url):
    """Extract base domain for dedup matching."""
    try:
        parsed = urlparse(url if '://' in str(url) else f'https://{url}')
        domain = parsed.netloc.lower().replace('www.', '')
        return domain
    except:
        return str(url).lower()


# ===== CRUD OPERATIONS =====

def upsert_lead(lead: dict) -> tuple:
    """Insert or update a lead. Returns (business_id, is_new)."""
    domain = normalize_domain(lead.get('website') or lead.get('source_url', ''))
    if not domain:
        return None, False

    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Check if exists
        cur.execute("SELECT id FROM businesses WHERE domain = %s", (domain,))
        existing = cur.fetchone()
        is_new = existing is None

        phones = lead.get("phones", []) or []
        emails = lead.get("emails", []) or []
        social = lead.get("social_links", {}) or {}
        owner = lead.get("owner_info", {}) or {}
        meta = lead.get("meta", {}) or {}

        if is_new:
            cur.execute("""
                INSERT INTO businesses (
                    domain, business_name, source_url, final_url, place_id,
                    phones, emails, primary_phone, primary_email,
                    owner_name, owner_source,
                    meta_title, meta_description,
                    facebook_url, instagram_url, linkedin_url, twitter_url, youtube_url,
                    subpages_scraped, scraped_at, batch_id, source
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                ) RETURNING id
            """, (
                domain,
                lead.get('business_name', ''),
                lead.get('source_url', ''),
                lead.get('final_url', ''),
                lead.get('place_id', ''),
                phones, emails,
                phones[0] if phones else None,
                emails[0] if emails else None,
                owner.get('name', ''),
                owner.get('source', ''),
                meta.get('title', ''),
                meta.get('description', ''),
                social.get('facebook', ''),
                social.get('instagram', ''),
                social.get('linkedin', ''),
                social.get('twitter', ''),
                social.get('youtube', ''),
                lead.get('subpages_scraped', []),
                lead.get('scraped_at', datetime.utcnow().isoformat()),
                lead.get('batch_id', ''),
                lead.get('source', 'scraper')
            ))
            row = cur.fetchone()
            return row['id'], True
        else:
            # Merge: update fields that are empty, extend arrays
            bid = existing['id']
            cur.execute("SELECT * FROM businesses WHERE id = %s", (bid,))
            current = cur.fetchone()

            # Merge phones and emails
            merged_phones = list(set((current.get('phones') or []) + phones))
            merged_emails = list(set((current.get('emails') or []) + emails))
            merged_subpages = list(set((current.get('subpages_scraped') or []) + lead.get('subpages_scraped', []) or []))

            cur.execute("""
                UPDATE businesses SET
                    business_name = COALESCE(NULLIF(business_name, ''), %s),
                    phones = %s,
                    emails = %s,
                    primary_phone = COALESCE(primary_phone, %s),
                    primary_email = COALESCE(primary_email, %s),
                    owner_name = COALESCE(NULLIF(owner_name, ''), %s),
                    owner_source = COALESCE(NULLIF(owner_source, ''), %s),
                    meta_title = COALESCE(NULLIF(meta_title, ''), %s),
                    meta_description = COALESCE(NULLIF(meta_description, ''), %s),
                    facebook_url = COALESCE(NULLIF(facebook_url, ''), %s),
                    instagram_url = COALESCE(NULLIF(instagram_url, ''), %s),
                    linkedin_url = COALESCE(NULLIF(linkedin_url, ''), %s),
                    twitter_url = COALESCE(NULLIF(twitter_url, ''), %s),
                    youtube_url = COALESCE(NULLIF(youtube_url, ''), %s),
                    subpages_scraped = %s,
                    updated_at = NOW()
                WHERE id = %s
            """, (
                lead.get('business_name', ''),
                merged_phones, merged_emails,
                merged_phones[0] if merged_phones else None,
                merged_emails[0] if merged_emails else None,
                owner.get('name', ''),
                owner.get('source', ''),
                meta.get('title', ''),
                meta.get('description', ''),
                social.get('facebook', ''),
                social.get('instagram', ''),
                social.get('linkedin', ''),
                social.get('twitter', ''),
                social.get('youtube', ''),
                merged_subpages,
                bid
            ))
            return bid, False


def get_lead_by_domain(domain: str) -> dict:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM businesses WHERE domain = %s", (domain,))
        row = cur.fetchone()
        return dict(row) if row else None


def get_lead_by_id(lead_id: int) -> dict:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM businesses WHERE id = %s", (lead_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def list_leads(limit=100, offset=0, status=None, tier=None, min_score=None, sort_by='created_at', order='DESC') -> dict:
    """List leads with filtering and pagination."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        conditions = []
        params = []
        
        if status:
            conditions.append("pipeline_status = %s")
            params.append(status)
        if tier:
            conditions.append("quality_tier = %s")
            params.append(tier)
        if min_score is not None:
            conditions.append("lead_score >= %s")
            params.append(min_score)
        
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        # Whitelist sort columns
        allowed_sorts = {'created_at', 'updated_at', 'lead_score', 'business_name', 'domain', 'scraped_at'}
        sort_col = sort_by if sort_by in allowed_sorts else 'created_at'
        sort_order = 'ASC' if order.upper() == 'ASC' else 'DESC'
        
        # Count
        cur.execute(f"SELECT COUNT(*) as total FROM businesses {where}", params)
        total = cur.fetchone()['total']
        
        # Fetch
        cur.execute(f"SELECT * FROM businesses {where} ORDER BY {sort_col} {sort_order} LIMIT %s OFFSET %s",
                    params + [limit, offset])
        leads = [dict(r) for r in cur.fetchall()]
        
        return {"leads": leads, "total": total, "limit": limit, "offset": offset}


def get_stats() -> dict:
    """Pipeline statistics."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT 
                COUNT(*) as total_leads,
                COUNT(*) FILTER (WHERE primary_phone IS NOT NULL) as with_phone,
                COUNT(*) FILTER (WHERE primary_email IS NOT NULL) as with_email,
                COUNT(*) FILTER (WHERE enrichment_status = 'complete') as enriched,
                COUNT(*) FILTER (WHERE lead_score > 0) as scored,
                COUNT(*) FILTER (WHERE exported_to_instantly) as exported,
                AVG(lead_score) FILTER (WHERE lead_score > 0) as avg_score,
                COUNT(DISTINCT category) as categories,
                COUNT(DISTINCT pipeline_status) as statuses
            FROM businesses
        """)
        stats = dict(cur.fetchone())
        
        # Pipeline breakdown
        cur.execute("SELECT pipeline_status, COUNT(*) as count FROM businesses GROUP BY pipeline_status ORDER BY count DESC")
        stats['pipeline_breakdown'] = {r['pipeline_status']: r['count'] for r in cur.fetchall()}
        
        # Tier breakdown
        cur.execute("SELECT quality_tier, COUNT(*) as count FROM businesses GROUP BY quality_tier ORDER BY count DESC")
        stats['tier_breakdown'] = {r['quality_tier']: r['count'] for r in cur.fetchall()}
        
        return stats


def update_lead(lead_id: int, updates: dict) -> dict:
    """Update specific fields on a lead."""
    allowed = {
        'business_name', 'category', 'primary_phone', 'primary_email', 'source_url', 'final_url', 'website_url', 'place_id', 'linkedin_url',
        'address', 'city', 'state', 'zip_code',
        'owner_name', 'pipeline_status', 'enrichment_status',
        'lead_score', 'quality_tier', 'notes',
        'google_rating', 'google_review_count', 'yelp_rating', 'yelp_review_count',
        'bbb_accredited', 'bbb_rating', 'employee_count', 'years_in_business',
        'revenue_estimate', 'license_number', 'license_status',
        'exported_to_instantly', 'instantly_campaign_id', 'instantly_lead_id',
        'social_links', 'tech_stack', 'has_booking_system', 'estimated_employees', 'email_source', 'emails', 'domain',
        'website_quality_score', 'website_issues', 'is_mobile_friendly', 'has_ssl',
        'domain_age_years', 'domain_created', 'email_provider',
        'gbp_completeness', 'gbp_missing', 'website_stale', 'days_since_archived',
        'pain_points', 'sos_entity_type', 'sos_reg_date'
    }
    
    fields = {k: v for k, v in updates.items() if k in allowed}
    if not fields:
        return None
    
    # Serialize JSON fields - JSONB columns need Json(), TEXT[] columns need plain lists
    import psycopg2.extras as pge
    # JSONB columns: wrap dicts in Json()
    for k in ('social_links',):
        if k in fields and isinstance(fields[k], dict):
            fields[k] = pge.Json(fields[k])
    # TEXT[] columns: psycopg2 handles Python lists natively, no wrapping needed
    # emails, phones, website_issues, gbp_missing, pain_points are all TEXT[]
    fields['updated_at'] = datetime.utcnow()
    
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        set_clause = ", ".join(f"{k} = %s" for k in fields.keys())
        values = list(fields.values()) + [lead_id]
        cur.execute(f"UPDATE businesses SET {set_clause} WHERE id = %s RETURNING *", values)
        row = cur.fetchone()
        return dict(row) if row else None


def delete_lead(lead_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM businesses WHERE id = %s", (lead_id,))
        return cur.rowcount > 0


def log_enrichment(business_id: int, source: str, status: str = 'success', data: dict = None, error: str = None):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO enrichment_log (business_id, source, status, data, error)
            VALUES (%s, %s, %s, %s, %s)
        """, (business_id, source, status, json.dumps(data) if data else None, error))


def log_event(business_id: int, event_type: str, details: dict = None):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_events (business_id, event_type, details)
            VALUES (%s, %s, %s)
        """, (business_id, event_type, json.dumps(details) if details else None))


def export_leads_csv(min_score=0, status=None, limit=1000) -> list:
    """Export leads for Instantly or CSV download."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        conditions = ["lead_score >= %s"]
        params = [min_score]
        if status:
            conditions.append("pipeline_status = %s")
            params.append(status)
        where = " AND ".join(conditions)
        cur.execute(f"""
            SELECT domain, business_name, primary_email, primary_phone,
                   owner_name, category, address, city, state, zip_code,
                   google_rating, google_review_count, lead_score, quality_tier,
                   website_url, source_url, facebook_url, linkedin_url
            FROM businesses
            WHERE {where} AND exported_to_instantly = false
            ORDER BY lead_score DESC
            LIMIT %s
        """, params + [limit])
        return [dict(r) for r in cur.fetchall()]


def mark_exported(lead_ids: list, campaign_id: str = None):
    """Mark leads as exported to Instantly."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE businesses SET
                exported_to_instantly = true,
                instantly_campaign_id = %s,
                exported_at = NOW(),
                pipeline_status = 'exported',
                updated_at = NOW()
            WHERE id = ANY(%s)
        """, (campaign_id, lead_ids))
        return cur.rowcount


# ===== COMPATIBILITY LAYER =====
# These functions match the old JSON load_master/save_master interface
# so existing code can be migrated incrementally

def load_master_compat() -> dict:
    """Return data in the old master_leads.json format."""
    result = list_leads(limit=10000)
    leads_dict = {}
    for lead in result['leads']:
        domain = lead['domain']
        leads_dict[domain] = _row_to_legacy(lead)
    return {"leads": leads_dict, "updated_at": datetime.utcnow().isoformat()}


def _row_to_legacy(row: dict) -> dict:
    """Convert a DB row to the old JSON lead format."""
    return {
        "business_name": row.get('business_name', ''),
        "source_url": row.get("source_url", ""),
        "website": row.get("source_url", "") or row.get("final_url", ""),
        "final_url": row.get('final_url', ''),
        "place_id": row.get('place_id', ''),
        "phones": row.get('phones', []),
        "emails": row.get('emails', []),
        "owner_info": {"name": row.get('owner_name', ''), "source": row.get('owner_source', '')},
        "meta": {"title": row.get('meta_title', ''), "description": row.get('meta_description', '')},
        "social_links": {
            "facebook": row.get('facebook_url', ''),
            "instagram": row.get('instagram_url', ''),
            "linkedin": row.get('linkedin_url', ''),
            "twitter": row.get('twitter_url', ''),
            "youtube": row.get('youtube_url', ''),
        },
        "subpages_scraped": row.get('subpages_scraped', []),
        "scraped_at": row.get('scraped_at', ''),
        "enrichment_status": row.get('enrichment_status', 'pending'),
        "lead_score": row.get('lead_score', 0),
        "quality_tier": row.get('quality_tier', 'unscored'),
        "pipeline_status": row.get('pipeline_status', 'new'),
        "_db_id": row.get('id')
    }


def init_db():
    """Test connection on startup."""
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            print("[DB] PostgreSQL connection OK")
            return True
    except Exception as e:
        print(f"[DB] PostgreSQL connection FAILED: {e}")
        return False


def get_pool_safe():
    """Get connection pool with automatic reconnect on failure."""
    global _pool
    try:
        if _pool:
            conn = _pool.getconn()
            conn.cursor().execute('SELECT 1')
            _pool.putconn(conn)
            return _pool
    except Exception:
        print('[DB] Connection lost, reconnecting...')
        _pool = None
    return get_pool()
