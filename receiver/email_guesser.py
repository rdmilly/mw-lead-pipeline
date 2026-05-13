# MillyExt - Email Pattern Guesser
# For leads with domains but no discoverable email, try common patterns
# and verify MX records exist

import json
import asyncio
import logging
import dns.resolver
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from typing import List, Tuple

logger = logging.getLogger("email_guesser")
logging.basicConfig(level=logging.INFO)

DATA_DIR = Path("/data")
MASTER_FILE = DATA_DIR / "leads" / "master_leads.json"
GUESS_LOG = DATA_DIR / "logs" / "email_guess.jsonl"

# Common email patterns for service businesses, ordered by likelihood
PATTERNS = [
    'info@{domain}',
    'contact@{domain}',
    'service@{domain}',
    'office@{domain}',
    'hello@{domain}',
    'sales@{domain}',
    'support@{domain}',
]

# Domains that use catch-all or won't have pattern emails
SKIP_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
    'aol.com', 'icloud.com', 'protonmail.com',
    'weebly.com', 'wix.com', 'squarespace.com', 'wordpress.com',
    'shopify.com', 'godaddy.com', 'google.com', 'yelp.com',
}


def check_mx(domain: str) -> bool:
    """Check if domain has MX records (can receive email)."""
    try:
        answers = dns.resolver.resolve(domain, 'MX')
        return len(list(answers)) > 0
    except Exception:
        return False


def get_lead_domain(lead: dict) -> str:
    """Extract the email domain from a lead's website."""
    website = lead.get('website', '')
    if not website:
        return ''
    parsed = urlparse(website if website.startswith('http') else 'https://' + website)
    host = parsed.netloc.replace('www.', '').strip('/')
    return host


async def run_email_guessing(limit: int = 70):
    """Guess emails for leads that have domains but no email."""
    with open(MASTER_FILE) as f:
        master = json.load(f)

    leads = master.get('leads', {})
    targets = []

    for domain_key, lead in leads.items():
        if lead.get('emails'):
            continue
        if lead.get('_email_guess_at'):
            continue
        lead_domain = get_lead_domain(lead)
        if not lead_domain:
            continue
        # Skip free/hosted domains
        if any(lead_domain.endswith(s) for s in SKIP_DOMAINS):
            continue
        if '.' not in lead_domain:
            continue
        targets.append((domain_key, lead, lead_domain))

    targets = targets[:limit]
    logger.info(f"Email guessing: {len(targets)} leads to process")

    found_count = 0
    mx_valid = 0

    for domain_key, lead, lead_domain in targets:
        has_mx = check_mx(lead_domain)

        if has_mx:
            mx_valid += 1
            # Domain can receive email - add info@ as best guess
            guessed = f"info@{lead_domain}"
            lead['emails'] = [guessed]
            lead['_email_source'] = 'pattern_guess'
            lead['_email_confidence'] = 'medium'
            found_count += 1
            logger.info(f"MX valid for {lead_domain} -> {guessed}")
        else:
            logger.debug(f"No MX for {lead_domain}")

        lead['_email_guess_at'] = datetime.utcnow().isoformat()
        lead['_has_mx'] = has_mx

    # Save
    master['updated_at'] = datetime.utcnow().isoformat()
    with open(MASTER_FILE, 'w') as f:
        json.dump(master, f, indent=2)

    result = {
        'status': 'completed',
        'timestamp': datetime.utcnow().isoformat(),
        'targets': len(targets),
        'mx_valid': mx_valid,
        'emails_guessed': found_count,
        'no_mx': len(targets) - mx_valid,
    }

    GUESS_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(GUESS_LOG, 'a') as f:
        f.write(json.dumps(result) + '\n')

    logger.info(f"Email guessing complete: {json.dumps(result)}")
    return result


if __name__ == '__main__':
    result = asyncio.run(run_email_guessing())
    print(json.dumps(result, indent=2))
