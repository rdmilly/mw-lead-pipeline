"""Camoufox Browser Fallback
Used when httpx gets 403/429/empty responses.
Launches a stealth browser to render JavaScript and bypass WAF.
"""
import asyncio
import logging
import os
from typing import Dict, Optional

log = logging.getLogger('browser_fallback')

PROXY = os.environ.get('RESIDENTIAL_PROXY', '')


async def scrape_with_browser(url: str, timeout: int = 20) -> Optional[str]:
    """Render a page using Camoufox and return the HTML."""
    try:
        from camoufox.async_api import AsyncCamoufox

        proxy_config = None
        if PROXY:
            proxy_config = {'server': PROXY}

        async with AsyncCamoufox(
            headless=True,
            proxy=proxy_config,
            geoip=bool(PROXY),
        ) as browser:
            page = await browser.new_page()
            try:
                await page.goto(url, timeout=timeout * 1000, wait_until='domcontentloaded')
                # Wait a bit for dynamic content
                await asyncio.sleep(2)
                html = await page.content()
                return html
            except Exception as e:
                log.warning(f'Browser navigation error for {url}: {e}')
                return None
            finally:
                await page.close()
    except ImportError:
        log.error('Camoufox not installed')
        return None
    except Exception as e:
        log.error(f'Browser launch error: {e}')
        return None


async def scrape_with_fallback(url: str, httpx_status: int = 0, httpx_html: str = '') -> Dict:
    """Only call Camoufox if httpx failed (403/429/empty)."""
    should_fallback = (
        httpx_status in (403, 429, 503, 520, 521, 522, 523, 524) or
        (httpx_status == 200 and len(httpx_html) < 500) or
        httpx_status == 0
    )

    if not should_fallback:
        return {'used_browser': False, 'html': httpx_html}

    log.info(f'Fallback to Camoufox for {url} (httpx status={httpx_status})')
    html = await scrape_with_browser(url)

    if html and len(html) > 500:
        log.info(f'Camoufox success for {url}: {len(html)} chars')
        return {'used_browser': True, 'html': html}
    else:
        log.warning(f'Camoufox also failed for {url}')
        return {'used_browser': True, 'html': ''}
