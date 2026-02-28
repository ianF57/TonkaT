"""Shared Yahoo Finance HTTP client.

The v8/finance/chart endpoint returns 200 OK with a browser-like User-Agent
and does NOT require a crumb token or session cookie.  The getcrumb endpoint
(/v1/test/getcrumb) returns 401 when called without a logged-in Yahoo session,
so crumb fetching must not be attempted — it will always fail and spam logs.

Fetch strategy:
  1. Try query2.finance.yahoo.com first (generally faster).
  2. Fall back to query1.finance.yahoo.com on any network/HTTP error.
  3. Retry up to MAX_ATTEMPTS per host with exponential back-off on 429/5xx.
  4. Raise RuntimeError if both hosts fail.
"""
from __future__ import annotations

import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

# ── Browser-like headers required to avoid bot-detection blocks ───────────────
YAHOO_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://finance.yahoo.com/",
}

_CHART_HOSTS = [
    "https://query2.finance.yahoo.com/v8/finance/chart",
    "https://query1.finance.yahoo.com/v8/finance/chart",
]
MAX_ATTEMPTS = 3   # per host


async def yahoo_chart_get(symbol: str, params: dict[str, object]) -> dict:
    """Fetch a Yahoo Finance v8 chart payload for *symbol*.

    No crumb is needed — the chart endpoint works with headers alone.
    Retries with exponential back-off on 429 / transient 5xx errors.
    Falls back to the secondary host if the primary fails entirely.
    """
    last_exc: Exception = RuntimeError(
        f"All Yahoo Finance hosts failed for '{symbol}'"
    )

    async with httpx.AsyncClient(
        headers=YAHOO_HEADERS,
        follow_redirects=True,
        timeout=20.0,
    ) as client:
        for host in _CHART_HOSTS:
            url = f"{host}/{symbol}"

            for attempt in range(MAX_ATTEMPTS):
                try:
                    resp = await client.get(url, params=params)

                    if resp.status_code == 429:
                        wait = 2.0 ** attempt
                        logger.warning(
                            "Yahoo rate-limited for %s — backing off %.1fs", symbol, wait
                        )
                        await asyncio.sleep(wait)
                        continue  # retry same host

                    # 5xx: transient server error, back off and retry
                    if resp.status_code >= 500:
                        wait = 1.5 * (attempt + 1)
                        logger.warning(
                            "Yahoo %s for %s — backing off %.1fs (attempt %d/%d)",
                            resp.status_code, symbol, wait, attempt + 1, MAX_ATTEMPTS,
                        )
                        await asyncio.sleep(wait)
                        continue

                    resp.raise_for_status()
                    logger.debug(
                        "Yahoo chart OK for %s via %s (attempt %d)", symbol, host, attempt + 1
                    )
                    return resp.json()

                except httpx.HTTPStatusError as exc:
                    # 4xx that isn't 429 — no point retrying on same host
                    logger.warning(
                        "Yahoo HTTP %s for %s — trying next host",
                        exc.response.status_code, symbol,
                    )
                    last_exc = exc
                    break  # try next host

                except httpx.RequestError as exc:
                    logger.warning(
                        "Yahoo connection error for %s (attempt %d/%d): %s",
                        symbol, attempt + 1, MAX_ATTEMPTS, exc,
                    )
                    last_exc = exc
                    if attempt < MAX_ATTEMPTS - 1:
                        await asyncio.sleep(1.0 * (attempt + 1))

    raise last_exc
