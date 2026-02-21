"""Universal Market Price Validation API
===========================================
Fetches live prices for ALL dashboard assets via Yahoo Finance:
  - Crypto        (BTC-USD, ETH-USD, …)
  - Stocks        (AAPL, TSLA, NVDA, …)
  - Forex         (EURUSD=X, GBPUSD=X, …)
  - Commodities   (GC=F, SI=F, CL=F, NG=F)
  - Indices       (^GSPC, ^NDX, ^DJI)

Design principles:
  • Live price IS the truth — no comparison vs stale hardcoded reference.
  • In-process 30-second TTL cache avoids hammering upstream on every poll.
  • Sanity range check rejects clearly bad API responses.
  • Anomaly guard: >25% single-poll move → hold cached price & flag ANOMALY.
  • STALE flag when exchange timestamp is >60 min old (market closed / delayed).
  • Parallel fetch via asyncio.gather for all symbols at once.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Literal

import httpx
from fastapi import APIRouter, Depends, HTTPException
from app.api.auth import require_api_key

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/validate", tags=["validation"], dependencies=[Depends(require_api_key)])

# ──────────────────────────────────────────────────────────────────────────────
# Universal asset registry  (dashboard_id → Yahoo Finance symbol)
# ──────────────────────────────────────────────────────────────────────────────
ASSETS: list[dict] = [
    # ── Crypto ──────────────────────────────────────────────────────────────
    {"id": "BTC",    "symbol": "BTC-USD",  "name": "Bitcoin",    "cat": "crypto",      "emoji": "🟡",  "unit": "USD",       "src": "Yahoo Finance / Binance", "smin": 1000,    "smax": 10_000_000},
    {"id": "ETH",    "symbol": "ETH-USD",  "name": "Ethereum",   "cat": "crypto",      "emoji": "💎",  "unit": "USD",       "src": "Yahoo Finance / Binance", "smin": 10,      "smax": 100_000},
    {"id": "SOL",    "symbol": "SOL-USD",  "name": "Solana",     "cat": "crypto",      "emoji": "💜",  "unit": "USD",       "src": "Yahoo Finance / Binance", "smin": 0.1,     "smax": 10_000},
    {"id": "ADA",    "symbol": "ADA-USD",  "name": "Cardano",    "cat": "crypto",      "emoji": "🔷",  "unit": "USD",       "src": "Yahoo Finance / Binance", "smin": 0.001,   "smax": 100},
    {"id": "XRP",    "symbol": "XRP-USD",  "name": "Ripple",     "cat": "crypto",      "emoji": "⚫",  "unit": "USD",       "src": "Yahoo Finance / Binance", "smin": 0.001,   "smax": 10_000},
    {"id": "DOGE",   "symbol": "DOGE-USD", "name": "Dogecoin",   "cat": "crypto",      "emoji": "🐕",  "unit": "USD",       "src": "Yahoo Finance / Binance", "smin": 0.0001,  "smax": 100},
    {"id": "BNB",    "symbol": "BNB-USD",  "name": "BNB",        "cat": "crypto",      "emoji": "🟠",  "unit": "USD",       "src": "Yahoo Finance / Binance", "smin": 1,       "smax": 100_000},
    {"id": "AVAX",   "symbol": "AVAX-USD", "name": "Avalanche",  "cat": "crypto",      "emoji": "🔺",  "unit": "USD",       "src": "Yahoo Finance / Binance", "smin": 0.01,    "smax": 10_000},
    {"id": "MATIC",  "symbol": "POL-USD",  "name": "Polygon",    "cat": "crypto",      "emoji": "🟣",  "unit": "USD",       "src": "Yahoo Finance / Binance", "smin": 0.0001,  "smax": 1_000},
    {"id": "LINK",   "symbol": "LINK-USD", "name": "Chainlink",  "cat": "crypto",      "emoji": "🔵",  "unit": "USD",       "src": "Yahoo Finance / Binance", "smin": 0.01,    "smax": 10_000},
    # ── Stocks ──────────────────────────────────────────────────────────────
    {"id": "AAPL",   "symbol": "AAPL",     "name": "Apple",      "cat": "stocks",      "emoji": "🍎",  "unit": "USD",       "src": "NASDAQ via Yahoo Finance", "smin": 1,       "smax": 10_000},
    {"id": "TSLA",   "symbol": "TSLA",     "name": "Tesla",      "cat": "stocks",      "emoji": "⚡",  "unit": "USD",       "src": "NASDAQ via Yahoo Finance", "smin": 1,       "smax": 10_000},
    {"id": "AMZN",   "symbol": "AMZN",     "name": "Amazon",     "cat": "stocks",      "emoji": "📦",  "unit": "USD",       "src": "NASDAQ via Yahoo Finance", "smin": 1,       "smax": 10_000},
    {"id": "MSFT",   "symbol": "MSFT",     "name": "Microsoft",  "cat": "stocks",      "emoji": "🪟",  "unit": "USD",       "src": "NASDAQ via Yahoo Finance", "smin": 1,       "smax": 10_000},
    {"id": "NVDA",   "symbol": "NVDA",     "name": "NVIDIA",     "cat": "stocks",      "emoji": "💚",  "unit": "USD",       "src": "NASDAQ via Yahoo Finance", "smin": 1,       "smax": 10_000},
    {"id": "META",   "symbol": "META",     "name": "Meta",       "cat": "stocks",      "emoji": "🌐",  "unit": "USD",       "src": "NASDAQ via Yahoo Finance", "smin": 1,       "smax": 10_000},
    {"id": "GOOGL",  "symbol": "GOOGL",    "name": "Alphabet",   "cat": "stocks",      "emoji": "🔍",  "unit": "USD",       "src": "NASDAQ via Yahoo Finance", "smin": 1,       "smax": 10_000},
    {"id": "NFLX",   "symbol": "NFLX",     "name": "Netflix",    "cat": "stocks",      "emoji": "🎬",  "unit": "USD",       "src": "NASDAQ via Yahoo Finance", "smin": 1,       "smax": 10_000},
    # ── Forex ────────────────────────────────────────────────────────────────
    {"id": "EURUSD", "symbol": "EURUSD=X", "name": "EUR/USD",    "cat": "forex",       "emoji": "🇪🇺", "unit": "USD",       "src": "FX via Yahoo Finance",    "smin": 0.5,     "smax": 3.0},
    {"id": "GBPUSD", "symbol": "GBPUSD=X", "name": "GBP/USD",   "cat": "forex",       "emoji": "🇬🇧", "unit": "USD",       "src": "FX via Yahoo Finance",    "smin": 0.5,     "smax": 3.0},
    {"id": "USDJPY", "symbol": "USDJPY=X", "name": "USD/JPY",   "cat": "forex",       "emoji": "🇯🇵", "unit": "JPY",       "src": "FX via Yahoo Finance",    "smin": 50,      "smax": 300},
    {"id": "AUDUSD", "symbol": "AUDUSD=X", "name": "AUD/USD",   "cat": "forex",       "emoji": "🇦🇺", "unit": "USD",       "src": "FX via Yahoo Finance",    "smin": 0.3,     "smax": 2.0},
    # ── Commodities ──────────────────────────────────────────────────────────
    {"id": "GOLD",   "symbol": "GC=F",     "name": "Gold",       "cat": "commodities", "emoji": "🥇",  "unit": "USD/oz",    "src": "COMEX via Yahoo Finance",  "smin": 1000,    "smax": 20_000},
    {"id": "SILVER", "symbol": "SI=F",     "name": "Silver",     "cat": "commodities", "emoji": "🥈",  "unit": "USD/oz",    "src": "COMEX via Yahoo Finance",  "smin": 5,       "smax": 1000},
    {"id": "OIL",    "symbol": "CL=F",     "name": "Crude Oil",  "cat": "commodities", "emoji": "🛢️",  "unit": "USD/bbl",   "src": "NYMEX via Yahoo Finance",  "smin": 5,       "smax": 500},
    {"id": "NATGAS", "symbol": "NG=F",     "name": "Nat. Gas",   "cat": "commodities", "emoji": "🔥",  "unit": "USD/MMBtu", "src": "NYMEX via Yahoo Finance",  "smin": 0.5,     "smax": 100},
    # ── Indices ──────────────────────────────────────────────────────────────
    {"id": "SPX",    "symbol": "^GSPC",    "name": "S&P 500",    "cat": "indices",     "emoji": "📈",  "unit": "pts",       "src": "CBOE via Yahoo Finance",   "smin": 500,     "smax": 20_000},
    {"id": "NDX",    "symbol": "^NDX",     "name": "NASDAQ 100", "cat": "indices",     "emoji": "💻",  "unit": "pts",       "src": "NASDAQ via Yahoo Finance", "smin": 500,     "smax": 100_000},
    {"id": "DJI",    "symbol": "^DJI",     "name": "Dow Jones",  "cat": "indices",     "emoji": "🏛️",  "unit": "pts",       "src": "DJIA via Yahoo Finance",   "smin": 1000,    "smax": 200_000},
]

ASSETS_BY_ID: dict[str, dict] = {a["id"]: a for a in ASSETS}

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
CACHE_TTL       = 30        # seconds between full upstream refreshes
STALE_SECONDS   = 3600      # exchange ts >60 min → STALE
ANOMALY_PCT     = 25.0      # single-poll move > 25% → ANOMALY (hold cache)

ValidationStatus = Literal["LIVE", "STALE", "ANOMALY", "ERROR"]

_YAHOO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# In-process price cache
# { asset_id: {"price": float, "exchange_ts": str|None, "exchange_raw": int|None, "fetched_at": float} }
_cache: dict[str, dict] = {}
_cache_refreshed_at: float = 0.0


# ──────────────────────────────────────────────────────────────────────────────
# Yahoo Finance fetch
# ──────────────────────────────────────────────────────────────────────────────
async def _yahoo_spot(symbol: str, client: httpx.AsyncClient) -> dict | None:
    """Return {price, exchange_ts, exchange_raw} or None on failure."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"interval": "1m", "range": "1d", "includePrePost": "false"}
    try:
        r = await client.get(url, params=params, timeout=14.0)
        r.raise_for_status()
        data   = r.json()
        result = data.get("chart", {}).get("result")
        if not result:
            logger.warning("No chart result for %s", symbol)
            return None
        meta  = result[0].get("meta", {})
        price = meta.get("regularMarketPrice") or meta.get("previousClose")
        ts    = meta.get("regularMarketTime")
        if price is None:
            logger.warning("No price in meta for %s", symbol)
            return None
        return {
            "price":        float(price),
            "exchange_ts":  datetime.fromtimestamp(ts, tz=UTC).isoformat() if ts else None,
            "exchange_raw": int(ts) if ts else None,
        }
    except httpx.HTTPStatusError as e:
        logger.warning("HTTP %s for %s", e.response.status_code, symbol)
    except Exception as e:
        logger.warning("Error fetching %s: %s", symbol, e)
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Classification
# ──────────────────────────────────────────────────────────────────────────────
def _classify(asset: dict, spot: dict | None, now: float) -> dict:
    aid    = asset["id"]
    cached = _cache.get(aid)

    def _record(price, exchange_ts, status: ValidationStatus, alert=None, note=None):
        return {
            "id":          aid,
            "symbol":      asset["symbol"],
            "name":        asset["name"],
            "cat":         asset["cat"],
            "emoji":       asset["emoji"],
            "unit":        asset["unit"],
            "source":      note or asset["src"],
            "live_price":  round(price, 6) if price is not None else None,
            "exchange_ts": exchange_ts,
            "status":      status,
            "alert":       alert,
            "validated_at": datetime.now(UTC).isoformat(),
        }

    if spot is None:
        if cached and (now - cached["fetched_at"]) < 600:
            return _record(
                cached["price"], cached["exchange_ts"], "STALE",
                f"⚠️ Fetch failed for {asset['name']} — showing cached value",
                "Cached (upstream error)",
            )
        return _record(
            None, None, "ERROR",
            f"🚨 Cannot retrieve live price for {asset['name']}",
        )

    p = spot["price"]

    # Sanity range
    if not (asset["smin"] <= p <= asset["smax"]):
        logger.warning("Sanity fail %s: %.6f (range %.4f–%.4f)", aid, p, asset["smin"], asset["smax"])
        if cached:
            return _record(
                cached["price"], cached["exchange_ts"], "ANOMALY",
                f"⚠️ Received implausible price ${p:,.4f} for {asset['name']} — reverting to cached",
                "Cached (sanity check failed)",
            )
        return _record(None, None, "ERROR", f"🚨 Implausible price for {asset['name']}: {p}")

    # Anomaly detection (vs cached)
    if cached and cached["price"]:
        pct = abs((p - cached["price"]) / cached["price"]) * 100
        if pct > ANOMALY_PCT:
            logger.warning("Anomaly %s: %.1f%% move %.4f→%.4f", aid, pct, cached["price"], p)
            return _record(
                cached["price"], cached["exchange_ts"], "ANOMALY",
                f"⚠️ {asset['name']} moved {pct:.0f}% in one poll (${cached['price']:,.4f}→${p:,.4f}) — holding cache pending confirmation",
                "Cached (anomaly detected)",
            )

    # Staleness
    status: ValidationStatus = "LIVE"
    alert = None
    if spot["exchange_raw"] and (now - spot["exchange_raw"]) > STALE_SECONDS:
        age = int((now - spot["exchange_raw"]) / 60)
        status = "STALE"
        alert  = f"⏰ {asset['name']} data is {age} min old (market closed or feed delayed)"

    # Update cache
    _cache[aid] = {
        "price":        p,
        "exchange_ts":  spot["exchange_ts"],
        "exchange_raw": spot["exchange_raw"],
        "fetched_at":   now,
    }

    return _record(p, spot["exchange_ts"], status, alert)


# ──────────────────────────────────────────────────────────────────────────────
# Refresh all
# ──────────────────────────────────────────────────────────────────────────────
async def _refresh_all() -> list[dict]:
    global _cache_refreshed_at
    now = time.time()

    async with httpx.AsyncClient(headers=_YAHOO_HEADERS) as client:
        spots = await asyncio.gather(*[_yahoo_spot(a["symbol"], client) for a in ASSETS])

    results = [_classify(a, s, now) for a, s in zip(ASSETS, spots)]
    _cache_refreshed_at = now
    return results


def _from_cache_records(now: float) -> list[dict]:
    records = []
    for a in ASSETS:
        c = _cache.get(a["id"])
        if c:
            records.append({
                "id":          a["id"],
                "symbol":      a["symbol"],
                "name":        a["name"],
                "cat":         a["cat"],
                "emoji":       a["emoji"],
                "unit":        a["unit"],
                "source":      a["src"],
                "live_price":  round(c["price"], 6),
                "exchange_ts": c["exchange_ts"],
                "status":      "LIVE",
                "alert":       None,
                "validated_at": datetime.now(UTC).isoformat(),
            })
        else:
            records.append({
                "id": a["id"], "symbol": a["symbol"], "name": a["name"],
                "cat": a["cat"], "emoji": a["emoji"], "unit": a["unit"],
                "source": a["src"], "live_price": None,
                "exchange_ts": None, "status": "ERROR",
                "alert": f"No cached price for {a['name']}",
                "validated_at": datetime.now(UTC).isoformat(),
            })
    return records


def _build_response(records: list[dict], *, from_cache: bool) -> dict:
    by_cat: dict[str, list] = {}
    for r in records:
        by_cat.setdefault(r["cat"], []).append(r)

    statuses = [r["status"] for r in records]
    errors   = [r["id"] for r in records if r["status"] == "ERROR"]
    anomalies = [r["id"] for r in records if r["status"] == "ANOMALY"]

    if "ERROR" in statuses:
        feed = "DEGRADED"
        msg  = f"Feed errors for: {', '.join(errors[:5])}" + ("…" if len(errors) > 5 else "")
    elif "ANOMALY" in statuses:
        feed = "ANOMALY_DETECTED"
        msg  = f"Anomalous move detected for: {', '.join(anomalies)}"
    elif "STALE" in statuses:
        feed = "STALE"
        msg  = "Some prices are from a delayed or closed exchange"
    else:
        feed = "HEALTHY"
        msg  = f"All {len(records)} asset prices are live and validated ✓"

    return {
        "feed_status":   feed,
        "feed_message":  msg,
        "from_cache":    from_cache,
        "cache_ttl_s":   CACHE_TTL,
        "stale_s":       STALE_SECONDS,
        "anomaly_pct":   ANOMALY_PCT,
        "served_at":     datetime.now(UTC).isoformat(),
        "assets":        records,           # flat list, all categories
        "by_category":   by_cat,            # grouped for category views
        "summary": {
            "total":    len(records),
            "live":     statuses.count("LIVE"),
            "stale":    statuses.count("STALE"),
            "anomaly":  statuses.count("ANOMALY"),
            "error":    statuses.count("ERROR"),
            "categories": list(by_cat.keys()),
        },
    }


# ──────────────────────────────────────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/all")
async def validate_all() -> dict:
    """
    Validate ALL dashboard assets (crypto, stocks, forex, commodities, indices).

    Prices are fetched live from Yahoo Finance, sanity-checked, and
    anomaly-detected before being served. Results are cached for 30 s.
    """
    global _cache_refreshed_at
    if _cache and (time.time() - _cache_refreshed_at) < CACHE_TTL:
        return _build_response(_from_cache_records(time.time()), from_cache=True)
    records = await _refresh_all()
    return _build_response(records, from_cache=False)


@router.get("/category/{cat}")
async def validate_category(cat: str) -> dict:
    """Validate a single category: crypto, stocks, forex, commodities, indices."""
    valid_cats = {a["cat"] for a in ASSETS}
    if cat not in valid_cats:
        raise HTTPException(404, {"error": f"Unknown category '{cat}'", "valid": sorted(valid_cats)})

    subset = [a for a in ASSETS if a["cat"] == cat]
    now    = time.time()
    async with httpx.AsyncClient(headers=_YAHOO_HEADERS) as client:
        spots = await asyncio.gather(*[_yahoo_spot(a["symbol"], client) for a in subset])
    records = [_classify(a, s, now) for a, s in zip(subset, spots)]
    return _build_response(records, from_cache=False)


@router.get("/asset/{asset_id}")
async def validate_asset(asset_id: str) -> dict:
    """Validate a single asset by dashboard ID (BTC, AAPL, GOLD, EURUSD, SPX…)."""
    asset = ASSETS_BY_ID.get(asset_id.upper())
    if not asset:
        raise HTTPException(404, {"error": f"Unknown asset '{asset_id}'", "valid": list(ASSETS_BY_ID)})
    now = time.time()
    async with httpx.AsyncClient(headers=_YAHOO_HEADERS) as client:
        spot = await _yahoo_spot(asset["symbol"], client)
    return _classify(asset, spot, now)


# ── Backwards-compat alias for /api/validate/commodities (used by old JS) ────
@router.get("/commodities")
async def validate_commodities_compat() -> dict:
    """Backwards-compatible endpoint — now returns all assets."""
    return await validate_all()
