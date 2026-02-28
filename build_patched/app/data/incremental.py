"""Incremental candle updater.

Algorithm overview
──────────────────

                   ┌──────────────────────────────────────────────┐
                   │  IncrementalEngine.run(symbol, tf)           │
                   └───────────────────┬──────────────────────────┘
                                       │
                    ┌──────────────────▼───────────────────────────┐
                    │  1. Guard: backfill COMPLETE?                │
                    │     No  → raise (run backfill first)        │
                    │     Yes → continue                          │
                    └──────────────────┬───────────────────────────┘
                                       │
                    ┌──────────────────▼───────────────────────────┐
                    │  2. MAX(open_time) anchor                    │
                    │     fetch_from = latest_epoch + tf_secs      │
                    │     If fetch_from ≥ now - tf_secs → done     │
                    └──────────────────┬───────────────────────────┘
                                       │
                    ┌──────────────────▼───────────────────────────┐
                    │  3. Paginate FORWARD until now               │
                    │     Binance: startTime= cursor               │
                    │     Yahoo:   fetch recent range, filter old  │
                    │     Per window:                              │
                    │       a. Fetch raw candles                   │
                    │       b. Drop partial candle                 │
                    │       c. Validate OHLCV                      │
                    │       d. INSERT OR IGNORE                    │
                    │       e. Advance cursor to last_open + tf    │
                    │       f. Break when < full page returned     │
                    └──────────────────┬───────────────────────────┘
                                       │
                    ┌──────────────────▼───────────────────────────┐
                    │  4. Gap detection (post-update)             │
                    │     LAG window-function scan for             │
                    │     consecutive open_time deltas > tf_secs  │
                    └──────────────────┬───────────────────────────┘
                                       │
                    ┌──────────────────▼───────────────────────────┐
                    │  5. Gap fill (Binance only, auto for small)  │
                    │     Re-fetch each missing window              │
                    │     INSERT OR IGNORE (safe if exchange gap)  │
                    └──────────────────┬───────────────────────────┘
                                       │
                    ┌──────────────────▼───────────────────────────┐
                    │  6. Write IncrementalUpdateLog row           │
                    └──────────────────────────────────────────────┘

Idempotency guarantees
──────────────────────
• MAX(open_time) anchor ensures we never re-request candles we already have.
• INSERT OR IGNORE on the composite PK (symbol, timeframe, open_time) makes
  every store operation safe to repeat without creating duplicates.
• Gap-fill requests overlap with the existing data on both edges; the PK
  constraint silently absorbs the overlap rows.
• Running the engine when already up-to-date is a zero-API-call no-op.

Partial candle guard
────────────────────
A candle whose period has not yet closed must never be stored — the OHLCV
values are still changing.  We reject any candle where:
    open_time + tf_secs > now_epoch + TOLERANCE_SECS (60 s)
This is identical to the guard used in the backfill engine.

Gap detection algorithm
───────────────────────
Uses SQLite window functions (available since 3.25 / 2018-09-15) to compute
the LAG of open_time and flag gaps where the delta exceeds tf_secs:

    WITH ordered AS (
        SELECT open_time,
               LAG(open_time) OVER (ORDER BY open_time) AS prev_time
        FROM candle
        WHERE symbol = :sym AND timeframe = :tf
    )
    SELECT prev_time AS gap_start,
           open_time AS gap_end,
           (open_time - prev_time) / :tf_secs - 1 AS missing_candles
    FROM ordered
    WHERE open_time - prev_time > :tf_secs;

For large datasets this runs against the clustered WITHOUT ROWID B-tree and
completes in a single sequential leaf walk — no separate index is needed.

Rate limits & throttling
────────────────────────
Binance: same constants as backfill (0.25 s inter-request, 4 retries).
Yahoo:   single re-fetch of the recent range; no pagination needed.

Performance characteristics (large datasets)
────────────────────────────────────────────
After a full historical backfill the candle table may contain 50 000–200 000
rows per (symbol, timeframe) pair.  The incremental update is designed to be
a lightweight O(new_candles) operation:

• MAX(open_time) uses the WITHOUT ROWID clustered B-tree rightmost-leaf read
  — effectively O(1) for SQLite regardless of total row count.
• Gap detection is a single ordered scan scoped to one (symbol, tf) partition;
  because the PK B-tree is clustered by (symbol, tf, open_time) this is a
  sequential leaf walk with no random I/O.
• INSERT OR IGNORE on the PK is a point-lookup per row; cheap even for large
  existing datasets because the index is already warm from the MAX read.
• fetched_at is updated on newly inserted rows only — existing rows are not
  touched, preserving cache-freshness semantics in data_manager.py.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import TypedDict

import httpx
from sqlalchemy import desc, func, select, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.data.backfill import (
    BINANCE_INTERVAL,
    TF_SECONDS,
    YAHOO_INTERVAL,
    YAHOO_RANGE,
    BackfillTarget,
    DEFAULT_TARGETS,
    _BINANCE_CANDLES_PER_REQUEST,
    _BINANCE_INTER_REQUEST_SLEEP,
    _INSERT_BATCH_SIZE,
    _MAX_RETRIES,
    _RETRY_BASE_SLEEP,
    _YAHOO_INTER_SYMBOL_SLEEP,
    _from_epoch,
    _is_partial,
    _now_epoch,
    _valid_ohlcv,
)
from app.data.database import get_db_session
from app.data.models import BackfillProgress, BackfillStatus, Candle, IncrementalUpdateLog

logger = logging.getLogger(__name__)

# ── Gap-fill policy ───────────────────────────────────────────────────────────

# Gaps larger than this many candles are logged but NOT auto-filled
# (likely an exchange outage or delisted period — don't hammer the API).
_MAX_AUTO_FILL_CANDLES = 500


# ── Result types ──────────────────────────────────────────────────────────────

class GapRecord(TypedDict):
    gap_start:       int    # open_time of candle BEFORE the gap (epoch)
    gap_end:         int    # open_time of candle AFTER  the gap (epoch)
    missing_candles: int    # number of expected but absent bars
    gap_start_iso:   str
    gap_end_iso:     str


@dataclass
class UpdateResult:
    symbol:          str
    timeframe:       str
    status:          str          # "up_to_date" | "updated" | "error" | "no_backfill"
    candles_fetched: int  = 0
    candles_inserted:int  = 0
    windows_fetched: int  = 0
    gaps_found:      int  = 0
    gaps_filled:     int  = 0
    candles_gap_filled: int = 0
    duration_secs:   float = 0.0
    error:           str | None = None
    latest_open_time: int | None = None


# ── Anchor query ──────────────────────────────────────────────────────────────

def _get_latest_stored_epoch(symbol: str, timeframe: str) -> int | None:
    """Return MAX(open_time) for (symbol, timeframe) or None if no data exists.

    Uses the WITHOUT ROWID clustered B-tree rightmost-leaf scan — essentially
    O(1) regardless of partition size.

    Example equivalent SQL:
        SELECT MAX(open_time) FROM candle
        WHERE symbol = 'BTCUSDT' AND timeframe = '1h';
    """
    with get_db_session() as session:
        result = session.execute(
            select(func.max(Candle.open_time))
            .where(Candle.symbol    == symbol)
            .where(Candle.timeframe == timeframe)
        ).scalar_one_or_none()
    return result  # None when no rows exist


def _get_backfill_status(symbol: str, timeframe: str) -> str | None:
    """Return the BackfillProgress.status for this pair, or None if no record."""
    with get_db_session() as session:
        row = session.execute(
            select(BackfillProgress.status)
            .where(BackfillProgress.symbol    == symbol)
            .where(BackfillProgress.timeframe == timeframe)
        ).scalar_one_or_none()
    return row


# ── Gap detection ─────────────────────────────────────────────────────────────

def detect_gaps(symbol: str, timeframe: str) -> list[GapRecord]:
    """Scan the (symbol, timeframe) partition for missing candles.

    Algorithm: use the SQLite LAG window function to compare each open_time
    to its predecessor.  Any delta > tf_secs indicates a gap.

    The query runs against the clustered B-tree (sequential scan of the
    (symbol, tf) leaf pages) — no extra index needed.

    Example SQL for BTCUSDT/1h (tf_secs = 3600):

        WITH ordered AS (
            SELECT open_time,
                   LAG(open_time) OVER (ORDER BY open_time) AS prev_time
            FROM candle
            WHERE symbol = 'BTCUSDT' AND timeframe = '1h'
        )
        SELECT
            prev_time                               AS gap_start,
            open_time                               AS gap_end,
            (open_time - prev_time) / 3600 - 1     AS missing_candles
        FROM ordered
        WHERE open_time - prev_time > 3600;
    """
    tf_secs = TF_SECONDS.get(timeframe, 3600)

    sql = text("""
        WITH ordered AS (
            SELECT open_time,
                   LAG(open_time) OVER (ORDER BY open_time) AS prev_time
            FROM candle
            WHERE symbol    = :sym
              AND timeframe = :tf
        )
        SELECT
            prev_time                                   AS gap_start,
            open_time                                   AS gap_end,
            (open_time - prev_time) / :tf_secs - 1     AS missing_candles
        FROM ordered
        WHERE prev_time IS NOT NULL
          AND open_time - prev_time > :tf_secs
        ORDER BY gap_start ASC;
    """)

    with get_db_session() as session:
        rows = session.execute(
            sql,
            {"sym": symbol, "tf": timeframe, "tf_secs": tf_secs},
        ).fetchall()

    gaps: list[GapRecord] = []
    for row in rows:
        gap_start, gap_end, missing = row
        # SQL WHERE already guarantees missing >= 1; no further filter needed
        gaps.append(
            GapRecord(
                gap_start       = gap_start,
                gap_end         = gap_end,
                missing_candles = missing,
                gap_start_iso   = _from_epoch(gap_start),
                gap_end_iso     = _from_epoch(gap_end),
            )
        )

    return gaps


# ── Candle store (re-use backfill's implementation) ───────────────────────────

def _store_batch(symbol: str, timeframe: str, rows: list[dict]) -> int:
    """INSERT OR IGNORE a validated list of candle dicts.  Returns rows inserted."""
    if not rows:
        return 0
    inserted = 0
    with get_db_session() as session:
        for i in range(0, len(rows), _INSERT_BATCH_SIZE):
            batch  = rows[i : i + _INSERT_BATCH_SIZE]
            stmt   = sqlite_insert(Candle).values(batch).on_conflict_do_nothing()
            result = session.execute(stmt)
            inserted += result.rowcount
    return inserted


# ── Binance forward fetch ─────────────────────────────────────────────────────

async def _binance_fetch_forward(
    symbol:   str,
    tf:       str,
    start_ms: int,
    end_ms:   int | None = None,
    limit:    int = _BINANCE_CANDLES_PER_REQUEST,
) -> list[list]:
    """Fetch Binance klines starting from *start_ms* (epoch milliseconds).

    Uses the ``startTime`` parameter (forward direction), which is the correct
    approach for incremental updates.  The backfill engine uses ``endTime``
    (backward direction) — these are complementary and never interfere.

    Example HTTP request:
        GET https://api.binance.com/api/v3/klines
            ?symbol=BTCUSDT
            &interval=1h
            &startTime=1700000000000
            &limit=1000
    """
    url    = "https://api.binance.com/api/v3/klines"
    params: dict[str, object] = {
        "symbol":    symbol,
        "interval":  BINANCE_INTERVAL[tf],
        "startTime": start_ms,
        "limit":     limit,
    }
    if end_ms is not None:
        params["endTime"] = end_ms

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }

    for attempt in range(_MAX_RETRIES):
        try:
            async with httpx.AsyncClient(
                headers=headers,
                follow_redirects=True,
                timeout=20.0,
            ) as client:
                resp = await client.get(url, params=params)

            if resp.status_code == 429:
                wait = _RETRY_BASE_SLEEP * (2 ** attempt)
                logger.warning(
                    "[Incremental/Binance] 429 rate-limit %s/%s — sleeping %.1fs (attempt %d/%d)",
                    symbol, tf, wait, attempt + 1, _MAX_RETRIES,
                )
                await asyncio.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = _RETRY_BASE_SLEEP * (2 ** attempt)
                logger.warning(
                    "[Incremental/Binance] %s server error %s/%s — sleeping %.1fs",
                    resp.status_code, symbol, tf, wait,
                )
                await asyncio.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except httpx.HTTPStatusError as exc:
            # Unexpected 4xx (not 429) — no point retrying the same request
            logger.warning(
                "[Incremental/Binance] HTTP %s for %s/%s — not retrying",
                exc.response.status_code, symbol, tf,
            )
            raise

        except httpx.RequestError as exc:
            wait = _RETRY_BASE_SLEEP * (2 ** attempt)
            logger.warning(
                "[Incremental/Binance] Network error %s/%s (attempt %d/%d): %s — sleeping %.1fs",
                symbol, tf, attempt + 1, _MAX_RETRIES, exc, wait,
            )
            if attempt < _MAX_RETRIES - 1:
                await asyncio.sleep(wait)

    raise RuntimeError(
        f"[Incremental/Binance] Failed to fetch {symbol}/{tf} after {_MAX_RETRIES} retries"
    )


def _parse_binance_klines(
    raw:     list[list],
    symbol:  str,
    tf:      str,
    now_ep:  int,
) -> tuple[list[dict], int | None]:
    """Parse and validate raw Binance kline data.

    Returns (valid_rows, latest_open_time_in_window).
    Drops partial candles and invalid OHLCV rows.
    """
    valid_rows:   list[dict] = []
    latest_epoch: int | None = None

    for kline in raw:
        try:
            open_time_ms = int(kline[0])
            open_time_ep = open_time_ms // 1000
            o, h, l, c, v = (
                float(kline[1]), float(kline[2]),
                float(kline[3]), float(kline[4]),
                float(kline[5]),
            )
        except (IndexError, ValueError, TypeError):
            continue

        if _is_partial(open_time_ep, tf, now_ep):
            logger.debug(
                "[Incremental] Discarding partial candle %s/%s @ %s",
                symbol, tf, _from_epoch(open_time_ep),
            )
            continue

        if not _valid_ohlcv(o, h, l, c, v):
            continue

        valid_rows.append({
            "symbol":    symbol,
            "timeframe": tf,
            "open_time": open_time_ep,
            "open":      o,
            "high":      h,
            "low":       l,
            "close":     c,
            "volume":    v,
            "fetched_at": now_ep,
        })

        if latest_epoch is None or open_time_ep > latest_epoch:
            latest_epoch = open_time_ep

    return valid_rows, latest_epoch


# ── Binance incremental update ────────────────────────────────────────────────

async def incremental_update_binance(symbol: str, tf: str) -> UpdateResult:
    """Fetch all candles newer than MAX(open_time) for a Binance symbol.

    Pseudocode:
        latest = MAX(open_time) WHERE symbol=:s AND timeframe=:tf
        if latest is None:
            raise NeedsBackfillError
        cursor_ms = (latest + tf_secs) * 1000
        while cursor_ms / 1000 < now_epoch - tf_secs:
            raw = binance_klines(startTime=cursor_ms, limit=1000)
            if not raw: break
            valid = filter(not partial, valid_ohlcv, raw)
            insert_or_ignore(valid)
            if len(raw) < 1000: break   # last page
            cursor_ms = (last_open_time + tf_secs) * 1000
            sleep(0.25)
    """
    start_time = time.monotonic()
    result = UpdateResult(symbol=symbol, timeframe=tf, status="updated")

    if tf not in BINANCE_INTERVAL:
        result.status = "error"
        result.error  = f"Unsupported Binance timeframe '{tf}'"
        return result

    # 1. Anchor
    latest_epoch = _get_latest_stored_epoch(symbol, tf)
    if latest_epoch is None:
        result.status = "no_backfill"
        result.error  = (
            f"No historical data for {symbol}/{tf}. "
            "Run a full backfill before using the incremental updater."
        )
        return result

    tf_secs = TF_SECONDS[tf]
    now_ep  = _now_epoch()

    # 2. Check if already up-to-date
    next_expected_epoch = latest_epoch + tf_secs
    if next_expected_epoch >= now_ep - tf_secs:
        result.status           = "up_to_date"
        result.latest_open_time = latest_epoch
        result.duration_secs    = time.monotonic() - start_time
        logger.info(
            "[Incremental/Binance] %s/%s already up-to-date (latest: %s)",
            symbol, tf, _from_epoch(latest_epoch),
        )
        return result

    logger.info(
        "[Incremental/Binance] %s/%s updating from %s → now",
        symbol, tf, _from_epoch(next_expected_epoch),
    )

    # 3. Paginate forward
    cursor_ms        = next_expected_epoch * 1000
    total_fetched    = 0
    total_inserted   = 0
    windows          = 0
    latest_seen: int | None = None

    while cursor_ms // 1000 < now_ep:
        try:
            raw = await _binance_fetch_forward(symbol, tf, start_ms=cursor_ms)
        except Exception as exc:
            logger.error(
                "[Incremental/Binance] Fetch error %s/%s: %s", symbol, tf, exc
            )
            result.status = "error"
            result.error  = str(exc)
            break

        if not raw:
            logger.debug("[Incremental/Binance] %s/%s empty response — done", symbol, tf)
            break

        total_fetched += len(raw)
        valid_rows, window_latest = _parse_binance_klines(raw, symbol, tf, now_ep)
        inserted = _store_batch(symbol, tf, valid_rows)
        total_inserted += inserted
        windows        += 1

        if window_latest is not None:
            latest_seen = window_latest

        logger.info(
            "[Incremental/Binance] %s/%s window #%d: %d raw → %d inserted",
            symbol, tf, windows, len(raw), inserted,
        )

        # Last page reached — no more new data
        if len(raw) < _BINANCE_CANDLES_PER_REQUEST:
            break

        # Advance cursor to candle after last received
        last_open_ep = int(raw[-1][0]) // 1000
        cursor_ms    = (last_open_ep + tf_secs) * 1000

        await asyncio.sleep(_BINANCE_INTER_REQUEST_SLEEP)

    result.candles_fetched  = total_fetched
    result.candles_inserted = total_inserted
    result.windows_fetched  = windows
    result.latest_open_time = latest_seen or latest_epoch
    result.duration_secs    = time.monotonic() - start_time
    # If the loop ran but nothing was new (race condition / overlap), reflect that
    if result.status == "updated" and total_inserted == 0 and total_fetched > 0:
        result.status = "up_to_date"
    return result


# ── Yahoo incremental update ──────────────────────────────────────────────────

async def incremental_update_yahoo(
    symbol:       str,
    tf:           str,
    yahoo_symbol: str,
) -> UpdateResult:
    """Fetch recent candles for a Yahoo Finance symbol and store new ones only.

    Yahoo Finance does not support a ``startTime`` parameter for arbitrary
    historical ranges; it uses fixed range strings ("7d", "60d", "10y", …).
    Strategy: re-fetch the most recent range, then INSERT OR IGNORE — new
    candles are stored, existing ones are silently skipped.

    For incremental efficiency we clip the candidate rows at latest+tf_secs
    before inserting, so we never attempt to INSERT existing rows (saving the
    PK lookup cost for large datasets).

    Pseudocode:
        latest = MAX(open_time) WHERE symbol=:s AND timeframe=:tf
        payload = yahoo_chart_get(yahoo_symbol, interval=tf, range=recent_range)
        for candle in payload:
            if candle.open_time <= latest: skip   # already stored
            if is_partial(candle): skip
            if not valid_ohlcv(candle): skip
            candidate_rows.append(candle)
        insert_or_ignore(candidate_rows)
    """
    from app.data.yahoo_client import yahoo_chart_get

    start_time = time.monotonic()
    result = UpdateResult(symbol=symbol, timeframe=tf, status="updated")

    if tf not in YAHOO_INTERVAL:
        result.status = "error"
        result.error  = f"Unsupported Yahoo timeframe '{tf}'"
        return result

    latest_epoch = _get_latest_stored_epoch(symbol, tf)
    if latest_epoch is None:
        result.status = "no_backfill"
        result.error  = (
            f"No historical data for {symbol}/{tf}. "
            "Run a full backfill before using the incremental updater."
        )
        return result

    tf_secs = TF_SECONDS[tf]
    now_ep  = _now_epoch()

    next_expected = latest_epoch + tf_secs
    if next_expected >= now_ep - tf_secs:
        result.status           = "up_to_date"
        result.latest_open_time = latest_epoch
        result.duration_secs    = time.monotonic() - start_time
        logger.info(
            "[Incremental/Yahoo] %s/%s already up-to-date (latest: %s)",
            symbol, tf, _from_epoch(latest_epoch),
        )
        return result

    logger.info(
        "[Incremental/Yahoo] %s/%s updating from %s → now",
        symbol, tf, _from_epoch(next_expected),
    )

    # Use a shorter range for incremental (we don't need full history)
    _INCREMENTAL_RANGE: dict[str, str] = {
        "1m": "7d",
        "5m": "30d",
        "1h": "60d",
        "1d": "1y",
        "1w": "1y",
    }

    params = {
        "interval":       YAHOO_INTERVAL[tf],
        "range":          _INCREMENTAL_RANGE.get(tf, YAHOO_RANGE[tf]),
        "includePrePost": "false",
        "events":         "div,splits",
    }

    last_exc: Exception | None = None
    payload = None
    for attempt in range(_MAX_RETRIES):
        try:
            payload = await yahoo_chart_get(yahoo_symbol, params)
            break
        except Exception as exc:
            wait = _RETRY_BASE_SLEEP * (2 ** attempt)
            logger.warning(
                "[Incremental/Yahoo] Fetch failed %s/%s (attempt %d/%d): %s — sleeping %.1fs",
                symbol, tf, attempt + 1, _MAX_RETRIES, exc, wait,
            )
            last_exc = exc
            if attempt < _MAX_RETRIES - 1:
                await asyncio.sleep(wait)

    if payload is None:
        result.status = "error"
        result.error  = str(last_exc)
        result.duration_secs = time.monotonic() - start_time
        return result

    chart_result = payload.get("chart", {}).get("result")
    if not chart_result:
        result.status = "error"
        result.error  = (
            (payload.get("chart", {}).get("error") or {}).get("description", "")
            or "No data returned from Yahoo"
        )
        result.duration_secs = time.monotonic() - start_time
        return result

    chart      = chart_result[0]
    timestamps = chart.get("timestamp") or []
    quote      = chart.get("indicators", {}).get("quote", [{}])[0]
    opens      = quote.get("open")   or []
    highs      = quote.get("high")   or []
    lows       = quote.get("low")    or []
    closes     = quote.get("close")  or []
    volumes    = quote.get("volume") or []

    valid_rows:   list[dict] = []
    latest_seen:  int | None = None
    total_fetched = len(timestamps)

    for idx, ts in enumerate(timestamps):
        open_time_ep = int(ts)

        # Skip candles we already have (the core incremental optimization)
        if open_time_ep <= latest_epoch:
            continue

        if _is_partial(open_time_ep, tf, now_ep):
            continue

        try:
            o = float(opens[idx])   if idx < len(opens)   else None
            h = float(highs[idx])   if idx < len(highs)   else None
            l = float(lows[idx])    if idx < len(lows)    else None
            c = float(closes[idx])  if idx < len(closes)  else None
            v = float(volumes[idx]) if idx < len(volumes) and volumes[idx] is not None else 0.0
        except (TypeError, ValueError):
            continue

        if None in (o, h, l, c):
            continue
        if not _valid_ohlcv(o, h, l, c, v):
            continue

        valid_rows.append({
            "symbol":    symbol,
            "timeframe": tf,
            "open_time": open_time_ep,
            "open":      o,
            "high":      h,
            "low":       l,
            "close":     c,
            "volume":    v,
            "fetched_at": now_ep,
        })

        if latest_seen is None or open_time_ep > latest_seen:
            latest_seen = open_time_ep

    inserted = _store_batch(symbol, tf, valid_rows)

    result.candles_fetched  = total_fetched
    result.candles_inserted = inserted
    result.windows_fetched  = 1
    result.latest_open_time = latest_seen or latest_epoch
    result.duration_secs    = time.monotonic() - start_time

    logger.info(
        "[Incremental/Yahoo] %s/%s: %d fetched → %d new inserted",
        symbol, tf, total_fetched, inserted,
    )
    return result


# ── Gap fill ──────────────────────────────────────────────────────────────────

async def fill_gaps_binance(
    symbol: str,
    tf:     str,
    gaps:   list[GapRecord],
) -> tuple[int, int]:
    """Attempt to fill detected gaps by re-fetching the missing windows.

    Only fills gaps ≤ _MAX_AUTO_FILL_CANDLES.  Larger gaps are logged and
    skipped (they likely represent exchange outages or delistings — fabricating
    data would corrupt backtests).

    INSERT OR IGNORE ensures this is safe to call even if some candles in the
    gap range already exist (e.g. from a parallel update).

    Returns (gaps_filled_count, candles_inserted_total).
    """
    tf_secs       = TF_SECONDS[tf]
    gaps_filled   = 0
    total_inserted = 0
    now_ep        = _now_epoch()

    for gap in gaps:
        missing = gap["missing_candles"]

        if missing > _MAX_AUTO_FILL_CANDLES:
            logger.warning(
                "[GapFill] %s/%s: gap %s → %s has %d missing candles — too large, skipping",
                symbol, tf,
                gap["gap_start_iso"], gap["gap_end_iso"],
                missing,
            )
            continue

        # Fetch the window covering the gap:
        # start_ms = candle after gap_start
        # end_ms   = candle before gap_end
        fill_start_ms = (gap["gap_start"] + tf_secs) * 1000
        fill_end_ms   = (gap["gap_end"]   - 1) * 1000  # exclusive: Binance endTime is inclusive

        logger.info(
            "[GapFill] %s/%s: filling gap %s → %s (%d missing candles)",
            symbol, tf,
            gap["gap_start_iso"], gap["gap_end_iso"],
            missing,
        )

        try:
            raw = await _binance_fetch_forward(
                symbol,
                tf,
                start_ms=fill_start_ms,
                end_ms=fill_end_ms,
                limit=min(missing + 1, _BINANCE_CANDLES_PER_REQUEST),
            )
        except Exception as exc:
            logger.error(
                "[GapFill] %s/%s: fetch error for gap %s: %s",
                symbol, tf, gap["gap_start_iso"], exc,
            )
            continue

        if not raw:
            logger.info(
                "[GapFill] %s/%s: no data returned for gap %s — likely exchange outage",
                symbol, tf, gap["gap_start_iso"],
            )
            gaps_filled += 1  # counted as "handled" even if no data
            continue

        valid_rows, _ = _parse_binance_klines(raw, symbol, tf, now_ep)
        inserted = _store_batch(symbol, tf, valid_rows)
        total_inserted += inserted
        gaps_filled    += 1

        logger.info(
            "[GapFill] %s/%s: gap %s — %d raw → %d inserted",
            symbol, tf, gap["gap_start_iso"], len(raw), inserted,
        )

        await asyncio.sleep(_BINANCE_INTER_REQUEST_SLEEP)

    return gaps_filled, total_inserted


# ── Logging helper ────────────────────────────────────────────────────────────

def _write_update_log(result: UpdateResult) -> None:
    """Persist an IncrementalUpdateLog row for this update run."""
    now_ep = _now_epoch()
    with get_db_session() as session:
        row = IncrementalUpdateLog(
            symbol             = result.symbol,
            timeframe          = result.timeframe,
            run_at             = now_ep,
            status             = result.status,
            candles_fetched    = result.candles_fetched,
            candles_inserted   = result.candles_inserted,
            gaps_found         = result.gaps_found,
            gaps_filled        = result.gaps_filled,
            candles_gap_filled = result.candles_gap_filled,
            duration_ms        = int(result.duration_secs * 1000),
            error              = result.error,
        )
        session.add(row)


# ── Orchestrator ──────────────────────────────────────────────────────────────

async def run_incremental_update(
    targets:    list[BackfillTarget] | None = None,
    *,
    fill_gaps:  bool = True,
    require_complete_backfill: bool = True,
) -> dict[str, dict]:
    """Run incremental updates across all targets.

    Args:
        targets:
            List of BackfillTarget descriptors (same format as backfill).
            Defaults to DEFAULT_TARGETS.
        fill_gaps:
            If True, attempt to auto-fill gaps ≤ _MAX_AUTO_FILL_CANDLES after
            the forward update completes.
        require_complete_backfill:
            If True (default), skip targets whose backfill status is not
            COMPLETE or PARTIAL.  Set to False in testing to allow updates
            on partially-filled datasets.

    Returns:
        Mapping of "SYMBOL/TF" → summary dict.
    """
    if targets is None:
        targets = DEFAULT_TARGETS

    results: dict[str, dict] = {}

    for target in targets:
        key = f"{target.symbol}/{target.timeframe}"

        # Guard: require a completed backfill
        if require_complete_backfill:
            bf_status = _get_backfill_status(target.symbol, target.timeframe)
            if bf_status not in (BackfillStatus.COMPLETE, BackfillStatus.PARTIAL):
                logger.info(
                    "[Incremental] Skipping %s — backfill status=%s (not complete)",
                    key, bf_status,
                )
                results[key] = {
                    "status": "skipped_no_backfill",
                    "backfill_status": bf_status,
                }
                continue

        try:
            # Forward update
            if target.market == "crypto":
                result = await incremental_update_binance(target.symbol, target.timeframe)
            else:
                await asyncio.sleep(_YAHOO_INTER_SYMBOL_SLEEP)
                result = await incremental_update_yahoo(
                    target.symbol,
                    target.timeframe,
                    target.yahoo_symbol,
                )

            # Gap detection & fill (Binance only for auto-fill)
            if fill_gaps and result.status in ("updated", "up_to_date"):
                gaps = detect_gaps(target.symbol, target.timeframe)
                result.gaps_found = len(gaps)

                if gaps and target.market == "crypto":
                    gaps_filled, gap_candles = await fill_gaps_binance(
                        target.symbol, target.timeframe, gaps
                    )
                    result.gaps_filled        = gaps_filled
                    result.candles_gap_filled = gap_candles
                elif gaps:
                    logger.info(
                        "[Incremental] %s: %d gaps detected (Yahoo — no auto-fill)",
                        key, len(gaps),
                    )

            _write_update_log(result)

            results[key] = {
                "status":             result.status,
                "candles_fetched":    result.candles_fetched,
                "candles_inserted":   result.candles_inserted,
                "gaps_found":         result.gaps_found,
                "gaps_filled":        result.gaps_filled,
                "candles_gap_filled": result.candles_gap_filled,
                "duration_secs":      round(result.duration_secs, 3),
                "latest_open_time":   _from_epoch(result.latest_open_time)
                                      if result.latest_open_time else None,
                "error":              result.error,
            }

            logger.info(
                "[Incremental] %s complete: %s | +%d candles | %d gaps",
                key, result.status, result.candles_inserted, result.gaps_found,
            )

        except Exception as exc:
            logger.exception("[Incremental] Fatal error for %s: %s", key, exc)
            err_result = UpdateResult(symbol=target.symbol, timeframe=target.timeframe,
                                      status="error", error=str(exc))
            try:
                _write_update_log(err_result)
            except Exception:
                logger.warning("[Incremental] Could not write error log for %s", key)
            results[key] = {"status": "error", "error": str(exc)}

    return results


async def get_incremental_log(
    symbol:    str | None = None,
    timeframe: str | None = None,
    limit:     int = 50,
) -> list[dict]:
    """Return recent IncrementalUpdateLog entries, newest first."""
    with get_db_session() as session:
        q = select(IncrementalUpdateLog)
        if symbol:
            q = q.where(IncrementalUpdateLog.symbol == symbol)
        if timeframe:
            q = q.where(IncrementalUpdateLog.timeframe == timeframe)
        q = q.order_by(desc(IncrementalUpdateLog.run_at)).limit(limit)
        rows = session.execute(q).scalars().all()

        return [
            {
                "id":                r.id,
                "symbol":            r.symbol,
                "timeframe":         r.timeframe,
                "run_at":            _from_epoch(r.run_at),
                "status":            r.status,
                "candles_fetched":   r.candles_fetched,
                "candles_inserted":  r.candles_inserted,
                "gaps_found":        r.gaps_found,
                "gaps_filled":       r.gaps_filled,
                "candles_gap_filled":r.candles_gap_filled,
                "duration_ms":       r.duration_ms,
                "error":             r.error,
            }
            for r in rows
        ]
