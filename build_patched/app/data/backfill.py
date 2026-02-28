"""Historical backfill engine.

Algorithm overview
──────────────────
                     ┌─────────────────────────────────────┐
                     │  BackfillEngine.run(symbol, tf)     │
                     └──────────────┬──────────────────────┘
                                    │
                   ┌────────────────▼─────────────────────┐
                   │  1. Load / create BackfillProgress   │
                   │     If status == COMPLETE → skip      │
                   │     If status == RUNNING  → resume    │
                   └────────────────┬─────────────────────┘
                                    │
                   ┌────────────────▼─────────────────────┐
                   │  2. Determine fetch range             │
                   │     window_end = earliest stored      │
                   │                 OR now()              │
                   │     window_start = target_launch_date │
                   └────────────────┬─────────────────────┘
                                    │
              ┌─────────────────────▼──────────────────────────┐
              │  3. Paginate backward (Binance) or one-shot    │
              │     (Yahoo) until window_end <= target_start   │
              │                                                 │
              │  Per window:                                    │
              │    a. Fetch raw candles from provider           │
              │    b. Discard partial candle (open_time+tf>now) │
              │    c. Filter OHLCV validity                     │
              │    d. Batch INSERT OR IGNORE into candle table  │
              │    e. Update BackfillProgress.earliest_open_time│
              │    f. Throttle (sleep between requests)         │
              │    g. On error: retry ≤3× with backoff,        │
              │                 then record & continue          │
              └──────────────────────────────────────────────  ┘
                                    │
                   ┌────────────────▼─────────────────────┐
                   │  4. Mark status COMPLETE / PARTIAL    │
                   │     Write completed_at epoch          │
                   └──────────────────────────────────────┘

Idempotency
───────────
• INSERT OR IGNORE prevents duplicate rows.
• BackfillProgress.earliest_open_time is updated per window so a restart
  skips all already-fetched windows (it picks up from the last checkpoint).
• Running the engine a second time on a COMPLETE symbol is a no-op.

Rate limiting
─────────────
Binance: max 1200 weight/min; klines weight = 2 → max 600 req/min.
  We use _BINANCE_INTER_REQUEST_SLEEP = 0.25s → 4 req/s (safe headroom).
  On 429: exponential back-off, sleep doubles up to 60s, max 5 retries.

Yahoo:   informal limit; single request per symbol/timeframe (no pagination).
  We use _YAHOO_INTER_SYMBOL_SLEEP = 2.0s between symbols.
  On 429: same back-off as Binance.

Partial candle detection
────────────────────────
A candle is partial (in-progress) when:
    open_time + timeframe_seconds > utc_now_epoch
This is always the last candle in any ascending window. We drop it before
storing so backtests never see an incomplete bar.

Edge cases handled
──────────────────
• Symbol launched after our default lookback → capped at launch date.
• Empty response (no new data before the earliest known point) → done.
• Gap in data (exchange outage) → INSERT OR IGNORE stores surrounding rows,
  gap remains empty (correct — don't fabricate candles).
• Clock skew: we compare open_time + tf_secs > now_epoch with a 60s
  tolerance to avoid discarding the just-completed bar.
• Overlapping windows: Binance returns endTime-inclusive, so we subtract
  one tf_step when computing the next window_end to avoid re-fetching the
  boundary candle (handled by INSERT OR IGNORE even if we don't).
"""
from __future__ import annotations

import asyncio
import calendar
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import AsyncIterator, Callable, Protocol

import httpx
from sqlalchemy import select, func, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.data.database import get_db_session
from app.data.models import Candle, BackfillProgress, BackfillStatus

# BackfillDisplay is imported only for type annotations to avoid circular imports.
# progress_display.py does not import from backfill.py.
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from app.data.progress_display import BackfillDisplay

logger = logging.getLogger(__name__)

# ── Timeframe metadata ────────────────────────────────────────────────────────

TF_SECONDS: dict[str, int] = {
    "1m":  60,
    "5m":  300,
    "15m": 900,
    "1h":  3600,
    "4h":  14400,
    "1d":  86400,
    "1w":  604800,
}

# Binance interval strings (API parameter)
BINANCE_INTERVAL: dict[str, str] = {
    "1m": "1m", "5m": "5m", "15m": "15m",
    "1h": "1h", "4h": "4h", "1d": "1d", "1w": "1w",
}

# Yahoo Finance interval / range mapping (single-shot, no pagination)
YAHOO_INTERVAL: dict[str, str] = {
    "1m": "1m", "5m": "5m", "1h": "60m", "1d": "1d", "1w": "1wk",
}
YAHOO_RANGE: dict[str, str] = {
    "1m": "7d", "5m": "60d", "1h": "730d", "1d": "10y", "1w": "10y",
}

# Symbol launch dates (oldest open_time we should try to fetch)
# Binance listing dates; Yahoo symbols are effectively "as far back as available"
BINANCE_LAUNCH_EPOCHS: dict[str, int] = {
    "BTCUSDT": 1502928000,  # 2017-08-17
    "ETHUSDT": 1502928000,  # 2017-08-17
    "BNBUSDT": 1510012800,  # 2017-11-07
    "SOLUSDT": 1597104000,  # 2020-08-11
    "XRPUSDT": 1525478400,  # 2018-05-05
    "ADAUSDT": 1524009600,  # 2018-04-18
}
# Default lookback for Yahoo symbols (10 years back from now)
YAHOO_DEFAULT_LOOKBACK_DAYS = 3650

# ── Throttling constants ──────────────────────────────────────────────────────

_BINANCE_INTER_REQUEST_SLEEP = 0.25   # 4 req/s  (Binance allows ~10/s but we stay conservative)
_YAHOO_INTER_SYMBOL_SLEEP    = 2.0    # 0.5 req/s per symbol (Yahoo informal limit)
_MAX_RETRIES                 = 4      # per window
_RETRY_BASE_SLEEP            = 2.0    # seconds; doubles each retry (2, 4, 8, 16)
_BINANCE_CANDLES_PER_REQUEST = 1000   # Binance API max
_INSERT_BATCH_SIZE           = 100    # SQLite variable limit safety

# ── Progress callback type ────────────────────────────────────────────────────
# Called after each successful window fetch with:
#   (cursor_epoch, windows_fetched, total_inserted)
ProgressCallback = Callable[[int, int, int], None]

# ── Epoch helpers (identical to data_manager.py) ──────────────────────────────

def _now_epoch() -> int:
    return calendar.timegm(datetime.now(timezone.utc).timetuple())


def _to_epoch(dt: datetime) -> int:
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    else:
        dt = dt.replace(tzinfo=timezone.utc)
    return calendar.timegm(dt.timetuple())


def _from_epoch(ep: int) -> str:
    return datetime.fromtimestamp(ep, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


# ── Partial-candle guard ───────────────────────────────────────────────────────

def _is_partial(open_time_epoch: int, tf: str, now_epoch: int, tolerance_secs: int = 60) -> bool:
    """Return True if this candle's period has not yet closed.

    We allow tolerance_secs of slack so a just-completed bar (which closed
    a few seconds ago) is retained rather than discarded.
    """
    tf_secs = TF_SECONDS.get(tf, 3600)
    close_time = open_time_epoch + tf_secs
    return close_time > (now_epoch + tolerance_secs)


# ── Row validation ────────────────────────────────────────────────────────────

def _valid_ohlcv(o: float, h: float, l: float, c: float, v: float) -> bool:
    """Reject rows with invalid price relationships or non-finite values."""
    import math
    if not all(math.isfinite(x) for x in (o, h, l, c, v)):
        return False
    if o <= 0 or h <= 0 or l <= 0 or c <= 0:
        return False
    if h < max(o, c) or l > min(o, c):
        return False
    return True


# ── Database helpers ──────────────────────────────────────────────────────────

def _get_or_create_progress(
    symbol: str,
    timeframe: str,
    target_start: int,
) -> BackfillProgress:
    """Return existing BackfillProgress row or create a new PENDING one."""
    with get_db_session() as session:
        row = session.execute(
            select(BackfillProgress)
            .where(BackfillProgress.symbol    == symbol)
            .where(BackfillProgress.timeframe == timeframe)
        ).scalar_one_or_none()

        if row is None:
            row = BackfillProgress(
                symbol=symbol,
                timeframe=timeframe,
                status=BackfillStatus.PENDING,
                target_start_epoch=target_start,
                total_inserted=0,
                windows_fetched=0,
                error_count=0,
            )
            session.add(row)
            session.flush()
            # Detach a plain dict to avoid session lifetime issues
            snapshot = _progress_snapshot(row)
        else:
            snapshot = _progress_snapshot(row)

    return snapshot


def _progress_snapshot(row: BackfillProgress) -> BackfillProgress:
    """Return a detached copy of BackfillProgress with all attributes loaded."""
    # We can't keep the ORM object alive outside the session (SQLite thread
    # safety), so capture all values into a plain namespace.
    p = BackfillProgress.__new__(BackfillProgress)
    for col in ("id", "symbol", "timeframe", "status", "earliest_open_time",
                "latest_open_time", "target_start_epoch", "total_inserted",
                "windows_fetched", "error_count", "started_at",
                "completed_at", "last_error"):
        setattr(p, col, getattr(row, col, None))
    return p


def _update_progress(
    symbol: str,
    timeframe: str,
    *,
    status: str | None = None,
    earliest_open_time: int | None = None,
    latest_open_time:   int | None = None,
    delta_inserted:     int = 0,
    delta_windows:      int = 0,
    delta_errors:       int = 0,
    started_at:         int | None = None,
    completed_at:       int | None = None,
    last_error:         str | None = None,
) -> None:
    with get_db_session() as session:
        row = session.execute(
            select(BackfillProgress)
            .where(BackfillProgress.symbol    == symbol)
            .where(BackfillProgress.timeframe == timeframe)
        ).scalar_one()

        if status              is not None: row.status             = status
        if earliest_open_time  is not None:
            if row.earliest_open_time is None or earliest_open_time < row.earliest_open_time:
                row.earliest_open_time = earliest_open_time
        if latest_open_time    is not None:
            if row.latest_open_time is None or latest_open_time > row.latest_open_time:
                row.latest_open_time = latest_open_time
        if delta_inserted:   row.total_inserted  += delta_inserted
        if delta_windows:    row.windows_fetched += delta_windows
        if delta_errors:     row.error_count     += delta_errors
        if started_at   is not None: row.started_at   = started_at
        if completed_at is not None: row.completed_at = completed_at
        if last_error   is not None: row.last_error   = last_error


def _store_candles_batch(
    symbol: str,
    timeframe: str,
    rows: list[dict],
) -> int:
    """INSERT OR IGNORE a list of validated candle dicts. Returns insert count."""
    if not rows:
        return 0

    inserted = 0
    with get_db_session() as session:
        for i in range(0, len(rows), _INSERT_BATCH_SIZE):
            batch = rows[i : i + _INSERT_BATCH_SIZE]
            stmt   = sqlite_insert(Candle).values(batch).on_conflict_do_nothing()
            result = session.execute(stmt)
            inserted += result.rowcount
    return inserted


# ── Binance paginated backfill ────────────────────────────────────────────────

async def _binance_fetch_window(
    symbol: str,
    tf: str,
    end_ms: int,
    limit: int = _BINANCE_CANDLES_PER_REQUEST,
) -> list[list]:
    """Fetch one window of Binance klines ending at *end_ms* (milliseconds)."""
    url    = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol":   symbol,
        "interval": BINANCE_INTERVAL[tf],
        "limit":    limit,
        "endTime":  end_ms,
    }
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
                    "[Binance] 429 rate limit for %s/%s — sleeping %.1fs (attempt %d/%d)",
                    symbol, tf, wait, attempt + 1, _MAX_RETRIES,
                )
                await asyncio.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = _RETRY_BASE_SLEEP * (2 ** attempt)
                logger.warning(
                    "[Binance] %s server error for %s/%s — sleeping %.1fs",
                    resp.status_code, symbol, tf, wait,
                )
                await asyncio.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except httpx.RequestError as exc:
            wait = _RETRY_BASE_SLEEP * (2 ** attempt)
            logger.warning(
                "[Binance] Network error for %s/%s (attempt %d/%d): %s — sleeping %.1fs",
                symbol, tf, attempt + 1, _MAX_RETRIES, exc, wait,
            )
            if attempt < _MAX_RETRIES - 1:
                await asyncio.sleep(wait)

    raise RuntimeError(
        f"Binance: failed to fetch {symbol}/{tf} after {_MAX_RETRIES} retries"
    )


async def backfill_binance(
    symbol: str,
    tf: str,
    on_progress: ProgressCallback | None = None,
) -> BackfillProgress:
    """Paginate backward from now to the symbol launch date.

    Windows are fetched newest-first (endTime parameter walks backward).
    Each window of 1000 candles is stored then the cursor moves to
    (window_start - 1 ms) for the next request.
    """
    if tf not in BINANCE_INTERVAL:
        raise ValueError(f"Binance does not support timeframe '{tf}'")

    tf_secs        = TF_SECONDS[tf]
    target_start   = BINANCE_LAUNCH_EPOCHS.get(symbol, _now_epoch() - 86400 * YAHOO_DEFAULT_LOOKBACK_DAYS)
    now_ep         = _now_epoch()

    progress = _get_or_create_progress(symbol, tf, target_start)

    if progress.status == BackfillStatus.COMPLETE:
        logger.info("[Binance] %s/%s already complete — skipping", symbol, tf)
        return progress

    # Resume: pick up from where we left off
    if progress.earliest_open_time is not None:
        # Walk backward from one step before our earliest known candle
        window_end_ms = (progress.earliest_open_time - tf_secs) * 1000
        logger.info(
            "[Binance] Resuming %s/%s from %s (target: %s)",
            symbol, tf,
            _from_epoch(progress.earliest_open_time - tf_secs),
            _from_epoch(target_start),
        )
    else:
        # Fresh start: begin from now
        window_end_ms = now_ep * 1000
        logger.info(
            "[Binance] Starting backfill %s/%s → target %s",
            symbol, tf, _from_epoch(target_start),
        )

    _update_progress(
        symbol, tf,
        status=BackfillStatus.RUNNING,
        started_at=(progress.started_at or _now_epoch()),
    )

    total_inserted = 0
    windows        = 0
    errors         = 0

    while True:
        # Stop condition: cursor has passed the launch date
        window_end_epoch = window_end_ms // 1000
        if window_end_epoch <= target_start:
            logger.info(
                "[Binance] %s/%s reached target start — done (%d windows, %d rows)",
                symbol, tf, windows, total_inserted,
            )
            break

        try:
            raw = await _binance_fetch_window(symbol, tf, end_ms=window_end_ms)
        except Exception as exc:
            errors += 1
            err_msg = str(exc)
            logger.error("[Binance] Window error %s/%s: %s", symbol, tf, err_msg)
            _update_progress(symbol, tf, delta_errors=1, last_error=err_msg)
            # Don't abort — move cursor back by one full window and keep going
            window_end_ms -= tf_secs * _BINANCE_CANDLES_PER_REQUEST * 1000
            await asyncio.sleep(_RETRY_BASE_SLEEP * 4)
            continue

        if not raw:
            logger.info(
                "[Binance] %s/%s: empty response before target — stopping",
                symbol, tf,
            )
            break

        # Parse and validate
        valid_rows: list[dict] = []
        earliest_in_window = None
        latest_in_window   = None

        for kline in raw:
            open_time_ms  = int(kline[0])
            open_time_ep  = open_time_ms // 1000
            try:
                o, h, l, c, v = (
                    float(kline[1]), float(kline[2]),
                    float(kline[3]), float(kline[4]),
                    float(kline[5]),
                )
            except (IndexError, ValueError, TypeError):
                continue

            # Discard partial candle (still open)
            if _is_partial(open_time_ep, tf, now_ep):
                logger.debug(
                    "[Binance] Discarding partial candle %s/%s @ %s",
                    symbol, tf, _from_epoch(open_time_ep),
                )
                continue

            # Discard before launch date
            if open_time_ep < target_start:
                continue

            # OHLCV sanity
            if not _valid_ohlcv(o, h, l, c, v):
                logger.debug(
                    "[Binance] Rejecting invalid OHLCV %s/%s @ %s: O=%s H=%s L=%s C=%s",
                    symbol, tf, _from_epoch(open_time_ep), o, h, l, c,
                )
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

            if earliest_in_window is None or open_time_ep < earliest_in_window:
                earliest_in_window = open_time_ep
            if latest_in_window is None or open_time_ep > latest_in_window:
                latest_in_window = open_time_ep

        # Store this window
        inserted = _store_candles_batch(symbol, tf, valid_rows)
        total_inserted += inserted
        windows        += 1

        _update_progress(
            symbol, tf,
            earliest_open_time=earliest_in_window,
            latest_open_time=latest_in_window,
            delta_inserted=inserted,
            delta_windows=1,
        )

        logger.info(
            "[Binance] %s/%s window #%d: %d raw → %d stored | cursor now %s",
            symbol, tf, windows,
            len(raw), inserted,
            _from_epoch(earliest_in_window) if earliest_in_window else "?",
        )

        # Notify caller of progress (used by the terminal progress display)
        if on_progress is not None and earliest_in_window is not None:
            on_progress(earliest_in_window, windows, total_inserted)

        # Advance cursor to just before the start of this window
        if earliest_in_window is not None:
            window_end_ms = (earliest_in_window - tf_secs) * 1000
        else:
            # No valid data in window; step back by a full batch
            window_end_ms -= tf_secs * _BINANCE_CANDLES_PER_REQUEST * 1000

        # Throttle
        await asyncio.sleep(_BINANCE_INTER_REQUEST_SLEEP)

    final_status = BackfillStatus.COMPLETE if errors == 0 else BackfillStatus.PARTIAL
    _update_progress(
        symbol, tf,
        status=final_status,
        completed_at=_now_epoch(),
    )

    logger.info(
        "[Binance] Backfill complete %s/%s: status=%s, %d windows, %d rows, %d errors",
        symbol, tf, final_status, windows, total_inserted, errors,
    )
    return _get_or_create_progress(symbol, tf, target_start)


# ── Yahoo one-shot backfill ───────────────────────────────────────────────────

async def backfill_yahoo(
    symbol: str,
    tf: str,
    yahoo_symbol: str,
    on_progress: ProgressCallback | None = None,
) -> BackfillProgress:
    """Single-request full-range fetch for Yahoo Finance symbols.

    Yahoo returns up to 10 years of daily data in a single API call.
    For hourly data the practical limit is ~730 days.
    No pagination is needed — Yahoo handles the windowing internally.
    """
    from app.data.yahoo_client import yahoo_chart_get

    if tf not in YAHOO_INTERVAL:
        raise ValueError(f"Yahoo does not support timeframe '{tf}'")

    now_ep       = _now_epoch()
    target_start = now_ep - (YAHOO_DEFAULT_LOOKBACK_DAYS * 86400)
    progress     = _get_or_create_progress(symbol, tf, target_start)

    if progress.status == BackfillStatus.COMPLETE:
        logger.info("[Yahoo] %s/%s already complete — skipping", symbol, tf)
        return progress

    logger.info(
        "[Yahoo] Starting backfill %s/%s (yahoo_symbol=%s)",
        symbol, tf, yahoo_symbol,
    )

    _update_progress(
        symbol, tf,
        status=BackfillStatus.RUNNING,
        started_at=(progress.started_at or now_ep),
    )

    params = {
        "interval":       YAHOO_INTERVAL[tf],
        "range":          YAHOO_RANGE[tf],
        "includePrePost": "false",
        "events":         "div,splits",
    }

    # Retry loop for the single request
    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            payload = await yahoo_chart_get(yahoo_symbol, params)
            break
        except Exception as exc:
            wait = _RETRY_BASE_SLEEP * (2 ** attempt)
            logger.warning(
                "[Yahoo] Fetch failed %s/%s (attempt %d/%d): %s — sleeping %.1fs",
                symbol, tf, attempt + 1, _MAX_RETRIES, exc, wait,
            )
            last_exc = exc
            if attempt < _MAX_RETRIES - 1:
                await asyncio.sleep(wait)
    else:
        err_msg = str(last_exc)
        _update_progress(
            symbol, tf,
            status=BackfillStatus.FAILED,
            delta_errors=1,
            last_error=err_msg,
            completed_at=now_ep,
        )
        raise RuntimeError(
            f"Yahoo: failed to fetch {symbol}/{tf} after {_MAX_RETRIES} retries: {err_msg}"
        )

    # Parse response
    result = payload.get("chart", {}).get("result")
    if not result:
        err_msg = (
            (payload.get("chart", {}).get("error") or {}).get("description", "")
            or "No data returned"
        )
        _update_progress(
            symbol, tf,
            status=BackfillStatus.FAILED,
            delta_errors=1,
            last_error=err_msg,
            completed_at=now_ep,
        )
        raise ValueError(f"Yahoo returned no data for {yahoo_symbol}/{tf}: {err_msg}")

    chart   = result[0]
    timestamps = chart.get("timestamp") or []
    quote      = chart.get("indicators", {}).get("quote", [{}])[0]
    opens      = quote.get("open")   or []
    highs      = quote.get("high")   or []
    lows       = quote.get("low")    or []
    closes     = quote.get("close")  or []
    volumes    = quote.get("volume") or []

    valid_rows: list[dict] = []
    earliest   = None
    latest     = None

    for idx, ts in enumerate(timestamps):
        open_time_ep = int(ts)

        # Discard partial candle
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

        if earliest is None or open_time_ep < earliest: earliest = open_time_ep
        if latest   is None or open_time_ep > latest:   latest   = open_time_ep

    inserted = _store_candles_batch(symbol, tf, valid_rows)

    _update_progress(
        symbol, tf,
        status=BackfillStatus.COMPLETE,
        earliest_open_time=earliest,
        latest_open_time=latest,
        delta_inserted=inserted,
        delta_windows=1,
        completed_at=now_ep,
    )

    logger.info(
        "[Yahoo] Backfill complete %s/%s: %d raw → %d stored | %s → %s",
        symbol, tf, len(valid_rows), inserted,
        _from_epoch(earliest) if earliest else "?",
        _from_epoch(latest)   if latest   else "?",
    )
    # Notify caller that this single-shot fetch is done
    if on_progress is not None and earliest is not None:
        on_progress(earliest, 1, inserted)
    return _get_or_create_progress(symbol, tf, target_start)


# ── Orchestrator ──────────────────────────────────────────────────────────────

@dataclass
class BackfillTarget:
    """Descriptor for one (symbol, timeframe) job."""
    market:       str          # "crypto" | "forex" | "futures"
    symbol:       str          # bare symbol, e.g. "BTCUSDT"
    timeframe:    str
    yahoo_symbol: str = ""     # set for forex/futures, e.g. "EURUSD=X"


# Default target manifest — all registered symbols and their primary timeframes
DEFAULT_TARGETS: list[BackfillTarget] = [
    # ── Crypto (Binance) ────────────────────────────────────────────────────
    BackfillTarget("crypto", "BTCUSDT",  "1h"),
    BackfillTarget("crypto", "BTCUSDT",  "1d"),
    BackfillTarget("crypto", "ETHUSDT",  "1h"),
    BackfillTarget("crypto", "ETHUSDT",  "1d"),
    BackfillTarget("crypto", "SOLUSDT",  "1h"),
    BackfillTarget("crypto", "SOLUSDT",  "1d"),
    BackfillTarget("crypto", "BNBUSDT",  "1h"),
    BackfillTarget("crypto", "ADAUSDT",  "1h"),
    BackfillTarget("crypto", "XRPUSDT",  "1h"),
    BackfillTarget("crypto", "DOGEUSDT",  "1h"),
    BackfillTarget("crypto", "AVAXUSDT",  "1h"),
    BackfillTarget("crypto", "MATICUSDT", "1h"),
    BackfillTarget("crypto", "LINKUSDT",  "1h"),
    # ── Forex (Yahoo) ───────────────────────────────────────────────────────
    BackfillTarget("forex",  "EURUSD",  "1h",  "EURUSD=X"),
    BackfillTarget("forex",  "EURUSD",  "1d",  "EURUSD=X"),
    BackfillTarget("forex",  "GBPUSD",  "1d",  "GBPUSD=X"),
    BackfillTarget("forex",  "USDJPY",  "1d",  "USDJPY=X"),
    BackfillTarget("forex",  "AUDUSD",  "1d",  "AUDUSD=X"),
    # ── Futures (Yahoo) ─────────────────────────────────────────────────────
    BackfillTarget("futures","GC",      "1d",  "GC=F"),
    BackfillTarget("futures","SI",      "1d",  "SI=F"),
    BackfillTarget("futures","CL",      "1d",  "CL=F"),
    BackfillTarget("futures","NG",      "1d",  "NG=F"),
]


async def run_backfill(
    targets: list[BackfillTarget] | None = None,
    *,
    skip_complete: bool = True,
    display: "BackfillDisplay | None" = None,
) -> dict[str, dict]:
    """Run the full backfill across all targets.

    Args:
        targets:       List of BackfillTarget descriptors; defaults to
                       DEFAULT_TARGETS.
        skip_complete: If True, targets already marked COMPLETE are skipped
                       without touching the API.
        display:       Optional BackfillDisplay instance for terminal progress.
                       When provided each symbol's progress is rendered live.

    Returns:
        Mapping of "SYMBOL/TF" → summary dict for each target.
    """
    if targets is None:
        targets = DEFAULT_TARGETS

    results: dict[str, dict] = {}

    for target in targets:
        key = f"{target.symbol}/{target.timeframe}"
        try:
            # Compute date range bounds for the progress display
            now_ep = _now_epoch()
            if target.market == "crypto":
                target_ep = BINANCE_LAUNCH_EPOCHS.get(
                    target.symbol,
                    now_ep - 86400 * YAHOO_DEFAULT_LOOKBACK_DAYS,
                )
            else:
                target_ep = now_ep - 86400 * YAHOO_DEFAULT_LOOKBACK_DAYS

            market_label = "Binance" if target.market == "crypto" else "Yahoo"

            if display is not None:
                display.begin_symbol(
                    symbol       = target.symbol,
                    timeframe    = target.timeframe,
                    market       = market_label,
                    now_epoch    = now_ep,
                    target_epoch = target_ep,
                )

            # Build per-symbol progress callback
            def _make_cb(disp: "BackfillDisplay") -> "ProgressCallback":
                def _cb(cursor_epoch: int, windows: int, candles: int) -> None:
                    disp.update_window(cursor_epoch, windows, candles)
                return _cb

            on_progress: "ProgressCallback | None" = (
                _make_cb(display) if display is not None else None
            )

            if target.market == "crypto":
                progress = await backfill_binance(
                    target.symbol, target.timeframe,
                    on_progress=on_progress,
                )
            else:
                await asyncio.sleep(_YAHOO_INTER_SYMBOL_SLEEP)
                if display is not None:
                    display.update_window(target_ep, 0, 0, status="fetching…")
                progress = await backfill_yahoo(
                    target.symbol,
                    target.timeframe,
                    target.yahoo_symbol,
                    on_progress=on_progress,
                )

            if display is not None:
                display.finish_symbol(status=progress.status)

            results[key] = {
                "status":         progress.status,
                "total_inserted": progress.total_inserted,
                "windows_fetched": progress.windows_fetched,
                "error_count":    progress.error_count,
                "earliest":       _from_epoch(progress.earliest_open_time) if progress.earliest_open_time else None,
                "latest":         _from_epoch(progress.latest_open_time)   if progress.latest_open_time   else None,
            }

        except Exception as exc:
            logger.exception("[Backfill] Fatal error for %s: %s", key, exc)
            if display is not None:
                display.finish_symbol(status="error")
            results[key] = {"status": "error", "error": str(exc)}

    return results


async def get_backfill_status() -> list[dict]:
    """Return current BackfillProgress rows as JSON-serialisable dicts."""
    with get_db_session() as session:
        rows = session.execute(
            select(BackfillProgress).order_by(
                BackfillProgress.symbol,
                BackfillProgress.timeframe,
            )
        ).scalars().all()

        return [
            {
                "symbol":            r.symbol,
                "timeframe":         r.timeframe,
                "status":            r.status,
                "total_inserted":    r.total_inserted,
                "windows_fetched":   r.windows_fetched,
                "error_count":       r.error_count,
                "earliest":          _from_epoch(r.earliest_open_time) if r.earliest_open_time else None,
                "latest":            _from_epoch(r.latest_open_time)   if r.latest_open_time   else None,
                "target_start":      _from_epoch(r.target_start_epoch) if r.target_start_epoch else None,
                "started_at":        _from_epoch(r.started_at)         if r.started_at         else None,
                "completed_at":      _from_epoch(r.completed_at)       if r.completed_at       else None,
                "last_error":        r.last_error,
            }
            for r in rows
        ]
