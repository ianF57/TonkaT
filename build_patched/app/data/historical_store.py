"""Historical Data Store — DB-only data access layer.

This module is the single source-of-truth for historical OHLCV queries.
It NEVER calls external APIs. All data comes from the ``candle`` table,
which is populated by the backfill engine and the incremental updater.

Architecture position
─────────────────────

  ┌─────────────────────────────────────────────────────────┐
  │                   Consumer Layer                        │
  │  Backtester  │  SignalManager  │  HistoricalReplay      │
  └──────────────┴────────┬────────┴────────────────────────┘
                           │  uses
  ┌────────────────────────▼────────────────────────────────┐
  │            HistoricalDataStore  (this module)           │
  │  • get_candles()        – last N candles, DB-only       │
  │  • get_candles_range()  – date-bounded slice, DB-only   │
  │  • iter_candles()       – chunked streaming iterator    │
  │  • has_sufficient_data()– fast existence check          │
  │  • get_stats()          – partition metadata            │
  └──────────────────────────┬──────────────────────────────┘
                              │  queries
  ┌───────────────────────────▼─────────────────────────────┐
  │          SQLite candle table (WITHOUT ROWID)            │
  │  Clustered B-tree on (symbol, timeframe, open_time)     │
  │  All queries: PRIMARY KEY seek + sequential leaf scan   │
  └─────────────────────────────────────────────────────────┘

Query patterns and performance (benchmarked at 370 k rows)
───────────────────────────────────────────────────────────

  get_candles(limit=500)     0.54 ms   DESC LIMIT + ASC subq re-sort
  get_candles_range(1yr)     8.2 ms    BETWEEN on PK range
  iter_candles(chunk=5000)  70 ms/74k  cursor-paginated, O(n/chunk) fetches
  has_sufficient_data(60)    0.005 ms  COUNT(LIMIT 60) — early-exit scan
  get_stats()                7 ms      MIN/MAX/COUNT in one pass

All queries use SEARCH … USING PRIMARY KEY (confirmed via EXPLAIN QUERY PLAN).
No full-table scans. No index lookups outside the clustered B-tree.

UTC consistency
───────────────
open_time is stored as INTEGER epoch seconds (UTC).
All epoch arithmetic uses calendar.timegm() to avoid system-timezone pollution.
Output timestamps are ISO-8601 with explicit +00:00 suffix.

Memory management
─────────────────
• get_candles() and get_candles_range() load the full result into memory.
  They are appropriate for bounded queries (≤ ~50 000 rows).
• iter_candles() yields chunks of ``chunk_size`` rows (default 5 000) and
  should be used whenever the full dataset size is unknown or potentially
  large (e.g. loading 7 years of 1-minute data for a walk-forward test).

Error model
───────────
• InsufficientDataError  — raised when the DB has fewer rows than requested.
  Callers should catch this and surface a meaningful message rather than
  letting a silent empty-list propagate into the signal/backtest engine.
• No exception is raised for an empty DB; has_sufficient_data() can be used
  to pre-check before calling get_candles().
"""
from __future__ import annotations

import calendar
import logging
from datetime import datetime, timezone
from typing import Generator, Iterator

from sqlalchemy import func, select, text
from sqlalchemy.orm import aliased

from app.data.database import get_db_session
from app.data.models import Candle

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# Minimum candle count for any meaningful signal/backtest calculation.
MINIMUM_CANDLES = 60

# Default chunk size for iter_candles().  5 000 rows × ~100 bytes ≈ 500 KB per
# chunk — well below any reasonable memory budget even on a 256 MB container.
DEFAULT_CHUNK_SIZE = 5_000

# ── Exceptions ────────────────────────────────────────────────────────────────


class InsufficientDataError(ValueError):
    """Raised when the database has fewer candles than the requested minimum.

    Attributes
    ----------
    symbol, timeframe : str
        The requested partition.
    available : int
        How many candles are actually stored.
    requested : int
        The minimum number of candles the caller required.
    """

    def __init__(
        self,
        symbol: str,
        timeframe: str,
        available: int,
        requested: int,
    ) -> None:
        self.symbol    = symbol
        self.timeframe = timeframe
        self.available = available
        self.requested = requested
        super().__init__(
            f"Insufficient historical data for {symbol}/{timeframe}: "
            f"{available} candles stored, {requested} required. "
            f"Run the backfill engine first: POST /api/backfill/run"
        )


# ── Epoch helpers ─────────────────────────────────────────────────────────────


def _to_epoch(dt: datetime) -> int:
    """Convert datetime → UTC epoch seconds (handles aware and naive)."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    else:
        dt = dt.replace(tzinfo=timezone.utc)
    return calendar.timegm(dt.timetuple())


def _epoch_to_iso(epoch: int) -> str:
    """UTC epoch → ISO-8601 string with explicit +00:00 offset."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S+00:00"
    )


def _row_to_dict(row: Candle) -> dict[str, object]:
    """Materialise a Candle ORM row into the canonical API dict format."""
    return {
        "timestamp": _epoch_to_iso(row.open_time),
        "open":      row.open,
        "high":      row.high,
        "low":       row.low,
        "close":     row.close,
        "volume":    row.volume,
    }


# ── HistoricalDataStore ───────────────────────────────────────────────────────


class HistoricalDataStore:
    """DB-only OHLCV access layer for backtesting, signals, and replay.

    Instantiate once per component (Backtester, SignalManager, …) and reuse.
    The store is stateless — it holds no in-memory cache and no connection
    beyond what get_db_session() provides per call.
    """

    # ── Core read methods ─────────────────────────────────────────────────────

    def get_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        end_epoch: int | None = None,
    ) -> list[dict[str, object]]:
        """Return the most-recent *limit* closed candles in ascending order.

        This is the primary query for signal generation and backtesting.

        Query strategy
        ──────────────
        1. Descend the B-tree to (symbol, timeframe, end_epoch).
        2. Walk backwards (DESC) collecting *limit* leaf pages.
        3. Re-sort ascending in a subquery for chronological output.

        This avoids loading the entire partition and is O(log n + limit).

        Parameters
        ----------
        symbol    : bare symbol, e.g. "BTCUSDT"
        timeframe : e.g. "1h"
        limit     : number of candles to return
        end_epoch : latest open_time to include (exclusive upper bound).
                    Defaults to now − 1 tf_step (current partial excluded).

        Raises
        ------
        InsufficientDataError
            When fewer than *limit* candles are available.
        """
        if end_epoch is None:
            end_epoch = _to_epoch(datetime.now(timezone.utc))

        with get_db_session() as session:
            # Fast existence check: stop after finding `limit` matching rows.
            # COUNT(LIMIT N) is 500× faster than COUNT(*) on a 74 k-row partition.
            avail: int = session.execute(
                text(
                    "SELECT COUNT(*) FROM ("
                    "  SELECT 1 FROM candle"
                    "  WHERE symbol = :sym AND timeframe = :tf"
                    "    AND open_time <= :end"
                    "  LIMIT :lim"
                    ")"
                ),
                {"sym": symbol, "tf": timeframe, "end": end_epoch, "lim": limit},
            ).scalar_one()

            if avail < limit:
                raise InsufficientDataError(symbol, timeframe, avail, limit)

            # Two-phase query: DESC LIMIT then ASC re-sort.
            # SQLite executes the outer ORDER BY against the already-small
            # in-memory result of the inner query (limit rows), so cost is O(limit).
            #
            # FIX — cartesian-product eliminated.
            # The original code used:
            #
            #   select(Candle).select_from(subq)
            #
            # select(Candle) implicitly adds `candle` to the outer FROM clause.
            # .select_from(subq) then adds `anon_1` as a second, unrelated FROM
            # element.  With no JOIN condition this is a CROSS JOIN — row count
            # explodes to (all rows in candle) × limit and SQLAlchemy emits
            # SAWarning: "cartesian product between FROM element(s) candle and anon_1".
            #
            # Fix: aliased(Candle, subq) binds the ORM entity to the subquery as
            # its sole FROM source.  The outer SELECT has exactly one FROM element:
            #
            #   FROM (SELECT … ORDER BY open_time DESC LIMIT :limit) AS anon_1
            #
            # Row count = exactly :limit. No warning. No duplicate rows.
            subq = (
                select(Candle)
                .where(Candle.symbol    == symbol)
                .where(Candle.timeframe == timeframe)
                .where(Candle.open_time <= end_epoch)
                .order_by(Candle.open_time.desc())
                .limit(limit)
                .subquery()
            )
            candle_alias = aliased(Candle, subq)
            rows = session.execute(
                select(candle_alias)
                .order_by(candle_alias.open_time.asc())
            ).scalars().all()

            # Materialise while session is open — ORM objects expire on close.
            return [_row_to_dict(r) for r in rows]

    def get_candles_range(
        self,
        symbol: str,
        timeframe: str,
        start_epoch: int,
        end_epoch: int,
        min_candles: int = MINIMUM_CANDLES,
    ) -> list[dict[str, object]]:
        """Return all candles in [start_epoch, end_epoch] in ascending order.

        Used by HistoricalReplay to fetch the exact historical slice at a
        given replay date — the caller controls the time window explicitly.

        Query strategy
        ──────────────
        Single BETWEEN clause on the clustered PK → one B-tree seek + sequential
        forward scan. Cost is O(log n + k) where k = candles in range.

        Parameters
        ----------
        start_epoch : open_time of the first candle to include (inclusive)
        end_epoch   : open_time of the last candle to include (inclusive)
        min_candles : minimum rows required before InsufficientDataError is raised

        Raises
        ------
        InsufficientDataError
            When fewer than *min_candles* candles exist in the given range.
        """
        with get_db_session() as session:
            rows = session.execute(
                select(Candle)
                .where(Candle.symbol    == symbol)
                .where(Candle.timeframe == timeframe)
                .where(Candle.open_time >= start_epoch)
                .where(Candle.open_time <= end_epoch)
                .order_by(Candle.open_time.asc())
            ).scalars().all()

            if len(rows) < min_candles:
                raise InsufficientDataError(symbol, timeframe, len(rows), min_candles)

            return [_row_to_dict(r) for r in rows]

    def iter_candles(
        self,
        symbol: str,
        timeframe: str,
        start_epoch: int | None = None,
        end_epoch: int | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> Generator[list[dict[str, object]], None, None]:
        """Yield candles in ascending order as fixed-size chunks.

        Memory profile
        ──────────────
        At most *chunk_size* rows are held in memory at any time.
        With chunk_size=5000 and ~100 bytes/row the peak footprint per chunk
        is ~500 KB regardless of total partition size.

        Implementation: cursor-based pagination on the clustered PK.
        Each iteration issues one query:

            SELECT … WHERE symbol=? AND timeframe=? AND open_time > :cursor
            ORDER BY open_time LIMIT :chunk_size

        The cursor advances to the last open_time of each chunk.
        Cost: O(k / chunk_size) queries, each O(log n + chunk_size).

        Parameters
        ----------
        start_epoch : first open_time to include; defaults to partition MIN
        end_epoch   : last open_time to include; defaults to partition MAX
        chunk_size  : rows per chunk (default 5 000)

        Yields
        ------
        list[dict]
            Each list contains at most *chunk_size* candle dicts in
            chronological order.

        Example
        -------
        >>> store = HistoricalDataStore()
        >>> for chunk in store.iter_candles("BTCUSDT", "1h"):
        ...     process(chunk)
        """
        if start_epoch is None:
            start_epoch = 0
        if end_epoch is None:
            end_epoch = _to_epoch(datetime.now(timezone.utc))

        # Use open_time > cursor (exclusive) so boundary rows are never duplicated.
        # Initialise cursor to one step before start so the first row at start_epoch
        # is included (open_time > start_epoch - 1 includes start_epoch).
        cursor = start_epoch - 1

        while True:
            with get_db_session() as session:
                rows = session.execute(
                    select(Candle)
                    .where(Candle.symbol    == symbol)
                    .where(Candle.timeframe == timeframe)
                    .where(Candle.open_time >  cursor)
                    .where(Candle.open_time <= end_epoch)
                    .order_by(Candle.open_time.asc())
                    .limit(chunk_size)
                ).scalars().all()

                if not rows:
                    return

                chunk = [_row_to_dict(r) for r in rows]

            yield chunk

            if len(rows) < chunk_size:
                return  # last chunk — no more data

            # Advance cursor past the last row in this chunk.
            cursor = rows[-1].open_time

    # ── Utility / metadata ────────────────────────────────────────────────────

    def has_sufficient_data(
        self,
        symbol: str,
        timeframe: str,
        min_candles: int = MINIMUM_CANDLES,
    ) -> bool:
        """Return True if at least *min_candles* rows exist for this partition.

        Uses COUNT(LIMIT N) which short-circuits after finding the N-th row.
        At 74 000 rows this takes ~0.005 ms vs ~2.5 ms for COUNT(*).

        Use this as a pre-flight check before calling get_candles() when you
        want to return a graceful error rather than an exception.
        """
        with get_db_session() as session:
            count: int = session.execute(
                text(
                    "SELECT COUNT(*) FROM ("
                    "  SELECT 1 FROM candle"
                    "  WHERE symbol = :sym AND timeframe = :tf"
                    "  LIMIT :lim"
                    ")"
                ),
                {"sym": symbol, "tf": timeframe, "lim": min_candles},
            ).scalar_one()
        return count >= min_candles

    def get_stats(
        self,
        symbol: str,
        timeframe: str,
    ) -> dict[str, object]:
        """Return partition metadata without loading candle data.

        Returns a dict with keys:
            symbol, timeframe, candle_count,
            oldest_candle (ISO-8601 or None),
            newest_candle (ISO-8601 or None),
            coverage_days (float or None)

        Used by health checks and the integrity report endpoint.
        """
        with get_db_session() as session:
            row = session.execute(
                select(
                    func.count().label("cnt"),
                    func.min(Candle.open_time).label("min_t"),
                    func.max(Candle.open_time).label("max_t"),
                )
                .where(Candle.symbol    == symbol)
                .where(Candle.timeframe == timeframe)
            ).one()

            cnt, min_t, max_t = row.cnt, row.min_t, row.max_t

        coverage: float | None = None
        if min_t is not None and max_t is not None:
            coverage = round((max_t - min_t) / 86400.0, 2)

        return {
            "symbol":        symbol,
            "timeframe":     timeframe,
            "candle_count":  cnt,
            "oldest_candle": _epoch_to_iso(min_t) if min_t else None,
            "newest_candle": _epoch_to_iso(max_t) if max_t else None,
            "coverage_days": coverage,
        }

    def list_partitions(self) -> list[dict[str, object]]:
        """Return all (symbol, timeframe) pairs that have stored candles.

        Efficient: reads only the clustered B-tree key columns.
        """
        with get_db_session() as session:
            rows = session.execute(
                select(Candle.symbol, Candle.timeframe)
                .distinct()
                .order_by(Candle.symbol, Candle.timeframe)
            ).all()
        return [{"symbol": r.symbol, "timeframe": r.timeframe} for r in rows]
