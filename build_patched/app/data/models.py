"""SQLAlchemy 2.x ORM models.

Tables
──────
Candle               — production OHLCV storage (WITHOUT ROWID, clustered B-tree)
BackfillProgress     — per-(symbol, timeframe) backfill job state and progress
IncrementalUpdateLog — audit trail for every incremental update run
OHLCVCacheLegacy     — old ohlcv_cache table kept mapped for safe migration only
"""
from __future__ import annotations

import enum

from sqlalchemy import (
    BigInteger, Float, Index, Integer, String, Text, UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.data.database import Base


# ─────────────────────────────────────────────────────────────────────────────
# Production candle table
# ─────────────────────────────────────────────────────────────────────────────

class Candle(Base):
    """Persisted OHLCV candle.

    WITHOUT ROWID clusters the B-tree on (symbol, timeframe, open_time) so
    backtesting range-scans are sequential leaf walks with zero random I/O.
    open_time is Unix epoch seconds UTC — no TEXT/timezone ambiguity.
    INSERT OR IGNORE on the composite PK prevents all duplicate rows.
    """

    __tablename__ = "candle"
    __table_args__ = (
        # Powers the cache-freshness COUNT: 3.9× faster than partition scan.
        Index("idx_candle_freshness", "symbol", "timeframe", "fetched_at"),
        {"sqlite_with_rowid": False},   # clustered B-tree on PK
    )

    symbol:    Mapped[str] = mapped_column(String(32), primary_key=True, nullable=False)
    timeframe: Mapped[str] = mapped_column(String(8),  primary_key=True, nullable=False)
    open_time: Mapped[int] = mapped_column(BigInteger, primary_key=True, nullable=False)

    open:   Mapped[float] = mapped_column(Float, nullable=False)
    high:   Mapped[float] = mapped_column(Float, nullable=False)
    low:    Mapped[float] = mapped_column(Float, nullable=False)
    close:  Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    fetched_at: Mapped[int] = mapped_column(BigInteger, nullable=False)

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Candle {self.symbol}/{self.timeframe} @ {self.open_time} C={self.close}>"


# ─────────────────────────────────────────────────────────────────────────────
# Backfill progress tracking
# ─────────────────────────────────────────────────────────────────────────────

class BackfillStatus(str, enum.Enum):
    PENDING  = "pending"
    RUNNING  = "running"
    COMPLETE = "complete"
    FAILED   = "failed"
    PARTIAL  = "partial"   # completed but with some window-level errors


class BackfillProgress(Base):
    """Tracks backfill state per (symbol, timeframe) pair.

    Design intent
    ─────────────
    earliest_open_time  — the oldest epoch we have successfully stored.
                          Moves backward as each window is fetched and committed.
                          On resume: start next window at (earliest - tf_secs).
    latest_open_time    — the newest epoch stored, set on first run.
    target_start_epoch  — the exchange launch date / maximum lookback we aim for.
    status              — pending → running → complete | failed | partial.
    total_inserted      — cumulative new rows (INSERT OR IGNORE skips counted out).
    windows_fetched     — API request count (for rate-limit auditing).
    error_count         — windows that failed after all retries.

    Idempotency guarantee
    ──────────────────────
    The unique constraint on (symbol, timeframe) means INSERT OR IGNORE in
    _get_or_create_progress will never create duplicate progress rows, and
    re-running the engine on a COMPLETE symbol is an immediate no-op.
    """

    __tablename__ = "backfill_progress"
    __table_args__ = (
        UniqueConstraint("symbol", "timeframe", name="uq_backfill_sym_tf"),
    )

    id:       Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol:   Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    timeframe: Mapped[str] = mapped_column(String(8),  nullable=False)

    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default=BackfillStatus.PENDING
    )

    # Progress cursors (epochs)
    earliest_open_time:  Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    latest_open_time:    Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    target_start_epoch:  Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Counters
    total_inserted:  Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    windows_fetched: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_count:     Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Timestamps
    started_at:   Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    completed_at: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Last error message for debugging
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<BackfillProgress {self.symbol}/{self.timeframe} "
            f"status={self.status} inserted={self.total_inserted}>"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Incremental update audit log
# ─────────────────────────────────────────────────────────────────────────────

class IncrementalUpdateLog(Base):
    """Audit record for every incremental update run.

    One row is written per (symbol, timeframe) per invocation of
    run_incremental_update(), whether or not new candles were found.

    Design intent
    ─────────────
    candles_fetched     — raw candle count returned by the upstream API.
    candles_inserted    — rows actually written (INSERT OR IGNORE new rows only).
    gaps_found          — number of gaps detected after the forward update.
    gaps_filled         — number of gaps where a fill was attempted.
    candles_gap_filled  — rows inserted during gap-fill passes.
    duration_ms         — wall-clock time for the entire run (API + DB).
    status              — "updated" | "up_to_date" | "error" | "no_backfill"

    The (symbol, timeframe, run_at) index supports efficient per-symbol history
    queries without a full table scan.
    """

    __tablename__ = "incremental_update_log"
    __table_args__ = (
        Index("idx_incr_log_sym_tf_run", "symbol", "timeframe", "run_at"),
    )

    id:        Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol:    Mapped[str] = mapped_column(String(32), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(8),  nullable=False)
    run_at:    Mapped[int] = mapped_column(BigInteger,  nullable=False)  # epoch UTC

    status: Mapped[str] = mapped_column(String(16), nullable=False)

    # Outcome counters
    candles_fetched:    Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    candles_inserted:   Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    gaps_found:         Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    gaps_filled:        Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    candles_gap_filled: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duration_ms:        Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Error detail (NULL on success)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<IncrementalUpdateLog {self.symbol}/{self.timeframe} "
            f"@ {self.run_at} status={self.status} +{self.candles_inserted}>"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Data integrity audit log
# ─────────────────────────────────────────────────────────────────────────────

class CandleIntegrityReport(Base):
    """Audit record for every integrity validation run.

    One row is written per (symbol, timeframe) per invocation of
    validate_and_repair().  The scan_mode field distinguishes between a full
    partition scan and an incremental scan starting from the previous
    scanned_to checkpoint.

    Health values
    ─────────────
    "clean"        — no issues detected.
    "repaired"     — issues were found and fully repaired.
    "issues_remain"— issues found but not all could be repaired (e.g. large
                     exchange gap, upstream error).
    "error"        — the integrity check itself raised an exception.

    The (symbol, timeframe, run_at) index supports per-pair history queries
    and checkpoint lookup (finding scanned_to from the last successful run)
    without a full table scan.
    """

    __tablename__ = "candle_integrity_report"
    __table_args__ = (
        Index("idx_integrity_sym_tf_run", "symbol", "timeframe", "run_at"),
    )

    id:        Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol:    Mapped[str] = mapped_column(String(32), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(8),  nullable=False)
    run_at:    Mapped[int] = mapped_column(BigInteger,  nullable=False)

    scan_mode: Mapped[str] = mapped_column(String(16), nullable=False, default="full")

    # Scan boundaries (epochs, NULL for full scan that found no data)
    scanned_from:    Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    scanned_to:      Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    candles_scanned: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Issues found
    gaps_found:              Mapped[int]  = mapped_column(Integer, nullable=False, default=0)
    gaps_total_missing:      Mapped[int]  = mapped_column(Integer, nullable=False, default=0)
    ohlcv_anomalies_found:   Mapped[int]  = mapped_column(Integer, nullable=False, default=0)
    partial_candles_found:   Mapped[int]  = mapped_column(Integer, nullable=False, default=0)
    monotonicity_violations: Mapped[int]  = mapped_column(Integer, nullable=False, default=0)
    dataset_stale:           Mapped[bool] = mapped_column(Integer, nullable=False, default=False)
    stale_by_periods:        Mapped[int]  = mapped_column(Integer, nullable=False, default=0)

    # Repair outcomes
    gaps_repaired:            Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    candles_repair_inserted:  Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ohlcv_anomalies_repaired: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    partial_candles_removed:  Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Overall health classification
    health:      Mapped[str]       = mapped_column(String(16), nullable=False, default="clean")
    duration_ms: Mapped[int]       = mapped_column(Integer,    nullable=False, default=0)
    error:       Mapped[str | None] = mapped_column(Text, nullable=True)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<CandleIntegrityReport {self.symbol}/{self.timeframe} "
            f"@ {self.run_at} health={self.health} mode={self.scan_mode}>"
        )




class OHLCVCacheLegacy(Base):
    """Old ohlcv_cache table — kept for zero-data-loss migration only.

    The database.py migration renames this to ohlcv_cache_v1_backup and
    copies rows into Candle on first startup. Do not write to this table.
    """

    __tablename__ = "ohlcv_cache"
    __table_args__ = (
        UniqueConstraint(
            "provider", "asset", "timeframe", "timestamp",
            name="uq_ohlcv_cache_key",
        ),
        {"extend_existing": True},
    )

    id:         Mapped[int]   = mapped_column(Integer,    primary_key=True, autoincrement=True)
    provider:   Mapped[str]   = mapped_column(String(32), nullable=False, index=True)
    asset:      Mapped[str]   = mapped_column(String(64), nullable=False, index=True)
    timeframe:  Mapped[str]   = mapped_column(String(8),  nullable=False, index=True)
    timestamp:  Mapped[str]   = mapped_column(Text,       nullable=False, index=True)
    open:       Mapped[float] = mapped_column(Float, nullable=False)
    high:       Mapped[float] = mapped_column(Float, nullable=False)
    low:        Mapped[float] = mapped_column(Float, nullable=False)
    close:      Mapped[float] = mapped_column(Float, nullable=False)
    volume:     Mapped[float] = mapped_column(Float, nullable=False)
    fetched_at: Mapped[str]   = mapped_column(Text,  nullable=False)


class BacktestResultCache(Base):
    __tablename__ = "backtest_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    config_hash: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    metrics_json: Mapped[str] = mapped_column(Text, nullable=False)
    trades_json: Mapped[str] = mapped_column(Text, nullable=False)
    equity_curve_json: Mapped[str] = mapped_column(Text, nullable=False)
    execution_time: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    created_at: Mapped[int] = mapped_column(BigInteger, nullable=False)


class BacktestJob(Base):
    __tablename__ = "backtest_job"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    request_json: Mapped[str] = mapped_column(Text, nullable=False)
    response_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
