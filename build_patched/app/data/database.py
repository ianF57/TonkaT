"""Database engine, session factory, and initialisation with migration.

Startup sequence
────────────────
1. Ensure the DB directory exists.
2. Apply PRAGMA optimisations (WAL, cache, temp-store).
3. Import models so Base.metadata knows about all tables.
4. Run create_all — idempotent; skips tables that already exist.
5. Run _migrate_legacy_ohlcv_cache — one-shot migration from the v1
   ``ohlcv_cache`` table to the production ``candle`` table.
6. Health-check SELECT 1.

Migration safety
────────────────
• The old ``ohlcv_cache`` table is renamed to ``ohlcv_cache_v1_backup``
  (not dropped) so data is never permanently lost.
• INSERT OR IGNORE prevents duplicate rows if the migration is re-run.
• Migration state is tracked via the ``schema_migration`` table so it
  only executes once even if the process restarts.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from config import settings

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    """SQLAlchemy 2.x declarative base."""


# ── Engine ────────────────────────────────────────────────────────────────────

engine = create_engine(
    settings.database_url,
    connect_args={"check_same_thread": False},
    # pool_pre_ping keeps connections healthy across long idle periods
    pool_pre_ping=True,
)


@event.listens_for(engine, "connect")
def _apply_sqlite_pragmas(dbapi_conn, _connection_record) -> None:
    """Apply per-connection SQLite PRAGMAs.

    These must be set on every new connection; they are not persisted in the
    database file (except journal_mode which is file-level).

    WAL mode
        Write-Ahead Logging allows concurrent reads during a write.  This
        matters when the data-manager background task is storing candles
        while a backtest query is running in another async task.

    synchronous = NORMAL
        Fsync on WAL checkpoints only (not every commit).  Safe against OS
        crashes; only risks data loss on a power failure mid-checkpoint, which
        is acceptable for a market-data cache that can be rebuilt from upstream.

    cache_size = -65536
        Negative value → kilobytes.  64 MB page cache keeps hot B-tree pages
        in memory, avoiding repeated disk reads for the same symbol/timeframe
        partition during repeated backtest runs.

    temp_store = MEMORY
        Sort and group-by temporaries are built in RAM instead of temp files.
        Matters for ORDER BY open_time DESC LIMIT 300 on large partitions.

    mmap_size = 256 MB
        Memory-map the database file.  On 64-bit OS the kernel uses the page
        cache for mmap'd reads, avoiding an extra copy through the SQLite
        read buffer for large sequential scans.
    """
    cursor = dbapi_conn.cursor()
    pragmas = [
        "PRAGMA journal_mode=WAL",
        "PRAGMA synchronous=NORMAL",
        "PRAGMA cache_size=-65536",   # 64 MB
        "PRAGMA temp_store=MEMORY",
        "PRAGMA mmap_size=268435456", # 256 MB
        "PRAGMA foreign_keys=ON",
    ]
    for pragma in pragmas:
        cursor.execute(pragma)
    cursor.close()


# ── Session factory ───────────────────────────────────────────────────────────

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """Yield a managed SQLAlchemy session with automatic commit/rollback."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ── Migration helpers ─────────────────────────────────────────────────────────

_MIGRATION_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS schema_migration (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version     TEXT    NOT NULL UNIQUE,
    applied_at  INTEGER NOT NULL  -- Unix epoch UTC
)
"""

_MIGRATION_V2_VERSION = "v2_candle_table_2024"

# ── Migration v3: incremental_update_log ──────────────────────────────────────

_MIGRATION_V3_VERSION = "v3_incremental_update_log_2025"

# ── Migration v4: candle_integrity_report ─────────────────────────────────────

_MIGRATION_V4_VERSION = "v4_integrity_report_2025"

_MIGRATION_V4_SQL = """
CREATE TABLE IF NOT EXISTS candle_integrity_report (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                   TEXT    NOT NULL,
    timeframe                TEXT    NOT NULL,
    run_at                   INTEGER NOT NULL,
    scan_mode                TEXT    NOT NULL DEFAULT 'full',
    scanned_from             INTEGER,
    scanned_to               INTEGER,
    candles_scanned          INTEGER NOT NULL DEFAULT 0,
    gaps_found               INTEGER NOT NULL DEFAULT 0,
    gaps_total_missing       INTEGER NOT NULL DEFAULT 0,
    ohlcv_anomalies_found    INTEGER NOT NULL DEFAULT 0,
    partial_candles_found    INTEGER NOT NULL DEFAULT 0,
    monotonicity_violations  INTEGER NOT NULL DEFAULT 0,
    dataset_stale            INTEGER NOT NULL DEFAULT 0,
    stale_by_periods         INTEGER NOT NULL DEFAULT 0,
    gaps_repaired            INTEGER NOT NULL DEFAULT 0,
    candles_repair_inserted  INTEGER NOT NULL DEFAULT 0,
    ohlcv_anomalies_repaired INTEGER NOT NULL DEFAULT 0,
    partial_candles_removed  INTEGER NOT NULL DEFAULT 0,
    health                   TEXT    NOT NULL DEFAULT 'clean',
    duration_ms              INTEGER NOT NULL DEFAULT 0,
    error                    TEXT
);
CREATE INDEX IF NOT EXISTS idx_integrity_sym_tf_run
    ON candle_integrity_report (symbol, timeframe, run_at);
"""


def _migrate_v4_integrity_report(conn) -> None:
    """Create candle_integrity_report table if not already present.

    Idempotent: guarded by schema_migration version string.
    The table is also created by create_all() via the ORM, so the SQL here
    is a no-op after the first startup — but it ensures the table exists on
    any database that skipped the ORM path.
    """
    conn.execute(text(_MIGRATION_TABLE_DDL))
    conn.commit()

    row = conn.execute(
        text("SELECT version FROM schema_migration WHERE version = :v"),
        {"v": _MIGRATION_V4_VERSION},
    ).fetchone()
    if row:
        logger.debug("Migration %s already applied — skipping", _MIGRATION_V4_VERSION)
        return

    logger.info("Applying migration %s — creating candle_integrity_report …", _MIGRATION_V4_VERSION)
    try:
        for stmt in _MIGRATION_V4_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--"):
                conn.execute(text(stmt))
        conn.commit()
        logger.info("Migration %s applied successfully", _MIGRATION_V4_VERSION)
    except Exception:
        conn.rollback()
        logger.exception("Migration %s failed", _MIGRATION_V4_VERSION)
        raise

    conn.execute(
        text(
            "INSERT OR IGNORE INTO schema_migration (version, applied_at) "
            "VALUES (:v, CAST(strftime('%s', 'now') AS INTEGER))"
        ),
        {"v": _MIGRATION_V4_VERSION},
    )
    conn.commit()

_MIGRATION_V3_SQL = """
CREATE TABLE IF NOT EXISTS incremental_update_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT    NOT NULL,
    timeframe           TEXT    NOT NULL,
    run_at              INTEGER NOT NULL,
    status              TEXT    NOT NULL,
    candles_fetched     INTEGER NOT NULL DEFAULT 0,
    candles_inserted    INTEGER NOT NULL DEFAULT 0,
    gaps_found          INTEGER NOT NULL DEFAULT 0,
    gaps_filled         INTEGER NOT NULL DEFAULT 0,
    candles_gap_filled  INTEGER NOT NULL DEFAULT 0,
    duration_ms         INTEGER NOT NULL DEFAULT 0,
    error               TEXT
);
CREATE INDEX IF NOT EXISTS idx_incr_log_sym_tf_run
    ON incremental_update_log (symbol, timeframe, run_at);
"""


def _migrate_v3_incremental_log(conn) -> None:
    """Create incremental_update_log table if not already present.

    Idempotent: guarded by schema_migration version string so it runs exactly
    once even if initialize_database() is called on every startup.
    """
    conn.execute(text(_MIGRATION_TABLE_DDL))
    conn.commit()

    row = conn.execute(
        text("SELECT version FROM schema_migration WHERE version = :v"),
        {"v": _MIGRATION_V3_VERSION},
    ).fetchone()
    if row:
        logger.debug("Migration %s already applied — skipping", _MIGRATION_V3_VERSION)
        return

    logger.info("Applying migration %s — creating incremental_update_log table …", _MIGRATION_V3_VERSION)
    try:
        for stmt in _MIGRATION_V3_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--"):
                conn.execute(text(stmt))
        conn.commit()
        logger.info("Migration %s applied successfully", _MIGRATION_V3_VERSION)
    except Exception:
        conn.rollback()
        logger.exception("Migration %s failed", _MIGRATION_V3_VERSION)
        raise

    conn.execute(
        text(
            "INSERT OR IGNORE INTO schema_migration (version, applied_at) "
            "VALUES (:v, CAST(strftime('%s', 'now') AS INTEGER))"
        ),
        {"v": _MIGRATION_V3_VERSION},
    )
    conn.commit()

_MIGRATION_V2_SQL = """
-- Rename legacy table (keeps data safe, no DROP)
ALTER TABLE ohlcv_cache RENAME TO ohlcv_cache_v1_backup;

-- Copy rows from legacy table into the new Candle table.
-- strftime('%s', ...) converts ISO-8601 TEXT to Unix epoch INTEGER.
-- INSERT OR IGNORE skips duplicates if the migration is somehow re-triggered.
INSERT OR IGNORE INTO candle (symbol, timeframe, open_time, open, high, low, close, volume, fetched_at)
SELECT
    asset                                          AS symbol,
    timeframe,
    CAST(strftime('%s', timestamp)   AS INTEGER)   AS open_time,
    open, high, low, close, volume,
    CAST(strftime('%s', fetched_at)  AS INTEGER)   AS fetched_at
FROM ohlcv_cache_v1_backup
WHERE timestamp IS NOT NULL
  AND fetched_at IS NOT NULL;
"""


def _migrate_legacy_ohlcv_cache(conn) -> None:
    """One-shot migration from ohlcv_cache → candle.

    Safe to call on every startup: the schema_migration guard ensures the
    migration SQL is executed exactly once.
    """
    conn.execute(text(_MIGRATION_TABLE_DDL))
    conn.commit()

    # Check if this migration has already run
    row = conn.execute(
        text("SELECT version FROM schema_migration WHERE version = :v"),
        {"v": _MIGRATION_V2_VERSION},
    ).fetchone()
    if row:
        logger.debug("Migration %s already applied — skipping", _MIGRATION_V2_VERSION)
        return

    # Check whether the legacy table actually exists
    legacy_exists = conn.execute(
        text(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='ohlcv_cache'"
        )
    ).fetchone()

    if legacy_exists:
        logger.info("Migrating legacy ohlcv_cache → candle …")
        try:
            # SQLite doesn't support multi-statement text() calls via execute();
            # run each statement individually.
            for stmt in _MIGRATION_V2_SQL.strip().split(";"):
                stmt = stmt.strip()
                if stmt and not stmt.startswith("--"):
                    conn.execute(text(stmt))
            conn.commit()
            migrated = conn.execute(text("SELECT COUNT(*) FROM candle")).fetchone()[0]
            logger.info("Migration complete — %d rows in candle table", migrated)
        except Exception:
            conn.rollback()
            logger.exception("Migration failed — legacy table left intact")
            raise
    else:
        logger.debug("No legacy ohlcv_cache table found — migration not needed")

    # Record that this migration is done
    conn.execute(
        text(
            "INSERT OR IGNORE INTO schema_migration (version, applied_at) "
            "VALUES (:v, CAST(strftime('%s', 'now') AS INTEGER))"
        ),
        {"v": _MIGRATION_V2_VERSION},
    )
    conn.commit()


# ── Public initialisation entry-point ─────────────────────────────────────────

def initialize_database() -> None:
    """Create all tables, run migrations, and verify connectivity.

    Called once at application startup from ``main.py``.
    Idempotent — safe to call on every startup.
    """
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)

    # Import models to register them with Base.metadata before create_all.
    import app.data.models  # noqa: F401

    # Create any tables that do not yet exist (skips existing ones).
    Base.metadata.create_all(bind=engine)

    # Run the one-shot migrations if needed.
    with engine.connect() as conn:
        _migrate_legacy_ohlcv_cache(conn)
        _migrate_v3_incremental_log(conn)
        _migrate_v4_integrity_report(conn)

    # Health check.
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    logger.info("Database initialised at %s", settings.db_path)
