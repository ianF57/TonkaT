-- =============================================================================
-- Migration v2: ohlcv_cache → candle
-- =============================================================================
-- Purpose : Replace the v1 ohlcv_cache table (ROWID, TEXT timestamps) with the
--           production candle table (WITHOUT ROWID, INTEGER epochs).
--
-- Safety  : The v1 table is RENAMED, never dropped.  All historical data is
--           preserved in ohlcv_cache_v1_backup and can be restored manually.
--
-- This file is provided for reference and manual re-runs.
-- Automated execution is handled by database.py::_migrate_legacy_ohlcv_cache.
-- =============================================================================

-- Step 1: Create the migration-tracking table (idempotent)
CREATE TABLE IF NOT EXISTS schema_migration (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version     TEXT    NOT NULL UNIQUE,
    applied_at  INTEGER NOT NULL  -- Unix epoch UTC
);

-- Step 2: Create the new production table (if not exists)
CREATE TABLE IF NOT EXISTS candle (
    symbol      TEXT    NOT NULL,
    timeframe   TEXT    NOT NULL,
    open_time   INTEGER NOT NULL,
    open        REAL    NOT NULL,
    high        REAL    NOT NULL,
    low         REAL    NOT NULL,
    close       REAL    NOT NULL,
    volume      REAL    NOT NULL DEFAULT 0,
    fetched_at  INTEGER NOT NULL,
    PRIMARY KEY (symbol, timeframe, open_time)
) WITHOUT ROWID;

-- Step 3: Freshness index on new table
CREATE INDEX IF NOT EXISTS idx_candle_freshness
    ON candle (symbol, timeframe, fetched_at);

-- Step 4: Rename legacy table (preserves all data)
ALTER TABLE ohlcv_cache RENAME TO ohlcv_cache_v1_backup;

-- Step 5: Copy legacy rows with type conversion
--   asset     → symbol
--   timestamp (TEXT ISO-8601) → open_time (INTEGER epoch via strftime)
--   fetched_at (TEXT ISO-8601) → fetched_at (INTEGER epoch via strftime)
INSERT OR IGNORE INTO candle (symbol, timeframe, open_time, open, high, low, close, volume, fetched_at)
SELECT
    asset                                           AS symbol,
    timeframe,
    CAST(strftime('%s', timestamp)    AS INTEGER)   AS open_time,
    open,
    high,
    low,
    close,
    volume,
    CAST(strftime('%s', fetched_at)   AS INTEGER)   AS fetched_at
FROM ohlcv_cache_v1_backup
WHERE timestamp IS NOT NULL
  AND fetched_at IS NOT NULL;

-- Step 6: Record migration completion
INSERT OR IGNORE INTO schema_migration (version, applied_at)
VALUES ('v2_candle_table_2024', CAST(strftime('%s', 'now') AS INTEGER));
