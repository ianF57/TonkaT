-- =============================================================================
-- Migration v3: incremental_update_log
-- =============================================================================
-- Purpose : Create the audit table for incremental update runs.
--           Records every invocation of run_incremental_update() so operators
--           can inspect update history, detect persistent gaps, and audit API
--           call volumes.
--
-- Safety  : CREATE TABLE IF NOT EXISTS — fully idempotent.
--           Guarded by schema_migration version "v3_incremental_update_log_2025"
--           so the automated path in database.py runs it exactly once.
--
-- This file is provided for reference and manual re-runs.
-- Automated execution is handled by database.py::_migrate_v3_incremental_log.
-- =============================================================================

-- Step 1: Create the migration-tracking table (idempotent, shared with v2)
CREATE TABLE IF NOT EXISTS schema_migration (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version     TEXT    NOT NULL UNIQUE,
    applied_at  INTEGER NOT NULL  -- Unix epoch UTC
);

-- Step 2: Create the incremental update log table
CREATE TABLE IF NOT EXISTS incremental_update_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT    NOT NULL,
    timeframe           TEXT    NOT NULL,
    run_at              INTEGER NOT NULL,   -- Unix epoch UTC
    status              TEXT    NOT NULL,   -- updated|up_to_date|error|no_backfill
    candles_fetched     INTEGER NOT NULL DEFAULT 0,
    candles_inserted    INTEGER NOT NULL DEFAULT 0,
    gaps_found          INTEGER NOT NULL DEFAULT 0,
    gaps_filled         INTEGER NOT NULL DEFAULT 0,
    candles_gap_filled  INTEGER NOT NULL DEFAULT 0,
    duration_ms         INTEGER NOT NULL DEFAULT 0,
    error               TEXT                        -- NULL on success
);

-- Step 3: Index for per-symbol history queries (avoids full table scan)
-- Covers: WHERE symbol=? AND timeframe=? ORDER BY run_at DESC
CREATE INDEX IF NOT EXISTS idx_incr_log_sym_tf_run
    ON incremental_update_log (symbol, timeframe, run_at);

-- Step 4: Record migration completion
INSERT OR IGNORE INTO schema_migration (version, applied_at)
VALUES ('v3_incremental_update_log_2025', CAST(strftime('%s', 'now') AS INTEGER));
