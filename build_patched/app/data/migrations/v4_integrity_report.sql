-- Migration v4: candle_integrity_report
-- Applied automatically by database.py:_migrate_v4_integrity_report()
-- This file is a human-readable reference — do NOT execute directly.
-- The migration guard in schema_migration prevents double-application.

CREATE TABLE IF NOT EXISTS candle_integrity_report (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                   TEXT    NOT NULL,
    timeframe                TEXT    NOT NULL,
    run_at                   INTEGER NOT NULL,       -- epoch UTC

    scan_mode                TEXT    NOT NULL DEFAULT 'full',   -- 'full' | 'incremental'

    -- Scan boundaries (NULL when the partition is empty)
    scanned_from             INTEGER,               -- earliest open_time checked (epoch)
    scanned_to               INTEGER,               -- latest  open_time checked (epoch)
    candles_scanned          INTEGER NOT NULL DEFAULT 0,

    -- Issues found
    gaps_found               INTEGER NOT NULL DEFAULT 0,
    gaps_total_missing       INTEGER NOT NULL DEFAULT 0,   -- sum of missing_candles across all gaps
    ohlcv_anomalies_found    INTEGER NOT NULL DEFAULT 0,
    partial_candles_found    INTEGER NOT NULL DEFAULT 0,
    monotonicity_violations  INTEGER NOT NULL DEFAULT 0,   -- should always be 0 (PK guarantee)
    dataset_stale            INTEGER NOT NULL DEFAULT 0,   -- boolean (0/1)
    stale_by_periods         INTEGER NOT NULL DEFAULT 0,   -- how many tf_secs periods behind

    -- Repair outcomes
    gaps_repaired            INTEGER NOT NULL DEFAULT 0,
    candles_repair_inserted  INTEGER NOT NULL DEFAULT 0,
    ohlcv_anomalies_repaired INTEGER NOT NULL DEFAULT 0,
    partial_candles_removed  INTEGER NOT NULL DEFAULT 0,

    -- Overall classification
    health                   TEXT    NOT NULL DEFAULT 'clean',  -- 'clean'|'repaired'|'issues_remain'|'error'
    duration_ms              INTEGER NOT NULL DEFAULT 0,
    error                    TEXT                               -- NULL on success
);

-- Supports per-pair history and checkpoint lookup (MAX(run_at) per sym/tf)
CREATE INDEX IF NOT EXISTS idx_integrity_sym_tf_run
    ON candle_integrity_report (symbol, timeframe, run_at);

-- Record that this migration has been applied
INSERT OR IGNORE INTO schema_migration (version, applied_at)
VALUES ('v4_integrity_report_2025', CAST(strftime('%s', 'now') AS INTEGER));
