# Data Access Layer — Architecture & Reference

## Architecture Diagram

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                           CONSUMER LAYER                                    ║
║                                                                              ║
║  ┌─────────────────┐  ┌─────────────────┐  ┌────────────────────────────┐   ║
║  │   Backtester    │  │  SignalManager  │  │    HistoricalReplay        │   ║
║  │ (backtester.py) │  │(signal_manager) │  │    (replay.py)             │   ║
║  │                 │  │                 │  │                            │   ║
║  │ get_candles()   │  │ get_candles()   │  │ get_candles_range()        │   ║
║  │ iter_candles()  │  │                 │  │ _split_at_cutoff()         │   ║
║  └────────┬────────┘  └────────┬────────┘  └────────────┬───────────────┘   ║
║           │                   │                          │                  ║
╚═══════════╪═══════════════════╪══════════════════════════╪══════════════════╝
            │    DB-ONLY (never API)                       │
╔═══════════╪═══════════════════╪══════════════════════════╪══════════════════╗
║           └───────────────────┼──────────────────────────┘                 ║
║                               ▼                                             ║
║              ┌────────────────────────────────────┐                         ║
║              │      HistoricalDataStore            │  DATA ACCESS LAYER     ║
║              │      (historical_store.py)          │                         ║
║              │                                     │                         ║
║              │  get_candles(sym, tf, limit)        │  last N rows, ASC       ║
║              │  get_candles_range(sym, tf, s, e)  │  date-bounded slice     ║
║              │  iter_candles(sym, tf, chunk=5000) │  streaming generator    ║
║              │  has_sufficient_data(sym, tf, min)  │  O(1) preflight         ║
║              │  get_stats(sym, tf)                 │  MIN/MAX/COUNT meta     ║
║              │  list_partitions()                  │  all stored symbols     ║
║              └────────────────────────────────────┘                         ║
║                               │                                             ║
╚═══════════════════════════════╪═════════════════════════════════════════════╝
                                │
╔═══════════════════════════════╪═════════════════════════════════════════════╗
║                               ▼                                             ║
║              ┌────────────────────────────────────┐                         ║
║              │   SQLite candle table              │  STORAGE LAYER          ║
║              │   WITHOUT ROWID                    │                         ║
║              │   Clustered B-tree on:             │                         ║
║              │   PRIMARY KEY(symbol, timeframe,   │                         ║
║              │               open_time)           │                         ║
║              │                                    │                         ║
║              │   + idx_freshness(symbol,          │                         ║
║              │                   timeframe,       │                         ║
║              │                   fetched_at)      │                         ║
║              └────────────────────────────────────┘                         ║
╚═════════════════════════════════════════════════════════════════════════════╝

  ┌──────────────────────────────────────────────────────────────┐
  │                    LIVE DATA PATH (separate)                 │
  │                                                              │
  │  GET /api/data/{asset}                                       │
  │     └── DataManager.get_ohlcv()                             │
  │              ├── cache hit? → return DB rows                 │
  │              └── cache miss? → provider API → store → return │
  └──────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────┐
  │                    WRITE PATH (separate)                     │
  │                                                              │
  │  BackfillEngine    → INSERT OR IGNORE → candle table         │
  │  IncrementalUpdater→ INSERT OR IGNORE → candle table         │
  │                                                              │
  │  All writes go through the same clustered B-tree.            │
  │  WAL mode means reads never block on writes.                 │
  └──────────────────────────────────────────────────────────────┘
```

---

## API Endpoints Added

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/store/partitions` | List all stored (symbol, timeframe) pairs |
| `GET` | `/api/store/stats` | Metadata for every partition |
| `GET` | `/api/store/stats/{symbol}/{tf}` | Metadata for one partition |
| `GET` | `/api/store/ready/{symbol}/{tf}` | Fast readiness check (< 1 ms) |
| `GET` | `/api/store/candles/{symbol}/{tf}` | DB-only candle retrieval |

---

## Query Examples

### 1 — Signal generation (last 500 candles, chronological)
```sql
-- Phase 1: DESC LIMIT (B-tree walk backwards)
SELECT open_time,open,high,low,close,volume
FROM   candle
WHERE  symbol = 'BTCUSDT' AND timeframe = '1h'
   AND open_time <= :now_epoch
ORDER  BY open_time DESC
LIMIT  500

-- Phase 2: re-sort ASC for signal engines
SELECT * FROM (...above...) ORDER BY open_time ASC

-- Plan: SEARCH candle USING PRIMARY KEY (symbol=? AND timeframe=? AND open_time<?)
-- Cost: O(log n + 500)   Benchmark: 0.53 ms at 300k rows
```

### 2 — Replay date range (exact historical window)
```sql
SELECT open_time,open,high,low,close,volume
FROM   candle
WHERE  symbol    = 'BTCUSDT'
  AND  timeframe = '1h'
  AND  open_time BETWEEN :start_epoch AND :end_epoch
ORDER  BY open_time ASC

-- Plan: SEARCH candle USING PRIMARY KEY (symbol=? AND timeframe=? AND open_time>? AND open_time<?)
-- Cost: O(log n + k)   Benchmark: 2.9 ms for 1-year range at 300k rows
```

### 3 — Chunked streaming (large backtests, memory-safe)
```sql
-- Repeated with cursor advancing to last open_time of each chunk
SELECT open_time,open,high,low,close,volume
FROM   candle
WHERE  symbol    = 'BTCUSDT'
  AND  timeframe = '1h'
  AND  open_time >  :cursor
  AND  open_time <= :end_epoch
ORDER  BY open_time ASC
LIMIT  5000

-- Cost: O(k / chunk_size) queries, each O(log n + chunk_size)
-- Benchmark: 60k rows in 20.8 ms total, peak memory = 5000 rows × ~100 B = 500 KB
```

### 4 — Fast preflight check (before any signal/backtest call)
```sql
-- Early-exit COUNT — stops scanning after finding the 60th row
SELECT COUNT(*)
FROM   (SELECT 1 FROM candle WHERE symbol=? AND timeframe=? LIMIT 60)

-- Benchmark: 5 µs regardless of partition size
-- 500× faster than COUNT(*) at 74k rows
```

### 5 — Partition metadata (dashboard health display)
```sql
SELECT COUNT(*), MIN(open_time), MAX(open_time)
FROM   candle
WHERE  symbol = 'BTCUSDT' AND timeframe = '1h'

-- Plan: SEARCH candle USING COVERING INDEX idx_freshness
-- Benchmark: 5.9 ms at 300k rows
```

---

## Performance Optimization Plan

### Implemented (active)

| Technique | Effect | Benchmark |
|-----------|--------|-----------|
| **WITHOUT ROWID** — clustered B-tree on `(symbol, timeframe, open_time)` | Eliminates rowid lookup + random I/O for all range scans | 1.28× speedup vs ROWID table |
| **DESC LIMIT + ASC subquery** — two-phase fetch | `get_candles` never loads more than `limit` rows; sort is O(limit) not O(n) | 0.53 ms for 500 candles @ 300k rows |
| **Cursor pagination** — `open_time > :cursor LIMIT chunk` | `iter_candles` memory is O(chunk_size) not O(n); no offset skipping | 20.8 ms for 60k rows, 500 KB peak |
| **COUNT(LIMIT N)** — early-exit existence check | `has_sufficient_data` short-circuits after N rows found | 5 µs vs 2.6 ms for COUNT(*) |
| **WAL mode** — concurrent reads during writes | Backfill writes never block backtest reads | Zero reader stalls |
| **64 MB page cache** — `PRAGMA cache_size=-65536` | Hot B-tree pages stay in memory across repeated backtest runs | Repeated queries: < 0.1 ms |
| **256 MB mmap** — `PRAGMA mmap_size=268435456` | Large sequential scans bypass SQLite read buffer | Linear scan speedup ~1.3× |
| **MEMORY temp_store** — `PRAGMA temp_store=MEMORY` | ORDER BY temporaries in RAM, not temp files | Relevant for re-sort subquery |

### Recommended next steps

| Improvement | When to apply | Expected gain |
|-------------|---------------|---------------|
| **Application-level LRU cache** — `functools.lru_cache` or `cachetools.TTLCache` keyed on `(symbol, tf, limit)` | When the same backtest runs > 10×/min (e.g. parameter sweep UI) | Eliminate DB round-trip for repeated identical queries: ~0.5 ms → ~0.01 ms |
| **Materialized summary table** — pre-aggregate daily/weekly candles from 1h data | When adding multi-timeframe signal correlation | Avoid recomputing OHLCV aggregates in every backtest |
| **Parallel partition reads** — `asyncio.gather` over multiple symbols | When ranking signals across all 6 crypto symbols simultaneously | 6× throughput for signal-scan endpoint |
| **Write-ahead checkpoint tuning** — `PRAGMA wal_autocheckpoint=1000` | When the WAL file grows > 50 MB during heavy backfill | Keeps mmap-able DB file compact |
| **Read replica** — copy DB file to `/dev/shm` (RAM disk) | When deployed on a server with > 4 GB RAM | Full partition scans: disk latency → 0 |

---

## Potential Scaling Improvements

### SQLite (current, up to ~50 M rows / ~5 GB)

The WITHOUT ROWID B-tree with the current indexing strategy handles:
- **~10 M rows**: all queries remain under 50 ms. `iter_candles` streams safely.
- **~50 M rows**: consider partitioning the DB file per symbol or per year.
- **Practical ceiling**: 1 SQLite file performs well up to ~5 GB on SSD. Beyond that, split by symbol into separate files and use a connection pool keyed by symbol.

### Migration path to PostgreSQL / DuckDB (> 50 M rows)

| Component | Change required |
|-----------|-----------------|
| `HistoricalDataStore` | Replace `sqlite_insert().on_conflict_do_nothing()` with `INSERT … ON CONFLICT DO NOTHING` (same SQL) |
| `historical_store.py` | Remove `PRAGMA` calls; PostgreSQL handles these via server config |
| Query logic | All queries use standard SQL; no SQLite-specific syntax |
| `database.py` | Change `create_engine("sqlite://…")` to `create_engine("postgresql://…")` |
| `WITHOUT ROWID` | Replace with PostgreSQL clustered index: `CLUSTER candle USING candle_pkey` |

**DuckDB** is particularly suitable for analytical (backtest) workloads: columnar storage gives 10–50× speedup for aggregations over millions of rows. The `HistoricalDataStore` interface is the abstraction boundary — swap the engine, keep the interface.

### Horizontal scaling (multiple workers)

WAL mode already supports concurrent reads from multiple processes. To scale across machines:
1. Promote SQLite to PostgreSQL (read replicas for backtest workers).
2. Keep `HistoricalDataStore` interface identical — consumers never change.
3. Add Redis TTL cache in front of `get_candles()` for repeated signal-tab queries.

---

## Error Model

| Error | HTTP status | Meaning | Resolution |
|-------|-------------|---------|------------|
| `InsufficientDataError` | 422 | DB has fewer candles than requested | Run `POST /api/backfill/run` |
| `ValueError` | 400 | Bad asset/timeframe/date format | Fix request parameters |
| `RuntimeError` | 502 | Unexpected DB error | Check logs |

The `InsufficientDataError` carries `symbol`, `timeframe`, `available`, and `requested` attributes so the UI can display a precise message: *"42 of 500 required candles stored — run backfill."*
