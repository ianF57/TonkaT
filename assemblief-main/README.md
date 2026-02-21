# Assemblief FastAPI Starter

A local-first FastAPI application skeleton with SQLite initialization, modular package layout, and a server-rendered dashboard.

- Interactive dashboard controls for asset/timeframe selection, regime fetch/refresh, and chart summary actions

## Quick start

```bash
pip install -r requirements.txt
python main.py
```

When started via `python main.py` the app will:

1. Configure logging
2. Initialize SQLite at `app/data/assemblief.db`
3. Start a Uvicorn-powered FastAPI server
4. Attempt to open your default browser at `http://127.0.0.1:8000/`

## Routes

- `/` – institutional style dashboard (Jinja2 template)
- `/health` – JSON health status
- `/api/data/{asset}?timeframe=1h` – unified OHLCV market data with SQLite caching
- `/api/regime/{asset}?timeframe=1h` – regime classification with confidence score and historical distribution
- `/api/signals/{asset}?timeframe=1h` – generate and rank candidate strategy signals

## Unified market data

Supported timeframes:
- `1m`, `5m`, `1h`, `1d`, `1w`

Supported providers:
- `crypto` → Binance public API
- `forex` → Yahoo Finance public chart API
- `futures` → Yahoo Finance public chart API

Asset format examples:

- `crypto:BTCUSDT`
- `forex:EURUSD`
- `futures:ES`

Response includes `source`:
- `provider` when fetched from upstream
- `cache` when served from local SQLite

## Regime engine

Regime classifier combines:
- rolling volatility
- ADX
- RSI distribution profile
- volatility clustering
- optional Hurst exponent proxy

Output schema:

```json
{
  "current_regime": "trending",
  "confidence_score": 78.2,
  "historical_distribution": {
    "trending": 42.5,
    "ranging": 21.0,
    "high_volatility": 10.0,
    "low_volatility": 12.5,
    "momentum_breakout": 9.0,
    "mean_reversion": 5.0
  }
}
```

## Configuration

Runtime config is managed through environment variables in `config.py`:

- `APP_NAME`
- `APP_HOST`
- `APP_PORT`
- `LOG_LEVEL`
- `DB_PATH`
