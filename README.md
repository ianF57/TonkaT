# 🌈 Assemblief Trader — Live Edition

A full-stack trading dashboard with **universal live price validation** across crypto, stocks, forex, commodities, and indices.

## ▶ How to Run

### Mac / Linux
```bash
./run.sh
```

### Windows
Double-click **`run.bat`**

That's it. The script will:
1. Check for Python 3
2. Create a virtual environment automatically
3. Install all dependencies
4. Start the server and open your browser at **http://127.0.0.1:8000**

---

## Requirements
- **Python 3.9+** — download from https://python.org if needed
- Internet connection (for live market data)

## Features
- 🛡️ **Universal Live Price Validator** — 29 assets across 5 categories, validated every 30s from Yahoo Finance
- 📊 **Real-time strategy scoring** — scores adjust to live market regime (Bullish / Bearish / Volatile / Ranging)
- ⚠️ **Anomaly detection** — flags >25% single-poll price moves and holds cached price
- 🔴 **Feed interruption alerts** — offline banner + exponential backoff retry after 3 failures
- ✓ **LIVE / STALE / ANOMALY / ERROR** status chips on every asset price
- 📋 **Audit log** — timestamped validation events

## Stopping
Press `Ctrl+C` in the terminal window.
