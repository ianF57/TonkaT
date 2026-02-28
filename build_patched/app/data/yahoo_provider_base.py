"""Yahoo Finance chart API base provider.

Uses the shared ``yahoo_client`` module which handles:
  - Browser-like User-Agent / Referer headers
  - Fallback between query2 and query1 Yahoo Finance hosts
  - Exponential back-off retry on 429 / 5xx responses
  - follow_redirects support

No crumb token is required — the v8/finance/chart endpoint works without one.
"""
from __future__ import annotations

from datetime import UTC, datetime
import logging

from app.data.base_provider import BaseDataProvider, OHLCVPoint
from app.data.yahoo_client import yahoo_chart_get

logger = logging.getLogger(__name__)


class YahooChartProvider(BaseDataProvider):
    """Common Yahoo Finance chart API parser for OHLCV data."""

    _timeframe_map: dict[str, str] = {
        "1m": "1m",
        "5m": "5m",
        "1h": "60m",
        "1d": "1d",
        "1w": "1wk",
    }
    _range_map: dict[str, str] = {
        "1m": "7d",
        "5m": "30d",
        "1h": "730d",
        "1d": "10y",
        "1w": "10y",
    }

    async def _fetch_chart(self, symbol: str, timeframe: str) -> list[OHLCVPoint]:
        self.validate_timeframe(timeframe)

        params: dict[str, object] = {
            "interval":       self._timeframe_map[timeframe],
            "range":          self._range_map[timeframe],
            "includePrePost": "false",
            "events":         "div,splits",
        }

        logger.info("Fetching Yahoo chart for %s (%s)", symbol, timeframe)
        payload = await yahoo_chart_get(symbol, params)

        result = payload.get("chart", {}).get("result")
        if not result:
            err_desc = (
                (payload.get("chart", {}).get("error") or {}).get("description", "")
                or "No market data returned"
            )
            raise ValueError(f"No market data for '{symbol}': {err_desc}")

        chart      = result[0]
        timestamps: list[int] = chart.get("timestamp") or []
        quote      = chart.get("indicators", {}).get("quote", [{}])[0]
        opens      = quote.get("open")   or []
        highs      = quote.get("high")   or []
        lows       = quote.get("low")    or []
        closes     = quote.get("close")  or []
        volumes    = quote.get("volume") or []

        points: list[OHLCVPoint] = []
        for idx, ts in enumerate(timestamps):
            o = opens[idx]   if idx < len(opens)   else None
            h = highs[idx]   if idx < len(highs)   else None
            l = lows[idx]    if idx < len(lows)    else None
            c = closes[idx]  if idx < len(closes)  else None
            v = volumes[idx] if idx < len(volumes) else 0
            if None in (o, h, l, c):
                continue
            points.append(
                {
                    "timestamp": datetime.fromtimestamp(ts, tz=UTC),
                    "open":      float(o),
                    "high":      float(h),
                    "low":       float(l),
                    "close":     float(c),
                    "volume":    float(v or 0.0),
                }
            )

        if not points:
            raise ValueError(f"Yahoo Finance returned an empty candle list for '{symbol}'")

        logger.info("Fetched %d candles for %s (%s)", len(points), symbol, timeframe)
        return points
