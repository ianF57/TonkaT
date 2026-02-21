from __future__ import annotations

from datetime import UTC, datetime
import logging

import httpx

from app.data.base_provider import BaseDataProvider, OHLCVPoint

logger = logging.getLogger(__name__)


class YahooChartProvider(BaseDataProvider):
    """Common Yahoo Finance chart API parser for OHLCV data."""

    _base_url = "https://query1.finance.yahoo.com/v8/finance/chart"
    _timeframe_map = {
        "1m": "1m",
        "5m": "5m",
        "1h": "60m",
        "1d": "1d",
        "1w": "1wk",
    }
    _range_map = {
        "1m": "7d",
        "5m": "30d",
        "1h": "730d",
        "1d": "10y",
        "1w": "10y",
    }

    async def _fetch_chart(self, symbol: str, timeframe: str) -> list[OHLCVPoint]:
        self.validate_timeframe(timeframe)
        params = {
            "interval": self._timeframe_map[timeframe],
            "range": self._range_map[timeframe],
            "includePrePost": "false",
            "events": "div,splits",
        }

        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(f"{self._base_url}/{symbol}", params=params)
            response.raise_for_status()
            payload = response.json()

        result = payload.get("chart", {}).get("result")
        if not result:
            raise ValueError(f"No market data returned for symbol '{symbol}'")

        chart = result[0]
        timestamps = chart.get("timestamp") or []
        quote = chart.get("indicators", {}).get("quote", [{}])[0]
        opens = quote.get("open") or []
        highs = quote.get("high") or []
        lows = quote.get("low") or []
        closes = quote.get("close") or []
        volumes = quote.get("volume") or []

        points: list[OHLCVPoint] = []
        for idx, ts in enumerate(timestamps):
            o = opens[idx] if idx < len(opens) else None
            h = highs[idx] if idx < len(highs) else None
            l = lows[idx] if idx < len(lows) else None
            c = closes[idx] if idx < len(closes) else None
            v = volumes[idx] if idx < len(volumes) else 0
            if None in (o, h, l, c):
                continue
            points.append(
                {
                    "timestamp": datetime.fromtimestamp(ts, tz=UTC),
                    "open": float(o),
                    "high": float(h),
                    "low": float(l),
                    "close": float(c),
                    "volume": float(v or 0.0),
                }
            )
        return points
