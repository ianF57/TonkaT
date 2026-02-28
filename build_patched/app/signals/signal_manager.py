"""Signal manager — DB-only data access via HistoricalDataStore."""
from __future__ import annotations

from typing import Any

from app.data.historical_store import HistoricalDataStore
from app.signals.breakout_v1 import BreakoutV1
from app.signals.mean_reversion_v1 import MeanReversionV1
from app.signals.trend_signal_v1 import TrendSignalV1


class SignalManager:
    def __init__(self) -> None:
        self.store = HistoricalDataStore()
        self.strategies = {
            "trend_v1": TrendSignalV1(),
            "mean_reversion_v1": MeanReversionV1(),
            "breakout_v1": BreakoutV1(),
        }

    async def generate_signals(self, asset: str, timeframe: str = "1h") -> dict[str, Any]:
        candles = self.store.get_candles(asset.strip().upper(), timeframe, limit=250)
        ranked: list[dict[str, Any]] = []
        for name, strategy in self.strategies.items():
            sig = strategy.generate(candles)
            action = sig["action"]
            ranked.append({
                "strategy_label": name,
                "version": "v1",
                "direction": "long" if action == "buy" else ("short" if action == "sell" else "flat"),
                "entry_rule": "model_signal",
                "exit_rule": "signal_flip_or_risk",
                "parameters": {},
                "performance_score": round(float(sig["confidence"]) * 100, 2),
                "metadata": sig.get("metadata", {}),
            })

        ranked.sort(key=lambda row: row["performance_score"], reverse=True)
        return {
            "asset": asset.strip().upper(),
            "timeframe": timeframe,
            "source": "database",
            "candles_used": len(candles),
            "signals": ranked,
            "signal_count": len(ranked),
        }
