from __future__ import annotations

from typing import Any

from app.backtesting.backtester import Backtester
from app.data.data_manager import DataManager
from app.regime.regime_classifier import RegimeClassifier
from app.scoring.confidence import confidence_score


class SignalRanker:
    """Institutional ranking for signal alternatives on one asset."""

    def __init__(self) -> None:
        self.data_manager = DataManager()
        self.backtester = Backtester()
        self.regime_classifier = RegimeClassifier()
        self.signal_ids = ["trend_v1", "mean_reversion_v1", "breakout_v1"]
        self.cross_assets = ["crypto:BTCUSDT", "crypto:ETHUSDT", "forex:EURUSD"]
        self.cross_times = ["5m", "1h", "1d"]

    async def rank_asset(self, asset: str, timeframe: str = "1h") -> dict[str, Any]:
        ranked: list[dict[str, Any]] = []

        base_data = await self.data_manager.get_ohlcv(asset=asset, timeframe=timeframe)
        regime = self.regime_classifier.classify(base_data["data"]).current_regime

        for signal in self.signal_ids:
            bt = await self.backtester.run(asset=asset, timeframe=timeframe, signal_name=signal)
            metrics = bt["metrics"]
            oos = bt["out_of_sample_metrics"]
            robust = bt["robustness"]

            out_sample_perf = max(0.0, min(100.0, oos["cagr"] * 0.6 + oos["sharpe"] * 10.0))
            cross_asset = await self._cross_asset_stability(signal, timeframe)
            cross_time = await self._cross_time_stability(asset, signal)
            regime_align = self._regime_alignment_score(signal, regime)
            robustness = float(robust["sensitivity_score"])
            dd_control = max(0.0, 100.0 - float(metrics["max_drawdown"]))

            conf = confidence_score(
                out_sample_performance=out_sample_perf,
                cross_asset_stability=cross_asset,
                cross_time_stability=cross_time,
                regime_alignment=regime_align,
                parameter_robustness=robustness,
                drawdown_control=dd_control,
                in_sample_cagr=float(metrics["cagr"]),
                out_sample_cagr=float(oos["cagr"]),
            )

            direction = self._direction_from_signal(signal)
            expected_return = self._expected_return_range(metrics)
            expected_drawdown = round(float(metrics["max_drawdown"]), 2)

            ranked.append(
                {
                    "signal": signal,
                    "suggested_direction": direction,
                    "expected_return_range": expected_return,
                    "expected_drawdown": expected_drawdown,
                    "confidence_score": conf,
                    "regime_alignment": regime_align,
                    "robustness": robust,
                }
            )

        ranked.sort(key=lambda item: item["confidence_score"], reverse=True)
        return {
            "asset": base_data["asset"],
            "timeframe": timeframe,
            "top_signals": ranked[:3],
        }

    async def _cross_asset_stability(self, signal: str, timeframe: str) -> float:
        scores: list[float] = []
        for asset in self.cross_assets:
            try:
                bt = await self.backtester.run(asset=asset, timeframe=timeframe, signal_name=signal)
                scores.append(max(0.0, float(bt["out_of_sample_metrics"]["cagr"])))
            except Exception:
                continue
        if not scores:
            return 0.0
        avg = sum(scores) / len(scores)
        spread = max(scores) - min(scores) if len(scores) > 1 else 0.0
        return max(0.0, min(100.0, avg * 2.0 + max(0.0, 25 - spread)))

    async def _cross_time_stability(self, asset: str, signal: str) -> float:
        scores: list[float] = []
        for timeframe in self.cross_times:
            try:
                bt = await self.backtester.run(asset=asset, timeframe=timeframe, signal_name=signal)
                scores.append(float(bt["out_of_sample_metrics"]["sharpe"]))
            except Exception:
                continue
        if not scores:
            return 0.0
        avg = sum(scores) / len(scores)
        spread = max(scores) - min(scores) if len(scores) > 1 else 0.0
        return max(0.0, min(100.0, avg * 20.0 + max(0.0, 20 - spread * 10.0)))

    def _regime_alignment_score(self, signal: str, regime: str) -> float:
        mapping = {
            "trend_v1": {"trending", "momentum_breakout", "high_volatility"},
            "mean_reversion_v1": {"ranging", "mean_reversion", "low_volatility"},
            "breakout_v1": {"momentum_breakout", "trending", "high_volatility"},
        }
        return 95.0 if regime in mapping.get(signal, set()) else 40.0

    def _direction_from_signal(self, signal: str) -> str:
        if signal == "mean_reversion_v1":
            return "counter-trend"
        return "trend-aligned"

    def _expected_return_range(self, metrics: dict[str, Any]) -> str:
        cagr = float(metrics.get("cagr", 0.0))
        low = round(max(-30.0, cagr * 0.5), 2)
        high = round(min(120.0, cagr * 1.3 + 5), 2)
        return f"{low}% to {high}%"
