from __future__ import annotations

from datetime import UTC, date, datetime, time
from typing import Any

from app.backtesting.backtester import Backtester
from app.data.data_manager import DataManager
from app.regime.regime_classifier import RegimeClassifier
from app.scoring.confidence import confidence_score


class HistoricalReplay:
    """Historical replay mode for regime, ranking, and trade outcome transparency."""

    def __init__(self) -> None:
        self.data_manager = DataManager()
        self.backtester = Backtester()
        self.regime_classifier = RegimeClassifier()
        self.signal_ids = ["trend_v1", "mean_reversion_v1", "breakout_v1"]

    async def replay(self, asset: str, timeframe: str, replay_date: str) -> dict[str, Any]:
        target = date.fromisoformat(replay_date)
        all_data_response = await self.data_manager.get_ohlcv(asset=asset, timeframe=timeframe)
        candles = all_data_response["data"]

        historical, forward = self._split_by_date(candles, target)
        if len(historical) < 60:
            raise ValueError("Insufficient candles before selected date. Choose a later date.")

        regime_snapshot = self.regime_classifier.classify(historical)
        ranked = await self._rank_historical(asset=all_data_response["asset"], timeframe=timeframe, candles=historical, regime=regime_snapshot.current_regime)
        if not ranked:
            raise ValueError("No valid historical signals for selected replay date.")

        top = ranked[0]
        trade_outcome = self._simulate_trade_outcome(top["signal"], historical, forward)

        return {
            "asset": all_data_response["asset"],
            "timeframe": timeframe,
            "date": replay_date,
            "regime": {
                "current_regime": regime_snapshot.current_regime,
                "confidence_score": regime_snapshot.confidence_score,
                "historical_distribution": regime_snapshot.historical_distribution,
            },
            "top_signal": top,
            "trade_outcome": trade_outcome,
            "full_metrics": top["full_metrics"],
        }

    def _split_by_date(self, candles: list[dict[str, Any]], target: date) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        cutoff = datetime.combine(target, time(23, 59, 59), tzinfo=UTC)
        historical: list[dict[str, Any]] = []
        forward: list[dict[str, Any]] = []
        for candle in candles:
            ts = candle.get("timestamp")
            parsed = self._parse_timestamp(ts)
            if parsed <= cutoff:
                historical.append(candle)
            else:
                forward.append(candle)
        return historical, forward

    def _parse_timestamp(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=UTC)
        if isinstance(value, str):
            sanitized = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(sanitized)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        raise ValueError("Unsupported candle timestamp format")

    async def _rank_historical(self, asset: str, timeframe: str, candles: list[dict[str, Any]], regime: str) -> list[dict[str, Any]]:
        ranked: list[dict[str, Any]] = []
        for signal in self.signal_ids:
            backtest = await self.backtester.run(asset=asset, timeframe=timeframe, signal_name=signal, candles_override=candles)
            metrics = backtest["metrics"]
            oos = backtest["out_of_sample_metrics"]
            robustness = backtest["robustness"]

            regime_align = 95.0 if regime in {
                "trending", "momentum_breakout", "high_volatility"
            } and signal in {"trend_v1", "breakout_v1"} else 95.0 if regime in {
                "ranging", "mean_reversion", "low_volatility"
            } and signal == "mean_reversion_v1" else 40.0

            conf = confidence_score(
                out_sample_performance=max(0.0, min(100.0, oos["cagr"] * 0.6 + oos["sharpe"] * 10.0)),
                cross_asset_stability=max(0.0, min(100.0, float(robustness["monte_carlo_score"]))),
                cross_time_stability=max(0.0, min(100.0, float(robustness["robustness_score"]))),
                regime_alignment=regime_align,
                parameter_robustness=float(robustness["sensitivity_score"]),
                drawdown_control=max(0.0, 100.0 - float(metrics["max_drawdown"])),
                in_sample_cagr=float(metrics["cagr"]),
                out_sample_cagr=float(oos["cagr"]),
            )

            ranked.append(
                {
                    "signal": signal,
                    "suggested_direction": self._direction(signal),
                    "expected_return_range": self._expected_return_range(metrics),
                    "expected_drawdown": round(float(metrics["max_drawdown"]), 2),
                    "confidence_score": conf,
                    "full_metrics": {
                        "metrics": metrics,
                        "out_of_sample_metrics": oos,
                        "robustness": robustness,
                    },
                }
            )

        ranked.sort(key=lambda item: item["confidence_score"], reverse=True)
        return ranked

    def _direction(self, signal: str) -> str:
        return "counter-trend" if signal == "mean_reversion_v1" else "trend-aligned"

    def _expected_return_range(self, metrics: dict[str, Any]) -> str:
        cagr = float(metrics.get("cagr", 0.0))
        low = round(max(-30.0, cagr * 0.5), 2)
        high = round(min(120.0, cagr * 1.3 + 5), 2)
        return f"{low}% to {high}%"

    def _simulate_trade_outcome(self, signal: str, historical: list[dict[str, Any]], forward: list[dict[str, Any]]) -> dict[str, Any]:
        if not forward:
            return {
                "bars_held": 0,
                "entry_price": float(historical[-1]["close"]),
                "exit_price": float(historical[-1]["close"]),
                "return_pct": 0.0,
                "status": "No forward candles after selected date.",
            }

        entry = float(historical[-1]["close"])
        holding = forward[: min(10, len(forward))]
        exit_price = float(holding[-1]["close"])

        trend_bias = 1.0
        if signal == "mean_reversion_v1":
            recent = [float(c["close"]) for c in historical[-20:]]
            short = sum(recent[-5:]) / min(5, len(recent))
            long = sum(recent) / len(recent)
            trend_bias = -1.0 if short >= long else 1.0

        raw = ((exit_price - entry) / entry) * 100 if entry else 0.0
        signed = raw * trend_bias
        return {
            "bars_held": len(holding),
            "entry_price": round(entry, 6),
            "exit_price": round(exit_price, 6),
            "return_pct": round(signed, 4),
            "status": "simulated",
        }
