from __future__ import annotations

from dataclasses import asdict
from typing import Any

from app.backtesting.metrics import calculate_metrics
from app.backtesting.robustness import evaluate_robustness, monte_carlo_stability, parameter_sensitivity
from app.data.data_manager import DataManager


class Backtester:
    """Backtesting engine with walk-forward and robustness checks."""

    def __init__(self) -> None:
        self.data_manager = DataManager()
        self.transaction_cost = 0.0005
        self.slippage = 0.0008

    async def run(self, asset: str, timeframe: str, signal_name: str, candles_override: "list[dict[str, Any]] | None" = None) -> dict[str, Any]:
        if candles_override is not None:
            response = {"asset": asset, "data": candles_override}
        else:
            response = await self.data_manager.get_ohlcv(asset=asset, timeframe=timeframe)
        candles: list[dict[str, Any]] = response["data"]
        closes = [float(c["close"]) for c in candles]
        if len(closes) < 60:
            raise ValueError("Insufficient data for backtesting. Need at least 60 candles.")

        split = int(len(closes) * 0.7)
        in_sample = closes[:split]
        out_sample = closes[split:]

        walk_forward = self._walk_forward(candles, signal_name)
        oos_equity, oos_trades = self._simulate(out_sample, signal_name)

        overall_metrics = calculate_metrics(walk_forward["equity_curve"], walk_forward["trades"])
        oos_metrics = calculate_metrics(oos_equity, oos_trades)

        mc_score = monte_carlo_stability(oos_trades)
        sensitivity = self._parameter_sensitivity_test(closes, signal_name)
        robust = evaluate_robustness(oos_metrics.cagr, oos_metrics.sharpe, mc_score, sensitivity)

        return {
            "asset": response["asset"],
            "timeframe": timeframe,
            "signal": signal_name,
            "walk_forward": walk_forward,
            "out_of_sample_split": {
                "in_sample_points": len(in_sample),
                "out_sample_points": len(out_sample),
            },
            "transaction_cost": self.transaction_cost,
            "slippage": self.slippage,
            "metrics": asdict(overall_metrics),
            "out_of_sample_metrics": asdict(oos_metrics),
            "robustness": robust,
            "equity_curve": walk_forward["equity_curve"],
            "drawdown_curve": walk_forward["drawdown_curve"],
        }

    def _walk_forward(self, candles: list[dict[str, Any]], signal_name: str) -> dict[str, Any]:
        equity = [10000.0]
        trades: list[float] = []
        window = 40
        for end in range(window, len(candles)):
            train = candles[end - window:end]
            test_close = float(candles[end]["close"])
            prev_close = float(candles[end - 1]["close"])
            if prev_close == 0:
                equity.append(equity[-1])
                continue
            raw_ret = (test_close - prev_close) / prev_close
            direction = self._signal_direction(train, signal_name)
            trade_ret = direction * raw_ret - self.transaction_cost - self.slippage
            pnl = equity[-1] * trade_ret
            trades.append(pnl)
            equity.append(max(1.0, equity[-1] + pnl))

        peak = equity[0]
        drawdown: list[float] = []
        for value in equity:
            peak = max(peak, value)
            dd = ((value - peak) / peak) * 100 if peak else 0.0
            drawdown.append(round(dd, 4))

        return {
            "equity_curve": [round(e, 4) for e in equity],
            "drawdown_curve": drawdown,
            "trades": [round(t, 6) for t in trades],
        }

    def _simulate(self, closes: list[float], signal_name: str) -> tuple[list[float], list[float]]:
        equity = [10000.0]
        trades: list[float] = []
        for i in range(20, len(closes)):
            hist = [{"close": c} for c in closes[i - 20:i]]
            direction = self._signal_direction(hist, signal_name)
            prev = closes[i - 1]
            if prev == 0:
                equity.append(equity[-1])
                continue
            ret = direction * ((closes[i] - prev) / prev) - self.transaction_cost - self.slippage
            pnl = equity[-1] * ret
            trades.append(pnl)
            equity.append(max(1.0, equity[-1] + pnl))
        return equity, trades

    def _signal_direction(self, candles: list[dict[str, Any]], signal_name: str) -> float:
        closes = [float(c["close"]) for c in candles if "close" in c]
        if len(closes) < 3:
            return 0.0
        short = sum(closes[-5:]) / min(5, len(closes))
        long = sum(closes) / len(closes)
        if signal_name in {"trend_v1", "breakout_v1"}:
            return 1.0 if short >= long else -1.0
        if signal_name == "mean_reversion_v1":
            return -1.0 if short >= long else 1.0
        raise ValueError("Unsupported signal. Use trend_v1, mean_reversion_v1, or breakout_v1.")

    def _parameter_sensitivity_test(self, closes: list[float], signal_name: str) -> float:
        scores: list[float] = []
        for shift in (10, 15, 20, 25):
            subset = closes[-(shift + 40):] if len(closes) > shift + 40 else closes
            equity, trades = self._simulate(subset, signal_name)
            m = calculate_metrics(equity, trades)
            scores.append(m.sharpe)
        return parameter_sensitivity(scores)
