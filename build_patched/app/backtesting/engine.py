from __future__ import annotations

from datetime import datetime
from typing import Any

from app.backtesting.metrics import calculate_metrics
from app.backtesting.types import BacktestConfig, BacktestResult
from app.signals.breakout_v1 import BreakoutV1
from app.signals.mean_reversion_v1 import MeanReversionV1
from app.signals.trend_signal_v1 import TrendSignalV1

_PERIODS_PER_YEAR = {
    "1d": 252,
    "1h": 24 * 252,
    "5m": 12 * 24 * 252,
    "1w": 52,
}


class Backtester:
    """Stateless deterministic backtesting engine."""

    def __init__(self) -> None:
        self._strategies = {
            "trend_v1": TrendSignalV1,
            "mean_reversion_v1": MeanReversionV1,
            "breakout_v1": BreakoutV1,
        }

    def run(self, config: BacktestConfig, candles: list[dict[str, Any]]) -> BacktestResult:
        if config.strategy_name not in self._strategies:
            raise ValueError("Unsupported strategy")

        strategy = self._strategies[config.strategy_name](**config.strategy_params)
        start_epoch = int(config.start_date.timestamp())
        end_epoch = int(config.end_date.timestamp())

        filtered = [c for c in candles if start_epoch <= self._epoch(c["timestamp"]) <= end_epoch]
        if len(filtered) < 50:
            raise ValueError("Insufficient candles in requested range")

        position = 0
        entry_price = 0.0
        units = 0.0
        equity = [config.initial_capital]
        trades: list[dict[str, Any]] = []
        bars_in_market = 0

        risk_per_trade = float(config.risk_config.get("risk_per_trade", 0.01))
        sl_pct = float(config.risk_config.get("stop_loss_pct", 0.02))
        tp_pct = float(config.risk_config.get("take_profit_pct", 0.04))

        for idx in range(30, len(filtered)):
            history = filtered[:idx]
            bar = filtered[idx]
            signal = strategy.generate(history)

            close = float(bar["close"])
            high = float(bar["high"])
            low = float(bar["low"])
            capital = equity[-1]

            if position != 0:
                bars_in_market += 1
                stop = entry_price * (1 - sl_pct) if position > 0 else entry_price * (1 + sl_pct)
                take = entry_price * (1 + tp_pct) if position > 0 else entry_price * (1 - tp_pct)

                exit_price: float | None = None
                reason = ""
                if position > 0:
                    if low <= stop:
                        exit_price, reason = stop, "stop_loss"
                    elif high >= take:
                        exit_price, reason = take, "take_profit"
                    elif signal["action"] == "sell":
                        exit_price, reason = close, "signal_flip"
                else:
                    if high >= stop:
                        exit_price, reason = stop, "stop_loss"
                    elif low <= take:
                        exit_price, reason = take, "take_profit"
                    elif signal["action"] == "buy":
                        exit_price, reason = close, "signal_flip"

                if exit_price is not None:
                    pnl = (exit_price - entry_price) * units * position
                    equity.append(max(1.0, capital + pnl))
                    trades.append({
                        "entry_price": round(entry_price, 6),
                        "exit_price": round(exit_price, 6),
                        "pnl": round(pnl, 6),
                        "side": "long" if position > 0 else "short",
                        "exit_reason": reason,
                        "timestamp": bar["timestamp"],
                    })
                    position = 0
                    units = 0.0
                    continue

            if position == 0 and signal["action"] in {"buy", "sell"}:
                position = 1 if signal["action"] == "buy" else -1
                entry_price = close
                stake = capital * risk_per_trade
                units = stake / close if close > 0 else 0.0

            equity.append(equity[-1])

        drawdown = self._drawdown_curve(equity)
        metrics = calculate_metrics(
            equity_curve=equity,
            trades=trades,
            periods_per_year=_PERIODS_PER_YEAR.get(config.timeframe, 252),
            bars_in_market=bars_in_market,
        )

        return BacktestResult(
            metrics=metrics,
            trades=trades,
            equity_curve=[round(x, 6) for x in equity],
            drawdown_curve=drawdown,
            metadata={
                "symbol": config.symbol,
                "timeframe": config.timeframe,
                "strategy_name": config.strategy_name,
                "executed_at": datetime.utcnow().isoformat(),
                "candles": len(filtered),
            },
        )

    @staticmethod
    def _drawdown_curve(equity: list[float]) -> list[float]:
        peak = equity[0]
        out: list[float] = []
        for value in equity:
            peak = max(peak, value)
            out.append(round(((value - peak) / peak) * 100 if peak else 0.0, 6))
        return out

    @staticmethod
    def _epoch(ts: str) -> int:
        return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
