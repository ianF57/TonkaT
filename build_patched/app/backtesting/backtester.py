from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.backtesting.engine import Backtester as EngineBacktester
from app.backtesting.types import BacktestConfig
from app.data.asset_registry import split_asset_identifier
from app.data.historical_store import HistoricalDataStore


class Backtester:
    """Compatibility wrapper around the new stateless engine."""

    def __init__(self) -> None:
        self.store = HistoricalDataStore()
        self.engine = EngineBacktester()

    async def run(
        self,
        asset: str,
        timeframe: str,
        signal_name: str,
        candles_override: list[dict[str, Any]] | None = None,
        limit: int = 250,
    ) -> dict[str, Any]:
        _market, symbol = split_asset_identifier(asset)
        candles = candles_override or self.store.get_candles(symbol, timeframe, limit=limit)
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=365)
        config = BacktestConfig(
            symbol=symbol,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
            strategy_name=signal_name,
            strategy_params={},
            initial_capital=10_000.0,
            risk_config={},
        )
        result = self.engine.run(config, candles)
        return {
            "asset": symbol,
            "timeframe": timeframe,
            "signal": signal_name,
            "metrics": result.metrics,
            "trades": result.trades,
            "equity_curve": result.equity_curve,
            "drawdown_curve": result.drawdown_curve,
            "metadata": result.metadata,
            "source": "database",
            "candles": len(candles),
        }
