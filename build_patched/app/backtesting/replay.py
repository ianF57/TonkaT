"""Historical replay engine — DB-only data access via HistoricalDataStore.

The replay loads candles for a specific date boundary from the local SQLite
database.  No external API calls are made.

Date-range query
────────────────
HistoricalDataStore.get_candles_range(start, end) is used so that:
1. Only rows up to and including the replay date's end-of-day are returned
   as the "historical" context the strategy would have seen.
2. Rows after the replay date are returned as "forward" candles to simulate
   what the trade outcome would have been.

Both slices come from a single range query on the clustered B-tree:

    SELECT … FROM candle
    WHERE symbol=? AND timeframe=?
      AND open_time BETWEEN :epoch_min AND :epoch_max
    ORDER BY open_time ASC

This is O(log n + k) — one seek to the start of the date range, then a
sequential forward walk.
"""
from __future__ import annotations

import calendar
from datetime import UTC, date, datetime, time, timezone
from typing import Any

from app.backtesting.backtester import Backtester
from app.data.asset_registry import split_asset_identifier
from app.data.historical_store import HistoricalDataStore, InsufficientDataError
from app.regime.regime_classifier import RegimeClassifier
from app.scoring.confidence import confidence_score


class HistoricalReplay:
    """Replay historical regime, ranking, and trade outcome at a chosen date."""

    # How far back before the replay date we look for context candles.
    # 2 years of 1h = 17 520 candles — enough for all signals + walk-forward.
    _LOOKBACK_DAYS = 730

    def __init__(self) -> None:
        self.store   = HistoricalDataStore()
        self.backtester = Backtester()
        self.regime_classifier = RegimeClassifier()
        self.signal_ids = ["trend_v1", "mean_reversion_v1", "breakout_v1"]

    async def replay(self, asset: str, timeframe: str, replay_date: str) -> dict[str, Any]:
        """Replay the signal landscape as it would have appeared on replay_date.

        Steps
        ─────
        1. Parse the replay date and compute start/end epoch boundaries.
        2. Load the full range [start, end_of_day] from the DB in one query.
        3. Split at end-of-day: historical ≤ cutoff, forward > cutoff.
        4. Run regime classification + signal ranking on the historical slice.
        5. Simulate trade outcome using the forward slice.

        Raises
        ──────
        InsufficientDataError  — fewer than 60 candles before the replay date.
        ValueError             — bad date format or no signals generated.
        """
        _market, symbol = split_asset_identifier(asset)
        target          = date.fromisoformat(replay_date)

        # Epoch boundaries for the full range query
        end_of_day_epoch = calendar.timegm(
            datetime.combine(target, time(23, 59, 59), tzinfo=UTC).timetuple()
        )
        lookback_epoch = end_of_day_epoch - self._LOOKBACK_DAYS * 86400

        # Single DB range query — no API calls.
        # get_candles_range raises InsufficientDataError if < 60 rows.
        all_candles = self.store.get_candles_range(
            symbol,
            timeframe,
            start_epoch=lookback_epoch,
            end_epoch=end_of_day_epoch,
            min_candles=60,
        )

        historical, forward = self._split_at_cutoff(all_candles, end_of_day_epoch)

        if len(historical) < 60:
            raise ValueError(
                "Insufficient candles before selected date. Choose a later date."
            )

        regime_snapshot = self.regime_classifier.classify(
            historical, asset=asset, timeframe=timeframe
        )

        ranked = await self._rank_historical(
            asset=asset,
            timeframe=timeframe,
            candles=historical,
            regime=regime_snapshot.current_regime,
        )
        if not ranked:
            raise ValueError("No valid historical signals for selected replay date.")

        top          = ranked[0]
        trade_outcome = self._simulate_trade_outcome(top["signal"], historical, forward)

        return {
            "asset":     asset,
            "timeframe": timeframe,
            "date":      replay_date,
            "source":    "database",
            "candles_used": len(historical),
            "regime": {
                "current_regime":          regime_snapshot.current_regime,
                "confidence_score":        regime_snapshot.confidence_score,
                "historical_distribution": regime_snapshot.historical_distribution,
            },
            "top_signal":    top,
            "trade_outcome": trade_outcome,
            "full_metrics":  top["full_metrics"],
        }

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _split_at_cutoff(
        self,
        candles: list[dict[str, Any]],
        cutoff_epoch: int,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Split candles at *cutoff_epoch* (inclusive on the historical side)."""
        historical: list[dict[str, Any]] = []
        forward:    list[dict[str, Any]] = []
        for candle in candles:
            ts_raw = candle.get("timestamp", "")
            epoch  = self._iso_to_epoch(str(ts_raw))
            if epoch <= cutoff_epoch:
                historical.append(candle)
            else:
                forward.append(candle)
        return historical, forward

    @staticmethod
    def _iso_to_epoch(iso: str) -> int:
        """Parse an ISO-8601 UTC string (from _epoch_to_iso) back to epoch."""
        sanitized = iso.replace("Z", "+00:00")
        dt = datetime.fromisoformat(sanitized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return calendar.timegm(dt.astimezone(timezone.utc).timetuple())

    async def _rank_historical(
        self,
        asset: str,
        timeframe: str,
        candles: list[dict[str, Any]],
        regime: str,
    ) -> list[dict[str, Any]]:
        ranked: list[dict[str, Any]] = []
        for signal in self.signal_ids:
            # candles_override avoids a second DB round-trip per signal.
            backtest   = await self.backtester.run(
                asset=asset,
                timeframe=timeframe,
                signal_name=signal,
                candles_override=candles,
            )
            metrics    = backtest["metrics"]
            oos        = backtest["out_of_sample_metrics"]
            robustness = backtest["robustness"]

            regime_align = (
                95.0
                if regime in {"trending", "momentum_breakout", "high_volatility"}
                and signal in {"trend_v1", "breakout_v1"}
                else 95.0
                if regime in {"ranging", "mean_reversion", "low_volatility"}
                and signal == "mean_reversion_v1"
                else 40.0
            )

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

            ranked.append({
                "signal":               signal,
                "suggested_direction":  self._direction(signal),
                "expected_return_range": self._expected_return_range(metrics),
                "expected_drawdown":    round(float(metrics["max_drawdown"]), 2),
                "confidence_score":     conf,
                "full_metrics": {
                    "metrics":               metrics,
                    "out_of_sample_metrics": oos,
                    "robustness":            robustness,
                },
            })

        ranked.sort(key=lambda item: item["confidence_score"], reverse=True)
        return ranked

    @staticmethod
    def _direction(signal: str) -> str:
        return "counter-trend" if signal == "mean_reversion_v1" else "trend-aligned"

    @staticmethod
    def _expected_return_range(metrics: dict[str, Any]) -> str:
        cagr = float(metrics.get("cagr", 0.0))
        low  = round(max(-30.0, cagr * 0.5), 2)
        high = round(min(120.0, cagr * 1.3 + 5), 2)
        return f"{low}% to {high}%"

    def _simulate_trade_outcome(
        self,
        signal: str,
        historical: list[dict[str, Any]],
        forward:    list[dict[str, Any]],
    ) -> dict[str, Any]:
        if not forward:
            return {
                "bars_held":  0,
                "entry_price": float(historical[-1]["close"]),
                "exit_price":  float(historical[-1]["close"]),
                "return_pct":  0.0,
                "status":      "No forward candles after selected date.",
            }
        entry   = float(historical[-1]["close"])
        holding = forward[: min(10, len(forward))]
        exit_price = float(holding[-1]["close"])

        trend_bias = 1.0
        if signal == "mean_reversion_v1":
            recent = [float(c["close"]) for c in historical[-20:]]
            short  = sum(recent[-5:]) / min(5, len(recent))
            long_  = sum(recent) / len(recent)
            trend_bias = -1.0 if short >= long_ else 1.0

        raw    = ((exit_price - entry) / entry) * 100 if entry else 0.0
        signed = raw * trend_bias
        return {
            "bars_held":  len(holding),
            "entry_price": round(entry, 6),
            "exit_price":  round(exit_price, 6),
            "return_pct":  round(signed, 4),
            "status":      "simulated",
        }
