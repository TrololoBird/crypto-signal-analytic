from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import polars as pl


@dataclass
class BacktestResult:
    total_return: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    profit_factor: float
    trades: pl.DataFrame
    equity_curve: pl.DataFrame
    trade_count: int = 0
    expectancy: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["trades"] = self.trades.to_dicts()
        payload["equity_curve"] = self.equity_curve.to_dicts()
        return payload

    @classmethod
    def from_frames(cls, trades: pl.DataFrame, equity_curve: pl.DataFrame) -> "BacktestResult":
        def _as_float(value: Any, default: float = 0.0) -> float:
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        equity = equity_curve["equity"] if "equity" in equity_curve.columns else pl.Series("equity", [1.0], dtype=pl.Float64)
        total_return = _as_float((equity.item(-1) if equity.len() else 1.0), 1.0) - 1.0
        running_max = equity.cum_max()
        drawdown = ((equity / running_max) - 1.0).min()
        max_dd = abs(_as_float(drawdown, 0.0))

        returns = trades["ret"] if "ret" in trades.columns else pl.Series("ret", [], dtype=pl.Float64)
        ret_std = _as_float(returns.std(), 0.0)
        mean_ret = _as_float(returns.mean(), 0.0)
        sharpe = (mean_ret / ret_std) if ret_std > 0 else 0.0
        wins = returns.filter(returns > 0)
        losses = returns.filter(returns < 0)
        win_rate = float(wins.len() / max(returns.len(), 1))
        wins_sum = _as_float(wins.sum(), 0.0)
        losses_sum = abs(_as_float(losses.sum(), 1e-9))
        profit_factor = float(wins_sum / losses_sum) if losses.len() > 0 else 0.0
        expectancy = _as_float(returns.mean(), 0.0) if returns.len() > 0 else 0.0
        return cls(
            total_return=total_return,
            sharpe_ratio=sharpe,
            max_drawdown=max_dd,
            win_rate=win_rate,
            profit_factor=profit_factor,
            trades=trades,
            equity_curve=equity_curve,
            trade_count=int(returns.len()),
            expectancy=expectancy,
        )
