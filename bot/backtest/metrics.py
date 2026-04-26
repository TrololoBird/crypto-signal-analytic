from __future__ import annotations

from dataclasses import asdict, dataclass

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

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["trades"] = self.trades.to_dicts()
        payload["equity_curve"] = self.equity_curve.to_dicts()
        return payload
