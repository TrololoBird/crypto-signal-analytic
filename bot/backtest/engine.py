from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from bot.config import BotSettings

from .metrics import BacktestResult


class VectorizedBacktester:
    def __init__(self, settings: BotSettings) -> None:
        self.settings = settings

    def _load_ohlcv(self, symbol: str, timeframe: str) -> pl.DataFrame:
        parquet_path = self.settings.data_dir / "parquet" / f"{symbol}_{timeframe}.parquet"
        if parquet_path.exists():
            return pl.read_parquet(parquet_path)
        return pl.DataFrame()

    def run(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "15m",
    ) -> BacktestResult:
        df = self._load_ohlcv(symbol, timeframe)
        if not df.is_empty() and "close_time" in df.columns:
            df = df.filter((pl.col("close_time") >= start) & (pl.col("close_time") <= end))

        if df.is_empty() or "close" not in df.columns:
            empty = pl.DataFrame({"ts": [datetime.now(UTC)], "equity": [1.0]})
            return BacktestResult(
                total_return=0.0,
                sharpe_ratio=0.0,
                max_drawdown=0.0,
                win_rate=0.0,
                profit_factor=0.0,
                trades=pl.DataFrame(),
                equity_curve=empty,
            )

        close = df["close"].cast(pl.Float64)
        returns = close.pct_change().fill_null(0.0)
        spread_bps = 4.0
        slippage_bps = spread_bps / 2 + 2
        commission_pct = 0.04 / 100
        per_bar_cost = (slippage_bps / 10_000) + commission_pct
        net_returns = (returns - per_bar_cost).alias("net_returns")
        equity = (1 + net_returns).cum_prod().alias("equity")
        eq_df = pl.DataFrame({"ts": df.get_column("close_time"), "equity": equity})

        total_return = float(eq_df["equity"].item(-1) - 1.0)
        ret_std = float(net_returns.std() or 0.0)
        sharpe = float((net_returns.mean() or 0.0) / ret_std) if ret_std > 0 else 0.0
        running_max = eq_df["equity"].cum_max()
        drawdown = ((eq_df["equity"] / running_max) - 1.0).min()
        max_dd = abs(float(drawdown or 0.0))

        pos = net_returns.filter(net_returns > 0)
        neg = net_returns.filter(net_returns < 0)
        win_rate = float(pos.len() / max(net_returns.len(), 1))
        profit_factor = float((pos.sum() or 0.0) / abs(neg.sum() or 1e-9)) if neg.len() > 0 else 0.0

        trades = pl.DataFrame(
            {
                "ts": df.get_column("close_time"),
                "ret": net_returns,
                "side": ["long"] * df.height,
            }
        )

        return BacktestResult(
            total_return=total_return,
            sharpe_ratio=sharpe,
            max_drawdown=max_dd,
            win_rate=win_rate,
            profit_factor=profit_factor,
            trades=trades,
            equity_curve=eq_df,
        )
