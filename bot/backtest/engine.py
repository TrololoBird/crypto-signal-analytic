from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import polars as pl

from bot.config import BotSettings

from .metrics import BacktestResult


class VectorizedBacktester:
    SUPPORTED_SETUPS = {"ema_cross", "momentum_breakout"}

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
        setup_id: str | None = None,
        initial_equity: float = 1.0,
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
                trade_count=0,
                expectancy=0.0,
            )

        work = self._prepare_frame(df)
        if work.is_empty():
            empty = pl.DataFrame({"ts": [datetime.now(UTC)], "equity": [1.0]})
            return BacktestResult(0.0, 0.0, 0.0, 0.0, 0.0, pl.DataFrame(), empty, 0, 0.0)

        ts = work.get_column("close_time").to_list()
        closes = work.get_column("close").cast(pl.Float64).to_list()
        highs = work.get_column("high").cast(pl.Float64).to_list()
        lows = work.get_column("low").cast(pl.Float64).to_list()
        ema_fast = work.get_column("ema20").cast(pl.Float64).to_list()
        ema_slow = work.get_column("ema50").cast(pl.Float64).to_list()
        atr = work.get_column("atr14").cast(pl.Float64).to_list()

        spread_bps = 4.0
        slippage_bps = (spread_bps / 2.0) + 2.0
        per_trade_cost = (slippage_bps / 10_000.0) + (0.04 / 100.0)

        equity = max(0.01, float(initial_equity))
        equity_points: list[dict[str, Any]] = []
        trades: list[dict[str, Any]] = []

        in_position = False
        side = ""
        entry = 0.0
        stop = 0.0
        take = 0.0
        entry_idx = 0
        max_holding_bars = 24
        atr_mult_stop = 1.5
        atr_mult_take = 2.0
        risk_per_trade = 0.01
        max_leverage = 3.0
        position_leverage = 1.0

        setup = (setup_id or "ema_cross").strip().lower()
        if setup not in self.SUPPORTED_SETUPS:
            raise ValueError(f"unsupported setup_id={setup!r}, supported={sorted(self.SUPPORTED_SETUPS)}")

        prev_breakout_high = self._rolling_prev_high(work, window=20)
        prev_breakout_low = self._rolling_prev_low(work, window=20)

        for i in range(1, len(closes)):
            signal = 0
            if not in_position:
                signal = self._entry_signal(
                    idx=i,
                    setup_id=setup,
                    close=closes,
                    ema_fast=ema_fast,
                    ema_slow=ema_slow,
                    prev_high=prev_breakout_high,
                    prev_low=prev_breakout_low,
                    signal_long=work.get_column("signal_long").to_list() if "signal_long" in work.columns else None,
                    signal_short=work.get_column("signal_short").to_list() if "signal_short" in work.columns else None,
                )

                if signal != 0:
                    in_position = True
                    side = "long" if signal > 0 else "short"
                    entry = closes[i]
                    risk = max(float(atr[i] or 0.0) * atr_mult_stop, entry * 0.003)
                    stop_distance_pct = max(risk / max(entry, 1e-9), 1e-6)
                    position_leverage = min(max_leverage, risk_per_trade / stop_distance_pct)
                    if side == "long":
                        stop = entry - risk
                        take = entry + (risk * atr_mult_take)
                    else:
                        stop = entry + risk
                        take = entry - (risk * atr_mult_take)
                    entry_idx = i
                    continue

            if in_position:
                hit_stop = False
                hit_take = False
                exit_price = closes[i]
                reason = "time"
                if side == "long":
                    hit_stop = lows[i] <= stop
                    hit_take = highs[i] >= take
                else:
                    hit_stop = highs[i] >= stop
                    hit_take = lows[i] <= take

                if hit_stop:
                    exit_price = stop
                    reason = "sl"
                elif hit_take:
                    exit_price = take
                    reason = "tp"
                elif (i - entry_idx) >= max_holding_bars:
                    reason = "timeout"
                else:
                    equity_points.append({"ts": ts[i], "equity": equity})
                    continue

                gross_ret = ((exit_price - entry) / entry) if side == "long" else ((entry - exit_price) / entry)
                gross_ret *= position_leverage
                net_ret = gross_ret - per_trade_cost
                equity *= max(0.0, 1.0 + net_ret)
                trades.append(
                    {
                        "entry_ts": ts[entry_idx],
                        "exit_ts": ts[i],
                        "side": side,
                        "entry": entry,
                        "exit": exit_price,
                        "ret": net_ret,
                        "reason": reason,
                        "setup_id": setup_id or "ema_cross",
                        "position_leverage": position_leverage,
                        "risk_per_trade": risk_per_trade,
                    }
                )
                in_position = False
                side = ""
            equity_points.append({"ts": ts[i], "equity": equity})

        eq_df = pl.DataFrame(equity_points) if equity_points else pl.DataFrame({"ts": [datetime.now(UTC)], "equity": [equity]})
        trades_df = pl.DataFrame(trades) if trades else pl.DataFrame()

        return BacktestResult.from_frames(trades=trades_df, equity_curve=eq_df)

    @staticmethod
    def _entry_signal(
        *,
        idx: int,
        setup_id: str,
        close: list[float],
        ema_fast: list[float],
        ema_slow: list[float],
        prev_high: list[float | None],
        prev_low: list[float | None],
        signal_long: list[Any] | None = None,
        signal_short: list[Any] | None = None,
    ) -> int:
        if signal_long is not None and signal_short is not None:
            long_value = bool(signal_long[idx]) if idx < len(signal_long) else False
            short_value = bool(signal_short[idx]) if idx < len(signal_short) else False
            if long_value and not short_value:
                return 1
            if short_value and not long_value:
                return -1
        if setup_id == "ema_cross":
            crossed_up = ema_fast[idx - 1] <= ema_slow[idx - 1] and ema_fast[idx] > ema_slow[idx]
            crossed_down = ema_fast[idx - 1] >= ema_slow[idx - 1] and ema_fast[idx] < ema_slow[idx]
            if crossed_up:
                return 1
            if crossed_down:
                return -1
            return 0

        # momentum_breakout
        high_ref = prev_high[idx]
        low_ref = prev_low[idx]
        if high_ref is not None and close[idx] > high_ref:
            return 1
        if low_ref is not None and close[idx] < low_ref:
            return -1
        return 0

    @staticmethod
    def _prepare_frame(df: pl.DataFrame) -> pl.DataFrame:
        frame = df.with_columns(
            [
                pl.col("close").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
            ]
        )
        if "ema20" not in frame.columns:
            frame = frame.with_columns([pl.col("close").ewm_mean(span=20, adjust=False).alias("ema20")])
        if "ema50" not in frame.columns:
            frame = frame.with_columns([pl.col("close").ewm_mean(span=50, adjust=False).alias("ema50")])
        if "atr14" not in frame.columns:
            tr = pl.max_horizontal(
                [
                    (pl.col("high") - pl.col("low")).abs(),
                    (pl.col("high") - pl.col("close").shift(1)).abs(),
                    (pl.col("low") - pl.col("close").shift(1)).abs(),
                ]
            ).alias("tr")
            frame = frame.with_columns([tr]).with_columns([pl.col("tr").rolling_mean(window_size=14).fill_null(0.0).alias("atr14")]).drop("tr")
        return frame

    @staticmethod
    def _rolling_prev_high(frame: pl.DataFrame, window: int) -> list[float | None]:
        if "high" not in frame.columns:
            return [None] * frame.height
        series = frame.get_column("high").rolling_max(window_size=window).shift(1)
        return [float(v) if v is not None else None for v in series.to_list()]

    @staticmethod
    def _rolling_prev_low(frame: pl.DataFrame, window: int) -> list[float | None]:
        if "low" not in frame.columns:
            return [None] * frame.height
        series = frame.get_column("low").rolling_min(window_size=window).shift(1)
        return [float(v) if v is not None else None for v in series.to_list()]
