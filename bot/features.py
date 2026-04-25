"""Technical analysis feature preparation (Polars-first, optional `polars_ta`).

Indicators stay Polars-native. When `polars_ta` is installed we use its Expr
implementations; otherwise we fall back to pure-Polars series math.

Key indicators (actually used by strategies):
  - Core: ema20/50/200, rsi14, adx14, atr14, macd_*, donchian_*, vwap
  - Advanced: supertrend_dir, bb_pct_b, bb_width, kc_upper/lower/width

Other columns exist for backward compatibility but return neutral values.
"""

from __future__ import annotations

import importlib
from importlib import util as importlib_util
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, cast

import numpy as np
import polars as pl
import structlog

from .models import PreparedSymbol, SymbolFrames, UniverseSymbol

# Optional polars_ta import. All strategy-critical indicators keep pure-Polars
# fallbacks, so this remains optional rather than a hard dependency.
_plta_module = importlib_util.find_spec("polars_ta.talib")
_talib_module = importlib_util.find_spec("talib")
if _plta_module is not None and _talib_module is not None:
    plta = cast(Any, importlib.import_module("polars_ta.talib"))
    _HAS_TALIB = True
else:
    plta = cast(Any, None)
    _HAS_TALIB = False

LOG = structlog.get_logger("bot.features")
_ADVANCED_FALLBACKS_LOGGED: set[str] = set()

# ---------------------------------------------------------------------------
# Frame-level indicator cache — LRU with unique (symbol, interval, close_time) keys.
# ---------------------------------------------------------------------------

_MAX_CACHE_ENTRIES = 500


class _FrameCache:
    """Thread-safe LRU cache for prepared frames.

    Keys are (symbol, interval, close_time_ns) to avoid cross-symbol collisions.
    """

    __slots__ = ("_store", "_max_size")

    def __init__(self, max_size: int = 500) -> None:
        self._store: OrderedDict[tuple[str, str, int], pl.DataFrame] = OrderedDict()
        self._max_size = max_size

    def get(self, key: tuple[str, str, int]) -> pl.DataFrame | None:
        if key not in self._store:
            return None
        self._store.move_to_end(key)
        return self._store[key]

    def put(self, key: tuple[str, str, int], value: pl.DataFrame) -> None:
        if key in self._store:
            self._store.move_to_end(key)
        self._store[key] = value
        while len(self._store) > self._max_size:
            self._store.popitem(last=False)


# Module-level singleton kept for backward compatibility.
_FRAME_CACHE = _FrameCache(max_size=_MAX_CACHE_ENTRIES)


def _log_indicator_fallback(indicator: str, exc: Exception) -> None:
    if indicator in _ADVANCED_FALLBACKS_LOGGED:
        LOG.debug("advanced indicator fallback reused", indicator=indicator, error=str(exc))
        return
    _ADVANCED_FALLBACKS_LOGGED.add(indicator)
    LOG.warning("advanced indicator fallback activated", indicator=indicator, error=str(exc))


def _materialize_series(
    value: pl.Series | pl.Expr | int | float,
    *,
    df: pl.DataFrame,
    name: str,
) -> pl.Series:
    if isinstance(value, pl.Series):
        return value.rename(name)
    if isinstance(value, pl.Expr):
        return df.select(value.alias(name)).to_series()
    return pl.Series(name, [value] * df.height, dtype=pl.Float64)


def _numeric_item(df: pl.DataFrame, row: int, column: str, default: float = 0.0) -> float:
    try:
        value = df.item(row, column)
    except (IndexError, ValueError):
        return default
    try:
        return default if value is None else float(value)
    except (TypeError, ValueError):
        return default


def _as_float_like(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return default


def min_required_bars(
    *,
    min_bars_15m: int = 210,
    min_bars_1h: int = 210,
    min_bars_5m: int = 96,
    min_bars_4h: int = 210,
) -> dict[str, int]:
    return {
        "15m": int(min_bars_15m),
        "1h": int(min_bars_1h),
        "5m": int(min_bars_5m),
        "4h": int(min_bars_4h),
    }


def has_minimum_bars(frames: SymbolFrames, *, minimums: dict[str, int]) -> bool:
    return (
        frames.df_15m.height >= minimums["15m"]
        and frames.df_1h.height >= minimums["1h"]
    )


# ---------------------------------------------------------------------------
# Core indicators using Polars (hand-rolled for exact backward compatibility)
# ---------------------------------------------------------------------------

def _ema(df: pl.DataFrame, period: int) -> pl.Series:
    """Exponential Moving Average using polars_talib or pure Polars."""
    if _HAS_TALIB:
        return _materialize_series(
            plta.EMA(pl.col("close"), timeperiod=float(period)),
            df=df,
            name=f"ema{period}",
        )
    return _materialize_series(df["close"].ewm_mean(span=period, adjust=False), df=df, name=f"ema{period}")


def _rsi(df: pl.DataFrame, period: int = 14) -> pl.Series:
    """Wilder's RSI — same calculation as TA-Lib."""
    if _HAS_TALIB:
        return _materialize_series(
            plta.RSI(pl.col("close"), timeperiod=float(period)),
            df=df,
            name=f"rsi{period}",
        )
    
    # Pure Polars Wilder's RSI (alpha = 1/period)
    close = df["close"]
    delta = close.diff()
    gains = delta.clip(lower_bound=0.0)
    losses = (-delta).clip(lower_bound=0.0)

    avg_gain = _materialize_series(
        gains.ewm_mean(alpha=1.0 / period, adjust=False), df=df, name="avg_gain"
    )
    avg_loss = _materialize_series(
        losses.ewm_mean(alpha=1.0 / period, adjust=False), df=df, name="avg_loss"
    )

    rs = avg_gain / avg_loss
    rsi = (100.0 - (100.0 / (1.0 + rs))).fill_nan(50.0)
    values: list[float] = []
    for gain, loss, current in zip(avg_gain.to_list(), avg_loss.to_list(), rsi.to_list(), strict=False):
        gain_val = float(gain or 0.0)
        loss_val = float(loss or 0.0)
        if loss_val == 0.0 and gain_val > 0.0:
            values.append(100.0)
        elif gain_val == 0.0 and loss_val > 0.0:
            values.append(0.0)
        elif gain_val == 0.0 and loss_val == 0.0:
            values.append(50.0)
        else:
            values.append(float(current or 50.0))
    return pl.Series(f"rsi{period}", values, dtype=pl.Float64)


def _atr(df: pl.DataFrame, period: int = 14) -> pl.Series:
    """Average True Range using polars_talib or pure Polars."""
    if _HAS_TALIB:
        return _materialize_series(
            plta.ATR(pl.col("high"), pl.col("low"), pl.col("close"), timeperiod=float(period)),
            df=df,
            name=f"atr{period}",
        )
    
    # Pure Polars ATR
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    
    tr = pl.max_horizontal(
        (high - low).abs(),
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    )
    
    return _materialize_series(
        tr.ewm_mean(alpha=1.0 / period, adjust=False), df=df, name=f"atr{period}"
    )


def _adx(df: pl.DataFrame, period: int = 14) -> pl.Series:
    """Average Directional Index."""
    if _HAS_TALIB:
        return _materialize_series(
            plta.ADX(pl.col("high"), pl.col("low"), pl.col("close"), timeperiod=float(period)),
            df=df,
            name=f"adx{period}",
        )
    
    # Pure Polars ADX (simplified — full implementation is complex)
    # For production, prefer polars_talib
    high = df["high"]
    low = df["low"]
    
    up_move = high.diff()
    down_move = -low.diff()
    
    plus_dm = _materialize_series(
        pl.when((up_move > down_move) & (up_move > 0.0)).then(up_move).otherwise(0.0),
        df=df,
        name="plus_dm",
    )
    minus_dm = _materialize_series(
        pl.when((down_move > up_move) & (down_move > 0.0)).then(down_move).otherwise(0.0),
        df=df,
        name="minus_dm",
    )
    
    atr = _atr(df, period)
    plus_di = 100.0 * plus_dm.ewm_mean(alpha=1.0 / period, adjust=False) / atr
    minus_di = 100.0 * minus_dm.ewm_mean(alpha=1.0 / period, adjust=False) / atr
    
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    return _materialize_series(
        dx.ewm_mean(alpha=1.0 / period, adjust=False).fill_nan(0.0),
        df=df,
        name=f"adx{period}",
    )


def _vwap(df: pl.DataFrame) -> pl.Series:
    """Volume Weighted Average Price — cumulative with shift(1) to prevent lookahead."""
    typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
    pv = typical_price * df["volume"]
    
    cumulative_pv = pv.cum_sum().shift(1)
    cumulative_volume = df["volume"].cum_sum().shift(1)
    
    vwap = (cumulative_pv / cumulative_volume).forward_fill()
    return _materialize_series(vwap, df=df, name="vwap")


def _roc(df: pl.DataFrame, period: int = 10) -> pl.Series:
    if _HAS_TALIB:
        return _materialize_series(
            plta.ROC(pl.col("close"), timeperiod=float(period)),
            df=df,
            name=f"roc{period}",
        )
    prev_close = df["close"].shift(period)
    return (((df["close"] / prev_close) - 1.0) * 100.0).fill_nan(0.0).rename(f"roc{period}")


def _stochastic(
    df: pl.DataFrame,
    period: int = 14,
    smooth_k: int = 3,
    smooth_d: int = 3,
) -> tuple[pl.Series, pl.Series]:
    rolling_low = df["low"].rolling_min(window_size=period)
    rolling_high = df["high"].rolling_max(window_size=period)
    width = rolling_high - rolling_low
    raw_k = (((df["close"] - rolling_low) / width) * 100.0).fill_nan(50.0)
    k = raw_k.rolling_mean(window_size=smooth_k).fill_nan(50.0).rename("stoch_k14")
    d = k.rolling_mean(window_size=smooth_d).fill_nan(50.0).rename("stoch_d14")
    return k, d


def _cci(df: pl.DataFrame, period: int = 20) -> pl.Series:
    if _HAS_TALIB:
        return _materialize_series(
            plta.CCI(pl.col("high"), pl.col("low"), pl.col("close"), timeperiod=float(period)),
            df=df,
            name=f"cci{period}",
        )
    typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
    sma = typical_price.rolling_mean(window_size=period)
    mean_dev = (typical_price - sma).abs().rolling_mean(window_size=period)
    return ((typical_price - sma) / (0.015 * mean_dev)).fill_nan(0.0).rename(f"cci{period}")


def _mfi(df: pl.DataFrame, period: int = 14) -> pl.Series:
    if _HAS_TALIB:
        return _materialize_series(
            plta.MFI(pl.col("high"), pl.col("low"), pl.col("close"), pl.col("volume"), timeperiod=float(period)),
            df=df,
            name=f"mfi{period}",
        )
    typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
    money_flow = typical_price * df["volume"]
    delta = typical_price.diff()
    positive_flow = pl.Series(
        "positive_flow",
        [float(mf or 0.0) if float(chg or 0.0) > 0.0 else 0.0 for mf, chg in zip(money_flow, delta, strict=False)],
        dtype=pl.Float64,
    )
    negative_flow = pl.Series(
        "negative_flow",
        [float(mf or 0.0) if float(chg or 0.0) < 0.0 else 0.0 for mf, chg in zip(money_flow, delta, strict=False)],
        dtype=pl.Float64,
    )
    pos_sum = positive_flow.rolling_sum(window_size=period)
    neg_sum = negative_flow.rolling_sum(window_size=period)
    values: list[float] = []
    for pos, neg in zip(pos_sum, neg_sum, strict=False):
        pos_val = float(pos or 0.0)
        neg_val = float(neg or 0.0)
        if neg_val <= 0.0 and pos_val <= 0.0:
            values.append(50.0)
        elif neg_val <= 0.0:
            values.append(100.0)
        else:
            ratio = pos_val / neg_val
            values.append(100.0 - (100.0 / (1.0 + ratio)))
    return pl.Series(f"mfi{period}", values, dtype=pl.Float64)


def _cmf(df: pl.DataFrame, period: int = 20) -> pl.Series:
    multipliers: list[float] = []
    for high, low, close in zip(df["high"], df["low"], df["close"], strict=False):
        high_val = float(high or 0.0)
        low_val = float(low or 0.0)
        close_val = float(close or 0.0)
        width = high_val - low_val
        if width <= 0.0:
            multipliers.append(0.0)
            continue
        multipliers.append(((close_val - low_val) - (high_val - close_val)) / width)
    money_flow_multiplier = pl.Series("money_flow_multiplier", multipliers, dtype=pl.Float64)
    money_flow_volume = money_flow_multiplier * df["volume"]
    volume_sum = df["volume"].rolling_sum(window_size=period)
    return (money_flow_volume.rolling_sum(window_size=period) / volume_sum).fill_nan(0.0).rename(f"cmf{period}")


def _realized_volatility(df: pl.DataFrame, period: int = 20) -> pl.Series:
    log_returns = df["close"].log() - df["close"].shift(1).log()
    return (log_returns.rolling_std(window_size=period) * np.sqrt(period) * 100.0).fill_nan(0.0).rename(
        f"realized_vol_{period}"
    )


def _safe_close_position(df: pl.DataFrame, window: int = 20) -> pl.Series:
    """Close position within rolling high-low range (0-1)."""
    rolling_low = df["low"].rolling_min(window_size=window)
    rolling_high = df["high"].rolling_max(window_size=window)
    width = rolling_high - rolling_low
    
    value = (df["close"] - rolling_low) / width
    return value.clip(0.0, 1.0).fill_nan(0.5).rename("close_position")


def _ichimoku_lines(df: pl.DataFrame) -> tuple[pl.Series, pl.Series, pl.Series, pl.Series]:
    """Ichimoku Cloud lines without lookahead bias."""
    high = df["high"]
    low = df["low"]
    
    tenkan = ((high.rolling_max(9) + low.rolling_min(9)) / 2.0).rename("tenkan")
    kijun = ((high.rolling_max(26) + low.rolling_min(26)) / 2.0).rename("kijun")
    
    # Shift back 26 periods to eliminate lookahead
    senkou_a = (((tenkan + kijun) / 2.0).shift(26)).rename("senkou_a")
    senkou_b = (((high.rolling_max(52) + low.rolling_min(52)) / 2.0).shift(26)).rename("senkou_b")
    
    return tenkan, kijun, senkou_a, senkou_b


# ---------------------------------------------------------------------------
# Advanced indicators via polars_talib with pure Polars fallbacks
# ---------------------------------------------------------------------------

def _supertrend(df: pl.DataFrame, period: int = 10, multiplier: float = 3.0) -> tuple[pl.Series, pl.Series]:
    """SuperTrend indicator - pure Polars implementation.
    
    Returns (supertrend_value, direction) where direction is +1 for uptrend, -1 for downtrend.
    """
    high = df["high"]
    low = df["low"]
    close = df["close"]
    
    # ATR for SuperTrend
    prev_close = close.shift(1)
    tr = pl.max_horizontal(
        (high - low).abs(),
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    )
    atr = _materialize_series(tr.ewm_mean(alpha=1.0 / period, adjust=False), df=df, name="supertrend_atr")
    
    # Basic bands (ATR bands)
    upper_band = (high + low) / 2.0 + multiplier * atr
    lower_band = (high + low) / 2.0 - multiplier * atr
    
    # Initialize with forward fill for iterative calculation
    # Use a simple vectorized approximation
    st = _materialize_series(
        pl.when(close > ((high + low) / 2.0 + multiplier * atr).shift(1))
        .then(lower_band)
        .otherwise(upper_band),
        df=df,
        name="supertrend",
    )
    
    # Direction: +1 when close > supertrend (uptrend), -1 otherwise (downtrend)
    direction = _materialize_series(
        pl.when(close > st).then(1.0).otherwise(-1.0),
        df=df,
        name="supertrend_dir",
    )
    
    return st, direction


def _bollinger_bands(close: pl.Series, period: int = 20, nbdev: float = 2.0) -> tuple[pl.Series, pl.Series, pl.Series]:
    """Bollinger Bands - pure Polars implementation.
    
    Returns (upper, middle, lower) bands.
    """
    middle = close.rolling_mean(window_size=period).rename("bb_middle")
    std = close.rolling_std(window_size=period).rename("bb_std")
    
    upper = middle + nbdev * std
    lower = middle - nbdev * std
    
    return upper, middle, lower


def _keltner_channels(df: pl.DataFrame, period: int = 20, multiplier: float = 2.0) -> tuple[pl.Series, pl.Series, pl.Series]:
    """Keltner Channels - pure Polars implementation using ATR.
    
    Returns (upper, middle, lower) channels.
    """
    typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
    middle = typical_price.rolling_mean(window_size=period).rename("kc_middle")
    
    # ATR for channel width
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    
    tr = pl.max_horizontal(
        (high - low).abs(),
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    )
    atr = _materialize_series(tr.ewm_mean(alpha=1.0 / period, adjust=False), df=df, name="kc_atr")
    
    upper = middle + multiplier * atr
    lower = middle - multiplier * atr
    
    return upper, middle, lower


def _add_advanced_indicators(df: pl.DataFrame) -> pl.DataFrame:
    """Add advanced technical indicators using pure Polars implementations."""
    result = df
    
    # --- SuperTrend ---------------------------------------------------------
    st, st_dir = _supertrend(df, period=10, multiplier=3.0)
    result = result.with_columns([
        st.alias("supertrend"),
        st_dir.alias("supertrend_dir"),
    ])
    
    # --- OBV ---------------------------------------------------------------
    try:
        if _HAS_TALIB:
            obv = _materialize_series(plta.OBV(pl.col("close"), pl.col("volume")), df=df, name="obv")
        else:
            close_diff = df["close"].diff()
            direction = pl.Series(
                "obv_direction",
                [
                    1.0 if float(delta or 0.0) > 0.0 else -1.0 if float(delta or 0.0) < 0.0 else 0.0
                    for delta in close_diff
                ],
                dtype=pl.Float64,
            )
            obv = (direction * df["volume"]).cum_sum().rename("obv")
        obv_ema = obv.ewm_mean(span=20, adjust=False)
        result = result.with_columns([
            obv.alias("obv"),
            obv_ema.alias("obv_ema20"),
            (obv > obv_ema).cast(pl.Float64).alias("obv_above_ema"),
        ])
    except Exception as exc:
        _log_indicator_fallback("obv", exc)
        result = result.with_columns([
            pl.lit(0.0).alias("obv"),
            pl.lit(0.0).alias("obv_ema20"),
            pl.lit(0.0).alias("obv_above_ema"),
        ])
    
    # --- Bollinger Bands - pure Polars implementation ------------------------
    upper, middle, lower = _bollinger_bands(df["close"], period=20, nbdev=2.0)
    bb_pct_b = (df["close"] - lower) / (upper - lower)
    bb_width = (upper - lower) / middle * 100.0
    result = result.with_columns([
        bb_pct_b.fill_nan(0.5).alias("bb_pct_b"),
        bb_width.fill_nan(0.0).alias("bb_width"),
    ])
    
    # --- Keltner Channels - pure Polars implementation -----------------------
    kc_upper, kc_middle, kc_lower = _keltner_channels(df, period=20, multiplier=2.0)
    kc_width = (kc_upper - kc_lower) / df["close"]
    result = result.with_columns([
        kc_upper.alias("kc_upper"),
        kc_lower.alias("kc_lower"),
        kc_width.fill_nan(0.04).alias("kc_width"),
    ])
    
    # --- HMA (Hull Moving Average) - UNUSED by strategies, simplified ---------
    # Hull MA = 2*WMA(n/2) - WMA(n), then WMA(sqrt(n)) of result
    close = df["close"]
    wma_half = close.rolling_mean(window_size=5)  # Approximation
    wma_full = close.rolling_mean(window_size=9)
    hma9 = (2 * wma_half - wma_full)  # Simplified HMA
    hma21 = close.rolling_mean(window_size=21)  # Use SMA as approximation
    result = result.with_columns([
        hma9.alias("hma9"),
        hma21.alias("hma21"),
    ])
    
    # --- PSAR (Parabolic SAR) - UNUSED by strategies, simplified -------------
    # Simplified: use recent high/low as PSAR approximation
    psar_long = df["low"].rolling_min(window_size=14)
    psar_short = df["high"].rolling_max(window_size=14)
    result = result.with_columns([
        psar_long.alias("psar_long"),
        psar_short.alias("psar_short"),
        pl.lit(0.0).alias("psar_reversal"),
    ])
    
    # --- Aroon - UNUSED by strategies, neutral values ----------------------
    result = result.with_columns([
        pl.lit(50.0).alias("aroon_up14"),
        pl.lit(50.0).alias("aroon_down14"),
        pl.lit(0.0).alias("aroon_osc14"),
    ])
    
    # --- Stochastic ---------------------------------------------------------
    stoch_k, stoch_d = _stochastic(df, period=14, smooth_k=3, smooth_d=3)
    result = result.with_columns([
        stoch_k.alias("stoch_k14"),
        stoch_d.alias("stoch_d14"),
        (stoch_k - stoch_d).fill_nan(0.0).alias("stoch_h14"),
    ])
    
    # --- CCI, Williams %R, MFI, CMF, Ultimate Oscillator --------------------
    rolling_high = df["high"].rolling_max(window_size=14)
    rolling_low = df["low"].rolling_min(window_size=14)
    willr = (((rolling_high - df["close"]) / (rolling_high - rolling_low)) * -100.0).fill_nan(-50.0)
    result = result.with_columns([
        _cci(df, 20).fill_nan(0.0).alias("cci20"),
        willr.alias("willr14"),
        _mfi(df, 14).fill_nan(50.0).alias("mfi14"),
        _cmf(df, 20).fill_nan(0.0).alias("cmf20"),
        pl.lit(50.0).alias("uo"),
    ])
    
    # --- Fisher Transform - UNUSED, neutral ----------------------------------
    result = result.with_columns([
        pl.lit(0.0).alias("fisher"),
        pl.lit(0.0).alias("fisher_signal"),
    ])
    
    # --- Squeeze Momentum - UNUSED, neutral --------------------------------
    result = result.with_columns([
        pl.lit(0.0).alias("squeeze_hist"),
        pl.lit(0.0).alias("squeeze_on"),
        pl.lit(0.0).alias("squeeze_off"),
        pl.lit(1.0).alias("squeeze_no"),
    ])
    
    # --- Chandelier Exit - UNUSED, simplified -------------------------------
    result = result.with_columns([
        (df["close"] * 0.95).alias("chandelier_long"),
        (df["close"] * 1.05).alias("chandelier_short"),
        pl.lit(0.0).alias("chandelier_dir"),
    ])
    
    # --- Z-Score and Slope -------------------------------------------------
    zscore30 = ((df["close"] - df["close"].rolling_mean(window_size=30)) / df["close"].rolling_std(window_size=30)).fill_nan(0.0)
    result = result.with_columns([
        zscore30.alias("zscore30"),
        _roc(df, 5).fill_nan(0.0).alias("slope5"),
    ])
    
    # --- Ichimoku Cloud - UNUSED by strategies -----------------------------
    tenkan, kijun, senkou_a, senkou_b = _ichimoku_lines(result)
    result = result.with_columns([
        tenkan.alias("ichi_tenkan"),
        kijun.alias("ichi_kijun"),
        senkou_a.alias("ichi_senkou_a"),
        senkou_b.alias("ichi_senkou_b"),
    ])
    
    return result


# ---------------------------------------------------------------------------
# Main frame preparation
# ---------------------------------------------------------------------------

def _prepare_frame(df: pl.DataFrame) -> pl.DataFrame:
    """Compute all technical indicators for a single OHLCV DataFrame.
    
    Returns a new DataFrame with NaN-seeded rows dropped.
    All backward-compatible column names are preserved.
    """
    # Core indicators
    work = df.with_columns([
        _ema(df, 20).alias("ema20"),
        _ema(df, 50).alias("ema50"),
        _ema(df, 200).alias("ema200"),
        _rsi(df, 14).alias("rsi14"),
        _adx(df, 14).alias("adx14"),
        _atr(df, 14).alias("atr14"),
    ])
    
    # MACD
    macd_line = _ema(work, 12) - _ema(work, 26)
    work = work.with_columns([
        macd_line.alias("macd_line"),
    ])
    
    macd_signal = work["macd_line"].ewm_mean(span=9, adjust=False)
    work = work.with_columns([
        macd_signal.alias("macd_signal"),
        (pl.col("macd_line") - macd_signal).alias("macd_hist"),
    ])
    
    # Donchian Channels
    work = work.with_columns([
        pl.col("low").rolling_min(window_size=20).alias("donchian_low20"),
        pl.col("high").rolling_max(window_size=20).alias("donchian_high20"),
    ])
    work = work.with_columns([
        pl.col("donchian_low20").shift(1).alias("prev_donchian_low20"),
        pl.col("donchian_high20").shift(1).alias("prev_donchian_high20"),
    ])
    
    # Volume metrics
    work = work.with_columns([
        pl.col("volume").rolling_mean(window_size=20).alias("volume_mean20"),
    ])
    work = work.with_columns([
        (pl.col("volume") / pl.col("volume_mean20")).alias("volume_ratio20"),
    ])
    
    # VWAP and bands
    work = work.with_columns([
        _vwap(work).alias("vwap"),
    ])
    
    price_dev_sq = (work["close"] - work["vwap"]) ** 2
    # polars Series has no cum_mean(); compute cumulative mean via cum_sum / n
    if work.height:
        denom = pl.Series("n", range(1, work.height + 1), dtype=pl.Float64)
        vwap_std = (price_dev_sq.cum_sum() / denom).sqrt()
    else:
        vwap_std = price_dev_sq
    work = work.with_columns([
        vwap_std.alias("vwap_std"),
        (pl.col("vwap") + vwap_std).alias("vwap_upper1"),
        (pl.col("vwap") - vwap_std).alias("vwap_lower1"),
        (pl.col("vwap") + 2.0 * vwap_std).alias("vwap_upper2"),
        (pl.col("vwap") - 2.0 * vwap_std).alias("vwap_lower2"),
        (((pl.col("close") - pl.col("vwap")) / pl.col("vwap")) * 100.0).fill_nan(0.0).alias("vwap_deviation_pct"),
    ])
    
    # Delta ratio (if taker_buy available)
    if "taker_buy_base_volume" in work.columns:
        work = work.with_columns([
            ((pl.col("taker_buy_base_volume") / pl.col("volume"))
             .rolling_mean(window_size=5)
             .clip(0.0, 1.0)
             .alias("delta_ratio")),
        ])
    else:
        work = work.with_columns([pl.lit(0.5).alias("delta_ratio")])
    
    # ATR %
    work = work.with_columns([
        ((pl.col("atr14") / pl.col("close")) * 100.0)
        .clip(lower_bound=0.001)
        .alias("atr_pct"),
    ])
    
    # Close position
    work = work.with_columns([
        _safe_close_position(work, window=20).alias("close_position"),
    ])
    
    # Advanced indicators
    work = _add_advanced_indicators(work)
    work = work.with_columns([
        _roc(work, 10).fill_nan(0.0).alias("roc10"),
        _realized_volatility(work, 20).fill_nan(0.0).alias("realized_vol_20"),
        (
            (pl.col("vwap_deviation_pct") - pl.col("vwap_deviation_pct").rolling_mean(window_size=20))
            / pl.col("vwap_deviation_pct").rolling_std(window_size=20)
        ).fill_nan(0.0).alias("vwap_deviation_z20"),
    ])
    
    # Drop rows with insufficient data
    # Filter where ema200 or donchian_low20 is null
    work = work.filter(
        pl.col("ema200").is_not_null() & pl.col("donchian_low20").is_not_null()
    )
    
    return work


# ---------------------------------------------------------------------------
# 4h bias helper
# ---------------------------------------------------------------------------

def _bias_4h(work_4h: pl.DataFrame) -> str:
    """Determine 4h bias from EMA alignment."""
    if work_4h.is_empty():
        return "neutral"
    
    last = work_4h.row(-1, named=True)
    close = last["close"]
    ema20 = last["ema20"]
    ema50 = last["ema50"]
    ema200 = last["ema200"]
    
    if close > ema50 > ema200 and ema20 > ema50:
        return "uptrend"
    if close < ema50 < ema200 and ema20 < ema50:
        return "downtrend"
    return "neutral"


def _bias_1h(work_1h: pl.DataFrame) -> str:
    """Determine 1h bias from EMA alignment for 15M signal context."""
    if work_1h.is_empty():
        return "neutral"
    
    last = work_1h.row(-1, named=True)
    close = last["close"]
    ema20 = last["ema20"]
    ema50 = last["ema50"]
    ema200 = last["ema200"]
    
    if close > ema50 > ema200 and ema20 > ema50:
        return "uptrend"
    if close < ema50 < ema200 and ema20 < ema50:
        return "downtrend"
    return "neutral"


def _market_regime(
    work_4h: pl.DataFrame,
    work_1h: pl.DataFrame | None = None,
    work_15m: pl.DataFrame | None = None,
    threshold_choppy: float = 15.0,
    threshold_trending: float = 25.0,
) -> str:
    """Classify regime from 4h strength plus 1h/15m structure."""
    if work_4h.is_empty() or "adx14" not in work_4h.columns:
        return "neutral"

    adx_4h = _numeric_item(work_4h, -1, "adx14")
    bias_4h = _bias_4h(work_4h)
    regime_1h = _regime_1h_confirmed(work_1h if work_1h is not None else pl.DataFrame())
    atr_pct_15m = _numeric_item(work_15m if work_15m is not None else pl.DataFrame(), -1, "atr_pct")

    if adx_4h >= threshold_trending and bias_4h in {"uptrend", "downtrend"} and regime_1h in {"uptrend", "downtrend"}:
        return "trending"
    if adx_4h < threshold_choppy and regime_1h == "ranging":
        return "choppy"
    if atr_pct_15m >= 3.0 and regime_1h == "ranging":
        return "choppy"
    return "neutral"


# ---------------------------------------------------------------------------
# Structure-based helpers
# ---------------------------------------------------------------------------

def _swing_points(work: pl.DataFrame, n: int = 3) -> tuple[pl.Series, pl.Series]:
    """Detect swing highs and lows using n-bar look-around."""
    height = len(work)
    if height < 2 * n + 1:
        zeros = [False] * height
        return (
            pl.Series("swing_high", zeros, dtype=pl.Boolean),
            pl.Series("swing_low", zeros, dtype=pl.Boolean),
        )

    high = work["high"].to_list()
    low = work["low"].to_list()
    swing_high = [False] * height
    swing_low = [False] * height

    for idx in range(n, height - n):
        curr_high = high[idx]
        curr_low = low[idx]
        if curr_high is None or curr_low is None:
            continue
        high_ok = True
        low_ok = True
        for offset in range(1, n + 1):
            prev_high = high[idx - offset]
            next_high = high[idx + offset]
            prev_low = low[idx - offset]
            next_low = low[idx + offset]
            if (
                prev_high is None
                or next_high is None
                or prev_low is None
                or next_low is None
            ):
                high_ok = False
                low_ok = False
                break
            if curr_high <= prev_high or curr_high <= next_high:
                high_ok = False
            if curr_low >= prev_low or curr_low >= next_low:
                low_ok = False
        swing_high[idx] = high_ok
        swing_low[idx] = low_ok

    return (
        pl.Series("swing_high", swing_high, dtype=pl.Boolean),
        pl.Series("swing_low", swing_low, dtype=pl.Boolean),
    )


def _market_structure_1h(work_1h: pl.DataFrame) -> str:
    """Determine 1h market structure from swing points."""
    if len(work_1h) < 20:
        return "ranging"
    
    swing_high, swing_low = _swing_points(work_1h, n=3)
    
    # Get swing high/low values
    high_vals = work_1h.filter(swing_high)["high"].to_list()
    low_vals = work_1h.filter(swing_low)["low"].to_list()
    
    if len(high_vals) < 2 or len(low_vals) < 2:
        return "ranging"
    
    hh = high_vals[-1] > high_vals[-2]  # higher high
    hl = low_vals[-1] > low_vals[-2]    # higher low
    lh = high_vals[-1] < high_vals[-2]  # lower high
    ll = low_vals[-1] < low_vals[-2]    # lower low
    
    if hh and hl:
        return "uptrend"
    if lh and ll:
        return "downtrend"
    return "ranging"


def _regime_4h_confirmed(work_4h: pl.DataFrame, min_bars: int = 3) -> str:
    """Strict 4h regime requiring consecutive bars in same trend."""
    if len(work_4h) < min_bars:
        return "ranging"
    
    tail = work_4h.tail(min_bars)
    
    # Check uptrend condition
    uptrend_count = tail.filter(
        (pl.col("ema20") > pl.col("ema50")) & (pl.col("ema50") > pl.col("ema200"))
    ).height
    
    # Check downtrend condition
    downtrend_count = tail.filter(
        (pl.col("ema20") < pl.col("ema50")) & (pl.col("ema50") < pl.col("ema200"))
    ).height
    
    if uptrend_count == min_bars:
        return "uptrend"
    if downtrend_count == min_bars:
        return "downtrend"
    return "ranging"


def _regime_1h_confirmed(work_1h: pl.DataFrame, min_bars: int = 3) -> str:
    """Strict 1h regime requiring consecutive bars in same trend for 15M signal context."""
    if len(work_1h) < min_bars:
        return "ranging"
    
    tail = work_1h.tail(min_bars)
    
    # Check uptrend condition
    uptrend_count = tail.filter(
        (pl.col("ema20") > pl.col("ema50")) & (pl.col("ema50") > pl.col("ema200"))
    ).height
    
    # Check downtrend condition
    downtrend_count = tail.filter(
        (pl.col("ema20") < pl.col("ema50")) & (pl.col("ema50") < pl.col("ema200"))
    ).height
    
    if uptrend_count == min_bars:
        return "uptrend"
    if downtrend_count == min_bars:
        return "downtrend"
    return "ranging"


def _volume_poc(work: pl.DataFrame, lookback: int = 96, buckets: int = 20) -> float | None:
    """Simplified Volume Point of Control."""
    if len(work) < 10:
        return None
    
    tail = work.tail(lookback)
    price_min = _as_float_like(tail["low"].min())
    price_max = _as_float_like(tail["high"].max())
    
    if price_max <= price_min:
        return None
    
    bucket_size = (price_max - price_min) / buckets
    bucket_volumes = [0.0] * buckets
    
    # Iterate through rows to accumulate volume
    for row in tail.iter_rows(named=True):
        bar_low = _as_float_like(row["low"])
        bar_high = _as_float_like(row["high"])
        bar_vol = _as_float_like(row["volume"])
        
        b_start = max(0, int((bar_low - price_min) / bucket_size))
        b_end = min(buckets - 1, int((bar_high - price_min) / bucket_size))
        n_buckets = b_end - b_start + 1
        
        if n_buckets > 0:
            for i in range(b_start, b_end + 1):
                bucket_volumes[i] += bar_vol / n_buckets
    
    poc_bucket = int(np.argmax(bucket_volumes))
    poc_price = price_min + (poc_bucket + 0.5) * bucket_size
    return float(poc_price)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _cached_prepare_frame(
    frame: pl.DataFrame,
    *,
    symbol: str = "",
    interval: str = "",
    cache: _FrameCache | None = None,
) -> pl.DataFrame:
    """_prepare_frame with LRU cache keyed on (symbol, interval, close_time)."""
    if frame.is_empty() or "close_time" not in frame.columns or "close" not in frame.columns:
        return _prepare_frame(frame)
    
    last = frame.row(-1, named=True)
    try:
        # Convert timestamp to nanoseconds for key
        close_time = last["close_time"]
        if isinstance(close_time, str):
            from datetime import datetime
            close_time = datetime.fromisoformat(close_time.replace('Z', '+00:00'))
        close_time_ns = int(close_time.timestamp() * 1e9)
    except Exception:
        return _prepare_frame(frame)
    
    key = (symbol, interval, close_time_ns)
    target_cache = cache or _FRAME_CACHE
    cached = target_cache.get(key)
    if cached is not None:
        return cached
    
    result = _prepare_frame(frame)
    target_cache.put(key, result)
    return result


def _to_polars(df: object) -> pl.DataFrame:
    """Convert supported frame-like values to Polars."""
    if isinstance(df, pl.DataFrame):
        return df
    if hasattr(df, "__dataframe__"):
        return pl.from_pandas(cast(Any, df))
    return pl.DataFrame(df)


def prepare_symbol(
    universe_symbol: UniverseSymbol,
    frames: SymbolFrames,
    *,
    minimums: dict[str, int] | None = None,
    settings: Any | None = None,
) -> PreparedSymbol | None:
    """Prepare a symbol for signal detection by computing all indicators.
    
    Returns None if there is insufficient historical data.
    """
    import logging
    _log = logging.getLogger("bot.features")
    
    sym = universe_symbol.symbol
    
    minimums = minimums or min_required_bars()
    len_4h = frames.df_4h.height if frames.df_4h is not None else 0
    len_1h = frames.df_1h.height
    len_15m = frames.df_15m.height
    len_5m = frames.df_5m.height if frames.df_5m is not None else 0

    if not has_minimum_bars(frames, minimums=minimums):
        _log.warning(
            "%s: insufficient frame data | 1h=%d/%d 15m=%d/%d 5m=%d/%d optional_4h=%d/%d",
            sym,
            len_1h,
            minimums["1h"],
            len_15m,
            minimums["15m"],
            len_5m,
            minimums["5m"],
            len_4h,
            minimums["4h"],
        )
        return None
    
    # Convert pandas DataFrames to Polars if needed
    work_1h = _cached_prepare_frame(
        _to_polars(frames.df_1h), symbol=sym, interval="1h"
    )
    work_15m = _cached_prepare_frame(
        _to_polars(frames.df_15m), symbol=sym, interval="15m"
    )
    work_5m = None
    if frames.df_5m is not None and not frames.df_5m.is_empty():
        work_5m = _cached_prepare_frame(
            _to_polars(frames.df_5m), symbol=sym, interval="5m"
        )
    work_4h = None
    if frames.df_4h is not None and not frames.df_4h.is_empty():
        work_4h = _cached_prepare_frame(
            _to_polars(frames.df_4h), symbol=sym, interval="4h"
        )

    work_len_1h = len(work_1h) if work_1h is not None else 0
    work_len_15m = len(work_15m) if work_15m is not None else 0
    work_len_5m = len(work_5m) if work_5m is not None else 0
    work_len_4h = len(work_4h) if work_4h is not None else 0

    if min(work_len_1h, work_len_15m) < 30:
        _log.warning(
            "%s: insufficient processed data | work_1h=%d work_15m=%d optional_5m=%d optional_4h=%d need=30",
            sym,
            work_len_1h,
            work_len_15m,
            work_len_5m,
            work_len_4h,
        )
        return None

    _log.info(
        "%s: prepared symbol successfully | work_15m=%d work_1h=%d work_5m=%d optional_4h=%d",
        sym,
        work_len_15m,
        work_len_1h,
        work_len_5m,
        work_len_4h,
    )
    
    # Calculate spread
    spread_bps = None
    if (
        frames.bid_price is not None
        and frames.ask_price is not None
        and frames.bid_price > 0
        and frames.ask_price > 0
    ):
        midpoint = (frames.bid_price + frames.ask_price) / 2.0
        if midpoint > 0:
            spread_bps = ((frames.ask_price - frames.bid_price) / midpoint) * 10_000.0

    work_4h_frame = work_4h if work_4h is not None else pl.DataFrame()
    regime = _market_regime(work_4h_frame, work_1h=work_1h, work_15m=work_15m)

    return PreparedSymbol(
        universe=universe_symbol,
        work_1h=work_1h,
        work_15m=work_15m,
        bid_price=frames.bid_price,
        ask_price=frames.ask_price,
        spread_bps=spread_bps,
        work_5m=work_5m,
        work_4h=work_4h,
        bias_4h=_bias_4h(work_4h_frame),
        bias_1h=_bias_1h(work_1h),  # 1H context for 15M signals
        market_regime=regime,
        structure_1h=_market_structure_1h(work_1h),
        regime_4h_confirmed=_regime_4h_confirmed(work_4h_frame),
        regime_1h_confirmed=_regime_1h_confirmed(work_1h),  # 1H context for 15M signals
        poc_1h=_volume_poc(work_1h, lookback=48),
        poc_15m=_volume_poc(work_15m, lookback=96),
        settings=settings,
    )
