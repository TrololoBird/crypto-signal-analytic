"""
Outcome tracking - сохранение результатов сигналов для ML обучения.

Сохраняет полные данные о сигналах:
- Входные признаки (features)
- Результат (TP/SL/expired)
- Метрики качества (PnL, MAE, MFE)
- LLM вердикты и их точность
"""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import msgspec

from .models import Signal
from .tracked_signals import TrackedSignalState, parse_state_dt


UTC = timezone.utc


def _normalized_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def _normalized_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    return bool(value)


def build_prepared_feature_snapshot(prepared: Any) -> dict[str, Any]:
    """Build a normalized feature snapshot from PreparedSymbol-like data."""
    if prepared is None:
        return {}

    features: dict[str, Any] = {}

    def _frame_value(frame: Any, column: str) -> float | None:
        if frame is None or getattr(frame, "is_empty", lambda: True)():
            return None
        if column not in getattr(frame, "columns", []):
            return None
        try:
            return _normalized_float(frame.item(-1, column))
        except Exception:
            return None

    def _ema_stack(frame: Any, fast: str, slow: str) -> bool | None:
        fast_value = _frame_value(frame, fast)
        slow_value = _frame_value(frame, slow)
        if fast_value is None or slow_value is None or slow_value <= 0.0:
            return None
        return fast_value > slow_value

    work_15m = getattr(prepared, "work_15m", None)
    work_1h = getattr(prepared, "work_1h", None)
    work_4h = getattr(prepared, "work_4h", None)

    features["rsi_15m"] = _frame_value(work_15m, "rsi14")
    features["rsi_1h"] = _frame_value(work_1h, "rsi14")
    features["rsi_4h"] = _frame_value(work_4h, "rsi14")
    features["adx_1h"] = _frame_value(work_1h, "adx14")
    features["adx_4h"] = _frame_value(work_4h, "adx14")
    features["atr_pct_15m"] = _frame_value(work_15m, "atr_pct")
    features["volume_ratio_15m"] = _frame_value(work_15m, "volume_ratio20")
    features["macd_histogram_15m"] = _frame_value(work_15m, "macd_hist")

    features["ema20_above_ema50_15m"] = _normalized_bool(_ema_stack(work_15m, "ema20", "ema50"))
    features["ema50_above_ema200_15m"] = _normalized_bool(_ema_stack(work_15m, "ema50", "ema200"))
    features["ema20_above_ema50_1h"] = _normalized_bool(_ema_stack(work_1h, "ema20", "ema50"))
    features["ema50_above_ema200_1h"] = _normalized_bool(_ema_stack(work_1h, "ema50", "ema200"))

    features["supertrend_dir_1h"] = _frame_value(work_1h, "supertrend_dir")
    features["supertrend_dir_15m"] = _frame_value(work_15m, "supertrend_dir")
    features["obv_above_ema_15m"] = _frame_value(work_15m, "obv_above_ema")
    features["bb_pct_b_15m"] = _frame_value(work_15m, "bb_pct_b")
    features["bb_width_15m"] = _frame_value(work_15m, "bb_width")

    features["funding_rate"] = _normalized_float(getattr(prepared, "funding_rate", None))
    features["oi_current"] = _normalized_float(getattr(prepared, "oi_current", None))
    features["oi_change_pct"] = _normalized_float(getattr(prepared, "oi_change_pct", None))
    features["oi_slope_5m"] = _normalized_float(getattr(prepared, "oi_slope_5m", None))
    features["ls_ratio"] = _normalized_float(getattr(prepared, "ls_ratio", None))
    features["global_ls_ratio"] = _normalized_float(getattr(prepared, "global_ls_ratio", None))
    features["top_trader_position_ratio"] = _normalized_float(getattr(prepared, "top_trader_position_ratio", None))
    features["top_vs_global_ls_gap"] = _normalized_float(getattr(prepared, "top_vs_global_ls_gap", None))
    features["liquidation_score"] = _normalized_float(getattr(prepared, "liquidation_score", None))
    features["mark_index_spread_bps"] = _normalized_float(getattr(prepared, "mark_index_spread_bps", None))
    features["premium_zscore_5m"] = _normalized_float(getattr(prepared, "premium_zscore_5m", None))
    features["premium_slope_5m"] = _normalized_float(getattr(prepared, "premium_slope_5m", None))
    features["context_snapshot_age_seconds"] = _normalized_float(getattr(prepared, "context_snapshot_age_seconds", None))
    features["data_source_mix"] = getattr(prepared, "data_source_mix", "futures_only") or "futures_only"
    features["market_regime"] = getattr(prepared, "market_regime", "neutral") or "neutral"

    return features


@dataclass(frozen=True, slots=True)
class SignalFeatures:
    """Входные признаки сигнала для ML."""

    # Основной score
    base_score: float

    # LLM верификация
    llm_verdict: str | None = None  # YES/NO/None
    llm_reason: str | None = None

    # Технические индикаторы
    rsi_15m: float | None = None
    rsi_1h: float | None = None
    rsi_4h: float | None = None
    adx_1h: float | None = None
    adx_4h: float | None = None
    atr_pct_15m: float | None = None
    volume_ratio_15m: float | None = None
    macd_histogram_15m: float | None = None

    # EMA alignment
    ema20_above_ema50_15m: bool | None = None
    ema50_above_ema200_15m: bool | None = None
    ema20_above_ema50_1h: bool | None = None
    ema50_above_ema200_1h: bool | None = None

    # Рыночные данные
    spread_bps: float | None = None
    quote_volume: float | None = None
    delta_ratio: float | None = None

    # Параметры сделки (без дефолтов)
    risk_reward: float = 0.0
    stop_distance_pct: float = 0.0
    entry_mid: float = 0.0

    # Bias
    bias_4h: str = "neutral"

    # Сетап информация
    setup_id: str = ""
    direction: str = ""
    timeframe: str = ""

    # Advanced indicators from pandas-ta (None if pandas-ta not installed)
    supertrend_dir_1h: float | None = None
    supertrend_dir_15m: float | None = None
    obv_above_ema_15m: float | None = None
    bb_pct_b_15m: float | None = None
    bb_width_15m: float | None = None

    # WS-enriched market context
    funding_rate: float | None = None
    oi_current: float | None = None
    oi_change_pct: float | None = None
    oi_slope_5m: float | None = None
    ls_ratio: float | None = None
    global_ls_ratio: float | None = None
    top_trader_position_ratio: float | None = None
    top_vs_global_ls_gap: float | None = None
    liquidation_score: float | None = None
    mark_index_spread_bps: float | None = None
    premium_zscore_5m: float | None = None
    premium_slope_5m: float | None = None
    context_snapshot_age_seconds: float | None = None
    data_source_mix: str = "futures_only"
    market_regime: str = "neutral"  # "trending" | "neutral" | "choppy"

    def to_dict(self) -> dict[str, Any]:
        return {
            "base_score": self.base_score,
            "llm_verdict": self.llm_verdict,
            "llm_reason": self.llm_reason,
            "rsi_15m": self.rsi_15m,
            "rsi_1h": self.rsi_1h,
            "rsi_4h": self.rsi_4h,
            "adx_1h": self.adx_1h,
            "adx_4h": self.adx_4h,
            "atr_pct_15m": self.atr_pct_15m,
            "volume_ratio_15m": self.volume_ratio_15m,
            "macd_histogram_15m": self.macd_histogram_15m,
            "ema20_above_ema50_15m": self.ema20_above_ema50_15m,
            "ema50_above_ema200_15m": self.ema50_above_ema200_15m,
            "ema20_above_ema50_1h": self.ema20_above_ema50_1h,
            "ema50_above_ema200_1h": self.ema50_above_ema200_1h,
            "spread_bps": self.spread_bps,
            "quote_volume": self.quote_volume,
            "delta_ratio": self.delta_ratio,
            "risk_reward": self.risk_reward,
            "stop_distance_pct": self.stop_distance_pct,
            "entry_mid": self.entry_mid,
            "bias_4h": self.bias_4h,
            "setup_id": self.setup_id,
            "direction": self.direction,
            "timeframe": self.timeframe,
            # New fields
            "supertrend_dir_1h": self.supertrend_dir_1h,
            "supertrend_dir_15m": self.supertrend_dir_15m,
            "obv_above_ema_15m": self.obv_above_ema_15m,
            "bb_pct_b_15m": self.bb_pct_b_15m,
            "bb_width_15m": self.bb_width_15m,
            "funding_rate": self.funding_rate,
            "oi_current": self.oi_current,
            "oi_change_pct": self.oi_change_pct,
            "oi_slope_5m": self.oi_slope_5m,
            "ls_ratio": self.ls_ratio,
            "global_ls_ratio": self.global_ls_ratio,
            "top_trader_position_ratio": self.top_trader_position_ratio,
            "top_vs_global_ls_gap": self.top_vs_global_ls_gap,
            "liquidation_score": self.liquidation_score,
            "mark_index_spread_bps": self.mark_index_spread_bps,
            "premium_zscore_5m": self.premium_zscore_5m,
            "premium_slope_5m": self.premium_slope_5m,
            "context_snapshot_age_seconds": self.context_snapshot_age_seconds,
            "data_source_mix": self.data_source_mix,
            "market_regime": self.market_regime,
        }


@dataclass(frozen=True, slots=True)
class SignalOutcome:
    """Результат завершенного сигнала."""

    # Идентификаторы
    signal_id: str
    tracking_id: str
    tracking_ref: str

    # Сигнал информация
    symbol: str
    setup_id: str
    direction: str
    timeframe: str

    # Время
    created_at: str
    activated_at: str | None = None
    closed_at: str | None = None

    # Цены входа/выхода
    entry_price: float | None = None
    exit_price: float | None = None

    # Результат
    result: str = ""  # tp1_hit, tp2_hit, stop_loss, expired, ambiguous_exit
    pnl_pct: float = 0.0
    pnl_r_multiple: float = 0.0

    # Экскурсии (MAE/MFE)
    max_profit_pct: float = 0.0
    max_loss_pct: float = 0.0
    mae: float = 0.0  # Maximum Adverse Excursion
    mfe: float = 0.0  # Maximum Favorable Excursion

    # Время до событий (минуты)
    time_to_entry_min: int = 0
    time_to_exit_min: int = 0

    # Признаки
    features: dict[str, Any] = field(default_factory=dict)

    # Обучение
    was_profitable: bool = False
    llm_was_correct: bool | None = None  # None если LLM не использовался
    setup_quality: str = "neutral"  # good, bad, neutral

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "tracking_id": self.tracking_id,
            "tracking_ref": self.tracking_ref,
            "symbol": self.symbol,
            "setup_id": self.setup_id,
            "direction": self.direction,
            "timeframe": self.timeframe,
            "created_at": self.created_at,
            "activated_at": self.activated_at,
            "closed_at": self.closed_at,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "result": self.result,
            "pnl_pct": round(self.pnl_pct, 4),
            "pnl_r_multiple": round(self.pnl_r_multiple, 4),
            "max_profit_pct": round(self.max_profit_pct, 4),
            "max_loss_pct": round(self.max_loss_pct, 4),
            "mae": round(self.mae, 4),
            "mfe": round(self.mfe, 4),
            "time_to_entry_min": self.time_to_entry_min,
            "time_to_exit_min": self.time_to_exit_min,
            "features": self.features,
            "was_profitable": self.was_profitable,
            "llm_was_correct": self.llm_was_correct,
            "setup_quality": self.setup_quality,
        }

def extract_features_from_signal(
    signal: Signal,
    prepared_data: dict[str, Any] | None = None,
    llm_verdict: str | None = None,
    llm_reason: str | None = None,
) -> SignalFeatures:
    """Извлекает признаки из сигнала для последующего сохранения."""
    return SignalFeatures(
        base_score=signal.score,
        llm_verdict=llm_verdict,
        llm_reason=llm_reason,
        rsi_15m=prepared_data.get("rsi_15m") if prepared_data else None,
        rsi_1h=prepared_data.get("rsi_1h") if prepared_data else None,
        rsi_4h=prepared_data.get("rsi_4h") if prepared_data else None,
        adx_1h=prepared_data.get("adx_1h") if prepared_data else None,
        adx_4h=prepared_data.get("adx_4h") if prepared_data else None,
        atr_pct_15m=prepared_data.get("atr_pct_15m") if prepared_data else None,
        volume_ratio_15m=prepared_data.get("volume_ratio_15m") if prepared_data else None,
        macd_histogram_15m=prepared_data.get("macd_histogram_15m") if prepared_data else None,
        ema20_above_ema50_15m=prepared_data.get("ema20_above_ema50_15m") if prepared_data else None,
        ema50_above_ema200_15m=prepared_data.get("ema50_above_ema200_15m") if prepared_data else None,
        ema20_above_ema50_1h=prepared_data.get("ema20_above_ema50_1h") if prepared_data else None,
        ema50_above_ema200_1h=prepared_data.get("ema50_above_ema200_1h") if prepared_data else None,
        spread_bps=signal.spread_bps,
        quote_volume=signal.quote_volume,
        delta_ratio=signal.orderflow_delta_ratio,
        risk_reward=signal.risk_reward,
        stop_distance_pct=signal.stop_distance_pct,
        entry_mid=signal.entry_mid,
        bias_4h=signal.bias_4h,
        setup_id=signal.setup_id,
        direction=signal.direction,
        timeframe=signal.timeframe,
        # Advanced indicators
        supertrend_dir_1h=prepared_data.get("supertrend_dir_1h") if prepared_data else None,
        supertrend_dir_15m=prepared_data.get("supertrend_dir_15m") if prepared_data else None,
        obv_above_ema_15m=prepared_data.get("obv_above_ema_15m") if prepared_data else None,
        bb_pct_b_15m=prepared_data.get("bb_pct_b_15m") if prepared_data else None,
        bb_width_15m=prepared_data.get("bb_width_15m") if prepared_data else None,
        # WS market context
        funding_rate=prepared_data.get("funding_rate") if prepared_data else None,
        oi_current=prepared_data.get("oi_current") if prepared_data else None,
        oi_change_pct=prepared_data.get("oi_change_pct") if prepared_data else None,
        oi_slope_5m=prepared_data.get("oi_slope_5m") if prepared_data else None,
        ls_ratio=prepared_data.get("ls_ratio") if prepared_data else None,
        global_ls_ratio=prepared_data.get("global_ls_ratio") if prepared_data else None,
        top_trader_position_ratio=prepared_data.get("top_trader_position_ratio") if prepared_data else None,
        top_vs_global_ls_gap=prepared_data.get("top_vs_global_ls_gap") if prepared_data else None,
        liquidation_score=prepared_data.get("liquidation_score") if prepared_data else None,
        mark_index_spread_bps=prepared_data.get("mark_index_spread_bps") if prepared_data else None,
        premium_zscore_5m=prepared_data.get("premium_zscore_5m") if prepared_data else None,
        premium_slope_5m=prepared_data.get("premium_slope_5m") if prepared_data else None,
        context_snapshot_age_seconds=prepared_data.get("context_snapshot_age_seconds") if prepared_data else None,
        data_source_mix=prepared_data.get("data_source_mix", "futures_only") if prepared_data else "futures_only",
        market_regime=prepared_data.get("market_regime", "neutral") if prepared_data else "neutral",
    )


def create_outcome_from_tracked(
    tracked: TrackedSignalState,
    features: SignalFeatures,
    max_profit_pct: float = 0.0,
    max_loss_pct: float = 0.0,
) -> SignalOutcome:
    """Создает Outcome из завершенного tracked сигнала."""
    outcome_result = getattr(tracked, "close_reason", None) or getattr(tracked, "result", "")

    # Определяем entry price
    entry_price = tracked.activation_price or tracked.entry_mid
    exit_price = tracked.close_price

    # Рассчитываем PnL
    pnl_pct = 0.0
    pnl_r_multiple = 0.0
    if entry_price and exit_price:
        if tracked.direction == "long":
            pnl_pct = (exit_price - entry_price) / entry_price * 100.0
        else:
            pnl_pct = (entry_price - exit_price) / entry_price * 100.0

        # PnL в R множителях
        risk = abs(entry_price - tracked.stop)
        if risk > 0:
            pnl_r_multiple = pnl_pct / (risk / entry_price * 100.0) if risk > 0 else 0.0

    # Время до событий
    created_at = parse_state_dt(tracked.created_at) or datetime.now(UTC)
    activated_at = parse_state_dt(tracked.activated_at)
    closed_at = parse_state_dt(tracked.closed_at)

    time_to_entry_min = 0
    time_to_exit_min = 0
    if activated_at:
        time_to_entry_min = int((activated_at - created_at).total_seconds() / 60)
    if closed_at:
        time_to_exit_min = int((closed_at - created_at).total_seconds() / 60)

    # Определение качества сетапа
    was_profitable = pnl_pct > 0
    setup_quality = "good" if pnl_r_multiple >= 1.0 else ("bad" if pnl_r_multiple <= -1.0 else "neutral")

    # LLM корректность
    llm_was_correct: bool | None = None
    if features.llm_verdict == "YES":
        llm_was_correct = was_profitable
    elif features.llm_verdict == "NO":
        llm_was_correct = not was_profitable

    return SignalOutcome(
        signal_id=tracked.tracking_id,
        tracking_id=tracked.tracking_id,
        tracking_ref=tracked.tracking_ref,
        symbol=tracked.symbol,
        setup_id=tracked.setup_id,
        direction=tracked.direction,
        timeframe=tracked.timeframe,
        created_at=tracked.created_at,
        activated_at=tracked.activated_at,
        closed_at=tracked.closed_at,
        entry_price=entry_price,
        exit_price=exit_price,
        result=outcome_result,
        pnl_pct=pnl_pct,
        pnl_r_multiple=pnl_r_multiple,
        max_profit_pct=max_profit_pct,
        max_loss_pct=max_loss_pct,
        mae=max_loss_pct,  # Упрощенно
        mfe=max_profit_pct,  # Упрощенно
        time_to_entry_min=time_to_entry_min,
        time_to_exit_min=time_to_exit_min,
        features=features.to_dict(),
        was_profitable=was_profitable,
        llm_was_correct=llm_was_correct,
        setup_quality=setup_quality,
    )
