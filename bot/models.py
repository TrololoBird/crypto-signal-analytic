from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import polars as pl

if TYPE_CHECKING:
    from .config import BotSettings


UTC = timezone.utc


@dataclass(frozen=True, slots=True)
class SymbolMeta:
    symbol: str
    base_asset: str
    quote_asset: str
    contract_type: str
    status: str
    onboard_date_ms: int


@dataclass(frozen=True, slots=True)
class UniverseSymbol:
    symbol: str
    base_asset: str
    quote_asset: str
    contract_type: str
    status: str
    onboard_date_ms: int
    quote_volume: float
    price_change_pct: float
    last_price: float
    shortlist_bucket: str = ""


@dataclass(slots=True)
class SymbolFrames:
    symbol: str
    df_1h: pl.DataFrame
    df_15m: pl.DataFrame
    bid_price: float | None
    ask_price: float | None
    df_5m: pl.DataFrame | None = None
    df_4h: pl.DataFrame | None = None


@dataclass(frozen=True, slots=True)
class AggTradeSnapshot:
    symbol: str
    trade_count: int
    buy_qty: float
    sell_qty: float
    delta_ratio: float | None


@dataclass(frozen=True, slots=True)
class AggTrade:
    symbol: str
    trade_id: int
    price: float
    quantity: float
    trade_time_ms: int
    is_buyer_maker: bool

    @property
    def trade_time(self) -> datetime:
        return datetime.fromtimestamp(self.trade_time_ms / 1000.0, tz=UTC)


@dataclass(slots=True)
class PreparedSymbol:
    universe: UniverseSymbol
    work_1h: pl.DataFrame
    work_15m: pl.DataFrame
    bid_price: float | None
    ask_price: float | None
    spread_bps: float | None
    work_5m: pl.DataFrame | None = None
    work_4h: pl.DataFrame | None = None
    bias_4h: str = "neutral"  # 4H macro context (market regime)
    bias_1h: str = "neutral"  # 1H trading context for 15M signals
    # Optional fields populated from WS global streams (mark price / liquidations)
    mark_price: float | None = None
    ticker_price: float | None = None
    funding_rate: float | None = None
    oi_current: float | None = None
    oi_change_pct: float | None = None
    ls_ratio: float | None = None
    taker_ratio: float | None = None  # taker buy/sell volume ratio (>1.0 = net buyers)
    liquidation_score: float | None = None  # -1.0 (bearish liq) … +1.0 (bullish liq)
    funding_trend: str | None = None  # "rising" | "falling" | "flat" | None
    basis_pct: float | None = None    # (futures - index) / index * 100; + = contango, - = backwardation
    global_ls_ratio: float | None = None
    top_trader_position_ratio: float | None = None
    top_vs_global_ls_gap: float | None = None
    mark_index_spread_bps: float | None = None
    premium_zscore_5m: float | None = None
    premium_slope_5m: float | None = None
    oi_slope_5m: float | None = None
    depth_imbalance: float | None = None
    microprice_bias: float | None = None
    agg_trade_delta_30s: float | None = None
    aggression_shift: float | None = None
    spot_lead_return_1m: float | None = None
    spot_futures_spread_bps: float | None = None
    mark_price_age_seconds: float | None = None
    ticker_price_age_seconds: float | None = None
    book_ticker_age_seconds: float | None = None
    context_snapshot_age_seconds: float | None = None
    data_source_mix: str = "futures_only"
    market_regime: str = "neutral"  # "trending" | "neutral" | "choppy"
    # Structure-based fields (Фаза 2 рефакторинга)
    structure_1h: str = "ranging"       # "uptrend" | "downtrend" | "ranging"
    regime_4h_confirmed: str = "ranging"  # "uptrend" | "downtrend" | "ranging" (3+ bars) - macro only
    regime_1h_confirmed: str = "ranging"  # "uptrend" | "downtrend" | "ranging" (3+ bars) - trading context
    poc_1h: float | None = None         # Point of Control on 1h (highest volume price)
    poc_15m: float | None = None        # Point of Control on 15m
    settings: BotSettings | None = None
    reject_log: list[dict[str, Any]] = field(default_factory=list)

    @property
    def symbol(self) -> str:
        return self.universe.symbol

    @property
    def atr_pct(self) -> float | None:
        if self.work_15m.is_empty() or "atr_pct" not in self.work_15m.columns:
            return None
        value = self.work_15m.item(-1, "atr_pct")
        try:
            return None if value is None else float(value)
        except (TypeError, ValueError):
            return None


@dataclass(frozen=True, slots=True)
class Signal:
    symbol: str
    setup_id: str
    direction: str
    score: float
    timeframe: str
    entry_low: float
    entry_high: float
    stop: float
    take_profit_1: float
    take_profit_2: float
    reasons: tuple[str, ...] = ()
    bias_4h: str = "neutral"
    quote_volume: float | None = None
    spread_bps: float | None = None
    atr_pct: float | None = None
    orderflow_delta_ratio: float | None = None
    oi_change_pct: float | None = None
    funding_rate: float | None = None
    strategy_family: str = "continuation"
    confirmation_profile: str = "trend_follow"
    target_integrity_status: str | None = None
    single_target_mode: bool = False
    passed_filters: tuple[str, ...] = ()
    mark_price: float | None = None
    volume_ratio: float | None = None  # current volume / 20-bar avg (for analytics companion)
    adx_1h: float | None = None
    risk_reward: float | None = None
    trend_direction: str | None = None
    trend_score: float | None = None
    premium_zscore_5m: float | None = None
    premium_slope_5m: float | None = None
    ls_ratio: float | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if self.risk_reward is None:
            risk = abs(self.entry_mid - self.stop)
            reward = abs(self.take_profit_2 - self.entry_mid)
            computed = (reward / risk) if risk > 0 else 0.0
            object.__setattr__(self, "risk_reward", computed)

    @property
    def entry_mid(self) -> float:
        mid = (self.entry_low + self.entry_high) / 2.0
        if self.mark_price and self.mark_price > 0 and mid > 0:
            if abs(mid - self.mark_price) / mid < 0.002:
                return self.mark_price
        return mid

    @property
    def stop_distance_pct(self) -> float:
        if self.entry_mid <= 0:
            return 0.0
        return abs(self.entry_mid - self.stop) / self.entry_mid * 100.0

    @property
    def signal_key(self) -> str:
        return f"{self.symbol}|{self.setup_id}|{self.direction}"

    @property
    def tracking_id(self) -> str:
        stamp = self.created_at.astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        return f"{self.signal_key}|{stamp}"

    @property
    def tracking_ref(self) -> str:
        digest = hashlib.sha1(self.tracking_id.encode("utf-8")).hexdigest()
        return digest[:8].upper()

    @property
    def side(self) -> str:
        return self.direction

    @property
    def entry(self) -> float:
        return self.entry_mid

    @property
    def sl(self) -> float:
        return self.stop

    @property
    def tp1(self) -> float:
        return self.take_profit_1

    @property
    def tp2(self) -> float:
        return self.take_profit_2

    @property
    def target_count(self) -> int:
        return 1 if self.single_target_mode else 2

    @property
    def metadata(self) -> dict[str, Any]:
        return {
            "tracking_id": self.tracking_id,
            "tracking_ref": self.tracking_ref,
            "signal_key": self.signal_key,
            "timeframe": self.timeframe,
            "strategy_family": self.strategy_family,
            "confirmation_profile": self.confirmation_profile,
            "target_integrity_status": self.target_integrity_status,
            "single_target_mode": self.single_target_mode,
        }

    def same_target(self, tolerance: float | None = None) -> bool:
        tol = tolerance
        if tol is None:
            anchor = max(abs(self.entry_mid), abs(self.take_profit_1), abs(self.take_profit_2), 1.0)
            tol = anchor * 1e-8
        return math.isclose(self.take_profit_1, self.take_profit_2, abs_tol=tol, rel_tol=0.0)

    def to_log_row(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "setup_id": self.setup_id,
            "direction": self.direction,
            "score": round(self.score, 4),
            "timeframe": self.timeframe,
            "entry_low": round(self.entry_low, 8),
            "entry_high": round(self.entry_high, 8),
            "entry_mid": round(self.entry_mid, 8),
            "stop": round(self.stop, 8),
            "take_profit_1": round(self.take_profit_1, 8),
            "take_profit_2": round(self.take_profit_2, 8),
            "risk_reward": round(float(self.risk_reward or 0.0), 4),
            "stop_distance_pct": round(self.stop_distance_pct, 4),
            "bias_4h": self.bias_4h,
            "quote_volume": self.quote_volume,
            "spread_bps": self.spread_bps,
            "atr_pct": self.atr_pct,
            "orderflow_delta_ratio": self.orderflow_delta_ratio,
            "strategy_family": self.strategy_family,
            "confirmation_profile": self.confirmation_profile,
            "target_integrity_status": self.target_integrity_status,
            "single_target_mode": self.single_target_mode,
            "passed_filters": list(self.passed_filters),
            "reasons": list(self.reasons),
            "created_at": self.created_at.isoformat(),
            "tracking_id": self.tracking_id,
            "tracking_ref": self.tracking_ref,
        }


@dataclass(slots=True)
class PipelineResult:
    """Result container for signal analysis pipeline.
    
    Replaces legacy PipelineResult from pipeline.py for backward compatibility.
    Modern engine uses SignalResult in core/engine/base.py.
    """
    symbol: str
    trigger: str
    event_ts: datetime
    raw_setups: int
    candidates: list[Signal] = field(default_factory=list)
    rejected: list[dict[str, Any]] = field(default_factory=list)
    delivered: list[Signal] = field(default_factory=list)
    error: str | None = None
    status: str | None = None
    prepared: PreparedSymbol | None = None
    funnel: dict[str, Any] = field(default_factory=dict)
