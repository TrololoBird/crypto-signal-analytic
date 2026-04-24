from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal, cast

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    load_dotenv = lambda: None  # Fallback if dotenv not installed

from pydantic import BaseModel, Field, field_validator, model_validator


class RuntimeConfig(BaseModel):
    analysis_concurrency: int = Field(default=6, ge=1, le=20)
    max_signals_per_cycle: int = Field(default=3, ge=1, le=10)
    circuit_breaker_failure_threshold: int = Field(default=5, ge=0, le=100)
    metrics_port: int = Field(default=9090, ge=1000, le=65535)
    dashboard_port: int = Field(default=8080, ge=1000, le=65535)
    circuit_breaker_cooldown_seconds: int = Field(default=60, ge=0, le=3600)
    telemetry_subdir: str = "telemetry"
    log_level: str = "INFO"
    shortlist_refresh_interval_seconds: int = Field(default=7200, ge=300, le=86400)
    emergency_fallback_seconds: int = Field(default=1800, ge=300, le=7200)
    strict_data_quality: bool = True
    diagnostic_trace_limit_per_symbol: int = Field(default=20, ge=0, le=500)
    # Startup throttling to prevent REST API flood
    startup_batch_size: int = Field(default=3, ge=1, le=10)
    startup_batch_delay_seconds: float = Field(default=2.0, ge=0.5, le=10.0)
    max_concurrent_rest_requests: int = Field(default=5, ge=1, le=20)

    @field_validator("log_level")
    @classmethod
    def _normalize_log_level(cls, value: str) -> str:
        return str(value or "INFO").strip().upper()


class UniverseConfig(BaseModel):
    quote_asset: str = "USDT"
    dynamic_limit: int = Field(default=60, ge=10, le=200)
    shortlist_limit: int = Field(default=45, ge=5, le=100)  # Reduced from 60 to prevent WS overload
    min_quote_volume_usd: float = Field(default=10_000_000.0, ge=0.0)
    min_listing_age_days: int = Field(default=14, ge=0, le=3650)
    pinned_symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT")

    @field_validator("quote_asset")
    @classmethod
    def _normalize_quote_asset(cls, value: str) -> str:
        return str(value or "USDT").strip().upper()

    @field_validator("pinned_symbols")
    @classmethod
    def _normalize_pins(cls, value: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
        return tuple(str(item).strip().upper() for item in (value or ()) if str(item).strip())


class FilterConfig(BaseModel):
    """Runtime trading filters and stop placement heuristics.

    Stop distance is derived from the setup anchor plus/minus ATR multiplied by
    the setup-specific `stop_atr_multiplier_*` value. Higher multipliers widen
    stops and reduce premature exits at the cost of larger per-trade risk.
    """

    cooldown_minutes: int = Field(default=60, ge=0, le=1440)
    symbol_cooldown_minutes: int = Field(default=120, ge=0, le=10080)
    max_spread_bps: float = Field(default=8.0, ge=0.1, le=100.0)
    min_atr_pct: float = Field(default=0.40, ge=0.01, le=10.0)
    max_atr_pct: float = Field(default=10.0, ge=0.1, le=50.0)
    min_risk_reward: float = Field(default=1.9, ge=0.5, le=10.0)
    # Minimum ADX on 1h required for structure_pullback (trend strength gate).
    min_adx_1h: float = Field(default=20.0, ge=0.0, le=50.0)
    # Minimum final score after the simplified structure-based scoring.
    min_score: float = Field(default=0.66, ge=0.0, le=1.0)
    # Data freshness gates (avoid hidden "magic numbers" in filters.py).
    freshness_15m_minutes: int = Field(default=30, ge=5, le=240)
    freshness_1h_hours: int = Field(default=3, ge=1, le=48)
    freshness_4h_hours: int = Field(default=10, ge=1, le=240)
    min_bars_15m: int = Field(default=210, ge=30, le=5000)
    min_bars_1h: int = Field(default=210, ge=30, le=5000)
    min_bars_4h: int = Field(default=210, ge=30, le=5000)
    # Mark price sanity guard: reject if mark and last diverge beyond this pct.
    max_mark_price_deviation_pct: float = Field(default=0.005, ge=0.0, le=0.10)
    setups: dict[str, dict[str, float]] = Field(default_factory=dict)

    @field_validator("setups")
    @classmethod
    def _normalize_setup_overrides(
        cls, value: Mapping[str, Mapping[str, Any]] | None
    ) -> dict[str, dict[str, float]]:
        normalized: dict[str, dict[str, float]] = {}
        for setup_id, params in (value or {}).items():
            normalized[str(setup_id)] = {
                str(param_name): float(param_value)
                for param_name, param_value in params.items()
            }
        return normalized


class TrackingConfig(BaseModel):
    enabled: bool = True
    pending_expiry_minutes: int = Field(default=180, ge=15, le=1440)
    active_expiry_minutes: int = Field(default=720, ge=30, le=10080)
    move_stop_to_break_even_on_tp1: bool = True
    min_stop_distance_pct: float = Field(default=0.5, ge=0.0, le=100.0)
    max_stop_distance_pct: float = Field(default=15.0, ge=0.5, le=100.0)
    agg_trade_page_limit: int = Field(default=6, ge=1, le=20)
    agg_trade_page_size: int = Field(default=1000, ge=100, le=1000)


_ALL_SETUP_IDS: tuple[str, ...] = (
    # Original 5
    "structure_pullback",
    "structure_break_retest",
    "wick_trap_reversal",
    "squeeze_setup",
    "ema_bounce",
    # New 10
    "fvg_setup",
    "order_block",
    "liquidity_sweep",
    "bos_choch",
    "hidden_divergence",
    "funding_reversal",
    "cvd_divergence",
    "session_killzone",
    "breaker_block",
    "turtle_soup",
)


class SetupConfig(BaseModel):
    # Original 5 setups
    structure_pullback: bool = True
    structure_break_retest: bool = True
    wick_trap_reversal: bool = True
    squeeze_setup: bool = True
    ema_bounce: bool = True
    # New 10 setups (disabled by default until registered in SetupRegistry)
    fvg_setup: bool = True
    order_block: bool = True
    liquidity_sweep: bool = True
    bos_choch: bool = True
    hidden_divergence: bool = True
    funding_reversal: bool = True
    cvd_divergence: bool = True
    session_killzone: bool = True
    breaker_block: bool = True
    turtle_soup: bool = True

    def enabled_setup_ids(self) -> tuple[str, ...]:
        enabled: list[str] = []
        for setup_id in _ALL_SETUP_IDS:
            if bool(getattr(self, setup_id, False)):
                enabled.append(setup_id)
        return tuple(enabled)


class ScoringConfig(BaseModel):
    """Weights for the simplified structure-based scoring engine."""

    enabled: bool = True
    setup_prior_weight: float = Field(default=0.65, ge=0.0, le=1.0)
    weight_mtf_alignment: float = Field(default=0.25, ge=0.0, le=1.0)
    weight_volume_quality: float = Field(default=0.20, ge=0.0, le=1.0)
    weight_structure_clarity: float = Field(default=0.20, ge=0.0, le=1.0)
    weight_risk_reward: float = Field(default=0.15, ge=0.0, le=1.0)
    weight_crowd_position: float = Field(default=0.10, ge=0.0, le=1.0)
    weight_oi_momentum: float = Field(default=0.10, ge=0.0, le=1.0)
    funding_rate_extreme: float = Field(default=0.0010, ge=0.0, le=0.02)
    funding_rate_moderate: float = Field(default=0.0005, ge=0.0, le=0.01)

    @model_validator(mode="after")
    def _validate_weights_not_effectively_disabled(self) -> "ScoringConfig":
        if not self.enabled:
            return self
        weights = {
            "weight_mtf_alignment": float(self.weight_mtf_alignment),
            "weight_volume_quality": float(self.weight_volume_quality),
            "weight_structure_clarity": float(self.weight_structure_clarity),
            "weight_risk_reward": float(self.weight_risk_reward),
            "weight_crowd_position": float(self.weight_crowd_position),
            "weight_oi_momentum": float(self.weight_oi_momentum),
        }
        total_weight = sum(weights.values())
        if abs(total_weight - 1.0) > 1e-6:
            fields_set = getattr(self, "model_fields_set", set())
            missing_in_config = sorted(k for k in weights if k not in fields_set)
            hint = ""
            if missing_in_config:
                hint = f" (missing in config: {', '.join(missing_in_config)})"
            pretty = ", ".join(f"{k}={v:.2f}" for k, v in weights.items())
            raise ValueError(
                "ScoringConfig: weight_* fields must sum to 1.0, "
                f"got {total_weight:.2f}{hint}. "
                f"Current weights: {pretty}"
            )
        return self


class AlertConfig(BaseModel):
    """Pre-alert funnel configuration.

    Designed for a signal-only bot (no auto-trading):
    - Watch alerts fire only when price enters an "interest zone"
    - Entry-zone alerts are optional and rate-limited
    - Invalidations are suppressed until the watch is old enough (anti-spam)
    """

    enabled: bool = Field(default=False)
    enable_entry_zone: bool = Field(default=True)

    # Zones are centered on the chosen structural level.
    watch_interest_pct: float = Field(default=0.015, ge=0.001, le=0.10)
    entry_zone_pct: float = Field(default=0.003, ge=0.0005, le=0.05)

    # Invalidation requires price to move beyond the interest zone by this buffer.
    invalidate_buffer_pct: float = Field(default=0.005, ge=0.0, le=0.10)

    # Anti-spam limits.
    max_watch_alerts_per_hour: int = Field(default=3, ge=0, le=1000)
    watch_symbol_cooldown_minutes: int = Field(default=60, ge=0, le=1440)
    entry_symbol_cooldown_minutes: int = Field(default=15, ge=0, le=1440)

    # Suppress invalidated alerts until the watch lives long enough.
    invalidated_min_age_minutes: int = Field(default=10, ge=0, le=240)

    # Hard expiry for a watch state (prevents stale alerts hours later).
    watch_expiry_minutes: int = Field(default=60, ge=5, le=1440)


class MLConfig(BaseModel):
    """Machine learning configuration for signal enhancement."""

    enabled: bool = False
    use_ml_in_live: bool = False
    ml_confidence_threshold: float = Field(default=0.5, ge=0.0, le=1.0)
    model_dir: str = "models"
    model_type: str = "xgboost"  # xgboost, lightgbm, sklearn
    auto_retrain: bool = False
    retrain_interval_hours: int = Field(default=24, ge=1, le=168)


class IntelligenceConfig(BaseModel):
    """Public-only analytics, guardrails, and AI-agent telemetry."""

    enabled: bool = True
    runtime_mode: Literal["signal_only"] = "signal_only"
    source_policy: Literal["binance_only"] = "binance_only"
    smart_exit_mode: Literal["heuristic_v1"] = "heuristic_v1"
    gamma_semantics: Literal["proxy_only"] = "proxy_only"
    refresh_interval_seconds: int = Field(default=900, ge=60, le=86400)
    write_hourly_reports: bool = True
    options_expiry_count: int = Field(default=2, ge=1, le=8)
    barrier_symbol_count: int = Field(default=2, ge=1, le=8)
    hard_barrier_pct: float = Field(default=1.5, ge=0.1, le=20.0)
    hard_barrier_window_minutes: int = Field(default=15, ge=5, le=240)
    smart_exit_enabled: bool = True
    smart_exit_threshold: float = Field(default=0.62, ge=0.0, le=1.0)
    max_consecutive_stop_losses: int = Field(default=3, ge=1, le=20)
    stop_loss_pause_hours: int = Field(default=5, ge=0, le=168)
    benchmark_symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    option_underlyings: tuple[str, ...] = ("BTC", "ETH")
    macro_symbols: tuple[str, ...] = ("^VIX", "DX-Y.NYB", "^TNX", "^GSPC")

    @field_validator("benchmark_symbols")
    @classmethod
    def _normalize_benchmark_symbols(
        cls, value: tuple[str, ...] | list[str] | None
    ) -> tuple[str, ...]:
        return tuple(str(item).strip().upper() for item in (value or ()) if str(item).strip())

    @field_validator("option_underlyings")
    @classmethod
    def _normalize_option_underlyings(
        cls, value: tuple[str, ...] | list[str] | None
    ) -> tuple[str, ...]:
        return tuple(str(item).strip().upper() for item in (value or ()) if str(item).strip())

    @field_validator("macro_symbols")
    @classmethod
    def _normalize_macro_symbols(
        cls, value: tuple[str, ...] | list[str] | None
    ) -> tuple[str, ...]:
        return tuple(str(item).strip() for item in (value or ()) if str(item).strip())


class WSConfig(BaseModel):
    """WebSocket configuration.

    Runtime policy:
    - `bookTicker` is routed over `/public`
    - klines, aggTrade, mark price, ticker, and liquidation streams are routed over `/market`
    - aggTrade, when enabled, is restricted to tracked/active symbols only
    """

    enabled: bool = True
    base_url: str = "wss://fstream.binance.com"
    public_base_url: str = "wss://fstream.binance.com/public"
    market_base_url: str = "wss://fstream.binance.com/market"
    # 4h stays REST-authoritative; WS carries 5m/15m/1h live trigger/context data.
    kline_intervals: tuple[str, ...] = ("5m", "15m", "1h")
    subscription_scope: str = "shortlist"
    subscribe_book_ticker: bool = True
    subscribe_agg_trade: bool = True
    subscribe_chunk_size: int = Field(default=10, ge=5, le=200)  # Reduced for Binance limits
    subscribe_chunk_delay_ms: int = Field(default=500, ge=50, le=2000)  # Increased for stability
    health_check_silence_seconds: float = Field(default=60.0, ge=10.0, le=300.0)
    market_reconnect_grace_seconds: float = Field(default=90.0, ge=60.0, le=120.0)
    agg_trade_window_seconds: int = Field(default=300, ge=10, le=3600)
    kline_cache_size: int = Field(default=300, ge=50, le=1000)
    warmup_timeout_seconds: float = Field(default=60.0, ge=5.0, le=300.0)
    reconnect_max_delay_seconds: float = Field(default=300.0, ge=1.0, le=3600.0)
    max_agg_trade_buffer: int = Field(default=10000, ge=100, le=100000)
    rest_timeout_seconds: float = Field(default=10.0, ge=1.0, le=120.0)
    backfill_failure_cooldown_seconds: int = Field(default=900, ge=0, le=86400)
    # Global market streams (no per-symbol subscription needed)
    subscribe_market_streams: bool = True
    market_ticker_freshness_seconds: float = Field(default=120.0, ge=10.0, le=600.0)
    force_order_window_seconds: int = Field(default=60, ge=10, le=3600)
    # Intra-candle analysis throttle (seconds between analysis per symbol)
    # Lower = more real-time, higher = less CPU load
    intra_candle_throttle_seconds: float = Field(default=60.0, ge=1.0, le=300.0)

    @field_validator("base_url", "public_base_url", "market_base_url")
    @classmethod
    def _normalize_ws_base_url(cls, value: str) -> str:
        raw = str(value or "wss://fstream.binance.com").strip().rstrip("/")
        if raw.endswith("/stream"):
            raw = raw.removesuffix("/stream")
        if raw.endswith("/ws"):
            raw = raw.removesuffix("/ws")
        return raw

    @field_validator("subscription_scope")
    @classmethod
    def _normalize_subscription_scope(cls, value: str) -> str:
        raw = str(value or "shortlist").strip().lower()
        if raw not in {"tracked_only", "shortlist"}:
            raise ValueError("ws.subscription_scope must be one of: tracked_only, shortlist")
        return raw

    @field_validator("kline_intervals")
    @classmethod
    def _normalize_intervals(cls, value: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
        return tuple(str(v).strip() for v in (value or ()) if str(v).strip())

    @model_validator(mode="after")
    def _resolve_endpoint_urls(self) -> "WSConfig":
        default_root = "wss://fstream.binance.com"
        default_public = f"{default_root}/public"
        default_market = f"{default_root}/market"
        root = self.base_url
        derived_public = self.public_base_url
        derived_market = self.market_base_url
        if root != default_root:
            if "/public" in root:
                root_base = root.replace("/public", "")
                derived_public = root
                derived_market = f"{root_base}/market"
            elif "/market" in root:
                root_base = root.replace("/market", "")
                derived_public = f"{root_base}/public"
                derived_market = root
            else:
                derived_public = f"{root}/public"
                derived_market = f"{root}/market"
        if self.public_base_url == default_public:
            self.public_base_url = derived_public
        if self.market_base_url == default_market:
            self.market_base_url = derived_market
        return self

    def endpoint_base_url(self, endpoint_class: str) -> str:
        if endpoint_class == "public":
            return self.public_base_url
        if endpoint_class == "market":
            return self.market_base_url
        raise ValueError(f"unsupported ws endpoint_class={endpoint_class}")


class BotSettings(BaseModel):
    tg_token: str
    target_chat_id: str
    data_dir: Path = Path("data") / "bot"
    config_path: Path = Path("config.toml")
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    universe: UniverseConfig = Field(default_factory=UniverseConfig)
    filters: FilterConfig = Field(default_factory=FilterConfig)
    tracking: TrackingConfig = Field(default_factory=TrackingConfig)
    setups: SetupConfig = Field(default_factory=SetupConfig)
    ws: WSConfig = Field(default_factory=WSConfig)
    scoring: ScoringConfig = Field(default_factory=ScoringConfig)
    alerts: AlertConfig = Field(default_factory=AlertConfig)
    ml: MLConfig = Field(default_factory=MLConfig)
    intelligence: IntelligenceConfig = Field(default_factory=IntelligenceConfig)

    @property
    def telemetry_dir(self) -> Path:
        return self.data_dir / cast(str, self.runtime.telemetry_subdir)

    @property
    def logs_dir(self) -> Path:
        return self.data_dir / "logs"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "bot.db"

    @property
    def features_store_file(self) -> Path:
        return self.data_dir / "features_store.json"

    @property
    def pid_file(self) -> Path:
        return self.data_dir / "bot.pid"

    @property
    def ml_dir(self) -> Path:
        return self.data_dir / "ml"

    @property
    def log_level(self) -> str:
        return cast(str, self.runtime.log_level)

    @field_validator("tg_token")
    @classmethod
    def _validate_tg_token(cls, value: str) -> str:
        token = str(value or "").strip()
        # Telegram tokens format: 123456789:ABCdefGHIjklMNOpqrsTUVwxyZ
        # Allow alphanumerics, underscore, hyphen, and colon
        if token:
            allowed_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_:-")
            if not all(c in allowed_chars for c in token):
                raise ValueError("tg_token contains invalid characters")
        return token

    @field_validator("target_chat_id")
    @classmethod
    def _validate_target_chat_id(cls, value: str) -> str:
        chat_id = str(value or "").strip()
        if chat_id:
            # Can be numeric ID or channel username (starts with @)
            if chat_id.startswith("@"):
                if len(chat_id) < 2:
                    raise ValueError("target_chat_id channel username too short")
            elif not chat_id.lstrip("-").isdigit():
                raise ValueError("target_chat_id must be numeric ID or @channel")
        return chat_id

    @model_validator(mode="after")
    def _validate_timing_coherence(self) -> "BotSettings":
        cooldown = cast(int, self.filters.cooldown_minutes)
        pending = cast(int, self.tracking.pending_expiry_minutes)
        if cooldown > pending:
            raise ValueError(
                f"cooldown_minutes ({cooldown}) must be <= "
                f"pending_expiry_minutes ({pending})"
            )
        return self

    @model_validator(mode="after")
    def _validate_kline_intervals(self) -> "BotSettings":
        """Validate kline intervals are supported by Binance."""
        valid_intervals = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"}
        intervals = cast(list[str], self.ws.kline_intervals)
        for interval in intervals:
            if interval not in valid_intervals:
                raise ValueError(f"Invalid kline interval: {interval}. Valid: {valid_intervals}")
        return self

    def validate_for_runtime(self, *, require_telegram: bool) -> None:
        """Validate settings for runtime execution."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        # Check data_dir is writable
        test_file = self.data_dir / ".write_test"
        try:
            test_file.write_text("test")
            test_file.unlink()
        except Exception as exc:
            raise ValueError(f"data_dir is not writable: {self.data_dir} ({exc})")

        if require_telegram:
            if not self.tg_token.strip():
                raise ValueError("TG_TOKEN is required for runtime (set in .env)")
            if not self.target_chat_id.strip():
                raise ValueError("TARGET_CHAT_ID is required for runtime (set in .env)")
            if len(self.tg_token) < 20:
                raise ValueError("TG_TOKEN looks too short (expected ~46 chars)")


def _load_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("rb") as handle:
        parsed = tomllib.load(handle)
    return parsed if isinstance(parsed, dict) else {}


def _flatten_legacy_strategy_config(config: Mapping[str, Any]) -> dict[str, float]:
    """Flatten nested legacy strategy config into flat numeric overrides."""
    flattened: dict[str, float] = {}
    stack: list[Mapping[str, Any]] = [config]
    while stack:
        current = stack.pop()
        for key, value in current.items():
            if isinstance(value, Mapping):
                stack.append(value)
                continue
            if isinstance(value, bool):
                flattened[str(key)] = float(value)
                continue
            if isinstance(value, (int, float)):
                flattened[str(key)] = float(value)
    return flattened


def _load_legacy_strategy_overrides(config_root: Path) -> dict[str, dict[str, float]]:
    """Load config/strategies/*.toml once and map to filters.setups format."""
    overrides: dict[str, dict[str, float]] = {}
    legacy_dir = config_root / "config" / "strategies"
    if not legacy_dir.exists():
        return overrides
    for file_path in sorted(legacy_dir.glob("*.toml")):
        parsed = _load_toml(file_path)
        if not parsed:
            continue
        setup_id = file_path.stem
        overrides[setup_id] = _flatten_legacy_strategy_config(parsed)
    return overrides


def _convert_toml_dict(d: dict[Any, Any]) -> dict[str, Any]:
    """Convert TOML dict with possible bytes keys to string keys."""
    result: dict[str, Any] = {}
    for k, v in d.items():
        key = k.decode() if isinstance(k, bytes) else str(k)
        if isinstance(v, dict):
            result[key] = _convert_toml_dict(v)
        elif isinstance(v, list):
            result[key] = [_convert_toml_dict(i) if isinstance(i, dict) else i for i in v]
        else:
            result[key] = v
    return result


def load_settings(config_path: str | Path = "config.toml") -> BotSettings:
    load_dotenv()
    config_file = Path(config_path)
    parsed = _load_toml(config_file)
    bot_raw = parsed.get("bot") if isinstance(parsed.get("bot"), dict) else {}
    payload = _convert_toml_dict(cast(dict[Any, Any], bot_raw))
    payload["tg_token"] = os.getenv("TG_TOKEN", "")
    payload["target_chat_id"] = os.getenv("TARGET_CHAT_ID", "")
    payload["config_path"] = config_file
    payload.setdefault("data_dir", Path("data") / "bot")
    filters_payload = payload.setdefault("filters", {})
    if not isinstance(filters_payload, dict):
        filters_payload = {}
        payload["filters"] = filters_payload
    setup_overrides = filters_payload.setdefault("setups", {})
    if not isinstance(setup_overrides, dict):
        setup_overrides = {}
        filters_payload["setups"] = setup_overrides
    legacy_overrides = _load_legacy_strategy_overrides(config_file.parent)
    for setup_id, legacy_params in legacy_overrides.items():
        existing = setup_overrides.get(setup_id)
        if isinstance(existing, dict):
            setup_overrides[setup_id] = {**legacy_params, **existing}
        else:
            setup_overrides[setup_id] = dict(legacy_params)
    settings = BotSettings.model_validate(payload)
    return settings
