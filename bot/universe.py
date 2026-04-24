from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone

from .config import BotSettings
from .models import SymbolMeta, UniverseSymbol


UTC = timezone.utc
STABLE_BASE_ASSETS = {"USDC", "BUSD", "FDUSD", "TUSD", "USDP", "USDS", "DAI"}
_ASCII_CONTRACT_RE = re.compile(r"^[A-Z0-9]{4,24}$")
_ASCII_ASSET_RE = re.compile(r"^[A-Z0-9]{2,16}$")


def _bucket_for_price_change(price_change_pct: float) -> str:
    move = abs(float(price_change_pct))
    if move >= 8.0:
        return "reversal"
    if move >= 2.0:
        return "breakout"
    return "trend"


def _scaled_bucket_targets(total_slots: int) -> dict[str, int]:
    base = {"trend": 12, "breakout": 10, "reversal": 8}
    if total_slots <= 0:
        return {key: 0 for key in base}
    base_total = sum(base.values())
    scaled = {key: int(round(total_slots * weight / base_total)) for key, weight in base.items()}
    assigned = sum(scaled.values())
    if assigned < total_slots:
        for key in ("trend", "breakout", "reversal"):
            if assigned >= total_slots:
                break
            scaled[key] += 1
            assigned += 1
    elif assigned > total_slots:
        for key in ("reversal", "breakout", "trend"):
            while assigned > total_slots and scaled[key] > 0:
                scaled[key] -= 1
                assigned -= 1
    return scaled


def _is_supported_contract_symbol(symbol: str, base_asset: str) -> bool:
    if not _ASCII_CONTRACT_RE.fullmatch(symbol):
        return False
    if not _ASCII_ASSET_RE.fullmatch(base_asset):
        return False
    return True


def _bucket_priority(item: UniverseSymbol) -> tuple[float, float, str]:
    move = abs(item.price_change_pct)
    volume_score = math.log10(max(item.quote_volume, 1.0))
    if item.shortlist_bucket == "trend":
        move_fit = max(0.0, 1.0 - min(move, 2.0) / 2.0)
    elif item.shortlist_bucket == "breakout":
        move_fit = max(0.0, 1.0 - min(abs(move - 4.5) / 4.5, 1.0))
    else:
        move_fit = max(0.0, 1.0 - min(abs(move - 11.0) / 12.0, 1.0))
    quality = round(move_fit * 10.0 + volume_score, 6)
    return quality, item.quote_volume, item.symbol


def build_shortlist(
    symbol_meta: list[SymbolMeta],
    tickers_24h: list[dict[str, float | str]],
    settings: BotSettings,
) -> tuple[list[UniverseSymbol], dict[str, int]]:
    meta_map = {row.symbol: row for row in symbol_meta}
    pinned = {symbol for symbol in settings.universe.pinned_symbols}
    min_onboard = datetime.now(UTC) - timedelta(days=settings.universe.min_listing_age_days)
    min_onboard_ms = int(min_onboard.timestamp() * 1000)
    eligible: list[UniverseSymbol] = []

    for row in tickers_24h:
        symbol = str(row.get("symbol") or "").strip().upper()
        meta = meta_map.get(symbol)
        if meta is None:
            continue
        if not _is_supported_contract_symbol(symbol, meta.base_asset.upper()):
            continue
        if meta.status.upper() != "TRADING":
            continue
        if meta.contract_type.upper() != "PERPETUAL":
            continue
        if meta.quote_asset.upper() != settings.universe.quote_asset:
            continue
        if meta.base_asset.upper() in STABLE_BASE_ASSETS:
            continue
        quote_volume = float(row.get("quote_volume") or 0.0)
        last_price = float(row.get("last_price") or 0.0)
        price_change_pct = float(row.get("price_change_percent") or 0.0)
        if quote_volume <= 0.0 or last_price <= 0.0:
            continue
        if symbol not in pinned:
            if quote_volume < settings.universe.min_quote_volume_usd:
                continue
            if meta.onboard_date_ms and meta.onboard_date_ms > min_onboard_ms:
                continue
        eligible.append(
            UniverseSymbol(
                symbol=symbol,
                base_asset=meta.base_asset,
                quote_asset=meta.quote_asset,
                contract_type=meta.contract_type,
                status=meta.status,
                onboard_date_ms=meta.onboard_date_ms,
                quote_volume=quote_volume,
                price_change_pct=price_change_pct,
                last_price=last_price,
                shortlist_bucket=_bucket_for_price_change(price_change_pct),
            )
        )

    eligible.sort(key=lambda item: _bucket_priority(item), reverse=True)
    pinned_rows = [row for row in eligible if row.symbol in pinned]
    dynamic_pool = [row for row in eligible if row.symbol not in pinned][: settings.universe.dynamic_limit]
    bucket_pool: dict[str, list[UniverseSymbol]] = {"trend": [], "breakout": [], "reversal": []}
    for row in dynamic_pool:
        bucket_pool[row.shortlist_bucket].append(row)
    for bucket in bucket_pool.values():
        bucket.sort(key=lambda item: _bucket_priority(item), reverse=True)

    shortlist: list[UniverseSymbol] = []
    seen: set[str] = set()
    for row in pinned_rows:
        if row.symbol in seen:
            continue
        shortlist.append(row)
        seen.add(row.symbol)

    targets = _scaled_bucket_targets(max(settings.universe.shortlist_limit - len(shortlist), 0))
    summary = {
        "eligible": len(eligible),
        "dynamic_pool": len(dynamic_pool),
        "pinned": len(pinned_rows),
        "trend": 0,
        "breakout": 0,
        "reversal": 0,
        "fill": 0,
    }

    for bucket in ("trend", "breakout", "reversal"):
        for row in bucket_pool[bucket]:
            if len(shortlist) >= settings.universe.shortlist_limit or summary[bucket] >= targets[bucket]:
                break
            if row.symbol in seen:
                continue
            shortlist.append(row)
            seen.add(row.symbol)
            summary[bucket] += 1

    for row in dynamic_pool:
        if len(shortlist) >= settings.universe.shortlist_limit:
            break
        if row.symbol in seen:
            continue
        shortlist.append(row)
        seen.add(row.symbol)
        summary["fill"] += 1

    shortlist.sort(
        key=lambda item: (
            item.symbol not in pinned,
            -_bucket_priority(item)[0],
            -item.quote_volume,
            item.symbol,
        )
    )
    return shortlist, summary
