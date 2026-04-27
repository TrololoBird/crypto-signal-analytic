from __future__ import annotations


def depth_imbalance_from_delta_ratio(delta_ratio: float | None) -> float | None:
    """Return directional imbalance proxy in [-1, 1] from signed delta ratio."""
    if delta_ratio is None:
        return None
    return round(max(-1.0, min(1.0, float(delta_ratio))), 4)


def microprice_bias_from_book(
    *,
    bid: float | None,
    ask: float | None,
    delta_ratio: float | None,
) -> float | None:
    """Return signed microprice bias proxy in [-1, 1] from book + delta ratio."""
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return None
    spread = ask - bid
    mid = (bid + ask) / 2.0
    if mid <= 0 or spread <= 0:
        return None
    if delta_ratio is None:
        return None
    return round(max(-1.0, min(1.0, float(delta_ratio))), 4)
