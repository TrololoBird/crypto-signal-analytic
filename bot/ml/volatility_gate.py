from __future__ import annotations


class VolatilityGate:
    def should_use_ml(self, regime: str, signal_confidence: float) -> bool:
        if regime in ("trending", "strong_uptrend", "strong_downtrend", "bull", "bear"):
            return False
        if regime in ("choppy", "ranging", "neutral", "volatile"):
            return signal_confidence >= 0.55
        return True
