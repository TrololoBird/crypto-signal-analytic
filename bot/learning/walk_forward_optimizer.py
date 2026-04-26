from __future__ import annotations

from typing import Any


class WalkForwardOptimizer:
    """Simple walk-forward evaluator for parameter candidates."""

    def __init__(self, min_fold_size: int = 20) -> None:
        self.min_fold_size = max(10, int(min_fold_size))

    def evaluate(self, outcomes: list[dict[str, Any]], params: dict[str, float]) -> float:
        if len(outcomes) < self.min_fold_size:
            return -1.0

        fold_size = max(self.min_fold_size, len(outcomes) // 3)
        scores: list[float] = []
        for start in range(0, len(outcomes), fold_size):
            fold = outcomes[start : start + fold_size]
            if len(fold) < self.min_fold_size:
                continue
            scores.append(self._score_fold(fold, params))
        if not scores:
            return -1.0
        return sum(scores) / len(scores)

    def _score_fold(self, outcomes: list[dict[str, Any]], params: dict[str, float]) -> float:
        filtered = []
        min_score = params.get("base_score", 0.5) * 0.9
        for outcome in outcomes:
            if float(outcome.get("score", 0.5) or 0.5) < min_score:
                continue
            filtered.append(outcome)
        if len(filtered) < 5:
            return 0.0

        wins = sum(1 for row in filtered if row.get("outcome") == "win")
        losses = sum(1 for row in filtered if row.get("outcome") == "loss")
        total = wins + losses
        if total == 0:
            return 0.0
        win_rate = wins / total

        r_values: list[float] = []
        for row in filtered:
            if row.get("outcome") == "win":
                entry = float(row.get("entry", 0.0) or 0.0)
                stop = float(row.get("stop", 0.0) or 0.0)
                tp1_hit = float(row.get("tp1_hit", 0.0) or 0.0)
                risk = abs(entry - stop)
                if risk <= 1e-9:
                    continue
                r_values.append(abs(tp1_hit - entry) / risk)
            else:
                r_values.append(-1.0)

        avg_r = (sum(r_values) / len(r_values)) if r_values else 0.0
        expectancy = win_rate * avg_r - (1 - win_rate) * 1.0
        confidence = min(1.0, len(filtered) / 100.0)
        return expectancy * confidence
