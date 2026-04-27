from __future__ import annotations

from pathlib import Path

from bot.config import BotSettings
from bot.ml.filter import MLFilter
from bot.ml.signal_classifier import SignalClassifier


def test_signal_classifier_model_kind_reports_centroid_baseline(tmp_path: Path) -> None:
    classifier = SignalClassifier(model_dir=tmp_path, model_type="centroid")
    classifier.model = classifier._build_model()
    assert classifier.model_kind() == "centroid_baseline"


def test_ml_filter_disables_live_when_only_centroid_baseline_available(monkeypatch, tmp_path: Path) -> None:
    settings = BotSettings(tg_token="1" * 30, target_chat_id="123")
    settings.ml.enabled = True
    settings.ml.use_ml_in_live = True
    settings.data_dir = tmp_path

    monkeypatch.setattr("bot.ml.signal_classifier.SignalClassifier.load", lambda self: True)
    monkeypatch.setattr("bot.ml.signal_classifier.SignalClassifier.model_kind", lambda self: "centroid_baseline")
    monkeypatch.setattr("pathlib.Path.glob", lambda self, pattern: [])

    ml_filter = MLFilter(settings)
    assert ml_filter.enabled is False
