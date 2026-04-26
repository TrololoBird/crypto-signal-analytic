from __future__ import annotations

import random

import polars as pl

from bot.ml.signal_classifier import SignalClassifier


def _dataset(rows: int = 140) -> tuple[pl.DataFrame, pl.Series]:
    random.seed(42)
    data: dict[str, list[float]] = {name: [] for name in SignalClassifier.FEATURES}
    labels: list[int] = []
    for i in range(rows):
        base = 0.2 + (i / rows) * 0.6
        for name in SignalClassifier.FEATURES:
            noise = random.uniform(-0.1, 0.1)
            data[name].append(base + noise)
        labels.append(1 if base > 0.5 else 0)
    return pl.DataFrame(data), pl.Series("label", labels, dtype=pl.Int8)


def test_signal_classifier_train_predict_and_load(tmp_path) -> None:
    features, labels = _dataset()
    model_dir = tmp_path / "models"

    clf = SignalClassifier(model_dir=model_dir, model_type="rf")
    clf.train(features, labels)
    assert clf.model_path.exists()

    sample = features.row(0, named=True)
    prob = clf.predict_proba(sample)
    assert 0.0 <= prob <= 1.0

    loaded = SignalClassifier(model_dir=model_dir, model_type="rf")
    assert loaded.load()
    prob_loaded = loaded.predict_proba(sample)
    assert 0.0 <= prob_loaded <= 1.0
    assert loaded.get_feature_importance()


def test_signal_classifier_centroid_mode(tmp_path) -> None:
    features, labels = _dataset()
    model_dir = tmp_path / "models"

    clf = SignalClassifier(model_dir=model_dir, model_type="centroid")
    clf.train(features, labels)
    sample = features.row(1, named=True)
    prob = clf.predict_proba(sample)
    assert 0.0 <= prob <= 1.0


def test_signal_classifier_unknown_type_falls_back(tmp_path) -> None:
    features, labels = _dataset()
    model_dir = tmp_path / "models"
    clf = SignalClassifier(model_dir=model_dir, model_type="definitely-unknown")
    clf.train(features, labels)
    assert clf.model is not None
