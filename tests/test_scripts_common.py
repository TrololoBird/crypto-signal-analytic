from __future__ import annotations

import importlib

import pytest


def test_scripts_common_import_smoke() -> None:
    module = importlib.import_module("scripts.common")
    assert module is not None


@pytest.mark.parametrize(
    ("args_symbols", "symbols_from_run", "fallback_symbols", "expected"),
    [
        ([" btcusdt ", "ETHUSDT", "btcusdt"], ["SOLUSDT"], ["BNBUSDT"], ["BTCUSDT", "ETHUSDT"]),
        ([], [" solusdt ", "SOLUSDT", "adausdt"], ["BNBUSDT"], ["SOLUSDT", "ADAUSDT"]),
        ([], [], ["bnbusdt", "BNBUSDT", "ethusdt"], ["BNBUSDT", "ETHUSDT"]),
    ],
)
def test_resolve_symbols(
    args_symbols: list[str],
    symbols_from_run: list[str],
    fallback_symbols: list[str],
    expected: list[str],
) -> None:
    common = importlib.import_module("scripts.common")

    assert common.resolve_symbols(args_symbols, symbols_from_run, fallback_symbols) == expected
