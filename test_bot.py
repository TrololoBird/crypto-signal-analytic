#!/usr/bin/env python
"""Smoke checks for critical imports.

This module can be executed directly as a script and can also be safely
collected by pytest without executing import checks at import time.
"""

import traceback


def run_import_smoke_check() -> None:
    print("=" * 60)
    print("TESTING BOT IMPORTS")
    print("=" * 60)

    print("\n1. Testing config import...")
    from bot.config import BotSettings
    print("   [OK] BotSettings imported:", BotSettings is not None)

    print("\n2. Testing models import...")
    from bot.models import PreparedSymbol, Signal
    print("   [OK] Signal has adx_1h:", hasattr(Signal, "adx_1h"))
    print("   [OK] Signal has risk_reward:", hasattr(Signal, "risk_reward"))
    print("   [OK] Signal has trend_direction:", hasattr(Signal, "trend_direction"))
    print("   [OK] PreparedSymbol imported:", PreparedSymbol is not None)

    print("\n3. Testing setups import...")
    from bot.setups import _build_signal
    print("   [OK] _build_signal imported:", _build_signal is not None)

    print("\n4. Testing strategies import...")
    from bot.strategies.wick_trap_reversal import WickTrapReversal
    print("   [OK] WickTrapReversal imported:", WickTrapReversal is not None)

    print("\n" + "=" * 60)
    print("ALL IMPORTS SUCCESSFUL!")
    print("=" * 60)


def test_import_smoke_check() -> None:
    run_import_smoke_check()


if __name__ == "__main__":
    try:
        run_import_smoke_check()
    except Exception as error:  # pragma: no cover - CLI failure path
        print(f"\n[ERROR] {error}")
        traceback.print_exc()
        raise SystemExit(1) from error
