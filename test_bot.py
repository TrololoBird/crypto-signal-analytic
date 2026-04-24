#!/usr/bin/env python
"""Test script to verify bot can start without errors"""
import sys
import traceback

print("=" * 60)
print("TESTING BOT IMPORTS")
print("=" * 60)

try:
    print("\n1. Testing config import...")
    from bot.config import BotSettings
    print("   [OK] BotSettings imported")
    
    print("\n2. Testing models import...")
    from bot.models import Signal, PreparedSymbol
    print("   [OK] Signal has adx_1h:", hasattr(Signal, 'adx_1h'))
    print("   [OK] Signal has risk_reward:", hasattr(Signal, 'risk_reward'))
    print("   [OK] Signal has trend_direction:", hasattr(Signal, 'trend_direction'))
    
    print("\n3. Testing setups import...")
    from bot.setups import _build_signal
    print("   [OK] _build_signal imported")
    
    print("\n4. Testing strategies import...")
    from bot.strategies.wick_trap_reversal import WickTrapReversal
    print("   [OK] WickTrapReversal imported")
    
    print("\n" + "=" * 60)
    print("ALL IMPORTS SUCCESSFUL!")
    print("=" * 60)
    
except Exception as e:
    print(f"\n[ERROR] {e}")
    traceback.print_exc()
    sys.exit(1)
