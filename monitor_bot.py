#!/usr/bin/env python
"""Monitor bot startup and log all output"""
import sys
import subprocess
import os
from datetime import datetime

log_file = "bot_monitor.log"

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line)

# Clear old log
with open(log_file, "w", encoding="utf-8") as f:
    f.write("")

log("=" * 60)
log("STARTING BOT MONITOR")
log("=" * 60)

try:
    # Try importing bot
    log("Testing imports...")
    from bot.config import BotSettings
    log("[OK] BotSettings imported")
    
    from bot.models import Signal
    log(f"[OK] Signal has adx_1h: {hasattr(Signal, 'adx_1h')}")
    log(f"[OK] Signal has risk_reward: {hasattr(Signal, 'risk_reward')}")
    
    from bot.setups import _build_signal
    log("[OK] _build_signal imported")
    
    log("\nAll imports successful! Starting bot...")
    log("=" * 60)
    
    # Start the bot
    import bot.cli
    bot.cli.run()
    
except Exception as e:
    log(f"[ERROR] {type(e).__name__}: {e}")
    import traceback
    log(traceback.format_exc())
    sys.exit(1)
