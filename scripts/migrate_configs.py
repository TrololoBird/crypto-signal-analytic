#!/usr/bin/env python3
"""Mass migration script to add config_loader imports to all strategies."""
import re
from pathlib import Path

STRATEGIES_DIR = Path("bot/strategies")

# Pattern to find the imports section
IMPORT_PATTERN = r"(from \.\.config import BotSettings)"

# Replacement with config_loader import
IMPORT_REPLACEMENT = r"\1\nfrom ..config_loader import load_strategy_config, get_nested"

# Pattern to find detect method start
DETECT_PATTERN = r"(    def detect\(self, prepared: PreparedSymbol, settings: BotSettings\) -> Signal \| None:\n)(        work_\S+ = prepared\.work_\S+\n)+"

# Config loading code to insert
CONFIG_LOADING = '''\1\2
        # Load config-driven parameters
        config = load_strategy_config("{strategy_name}")
'''

def migrate_strategy(filepath: Path) -> bool:
    """Add config_loader import to a strategy file."""
    content = filepath.read_text(encoding="utf-8")
    
    # Check if already migrated
    if "config_loader" in content:
        print(f"  Already migrated: {filepath.name}")
        return False
    
    # Add import
    if "from ..config import BotSettings" in content:
        content = re.sub(IMPORT_PATTERN, IMPORT_REPLACEMENT, content)
        filepath.write_text(content, encoding="utf-8")
        print(f"  Migrated imports: {filepath.name}")
        return True
    else:
        print(f"  No BotSettings import found: {filepath.name}")
        return False

def main():
    """Migrate all strategy files."""
    strategies = [
        "ema_bounce.py",
        "session_killzone.py", 
        "liquidity_sweep.py",
        "order_block.py",
        "hidden_divergence.py",
        "funding_reversal.py",
        "cvd_divergence.py",
        "breaker_block.py",
        "structure_break_retest.py",
        "bos_choch.py",
        "fvg.py",
    ]
    
    print("Migrating strategy files...")
    migrated = 0
    
    for name in strategies:
        filepath = STRATEGIES_DIR / name
        if filepath.exists():
            if migrate_strategy(filepath):
                migrated += 1
        else:
            print(f"  File not found: {name}")
    
    print(f"\nTotal migrated: {migrated}/{len(strategies)}")

if __name__ == "__main__":
    main()
