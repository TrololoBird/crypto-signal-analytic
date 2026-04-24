#!/usr/bin/env python
"""Check if all dependencies are installed"""
import sys
import os

log_file = "check_result.txt"

def log(msg):
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

# Clear old log
with open(log_file, "w", encoding="utf-8") as f:
    f.write("")

log("=" * 50)
log("CHECKING DEPENDENCIES")
log("=" * 50)

modules = [
    "polars",
    "aiohttp", 
    "toml",
    "numpy",
    "pandas",
    "pydantic"
]

all_ok = True
for mod in modules:
    try:
        __import__(mod)
        log(f"[OK] {mod}")
    except ImportError as e:
        log(f"[FAIL] {mod}: {e}")
        all_ok = False

log("=" * 50)
if all_ok:
    log("ALL DEPENDENCIES INSTALLED!")
else:
    log("MISSING DEPENDENCIES - INSTALL WITH:")
    log("pip install polars aiohttp toml numpy pandas pydantic")
log("=" * 50)

sys.exit(0 if all_ok else 1)
