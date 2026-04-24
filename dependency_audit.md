# Dependency Audit

## Classified Dependencies
- required runtime: aiohttp, aiosqlite, aiogram, binance-connector, python-binance, binance-sdk-derivatives-trading-usds-futures, msgspec, numpy, orjson, polars, polars_ta, prometheus-client, pydantic, python-dotenv, structlog, tenacity, websockets
- optional runtime: fastapi, uvicorn, joblib, xgboost, optuna
- dev/test: pytest, pytest-asyncio, pytest-xdist, pyright, ruff, pre-commit
- stale/dead in the checked environment: black/pathspec mismatch and stock-indicators/pythonnet mismatch came from the ambient interpreter, not from repo requirement files

## Requirement Files
- requirements.txt: 20 declared entries
- requirements-modern.txt: 23 declared entries
- requirements-frozen.txt: 20 declared entries
- requirements-optional.txt: 5 declared entries

## External Verification References
- Binance USDⓈ-M futures WebSocket market streams: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Connect
- Binance options endpoint inventory (exchangeInfo/openInterest/mark): https://www.binance.com/en/skills/detail/binance/derivatives-trading-options
- PyPI aiogram release metadata: https://pypi.org/project/aiogram/
- PyPI websockets release metadata: https://pypi.org/project/websockets/

## Imported Top-Level Modules Seen On Runtime Files
- __future__, bot, typing, logging, datetime, dataclasses, polars, asyncio, math, collections, json, pathlib, time, hashlib, os, enum, re, contextlib, numpy, msgspec

## Notes
- `bot.dashboard` degrades cleanly when FastAPI/Uvicorn are absent, but the optional dependency is now explicit.
- `bot.ml_filter` imports `joblib` only when live ML scoring is enabled.
- `bot.core.self_learner` imports `optuna` lazily and remains out of the live runtime path.