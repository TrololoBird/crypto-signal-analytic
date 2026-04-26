"""Environment-backed runtime secrets.

Centralizes secret loading so config/model code doesn't duplicate env parsing.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Secrets:
    tg_token: str
    target_chat_id: str
    binance_api_key: str
    binance_api_secret: str


def load_secrets() -> Secrets:
    load_dotenv()
    tg_token = (os.getenv("TG_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    target_chat_id = (os.getenv("TARGET_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    return Secrets(
        tg_token=tg_token,
        target_chat_id=target_chat_id,
        binance_api_key=(os.getenv("BINANCE_API_KEY") or "").strip(),
        binance_api_secret=(os.getenv("BINANCE_API_SECRET") or "").strip(),
    )
