from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    telegram_token: str
    admin_chat_id: int
    analysis_mode: str
    polling_interval_seconds: int
    insider_min_trade_usd: float
    insider_top_n: int
    probability_top_n: int
    hot_top_n: int
    probability_gap_threshold: float
    probability_min_value: float
    polymarket_markets_url: str
    polymarket_trades_url: str
    subscriptions_db: str
    telegram_payments_provider_token: str


def load_settings() -> Settings:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN is required")

    admin_chat_id = os.getenv("ADMIN_CHAT_ID", "").strip()
    if not admin_chat_id:
        raise ValueError("ADMIN_CHAT_ID is required")

    mode = os.getenv("ANALYSIS_MODE", "both").strip().lower()
    if mode not in {"insider", "probability", "hot", "both"}:
        raise ValueError("ANALYSIS_MODE must be one of: insider, probability, hot, both")

    base_dir = Path(__file__).resolve().parent.parent
    env_db = os.getenv("SUBSCRIPTIONS_DB", "").strip() or os.getenv("SUBSCRIPTIONS_FILE", "").strip()
    subscriptions_db = str((Path(env_db).expanduser() if env_db else base_dir / "subscriptions.db").resolve())

    return Settings(
        telegram_token=token,
        admin_chat_id=int(admin_chat_id),
        analysis_mode=mode,
        polling_interval_seconds=int(os.getenv("POLLING_INTERVAL_SECONDS", "3600")),
        insider_min_trade_usd=float(os.getenv("INSIDER_MIN_TRADE_USD", "5000")),
        insider_top_n=int(os.getenv("INSIDER_TOP_N", "3")),
        probability_top_n=int(os.getenv("PROBABILITY_TOP_N", "10")),
        hot_top_n=int(os.getenv("HOT_TOP_N", "10")),
        probability_gap_threshold=float(os.getenv("PROBABILITY_GAP_THRESHOLD", "0.35")),
        probability_min_value=float(os.getenv("PROBABILITY_MIN_VALUE", "0.7")),
        polymarket_markets_url=os.getenv(
            "POLYMARKET_MARKETS_URL", "https://gamma-api.polymarket.com/markets"
        ),
        polymarket_trades_url=os.getenv(
            "POLYMARKET_TRADES_URL", "https://data-api.polymarket.com/trades"
        ),
        subscriptions_db=subscriptions_db,
        telegram_payments_provider_token=os.getenv("TELEGRAM_PAYMENTS_PROVIDER_TOKEN", "").strip(),
    )
