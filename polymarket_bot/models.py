from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class InsiderSignal:
    market_id: str
    market_name_en: str
    market_name_ru: str
    wallet: str
    amount_usd: float
    outcome: str
    price: float
    total_volume: float = 0.0
    trade_count: int = 0
    is_whale: bool = False


@dataclass(frozen=True)
class ProbabilitySignal:
    market_id: str
    market_name_en: str
    market_name_ru: str
    leading_outcome: str
    leading_probability: float
    second_probability: float
    gap: float
    win_if_1_dollar: float
    market_url: str


@dataclass(frozen=True)
class MarketView:
    market_id: str
    market_name: str
    outcomes: list[str]
    probabilities: list[float]
    market_url: str
    end_datetime: datetime | None
    condition_id: str = ""
