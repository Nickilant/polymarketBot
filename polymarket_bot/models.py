from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class InsiderSignal:
    market_id: str
    market_name_en: str
    market_name_ru: str
    wallet: str
    amount_usd: float
    outcome: str
    price: float


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
