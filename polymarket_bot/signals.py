from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


# ── Типы сигналов ─────────────────────────────────────────────────────────────

class SignalKind(str, Enum):
    WHALE_TRADE           = "WHALE_TRADE"
    WHALE_POSITION        = "WHALE_POSITION"
    POSITION_SWEEP        = "POSITION_SWEEP"
    COORDINATED           = "COORDINATED"
    CLUSTERED_COORD       = "CLUSTERED_COORD"
    CROSS_MARKET_INSIDER  = "CROSS_MARKET_INSIDER"
    LIQUIDITY_SWEEP       = "LIQUIDITY_SWEEP"
    STEALTH_ACCUMULATION  = "STEALTH_ACCUMULATION"


# ── Базовые очки за тип сигнала ───────────────────────────────────────────────

SIGNAL_BASE_SCORES: dict[SignalKind, int] = {
    SignalKind.WHALE_TRADE:           1,
    SignalKind.WHALE_POSITION:        2,
    SignalKind.POSITION_SWEEP:        3,
    SignalKind.COORDINATED:           3,
    SignalKind.CLUSTERED_COORD:       4,
    SignalKind.CROSS_MARKET_INSIDER:  4,
    SignalKind.LIQUIDITY_SWEEP:       3,
    SignalKind.STEALTH_ACCUMULATION:  5,
}

STRONG_SIGNAL_THRESHOLD = 5


# ── Модель сигнала ────────────────────────────────────────────────────────────

@dataclass
class RichSignal:
    """
    Единый формат для всех типов сигналов.
    Используется в форматировании Telegram-сообщений.
    """
    kind: SignalKind
    wallet: str                        # сокращённый кошелёк или "N кошельков"
    market_id: str
    market_name_en: str
    market_name_ru: str
    market_url: str
    outcome: str
    side: str                          # BUY | SELL | UNKNOWN
    usd_value: float                   # крупнейшая сделка в сигнале
    total_volume: float                # суммарный объём
    trade_count: int
    price: float
    timestamp: float                   # unix ts последней сделки
    signal_score: int = 0
    is_strong: bool = False

    # Дополнительные поля для специфичных типов
    signal_strength: int = 0           # для CROSS_MARKET_INSIDER (1-3)
    markets_involved: list[str] = field(default_factory=list)   # CROSS_MARKET_INSIDER
    price_change_pct: float = 0.0      # LIQUIDITY_SWEEP
    window_sec: int = 0                # для координации/sweep

    @classmethod
    def build(
        cls,
        kind: SignalKind,
        wallet: str,
        market_id: str,
        market_name_en: str,
        market_name_ru: str,
        market_url: str,
        outcome: str,
        side: str,
        usd_value: float,
        total_volume: float,
        trade_count: int,
        price: float,
        timestamp: float,
        extra_score: int = 0,
        **kwargs,
    ) -> "RichSignal":
        base = SIGNAL_BASE_SCORES.get(kind, 0)
        score = base + extra_score
        return cls(
            kind=kind,
            wallet=wallet,
            market_id=market_id,
            market_name_en=market_name_en,
            market_name_ru=market_name_ru,
            market_url=market_url,
            outcome=outcome,
            side=side,
            usd_value=usd_value,
            total_volume=total_volume,
            trade_count=trade_count,
            price=price,
            timestamp=timestamp,
            signal_score=score,
            is_strong=score >= STRONG_SIGNAL_THRESHOLD,
            **kwargs,
        )
