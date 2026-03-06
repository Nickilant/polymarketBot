from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from polymarket_bot.models import InsiderSignal, MarketView, ProbabilitySignal
from polymarket_bot.translator import RuTranslator


class Analyzer:
    def __init__(
        self,
        translator: RuTranslator,
        insider_min_trade_usd: float,
        probability_gap_threshold: float,
        probability_min_value: float,
    ) -> None:
        self._translator = translator
        self._insider_min_trade_usd = insider_min_trade_usd
        self._probability_gap_threshold = probability_gap_threshold
        self._probability_min_value = probability_min_value

    def insider_signals(
        self,
        trades: list[dict[str, Any]],
        markets: list[MarketView],
        top_n: int,
    ) -> list[InsiderSignal]:
        market_by_id = {m.market_id: m for m in markets}
        max_trade_by_key: dict[tuple[str, str, str], float] = defaultdict(float)
        last_trade: dict[tuple[str, str, str], dict[str, Any]] = {}

        for trade in trades:
            market_id = str(trade.get("market") or trade.get("marketId") or "")
            wallet = str(trade.get("maker") or trade.get("trader") or trade.get("wallet") or "")
            outcome = str(trade.get("outcome") or trade.get("side") or "").strip() or "N/A"
            if not market_id or not wallet:
                continue
            try:
                size = float(trade.get("usdcSize") or trade.get("size") or trade.get("amount") or 0)
            except (TypeError, ValueError):
                continue

            key = (market_id, wallet, outcome)
            trade_size = abs(size)
            if trade_size >= max_trade_by_key[key]:
                max_trade_by_key[key] = trade_size
                last_trade[key] = trade

        signals: list[InsiderSignal] = []
        for key, max_trade_size in max_trade_by_key.items():
            if max_trade_size < self._insider_min_trade_usd:
                continue
            market_id, wallet, outcome = key
            market = market_by_id.get(market_id)
            if not market:
                continue
            trade = last_trade[key]
            price = float(trade.get("price") or trade.get("outcomePrice") or 0.5)
            name_ru = self._translator.translate(market.market_name)
            signals.append(
                InsiderSignal(
                    market_id=market_id,
                    market_name_en=market.market_name,
                    market_name_ru=name_ru,
                    wallet=self._short_wallet(wallet),
                    amount_usd=max_trade_size,
                    outcome=outcome,
                    price=price,
                )
            )

        signals.sort(key=lambda x: x.amount_usd, reverse=True)
        return signals[:top_n]

    def probability_signals(self, markets: list[MarketView], top_n: int) -> list[ProbabilitySignal]:
        signals = self._collect_probability_signals(markets)
        signals.sort(key=lambda x: (x.gap, x.leading_probability), reverse=True)
        return signals[:top_n]

    def hot_signals(self, markets: list[MarketView], top_n: int) -> list[ProbabilitySignal]:
        now = datetime.now(timezone.utc)
        deadline = now + timedelta(days=5)
        eligible = [
            market
            for market in markets
            if market.end_datetime and now <= market.end_datetime <= deadline
        ]
        signals = self._collect_probability_signals(eligible)
        signals.sort(key=lambda x: (x.gap, x.leading_probability), reverse=True)
        return signals[:top_n]

    def _collect_probability_signals(self, markets: list[MarketView]) -> list[ProbabilitySignal]:
        signals: list[ProbabilitySignal] = []
        seen_markets: set[str] = set()
        for market in markets:
            if market.market_id in seen_markets:
                continue
            paired = list(zip(market.outcomes, market.probabilities))
            if len(paired) < 2:
                continue
            paired.sort(key=lambda item: item[1], reverse=True)
            lead_outcome, lead_prob = paired[0]
            _, second_prob = paired[1]
            gap = lead_prob - second_prob
            if lead_prob < self._probability_min_value or gap < self._probability_gap_threshold:
                continue

            win = self._win_if_one_dollar(lead_prob)
            if win < 0.1:
                continue

            name_ru = self._translator.translate(market.market_name)
            seen_markets.add(market.market_id)
            signals.append(
                ProbabilitySignal(
                    market_id=market.market_id,
                    market_name_en=market.market_name,
                    market_name_ru=name_ru,
                    leading_outcome=lead_outcome,
                    leading_probability=lead_prob,
                    second_probability=second_prob,
                    gap=gap,
                    win_if_1_dollar=win,
                    market_url=market.market_url,
                )
            )
        return signals

    @staticmethod
    def _win_if_one_dollar(probability: float) -> float:
        probability = max(0.01, min(0.99, probability))
        gross = 1.0 / probability
        return gross - 1.0

    @staticmethod
    def _short_wallet(wallet: str) -> str:
        if len(wallet) <= 12:
            return wallet
        return f"{wallet[:6]}...{wallet[-4:]}"
