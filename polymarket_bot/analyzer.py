from __future__ import annotations

from collections import defaultdict
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
        aggregate: dict[tuple[str, str, str], float] = defaultdict(float)
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
            aggregate[key] += abs(size)
            last_trade[key] = trade

        signals: list[InsiderSignal] = []
        for key, total in aggregate.items():
            if total < self._insider_min_trade_usd:
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
                    amount_usd=total,
                    outcome=outcome,
                    price=price,
                )
            )

        signals.sort(key=lambda x: x.amount_usd, reverse=True)
        return signals[:top_n]

    def probability_signals(self, markets: list[MarketView], top_n: int) -> list[ProbabilitySignal]:
        signals: list[ProbabilitySignal] = []
        for market in markets:
            paired = list(zip(market.outcomes, market.probabilities))
            if len(paired) < 2:
                continue
            paired.sort(key=lambda item: item[1], reverse=True)
            lead_outcome, lead_prob = paired[0]
            _, second_prob = paired[1]
            gap = lead_prob - second_prob
            if lead_prob < self._probability_min_value or gap < self._probability_gap_threshold:
                continue

            name_ru = self._translator.translate(market.market_name)
            win = self._win_if_one_dollar(lead_prob)
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
                )
            )

        signals.sort(key=lambda x: (x.gap, x.leading_probability), reverse=True)
        return signals[:top_n]

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
