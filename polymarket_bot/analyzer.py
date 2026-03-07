from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import logging
from typing import Any

from polymarket_bot.models import InsiderSignal, MarketView, ProbabilitySignal
from polymarket_bot.translator import RuTranslator

logger = logging.getLogger(__name__)


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
        market_by_id: dict[str, MarketView] = {}
        for market in markets:
            if market.market_id:
                market_by_id[market.market_id] = market
            if getattr(market, "condition_id", ""):
                market_by_id[market.condition_id] = market

        market_by_name = {m.market_name.strip().lower(): m for m in markets}

        wallet_stats: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(
            lambda: {"total_volume": 0.0, "trade_count": 0, "max_trade": 0.0, "last_trade": None}
        )
        wallet_totals: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "total_volume": 0.0,
                "trade_count": 0,
                "max_trade": 0.0,
                "last_trade": None,
                "market_ids": set(),
                "market_totals": defaultdict(float),
            }
        )

        for trade in trades:
            market_id = str(
                trade.get("conditionId")
                or trade.get("market")
                or trade.get("marketId")
                or ""
            )
            wallet = str(
                trade.get("maker")
                or trade.get("trader")
                or trade.get("wallet")
                or trade.get("proxyWallet")
                or ""
            )
            outcome = (
                trade.get("outcome")
                or trade.get("side")
                or str(trade.get("outcomeIndex"))
                or "N/A"
            )
            outcome = str(outcome).strip() or "N/A"
            if not market_id or not wallet:
                continue

            trade_size = self._trade_size_usd(trade)
            if trade_size is None:
                continue

            key = (market_id, wallet, outcome)
            stats = wallet_stats[key]
            stats["total_volume"] += trade_size
            stats["trade_count"] += 1
            if trade_size >= stats["max_trade"]:
                stats["max_trade"] = trade_size
                stats["last_trade"] = trade

            wallet_summary = wallet_totals[wallet]
            wallet_summary["total_volume"] += trade_size
            wallet_summary["trade_count"] += 1
            wallet_summary["market_ids"].add(market_id)
            wallet_summary["market_totals"][market_id] += trade_size
            if trade_size >= wallet_summary["max_trade"]:
                wallet_summary["max_trade"] = trade_size
                wallet_summary["last_trade"] = trade

        signals: list[InsiderSignal] = []
        for key, stats in wallet_stats.items():
            total_volume = stats["total_volume"]
            if total_volume < self._insider_min_trade_usd:
                continue

            market_id, wallet, outcome = key
            market = market_by_id.get(market_id)
            trade = stats["last_trade"] or {}
            if not market:
                logger.debug("Unmatched trade market id: %s", market_id)
                title = str(trade.get("title") or "").strip().lower()
                if title:
                    market = market_by_name.get(title)
            if not market:
                continue

            price = float(trade.get("price") or trade.get("outcomePrice") or 0.5)
            name_ru = self._translator.translate(market.market_name)
            signals.append(
                InsiderSignal(
                    market_id=market.market_id,
                    market_name_en=market.market_name,
                    market_name_ru=name_ru,
                    wallet=self._short_wallet(wallet),
                    amount_usd=stats["max_trade"],
                    total_volume=total_volume,
                    trade_count=stats["trade_count"],
                    is_whale=total_volume >= 50000,
                    outcome=outcome,
                    price=price,
                    market_url=market.market_url,
                )
            )

        for wallet, stats in wallet_totals.items():
            total_volume = float(stats["total_volume"])
            unique_markets = len(stats["market_ids"])
            market_totals: dict[str, float] = dict(stats["market_totals"])
            largest_market_volume = max(market_totals.values(), default=0.0)

            # Дополнительно отслеживаем кошельки, которые набирают крупный объём
            # суммой мелких ставок на разных рынках.
            if (
                total_volume < self._insider_min_trade_usd
                or unique_markets < 2
                or largest_market_volume >= self._insider_min_trade_usd
            ):
                continue

            trade = stats["last_trade"] or {}
            market_id = str(
                trade.get("conditionId")
                or trade.get("market")
                or trade.get("marketId")
                or ""
            )
            market = market_by_id.get(market_id)
            if not market:
                title = str(trade.get("title") or "").strip().lower()
                if title:
                    market = market_by_name.get(title)
            if not market:
                continue

            outcome = (
                trade.get("outcome")
                or trade.get("side")
                or str(trade.get("outcomeIndex"))
                or "N/A"
            )
            price = float(trade.get("price") or trade.get("outcomePrice") or 0.5)
            name_ru = self._translator.translate(market.market_name)
            signals.append(
                InsiderSignal(
                    market_id=market.market_id,
                    market_name_en=market.market_name,
                    market_name_ru=f"{name_ru} (активность кита на {unique_markets} рынках)",
                    wallet=self._short_wallet(wallet),
                    amount_usd=float(stats["max_trade"]),
                    total_volume=total_volume,
                    trade_count=int(stats["trade_count"]),
                    is_whale=True,
                    outcome=str(outcome).strip() or "N/A",
                    price=price,
                    market_url=market.market_url,
                )
            )

        signals.sort(key=lambda x: (x.total_volume, x.amount_usd, x.trade_count), reverse=True)
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
    def _trade_size_usd(trade: dict[str, Any]) -> float | None:
        for raw in (trade.get("usdcSize"), trade.get("amount")):
            if raw in (None, ""):
                continue
            try:
                return abs(float(raw))
            except (TypeError, ValueError):
                continue

        size_raw = trade.get("size")
        if size_raw not in (None, ""):
            try:
                size = abs(float(size_raw))
            except (TypeError, ValueError):
                return None

            price_raw = trade.get("price") or trade.get("outcomePrice")
            if price_raw not in (None, ""):
                try:
                    price = float(price_raw)
                except (TypeError, ValueError):
                    return None
                return size * price

            return size

        return None

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
