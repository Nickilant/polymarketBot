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
        market_by_condition_id: dict[str, MarketView] = {
            market.condition_id: market
            for market in markets
            if market.condition_id
        }

        by_wallet_market_outcome: dict[tuple[str, str, int], dict[str, Any]] = defaultdict(
            lambda: {"total_volume": 0.0, "trade_count": 0, "max_trade": 0.0, "last_trade": None}
        )
        by_wallet_market: dict[tuple[str, str], dict[str, Any]] = defaultdict(
            lambda: {
                "total_volume": 0.0,
                "trade_count": 0,
                "max_trade": 0.0,
                "last_trade": None,
                "outcome_totals": defaultdict(float),
            }
        )
        by_wallet: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "total_volume": 0.0,
                "trade_count": 0,
                "max_trade": 0.0,
                "last_trade": None,
                "markets": set(),
                "market_totals": defaultdict(float),
            }
        )

        for trade in trades:
            if str(trade.get("type") or "").upper() != "TRADE":
                continue

            wallet = str(trade.get("proxyWallet") or "").strip()
            market_id = str(trade.get("conditionId") or "").strip()
            outcome_index = self._outcome_index(trade.get("outcomeIndex"))
            trade_size = self._trade_size_usd(trade)

            if not wallet or not market_id or outcome_index is None or trade_size is None:
                continue

            market_outcome_key = (wallet, market_id, outcome_index)
            market_key = (wallet, market_id)

            stats_outcome = by_wallet_market_outcome[market_outcome_key]
            stats_outcome["total_volume"] += trade_size
            stats_outcome["trade_count"] += 1
            if trade_size >= stats_outcome["max_trade"]:
                stats_outcome["max_trade"] = trade_size
                stats_outcome["last_trade"] = trade

            stats_market = by_wallet_market[market_key]
            stats_market["total_volume"] += trade_size
            stats_market["trade_count"] += 1
            stats_market["outcome_totals"][outcome_index] += trade_size
            if trade_size >= stats_market["max_trade"]:
                stats_market["max_trade"] = trade_size
                stats_market["last_trade"] = trade

            stats_wallet = by_wallet[wallet]
            stats_wallet["total_volume"] += trade_size
            stats_wallet["trade_count"] += 1
            stats_wallet["markets"].add(market_id)
            stats_wallet["market_totals"][market_id] += trade_size
            if trade_size >= stats_wallet["max_trade"]:
                stats_wallet["max_trade"] = trade_size
                stats_wallet["last_trade"] = trade

        signals: list[InsiderSignal] = []

        for (wallet, market_id, outcome_index), stats in by_wallet_market_outcome.items():
            total_volume = float(stats["total_volume"])
            if total_volume < self._insider_min_trade_usd:
                continue

            market = market_by_condition_id.get(market_id)
            if not market:
                logger.debug("Unmatched conditionId: %s", market_id)
                continue

            trade = stats["last_trade"] or {}
            outcome_label = self._outcome_label(trade, outcome_index)
            signals.append(
                self._build_insider_signal(
                    market=market,
                    wallet=wallet,
                    outcome=outcome_label,
                    amount_usd=float(stats["max_trade"]),
                    total_volume=total_volume,
                    trade_count=int(stats["trade_count"]),
                    price=self._trade_price(trade),
                )
            )

        for (wallet, market_id), stats in by_wallet_market.items():
            total_volume = float(stats["total_volume"])
            if total_volume < self._insider_min_trade_usd:
                continue

            if max(stats["outcome_totals"].values(), default=0.0) >= self._insider_min_trade_usd:
                continue

            market = market_by_condition_id.get(market_id)
            if not market:
                logger.debug("Unmatched conditionId: %s", market_id)
                continue

            trade = stats["last_trade"] or {}
            lead_outcome_index = max(stats["outcome_totals"], key=stats["outcome_totals"].get, default=0)
            outcome_label = f"{self._outcome_label(trade, lead_outcome_index)} (серия на одном рынке)"
            signals.append(
                self._build_insider_signal(
                    market=market,
                    wallet=wallet,
                    outcome=outcome_label,
                    amount_usd=float(stats["max_trade"]),
                    total_volume=total_volume,
                    trade_count=int(stats["trade_count"]),
                    price=self._trade_price(trade),
                )
            )

        for wallet, stats in by_wallet.items():
            total_volume = float(stats["total_volume"])
            markets_count = len(stats["markets"])
            largest_market_volume = max(stats["market_totals"].values(), default=0.0)
            if total_volume < self._insider_min_trade_usd or markets_count < 2:
                continue
            if largest_market_volume >= self._insider_min_trade_usd:
                continue

            trade = stats["last_trade"] or {}
            market_id = str(trade.get("conditionId") or "").strip()
            market = market_by_condition_id.get(market_id)
            if not market:
                continue

            outcome_index = self._outcome_index(trade.get("outcomeIndex"))
            outcome_label = self._outcome_label(trade, outcome_index)
            signals.append(
                self._build_insider_signal(
                    market=market,
                    wallet=wallet,
                    outcome=f"{outcome_label} (кит на {markets_count} рынках)",
                    amount_usd=float(stats["max_trade"]),
                    total_volume=total_volume,
                    trade_count=int(stats["trade_count"]),
                    price=self._trade_price(trade),
                    force_whale=True,
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
        raw = trade.get("usdcSize")
        if raw in (None, ""):
            return None
        try:
            return abs(float(raw))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _outcome_index(value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _outcome_label(trade: dict[str, Any], outcome_index: int | None) -> str:
        outcome = str(trade.get("outcome") or "").strip()
        if outcome:
            return outcome
        if outcome_index is None:
            return "N/A"
        return f"outcome #{outcome_index}"

    @staticmethod
    def _trade_price(trade: dict[str, Any]) -> float:
        try:
            return float(trade.get("price") or 0.5)
        except (TypeError, ValueError):
            return 0.5

    def _build_insider_signal(
        self,
        market: MarketView,
        wallet: str,
        outcome: str,
        amount_usd: float,
        total_volume: float,
        trade_count: int,
        price: float,
        force_whale: bool = False,
    ) -> InsiderSignal:
        name_ru = self._translator.translate(market.market_name)
        return InsiderSignal(
            market_id=market.market_id,
            market_name_en=market.market_name,
            market_name_ru=name_ru,
            wallet=self._short_wallet(wallet),
            amount_usd=amount_usd,
            total_volume=total_volume,
            trade_count=trade_count,
            is_whale=force_whale or total_volume >= 50000,
            outcome=outcome,
            price=price,
            market_url=market.market_url,
        )

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
