from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from polymarket_bot.models import MarketView


class PolymarketClient:
    def __init__(self, markets_url: str, trades_url: str) -> None:
        self._markets_url = markets_url
        self._trades_url = trades_url

    async def fetch_markets(self) -> list[MarketView]:
        params = {
            "active": "true",
            "closed": "false",
            "limit": 500,
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(self._markets_url, params=params)
            response.raise_for_status()
            data = response.json()

        if not isinstance(data, list):
            return []

        markets: list[MarketView] = []
        for item in data:
            market = self._parse_market(item)
            if market:
                markets.append(market)
        return markets

    async def fetch_recent_trades(self) -> list[dict[str, Any]]:
        since = datetime.now(tz=timezone.utc) - timedelta(hours=1)
        params = {
            "limit": 500,
            "offset": 0,
            "startTime": since.isoformat().replace("+00:00", "Z"),
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(self._trades_url, params=params)
            response.raise_for_status()
            data = response.json()
        return data if isinstance(data, list) else []

    def _parse_market(self, item: dict[str, Any]) -> MarketView | None:
        market_id = str(item.get("id") or item.get("questionID") or "")
        name = str(item.get("question") or item.get("title") or "").strip()
        if not market_id or not name:
            return None

        outcomes = self._parse_list(item.get("outcomes"))
        prices = self._parse_probabilities(item)

        if not outcomes or not prices or len(outcomes) != len(prices):
            return None

        return MarketView(
            market_id=market_id,
            market_name=name,
            outcomes=outcomes,
            probabilities=prices,
        )

    @staticmethod
    def _parse_list(raw: Any) -> list[str]:
        if isinstance(raw, list):
            return [str(x).strip() for x in raw if str(x).strip()]
        if isinstance(raw, str):
            text = raw.strip()
            if not text:
                return []
            try:
                decoded = json.loads(text)
                if isinstance(decoded, list):
                    return [str(x).strip() for x in decoded if str(x).strip()]
            except json.JSONDecodeError:
                pass
        return []

    def _parse_probabilities(self, item: dict[str, Any]) -> list[float]:
        for field in ("outcomePrices", "probabilities"):
            values = self._parse_float_list(item.get(field))
            if values:
                return values

        yes = item.get("bestBid") or item.get("lastTradePrice")
        if yes is None:
            return []
        yes_p = float(yes)
        no_p = max(0.0, 1.0 - yes_p)
        return [yes_p, no_p]

    @staticmethod
    def _parse_float_list(raw: Any) -> list[float]:
        values: list[float] = []
        candidate: Any = raw
        if isinstance(raw, str):
            raw = raw.strip()
            if not raw:
                return []
            try:
                candidate = json.loads(raw)
            except json.JSONDecodeError:
                return []

        if not isinstance(candidate, list):
            return []

        for value in candidate:
            try:
                values.append(float(value))
            except (TypeError, ValueError):
                return []

        if not values:
            return []

        if max(values) > 1.0:
            values = [v / 100.0 for v in values]

        normalized = [max(0.0, min(1.0, v)) for v in values]
        return normalized
