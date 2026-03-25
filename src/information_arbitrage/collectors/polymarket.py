from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import requests

from information_arbitrage.collectors.base import BaseCollector
from information_arbitrage.models import HeadlineEvent


class PolymarketCollector(BaseCollector):
    def __init__(self, keywords: list[str], poll_interval_seconds: float, limit: int = 200) -> None:
        super().__init__(name="polymarket", source_kind="polymarket", poll_interval_seconds=poll_interval_seconds)
        self.keywords = [keyword.lower() for keyword in keywords]
        self.limit = limit
        self.endpoint = "https://gamma-api.polymarket.com/markets"
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "information-arbitrage/0.1"})
        self._price_memory: dict[str, float] = {}

    async def collect_once(self) -> list[HeadlineEvent]:
        markets = await asyncio.to_thread(self._fetch_markets)
        headlines: list[HeadlineEvent] = []

        for market in markets:
            market_id = str(market.get("id") or market.get("conditionId") or market.get("slug") or "")
            question = str(market.get("question") or market.get("title") or "").strip()
            if not market_id or not question or not self._matches_keywords(question, market):
                continue

            price = self._extract_probability(market)
            if price is None:
                continue

            previous = self._price_memory.get(market_id)
            one_hour_change = self._coerce_float(market.get("oneHourPriceChange"))
            delta = (price - previous) if previous is not None else one_hour_change
            self._price_memory[market_id] = price

            if delta is None or abs(delta) < 0.05:
                continue

            direction = "up" if delta > 0 else "down"
            headlines.append(
                HeadlineEvent(
                    source="Polymarket",
                    source_kind=self.source_kind,
                    provider="gamma-api.polymarket.com",
                    text=f"Polymarket probability shift {direction}: {question} moved {delta:+.1%} to {price:.1%}",
                    published_at=datetime.now(UTC),
                    url=f"https://polymarket.com/event/{market.get('slug')}" if market.get("slug") else None,
                    metadata={
                        "market_id": market_id,
                        "question": question,
                        "current_probability": price,
                        "delta_probability": delta,
                        "slug": market.get("slug"),
                    },
                )
            )
        return headlines

    def _fetch_markets(self) -> list[dict]:
        response = self._session.get(
            self.endpoint,
            params={"limit": self.limit, "active": "true", "closed": "false"},
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, list) else payload.get("markets", [])

    def _matches_keywords(self, question: str, market: dict) -> bool:
        haystacks = [
            question.lower(),
            str(market.get("description") or "").lower(),
            " ".join(str(tag).lower() for tag in market.get("tags", [])),
        ]
        return any(keyword in haystack for haystack in haystacks for keyword in self.keywords)

    @staticmethod
    def _coerce_float(value: object) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _extract_probability(self, market: dict) -> float | None:
        for field in ("lastTradePrice", "price", "probability", "bestAsk"):
            value = self._coerce_float(market.get(field))
            if value is not None:
                return value

        best_bid = self._coerce_float(market.get("bestBid"))
        best_ask = self._coerce_float(market.get("bestAsk"))
        if best_bid is not None and best_ask is not None:
            return (best_bid + best_ask) / 2
        return None
