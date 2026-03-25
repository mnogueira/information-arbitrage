from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import requests

from information_arbitrage.collectors.base import BaseCollector
from information_arbitrage.config import Settings
from information_arbitrage.models import HeadlineEvent


class PolymarketCollector(BaseCollector):
    endpoint = "https://gamma-api.polymarket.com/markets"

    def __init__(self, settings: Settings) -> None:
        super().__init__(
            name="polymarket",
            source_kind="polymarket",
            poll_interval_seconds=settings.polymarket_poll_interval_seconds,
        )
        self.settings = settings
        self._session = requests.Session()
        self._previous_probabilities: dict[str, float] = {}

    async def stop(self) -> None:
        self._session.close()

    async def collect_once(self) -> list[HeadlineEvent]:
        markets = await asyncio.to_thread(self._fetch_markets)
        headlines: list[HeadlineEvent] = []
        for market in markets:
            question = str(market.get("question") or "").strip()
            if not question or not self._matches_keywords(question, market):
                continue

            market_id = str(market.get("id") or market.get("slug") or question)
            current_probability = self._extract_probability(market)
            if current_probability is None:
                continue

            previous_probability = self._previous_probabilities.get(market_id)
            self._previous_probabilities[market_id] = current_probability

            if previous_probability is None:
                one_hour_change = float(market.get("oneHourPriceChange") or 0.0)
                if abs(one_hour_change) < 0.05:
                    continue
                delta = one_hour_change
            else:
                delta = current_probability - previous_probability
                if abs(delta) < 0.05:
                    continue

            direction = "up" if delta >= 0 else "down"
            headlines.append(
                HeadlineEvent(
                    source="Polymarket",
                    source_kind=self.source_kind,
                    text=(
                        f"Polymarket shift: {question} moved {direction} "
                        f"{abs(delta) * 100:.1f} pts to {current_probability * 100:.1f}%"
                    ),
                    published_at=datetime.now(UTC),
                    url=f"https://polymarket.com/event/{market.get('slug')}" if market.get("slug") else None,
                    metadata={
                        "market_id": market_id,
                        "slug": market.get("slug"),
                        "delta_probability": delta,
                        "last_trade_price": current_probability,
                        "one_hour_price_change": market.get("oneHourPriceChange"),
                        "best_bid": market.get("bestBid"),
                        "best_ask": market.get("bestAsk"),
                        "question": question,
                    },
                )
            )
        return headlines

    def _fetch_markets(self) -> list[dict]:
        params = {
            "limit": self.settings.polymarket_limit,
            "active": "true",
            "closed": "false",
        }
        response = self._session.get(self.endpoint, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, list) else []

    def _matches_keywords(self, question: str, market: dict) -> bool:
        haystacks = [
            question.lower(),
            str(market.get("description") or "").lower(),
            str(market.get("category") or "").lower(),
        ]
        text = " ".join(haystacks)
        return any(keyword.lower() in text for keyword in self.settings.polymarket_keywords)

    @staticmethod
    def _extract_probability(market: dict) -> float | None:
        for key in ("lastTradePrice", "bestBid", "bestAsk"):
            value = market.get(key)
            if value is None:
                continue
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue
            if numeric > 1:
                numeric /= 100.0
            return max(0.0, min(1.0, numeric))
        return None
