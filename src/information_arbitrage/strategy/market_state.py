from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pandas as pd

from information_arbitrage.models import Direction


@dataclass(slots=True)
class PriceAssessment:
    reference_symbol: str
    current_price: float | None
    price_5m_ago: float | None
    directional_move: float | None
    atr: float | None
    ready: bool
    skip: bool = False
    reduce: bool = False
    reason: str | None = None


class RollingPriceBuffer:
    def __init__(self, retention_minutes: int = 10) -> None:
        self.retention = timedelta(minutes=retention_minutes)
        self._prices: dict[str, deque[tuple[datetime, float]]] = defaultdict(deque)

    def update(self, symbol: str, price: float, observed_at: datetime | None = None) -> None:
        timestamp = observed_at or datetime.now(UTC)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        bucket = self._prices[symbol.upper()]
        bucket.append((timestamp, float(price)))
        self._trim(symbol.upper(), timestamp)

    def latest_price(self, symbol: str) -> float | None:
        bucket = self._prices.get(symbol.upper())
        return bucket[-1][1] if bucket else None

    def price_minutes_ago(self, symbol: str, minutes: int = 5) -> float | None:
        bucket = self._prices.get(symbol.upper())
        if not bucket:
            return None
        target = bucket[-1][0] - timedelta(minutes=minutes)
        candidate: float | None = None
        for observed_at, price in bucket:
            if observed_at <= target:
                candidate = price
            else:
                break
        return candidate

    def atr(self, symbol: str) -> float | None:
        bucket = self._prices.get(symbol.upper())
        if not bucket:
            return None
        frame = pd.DataFrame(bucket, columns=["observed_at", "price"]).set_index("observed_at")
        ohlc = frame["price"].resample("1min").ohlc().dropna()
        latest = float(frame["price"].iloc[-1])
        if len(ohlc) < 2:
            return latest * 0.003
        previous_close = ohlc["close"].shift(1)
        true_ranges = pd.concat(
            [
                (ohlc["high"] - ohlc["low"]).abs(),
                (ohlc["high"] - previous_close).abs(),
                (ohlc["low"] - previous_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        return float(true_ranges.tail(min(14, len(true_ranges))).mean())

    def assess_move(self, symbol: str, direction: Direction, minutes: int = 5) -> PriceAssessment:
        current_price = self.latest_price(symbol)
        price_5m_ago = self.price_minutes_ago(symbol, minutes=minutes)
        atr = self.atr(symbol)
        if current_price is None or price_5m_ago is None or price_5m_ago == 0:
            return PriceAssessment(
                reference_symbol=symbol.upper(),
                current_price=current_price,
                price_5m_ago=price_5m_ago,
                directional_move=None,
                atr=atr,
                ready=False,
                reason="missing-5m-price-history",
            )
        pct_move = (current_price - price_5m_ago) / price_5m_ago
        directional_move = pct_move if direction == Direction.LONG else -pct_move
        if directional_move > 0.005:
            return PriceAssessment(
                reference_symbol=symbol.upper(),
                current_price=current_price,
                price_5m_ago=price_5m_ago,
                directional_move=directional_move,
                atr=atr,
                ready=True,
                skip=True,
                reason="already-priced-in",
            )
        if directional_move > 0.0025:
            return PriceAssessment(
                reference_symbol=symbol.upper(),
                current_price=current_price,
                price_5m_ago=price_5m_ago,
                directional_move=directional_move,
                atr=atr,
                ready=True,
                reduce=True,
                reason="partially-priced-in",
            )
        return PriceAssessment(
            reference_symbol=symbol.upper(),
            current_price=current_price,
            price_5m_ago=price_5m_ago,
            directional_move=directional_move,
            atr=atr,
            ready=True,
            reason="fresh-price-window",
        )

    def _trim(self, symbol: str, reference_time: datetime) -> None:
        bucket = self._prices[symbol]
        cutoff = reference_time - self.retention
        while bucket and bucket[0][0] < cutoff:
            bucket.popleft()
