from __future__ import annotations

from datetime import UTC, datetime

from information_arbitrage.instruments.registry import InstrumentRegistry
from information_arbitrage.models import (
    Broker,
    Direction,
    HeadlineEvent,
    PositionExposure,
    ScoredHeadline,
    TradeDecision,
)
from information_arbitrage.strategy.sizing import calculate_order_quantity


class StrategyEngine:
    def __init__(self, registry: InstrumentRegistry, confidence_threshold: float = 0.6) -> None:
        self.registry = registry
        self.confidence_threshold = confidence_threshold

    def generate_trades(
        self,
        headline: HeadlineEvent,
        score: ScoredHeadline,
        positions: list[PositionExposure],
    ) -> list[TradeDecision]:
        exposure_map = {(position.broker.value, position.symbol.upper()): position for position in positions}
        decisions: list[TradeDecision] = []
        planned_symbols: set[tuple[str, str]] = set()

        for signal in score.instruments:
            if signal.confidence < self.confidence_threshold:
                continue
            instrument = self.registry.resolve(signal.symbol)
            if instrument is None:
                continue

            if self._already_exposed(instrument.symbol, instrument.broker, signal.direction, exposure_map):
                continue

            decisions.append(
                self._build_decision(
                    headline=headline,
                    score=score,
                    instrument=instrument,
                    direction=signal.direction,
                    confidence=signal.confidence,
                )
            )
            planned_symbols.add((instrument.broker.value, instrument.symbol))

            for related in self._pair_expansion(instrument.symbol):
                key = (related.broker.value, related.symbol)
                if key in planned_symbols or self._already_exposed(
                    related.symbol,
                    related.broker,
                    signal.direction,
                    exposure_map,
                ):
                    continue
                decisions.append(
                    self._build_decision(
                        headline=headline,
                        score=score,
                        instrument=related,
                        direction=signal.direction,
                        confidence=max(self.confidence_threshold, signal.confidence - 0.1),
                        strategy_name="pairs-arbitrage",
                        reason=f"Paired with {instrument.symbol} after {headline.source} headline",
                    )
                )
                planned_symbols.add(key)

        return decisions

    @staticmethod
    def _already_exposed(
        symbol: str,
        broker: Broker,
        direction: Direction,
        exposure_map: dict[tuple[str, str], PositionExposure],
    ) -> bool:
        existing = exposure_map.get((broker.value, symbol.upper()))
        return existing is not None and existing.direction == direction

    def _pair_expansion(self, symbol: str):
        if symbol not in {"PETR4", "PBR", "VALE", "VALE3"}:
            return []
        return self.registry.related(symbol)[:1]

    def _build_decision(
        self,
        headline: HeadlineEvent,
        score: ScoredHeadline,
        instrument,
        direction: Direction,
        confidence: float,
        strategy_name: str | None = None,
        reason: str | None = None,
    ) -> TradeDecision:
        now = datetime.now(UTC)
        age_seconds = max(0.0, (now - headline.published_at).total_seconds())
        strategy = strategy_name or self._select_strategy(headline, instrument.symbol)
        return TradeDecision(
            headline_id=headline.id,
            symbol=instrument.symbol,
            broker=instrument.broker,
            direction=direction,
            quantity=calculate_order_quantity(instrument, confidence, score.urgency),
            confidence=confidence,
            urgency=score.urgency,
            time_horizon=score.time_horizon,
            strategy_name=strategy,
            reason=reason
            or f"{headline.source} signal on {headline.source_kind} feed routed to {instrument.broker.value}",
            metadata={
                "age_seconds": age_seconds,
                "speed_window": age_seconds <= 30.0,
                "headline_source": headline.source,
                "headline_kind": headline.source_kind,
                "category": score.category.value,
            },
        )

    def _select_strategy(self, headline: HeadlineEvent, symbol: str) -> str:
        text = headline.text.lower()
        instrument = self.registry.resolve(symbol)
        if headline.source_kind == "polymarket" and "ceasefire" in text and symbol in {"CL", "BRN", "BZ"}:
            return "fade-the-news"
        if headline.source_kind == "ib_news" and instrument and instrument.broker == Broker.MT5:
            return "cross-market"
        return "speed-advantage"
