from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from difflib import SequenceMatcher

from information_arbitrage.config import Settings
from information_arbitrage.instruments.registry import InstrumentRegistry
from information_arbitrage.monitor.storage import MarketStore
from information_arbitrage.models import (
    Broker,
    Direction,
    HeadlineEvent,
    RiskSnapshot,
    ScoredHeadline,
    TradeDecision,
    Urgency,
)
from information_arbitrage.strategy.market_state import RollingPriceBuffer
from information_arbitrage.strategy.sizing import calculate_order_quantity, confidence_contracts


class StrategyEngine:
    def __init__(
        self,
        settings: Settings,
        registry: InstrumentRegistry,
        store: MarketStore,
        price_buffer: RollingPriceBuffer,
    ) -> None:
        self.settings = settings
        self.registry = registry
        self.store = store
        self.price_buffer = price_buffer

    def generate_trades(
        self,
        headline: HeadlineEvent,
        score: ScoredHeadline,
        risk_snapshot: RiskSnapshot,
        now: datetime | None = None,
    ) -> list[TradeDecision]:
        evaluation_time = now or datetime.now(UTC)
        if not self._headline_is_fresh(headline, evaluation_time):
            return []
        if risk_snapshot.total_open_positions >= self.settings.max_open_positions:
            return []
        if risk_snapshot.daily_pnl <= -(risk_snapshot.account_equity * self.settings.daily_loss_limit_fraction):
            return []

        effective_urgency = self._adjust_urgency(score.urgency, headline)
        exposure_map = {
            (position.broker.value, position.symbol.upper()): position
            for position in risk_snapshot.positions
            if position.quantity != 0
        }
        decisions: list[TradeDecision] = []
        planned_symbols: set[tuple[str, str]] = set()

        for signal in score.instruments:
            if signal.confidence < self.settings.confidence_threshold:
                continue
            instrument = self.registry.resolve(signal.symbol)
            if instrument is None:
                continue
            if self._already_open(instrument.symbol, instrument.broker, exposure_map):
                continue
            if len(exposure_map) + len(planned_symbols) >= self.settings.max_open_positions:
                continue

            price_assessment = self._assess_price_action(instrument, signal.direction)
            if not price_assessment.ready or price_assessment.skip:
                continue

            contracts = confidence_contracts(signal.confidence)
            confirmations = self.store.count_recent_confirmations(
                instrument.symbol,
                signal.direction.value,
                self.settings.confirmation_window_minutes,
            )
            if confirmations > 0:
                contracts = min(3, contracts + 1)
            if price_assessment.reduce:
                contracts = max(1, contracts - 1)

            decisions.append(
                self._build_decision(
                    headline=headline,
                    score=score,
                    instrument=instrument,
                    direction=signal.direction,
                    confidence=signal.confidence,
                    urgency=effective_urgency,
                    contracts=contracts,
                    price_assessment=price_assessment,
                    reason_suffix=f"confirmations={confirmations}",
                    evaluation_time=evaluation_time,
                )
            )
            planned_symbols.add((instrument.broker.value, instrument.symbol))

            for related in self._pair_expansion(instrument.symbol):
                if len(exposure_map) + len(planned_symbols) >= self.settings.max_open_positions:
                    break
                key = (related.broker.value, related.symbol)
                if key in planned_symbols or self._already_open(related.symbol, related.broker, exposure_map):
                    continue
                related_price = self._assess_price_action(related, signal.direction)
                if not related_price.ready or related_price.skip:
                    continue
                decisions.append(
                    self._build_decision(
                        headline=headline,
                        score=score,
                        instrument=related,
                        direction=signal.direction,
                        confidence=max(self.settings.confidence_threshold, signal.confidence - 0.1),
                        urgency=effective_urgency,
                        contracts=max(1, contracts - 1),
                        price_assessment=related_price,
                        strategy_name="pairs-arbitrage",
                        reason_suffix=f"paired-with={instrument.symbol}",
                        evaluation_time=evaluation_time,
                    )
                )
                planned_symbols.add(key)

        return decisions

    def _headline_is_fresh(self, headline: HeadlineEvent, now: datetime) -> bool:
        age_seconds = (now - headline.published_at).total_seconds()
        if age_seconds > self.settings.max_headline_age_seconds:
            return False
        normalized = self._normalize_text(headline.text)
        for recent in self.store.recent_scored_headlines(self.settings.stale_similarity_window_minutes):
            if recent.id == headline.id:
                continue
            if recent.dedupe_key == headline.dedupe_key:
                return False
            similarity = SequenceMatcher(None, normalized, self._normalize_text(recent.text)).ratio()
            if similarity >= 0.82:
                return False
        return True

    @staticmethod
    def _already_open(
        symbol: str,
        broker: Broker,
        exposure_map,
    ) -> bool:
        return (broker.value, symbol.upper()) in exposure_map

    def _pair_expansion(self, symbol: str):
        if symbol not in {"PETR4", "PBR", "VALE", "VALE3"}:
            return []
        return self.registry.related(symbol)[:1]

    def _adjust_urgency(self, urgency: Urgency, headline: HeadlineEvent) -> Urgency:
        if headline.source_kind == "ib_news" and (headline.provider or "").upper().startswith("DJ"):
            return Urgency.CRITICAL
        if headline.source_kind == "rss":
            return {
                Urgency.CRITICAL: Urgency.HIGH,
                Urgency.HIGH: Urgency.MEDIUM,
                Urgency.MEDIUM: Urgency.LOW,
                Urgency.LOW: Urgency.LOW,
            }[urgency]
        return urgency

    def _assess_price_action(self, instrument, direction: Direction):
        reference_symbol = instrument.price_reference_symbol or instrument.symbol
        assessment = self.price_buffer.assess_move(reference_symbol, direction)
        assessment.reference_symbol = reference_symbol
        return assessment

    def _build_decision(
        self,
        headline: HeadlineEvent,
        score: ScoredHeadline,
        instrument,
        direction: Direction,
        confidence: float,
        urgency: Urgency,
        contracts: int,
        price_assessment,
        evaluation_time: datetime,
        strategy_name: str | None = None,
        reason_suffix: str | None = None,
    ) -> TradeDecision:
        age_seconds = max(0.0, (evaluation_time - headline.published_at).total_seconds())
        strategy = strategy_name or self._select_strategy(headline, instrument.symbol)
        quantity = calculate_order_quantity(instrument, contracts)
        atr = price_assessment.atr or (price_assessment.current_price or 0.0) * 0.003
        stop_loss, take_profit = self._exit_prices(price_assessment.current_price or 0.0, atr, direction)
        time_exit_minutes = 15 if urgency in {Urgency.HIGH, Urgency.CRITICAL} else 30
        reason = (
            f"{headline.source} signal on {headline.source_kind} feed routed to {instrument.broker.value}"
            if reason_suffix is None
            else f"{headline.source} signal on {headline.source_kind} feed routed to {instrument.broker.value}; {reason_suffix}"
        )
        return TradeDecision(
            headline_id=headline.id,
            symbol=instrument.symbol,
            broker=instrument.broker,
            direction=direction,
            quantity=quantity,
            confidence=confidence,
            urgency=urgency,
            time_horizon=score.time_horizon,
            strategy_name=strategy,
            reason=reason,
            reference_price=price_assessment.current_price,
            atr=atr,
            stop_loss=stop_loss,
            take_profit=take_profit,
            time_exit_at=evaluation_time + timedelta(minutes=time_exit_minutes),
            metadata={
                "age_seconds": age_seconds,
                "speed_window": age_seconds <= 30.0,
                "headline_source": headline.source,
                "headline_kind": headline.source_kind,
                "category": score.category.value,
                "contracts": contracts,
                "price_reference_symbol": price_assessment.reference_symbol,
                "price_5m_ago": price_assessment.price_5m_ago,
                "directional_move_5m": price_assessment.directional_move,
                "price_assessment": price_assessment.reason,
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

    @staticmethod
    def _exit_prices(entry_price: float, atr: float, direction: Direction) -> tuple[float, float]:
        stop_distance = 1.5 * atr
        take_distance = 2.5 * atr
        if direction == Direction.LONG:
            return entry_price - stop_distance, entry_price + take_distance
        return entry_price + stop_distance, entry_price - take_distance

    @staticmethod
    def _normalize_text(text: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", " ", text.lower())).strip()
