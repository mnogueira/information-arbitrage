from __future__ import annotations

import json
import logging
import re

from information_arbitrage.config import Settings
from information_arbitrage.instruments.registry import InstrumentRegistry
from information_arbitrage.models import (
    Broker,
    Category,
    Direction,
    HeadlineEvent,
    HeadlineScorePayload,
    InstrumentSignal,
    ScoredHeadline,
    TimeHorizon,
    Urgency,
)

logger = logging.getLogger(__name__)
DJ_SENTIMENT_PATTERN = re.compile(
    r"\{A:(?P<asset_id>[^:}]+):L:(?P<language>[^:}]+):K:(?P<score>[-+]?\d*\.?\d+):C:(?P<confidence>[-+]?\d*\.?\d+)\}"
)

try:
    from openai import AsyncOpenAI
except ImportError:  # pragma: no cover - depends on optional install
    AsyncOpenAI = None


class HeuristicScoringEngine:
    def score(self, headline: HeadlineEvent) -> ScoredHeadline:
        text = headline.text.lower()
        metadata = {"engine": "heuristic"}

        if any(keyword in text for keyword in ["iran", "oil facility", "hormuz", "missile", "bomb", "strike"]):
            instruments = [
                InstrumentSignal(symbol="CL", direction=Direction.LONG, confidence=0.88, broker=Broker.IB),
                InstrumentSignal(symbol="BRN", direction=Direction.LONG, confidence=0.84, broker=Broker.IB),
                InstrumentSignal(symbol="PETR4", direction=Direction.LONG, confidence=0.79, broker=Broker.MT5),
                InstrumentSignal(symbol="PBR", direction=Direction.LONG, confidence=0.76, broker=Broker.IB),
                InstrumentSignal(symbol="AZUL", direction=Direction.SHORT, confidence=0.67, broker=Broker.IB),
                InstrumentSignal(symbol="GC", direction=Direction.LONG, confidence=0.72, broker=Broker.IB),
            ]
            return ScoredHeadline(
                headline_id=headline.id,
                model="heuristic",
                instruments=instruments,
                escalation_score=0.8,
                category=Category.GEOPOLITICAL,
                urgency=Urgency.CRITICAL,
                time_horizon=TimeHorizon.MINUTES,
                metadata=metadata,
            )

        if "ceasefire" in text or "truce" in text or "peace talks" in text:
            instruments = [
                InstrumentSignal(symbol="CL", direction=Direction.SHORT, confidence=0.85, broker=Broker.IB),
                InstrumentSignal(symbol="BRN", direction=Direction.SHORT, confidence=0.82, broker=Broker.IB),
                InstrumentSignal(symbol="AZUL", direction=Direction.LONG, confidence=0.66, broker=Broker.IB),
            ]
            return ScoredHeadline(
                headline_id=headline.id,
                model="heuristic",
                instruments=instruments,
                escalation_score=-0.75,
                category=Category.GEOPOLITICAL,
                urgency=Urgency.HIGH,
                time_horizon=TimeHorizon.MINUTES,
                metadata=metadata,
            )

        if any(keyword in text for keyword in ["cpi", "inflation", "fed", "rate hike", "rate cut"]):
            instruments = [
                InstrumentSignal(symbol="SPY", direction=Direction.SHORT, confidence=0.64, broker=Broker.IB),
                InstrumentSignal(symbol="QQQ", direction=Direction.SHORT, confidence=0.67, broker=Broker.IB),
                InstrumentSignal(symbol="GC", direction=Direction.LONG, confidence=0.61, broker=Broker.IB),
            ]
            return ScoredHeadline(
                headline_id=headline.id,
                model="heuristic",
                instruments=instruments,
                escalation_score=-0.2,
                category=Category.ECONOMIC,
                urgency=Urgency.MEDIUM,
                time_horizon=TimeHorizon.HOURS,
                metadata=metadata,
            )

        return ScoredHeadline(
            headline_id=headline.id,
            model="heuristic",
            instruments=[],
            escalation_score=0.0,
            category=Category.OTHER,
            urgency=Urgency.LOW,
            time_horizon=TimeHorizon.DAYS,
            metadata=metadata,
        )


class HeadlineScoringEngine:
    def __init__(self, settings: Settings, registry: InstrumentRegistry) -> None:
        self.settings = settings
        self.registry = registry
        self._fallback = HeuristicScoringEngine()
        self._client = (
            AsyncOpenAI(api_key=settings.openai_api_key)
            if settings.openai_api_key and AsyncOpenAI is not None
            else None
        )

    async def score(self, headline: HeadlineEvent) -> ScoredHeadline:
        native_score = self._score_from_native_dj_sentiment(headline)
        if native_score is not None:
            return native_score

        if not self._should_use_llm(headline):
            fallback = self._fallback.score(headline)
            fallback.metadata["llm_skipped"] = True
            fallback.metadata["skip_reason"] = "ib-news-without-native-dj-score"
            return fallback

        if self._client is None:
            return self._fallback.score(headline)

        prompt = self._build_prompt(headline)
        try:
            response = await self._client.responses.parse(
                model=self.settings.openai_model,
                reasoning={"effort": self.settings.openai_reasoning_effort},
                input=[
                    {"role": "system", "content": self._system_prompt()},
                    {"role": "user", "content": prompt},
                ],
                text_format=HeadlineScorePayload,
            )
            parsed = getattr(response, "output_parsed", None)
            if parsed is None:
                output_text = getattr(response, "output_text", None)
                if not output_text:
                    raise ValueError("OpenAI response did not include parsed output")
                parsed = HeadlineScorePayload.model_validate_json(output_text)
            return ScoredHeadline(
                headline_id=headline.id,
                model=self.settings.openai_model,
                instruments=parsed.instruments,
                escalation_score=parsed.escalation_score,
                category=parsed.category,
                urgency=parsed.urgency,
                time_horizon=parsed.time_horizon,
                metadata={"engine": "openai"},
            )
        except Exception:
            logger.exception("Falling back to heuristic scoring for headline %s", headline.id)
            fallback = self._fallback.score(headline)
            fallback.metadata["openai_error"] = True
            fallback.metadata["requested_model"] = self.settings.openai_model
            return fallback

    def _score_from_native_dj_sentiment(self, headline: HeadlineEvent) -> ScoredHeadline | None:
        if headline.source_kind != "ib_news":
            return None
        provider = (headline.provider or "").upper()
        if not provider.startswith("DJ"):
            return None

        match = self._extract_native_sentiment(headline)
        if match is None:
            return None

        fallback = self._fallback.score(headline)
        sentiment = float(match["score"])
        native_confidence = float(match["confidence"])
        direction = Direction.LONG if sentiment >= 0 else Direction.SHORT
        signal_confidence = max(0.6, min(1.0, (abs(sentiment) * 0.7) + (native_confidence * 0.3)))

        instruments = self._native_instruments(headline, fallback, direction, signal_confidence)
        urgency = (
            Urgency.CRITICAL
            if abs(sentiment) >= 0.8
            else Urgency.HIGH
            if abs(sentiment) >= 0.5
            else fallback.urgency
        )
        return ScoredHeadline(
            headline_id=headline.id,
            model="dj-native",
            instruments=instruments,
            escalation_score=sentiment,
            category=fallback.category,
            urgency=urgency,
            time_horizon=TimeHorizon.MINUTES if abs(sentiment) >= 0.5 else fallback.time_horizon,
            metadata={
                "engine": "dj-native",
                "llm_skipped": True,
                "native_asset_id": match["asset_id"],
                "native_language": match["language"],
                "native_sentiment": sentiment,
                "native_confidence": native_confidence,
                "provider": provider,
            },
        )

    def _native_instruments(
        self,
        headline: HeadlineEvent,
        fallback: ScoredHeadline,
        direction: Direction,
        confidence: float,
    ) -> list[InstrumentSignal]:
        resolved: dict[str, InstrumentSignal] = {}
        candidate_symbols = [*headline.symbols]
        native_match = self._extract_native_sentiment(headline)
        if native_match is not None:
            candidate_symbols.append(native_match["asset_id"])

        for candidate in candidate_symbols:
            instrument = self.registry.resolve(candidate)
            if instrument is None:
                continue
            resolved[instrument.symbol] = InstrumentSignal(
                symbol=instrument.symbol,
                direction=direction,
                confidence=confidence,
                broker=instrument.broker,
            )

        if not resolved:
            for signal in fallback.instruments:
                resolved[signal.symbol] = signal.model_copy(update={"direction": direction, "confidence": confidence})

        return list(resolved.values())

    @staticmethod
    def _extract_native_sentiment(headline: HeadlineEvent) -> dict[str, str] | None:
        candidates = [
            headline.text,
            headline.body or "",
            json.dumps(headline.metadata, ensure_ascii=True, sort_keys=True),
        ]
        for candidate in candidates:
            match = DJ_SENTIMENT_PATTERN.search(candidate)
            if match is not None:
                return match.groupdict()
        return None

    @staticmethod
    def _should_use_llm(headline: HeadlineEvent) -> bool:
        provider = (headline.provider or "").upper()
        return headline.source_kind == "rss" or provider in {"BZ", "BRFG", "BRFUPDN"}

    def _system_prompt(self) -> str:
        return (
            "You are scoring breaking-news headlines for latency-sensitive paper trading. "
            "Return only the requested structured output. "
            "Map one headline to multiple instruments when appropriate. "
            "Prefer the provided instrument universe and broker routing.\n\n"
            f"Tradable universe:\n{self.registry.prompt_summary()}"
        )

    @staticmethod
    def _build_prompt(headline: HeadlineEvent) -> str:
        payload = {
            "headline": headline.text,
            "source": headline.source,
            "source_kind": headline.source_kind,
            "timestamp": headline.published_at.isoformat(),
            "article_body": headline.body,
            "symbols": headline.symbols,
            "metadata": headline.metadata,
        }
        return json.dumps(payload, ensure_ascii=True, indent=2)
