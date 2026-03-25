from __future__ import annotations

from dataclasses import dataclass

from information_arbitrage.config import Settings
from information_arbitrage.instruments.registry import InstrumentRegistry
from information_arbitrage.monitor.storage import MarketStore
from information_arbitrage.scoring.engine import HeadlineScoringEngine
from information_arbitrage.strategy.engine import StrategyEngine


@dataclass(slots=True)
class ReplayResult:
    headlines: int
    scored_signals: int
    generated_trades: int


class BacktestRunner:
    def __init__(self, settings: Settings, store: MarketStore, registry: InstrumentRegistry) -> None:
        self.settings = settings
        self.store = store
        self.registry = registry
        self.scorer = HeadlineScoringEngine(settings, registry)
        self.strategy = StrategyEngine(registry, settings.confidence_threshold)

    async def replay(self, limit: int = 500) -> ReplayResult:
        headlines = self.store.load_headlines(limit=limit)
        scored_signals = 0
        generated_trades = 0

        for headline in headlines:
            score = await self.scorer.score(headline)
            if score.instruments:
                scored_signals += 1
            trades = self.strategy.generate_trades(headline, score, positions=[])
            generated_trades += len(trades)

        return ReplayResult(
            headlines=len(headlines),
            scored_signals=scored_signals,
            generated_trades=generated_trades,
        )
