from datetime import UTC, datetime, timedelta

import pytest

from information_arbitrage.config import Settings
from information_arbitrage.execution.ib import IBDataClient, IBExecClient
from information_arbitrage.execution.mt5 import MT5Client
from information_arbitrage.execution.router import ExecutionRouter
from information_arbitrage.instruments.registry import InstrumentRegistry
from information_arbitrage.models import HeadlineEvent, RiskSnapshot
from information_arbitrage.monitor.storage import MarketStore
from information_arbitrage.scoring.engine import HeadlineScoringEngine
from information_arbitrage.strategy.engine import StrategyEngine
from information_arbitrage.strategy.market_state import RollingPriceBuffer


@pytest.mark.asyncio
async def test_fake_headline_flows_to_paper_execution(tmp_path):
    settings = Settings.from_env(tmp_path)
    settings.simulate_only = True
    settings.openai_api_key = None
    settings.db_path = tmp_path / "pipeline.duckdb"

    store = MarketStore(settings.db_path)
    registry = InstrumentRegistry.default(settings)
    price_buffer = RollingPriceBuffer()
    now = datetime.now(UTC)
    price_buffer.update("CL", 80.0, observed_at=now - timedelta(minutes=5, seconds=5))
    price_buffer.update("CL", 79.9, observed_at=now)

    scorer = HeadlineScoringEngine(settings, registry)
    strategy = StrategyEngine(settings, registry, store, price_buffer)
    router = ExecutionRouter(IBDataClient(settings), IBExecClient(settings), MT5Client(settings))

    headline = HeadlineEvent(
        source="Polymarket",
        source_kind="polymarket",
        provider="gamma-api.polymarket.com",
        text="Ceasefire odds jump after overnight talks",
        metadata={"delta_probability": 0.08, "source_priority": 0.45},
    )

    inserted = store.insert_headline(headline)
    claimed = store.claim_pending_headlines("test-runner", 10)
    score = await scorer.score(claimed[0])
    decisions = strategy.generate_trades(claimed[0], score, RiskSnapshot())
    store.record_score(score)
    store.record_trade_decisions(decisions)

    assert inserted is True
    assert claimed[0].id == headline.id
    assert decisions

    first_instrument = registry.resolve(decisions[0].symbol)
    assert first_instrument is not None
    report = await router.execute(decisions[0], first_instrument)
    store.record_execution(report)
    store.record_trade(decisions[0], report)

    assert report.status in {"simulate-only", "ib-unavailable", "mt5-unavailable", "missing-contract"}
