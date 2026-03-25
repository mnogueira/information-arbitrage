from datetime import UTC, datetime, timedelta

import pytest

from information_arbitrage.config import Settings
from information_arbitrage.instruments.registry import InstrumentRegistry
from information_arbitrage.models import HeadlineEvent, RiskSnapshot
from information_arbitrage.monitor.storage import MarketStore
from information_arbitrage.scoring.engine import HeadlineScoringEngine
from information_arbitrage.strategy.engine import StrategyEngine
from information_arbitrage.strategy.market_state import RollingPriceBuffer


@pytest.mark.asyncio
async def test_dj_native_sentiment_skips_llm_and_generates_contrarian_shadow_pair(tmp_path):
    settings = Settings.from_env(tmp_path)
    settings.openai_api_key = "test-key"
    settings.db_path = tmp_path / "dj_native.duckdb"

    store = MarketStore(settings.db_path)
    registry = InstrumentRegistry.default(settings)
    scorer = HeadlineScoringEngine(settings, registry)
    price_buffer = RollingPriceBuffer()
    strategy = StrategyEngine(settings, registry, store, price_buffer)

    now = datetime.now(UTC)
    price_buffer.update("PBR", 10.0, observed_at=now - timedelta(minutes=5, seconds=5))
    price_buffer.update("PBR", 9.75, observed_at=now)

    headline = HeadlineEvent(
        source="Interactive Brokers",
        source_kind="ib_news",
        provider="DJ-RT",
        text="Petrobras sentiment shock {A:PBR:L:en:K:-0.97:C:0.91}",
        published_at=now,
        symbols=["PBR"],
        metadata={"provider_name": "Dow Jones Trader News", "source_priority": 0.99},
    )

    score = await scorer.score(headline)
    decisions = strategy.generate_trades(headline, score, RiskSnapshot(), now=now)

    assert score.model == "dj-native"
    assert score.metadata["llm_skipped"] is True
    assert score.metadata["native_sentiment"] == pytest.approx(-0.97)
    assert {decision.strategy_type for decision in decisions} == {"contrarian", "momentum"}
    assert any(decision.metadata.get("shadow") is True for decision in decisions)
    assert any(decision.strategy_type == "contrarian" and decision.direction.value == "long" for decision in decisions)
