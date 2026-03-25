import pytest

from information_arbitrage.config import Settings
from information_arbitrage.instruments.registry import InstrumentRegistry
from information_arbitrage.models import Category, HeadlineEvent
from information_arbitrage.scoring.engine import HeadlineScoringEngine


@pytest.mark.asyncio
async def test_heuristic_scoring_generates_multi_instrument_signal(tmp_path):
    settings = Settings.from_env(tmp_path)
    settings.openai_api_key = None
    registry = InstrumentRegistry.default(settings)
    scorer = HeadlineScoringEngine(settings, registry)

    headline = HeadlineEvent(
        source="Reuters",
        source_kind="rss",
        text="Iran bombs Saudi oil facility and crude spikes in early trading",
    )

    score = await scorer.score(headline)

    assert score.category == Category.GEOPOLITICAL
    assert len(score.instruments) >= 3
    assert any(signal.symbol == "CL" for signal in score.instruments)
