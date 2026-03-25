from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import sys

import nest_asyncio

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from information_arbitrage.backtest.replay import BacktestRunner
from information_arbitrage.config import Settings
from information_arbitrage.instruments.registry import InstrumentRegistry
from information_arbitrage.logging import configure_logging
from information_arbitrage.monitor.storage import MarketStore


async def _run(limit: int, simulate_only: bool) -> None:
    settings = Settings.from_env(ROOT)
    settings.simulate_only = simulate_only
    store = MarketStore(settings.db_path)
    registry = InstrumentRegistry.default(settings)
    runner = BacktestRunner(settings, store, registry)
    result = await runner.replay(limit=limit)
    print(f"Headlines replayed: {result.headlines}")
    print(f"Headlines with signals: {result.scored_signals}")
    print(f"Generated trades: {result.generated_trades}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay stored headlines and measure generated signals.")
    parser.add_argument("--limit", type=int, default=500, help="Number of stored headlines to replay.")
    parser.add_argument("--simulate-only", action="store_true", help="Force simulate-only mode.")
    args = parser.parse_args()

    nest_asyncio.apply()
    configure_logging()
    asyncio.run(_run(args.limit, args.simulate_only))


if __name__ == "__main__":
    main()
