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

from information_arbitrage.app import PipelineService
from information_arbitrage.config import Settings
from information_arbitrage.logging import configure_logging


def main() -> None:
    parser = argparse.ArgumentParser(description="Run news collectors only.")
    parser.add_argument("--duration", type=float, default=None, help="Optional run duration in seconds.")
    parser.add_argument("--simulate-only", action="store_true", help="Force simulate-only mode.")
    args = parser.parse_args()

    nest_asyncio.apply()
    configure_logging()
    settings = Settings.from_env(ROOT)
    if args.simulate_only:
        settings.simulate_only = True
    pipeline = PipelineService(settings)
    asyncio.run(pipeline.run_collectors(duration_seconds=args.duration))


if __name__ == "__main__":
    main()
