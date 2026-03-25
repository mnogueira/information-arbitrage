from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
import sys
import time

import nest_asyncio

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from information_arbitrage.app import PipelineService
from information_arbitrage.config import Settings
from information_arbitrage.logging import configure_logging

logger = logging.getLogger(__name__)


def _run_pipeline(settings: Settings, duration_seconds: float | None) -> None:
    pipeline = PipelineService(settings)
    asyncio.run(pipeline.run_all(duration_seconds=duration_seconds))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run collectors and trader together.")
    parser.add_argument("--duration", type=float, default=None, help="Optional run duration in seconds.")
    parser.add_argument("--simulate-only", action="store_true", help="Force simulate-only mode.")
    args = parser.parse_args()

    nest_asyncio.apply()
    configure_logging()
    settings = Settings.from_env(ROOT)
    if args.simulate_only:
        settings.simulate_only = True
    while True:
        try:
            _run_pipeline(settings, args.duration)
        except Exception as exc:
            logger.error("Pipeline crashed: %s", exc, exc_info=True)
            time.sleep(30)
            continue
        if args.duration is not None:
            return
        logger.warning("Pipeline exited without error; restarting in 30 seconds")
        time.sleep(30)


if __name__ == "__main__":
    main()
