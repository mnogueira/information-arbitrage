from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

from information_arbitrage.models import HeadlineEvent

logger = logging.getLogger(__name__)

PublishHeadline = Callable[[HeadlineEvent], Awaitable[None]]


class BaseCollector(ABC):
    def __init__(self, name: str, source_kind: str, poll_interval_seconds: float | None) -> None:
        self.name = name
        self.source_kind = source_kind
        self.poll_interval_seconds = poll_interval_seconds
        self._seen_keys: set[str] = set()

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    @abstractmethod
    async def collect_once(self) -> list[HeadlineEvent]:
        raise NotImplementedError

    async def run(self, publish: PublishHeadline, stop_event: asyncio.Event) -> None:
        await self.start()
        try:
            while not stop_event.is_set():
                try:
                    headlines = await self.collect_once()
                except Exception:
                    logger.exception("Collector %s failed during collection cycle", self.name)
                    headlines = []
                for headline in headlines:
                    dedupe_key = headline.dedupe_key or headline.id
                    if dedupe_key in self._seen_keys:
                        continue
                    self._seen_keys.add(dedupe_key)
                    await publish(headline)

                if self.poll_interval_seconds is None:
                    await stop_event.wait()
                    break

                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self.poll_interval_seconds)
                except TimeoutError:
                    continue
        finally:
            await self.stop()
