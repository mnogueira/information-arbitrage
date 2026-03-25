from __future__ import annotations

from information_arbitrage.collectors.base import BaseCollector
from information_arbitrage.models import HeadlineEvent


class GoogleTrendsCollector(BaseCollector):
    def __init__(self) -> None:
        super().__init__(name="google_trends", source_kind="google_trends", poll_interval_seconds=300.0)

    async def collect_once(self) -> list[HeadlineEvent]:
        return []
