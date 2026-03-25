from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import requests

from information_arbitrage.collectors.base import BaseCollector
from information_arbitrage.config import Settings
from information_arbitrage.models import HeadlineEvent

try:
    import feedparser
except ImportError:  # pragma: no cover - depends on optional install
    feedparser = None


class RSSCollector(BaseCollector):
    def __init__(self, settings: Settings) -> None:
        super().__init__(
            name="rss",
            source_kind="rss",
            poll_interval_seconds=settings.rss_poll_interval_seconds,
        )
        self.settings = settings
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "information-arbitrage/0.1"})

    async def stop(self) -> None:
        self._session.close()

    async def collect_once(self) -> list[HeadlineEvent]:
        if feedparser is None:
            return []
        results: list[HeadlineEvent] = []
        for feed_url in self.settings.rss_feeds:
            content = await asyncio.to_thread(self._fetch_feed, feed_url)
            if content is None:
                continue
            parsed = feedparser.parse(content)
            source_name = parsed.feed.get("title") or urlparse(feed_url).netloc
            for entry in parsed.entries[:20]:
                title = (entry.get("title") or "").strip()
                if not title:
                    continue
                results.append(
                    HeadlineEvent(
                        source=source_name,
                        source_kind=self.source_kind,
                        text=title,
                        url=entry.get("link"),
                        published_at=self._parse_entry_datetime(entry),
                        metadata={"feed_url": feed_url},
                    )
                )
        return results

    def _fetch_feed(self, feed_url: str) -> bytes | None:
        try:
            response = self._session.get(feed_url, timeout=10)
            response.raise_for_status()
        except requests.RequestException:
            return None
        return response.content

    @staticmethod
    def _parse_entry_datetime(entry: feedparser.FeedParserDict) -> datetime:
        if entry.get("published_parsed"):
            return datetime(*entry.published_parsed[:6], tzinfo=UTC)
        if entry.get("updated_parsed"):
            return datetime(*entry.updated_parsed[:6], tzinfo=UTC)
        published = entry.get("published") or entry.get("updated")
        if published:
            try:
                parsed = parsedate_to_datetime(published)
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
            except (TypeError, ValueError):
                pass
        return datetime.now(UTC)
