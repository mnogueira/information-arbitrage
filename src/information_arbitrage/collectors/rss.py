from __future__ import annotations

import asyncio
import logging
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from time import struct_time
from urllib.parse import urlparse

import requests

try:
    import feedparser
except ImportError:  # pragma: no cover - optional dependency
    feedparser = None

from information_arbitrage.collectors.base import BaseCollector
from information_arbitrage.models import HeadlineEvent, scoped_headline_dedupe_key

logger = logging.getLogger(__name__)


class RSSCollector(BaseCollector):
    def __init__(self, feeds: list[str], poll_interval_seconds: float) -> None:
        super().__init__(name="rss", source_kind="rss", poll_interval_seconds=poll_interval_seconds)
        self.feeds = feeds
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "information-arbitrage/0.1"})

    async def collect_once(self) -> list[HeadlineEvent]:
        headlines: list[HeadlineEvent] = []
        for feed_url in self.feeds:
            try:
                source_name, entries = await asyncio.to_thread(self._fetch_feed, feed_url)
            except Exception:
                logger.exception("RSS fetch failed for %s", feed_url)
                continue

            for entry in entries[:25]:
                title = entry.get("title")
                if not title:
                    continue
                headlines.append(
                    HeadlineEvent(
                        source=source_name,
                        source_kind=self.source_kind,
                        provider=urlparse(feed_url).netloc,
                        text=title,
                        url=entry.get("link"),
                        published_at=self._entry_timestamp(entry),
                        metadata={
                            "feed_url": feed_url,
                            "source_priority": 0.2,
                            "provider_name": source_name,
                            "provider_family": "rss",
                        },
                        dedupe_key=scoped_headline_dedupe_key(title, self.source_kind, urlparse(feed_url).netloc),
                    )
                )
        return headlines

    def _fetch_feed(self, feed_url: str) -> tuple[str, list[dict]]:
        response = self._session.get(feed_url, timeout=10)
        response.raise_for_status()
        if feedparser is not None:
            parsed = feedparser.parse(response.content)
            return parsed.feed.get("title") or urlparse(feed_url).netloc, list(parsed.entries)

        root = ET.fromstring(response.content)
        channel = root.find("./channel")
        source_name = (channel.findtext("title") if channel is not None else None) or urlparse(feed_url).netloc
        entries: list[dict] = []
        for item in root.findall(".//item"):
            entries.append(
                {
                    "title": item.findtext("title"),
                    "link": item.findtext("link"),
                    "published": item.findtext("pubDate"),
                    "updated": item.findtext("updated"),
                }
            )
        return source_name, entries

    @staticmethod
    def _entry_timestamp(entry) -> datetime:
        if isinstance(entry.get("published_parsed"), struct_time):
            return datetime(*entry.published_parsed[:6], tzinfo=UTC)
        if isinstance(entry.get("updated_parsed"), struct_time):
            return datetime(*entry.updated_parsed[:6], tzinfo=UTC)

        for field in ("published", "updated"):
            raw = entry.get(field)
            if isinstance(raw, str):
                try:
                    parsed = parsedate_to_datetime(raw)
                    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
                except ValueError:
                    continue

        return datetime.now(UTC)
