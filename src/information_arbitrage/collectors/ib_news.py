from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from information_arbitrage.collectors.base import BaseCollector, PublishHeadline
from information_arbitrage.config import Settings
from information_arbitrage.execution.ib import IBDataClient, build_ib_contract, call_ib_method
from information_arbitrage.instruments.registry import InstrumentRegistry
from information_arbitrage.models import HeadlineEvent

logger = logging.getLogger(__name__)


class IBNewsCollector(BaseCollector):
    def __init__(self, settings: Settings, registry: InstrumentRegistry) -> None:
        super().__init__(
            name="ib_news",
            source_kind="ib_news",
            poll_interval_seconds=settings.ib_news_poll_interval_seconds,
        )
        self.settings = settings
        self.registry = registry
        self.data_client = IBDataClient(settings)
        self._publish: PublishHeadline | None = None
        self._qualified_contracts: dict[str, object] = {}
        self._available_providers: list[str] = []
        self._last_historical_poll = datetime.now(UTC) - timedelta(minutes=5)

    async def start(self) -> None:
        try:
            await self.data_client.connect()
        except Exception:
            logger.exception("Failed to connect IB data client for news collection")
            return

        ib = self.data_client.ib
        if ib is None:
            return

        try:
            providers = await call_ib_method(ib, "reqNewsProviders")
            self._available_providers = [getattr(item, "code", str(item)) for item in providers or []]
        except Exception:
            logger.exception("Failed to fetch IB news providers")

        for event_name, handler in (
            ("newsBulletinEvent", self._handle_bulletin),
            ("tickNewsEvent", self._handle_news_tick),
        ):
            event = getattr(ib, event_name, None)
            if event is not None:
                try:
                    event += handler
                except Exception:
                    logger.debug("Unable to attach %s handler", event_name, exc_info=True)

        try:
            await call_ib_method(ib, "reqNewsBulletins", True)
        except Exception:
            logger.exception("Failed to subscribe to IB news bulletins")

    async def stop(self) -> None:
        ib = self.data_client.ib
        if ib is not None:
            try:
                await call_ib_method(ib, "cancelNewsBulletins")
            except Exception:
                logger.debug("Unable to cancel IB bulletins", exc_info=True)
        await self.data_client.disconnect()

    async def run(self, publish: PublishHeadline, stop_event: asyncio.Event) -> None:
        self._publish = publish
        await super().run(publish, stop_event)

    async def collect_once(self) -> list[HeadlineEvent]:
        ib = self.data_client.ib
        if ib is None:
            return []

        now = datetime.now(UTC)
        if (now - self._last_historical_poll).total_seconds() < self.settings.ib_news_poll_interval_seconds:
            return []
        self._last_historical_poll = now

        providers = ",".join(
            provider
            for provider in self.settings.ib_news_provider_codes
            if not self._available_providers or provider in self._available_providers
        )
        if not providers:
            providers = ",".join(self.settings.ib_news_provider_codes)

        headlines: list[HeadlineEvent] = []
        start_time = now - timedelta(minutes=5)
        for symbol in self.settings.ib_news_watch_symbols:
            con_id = await self._resolve_con_id(symbol)
            if con_id is None:
                continue
            try:
                news_rows = await call_ib_method(
                    ib,
                    "reqHistoricalNews",
                    con_id,
                    providers,
                    start_time.strftime("%Y%m%d %H:%M:%S"),
                    now.strftime("%Y%m%d %H:%M:%S"),
                    20,
                    [],
                )
            except Exception:
                logger.debug("Historical IB news request failed for %s", symbol, exc_info=True)
                continue

            for row in news_rows or []:
                article_id = getattr(row, "articleId", None)
                provider_code = getattr(row, "providerCode", None)
                body = await self._fetch_article_body(provider_code, article_id) if article_id and provider_code else None
                headlines.append(
                    HeadlineEvent(
                        source="Interactive Brokers",
                        source_kind=self.source_kind,
                        text=getattr(row, "headline", f"IB news for {symbol}"),
                        published_at=self._coerce_ib_time(getattr(row, "time", now)),
                        provider=provider_code,
                        article_id=article_id,
                        body=body,
                        symbols=[symbol],
                        metadata={"symbol": symbol, "provider_codes": providers},
                    )
                )

        return headlines

    async def _resolve_con_id(self, symbol: str) -> int | None:
        if symbol in self._qualified_contracts:
            return getattr(self._qualified_contracts[symbol], "conId", None)

        ib = self.data_client.ib
        if ib is None:
            return None
        instrument = self.registry.resolve(symbol)
        if instrument is None:
            return None
        contract = build_ib_contract(instrument)
        if contract is None:
            return None

        try:
            qualified = await call_ib_method(ib, "qualifyContracts", contract)
        except Exception:
            logger.debug("Unable to qualify IB contract for %s", symbol, exc_info=True)
            return None

        if not qualified:
            return None
        self._qualified_contracts[symbol] = qualified[0]
        return getattr(qualified[0], "conId", None)

    async def _fetch_article_body(self, provider_code: str, article_id: str) -> str | None:
        ib = self.data_client.ib
        if ib is None:
            return None
        try:
            article = await call_ib_method(ib, "reqNewsArticle", provider_code, article_id, [])
        except Exception:
            logger.debug("Unable to fetch IB article %s:%s", provider_code, article_id, exc_info=True)
            return None
        return getattr(article, "articleText", None)

    def _handle_bulletin(self, bulletin) -> None:
        if self._publish is None:
            return
        headline = HeadlineEvent(
            source="Interactive Brokers",
            source_kind=self.source_kind,
            text=getattr(bulletin, "message", "IB bulletin"),
            published_at=datetime.now(UTC),
            provider="bulletin",
            metadata={
                "msg_id": getattr(bulletin, "msgId", None),
                "msg_type": getattr(bulletin, "msgType", None),
                "orig_exchange": getattr(bulletin, "origExchange", None),
            },
        )
        asyncio.create_task(self._publish(headline))

    def _handle_news_tick(self, news_tick) -> None:
        if self._publish is None:
            return
        headline = HeadlineEvent(
            source="Interactive Brokers",
            source_kind=self.source_kind,
            text=getattr(news_tick, "headline", "IB news tick"),
            published_at=self._coerce_ib_time(getattr(news_tick, "timeStamp", datetime.now(UTC))),
            provider=getattr(news_tick, "providerCode", None),
            article_id=getattr(news_tick, "articleId", None),
            metadata={"extra_data": getattr(news_tick, "extraData", None)},
        )
        asyncio.create_task(self._publish(headline))

    @staticmethod
    def _coerce_ib_time(value) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=UTC)
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return datetime.now(UTC)
        return datetime.fromtimestamp(numeric, tz=UTC)
