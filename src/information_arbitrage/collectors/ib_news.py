from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from information_arbitrage.collectors.base import BaseCollector, PublishHeadline
from information_arbitrage.config import Settings
from information_arbitrage.execution.ib import IBDataClient, build_ib_contract, call_ib_method
from information_arbitrage.instruments.registry import InstrumentRegistry
from information_arbitrage.models import HeadlineEvent, scoped_headline_dedupe_key

logger = logging.getLogger(__name__)

MACRO_SYMBOLS = {"CL", "BRN", "BZ", "GC", "EURUSD", "USDJPY"}
MACRO_KEYWORDS = {
    "iran",
    "oil",
    "war",
    "ceasefire",
    "sanction",
    "hormuz",
    "opec",
    "fed",
    "inflation",
    "rate",
    "treasury",
    "dollar",
    "yen",
    "euro",
    "geopolitical",
}
PROVIDER_ORDER = {
    "BZ": 110,
    "BRFG": 105,
    "BRFUPDN": 100,
    "DJ-RT": 98,
    "DJ-RTG": 96,
    "DJ-RTE": 94,
    "DJ-RTA": 92,
    "DJ-GM": 90,
    "DJ-N": 88,
    "DJNL": 86,
}


class IBNewsCollector(BaseCollector):
    def __init__(self, settings: Settings, registry: InstrumentRegistry) -> None:
        super().__init__(name="ib_news", source_kind="ib_news", poll_interval_seconds=1.0)
        self.settings = settings
        self.registry = registry
        self.data_client = IBDataClient(settings)
        self._publish: PublishHeadline | None = None
        self._qualified_contracts: dict[str, object] = {}
        self._provider_names: dict[str, str] = {}
        self._historical_provider_codes: list[str] = []
        self._historical_refresh_requested = True
        self._bulletins_subscribed = False
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
            self._provider_names = {
                getattr(item, "code", ""): getattr(item, "name", getattr(item, "code", ""))
                for item in providers or []
                if getattr(item, "code", None)
            }
            self._historical_provider_codes = self._select_provider_codes()
            logger.info(
                "IB news collector active providers: %s",
                ", ".join(self._historical_provider_codes) or "none",
            )
        except Exception:
            logger.exception("Failed to fetch IB news providers")

        bulletin_event = getattr(ib, "newsBulletinEvent", None)
        if bulletin_event is not None:
            try:
                bulletin_event += self._handle_bulletin
            except Exception:
                logger.debug("Unable to attach newsBulletinEvent handler", exc_info=True)

        tick_news_event = getattr(ib, "tickNewsEvent", None)
        if tick_news_event is not None:
            try:
                tick_news_event += self._handle_news_tick
            except Exception:
                logger.debug("Unable to attach tickNewsEvent handler", exc_info=True)

        try:
            await call_ib_method(ib, "reqNewsBulletins", True)
            self._bulletins_subscribed = True
            logger.info("IB news bulletin subscription enabled on port %s", self.settings.ib_data_port)
        except Exception:
            logger.exception("Failed to subscribe to IB news bulletins")

    async def stop(self) -> None:
        ib = self.data_client.ib
        if ib is not None and self._bulletins_subscribed:
            try:
                await call_ib_method(ib, "cancelNewsBulletins")
            except Exception:
                logger.debug("Unable to cancel IB bulletins", exc_info=True)
        await self.data_client.disconnect()

    async def run(self, publish: PublishHeadline, stop_event: asyncio.Event) -> None:
        self._publish = publish
        try:
            await super().run(publish, stop_event)
        finally:
            self._publish = None

    async def collect_once(self) -> list[HeadlineEvent]:
        ib = self.data_client.ib
        if ib is None or not self._historical_provider_codes:
            return []

        now = datetime.now(UTC)
        elapsed = (now - self._last_historical_poll).total_seconds()
        if not self._historical_refresh_requested and elapsed < self.settings.ib_news_poll_interval_seconds:
            return []

        self._historical_refresh_requested = False
        self._last_historical_poll = now
        start_time = now - timedelta(minutes=5)

        tasks = [
            self._fetch_symbol_historical_news(symbol, start_time, now)
            for symbol in self.settings.ib_news_watch_symbols
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        headlines: list[HeadlineEvent] = []
        for symbol, result in zip(self.settings.ib_news_watch_symbols, results):
            if isinstance(result, Exception):
                logger.warning("Historical IB news request failed for %s: %s", symbol, result)
                continue
            headlines.extend(result)

        headlines.sort(
            key=lambda headline: (
                float(headline.metadata.get("source_priority", 0.0)),
                headline.published_at,
            ),
            reverse=True,
        )
        return headlines

    async def _fetch_symbol_historical_news(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[HeadlineEvent]:
        ib = self.data_client.ib
        if ib is None:
            return []

        con_id = await self._resolve_con_id(symbol)
        if con_id is None:
            return []

        news_rows = await call_ib_method(
            ib,
            "reqHistoricalNews",
            con_id,
            ",".join(self._historical_provider_codes),
            start_time.strftime("%Y%m%d %H:%M:%S"),
            end_time.strftime("%Y%m%d %H:%M:%S"),
            50,
            [],
        )

        headlines: list[HeadlineEvent] = []
        for row in news_rows or []:
            provider_code = getattr(row, "providerCode", None)
            provider_name = self._provider_names.get(provider_code or "", provider_code or "Interactive Brokers")
            headline_text = getattr(row, "headline", f"IB news for {symbol}")
            published_at = self._coerce_ib_time(getattr(row, "time", end_time))
            source_priority = self._source_priority(provider_code, headline_text, [symbol])
            headlines.append(
                HeadlineEvent(
                    source="Interactive Brokers",
                    source_kind=self.source_kind,
                    text=headline_text,
                    published_at=published_at,
                    provider=provider_code,
                    article_id=getattr(row, "articleId", None),
                    symbols=[symbol],
                    metadata={
                        "symbol": symbol,
                        "source_priority": source_priority,
                        "provider_name": provider_name,
                        "provider_family": self._provider_family(provider_code),
                        "provider_codes": self._historical_provider_codes,
                    },
                    dedupe_key=scoped_headline_dedupe_key(headline_text, self.source_kind, provider_code, symbol),
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

    async def _publish_stream_headline(self, headline: HeadlineEvent) -> None:
        if self._publish is None:
            return
        dedupe_key = headline.dedupe_key or headline.id
        if dedupe_key in self._seen_keys:
            return
        self._seen_keys.add(dedupe_key)
        await self._publish(headline)

    def _handle_bulletin(self, bulletin) -> None:
        self._historical_refresh_requested = True
        if self._publish is None:
            return

        text = getattr(bulletin, "message", "IB bulletin")
        headline = HeadlineEvent(
            source="Interactive Brokers",
            source_kind=self.source_kind,
            text=text,
            published_at=datetime.now(UTC),
            provider="IB-BULLETIN",
            metadata={
                "msg_id": getattr(bulletin, "msgId", None),
                "msg_type": getattr(bulletin, "msgType", None),
                "orig_exchange": getattr(bulletin, "origExchange", None),
                "source_priority": 0.85,
                "provider_name": "IB Bulletin",
                "provider_family": "bulletin",
            },
            dedupe_key=scoped_headline_dedupe_key(text, self.source_kind, "IB-BULLETIN"),
        )
        asyncio.create_task(self._publish_stream_headline(headline))

    def _handle_news_tick(self, news_tick) -> None:
        self._historical_refresh_requested = True
        if self._publish is None:
            return

        provider_code = getattr(news_tick, "providerCode", None)
        headline_text = getattr(news_tick, "headline", "IB news tick")
        provider_name = self._provider_names.get(provider_code or "", provider_code or "Interactive Brokers")
        published_at = self._coerce_ib_time(getattr(news_tick, "timeStamp", datetime.now(UTC)))
        headline = HeadlineEvent(
            source="Interactive Brokers",
            source_kind=self.source_kind,
            text=headline_text,
            published_at=published_at,
            provider=provider_code,
            article_id=getattr(news_tick, "articleId", None),
            metadata={
                "extra_data": getattr(news_tick, "extraData", None),
                "source_priority": self._source_priority(provider_code, headline_text, []),
                "provider_name": provider_name,
                "provider_family": self._provider_family(provider_code),
            },
            dedupe_key=scoped_headline_dedupe_key(headline_text, self.source_kind, provider_code),
        )
        asyncio.create_task(self._publish_stream_headline(headline))

    def _select_provider_codes(self) -> list[str]:
        requested = {code.upper() for code in self.settings.ib_news_provider_codes}
        for code, name in self._provider_names.items():
            lowered = name.lower()
            if "benzinga" in lowered or "dow jones" in lowered or "briefing.com" in lowered:
                requested.add(code.upper())
        available = [code for code in requested if code in self._provider_names]
        return sorted(available, key=lambda code: PROVIDER_ORDER.get(code, 0), reverse=True)

    @staticmethod
    def _provider_family(provider_code: str | None) -> str:
        if not provider_code:
            return "ib"
        if provider_code in {"BZ", "BRFG", "BRFUPDN"}:
            return "us-breaking"
        if provider_code.startswith("DJ"):
            return "dow-jones"
        return "ib"

    def _source_priority(self, provider_code: str | None, headline_text: str, symbols: list[str]) -> float:
        normalized_symbols = {symbol.upper() for symbol in symbols}
        lowered_text = headline_text.lower()
        if provider_code in {"BZ", "BRFG", "BRFUPDN"}:
            if normalized_symbols and normalized_symbols.isdisjoint(MACRO_SYMBOLS):
                return 1.0
            return 0.95
        if provider_code and provider_code.startswith("DJ"):
            is_macro = bool(normalized_symbols & MACRO_SYMBOLS) or any(keyword in lowered_text for keyword in MACRO_KEYWORDS)
            return 0.99 if is_macro else 0.9
        if provider_code == "IB-BULLETIN":
            return 0.85
        return 0.75

    @staticmethod
    def _coerce_ib_time(value) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=UTC)
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return datetime.now(UTC)
        return datetime.fromtimestamp(numeric, tz=UTC)
