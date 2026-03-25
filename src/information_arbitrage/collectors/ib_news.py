from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from information_arbitrage.collectors.base import BaseCollector
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

    async def stop(self) -> None:
        await self.data_client.disconnect()

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
                headlines.append(
                    HeadlineEvent(
                        source="Interactive Brokers",
                        source_kind=self.source_kind,
                        text=getattr(row, "headline", f"IB news for {symbol}"),
                        published_at=self._coerce_ib_time(getattr(row, "time", now)),
                        provider=getattr(row, "providerCode", None),
                        article_id=getattr(row, "articleId", None),
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

    @staticmethod
    def _coerce_ib_time(value) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=UTC)
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return datetime.now(UTC)
        return datetime.fromtimestamp(numeric, tz=UTC)
