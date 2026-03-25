from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from uuid import uuid4

from information_arbitrage.collectors.google_trends import GoogleTrendsCollector
from information_arbitrage.collectors.ib_news import IBNewsCollector
from information_arbitrage.collectors.polymarket import PolymarketCollector
from information_arbitrage.collectors.rss import RSSCollector
from information_arbitrage.config import Settings
from information_arbitrage.execution.ib import IBDataClient, IBExecClient
from information_arbitrage.execution.mt5 import MT5Client
from information_arbitrage.execution.router import ExecutionRouter
from information_arbitrage.instruments.registry import InstrumentRegistry
from information_arbitrage.models import ExecutionReport, HeadlineEvent
from information_arbitrage.monitor.storage import MarketStore
from information_arbitrage.monitor.telegram import TelegramNotifier
from information_arbitrage.scoring.engine import HeadlineScoringEngine
from information_arbitrage.strategy.engine import StrategyEngine
from information_arbitrage.strategy.market_state import RollingPriceBuffer

logger = logging.getLogger(__name__)


async def _stop_after(duration_seconds: float, stop_event: asyncio.Event) -> None:
    await asyncio.sleep(duration_seconds)
    stop_event.set()


class CollectorService:
    def __init__(
        self,
        settings: Settings,
        store: MarketStore,
        registry: InstrumentRegistry,
        data_client: IBDataClient,
    ) -> None:
        self.settings = settings
        self.store = store
        self.registry = registry
        ib_news = IBNewsCollector(settings, registry)
        ib_news.data_client = data_client
        self.collectors = [
            ib_news,
            RSSCollector(settings.rss_feeds, settings.rss_poll_interval_seconds),
            PolymarketCollector(settings.polymarket_keywords, settings.polymarket_poll_interval_seconds),
        ]
        if settings.enable_google_trends_collector:
            self.collectors.append(GoogleTrendsCollector())

    async def run(self, duration_seconds: float | None = None) -> None:
        stop_event = asyncio.Event()
        timer = None
        if duration_seconds is not None:
            timer = asyncio.create_task(_stop_after(duration_seconds, stop_event))

        try:
            async with asyncio.TaskGroup() as task_group:
                for collector in self.collectors:
                    task_group.create_task(collector.run(self.publish, stop_event))
        finally:
            if timer:
                timer.cancel()
                with suppress(asyncio.CancelledError):
                    await timer

    async def publish(self, headline: HeadlineEvent) -> None:
        inserted = self.store.insert_headline(headline)
        if inserted:
            logger.info("Headline ingested from %s: %s", headline.source, headline.text)


class TraderService:
    def __init__(
        self,
        settings: Settings,
        store: MarketStore,
        registry: InstrumentRegistry,
        data_client: IBDataClient,
    ) -> None:
        self.settings = settings
        self.store = store
        self.registry = registry
        self.price_buffer = RollingPriceBuffer()
        self.data_client = data_client
        self.scorer = HeadlineScoringEngine(settings, registry)
        self.strategy = StrategyEngine(settings, registry, store, self.price_buffer)
        self.router = ExecutionRouter(
            self.data_client,
            IBExecClient(settings, self.data_client),
            MT5Client(settings),
        )
        self.notifier = TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id)
        self.consumer_id = f"trader-{uuid4()}"

    async def run(self, duration_seconds: float | None = None) -> None:
        stop_event = asyncio.Event()
        timer = None
        price_task = None
        if duration_seconds is not None:
            timer = asyncio.create_task(_stop_after(duration_seconds, stop_event))

        try:
            await self.router.connect()
            price_task = asyncio.create_task(
                self.router.data_client.start_price_polling(self.registry.all(), self.price_buffer, stop_event)
            )
            while not stop_event.is_set():
                headlines = self.store.claim_pending_headlines(
                    consumer_id=self.consumer_id,
                    limit=self.settings.headline_claim_batch_size,
                )
                if not headlines:
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(),
                            timeout=self.settings.trader_poll_interval_seconds,
                        )
                    except TimeoutError:
                        continue
                    break

                for headline in headlines:
                    await self.process_headline(headline)
        finally:
            stop_event.set()
            if price_task:
                with suppress(asyncio.CancelledError):
                    await price_task
            await self.router.disconnect()
            if timer:
                timer.cancel()
                with suppress(asyncio.CancelledError):
                    await timer

    async def process_headline(self, headline: HeadlineEvent) -> None:
        try:
            score = await self.scorer.score(headline)
            self.store.record_score(score)

            risk_snapshot = await self.router.risk_snapshot(self.settings.account_equity)
            decisions = self.strategy.generate_trades(headline, score, risk_snapshot)
            self.store.record_trade_decisions(decisions)

            for decision in decisions:
                instrument = self.registry.resolve(decision.symbol)
                if instrument is None:
                    continue
                if decision.metadata.get("shadow"):
                    report = ExecutionReport(
                        decision_id=decision.id,
                        symbol=decision.symbol,
                        broker=decision.broker,
                        status="shadow",
                        filled_quantity=0.0,
                        average_fill_price=decision.reference_price,
                        metadata={"shadow": True},
                    )
                else:
                    report = await self.router.execute(decision, instrument)
                    self.store.record_execution(report)
                self.store.record_trade(decision, report)
                if not decision.metadata.get("shadow") and decision.confidence >= max(0.75, self.settings.confidence_threshold):
                    await self.notifier.notify_trade(decision, report)

            self.store.mark_headline_processed(headline.id)
        except Exception as exc:
            logger.exception("Failed to process headline %s", headline.id)
            self.store.mark_headline_failed(headline.id, str(exc))


class PipelineService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.store = MarketStore(settings.db_path)
        self.registry = InstrumentRegistry.default(settings)
        self.data_client = IBDataClient(settings)
        self.collectors = CollectorService(settings, self.store, self.registry, self.data_client)
        self.trader = TraderService(settings, self.store, self.registry, self.data_client)

    async def run_collectors(self, duration_seconds: float | None = None) -> None:
        await self.collectors.run(duration_seconds)

    async def run_trader(self, duration_seconds: float | None = None) -> None:
        await self.trader.run(duration_seconds)

    async def run_all(self, duration_seconds: float | None = None) -> None:
        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(self.collectors.run(duration_seconds))
            task_group.create_task(self.trader.run(duration_seconds))
