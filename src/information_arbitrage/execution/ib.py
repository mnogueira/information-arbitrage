from __future__ import annotations

import asyncio
import inspect
import logging
import math
from datetime import UTC, datetime

from information_arbitrage.config import Settings
from information_arbitrage.execution.base import BrokerClient
from information_arbitrage.models import (
    Broker,
    Direction,
    ExecutionReport,
    InstrumentDefinition,
    PositionExposure,
    TradeDecision,
)
from information_arbitrage.strategy.market_state import RollingPriceBuffer

logger = logging.getLogger(__name__)

try:
    from ib_async import Contract, Forex, Future, IB, MarketOrder, Stock
except ImportError:  # pragma: no cover - optional native dependency
    Contract = Forex = Future = IB = MarketOrder = Stock = None


async def call_ib_method(target: object, method_name: str, *args, **kwargs):
    for candidate in (f"{method_name}Async", method_name):
        method = getattr(target, candidate, None)
        if method is None:
            continue
        result = method(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result
    raise AttributeError(f"{type(target).__name__} has no method {method_name}")


def _nan_to_none(value) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric):
        return None
    return numeric


def ticker_price(ticker) -> float | None:
    for attribute in ("last", "close", "marketPrice", "bid", "ask"):
        value = getattr(ticker, attribute, None)
        value = value() if callable(value) else value
        numeric = _nan_to_none(value)
        if numeric is not None:
            return numeric
    return None


def build_ib_contract(instrument: InstrumentDefinition) -> object | None:
    if Contract is None:
        return None
    if instrument.asset_class in {"equity", "adr", "etf"} and Stock is not None:
        kwargs = {}
        if instrument.ib_primary_exchange:
            kwargs["primaryExchange"] = instrument.ib_primary_exchange
        return Stock(instrument.symbol, instrument.exchange, instrument.currency, **kwargs)
    if instrument.asset_class == "fx" and Forex is not None:
        return Forex(instrument.symbol)
    if instrument.asset_class == "future" and Future is not None and instrument.ib_contract_month:
        return Future(instrument.symbol, instrument.ib_contract_month, instrument.exchange, "", "", instrument.currency)
    contract = Contract()
    contract.symbol = instrument.symbol
    contract.secType = instrument.ib_sec_type or "STK"
    contract.exchange = instrument.exchange
    contract.currency = instrument.currency
    if instrument.ib_primary_exchange:
        contract.primaryExchange = instrument.ib_primary_exchange
    if instrument.ib_contract_month:
        contract.lastTradeDateOrContractMonth = instrument.ib_contract_month
    return contract


class IBDataClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._ib = None
        self._connected_port: int | None = None
        self._contract_cache: dict[str, object] = {}
        self._connect_lock = asyncio.Lock()

    @property
    def ib(self):
        return self._ib

    @property
    def connected_port(self) -> int | None:
        return self._connected_port

    async def connect(self) -> None:
        async with self._connect_lock:
            if IB is None:
                logger.warning("ib_async is not installed; IB data client disabled")
                return
            if self._ib is not None:
                return
            client = IB()
            # Use the low-level API handshake so the live data session on port 4001
            # never triggers ib_async's startup account/position synchronization.
            await client.client.connectAsync(
                self.settings.ib_data_host,
                self.settings.ib_data_port,
                clientId=self.settings.ib_data_client_id,
            )
            self._ib = client
            self._connected_port = self.settings.ib_data_port
            logger.info("IB data client connected on port %s", self.settings.ib_data_port)

    async def disconnect(self) -> None:
        if self._ib is None:
            return
        self._ib.disconnect()
        self._ib = None
        self._connected_port = None

    async def ensure_connected(self) -> None:
        if self._ib is None:
            await self.connect()

    async def qualify_contract(self, instrument: InstrumentDefinition) -> object | None:
        cache_key = instrument.symbol
        if cache_key in self._contract_cache:
            return self._contract_cache[cache_key]
        await self.ensure_connected()
        if self._ib is None:
            return None
        contract = build_ib_contract(instrument)
        if contract is None and instrument.asset_class != "future":
            return None
        if instrument.asset_class == "future" and instrument.ib_contract_month is None:
            return None
        if contract is None:
            return None
        qualified = await call_ib_method(self._ib, "qualifyContracts", contract)
        if not qualified or qualified[0] is None:
            return None
        self._contract_cache[cache_key] = qualified[0]
        return qualified[0]

    async def get_last_price(self, instrument: InstrumentDefinition) -> float | None:
        await self.ensure_connected()
        if self._ib is None:
            return None
        contract = await self.qualify_contract(instrument)
        if contract is None:
            return None
        tickers = await call_ib_method(self._ib, "reqTickers", contract)
        if not tickers:
            return None
        return ticker_price(tickers[0])

    async def start_price_polling(
        self,
        instruments: list[InstrumentDefinition],
        price_buffer: RollingPriceBuffer,
        stop_event: asyncio.Event,
    ) -> None:
        tracked = {
            (instrument.price_reference_symbol or instrument.symbol).upper(): instrument
            for instrument in instruments
            if instrument.broker == Broker.IB or instrument.price_reference_symbol
        }
        while not stop_event.is_set():
            try:
                await self.ensure_connected()
                for symbol in sorted(tracked):
                    definition = tracked[symbol]
                    price = await self.get_last_price(
                        definition
                        if definition.symbol == symbol
                        else definition.model_copy(
                            update={
                                "symbol": symbol,
                                "price_reference_symbol": symbol,
                                "broker": Broker.IB,
                                "asset_class": "adr",
                                "exchange": "SMART",
                                "currency": "USD",
                                "ib_sec_type": "STK",
                            }
                        )
                    )
                    if price is not None:
                        price_buffer.update(symbol, price)
            except Exception:
                logger.exception("IB price polling loop failed; retrying")
                await self.disconnect()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.settings.ib_price_poll_interval_seconds)
            except TimeoutError:
                continue

class IBExecClient(BrokerClient):
    def __init__(self, settings: Settings, data_client: IBDataClient | None = None) -> None:
        self.settings = settings
        self.data_client = data_client
        self._ib = None

    async def connect(self) -> None:
        if IB is None:
            logger.warning("ib_async is not installed; IB execution disabled")
            return
        if self._ib is not None:
            return
        client = IB()
        await client.connectAsync(self.settings.ib_exec_host, self.settings.ib_exec_port, clientId=self.settings.ib_exec_client_id)
        self._ib = client
        logger.info("IB execution client connected on port %s", self.settings.ib_exec_port)

    async def disconnect(self) -> None:
        if self._ib is None:
            return
        self._ib.disconnect()
        self._ib = None

    async def ensure_connected(self) -> None:
        if self._ib is None:
            await self.connect()

    async def get_positions(self) -> list[PositionExposure]:
        await self.ensure_connected()
        if self._ib is None:
            return []
        rows = await call_ib_method(self._ib, "positions")
        positions: list[PositionExposure] = []
        for row in rows or []:
            contract = getattr(row, "contract", None)
            symbol = getattr(contract, "symbol", None)
            quantity = getattr(row, "position", None)
            if symbol is None or quantity is None:
                continue
            positions.append(PositionExposure(symbol=str(symbol), broker=Broker.IB, quantity=float(quantity)))
        return positions

    async def get_account_equity(self) -> float | None:
        await self.ensure_connected()
        if self._ib is None:
            return None
        rows = await self._ib.accountSummaryAsync()
        for row in rows or []:
            if getattr(row, "tag", "") == "NetLiquidation":
                return _nan_to_none(getattr(row, "value", None))
        return None

    async def get_daily_pnl(self) -> float:
        await self.ensure_connected()
        if self._ib is None:
            return 0.0
        rows = await self._ib.accountSummaryAsync()
        realized = 0.0
        unrealized = 0.0
        for row in rows or []:
            if getattr(row, "tag", "") == "RealizedPnL":
                realized = _nan_to_none(getattr(row, "value", None)) or realized
            if getattr(row, "tag", "") == "UnrealizedPnL":
                unrealized = _nan_to_none(getattr(row, "value", None)) or unrealized
        return realized + unrealized

    async def place_order(self, decision: TradeDecision, instrument: InstrumentDefinition) -> ExecutionReport:
        await self.ensure_connected()
        if self.settings.simulate_only or self._ib is None or MarketOrder is None:
            return self._simulated_report(decision, "simulate-only" if self.settings.simulate_only else "ib-unavailable")

        contract = None
        if self.data_client is not None:
            contract = await self.data_client.qualify_contract(instrument)
        if contract is None:
            contract = build_ib_contract(instrument)
        if contract is None:
            return self._simulated_report(decision, "missing-contract")

        order = MarketOrder("BUY" if decision.direction == Direction.LONG else "SELL", abs(decision.quantity))
        trade = self._ib.placeOrder(contract, order)
        await asyncio.sleep(1.0)
        order_id = getattr(getattr(trade, "order", None), "orderId", None)
        status = getattr(getattr(trade, "orderStatus", None), "status", "submitted")
        average_fill = _nan_to_none(getattr(getattr(trade, "orderStatus", None), "avgFillPrice", None))
        return ExecutionReport(
            decision_id=decision.id,
            symbol=decision.symbol,
            broker=Broker.IB,
            status=str(status).lower(),
            broker_order_id=str(order_id) if order_id is not None else None,
            filled_quantity=decision.quantity if str(status).lower() in {"submitted", "filled", "presubmitted"} else 0.0,
            average_fill_price=average_fill or decision.reference_price,
            metadata={"port": self.settings.ib_exec_port},
        )

    @staticmethod
    def _simulated_report(decision: TradeDecision, reason: str) -> ExecutionReport:
        return ExecutionReport(
            decision_id=decision.id,
            symbol=decision.symbol,
            broker=Broker.IB,
            status=reason,
            broker_order_id=f"SIM-IB-{decision.id[:8]}",
            filled_quantity=decision.quantity,
            average_fill_price=decision.reference_price,
            metadata={"mode": reason},
        )
