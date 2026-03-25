from __future__ import annotations

import inspect
import logging
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

logger = logging.getLogger(__name__)

try:
    from ib_async import Contract, Forex, Future, IB, MarketOrder, Stock
except ImportError:  # pragma: no cover - depends on optional native install
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

    if instrument.asset_class == "future" and Future is not None:
        if not instrument.ib_contract_month:
            return None
        return Future(instrument.symbol, instrument.ib_contract_month, instrument.exchange, instrument.currency)

    contract = Contract()
    contract.symbol = instrument.symbol
    contract.secType = instrument.ib_sec_type or "STK"
    contract.exchange = instrument.exchange
    contract.currency = instrument.currency
    if instrument.ib_primary_exchange:
        contract.primaryExchange = instrument.ib_primary_exchange
    if instrument.ib_contract_month:
        contract.lastTradeDateOrContractMonth = instrument.ib_contract_month
    if instrument.ib_right:
        contract.right = instrument.ib_right
    if instrument.ib_strike:
        contract.strike = instrument.ib_strike
    return contract


class IBDataClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._ib = None

    @property
    def ib(self):
        return self._ib

    async def connect(self) -> None:
        if IB is None:
            logger.warning("ib_async is not installed; IB data client will stay disabled")
            return
        if self._ib is not None:
            return
        client = IB()
        try:
            await client.connectAsync(
                self.settings.ib_data_host,
                self.settings.ib_data_port,
                clientId=self.settings.ib_data_client_id,
            )
        except Exception:
            logger.warning("IB data connection failed; continuing without live IB data", exc_info=True)
            return
        self._ib = client
        try:
            client.reqMarketDataType(1)
        except Exception:
            logger.debug("Unable to switch IB market data type to realtime", exc_info=True)

    async def disconnect(self) -> None:
        if self._ib is None:
            return
        self._ib.disconnect()
        self._ib = None


class IBExecClient(BrokerClient):
    broker = Broker.IB

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._ib = None

    async def connect(self) -> None:
        if IB is None:
            logger.warning("ib_async is not installed; IB execution will run in simulated mode")
            return
        if self._ib is not None:
            return
        client = IB()
        try:
            await client.connectAsync(
                self.settings.ib_exec_host,
                self.settings.ib_exec_port,
                clientId=self.settings.ib_exec_client_id,
            )
        except Exception:
            logger.warning("IB execution connection failed; continuing in simulated mode", exc_info=True)
            return
        self._ib = client

    async def disconnect(self) -> None:
        if self._ib is None:
            return
        self._ib.disconnect()
        self._ib = None

    async def get_positions(self) -> list[PositionExposure]:
        if self._ib is None:
            return []
        positions = await call_ib_method(self._ib, "positions")
        exposures: list[PositionExposure] = []
        for position in positions or []:
            contract = getattr(position, "contract", None)
            symbol = getattr(contract, "symbol", None)
            quantity = getattr(position, "position", None)
            if symbol is None or quantity is None:
                continue
            exposures.append(PositionExposure(symbol=symbol, broker=self.broker, quantity=float(quantity)))
        return exposures

    async def place_order(
        self,
        decision: TradeDecision,
        instrument: InstrumentDefinition,
    ) -> ExecutionReport:
        contract = build_ib_contract(instrument)
        if self.settings.simulate_only or self._ib is None or MarketOrder is None or contract is None:
            return self._simulated_report(decision, instrument, contract)

        order = MarketOrder("BUY" if decision.direction == Direction.LONG else "SELL", abs(decision.quantity))
        trade = await call_ib_method(self._ib, "placeOrder", contract, order)
        order_id = getattr(getattr(trade, "order", None), "orderId", None)
        status = getattr(getattr(trade, "orderStatus", None), "status", "submitted")
        return ExecutionReport(
            decision_id=decision.id,
            symbol=decision.symbol,
            broker=self.broker,
            status=str(status).lower(),
            broker_order_id=str(order_id) if order_id is not None else None,
            metadata={"mode": "ib-paper"},
        )

    def _simulated_report(
        self,
        decision: TradeDecision,
        instrument: InstrumentDefinition,
        contract: object | None,
    ) -> ExecutionReport:
        reason = "simulated"
        if contract is None and instrument.asset_class == "future":
            reason = "missing-contract-month"
        elif self._ib is None:
            reason = "ib-unavailable"
        elif self.settings.simulate_only:
            reason = "simulate-only"
        return ExecutionReport(
            decision_id=decision.id,
            symbol=decision.symbol,
            broker=self.broker,
            status=reason,
            broker_order_id=f"SIM-IB-{decision.id[:8]}",
            filled_quantity=decision.quantity,
            executed_at=datetime.now(UTC),
            metadata={"mode": reason},
        )
