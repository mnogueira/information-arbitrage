from __future__ import annotations

import asyncio
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
    import MetaTrader5 as mt5
except ImportError:  # pragma: no cover - depends on optional native install
    mt5 = None


class MT5Client(BrokerClient):
    broker = Broker.MT5

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._connected = False

    async def connect(self) -> None:
        if mt5 is None:
            logger.warning("MetaTrader5 package is not installed; MT5 execution will run in simulated mode")
            return
        if self._connected:
            return
        if not self.settings.mt5_password:
            logger.warning("MT5_PASSWORD is not set; MT5 execution will run in simulated mode")
            return

        initialized = await asyncio.to_thread(
            mt5.initialize,
            path=self.settings.mt5_path,
            login=self.settings.mt5_login,
            password=self.settings.mt5_password,
            server=self.settings.mt5_server,
        )
        self._connected = bool(initialized)
        if not self._connected:
            logger.warning("MT5 initialize failed: %s", mt5.last_error())

    async def disconnect(self) -> None:
        if mt5 is None or not self._connected:
            return
        await asyncio.to_thread(mt5.shutdown)
        self._connected = False

    async def get_positions(self) -> list[PositionExposure]:
        if mt5 is None or not self._connected:
            return []
        rows = await asyncio.to_thread(mt5.positions_get)
        exposures: list[PositionExposure] = []
        for row in rows or []:
            volume = float(getattr(row, "volume", 0.0))
            order_type = getattr(row, "type", None)
            quantity = volume if order_type == getattr(mt5, "POSITION_TYPE_BUY", 0) else -volume
            exposures.append(
                PositionExposure(
                    symbol=str(getattr(row, "symbol", "")),
                    broker=self.broker,
                    quantity=quantity,
                )
            )
        return exposures

    async def place_order(
        self,
        decision: TradeDecision,
        instrument: InstrumentDefinition,
    ) -> ExecutionReport:
        symbol = instrument.mt5_symbol or instrument.symbol
        if self.settings.simulate_only or mt5 is None or not self._connected:
            reason = "simulate-only" if self.settings.simulate_only else "mt5-unavailable"
            return self._simulated_report(decision, symbol, reason)

        await asyncio.to_thread(mt5.symbol_select, symbol, True)
        tick = await asyncio.to_thread(mt5.symbol_info_tick, symbol)
        price = getattr(tick, "ask", None) if decision.direction == Direction.LONG else getattr(tick, "bid", None)
        if price is None:
            return self._simulated_report(decision, symbol, "missing-price")

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(decision.quantity),
            "type": mt5.ORDER_TYPE_BUY if decision.direction == Direction.LONG else mt5.ORDER_TYPE_SELL,
            "price": float(price),
            "deviation": 20,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
            "comment": decision.strategy_name,
        }
        result = await asyncio.to_thread(mt5.order_send, request)
        retcode = getattr(result, "retcode", None)
        order_id = getattr(result, "order", None)
        status = "submitted" if retcode == getattr(mt5, "TRADE_RETCODE_DONE", None) else f"mt5-retcode-{retcode}"
        return ExecutionReport(
            decision_id=decision.id,
            symbol=symbol,
            broker=self.broker,
            status=status,
            broker_order_id=str(order_id) if order_id is not None else None,
            filled_quantity=decision.quantity if status == "submitted" else 0.0,
            average_fill_price=float(price),
            metadata={"retcode": retcode},
        )

    def _simulated_report(self, decision: TradeDecision, symbol: str, reason: str) -> ExecutionReport:
        return ExecutionReport(
            decision_id=decision.id,
            symbol=symbol,
            broker=self.broker,
            status=reason,
            broker_order_id=f"SIM-MT5-{decision.id[:8]}",
            filled_quantity=decision.quantity,
            executed_at=datetime.now(UTC),
            metadata={"mode": reason},
        )
