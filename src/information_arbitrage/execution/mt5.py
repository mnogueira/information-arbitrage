from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from information_arbitrage.config import Settings
from information_arbitrage.execution.base import BrokerClient
from information_arbitrage.models import Broker, Direction, ExecutionReport, InstrumentDefinition, PositionExposure, TradeDecision

logger = logging.getLogger(__name__)

try:
    import MetaTrader5 as mt5
except ImportError:  # pragma: no cover - optional native dependency
    mt5 = None


class MT5Client(BrokerClient):
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._connected = False

    async def connect(self) -> None:
        if mt5 is None:
            logger.warning("MetaTrader5 package unavailable; MT5 disabled")
            return
        if self._connected:
            return
        if not self.settings.mt5_password:
            logger.warning("MT5 password missing; MT5 disabled until market open credentials are available")
            return
        self._connected = bool(
            await asyncio.to_thread(
                mt5.initialize,
                path=self.settings.mt5_path,
                login=self.settings.mt5_login,
                password=self.settings.mt5_password,
                server=self.settings.mt5_server,
            )
        )
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
        positions: list[PositionExposure] = []
        for row in rows or []:
            volume = float(getattr(row, "volume", 0.0))
            direction = 1.0 if getattr(row, "type", 0) == getattr(mt5, "POSITION_TYPE_BUY", 0) else -1.0
            positions.append(
                PositionExposure(
                    symbol=str(getattr(row, "symbol", "")),
                    broker=Broker.MT5,
                    quantity=direction * volume,
                )
            )
        return positions

    async def get_account_equity(self) -> float | None:
        if mt5 is None or not self._connected:
            return None
        info = await asyncio.to_thread(mt5.account_info)
        return float(getattr(info, "equity", 0.0)) if info else None

    async def get_daily_pnl(self) -> float:
        if mt5 is None or not self._connected:
            return 0.0
        info = await asyncio.to_thread(mt5.account_info)
        return float(getattr(info, "profit", 0.0)) if info else 0.0

    async def place_order(self, decision: TradeDecision, instrument: InstrumentDefinition) -> ExecutionReport:
        symbol = instrument.mt5_symbol or instrument.symbol
        if self.settings.simulate_only or mt5 is None or not self._connected:
            return self._simulated_report(decision, symbol, "mt5-unavailable")

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
        status = "submitted" if retcode == getattr(mt5, "TRADE_RETCODE_DONE", None) else f"mt5-retcode-{retcode}"
        return ExecutionReport(
            decision_id=decision.id,
            symbol=symbol,
            broker=Broker.MT5,
            status=status,
            broker_order_id=str(getattr(result, "order", None)) if getattr(result, "order", None) is not None else None,
            filled_quantity=decision.quantity if status == "submitted" else 0.0,
            average_fill_price=float(price),
            metadata={"retcode": retcode},
        )

    @staticmethod
    def _simulated_report(decision: TradeDecision, symbol: str, reason: str) -> ExecutionReport:
        return ExecutionReport(
            decision_id=decision.id,
            symbol=symbol,
            broker=Broker.MT5,
            status=reason,
            broker_order_id=f"SIM-MT5-{decision.id[:8]}",
            filled_quantity=decision.quantity,
            average_fill_price=decision.reference_price,
            metadata={"mode": reason},
        )
