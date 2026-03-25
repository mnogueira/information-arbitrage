from __future__ import annotations

from abc import ABC, abstractmethod

from information_arbitrage.models import ExecutionReport, InstrumentDefinition, PositionExposure, TradeDecision


class BrokerClient(ABC):
    @abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def disconnect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_positions(self) -> list[PositionExposure]:
        raise NotImplementedError

    @abstractmethod
    async def place_order(self, decision: TradeDecision, instrument: InstrumentDefinition) -> ExecutionReport:
        raise NotImplementedError

    async def get_account_equity(self) -> float | None:
        return None

    async def get_daily_pnl(self) -> float:
        return 0.0
