from __future__ import annotations

from abc import ABC, abstractmethod

from information_arbitrage.models import Broker, ExecutionReport, InstrumentDefinition, PositionExposure, TradeDecision


class BrokerClient(ABC):
    broker: Broker

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
    async def place_order(
        self,
        decision: TradeDecision,
        instrument: InstrumentDefinition,
    ) -> ExecutionReport:
        raise NotImplementedError
