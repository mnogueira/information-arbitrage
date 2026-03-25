from __future__ import annotations

import asyncio

from information_arbitrage.execution.ib import IBExecClient
from information_arbitrage.execution.mt5 import MT5Client
from information_arbitrage.models import Broker, ExecutionReport, InstrumentDefinition, PositionExposure, TradeDecision


class ExecutionRouter:
    def __init__(self, ib_client: IBExecClient, mt5_client: MT5Client) -> None:
        self._clients = {
            Broker.IB: ib_client,
            Broker.MT5: mt5_client,
        }

    async def connect(self) -> None:
        await asyncio.gather(*(client.connect() for client in self._clients.values()))

    async def disconnect(self) -> None:
        await asyncio.gather(*(client.disconnect() for client in self._clients.values()))

    async def get_positions(self) -> list[PositionExposure]:
        position_groups = await asyncio.gather(*(client.get_positions() for client in self._clients.values()))
        positions: list[PositionExposure] = []
        for group in position_groups:
            positions.extend(group)
        return positions

    async def execute(self, decision: TradeDecision, instrument: InstrumentDefinition) -> ExecutionReport:
        client = self._clients[decision.broker]
        return await client.place_order(decision, instrument)
