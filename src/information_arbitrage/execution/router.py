from __future__ import annotations

import asyncio

from information_arbitrage.execution.ib import IBDataClient, IBExecClient
from information_arbitrage.execution.mt5 import MT5Client
from information_arbitrage.models import Broker, ExecutionReport, InstrumentDefinition, RiskSnapshot, TradeDecision


class ExecutionRouter:
    def __init__(self, data_client: IBDataClient, ib_client: IBExecClient, mt5_client: MT5Client) -> None:
        self.data_client = data_client
        self.ib_client = ib_client
        self.mt5_client = mt5_client

    async def connect(self) -> None:
        await asyncio.gather(
            self.data_client.connect(),
            self.ib_client.connect(),
            self.mt5_client.connect(),
        )

    async def disconnect(self) -> None:
        await asyncio.gather(
            self.data_client.disconnect(),
            self.ib_client.disconnect(),
            self.mt5_client.disconnect(),
        )

    async def risk_snapshot(self, default_equity: float) -> RiskSnapshot:
        positions_ib, positions_mt5 = await asyncio.gather(
            self.ib_client.get_positions(),
            self.mt5_client.get_positions(),
        )
        equity_ib, equity_mt5 = await asyncio.gather(
            self.ib_client.get_account_equity(),
            self.mt5_client.get_account_equity(),
        )
        pnl_ib, pnl_mt5 = await asyncio.gather(
            self.ib_client.get_daily_pnl(),
            self.mt5_client.get_daily_pnl(),
        )
        positions = [*positions_ib, *positions_mt5]
        open_positions = len([position for position in positions if position.quantity != 0])
        account_equity = equity_ib or equity_mt5 or default_equity
        return RiskSnapshot(
            positions=positions,
            total_open_positions=open_positions,
            account_equity=account_equity,
            daily_pnl=pnl_ib + pnl_mt5,
        )

    async def execute(self, decision: TradeDecision, instrument: InstrumentDefinition) -> ExecutionReport:
        if decision.broker == Broker.IB:
            return await self.ib_client.place_order(decision, instrument)
        return await self.mt5_client.place_order(decision, instrument)
