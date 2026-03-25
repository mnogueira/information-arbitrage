from __future__ import annotations

import asyncio
import logging

import requests

from information_arbitrage.models import ExecutionReport, TradeDecision

logger = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, bot_token: str | None, chat_id: str | None) -> None:
        self.bot_token = bot_token
        self.chat_id = chat_id

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    async def notify_trade(self, decision: TradeDecision, report: ExecutionReport) -> None:
        if not self.enabled:
            return
        message = (
            f"Trade executed\n"
            f"Strategy: {decision.strategy_name}\n"
            f"Symbol: {decision.symbol}\n"
            f"Direction: {decision.direction.value}\n"
            f"Qty: {decision.quantity}\n"
            f"Status: {report.status}\n"
            f"Confidence: {decision.confidence:.2f}"
        )
        await asyncio.to_thread(self._send, message)

    def _send(self, text: str) -> None:
        assert self.bot_token is not None
        assert self.chat_id is not None
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        try:
            response = requests.post(url, json={"chat_id": self.chat_id, "text": text}, timeout=10)
            response.raise_for_status()
        except requests.RequestException:
            logger.exception("Failed to send Telegram notification")
