from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


def headline_dedupe_key(text: str) -> str:
    normalized = " ".join(text.lower().strip().split())
    return sha256(normalized.encode("utf-8")).hexdigest()


class Broker(StrEnum):
    IB = "ib"
    MT5 = "mt5"


class Direction(StrEnum):
    LONG = "long"
    SHORT = "short"


class Urgency(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class TimeHorizon(StrEnum):
    MINUTES = "minutes"
    HOURS = "hours"
    DAYS = "days"


class Category(StrEnum):
    GEOPOLITICAL = "geopolitical"
    ECONOMIC = "economic"
    EARNINGS = "earnings"
    REGULATORY = "regulatory"
    MA = "m&a"
    OTHER = "other"


class HeadlineEvent(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    source: str
    source_kind: str
    text: str
    published_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    url: str | None = None
    provider: str | None = None
    article_id: str | None = None
    body: str | None = None
    symbols: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    dedupe_key: str | None = None

    @model_validator(mode="after")
    def normalize(self) -> "HeadlineEvent":
        if self.dedupe_key is None:
            self.dedupe_key = headline_dedupe_key(self.text)
        if self.published_at.tzinfo is None:
            self.published_at = self.published_at.replace(tzinfo=UTC)
        return self


class InstrumentSignal(BaseModel):
    symbol: str
    direction: Direction
    confidence: float = Field(ge=0.0, le=1.0)
    broker: Broker


class HeadlineScorePayload(BaseModel):
    instruments: list[InstrumentSignal] = Field(default_factory=list)
    escalation_score: float = Field(ge=-1.0, le=1.0)
    category: Category
    urgency: Urgency
    time_horizon: TimeHorizon


class ScoredHeadline(BaseModel):
    headline_id: str
    model: str
    scored_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    instruments: list[InstrumentSignal] = Field(default_factory=list)
    escalation_score: float = Field(ge=-1.0, le=1.0)
    category: Category
    urgency: Urgency
    time_horizon: TimeHorizon
    metadata: dict[str, Any] = Field(default_factory=dict)


class InstrumentDefinition(BaseModel):
    symbol: str
    broker: Broker
    asset_class: str
    exchange: str
    currency: str
    position_unit: float = 1.0
    price_reference_symbol: str | None = None
    ib_sec_type: str | None = None
    ib_primary_exchange: str | None = None
    ib_contract_month: str | None = None
    ib_right: str | None = None
    ib_strike: float | None = None
    mt5_symbol: str | None = None
    aliases: list[str] = Field(default_factory=list)
    related_symbols: list[str] = Field(default_factory=list)
    supports_options: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class PositionExposure(BaseModel):
    symbol: str
    broker: Broker
    quantity: float

    @property
    def direction(self) -> Direction | None:
        if self.quantity > 0:
            return Direction.LONG
        if self.quantity < 0:
            return Direction.SHORT
        return None


class TradeDecision(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    headline_id: str
    symbol: str
    broker: Broker
    direction: Direction
    quantity: float
    confidence: float = Field(ge=0.0, le=1.0)
    urgency: Urgency
    time_horizon: TimeHorizon
    strategy_name: str
    reason: str
    order_type: str = "market"
    reference_price: float | None = None
    atr: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None
    time_exit_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExecutionReport(BaseModel):
    decision_id: str
    symbol: str
    broker: Broker
    status: str
    broker_order_id: str | None = None
    filled_quantity: float = 0.0
    average_fill_price: float | None = None
    executed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)


class RiskSnapshot(BaseModel):
    positions: list[PositionExposure] = Field(default_factory=list)
    total_open_positions: int = 0
    account_equity: float = 100_000.0
    daily_pnl: float = 0.0
