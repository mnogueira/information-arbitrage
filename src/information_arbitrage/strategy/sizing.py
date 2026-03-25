from __future__ import annotations

from information_arbitrage.models import InstrumentDefinition


def confidence_contracts(confidence: float) -> int:
    if confidence < 0.7:
        return 1
    if confidence < 0.85:
        return 2
    return 3


def calculate_order_quantity(instrument: InstrumentDefinition, contracts: int) -> float:
    return float(max(1, contracts) * instrument.position_unit)
