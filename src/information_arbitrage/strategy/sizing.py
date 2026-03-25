from __future__ import annotations

from information_arbitrage.models import InstrumentDefinition, Urgency

BASE_UNITS = {
    "adr": 10,
    "equity": 100,
    "etf": 20,
    "fx": 10_000,
    "future": 1,
    "option": 1,
}

URGENCY_MULTIPLIERS = {
    Urgency.LOW: 0.5,
    Urgency.MEDIUM: 1.0,
    Urgency.HIGH: 1.5,
    Urgency.CRITICAL: 2.0,
}


def calculate_order_quantity(
    instrument: InstrumentDefinition,
    confidence: float,
    urgency: Urgency,
) -> float:
    base = BASE_UNITS.get(instrument.asset_class, 1)
    scaled = base * max(0.5, confidence) * URGENCY_MULTIPLIERS[urgency]

    if instrument.asset_class == "fx":
        rounded = int(round(scaled / 1_000)) * 1_000
        return float(max(1_000, rounded))

    if instrument.asset_class in {"equity", "adr", "etf"}:
        return float(max(1, int(round(scaled))))

    return float(max(1, int(round(scaled))))
