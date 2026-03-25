from __future__ import annotations

from collections.abc import Iterable

from information_arbitrage.config import Settings
from information_arbitrage.models import Broker, InstrumentDefinition


DEFAULT_DEFINITIONS = [
    InstrumentDefinition(
        symbol="CL",
        broker=Broker.IB,
        asset_class="future",
        exchange="NYMEX",
        currency="USD",
        position_unit=1.0,
        ib_sec_type="FUT",
        related_symbols=["BRN", "BZ", "PETR4", "PBR", "GC"],
        supports_options=True,
        metadata={"theme": "oil"},
    ),
    InstrumentDefinition(
        symbol="BRN",
        broker=Broker.IB,
        asset_class="future",
        exchange="ICEEU",
        currency="USD",
        position_unit=1.0,
        ib_sec_type="FUT",
        aliases=["BRENT"],
        related_symbols=["CL", "BZ", "PETR4", "PBR"],
        supports_options=True,
        metadata={"theme": "oil"},
    ),
    InstrumentDefinition(
        symbol="BZ",
        broker=Broker.IB,
        asset_class="future",
        exchange="NYMEX",
        currency="USD",
        position_unit=1.0,
        ib_sec_type="FUT",
        aliases=["BRENT_NYMEX"],
        related_symbols=["CL", "BRN"],
        supports_options=True,
        metadata={"theme": "oil"},
    ),
    InstrumentDefinition(
        symbol="GC",
        broker=Broker.IB,
        asset_class="future",
        exchange="COMEX",
        currency="USD",
        position_unit=1.0,
        ib_sec_type="FUT",
        related_symbols=["CL", "SPY"],
        supports_options=True,
        metadata={"theme": "gold"},
    ),
    InstrumentDefinition(
        symbol="EURUSD",
        broker=Broker.IB,
        asset_class="fx",
        exchange="IDEALPRO",
        currency="USD",
        position_unit=10_000.0,
        ib_sec_type="CASH",
    ),
    InstrumentDefinition(
        symbol="USDJPY",
        broker=Broker.IB,
        asset_class="fx",
        exchange="IDEALPRO",
        currency="JPY",
        position_unit=10_000.0,
        ib_sec_type="CASH",
    ),
    InstrumentDefinition(
        symbol="PBR",
        broker=Broker.IB,
        asset_class="adr",
        exchange="SMART",
        currency="USD",
        position_unit=1.0,
        ib_sec_type="STK",
        ib_primary_exchange="NYSE",
        aliases=["PETROBRAS"],
        related_symbols=["PETR4", "CL", "BRN"],
        supports_options=True,
    ),
    InstrumentDefinition(
        symbol="VALE",
        broker=Broker.IB,
        asset_class="adr",
        exchange="SMART",
        currency="USD",
        position_unit=1.0,
        ib_sec_type="STK",
        ib_primary_exchange="NYSE",
        related_symbols=["VALE3"],
        supports_options=True,
    ),
    InstrumentDefinition(
        symbol="ITUB",
        broker=Broker.IB,
        asset_class="adr",
        exchange="SMART",
        currency="USD",
        position_unit=1.0,
        ib_sec_type="STK",
        ib_primary_exchange="NYSE",
        supports_options=True,
    ),
    InstrumentDefinition(
        symbol="BBD",
        broker=Broker.IB,
        asset_class="adr",
        exchange="SMART",
        currency="USD",
        position_unit=1.0,
        ib_sec_type="STK",
        ib_primary_exchange="NYSE",
        supports_options=True,
    ),
    InstrumentDefinition(
        symbol="EWZ",
        broker=Broker.IB,
        asset_class="etf",
        exchange="SMART",
        currency="USD",
        position_unit=1.0,
        ib_sec_type="STK",
        ib_primary_exchange="ARCA",
        supports_options=True,
    ),
    InstrumentDefinition(
        symbol="ERJ",
        broker=Broker.IB,
        asset_class="adr",
        exchange="SMART",
        currency="USD",
        position_unit=1.0,
        ib_sec_type="STK",
        ib_primary_exchange="NYSE",
        supports_options=True,
    ),
    InstrumentDefinition(
        symbol="ABEV",
        broker=Broker.IB,
        asset_class="adr",
        exchange="SMART",
        currency="USD",
        position_unit=1.0,
        ib_sec_type="STK",
        ib_primary_exchange="NYSE",
        supports_options=True,
    ),
    InstrumentDefinition(
        symbol="GGB",
        broker=Broker.IB,
        asset_class="adr",
        exchange="SMART",
        currency="USD",
        position_unit=1.0,
        ib_sec_type="STK",
        ib_primary_exchange="NYSE",
        supports_options=True,
    ),
    InstrumentDefinition(
        symbol="AZUL",
        broker=Broker.IB,
        asset_class="adr",
        exchange="SMART",
        currency="USD",
        position_unit=1.0,
        ib_sec_type="STK",
        ib_primary_exchange="NYSE",
        related_symbols=["CL", "PETR4"],
        supports_options=True,
    ),
    InstrumentDefinition(
        symbol="SPY",
        broker=Broker.IB,
        asset_class="etf",
        exchange="SMART",
        currency="USD",
        position_unit=1.0,
        ib_sec_type="STK",
        ib_primary_exchange="ARCA",
        supports_options=True,
    ),
    InstrumentDefinition(
        symbol="QQQ",
        broker=Broker.IB,
        asset_class="etf",
        exchange="SMART",
        currency="USD",
        position_unit=1.0,
        ib_sec_type="STK",
        ib_primary_exchange="NASDAQ",
        supports_options=True,
    ),
    InstrumentDefinition(
        symbol="PETR4",
        broker=Broker.MT5,
        asset_class="equity",
        exchange="B3",
        currency="BRL",
        position_unit=1.0,
        price_reference_symbol="PBR",
        mt5_symbol="PETR4",
        related_symbols=["PBR", "CL", "BRN"],
        supports_options=True,
    ),
    InstrumentDefinition(
        symbol="VALE3",
        broker=Broker.MT5,
        asset_class="equity",
        exchange="B3",
        currency="BRL",
        position_unit=1.0,
        price_reference_symbol="VALE",
        mt5_symbol="VALE3",
        related_symbols=["VALE"],
        supports_options=True,
    ),
    InstrumentDefinition(
        symbol="BOVA11",
        broker=Broker.MT5,
        asset_class="etf",
        exchange="B3",
        currency="BRL",
        position_unit=1.0,
        price_reference_symbol="EWZ",
        mt5_symbol="BOVA11",
    ),
    InstrumentDefinition(
        symbol="WDOJ26",
        broker=Broker.MT5,
        asset_class="future",
        exchange="B3",
        currency="BRL",
        position_unit=1.0,
        price_reference_symbol="EWZ",
        mt5_symbol="WDOJ26",
    ),
    InstrumentDefinition(
        symbol="WINJ26",
        broker=Broker.MT5,
        asset_class="future",
        exchange="B3",
        currency="BRL",
        position_unit=1.0,
        price_reference_symbol="EWZ",
        mt5_symbol="WINJ26",
    ),
]


class InstrumentRegistry:
    def __init__(self, definitions: Iterable[InstrumentDefinition]) -> None:
        self._by_symbol: dict[str, InstrumentDefinition] = {}
        self._alias_map: dict[str, str] = {}
        for definition in definitions:
            self._by_symbol[definition.symbol.upper()] = definition
            self._alias_map[definition.symbol.upper()] = definition.symbol.upper()
            for alias in definition.aliases:
                self._alias_map[alias.upper()] = definition.symbol.upper()

    @classmethod
    def default(cls, settings: Settings) -> "InstrumentRegistry":
        definitions = [definition.model_copy(deep=True) for definition in DEFAULT_DEFINITIONS]
        for definition in definitions:
            if definition.symbol in settings.ib_contract_months:
                definition.ib_contract_month = settings.ib_contract_months[definition.symbol]
        return cls(definitions)

    def resolve(self, symbol: str) -> InstrumentDefinition | None:
        canonical = self._alias_map.get(symbol.upper())
        return self._by_symbol.get(canonical) if canonical else None

    def related(self, symbol: str) -> list[InstrumentDefinition]:
        definition = self.resolve(symbol)
        if not definition:
            return []
        return [candidate for item in definition.related_symbols if (candidate := self.resolve(item))]

    def all(self) -> list[InstrumentDefinition]:
        return list(self._by_symbol.values())

    def prompt_summary(self) -> str:
        lines = []
        for definition in self.all():
            lines.append(
                f"- {definition.symbol}: broker={definition.broker.value}, asset_class={definition.asset_class}"
            )
        return "\n".join(lines)
