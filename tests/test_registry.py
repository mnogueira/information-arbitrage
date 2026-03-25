from information_arbitrage.config import Settings
from information_arbitrage.instruments.registry import InstrumentRegistry
from information_arbitrage.models import Broker


def test_registry_resolves_primary_and_alias_symbols(tmp_path):
    settings = Settings.from_env(tmp_path)
    registry = InstrumentRegistry.default(settings)

    pbr = registry.resolve("PBR")
    brent = registry.resolve("brent")

    assert pbr is not None
    assert pbr.broker == Broker.IB
    assert brent is not None
    assert brent.symbol == "BRN"
