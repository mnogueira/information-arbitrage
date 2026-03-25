from information_arbitrage.models import Broker, InstrumentDefinition, Urgency
from information_arbitrage.strategy.sizing import calculate_order_quantity


def test_calculate_order_quantity_scales_fx_and_equities():
    fx = InstrumentDefinition(
        symbol="EURUSD",
        broker=Broker.IB,
        asset_class="fx",
        exchange="IDEALPRO",
        currency="USD",
    )
    equity = InstrumentDefinition(
        symbol="PETR4",
        broker=Broker.MT5,
        asset_class="equity",
        exchange="B3",
        currency="BRL",
    )

    assert calculate_order_quantity(fx, 0.8, Urgency.HIGH) >= 1000
    assert calculate_order_quantity(equity, 0.8, Urgency.HIGH) >= 1
