from dataclasses import dataclass
from decimal import Decimal

from spread_monitor.sizing import qty_for_notional
from spread_monitor.sizing import roundtrip_qty


@dataclass
class FakeInstrument:
    size_increment: Decimal
    min_quantity: Decimal | None = None
    max_quantity: Decimal | None = None
    min_notional: Decimal | None = None


def test_qty_for_notional_basic_rounding():
    inst = FakeInstrument(size_increment=Decimal("0.001"))
    qty = qty_for_notional(notional_usdt=500.0, price=100.0, instrument=inst)
    assert qty == Decimal("5.000")


def test_qty_for_notional_rounds_down_to_increment():
    inst = FakeInstrument(size_increment=Decimal("0.01"))
    # 500 / 33.333 = 15.00015... 应该向下取整到 15.00
    qty = qty_for_notional(notional_usdt=500.0, price=33.333, instrument=inst)
    assert qty == Decimal("15.00")


def test_qty_for_notional_below_min_quantity_returns_none():
    inst = FakeInstrument(size_increment=Decimal("0.001"), min_quantity=Decimal("1"))
    qty = qty_for_notional(notional_usdt=10.0, price=100.0, instrument=inst)
    assert qty is None


def test_qty_for_notional_above_max_quantity_returns_none():
    inst = FakeInstrument(size_increment=Decimal("0.001"), max_quantity=Decimal("1"))
    qty = qty_for_notional(notional_usdt=500.0, price=100.0, instrument=inst)
    assert qty is None


def test_qty_for_notional_below_min_notional_returns_none():
    inst = FakeInstrument(size_increment=Decimal("0.001"), min_notional=Decimal("50"))
    qty = qty_for_notional(notional_usdt=10.0, price=100.0, instrument=inst)
    assert qty is None


def test_qty_for_notional_zero_price_returns_none():
    inst = FakeInstrument(size_increment=Decimal("0.001"))
    assert qty_for_notional(notional_usdt=500.0, price=0.0, instrument=inst) is None


def test_roundtrip_qty_picks_larger_min_qty():
    """两所最小下单量不同时，选较大值"""
    buy_inst = FakeInstrument(size_increment=Decimal("0.001"), min_quantity=Decimal("5"))
    sell_inst = FakeInstrument(size_increment=Decimal("0.001"), min_quantity=Decimal("3"))
    qty = roundtrip_qty(
        ask=100.0,
        buy_instrument=buy_inst,
        sell_instrument=sell_inst,
    )
    # max(5, 3) = 5
    assert qty == Decimal("5.000")


def test_roundtrip_qty_uses_coarser_increment():
    """使用较粗的步长向下取整"""
    buy_inst = FakeInstrument(size_increment=Decimal("0.1"), min_quantity=Decimal("5.55"))
    sell_inst = FakeInstrument(size_increment=Decimal("1"), min_quantity=Decimal("3"))
    qty = roundtrip_qty(
        ask=100.0,
        buy_instrument=buy_inst,
        sell_instrument=sell_inst,
    )
    # max(5.55, 3) = 5.55，按较粗步长 1 向下取整 = 5
    assert qty == Decimal("5")


def test_roundtrip_qty_respects_min_notional_on_either_side():
    """任一侧不满足 min_notional 时返回 None"""
    buy_inst = FakeInstrument(size_increment=Decimal("0.001"), min_quantity=Decimal("5"), min_notional=Decimal("1000"))
    sell_inst = FakeInstrument(size_increment=Decimal("0.001"), min_quantity=Decimal("3"))
    qty = roundtrip_qty(
        ask=100.0,
        buy_instrument=buy_inst,
        sell_instrument=sell_inst,
    )
    # max(5, 3) = 5, 但 5 * 100 = 500 < 1000 (buy_inst.min_notional)
    assert qty is None


def test_roundtrip_qty_zero_ask_returns_none():
    """ask 为 0 时返回 None"""
    inst = FakeInstrument(size_increment=Decimal("0.001"), min_quantity=Decimal("1"))
    qty = roundtrip_qty(
        ask=0.0,
        buy_instrument=inst,
        sell_instrument=inst,
    )
    assert qty is None


def test_roundtrip_qty_no_min_quantity_returns_none():
    """两所都没有 min_quantity 时返回 None"""
    buy_inst = FakeInstrument(size_increment=Decimal("0.001"))
    sell_inst = FakeInstrument(size_increment=Decimal("0.001"))
    qty = roundtrip_qty(
        ask=100.0,
        buy_instrument=buy_inst,
        sell_instrument=sell_inst,
    )
    assert qty is None
