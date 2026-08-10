"""下单数量计算的纯函数，不依赖 nautilus_trader，方便单测。

`instrument` 参数只要求具备 `size_increment`/`min_quantity`/`max_quantity`/`min_notional`
四个属性（真实 nautilus Instrument 满足，单测里传一个简单的 stub/namedtuple 也可以）。
"""

from decimal import ROUND_DOWN
from decimal import Decimal


def _dec(value) -> Decimal:
    """统一转成 Decimal。`Quantity`/`Money` 的 `str()` 可能带币种后缀（如 "5.00000000 USDT"），
    不能直接喂给 `Decimal()`，必须走它们的 `as_decimal()`。
    """
    if isinstance(value, Decimal):
        return value
    as_decimal = getattr(value, "as_decimal", None)
    if callable(as_decimal):
        return as_decimal()
    return Decimal(str(value))


def _quantize_down(value: Decimal, increment: Decimal) -> Decimal:
    if increment <= 0:
        return value
    steps = (value / increment).to_integral_value(rounding=ROUND_DOWN)
    return steps * increment


def _within_limits(qty: Decimal, price: Decimal, instrument) -> bool:
    min_qty = instrument.min_quantity
    if min_qty is not None and qty < _dec(min_qty):
        return False
    max_qty = instrument.max_quantity
    if max_qty is not None and qty > _dec(max_qty):
        return False
    min_notional = instrument.min_notional
    if min_notional is not None and qty * price < _dec(min_notional):
        return False
    return True


def qty_for_notional(notional_usdt, price, instrument) -> Decimal | None:
    """按名义金额换算下单数量，向下取整到 `size_increment`。

    低于 `min_quantity`/`min_notional` 或超过 `max_quantity` 时返回 None
    （调用方应视为"算不出安全数量，本轮跳过"）。
    """
    price_d = _dec(price)
    if price_d <= 0:
        return None

    raw_qty = _dec(notional_usdt) / price_d
    qty = _quantize_down(raw_qty, _dec(instrument.size_increment))
    if qty <= 0:
        return None

    if not _within_limits(qty, price_d, instrument):
        return None

    return qty


def roundtrip_qty(ask, buy_instrument, sell_instrument) -> Decimal | None:
    """套利下单数量 = max(两所的最小下单量)，用于库存调仓。

    不再考虑可用余额和单笔上限，直接用两所最小下单量中较大的那个作为固定下单量。
    只检查两边的 `min_quantity`/`max_quantity`/`min_notional` 是否能满足这个固定量。
    """
    ask_d = _dec(ask)
    if ask_d <= 0:
        return None

    # 获取两所的最小下单量，取较大值
    buy_min_qty = _dec(buy_instrument.min_quantity) if buy_instrument.min_quantity is not None else Decimal("0")
    sell_min_qty = _dec(sell_instrument.min_quantity) if sell_instrument.min_quantity is not None else Decimal("0")
    fixed_qty = max(buy_min_qty, sell_min_qty)

    if fixed_qty <= 0:
        return None

    # 按两边中较粗的 size_increment 向下取整
    increment = max(_dec(buy_instrument.size_increment), _dec(sell_instrument.size_increment))
    qty = _quantize_down(fixed_qty, increment)
    if qty <= 0:
        return None

    # 检查是否满足两边的限制
    if not _within_limits(qty, ask_d, buy_instrument):
        return None
    if not _within_limits(qty, ask_d, sell_instrument):
        return None

    return qty
