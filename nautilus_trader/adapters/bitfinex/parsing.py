# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

from decimal import Decimal

from nautilus_trader.adapters.bitfinex.constants import BITFINEX_VENUE
from nautilus_trader.adapters.bitfinex.http.models import BitfinexOrder
from nautilus_trader.adapters.bitfinex.http.models import BitfinexPosition
from nautilus_trader.adapters.bitfinex.http.models import BitfinexTrade
from nautilus_trader.adapters.bitfinex.http.models import BitfinexWallet
from nautilus_trader.adapters.bitfinex.providers import _CURRENCY_REMAP
from nautilus_trader.adapters.bitfinex.providers import bitfinex_pair_to_nautilus
from nautilus_trader.adapters.bitfinex.providers import nautilus_to_bitfinex_pair
from nautilus_trader.core.datetime import millis_to_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


# POST_ONLY order flag bit, see https://docs.bitfinex.com/reference/rest-auth-submit-order
POST_ONLY_FLAG = 4096


def bitfinex_symbol_to_instrument_id(symbol: str) -> InstrumentId:
    """Convert a Bitfinex WS/REST trading symbol (e.g. ``"tBTCUSD"``) to an `InstrumentId`."""
    pair = symbol[1:] if symbol.startswith("t") else symbol
    return InstrumentId(Symbol(bitfinex_pair_to_nautilus(pair)), BITFINEX_VENUE)


def instrument_id_to_bitfinex_symbol(raw_symbol: str) -> str:
    """Convert an instrument's Nautilus `raw_symbol` value to a Bitfinex trading symbol."""
    return f"t{nautilus_to_bitfinex_pair(raw_symbol)}"


def bitfinex_order_side_from_amount(amount: float) -> OrderSide:
    return OrderSide.BUY if amount >= 0 else OrderSide.SELL


def bitfinex_order_type_to_nautilus(order_type: str) -> OrderType:
    base_type = order_type.removeprefix("EXCHANGE ")
    if base_type in ("LIMIT", "IOC", "FOK"):
        return OrderType.LIMIT
    if base_type == "MARKET":
        return OrderType.MARKET
    raise ValueError(f"Unsupported Bitfinex order type: {order_type!r}")


def bitfinex_tif_from_order_type(order_type: str) -> TimeInForce:
    base_type = order_type.removeprefix("EXCHANGE ")
    if base_type == "IOC":
        return TimeInForce.IOC
    if base_type == "FOK":
        return TimeInForce.FOK
    return TimeInForce.GTC


def bitfinex_order_status_to_nautilus(status: str) -> OrderStatus:
    # Bitfinex status strings can carry a trailing detail, e.g. "EXECUTED @ 100.0(...)"
    s = status.upper()
    if s.startswith("ACTIVE"):
        return OrderStatus.ACCEPTED
    if s.startswith("PARTIALLY FILLED"):
        return OrderStatus.PARTIALLY_FILLED
    if s.startswith("EXECUTED"):
        return OrderStatus.FILLED
    if "CANCELED" in s:
        return OrderStatus.CANCELED
    if s.startswith("INSUFFICIENT MARGIN") or s.startswith("RSN_"):
        return OrderStatus.REJECTED
    raise ValueError(f"Unknown Bitfinex order status: {status!r}")


def nautilus_order_type_tif_to_bitfinex(
    order_type: OrderType,
    time_in_force: TimeInForce,
    post_only: bool,
    is_perpetual: bool,
) -> tuple[str, int]:
    """
    Map a Nautilus order type/TIF/post_only combination to a Bitfinex order type string
    and flags bitmask.

    Perpetual (margin wallet) orders use unprefixed type strings (``"LIMIT"``, ``"MARKET"``,
    ``"IOC"``, ``"FOK"``); spot (exchange wallet) orders are prefixed with ``"EXCHANGE "``.

    """
    prefix = "" if is_perpetual else "EXCHANGE "
    flags = POST_ONLY_FLAG if post_only else 0

    if order_type == OrderType.MARKET:
        return f"{prefix}MARKET", flags
    if order_type == OrderType.LIMIT:
        if time_in_force == TimeInForce.IOC:
            return f"{prefix}IOC", flags
        if time_in_force == TimeInForce.FOK:
            return f"{prefix}FOK", flags
        if time_in_force == TimeInForce.GTC:
            return f"{prefix}LIMIT", flags
        raise ValueError(f"Unsupported time in force for Bitfinex LIMIT order: {time_in_force!r}")
    raise ValueError(f"Unsupported order type for Bitfinex: {order_type!r}")


def bitfinex_submit_order_params(
    raw_symbol: str,
    cid: int,
    order_side: OrderSide,
    order_type: OrderType,
    quantity: Quantity,
    time_in_force: TimeInForce,
    post_only: bool,
    price: Price | None,
    is_perpetual: bool,
) -> dict[str, object]:
    """Build the POST auth/w/order/submit request body from a Nautilus order."""
    bitfinex_type, flags = nautilus_order_type_tif_to_bitfinex(
        order_type,
        time_in_force,
        post_only,
        is_perpetual,
    )
    signed_amount = str(quantity) if order_side == OrderSide.BUY else f"-{quantity}"
    body: dict[str, object] = {
        "type": bitfinex_type,
        "symbol": instrument_id_to_bitfinex_symbol(raw_symbol),
        "amount": signed_amount,
        "cid": cid,
    }
    if flags:
        body["flags"] = flags
    if bitfinex_type.removeprefix("EXCHANGE ") != "MARKET":
        if price is None:
            raise ValueError("Bitfinex LIMIT/IOC/FOK order requires a price")
        body["price"] = str(price)
    return body


def parse_order_status_report(
    account_id: AccountId,
    instrument_id: InstrumentId,
    order: BitfinexOrder,
    client_order_id: ClientOrderId | None,
    price_precision: int,
    size_precision: int,
    ts_init: int,
) -> OrderStatusReport:
    quantity = Quantity(abs(order.amount_orig), size_precision)
    filled_qty = Quantity(max(abs(order.amount_orig) - abs(order.amount), 0.0), size_precision)
    order_type = bitfinex_order_type_to_nautilus(order.order_type)
    price = Price(order.price, price_precision) if order_type == OrderType.LIMIT else None
    ts_accepted = millis_to_nanos(order.mts_create) if order.mts_create else ts_init
    ts_last = millis_to_nanos(order.mts_update) if order.mts_update else ts_accepted

    return OrderStatusReport(
        account_id=account_id,
        instrument_id=instrument_id,
        client_order_id=client_order_id,
        venue_order_id=VenueOrderId(str(order.id)),
        order_side=bitfinex_order_side_from_amount(order.amount_orig),
        order_type=order_type,
        time_in_force=bitfinex_tif_from_order_type(order.order_type),
        order_status=bitfinex_order_status_to_nautilus(order.status),
        quantity=quantity,
        filled_qty=filled_qty,
        report_id=UUID4(),
        ts_accepted=ts_accepted,
        ts_last=ts_last,
        ts_init=ts_init,
        price=price,
        post_only=bool(order.flags and order.flags & POST_ONLY_FLAG),
        avg_px=Decimal(str(order.price_avg)) if order.price_avg else None,
    )


def parse_fill_report(
    account_id: AccountId,
    instrument_id: InstrumentId,
    trade: BitfinexTrade,
    client_order_id: ClientOrderId | None,
    price_precision: int,
    size_precision: int,
    quote_currency: Currency,
    ts_init: int,
) -> FillReport:
    ts_event = millis_to_nanos(trade.mts_create) if trade.mts_create else ts_init
    commission_currency = trade.fee_currency or quote_currency.code
    fee = abs(trade.fee) if trade.fee is not None else 0.0

    return FillReport(
        account_id=account_id,
        instrument_id=instrument_id,
        venue_order_id=VenueOrderId(str(trade.order_id)),
        trade_id=TradeId(str(trade.id)),
        order_side=bitfinex_order_side_from_amount(trade.exec_amount),
        last_qty=Quantity(abs(trade.exec_amount), size_precision),
        last_px=Price(trade.exec_price, price_precision),
        commission=Money(Decimal(str(fee)), Currency.from_str(commission_currency)),
        liquidity_side=LiquiditySide.MAKER if trade.maker == 1 else LiquiditySide.TAKER,
        report_id=UUID4(),
        ts_event=ts_event,
        ts_init=ts_init,
        client_order_id=client_order_id,
    )


def parse_position_status_report(
    account_id: AccountId,
    instrument_id: InstrumentId,
    position: BitfinexPosition,
    size_precision: int,
    ts_init: int,
) -> PositionStatusReport:
    ts_last = millis_to_nanos(position.mts_update) if position.mts_update else ts_init
    if position.amount > 0:
        side = PositionSide.LONG
    elif position.amount < 0:
        side = PositionSide.SHORT
    else:
        side = PositionSide.FLAT

    return PositionStatusReport(
        account_id=account_id,
        instrument_id=instrument_id,
        position_side=side,
        quantity=Quantity(abs(position.amount), size_precision),
        report_id=UUID4(),
        ts_last=ts_last,
        ts_init=ts_init,
        avg_px_open=Decimal(str(position.base_price)) if position.base_price else None,
    )


def parse_account_balances(wallets: list[BitfinexWallet]) -> list[AccountBalance]:
    """
    Merge Bitfinex ``exchange`` (spot) and ``margin`` (perpetual) wallet balances into a
    single list of `AccountBalance`, one per currency.
    """
    totals: dict[str, Decimal] = {}
    frees: dict[str, Decimal] = {}

    for wallet in wallets:
        if wallet.wallet_type not in ("exchange", "margin"):
            continue
        currency_code = _CURRENCY_REMAP.get(wallet.currency, wallet.currency)
        balance = Decimal(str(wallet.balance))
        available = Decimal(str(wallet.balance_available)) if (
            wallet.balance_available is not None
        ) else balance
        totals[currency_code] = totals.get(currency_code, Decimal(0)) + balance
        frees[currency_code] = frees.get(currency_code, Decimal(0)) + available

    balances = []
    for code, total in totals.items():
        currency = Currency.from_str(code)
        free = max(min(frees.get(code, total), total), Decimal(0))
        locked = total - free
        balances.append(
            AccountBalance(
                total=Money(total, currency),
                locked=Money(locked, currency),
                free=Money(free, currency),
            ),
        )
    return balances
