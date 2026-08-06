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

from nautilus_trader.adapters.gateio.spot.http.models import GateIoOrder
from nautilus_trader.adapters.gateio.spot.http.models import GateIoTrade
from nautilus_trader.core.datetime import millis_to_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


def gateio_order_side_to_nautilus(side: str) -> OrderSide:
    if side == "buy":
        return OrderSide.BUY
    if side == "sell":
        return OrderSide.SELL
    raise ValueError(f"Unknown Gate.io spot order side: {side!r}")


def nautilus_order_side_to_gateio(side: OrderSide) -> str:
    if side == OrderSide.BUY:
        return "buy"
    if side == OrderSide.SELL:
        return "sell"
    raise ValueError(f"Unsupported Nautilus order side for Gate.io spot: {side!r}")


def gateio_order_type_to_nautilus(order_type: str) -> OrderType:
    if order_type == "limit":
        return OrderType.LIMIT
    if order_type == "market":
        return OrderType.MARKET
    raise ValueError(f"Unknown Gate.io spot order type: {order_type!r}")


def gateio_tif_to_nautilus(tif: str) -> TimeInForce:
    mapping = {
        "gtc": TimeInForce.GTC,
        "ioc": TimeInForce.IOC,
        "poc": TimeInForce.GTC,  # post-only is a GTC order with maker-only execution instruction
        "fok": TimeInForce.FOK,
    }
    if tif not in mapping:
        raise ValueError(f"Unknown Gate.io spot time in force: {tif!r}")
    return mapping[tif]


def nautilus_order_type_tif_to_gateio(
    order_type: OrderType,
    time_in_force: TimeInForce,
    post_only: bool,
) -> tuple[str, str]:
    """Map a Nautilus order type/TIF/post_only combination to Gate.io (type, time_in_force)."""
    if order_type == OrderType.MARKET:
        return "market", "ioc"
    if order_type == OrderType.LIMIT:
        if post_only:
            return "limit", "poc"
        tif_map = {
            TimeInForce.GTC: "gtc",
            TimeInForce.IOC: "ioc",
            TimeInForce.FOK: "fok",
        }
        if time_in_force not in tif_map:
            raise ValueError(
                f"Unsupported time in force for Gate.io spot LIMIT order: {time_in_force!r}",
            )
        return "limit", tif_map[time_in_force]
    raise ValueError(f"Unsupported order type for Gate.io spot: {order_type!r}")


def gateio_order_status_to_nautilus(order: GateIoOrder) -> OrderStatus:
    if order.status == "open":
        if Decimal(order.filled_total) > 0:
            return OrderStatus.PARTIALLY_FILLED
        return OrderStatus.ACCEPTED
    if order.status == "closed":
        return OrderStatus.FILLED
    if order.status == "cancelled":
        return OrderStatus.CANCELED
    raise ValueError(f"Unknown Gate.io spot order status: {order.status!r}")


def format_client_order_id_as_text(client_order_id: str) -> str:
    """
    Gate.io spot requires custom order text to start with 't-' and be <= 30 chars total.
    """
    text = client_order_id if client_order_id.startswith("t-") else f"t-{client_order_id}"
    if len(text) > 30:
        raise ValueError(f"Gate.io order text exceeds 30 chars after 't-' prefix: {text!r}")
    return text


def spot_submit_order_params(
    currency_pair: str,
    client_order_id: ClientOrderId,
    order_side: OrderSide,
    order_type: OrderType,
    quantity: Quantity,
    time_in_force: TimeInForce,
    post_only: bool,
    price: Price | None,
    is_quote_quantity: bool,
) -> dict[str, object]:
    """
    Build the POST /spot/orders request body from a Nautilus order.

    For MARKET BUY orders Gate.io's `amount` is denominated in quote currency, so the
    caller's order must have `is_quote_quantity=True` in that case; MARKET SELL and all
    LIMIT orders use base-currency amounts.

    """
    gateio_type, gateio_tif = nautilus_order_type_tif_to_gateio(
        order_type,
        time_in_force,
        post_only,
    )
    body: dict[str, object] = {
        "currency_pair": currency_pair,
        "side": nautilus_order_side_to_gateio(order_side),
        "type": gateio_type,
        "time_in_force": gateio_tif,
        "text": format_client_order_id_as_text(client_order_id.value),
        "amount": str(quantity),
    }
    if gateio_type == "limit":
        if price is None:
            raise ValueError("Gate.io spot LIMIT order requires a price")
        body["price"] = str(price)
    elif order_side == OrderSide.BUY and not is_quote_quantity:
        raise ValueError(
            "Gate.io spot MARKET BUY orders require quote-denominated quantity "
            "(order.is_quote_quantity must be True)",
        )
    return body


def parse_spot_order_status_report(
    account_id: AccountId,
    instrument_id: InstrumentId,
    order: GateIoOrder,
    client_order_id: ClientOrderId | None,
    price_precision: int,
    size_precision: int,
    ts_init: int,
) -> OrderStatusReport:
    quantity = Quantity(Decimal(order.amount), size_precision)
    filled_qty = Quantity(Decimal(order.filled_total), size_precision)
    price = Price(Decimal(order.price), price_precision) if order.type == "limit" else None
    ts_accepted = millis_to_nanos(order.create_time_ms) if order.create_time_ms else ts_init
    ts_last = millis_to_nanos(order.update_time_ms) if order.update_time_ms else ts_accepted

    return OrderStatusReport(
        account_id=account_id,
        instrument_id=instrument_id,
        client_order_id=client_order_id,
        venue_order_id=VenueOrderId(order.id),
        order_side=gateio_order_side_to_nautilus(order.side),
        order_type=gateio_order_type_to_nautilus(order.type),
        time_in_force=gateio_tif_to_nautilus(order.time_in_force),
        order_status=gateio_order_status_to_nautilus(order),
        quantity=quantity,
        filled_qty=filled_qty,
        report_id=UUID4(),
        ts_accepted=ts_accepted,
        ts_last=ts_last,
        ts_init=ts_init,
        price=price,
        post_only=order.time_in_force == "poc",
        avg_px=Decimal(order.avg_deal_price) if order.avg_deal_price else None,
    )


def parse_spot_fill_report(
    account_id: AccountId,
    instrument_id: InstrumentId,
    trade: GateIoTrade,
    client_order_id: ClientOrderId | None,
    price_precision: int,
    size_precision: int,
    quote_currency: Currency,
    ts_init: int,
) -> FillReport:
    ts_event = millis_to_nanos(trade.create_time_ms) if trade.create_time_ms else ts_init
    commission_currency = trade.fee_currency or quote_currency.code

    return FillReport(
        account_id=account_id,
        instrument_id=instrument_id,
        venue_order_id=VenueOrderId(trade.order_id),
        trade_id=TradeId(trade.id),
        order_side=gateio_order_side_to_nautilus(trade.side),
        last_qty=Quantity(Decimal(trade.amount), size_precision),
        last_px=Price(Decimal(trade.price), price_precision),
        commission=Money(Decimal(trade.fee), Currency.from_str(commission_currency)),
        liquidity_side=LiquiditySide.MAKER if trade.role == "maker" else LiquiditySide.TAKER,
        report_id=UUID4(),
        ts_event=ts_event,
        ts_init=ts_init,
        client_order_id=client_order_id,
    )
