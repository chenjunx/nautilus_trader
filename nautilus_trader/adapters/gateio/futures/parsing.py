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

from nautilus_trader.adapters.gateio.futures.http.models import GateIoFuturesOrder
from nautilus_trader.adapters.gateio.futures.http.models import GateIoFuturesPosition
from nautilus_trader.adapters.gateio.futures.http.models import GateIoFuturesTrade
from nautilus_trader.core.datetime import secs_to_nanos
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
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


def is_dual_mode(position: GateIoFuturesPosition) -> bool:
    """Return True if the position is reported under dual (hedge) mode."""
    return position.mode != "single"


def nautilus_order_side_to_gateio_futures_side(side: OrderSide) -> str:
    """Map a Nautilus order side to Gate.io's futures cancel-all `side` filter ('bid'/'ask')."""
    if side == OrderSide.BUY:
        return "bid"
    if side == OrderSide.SELL:
        return "ask"
    raise ValueError(f"Unsupported Nautilus order side for Gate.io futures cancel-all: {side!r}")


def gateio_futures_tif_to_nautilus(tif: str) -> TimeInForce:
    mapping = {
        "gtc": TimeInForce.GTC,
        "ioc": TimeInForce.IOC,
        "poc": TimeInForce.GTC,  # post-only is a GTC order with maker-only execution instruction
        "fok": TimeInForce.FOK,
    }
    if tif not in mapping:
        raise ValueError(f"Unknown Gate.io futures time in force: {tif!r}")
    return mapping[tif]


def format_client_order_id_as_text(client_order_id: str) -> str:
    """
    Gate.io futures requires custom order text to start with 't-' and be <= 30 chars total.
    """
    text = client_order_id if client_order_id.startswith("t-") else f"t-{client_order_id}"
    if len(text) > 30:
        raise ValueError(f"Gate.io order text exceeds 30 chars after 't-' prefix: {text!r}")
    return text


def futures_submit_order_params(
    contract: str,
    client_order_id: ClientOrderId,
    order_side: OrderSide,
    order_type: OrderType,
    quantity: Quantity,
    time_in_force: TimeInForce,
    post_only: bool,
    price: Price | None,
    reduce_only: bool,
) -> dict[str, object]:
    """
    Build the POST /futures/{settle}/orders request body from a Nautilus order.

    Gate.io futures `size` is a signed integer contract count: positive for BUY/long,
    negative for SELL/short. A `price` of "0" together with `tif="ioc"` submits a market
    order.

    """
    signed_size = int(quantity) if order_side == OrderSide.BUY else -int(quantity)

    if order_type == OrderType.MARKET:
        gateio_price = "0"
        gateio_tif = "ioc"
    elif order_type == OrderType.LIMIT:
        if price is None:
            raise ValueError("Gate.io futures LIMIT order requires a price")
        gateio_price = str(price)
        if post_only:
            gateio_tif = "poc"
        else:
            tif_map = {
                TimeInForce.GTC: "gtc",
                TimeInForce.IOC: "ioc",
                TimeInForce.FOK: "fok",
            }
            if time_in_force not in tif_map:
                raise ValueError(
                    f"Unsupported time in force for Gate.io futures LIMIT order: {time_in_force!r}",
                )
            gateio_tif = tif_map[time_in_force]
    else:
        raise ValueError(f"Unsupported order type for Gate.io futures: {order_type!r}")

    return {
        "contract": contract,
        "size": signed_size,
        "price": gateio_price,
        "tif": gateio_tif,
        "text": format_client_order_id_as_text(client_order_id.value),
        "reduce_only": reduce_only,
    }


def gateio_futures_order_status_to_nautilus(order: GateIoFuturesOrder) -> OrderStatus:
    if order.status == "open":
        if abs(order.size) != abs(order.left):
            return OrderStatus.PARTIALLY_FILLED
        return OrderStatus.ACCEPTED
    if order.status == "finished":
        if order.finish_as in ("filled", "liquidated"):
            return OrderStatus.FILLED
        return OrderStatus.CANCELED
    raise ValueError(f"Unknown Gate.io futures order status: {order.status!r}")


def parse_futures_order_status_report(
    account_id: AccountId,
    instrument_id: InstrumentId,
    order: GateIoFuturesOrder,
    client_order_id: ClientOrderId | None,
    price_precision: int,
    ts_init: int,
) -> OrderStatusReport:
    order_side = OrderSide.BUY if order.size >= 0 else OrderSide.SELL
    quantity = Quantity(abs(order.size), 0)
    filled_qty = Quantity(abs(order.size) - abs(order.left), 0)
    is_market = order.price == "0" and order.tif == "ioc"
    price = None if is_market else Price(Decimal(order.price), price_precision)
    ts_accepted = secs_to_nanos(order.create_time) if order.create_time else ts_init
    ts_last = secs_to_nanos(order.update_time) if order.update_time else ts_accepted

    return OrderStatusReport(
        account_id=account_id,
        instrument_id=instrument_id,
        client_order_id=client_order_id,
        venue_order_id=VenueOrderId(str(order.id)),
        order_side=order_side,
        order_type=OrderType.MARKET if is_market else OrderType.LIMIT,
        time_in_force=gateio_futures_tif_to_nautilus(order.tif),
        order_status=gateio_futures_order_status_to_nautilus(order),
        quantity=quantity,
        filled_qty=filled_qty,
        report_id=UUID4(),
        ts_accepted=ts_accepted,
        ts_last=ts_last,
        ts_init=ts_init,
        price=price,
        post_only=order.tif == "poc",
        reduce_only=order.is_reduce_only,
        avg_px=Decimal(order.fill_price) if order.fill_price else None,
    )


def parse_futures_fill_report(
    account_id: AccountId,
    instrument_id: InstrumentId,
    trade: GateIoFuturesTrade,
    client_order_id: ClientOrderId | None,
    price_precision: int,
    settle_currency: Currency,
    ts_init: int,
) -> FillReport:
    # The futures my_trades endpoint does not report a per-trade fee; funding/trading
    # fees are settled directly against the account balance instead.
    order_side = OrderSide.BUY if trade.size >= 0 else OrderSide.SELL
    ts_event = secs_to_nanos(trade.create_time) if trade.create_time else ts_init

    return FillReport(
        account_id=account_id,
        instrument_id=instrument_id,
        venue_order_id=VenueOrderId(str(trade.order_id)),
        trade_id=TradeId(str(trade.id)),
        order_side=order_side,
        last_qty=Quantity(abs(trade.size), 0),
        last_px=Price(Decimal(trade.price), price_precision),
        commission=Money(0, settle_currency),
        liquidity_side=LiquiditySide.MAKER if trade.role == "maker" else LiquiditySide.TAKER,
        report_id=UUID4(),
        ts_event=ts_event,
        ts_init=ts_init,
        client_order_id=client_order_id,
    )


def parse_futures_position_status_report(
    account_id: AccountId,
    instrument_id: InstrumentId,
    position: GateIoFuturesPosition,
    ts_init: int,
) -> PositionStatusReport:
    if position.size > 0:
        position_side = PositionSide.LONG
    elif position.size < 0:
        position_side = PositionSide.SHORT
    else:
        position_side = PositionSide.FLAT

    ts_last = secs_to_nanos(position.update_time) if position.update_time else ts_init

    return PositionStatusReport(
        account_id=account_id,
        instrument_id=instrument_id,
        position_side=position_side,
        quantity=Quantity(abs(position.size), 0),
        report_id=UUID4(),
        ts_last=ts_last,
        ts_init=ts_init,
        avg_px_open=Decimal(position.entry_price) if position.size != 0 else None,
    )
