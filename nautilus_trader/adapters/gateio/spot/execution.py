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

import asyncio
from decimal import Decimal

import msgspec

from nautilus_trader.adapters.gateio.common.constants import GATEIO_SPOT_WS_BASE_URL
from nautilus_trader.adapters.gateio.common.credentials import get_api_key
from nautilus_trader.adapters.gateio.common.credentials import get_api_secret
from nautilus_trader.adapters.gateio.config import GateIoExecClientConfig
from nautilus_trader.adapters.gateio.spot.http.client import GateIoSpotHttpClient
from nautilus_trader.adapters.gateio.spot.http.models import GateIoBalance
from nautilus_trader.adapters.gateio.spot.http.models import GateIoOrder
from nautilus_trader.adapters.gateio.spot.http.models import GateIoTrade
from nautilus_trader.adapters.gateio.spot.parsing import gateio_order_side_to_nautilus
from nautilus_trader.adapters.gateio.spot.parsing import nautilus_order_side_to_gateio
from nautilus_trader.adapters.gateio.spot.parsing import parse_spot_fill_report
from nautilus_trader.adapters.gateio.spot.parsing import parse_spot_order_status_report
from nautilus_trader.adapters.gateio.spot.parsing import spot_submit_order_params
from nautilus_trader.adapters.gateio.spot.providers import GateIoSpotInstrumentProvider
from nautilus_trader.adapters.gateio.spot.websocket.client import GateIoSpotWebSocketClient
from nautilus_trader.adapters.gateio.spot.websocket.schemas import GateIoWsMessage
from nautilus_trader.adapters.gateio.spot.websocket.schemas import GateIoWsSpotBalance
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.common.secure import mask_api_key
from nautilus_trader.core.nautilus_pyo3 import Quota
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import BatchCancelOrders
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReport
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import QueryAccount
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import Order


class GateIoSpotExecutionClient(LiveExecutionClient):
    """
    Provides an execution client for the Gate.io spot exchange.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    config : GateIoExecClientConfig
        The configuration for the client.
    name : str, optional
        The custom client ID.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        config: GateIoExecClientConfig,
        name: str | None = None,
    ) -> None:
        self._api_key = config.api_key or get_api_key()
        self._api_secret = config.api_secret or get_api_secret()

        self._http_client = GateIoSpotHttpClient(
            api_key=self._api_key,
            api_secret=self._api_secret,
            base_url=config.base_url_http,
            default_quota=Quota.rate_per_second(config.ratelimiter_default_quota_per_second),
        )
        instrument_provider = GateIoSpotInstrumentProvider(
            http_client=self._http_client,
            clock=clock,
            venue=config.venue,
            config=config.instrument_provider,
        )

        super().__init__(
            loop=loop,
            client_id=ClientId(name or config.venue.value),
            venue=config.venue,
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            base_currency=None,
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )

        account_id = AccountId(f"{self.id.value}-master")
        self._set_account_id(account_id)

        ws_url = config.base_url_ws or GATEIO_SPOT_WS_BASE_URL
        self._ws_client = GateIoSpotWebSocketClient(
            loop=loop,
            url=ws_url,
            handler=self._handle_ws_message,
            handler_reconnect=self._resubscribe,
            api_key=self._api_key,
            api_secret=self._api_secret,
        )
        self._ws_decoder = msgspec.json.Decoder(GateIoWsMessage)

    # -- CONNECTION ---------------------------------------------------------------------------

    async def _connect(self) -> None:
        await self._instrument_provider.initialize()

        await self._update_account_state()
        await self._await_account_registered()

        await self._ws_client.connect()
        await self._ws_client.subscribe_orders()
        await self._ws_client.subscribe_usertrades()
        await self._ws_client.subscribe_balances()

        if self._api_key:
            self._log.info(f"Gate.io API key {mask_api_key(self._api_key)}", LogColor.BLUE)
        self._log.info("Gate.io spot API key authenticated", LogColor.GREEN)

    async def _disconnect(self) -> None:
        await self._ws_client.disconnect()

    async def _resubscribe(self) -> None:
        self._log.info("Resubscribing to private channels after reconnect...", LogColor.BLUE)
        await self._ws_client.subscribe_orders()
        await self._ws_client.subscribe_usertrades()
        await self._ws_client.subscribe_balances()

        self.create_task(
            self._reconcile_after_reconnect(),
            log_msg="reconcile_after_reconnect",
        )

    async def _reconcile_after_reconnect(self) -> None:
        try:
            command = GenerateOrderStatusReports(
                instrument_id=None,
                start=None,
                end=None,
                open_only=True,
                command_id=UUID4(),
                ts_init=self._clock.timestamp_ns(),
            )
            reports = await self.generate_order_status_reports(command)
            for report in reports:
                self._send_order_status_report(report)
        except Exception as e:
            self._log.exception("Failed to reconcile after reconnect", e)

    # -- ACCOUNT --------------------------------------------------------------------------------

    async def _query_account(self, _command: QueryAccount) -> None:
        await self._update_account_state()

    async def _update_account_state(self) -> None:
        balances = await self._http_client.list_spot_accounts()
        account_balances = [self._parse_account_balance(b) for b in balances]

        self.generate_account_state(
            balances=account_balances,
            margins=[],
            reported=True,
            ts_event=self._clock.timestamp_ns(),
        )

    def _parse_account_balance(self, balance: GateIoBalance) -> AccountBalance:
        currency = Currency.from_str(balance.currency)
        free = Money(Decimal(balance.available), currency)
        locked = Money(Decimal(balance.locked), currency)
        total = free + locked
        return AccountBalance(total=total, locked=locked, free=free)

    # -- ORDER SUBMISSION -------------------------------------------------------------------------

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        for order in command.order_list.orders:
            await self._submit_order(
                SubmitOrder(
                    trader_id=command.trader_id,
                    strategy_id=command.strategy_id,
                    order=order,
                    command_id=UUID4(),
                    ts_init=self._clock.timestamp_ns(),
                    position_id=command.position_id,
                    client_id=command.client_id,
                ),
            )

    async def _submit_order(self, command: SubmitOrder) -> None:
        order = command.order

        if order.is_closed:
            self._log.warning(f"Cannot submit already closed order: {order}")
            return

        instrument = self._cache.instrument(order.instrument_id)
        if instrument is None:
            self._deny_order(order, f"No instrument found for {order.instrument_id}")
            return

        if not self._validate_order_pre_submit(order, instrument):
            return

        try:
            params = spot_submit_order_params(
                currency_pair=instrument.raw_symbol.value,
                client_order_id=order.client_order_id,
                order_side=order.side,
                order_type=order.order_type,
                quantity=order.quantity,
                time_in_force=order.time_in_force,
                post_only=order.is_post_only,
                price=order.price if order.has_price else None,
                is_quote_quantity=order.is_quote_quantity,
            )
        except ValueError as e:
            self._deny_order(order, str(e))
            return

        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

        try:
            gateio_order = await self._http_client.submit_order(
                currency_pair=params["currency_pair"],
                side=params["side"],
                order_type=params["type"],
                amount=params["amount"],
                time_in_force=params["time_in_force"],
                text=params["text"],
                price=params.get("price"),
            )
        except Exception as e:
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason=str(e),
                ts_event=self._clock.timestamp_ns(),
            )
            return

        self.generate_order_accepted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=VenueOrderId(gateio_order.id),
            ts_event=self._clock.timestamp_ns(),
        )

    def _validate_order_pre_submit(self, order: Order, instrument: Instrument) -> bool:
        size = Decimal(str(order.quantity))
        size_step = Decimal(str(instrument.size_increment))
        if size % size_step != 0:
            self._deny_order(
                order,
                f"quantity {order.quantity} not aligned with size increment "
                f"{instrument.size_increment}",
            )
            return False

        if order.has_price:
            price = Decimal(str(order.price))
            price_step = Decimal(str(instrument.price_increment))
            if price % price_step != 0:
                self._deny_order(
                    order,
                    f"price {order.price} not aligned with price increment "
                    f"{instrument.price_increment}",
                )
                return False

        return True

    def _deny_order(self, order: Order, reason: str) -> None:
        self._log.error(f"Cannot submit order {order.client_order_id}: {reason}", LogColor.RED)
        self.generate_order_denied(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            reason=reason,
            ts_event=self._clock.timestamp_ns(),
        )

    # -- ORDER MODIFICATION / CANCELLATION -------------------------------------------------------

    async def _modify_order(self, command: ModifyOrder) -> None:
        order = self._cache.order(command.client_order_id)
        reason = "Gate.io spot has no native order amend endpoint; cancel and resubmit instead"
        self._log.error(f"Cannot modify order {command.client_order_id}: {reason}")
        self.generate_order_modify_rejected(
            strategy_id=command.strategy_id,
            instrument_id=command.instrument_id,
            client_order_id=command.client_order_id,
            venue_order_id=command.venue_order_id or (order.venue_order_id if order else None),
            reason=reason,
            ts_event=self._clock.timestamp_ns(),
        )

    async def _cancel_order(self, command: CancelOrder) -> None:
        order = self._cache.order(command.client_order_id)
        if order is None:
            self._log.error(f"Order not found: {command.client_order_id}")
            self.generate_order_cancel_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=command.venue_order_id,
                reason=f"Order not found: {command.client_order_id}",
                ts_event=self._clock.timestamp_ns(),
            )
            return

        venue_order_id = command.venue_order_id or order.venue_order_id
        if venue_order_id is None:
            self._log.error(
                f"Cannot cancel order without venue_order_id: {command.client_order_id}",
            )
            self.generate_order_cancel_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=None,
                reason=f"Cannot cancel order without venue_order_id: {command.client_order_id}",
                ts_event=self._clock.timestamp_ns(),
            )
            return

        instrument = self._cache.instrument(order.instrument_id)
        currency_pair = (
            instrument.raw_symbol.value if instrument else order.instrument_id.symbol.value
        )

        try:
            await self._http_client.cancel_order(
                order_id=venue_order_id.value,
                currency_pair=currency_pair,
            )
        except Exception as e:
            self._log.error(f"Failed to cancel order {order.client_order_id}: {e}")
            self.generate_order_cancel_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=venue_order_id,
                reason=str(e),
                ts_event=self._clock.timestamp_ns(),
            )

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        instrument = self._cache.instrument(command.instrument_id)
        if instrument is None:
            self._log.error(f"No instrument found for {command.instrument_id}")
            return

        side = None
        if command.order_side != OrderSide.NO_ORDER_SIDE:
            side = nautilus_order_side_to_gateio(command.order_side)

        try:
            await self._http_client.cancel_all_open_orders(
                currency_pair=instrument.raw_symbol.value,
                side=side,
            )
        except Exception as e:
            self._log.error(f"Failed to cancel all orders for {command.instrument_id}: {e}")

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        if not command.cancels:
            self._log.debug("batch_cancel_orders called with empty cancels list")
            return

        order_lookup: dict[str, Order] = {}
        cancels_body: list[dict[str, str]] = []

        for cancel in command.cancels:
            order = self._cache.order(cancel.client_order_id)
            if order is None:
                self._log.warning(f"Skipping cancel - order not found: {cancel.client_order_id}")
                self.generate_order_cancel_rejected(
                    strategy_id=cancel.strategy_id,
                    instrument_id=cancel.instrument_id,
                    client_order_id=cancel.client_order_id,
                    venue_order_id=cancel.venue_order_id,
                    reason=f"Order not found: {cancel.client_order_id}",
                    ts_event=self._clock.timestamp_ns(),
                )
                continue

            venue_order_id = cancel.venue_order_id or order.venue_order_id
            if venue_order_id is None:
                self._log.warning(
                    f"Skipping cancel for {cancel.client_order_id} - no venue_order_id",
                )
                self.generate_order_cancel_rejected(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=None,
                    reason=f"Cannot cancel order without venue_order_id: {cancel.client_order_id}",
                    ts_event=self._clock.timestamp_ns(),
                )
                continue

            instrument = self._cache.instrument(order.instrument_id)
            currency_pair = (
                instrument.raw_symbol.value if instrument else order.instrument_id.symbol.value
            )

            order_lookup[venue_order_id.value] = order
            cancels_body.append({"currency_pair": currency_pair, "id": venue_order_id.value})

        if not cancels_body:
            return

        try:
            results = await self._http_client.batch_cancel_orders(cancels_body)
        except Exception as e:
            self._log.error(f"Batch cancel request failed: {e}")
            for order in order_lookup.values():
                self.generate_order_cancel_rejected(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=order.venue_order_id,
                    reason=str(e),
                    ts_event=self._clock.timestamp_ns(),
                )
            return

        for result in results:
            order = order_lookup.get(result.id)
            if order is None:
                continue
            if result.succeeded is False:
                self.generate_order_cancel_rejected(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=order.venue_order_id,
                    reason=result.message or "Batch cancel failed",
                    ts_event=self._clock.timestamp_ns(),
                )

    # -- EXECUTION REPORTS ------------------------------------------------------------------------

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        if command.client_order_id is None and command.venue_order_id is None:
            raise ValueError("Both `client_order_id` and `venue_order_id` cannot be None")

        if command.instrument_id is None:
            self._log.warning("Cannot query order status: no instrument_id provided")
            return None

        instrument = self._cache.instrument(command.instrument_id)
        if instrument is None:
            self._log_report_error(
                ValueError(f"No instrument found for {command.instrument_id}"),
                "OrderStatusReport",
            )
            return None

        order_id = command.venue_order_id.value if command.venue_order_id is not None else None
        if order_id is None and command.client_order_id is not None:
            cached_order = self._cache.order(command.client_order_id)
            if cached_order is not None and cached_order.venue_order_id is not None:
                order_id = cached_order.venue_order_id.value

        if order_id is None:
            self._log.warning(
                f"Cannot query order status: no venue_order_id resolvable for "
                f"{command.client_order_id}",
            )
            return None

        try:
            gateio_order = await self._http_client.query_order(
                order_id=order_id,
                currency_pair=instrument.raw_symbol.value,
            )
        except Exception as e:
            self._log_report_error(e, "OrderStatusReport")
            return None

        return parse_spot_order_status_report(
            account_id=self.account_id,
            instrument_id=command.instrument_id,
            order=gateio_order,
            client_order_id=command.client_order_id,
            price_precision=instrument.price_precision,
            size_precision=instrument.size_precision,
            ts_init=self._clock.timestamp_ns(),
        )

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        reports: list[OrderStatusReport] = []
        ts_init = self._clock.timestamp_ns()

        try:
            if command.instrument_id is not None:
                instrument = self._cache.instrument(command.instrument_id)
                if instrument is None:
                    self._log.warning(f"No instrument found for {command.instrument_id}")
                    return []

                statuses = ["open"] if command.open_only else ["open", "finished"]
                for status in statuses:
                    gateio_orders = await self._http_client.list_orders(
                        currency_pair=instrument.raw_symbol.value,
                        status=status,
                    )
                    for gateio_order in gateio_orders:
                        reports.append(
                            self._parse_order_status_report(
                                command.instrument_id,
                                instrument,
                                gateio_order,
                                ts_init,
                            ),
                        )
            else:
                open_orders_by_pair = await self._http_client.list_open_orders()
                for entry in open_orders_by_pair:
                    instrument_id = InstrumentId(Symbol(entry.currency_pair), self.venue)
                    instrument = self._cache.instrument(instrument_id)
                    if instrument is None:
                        continue
                    for gateio_order in entry.orders:
                        reports.append(
                            self._parse_order_status_report(
                                instrument_id,
                                instrument,
                                gateio_order,
                                ts_init,
                            ),
                        )
        except Exception as e:
            self._log_report_error(e, "OrderStatusReport")
            return []

        self._log_report_receipt(len(reports), "OrderStatusReport", command.log_receipt_level)
        return reports

    def _parse_order_status_report(
        self,
        instrument_id: InstrumentId,
        instrument: Instrument,
        gateio_order: GateIoOrder,
        ts_init: int,
    ) -> OrderStatusReport:
        client_order_id = self._resolve_client_order_id(
            VenueOrderId(gateio_order.id),
            gateio_order.text,
        )
        return parse_spot_order_status_report(
            account_id=self.account_id,
            instrument_id=instrument_id,
            order=gateio_order,
            client_order_id=client_order_id,
            price_precision=instrument.price_precision,
            size_precision=instrument.size_precision,
            ts_init=ts_init,
        )

    async def generate_fill_reports(
        self,
        command: GenerateFillReports,
    ) -> list[FillReport]:
        reports: list[FillReport] = []
        ts_init = self._clock.timestamp_ns()

        currency_pair = None
        if command.instrument_id is not None:
            instrument = self._cache.instrument(command.instrument_id)
            if instrument is None:
                self._log.warning(f"No instrument found for {command.instrument_id}")
                return []
            currency_pair = instrument.raw_symbol.value

        order_id = command.venue_order_id.value if command.venue_order_id is not None else None

        try:
            trades = await self._http_client.list_my_trades(
                currency_pair=currency_pair,
                order_id=order_id,
            )
        except Exception as e:
            self._log_report_error(e, "FillReport")
            return []

        for trade in trades:
            instrument_id = command.instrument_id or InstrumentId(
                Symbol(trade.currency_pair),
                self.venue,
            )
            instrument = self._cache.instrument(instrument_id)
            if instrument is None:
                continue
            client_order_id = self._resolve_client_order_id(
                VenueOrderId(trade.order_id),
                trade.text,
            )
            reports.append(
                parse_spot_fill_report(
                    account_id=self.account_id,
                    instrument_id=instrument_id,
                    trade=trade,
                    client_order_id=client_order_id,
                    price_precision=instrument.price_precision,
                    size_precision=instrument.size_precision,
                    quote_currency=instrument.quote_currency,
                    ts_init=ts_init,
                ),
            )

        self._log_report_receipt(len(reports), "FillReport", LogLevel.INFO)
        return reports

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        # Gate.io spot is a cash account: no positions to report
        return []

    # -- WEBSOCKET MESSAGE HANDLING ---------------------------------------------------------------

    def _handle_ws_message(self, raw: bytes) -> None:
        try:
            msg = self._ws_decoder.decode(raw)
        except Exception as e:
            self._log.warning(f"Failed to decode WS message: {e!r} | {raw!r}")
            return

        if msg.event == "error":
            self._log.error(f"Gate.io WS error: {msg.error}")
            return

        if msg.event != "update" or msg.result is None:
            return

        if msg.channel == "spot.orders":
            self._handle_orders_update(msg.result)
        elif msg.channel == "spot.usertrades":
            self._handle_usertrades_update(msg.result)
        elif msg.channel == "spot.balances":
            self._handle_balances_update(msg.result)

    def _handle_orders_update(self, result: object) -> None:
        try:
            gateio_orders = msgspec.convert(result, list[GateIoOrder])
        except Exception as e:
            self._log.warning(f"Failed to convert orders update: {e!r}")
            return

        for gateio_order in gateio_orders:
            self._process_order_update(gateio_order)

    def _process_order_update(self, gateio_order: GateIoOrder) -> None:
        venue_order_id = VenueOrderId(gateio_order.id)
        client_order_id = self._resolve_client_order_id(venue_order_id, gateio_order.text)

        if not self._is_order_internal(client_order_id):
            instrument_id = InstrumentId(Symbol(gateio_order.currency_pair), self.venue)
            instrument = self._cache.instrument(instrument_id)
            if instrument is None:
                return
            report = parse_spot_order_status_report(
                account_id=self.account_id,
                instrument_id=instrument_id,
                order=gateio_order,
                client_order_id=client_order_id,
                price_precision=instrument.price_precision,
                size_precision=instrument.size_precision,
                ts_init=self._clock.timestamp_ns(),
            )
            self._send_order_status_report(report)
            return

        order = self._cache.order(client_order_id)
        if order is None:
            return

        ts_event = self._clock.timestamp_ns()

        if gateio_order.status == "cancelled":
            self.generate_order_canceled(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=ts_event,
            )
        elif order.venue_order_id is None:
            self.generate_order_accepted(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=ts_event,
            )

    def _handle_usertrades_update(self, result: object) -> None:
        try:
            trades = msgspec.convert(result, list[GateIoTrade])
        except Exception as e:
            self._log.warning(f"Failed to convert usertrades update: {e!r}")
            return

        for trade in trades:
            self._process_trade_update(trade)

    def _process_trade_update(self, trade: GateIoTrade) -> None:
        venue_order_id = VenueOrderId(trade.order_id)
        client_order_id = self._resolve_client_order_id(venue_order_id, trade.text)

        instrument_id = InstrumentId(Symbol(trade.currency_pair), self.venue)
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            return

        if not self._is_order_internal(client_order_id):
            report = parse_spot_fill_report(
                account_id=self.account_id,
                instrument_id=instrument_id,
                trade=trade,
                client_order_id=client_order_id,
                price_precision=instrument.price_precision,
                size_precision=instrument.size_precision,
                quote_currency=instrument.quote_currency,
                ts_init=self._clock.timestamp_ns(),
            )
            self._send_fill_report(report)
            return

        order = self._cache.order(client_order_id)
        if order is None:
            return

        commission_currency = trade.fee_currency or instrument.quote_currency.code

        self.generate_order_filled(
            strategy_id=order.strategy_id,
            instrument_id=instrument_id,
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            venue_position_id=None,
            trade_id=TradeId(trade.id),
            order_side=gateio_order_side_to_nautilus(trade.side),
            order_type=order.order_type,
            last_qty=Quantity(Decimal(trade.amount), instrument.size_precision),
            last_px=Price(Decimal(trade.price), instrument.price_precision),
            quote_currency=instrument.quote_currency,
            commission=Money(Decimal(trade.fee), Currency.from_str(commission_currency)),
            liquidity_side=LiquiditySide.MAKER if trade.role == "maker" else LiquiditySide.TAKER,
            ts_event=self._clock.timestamp_ns(),
        )

    def _handle_balances_update(self, result: object) -> None:
        try:
            balances = msgspec.convert(result, list[GateIoWsSpotBalance])
        except Exception as e:
            self._log.warning(f"Failed to convert balances update: {e!r}")
            return

        if not balances:
            return

        self.create_task(self._update_account_state(), log_msg="update_account_state")

    # -- HELPERS ------------------------------------------------------------------------------

    def _is_order_internal(self, client_order_id: ClientOrderId | None) -> bool:
        if client_order_id is None:
            return False
        return self._cache.strategy_id_for_order(client_order_id) is not None

    def _resolve_client_order_id(
        self,
        venue_order_id: VenueOrderId,
        text: str | None,
    ) -> ClientOrderId | None:
        client_order_id = self._cache.client_order_id(venue_order_id)
        if client_order_id is not None:
            return client_order_id
        if text and text.startswith("t-"):
            return ClientOrderId(text[2:])
        return None
