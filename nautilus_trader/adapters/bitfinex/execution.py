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
import itertools

import msgspec

from nautilus_trader.adapters.bitfinex.common.credentials import get_api_key
from nautilus_trader.adapters.bitfinex.common.credentials import get_api_secret
from nautilus_trader.adapters.bitfinex.config import BitfinexExecClientConfig
from nautilus_trader.adapters.bitfinex.constants import BITFINEX_VENUE
from nautilus_trader.adapters.bitfinex.constants import BITFINEX_WS_AUTH_BASE_URL
from nautilus_trader.adapters.bitfinex.http.client import BitfinexHttpClient
from nautilus_trader.adapters.bitfinex.http.models import BitfinexOrder
from nautilus_trader.adapters.bitfinex.http.models import BitfinexTrade
from nautilus_trader.adapters.bitfinex.parsing import bitfinex_order_side_from_amount
from nautilus_trader.adapters.bitfinex.parsing import bitfinex_order_status_to_nautilus
from nautilus_trader.adapters.bitfinex.parsing import bitfinex_submit_order_params
from nautilus_trader.adapters.bitfinex.parsing import bitfinex_symbol_to_instrument_id
from nautilus_trader.adapters.bitfinex.parsing import instrument_id_to_bitfinex_symbol
from nautilus_trader.adapters.bitfinex.parsing import parse_account_balances
from nautilus_trader.adapters.bitfinex.parsing import parse_fill_report
from nautilus_trader.adapters.bitfinex.parsing import parse_order_status_report
from nautilus_trader.adapters.bitfinex.parsing import parse_position_status_report
from nautilus_trader.adapters.bitfinex.providers import BitfinexInstrumentProvider
from nautilus_trader.adapters.bitfinex.websocket.private_client import BitfinexPrivateWebSocketClient
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
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Decimal
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import Order


class BitfinexExecutionClient(LiveExecutionClient):
    """
    Provides an execution client for the Bitfinex exchange (spot and USDT-margined
    perpetual futures, sharing a single ``BITFINEX`` venue and margin account).

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
    config : BitfinexExecClientConfig
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
        config: BitfinexExecClientConfig,
        name: str | None = None,
    ) -> None:
        self._api_key = config.api_key or get_api_key()
        self._api_secret = config.api_secret or get_api_secret()

        self._http_client = BitfinexHttpClient(
            api_key=self._api_key,
            api_secret=self._api_secret,
            base_url_auth=config.base_url_http,
            default_quota=Quota.rate_per_second(config.ratelimiter_default_quota_per_second),
        )
        instrument_provider = BitfinexInstrumentProvider(
            http_client=self._http_client,
            clock=clock,
            config=config.instrument_provider,
            instrument_types=config.instrument_types,
        )

        super().__init__(
            loop=loop,
            client_id=ClientId(name or BITFINEX_VENUE.value),
            venue=BITFINEX_VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            base_currency=None,
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )

        account_id = AccountId(f"{self.id.value}-master")
        self._set_account_id(account_id)

        ws_url = config.base_url_ws or BITFINEX_WS_AUTH_BASE_URL
        self._ws_client = BitfinexPrivateWebSocketClient(
            loop=loop,
            url=ws_url,
            api_key=self._api_key,
            api_secret=self._api_secret,
            handler=self._handle_ws_message,
            handler_reconnect=self._resubscribe,
        )

        # Bitfinex requires an integer `cid` per order; transient mapping back to the
        # Nautilus `ClientOrderId` until the venue order id is known to the cache.
        self._cid_to_client_order_id: dict[int, ClientOrderId] = {}
        self._cid_counter = itertools.count(clock.timestamp_ns() // 1_000_000)

    # -- CONNECTION ---------------------------------------------------------------------------

    async def _connect(self) -> None:
        await self._instrument_provider.initialize()

        await self._update_account_state()
        await self._await_account_registered()

        await self._ws_client.connect()

        if self._api_key:
            self._log.info(f"Bitfinex API key {mask_api_key(self._api_key)}", LogColor.BLUE)
        self._log.info("Bitfinex private API authenticated", LogColor.GREEN)

    async def _disconnect(self) -> None:
        await self._ws_client.disconnect()

    async def _resubscribe(self) -> None:
        self._log.info("Reconciling private state after reconnect...", LogColor.BLUE)
        try:
            await self._update_account_state()
        except Exception as e:
            self._log.exception("Failed to refresh account state after reconnect", e)

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
        wallets = await self._http_client.list_wallets()
        balances = parse_account_balances(wallets)

        self.generate_account_state(
            balances=balances,
            margins=[],
            reported=True,
            ts_event=self._clock.timestamp_ns(),
        )

    # -- ORDER SUBMISSION -------------------------------------------------------------------------

    def _next_cid(self) -> int:
        return next(self._cid_counter)

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

        is_perpetual = isinstance(instrument, CryptoPerpetual)
        cid = self._next_cid()

        try:
            params = bitfinex_submit_order_params(
                raw_symbol=instrument.raw_symbol.value,
                cid=cid,
                order_side=order.side,
                order_type=order.order_type,
                quantity=order.quantity,
                time_in_force=order.time_in_force,
                post_only=order.is_post_only,
                price=order.price if order.has_price else None,
                is_perpetual=is_perpetual,
            )
        except ValueError as e:
            self._deny_order(order, str(e))
            return

        self._cid_to_client_order_id[cid] = order.client_order_id

        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

        try:
            bitfinex_order = await self._http_client.submit_order(**params)
        except Exception as e:
            self._cid_to_client_order_id.pop(cid, None)
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
            venue_order_id=VenueOrderId(str(bitfinex_order.id)),
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
        if order is None:
            reason = f"Order not found: {command.client_order_id}"
            self._log.error(reason)
            self.generate_order_modify_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=command.venue_order_id,
                reason=reason,
                ts_event=self._clock.timestamp_ns(),
            )
            return

        venue_order_id = command.venue_order_id or order.venue_order_id
        if venue_order_id is None:
            reason = f"Cannot modify order without venue_order_id: {command.client_order_id}"
            self._log.error(reason)
            self.generate_order_modify_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=None,
                reason=reason,
                ts_event=self._clock.timestamp_ns(),
            )
            return

        params: dict[str, object] = {"id": int(venue_order_id.value)}
        if command.price is not None:
            params["price"] = str(command.price)
        if command.quantity is not None:
            signed_amount = (
                str(command.quantity) if order.side == OrderSide.BUY else f"-{command.quantity}"
            )
            params["amount"] = signed_amount

        try:
            # Real amend via POST /v2/auth/w/order/update; success is confirmed by the
            # subsequent WS `ou` push rather than generated here.
            await self._http_client.update_order(**params)
        except Exception as e:
            self._log.error(f"Failed to modify order {order.client_order_id}: {e}")
            self.generate_order_modify_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=venue_order_id,
                reason=str(e),
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
            reason = f"Cannot cancel order without venue_order_id: {command.client_order_id}"
            self._log.error(reason)
            self.generate_order_cancel_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=None,
                reason=reason,
                ts_event=self._clock.timestamp_ns(),
            )
            return

        try:
            await self._http_client.cancel_order(id=int(venue_order_id.value))
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

        if command.order_side != OrderSide.NO_ORDER_SIDE:
            self._log.warning(
                "Bitfinex cancel-all has no side filter; cancelling all orders for "
                f"{command.instrument_id} regardless of side",
            )

        raw_symbol = instrument_id_to_bitfinex_symbol(instrument.raw_symbol.value)

        try:
            await self._http_client.cancel_all_orders(symbol=raw_symbol)
        except Exception as e:
            self._log.error(f"Failed to cancel all orders for {command.instrument_id}: {e}")

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        if not command.cancels:
            self._log.debug("batch_cancel_orders called with empty cancels list")
            return

        order_lookup: dict[int, Order] = {}
        order_ids: list[int] = []

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

            order_id = int(venue_order_id.value)
            order_lookup[order_id] = order
            order_ids.append(order_id)

        if not order_ids:
            return

        try:
            await self._http_client.batch_cancel_orders(order_ids)
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

        venue_order_id = command.venue_order_id
        if venue_order_id is None and command.client_order_id is not None:
            cached_order = self._cache.order(command.client_order_id)
            if cached_order is not None and cached_order.venue_order_id is not None:
                venue_order_id = cached_order.venue_order_id

        if venue_order_id is None:
            self._log.warning(
                f"Cannot query order status: no venue_order_id resolvable for "
                f"{command.client_order_id}",
            )
            return None

        raw_symbol = instrument_id_to_bitfinex_symbol(instrument.raw_symbol.value)

        try:
            bitfinex_order = await self._find_order(raw_symbol, venue_order_id)
        except Exception as e:
            self._log_report_error(e, "OrderStatusReport")
            return None

        if bitfinex_order is None:
            self._log.warning(f"Order {venue_order_id} not found on Bitfinex")
            return None

        return parse_order_status_report(
            account_id=self.account_id,
            instrument_id=command.instrument_id,
            order=bitfinex_order,
            client_order_id=command.client_order_id,
            price_precision=instrument.price_precision,
            size_precision=instrument.size_precision,
            ts_init=self._clock.timestamp_ns(),
        )

    async def _find_order(
        self,
        raw_symbol: str,
        venue_order_id: VenueOrderId,
    ) -> BitfinexOrder | None:
        active_orders = await self._http_client.list_active_orders(symbol=raw_symbol)
        for bitfinex_order in active_orders:
            if str(bitfinex_order.id) == venue_order_id.value:
                return bitfinex_order

        hist_orders = await self._http_client.list_orders_history(symbol=raw_symbol)
        for bitfinex_order in hist_orders:
            if str(bitfinex_order.id) == venue_order_id.value:
                return bitfinex_order

        return None

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

                raw_symbol = instrument_id_to_bitfinex_symbol(instrument.raw_symbol.value)
                bitfinex_orders = list(
                    await self._http_client.list_active_orders(symbol=raw_symbol),
                )
                if not command.open_only:
                    bitfinex_orders += await self._http_client.list_orders_history(
                        symbol=raw_symbol,
                    )
                for bitfinex_order in bitfinex_orders:
                    reports.append(
                        self._parse_order_status_report(
                            command.instrument_id,
                            instrument,
                            bitfinex_order,
                            ts_init,
                        ),
                    )
            else:
                bitfinex_orders = list(await self._http_client.list_active_orders())
                if not command.open_only:
                    bitfinex_orders += await self._http_client.list_orders_history()
                for bitfinex_order in bitfinex_orders:
                    instrument_id = bitfinex_symbol_to_instrument_id(bitfinex_order.symbol)
                    instrument = self._cache.instrument(instrument_id)
                    if instrument is None:
                        continue
                    reports.append(
                        self._parse_order_status_report(
                            instrument_id,
                            instrument,
                            bitfinex_order,
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
        instrument_id,
        instrument: Instrument,
        bitfinex_order: BitfinexOrder,
        ts_init: int,
    ) -> OrderStatusReport:
        client_order_id = self._resolve_client_order_id(
            VenueOrderId(str(bitfinex_order.id)),
            bitfinex_order.cid,
        )
        return parse_order_status_report(
            account_id=self.account_id,
            instrument_id=instrument_id,
            order=bitfinex_order,
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

        raw_symbol = None
        if command.instrument_id is not None:
            instrument = self._cache.instrument(command.instrument_id)
            if instrument is None:
                self._log.warning(f"No instrument found for {command.instrument_id}")
                return []
            raw_symbol = instrument_id_to_bitfinex_symbol(instrument.raw_symbol.value)

        try:
            trades = await self._http_client.list_trades_history(symbol=raw_symbol)
        except Exception as e:
            self._log_report_error(e, "FillReport")
            return []

        for trade in trades:
            if (
                command.venue_order_id is not None
                and str(trade.order_id) != command.venue_order_id.value
            ):
                continue

            instrument_id = command.instrument_id or bitfinex_symbol_to_instrument_id(
                trade.symbol,
            )
            instrument = self._cache.instrument(instrument_id)
            if instrument is None:
                continue

            client_order_id = self._resolve_client_order_id(
                VenueOrderId(str(trade.order_id)),
                trade.cid,
            )
            reports.append(
                parse_fill_report(
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
        reports: list[PositionStatusReport] = []
        ts_init = self._clock.timestamp_ns()

        try:
            positions = await self._http_client.list_positions()
        except Exception as e:
            self._log_report_error(e, "PositionStatusReport")
            return []

        for position in positions:
            if position.amount == 0:
                continue

            instrument_id = bitfinex_symbol_to_instrument_id(position.symbol)
            if command.instrument_id is not None and instrument_id != command.instrument_id:
                continue

            instrument = self._cache.instrument(instrument_id)
            if instrument is None or not isinstance(instrument, CryptoPerpetual):
                # Bitfinex spot has no position concept; only perpetuals are reported
                continue

            reports.append(
                parse_position_status_report(
                    account_id=self.account_id,
                    instrument_id=instrument_id,
                    position=position,
                    size_precision=instrument.size_precision,
                    ts_init=ts_init,
                ),
            )

        self._log_report_receipt(
            len(reports),
            "PositionStatusReport",
            command.log_receipt_level,
        )
        return reports

    # -- WEBSOCKET MESSAGE HANDLING ---------------------------------------------------------------

    def _handle_ws_message(self, raw: bytes) -> None:
        try:
            msg = msgspec.json.decode(raw)
        except Exception as e:
            self._log.warning(f"Failed to decode WS message: {e!r} | {raw!r}")
            return

        if isinstance(msg, dict):
            event = msg.get("event")
            if event == "auth":
                if msg.get("status") == "OK":
                    self._log.info("Bitfinex private WS authenticated", LogColor.GREEN)
                else:
                    self._log.error(f"Bitfinex private WS auth failed: {msg}")
            elif event == "error":
                self._log.error(f"Bitfinex private WS error: {msg}")
            return

        if not isinstance(msg, list) or len(msg) < 3:
            return

        msg_type, payload = msg[1], msg[2]

        if msg_type in ("os", "on", "ou", "oc"):
            self._handle_orders_update(msg_type, payload)
        elif msg_type == "tu":
            # Only `tu` (trade update) carries fee information; `te` (trade executed) is
            # ignored to avoid generating the same fill twice.
            self._handle_trade_update(payload)
        elif msg_type in ("ws", "wu"):
            self._handle_wallet_update()
        elif msg_type in ("ps", "pn", "pu", "pc"):
            self._log.debug(f"Position update ({msg_type}): {payload}")

    def _handle_orders_update(self, msg_type: str, payload: object) -> None:
        try:
            if msg_type == "os":
                orders = msgspec.convert(payload, list[BitfinexOrder])
            else:
                orders = [msgspec.convert(payload, type=BitfinexOrder)]
        except Exception as e:
            self._log.warning(f"Failed to convert order update: {e!r}")
            return

        for bitfinex_order in orders:
            self._process_order_update(bitfinex_order)

    def _process_order_update(self, bitfinex_order: BitfinexOrder) -> None:
        venue_order_id = VenueOrderId(str(bitfinex_order.id))
        client_order_id = self._resolve_client_order_id(venue_order_id, bitfinex_order.cid)

        if not self._is_order_internal(client_order_id):
            instrument_id = bitfinex_symbol_to_instrument_id(bitfinex_order.symbol)
            instrument = self._cache.instrument(instrument_id)
            if instrument is None:
                return
            report = parse_order_status_report(
                account_id=self.account_id,
                instrument_id=instrument_id,
                order=bitfinex_order,
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

        try:
            order_status = bitfinex_order_status_to_nautilus(bitfinex_order.status)
        except ValueError:
            order_status = None

        if order_status == OrderStatus.CANCELED:
            self.generate_order_canceled(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=ts_event,
            )
            self._cid_to_client_order_id.pop(bitfinex_order.cid, None)
        elif order.venue_order_id is None:
            self.generate_order_accepted(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=ts_event,
            )

    def _handle_trade_update(self, payload: object) -> None:
        try:
            trade = msgspec.convert(payload, type=BitfinexTrade)
        except Exception as e:
            self._log.warning(f"Failed to convert trade update: {e!r}")
            return

        self._process_trade_update(trade)

    def _process_trade_update(self, trade: BitfinexTrade) -> None:
        venue_order_id = VenueOrderId(str(trade.order_id))
        client_order_id = self._resolve_client_order_id(venue_order_id, trade.cid)

        instrument_id = bitfinex_symbol_to_instrument_id(trade.symbol)
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            return

        if not self._is_order_internal(client_order_id):
            report = parse_fill_report(
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
        fee = abs(trade.fee) if trade.fee is not None else 0.0

        self.generate_order_filled(
            strategy_id=order.strategy_id,
            instrument_id=instrument_id,
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            venue_position_id=None,
            trade_id=TradeId(str(trade.id)),
            order_side=bitfinex_order_side_from_amount(trade.exec_amount),
            order_type=order.order_type,
            last_qty=Quantity(abs(trade.exec_amount), instrument.size_precision),
            last_px=Price(trade.exec_price, instrument.price_precision),
            quote_currency=instrument.quote_currency,
            commission=Money(Decimal(str(fee)), Currency.from_str(commission_currency)),
            liquidity_side=LiquiditySide.MAKER if trade.maker == 1 else LiquiditySide.TAKER,
            ts_event=self._clock.timestamp_ns(),
        )

    def _handle_wallet_update(self) -> None:
        self.create_task(self._update_account_state(), log_msg="update_account_state")

    # -- HELPERS ------------------------------------------------------------------------------

    def _is_order_internal(self, client_order_id: ClientOrderId | None) -> bool:
        if client_order_id is None:
            return False
        return self._cache.strategy_id_for_order(client_order_id) is not None

    def _resolve_client_order_id(
        self,
        venue_order_id: VenueOrderId,
        cid: int | None,
    ) -> ClientOrderId | None:
        client_order_id = self._cache.client_order_id(venue_order_id)
        if client_order_id is not None:
            return client_order_id
        if cid is not None:
            return self._cid_to_client_order_id.get(cid)
        return None
