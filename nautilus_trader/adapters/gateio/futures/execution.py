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

from nautilus_trader.adapters.gateio.common.constants import GATEIO_FUTURES_WS_BASE_URL_TEMPLATE
from nautilus_trader.adapters.gateio.common.credentials import get_api_key
from nautilus_trader.adapters.gateio.common.credentials import get_api_secret
from nautilus_trader.adapters.gateio.config import GateIoExecClientConfig
from nautilus_trader.adapters.gateio.futures.http.client import GateIoFuturesHttpClient
from nautilus_trader.adapters.gateio.futures.http.models import GateIoFuturesAccount
from nautilus_trader.adapters.gateio.futures.http.models import GateIoFuturesOrder
from nautilus_trader.adapters.gateio.futures.http.models import GateIoFuturesPosition
from nautilus_trader.adapters.gateio.futures.http.models import GateIoFuturesTrade
from nautilus_trader.adapters.gateio.futures.parsing import futures_submit_order_params
from nautilus_trader.adapters.gateio.futures.parsing import is_dual_mode
from nautilus_trader.adapters.gateio.futures.parsing import nautilus_order_side_to_gateio_futures_side
from nautilus_trader.adapters.gateio.futures.parsing import parse_futures_fill_report
from nautilus_trader.adapters.gateio.futures.parsing import parse_futures_order_status_report
from nautilus_trader.adapters.gateio.futures.parsing import parse_futures_position_status_report
from nautilus_trader.adapters.gateio.futures.providers import GateIoFuturesInstrumentProvider
from nautilus_trader.adapters.gateio.futures.websocket.client import GateIoFuturesWebSocketClient
from nautilus_trader.adapters.gateio.futures.websocket.schemas import GateIoFuturesWsMessage
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


class GateIoFuturesExecutionClient(LiveExecutionClient):
    """
    Provides an execution client for Gate.io USDT-margined linear perpetual futures.

    Only single (net) position mode is supported: `_connect` rejects startup if the
    account is found to be in dual (hedge) position mode.

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
        self._settle = config.settle

        self._http_client = GateIoFuturesHttpClient(
            api_key=self._api_key,
            api_secret=self._api_secret,
            base_url=config.base_url_http,
            default_quota=Quota.rate_per_second(config.ratelimiter_default_quota_per_second),
        )
        instrument_provider = GateIoFuturesInstrumentProvider(
            http_client=self._http_client,
            clock=clock,
            settle=self._settle,
            venue=config.venue,
            config=config.instrument_provider,
        )

        super().__init__(
            loop=loop,
            client_id=ClientId(name or config.venue.value),
            venue=config.venue,
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

        self._user_id: str | None = None

        ws_url = config.base_url_ws or GATEIO_FUTURES_WS_BASE_URL_TEMPLATE.format(
            settle=self._settle,
        )
        self._ws_client = GateIoFuturesWebSocketClient(
            loop=loop,
            url=ws_url,
            handler=self._handle_ws_message,
            handler_reconnect=self._resubscribe,
            api_key=self._api_key,
            api_secret=self._api_secret,
        )
        self._ws_decoder = msgspec.json.Decoder(GateIoFuturesWsMessage)

    # -- CONNECTION ---------------------------------------------------------------------------

    async def _connect(self) -> None:
        await self._instrument_provider.initialize()

        positions = await self._http_client.list_positions(self._settle)
        if any(is_dual_mode(p) for p in positions):
            raise RuntimeError(
                "Gate.io futures account is in dual (hedge) position mode; only single "
                "(net) position mode is supported by this client. Switch to single mode "
                "via the Gate.io app/website before connecting.",
            )

        account_detail = await self._http_client.get_account_detail()
        if account_detail.user_id is None:
            raise RuntimeError("Gate.io GET /account/detail did not return a user_id")
        self._user_id = str(account_detail.user_id)

        await self._update_account_state()
        await self._await_account_registered()

        await self._ws_client.connect()
        await self._ws_client.subscribe_orders(self._user_id)
        await self._ws_client.subscribe_usertrades(self._user_id)
        await self._ws_client.subscribe_positions(self._user_id)

        if self._api_key:
            self._log.info(f"Gate.io API key {mask_api_key(self._api_key)}", LogColor.BLUE)
        self._log.info("Gate.io futures API key authenticated", LogColor.GREEN)

    async def _disconnect(self) -> None:
        await self._ws_client.disconnect()

    async def _resubscribe(self) -> None:
        if self._user_id is None:
            self._log.error("Cannot resubscribe: user_id not resolved")
            return

        self._log.info("Resubscribing to private channels after reconnect...", LogColor.BLUE)
        await self._ws_client.subscribe_orders(self._user_id)
        await self._ws_client.subscribe_usertrades(self._user_id)
        await self._ws_client.subscribe_positions(self._user_id)

        self.create_task(
            self._reconcile_after_reconnect(),
            log_msg="reconcile_after_reconnect",
        )

    async def _reconcile_after_reconnect(self) -> None:
        try:
            order_command = GenerateOrderStatusReports(
                instrument_id=None,
                start=None,
                end=None,
                open_only=True,
                command_id=UUID4(),
                ts_init=self._clock.timestamp_ns(),
            )
            for report in await self.generate_order_status_reports(order_command):
                self._send_order_status_report(report)

            position_command = GeneratePositionStatusReports(
                instrument_id=None,
                start=None,
                end=None,
                command_id=UUID4(),
                ts_init=self._clock.timestamp_ns(),
            )
            for report in await self.generate_position_status_reports(position_command):
                self._send_position_status_report(report)
        except Exception as e:
            self._log.exception("Failed to reconcile after reconnect", e)

    # -- ACCOUNT --------------------------------------------------------------------------------

    async def _query_account(self, _command: QueryAccount) -> None:
        await self._update_account_state()

    async def _update_account_state(self) -> None:
        account = await self._http_client.list_futures_accounts(self._settle)
        balance = self._parse_account_balance(account)

        self.generate_account_state(
            balances=[balance],
            margins=[],
            reported=True,
            ts_event=self._clock.timestamp_ns(),
        )

    def _parse_account_balance(self, account: GateIoFuturesAccount) -> AccountBalance:
        currency = Currency.from_str(account.currency)
        free = Money(Decimal(account.available), currency)
        used = Decimal(account.position_margin) + Decimal(account.order_margin)
        locked = Money(used, currency)
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
            params = futures_submit_order_params(
                contract=instrument.raw_symbol.value,
                client_order_id=order.client_order_id,
                order_side=order.side,
                order_type=order.order_type,
                quantity=order.quantity,
                time_in_force=order.time_in_force,
                post_only=order.is_post_only,
                price=order.price if order.has_price else None,
                reduce_only=order.is_reduce_only,
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
                settle=self._settle,
                contract=params["contract"],
                size=params["size"],
                price=params["price"],
                tif=params["tif"],
                text=params["text"],
                reduce_only=params["reduce_only"],
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
            venue_order_id=VenueOrderId(str(gateio_order.id)),
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
            self._log.error(f"Order not found: {command.client_order_id}")
            self.generate_order_modify_rejected(
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

        size = None
        if command.quantity is not None:
            size = (
                int(command.quantity)
                if order.side == OrderSide.BUY
                else -int(command.quantity)
            )
        price = str(command.price) if command.price is not None else None

        try:
            updated = await self._http_client.amend_order(
                settle=self._settle,
                order_id=venue_order_id.value,
                size=size,
                price=price,
            )
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
            return

        instrument = self._cache.instrument(order.instrument_id)
        new_price = order.price
        if instrument is not None and updated.price != "0":
            new_price = Price(Decimal(updated.price), instrument.price_precision)

        self.generate_order_updated(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=venue_order_id,
            quantity=Quantity(abs(updated.size), 0),
            price=new_price,
            trigger_price=None,
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
            await self._http_client.cancel_order(
                settle=self._settle,
                order_id=venue_order_id.value,
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
            side = nautilus_order_side_to_gateio_futures_side(command.order_side)

        try:
            await self._http_client.cancel_all_orders(
                settle=self._settle,
                contract=instrument.raw_symbol.value,
                side=side,
            )
        except Exception as e:
            self._log.error(f"Failed to cancel all orders for {command.instrument_id}: {e}")

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        if not command.cancels:
            self._log.debug("batch_cancel_orders called with empty cancels list")
            return

        # Gate.io futures has no native batch-cancel endpoint: cancel individually,
        # bounded by a semaphore so we stay within the configured rate limit.
        semaphore = asyncio.Semaphore(5)

        async def _cancel_one(cancel: CancelOrder) -> None:
            async with semaphore:
                order = self._cache.order(cancel.client_order_id)
                if order is None:
                    self._log.warning(
                        f"Skipping cancel - order not found: {cancel.client_order_id}",
                    )
                    self.generate_order_cancel_rejected(
                        strategy_id=cancel.strategy_id,
                        instrument_id=cancel.instrument_id,
                        client_order_id=cancel.client_order_id,
                        venue_order_id=cancel.venue_order_id,
                        reason=f"Order not found: {cancel.client_order_id}",
                        ts_event=self._clock.timestamp_ns(),
                    )
                    return

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
                        reason=(
                            f"Cannot cancel order without venue_order_id: "
                            f"{cancel.client_order_id}"
                        ),
                        ts_event=self._clock.timestamp_ns(),
                    )
                    return

                try:
                    await self._http_client.cancel_order(
                        settle=self._settle,
                        order_id=venue_order_id.value,
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

        await asyncio.gather(*(_cancel_one(cancel) for cancel in command.cancels))

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
                settle=self._settle,
                order_id=order_id,
            )
        except Exception as e:
            self._log_report_error(e, "OrderStatusReport")
            return None

        return parse_futures_order_status_report(
            account_id=self.account_id,
            instrument_id=command.instrument_id,
            order=gateio_order,
            client_order_id=command.client_order_id,
            price_precision=instrument.price_precision,
            ts_init=self._clock.timestamp_ns(),
        )

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        reports: list[OrderStatusReport] = []
        ts_init = self._clock.timestamp_ns()
        statuses = ["open"] if command.open_only else ["open", "finished"]

        contract = None
        if command.instrument_id is not None:
            instrument = self._cache.instrument(command.instrument_id)
            if instrument is None:
                self._log.warning(f"No instrument found for {command.instrument_id}")
                return []
            contract = instrument.raw_symbol.value

        try:
            for status in statuses:
                gateio_orders = await self._http_client.list_orders(
                    settle=self._settle,
                    status=status,
                    contract=contract,
                )
                for gateio_order in gateio_orders:
                    instrument_id = command.instrument_id or InstrumentId(
                        Symbol(gateio_order.contract),
                        self.venue,
                    )
                    instrument = self._cache.instrument(instrument_id)
                    if instrument is None:
                        continue
                    reports.append(
                        self._parse_order_status_report(instrument_id, instrument, gateio_order, ts_init),
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
        gateio_order: GateIoFuturesOrder,
        ts_init: int,
    ) -> OrderStatusReport:
        client_order_id = self._resolve_client_order_id(
            VenueOrderId(str(gateio_order.id)),
            gateio_order.text,
        )
        return parse_futures_order_status_report(
            account_id=self.account_id,
            instrument_id=instrument_id,
            order=gateio_order,
            client_order_id=client_order_id,
            price_precision=instrument.price_precision,
            ts_init=ts_init,
        )

    async def generate_fill_reports(
        self,
        command: GenerateFillReports,
    ) -> list[FillReport]:
        reports: list[FillReport] = []
        ts_init = self._clock.timestamp_ns()

        contract = None
        if command.instrument_id is not None:
            instrument = self._cache.instrument(command.instrument_id)
            if instrument is None:
                self._log.warning(f"No instrument found for {command.instrument_id}")
                return []
            contract = instrument.raw_symbol.value

        order_id = command.venue_order_id.value if command.venue_order_id is not None else None

        try:
            trades = await self._http_client.list_my_trades(
                settle=self._settle,
                contract=contract,
                order_id=order_id,
            )
        except Exception as e:
            self._log_report_error(e, "FillReport")
            return []

        for trade in trades:
            instrument_id = command.instrument_id or InstrumentId(
                Symbol(trade.contract),
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
                parse_futures_fill_report(
                    account_id=self.account_id,
                    instrument_id=instrument_id,
                    trade=trade,
                    client_order_id=client_order_id,
                    price_precision=instrument.price_precision,
                    settle_currency=instrument.quote_currency,
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
            positions = await self._http_client.list_positions(self._settle)
        except Exception as e:
            self._log_report_error(e, "PositionStatusReport")
            return []

        for position in positions:
            if position.size == 0:
                continue
            instrument_id = InstrumentId(Symbol(position.contract), self.venue)
            if command.instrument_id is not None and instrument_id != command.instrument_id:
                continue
            reports.append(
                parse_futures_position_status_report(
                    account_id=self.account_id,
                    instrument_id=instrument_id,
                    position=position,
                    ts_init=ts_init,
                ),
            )

        self._log_report_receipt(len(reports), "PositionStatusReport", command.log_receipt_level)
        return reports

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

        if msg.channel == "futures.orders":
            self._handle_orders_update(msg.result)
        elif msg.channel == "futures.usertrades":
            self._handle_usertrades_update(msg.result)
        elif msg.channel == "futures.positions":
            self._handle_positions_update(msg.result)

    def _handle_orders_update(self, result: object) -> None:
        try:
            gateio_orders = msgspec.convert(result, list[GateIoFuturesOrder])
        except Exception as e:
            self._log.warning(f"Failed to convert orders update: {e!r}")
            return

        for gateio_order in gateio_orders:
            self._process_order_update(gateio_order)

    def _process_order_update(self, gateio_order: GateIoFuturesOrder) -> None:
        venue_order_id = VenueOrderId(str(gateio_order.id))
        client_order_id = self._resolve_client_order_id(venue_order_id, gateio_order.text)

        instrument_id = InstrumentId(Symbol(gateio_order.contract), self.venue)
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            return

        if not self._is_order_internal(client_order_id):
            report = parse_futures_order_status_report(
                account_id=self.account_id,
                instrument_id=instrument_id,
                order=gateio_order,
                client_order_id=client_order_id,
                price_precision=instrument.price_precision,
                ts_init=self._clock.timestamp_ns(),
            )
            self._send_order_status_report(report)
            return

        order = self._cache.order(client_order_id)
        if order is None:
            return

        ts_event = self._clock.timestamp_ns()

        if gateio_order.status == "finished" and gateio_order.finish_as == "cancelled":
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
            trades = msgspec.convert(result, list[GateIoFuturesTrade])
        except Exception as e:
            self._log.warning(f"Failed to convert usertrades update: {e!r}")
            return

        for trade in trades:
            self._process_trade_update(trade)

        if trades:
            self.create_task(self._update_account_state(), log_msg="update_account_state")

    def _process_trade_update(self, trade: GateIoFuturesTrade) -> None:
        venue_order_id = VenueOrderId(trade.order_id)
        client_order_id = self._resolve_client_order_id(venue_order_id, trade.text)

        instrument_id = InstrumentId(Symbol(trade.contract), self.venue)
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            return

        if not self._is_order_internal(client_order_id):
            report = parse_futures_fill_report(
                account_id=self.account_id,
                instrument_id=instrument_id,
                trade=trade,
                client_order_id=client_order_id,
                price_precision=instrument.price_precision,
                settle_currency=instrument.quote_currency,
                ts_init=self._clock.timestamp_ns(),
            )
            self._send_fill_report(report)
            return

        order = self._cache.order(client_order_id)
        if order is None:
            return

        order_side = OrderSide.BUY if trade.size >= 0 else OrderSide.SELL

        self.generate_order_filled(
            strategy_id=order.strategy_id,
            instrument_id=instrument_id,
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            venue_position_id=None,
            trade_id=TradeId(trade.id),
            order_side=order_side,
            order_type=order.order_type,
            last_qty=Quantity(abs(trade.size), 0),
            last_px=Price(Decimal(trade.price), instrument.price_precision),
            quote_currency=instrument.quote_currency,
            commission=Money(Decimal(0), instrument.quote_currency),
            liquidity_side=LiquiditySide.MAKER if trade.role == "maker" else LiquiditySide.TAKER,
            ts_event=self._clock.timestamp_ns(),
        )

    def _handle_positions_update(self, result: object) -> None:
        try:
            positions = msgspec.convert(result, list[GateIoFuturesPosition])
        except Exception as e:
            self._log.warning(f"Failed to convert positions update: {e!r}")
            return

        ts_init = self._clock.timestamp_ns()
        for position in positions:
            if is_dual_mode(position):
                self._log.error(
                    f"Received dual-mode position push for {position.contract}; "
                    "only single position mode is supported",
                )
                continue
            if position.size == 0:
                continue
            instrument_id = InstrumentId(Symbol(position.contract), self.venue)
            report = parse_futures_position_status_report(
                account_id=self.account_id,
                instrument_id=instrument_id,
                position=position,
                ts_init=ts_init,
            )
            self._send_position_status_report(report)

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
