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

import msgspec

from nautilus_trader.adapters.mexc.config import MexcDataClientConfig
from nautilus_trader.adapters.mexc.constants import MEXC_CLIENT_ID
from nautilus_trader.adapters.mexc.constants import MEXC_VENUE
from nautilus_trader.adapters.mexc.http.client import MexcHttpClient
from nautilus_trader.adapters.mexc.providers import MexcInstrumentProvider
from nautilus_trader.adapters.mexc.websocket.client import MexcWebSocketClient
from nautilus_trader.adapters.mexc.websocket.schemas import MexcWsMessage
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.core.datetime import millis_to_nanos
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeQuoteTicks
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class MexcDataClient(LiveMarketDataClient):
    """
    Provides a data client for the MEXC spot exchange.

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
    config : MexcDataClientConfig
        The configuration for the client.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        config: MexcDataClientConfig,
    ) -> None:
        self._http_client = MexcHttpClient(base_url=config.base_url_http)
        instrument_provider = MexcInstrumentProvider(
            http_client=self._http_client,
            clock=clock,
            config=config.instrument_provider,
        )

        super().__init__(
            loop=loop,
            client_id=MEXC_CLIENT_ID,
            venue=MEXC_VENUE,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )

        ws_url = config.base_url_ws or "wss://wbs.mexc.com/ws"
        self._ws_client = MexcWebSocketClient(
            loop=loop,
            url=ws_url,
            handler=self._handle_ws_message,
            handler_reconnect=self._resubscribe,
        )

        self._decoder = msgspec.json.Decoder(MexcWsMessage)
        self._subscribed_symbols: set[str] = set()

    def _send_all_instruments_to_data_engine(self) -> None:
        for instrument in self._instrument_provider.get_all().values():
            self._handle_data(instrument)
        for currency in self._instrument_provider.currencies().values():
            self._cache.add_currency(currency)

    async def _connect(self) -> None:
        self._log.info("Initializing MEXC instruments...")
        await self._instrument_provider.initialize()
        self._send_all_instruments_to_data_engine()
        await self._ws_client.connect()

    async def _disconnect(self) -> None:
        await self._ws_client.disconnect()

    # -- Subscriptions ---------------------------------------------------------------------------

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        symbol = command.instrument_id.symbol.value
        if symbol not in self._subscribed_symbols:
            self._subscribed_symbols.add(symbol)
            await self._ws_client.subscribe_book_ticker([symbol])

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        symbol = command.instrument_id.symbol.value
        if symbol in self._subscribed_symbols:
            self._subscribed_symbols.discard(symbol)
            await self._ws_client.unsubscribe_book_ticker([symbol])

    # -- WebSocket message handler ---------------------------------------------------------------

    def _handle_ws_message(self, raw: bytes) -> None:
        try:
            msg = self._decoder.decode(raw)
        except Exception as e:
            self._log.warning(f"Failed to decode WS message: {e!r} | {raw!r}")
            return

        # 服务端主动发 PING，必须回 PONG，否则服务器断连
        if msg.method and msg.method.upper() == "PING":
            self._log.debug(f"Server PING received, sending PONG | raw={raw!r}")
            self._loop.create_task(self._ws_client._send({"method": "PONG"}))
            return

        # Control messages (subscription confirmations, PONG, etc.) have no `d` field
        if msg.d is None:
            self._log.debug(f"Control message received | raw={raw!r}")
            return

        data = msg.d
        # Prefer top-level symbol field; fall back to data payload symbol
        raw_symbol = msg.s or data.s
        ts_ms = msg.t

        if not raw_symbol:
            self._log.debug("WS message missing symbol, dropping")
            return

        instrument_id = InstrumentId.from_str(f"{raw_symbol}.MEXC")
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            self._log.debug(f"No instrument found for {instrument_id}, dropping tick")
            return

        ts_event = millis_to_nanos(ts_ms) if ts_ms is not None else self._clock.timestamp_ns()

        try:
            quote = QuoteTick(
                instrument_id=instrument_id,
                bid_price=Price(float(data.b), precision=instrument.price_precision),
                ask_price=Price(float(data.a), precision=instrument.price_precision),
                bid_size=Quantity(float(data.B), precision=instrument.size_precision),
                ask_size=Quantity(float(data.A), precision=instrument.size_precision),
                ts_event=ts_event,
                ts_init=self._clock.timestamp_ns(),
            )
        except Exception as e:
            self._log.warning(f"Failed to build QuoteTick for {raw_symbol}: {e!r}")
            return

        self._handle_data(quote)

    async def _resubscribe(self) -> None:
        """Re-subscribe to all active streams after reconnection."""
        if self._subscribed_symbols:
            self._log.info(f"Resubscribing to {len(self._subscribed_symbols)} symbols...")
            await self._ws_client.subscribe_book_ticker(list(self._subscribed_symbols))
