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

from nautilus_trader.adapters.gateio.common.constants import GATEIO_SPOT_WS_BASE_URL
from nautilus_trader.adapters.gateio.config import GateIoDataClientConfig
from nautilus_trader.adapters.gateio.spot.http.client import GateIoSpotHttpClient
from nautilus_trader.adapters.gateio.spot.providers import GateIoSpotInstrumentProvider
from nautilus_trader.adapters.gateio.spot.websocket.client import GateIoSpotWebSocketClient
from nautilus_trader.adapters.gateio.spot.websocket.schemas import GateIoBookTickerResult
from nautilus_trader.adapters.gateio.spot.websocket.schemas import GateIoWsMessage
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.core.datetime import millis_to_nanos
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeQuoteTicks
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class GateIoSpotDataClient(LiveMarketDataClient):
    """
    Provides a data client for the Gate.io spot exchange.

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
    name : str, optional
        The custom client ID.
    config : GateIoDataClientConfig
        The configuration for the client.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        config: GateIoDataClientConfig,
        name: str | None = None,
    ) -> None:
        self._http_client = GateIoSpotHttpClient(base_url=config.base_url_http)
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
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )

        ws_url = config.base_url_ws or GATEIO_SPOT_WS_BASE_URL
        self._ws_client = GateIoSpotWebSocketClient(
            loop=loop,
            url=ws_url,
            handler=self._handle_ws_message,
            handler_reconnect=self._resubscribe,
        )

        self._decoder = msgspec.json.Decoder(GateIoWsMessage)
        self._subscribed_symbols: set[str] = set()

    def _send_all_instruments_to_data_engine(self) -> None:
        for instrument in self._instrument_provider.get_all().values():
            self._handle_data(instrument)
        for currency in self._instrument_provider.currencies().values():
            self._cache.add_currency(currency)

    async def _connect(self) -> None:
        self._log.info("Initializing Gate.io instruments...")
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

        if msg.event == "error":
            self._log.error(f"Gate.io WS error: {msg.error}")
            return

        if msg.event != "update" or msg.channel != "spot.book_ticker":
            return

        if msg.result is None:
            return

        try:
            result = msgspec.convert(msg.result, GateIoBookTickerResult)
        except Exception as e:
            self._log.warning(f"Failed to convert ticker result: {e!r}")
            return

        instrument_id = InstrumentId.from_str(f"{result.s}.GATEIO")
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            self._log.debug(f"No instrument found for {instrument_id}, dropping tick")
            return

        try:
            quote = QuoteTick(
                instrument_id=instrument_id,
                bid_price=Price(float(result.b), precision=instrument.price_precision),
                ask_price=Price(float(result.a), precision=instrument.price_precision),
                bid_size=Quantity(float(result.B), precision=instrument.size_precision),
                ask_size=Quantity(float(result.A), precision=instrument.size_precision),
                ts_event=millis_to_nanos(result.t),
                ts_init=self._clock.timestamp_ns(),
            )
        except Exception as e:
            self._log.warning(f"Failed to build QuoteTick for {result.s}: {e!r}")
            return

        self._handle_data(quote)

    async def _resubscribe(self) -> None:
        """Re-subscribe to all active streams after reconnection."""
        if self._subscribed_symbols:
            self._log.info(f"Resubscribing to {len(self._subscribed_symbols)} symbols...")
            await self._ws_client.subscribe_book_ticker(list(self._subscribed_symbols))
