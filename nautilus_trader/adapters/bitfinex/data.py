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

from nautilus_trader.adapters.bitfinex.config import BitfinexDataClientConfig
from nautilus_trader.adapters.bitfinex.constants import BITFINEX_CLIENT_ID
from nautilus_trader.adapters.bitfinex.constants import BITFINEX_VENUE
from nautilus_trader.adapters.bitfinex.constants import BITFINEX_WS_BASE_URL
from nautilus_trader.adapters.bitfinex.http.client import BitfinexHttpClient
from nautilus_trader.adapters.bitfinex.providers import BitfinexInstrumentProvider
from nautilus_trader.adapters.bitfinex.providers import bitfinex_pair_to_nautilus
from nautilus_trader.adapters.bitfinex.providers import nautilus_to_bitfinex_pair
from nautilus_trader.adapters.bitfinex.websocket.client import BitfinexWebSocketClient
from nautilus_trader.adapters.bitfinex.websocket.schemas import BitfinexEventMessage
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeQuoteTicks
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class BitfinexDataClient(LiveMarketDataClient):
    """
    Provides a data client for the Bitfinex spot exchange.

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
    config : BitfinexDataClientConfig
        The configuration for the client.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        config: BitfinexDataClientConfig,
    ) -> None:
        self._http_client = BitfinexHttpClient(base_url=config.base_url_http)
        instrument_provider = BitfinexInstrumentProvider(
            http_client=self._http_client,
            clock=clock,
            config=config.instrument_provider,
            instrument_types=config.instrument_types,
        )

        super().__init__(
            loop=loop,
            client_id=BITFINEX_CLIENT_ID,
            venue=BITFINEX_VENUE,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )

        ws_url = config.base_url_ws or BITFINEX_WS_BASE_URL
        self._ws_client = BitfinexWebSocketClient(
            loop=loop,
            url=ws_url,
            handler=self._handle_ws_message,
            handler_reconnect=self._resubscribe,
        )

        # chanId → pair (without t-prefix, e.g. "BTCUSD")
        self._channel_map: dict[int, str] = {}
        # pair → chanId (for unsubscribe)
        self._symbol_chan_map: dict[str, int] = {}

        self._decoder_event = msgspec.json.Decoder(BitfinexEventMessage)
        self._subscribed_symbols: set[str] = set()

    # -- InstrumentProvider helper ---------------------------------------------------------------

    def _send_all_instruments_to_data_engine(self) -> None:
        for instrument in self._instrument_provider.get_all().values():
            self._handle_data(instrument)
        for currency in self._instrument_provider.currencies().values():
            self._cache.add_currency(currency)

    # -- Lifecycle -----------------------------------------------------------------------------------

    async def _connect(self) -> None:
        self._log.info("Initializing Bitfinex instruments...")
        await self._instrument_provider.initialize()
        self._send_all_instruments_to_data_engine()
        await self._ws_client.connect()

    async def _disconnect(self) -> None:
        await self._ws_client.disconnect()

    # -- Subscriptions ---------------------------------------------------------------------------

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        symbol = command.instrument_id.symbol.value  # Nautilus symbol e.g. "BTCUSDT"
        if symbol not in self._subscribed_symbols:
            self._subscribed_symbols.add(symbol)
            # Convert back to Bitfinex native before sending WS subscribe
            bitfinex_native = nautilus_to_bitfinex_pair(symbol)  # e.g. "BTCUST"
            await self._ws_client.subscribe_ticker(bitfinex_native)

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        symbol = command.instrument_id.symbol.value  # Nautilus symbol
        if symbol in self._symbol_chan_map:
            chan_id = self._symbol_chan_map.pop(symbol)
            self._channel_map.pop(chan_id, None)
            await self._ws_client.unsubscribe_ticker(chan_id)
        self._subscribed_symbols.discard(symbol)

    # -- WebSocket message handlers --------------------------------------------------------------

    def _handle_ws_message(self, raw: bytes) -> None:
        """Dispatch incoming raw WebSocket bytes to the appropriate handler."""
        if not raw:
            return
        if raw[0:1] == b"{":
            # JSON object — control message (info / subscribed / error / unsubscribed)
            self._handle_event_message(raw)
        elif raw[0:1] == b"[":
            # JSON array — ticker data or heartbeat
            self._handle_data_message(raw)

    def _handle_event_message(self, raw: bytes) -> None:
        try:
            msg = self._decoder_event.decode(raw)
        except Exception as e:
            self._log.warning(f"Failed to decode event message: {e!r} | {raw!r}")
            return

        if msg.event == "info":
            # Server sends an info frame on connect; nothing to do
            return

        if msg.event == "subscribed":
            if msg.chanId is not None and msg.pair:
                # Store both maps keyed by Nautilus symbol for consistent lookup
                nautilus_sym = bitfinex_pair_to_nautilus(msg.pair)
                self._channel_map[msg.chanId] = msg.pair          # chanId → Bitfinex native
                self._symbol_chan_map[nautilus_sym] = msg.chanId  # Nautilus symbol → chanId
                self._log.debug(
                    f"Channel registered: chanId={msg.chanId} pair={msg.pair} → {nautilus_sym}"
                )

        elif msg.event == "unsubscribed":
            if msg.chanId is not None:
                raw_pair = self._channel_map.pop(msg.chanId, None)
                if raw_pair is not None:
                    self._symbol_chan_map.pop(bitfinex_pair_to_nautilus(raw_pair), None)
                self._log.debug(f"Channel removed: chanId={msg.chanId}")

        elif msg.event == "error":
            self._log.error(f"Bitfinex WS error code={msg.code}: {msg.msg}")

    def _handle_data_message(self, raw: bytes) -> None:
        try:
            data = msgspec.json.decode(raw)  # list
        except Exception:
            return

        if not isinstance(data, list) or len(data) < 2:
            return

        chan_id = data[0]
        payload = data[1]

        # Heartbeat: [chanId, "hb"]
        if payload == "hb":
            return

        # Ticker data must be a list with at least 4 elements: [bid, bid_sz, ask, ask_sz, ...]
        if not isinstance(payload, list) or len(payload) < 4:
            return

        raw_pair = self._channel_map.get(chan_id)
        if raw_pair is None:
            return

        # Map Bitfinex native codes (UST→USDT, UDC→USDC) to Nautilus symbol
        nautilus_symbol = bitfinex_pair_to_nautilus(raw_pair)
        instrument_id = InstrumentId.from_str(f"{nautilus_symbol}.BITFINEX")
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            self._log.debug(f"No instrument found for {instrument_id}, dropping tick")
            return

        bid = payload[0]
        bid_size = payload[1]
        ask = payload[2]
        ask_size = payload[3]

        try:
            quote = QuoteTick(
                instrument_id=instrument_id,
                bid_price=Price(float(bid), precision=instrument.price_precision),
                ask_price=Price(float(ask), precision=instrument.price_precision),
                bid_size=Quantity(float(bid_size), precision=instrument.size_precision),
                ask_size=Quantity(float(ask_size), precision=instrument.size_precision),
                ts_event=self._clock.timestamp_ns(),  # Bitfinex ticker frames have no timestamp
                ts_init=self._clock.timestamp_ns(),
            )
        except Exception as e:
            self._log.warning(f"Failed to build QuoteTick for {nautilus_symbol}: {e!r}")
            return

        self._handle_data(quote)

    async def _resubscribe(self) -> None:
        """Re-subscribe to all active ticker channels after WebSocket reconnection."""
        self._channel_map.clear()
        self._symbol_chan_map.clear()
        if self._subscribed_symbols:
            self._log.info(
                f"Resubscribing to {len(self._subscribed_symbols)} symbols..."
            )
            for symbol in self._subscribed_symbols:
                await self._ws_client.subscribe_ticker(symbol)
