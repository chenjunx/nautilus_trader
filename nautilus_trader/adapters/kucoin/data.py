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

from nautilus_trader.adapters.kucoin.config import KuCoinDataClientConfig
from nautilus_trader.adapters.kucoin.constants import KUCOIN_CLIENT_ID
from nautilus_trader.adapters.kucoin.constants import KUCOIN_VENUE
from nautilus_trader.adapters.kucoin.http.client import KuCoinHttpClient
from nautilus_trader.adapters.kucoin.providers import KuCoinInstrumentProvider
from nautilus_trader.adapters.kucoin.websocket.client import KuCoinWebSocketClient
from nautilus_trader.adapters.kucoin.websocket.schemas import KuCoinWsMessage
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


class KuCoinDataClient(LiveMarketDataClient):
    """
    Provides a data client for the KuCoin spot exchange.

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
    config : KuCoinDataClientConfig
        The configuration for the client.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        config: KuCoinDataClientConfig,
    ) -> None:
        self._http_client = KuCoinHttpClient(base_url=config.base_url_http)
        instrument_provider = KuCoinInstrumentProvider(
            http_client=self._http_client,
            clock=clock,
            config=config.instrument_provider,
        )

        super().__init__(
            loop=loop,
            client_id=KUCOIN_CLIENT_ID,
            venue=KUCOIN_VENUE,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )

        self._config = config
        self._ws_client: KuCoinWebSocketClient | None = None
        self._decoder = msgspec.json.Decoder(KuCoinWsMessage)
        self._subscribed_symbols: set[str] = set()

    def _send_all_instruments_to_data_engine(self) -> None:
        for instrument in self._instrument_provider.get_all().values():
            self._handle_data(instrument)
        for currency in self._instrument_provider.currencies().values():
            self._cache.add_currency(currency)

    # -- Lifecycle -----------------------------------------------------------------------------------

    async def _connect(self) -> None:
        self._log.info("Initializing KuCoin instruments...")
        await self._instrument_provider.initialize()
        self._send_all_instruments_to_data_engine()

        # Fetch a fresh WS token and build the connection URL
        ws_url = await self._build_ws_url()
        self._ws_client = KuCoinWebSocketClient(
            loop=self._loop,
            url=ws_url,
            handler=self._handle_ws_message,
            handler_reconnect=self._resubscribe,
        )
        await self._ws_client.connect()

    async def _disconnect(self) -> None:
        if self._ws_client is not None:
            await self._ws_client.disconnect()
            self._ws_client = None

    async def _build_ws_url(self) -> str:
        """Obtain a public WS token and build the full connection URL."""
        if self._config.base_url_ws:
            # Allow a static override (useful for testing)
            return self._config.base_url_ws
        token_data = await self._http_client.request_ws_token()
        server = token_data.instanceServers[0]
        url = f"{server.endpoint}?token={token_data.token}"
        return url

    # -- Subscriptions -------------------------------------------------------------------------------

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        symbol = command.instrument_id.symbol.value
        if symbol not in self._subscribed_symbols:
            self._subscribed_symbols.add(symbol)
            if self._ws_client is not None:
                await self._ws_client.subscribe_tickers([symbol])

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        symbol = command.instrument_id.symbol.value
        if symbol in self._subscribed_symbols:
            self._subscribed_symbols.discard(symbol)
            if self._ws_client is not None:
                await self._ws_client.unsubscribe_tickers([symbol])

    # -- WebSocket message handler -------------------------------------------------------------------

    def _handle_ws_message(self, raw: bytes) -> None:
        try:
            msg = self._decoder.decode(raw)
        except Exception as e:
            self._log.warning(f"Failed to decode WS message: {e!r} | {raw!r}")
            return

        # Only process ticker update messages; skip welcome, pong, ack, etc.
        if msg.type != "message" or msg.subject != "trade.ticker":
            return

        if msg.topic is None or msg.data is None:
            return

        # Extract symbol from topic: "/market/ticker:BTC-USDT" -> "BTC-USDT"
        try:
            raw_symbol = msg.topic.split(":", 1)[1]
        except IndexError:
            self._log.warning(f"Unexpected topic format: {msg.topic!r}")
            return

        instrument_id = InstrumentId.from_str(f"{raw_symbol}.KUCOIN")
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            self._log.debug(f"No instrument found for {instrument_id}, dropping tick")
            return

        data = msg.data
        if not data.bestBid or not data.bestAsk:
            return  # Incomplete tick, skip

        try:
            quote = QuoteTick(
                instrument_id=instrument_id,
                bid_price=Price(float(data.bestBid), precision=instrument.price_precision),
                ask_price=Price(float(data.bestAsk), precision=instrument.price_precision),
                bid_size=Quantity(float(data.bestBidSize), precision=instrument.size_precision),
                ask_size=Quantity(float(data.bestAskSize), precision=instrument.size_precision),
                ts_event=millis_to_nanos(data.time),
                ts_init=self._clock.timestamp_ns(),
            )
        except Exception as e:
            self._log.warning(f"Failed to build QuoteTick for {raw_symbol}: {e!r}")
            return

        self._handle_data(quote)

    async def _resubscribe(self) -> None:
        """Re-subscribe to all active ticker streams after a reconnection."""
        if self._subscribed_symbols and self._ws_client is not None:
            self._log.info(f"Resubscribing to {len(self._subscribed_symbols)} symbols...")
            await self._ws_client.subscribe_tickers(list(self._subscribed_symbols))
