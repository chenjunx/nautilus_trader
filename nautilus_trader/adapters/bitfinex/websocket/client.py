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
from collections.abc import Awaitable
from collections.abc import Callable

import msgspec

from nautilus_trader.common.component import Logger
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.nautilus_pyo3 import WebSocketClient
from nautilus_trader.core.nautilus_pyo3 import WebSocketClientError
from nautilus_trader.core.nautilus_pyo3 import WebSocketConfig


# Bitfinex enforces a minimum spacing between new connection opens on the public
# (unauthenticated) domain of ~20 connections/minute; stagger by a safe margin.
_MIN_CONNECT_INTERVAL_SECS = 3.0


class BitfinexWebSocketClient:
    """
    Provides a Bitfinex streaming WebSocket client.

    Manages a pool of WebSocket connections, each limited to a configurable number
    of ticker subscriptions (Bitfinex currently caps public channels at 25 per
    connection). New connections are opened on demand as existing ones fill up.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    url : str
        The WebSocket server URL.
    handler : Callable[[int, bytes], None]
        The callback handler for incoming messages, receiving the originating
        connection's ``client_id`` and the raw message bytes.
    handler_reconnect : Callable[[int], Awaitable[None]], optional
        Called after a connection reconnects, with that connection's ``client_id``,
        to allow re-subscribing to channels tied to that connection.
    max_subscriptions_per_connection : int, default 25
        The maximum number of ticker subscriptions per WebSocket connection
        (Bitfinex's documented public channel limit is 25).

    References
    ----------
    https://docs.bitfinex.com/docs/requirements-and-limitations

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        url: str,
        handler: Callable[[int, bytes], None],
        handler_reconnect: Callable[[int], Awaitable[None]] | None = None,
        max_subscriptions_per_connection: int = 25,
    ) -> None:
        self._log: Logger = Logger(type(self).__name__)
        self._loop = loop
        self._url = url
        self._handler = handler
        self._handler_reconnect = handler_reconnect
        self._max_subscriptions_per_connection = max_subscriptions_per_connection

        self._clients: dict[int, WebSocketClient | None] = {}
        self._client_symbols: dict[int, list[str]] = {}
        self._symbol_client_map: dict[str, int] = {}
        self._is_connecting: dict[int, bool] = {}
        self._next_client_id: int = 0
        self._last_connect_time: float | None = None
        self._lock: asyncio.Lock = asyncio.Lock()

    @property
    def is_connected(self) -> bool:
        """Return whether any pooled connection is active."""
        return any(
            client is not None and not client.is_closed() for client in self._clients.values()
        )

    async def connect(self) -> None:
        """Prepare the client for use (connections are opened lazily per subscription)."""
        self._log.info(f"Ready to connect to {self._url}", LogColor.BLUE)

    async def disconnect(self) -> None:
        """Disconnect all pooled connections from the Bitfinex WebSocket server."""
        tasks = [self._disconnect_client(client_id) for client_id in list(self._clients.keys())]
        if tasks:
            await asyncio.gather(*tasks)
        self._log.info("Disconnected", LogColor.BLUE)

    async def _disconnect_client(self, client_id: int) -> None:
        client = self._clients.get(client_id)
        if client is None:
            return
        if client.is_disconnecting() or client.is_closed():
            self._clients[client_id] = None
            return
        try:
            await client.disconnect()
        except WebSocketClientError as e:
            self._log.warning(f"ws-client {client_id}: disconnect error: {e!s}")
        self._clients[client_id] = None

    def _find_or_allocate_client_id(self, symbol: str) -> int:
        for client_id, symbols in self._client_symbols.items():
            if len(symbols) < self._max_subscriptions_per_connection:
                self._client_symbols[client_id].append(symbol)
                self._symbol_client_map[symbol] = client_id
                return client_id

        client_id = self._next_client_id
        self._next_client_id += 1
        self._clients[client_id] = None
        self._client_symbols[client_id] = [symbol]
        self._is_connecting[client_id] = False
        self._symbol_client_map[symbol] = client_id
        return client_id

    async def _ensure_connected(self, client_id: int) -> None:
        while self._is_connecting.get(client_id):
            await asyncio.sleep(0.01)

        if self._clients.get(client_id) is not None:
            return

        self._is_connecting[client_id] = True
        try:
            if self._last_connect_time is not None:
                elapsed = self._loop.time() - self._last_connect_time
                wait = _MIN_CONNECT_INTERVAL_SECS - elapsed
                if wait > 0:
                    await asyncio.sleep(wait)

            self._log.info(f"ws-client {client_id}: Connecting to {self._url}...", LogColor.BLUE)
            config = WebSocketConfig(
                url=self._url,
                headers=[],
                heartbeat=20,
                proxy_url=None,
            )
            self._clients[client_id] = await WebSocketClient.connect(
                loop_=self._loop,
                config=config,
                handler=lambda raw, cid=client_id: self._handler(cid, raw),
                post_reconnection=lambda cid=client_id: self._handle_reconnect(cid),
            )
            self._last_connect_time = self._loop.time()
            self._log.info(f"ws-client {client_id}: Connected to {self._url}", LogColor.BLUE)
        finally:
            self._is_connecting[client_id] = False

    def _handle_reconnect(self, client_id: int) -> None:
        self._log.warning(f"ws-client {client_id}: Reconnected to {self._url}")

        async def _resubscribe() -> None:
            if self._handler_reconnect is not None:
                try:
                    await self._handler_reconnect(client_id)
                except Exception as e:
                    self._log.warning(f"ws-client {client_id}: reconnect handler error: {e!r}")

            for symbol in self._client_symbols.get(client_id, []):
                await self._send_subscribe(client_id, symbol)

        task = self._loop.create_task(_resubscribe())
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._log.warning(f"Task error: {exc!r}")

    async def _send(self, client_id: int, payload: dict) -> None:
        client = self._clients.get(client_id)
        if client is None:
            self._log.warning(f"ws-client {client_id}: Cannot send: not connected")
            return
        try:
            await client.send_text(msgspec.json.encode(payload))
        except WebSocketClientError as e:
            self._log.warning(f"ws-client {client_id}: send error: {e!s}")

    async def _send_subscribe(self, client_id: int, symbol: str) -> None:
        await self._send(
            client_id,
            {
                "event": "subscribe",
                "channel": "ticker",
                "symbol": f"t{symbol}",
            },
        )
        self._log.debug(f"ws-client {client_id}: Subscribed ticker: t{symbol}")

    async def subscribe_ticker(self, symbol: str) -> None:
        """
        Subscribe to the ticker channel for *symbol*.

        Automatically routes the subscription to a pooled connection with spare
        capacity, opening a new connection if all existing ones are full.

        Parameters
        ----------
        symbol : str
            The pair symbol **without** the ``t`` prefix, e.g. ``"BTCUSD"``.

        """
        async with self._lock:
            if symbol in self._symbol_client_map:
                return
            client_id = self._find_or_allocate_client_id(symbol)

        await self._ensure_connected(client_id)
        await self._send_subscribe(client_id, symbol)

    async def unsubscribe_ticker(self, symbol: str, client_id: int, chan_id: int) -> None:
        """
        Unsubscribe from a ticker channel by its server-assigned channel ID.

        Parameters
        ----------
        symbol : str
            The pair symbol **without** the ``t`` prefix, e.g. ``"BTCUSD"``.
        client_id : int
            The pooled connection that owns this channel.
        chan_id : int
            The channel ID returned by the server in the ``subscribed`` event.
            This is scoped to the connection identified by *client_id*.

        """
        await self._send(client_id, {"event": "unsubscribe", "chanId": chan_id})
        self._log.debug(f"ws-client {client_id}: Unsubscribed chanId: {chan_id}")

        async with self._lock:
            self._symbol_client_map.pop(symbol, None)
            symbols = self._client_symbols.get(client_id)
            if symbols is not None and symbol in symbols:
                symbols.remove(symbol)
