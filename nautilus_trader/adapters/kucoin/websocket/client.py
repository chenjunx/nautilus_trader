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
import time
from collections.abc import Awaitable
from collections.abc import Callable

import msgspec

from nautilus_trader.common.component import Logger
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.nautilus_pyo3 import WebSocketClient
from nautilus_trader.core.nautilus_pyo3 import WebSocketClientError
from nautilus_trader.core.nautilus_pyo3 import WebSocketConfig


class KuCoinWebSocketClient:
    """
    Provides a KuCoin streaming WebSocket client with automatic heartbeat.

    KuCoin requires a ping frame every ``ping_interval`` milliseconds (default
    18 000 ms / 18 s) or the server will close the connection.  This client
    manages a background asyncio task that fires the ping automatically.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    url : str
        The full WebSocket URL (including ``?token=...`` query parameter).
    handler : Callable[[bytes], None]
        The callback handler for incoming raw message bytes.
    handler_reconnect : Callable[[], Awaitable[None]], optional
        Called after each reconnection to re-subscribe to active topics.
    ping_interval_ms : int, optional
        Heartbeat interval in milliseconds (default 18 000).

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        url: str,
        handler: Callable[[bytes], None],
        handler_reconnect: Callable[[], Awaitable[None]] | None = None,
        ping_interval_ms: int = 18_000,
    ) -> None:
        self._log: Logger = Logger(type(self).__name__)
        self._loop = loop
        self._url = url
        self._handler = handler
        self._handler_reconnect = handler_reconnect
        self._ping_interval_s: float = ping_interval_ms / 1_000.0
        self._client: WebSocketClient | None = None
        self._heartbeat_task: asyncio.Task | None = None

    @property
    def is_connected(self) -> bool:
        """Return True if the underlying WebSocket is open."""
        return self._client is not None and not self._client.is_closed()

    async def connect(self) -> None:
        """Connect to the KuCoin WebSocket server and start the heartbeat."""
        self._log.info(f"Connecting to {self._url}...", LogColor.BLUE)
        config = WebSocketConfig(
            url=self._url,
            headers=[],
            heartbeat=None,   # We manage the ping loop ourselves
            proxy_url=None,
        )
        self._client = await WebSocketClient.connect(
            loop_=self._loop,
            config=config,
            handler=self._handler,
            post_reconnection=self._handle_reconnect,
        )
        self._log.info(f"Connected to {self._url}", LogColor.BLUE)
        self._start_heartbeat()

    def _start_heartbeat(self) -> None:
        """Start the background ping task."""
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        self._heartbeat_task = self._loop.create_task(self._ping_loop())
        self._heartbeat_task.add_done_callback(self._on_heartbeat_done)

    def _on_heartbeat_done(self, task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._log.warning(f"Heartbeat task error: {exc!r}")

    async def _ping_loop(self) -> None:
        """Send a KuCoin ping every ping_interval seconds."""
        while True:
            await asyncio.sleep(self._ping_interval_s)
            if self._client is None or self._client.is_closed():
                break
            ping_id = str(int(time.time() * 1000))
            await self._send({"id": ping_id, "type": "ping"})
            self._log.debug(f"Sent ping id={ping_id}")

    def _handle_reconnect(self) -> None:
        """Called by the WebSocketClient after a reconnection."""
        # Restart heartbeat on the new connection
        self._start_heartbeat()
        if self._handler_reconnect is not None:
            task = self._loop.create_task(self._handler_reconnect())
            task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._log.warning(f"Reconnect handler error: {exc!r}")

    async def disconnect(self) -> None:
        """Disconnect from the server and stop the heartbeat."""
        # Cancel heartbeat task first
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

        if self._client is None:
            return
        if self._client.is_disconnecting() or self._client.is_closed():
            return
        try:
            await self._client.disconnect()
        except WebSocketClientError as e:
            self._log.warning(f"WebSocket disconnect error: {e!s}")
        self._client = None
        self._log.info("Disconnected", LogColor.BLUE)

    async def _send(self, payload: dict) -> None:
        if self._client is None:
            self._log.warning("Cannot send: client not connected")
            return
        try:
            await self._client.send_text(msgspec.json.encode(payload))
        except WebSocketClientError as e:
            self._log.warning(f"WebSocket send error: {e!s}")

    async def subscribe_tickers(self, symbols: list[str]) -> None:
        """
        Subscribe to /market/ticker for the given symbols.

        Multiple symbols are joined with commas in a single subscription frame.
        """
        topic = "/market/ticker:" + ",".join(symbols)
        msg_id = str(int(time.time() * 1000))
        await self._send({
            "id": msg_id,
            "type": "subscribe",
            "topic": topic,
            "privateChannel": False,
            "response": True,
        })
        self._log.debug(f"Subscribed /market/ticker: {symbols}")

    async def unsubscribe_tickers(self, symbols: list[str]) -> None:
        """
        Unsubscribe from /market/ticker for the given symbols.
        """
        topic = "/market/ticker:" + ",".join(symbols)
        msg_id = str(int(time.time() * 1000))
        await self._send({
            "id": msg_id,
            "type": "unsubscribe",
            "topic": topic,
            "privateChannel": False,
            "response": True,
        })
        self._log.debug(f"Unsubscribed /market/ticker: {symbols}")
