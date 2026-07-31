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


class GateIoWebSocketClient:
    """
    Provides a Gate.io streaming WebSocket client.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    url : str
        The WebSocket server URL.
    handler : Callable[[bytes], None]
        The callback handler for incoming messages.
    handler_reconnect : Callable[[], Awaitable[None]], optional
        Called after each reconnection to re-subscribe.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        url: str,
        handler: Callable[[bytes], None],
        handler_reconnect: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._log: Logger = Logger(type(self).__name__)
        self._loop = loop
        self._url = url
        self._handler = handler
        self._handler_reconnect = handler_reconnect
        self._client: WebSocketClient | None = None

    @property
    def is_connected(self) -> bool:
        return self._client is not None and not self._client.is_closed()

    async def connect(self) -> None:
        """Connect to the Gate.io WebSocket server."""
        self._log.info(f"Connecting to {self._url}...", LogColor.BLUE)
        config = WebSocketConfig(
            url=self._url,
            headers=[],
            heartbeat=20,
            proxy_url=None,
        )
        self._client = await WebSocketClient.connect(
            loop_=self._loop,
            config=config,
            handler=self._handler,
            post_reconnection=self._handle_reconnect,
        )
        self._log.info(f"Connected to {self._url}", LogColor.BLUE)

    def _handle_reconnect(self) -> None:
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
        """Disconnect from the server."""
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

    async def subscribe_book_ticker(self, symbols: list[str]) -> None:
        """Subscribe to spot.book_ticker for the given symbols."""
        await self._send({
            "time": int(time.time()),
            "channel": "spot.book_ticker",
            "event": "subscribe",
            "payload": symbols,
        })
        self._log.debug(f"Subscribed spot.book_ticker: {symbols}")

    async def unsubscribe_book_ticker(self, symbols: list[str]) -> None:
        """Unsubscribe from spot.book_ticker for the given symbols."""
        await self._send({
            "time": int(time.time()),
            "channel": "spot.book_ticker",
            "event": "unsubscribe",
            "payload": symbols,
        })
        self._log.debug(f"Unsubscribed spot.book_ticker: {symbols}")
