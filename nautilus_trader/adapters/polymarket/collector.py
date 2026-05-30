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

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import msgspec

from nautilus_trader.adapters.polymarket.common.symbol import get_polymarket_instrument_id
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketBookSnapshot
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketQuotes
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketTickSizeChange
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketTrade
from nautilus_trader.adapters.polymarket.websocket.client import PolymarketWebSocketChannel
from nautilus_trader.adapters.polymarket.websocket.client import PolymarketWebSocketClient
from nautilus_trader.adapters.polymarket.websocket.types import MARKET_WS_MESSAGE
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import Logger
from nautilus_trader.model.data import OrderBookDelta
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.instruments import BinaryOption
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog


@dataclass(frozen=True)
class PolymarketParquetCollectorConfig:
    catalog_path: str
    flush_interval_secs: float = 5.0
    max_buffer_size: int = 10_000
    ws_base_url: str | None = None
    proxy_url: str | None = None
    max_subscriptions_per_connection: int = 200


class PolymarketParquetCollector:
    def __init__(
        self,
        config: PolymarketParquetCollectorConfig,
        instrument: BinaryOption,
        catalog: ParquetDataCatalog,
        clock: LiveClock,
    ) -> None:
        self.config = config
        self.instrument = instrument
        self._catalog = catalog
        self._clock = clock
        self._log = Logger(type(self).__name__)
        self._decoder = msgspec.json.Decoder(MARKET_WS_MESSAGE)
        self._order_book_deltas: list[OrderBookDelta] = []
        self._trades: list[TradeTick] = []
        self._is_live = False
        self._last_flush_monotonic = time.monotonic()
        self.dropped_price_changes_before_snapshot = 0
        self.dropped_trades_before_snapshot = 0
        self.ignored_other_instrument_messages = 0
        self.snapshot_count = 0
        self.price_change_count = 0
        self.trade_count = 0

    @property
    def is_live(self) -> bool:
        return self._is_live

    @property
    def pending_order_book_delta_count(self) -> int:
        return len(self._order_book_deltas)

    @property
    def pending_trade_count(self) -> int:
        return len(self._trades)

    @property
    def pending_order_book_deltas(self) -> list[OrderBookDelta]:
        return list(self._order_book_deltas)

    @property
    def pending_trades(self) -> list[TradeTick]:
        return list(self._trades)

    def clear_buffers_for_test(self) -> None:
        self._order_book_deltas.clear()
        self._trades.clear()

    def reset_continuity(self, reason: str) -> None:
        if self._is_live:
            self._log.warning(f"Resetting Polymarket collector continuity: {reason}")
        self._is_live = False

    def handle_raw_message(self, raw: bytes) -> None:
        msg = self._decoder.decode(raw)
        if isinstance(msg, list):
            for item in msg:
                self.handle_message(item)
        else:
            self.handle_message(msg)

    def handle_message(self, msg: Any) -> None:
        if isinstance(msg, PolymarketBookSnapshot):
            self._handle_snapshot(msg)
        elif isinstance(msg, PolymarketQuotes):
            self._handle_quotes(msg)
        elif isinstance(msg, PolymarketTrade):
            self._handle_trade(msg)
        elif isinstance(msg, PolymarketTickSizeChange):
            self.reset_continuity("tick size change")
        else:
            self._log.error(f"Unknown Polymarket market message: {msg!r}")

        self.flush_if_needed()

    def _matches_instrument(self, market: str, asset_id: str) -> bool:
        return get_polymarket_instrument_id(market, asset_id) == self.instrument.id

    def _handle_snapshot(self, msg: PolymarketBookSnapshot) -> None:
        if not self._matches_instrument(msg.market, msg.asset_id):
            self.ignored_other_instrument_messages += 1
            return

        now_ns = self._clock.timestamp_ns()
        deltas = msg.parse_to_snapshot(self.instrument, ts_init=now_ns)
        if deltas is None:
            return

        self._order_book_deltas.extend(deltas.deltas)
        self._is_live = True
        self.snapshot_count += 1
        self._log.info(f"Accepted Polymarket book snapshot for {self.instrument.id}")

    def _handle_quotes(self, msg: PolymarketQuotes) -> None:
        if not self._is_live:
            self.dropped_price_changes_before_snapshot += len(msg.price_changes)
            return

        for change in msg.price_changes:
            if not self._matches_instrument(msg.market, change.asset_id):
                self.ignored_other_instrument_messages += 1
                continue

            now_ns = self._clock.timestamp_ns()
            deltas = PolymarketQuotes(
                market=msg.market,
                price_changes=[change],
                timestamp=msg.timestamp,
            ).parse_to_deltas(self.instrument, ts_init=now_ns)
            self._order_book_deltas.extend(deltas.deltas)
            self.price_change_count += 1

    def _handle_trade(self, msg: PolymarketTrade) -> None:
        if not self._matches_instrument(msg.market, msg.asset_id):
            self.ignored_other_instrument_messages += 1
            return

        if not self._is_live:
            self.dropped_trades_before_snapshot += 1
            return

        now_ns = self._clock.timestamp_ns()
        self._trades.append(msg.parse_to_trade_tick(self.instrument, ts_init=now_ns))
        self.trade_count += 1

    def flush_if_needed(self) -> None:
        if (
            self.pending_order_book_delta_count + self.pending_trade_count
            >= self.config.max_buffer_size
        ):
            self.flush()
            return

        elapsed = time.monotonic() - self._last_flush_monotonic
        if elapsed >= self.config.flush_interval_secs:
            self.flush()

    def flush(self) -> None:
        if self._order_book_deltas:
            data = sorted(self._order_book_deltas, key=lambda x: x.ts_init)
            self._catalog.write_data(data, skip_disjoint_check=True)
            self._log.info(f"Flushed {len(data)} Polymarket order book deltas")
            self._order_book_deltas.clear()

        if self._trades:
            data = sorted(self._trades, key=lambda x: x.ts_init)
            self._catalog.write_data(data, skip_disjoint_check=True)
            self._log.info(f"Flushed {len(data)} Polymarket trades")
            self._trades.clear()

        self._last_flush_monotonic = time.monotonic()

    async def run(self, token_id: str) -> None:
        loop = asyncio.get_running_loop()
        ws_client = PolymarketWebSocketClient(
            clock=self._clock,
            base_url=self.config.ws_base_url,
            channel=PolymarketWebSocketChannel.MARKET,
            handler=self.handle_raw_message,
            handler_reconnect=self._handle_reconnect,
            loop=loop,
            max_subscriptions_per_connection=self.config.max_subscriptions_per_connection,
            proxy_url=self.config.proxy_url,
        )
        ws_client.add_subscription(token_id)
        await ws_client.connect()
        try:
            while True:
                await asyncio.sleep(1.0)
                self.flush_if_needed()
        finally:
            await ws_client.disconnect()
            self.flush()

    async def _handle_reconnect(self) -> None:
        self.reset_continuity("websocket reconnect")
