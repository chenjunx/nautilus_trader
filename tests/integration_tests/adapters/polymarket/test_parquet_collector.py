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

from pathlib import Path

from nautilus_trader.adapters.polymarket.collector import PolymarketParquetCollector
from nautilus_trader.adapters.polymarket.collector import PolymarketParquetCollectorConfig
from nautilus_trader.adapters.polymarket.common.enums import PolymarketOrderSide
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketBookLevel
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketBookSnapshot
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketQuote
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketQuotes
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketTrade
from nautilus_trader.common.component import LiveClock
from nautilus_trader.model.enums import BookAction
from nautilus_trader.model.enums import RecordFlag
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.test_kit.providers import TestInstrumentProvider


def _collector(tmp_path: Path) -> PolymarketParquetCollector:
    instrument = TestInstrumentProvider.binary_option()
    catalog = ParquetDataCatalog(tmp_path / "catalog")
    return PolymarketParquetCollector(
        config=PolymarketParquetCollectorConfig(
            catalog_path=str(tmp_path / "catalog"),
            flush_interval_secs=60.0,
            max_buffer_size=100,
        ),
        instrument=instrument,
        catalog=catalog,
        clock=LiveClock(),
    )


def _snapshot(instrument) -> PolymarketBookSnapshot:
    condition_id, token_id = instrument.id.symbol.value.split("-")
    return PolymarketBookSnapshot(
        market=condition_id,
        asset_id=token_id,
        bids=[
            PolymarketBookLevel(price="0.49", size="200.00"),
            PolymarketBookLevel(price="0.48", size="100.00"),
        ],
        asks=[
            PolymarketBookLevel(price="0.51", size="150.00"),
            PolymarketBookLevel(price="0.52", size="250.00"),
        ],
        timestamp="1703875200000",
    )


def _price_change(instrument) -> PolymarketQuotes:
    condition_id, token_id = instrument.id.symbol.value.split("-")
    return PolymarketQuotes(
        market=condition_id,
        price_changes=[
            PolymarketQuote(
                asset_id=token_id,
                price="0.49",
                side=PolymarketOrderSide.BUY,
                size="225.00",
                hash="0xpricechange001",
                best_bid="0.49",
                best_ask="0.51",
            ),
        ],
        timestamp="1703875201000",
    )


def _trade(instrument) -> PolymarketTrade:
    condition_id, token_id = instrument.id.symbol.value.split("-")
    return PolymarketTrade(
        market=condition_id,
        asset_id=token_id,
        fee_rate_bps="0",
        price="0.51",
        side=PolymarketOrderSide.BUY,
        size="25.00",
        timestamp="1703875202000",
    )


def test_price_change_before_snapshot_is_dropped(tmp_path: Path) -> None:
    collector = _collector(tmp_path)
    collector.handle_message(_price_change(collector.instrument))

    assert collector.is_live is False
    assert collector.dropped_price_changes_before_snapshot == 1
    assert collector.pending_order_book_delta_count == 0


def test_book_snapshot_starts_live_segment(tmp_path: Path) -> None:
    collector = _collector(tmp_path)
    collector.handle_message(_snapshot(collector.instrument))

    assert collector.is_live is True
    assert collector.pending_order_book_delta_count == 5
    deltas = collector.pending_order_book_deltas
    assert deltas[0].action == BookAction.CLEAR
    assert deltas[0].flags & RecordFlag.F_SNAPSHOT
    assert deltas[-1].flags & RecordFlag.F_LAST


def test_price_change_after_snapshot_is_buffered_as_increment(tmp_path: Path) -> None:
    collector = _collector(tmp_path)
    collector.handle_message(_snapshot(collector.instrument))
    collector.clear_buffers_for_test()

    collector.handle_message(_price_change(collector.instrument))

    assert collector.pending_order_book_delta_count == 1
    delta = collector.pending_order_book_deltas[0]
    assert delta.action == BookAction.UPDATE
    assert not delta.flags & RecordFlag.F_SNAPSHOT
    assert delta.flags & RecordFlag.F_LAST


def test_trade_before_snapshot_is_dropped(tmp_path: Path) -> None:
    collector = _collector(tmp_path)
    collector.handle_message(_trade(collector.instrument))

    assert collector.dropped_trades_before_snapshot == 1
    assert collector.pending_trade_count == 0


def test_trade_after_snapshot_is_buffered(tmp_path: Path) -> None:
    collector = _collector(tmp_path)
    collector.handle_message(_snapshot(collector.instrument))
    collector.clear_buffers_for_test()

    collector.handle_message(_trade(collector.instrument))

    assert collector.pending_trade_count == 1
    assert collector.pending_trades[0].instrument_id == collector.instrument.id


def test_reset_continuity_requires_new_snapshot(tmp_path: Path) -> None:
    collector = _collector(tmp_path)
    collector.handle_message(_snapshot(collector.instrument))
    collector.clear_buffers_for_test()

    collector.reset_continuity("test reconnect")
    collector.handle_message(_price_change(collector.instrument))

    assert collector.is_live is False
    assert collector.dropped_price_changes_before_snapshot == 1
    assert collector.pending_order_book_delta_count == 0


def test_flush_writes_order_book_deltas_to_catalog(tmp_path: Path) -> None:
    collector = _collector(tmp_path)
    collector.handle_message(_snapshot(collector.instrument))

    collector.flush()

    assert collector.pending_order_book_delta_count == 0
    files = list((tmp_path / "catalog" / "data" / "order_book_deltas").rglob("*.parquet"))
    assert len(files) == 1


def test_flush_writes_trade_ticks_to_catalog(tmp_path: Path) -> None:
    collector = _collector(tmp_path)
    collector.handle_message(_snapshot(collector.instrument))
    collector.clear_buffers_for_test()
    collector.handle_message(_trade(collector.instrument))

    collector.flush()

    assert collector.pending_trade_count == 0
    files = list((tmp_path / "catalog" / "data" / "trade_tick").rglob("*.parquet"))
    assert len(files) == 1


def test_collector_exports_from_polymarket_package() -> None:
    from nautilus_trader.adapters.polymarket import PolymarketParquetCollector
    from nautilus_trader.adapters.polymarket import PolymarketParquetCollectorConfig

    assert PolymarketParquetCollector is not None
    assert PolymarketParquetCollectorConfig is not None
