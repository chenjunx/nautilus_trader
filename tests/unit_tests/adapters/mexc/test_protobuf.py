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

"""
Tests for the hand-rolled MEXC protobuf wire-format decoder.

Fixtures below are built with small local encoding helpers (mirroring the wire format
described in ``nautilus_trader/adapters/mexc/websocket/protobuf.py``) rather than a real
protobuf library, since the adapter intentionally avoids that dependency.
"""

from nautilus_trader.adapters.mexc.websocket.protobuf import decode_book_ticker_batch


def _encode_varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _encode_tag(field_no: int, wire_type: int) -> bytes:
    return _encode_varint((field_no << 3) | wire_type)


def _encode_string_field(field_no: int, value: str) -> bytes:
    encoded = value.encode()
    return _encode_tag(field_no, 2) + _encode_varint(len(encoded)) + encoded


def _encode_bytes_field(field_no: int, value: bytes) -> bytes:
    return _encode_tag(field_no, 2) + _encode_varint(len(value)) + value


def _encode_varint_field(field_no: int, value: int) -> bytes:
    return _encode_tag(field_no, 0) + _encode_varint(value)


def _encode_book_ticker_item(
    bid_price: str,
    bid_qty: str,
    ask_price: str,
    ask_qty: str,
) -> bytes:
    return (
        _encode_string_field(1, bid_price)
        + _encode_string_field(2, bid_qty)
        + _encode_string_field(3, ask_price)
        + _encode_string_field(4, ask_qty)
    )


def _encode_book_ticker_batch(items: list[bytes]) -> bytes:
    out = bytearray()
    for item in items:
        out += _encode_bytes_field(1, item)
    return bytes(out)


def _encode_wrapper(
    channel: str,
    batch: bytes,
    send_time_ms: int | None = None,
) -> bytes:
    out = bytearray()
    out += _encode_string_field(1, channel)
    if send_time_ms is not None:
        out += _encode_varint_field(6, send_time_ms)
    out += _encode_bytes_field(311, batch)
    return bytes(out)


class TestDecodeBookTickerBatch:
    def test_decodes_channel_and_single_item(self):
        item = _encode_book_ticker_item("100.5", "1.2", "100.6", "0.8")
        batch = _encode_book_ticker_batch([item])
        raw = _encode_wrapper("spot@public.bookTicker.batch.v3.api.pb@BTCUSDT", batch)

        channel, send_time_ms, items = decode_book_ticker_batch(raw)

        assert channel == "spot@public.bookTicker.batch.v3.api.pb@BTCUSDT"
        assert send_time_ms is None
        assert len(items) == 1
        assert items[0].bid_price == "100.5"
        assert items[0].bid_qty == "1.2"
        assert items[0].ask_price == "100.6"
        assert items[0].ask_qty == "0.8"

    def test_decodes_send_time(self):
        item = _encode_book_ticker_item("1", "2", "3", "4")
        batch = _encode_book_ticker_batch([item])
        raw = _encode_wrapper("spot@public.bookTicker.batch.v3.api.pb@ETHUSDT", batch, send_time_ms=1738368000123)

        _channel, send_time_ms, _items = decode_book_ticker_batch(raw)

        assert send_time_ms == 1738368000123

    def test_decodes_multiple_items(self):
        item_a = _encode_book_ticker_item("10", "1", "11", "1")
        item_b = _encode_book_ticker_item("20", "2", "21", "2")
        batch = _encode_book_ticker_batch([item_a, item_b])
        raw = _encode_wrapper("spot@public.bookTicker.batch.v3.api.pb@SOLUSDT", batch)

        _channel, _send_time_ms, items = decode_book_ticker_batch(raw)

        assert len(items) == 2
        assert items[0].bid_price == "10"
        assert items[1].bid_price == "20"

    def test_no_batch_field_returns_empty_items(self):
        raw = _encode_string_field(1, "spot@public.bookTicker.batch.v3.api.pb@BTCUSDT")

        channel, send_time_ms, items = decode_book_ticker_batch(raw)

        assert channel == "spot@public.bookTicker.batch.v3.api.pb@BTCUSDT"
        assert send_time_ms is None
        assert items == []
