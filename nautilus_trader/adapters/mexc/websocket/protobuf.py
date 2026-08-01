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
Minimal protobuf wire-format decoder for MEXC spot market data pushes.

MEXC's ``*.pb`` streams wrap payloads in ``PushDataV3ApiWrapper`` (see
https://github.com/mexcdevelop/websocket-proto). Only the book ticker batch
payload is needed here, so rather than depending on the ``protobuf`` package
and its codegen this module walks the wire format directly.

Relevant schema (field numbers from the official ``.proto`` files)::

    message PushDataV3ApiWrapper {
      string channel = 1;
      oneof body {
        PublicBookTickerBatchV3Api publicBookTickerBatch = 311;
      }
    }

    message PublicBookTickerBatchV3Api {
      repeated PublicBookTickerV3Api items = 1;
    }

    message PublicBookTickerV3Api {
      string bidPrice = 1;
      string bidQuantity = 2;
      string askPrice = 3;
      string askQuantity = 4;
    }

"""

from collections.abc import Iterator

import msgspec


_WIRE_VARINT = 0
_WIRE_FIXED64 = 1
_WIRE_LENGTH_DELIMITED = 2
_WIRE_FIXED32 = 5

_FIELD_WRAPPER_CHANNEL = 1
_FIELD_WRAPPER_SEND_TIME = 6
_FIELD_WRAPPER_BOOK_TICKER_BATCH = 311
_FIELD_BATCH_ITEMS = 1
_FIELD_TICKER_BID_PRICE = 1
_FIELD_TICKER_BID_QTY = 2
_FIELD_TICKER_ASK_PRICE = 3
_FIELD_TICKER_ASK_QTY = 4


class BookTickerItem(msgspec.Struct, frozen=True):
    """A single decoded MEXC book ticker entry."""

    bid_price: str
    bid_qty: str
    ask_price: str
    ask_qty: str


def _read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7


def _iter_fields(buf: bytes) -> Iterator[tuple[int, int, bytes | int]]:
    pos = 0
    length = len(buf)
    while pos < length:
        tag, pos = _read_varint(buf, pos)
        field_no = tag >> 3
        wire_type = tag & 0x7

        if wire_type == _WIRE_VARINT:
            value, pos = _read_varint(buf, pos)
        elif wire_type == _WIRE_LENGTH_DELIMITED:
            value_len, pos = _read_varint(buf, pos)
            value = buf[pos : pos + value_len]
            pos += value_len
        elif wire_type == _WIRE_FIXED64:
            value = buf[pos : pos + 8]
            pos += 8
        elif wire_type == _WIRE_FIXED32:
            value = buf[pos : pos + 4]
            pos += 4
        else:
            raise ValueError(f"Unsupported protobuf wire type {wire_type}")

        yield field_no, wire_type, value


def _decode_book_ticker(buf: bytes) -> BookTickerItem:
    fields: dict[int, bytes] = {}
    for field_no, _wire_type, value in _iter_fields(buf):
        fields[field_no] = value  # type: ignore[assignment]

    return BookTickerItem(
        bid_price=fields[_FIELD_TICKER_BID_PRICE].decode(),
        bid_qty=fields[_FIELD_TICKER_BID_QTY].decode(),
        ask_price=fields[_FIELD_TICKER_ASK_PRICE].decode(),
        ask_qty=fields[_FIELD_TICKER_ASK_QTY].decode(),
    )


def decode_book_ticker_batch(raw: bytes) -> tuple[str, int | None, list[BookTickerItem]]:
    """
    Decode a ``PushDataV3ApiWrapper`` frame carrying a book ticker batch payload.

    Parameters
    ----------
    raw : bytes
        The raw protobuf-encoded WebSocket frame.

    Returns
    -------
    tuple[str, int | None, list[BookTickerItem]]
        The channel name, the server send time in milliseconds (if present),
        and the decoded book ticker items (usually one, since each channel is
        subscribed per symbol).

    """
    channel = ""
    send_time_ms: int | None = None
    items: list[BookTickerItem] = []

    for field_no, _wire_type, value in _iter_fields(raw):
        if field_no == _FIELD_WRAPPER_CHANNEL:
            channel = value.decode()  # type: ignore[union-attr]
        elif field_no == _FIELD_WRAPPER_SEND_TIME:
            send_time_ms = value  # type: ignore[assignment]
        elif field_no == _FIELD_WRAPPER_BOOK_TICKER_BATCH:
            for item_field_no, _item_wire_type, item_value in _iter_fields(value):  # type: ignore[arg-type]
                if item_field_no == _FIELD_BATCH_ITEMS:
                    items.append(_decode_book_ticker(item_value))  # type: ignore[arg-type]

    return channel, send_time_ms, items
