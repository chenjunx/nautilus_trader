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

from typing import Any

import msgspec


class GateIoBookTickerResult(msgspec.Struct):
    """
    Gate.io spot.book_ticker update result.

    Fields
    ------
    t : int
        Server timestamp in milliseconds.
    s : str
        Currency pair symbol, e.g. "BTC_USDT".
    b : str
        Best bid price.
    B : str
        Best bid size.
    a : str
        Best ask price.
    A : str
        Best ask size.
    u : int, optional
        Update ID.

    """

    t: int          # timestamp ms
    s: str          # symbol
    b: str          # best bid price
    B: str          # best bid size
    a: str          # best ask price
    A: str          # best ask size
    u: int | None = None


class GateIoWsMessage(msgspec.Struct, omit_defaults=True):
    """Top-level Gate.io WebSocket frame."""

    time: int
    channel: str
    event: str
    result: GateIoBookTickerResult | None = None
    error: dict[str, Any] | None = None
