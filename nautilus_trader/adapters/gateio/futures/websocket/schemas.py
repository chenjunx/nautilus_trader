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


class GateIoFuturesBookTickerResult(msgspec.Struct, omit_defaults=True):
    """
    Gate.io futures.book_ticker update result.

    Fields
    ------
    t : int
        Server timestamp in milliseconds.
    s : str
        Contract symbol, e.g. "BTC_USDT".
    b : str
        Best bid price.
    B : int
        Best bid size, in contracts.
    a : str
        Best ask price.
    A : int
        Best ask size, in contracts.
    u : int, optional
        Update ID.

    """

    t: int          # timestamp ms
    s: str          # contract symbol
    b: str          # best bid price
    B: int          # best bid size (contracts)
    a: str          # best ask price
    A: int          # best ask size (contracts)
    u: int | None = None


class GateIoFuturesWsMessage(msgspec.Struct, omit_defaults=True):
    """Top-level Gate.io futures WebSocket frame.

    result is typed as Any so that both subscribe-ack {"status":"success"}
    and ticker update objects decode without error. The ticker payload is
    converted to GateIoFuturesBookTickerResult via msgspec.convert() only
    when event == "update".
    """

    time: int
    channel: str
    event: str
    result: Any = None
    error: Any = None
