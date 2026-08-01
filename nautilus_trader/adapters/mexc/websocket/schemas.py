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

import msgspec


class MexcBookTickerData(msgspec.Struct, omit_defaults=True):
    """
    MEXC spot book ticker data payload (``d`` field of WS message).

    Fields
    ------
    a : str
        Best ask price.
    A : str
        Best ask size.
    b : str
        Best bid price.
    B : str
        Best bid size.
    s : str
        Symbol, e.g. "BTCUSDT".

    """

    a: str   # best ask price
    A: str   # best ask size
    b: str   # best bid price
    B: str   # best bid size
    s: str   # symbol


class MexcWsMessage(msgspec.Struct, omit_defaults=True):
    """
    Top-level MEXC WebSocket frame.

    Fields
    ------
    c : str, optional
        Channel name, e.g. "spot@public.bookTicker.v3.api@BTCUSDT".
    d : MexcBookTickerData, optional
        Data payload (present for ticker updates, absent for control messages).
    s : str, optional
        Symbol (top-level mirror of ``d.s``).
    t : int, optional
        Server timestamp in milliseconds.

    """

    c: str | None = None
    d: MexcBookTickerData | None = None
    method: str | None = None   # 服务端控制消息字段，如 "PING"
    s: str | None = None
    t: int | None = None
