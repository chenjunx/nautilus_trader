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


class KuCoinTickerData(msgspec.Struct, omit_defaults=True):
    """
    KuCoin ticker data payload from a /market/ticker WebSocket message.

    Fields
    ------
    bestAsk : str
        Best ask price.
    bestAskSize : str
        Best ask size.
    bestBid : str
        Best bid price.
    bestBidSize : str
        Best bid size.
    time : int
        Server timestamp in milliseconds.

    """

    bestAsk: str = ""
    bestAskSize: str = ""
    bestBid: str = ""
    bestBidSize: str = ""
    time: int = 0


class KuCoinWsMessage(msgspec.Struct, omit_defaults=True):
    """
    Top-level KuCoin WebSocket frame.

    Fields
    ------
    type : str
        Message type: "welcome", "message", "pong", "ack", "error".
    id : str, optional
        Message ID (present on ping/pong and ack frames).
    topic : str, optional
        Subscription topic, e.g. "/market/ticker:BTC-USDT".
    subject : str, optional
        Subject within the topic, e.g. "trade.ticker".
    data : KuCoinTickerData, optional
        The ticker payload (present when type=="message").

    """

    type: str
    id: str | None = None
    topic: str | None = None
    subject: str | None = None
    data: KuCoinTickerData | None = None
