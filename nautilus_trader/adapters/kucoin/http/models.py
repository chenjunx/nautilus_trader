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


class KuCoinSymbol(msgspec.Struct, omit_defaults=True):
    """KuCoin spot symbol info from GET /api/v2/symbols."""

    symbol: str                         # e.g. "BTC-USDT"
    baseCurrency: str                   # e.g. "BTC"
    quoteCurrency: str                  # e.g. "USDT"
    baseIncrement: str                  # e.g. "0.00000001"
    quoteIncrement: str                 # e.g. "0.000001"
    priceIncrement: str                 # e.g. "0.1"
    enableTrading: bool = True
    feeCurrency: str = ""
    makerFeeCoefficient: str = "0"
    takerFeeCoefficient: str = "0"


class KuCoinSymbolsResponse(msgspec.Struct):
    """Wrapper for GET /api/v2/symbols response."""

    code: str
    data: list[KuCoinSymbol]


class KuCoinWsInstanceServer(msgspec.Struct):
    """A single WebSocket instance server entry from the token response."""

    endpoint: str
    pingInterval: int = 18000
    encrypt: bool = True
    protocol: str = "websocket"
    pingTimeout: int = 10000


class KuCoinWsTokenData(msgspec.Struct):
    """The `data` payload inside the bullet-public token response."""

    token: str
    instanceServers: list[KuCoinWsInstanceServer]


class KuCoinWsTokenResponse(msgspec.Struct):
    """Wrapper for POST /api/v1/bullet-public response."""

    code: str
    data: KuCoinWsTokenData
