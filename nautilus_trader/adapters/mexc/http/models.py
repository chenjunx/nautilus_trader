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


class MexcSymbol(msgspec.Struct, omit_defaults=True):
    """
    MEXC exchange symbol info from GET /api/v3/exchangeInfo.

    Fields
    ------
    symbol : str
        Trading pair symbol, e.g. "BTCUSDT".
    status : str
        Trading status, e.g. "TRADING".
    baseAsset : str
        Base asset, e.g. "BTC".
    baseAssetPrecision : int
        Base asset precision (size decimal places).
    quoteAsset : str
        Quote asset, e.g. "USDT".
    quotePrecision : int
        Quote precision (price decimal places).
    quoteAssetPrecision : int
        Quote asset precision.
    isSpotTradingAllowed : bool
        Whether spot trading is allowed.
    isMarginTradingAllowed : bool
        Whether margin trading is allowed.

    """

    symbol: str
    status: str
    baseAsset: str
    baseAssetPrecision: int
    quoteAsset: str
    quotePrecision: int = 8
    quoteAssetPrecision: int = 8
    isSpotTradingAllowed: bool = False
    isMarginTradingAllowed: bool = False
    takerCommission: str | None = None   # decimal string, e.g. "0.0005"


class MexcExchangeInfo(msgspec.Struct, omit_defaults=True):
    """Top-level response from GET /api/v3/exchangeInfo."""

    symbols: list[MexcSymbol]
    timezone: str = "UTC"
    serverTime: int | None = None
