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

from nautilus_trader.adapters.bitfinex.constants import BitfinexInstrumentType
from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.config import LiveExecClientConfig
from nautilus_trader.config import PositiveInt


class BitfinexDataClientConfig(LiveDataClientConfig, frozen=True):
    """
    Configuration for the Bitfinex data client.

    Parameters
    ----------
    base_url_http : str, optional
        Override the default HTTP base URL.
    base_url_ws : str, optional
        Override the default WebSocket URL.
    instrument_types : tuple[BitfinexInstrumentType, ...], default (BitfinexInstrumentType.SPOT,)
        The instrument types to load and subscribe. Include `BitfinexInstrumentType.PERPETUAL`
        to also load USDT-margined perpetual futures.

    """

    base_url_http: str | None = None
    base_url_ws: str | None = None
    instrument_types: tuple[BitfinexInstrumentType, ...] = (BitfinexInstrumentType.SPOT,)


class BitfinexExecClientConfig(LiveExecClientConfig, frozen=True):
    """
    Configuration for the Bitfinex execution client.

    Parameters
    ----------
    api_key : str, optional
        The Bitfinex API key. If ``None`` then will source the `BITFINEX_API_KEY` env var.
    api_secret : str, optional
        The Bitfinex API secret. If ``None`` then will source the `BITFINEX_API_SECRET` env var.
    instrument_types : tuple[BitfinexInstrumentType, ...], default (SPOT, PERPETUAL)
        The instrument types to load and trade.
    base_url_http : str, optional
        Override the default authenticated HTTP base URL.
    base_url_ws : str, optional
        Override the default authenticated WebSocket URL.
    ratelimiter_default_quota_per_second : PositiveInt, default 10
        The default rate limit quota (requests per second) applied to all HTTP requests.

    """

    api_key: str | None = None
    api_secret: str | None = None
    instrument_types: tuple[BitfinexInstrumentType, ...] = (
        BitfinexInstrumentType.SPOT,
        BitfinexInstrumentType.PERPETUAL,
    )
    base_url_http: str | None = None
    base_url_ws: str | None = None
    ratelimiter_default_quota_per_second: PositiveInt = 10
