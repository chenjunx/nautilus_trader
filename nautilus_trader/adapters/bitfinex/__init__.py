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

"""Bitfinex adapter for nautilus_trader — data-only (spot market data)."""

from nautilus_trader.adapters.bitfinex.config import BitfinexDataClientConfig
from nautilus_trader.adapters.bitfinex.constants import BITFINEX
from nautilus_trader.adapters.bitfinex.constants import BITFINEX_CLIENT_ID
from nautilus_trader.adapters.bitfinex.constants import BITFINEX_VENUE
from nautilus_trader.adapters.bitfinex.data import BitfinexDataClient
from nautilus_trader.adapters.bitfinex.factories import BitfinexLiveDataClientFactory
from nautilus_trader.adapters.bitfinex.providers import BitfinexInstrumentProvider


__all__ = [
    "BITFINEX",
    "BITFINEX_CLIENT_ID",
    "BITFINEX_VENUE",
    "BitfinexDataClient",
    "BitfinexDataClientConfig",
    "BitfinexInstrumentProvider",
    "BitfinexLiveDataClientFactory",
]
