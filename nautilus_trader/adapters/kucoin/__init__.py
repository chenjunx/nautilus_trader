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

"""KuCoin adapter for nautilus_trader — data-only (spot market data)."""

from nautilus_trader.adapters.kucoin.config import KuCoinDataClientConfig
from nautilus_trader.adapters.kucoin.constants import KUCOIN
from nautilus_trader.adapters.kucoin.constants import KUCOIN_CLIENT_ID
from nautilus_trader.adapters.kucoin.constants import KUCOIN_VENUE
from nautilus_trader.adapters.kucoin.data import KuCoinDataClient
from nautilus_trader.adapters.kucoin.factories import KuCoinLiveDataClientFactory
from nautilus_trader.adapters.kucoin.providers import KuCoinInstrumentProvider


__all__ = [
    "KUCOIN",
    "KUCOIN_CLIENT_ID",
    "KUCOIN_VENUE",
    "KuCoinDataClient",
    "KuCoinDataClientConfig",
    "KuCoinInstrumentProvider",
    "KuCoinLiveDataClientFactory",
]
