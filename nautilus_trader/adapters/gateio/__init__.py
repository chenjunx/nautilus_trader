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

"""Gate.io adapter for nautilus_trader — spot and USDT-margined linear futures, data and execution."""

from nautilus_trader.adapters.gateio.common.constants import GATEIO
from nautilus_trader.adapters.gateio.common.constants import GATEIO_CLIENT_ID
from nautilus_trader.adapters.gateio.common.constants import GATEIO_VENUE
from nautilus_trader.adapters.gateio.common.enums import GateIoAccountType
from nautilus_trader.adapters.gateio.config import GateIoDataClientConfig
from nautilus_trader.adapters.gateio.config import GateIoExecClientConfig
from nautilus_trader.adapters.gateio.factories import GateIoLiveDataClientFactory
from nautilus_trader.adapters.gateio.factories import GateIoLiveExecClientFactory
from nautilus_trader.adapters.gateio.futures.data import GateIoFuturesDataClient
from nautilus_trader.adapters.gateio.futures.execution import GateIoFuturesExecutionClient
from nautilus_trader.adapters.gateio.futures.providers import GateIoFuturesInstrumentProvider
from nautilus_trader.adapters.gateio.spot.data import GateIoSpotDataClient
from nautilus_trader.adapters.gateio.spot.execution import GateIoSpotExecutionClient
from nautilus_trader.adapters.gateio.spot.providers import GateIoSpotInstrumentProvider


__all__ = [
    "GATEIO",
    "GATEIO_CLIENT_ID",
    "GATEIO_VENUE",
    "GateIoAccountType",
    "GateIoDataClientConfig",
    "GateIoExecClientConfig",
    "GateIoFuturesDataClient",
    "GateIoFuturesExecutionClient",
    "GateIoFuturesInstrumentProvider",
    "GateIoLiveDataClientFactory",
    "GateIoLiveExecClientFactory",
    "GateIoSpotDataClient",
    "GateIoSpotExecutionClient",
    "GateIoSpotInstrumentProvider",
]
