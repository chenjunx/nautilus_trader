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

from nautilus_trader.adapters.gateio.common.enums import GateIoAccountType
from nautilus_trader.config import LiveDataClientConfig


class GateIoDataClientConfig(LiveDataClientConfig, frozen=True):
    """
    Configuration for the Gate.io data client.

    Parameters
    ----------
    account_type : GateIoAccountType, default GateIoAccountType.SPOT
        The account/product type for the client (spot or USDT-margined linear futures).
    settle : str, default 'usdt'
        The settlement asset for futures contracts. Only consulted when
        `account_type` is `GateIoAccountType.LINEAR`.
    base_url_http : str, optional
        Override the default HTTP base URL.
    base_url_ws : str, optional
        Override the default WebSocket URL.

    """

    account_type: GateIoAccountType = GateIoAccountType.SPOT
    settle: str = "usdt"
    base_url_http: str | None = None
    base_url_ws: str | None = None
