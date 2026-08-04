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


class GateIoFuturesContract(msgspec.Struct, omit_defaults=True):
    """Gate.io futures contract info from GET /futures/{settle}/contracts."""

    name: str                          # e.g. "BTC_USDT"
    type: str = "direct"               # "direct" (linear) | "inverse"
    quanto_multiplier: str = "1"       # contract multiplier, base-currency amount per 1 contract
    leverage_min: str = "1"
    leverage_max: str = "100"
    maintenance_rate: str = "0.005"
    order_price_round: str = "0.01"    # price tick size
    order_size_min: int = 1            # min order size in contracts
    order_size_max: int = 1000000      # max order size in contracts
    maker_fee_rate: str = "0.0002"
    taker_fee_rate: str = "0.0005"
    in_delisting: bool = False
