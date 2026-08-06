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


class GateIoFuturesOrder(msgspec.Struct, omit_defaults=True):
    """Gate.io futures order, from POST/GET/DELETE /futures/{settle}/orders (and futures.orders push)."""

    id: int | None = None
    contract: str = ""
    size: int = 0                # signed contracts: positive=buy/long, negative=sell/short
    left: int = 0                # signed remaining contracts
    price: str = "0"             # "0" together with tif="ioc" means a market order
    is_reduce_only: bool = False
    is_close: bool = False
    is_liq: bool = False
    tif: str = "gtc"             # "gtc" | "ioc" | "poc" | "fok"
    status: str = "open"         # "open" | "finished"
    finish_as: str | None = None  # "filled" | "cancelled" | "liquidated" | "ioc" | "stp" | ...
    fill_price: str | None = None
    text: str | None = None
    create_time: float | None = None
    update_time: float | None = None
    finish_time: float | None = None


class GateIoFuturesTrade(msgspec.Struct, omit_defaults=True):
    """Gate.io futures trade fill, from GET /futures/{settle}/my_trades (and futures.usertrades push)."""

    id: str
    order_id: str = ""
    contract: str = ""
    size: int = 0                # signed contracts
    price: str = "0"
    role: str = "taker"          # "taker" | "maker"
    text: str | None = None
    create_time: float | None = None


class GateIoFuturesPosition(msgspec.Struct, omit_defaults=True):
    """Gate.io futures position, from GET /futures/{settle}/positions (and futures.positions push)."""

    contract: str = ""
    size: int = 0                # signed contracts: positive=long, negative=short, 0=flat
    entry_price: str = "0"
    mark_price: str | None = None
    leverage: str | None = None
    mode: str = "single"         # "single" | "dual_long" | "dual_short"
    unrealised_pnl: str | None = None
    realised_pnl: str | None = None
    update_time: float | None = None


class GateIoFuturesAccount(msgspec.Struct, omit_defaults=True):
    """Gate.io futures account balance, from GET /futures/{settle}/accounts."""

    currency: str = "USDT"
    total: str = "0"
    available: str = "0"
    position_margin: str = "0"
    order_margin: str = "0"
    unrealised_pnl: str | None = None


class GateIoAccountDetail(msgspec.Struct, omit_defaults=True):
    """
    Gate.io unified account details, from GET /account/detail.

    Used to resolve the numeric account `user_id` required by the futures private
    WebSocket subscription payload (`[user_id, contract]`); the endpoint is not
    settle- or product-specific.
    """

    user_id: int | None = None
    tier: int | None = None
