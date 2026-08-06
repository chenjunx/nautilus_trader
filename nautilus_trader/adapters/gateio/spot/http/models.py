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


class GateIoCurrencyPair(msgspec.Struct, omit_defaults=True):
    """Gate.io spot currency pair info from GET /spot/currency_pairs."""

    id: str                         # e.g. "BTC_USDT"
    base: str                       # e.g. "BTC"
    quote: str                      # e.g. "USDT"
    fee: str                        # taker fee as percentage string, e.g. "0.2" means 0.2%
    precision: int = 6              # price decimal places
    amount_precision: int = 6       # amount decimal places
    trade_status: str = "tradable"  # "tradable" | "untradable"
    min_quote_amount: str | None = None
    min_base_amount: str | None = None
    amount_precision_step: str | None = None


class GateIoOrder(msgspec.Struct, omit_defaults=True):
    """Gate.io spot order, from POST/GET/DELETE /spot/orders (and spot.orders push)."""

    id: str
    currency_pair: str = ""
    status: str = "open"       # "open" | "closed" | "cancelled"
    type: str = "limit"        # "limit" | "market"
    account: str = "spot"
    side: str = "buy"          # "buy" | "sell"
    amount: str = "0"
    price: str = "0"
    time_in_force: str = "gtc"  # "gtc" | "ioc" | "poc" | "fok"
    left: str = "0"
    filled_total: str = "0"
    avg_deal_price: str | None = None
    fee: str = "0"
    fee_currency: str | None = None
    text: str | None = None
    create_time: str | None = None
    update_time: str | None = None
    create_time_ms: int | None = None
    update_time_ms: int | None = None
    finish_as: str | None = None  # "open" | "filled" | "cancelled" | "ioc" | "stp"
    succeeded: bool | None = None  # only present in cancel_batch_orders responses
    message: str | None = None     # error message when succeeded=False


class GateIoTrade(msgspec.Struct, omit_defaults=True):
    """Gate.io spot trade fill, from GET /spot/my_trades (and spot.usertrades push)."""

    id: str
    order_id: str = ""
    currency_pair: str = ""
    side: str = "buy"
    role: str = "taker"  # "taker" | "maker"
    amount: str = "0"
    price: str = "0"
    fee: str = "0"
    fee_currency: str | None = None
    create_time: str | None = None
    create_time_ms: int | None = None
    text: str | None = None


class GateIoBalance(msgspec.Struct, omit_defaults=True):
    """Gate.io spot account balance, from GET /spot/accounts (and spot.balances push)."""

    currency: str
    available: str = "0"
    locked: str = "0"


class GateIoOpenOrders(msgspec.Struct, omit_defaults=True):
    """Gate.io open orders for a single currency pair, from GET /spot/open_orders."""

    currency_pair: str
    total: int = 0
    orders: list[GateIoOrder] = []
