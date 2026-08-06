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

"""
Bitfinex HTTP response models.

The /conf/pub:list:pair:exchange endpoint returns a nested list:
  [["BTCUSD", "LTCUSD", ...]]

We use a plain msgspec list decoder — no Struct needed.

The authenticated trading/account endpoints return **positional JSON arrays** rather than
objects, so the structs below use `array_like=True`: msgspec maps array elements to fields
by position, and any extra trailing array elements (fields present in Bitfinex's API but not
declared here) are silently ignored.
"""

import msgspec


# Decoder for the outer wrapper: list[list[str]]
pair_list_decoder = msgspec.json.Decoder(list)


class BitfinexOrder(msgspec.Struct, array_like=True):
    """Bitfinex order, from the auth/w/order/* and auth/r/orders* endpoints (and `on`/`ou`/`oc` WS push)."""

    id: int
    gid: int | None
    cid: int
    symbol: str
    mts_create: int
    mts_update: int
    amount: float
    amount_orig: float
    order_type: str
    type_prev: str | None
    mts_tif: int | None
    placeholder_1: object = None
    flags: int | None = None
    status: str = ""
    placeholder_2: object = None
    placeholder_3: object = None
    price: float = 0.0
    price_avg: float = 0.0
    price_trailing: float | None = None
    price_aux_limit: float | None = None


class BitfinexTrade(msgspec.Struct, array_like=True):
    """Bitfinex trade fill, from auth/r/trades* endpoints (and `te`/`tu` WS push)."""

    id: int
    symbol: str
    mts_create: int
    order_id: int
    exec_amount: float
    exec_price: float
    order_type: str | None = None
    order_price: float | None = None
    maker: int | None = None
    fee: float | None = None
    fee_currency: str | None = None
    cid: int | None = None


class BitfinexWallet(msgspec.Struct, array_like=True):
    """Bitfinex wallet balance, from auth/r/wallets (and `ws`/`wu` WS push)."""

    wallet_type: str
    currency: str
    balance: float
    unsettled_interest: float
    balance_available: float | None = None


class BitfinexPosition(msgspec.Struct, array_like=True):
    """Bitfinex position, from auth/r/positions (and `ps`/`pn`/`pu`/`pc` WS push)."""

    symbol: str
    status: str
    amount: float
    base_price: float
    margin_funding: float
    margin_funding_type: int
    pl: float
    pl_perc: float
    price_liq: float
    leverage: float
    flags: int | None = None
    position_id: int | None = None
    mts_create: int | None = None
    mts_update: int | None = None
