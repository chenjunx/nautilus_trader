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
