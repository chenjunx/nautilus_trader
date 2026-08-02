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
"""

import msgspec


# Decoder for the outer wrapper: list[list[str]]
pair_list_decoder = msgspec.json.Decoder(list)
