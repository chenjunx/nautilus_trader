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
Bitfinex WebSocket message schemas.

Bitfinex WS messages come in two shapes:
- JSON object  (first byte ``{``): control events — info / subscribed / unsubscribed / error
- JSON array   (first byte ``[``): ticker data or heartbeat

This module defines the Struct used to decode the object-shaped control messages.
"""

from typing import Any

import msgspec


class BitfinexEventMessage(msgspec.Struct, omit_defaults=True):
    """
    Bitfinex WebSocket JSON-object control message.

    Covers the following ``event`` values:
    - ``"info"``         — server info on connect (version, serverId, platform)
    - ``"subscribed"``   — channel subscription confirmation
    - ``"unsubscribed"`` — channel unsubscription confirmation
    - ``"error"``        — subscription or protocol error

    Fields
    ------
    event : str
        The event type.
    chanId : int, optional
        Channel ID assigned by the server (present in subscribed / unsubscribed / error).
    symbol : str, optional
        Symbol including ``t`` prefix, e.g. ``"tBTCUSD"`` (present in subscribed).
    pair : str, optional
        Symbol without ``t`` prefix, e.g. ``"BTCUSD"`` (present in subscribed).
    channel : str, optional
        Channel name, e.g. ``"ticker"`` (present in subscribed).
    msg : str, optional
        Human-readable error description (present in error).
    code : int, optional
        Machine-readable error code (present in error).

    """

    event: str
    chanId: int | None = None
    symbol: str | None = None
    pair: str | None = None
    channel: str | None = None
    msg: str | None = None
    code: int | None = None
    # 'version', 'serverId', 'platform', and other info fields are ignored via omit_defaults
    version: Any = None
    serverId: Any = None
    platform: Any = None
