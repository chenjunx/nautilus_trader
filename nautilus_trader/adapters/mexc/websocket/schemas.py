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


class MexcWsMessage(msgspec.Struct, omit_defaults=True):
    """
    MEXC WebSocket control-plane frame (JSON text).

    Market data pushes for ``*.pb`` channels arrive as protobuf binary frames
    and are decoded separately, see ``nautilus_trader.adapters.mexc.websocket.protobuf``.

    Fields
    ------
    id : int, optional
        Request id echoed back for subscribe/unsubscribe acknowledgements.
    code : int, optional
        Result code, 0 on success.
    msg : str, optional
        Human-readable result message, e.g. subscription confirmation or error reason.
    method : str, optional
        Server-initiated control message, e.g. "PING".

    """

    id: int | None = None
    code: int | None = None
    msg: str | None = None
    method: str | None = None   # 服务端控制消息字段，如 "PING"
