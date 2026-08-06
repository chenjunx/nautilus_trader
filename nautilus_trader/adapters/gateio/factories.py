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

import asyncio

from nautilus_trader.adapters.gateio.common.enums import GateIoAccountType
from nautilus_trader.adapters.gateio.config import GateIoDataClientConfig
from nautilus_trader.adapters.gateio.config import GateIoExecClientConfig
from nautilus_trader.adapters.gateio.futures.data import GateIoFuturesDataClient
from nautilus_trader.adapters.gateio.futures.execution import GateIoFuturesExecutionClient
from nautilus_trader.adapters.gateio.spot.data import GateIoSpotDataClient
from nautilus_trader.adapters.gateio.spot.execution import GateIoSpotExecutionClient
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory


class GateIoLiveDataClientFactory(LiveDataClientFactory):
    """Provides a Gate.io live data client factory."""

    @staticmethod
    def create(  # type: ignore
        loop: asyncio.AbstractEventLoop,
        name: str | None,
        config: GateIoDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> GateIoSpotDataClient | GateIoFuturesDataClient:
        if config.account_type == GateIoAccountType.LINEAR:
            return GateIoFuturesDataClient(
                loop=loop,
                msgbus=msgbus,
                cache=cache,
                clock=clock,
                config=config,
                name=name,
            )
        return GateIoSpotDataClient(
            loop=loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
            name=name,
        )


class GateIoLiveExecClientFactory(LiveExecClientFactory):
    """Provides a Gate.io live execution client factory."""

    @staticmethod
    def create(  # type: ignore
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: GateIoExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> GateIoSpotExecutionClient | GateIoFuturesExecutionClient:
        if config.account_type == GateIoAccountType.LINEAR:
            return GateIoFuturesExecutionClient(
                loop=loop,
                msgbus=msgbus,
                cache=cache,
                clock=clock,
                config=config,
                name=name,
            )
        return GateIoSpotExecutionClient(
            loop=loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
            name=name,
        )
