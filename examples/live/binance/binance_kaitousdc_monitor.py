#!/usr/bin/env python3
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
Run a data-only monitor for Binance ``KAITOUSDC-PERP`` live market data.

This example does not configure execution clients and cannot submit orders.

"""

from nautilus_trader.adapters.binance import BINANCE
from nautilus_trader.adapters.binance import BinanceAccountType
from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceLiveDataClientFactory
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.examples.strategies.kaitousdc_monitor import KaitousdcMonitorConfig
from nautilus_trader.examples.strategies.kaitousdc_monitor import KaitousdcMonitorStrategy
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId


symbol = "KAITOUSDC-PERP"
instrument_id = InstrumentId.from_str(f"{symbol}.BINANCE")
bar_type = BarType.from_str(f"{instrument_id}-1-MINUTE-LAST-EXTERNAL")

# Configure the trading node for public market data only.
config_node = TradingNodeConfig(
    trader_id=TraderId("KAITOUSDC-MONITOR-001"),
    logging=LoggingConfig(log_level="INFO", use_pyo3=True),
    data_clients={
        BINANCE: BinanceDataClientConfig(
            environment=BinanceEnvironment.LIVE,
            account_type=BinanceAccountType.USDT_FUTURES,
            instrument_provider=InstrumentProviderConfig(load_ids=frozenset([instrument_id])),
        ),
    },
    timeout_connection=20.0,
    timeout_disconnection=10.0,
    timeout_post_stop=1.0,
)

# Instantiate the node.
node = TradingNode(config=config_node)

# Configure and add the monitor strategy.
strategy = KaitousdcMonitorStrategy(
    config=KaitousdcMonitorConfig(
        instrument_id=instrument_id,
        bar_type=bar_type,
    ),
)
node.trader.add_strategy(strategy)

# Register the Binance data client factory.
node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)
node.build()


# Run the node. Stop with SIGINT/CTRL+C.
if __name__ == "__main__":
    try:
        node.run()
    finally:
        node.dispose()
