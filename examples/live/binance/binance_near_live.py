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
Run a LIVE post-only GLFT market maker for Binance ``NEARUSDT-PERP`` (USDT futures).

REAL MONEY. This configures an execution client and the strategy will submit
real post-only orders when ``enable_trading=True``.

Operator prerequisites
----------------------
* API key/secret are read from the ``BINANCE_API_KEY`` / ``BINANCE_API_SECRET``
  environment variables (never hard-code them here). The key needs USDT-M
  Futures trading permission and (recommended) IP binding.
* Account MUST be in **one-way** position mode. Under Hedge mode
  ``close_all_positions(reduce_only=True)`` fails on connect.
* Leverage is set to 1x via ``futures_leverages``; verify it in the Binance UI
  on first run.
* trade_size (3) and max_position (30) are resolved from ``instrument_trade_sizes``
  and ``instrument_max_positions`` in ``GLFTMarketMakerConfig``.

Recommended first run: set ``enable_trading=False`` and confirm the data +
account connect and EWMA/reservation logs flow with NO orders, then flip to
``True``.
"""

import os

from dotenv import load_dotenv

load_dotenv()

from nautilus_trader.adapters.binance import BINANCE
from nautilus_trader.adapters.binance import BinanceAccountType
from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceExecClientConfig
from nautilus_trader.adapters.binance import BinanceInstrumentProviderConfig
from nautilus_trader.adapters.binance import BinanceLiveDataClientFactory
from nautilus_trader.adapters.binance import BinanceLiveExecClientFactory
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.config import CacheConfig
from nautilus_trader.config import DatabaseConfig
from nautilus_trader.config import LiveDataEngineConfig
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.examples.strategies.glft_market_maker import GLFTMarketMaker
from nautilus_trader.examples.strategies.glft_market_maker import GLFTMarketMakerConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId


symbol = "NEARUSDC-PERP"
instrument_id = InstrumentId.from_str(f"{symbol}.BINANCE")
bar_type = BarType.from_str(f"{instrument_id}-1-MINUTE-LAST-EXTERNAL")

# Configure the trading node for live USDT futures with an execution client.
config_node = TradingNodeConfig(
    trader_id=TraderId("NEAR-LIVE-MM-001"),
    logging=LoggingConfig(log_level="INFO", use_pyo3=True),
    data_engine=LiveDataEngineConfig(
        external_clients=[ClientId(BINANCE)],
    ),
    exec_engine=LiveExecEngineConfig(
        reconciliation=True,
        open_check_interval_secs=5.0,
        open_check_open_only=False,
        graceful_shutdown_on_exception=True,
    ),
    cache=CacheConfig(
        database=DatabaseConfig(
            type="redis",
            host="localhost",
            port=6379,
            password=os.environ.get("REDIS_PASSWORD"),
        ),
        timestamps_as_iso8601=True,
        flush_on_start=False,
    ),
    data_clients={
        BINANCE: BinanceDataClientConfig(
            environment=BinanceEnvironment.LIVE,
            account_type=BinanceAccountType.USDT_FUTURES,
            instrument_provider=BinanceInstrumentProviderConfig(
                load_ids=frozenset([instrument_id]),
            ),
        ),
    },
    exec_clients={
        BINANCE: BinanceExecClientConfig(
            environment=BinanceEnvironment.LIVE,
            account_type=BinanceAccountType.USDT_FUTURES,
            instrument_provider=BinanceInstrumentProviderConfig(
                load_ids=frozenset([instrument_id]),
                query_commission_rates=True,
            ),
            max_retries=3,
            log_rejected_due_post_only_as_warning=False,
            use_reduce_only=True,
        ),
    },
    timeout_connection=30.0,
    timeout_reconciliation=10.0,
    timeout_portfolio=10.0,
    timeout_disconnection=10.0,
    timeout_post_stop=5.0,
)

# Instantiate the node.
node = TradingNode(config=config_node)

# Configure and add the live market-maker strategy.
strategy = GLFTMarketMaker(
    config=GLFTMarketMakerConfig(
        instrument_id=instrument_id,
        quote_intensity_k=1717.0,   # fitted from 24.6h NEARUSDC-PERP history (2026-06-28/29)
        quote_arrival_a=0.293,      # same fit; config k=480/A=15 was ~50x off on fill rate
        bar_type=bar_type,
        subscribe_book_deltas=True,
        book_depth=100,
        # Re-quote on a tighter cadence than the 2.0s monitor default to limit
        # stale-quote exposure (driven by the mid-sample timer).
        mid_sample_interval_secs=2.0,
        persist_market_data=True,
        catalog_path="data/near/catalog",
        flush_interval_secs=5.0,
        max_buffer_size=10_000,
        # Online parameter adaptation via Recursive Least Squares.
        # Starts from the fitted k/A above and tracks daily changes.
        enable_rls_fitting=True,
        rls_forgetting=0.95,          # effective memory ~800s (~4 windows)
        rls_update_interval_secs=200.0,
        # Live trading opt-ins:
        enable_trading=True,
        external_order_claims=[instrument_id],
    ),
)
node.trader.add_strategy(strategy)

# Register the Binance data and execution client factories.
node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)
node.add_exec_client_factory(BINANCE, BinanceLiveExecClientFactory)
node.build()


# Run the node. Stop with SIGINT/CTRL+C.
if __name__ == "__main__":
    try:
        node.run()
    finally:
        node.dispose()
