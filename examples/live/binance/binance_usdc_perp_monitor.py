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
Run a data-only monitor for multiple Binance USDT-M perpetual futures.

Monitored symbols: KAITOUSDC, SOLUSDC, ETHUSDC, NEARUSDC, BTCUSDC.
No execution client — cannot submit orders.

"""

from nautilus_trader.adapters.binance import BINANCE
from nautilus_trader.adapters.binance import BinanceAccountType
from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceLiveDataClientFactory
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.examples.strategies.glft_market_maker import GLFTMarketMaker
from nautilus_trader.examples.strategies.glft_market_maker import GLFTMarketMakerConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId


# Per-symbol GLFT parameters fitted from ~4.8 h of live LOB + trade data (2026-06-28).
# quote_arrival_a (A) and quote_intensity_k (k) follow λ(δ) = A·exp(−k·δ).
# KAITOUSDC and BTCUSDC have no fitted data yet; defaults (A=1, k=1831) are placeholders.
GLFT_PARAMS: dict[str, dict] = {
    "KAITOUSDC-PERP": {"quote_arrival_a": 1.0,     "quote_intensity_k": 1831.0},
    "SOLUSDC-PERP":   {"quote_arrival_a": 34.33,   "quote_intensity_k": 111.70},
    "ETHUSDC-PERP":   {"quote_arrival_a": 43.59,   "quote_intensity_k": 15.81},
    "NEARUSDC-PERP":  {"quote_arrival_a": 15.31,   "quote_intensity_k": 480.51},
    "BTCUSDC-PERP":   {"quote_arrival_a": 1.0,     "quote_intensity_k": 1831.0},
}

SYMBOLS = list(GLFT_PARAMS)

instrument_ids = [InstrumentId.from_str(f"{s}.BINANCE") for s in SYMBOLS]

# Configure the trading node for public market data only.
config_node = TradingNodeConfig(
    trader_id=TraderId("MULTI-MONITOR-001"),
    logging=LoggingConfig(log_level="INFO", use_pyo3=True),
    data_clients={
        BINANCE: BinanceDataClientConfig(
            environment=BinanceEnvironment.LIVE,
            account_type=BinanceAccountType.USDT_FUTURES,
            instrument_provider=InstrumentProviderConfig(
                load_ids=frozenset(instrument_ids),
            ),
        ),
    },
    timeout_connection=20.0,
    timeout_disconnection=10.0,
    timeout_post_stop=1.0,
)

# Instantiate the node.
node = TradingNode(config=config_node)

# Add one monitor strategy per symbol.
for instrument_id in instrument_ids:
    sym_key = instrument_id.symbol.value          # e.g. "ETHUSDC-PERP"
    sym_dir = sym_key.replace("-PERP", "").lower()  # e.g. "ethusdc"
    bar_type = BarType.from_str(f"{instrument_id}-1-MINUTE-LAST-EXTERNAL")
    params = GLFT_PARAMS[sym_key]
    node.trader.add_strategy(
        GLFTMarketMaker(
            config=GLFTMarketMakerConfig(
                instrument_id=instrument_id,
                bar_type=bar_type,
                subscribe_book_deltas=True,
                book_depth=100,
                persist_market_data=True,
                catalog_path=f"data/{sym_dir}/catalog",
                flush_interval_secs=5.0,
                max_buffer_size=10_000,
                quote_arrival_a=params["quote_arrival_a"],
                quote_intensity_k=params["quote_intensity_k"],
            ),
        )
    )

# Register the Binance data client factory.
node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)
node.build()


# Run the node. Stop with SIGINT/CTRL+C.
if __name__ == "__main__":
    try:
        node.run()
    finally:
        node.dispose()
