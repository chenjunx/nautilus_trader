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

import importlib
from pathlib import Path

from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId


INSTRUMENT_ID = InstrumentId.from_str("KAITOUSDC-PERP.BINANCE")
BAR_TYPE = BarType.from_str("KAITOUSDC-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL")
STRATEGY_PATH = Path("nautilus_trader/examples/strategies/glft_market_maker.py")
RUNNER_PATH = Path("examples/live/binance/binance_usdc_perp_monitor.py")
LIVE_RUNNER_PATH = Path("examples/live/binance/binance_kaitousdc_live.py")


def test_kaitousdc_monitor_config_defaults_to_data_only_subscriptions() -> None:
    from nautilus_trader.examples.strategies.glft_market_maker import GLFTMarketMakerConfig

    config = GLFTMarketMakerConfig(instrument_id=INSTRUMENT_ID)

    assert config.instrument_id == INSTRUMENT_ID
    assert config.bar_type is None
    assert config.subscribe_quotes is True
    assert config.subscribe_trades is True
    assert config.subscribe_bars is True
    assert config.subscribe_book_snapshots is False
    assert config.subscribe_book_deltas is True
    assert config.book_interval_ms == 1000
    assert config.book_depth == 100
    assert config.sample_mid is True
    assert config.mid_sample_interval_secs == 2.0
    assert config.mid_sample_history_size == 2
    assert config.calculate_ewma_variance is True
    assert config.ewma_lambda == 0.94
    assert config.gamma == 0.1
    assert config.reservation_price_min_q == 1
    assert config.reservation_price_max_q == 10
    assert config.quote_intensity_k == 1831.0
    assert config.max_position == 110
    assert config.enable_trading is False
    assert config.trade_size is None
    assert config.persist_market_data is True
    assert config.catalog_path == "data/kaitousdc/catalog"
    assert config.flush_interval_secs == 5.0
    assert config.max_buffer_size == 10_000


def test_kaitousdc_monitor_config_accepts_explicit_bar_type() -> None:
    from nautilus_trader.examples.strategies.glft_market_maker import GLFTMarketMakerConfig

    config = GLFTMarketMakerConfig(
        instrument_id=INSTRUMENT_ID,
        bar_type=BAR_TYPE,
    )

    assert config.bar_type == BAR_TYPE


def test_kaitousdc_monitor_strategy_initializes_counters() -> None:
    from nautilus_trader.examples.strategies.glft_market_maker import GLFTMarketMakerConfig
    from nautilus_trader.examples.strategies.glft_market_maker import GLFTMarketMaker

    strategy = GLFTMarketMaker(
        config=GLFTMarketMakerConfig(
            instrument_id=INSTRUMENT_ID,
            bar_type=BAR_TYPE,
        ),
    )

    assert strategy.instrument is None
    assert strategy._quote_count == 0
    assert strategy._trade_count == 0
    assert strategy._bar_count == 0
    assert strategy._book_count == 0
    assert strategy._book_delta_count == 0
    assert strategy._last_quote is None
    assert strategy._book is None
    assert strategy._catalog is None
    assert strategy._trade_buffer == []
    assert strategy._book_delta_buffer == []
    assert strategy._mid_sample_count == 0
    assert strategy._mid_samples.maxlen == 2
    assert strategy._last_mid is None
    assert strategy._ewma_delta_s_var is None
    assert strategy._ewma_delta_s_count == 0
    assert strategy._reservation_prices is None
    assert strategy._quote_spread is None
    assert strategy._quote_prices is None
    assert strategy._position == 0
    assert strategy._trade_size is None
    assert strategy._pending_self_cancels == set()


def test_kaitousdc_monitor_strategy_defines_mid_sampling_timer() -> None:
    content = STRATEGY_PATH.read_text(encoding="utf-8")

    assert "MidPriceSample" in content
    assert "MID_SAMPLE_TIMER_NAME" in content
    assert 'MID_SAMPLE_TIMER_NAME = "mid_price_sample"' in content
    assert "clock.set_timer" in content
    assert "pd.Timedelta(seconds=self.config.mid_sample_interval_secs)" in content
    assert "def on_timer" in content


def test_kaitousdc_monitor_strategy_defines_variance_reservation_and_quote_fields() -> None:
    content = STRATEGY_PATH.read_text(encoding="utf-8")

    assert "calculate_ewma_variance" in content
    assert "calculate_ewma_sigma" not in content
    assert "ewma_lambda" in content
    assert "_last_mid" in content
    assert "_ewma_delta_s_var" in content
    assert "_ewma_delta_s_count" in content
    assert "_ewma_sigma" not in content
    assert "ewma_sigma" not in content
    assert "gamma" in content
    assert "_calculate_g" in content
    assert "reservation_price_min_q" in content
    assert "reservation_price_max_q" in content
    assert "reservation_prices" in content
    assert "quote_intensity_k" in content
    assert "max_position" in content
    assert "enable_trading" in content
    assert "trade_size" in content
    assert "_position" in content
    assert "_update_position" in content
    assert "_reservation_price_for" in content
    assert "_requote_live" in content
    assert "_pending_self_cancels" in content
    assert "post_only=True" in content
    assert "_quote_spread" in content
    assert "_quote_prices" in content
    assert "quote_spread" in content
    assert "quote_prices" in content
    assert ".ln()" in content


def test_kaitousdc_monitor_strategy_defines_market_data_persistence() -> None:
    content = STRATEGY_PATH.read_text(encoding="utf-8")

    assert "ParquetDataCatalog" in content
    assert "persist_market_data" in content
    assert "catalog_path" in content
    assert "flush_interval_secs" in content
    assert "max_buffer_size" in content
    assert "subscribe_book_deltas" in content
    assert "subscribe_order_book_deltas" in content
    assert "def on_order_book_deltas" in content
    assert "_trade_buffer" in content
    assert "_book_delta_buffer" in content
    assert "_flush_market_data" in content
    assert "write_data" in content


def test_kaitousdc_monitor_modules_import() -> None:
    importlib.import_module("nautilus_trader.examples.strategies.glft_market_maker")
    importlib.import_module("examples.live.binance.binance_usdc_perp_monitor")
    importlib.import_module("examples.live.binance.binance_kaitousdc_live")


def test_kaitousdc_monitor_runner_targets_binance_usdt_futures_data_only() -> None:
    content = RUNNER_PATH.read_text(encoding="utf-8")

    assert 'symbol = "KAITOUSDC-PERP"' in content
    assert 'instrument_id = InstrumentId.from_str(f"{symbol}.BINANCE")' in content
    assert 'BarType.from_str(f"{instrument_id}-1-MINUTE-LAST-EXTERNAL")' in content
    assert "BinanceAccountType.USDT_FUTURES" in content
    assert "BinanceEnvironment.LIVE" in content
    assert "BinanceDataClientConfig" in content
    assert "InstrumentProviderConfig(load_ids=frozenset([instrument_id]))" in content
    assert "BinanceLiveDataClientFactory" in content
    assert "GLFTMarketMaker" in content
    assert "subscribe_book_deltas=True" in content
    assert "book_depth=100" in content
    assert "persist_market_data=True" in content
    assert 'catalog_path="data/kaitousdc/catalog"' in content
    assert "flush_interval_secs=5.0" in content
    assert "max_buffer_size=10_000" in content
    assert "exec_clients" not in content
    assert "BinanceExecClientConfig" not in content
    assert "BinanceLiveExecClientFactory" not in content


def test_kaitousdc_monitor_strategy_trading_is_gated_behind_enable_trading() -> None:
    # The strategy can now trade (post-only market maker), but every trading
    # action is gated behind `enable_trading`, which defaults to False so the
    # data-only runner and monitor-only usage stay inert.
    content = STRATEGY_PATH.read_text(encoding="utf-8")

    for token in ["submit_order", "cancel_all_orders", "close_all_positions"]:
        assert token in content
    assert "if self.config.enable_trading" in content
    assert "def _requote_live" in content


def test_kaitousdc_live_runner_configures_exec_client() -> None:
    content = LIVE_RUNNER_PATH.read_text(encoding="utf-8")

    assert 'symbol = "KAITOUSDC-PERP"' in content
    assert "BinanceAccountType.USDT_FUTURES" in content
    assert "BinanceEnvironment.LIVE" in content
    assert "BinanceExecClientConfig" in content
    assert "BinanceLiveExecClientFactory" in content
    assert "node.add_exec_client_factory(BINANCE, BinanceLiveExecClientFactory)" in content
    assert "futures_leverages" in content
    assert "use_reduce_only=True" in content
    # Live trading opt-ins on the strategy config
    assert "enable_trading=True" in content
    assert "external_order_claims=[instrument_id]" in content
