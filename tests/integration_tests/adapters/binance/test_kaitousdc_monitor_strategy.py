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
STRATEGY_PATH = Path("nautilus_trader/examples/strategies/kaitousdc_monitor.py")
RUNNER_PATH = Path("examples/live/binance/binance_kaitousdc_monitor.py")


def test_kaitousdc_monitor_config_defaults_to_data_only_subscriptions() -> None:
    from nautilus_trader.examples.strategies.kaitousdc_monitor import KaitousdcMonitorConfig

    config = KaitousdcMonitorConfig(instrument_id=INSTRUMENT_ID)

    assert config.instrument_id == INSTRUMENT_ID
    assert config.bar_type is None
    assert config.subscribe_quotes is True
    assert config.subscribe_trades is True
    assert config.subscribe_bars is True
    assert config.subscribe_book_snapshots is False
    assert config.book_interval_ms == 1000
    assert config.sample_mid is True
    assert config.mid_sample_interval_secs == 2.0
    assert config.mid_sample_history_size == 2
    assert config.calculate_ewma_sigma is True
    assert config.ewma_lambda == 0.94


def test_kaitousdc_monitor_config_accepts_explicit_bar_type() -> None:
    from nautilus_trader.examples.strategies.kaitousdc_monitor import KaitousdcMonitorConfig

    config = KaitousdcMonitorConfig(
        instrument_id=INSTRUMENT_ID,
        bar_type=BAR_TYPE,
    )

    assert config.bar_type == BAR_TYPE


def test_kaitousdc_monitor_strategy_initializes_counters() -> None:
    from nautilus_trader.examples.strategies.kaitousdc_monitor import KaitousdcMonitorConfig
    from nautilus_trader.examples.strategies.kaitousdc_monitor import KaitousdcMonitorStrategy

    strategy = KaitousdcMonitorStrategy(
        config=KaitousdcMonitorConfig(
            instrument_id=INSTRUMENT_ID,
            bar_type=BAR_TYPE,
        ),
    )

    assert strategy.instrument is None
    assert strategy._quote_count == 0
    assert strategy._trade_count == 0
    assert strategy._bar_count == 0
    assert strategy._book_count == 0
    assert strategy._last_quote is None
    assert strategy._mid_sample_count == 0
    assert strategy._mid_samples.maxlen == 2
    assert strategy._last_mid is None
    assert strategy._ewma_delta_s_var is None
    assert strategy._ewma_sigma is None
    assert strategy._ewma_delta_s_count == 0


def test_kaitousdc_monitor_strategy_defines_mid_sampling_timer() -> None:
    content = STRATEGY_PATH.read_text(encoding="utf-8")

    assert "MidPriceSample" in content
    assert "MID_SAMPLE_TIMER_NAME" in content
    assert 'MID_SAMPLE_TIMER_NAME = "mid_price_sample"' in content
    assert "clock.set_timer" in content
    assert "pd.Timedelta(seconds=self.config.mid_sample_interval_secs)" in content
    assert "def on_timer" in content


def test_kaitousdc_monitor_strategy_defines_ewma_sigma_fields() -> None:
    content = STRATEGY_PATH.read_text(encoding="utf-8")

    assert "calculate_ewma_sigma" in content
    assert "ewma_lambda" in content
    assert "_last_mid" in content
    assert "_ewma_delta_s_var" in content
    assert "_ewma_sigma" in content
    assert "_ewma_delta_s_count" in content
    assert "delta_s" in content
    assert "ewma_delta_s_var" in content
    assert "ewma_sigma" in content


def test_kaitousdc_monitor_modules_import() -> None:
    importlib.import_module("nautilus_trader.examples.strategies.kaitousdc_monitor")
    importlib.import_module("examples.live.binance.binance_kaitousdc_monitor")


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
    assert "KaitousdcMonitorStrategy" in content
    assert "exec_clients" not in content
    assert "BinanceExecClientConfig" not in content
    assert "BinanceLiveExecClientFactory" not in content


def test_kaitousdc_monitor_strategy_contains_no_trading_actions() -> None:
    content = STRATEGY_PATH.read_text(encoding="utf-8")

    forbidden = [
        "submit_order",
        "submit_order_list",
        "cancel_order",
        "cancel_all_orders",
        "close_position",
        "close_all_positions",
    ]
    for token in forbidden:
        assert token not in content
