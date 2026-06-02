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

import json
from pathlib import Path

from examples.live.polymarket.polymarket_rtds_crypto_price_collector import (
    build_subscription_message,
)
from examples.live.polymarket.polymarket_rtds_crypto_price_collector import extract_symbol
from examples.live.polymarket.polymarket_rtds_crypto_price_collector import output_path_for
from examples.live.polymarket.polymarket_rtds_crypto_price_collector import parse_symbols
from examples.live.polymarket.polymarket_rtds_crypto_price_collector import write_message


def test_parse_symbols_defaults_to_btcusd() -> None:
    assert parse_symbols(None) == ["btcusd"]
    assert parse_symbols("") == ["btcusd"]


def test_parse_symbols_normalizes_comma_separated_values() -> None:
    assert parse_symbols(" BTCUSD, ethusdt ,btc-usd ") == ["btcusd", "ethusdt", "btc-usd"]


def test_build_subscription_message_uses_crypto_prices_update_filters() -> None:
    msg = build_subscription_message(["btcusd", "ethusdt"])

    assert msg == {
        "action": "subscribe",
        "subscriptions": [
            {"topic": "crypto_prices", "type": "update", "filters": "btcusd"},
            {"topic": "crypto_prices", "type": "update", "filters": "ethusdt"},
        ],
    }


def test_extract_symbol_uses_symbol_field_when_present() -> None:
    assert extract_symbol({"symbol": "BTCUSD", "price": "100000"}, ["btcusd"]) == "btcusd"


def test_extract_symbol_falls_back_to_single_subscription() -> None:
    assert extract_symbol({"price": "100000"}, ["btcusd"]) == "btcusd"


def test_extract_symbol_falls_back_to_unknown_for_ambiguous_messages() -> None:
    assert extract_symbol({"price": "100000"}, ["btcusd", "ethusdt"]) == "unknown"


def test_output_path_partitions_by_topic_symbol_and_date(tmp_path: Path) -> None:
    path = output_path_for(
        output_dir=tmp_path,
        symbol="btcusd",
        ts_recv_ns=1_787_817_600_000_000_000,
    )

    assert path == tmp_path / "crypto_prices" / "btcusd" / "2026-08-27.jsonl"


def test_write_message_appends_jsonl_with_receive_timestamp(tmp_path: Path) -> None:
    msg = {"symbol": "BTCUSD", "price": "100000"}

    path = write_message(
        output_dir=tmp_path,
        symbols=["btcusd"],
        msg=msg,
        ts_recv_ns=1_787_817_600_000_000_000,
    )

    assert path == tmp_path / "crypto_prices" / "btcusd" / "2026-08-27.jsonl"
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record == {
        "ts_recv_ns": 1_787_817_600_000_000_000,
        "symbol": "btcusd",
        "raw": msg,
    }
