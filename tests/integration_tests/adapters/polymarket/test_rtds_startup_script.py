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

from pathlib import Path


def test_rtds_startup_script_configures_btcusd_and_data_output() -> None:
    script = Path("scripts/run_polymarket_rtds_btcusd_collector.sh")

    content = script.read_text(encoding="utf-8")

    assert 'POLYMARKET_RTDS_SYMBOLS="btcusd"' in content
    assert 'POLYMARKET_RTDS_OUTPUT_DIR="/home/admin/data/polymarket/rtds"' in content
    assert "polymarket_rtds_crypto_price_collector.py" in content
    assert "uv run --no-sync python" in content
