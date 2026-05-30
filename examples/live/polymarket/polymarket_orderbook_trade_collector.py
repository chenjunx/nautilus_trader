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

import asyncio
import os
from pathlib import Path

from nautilus_trader.adapters.polymarket.collector import PolymarketParquetCollector
from nautilus_trader.adapters.polymarket.collector import PolymarketParquetCollectorConfig
from nautilus_trader.adapters.polymarket.common.symbol import get_polymarket_instrument_id
from nautilus_trader.adapters.polymarket.factories import get_polymarket_http_client
from nautilus_trader.adapters.polymarket.providers import PolymarketInstrumentProvider
from nautilus_trader.adapters.polymarket.providers import PolymarketInstrumentProviderConfig
from nautilus_trader.common.component import LiveClock
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is required")
    return value


async def run_collector() -> None:
    condition_id = _required_env("POLYMARKET_CONDITION_ID")
    token_id = _required_env("POLYMARKET_TOKEN_ID")
    catalog_path = os.environ.get("POLYMARKET_CATALOG_PATH", "data/polymarket/catalog")
    flush_interval_secs = float(os.environ.get("POLYMARKET_FLUSH_INTERVAL_SECS", "5"))
    max_buffer_size = int(os.environ.get("POLYMARKET_MAX_BUFFER_SIZE", "10000"))
    ws_base_url = os.environ.get("POLYMARKET_WS_BASE_URL")
    proxy_url = os.environ.get("POLYMARKET_PROXY_URL")

    instrument_id = get_polymarket_instrument_id(condition_id, token_id)
    clock = LiveClock()
    client = get_polymarket_http_client()
    provider = PolymarketInstrumentProvider(
        client=client,
        clock=clock,
        config=PolymarketInstrumentProviderConfig(load_ids=frozenset([str(instrument_id)])),
    )
    await provider.load_async(instrument_id)
    instrument = provider.find(instrument_id)
    if instrument is None:
        raise RuntimeError(f"Unable to load Polymarket instrument {instrument_id}")

    catalog = ParquetDataCatalog(catalog_path)
    collector = PolymarketParquetCollector(
        config=PolymarketParquetCollectorConfig(
            catalog_path=catalog_path,
            flush_interval_secs=flush_interval_secs,
            max_buffer_size=max_buffer_size,
            ws_base_url=ws_base_url,
            proxy_url=proxy_url,
        ),
        instrument=instrument,
        catalog=catalog,
        clock=clock,
    )

    await collector.run(token_id)


def main() -> None:
    catalog_path = os.environ.get("POLYMARKET_CATALOG_PATH", "data/polymarket/catalog")
    Path(catalog_path).mkdir(parents=True, exist_ok=True)
    asyncio.run(run_collector())


if __name__ == "__main__":
    main()
