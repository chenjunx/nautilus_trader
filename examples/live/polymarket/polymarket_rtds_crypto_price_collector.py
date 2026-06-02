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

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any


DEFAULT_WS_URL = "wss://ws-live-data.polymarket.com"
DEFAULT_SYMBOLS = ["btcusd"]
DEFAULT_OUTPUT_DIR = Path("/data/polymarket/rtds")
DEFAULT_PING_INTERVAL_SECS = 5.0


def parse_symbols(value: str | None) -> list[str]:
    if value is None or not value.strip():
        return list(DEFAULT_SYMBOLS)

    symbols = [item.strip().lower() for item in value.split(",")]
    return [item for item in symbols if item]


def subscription_filter_for_symbol(symbol: str) -> str:
    binance_symbol = "btcusdt" if symbol == "btcusd" else symbol
    return json.dumps({"symbol": binance_symbol})


def build_subscription_message(symbols: list[str]) -> dict[str, Any]:
    return {
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": "crypto_prices",
                "type": "update",
                "filters": subscription_filter_for_symbol(symbol),
            }
            for symbol in symbols
        ],
    }


def extract_symbol(msg: dict[str, Any], symbols: list[str]) -> str:
    for key in ("symbol", "asset", "ticker"):
        value = msg.get(key)
        if isinstance(value, str) and value:
            return value.lower()

    if len(symbols) == 1:
        return symbols[0]

    return "unknown"


def output_path_for(output_dir: Path, symbol: str, ts_recv_ns: int) -> Path:
    dt = datetime.fromtimestamp(ts_recv_ns / 1_000_000_000, tz=UTC)
    return output_dir / "crypto_prices" / symbol / f"{dt.date().isoformat()}.jsonl"


def write_message(
    output_dir: Path,
    symbols: list[str],
    msg: dict[str, Any],
    ts_recv_ns: int,
) -> Path:
    symbol = extract_symbol(msg, symbols)
    path = output_path_for(output_dir=output_dir, symbol=symbol, ts_recv_ns=ts_recv_ns)
    path.parent.mkdir(parents=True, exist_ok=True)

    record = {
        "ts_recv_ns": ts_recv_ns,
        "symbol": symbol,
        "raw": msg,
    }
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False))
        f.write("\n")

    return path


def handle_raw_message(raw: bytes, symbols: list[str], output_dir: Path) -> None:
    ts_recv_ns = time.time_ns()
    text = raw.decode("utf-8")
    if text in ("PONG", "PING"):
        return

    try:
        msg = json.loads(text)
    except json.JSONDecodeError:
        msg = {"message": text}

    if isinstance(msg, list):
        for item in msg:
            if isinstance(item, dict):
                write_message(output_dir, symbols, item, ts_recv_ns)
        return

    if isinstance(msg, dict):
        write_message(output_dir, symbols, msg, ts_recv_ns)


async def send_ping_loop(client: Any, interval_secs: float) -> None:
    while True:
        await asyncio.sleep(interval_secs)
        await client.send_text(b"PING")


async def collect_rtds_crypto_prices(
    ws_url: str,
    symbols: list[str],
    output_dir: Path,
    ping_interval_secs: float,
) -> None:
    from nautilus_trader.core.nautilus_pyo3 import WebSocketClient
    from nautilus_trader.core.nautilus_pyo3 import WebSocketConfig

    loop = asyncio.get_running_loop()
    config = WebSocketConfig(
        url=ws_url,
        headers=[],
        heartbeat=int(ping_interval_secs),
        idle_timeout_ms=60_000,
    )
    client = await WebSocketClient.connect(
        loop_=loop,
        config=config,
        handler=lambda raw: handle_raw_message(raw, symbols, output_dir),
    )
    ping_task = asyncio.create_task(send_ping_loop(client, ping_interval_secs))
    try:
        await client.send_text(json.dumps(build_subscription_message(symbols)).encode("utf-8"))
        while client.is_active():
            await asyncio.sleep(1.0)
    finally:
        ping_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ping_task
        if not client.is_closed() and not client.is_disconnecting():
            await client.disconnect()


async def run_forever() -> None:
    ws_url = os.environ.get("POLYMARKET_RTDS_WS_URL", DEFAULT_WS_URL)
    symbols = parse_symbols(os.environ.get("POLYMARKET_RTDS_SYMBOLS"))
    output_dir = Path(os.environ.get("POLYMARKET_RTDS_OUTPUT_DIR", str(DEFAULT_OUTPUT_DIR)))
    ping_interval_secs = float(
        os.environ.get("POLYMARKET_RTDS_PING_INTERVAL_SECS", str(DEFAULT_PING_INTERVAL_SECS)),
    )

    while True:
        try:
            await collect_rtds_crypto_prices(
                ws_url=ws_url,
                symbols=symbols,
                output_dir=output_dir,
                ping_interval_secs=ping_interval_secs,
            )
        except Exception as exc:
            print(f"RTDS connection lost: {exc}; reconnecting in 5 seconds", flush=True)
            await asyncio.sleep(5.0)


def main() -> None:
    asyncio.run(run_forever())


if __name__ == "__main__":
    main()
