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
DEFAULT_OUTPUT_FORMAT = "parquet"
DEFAULT_FLUSH_INTERVAL_SECS = 5.0
DEFAULT_MAX_BUFFER_SIZE = 1_000


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
                "topic": "crypto_prices_chainlink",
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


def parquet_path_for(output_dir: Path, symbol: str, ts_recv_ns: int) -> Path:
    dt = datetime.fromtimestamp(ts_recv_ns / 1_000_000_000, tz=UTC)
    return (
        output_dir
        / "crypto_prices"
        / symbol
        / f"date={dt.date().isoformat()}"
        / f"{ts_recv_ns}.parquet"
    )


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


def extract_price(msg: dict[str, Any]) -> str | None:
    for key in ("price", "value", "markPrice", "mark_price"):
        value = msg.get(key)
        if value is not None:
            return str(value)
    return None


def write_parquet_messages(
    output_dir: Path,
    symbols: list[str],
    rows: list[dict[str, Any]],
) -> Path | None:
    if not rows:
        return None

    import pyarrow as pa
    import pyarrow.parquet as pq

    first = rows[0]
    first_msg = first["msg"]
    first_ts_recv_ns = first["ts_recv_ns"]
    symbol = extract_symbol(first_msg, symbols)
    path = parquet_path_for(output_dir=output_dir, symbol=symbol, ts_recv_ns=first_ts_recv_ns)
    path.parent.mkdir(parents=True, exist_ok=True)

    records = []
    for row in rows:
        msg = row["msg"]
        records.append(
            {
                "ts_recv_ns": row["ts_recv_ns"],
                "symbol": extract_symbol(msg, symbols),
                "raw_json": json.dumps(msg, separators=(",", ":"), ensure_ascii=False),
                "price": extract_price(msg),
            },
        )

    table = pa.Table.from_pylist(records)
    pq.write_table(table, path)
    return path


def decode_raw_message(raw: bytes) -> Any | None:
    text = raw.decode("utf-8")
    if text in ("PONG", "PING"):
        return None

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"message": text}


def append_decoded_messages(
    buffer: list[dict[str, Any]],
    msg: Any,
    ts_recv_ns: int,
) -> None:
    if isinstance(msg, list):
        for item in msg:
            if isinstance(item, dict):
                buffer.append({"ts_recv_ns": ts_recv_ns, "msg": item})
        return

    if isinstance(msg, dict):
        buffer.append({"ts_recv_ns": ts_recv_ns, "msg": msg})


def handle_raw_message(raw: bytes, symbols: list[str], output_dir: Path) -> None:
    ts_recv_ns = time.time_ns()
    msg = decode_raw_message(raw)
    if msg is None:
        return

    buffer: list[dict[str, Any]] = []
    append_decoded_messages(buffer, msg, ts_recv_ns)
    for row in buffer:
        write_message(output_dir, symbols, row["msg"], row["ts_recv_ns"])


def make_runtime_handler(
    symbols: list[str],
    output_dir: Path,
    output_format: str,
    buffer: list[dict[str, Any]],
    max_buffer_size: int,
) -> Any:
    def handle(raw: bytes) -> None:
        ts_recv_ns = time.time_ns()
        msg = decode_raw_message(raw)
        if msg is None:
            return

        if output_format == "jsonl":
            jsonl_rows: list[dict[str, Any]] = []
            append_decoded_messages(jsonl_rows, msg, ts_recv_ns)
            for row in jsonl_rows:
                write_message(output_dir, symbols, row["msg"], row["ts_recv_ns"])
            return

        append_decoded_messages(buffer, msg, ts_recv_ns)
        if len(buffer) >= max_buffer_size:
            write_parquet_messages(output_dir, symbols, list(buffer))
            buffer.clear()

    return handle


async def send_ping_loop(client: Any, interval_secs: float) -> None:
    while True:
        await asyncio.sleep(interval_secs)
        await client.send_text(b"PING")


async def flush_parquet_loop(
    output_dir: Path,
    symbols: list[str],
    buffer: list[dict[str, Any]],
    interval_secs: float,
) -> None:
    while True:
        await asyncio.sleep(interval_secs)
        if buffer:
            write_parquet_messages(output_dir, symbols, list(buffer))
            buffer.clear()


async def collect_rtds_crypto_prices(
    ws_url: str,
    symbols: list[str],
    output_dir: Path,
    ping_interval_secs: float,
    output_format: str,
    flush_interval_secs: float,
    max_buffer_size: int,
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
    buffer: list[dict[str, Any]] = []
    client = await WebSocketClient.connect(
        loop_=loop,
        config=config,
        handler=make_runtime_handler(
            symbols=symbols,
            output_dir=output_dir,
            output_format=output_format,
            buffer=buffer,
            max_buffer_size=max_buffer_size,
        ),
    )
    ping_task = asyncio.create_task(send_ping_loop(client, ping_interval_secs))
    flush_task = None
    if output_format == "parquet":
        flush_task = asyncio.create_task(
            flush_parquet_loop(output_dir, symbols, buffer, flush_interval_secs),
        )
    try:
        await client.send_text(json.dumps(build_subscription_message(symbols)).encode("utf-8"))
        while client.is_active():
            await asyncio.sleep(1.0)
    finally:
        if output_format == "parquet" and buffer:
            write_parquet_messages(output_dir, symbols, list(buffer))
            buffer.clear()
        ping_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ping_task
        if flush_task is not None:
            flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await flush_task
        if not client.is_closed() and not client.is_disconnecting():
            await client.disconnect()


async def run_forever() -> None:
    ws_url = os.environ.get("POLYMARKET_RTDS_WS_URL", DEFAULT_WS_URL)
    symbols = parse_symbols(os.environ.get("POLYMARKET_RTDS_SYMBOLS"))
    output_dir = Path(os.environ.get("POLYMARKET_RTDS_OUTPUT_DIR", str(DEFAULT_OUTPUT_DIR)))
    ping_interval_secs = float(
        os.environ.get("POLYMARKET_RTDS_PING_INTERVAL_SECS", str(DEFAULT_PING_INTERVAL_SECS)),
    )
    output_format = os.environ.get("POLYMARKET_RTDS_OUTPUT_FORMAT", DEFAULT_OUTPUT_FORMAT).lower()
    flush_interval_secs = float(
        os.environ.get("POLYMARKET_RTDS_FLUSH_INTERVAL_SECS", str(DEFAULT_FLUSH_INTERVAL_SECS)),
    )
    max_buffer_size = int(
        os.environ.get("POLYMARKET_RTDS_MAX_BUFFER_SIZE", str(DEFAULT_MAX_BUFFER_SIZE)),
    )

    while True:
        try:
            await collect_rtds_crypto_prices(
                ws_url=ws_url,
                symbols=symbols,
                output_dir=output_dir,
                ping_interval_secs=ping_interval_secs,
                output_format=output_format,
                flush_interval_secs=flush_interval_secs,
                max_buffer_size=max_buffer_size,
            )
        except Exception as exc:
            print(f"RTDS connection lost: {exc}; reconnecting in 5 seconds", flush=True)
            await asyncio.sleep(5.0)


def main() -> None:
    asyncio.run(run_forever())


if __name__ == "__main__":
    main()
