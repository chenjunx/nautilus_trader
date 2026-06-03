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
DEFAULT_STALE_TIMEOUT_SECS = 60.0


def parse_symbols(value: str | None) -> list[str]:
    if value is None or not value.strip():
        return list(DEFAULT_SYMBOLS)

    symbols = [item.strip().lower() for item in value.split(",")]
    return [item for item in symbols if item]


def chainlink_symbol_for(symbol: str) -> str:
    normalized = symbol.lower().replace("/", "")
    if normalized.endswith("usdt"):
        normalized = normalized[:-1]
    if normalized.endswith("usd"):
        return f"{normalized[:-3]}/usd"
    return symbol.lower()


def storage_symbol_for(symbol: str) -> str:
    return symbol.lower().replace("/", "")


def should_reconnect_for_stale_messages(
    last_msg_ns: int,
    now_ns: int,
    timeout_secs: float,
) -> bool:
    return now_ns - last_msg_ns >= int(timeout_secs * 1_000_000_000)


def subscription_filter_for_symbol(symbol: str) -> str:
    return json.dumps({"symbol": chainlink_symbol_for(symbol)})


def build_subscription_message() -> dict[str, Any]:
    return {
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": "",
            },
        ],
    }


def symbol_from_message(msg: dict[str, Any]) -> str | None:
    payload = msg.get("payload")
    if isinstance(payload, dict):
        value = payload.get("symbol")
        if isinstance(value, str) and value:
            return storage_symbol_for(value)

    for key in ("symbol", "asset", "ticker"):
        value = msg.get(key)
        if isinstance(value, str) and value:
            return storage_symbol_for(value)

    return None


def message_matches_symbols(msg: dict[str, Any], symbols: list[str]) -> bool:
    symbol = symbol_from_message(msg)
    return symbol is None or symbol in {storage_symbol_for(item) for item in symbols}


def extract_symbol(msg: dict[str, Any], symbols: list[str]) -> str:
    symbol = symbol_from_message(msg)
    if symbol is not None:
        return symbol

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
    payload = msg.get("payload")
    if isinstance(payload, dict):
        value = payload.get("value")
        if value is not None:
            return str(value)

    for key in ("price", "value", "markPrice", "mark_price"):
        value = msg.get(key)
        if value is not None:
            return str(value)
    return None


def price_ts_ms_for(msg: dict[str, Any]) -> int | None:
    payload = msg.get("payload")
    if isinstance(payload, dict):
        value = payload.get("timestamp")
        if isinstance(value, int):
            return value

    value = msg.get("timestamp")
    if isinstance(value, int):
        return value
    return None


def parquet_records_for_message(
    msg: dict[str, Any],
    symbols: list[str],
    ts_recv_ns: int,
) -> list[dict[str, Any]]:
    if not message_matches_symbols(msg, symbols):
        return []

    raw_json = json.dumps(msg, separators=(",", ":"), ensure_ascii=False)
    symbol = extract_symbol(msg, symbols)
    payload = msg.get("payload")
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            records = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                value = item.get("value")
                timestamp = item.get("timestamp")
                if value is None or not isinstance(timestamp, int):
                    continue
                records.append(
                    {
                        "ts_recv_ns": ts_recv_ns,
                        "symbol": symbol,
                        "price_ts_ms": timestamp,
                        "price": str(value),
                        "raw_json": raw_json,
                    },
                )
            return records

    price = extract_price(msg)
    if price is None:
        return []

    return [
        {
            "ts_recv_ns": ts_recv_ns,
            "symbol": symbol,
            "price_ts_ms": price_ts_ms_for(msg),
            "price": price,
            "raw_json": raw_json,
        },
    ]


def write_parquet_messages(
    output_dir: Path,
    symbols: list[str],
    rows: list[dict[str, Any]],
) -> Path | None:
    if not rows:
        return None

    import pyarrow as pa
    import pyarrow.parquet as pq

    records = []
    for row in rows:
        records.extend(
            parquet_records_for_message(
                msg=row["msg"],
                symbols=symbols,
                ts_recv_ns=row["ts_recv_ns"],
            ),
        )

    if not records:
        return None

    first = records[0]
    path = parquet_path_for(
        output_dir=output_dir,
        symbol=first["symbol"],
        ts_recv_ns=first["ts_recv_ns"],
    )
    path.parent.mkdir(parents=True, exist_ok=True)

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
    symbols: list[str],
) -> None:
    if isinstance(msg, list):
        for item in msg:
            if isinstance(item, dict) and message_matches_symbols(item, symbols):
                buffer.append({"ts_recv_ns": ts_recv_ns, "msg": item})
        return

    if isinstance(msg, dict) and message_matches_symbols(msg, symbols):
        buffer.append({"ts_recv_ns": ts_recv_ns, "msg": msg})


def handle_raw_message(raw: bytes, symbols: list[str], output_dir: Path) -> None:
    ts_recv_ns = time.time_ns()
    msg = decode_raw_message(raw)
    if msg is None:
        return

    buffer: list[dict[str, Any]] = []
    append_decoded_messages(buffer, msg, ts_recv_ns, symbols)
    for row in buffer:
        write_message(output_dir, symbols, row["msg"], row["ts_recv_ns"])


def make_runtime_handler(
    symbols: list[str],
    output_dir: Path,
    output_format: str,
    buffer: list[dict[str, Any]],
    max_buffer_size: int,
    last_msg_ns: dict[str, int],
) -> Any:
    def handle(raw: bytes) -> None:
        ts_recv_ns = time.time_ns()
        last_msg_ns["value"] = ts_recv_ns
        msg = decode_raw_message(raw)
        if msg is None:
            return

        if output_format == "jsonl":
            jsonl_rows: list[dict[str, Any]] = []
            append_decoded_messages(jsonl_rows, msg, ts_recv_ns, symbols)
            for row in jsonl_rows:
                write_message(output_dir, symbols, row["msg"], row["ts_recv_ns"])
            return

        append_decoded_messages(buffer, msg, ts_recv_ns, symbols)
        if len(buffer) >= max_buffer_size:
            rows = list(buffer)
            path = write_parquet_messages(output_dir, symbols, rows)
            if path is not None:
                print(f"Flushed {len(rows)} RTDS messages to {path}", flush=True)
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
            rows = list(buffer)
            path = write_parquet_messages(output_dir, symbols, rows)
            if path is not None:
                print(f"Flushed {len(rows)} RTDS messages to {path}", flush=True)
            buffer.clear()


async def collect_rtds_crypto_prices(
    ws_url: str,
    symbols: list[str],
    output_dir: Path,
    ping_interval_secs: float,
    output_format: str,
    flush_interval_secs: float,
    max_buffer_size: int,
    stale_timeout_secs: float,
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
    last_msg_ns = {"value": time.time_ns()}
    client = await WebSocketClient.connect(
        loop_=loop,
        config=config,
        handler=make_runtime_handler(
            symbols=symbols,
            output_dir=output_dir,
            output_format=output_format,
            buffer=buffer,
            max_buffer_size=max_buffer_size,
            last_msg_ns=last_msg_ns,
        ),
    )
    ping_task = asyncio.create_task(send_ping_loop(client, ping_interval_secs))
    flush_task = None
    if output_format == "parquet":
        flush_task = asyncio.create_task(
            flush_parquet_loop(output_dir, symbols, buffer, flush_interval_secs),
        )
    try:
        subscription = build_subscription_message()
        print(f"Subscribing to Polymarket RTDS: {json.dumps(subscription)}", flush=True)
        await client.send_text(json.dumps(subscription).encode("utf-8"))
        while client.is_active():
            await asyncio.sleep(1.0)
            if should_reconnect_for_stale_messages(
                last_msg_ns=last_msg_ns["value"],
                now_ns=time.time_ns(),
                timeout_secs=stale_timeout_secs,
            ):
                print(
                    f"No RTDS messages for {stale_timeout_secs:g} seconds; reconnecting",
                    flush=True,
                )
                break
    finally:
        if output_format == "parquet" and buffer:
            rows = list(buffer)
            path = write_parquet_messages(output_dir, symbols, rows)
            if path is not None:
                print(f"Flushed {len(rows)} RTDS messages to {path}", flush=True)
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
    stale_timeout_secs = float(
        os.environ.get("POLYMARKET_RTDS_STALE_TIMEOUT_SECS", str(DEFAULT_STALE_TIMEOUT_SECS)),
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
                stale_timeout_secs=stale_timeout_secs,
            )
        except Exception as exc:
            print(f"RTDS connection lost: {exc}; reconnecting in 5 seconds", flush=True)
            await asyncio.sleep(5.0)


def main() -> None:
    asyncio.run(run_forever())


if __name__ == "__main__":
    main()
