import os
import sys
import time

import httpx

from nautilus_trader.adapters.binance import BINANCE
from nautilus_trader.adapters.gateio import GATEIO
from nautilus_trader.adapters.kraken import KRAKEN
from nautilus_trader.adapters.kraken.http.wallet import fetch_kraken_withdraw_methods
from spread_monitor.utils import _binance_sign_query
from spread_monitor.utils import _normalize_chain
from spread_monitor.utils import _parse_csv_set


def _fetch_binance_chains(api_key: str, api_secret: str) -> dict[str, set[str]]:
    """拉取 Binance 每个币种支持的提现网络。GET /sapi/v1/capital/config/getall（签名接口）。"""
    ts = int(time.time() * 1000)
    query = f"timestamp={ts}"
    sig = _binance_sign_query(query, api_secret)
    url = f"https://api.binance.com/sapi/v1/capital/config/getall?{query}&signature={sig}"
    resp = httpx.get(url, headers={"X-MBX-APIKEY": api_key}, timeout=10.0)
    resp.raise_for_status()

    result: dict[str, set[str]] = {}
    for coin in resp.json():
        base = str(coin.get("coin", "")).upper()
        chains = {_normalize_chain(n.get("network", "")) for n in coin.get("networkList", [])}
        chains.discard("")
        if base and chains:
            result[base] = chains
    return result


# Gate.io currency_chains 接口限速较严，按币种批量拉取时若无间隔很容易触发 429，
# 因此这里做了简单的请求间隔限速（跨调用共享，见 _gateio_last_call_at）。
_GATEIO_CHAIN_MIN_INTERVAL = 0.3  # 秒
_gateio_last_call_at = 0.0


def _fetch_gateio_chains(currency: str, max_retries: int = 3) -> set[str]:
    """拉取 Gate.io 单个币种支持的提现网络。GET /api/v4/wallet/currency_chains?currency=xxx（公开接口）。

    该接口不支持一次性拉取全部币种，必须按币种查询，因此调用方需自行传入候选币种列表。
    对请求间隔做了限速，并在遇到 429 时做指数退避重试。
    """
    global _gateio_last_call_at

    attempt = 0
    while True:
        wait = _GATEIO_CHAIN_MIN_INTERVAL - (time.monotonic() - _gateio_last_call_at)
        if wait > 0:
            time.sleep(wait)
        _gateio_last_call_at = time.monotonic()

        try:
            resp = httpx.get(
                "https://api.gateio.ws/api/v4/wallet/currency_chains",
                params={"currency": currency},
                timeout=10.0,
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 429 and attempt < max_retries:
                attempt += 1
                time.sleep(2**attempt)  # 2s, 4s, 8s...
                continue
            raise

        result: set[str] = set()
        for row in resp.json():
            norm = _normalize_chain(str(row.get("chain", "")))
            if norm:
                result.add(norm)
        return result


def _dump_chains(symbols_csv: str) -> None:
    """调试用：按所拉取并打印提现链原始数据（归一化后），不启动实时监控。"""
    symbols = _parse_csv_set(symbols_csv) if symbols_csv else None

    binance_key = os.environ.get("BINANCE_API_KEY")
    binance_secret = os.environ.get("BINANCE_API_SECRET")
    kraken_key = os.environ.get("KRAKEN_SPOT_API_KEY")
    kraken_secret = os.environ.get("KRAKEN_SPOT_API_SECRET")

    fetchers: dict[str, object] = {}

    if binance_key and binance_secret:
        fetchers[BINANCE] = lambda: _fetch_binance_chains(binance_key, binance_secret)
    else:
        print(f"[dump] 跳过 {BINANCE}：缺少 BINANCE_API_KEY/BINANCE_API_SECRET")

    if kraken_key and kraken_secret:
        def _fetch_kraken() -> dict[str, set[str]]:
            raw = fetch_kraken_withdraw_methods(kraken_key, kraken_secret)
            return {base: {_normalize_chain(m) for m in methods} for base, methods in raw.items()}
        fetchers[KRAKEN] = _fetch_kraken
    else:
        print(f"[dump] 跳过 {KRAKEN}：缺少 KRAKEN_SPOT_API_KEY/KRAKEN_SPOT_API_SECRET")

    # Gate.io 接口按币种查询，没有一次拉全部的接口，必须配合 --dump-symbols 指定币种
    if symbols:
        fetchers[GATEIO] = lambda: {sym: _fetch_gateio_chains(sym) for sym in sorted(symbols)}
    else:
        print(f"[dump] 跳过 {GATEIO}：该所接口按币种查询，需配合 --dump-symbols 指定币种")

    for venue, fetch in fetchers.items():
        print(f"\n{'='*60}\n[{venue}] 提现链数据\n{'='*60}")
        try:
            data = fetch()
        except Exception as exc:  # noqa: BLE001 - 调试工具，任何异常都要打印出来看
            print(f"  拉取失败: {exc!r}")
            continue

        shown = 0
        for base in sorted(data):
            if symbols and base not in symbols:
                continue
            print(f"  {base:<10} {sorted(data[base])}")
            shown += 1
        suffix = f"（已按 --dump-symbols 过滤，总共 {len(data)} 个币种）" if symbols else ""
        print(f"  共显示 {shown} 个币种{suffix}")


def _load_chain_support(relevant_venues: set[str]) -> dict[str, dict[str, set[str]]]:
    """按需拉取各所的提现链数据。缺少对应所的 API Key 时直接报错退出。

    Gate.io 接口按币种查询、没有一次拉全部的接口，因此不在此处预取，
    改为在 `_filter_by_common_chain` 中按实际候选币种懒加载。
    """
    chains: dict[str, dict[str, set[str]]] = {}

    if BINANCE in relevant_venues:
        key = os.environ.get("BINANCE_API_KEY")
        secret = os.environ.get("BINANCE_API_SECRET")
        if not key or not secret:
            sys.exit(
                "[chain] 缺少 BINANCE_API_KEY/BINANCE_API_SECRET，无法校验提现链"
                "（可用 --no-require-common-chain 关闭该规则）",
            )
        chains[BINANCE] = _fetch_binance_chains(key, secret)
        print(f"[chain] {BINANCE}: {len(chains[BINANCE])} 个币种的提现链数据")

    if KRAKEN in relevant_venues:
        key = os.environ.get("KRAKEN_SPOT_API_KEY")
        secret = os.environ.get("KRAKEN_SPOT_API_SECRET")
        if not key or not secret:
            sys.exit(
                "[chain] 缺少 KRAKEN_SPOT_API_KEY/KRAKEN_SPOT_API_SECRET，无法校验提现链"
                "（可用 --no-require-common-chain 关闭该规则）",
            )
        raw = fetch_kraken_withdraw_methods(key, secret)
        chains[KRAKEN] = {base: {_normalize_chain(m) for m in methods} for base, methods in raw.items()}
        print(f"[chain] {KRAKEN}: {len(chains[KRAKEN])} 个币种的提现链数据")

    return chains
