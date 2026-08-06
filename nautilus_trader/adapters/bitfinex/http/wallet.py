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
"""Bitfinex 钱包（充提币）REST API 封装，复用 common.signing 的 HMAC-SHA384 签名实现。

注意：Bitfinex v2 API 响应是固定索引数组而非 JSON 对象。
"""

from __future__ import annotations

import time
from typing import Any

import httpx

from nautilus_trader.adapters.bitfinex.common.signing import bitfinex_rest_signature
from nautilus_trader.adapters.bitfinex.constants import BITFINEX_HTTP_AUTH_BASE_URL


BITFINEX_PUBLIC_BASE_URL = "https://api-pub.bitfinex.com/v2"


def fetch_bitfinex_withdraw_methods() -> dict[str, list[str]]:
    """查询 Bitfinex 支持的提现方式及其对应的币种（公开接口，无需签名）。

    返回 {method: [currency_list]}，如 {"TETHERUSE": ["UST"], "BITCOIN": ["BTC"]}
    参考：https://api-pub.bitfinex.com/v2/conf/pub:map:tx:method
    """
    resp = httpx.get(f"{BITFINEX_PUBLIC_BASE_URL}/conf/pub:map:tx:method", timeout=10.0)
    resp.raise_for_status()
    result = resp.json()

    if not result or not isinstance(result, list) or not result[0]:
        return {}

    # 响应格式: [[["METHOD", ["CCY1", "CCY2"]], ...]]
    methods: dict[str, list[str]] = {}
    for item in result[0]:
        if isinstance(item, list) and len(item) >= 2 and isinstance(item[1], list):
            method = str(item[0])
            currencies = [str(c) for c in item[1]]
            methods[method] = currencies
    return methods


def fetch_bitfinex_currency_code(display_symbol: str) -> str:
    """将常见币种符号（如 "USDT"）映射为 Bitfinex 内部代码（如 "UST"）（公开接口，无需签名）。

    Bitfinex 的 movements/withdraw/deposit 接口使用内部代码而非通用符号，两者不一致时
    （典型例子: USDT -> UST, DOGE 等常见代码则相同），必须先做这层映射，否则会查错币种或
    提现方法列表为空。找不到映射时原样返回（覆盖大多数币种代码与符号相同的情况）。

    参考：https://api-pub.bitfinex.com/v2/conf/pub:map:currency:sym
    """
    resp = httpx.get(f"{BITFINEX_PUBLIC_BASE_URL}/conf/pub:map:currency:sym", timeout=10.0)
    resp.raise_for_status()
    result = resp.json()

    if not result or not isinstance(result, list) or not result[0]:
        return display_symbol.upper()

    target = display_symbol.upper()
    # 响应格式: [[["INTERNAL_CODE", "DISPLAY_SYMBOL"], ...]]，正向查找 symbol -> code
    for item in result[0]:
        if isinstance(item, list) and len(item) >= 2 and str(item[1]).upper() == target:
            return str(item[0])
    return display_symbol.upper()


def fetch_bitfinex_methods_for_currency(currency_code: str) -> list[str]:
    """查询指定币种（Bitfinex 内部代码，如 "UST"）支持的所有提现/充值方式（method 名）。

    返回小写 method 列表，如 ["tetheruse", "tetherusx", ...]，可直接用作 withdraw 的 method 参数。
    调用方需先用 `fetch_bitfinex_currency_code` 把通用符号转换成内部代码。
    """
    all_methods = fetch_bitfinex_withdraw_methods()
    currency_upper = currency_code.upper()
    return [
        method.lower()
        for method, currencies in all_methods.items()
        if currency_upper in currencies
    ]


def _bitfinex_private_post(
    path: str,
    api_key: str,
    api_secret: str,
    body: dict[str, Any] | None = None,
) -> Any:
    """Bitfinex 私有 API POST 请求通用封装。

    path 应为相对路径如 "w/withdraw"，会自动拼接 /auth/ 前缀和基础 URL。
    """
    import json

    nonce = str(time.time_ns() // 1_000)  # 微秒 nonce，必须严格递增
    body_str = json.dumps(body or {}, separators=(",", ":"))
    full_path = f"v2/auth/{path.lstrip('/')}"

    signature = bitfinex_rest_signature(full_path, nonce, body_str, api_secret)

    headers = {
        "Content-Type": "application/json",
        "bfx-apikey": api_key,
        "bfx-nonce": nonce,
        "bfx-signature": signature,
    }

    url = f"{BITFINEX_HTTP_AUTH_BASE_URL}/auth/{path.lstrip('/')}"

    resp = httpx.post(url, headers=headers, content=body_str, timeout=30.0)
    resp.raise_for_status()
    return resp.json()


def fetch_bitfinex_movements(
    api_key: str,
    api_secret: str,
    currency: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """查询 Bitfinex 充提记录（movements），返回最近 limit 条。

    响应是固定索引数组，参考：https://docs.bitfinex.com/reference/rest-auth-movements
    [ID, CURRENCY, CURRENCY_NAME, ..., STATUS, ..., AMOUNT, FEES, ...,
     DESTINATION_ADDRESS, PAYMENT_ID, ..., TRANSACTION_ID, NOTE]
    """
    path = f"r/movements/{currency}/hist" if currency else "r/movements/hist"
    result = _bitfinex_private_post(path, api_key, api_secret, body={"limit": limit})

    movements = []
    for item in result:
        if not isinstance(item, list) or len(item) < 21:
            continue
        movements.append({
            "id": str(item[0]),
            "currency": str(item[1]),
            "status": str(item[9]),
            "amount": float(item[12]),  # 正数=充值，负数=提现
            "fees": float(item[13]),
            "address": str(item[16] or ""),
            "payment_id": str(item[17] or ""),
            "txid": str(item[20] or ""),
            "mts_started": int(item[5]),
            "mts_updated": int(item[6]),
        })
    return movements


def bitfinex_withdraw(
    api_key: str,
    api_secret: str,
    wallet: str,
    method: str,
    amount: float,
    address: str,
    payment_id: str | None = None,
) -> str:
    """提交 Bitfinex 提现请求，返回提现单号（withdrawal_id）。

    wallet: "exchange" | "margin" | "funding"
    method: 小写的方法名，如 "bitcoin", "tetheruse"（USDT on Ethereum）等
    响应参考：https://docs.bitfinex.com/reference/rest-auth-withdraw
    """
    body: dict[str, Any] = {
        "wallet": wallet,
        "method": method.lower(),
        "amount": str(amount),
        "address": address,
    }
    if payment_id:
        body["payment_id"] = payment_id

    # 响应格式: [MTS, TYPE, MSG_ID, null, [WITHDRAWAL_ID, ...], CODE, STATUS, TEXT]
    result = _bitfinex_private_post("w/withdraw", api_key, api_secret, body=body)

    if not isinstance(result, list) or len(result) < 8:
        raise RuntimeError(f"Bitfinex withdraw 返回格式异常: {result}")

    status = result[6]
    if status != "SUCCESS":
        text = result[7] if len(result) > 7 else ""
        raise RuntimeError(f"Bitfinex withdraw 失败: {status} {text}")

    withdrawal_array = result[4]
    if not withdrawal_array or not isinstance(withdrawal_array, list):
        raise RuntimeError(f"Bitfinex withdraw 未返回 withdrawal_id: {result}")

    withdrawal_id = withdrawal_array[0]
    return str(withdrawal_id)


def bitfinex_withdraw_status(
    api_key: str,
    api_secret: str,
    withdrawal_id: str,
) -> dict | None:
    """查询 Bitfinex 提现单状态（通过 movements 接口按 ID 过滤）。"""
    result = _bitfinex_private_post(
        "r/movements/info",
        api_key,
        api_secret,
        body={"id": [int(withdrawal_id)]},
    )

    if not result or not isinstance(result, list) or not result[0]:
        return None

    # movements 返回的是数组列表
    for item in result:
        if not isinstance(item, list) or len(item) < 21:
            continue
        if str(item[0]) == withdrawal_id:
            return {
                "id": str(item[0]),
                "status": str(item[9]),
                "txid": str(item[20] or ""),
                "amount": float(item[12]),
            }
    return None


def fetch_bitfinex_deposit_address(
    api_key: str,
    api_secret: str,
    wallet: str,
    method: str,
    op_renew: bool = False,
) -> dict[str, str]:
    """获取 Bitfinex 某币种在指定 method 下的充值地址。

    wallet: "exchange" | "margin" | "funding"
    method: 小写的方法名，如 "bitcoin", "ethereum" 等
    op_renew: True 时生成新地址（旧地址依然有效）

    响应参考：https://docs.bitfinex.com/reference/rest-auth-deposit-address
    [MTS, TYPE, MSG_ID, null, [null, METHOD, CURRENCY_CODE, null, ADDRESS, POOL_ADDRESS],
     CODE, STATUS, TEXT]

    注意：对于需要 memo/tag 的币种（如 EOS/XRP），Bitfinex 文档明确说明
    "the deposit address cannot be retrieved through this endpoint"，需要在网页端查看。
    """
    body: dict[str, Any] = {"wallet": wallet, "method": method.lower()}
    if op_renew:
        body["op_renew"] = 1

    result = _bitfinex_private_post("w/deposit/address", api_key, api_secret, body=body)

    if not isinstance(result, list) or len(result) < 8:
        raise RuntimeError(f"Bitfinex deposit/address 返回格式异常: {result}")

    status = result[6]
    if status != "SUCCESS":
        text = result[7] if len(result) > 7 else ""
        raise RuntimeError(f"Bitfinex deposit/address 失败: {status} {text}")

    addr_array = result[4]
    if not addr_array or not isinstance(addr_array, list) or len(addr_array) < 6:
        raise RuntimeError(f"Bitfinex deposit/address 未返回地址: {result}")

    # [null, METHOD, CURRENCY_CODE, null, ADDRESS, POOL_ADDRESS]
    address = str(addr_array[4] or "")
    pool_address = str(addr_array[5] or "")

    # 对于需要 memo/tag 的币种，ADDRESS 字段会显示 tag，POOL_ADDRESS 是实际地址
    # 但文档说这种情况下无法通过 API 获取，这里尽力而为
    if pool_address:
        return {"address": pool_address, "memo": address}
    return {"address": address, "memo": ""}


def fetch_bitfinex_deposit_status(
    api_key: str,
    api_secret: str,
    currency: str,
    limit: int = 100,
) -> list[dict]:
    """查询 Bitfinex 充值记录（通过 movements 接口，只返回正数金额的记录）。"""
    movements = fetch_bitfinex_movements(api_key, api_secret, currency, limit)
    # 过滤出充值记录（amount > 0）
    return [m for m in movements if m["amount"] > 0]
