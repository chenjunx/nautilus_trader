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
"""Gate.io Spot 钱包（充提币）REST API 封装，复用 common.signing 的 HMAC-SHA512 签名实现。"""

from __future__ import annotations

import json
import time
from typing import Any

import httpx

from nautilus_trader.adapters.gateio.common.constants import GATEIO_SPOT_HTTP_BASE_URL
from nautilus_trader.adapters.gateio.common.signing import gateio_rest_signature


def _gateio_private_request(
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    params: dict[str, Any] | None = None,
    body: dict[str, Any] | None = None,
) -> Any:
    """Gate.io 私有 API 请求通用封装。

    path 为相对路径（如 "/wallet/currency_chains"），签名需要拼接 /api/v4 前缀，
    而 base_url 本身已经含 /api/v4，两者不能共用同一个变量，否则会重复。
    """
    timestamp = str(int(time.time()))
    query_string = ""
    if params:
        query_string = "&".join(f"{k}={v}" for k, v in sorted(params.items()))

    body_str = ""
    if body:
        body_str = json.dumps(body, separators=(",", ":"))

    headers = gateio_rest_signature(
        method=method.upper(),
        path=f"/api/v4{path}",
        query_string=query_string,
        body=body_str,
        api_key=api_key,
        api_secret=api_secret,
        timestamp=timestamp,
    )
    headers["Content-Type"] = "application/json"

    url = f"{GATEIO_SPOT_HTTP_BASE_URL}{path}"
    if query_string:
        url += f"?{query_string}"

    resp = httpx.request(
        method=method.upper(),
        url=url,
        headers=headers,
        content=body_str if body_str else None,
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_gateio_withdraw_chains(api_key: str, api_secret: str, currency: str) -> dict[str, dict]:
    """查询 Gate.io 某币种支持的提现链及其手续费明细。

    返回 {链名: {"fee": 手续费, "min": 最小提现量, ...}}
    """
    result = _gateio_private_request(
        method="GET",
        path="/wallet/currency_chains",
        api_key=api_key,
        api_secret=api_secret,
        params={"currency": currency.upper()},
    )

    chains: dict[str, dict] = {}
    for item in result:
        chain = str(item.get("chain", ""))
        if not chain:
            continue
        chains[chain] = {
            "fee": float(item.get("withdraw_fee", 0)),
            "min": float(item.get("withdraw_min", 0)),
            "is_withdraw_disabled": bool(item.get("is_withdraw_disabled", False)),
        }
    return chains


def gateio_withdraw(
    api_key: str,
    api_secret: str,
    currency: str,
    chain: str,
    address: str,
    amount: float,
    memo: str | None = None,
) -> str:
    """提交 Gate.io 提现请求，返回提现单号（withdrawal_id）。"""
    body: dict[str, Any] = {
        "currency": currency.upper(),
        "chain": chain,
        "address": address,
        "amount": str(amount),
    }
    if memo:
        body["memo"] = memo

    result = _gateio_private_request(
        method="POST",
        path="/withdrawals",
        api_key=api_key,
        api_secret=api_secret,
        body=body,
    )
    return str(result.get("id", ""))


def gateio_withdraw_status(api_key: str, api_secret: str, withdrawal_id: str) -> dict | None:
    """查询 Gate.io 提现单状态。"""
    result = _gateio_private_request(
        method="GET",
        path=f"/withdrawals/{withdrawal_id}",
        api_key=api_key,
        api_secret=api_secret,
    )
    if not result:
        return None
    return {
        "id": str(result.get("id", "")),
        "status": str(result.get("status", "")),  # DONE/CANCEL/REQUEST/MANUAL/BCODE/EXTPEND/FAIL/INVALID/VERIFY/PROCES/PEND/DMOVE/SPLITPEND
        "txid": str(result.get("txid", "")),
    }


def fetch_gateio_deposit_address(
    api_key: str,
    api_secret: str,
    currency: str,
    chain: str,
) -> dict[str, str]:
    """获取 Gate.io 某币种在指定链上的充值地址。"""
    result = _gateio_private_request(
        method="GET",
        path="/wallet/deposit_address",
        api_key=api_key,
        api_secret=api_secret,
        params={"currency": currency.upper(), "chain": chain},
    )
    return {
        "address": str(result.get("address", "")),
        "memo": str(result.get("payment_id", "") or result.get("payment_name", "") or ""),
    }


def fetch_gateio_deposit_status(api_key: str, api_secret: str, currency: str) -> list[dict]:
    """查询 Gate.io 充值记录（最近 30 天）。"""
    result = _gateio_private_request(
        method="GET",
        path="/wallet/deposits",
        api_key=api_key,
        api_secret=api_secret,
        params={"currency": currency.upper()},
    )
    return [
        {
            "id": str(item.get("id", "")),
            "txid": str(item.get("txid", "")),
            "amount": float(item.get("amount", 0)),
            "status": str(item.get("status", "")),  # DONE/CANCEL/REQUEST/MANUAL/BCODE/EXTPEND/FAIL/INVALID/VERIFY/PROCES/PEND
            "timestamp": int(item.get("timestamp", 0)),
        }
        for item in result
    ]
