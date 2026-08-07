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
"""Bybit 钱包相关的私有 REST 接口（V5 Asset：提现/充值地址/充提记录）。

Bybit 适配器主体（`data.py`/`execution.py`）完全基于 Rust/pyo3 实现（`BybitHttpClient`），
未覆盖 Asset 充提币接口，因此这里单独手搓签名请求，风格参考
`nautilus_trader/adapters/kraken/http/wallet.py`。

签名规则（V5，HMAC-SHA256）：
    sign = HMAC_SHA256(secret, timestamp_ms + api_key + recv_window + queryString_or_jsonBody)
GET 用排序前的原始 query string，POST 用发送的 JSON body 原始字符串，两者都必须和实际发出的
请求内容逐字节一致，否则签名校验失败。见 https://bybit-exchange.github.io/docs/v5/guide 。

重要操作前提（代码无法绕过，需要账户侧自行处理）：
- Bybit 提现地址通常需要先在网页端加入地址簿白名单，否则 API 提现会报错
  （如 131002 "Withdraw address chain or destination tag are not equal"）。
- 提现从 Funding 账户（accountType=FUND）出款，如果资产在 Unified Trading Account，
  需要先在 Bybit 内部转到 Funding 账户，否则提现会失败。
"""

import hashlib
import hmac
import json
import time
import urllib.parse

import httpx


BYBIT_HTTP_URL = "https://api.bybit.com"


def _bybit_signed_request(
    method: str,
    path: str,
    params: dict,
    api_key: str,
    api_secret: str,
    base_url: str = BYBIT_HTTP_URL,
    recv_window_ms: int = 5_000,
) -> dict:
    """向 Bybit V5 私有接口发起签名请求，返回 `result` 字段。

    GET 请求把 `params` 编码进 query string 参与签名；POST 请求把 `params` 序列化成 JSON body
    参与签名，两者必须和实际发出的请求内容完全一致。
    """
    timestamp = str(int(time.time() * 1000))
    recv_window = str(recv_window_ms)

    if method == "GET":
        query_string = urllib.parse.urlencode(params)
        payload = query_string
        url = f"{base_url}{path}"
        if query_string:
            url = f"{url}?{query_string}"
        body = None
    else:
        payload = json.dumps(params) if params else ""
        url = f"{base_url}{path}"
        body = payload

    sign_str = timestamp + api_key + recv_window + payload
    signature = hmac.new(
        api_secret.encode("utf-8"),
        sign_str.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type": "application/json",
    }

    if method == "GET":
        resp = httpx.get(url, headers=headers, timeout=10.0)
    else:
        resp = httpx.post(url, headers=headers, content=body, timeout=10.0)
    resp.raise_for_status()
    data = resp.json()
    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit {path} 返回错误: {data.get('retCode')} {data.get('retMsg')}")
    return data.get("result", {})


def fetch_bybit_coin_info(api_key: str, api_secret: str, coin: str) -> dict[str, dict]:
    """查询指定币种支持的提现/充值链及其手续费明细。GET /v5/asset/coin/query-info（签名接口）。

    返回 {原始 chain 代码: {"fee": 提现手续费, "min": 最小提现量,
    "enabled": 提现是否开放, "deposit_enabled": 充值是否开放}}。
    """
    result = _bybit_signed_request(
        "GET", "/v5/asset/coin/query-info", {"coin": coin.upper()}, api_key, api_secret,
    )
    rows = result.get("rows", [])
    if not rows:
        return {}

    chains: dict[str, dict] = {}
    for chain_info in rows[0].get("chains", []):
        chain = str(chain_info.get("chain", ""))
        if not chain:
            continue
        chains[chain] = {
            "fee": float(chain_info.get("withdrawFee") or 0),
            "min": float(chain_info.get("withdrawMin") or 0),
            "enabled": str(chain_info.get("chainWithdraw", "0")) == "1",
            "deposit_enabled": str(chain_info.get("chainDeposit", "0")) == "1",
        }
    return chains


def fetch_bybit_deposit_address(api_key: str, api_secret: str, coin: str, chain: str) -> dict[str, str]:
    """获取指定币种在指定链上的充值地址。GET /v5/asset/deposit/query-address（签名接口）。

    返回 {"address": 充值地址, "tag": memo/tag（不需要时为空字符串）}。
    """
    result = _bybit_signed_request(
        "GET",
        "/v5/asset/deposit/query-address",
        {"coin": coin.upper(), "chainType": chain},
        api_key,
        api_secret,
    )
    for chain_info in result.get("chains", []):
        if str(chain_info.get("chain", "")) == chain:
            return {
                "address": str(chain_info.get("addressDeposit", "")),
                "tag": str(chain_info.get("tagDeposit", "") or ""),
            }
    raise RuntimeError(f"Bybit 未返回 {coin} 在链 {chain} 上的充值地址")


def fetch_bybit_deposit_records(api_key: str, api_secret: str, coin: str, limit: int = 50) -> list[dict]:
    """查询充值到账记录。GET /v5/asset/deposit/query-record（签名接口）。"""
    result = _bybit_signed_request(
        "GET",
        "/v5/asset/deposit/query-record",
        {"coin": coin.upper(), "limit": limit},
        api_key,
        api_secret,
    )
    return result.get("rows", [])


def bybit_withdraw(
    api_key: str,
    api_secret: str,
    coin: str,
    chain: str,
    address: str,
    amount: float,
    tag: str | None = None,
    account_type: str = "FUND",
) -> str:
    """提交提现申请，返回提现单号。POST /v5/asset/withdraw/create（签名接口）。

    注意：目标地址通常需要先在 Bybit 网页端加入地址簿白名单，否则会报
    "Withdraw address chain or destination tag are not equal" 之类的错误；
    资金默认从 Funding 账户（accountType=FUND）出款。
    """
    params: dict = {
        "coin": coin.upper(),
        "chain": chain,
        "address": address,
        "amount": str(amount),
        "timestamp": int(time.time() * 1000),
        "forceChain": 0,
        "accountType": account_type,
    }
    if tag:
        params["tag"] = tag
    result = _bybit_signed_request(
        "POST", "/v5/asset/withdraw/create", params, api_key, api_secret,
    )
    return str(result["id"])


def fetch_bybit_withdraw_records(
    api_key: str,
    api_secret: str,
    coin: str,
    withdraw_id: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """查询提现记录。GET /v5/asset/withdraw/query-record（签名接口）。"""
    params: dict = {"coin": coin.upper(), "limit": limit}
    if withdraw_id:
        params["withdrawID"] = withdraw_id
    result = _bybit_signed_request(
        "GET", "/v5/asset/withdraw/query-record", params, api_key, api_secret,
    )
    return result.get("rows", [])
