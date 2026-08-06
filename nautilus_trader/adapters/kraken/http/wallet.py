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
"""Kraken 钱包相关的私有 REST 接口（提现/充值/费率查询）。

这些接口已从 examples/live/spread_monitor/kraken_api.py 迁移到框架适配器层，
供所有策略复用。签名逻辑统一在 `_kraken_private_post` 中实现。
"""

import base64
import hashlib
import hmac
import time
import urllib.parse
from collections import defaultdict

import httpx


def _kraken_private_post(path: str, extra_params: dict, api_key: str, api_secret: str) -> dict:
    """向 Kraken 私有接口发起签名 POST 请求，返回 `result` 字段。

    签名方案: HMAC-SHA512(path + SHA256(nonce + postdata), secret)，Kraken 私有接口通用。
    """
    nonce = str(int(time.time() * 1000))
    params = {"nonce": nonce, **extra_params}
    postdata = urllib.parse.urlencode(params)
    sha = hashlib.sha256((nonce + postdata).encode()).digest()
    sig = base64.b64encode(
        hmac.new(base64.b64decode(api_secret), path.encode() + sha, hashlib.sha512).digest(),
    ).decode()
    headers = {"API-Key": api_key, "API-Sign": sig}
    resp = httpx.post(
        f"https://api.kraken.com{path}",
        data=params,
        headers=headers,
        timeout=10.0,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("error"):
        raise RuntimeError(f"Kraken {path} 返回错误: {body['error']}")
    return body.get("result", {})


def fetch_kraken_withdraw_methods(api_key: str, api_secret: str) -> dict[str, set[str]]:
    """拉取 Kraken 每个币种支持的提现网络。POST /0/private/WithdrawMethods（签名接口）。

    返回 {base: {归一化链名}}，供链匹配逻辑使用。链名归一化由调用方自行处理。
    """
    rows = _kraken_private_post("/0/private/WithdrawMethods", {}, api_key, api_secret)

    result: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        base = str(row.get("asset", "")).upper()
        # 不在这里做 _normalize_chain，保持框架层职责单一，归一化由调用方处理
        method = str(row.get("network") or row.get("method", ""))
        if base and method:
            result[base].add(method)
    return dict(result)


def fetch_kraken_trade_fees(api_key: str, api_secret: str, pairs: set[str]) -> dict[str, float]:
    """拉取 Kraken 指定交易对的账户真实 taker 费率（按 30 天成交量分档）。

    POST /0/private/TradeVolume（签名接口）。不传 pair 参数该接口不返回 fees，
    因此必须传入实际用到的 pair 名称列表（即 instrument.raw_symbol）。

    返回 {pair: taker_fee_decimal}，如 {"XBTUSD": 0.0026}。
    """
    result = _kraken_private_post(
        "/0/private/TradeVolume",
        {"pair": ",".join(sorted(pairs))},
        api_key,
        api_secret,
    )
    fees = result.get("fees", {})
    return {pair: float(info["fee"]) / 100 for pair, info in fees.items() if "fee" in info}


def fetch_kraken_deposit_methods(api_key: str, api_secret: str, asset: str) -> list[dict]:
    """拉取 Kraken 某币种支持的入金方式。POST /0/private/DepositMethods（签名接口）。

    返回原始 API 响应（list of dict），供调用方按需提取 method 字段。
    """
    return _kraken_private_post(
        "/0/private/DepositMethods",
        {"asset": asset.upper()},
        api_key,
        api_secret,
    )


def fetch_kraken_deposit_addresses(
    api_key: str,
    api_secret: str,
    asset: str,
    method: str,
    new: bool = False,
) -> list[dict]:
    """获取 Kraken 某币种在指定入金方式下的充值地址，没有已存在地址时可新建一个。

    POST /0/private/DepositAddresses（签名接口）。

    返回原始 API 响应（list of dict，通常取第一条），每条包含 address/tag 字段。
    """
    params = {"asset": asset.upper(), "method": method}
    if new:
        params["new"] = "true"
    return _kraken_private_post("/0/private/DepositAddresses", params, api_key, api_secret)


def fetch_kraken_deposit_status(api_key: str, api_secret: str, asset: str) -> list[dict]:
    """查询 Kraken 入金到账记录。POST /0/private/DepositStatus（签名接口）。

    该接口无法直接按提现单号关联，调用方需要按金额（容差内）+ 时间新旧自行匹配。
    """
    return _kraken_private_post(
        "/0/private/DepositStatus",
        {"asset": asset.upper()},
        api_key,
        api_secret,
    )
