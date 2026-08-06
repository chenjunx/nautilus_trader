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

import msgspec

from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceSecurityType
from nautilus_trader.adapters.binance.common.symbol import BinanceSymbol
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient
from nautilus_trader.adapters.binance.http.endpoint import BinanceHttpEndpoint
from nautilus_trader.adapters.binance.spot.schemas.wallet import BinanceSpotTradeFee
from nautilus_trader.common.component import LiveClock
from nautilus_trader.core.nautilus_pyo3 import HttpMethod


class BinanceSpotTradeFeeHttp(BinanceHttpEndpoint):
    """
    Endpoint of maker/taker trade fee information.

    `GET /sapi/v1/asset/tradeFee` (Binance.com)
    `GET /sapi/v1/asset/query/trading-fee` (Binance.US)

    References
    ----------
    https://binance-docs.github.io/apidocs/spot/en/#trade-fee-user_data

    """

    def __init__(
        self,
        client: BinanceHttpClient,
        base_endpoint: str,
    ):
        methods = {
            HttpMethod.GET: BinanceSecurityType.USER_DATA,
        }

        # Check if Binance.US based on base URL
        if ".us" in client.base_url:
            endpoint_path = base_endpoint + "query/trading-fee"
        else:
            endpoint_path = base_endpoint + "tradeFee"

        super().__init__(
            client,
            methods,
            endpoint_path,
        )
        self._get_arr_resp_decoder = msgspec.json.Decoder(list[BinanceSpotTradeFee])

    class GetParameters(msgspec.Struct, omit_defaults=True, frozen=True):
        """
        GET parameters for requesting trade fees.

        Parameters
        ----------
        symbol : BinanceSymbol
            Optional symbol to receive individual trade fee
        recvWindow : str
            Optional number of milliseconds after timestamp the request is valid
        timestamp : str
            Millisecond timestamp of the request

        """

        timestamp: str
        symbol: BinanceSymbol | None = None
        recvWindow: str | None = None

    async def get(self, params: GetParameters) -> list[BinanceSpotTradeFee]:
        method_type = HttpMethod.GET
        raw = await self._method(method_type, params)
        return self._get_arr_resp_decoder.decode(raw)


class BinanceSpotWalletHttpAPI:
    """
    Provides access to the Binance Spot/Margin Wallet HTTP REST API.

    Parameters
    ----------
    client : BinanceHttpClient
        The Binance REST API client.

    """

    def __init__(
        self,
        client: BinanceHttpClient,
        clock: LiveClock,
        account_type: BinanceAccountType = BinanceAccountType.SPOT,
    ):
        self.client = client
        self._clock = clock
        self.base_endpoint = "/sapi/v1/asset/"

        if not account_type.is_spot_or_margin:
            raise RuntimeError(  # pragma: no cover (design-time error)
                f"`BinanceAccountType` not SPOT, MARGIN or ISOLATED_MARGIN, was {account_type}",  # pragma: no cover
            )

        self._endpoint_spot_trade_fee = BinanceSpotTradeFeeHttp(client, self.base_endpoint)

    def _timestamp(self) -> str:
        """
        Create Binance timestamp from internal clock.
        """
        return str(self._clock.timestamp_ms())

    async def query_spot_trade_fees(
        self,
        symbol: str | None = None,
        recv_window: str | None = None,
    ) -> list[BinanceSpotTradeFee]:
        fees = await self._endpoint_spot_trade_fee.get(
            params=self._endpoint_spot_trade_fee.GetParameters(
                timestamp=self._timestamp(),
                symbol=BinanceSymbol(symbol) if symbol is not None else None,
                recvWindow=recv_window,
            ),
        )
        return fees


# ==============================================================================
# 钱包管理（提现/充值）同步函数 - 供策略层直接调用
# ==============================================================================

import hashlib
import hmac
import time
import urllib.parse

import httpx


def _binance_sign_query(query: str, secret: str) -> str:
    """Binance 签名接口通用的 HMAC-SHA256 query 签名。"""
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()


def fetch_binance_withdraw_chain_details(api_key: str, api_secret: str) -> dict[str, dict[str, dict]]:
    """拉取 Binance 每个币种的提现网络明细（含手续费/最小提现量/是否开放提现）。

    GET /sapi/v1/capital/config/getall（签名接口）。

    返回 {base: {链名: {"fee": float, "min": float, "enabled": bool}}}。
    链名归一化由调用方自行处理（保持框架层职责单一）。
    """
    ts = int(time.time() * 1000)
    query = f"timestamp={ts}"
    sig = _binance_sign_query(query, api_secret)
    url = f"https://api.binance.com/sapi/v1/capital/config/getall?{query}&signature={sig}"
    resp = httpx.get(url, headers={"X-MBX-APIKEY": api_key}, timeout=10.0)
    resp.raise_for_status()

    result: dict[str, dict[str, dict]] = {}
    for coin in resp.json():
        base = str(coin.get("coin", "")).upper()
        networks: dict[str, dict] = {}
        for n in coin.get("networkList", []):
            network = str(n.get("network", ""))
            if not network:
                continue
            networks[network] = {
                "fee": float(n.get("withdrawFee", 0.0) or 0.0),
                "min": float(n.get("withdrawMin", 0.0) or 0.0),
                "enabled": bool(n.get("withdrawEnable", False)),
            }
        if base and networks:
            result[base] = networks
    return result


def binance_withdraw(
    api_key: str,
    api_secret: str,
    coin: str,
    network: str,
    address: str,
    amount: float,
    address_tag: str | None = None,
) -> str:
    """提交 Binance 提现申请，返回 withdrawal id。

    POST /sapi/v1/capital/withdraw/apply（签名接口）。

    调用方必须把这里抛出的任何异常（超时/网络错误/HTTP 错误）都当作"提现结果未知"
    而不是"提现失败"处理——绝不能因为这里出错就自动重发一次提现，必须先人工核实
    交易所网页端是否已经提交成功，否则有双花风险。
    """
    params = {
        "coin": coin.upper(),
        "network": network,
        "address": address,
        "amount": str(amount),
        "timestamp": str(int(time.time() * 1000)),
    }
    if address_tag:
        params["addressTag"] = address_tag

    query = urllib.parse.urlencode(params)
    sig = _binance_sign_query(query, api_secret)
    url = f"https://api.binance.com/sapi/v1/capital/withdraw/apply?{query}&signature={sig}"
    resp = httpx.post(url, headers={"X-MBX-APIKEY": api_key}, timeout=10.0)
    resp.raise_for_status()
    body = resp.json()
    return str(body["id"])


def binance_withdraw_status(api_key: str, api_secret: str, coin: str, withdrawal_id: str) -> dict | None:
    """查询指定提现单的状态。

    GET /sapi/v1/capital/withdraw/history（签名接口）。

    返回该提现单的详情，找不到时返回 None。
    """
    params = {"coin": coin.upper(), "timestamp": str(int(time.time() * 1000))}
    query = urllib.parse.urlencode(params)
    sig = _binance_sign_query(query, api_secret)
    url = f"https://api.binance.com/sapi/v1/capital/withdraw/history?{query}&signature={sig}"
    resp = httpx.get(url, headers={"X-MBX-APIKEY": api_key}, timeout=10.0)
    resp.raise_for_status()
    for row in resp.json():
        if str(row.get("id")) == str(withdrawal_id):
            return row
    return None
