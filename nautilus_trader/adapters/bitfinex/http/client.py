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

import time

import msgspec

import nautilus_trader
from nautilus_trader.adapters.bitfinex.common.signing import bitfinex_rest_signature
from nautilus_trader.adapters.bitfinex.constants import BITFINEX_HTTP_AUTH_BASE_URL
from nautilus_trader.adapters.bitfinex.http.models import BitfinexOrder
from nautilus_trader.adapters.bitfinex.http.models import BitfinexPosition
from nautilus_trader.adapters.bitfinex.http.models import BitfinexTrade
from nautilus_trader.adapters.bitfinex.http.models import BitfinexWallet
from nautilus_trader.adapters.bitfinex.http.models import pair_list_decoder
from nautilus_trader.common.component import Logger
from nautilus_trader.core.nautilus_pyo3 import HttpClient
from nautilus_trader.core.nautilus_pyo3 import HttpMethod
from nautilus_trader.core.nautilus_pyo3 import HttpResponse
from nautilus_trader.core.nautilus_pyo3 import Quota


DEFAULT_QUOTA = Quota.rate_per_second(10)


class BitfinexHttpClient:
    """
    Provides a Bitfinex asynchronous HTTP client for public market data, and (when
    `api_key`/`api_secret` are supplied) signed trading and account endpoints.

    Parameters
    ----------
    api_key : str, optional
        The Bitfinex API key. Required for signed (private) requests.
    api_secret : str, optional
        The Bitfinex API secret. Required for signed (private) requests.
    base_url : str, optional
        Override the default public HTTP base URL.
    base_url_auth : str, optional
        Override the default authenticated HTTP base URL.
    default_quota : Quota, optional
        The default rate limit quota applied to all requests.

    """

    BASE_URL = "https://api-pub.bitfinex.com/v2"

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        base_url: str | None = None,
        base_url_auth: str | None = None,
        default_quota: Quota | None = None,
    ) -> None:
        self._log: Logger = Logger(type(self).__name__)
        self._base_url = base_url or self.BASE_URL
        self._base_url_auth = base_url_auth or BITFINEX_HTTP_AUTH_BASE_URL
        self._api_key = api_key
        self._api_secret = api_secret
        self._headers: dict[str, str] = {
            "Content-Type": "application/json",
            "User-Agent": nautilus_trader.NAUTILUS_USER_AGENT,
        }
        self._client = HttpClient(default_quota=default_quota or DEFAULT_QUOTA)
        self._decoder_orders = msgspec.json.Decoder(list[BitfinexOrder])
        self._decoder_wallets = msgspec.json.Decoder(list[BitfinexWallet])
        self._decoder_positions = msgspec.json.Decoder(list[BitfinexPosition])
        self._decoder_trades = msgspec.json.Decoder(list[BitfinexTrade])

    async def _get(self, path: str) -> bytes:
        response: HttpResponse = await self._client.request(
            HttpMethod.GET,
            url=self._base_url + path,
            headers=self._headers,
            body=None,
        )
        if response.status >= 400:
            raise RuntimeError(
                f"Bitfinex HTTP error {response.status} for {path}: "
                + response.body.decode(errors="replace")
            )
        return response.body

    async def request_spot_pairs(self) -> list[str]:
        """
        Fetch the list of all spot trading pairs from Bitfinex.

        Returns
        -------
        list[str]
            Pair symbols such as ``"BTCUSD"``, ``"BTC:UDC"``.

        """
        raw = await self._get("/conf/pub:list:pair:exchange")
        outer = pair_list_decoder.decode(raw)  # list[list[str]]
        return outer[0]  # only element: the inner list of pair strings

    async def request_derivative_pairs(self) -> list[str]:
        """
        Fetch the list of all derivative (perpetual futures) trading pairs from Bitfinex.

        Returns
        -------
        list[str]
            Pair symbols such as ``"BTCF0:USTF0"``. Includes non-crypto (index/commodity)
            and non-USDT-margined pairs; callers should filter as needed.

        """
        raw = await self._get("/conf/pub:list:pair:futures")
        outer = pair_list_decoder.decode(raw)  # list[list[str]]
        return outer[0]  # only element: the inner list of pair strings

    async def _signed_request(self, path: str, body: dict | None = None) -> bytes:
        if not self._api_key or not self._api_secret:
            raise RuntimeError("Bitfinex API key/secret not configured for signed request")

        nonce = str(time.time_ns() // 1_000)  # microsecond nonce, must be strictly increasing
        body_str = msgspec.json.encode(body or {}).decode()
        full_path = f"v2/auth/{path.lstrip('/')}"

        headers = dict(self._headers)
        headers.update(
            {
                "bfx-apikey": self._api_key,
                "bfx-nonce": nonce,
                "bfx-signature": bitfinex_rest_signature(
                    full_path,
                    nonce,
                    body_str,
                    self._api_secret,
                ),
            },
        )

        response: HttpResponse = await self._client.request(
            HttpMethod.POST,
            url=f"{self._base_url_auth}/auth/{path.lstrip('/')}",
            headers=headers,
            body=body_str.encode(),
        )
        if response.status >= 400:
            raise RuntimeError(
                f"Bitfinex HTTP error {response.status} for {path}: "
                + response.body.decode(errors="replace")
            )
        return response.body

    def _decode_notification_order(self, raw: bytes) -> BitfinexOrder:
        # Order write endpoints wrap the order in a notification envelope:
        # [MTS, TYPE, MESSAGE_ID, null, ORDER_ARRAY, CODE, STATUS, TEXT]
        notification = msgspec.json.decode(raw)
        status, text, payload = notification[6], notification[7], notification[4]
        if status != "SUCCESS" or payload is None:
            raise RuntimeError(f"Bitfinex order request failed: {status} {text}")
        return msgspec.convert(payload, type=BitfinexOrder)

    def _decode_notification_orders(self, raw: bytes) -> list[BitfinexOrder]:
        # Multi-order write endpoints wrap a list of order arrays in the same envelope.
        notification = msgspec.json.decode(raw)
        status, text, payload = notification[6], notification[7], notification[4]
        if status != "SUCCESS" or payload is None:
            raise RuntimeError(f"Bitfinex order request failed: {status} {text}")
        return msgspec.convert(payload, type=list[BitfinexOrder])

    async def submit_order(self, **params: object) -> BitfinexOrder:
        """Submit a new order. POST auth/w/order/submit."""
        raw = await self._signed_request("w/order/submit", body=params)
        return self._decode_notification_order(raw)

    async def update_order(self, **params: object) -> BitfinexOrder:
        """Update (amend) an existing order. POST auth/w/order/update."""
        raw = await self._signed_request("w/order/update", body=params)
        return self._decode_notification_order(raw)

    async def cancel_order(self, **params: object) -> BitfinexOrder:
        """Cancel a single order by ``id`` or ``cid``/``cid_date``. POST auth/w/order/cancel."""
        raw = await self._signed_request("w/order/cancel", body=params)
        return self._decode_notification_order(raw)

    async def cancel_all_orders(self, symbol: str | None = None) -> list[BitfinexOrder]:
        """Cancel all open orders (optionally filtered by symbol). POST auth/w/order/cancel/multi."""
        body: dict[str, object] = {"all": 1} if symbol is None else {"symbol": [symbol]}
        raw = await self._signed_request("w/order/cancel/multi", body=body)
        return self._decode_notification_orders(raw)

    async def batch_cancel_orders(self, order_ids: list[int]) -> list[BitfinexOrder]:
        """Cancel a batch of orders by ID. POST auth/w/order/cancel/multi."""
        raw = await self._signed_request("w/order/cancel/multi", body={"id": order_ids})
        return self._decode_notification_orders(raw)

    async def list_active_orders(self, symbol: str | None = None) -> list[BitfinexOrder]:
        """List active orders (optionally filtered by symbol). POST auth/r/orders."""
        path = f"r/orders/{symbol}" if symbol is not None else "r/orders"
        raw = await self._signed_request(path)
        return self._decoder_orders.decode(raw)

    async def list_orders_history(
        self,
        symbol: str | None = None,
        limit: int = 100,
    ) -> list[BitfinexOrder]:
        """List historical (closed) orders. POST auth/r/orders/hist."""
        path = f"r/orders/{symbol}/hist" if symbol is not None else "r/orders/hist"
        raw = await self._signed_request(path, body={"limit": limit})
        return self._decoder_orders.decode(raw)

    async def list_wallets(self) -> list[BitfinexWallet]:
        """List all wallet balances. POST auth/r/wallets."""
        raw = await self._signed_request("r/wallets")
        return self._decoder_wallets.decode(raw)

    async def list_positions(self) -> list[BitfinexPosition]:
        """List all open derivative positions. POST auth/r/positions."""
        raw = await self._signed_request("r/positions")
        return self._decoder_positions.decode(raw)

    async def list_trades_history(
        self,
        symbol: str | None = None,
        limit: int = 100,
    ) -> list[BitfinexTrade]:
        """List trade fills (optionally filtered by symbol). POST auth/r/trades/hist."""
        path = f"r/trades/{symbol}/hist" if symbol is not None else "r/trades/hist"
        raw = await self._signed_request(path, body={"limit": limit})
        return self._decoder_trades.decode(raw)
