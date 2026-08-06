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
import urllib.parse

import msgspec

import nautilus_trader
from nautilus_trader.adapters.gateio.common.constants import GATEIO_SPOT_HTTP_BASE_URL
from nautilus_trader.adapters.gateio.common.signing import gateio_rest_signature
from nautilus_trader.adapters.gateio.spot.http.models import GateIoBalance
from nautilus_trader.adapters.gateio.spot.http.models import GateIoCurrencyPair
from nautilus_trader.adapters.gateio.spot.http.models import GateIoOpenOrders
from nautilus_trader.adapters.gateio.spot.http.models import GateIoOrder
from nautilus_trader.adapters.gateio.spot.http.models import GateIoTrade
from nautilus_trader.common.component import Logger
from nautilus_trader.core.nautilus_pyo3 import HttpClient
from nautilus_trader.core.nautilus_pyo3 import HttpMethod
from nautilus_trader.core.nautilus_pyo3 import HttpResponse
from nautilus_trader.core.nautilus_pyo3 import Quota


DEFAULT_QUOTA = Quota.rate_per_second(5)


class GateIoSpotHttpClient:
    """
    Provides a Gate.io asynchronous HTTP client for spot market data and trading.

    Parameters
    ----------
    api_key : str, optional
        The Gate.io API key. Required for signed (private) requests.
    api_secret : str, optional
        The Gate.io API secret. Required for signed (private) requests.
    base_url : str, optional
        Override the default HTTP base URL.
    default_quota : Quota, optional
        The default rate limit quota applied to all requests. Defaults to a
        conservative 5 requests/second if not provided.
    keyed_quotas : list[tuple[str, Quota]], optional
        Per-endpoint rate limit quota overrides.

    """

    BASE_URL = GATEIO_SPOT_HTTP_BASE_URL

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        base_url: str | None = None,
        default_quota: Quota | None = None,
        keyed_quotas: list[tuple[str, Quota]] | None = None,
    ) -> None:
        self._log: Logger = Logger(type(self).__name__)
        self._base_url = base_url or self.BASE_URL
        self._api_key = api_key
        self._api_secret = api_secret
        self._headers: dict[str, str] = {
            "Content-Type": "application/json",
            "User-Agent": nautilus_trader.NAUTILUS_USER_AGENT,
        }
        self._client = HttpClient(
            default_quota=default_quota or DEFAULT_QUOTA,
            keyed_quotas=keyed_quotas,
        )
        self._decoder_pairs = msgspec.json.Decoder(list[GateIoCurrencyPair])
        self._decoder_order = msgspec.json.Decoder(GateIoOrder)
        self._decoder_orders = msgspec.json.Decoder(list[GateIoOrder])
        self._decoder_trades = msgspec.json.Decoder(list[GateIoTrade])
        self._decoder_balances = msgspec.json.Decoder(list[GateIoBalance])
        self._decoder_open_orders = msgspec.json.Decoder(list[GateIoOpenOrders])

    async def _get(self, path: str) -> bytes:
        response: HttpResponse = await self._client.request(
            HttpMethod.GET,
            url=self._base_url + path,
            headers=self._headers,
            body=None,
        )
        if response.status >= 400:
            raise RuntimeError(
                f"Gate.io HTTP error {response.status} for {path}: "
                + response.body.decode(errors="replace")
            )
        return response.body

    async def _signed_request(
        self,
        method: HttpMethod,
        path: str,
        query: dict[str, str] | None = None,
        body: object = None,
    ) -> bytes:
        if not self._api_key or not self._api_secret:
            raise RuntimeError("Gate.io API key/secret not configured for signed request")

        query_string = urllib.parse.urlencode(query or {})
        body_str = msgspec.json.encode(body).decode() if body is not None else ""
        timestamp = str(int(time.time()))

        headers = dict(self._headers)
        headers.update(
            gateio_rest_signature(
                method=method.value,
                path=f"/api/v4{path}",
                query_string=query_string,
                body=body_str,
                api_key=self._api_key,
                api_secret=self._api_secret,
                timestamp=timestamp,
            ),
        )

        url = self._base_url + path
        if query_string:
            url += f"?{query_string}"

        response: HttpResponse = await self._client.request(
            method,
            url=url,
            headers=headers,
            body=body_str.encode() if body_str else None,
        )
        if response.status >= 400:
            raise RuntimeError(
                f"Gate.io HTTP error {response.status} for {path}: "
                + response.body.decode(errors="replace")
            )
        return response.body

    async def request_spot_currency_pairs(self) -> list[GateIoCurrencyPair]:
        """Fetch all spot currency pairs from Gate.io."""
        raw = await self._get("/spot/currency_pairs")
        return self._decoder_pairs.decode(raw)

    async def submit_order(
        self,
        currency_pair: str,
        side: str,
        order_type: str,
        amount: str,
        time_in_force: str,
        text: str,
        price: str | None = None,
    ) -> GateIoOrder:
        """Submit a new spot order. POST /spot/orders."""
        body: dict[str, object] = {
            "currency_pair": currency_pair,
            "type": order_type,
            "account": "spot",
            "side": side,
            "amount": amount,
            "time_in_force": time_in_force,
            "text": text,
        }
        if price is not None:
            body["price"] = price
        raw = await self._signed_request(HttpMethod.POST, "/spot/orders", body=body)
        return self._decoder_order.decode(raw)

    async def cancel_order(self, order_id: str, currency_pair: str) -> GateIoOrder:
        """Cancel a single spot order. DELETE /spot/orders/{order_id}."""
        raw = await self._signed_request(
            HttpMethod.DELETE,
            f"/spot/orders/{order_id}",
            query={"currency_pair": currency_pair},
        )
        return self._decoder_order.decode(raw)

    async def cancel_all_open_orders(
        self,
        currency_pair: str,
        side: str | None = None,
    ) -> list[GateIoOrder]:
        """Cancel all open spot orders for a currency pair. DELETE /spot/orders."""
        query = {"currency_pair": currency_pair}
        if side is not None:
            query["side"] = side
        raw = await self._signed_request(HttpMethod.DELETE, "/spot/orders", query=query)
        return self._decoder_orders.decode(raw)

    async def batch_cancel_orders(
        self,
        cancels: list[dict[str, str]],
    ) -> list[GateIoOrder]:
        """Batch cancel spot orders by ID. POST /spot/cancel_batch_orders."""
        raw = await self._signed_request(
            HttpMethod.POST,
            "/spot/cancel_batch_orders",
            body=cancels,
        )
        return self._decoder_orders.decode(raw)

    async def query_order(self, order_id: str, currency_pair: str) -> GateIoOrder:
        """Query a single spot order. GET /spot/orders/{order_id}."""
        raw = await self._signed_request(
            HttpMethod.GET,
            f"/spot/orders/{order_id}",
            query={"currency_pair": currency_pair},
        )
        return self._decoder_order.decode(raw)

    async def list_orders(
        self,
        currency_pair: str,
        status: str,
        limit: int = 100,
    ) -> list[GateIoOrder]:
        """List spot orders for a currency pair. GET /spot/orders."""
        raw = await self._signed_request(
            HttpMethod.GET,
            "/spot/orders",
            query={"currency_pair": currency_pair, "status": status, "limit": str(limit)},
        )
        return self._decoder_orders.decode(raw)

    async def list_open_orders(
        self,
        account: str | None = None,
        limit: int = 100,
    ) -> list[GateIoOpenOrders]:
        """List all open spot orders across all currency pairs. GET /spot/open_orders."""
        query: dict[str, str] = {"limit": str(limit)}
        if account is not None:
            query["account"] = account
        raw = await self._signed_request(HttpMethod.GET, "/spot/open_orders", query=query)
        return self._decoder_open_orders.decode(raw)

    async def list_my_trades(
        self,
        currency_pair: str | None = None,
        order_id: str | None = None,
        limit: int = 100,
    ) -> list[GateIoTrade]:
        """List spot trade fills. GET /spot/my_trades."""
        query: dict[str, str] = {"limit": str(limit)}
        if currency_pair is not None:
            query["currency_pair"] = currency_pair
        if order_id is not None:
            query["order_id"] = order_id
        raw = await self._signed_request(HttpMethod.GET, "/spot/my_trades", query=query)
        return self._decoder_trades.decode(raw)

    async def list_spot_accounts(self, currency: str | None = None) -> list[GateIoBalance]:
        """List spot account balances. GET /spot/accounts."""
        query = {"currency": currency} if currency is not None else None
        raw = await self._signed_request(HttpMethod.GET, "/spot/accounts", query=query)
        return self._decoder_balances.decode(raw)
