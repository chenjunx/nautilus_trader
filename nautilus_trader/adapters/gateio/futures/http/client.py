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
from nautilus_trader.adapters.gateio.common.constants import GATEIO_FUTURES_HTTP_BASE_URL
from nautilus_trader.adapters.gateio.common.signing import gateio_rest_signature
from nautilus_trader.adapters.gateio.futures.http.models import GateIoAccountDetail
from nautilus_trader.adapters.gateio.futures.http.models import GateIoFuturesAccount
from nautilus_trader.adapters.gateio.futures.http.models import GateIoFuturesContract
from nautilus_trader.adapters.gateio.futures.http.models import GateIoFuturesOrder
from nautilus_trader.adapters.gateio.futures.http.models import GateIoFuturesPosition
from nautilus_trader.adapters.gateio.futures.http.models import GateIoFuturesTrade
from nautilus_trader.common.component import Logger
from nautilus_trader.core.nautilus_pyo3 import HttpClient
from nautilus_trader.core.nautilus_pyo3 import HttpMethod
from nautilus_trader.core.nautilus_pyo3 import HttpResponse
from nautilus_trader.core.nautilus_pyo3 import Quota


DEFAULT_QUOTA = Quota.rate_per_second(5)


class GateIoFuturesHttpClient:
    """
    Provides a Gate.io asynchronous HTTP client for futures market data and trading.

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

    BASE_URL = GATEIO_FUTURES_HTTP_BASE_URL

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
            keyed_quotas=keyed_quotas or [],
        )
        self._decoder_contracts = msgspec.json.Decoder(list[GateIoFuturesContract])
        self._decoder_order = msgspec.json.Decoder(GateIoFuturesOrder)
        self._decoder_orders = msgspec.json.Decoder(list[GateIoFuturesOrder])
        self._decoder_trades = msgspec.json.Decoder(list[GateIoFuturesTrade])
        self._decoder_positions = msgspec.json.Decoder(list[GateIoFuturesPosition])
        self._decoder_accounts = msgspec.json.Decoder(GateIoFuturesAccount)
        self._decoder_account_detail = msgspec.json.Decoder(GateIoAccountDetail)

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

    async def request_futures_contracts(self, settle: str = "usdt") -> list[GateIoFuturesContract]:
        """Fetch all futures contracts for the given settlement asset from Gate.io."""
        raw = await self._get(f"/futures/{settle}/contracts")
        return self._decoder_contracts.decode(raw)

    async def submit_order(
        self,
        settle: str,
        contract: str,
        size: int,
        price: str,
        tif: str,
        text: str,
        reduce_only: bool = False,
        close: bool = False,
    ) -> GateIoFuturesOrder:
        """Submit a new futures order. POST /futures/{settle}/orders."""
        body: dict[str, object] = {
            "contract": contract,
            "size": size,
            "price": price,
            "tif": tif,
            "text": text,
            "reduce_only": reduce_only,
            "close": close,
        }
        raw = await self._signed_request(HttpMethod.POST, f"/futures/{settle}/orders", body=body)
        return self._decoder_order.decode(raw)

    async def amend_order(
        self,
        settle: str,
        order_id: str,
        size: int | None = None,
        price: str | None = None,
    ) -> GateIoFuturesOrder:
        """Amend an open futures order. PUT /futures/{settle}/orders/{order_id}."""
        body: dict[str, object] = {}
        if size is not None:
            body["size"] = size
        if price is not None:
            body["price"] = price
        raw = await self._signed_request(
            HttpMethod.PUT,
            f"/futures/{settle}/orders/{order_id}",
            body=body,
        )
        return self._decoder_order.decode(raw)

    async def cancel_order(self, settle: str, order_id: str) -> GateIoFuturesOrder:
        """Cancel a single futures order. DELETE /futures/{settle}/orders/{order_id}."""
        raw = await self._signed_request(
            HttpMethod.DELETE,
            f"/futures/{settle}/orders/{order_id}",
        )
        return self._decoder_order.decode(raw)

    async def cancel_all_orders(
        self,
        settle: str,
        contract: str,
        side: str | None = None,
    ) -> list[GateIoFuturesOrder]:
        """Cancel all open futures orders for a contract. DELETE /futures/{settle}/orders."""
        query = {"contract": contract}
        if side is not None:
            query["side"] = side
        raw = await self._signed_request(
            HttpMethod.DELETE,
            f"/futures/{settle}/orders",
            query=query,
        )
        return self._decoder_orders.decode(raw)

    async def query_order(self, settle: str, order_id: str) -> GateIoFuturesOrder:
        """Query a single futures order. GET /futures/{settle}/orders/{order_id}."""
        raw = await self._signed_request(
            HttpMethod.GET,
            f"/futures/{settle}/orders/{order_id}",
        )
        return self._decoder_order.decode(raw)

    async def list_orders(
        self,
        settle: str,
        status: str,
        contract: str | None = None,
        limit: int = 100,
    ) -> list[GateIoFuturesOrder]:
        """List futures orders. GET /futures/{settle}/orders.

        If `contract` is None, returns orders across all contracts for `settle`.
        """
        query: dict[str, str] = {"status": status, "limit": str(limit)}
        if contract is not None:
            query["contract"] = contract
        raw = await self._signed_request(
            HttpMethod.GET,
            f"/futures/{settle}/orders",
            query=query,
        )
        return self._decoder_orders.decode(raw)

    async def list_my_trades(
        self,
        settle: str,
        contract: str | None = None,
        order_id: str | None = None,
        limit: int = 100,
    ) -> list[GateIoFuturesTrade]:
        """List futures trade fills. GET /futures/{settle}/my_trades."""
        query: dict[str, str] = {"limit": str(limit)}
        if contract is not None:
            query["contract"] = contract
        if order_id is not None:
            query["order"] = order_id
        raw = await self._signed_request(
            HttpMethod.GET,
            f"/futures/{settle}/my_trades",
            query=query,
        )
        return self._decoder_trades.decode(raw)

    async def list_positions(self, settle: str) -> list[GateIoFuturesPosition]:
        """List futures positions. GET /futures/{settle}/positions."""
        raw = await self._signed_request(HttpMethod.GET, f"/futures/{settle}/positions")
        return self._decoder_positions.decode(raw)

    async def list_futures_accounts(self, settle: str) -> GateIoFuturesAccount:
        """Query futures account balance. GET /futures/{settle}/accounts."""
        raw = await self._signed_request(HttpMethod.GET, f"/futures/{settle}/accounts")
        return self._decoder_accounts.decode(raw)

    async def get_account_detail(self) -> GateIoAccountDetail:
        """Query unified account details (including numeric `user_id`). GET /account/detail."""
        raw = await self._signed_request(HttpMethod.GET, "/account/detail")
        return self._decoder_account_detail.decode(raw)
