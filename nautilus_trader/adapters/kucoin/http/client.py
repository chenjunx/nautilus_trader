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

import nautilus_trader
from nautilus_trader.adapters.kucoin.http.models import KuCoinSymbol
from nautilus_trader.adapters.kucoin.http.models import KuCoinSymbolsResponse
from nautilus_trader.adapters.kucoin.http.models import KuCoinWsTokenData
from nautilus_trader.adapters.kucoin.http.models import KuCoinWsTokenResponse
from nautilus_trader.common.component import Logger
from nautilus_trader.core.nautilus_pyo3 import HttpClient
from nautilus_trader.core.nautilus_pyo3 import HttpMethod
from nautilus_trader.core.nautilus_pyo3 import HttpResponse


class KuCoinHttpClient:
    """
    Provides a KuCoin asynchronous HTTP client for public market data.

    Parameters
    ----------
    base_url : str, optional
        Override the default HTTP base URL.

    """

    BASE_URL = "https://api.kucoin.com"

    def __init__(self, base_url: str | None = None) -> None:
        self._log: Logger = Logger(type(self).__name__)
        self._base_url = base_url or self.BASE_URL
        self._headers: dict[str, str] = {
            "Content-Type": "application/json",
            "User-Agent": nautilus_trader.NAUTILUS_USER_AGENT,
        }
        self._client = HttpClient()
        self._decoder_symbols = msgspec.json.Decoder(KuCoinSymbolsResponse)
        self._decoder_token = msgspec.json.Decoder(KuCoinWsTokenResponse)

    async def _get(self, path: str) -> bytes:
        response: HttpResponse = await self._client.request(
            HttpMethod.GET,
            url=self._base_url + path,
            headers=self._headers,
            body=None,
        )
        if response.status >= 400:
            raise RuntimeError(
                f"KuCoin HTTP GET error {response.status} for {path}: "
                + response.body.decode(errors="replace")
            )
        return response.body

    async def _post(self, path: str) -> bytes:
        response: HttpResponse = await self._client.request(
            HttpMethod.POST,
            url=self._base_url + path,
            headers=self._headers,
            body=None,
        )
        if response.status >= 400:
            raise RuntimeError(
                f"KuCoin HTTP POST error {response.status} for {path}: "
                + response.body.decode(errors="replace")
            )
        return response.body

    async def request_spot_symbols(self) -> list[KuCoinSymbol]:
        """Fetch all spot trading symbols from KuCoin."""
        raw = await self._get("/api/v2/symbols")
        resp = self._decoder_symbols.decode(raw)
        if resp.code != "200000":
            raise RuntimeError(f"KuCoin API returned error code: {resp.code}")
        return resp.data

    async def request_ws_token(self) -> KuCoinWsTokenData:
        """Fetch a public WebSocket connection token from KuCoin."""
        raw = await self._post("/api/v1/bullet-public")
        resp = self._decoder_token.decode(raw)
        if resp.code != "200000":
            raise RuntimeError(f"KuCoin bullet-public returned error code: {resp.code}")
        return resp.data
