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
from nautilus_trader.adapters.mexc.http.models import MexcExchangeInfo
from nautilus_trader.common.component import Logger
from nautilus_trader.core.nautilus_pyo3 import HttpClient
from nautilus_trader.core.nautilus_pyo3 import HttpMethod
from nautilus_trader.core.nautilus_pyo3 import HttpResponse


class MexcHttpClient:
    """
    Provides a MEXC asynchronous HTTP client for public market data.

    Parameters
    ----------
    base_url : str, optional
        Override the default HTTP base URL.

    """

    BASE_URL = "https://api.mexc.com"

    def __init__(self, base_url: str | None = None) -> None:
        self._log: Logger = Logger(type(self).__name__)
        self._base_url = base_url or self.BASE_URL
        self._headers: dict[str, str] = {
            "Content-Type": "application/json",
            "User-Agent": nautilus_trader.NAUTILUS_USER_AGENT,
        }
        self._client = HttpClient()
        self._decoder_exchange_info = msgspec.json.Decoder(MexcExchangeInfo)

    async def _get(self, path: str) -> bytes:
        response: HttpResponse = await self._client.request(
            HttpMethod.GET,
            url=self._base_url + path,
            headers=self._headers,
            body=None,
        )
        if response.status >= 400:
            raise RuntimeError(
                f"MEXC HTTP error {response.status} for {path}: "
                + response.body.decode(errors="replace")
            )
        return response.body

    async def request_exchange_info(self) -> MexcExchangeInfo:
        """Fetch exchange info (all spot symbols) from MEXC."""
        raw = await self._get("/api/v3/exchangeInfo")
        return self._decoder_exchange_info.decode(raw)
