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
from nautilus_trader.adapters.gateio.common.constants import GATEIO_SPOT_HTTP_BASE_URL
from nautilus_trader.adapters.gateio.spot.http.models import GateIoCurrencyPair
from nautilus_trader.common.component import Logger
from nautilus_trader.core.nautilus_pyo3 import HttpClient
from nautilus_trader.core.nautilus_pyo3 import HttpMethod
from nautilus_trader.core.nautilus_pyo3 import HttpResponse


class GateIoSpotHttpClient:
    """
    Provides a Gate.io asynchronous HTTP client for public spot market data.

    Parameters
    ----------
    base_url : str, optional
        Override the default HTTP base URL.

    """

    BASE_URL = GATEIO_SPOT_HTTP_BASE_URL

    def __init__(self, base_url: str | None = None) -> None:
        self._log: Logger = Logger(type(self).__name__)
        self._base_url = base_url or self.BASE_URL
        self._headers: dict[str, str] = {
            "Content-Type": "application/json",
            "User-Agent": nautilus_trader.NAUTILUS_USER_AGENT,
        }
        self._client = HttpClient()
        self._decoder_pairs = msgspec.json.Decoder(list[GateIoCurrencyPair])

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

    async def request_spot_currency_pairs(self) -> list[GateIoCurrencyPair]:
        """Fetch all spot currency pairs from Gate.io."""
        raw = await self._get("/spot/currency_pairs")
        return self._decoder_pairs.decode(raw)
