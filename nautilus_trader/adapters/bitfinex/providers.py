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

from decimal import Decimal

from nautilus_trader.adapters.bitfinex.constants import BITFINEX_VENUE
from nautilus_trader.adapters.bitfinex.http.client import BitfinexHttpClient
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.currencies import Currency
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments.currency_pair import CurrencyPair
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class BitfinexInstrumentProvider(InstrumentProvider):
    """
    Provides Bitfinex spot instruments via the public REST API.

    Parameters
    ----------
    http_client : BitfinexHttpClient
        The HTTP client for the provider.
    clock : LiveClock
        The clock for the provider.
    config : InstrumentProviderConfig, optional
        The configuration for the provider.

    """

    def __init__(
        self,
        http_client: BitfinexHttpClient,
        clock: LiveClock,
        config: InstrumentProviderConfig | None = None,
    ) -> None:
        super().__init__(config=config)
        self._http_client = http_client
        self._clock = clock

    async def load_all_async(self, filters: dict | None = None) -> None:
        """Load all Bitfinex spot instruments from the REST API."""
        self._log.info("Loading all Bitfinex spot instruments...")
        pairs = await self._http_client.request_spot_pairs()
        loaded = 0
        skipped = 0
        for pair in pairs:
            try:
                instrument = self._parse_instrument(pair)
                self.add_currency(currency=instrument.base_currency)
                self.add_currency(currency=instrument.quote_currency)
                self.add(instrument=instrument)
                loaded += 1
            except Exception as e:
                self._log.warning(f"Skipping pair {pair!r}: {e}")
                skipped += 1
        self._log.info(f"Loaded {loaded} instruments, skipped {skipped}")

    def _parse_symbol(self, pair: str) -> tuple[str, str]:
        """
        Split a Bitfinex pair string into (base, quote).

        Bitfinex uses two formats:
        - 6-character: ``"BTCUSD"``  → base=``"BTC"``, quote=``"USD"``
        - Colon-separated: ``"BTC:UDC"`` → base=``"BTC"``, quote=``"UDC"``

        """
        if ":" in pair:
            base, quote = pair.split(":", 1)
            return base, quote
        # Standard 6-character pairs: first 3 = base, last 3 = quote
        return pair[:3], pair[3:]

    def _parse_instrument(self, pair: str) -> CurrencyPair:
        """
        Build a :class:`CurrencyPair` from a raw Bitfinex pair string.

        Parameters
        ----------
        pair : str
            The pair symbol as returned by the REST API, e.g. ``"BTCUSD"`` or
            ``"BTC:UDC"``.

        """
        base_code, quote_code = self._parse_symbol(pair)

        # Nautilus symbol uses the raw pair string (no colon variant normalisation)
        raw_symbol = Symbol(pair)
        instrument_id = InstrumentId(symbol=raw_symbol, venue=BITFINEX_VENUE)

        # Bitfinex supports up to 8 decimal places; use unified defaults
        price_precision = 8
        size_precision = 8
        price_increment = Price(
            float(Decimal(10) ** -price_precision),
            precision=price_precision,
        )
        size_increment = Quantity(
            float(Decimal(10) ** -size_precision),
            precision=size_precision,
        )

        ts_now = self._clock.timestamp_ns()

        return CurrencyPair(
            instrument_id=instrument_id,
            raw_symbol=raw_symbol,
            base_currency=Currency.from_str(base_code),
            quote_currency=Currency.from_str(quote_code),
            price_precision=price_precision,
            size_precision=size_precision,
            price_increment=price_increment,
            size_increment=size_increment,
            maker_fee=Decimal("0.001"),  # Bitfinex standard maker 0.1%
            taker_fee=Decimal("0.002"),  # Bitfinex standard taker 0.2%
            ts_event=ts_now,
            ts_init=ts_now,
        )
