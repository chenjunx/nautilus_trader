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

import msgspec

from nautilus_trader.adapters.gateio.common.constants import GATEIO_VENUE
from nautilus_trader.adapters.gateio.spot.http.client import GateIoSpotHttpClient
from nautilus_trader.adapters.gateio.spot.http.models import GateIoCurrencyPair
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.currencies import Currency
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments.currency_pair import CurrencyPair
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class GateIoSpotInstrumentProvider(InstrumentProvider):
    """
    Provides Gate.io spot instruments via the public REST API.

    Parameters
    ----------
    http_client : GateIoSpotHttpClient
        The HTTP client for the provider.
    clock : LiveClock
        The clock for the provider.
    venue : Venue, default GATEIO_VENUE
        The venue to assign to loaded instruments.
    config : InstrumentProviderConfig, optional
        The configuration for the provider.

    """

    def __init__(
        self,
        http_client: GateIoSpotHttpClient,
        clock: LiveClock,
        venue: Venue = GATEIO_VENUE,
        config: InstrumentProviderConfig | None = None,
    ) -> None:
        super().__init__(config=config)
        self._http_client = http_client
        self._clock = clock
        self._venue = venue

    async def load_all_async(self, filters: dict | None = None) -> None:
        self._log.info("Loading all Gate.io spot instruments...")
        pairs = await self._http_client.request_spot_currency_pairs()
        loaded = 0
        skipped = 0
        for pair in pairs:
            if pair.trade_status != "tradable":
                skipped += 1
                continue
            try:
                instrument = self._parse_instrument(pair)
                self.add_currency(currency=instrument.base_currency)
                self.add_currency(currency=instrument.quote_currency)
                self.add(instrument=instrument)
                loaded += 1
            except Exception as e:
                self._log.warning(f"Skipping {pair.id}: {e}")
                skipped += 1
        self._log.info(f"Loaded {loaded} instruments, skipped {skipped}")

    def _parse_instrument(self, pair: GateIoCurrencyPair) -> CurrencyPair:
        raw_symbol = Symbol(pair.id)
        instrument_id = InstrumentId(symbol=raw_symbol, venue=self._venue)
        ts_now = self._clock.timestamp_ns()

        # Gate.io fee field is a percentage string, e.g. "0.2" means 0.2% = 0.002
        taker_fee = Decimal(pair.fee) / Decimal("100")

        price_precision = pair.precision
        size_precision = pair.amount_precision

        # Build increment from precision: 10^-precision
        price_tick = Decimal(10) ** -price_precision
        size_step = Decimal(10) ** -size_precision

        price_increment = Price(float(price_tick), precision=price_precision)
        size_increment = Quantity(float(size_step), precision=size_precision)

        base_currency = Currency.from_str(pair.base)
        quote_currency = Currency.from_str(pair.quote)

        return CurrencyPair(
            instrument_id=instrument_id,
            raw_symbol=raw_symbol,
            base_currency=base_currency,
            quote_currency=quote_currency,
            price_precision=price_precision,
            size_precision=size_precision,
            price_increment=price_increment,
            size_increment=size_increment,
            maker_fee=taker_fee,
            taker_fee=taker_fee,
            ts_event=ts_now,
            ts_init=ts_now,
            info=msgspec.structs.asdict(pair),
        )
