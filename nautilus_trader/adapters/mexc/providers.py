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

from nautilus_trader.adapters.mexc.constants import MEXC_VENUE
from nautilus_trader.adapters.mexc.http.client import MexcHttpClient
from nautilus_trader.adapters.mexc.http.models import MexcSymbol
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.currencies import Currency
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments.currency_pair import CurrencyPair
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

_MEXC_TAKER_FEE = Decimal("0.002")  # 0.2% standard taker fee


class MexcInstrumentProvider(InstrumentProvider):
    """
    Provides MEXC spot instruments via the public REST API.

    Parameters
    ----------
    http_client : MexcHttpClient
        The HTTP client for the provider.
    clock : LiveClock
        The clock for the provider.
    config : InstrumentProviderConfig, optional
        The configuration for the provider.

    """

    def __init__(
        self,
        http_client: MexcHttpClient,
        clock: LiveClock,
        config: InstrumentProviderConfig | None = None,
    ) -> None:
        super().__init__(config=config)
        self._http_client = http_client
        self._clock = clock

    async def load_all_async(self, filters: dict | None = None) -> None:
        self._log.info("Loading all MEXC spot instruments...")
        exchange_info = await self._http_client.request_exchange_info()
        loaded = 0
        skipped = 0
        for symbol in exchange_info.symbols:
            if symbol.status != "TRADING" or not symbol.isSpotTradingAllowed:
                skipped += 1
                continue
            try:
                instrument = self._parse_instrument(symbol)
                self.add_currency(currency=instrument.base_currency)
                self.add_currency(currency=instrument.quote_currency)
                self.add(instrument=instrument)
                loaded += 1
            except Exception as e:
                self._log.warning(f"Skipping {symbol.symbol}: {e}")
                skipped += 1
        self._log.info(f"Loaded {loaded} instruments, skipped {skipped}")

    def _parse_instrument(self, symbol: MexcSymbol) -> CurrencyPair:
        raw_symbol = Symbol(symbol.symbol)
        instrument_id = InstrumentId(symbol=raw_symbol, venue=MEXC_VENUE)
        ts_now = self._clock.timestamp_ns()

        # quotePrecision is the price decimal places; enforce minimum of 2
        price_precision = max(symbol.quotePrecision, 2)
        size_precision = symbol.baseAssetPrecision

        price_tick = Decimal(10) ** -price_precision
        size_step = Decimal(10) ** -size_precision

        price_increment = Price(float(price_tick), precision=price_precision)
        size_increment = Quantity(float(size_step), precision=size_precision)

        base_currency = Currency.from_str(symbol.baseAsset)
        quote_currency = Currency.from_str(symbol.quoteAsset)

        return CurrencyPair(
            instrument_id=instrument_id,
            raw_symbol=raw_symbol,
            base_currency=base_currency,
            quote_currency=quote_currency,
            price_precision=price_precision,
            size_precision=size_precision,
            price_increment=price_increment,
            size_increment=size_increment,
            maker_fee=_MEXC_TAKER_FEE,
            taker_fee=_MEXC_TAKER_FEE,
            ts_event=ts_now,
            ts_init=ts_now,
            info=msgspec.structs.asdict(symbol),
        )
