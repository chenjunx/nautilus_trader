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

from nautilus_trader.adapters.kucoin.constants import KUCOIN_VENUE
from nautilus_trader.adapters.kucoin.http.client import KuCoinHttpClient
from nautilus_trader.adapters.kucoin.http.models import KuCoinSymbol
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.currencies import Currency
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments.currency_pair import CurrencyPair
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class KuCoinInstrumentProvider(InstrumentProvider):
    """
    Provides KuCoin spot instruments via the public REST API.

    Parameters
    ----------
    http_client : KuCoinHttpClient
        The HTTP client used to fetch symbol data.
    clock : LiveClock
        The clock for timestamp generation.
    config : InstrumentProviderConfig, optional
        Provider configuration.

    """

    def __init__(
        self,
        http_client: KuCoinHttpClient,
        clock: LiveClock,
        config: InstrumentProviderConfig | None = None,
    ) -> None:
        super().__init__(config=config)
        self._http_client = http_client
        self._clock = clock

    async def load_all_async(self, filters: dict | None = None) -> None:
        """Load all tradable KuCoin spot instruments."""
        self._log.info("Loading all KuCoin spot instruments...")
        symbols = await self._http_client.request_spot_symbols()
        loaded = 0
        skipped = 0
        for sym in symbols:
            if not sym.enableTrading:
                skipped += 1
                continue
            try:
                instrument = self._parse_instrument(sym)
                self.add_currency(currency=instrument.base_currency)
                self.add_currency(currency=instrument.quote_currency)
                self.add(instrument=instrument)
                loaded += 1
            except Exception as e:
                self._log.warning(f"Skipping {sym.symbol}: {e}")
                skipped += 1
        self._log.info(f"Loaded {loaded} instruments, skipped {skipped}")

    @staticmethod
    def _precision_from_increment(increment: str) -> int:
        """Derive decimal precision from an increment string like '0.00000001'."""
        d = Decimal(increment)
        exp = d.as_tuple().exponent
        # exponent is negative for fractional values; abs gives the precision
        return abs(int(exp)) if exp < 0 else 0

    def _parse_instrument(self, sym: KuCoinSymbol) -> CurrencyPair:
        raw_symbol = Symbol(sym.symbol)
        instrument_id = InstrumentId(symbol=raw_symbol, venue=KUCOIN_VENUE)
        ts_now = self._clock.timestamp_ns()

        price_precision = self._precision_from_increment(sym.priceIncrement)
        size_precision = self._precision_from_increment(sym.baseIncrement)

        price_increment = Price.from_str(sym.priceIncrement)
        size_increment = Quantity.from_str(sym.baseIncrement)

        taker_fee = Decimal(sym.takerFeeCoefficient)
        maker_fee = Decimal(sym.makerFeeCoefficient)

        base_currency = Currency.from_str(sym.baseCurrency)
        quote_currency = Currency.from_str(sym.quoteCurrency)

        return CurrencyPair(
            instrument_id=instrument_id,
            raw_symbol=raw_symbol,
            base_currency=base_currency,
            quote_currency=quote_currency,
            price_precision=price_precision,
            size_precision=size_precision,
            price_increment=price_increment,
            size_increment=size_increment,
            maker_fee=maker_fee,
            taker_fee=taker_fee,
            ts_event=ts_now,
            ts_init=ts_now,
            info=msgspec.structs.asdict(sym),
        )
