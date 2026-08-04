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
from nautilus_trader.adapters.gateio.futures.http.client import GateIoFuturesHttpClient
from nautilus_trader.adapters.gateio.futures.http.models import GateIoFuturesContract
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.currencies import Currency
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments.crypto_perpetual import CryptoPerpetual
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


def _decimal_precision(value: str) -> int:
    """Return the number of decimal places represented by a numeric string."""
    exponent = Decimal(value).as_tuple().exponent
    if not isinstance(exponent, int):
        return 0
    return max(-exponent, 0)


class GateIoFuturesInstrumentProvider(InstrumentProvider):
    """
    Provides Gate.io USDT-margined perpetual futures instruments via the public REST API.

    Parameters
    ----------
    http_client : GateIoFuturesHttpClient
        The HTTP client for the provider.
    clock : LiveClock
        The clock for the provider.
    settle : str, default 'usdt'
        The settlement asset for the futures contracts to load.
    config : InstrumentProviderConfig, optional
        The configuration for the provider.

    """

    def __init__(
        self,
        http_client: GateIoFuturesHttpClient,
        clock: LiveClock,
        settle: str = "usdt",
        config: InstrumentProviderConfig | None = None,
    ) -> None:
        super().__init__(config=config)
        self._http_client = http_client
        self._clock = clock
        self._settle = settle

    async def load_all_async(self, filters: dict | None = None) -> None:
        self._log.info(f"Loading all Gate.io {self._settle}-settled futures instruments...")
        contracts = await self._http_client.request_futures_contracts(self._settle)
        loaded = 0
        skipped = 0
        for contract in contracts:
            if contract.in_delisting or contract.type == "inverse":
                skipped += 1
                continue
            try:
                instrument = self._parse_instrument(contract)
                self.add_currency(currency=instrument.base_currency)
                self.add_currency(currency=instrument.quote_currency)
                self.add(instrument=instrument)
                loaded += 1
            except Exception as e:
                self._log.warning(f"Skipping {contract.name}: {e}")
                skipped += 1
        self._log.info(f"Loaded {loaded} instruments, skipped {skipped}")

    def _parse_instrument(self, contract: GateIoFuturesContract) -> CryptoPerpetual:
        raw_symbol = Symbol(contract.name)
        instrument_id = InstrumentId(symbol=raw_symbol, venue=GATEIO_VENUE)
        ts_now = self._clock.timestamp_ns()

        base_str, _, quote_str = contract.name.partition("_")
        base_currency = Currency.from_str(base_str)
        quote_currency = Currency.from_str(quote_str)

        price_precision = _decimal_precision(contract.order_price_round)
        # Gate.io futures order sizes are whole numbers of contracts.
        size_precision = 0

        price_increment = Price(Decimal(contract.order_price_round), precision=price_precision)
        size_increment = Quantity(1, precision=size_precision)

        min_quantity = Quantity(contract.order_size_min, precision=size_precision)
        max_quantity = Quantity(contract.order_size_max, precision=size_precision)

        leverage_max = Decimal(contract.leverage_max)
        # Gate.io doesn't expose an initial margin rate directly; approximate it
        # from the max leverage (initial margin ~= 1 / max leverage).
        margin_init = Decimal(1) / leverage_max if leverage_max > 0 else Decimal(1)
        margin_maint = Decimal(contract.maintenance_rate)

        return CryptoPerpetual(
            instrument_id=instrument_id,
            raw_symbol=raw_symbol,
            base_currency=base_currency,
            quote_currency=quote_currency,
            settlement_currency=quote_currency,
            is_inverse=False,
            price_precision=price_precision,
            size_precision=size_precision,
            price_increment=price_increment,
            size_increment=size_increment,
            multiplier=Quantity(
                Decimal(contract.quanto_multiplier),
                precision=_decimal_precision(contract.quanto_multiplier),
            ),
            max_quantity=max_quantity,
            min_quantity=min_quantity,
            margin_init=margin_init,
            margin_maint=margin_maint,
            maker_fee=Decimal(contract.maker_fee_rate),
            taker_fee=Decimal(contract.taker_fee_rate),
            ts_event=ts_now,
            ts_init=ts_now,
            info=msgspec.structs.asdict(contract),
        )
