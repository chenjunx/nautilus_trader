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
from nautilus_trader.adapters.bitfinex.constants import BitfinexInstrumentType
from nautilus_trader.adapters.bitfinex.http.client import BitfinexHttpClient
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.currencies import Currency
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments.crypto_perpetual import CryptoPerpetual
from nautilus_trader.model.instruments.currency_pair import CurrencyPair
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


# Bitfinex derivative (perpetual futures) pairs are quoted as ``<BASE>F0:USTF0``;
# the Nautilus-side symbol appends this suffix to disambiguate from the spot pair
# sharing the same venue (e.g. spot ``BTCUSDT`` vs perpetual ``BTCUSDT-PERP``).
_DERIV_QUOTE_SUFFIX = ":USTF0"
_DERIV_BASE_SUFFIX = "F0"
_PERP_SUFFIX = "-PERP"


# Bitfinex uses non-standard ticker codes for stablecoins; map to canonical names
# so that cross-venue comparisons (e.g. spread monitor) work correctly.
_CURRENCY_REMAP: dict[str, str] = {
    "UST": "USDT",   # Tether
    "UDC": "USDC",   # USD Coin
    "EUT": "EURT",   # Tether EUR
}

# Reverse map: Nautilus canonical → Bitfinex native code
_CURRENCY_REMAP_INV: dict[str, str] = {v: k for k, v in _CURRENCY_REMAP.items()}


def _strip_deriv_base_suffix(base_raw: str) -> str:
    if base_raw.endswith(_DERIV_BASE_SUFFIX):
        return base_raw[: -len(_DERIV_BASE_SUFFIX)]
    return base_raw


def bitfinex_pair_to_nautilus(pair: str) -> str:
    """Convert a raw Bitfinex pair string to the Nautilus symbol string.

    Applies currency remapping so that e.g. ``BTCUST`` → ``BTCUSDT``
    and ``BTC:UDC`` → ``BTC:USDC``. USDT-margined perpetual pairs such as
    ``BTCF0:USTF0`` map to ``BTCUSDT-PERP``.
    """
    if pair.endswith(_DERIV_QUOTE_SUFFIX):
        base_raw = _strip_deriv_base_suffix(pair[: -len(_DERIV_QUOTE_SUFFIX)])
        base_code = _CURRENCY_REMAP.get(base_raw, base_raw)
        return f"{base_code}USDT{_PERP_SUFFIX}"
    if ":" in pair:
        base, quote = pair.split(":", 1)
        return f"{_CURRENCY_REMAP.get(base, base)}:{_CURRENCY_REMAP.get(quote, quote)}"
    base, quote = pair[:3], pair[3:]
    return f"{_CURRENCY_REMAP.get(base, base)}{_CURRENCY_REMAP.get(quote, quote)}"


def nautilus_to_bitfinex_pair(symbol: str) -> str:
    """Convert a Nautilus symbol string back to the Bitfinex native pair.

    Reverses the currency remapping so that e.g. ``BTCUSDT`` → ``BTCUST``
    and ``BTC:USDC`` → ``BTC:UDC``. Perpetual symbols such as ``BTCUSDT-PERP``
    map back to ``BTCF0:USTF0``.
    """
    if symbol.endswith(_PERP_SUFFIX):
        base_code = symbol[: -len(_PERP_SUFFIX) - len("USDT")]
        base_raw = _CURRENCY_REMAP_INV.get(base_code, base_code)
        return f"{base_raw}{_DERIV_BASE_SUFFIX}{_DERIV_QUOTE_SUFFIX}"
    if ":" in symbol:
        base, quote = symbol.split(":", 1)
        return f"{_CURRENCY_REMAP_INV.get(base, base)}:{_CURRENCY_REMAP_INV.get(quote, quote)}"
    # A remapped canonical name (USDT/USDC/EURT) is 4 chars, so a naive [:3]/[3:]
    # split misaligns whenever one of these appears as the *base* (e.g.
    # "USDCUSDT" would wrongly split into "USD"/"CUSDT"). Detect the canonical
    # name as a whole prefix/suffix first and only fall back to the fixed 3/3
    # split when neither side needs remapping.
    for canon, native in _CURRENCY_REMAP_INV.items():
        if symbol.startswith(canon):
            quote = symbol[len(canon):]
            return f"{native}{_CURRENCY_REMAP_INV.get(quote, quote)}"
        if symbol.endswith(canon):
            base = symbol[: -len(canon)]
            return f"{_CURRENCY_REMAP_INV.get(base, base)}{native}"
    base, quote = symbol[:3], symbol[3:]
    return f"{_CURRENCY_REMAP_INV.get(base, base)}{_CURRENCY_REMAP_INV.get(quote, quote)}"


class BitfinexInstrumentProvider(InstrumentProvider):
    """
    Provides Bitfinex spot and USDT-margined perpetual instruments via the public REST API.

    Parameters
    ----------
    http_client : BitfinexHttpClient
        The HTTP client for the provider.
    clock : LiveClock
        The clock for the provider.
    config : InstrumentProviderConfig, optional
        The configuration for the provider.
    instrument_types : tuple[BitfinexInstrumentType, ...], default (BitfinexInstrumentType.SPOT,)
        The instrument types to load.

    """

    def __init__(
        self,
        http_client: BitfinexHttpClient,
        clock: LiveClock,
        config: InstrumentProviderConfig | None = None,
        instrument_types: tuple[BitfinexInstrumentType, ...] = (BitfinexInstrumentType.SPOT,),
    ) -> None:
        super().__init__(config=config)
        self._http_client = http_client
        self._clock = clock
        self._instrument_types = instrument_types

    async def load_all_async(self, filters: dict | None = None) -> None:
        """Load all configured Bitfinex instruments from the REST API."""
        loaded = 0
        skipped = 0
        spot_bases: set[str] = set()

        load_spot = BitfinexInstrumentType.SPOT in self._instrument_types
        load_perp = BitfinexInstrumentType.PERPETUAL in self._instrument_types

        if load_spot or load_perp:
            # Spot pairs are always fetched when loading perpetuals: the set of
            # crypto base currencies Bitfinex lists on spot is used to filter out
            # non-crypto (index/commodity) derivative products, see below.
            if load_spot:
                self._log.info("Loading all Bitfinex spot instruments...")
            pairs = await self._http_client.request_spot_pairs()
            for pair in pairs:
                base = self._parse_symbol(pair)[0]
                spot_bases.add(base)
                if not load_spot:
                    continue
                try:
                    instrument = self._parse_instrument(pair)
                    self.add_currency(currency=instrument.base_currency)
                    self.add_currency(currency=instrument.quote_currency)
                    self.add(instrument=instrument)
                    loaded += 1
                except Exception as e:
                    self._log.warning(f"Skipping pair {pair!r}: {e}")
                    skipped += 1

        if load_perp:
            self._log.info("Loading all Bitfinex USDT perpetual instruments...")
            deriv_pairs = await self._http_client.request_derivative_pairs()
            for pair in deriv_pairs:
                if not pair.endswith(_DERIV_QUOTE_SUFFIX):
                    # Skip non-USDT-margined (e.g. BTC-margined/inverse) perpetuals
                    continue
                base_raw = _strip_deriv_base_suffix(pair[: -len(_DERIV_QUOTE_SUFFIX)])
                if base_raw not in spot_bases:
                    # Not a base Bitfinex also lists on spot: treat as non-crypto
                    # (index/commodity) derivative and skip it
                    continue
                try:
                    instrument = self._parse_perpetual_instrument(pair)
                    self.add_currency(currency=instrument.base_currency)
                    self.add_currency(currency=instrument.quote_currency)
                    self.add(instrument=instrument)
                    loaded += 1
                except Exception as e:
                    self._log.warning(f"Skipping derivative pair {pair!r}: {e}")
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

        # Apply currency remapping and use the normalised string as Nautilus symbol
        base_code = _CURRENCY_REMAP.get(base_code, base_code)
        quote_code = _CURRENCY_REMAP.get(quote_code, quote_code)
        nautilus_pair = bitfinex_pair_to_nautilus(pair)
        raw_symbol = Symbol(nautilus_pair)
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

    def _parse_perpetual_instrument(self, pair: str) -> CryptoPerpetual:
        """
        Build a :class:`CryptoPerpetual` from a raw Bitfinex derivative pair string.

        Parameters
        ----------
        pair : str
            The USDT-margined perpetual pair symbol as returned by the REST API,
            e.g. ``"BTCF0:USTF0"``.

        """
        base_raw = _strip_deriv_base_suffix(pair[: -len(_DERIV_QUOTE_SUFFIX)])
        base_code = _CURRENCY_REMAP.get(base_raw, base_raw)
        quote_code = "USDT"

        nautilus_symbol = bitfinex_pair_to_nautilus(pair)
        raw_symbol = Symbol(nautilus_symbol)
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

        return CryptoPerpetual(
            instrument_id=instrument_id,
            raw_symbol=raw_symbol,
            base_currency=Currency.from_str(base_code),
            quote_currency=Currency.from_str(quote_code),
            settlement_currency=Currency.from_str(quote_code),
            is_inverse=False,
            price_precision=price_precision,
            size_precision=size_precision,
            price_increment=price_increment,
            size_increment=size_increment,
            # Bitfinex derivatives have been zero-fee (no volume threshold) since
            # 2025-12-17; verify against https://support.bitfinex.com if this changes.
            maker_fee=Decimal("0"),
            taker_fee=Decimal("0"),
            ts_event=ts_now,
            ts_init=ts_now,
        )
