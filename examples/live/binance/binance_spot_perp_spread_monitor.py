#!/usr/bin/env python3
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
"""
Real-time spot vs perpetual basis monitor for Binance USDC-quoted altcoins.

Subscribes to all Binance USDC spot ticker prices, USDC perpetual ticker prices, and
perpetual funding rates for altcoins (excluding BTC/ETH), then continuously prints the
basis (perp mid vs spot mid, as a percentage) plus the current funding rate annualized
(APR, i.e. the raw per-settlement rate scaled up by settlements/year).

Handles Binance's scaled-contract naming (e.g. perp base asset "1000SHIB" vs spot base
asset "SHIB") by normalizing the perpetual price back to a per-token basis before
computing the spread.

Data-only monitor — no execution client, cannot submit orders. Every QuoteTick received in
``on_quote_tick`` (spot and perp) is also streamed to the ``catalog/`` feather catalog, one
file per instrument, see ``StreamingConfig`` below.

"""

import re
import sys
import time
from dataclasses import dataclass

from nautilus_trader.adapters.binance import BinanceAccountType
from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceLiveDataClientFactory
from nautilus_trader.adapters.binance.futures.types import BinanceFuturesMarkPriceUpdate
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import StrategyConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.core.data import Data
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import DataType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.persistence.config import StreamingConfig
from nautilus_trader.trading.strategy import Strategy


# *** THIS IS A MONITORING EXAMPLE — IT DOES NOT SUBMIT ANY ORDERS. ***

BINANCE_SPOT = Venue("BINANCE_SPOT")
BINANCE_FUTURES = Venue("BINANCE_FUTURES")

# Matches Binance's scaled-contract base assets, e.g. "1000SHIB", "1000000BABYDOGE".
# Requires "1" followed by 2+ zeros so real coded symbols like "1INCH" are not matched.
_SCALE_PATTERN = re.compile(r"^1(0{2,})([A-Z0-9]+)$")

_SECONDS_PER_YEAR = 365.0 * 24.0 * 3600.0
# Binance's standard funding interval is 8h (3 settlements/day); used until the actual
# interval for a symbol is measured from consecutive mark-price updates (some symbols use
# a shorter interval, e.g. 1h/4h, under Binance's adjustable funding rate mechanism).
_DEFAULT_FUNDING_INTERVAL_SECS = 8.0 * 3600.0


def _strip_scale(base: str) -> tuple[int, str]:
    """Return (scale, real_base) for a possibly scaled perpetual base asset."""
    match = _SCALE_PATTERN.match(base)
    if match is None:
        return 1, base
    scale = int("1" + match.group(1))
    return scale, match.group(2)


@dataclass
class _Pairing:
    spot_id: InstrumentId
    perp_id: InstrumentId
    scale: int


class SpotPerpSpreadMonitorConfig(StrategyConfig, frozen=True):
    """
    Configuration for ``SpotPerpSpreadMonitor``.

    Parameters
    ----------
    futures_client_id : ClientId
        The client ID used to subscribe to the custom funding rate data type.
    exclude_bases : frozenset[str], default frozenset({"BTC", "ETH"})
        Base currencies to exclude (major coins, not "altcoins").
    min_abs_spread_pct : float, default 0.0
        Minimum absolute basis (percentage) required to print a per-symbol update.
    throttle_secs : float, default 2.0
        Minimum interval between prints for the same base currency.
    summary_interval_secs : float, default 30.0
        Interval between printed summary rankings.
    top_n : int, default 20
        Number of rows shown in the summary ranking.

    """

    futures_client_id: ClientId
    exclude_bases: frozenset[str] = frozenset({"BTC", "ETH"})
    min_abs_spread_pct: float = 0.0
    throttle_secs: float = 2.0
    summary_interval_secs: float = 30.0
    top_n: int = 20


class SpotPerpSpreadMonitor(Strategy):
    """
    Monitors the real-time basis between Binance USDC spot and USDC perpetual prices
    for altcoins, alongside the perpetual funding rate.
    """

    def __init__(self, config: SpotPerpSpreadMonitorConfig) -> None:
        super().__init__(config)

        self._pairings: dict[str, _Pairing] = {}  # base -> pairing info
        self._inst_role: dict[str, tuple[str, str]] = {}  # instrument_id str -> (role, base)

        self._spot_mid: dict[str, float] = {}
        self._perp_mid: dict[str, float] = {}
        self._funding: dict[str, tuple[float, int]] = {}  # base -> (rate, next_funding_ns)
        # base -> measured seconds between funding settlements (from consecutive next_funding_ns)
        self._funding_interval_secs: dict[str, float] = {}

        self._last_print: dict[str, float] = {}
        self._last_summary: float = 0.0
        self._start_time: float = 0.0

    def on_start(self) -> None:
        instruments = self.cache.instruments()
        self.log.info(f"Cache contains {len(instruments)} instruments")

        spot_by_base: dict[str, InstrumentId] = {}
        perp_by_base: dict[str, InstrumentId] = {}

        for inst in instruments:
            try:
                base = str(inst.base_currency)
                quote = str(inst.quote_currency)
            except AttributeError:
                continue

            if quote != "USDC" or base in self.config.exclude_bases:
                continue

            if isinstance(inst, CurrencyPair) and inst.id.venue == BINANCE_SPOT:
                spot_by_base[base] = inst.id
            elif isinstance(inst, CryptoPerpetual) and inst.id.venue == BINANCE_FUTURES:
                perp_by_base[base] = inst.id

        for perp_base, perp_id in perp_by_base.items():
            if perp_base in spot_by_base:
                base, scale = perp_base, 1
            else:
                scale, real_base = _strip_scale(perp_base)
                if scale == 1 or real_base not in spot_by_base:
                    continue
                base = real_base

            self._pairings[base] = _Pairing(
                spot_id=spot_by_base[base],
                perp_id=perp_id,
                scale=scale,
            )

        for base, pairing in sorted(self._pairings.items()):
            self._inst_role[str(pairing.spot_id)] = ("spot", base)
            self._inst_role[str(pairing.perp_id)] = ("perp", base)

            self.subscribe_quote_ticks(pairing.spot_id)
            self.subscribe_quote_ticks(pairing.perp_id)
            self.subscribe_data(
                data_type=DataType(
                    BinanceFuturesMarkPriceUpdate,
                    metadata={"instrument_id": pairing.perp_id},
                ),
                client_id=self.config.futures_client_id,
            )

            scale_note = f" (x{pairing.scale} scaled contract)" if pairing.scale != 1 else ""
            self.log.info(f"Matched {base}/USDC{scale_note}: {pairing.spot_id} <-> {pairing.perp_id}")

        self.log.info(f"Subscribed to {len(self._pairings)} spot/perp altcoin pairs")
        self._start_time = time.monotonic()

    def on_quote_tick(self, tick: QuoteTick) -> None:
        role_info = self._inst_role.get(str(tick.instrument_id))
        if role_info is None:
            return

        role, base = role_info
        mid = (float(tick.bid_price) + float(tick.ask_price)) / 2.0

        if role == "spot":
            self._spot_mid[base] = mid
        else:
            self._perp_mid[base] = mid / self._pairings[base].scale

        spot_mid = self._spot_mid.get(base)
        perp_mid = self._perp_mid.get(base)
        if spot_mid is None or perp_mid is None or spot_mid == 0.0:
            return

        spread_pct = (perp_mid - spot_mid) / spot_mid * 100.0
        now = time.monotonic()

        if now - self._last_summary >= self.config.summary_interval_secs:
            self._last_summary = now
            self._print_summary()

        if now - self._last_print.get(base, 0.0) < self.config.throttle_secs:
            return

        if abs(spread_pct) < self.config.min_abs_spread_pct:
            return

        self._last_print[base] = now
        self._print_update(base, spot_mid, perp_mid, spread_pct)

    def on_data(self, data: Data) -> None:
        if not isinstance(data, BinanceFuturesMarkPriceUpdate):
            return

        role_info = self._inst_role.get(str(data.instrument_id))
        if role_info is None:
            return

        _, base = role_info
        prev = self._funding.get(base)
        if prev is not None:
            prev_next_funding_ns = prev[1]
            delta_ns = data.next_funding_ns - prev_next_funding_ns
            if delta_ns > 0:
                self._funding_interval_secs[base] = delta_ns / 1_000_000_000.0

        self._funding[base] = (float(data.funding_rate), data.next_funding_ns)

    def _label(self, base: str) -> str:
        scale = self._pairings[base].scale
        return f"{base}/USDC" if scale == 1 else f"{base}/USDC (x{scale})"

    def _funding_str(self, base: str) -> str:
        funding = self._funding.get(base)
        if funding is None:
            return "funding=N/A"
        rate, next_funding_ns = funding

        interval_secs = self._funding_interval_secs.get(base, _DEFAULT_FUNDING_INTERVAL_SECS)
        periods_per_year = _SECONDS_PER_YEAR / interval_secs
        apr_pct = rate * periods_per_year * 100.0

        next_str = time.strftime("%H:%M:%S", time.gmtime(next_funding_ns / 1_000_000_000))
        return f"funding_apr={apr_pct:+.2f}% (next {next_str} UTC)"

    def _print_update(self, base: str, spot_mid: float, perp_mid: float, spread_pct: float) -> None:
        ts = time.strftime("%H:%M:%S")
        print(
            f"{ts} {self._label(base):<20} "
            f"spot={spot_mid:.6g}  perp={perp_mid:.6g}  "
            f"basis={spread_pct * 100.0:+.2f}bp  {self._funding_str(base)}",
        )
        sys.stdout.flush()

    def _print_summary(self) -> None:
        rows = []
        for base in self._pairings:
            spot_mid = self._spot_mid.get(base)
            perp_mid = self._perp_mid.get(base)
            if spot_mid is None or perp_mid is None or spot_mid == 0.0:
                continue
            spread_pct = (perp_mid - spot_mid) / spot_mid * 100.0
            rows.append((spread_pct, base, spot_mid, perp_mid))

        if not rows:
            return

        rows.sort(reverse=True)
        ts = time.strftime("%H:%M:%S")
        print(
            f"\n{ts} == TOP {self.config.top_n} BASIS (spot vs perp, high to low) "
            f"— tracking {len(rows)}/{len(self._pairings)} pairs ==",
        )
        for spread_pct, base, spot_mid, perp_mid in rows[: self.config.top_n]:
            print(
                f"  {self._label(base):<20} "
                f"spot={spot_mid:.6g}  perp={perp_mid:.6g}  "
                f"basis={spread_pct * 100.0:+.2f}bp  {self._funding_str(base)}",
            )
        print()
        sys.stdout.flush()

    def on_stop(self) -> None:
        for pairing in self._pairings.values():
            self.unsubscribe_quote_ticks(pairing.spot_id)
            self.unsubscribe_quote_ticks(pairing.perp_id)


# Configure the trading node for public market data only (no execution clients).
config_node = TradingNodeConfig(
    trader_id=TraderId("SPOT-PERP-SPREAD-001"),
    logging=LoggingConfig(log_level="INFO", use_pyo3=True),
    data_clients={
        "BINANCE_SPOT": BinanceDataClientConfig(
            venue=BINANCE_SPOT,
            account_type=BinanceAccountType.SPOT,
            instrument_provider=InstrumentProviderConfig(load_all=True),
        ),
        "BINANCE_FUTURES": BinanceDataClientConfig(
            venue=BINANCE_FUTURES,
            account_type=BinanceAccountType.USDT_FUTURES,
            instrument_provider=InstrumentProviderConfig(load_all=True),
        ),
    },
    timeout_connection=30.0,
    timeout_disconnection=10.0,
    timeout_post_stop=5.0,
    # Streams every QuoteTick received (spot and perp) to feather files, one per instrument_id,
    # under catalog/live/{instance_id}/quote_tick/.
    streaming=StreamingConfig(
        catalog_path="catalog",
        include_types=[QuoteTick],
    ),
)

# Instantiate the node with a configuration.
node = TradingNode(config=config_node)

# Instantiate and add the strategy.
strategy = SpotPerpSpreadMonitor(
    config=SpotPerpSpreadMonitorConfig(futures_client_id=ClientId("BINANCE_FUTURES")),
)
node.trader.add_strategy(strategy)

# Register the Binance data client factory for both venues.
node.add_data_client_factory("BINANCE_SPOT", BinanceLiveDataClientFactory)
node.add_data_client_factory("BINANCE_FUTURES", BinanceLiveDataClientFactory)
node.build()


# Stop and dispose of the node with SIGINT/CTRL+C.
if __name__ == "__main__":
    try:
        node.run()
    finally:
        node.dispose()
