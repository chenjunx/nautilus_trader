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

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
import time

import pandas as pd

from nautilus_trader.common.events import TimeEvent
from nautilus_trader.config import PositiveFloat
from nautilus_trader.config import PositiveInt
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import OrderBookDelta
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.trading.strategy import Strategy


@dataclass(frozen=True)
class MidPriceSample:
    """
    A quote-derived mid price sample.
    """

    ts_event: int
    ts_init: int
    bid: str
    ask: str
    mid: str
    spread: str
    delta_s: str | None
    ewma_delta_s_var: str | None
    reservation_prices: dict[int, str] | None
    quote_spread: str | None
    quote_prices: dict[int, dict[str, str]] | None


class KaitousdcMonitorConfig(StrategyConfig, frozen=True):
    """
    Configuration for ``KaitousdcMonitorStrategy`` instances.

    Parameters
    ----------
    instrument_id : InstrumentId
        The Binance USDT futures instrument ID to monitor.
    bar_type : BarType, optional
        The bar type to subscribe to when ``subscribe_bars`` is enabled.
    subscribe_quotes : bool, default True
        If quote ticks should be subscribed.
    subscribe_trades : bool, default True
        If trade ticks should be subscribed.
    subscribe_bars : bool, default True
        If bars should be subscribed when ``bar_type`` is provided.
    subscribe_book_snapshots : bool, default False
        If order book snapshots should be subscribed at ``book_interval_ms``.
    subscribe_book_deltas : bool, default True
        If order book deltas should be subscribed and persisted.
    book_interval_ms : PositiveInt, default 1000
        The interval in milliseconds for order book snapshots.
    book_depth : PositiveInt, default 100
        The requested depth for order book delta subscriptions.
    sample_mid : bool, default True
        If mid price should be sampled from the latest quote on a timer.
    mid_sample_interval_secs : PositiveFloat, default 2.0
        The interval in seconds for mid price sampling.
    mid_sample_history_size : PositiveInt, default 2
        The number of recent mid price samples to keep in memory.
    calculate_ewma_variance : bool, default True
        If EWMA variance should be calculated from fixed-interval mid price increments.
    ewma_lambda : PositiveFloat, default 0.94
        The EWMA decay factor for squared mid price increments.
    reservation_price_gamma : PositiveFloat, default 0.1
        The risk aversion coefficient used for reservation prices.
    reservation_price_horizon : PositiveFloat, default 150.0
        The horizon/multiplier used for reservation prices.
    reservation_price_min_q : PositiveInt, default 1
        The first positive inventory level to calculate.
    reservation_price_max_q : PositiveInt, default 10
        The final positive inventory level to calculate.
    quote_intensity_k : PositiveFloat, default 1.5
        The order book intensity parameter used in the quote spread formula.
    persist_market_data : bool, default True
        If received trade ticks and order book deltas should be persisted.
    catalog_path : str, default "data/kaitousdc/catalog"
        The local parquet data catalog path for persisted market data.
    flush_interval_secs : PositiveFloat, default 5.0
        The maximum interval in seconds between market data flushes.
    max_buffer_size : PositiveInt, default 10000
        The maximum buffered trade and book delta count before flushing.

    """

    instrument_id: InstrumentId
    bar_type: BarType | None = None
    subscribe_quotes: bool = True
    subscribe_trades: bool = True
    subscribe_bars: bool = True
    subscribe_book_snapshots: bool = False
    subscribe_book_deltas: bool = True
    book_interval_ms: PositiveInt = 1000
    book_depth: PositiveInt = 100
    sample_mid: bool = True
    mid_sample_interval_secs: PositiveFloat = 2.0
    mid_sample_history_size: PositiveInt = 2
    calculate_ewma_variance: bool = True
    ewma_lambda: PositiveFloat = 0.94
    reservation_price_gamma: PositiveFloat = 0.1
    reservation_price_horizon: PositiveFloat = 150.0
    reservation_price_min_q: PositiveInt = 1
    reservation_price_max_q: PositiveInt = 10
    quote_intensity_k: PositiveFloat = 1.5
    persist_market_data: bool = True
    catalog_path: str = "data/kaitousdc/catalog"
    flush_interval_secs: PositiveFloat = 5.0
    max_buffer_size: PositiveInt = 10_000


class KaitousdcMonitorStrategy(Strategy):
    """
    A data-only Binance futures monitor for ``KAITOUSDC-PERP``.

    The strategy subscribes to configured market data streams, logs received
    data with simple counters, and persists trade ticks and order book deltas.
    It is intentionally monitoring-only.

    Parameters
    ----------
    config : KaitousdcMonitorConfig
        The configuration for the instance.

    """

    MID_SAMPLE_TIMER_NAME = "mid_price_sample"

    def __init__(self, config: KaitousdcMonitorConfig) -> None:
        super().__init__(config)

        self.instrument: Instrument | None = None
        self._quote_count = 0
        self._trade_count = 0
        self._bar_count = 0
        self._book_count = 0
        self._book_delta_count = 0
        self._last_quote: QuoteTick | None = None
        self._book: OrderBook | None = None
        self._catalog: ParquetDataCatalog | None = None
        self._trade_buffer: list[TradeTick] = []
        self._book_delta_buffer: list[OrderBookDelta] = []
        self._last_flush_monotonic = time.monotonic()
        self._mid_sample_count = 0
        self._mid_samples: deque[MidPriceSample] = deque(
            maxlen=config.mid_sample_history_size,
        )
        self._last_mid: Decimal | None = None
        self._ewma_delta_s_var: Decimal | None = None
        self._ewma_delta_s_count = 0
        self._reservation_prices: dict[int, str] | None = None
        self._quote_spread: str | None = None
        self._quote_prices: dict[int, dict[str, str]] | None = None

    def on_start(self) -> None:
        """
        Actions to be performed on strategy start.
        """
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return

        if self.config.persist_market_data:
            self._catalog = ParquetDataCatalog(self.config.catalog_path)
            self._catalog.write_data([self.instrument])

        if self.config.subscribe_book_deltas:
            self._book = OrderBook(
                instrument_id=self.config.instrument_id,
                book_type=BookType.L2_MBP,
            )

        if self.config.subscribe_quotes:
            self.subscribe_quote_ticks(instrument_id=self.config.instrument_id)
        if self.config.subscribe_trades:
            self.subscribe_trade_ticks(instrument_id=self.config.instrument_id)
        if self.config.subscribe_bars and self.config.bar_type is not None:
            self.subscribe_bars(self.config.bar_type)
        if self.config.subscribe_book_snapshots:
            self.subscribe_order_book_at_interval(
                instrument_id=self.config.instrument_id,
                book_type=BookType.L2_MBP,
                interval_ms=self.config.book_interval_ms,
            )
        if self.config.subscribe_book_deltas:
            self.subscribe_order_book_deltas(
                instrument_id=self.config.instrument_id,
                book_type=BookType.L2_MBP,
                depth=self.config.book_depth,
            )
        if self.config.sample_mid:
            self.clock.set_timer(
                name=self.MID_SAMPLE_TIMER_NAME,
                interval=pd.Timedelta(seconds=self.config.mid_sample_interval_secs),
                callback=self.on_timer,
            )

        self.log.info(
            "Started KAITOUSDC monitor | "
            f"instrument_id={self.config.instrument_id} | "
            f"quotes={self.config.subscribe_quotes} | "
            f"trades={self.config.subscribe_trades} | "
            f"bars={self.config.subscribe_bars and self.config.bar_type is not None} | "
            f"book_snapshots={self.config.subscribe_book_snapshots} | "
            f"book_deltas={self.config.subscribe_book_deltas} | "
            f"book_depth={self.config.book_depth} | "
            f"sample_mid={self.config.sample_mid} | "
            f"mid_sample_interval_secs={self.config.mid_sample_interval_secs} | "
            f"calculate_ewma_variance={self.config.calculate_ewma_variance} | "
            f"ewma_lambda={self.config.ewma_lambda} | "
            f"reservation_price_gamma={self.config.reservation_price_gamma} | "
            f"reservation_price_horizon={self.config.reservation_price_horizon} | "
            f"reservation_price_min_q={self.config.reservation_price_min_q} | "
            f"reservation_price_max_q={self.config.reservation_price_max_q} | "
            f"quote_intensity_k={self.config.quote_intensity_k} | "
            f"persist_market_data={self.config.persist_market_data} | "
            f"catalog_path={self.config.catalog_path} | "
            f"flush_interval_secs={self.config.flush_interval_secs} | "
            f"max_buffer_size={self.config.max_buffer_size}",
        )

    def on_quote_tick(self, tick: QuoteTick) -> None:
        """
        Actions to be performed when a quote tick is received.
        """
        self._quote_count += 1
        self._last_quote = tick
        spread = tick.ask_price - tick.bid_price
        self.log.info(
            "Quote tick | "
            f"instrument_id={tick.instrument_id} | "
            f"bid={tick.bid_price} | "
            f"ask={tick.ask_price} | "
            f"spread={spread} | "
            f"count={self._quote_count}",
        )

    def on_timer(self, event: TimeEvent) -> None:
        """
        Actions to be performed when a timer event is received.
        """
        if event.name != self.MID_SAMPLE_TIMER_NAME:
            return

        if self._last_quote is None:
            self.log.info(
                "Mid sample skipped | "
                f"instrument_id={self.config.instrument_id} | "
                "reason=no_quote",
            )
            return

        sample = self._create_mid_price_sample(self._last_quote)
        self._mid_samples.append(sample)
        self._mid_sample_count += 1
        self.log.info(
            "Mid sample | "
            f"instrument_id={self.config.instrument_id} | "
            f"bid={sample.bid} | "
            f"ask={sample.ask} | "
            f"mid={sample.mid} | "
            f"spread={sample.spread} | "
            f"delta_s={sample.delta_s} | "
            f"ewma_delta_s_var={sample.ewma_delta_s_var} | "
            f"reservation_prices={sample.reservation_prices} | "
            f"quote_spread={sample.quote_spread} | "
            f"quote_prices={sample.quote_prices} | "
            f"count={self._mid_sample_count}",
        )

    def _create_mid_price_sample(self, quote: QuoteTick) -> MidPriceSample:
        bid = quote.bid_price.as_decimal()
        ask = quote.ask_price.as_decimal()
        spread = ask - bid
        mid = bid + spread / Decimal("2")

        delta_s: Decimal | None = None
        if self.config.calculate_ewma_variance:
            delta_s = self._update_ewma_delta_s_var(mid)
            self._reservation_prices = self._calculate_reservation_prices(mid)
            self._quote_spread = self._calculate_quote_spread()
            self._quote_prices = self._calculate_quote_prices()

        return MidPriceSample(
            ts_event=quote.ts_event,
            ts_init=quote.ts_init,
            bid=str(quote.bid_price),
            ask=str(quote.ask_price),
            mid=str(mid),
            spread=str(spread),
            delta_s=str(delta_s) if delta_s is not None else None,
            ewma_delta_s_var=str(self._ewma_delta_s_var)
            if self._ewma_delta_s_var is not None
            else None,
            reservation_prices=self._reservation_prices,
            quote_spread=self._quote_spread,
            quote_prices=self._quote_prices,
        )

    def _update_ewma_delta_s_var(self, mid: Decimal) -> Decimal | None:
        if self._last_mid is None:
            self._last_mid = mid
            return None

        delta_s = mid - self._last_mid
        delta_s_sq = delta_s * delta_s

        if self._ewma_delta_s_var is None:
            self._ewma_delta_s_var = delta_s_sq
        else:
            lambda_ = Decimal(str(self.config.ewma_lambda))
            self._ewma_delta_s_var = (
                lambda_ * self._ewma_delta_s_var
                + (Decimal("1") - lambda_) * delta_s_sq
            )

        self._ewma_delta_s_count += 1
        self._last_mid = mid
        return delta_s

    def _calculate_reservation_prices(self, mid: Decimal) -> dict[int, str] | None:
        if self._ewma_delta_s_var is None:
            return None

        gamma = Decimal(str(self.config.reservation_price_gamma))
        horizon = Decimal(str(self.config.reservation_price_horizon))
        return {
            q: str(mid - Decimal(q) * gamma * self._ewma_delta_s_var * horizon)
            for q in range(
                self.config.reservation_price_min_q,
                self.config.reservation_price_max_q + 1,
            )
        }

    def _calculate_quote_spread(self) -> str | None:
        if self._ewma_delta_s_var is None:
            return None

        gamma = Decimal(str(self.config.reservation_price_gamma))
        horizon = Decimal(str(self.config.reservation_price_horizon))
        k = Decimal(str(self.config.quote_intensity_k))
        total_spread = (
            gamma * self._ewma_delta_s_var * horizon
            + (Decimal("2") / gamma) * (Decimal("1") + gamma / k).ln()
        )
        return str(total_spread)

    def _calculate_quote_prices(self) -> dict[int, dict[str, str]] | None:
        if self._reservation_prices is None or self._quote_spread is None:
            return None

        half_spread = Decimal(self._quote_spread) / Decimal("2")
        return {
            q: {
                "reservation_price": reservation_price,
                "bid": str(Decimal(reservation_price) - half_spread),
                "ask": str(Decimal(reservation_price) + half_spread),
            }
            for q, reservation_price in self._reservation_prices.items()
        }

    def _flush_market_data_if_needed(self) -> None:
        if not self.config.persist_market_data:
            return

        pending = len(self._trade_buffer) + len(self._book_delta_buffer)
        if pending >= self.config.max_buffer_size:
            self._flush_market_data()
            return

        elapsed = time.monotonic() - self._last_flush_monotonic
        if elapsed >= self.config.flush_interval_secs:
            self._flush_market_data()

    def _flush_market_data(self) -> None:
        if not self.config.persist_market_data or self._catalog is None:
            return

        if self._book_delta_buffer:
            data = sorted(self._book_delta_buffer, key=lambda x: x.ts_init)
            self._catalog.write_data(data, skip_disjoint_check=True)
            self.log.info(f"Flushed {len(data)} KAITOUSDC order book deltas")
            self._book_delta_buffer.clear()

        if self._trade_buffer:
            data = sorted(self._trade_buffer, key=lambda x: x.ts_init)
            self._catalog.write_data(data, skip_disjoint_check=True)
            self.log.info(f"Flushed {len(data)} KAITOUSDC trades")
            self._trade_buffer.clear()

        self._last_flush_monotonic = time.monotonic()

    def on_trade_tick(self, tick: TradeTick) -> None:
        """
        Actions to be performed when a trade tick is received.
        """
        self._trade_count += 1
        if self.config.persist_market_data:
            self._trade_buffer.append(tick)
            self._flush_market_data_if_needed()
        self.log.info(
            "Trade tick | "
            f"instrument_id={tick.instrument_id} | "
            f"price={tick.price} | "
            f"size={tick.size} | "
            f"aggressor_side={tick.aggressor_side} | "
            f"count={self._trade_count}",
        )

    def on_bar(self, bar: Bar) -> None:
        """
        Actions to be performed when a bar is received.
        """
        self._bar_count += 1
        self.log.info(
            "Bar | "
            f"bar_type={bar.bar_type} | "
            f"open={bar.open} | "
            f"high={bar.high} | "
            f"low={bar.low} | "
            f"close={bar.close} | "
            f"volume={bar.volume} | "
            f"count={self._bar_count}",
        )

    def on_order_book_deltas(self, deltas: OrderBookDeltas) -> None:
        """
        Actions to be performed when order book deltas are received.
        """
        self._book_delta_count += len(deltas.deltas)
        if self._book is not None:
            self._book.apply_deltas(deltas)

        if self.config.persist_market_data:
            self._book_delta_buffer.extend(deltas.deltas)
            self._flush_market_data_if_needed()

        bid = self._book.best_bid_price() if self._book is not None else None
        ask = self._book.best_ask_price() if self._book is not None else None
        spread = ask - bid if bid is not None and ask is not None else None
        self.log.info(
            "Order book deltas | "
            f"instrument_id={deltas.instrument_id} | "
            f"sequence={deltas.sequence} | "
            f"delta_count={len(deltas.deltas)} | "
            f"bid={bid} | "
            f"ask={ask} | "
            f"spread={spread} | "
            f"count={self._book_delta_count}",
        )

    def on_order_book(self, order_book: OrderBook) -> None:
        """
        Actions to be performed when an order book snapshot is received.
        """
        self._book_count += 1
        bid = order_book.best_bid_price()
        ask = order_book.best_ask_price()
        spread = ask - bid if bid is not None and ask is not None else None
        self.log.info(
            "Order book | "
            f"instrument_id={order_book.instrument_id} | "
            f"bid={bid} | "
            f"ask={ask} | "
            f"spread={spread} | "
            f"count={self._book_count}",
        )

    def on_stop(self) -> None:
        """
        Actions to be performed when the strategy is stopped.
        """
        self._flush_market_data()
        total = (
            self._quote_count
            + self._trade_count
            + self._bar_count
            + self._book_count
            + self._book_delta_count
            + self._mid_sample_count
            + self._ewma_delta_s_count
        )
        self.log.info(
            "Stopped KAITOUSDC monitor | "
            f"instrument_id={self.config.instrument_id} | "
            f"quotes={self._quote_count} | "
            f"trades={self._trade_count} | "
            f"bars={self._bar_count} | "
            f"book_snapshots={self._book_count} | "
            f"book_deltas={self._book_delta_count} | "
            f"pending_trades={len(self._trade_buffer)} | "
            f"pending_book_deltas={len(self._book_delta_buffer)} | "
            f"mid_samples={self._mid_sample_count} | "
            f"recent_mid_samples={list(self._mid_samples)} | "
            f"ewma_delta_s_count={self._ewma_delta_s_count} | "
            f"ewma_delta_s_var={self._ewma_delta_s_var} | "
            f"reservation_prices={self._reservation_prices} | "
            f"quote_spread={self._quote_spread} | "
            f"quote_prices={self._quote_prices} | "
            f"received_data={total > 0}",
        )
