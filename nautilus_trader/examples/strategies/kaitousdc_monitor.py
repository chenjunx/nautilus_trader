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

import pandas as pd

from nautilus_trader.common.events import TimeEvent
from nautilus_trader.config import PositiveFloat
from nautilus_trader.config import PositiveInt
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
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
    ewma_sigma: str | None


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
    book_interval_ms : PositiveInt, default 1000
        The interval in milliseconds for order book snapshots.
    sample_mid : bool, default True
        If mid price should be sampled from the latest quote on a timer.
    mid_sample_interval_secs : PositiveFloat, default 2.0
        The interval in seconds for mid price sampling.
    mid_sample_history_size : PositiveInt, default 2
        The number of recent mid price samples to keep in memory.
    calculate_ewma_sigma : bool, default True
        If EWMA sigma should be calculated from fixed-interval mid price increments.
    ewma_lambda : PositiveFloat, default 0.94
        The EWMA decay factor for squared mid price increments.

    """

    instrument_id: InstrumentId
    bar_type: BarType | None = None
    subscribe_quotes: bool = True
    subscribe_trades: bool = True
    subscribe_bars: bool = True
    subscribe_book_snapshots: bool = False
    book_interval_ms: PositiveInt = 1000
    sample_mid: bool = True
    mid_sample_interval_secs: PositiveFloat = 2.0
    mid_sample_history_size: PositiveInt = 2
    calculate_ewma_sigma: bool = True
    ewma_lambda: PositiveFloat = 0.94


class KaitousdcMonitorStrategy(Strategy):
    """
    A data-only Binance futures monitor for ``KAITOUSDC-PERP``.

    The strategy subscribes to configured market data streams and logs received
    data with simple counters. It is intentionally monitoring-only.

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
        self._last_quote: QuoteTick | None = None
        self._mid_sample_count = 0
        self._mid_samples: deque[MidPriceSample] = deque(
            maxlen=config.mid_sample_history_size,
        )
        self._last_mid: Decimal | None = None
        self._ewma_delta_s_var: Decimal | None = None
        self._ewma_sigma: Decimal | None = None
        self._ewma_delta_s_count = 0

    def on_start(self) -> None:
        """
        Actions to be performed on strategy start.
        """
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return

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
            f"sample_mid={self.config.sample_mid} | "
            f"mid_sample_interval_secs={self.config.mid_sample_interval_secs} | "
            f"calculate_ewma_sigma={self.config.calculate_ewma_sigma} | "
            f"ewma_lambda={self.config.ewma_lambda}",
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
            f"ewma_sigma={sample.ewma_sigma} | "
            f"count={self._mid_sample_count}",
        )

    def _create_mid_price_sample(self, quote: QuoteTick) -> MidPriceSample:
        bid = quote.bid_price.as_decimal()
        ask = quote.ask_price.as_decimal()
        spread = ask - bid
        mid = bid + spread / Decimal("2")

        delta_s: Decimal | None = None
        if self.config.calculate_ewma_sigma:
            delta_s = self._update_ewma_sigma(mid)

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
            ewma_sigma=str(self._ewma_sigma) if self._ewma_sigma is not None else None,
        )

    def _update_ewma_sigma(self, mid: Decimal) -> Decimal | None:
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

        self._ewma_sigma = (
            self._ewma_delta_s_var / Decimal(str(self.config.mid_sample_interval_secs))
        ).sqrt()
        self._ewma_delta_s_count += 1
        self._last_mid = mid
        return delta_s

    def on_trade_tick(self, tick: TradeTick) -> None:
        """
        Actions to be performed when a trade tick is received.
        """
        self._trade_count += 1
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
        total = (
            self._quote_count
            + self._trade_count
            + self._bar_count
            + self._book_count
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
            f"mid_samples={self._mid_sample_count} | "
            f"recent_mid_samples={list(self._mid_samples)} | "
            f"ewma_delta_s_count={self._ewma_delta_s_count} | "
            f"ewma_delta_s_var={self._ewma_delta_s_var} | "
            f"ewma_sigma={self._ewma_sigma} | "
            f"received_data={total > 0}",
        )
