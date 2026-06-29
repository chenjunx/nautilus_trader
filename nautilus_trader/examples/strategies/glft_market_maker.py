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
from decimal import ROUND_CEILING
from decimal import ROUND_FLOOR
from decimal import ROUND_HALF_UP
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
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderCanceled
from nautilus_trader.model.events import OrderExpired
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
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
    quote_prices: dict[int, dict[str, str | None]] | None
    position: str


class GLFTMarketMakerConfig(StrategyConfig, frozen=True):
    """
    Configuration for ``GLFTMarketMaker`` instances.

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
        The risk aversion coefficient (γ) in the GLFT formula.
    lot_size : PositiveInt, default 11
        The number of units per lot (one 手). Inventory levels are always multiples
        of this value; ``reservation_price_min_q`` and ``reservation_price_max_q``
        are expressed in lots, so the actual quantities used are
        ``min_q * lot_size`` … ``max_q * lot_size``.
    reservation_price_min_q : PositiveInt, default 1
        The first positive lot count to calculate (actual qty = min_q * lot_size).
    reservation_price_max_q : PositiveInt, default 10
        The final positive lot count to calculate (actual qty = max_q * lot_size).
    quote_intensity_k : PositiveFloat, default 1831.0
        The order book depth/decay parameter (k) in the GLFT formula.
    quote_arrival_a : PositiveFloat, default 1.0
        The market-order arrival rate scaling parameter (A) in the GLFT formula.
        With A=1 the formula reduces to the symmetric base case; increase A to
        widen c (and thus spreads) when arrival rates are higher than assumed.
    max_position : PositiveInt, default 110
        The maximum long inventory (in raw units) before buy quotes are
        suppressed. When the tracked net position reaches this cap, only sell
        (ask) quotes are emitted so inventory is drawn back down. The cap is
        one-directional (long only); the short side is never capped. Defaults
        to ``reservation_price_max_q * lot_size`` (10 * 11 = 110).
    enable_trading : bool, default False
        Safety kill-switch. When ``False`` (default) the strategy is
        monitoring-only and never submits, cancels, or closes orders — so it
        stays inert without an execution client and the data-only runner keeps
        working. When ``True`` it places post-only bid/ask limit orders on each
        mid sample, gated by ``max_position``.
    trade_size : PositiveInt, optional
        The per-order quantity (raw units) used when ``enable_trading`` is
        ``True``. If ``None`` (default) it resolves to ``lot_size`` so each
        quote is one lot and each fill moves inventory by one lot, matching the
        reservation-price grid.
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
    lot_size: PositiveInt = 11
    reservation_price_min_q: PositiveInt = 1
    reservation_price_max_q: PositiveInt = 10
    quote_intensity_k: PositiveFloat = 2473.25
    quote_arrival_a: PositiveFloat = 266.13
    max_position: PositiveInt = 110
    enable_trading: bool = False
    trade_size: PositiveInt | None = None
    instrument_trade_sizes: dict[str, int] = {
        "ETHUSDT-PERP": 1,
        "SOLUSDT-PERP": 1,
        "NEARUSDT-PERP": 3,
        "KAITOUSDC-PERP": 11,
    }
    instrument_max_positions: dict[str, int] = {
        "ETHUSDT-PERP": 4,
        "SOLUSDT-PERP": 10,
        "NEARUSDT-PERP": 30,
        "KAITOUSDC-PERP": 110,
    }
    persist_market_data: bool = True
    catalog_path: str = "data/catalog"
    flush_interval_secs: PositiveFloat = 5.0
    max_buffer_size: PositiveInt = 10_000


class GLFTMarketMaker(Strategy):
    """
    A GLFT (Guéant-Lehalle-Fernandez-Tapia) market maker for ``KAITOUSDC-PERP``.

    The strategy subscribes to configured market data streams, logs received
    data with simple counters, persists trade ticks and order book deltas, and
    on a timer computes an Avellaneda-Stoikov quote (reservation prices,
    spread, bid/ask per inventory level).

    It is monitoring-only by default. With ``enable_trading=True`` it also
    places post-only bid/ask limit orders around the reservation price for the
    current net inventory on each sample, cancelling and replacing them every
    cycle. A one-directional ``max_position`` long cap suppresses the buy leg
    once inventory reaches the cap.

    Parameters
    ----------
    config : GLFTMarketMakerConfig
        The configuration for the instance.

    """

    MID_SAMPLE_TIMER_NAME = "mid_price_sample"

    def __init__(self, config: GLFTMarketMakerConfig) -> None:
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
        self._quote_prices: dict[int, dict[str, str | None]] | None = None
        self._position: Decimal = Decimal(0)
        self._trade_size: Quantity | None = None
        self._pending_self_cancels: set[ClientOrderId] = set()

    def on_start(self) -> None:
        """
        Actions to be performed on strategy start.
        """
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return

        if self.config.enable_trading:
            try:
                symbol = self.config.instrument_id.symbol.value
                if self.config.trade_size is not None:
                    raw_trade_size = self.config.trade_size
                elif symbol in self.config.instrument_trade_sizes:
                    raw_trade_size = self.config.instrument_trade_sizes[symbol]
                elif self.instrument.min_quantity is not None:
                    raw_trade_size = self.instrument.min_quantity
                else:
                    raw_trade_size = self.instrument.size_increment
                self._trade_size = self.instrument.make_qty(raw_trade_size)
            except ValueError as e:
                self.log.error(
                    f"Invalid trade_size for {self.config.instrument_id}: {e}",
                )
                self.stop()
                return
            self.log.info(f"Trading ENABLED | trade_size={self._trade_size}")
        else:
            self.log.info("Trading DISABLED (monitor-only)")

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
            "Started GLFT market maker | "
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
            f"lot_size={self.config.lot_size} | "
            f"reservation_price_min_q={self.config.reservation_price_min_q} | "
            f"reservation_price_max_q={self.config.reservation_price_max_q} | "
            f"quote_intensity_k={self.config.quote_intensity_k} | "
            f"max_position={self.config.max_position} | "
            f"enable_trading={self.config.enable_trading} | "
            f"trade_size={self._trade_size} | "
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
            f"position={sample.position} | "
            f"count={self._mid_sample_count}",
        )

        realized = self.portfolio.realized_pnl(self.config.instrument_id)
        unrealized = self.portfolio.unrealized_pnl(self.config.instrument_id)
        total = self.portfolio.total_pnl(self.config.instrument_id)
        self.log.info(
            "PnL | "
            f"instrument_id={self.config.instrument_id} | "
            f"realized={realized} | "
            f"unrealized={unrealized} | "
            f"total={total}",
        )

        if self.config.enable_trading:
            self._requote_live()

    def on_order_filled(self, event: OrderFilled) -> None:
        """
        Actions to be performed when an order is filled.
        """
        # Only drop the self-cancel tag once fully filled; a partially filled
        # order must stay tagged so a later self-cancel isn't misread as
        # external.
        order = self.cache.order(event.client_order_id)
        if order is not None and order.is_closed:
            self._pending_self_cancels.discard(event.client_order_id)

    def on_order_canceled(self, event: OrderCanceled) -> None:
        """
        Actions to be performed when an order is canceled.
        """
        if event.client_order_id in self._pending_self_cancels:
            self._pending_self_cancels.discard(event.client_order_id)
            return
        # External cancel: nothing to do; the next timer tick re-quotes anyway.

    def on_order_expired(self, event: OrderExpired) -> None:
        """
        Actions to be performed when an order expires.

        Binance reports a post-only order that would cross as EXPIRED (not
        CANCELED), so this handler also drains the self-cancel tag.
        """
        self._pending_self_cancels.discard(event.client_order_id)

    def on_order_rejected(self, event: OrderRejected) -> None:
        """
        Actions to be performed when an order is rejected.
        """
        self._pending_self_cancels.discard(event.client_order_id)

    def _create_mid_price_sample(self, quote: QuoteTick) -> MidPriceSample:
        self._update_position()
        bid = quote.bid_price.as_decimal()
        ask = quote.ask_price.as_decimal()
        spread = ask - bid
        mid = bid + spread / Decimal("2")

        delta_s: Decimal | None = None
        if self.config.calculate_ewma_variance:
            prev_var = self._ewma_delta_s_var
            delta_s = self._update_ewma_delta_s_var(mid)
            if self._ewma_delta_s_var != prev_var:
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
            position=str(self._position),
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

    def _calculate_g(self) -> Decimal | None:
        """
        GLFT c = sqrt(σ²γ/(2kA) · (1+γ/k)^(1+k/γ)).

        Used as the per-unit reservation-price adjustment and as the first term
        of the total spread.  Returns None until the EWMA variance is seeded.
        """
        if self._ewma_delta_s_var is None:
            return None

        gamma = Decimal(str(self.config.reservation_price_gamma))
        k = Decimal(str(self.config.quote_intensity_k))
        a = Decimal(str(self.config.quote_arrival_a))
        ratio = gamma / k
        exponent = Decimal("1") + k / gamma
        g_sq = (gamma * self._ewma_delta_s_var / (Decimal("2") * k * a)) * (
            Decimal("1") + ratio
        ) ** exponent
        return g_sq.sqrt()

    def _calculate_reservation_prices(self, mid: Decimal) -> dict[int, str] | None:
        g = self._calculate_g()
        if g is None:
            return None

        symbol = self.config.instrument_id.symbol.value
        lot = self.config.instrument_trade_sizes.get(symbol, self.config.lot_size)
        return {
            q * lot: str(
                self._round_to_tick(
                    mid - Decimal(q * lot) * g,
                    ROUND_HALF_UP,
                )
            )
            for q in range(
                self.config.reservation_price_min_q,
                self.config.reservation_price_max_q + 1,
            )
        }

    def _reservation_price_for(
        self,
        mid: Decimal,
        q_raw_signed: Decimal,
    ) -> Decimal | None:
        """
        Reservation price for an arbitrary signed inventory level (raw units).

        GLFT formula: r = mid - q·g, where g is the per-unit price adjustment
        from :meth:`_calculate_g`.  Accepts any signed ``q`` (including zero
        and negative) so live quoting can key off the current net position.
        """
        g = self._calculate_g()
        if g is None:
            return None

        reservation = mid - q_raw_signed * g
        return self._round_to_tick(reservation, ROUND_HALF_UP)

    def _calculate_quote_spread(self) -> str | None:
        g = self._calculate_g()
        if g is None:
            return None

        gamma = Decimal(str(self.config.reservation_price_gamma))
        k = Decimal(str(self.config.quote_intensity_k))
        total_spread = g + (Decimal("2") / gamma) * (Decimal("1") + gamma / k).ln()
        return str(total_spread)

    def _calculate_quote_prices(self) -> dict[int, dict[str, str | None]] | None:
        if self._reservation_prices is None or self._quote_spread is None:
            return None

        half_spread = Decimal(self._quote_spread) / Decimal("2")
        # One-directional long cap: at or above max_position the strategy is
        # fully long, so buy (bid) quotes are suppressed and only sell (ask)
        # quotes are emitted to draw inventory back down. The short side is
        # never capped.
        suppress_bid = self._position >= self._effective_max_position()
        quote_prices: dict[int, dict[str, str | None]] = {}
        for q, reservation_price in self._reservation_prices.items():
            # Snap bid down / ask up to the tick grid so the realized spread
            # never narrows below the theoretical one due to rounding.
            quote_prices[q] = {
                "reservation_price": reservation_price,
                "bid": (
                    str(
                        self._round_to_tick(
                            Decimal(reservation_price) - half_spread,
                            ROUND_FLOOR,
                        ),
                    )
                    if not suppress_bid
                    else None
                ),
                "ask": str(
                    self._round_to_tick(
                        Decimal(reservation_price) + half_spread,
                        ROUND_CEILING,
                    ),
                ),
            }
        return quote_prices

    def _effective_max_position(self) -> Decimal:
        symbol = self.config.instrument_id.symbol.value
        raw = self.config.instrument_max_positions.get(symbol, self.config.max_position)
        return Decimal(raw)

    def _update_position(self) -> None:
        """
        Refresh the tracked net position for this strategy from the cache.

        Read-only and monitor-safe: it never submits, cancels, or closes
        orders. Scoped to this strategy's own positions. Under Binance USDT
        futures (HEDGING OMS) a LONG and SHORT position can coexist, so the
        signed quantities are summed for the net. In a pure data-only node
        (no execution client) there are no positions, so the tracked position
        stays at zero and the ``max_position`` gate never trips; the moment a
        real fill exists it is reflected automatically.
        """
        net = Decimal(0)
        for pos in self.cache.positions_open(
            instrument_id=self.config.instrument_id,
            strategy_id=self.id,
        ):
            qty = Decimal(str(pos.quantity))
            net += qty if pos.signed_qty > 0 else -qty
        self._position = net

    def _requote_live(self) -> None:
        """
        Refresh the post-only bid/ask quotes for the current inventory.

        Computes the reservation price for the current net position, cancels
        all resting orders, then places a sell (ask) and — subject to the
        one-directional ``max_position`` long cap — a buy (bid), each post-only
        GTC at one lot. Called from ``on_timer`` only when ``enable_trading``
        is True. ``self._position`` is refreshed earlier in the same tick by
        :meth:`_create_mid_price_sample`.
        """
        if (
            self.instrument is None
            or self._trade_size is None
            or self._ewma_delta_s_var is None
            or self._last_quote is None
        ):
            return

        instrument_id = self.config.instrument_id
        bid_dec = self._last_quote.bid_price.as_decimal()
        ask_dec = self._last_quote.ask_price.as_decimal()
        mid = bid_dec + (ask_dec - bid_dec) / Decimal("2")

        reservation = self._reservation_price_for(mid, self._position)
        spread_str = self._calculate_quote_spread()
        if reservation is None or spread_str is None:
            return
        half_spread = Decimal(spread_str) / Decimal("2")

        # Min-spread guard: if the modelled half-spread is below one tick the
        # snapped bid/ask would land on (or cross) the mid and Binance would
        # reject the post-only order every cycle (rate-limit risk). Wait until
        # the volatility estimate (sigma2) grows before quoting.
        tick = self.instrument.price_increment.as_decimal()
        if half_spread < tick:
            self.log.warning(
                "Skip requote | half_spread < tick | "
                f"half_spread={half_spread} | tick={tick}",
            )
            return

        bid_raw = reservation - half_spread
        ask_raw = reservation + half_spread
        trade_size = Decimal(str(self._trade_size))
        max_position = self._effective_max_position()

        # Record resting/inflight order IDs as self-cancels before the async
        # cancel_all_orders sweep so the resulting OrderCanceled/OrderExpired
        # events are recognised as our own; also accumulate worst-case long
        # exposure (canceled BUYs still count until the venue acks).
        pending_buy = Decimal(0)
        for order in (
            *self.cache.orders_open(instrument_id=instrument_id, strategy_id=self.id),
            *self.cache.orders_inflight(instrument_id=instrument_id, strategy_id=self.id),
        ):
            self._pending_self_cancels.add(order.client_order_id)
            if order.side == OrderSide.BUY:
                pending_buy += Decimal(str(order.leaves_qty))
        self.cancel_all_orders(instrument_id)

        # SELL (ask) suppressed at q=0 to avoid opening a short in one-way mode.
        suppress_ask = self._position <= Decimal(0)
        if not suppress_ask:
            ask_order = self.order_factory.limit(
                instrument_id=instrument_id,
                order_side=OrderSide.SELL,
                quantity=self._trade_size,
                price=self.instrument.make_price(
                    self._round_to_tick(ask_raw, ROUND_CEILING),
                ),
                time_in_force=TimeInForce.GTC,
                post_only=True,
            )
            self.submit_order(ask_order)
        else:
            self.log.info(
                "SELL suppressed | at zero position | "
                f"position={self._position}",
            )

        # BUY (bid) only if the worst-case long exposure stays within the cap.
        if self._position + pending_buy + trade_size <= max_position:
            bid_order = self.order_factory.limit(
                instrument_id=instrument_id,
                order_side=OrderSide.BUY,
                quantity=self._trade_size,
                price=self.instrument.make_price(
                    self._round_to_tick(bid_raw, ROUND_FLOOR),
                ),
                time_in_force=TimeInForce.GTC,
                post_only=True,
            )
            self.submit_order(bid_order)
        else:
            self.log.info(
                "BUY suppressed | at max_position | "
                f"position={self._position} | pending_buy={pending_buy} | "
                f"trade_size={trade_size} | max_position={max_position}",
            )

    def _round_to_tick(self, price: Decimal, rounding: str) -> Decimal:
        """
        Round a price onto the instrument tick grid.

        Uses ``self.instrument.price_increment`` as the grid spacing and applies
        the given decimal rounding mode (e.g. ``ROUND_FLOOR`` for bids,
        ``ROUND_CEILING`` for asks, ``ROUND_HALF_UP`` for reservation prices).

        When the instrument or its price increment is unavailable, the price is
        returned unchanged so callers fall back to the raw Decimal.

        """
        if self.instrument is None:
            return price

        tick = self.instrument.price_increment.as_decimal()
        if tick <= 0:
            return price

        ticks = (price / tick).to_integral_value(rounding=rounding)
        return ticks * tick

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
            self.log.info(f"Flushed {len(data)} order book deltas")
            self._book_delta_buffer.clear()

        if self._trade_buffer:
            data = sorted(self._trade_buffer, key=lambda x: x.ts_init)
            self._catalog.write_data(data, skip_disjoint_check=True)
            self.log.info(f"Flushed {len(data)} trades")
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
        if self.config.enable_trading and self.instrument is not None:
            self.cancel_all_orders(self.config.instrument_id)
            # reduce_only defaults to True; requires the Binance account to be
            # in one-way position mode (fails on connect under Hedge mode).
            self.close_all_positions(self.config.instrument_id)
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
            "Stopped GLFT market maker | "
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
            f"position={self._position} | "
            f"received_data={total > 0}",
        )
