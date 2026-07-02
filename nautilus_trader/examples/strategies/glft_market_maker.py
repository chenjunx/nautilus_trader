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
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from decimal import ROUND_CEILING
from decimal import ROUND_FLOOR
from decimal import ROUND_HALF_UP
from decimal import Decimal
import math
import time
from typing import TYPE_CHECKING

try:
    import redis as _redis_mod
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

if TYPE_CHECKING:
    import redis as _redis_type_mod

import pandas as pd

from nautilus_trader.common.enums import LogColor
from nautilus_trader.common.events import TimeEvent
from nautilus_trader.config import PositiveFloat
from nautilus_trader.config import PositiveInt
from nautilus_trader.config import StrategyConfig
import msgspec
from nautilus_trader.core.data import Data
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.custom import customdataclass
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


class _RLSEstimator:
    """
    Recursive Least Squares for GLFT λ(δ) = A·exp(−k·δ).

    Fits log(λ) = log(A) − k·δ with a forgetting factor so older windows
    are down-weighted automatically.  State is θ = [log(A), −k] and a 2×2
    covariance matrix P.  No external dependencies — pure Python floats.
    """

    __slots__ = ("_lf", "_theta", "_P")

    def __init__(self, k0: float, a0: float, forgetting: float = 0.95) -> None:
        self._lf: float = forgetting
        self._theta: list[float] = [math.log(max(a0, 1e-9)), -k0]
        # Large initial covariance → learns quickly from first observations.
        self._P: list[list[float]] = [[1000.0, 0.0], [0.0, 1000.0]]

    def update(self, delta: float, rate: float) -> None:
        """Incorporate one (δ, observed_rate) observation."""
        if rate <= 0.0:
            return
        y = math.log(rate)
        P = self._P
        th = self._theta
        lf = self._lf

        # Px = P @ x  where x = [1, delta]
        Px0 = P[0][0] + P[0][1] * delta
        Px1 = P[1][0] + P[1][1] * delta

        # scalar denominator: λ + x^T P x
        denom = lf + Px0 + delta * Px1

        # Kalman gain and innovation
        K0 = Px0 / denom
        K1 = Px1 / denom
        innov = y - (th[0] + th[1] * delta)

        th[0] += K0 * innov
        th[1] += K1 * innov

        # covariance update: (P − K x^T P) / λ
        P[0][0] = (P[0][0] - K0 * Px0) / lf
        P[0][1] = (P[0][1] - K0 * Px1) / lf
        P[1][0] = (P[1][0] - K1 * Px0) / lf
        P[1][1] = (P[1][1] - K1 * Px1) / lf

    @property
    def k(self) -> float:
        return max(1.0, -self._theta[1])

    @property
    def a(self) -> float:
        return max(1e-6, math.exp(self._theta[0]))


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


@customdataclass
class GLFTParamsSnapshot(Data):
    """
    A structured, catalog-persistable snapshot of the strategy's live GLFT
    parameters — for offline time-series reconstruction of a run, as an
    alternative to parsing free-text logs.
    """

    instrument_id: InstrumentId
    k: str
    a: str
    gamma: float
    sigma: str
    mid: str
    position: str
    quote_spread: str
    reservation_price: str
    bid: str
    ask: str
    rls_enabled: bool


class GLFTMarketMakerConfig(StrategyConfig, frozen=True):
    """
    Configuration for ``GLFTMarketMaker`` instances.

    Parameters
    ----------
    instrument_id : InstrumentId
        The Binance USDT futures instrument ID to monitor.
    quote_intensity_k : PositiveFloat
        The order book depth/decay parameter (k) in the GLFT formula.
        Must be fitted from live LOB data for the target instrument — no default.
    quote_arrival_a : PositiveFloat
        The market-order arrival rate scaling parameter (A) in the GLFT formula.
        Must be fitted from live LOB data for the target instrument — no default.
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
        working. When ``True`` it places post-only bid/ask limit orders, requoting
        immediately on fills (q change) or when σ drifts beyond ``theta_vol``
        relative to the last-requote anchor.
    trade_size : PositiveInt, optional
        The per-order quantity (raw units) used when ``enable_trading`` is
        ``True``. If ``None`` (default) it resolves to ``lot_size`` so each
        quote is one lot and each fill moves inventory by one lot, matching the
        reservation-price grid.
    theta_vol : PositiveFloat, default 0.25
        Relative σ drift threshold for timer-driven requotes.  A new quote is
        issued when ``|σ_new − σ_anchor| / σ_anchor > theta_vol``, where
        ``σ_anchor`` is the σ value locked at the last requote.  After each
        σ-triggered requote the anchor is updated to the current σ.
    dead_band_ticks : PositiveFloat, default 1.5
        Minimum dead-band width expressed in price increments (ticks).  Filters
        pure noise: a mid move of less than 1 tick is exchange jitter, not a
        real price shift.
    dead_band_ratio : PositiveFloat, default 0.3
        Dead-band width as a fraction of the current half-spread.  Scales the
        threshold to the instrument's natural liquidity — wide-spread coins get
        a wider dead band automatically.  The effective dead band is
        ``max(dead_band_ticks × tick_size, dead_band_ratio × half_spread)``
        so whichever standard is stricter (larger) wins.
    enable_dead_band : bool, default True
        If ``True``, requote when mid moves beyond the dead band since the last
        quote submission.  Set to ``False`` to disable quote-tick driven
        requotes while keeping the ``_check_dead_band`` code intact.
    max_requote_interval_secs : PositiveFloat, default 300.0
        Watchdog: if no requote has been triggered for this many seconds the
        strategy forces one unconditionally.  Guards against edge cases where
        none of the event-driven triggers fires for an extended period.
    enable_rls_fitting : bool, default False
        If ``True``, k and A are updated continuously from live trade ticks
        using Recursive Least Squares with forgetting factor, instead of
        using the static ``quote_intensity_k`` / ``quote_arrival_a`` values.
    rls_forgetting : PositiveFloat, default 0.95
        Forgetting factor λ_f ∈ (0, 1).  Each update window contributes with
        weight (1-λ_f) and older windows decay as λ_f^n.  With a 200 s
        update interval, λ_f=0.95 gives effective memory ≈ 4 windows (800 s).
    rls_update_interval_secs : PositiveFloat, default 200.0
        How often (in seconds) to run an RLS update from the accumulated
        trade buffer.  Aligned to the mid-sample timer; actual interval is
        rounded up to the next timer tick.
    rls_redis_url : str or None, default None
        Redis URL for persisting fitted k/A across restarts, e.g.
        ``"redis://localhost:6379/0"`` or ``"redis://:password@host:6379/0"``.
        Keys written: ``rls:{symbol}:k`` and ``rls:{symbol}:a``.
        On ``on_start`` the strategy reads these keys first; on each RLS
        update the background worker overwrites them.  When ``None`` or when
        the ``redis`` package is not installed, persistence is silently skipped.
    persist_market_data : bool, default True
        If received trade ticks and order book deltas should be persisted.
    catalog_path : str, default "data/kaitousdc/catalog"
        The local parquet data catalog path for persisted market data.
    flush_interval_secs : PositiveFloat, default 5.0
        The maximum interval in seconds between market data flushes.
    max_buffer_size : PositiveInt, default 10000
        The maximum buffered trade and book delta count before flushing.
    persist_params : bool, default True
        If per-tick GLFT parameter snapshots (k, A, gamma, sigma, mid,
        position, reservation price, quoted bid/ask) should be persisted to
        the catalog as ``GLFTParamsSnapshot`` records, for offline
        time-series reconstruction of a run. Independent of
        ``persist_market_data``. Writes to disk happen asynchronously on a
        dedicated background thread so they never block the strategy's main
        event handling.
    params_flush_interval_secs : PositiveFloat, default 5.0
        The maximum interval in seconds between parameter snapshot flushes.
    params_max_buffer_size : PositiveInt, default 100
        The maximum buffered parameter snapshot count before flushing.

    """

    instrument_id: InstrumentId
    quote_intensity_k: PositiveFloat
    quote_arrival_a: PositiveFloat
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
    max_position: PositiveInt = 110
    enable_trading: bool = False
    trade_size: PositiveInt | None = None
    theta_vol: PositiveFloat = 0.25
    enable_dead_band: bool = True
    dead_band_ticks: PositiveFloat = 1.5
    dead_band_ratio: PositiveFloat = 0.3
    max_requote_interval_secs: PositiveFloat = 300.0
    instrument_trade_sizes: dict[str, int] = msgspec.field(
        default_factory=lambda: {
            "ETHUSDT-PERP": 1,
            "SOLUSDT-PERP": 1,
            "NEARUSDC-PERP": 3,
            "KAITOUSDC-PERP": 11,
        },
    )
    instrument_max_positions: dict[str, int] = msgspec.field(
        default_factory=lambda: {
            "ETHUSDT-PERP": 4,
            "SOLUSDT-PERP": 10,
            "NEARUSDC-PERP": 30,
            "KAITOUSDC-PERP": 110,
        },
    )
    enable_rls_fitting: bool = False
    rls_forgetting: PositiveFloat = 0.95
    rls_update_interval_secs: PositiveFloat = 200.0
    rls_redis_url: str | None = None
    persist_market_data: bool = True
    catalog_path: str = "data/catalog"
    flush_interval_secs: PositiveFloat = 5.0
    max_buffer_size: PositiveInt = 10_000
    persist_params: bool = True
    params_flush_interval_secs: PositiveFloat = 5.0
    params_max_buffer_size: PositiveInt = 100


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
        self._sigma_anchor: Decimal | None = None
        self._mid_at_last_quote: Decimal | None = None
        self._last_requote_ns: int = 0
        self._last_cancel_reason: str = ""
        # RLS parameter estimator (created in on_start when enabled)
        self._rls: _RLSEstimator | None = None
        self._rls_trade_buf: list[tuple[int, int]] = []  # (ts_ns, sweep_depth)
        self._rls_last_side: object = None  # AggressorSide enum; None before first trade
        self._rls_last_trade_ns: int = 0
        self._rls_sweep_depth: int = 0
        self._rls_last_update_ns: int = 0
        # Cached fitted values — written by background thread, read by main thread.
        # Decimal assignment is atomic under Python's GIL so no explicit lock needed.
        self._rls_k: Decimal = Decimal(str(config.quote_intensity_k))
        self._rls_a: Decimal = Decimal(str(config.quote_arrival_a))
        # Background executor (max_workers=1 serialises fitting jobs)
        self._rls_executor: ThreadPoolExecutor | None = None
        # Redis client for cross-restart persistence (None when disabled/unavailable)
        self._rls_redis: "_redis_type_mod.Redis | None" = None  # type: ignore[name-defined]
        # Parameter snapshot buffer, flushed asynchronously to the catalog.
        self._param_snapshot_buffer: list[GLFTParamsSnapshot] = []
        self._last_params_flush_monotonic = time.monotonic()
        # Background executor (max_workers=1 serialises catalog writes)
        self._params_executor: ThreadPoolExecutor | None = None

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

        if self.config.persist_market_data or self.config.persist_params:
            self._catalog = ParquetDataCatalog(self.config.catalog_path)
            self._catalog.write_data([self.instrument])

        if self.config.persist_params:
            self._params_executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="params-flush",
            )

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

        if self.config.enable_rls_fitting:
            self._rls_executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="rls-fit",
            )

            # Try to warm-start k/A from Redis before constructing the estimator.
            k_init = self.config.quote_intensity_k
            a_init = self.config.quote_arrival_a
            if self.config.rls_redis_url and _REDIS_AVAILABLE:
                try:
                    self._rls_redis = _redis_mod.from_url(  # type: ignore[union-attr]
                        self.config.rls_redis_url,
                        decode_responses=True,
                        socket_connect_timeout=2,
                        socket_timeout=2,  # covers individual get/set; prevents on_stop hang
                    )
                    sym = self.config.instrument_id.symbol.value
                    k_raw = self._rls_redis.get(f"rls:{sym}:k")
                    a_raw = self._rls_redis.get(f"rls:{sym}:a")
                    if k_raw is not None and a_raw is not None:
                        k_init = float(k_raw)
                        a_init = float(a_raw)
                        self._rls_k = Decimal(str(round(k_init, 2)))
                        self._rls_a = Decimal(str(round(a_init, 6)))
                        self.log.info(
                            "RLS warm-start from Redis | "
                            f"k={k_init:.1f} | a={a_init:.6f}",
                        )
                    else:
                        self.log.info(
                            "RLS Redis connected | no saved values found | "
                            "using config defaults",
                        )
                except Exception as exc:
                    self.log.warning(
                        f"RLS Redis connection failed — using config defaults | {exc}",
                    )

            self._rls = _RLSEstimator(
                k0=k_init,
                a0=a_init,
                forgetting=self.config.rls_forgetting,
            )
            self.log.info(
                "RLS fitting ENABLED | "
                f"k0={k_init:.2f} | a0={a_init:.6f} | "
                f"forgetting={self.config.rls_forgetting} | "
                f"update_interval={self.config.rls_update_interval_secs}s | "
                f"redis={'yes' if self._rls_redis is not None else 'no'}",
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

        if self.config.enable_trading and self.config.enable_dead_band:
            self._check_dead_band(tick)

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

        if self.config.persist_params and self._ewma_delta_s_var is not None:
            self._capture_params_snapshot()

        realized = self.portfolio.realized_pnl(self.config.instrument_id)
        unrealized = self.portfolio.unrealized_pnl(self.config.instrument_id)
        total = self.portfolio.total_pnl(self.config.instrument_id)
        total_val = float(total) if total is not None else 0.0
        pnl_color = LogColor.GREEN if total_val > 0 else (LogColor.RED if total_val < 0 else LogColor.NORMAL)
        self.log.info(
            "PnL | "
            f"instrument_id={self.config.instrument_id} | "
            f"realized={realized} | "
            f"unrealized={unrealized} | "
            f"total={total}",
            color=pnl_color,
        )

        if self.config.enable_trading and self._ewma_delta_s_var is not None:
            sigma = self._ewma_delta_s_var.sqrt()
            if self._sigma_anchor is None or self._sigma_anchor == 0:
                # Seed the anchor on the first valid σ — no requote yet.
                # Also re-seed when anchor is zero to avoid division by zero.
                self._sigma_anchor = sigma
            elif abs(sigma - self._sigma_anchor) / self._sigma_anchor > Decimal(
                str(self.config.theta_vol)
            ):
                self.log.info(
                    "σ threshold crossed | requoting | "
                    f"sigma={sigma} | sigma_anchor={self._sigma_anchor} | "
                    f"theta_vol={self.config.theta_vol}",
                )
                self._requote_live(reason="sigma_threshold")
                self._sigma_anchor = sigma
                if self._last_quote is not None:
                    bid = self._last_quote.bid_price.as_decimal()
                    ask = self._last_quote.ask_price.as_decimal()
                    self._mid_at_last_quote = bid + (ask - bid) / Decimal("2")

        if self.config.enable_trading:
            interval_ns = int(self.config.max_requote_interval_secs * 1e9)
            if self.clock.timestamp_ns() - self._last_requote_ns > interval_ns:
                self.log.info(
                    "Watchdog | requoting | "
                    f"no requote for >{self.config.max_requote_interval_secs}s",
                )
                self._requote_live(reason="watchdog")

        if self.config.enable_rls_fitting and self._rls is not None:
            now_ns = self.clock.timestamp_ns()
            interval_ns = int(self.config.rls_update_interval_secs * 1e9)
            if now_ns - self._rls_last_update_ns >= interval_ns:
                self._rls_update()
                self._rls_last_update_ns = now_ns

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

        # q changed — requote immediately without waiting for the next timer tick.
        if self.config.enable_trading:
            self._update_position()
            self._requote_live(reason="fill")
            if self._last_quote is not None:
                bid = self._last_quote.bid_price.as_decimal()
                ask = self._last_quote.ask_price.as_decimal()
                self._mid_at_last_quote = bid + (ask - bid) / Decimal("2")

    def on_order_canceled(self, event: OrderCanceled) -> None:
        """
        Actions to be performed when an order is canceled.
        """
        if event.client_order_id in self._pending_self_cancels:
            self._pending_self_cancels.discard(event.client_order_id)
            self.log.info(
                "Order self-canceled | "
                f"client_order_id={event.client_order_id} | "
                f"reason={self._last_cancel_reason}",
            )
            return
        self.log.info(
            "Order externally canceled | "
            f"client_order_id={event.client_order_id}",
        )

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
        k = self._effective_k()
        a = self._effective_a()
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
        k = self._effective_k()
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

    def _effective_k(self) -> Decimal:
        """Return k from RLS estimate when enabled, otherwise from config."""
        if self.config.enable_rls_fitting:
            return self._rls_k
        return Decimal(str(self.config.quote_intensity_k))

    def _effective_a(self) -> Decimal:
        """Return A from RLS estimate when enabled, otherwise from config."""
        if self.config.enable_rls_fitting:
            return self._rls_a
        return Decimal(str(self.config.quote_arrival_a))

    def _rls_update(self) -> None:
        """
        Coordinator (main thread): atomically swap the trade buffer and submit
        a fitting job to the background executor.

        The actual RLS computation happens in ``_rls_fit_worker`` so the event
        loop is never blocked.  The fitted k/A land in ``_rls_k`` / ``_rls_a``
        (the cache) asynchronously; ``_effective_k/_effective_a`` read from
        there without waiting.
        """
        if self._rls is None or self.instrument is None:
            return

        # Atomic buffer swap: hand the window to the worker, start a fresh one.
        buf = self._rls_trade_buf
        self._rls_trade_buf = []

        if len(buf) < 4:
            return

        tick_size = float(self.instrument.price_increment.as_decimal())
        T = self.config.rls_update_interval_secs

        if self._rls_executor is not None:
            self._rls_executor.submit(self._rls_fit_worker, buf, tick_size, T)
        else:
            # Fallback: run inline when executor was shut down (e.g. during tests).
            self._rls_fit_worker(buf, tick_size, T)

    def _rls_fit_worker(
        self,
        buf: list[tuple[int, int]],
        tick_size: float,
        T: float,
    ) -> None:
        """
        Background worker: fit RLS, update the k/A cache, persist to Redis.

        Runs in a single-worker ``ThreadPoolExecutor`` — never concurrent with
        itself.  Writes to ``_rls_k`` / ``_rls_a`` are Decimal assignments which
        are atomic under Python's GIL, safe for the main thread to read at any
        time without explicit locking.
        """
        if self._rls is None:
            return

        depth_counts: dict[int, int] = {}
        for _, depth in buf:
            depth_counts[depth] = depth_counts.get(depth, 0) + 1

        if len(depth_counts) < 2:
            return

        for depth in sorted(depth_counts):
            rate = depth_counts[depth] / T
            delta = (depth + 0.5) * tick_size
            self._rls.update(delta, rate)

        k_new = self._rls.k
        a_new = self._rls.a

        # Atomic cache write (GIL-protected object reference assignment).
        self._rls_k = Decimal(str(round(k_new, 2)))
        self._rls_a = Decimal(str(round(a_new, 6)))

        # Persist to Redis (fire-and-forget; failure is non-fatal).
        if self._rls_redis is not None:
            try:
                sym = self.config.instrument_id.symbol.value
                self._rls_redis.set(f"rls:{sym}:k", str(k_new))
                self._rls_redis.set(f"rls:{sym}:a", str(a_new))
            except Exception as exc:
                self.log.warning(f"RLS Redis write failed | {exc}")

        self.log.info(
            "RLS update | "
            f"k={k_new:.1f} | a={a_new:.6f} | "
            f"window_trades={len(buf)} | depth_levels={len(depth_counts)}",
        )

    def _check_dead_band(self, tick: QuoteTick) -> None:
        """
        Trigger a requote when mid has walked outside the dead band since the
        last actual quote submission.

        The dead band is ``max(dead_band_ticks × tick_size,
        dead_band_ratio × half_spread)`` — whichever is stricter (larger).
        Both the mid anchor and spread come from the last requote, not rolling
        values, so intra-tick EWMA jitter cannot retrigger this check.
        """
        if (
            self._mid_at_last_quote is None
            or self._quote_spread is None
            or self.instrument is None
        ):
            return

        bid = tick.bid_price.as_decimal()
        ask = tick.ask_price.as_decimal()
        mid = bid + (ask - bid) / Decimal("2")

        tick_size = self.instrument.price_increment.as_decimal()
        half_spread = Decimal(self._quote_spread) / Decimal("2")
        dead_band = max(
            Decimal(str(self.config.dead_band_ticks)) * tick_size,
            Decimal(str(self.config.dead_band_ratio)) * half_spread,
        )

        if abs(mid - self._mid_at_last_quote) > dead_band:
            self.log.info(
                "Dead band crossed | requoting | "
                f"mid={mid} | mid_anchor={self._mid_at_last_quote} | "
                f"dead_band={dead_band}",
            )
            self._requote_live(reason="dead_band")
            self._mid_at_last_quote = mid

    def _requote_live(self, reason: str = "timer") -> None:
        """
        Refresh the post-only bid/ask quotes for the current inventory.

        Computes the reservation price for the current net position, cancels
        all resting orders, then places a sell (ask) and — subject to the
        one-directional ``max_position`` long cap — a buy (bid), each post-only
        GTC at one lot. Called from ``on_timer`` only when ``enable_trading``
        is True. ``self._position`` is refreshed earlier in the same tick by
        :meth:`_create_mid_price_sample`.
        """
        self._last_requote_ns = self.clock.timestamp_ns()

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
                "Clamp half_spread to tick | half_spread < tick | "
                f"half_spread={half_spread} | tick={tick}",
            )
            half_spread = tick

        bid_raw = reservation - half_spread
        ask_raw = reservation + half_spread
        trade_size = Decimal(str(self._trade_size))
        max_position = self._effective_max_position()
        suppress_ask = self._position <= Decimal(0)

        bid_snap = self._round_to_tick(bid_raw, ROUND_FLOOR)
        ask_snap = self._round_to_tick(ask_raw, ROUND_CEILING)

        # Skip the cancel/resubmit cycle when resting orders already reflect
        # the target prices — avoids unnecessary churn when the mid hasn't
        # moved between timer ticks.
        open_orders = [
            *self.cache.orders_open(instrument_id=instrument_id, strategy_id=self.id),
            *self.cache.orders_inflight(instrument_id=instrument_id, strategy_id=self.id),
        ]
        resting_bids = [o for o in open_orders if o.side == OrderSide.BUY]
        resting_asks = [o for o in open_orders if o.side == OrderSide.SELL]
        bid_ok = len(resting_bids) == 1 and resting_bids[0].price.as_decimal() == bid_snap
        ask_ok = (suppress_ask and not resting_asks) or (
            not suppress_ask
            and len(resting_asks) == 1
            and resting_asks[0].price.as_decimal() == ask_snap
        )
        if bid_ok and ask_ok:
            self.log.debug(
                "Skip requote | prices unchanged | "
                f"bid={bid_snap} | ask={ask_snap}",
            )
            return

        # Record resting/inflight order IDs as self-cancels before the async
        # cancel_all_orders sweep so the resulting OrderCanceled/OrderExpired
        # events are recognised as our own; also accumulate worst-case long
        # exposure (canceled BUYs still count until the venue acks).
        pending_buy = Decimal(0)
        for order in open_orders:
            self._pending_self_cancels.add(order.client_order_id)
            if order.side == OrderSide.BUY:
                pending_buy += Decimal(str(order.leaves_qty))
        self._last_cancel_reason = reason
        if open_orders:
            self.log.info(
                f"Cancelling orders | reason={reason} | "
                f"count={len(open_orders)} | "
                f"order_ids={[o.client_order_id.value for o in open_orders]}",
            )
        self.cancel_all_orders(instrument_id)

        # SELL (ask) suppressed at q=0 to avoid opening a short in one-way mode.
        if not suppress_ask:
            ask_order = self.order_factory.limit(
                instrument_id=instrument_id,
                order_side=OrderSide.SELL,
                quantity=self._trade_size,
                price=self.instrument.make_price(ask_snap),
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
                price=self.instrument.make_price(bid_snap),
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

    def _capture_params_snapshot(self) -> None:
        """
        Build a ``GLFTParamsSnapshot`` from current state and buffer it.

        Pure in-memory: no disk or network I/O on this (main) thread. Actual
        persistence happens later on the background executor via
        :meth:`_flush_params_if_needed`.
        """
        if self._last_quote is None or self._ewma_delta_s_var is None:
            return

        bid_dec = self._last_quote.bid_price.as_decimal()
        ask_dec = self._last_quote.ask_price.as_decimal()
        mid_dec = bid_dec + (ask_dec - bid_dec) / Decimal("2")

        reservation = self._reservation_price_for(mid_dec, self._position)
        if reservation is None or self._quote_spread is None:
            return

        half_spread = Decimal(self._quote_spread) / Decimal("2")
        bid_q = self._round_to_tick(reservation - half_spread, ROUND_FLOOR)
        ask_q = self._round_to_tick(reservation + half_spread, ROUND_CEILING)

        snapshot = GLFTParamsSnapshot(
            instrument_id=self.config.instrument_id,
            k=str(self._effective_k()),
            a=str(self._effective_a()),
            gamma=float(self.config.reservation_price_gamma),
            sigma=str(self._ewma_delta_s_var.sqrt()),
            mid=str(mid_dec),
            position=str(self._position),
            quote_spread=self._quote_spread,
            reservation_price=str(reservation),
            bid=str(bid_q),
            ask=str(ask_q),
            rls_enabled=self.config.enable_rls_fitting,
            ts_event=self._last_quote.ts_event,
            ts_init=self.clock.timestamp_ns(),
        )
        self._param_snapshot_buffer.append(snapshot)
        self._flush_params_if_needed()

    def _flush_params_if_needed(self) -> None:
        if not self.config.persist_params:
            return

        if len(self._param_snapshot_buffer) >= self.config.params_max_buffer_size:
            self._flush_params_async()
            return

        elapsed = time.monotonic() - self._last_params_flush_monotonic
        if elapsed >= self.config.params_flush_interval_secs:
            self._flush_params_async()

    def _flush_params_async(self) -> None:
        """
        Hand the buffered snapshots off to the background executor.

        Swaps the buffer for a fresh list (O(1), main-thread only) then
        submits the swapped-out list to a single-worker executor for the
        actual (blocking) Parquet write, so the main event loop never waits
        on disk I/O.
        """
        self._last_params_flush_monotonic = time.monotonic()

        if not self._param_snapshot_buffer or self._catalog is None:
            return

        buf = self._param_snapshot_buffer
        self._param_snapshot_buffer = []

        if self._params_executor is not None:
            self._params_executor.submit(self._flush_params_worker, buf)
        else:
            # Fallback: run inline when executor was shut down (e.g. during tests).
            self._flush_params_worker(buf)

    def _flush_params_worker(self, buf: list[GLFTParamsSnapshot]) -> None:
        """
        Background worker: write buffered parameter snapshots to the catalog.

        Runs in a single-worker ``ThreadPoolExecutor`` — never concurrent
        with itself. ``GLFTParamsSnapshot`` is its own Parquet dataset type
        (catalog partitions by type), so this cannot race with the
        main-thread trade/book-delta writes in :meth:`_flush_market_data`.
        """
        if self._catalog is None:
            return

        data = sorted(buf, key=lambda x: x.ts_init)
        self._catalog.write_data(data, skip_disjoint_check=True)
        self.log.info(f"Flushed {len(data)} param snapshots")

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

        if self.config.enable_rls_fitting:
            ts_ns = tick.ts_event
            side = tick.aggressor_side
            if side == self._rls_last_side and ts_ns - self._rls_last_trade_ns <= 1_000_000_000:
                self._rls_sweep_depth += 1
            else:
                self._rls_sweep_depth = 0
            self._rls_last_side = side
            self._rls_last_trade_ns = ts_ns
            self._rls_trade_buf.append((ts_ns, self._rls_sweep_depth))

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
        self._flush_params_async()
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

        # Wait for any in-flight RLS fitting job to finish so the final k/A
        # values are persisted to Redis before the process exits.
        if self._rls_executor is not None:
            self._rls_executor.shutdown(wait=True)
            self._rls_executor = None
        if self._rls_redis is not None:
            try:
                self._rls_redis.close()
            except Exception:
                pass
            self._rls_redis = None

        # Wait for any in-flight parameter-snapshot flush to finish so the
        # final snapshots are written before the process exits.
        if self._params_executor is not None:
            self._params_executor.shutdown(wait=True)
            self._params_executor = None
