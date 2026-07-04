# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""Backtest harness tests for the live (``enable_trading``) path of
``GLFTMarketMaker``.

These exercise the post-only market-making behaviour through a full
``SimulatedExchange`` + ``BacktestExecClient`` stack. Quoting is driven by the
strategy's mid-sample timer: each cycle feeds a quote tick (to seed
``_last_quote``) then synthesises the ``TimeEvent`` the strategy registered and
calls ``on_timer`` directly, followed by an ``exchange.process`` to flush the
submitted/cancelled orders through the matching engine.

A mid move of 0.2 between the first and second sample yields sigma2 = 0.04 and a
half-spread of ~0.30, comfortably above the ``BTCUSDT-PERP`` tick of 0.1 so the
min-spread guard lets quotes land.

These tests require the compiled Cython extensions to run.
"""

from collections import deque
from decimal import Decimal
from types import SimpleNamespace

import pytest

from nautilus_trader.backtest.data_client import BacktestMarketDataClient
from nautilus_trader.backtest.engine import SimulatedExchange
from nautilus_trader.backtest.execution_client import BacktestExecClient
from nautilus_trader.backtest.models import FillModel
from nautilus_trader.backtest.models import LatencyModel
from nautilus_trader.backtest.models import MakerTakerFeeModel
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.component import TestClock
from nautilus_trader.common.events import TimeEvent
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.data.engine import DataEngine
from nautilus_trader.examples.strategies.glft_market_maker import GLFTMarketMaker
from nautilus_trader.examples.strategies.glft_market_maker import GLFTMarketMakerConfig
from nautilus_trader.examples.strategies.glft_market_maker import _PendingDecrease
from nautilus_trader.examples.strategies.glft_market_maker import _QueueTracker
from nautilus_trader.examples.strategies.glft_market_maker import _TradeCacheRecord
from nautilus_trader.execution.engine import ExecutionEngine
from nautilus_trader.model.currencies import USDT
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.risk.engine import RiskEngine
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.data import TestDataStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


BINANCE = Venue("BINANCE")
_INSTRUMENT = TestInstrumentProvider.btcusdt_perp_binance()
# tick = price_increment = 0.1. half_spread must clear the tick before the
# min-spread guard lets any order rest. With gamma=0.1, horizon=150 a mid move
# of 0.2 -> sigma2 = 0.04 -> half_spread ~ 0.30 (case 2-6); a move of 0.1 ->
# sigma2 = 0.01 -> half_spread ~ 0.0755, below the tick (case 7).
_MID_SEED = Decimal("40000.0")  # above the instrument min_price of 261.1


@pytest.fixture
def env():
    """
    Full backtest environment wired for the KAITOUSDC live market-maker tests.
    """
    clock = TestClock()
    trader_id = TestIdStubs.trader_id()
    msgbus = MessageBus(trader_id=trader_id, clock=clock)
    cache = TestComponentStubs.cache()
    portfolio = Portfolio(msgbus=msgbus, cache=cache, clock=clock)
    data_engine = DataEngine(msgbus=msgbus, cache=cache, clock=clock)
    exec_engine = ExecutionEngine(msgbus=msgbus, cache=cache, clock=clock)
    RiskEngine(portfolio=portfolio, msgbus=msgbus, cache=cache, clock=clock)

    instrument = _INSTRUMENT
    exchange = SimulatedExchange(
        venue=BINANCE,
        oms_type=OmsType.NETTING,
        account_type=AccountType.MARGIN,
        base_currency=USDT,
        starting_balances=[Money(10_000_000.0, USDT)],
        default_leverage=Decimal(50),
        leverages={},
        modules=[],
        fill_model=FillModel(),
        fee_model=MakerTakerFeeModel(),
        portfolio=portfolio,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        latency_model=LatencyModel(0),
    )
    exchange.add_instrument(instrument)
    cache.add_instrument(instrument)

    data_client = BacktestMarketDataClient(
        client_id=ClientId("BINANCE"),
        msgbus=msgbus,
        cache=cache,
        clock=clock,
    )
    exec_client = BacktestExecClient(
        exchange=exchange,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
    )

    exchange.register_client(exec_client)
    data_engine.register_client(data_client)
    exec_engine.register_client(exec_client)
    exchange.reset()
    data_engine.start()
    exec_engine.start()

    return SimpleNamespace(
        clock=clock,
        trader_id=trader_id,
        msgbus=msgbus,
        cache=cache,
        portfolio=portfolio,
        data_engine=data_engine,
        exec_engine=exec_engine,
        exchange=exchange,
        instrument=instrument,
        # Monotonic nanosecond counter advanced strictly between every helper
        # call so the matching engine never sees a non-monotonic timestamp.
        t=1_646_199_312_128_000_000,
    )


def _next_ts(env) -> int:
    env.t += 1_000_000_000  # 1s
    env.clock.set_time(env.t)
    return env.t


def _process_quote(env, bid_price: float, ask_price: float) -> None:
    ts = _next_ts(env)
    tick = TestDataStubs.quote_tick(
        instrument=env.instrument,
        bid_price=bid_price,
        ask_price=ask_price,
        ts_event=ts,
        ts_init=ts,
    )
    env.data_engine.process(tick)
    env.exchange.process_quote_tick(tick)
    env.exchange.process(ts)


def _fire_mid_timer(env, strategy: GLFTMarketMaker) -> None:
    ts = _next_ts(env)
    event = TimeEvent(strategy.MID_SAMPLE_TIMER_NAME, UUID4(), ts, ts)
    strategy.on_timer(event)
    env.exchange.process(ts)


def _make_strategy(env, **overrides) -> GLFTMarketMaker:
    defaults = {
        "instrument_id": env.instrument.id,
        # quote_intensity_k/quote_arrival_a became required (no default) in
        # dbaa01063f; kept at their former default values here so the
        # half-spread numbers baked into this file's assertions don't shift.
        "quote_intensity_k": 2473.25,
        "quote_arrival_a": 266.13,
        "bar_type": None,
        "subscribe_quotes": True,
        "subscribe_trades": False,
        "subscribe_bars": False,
        "subscribe_book_deltas": False,
        "persist_market_data": False,
        "sample_mid": True,
        "calculate_ewma_variance": True,
    }
    defaults.update(overrides)
    config = GLFTMarketMakerConfig(**defaults)
    strategy = GLFTMarketMaker(config=config)
    strategy.register(
        trader_id=env.trader_id,
        portfolio=env.portfolio,
        msgbus=env.msgbus,
        cache=env.cache,
        clock=env.clock,
    )
    return strategy


def _seed_then_quote(
    env,
    strategy: GLFTMarketMaker,
    *,
    delta: float,
) -> None:
    """
    Run two quote+timer cycles so the EWMA variance is established (first cycle
    only seeds ``_last_mid``) and a second mid shifted by ``delta`` triggers a
    requote on the second timer tick.
    """
    seed = float(_MID_SEED)
    moved = seed + delta
    _process_quote(env, seed - 0.1, seed + 0.1)  # mid = seed
    _fire_mid_timer(env, strategy)
    _process_quote(env, moved - 0.1, moved + 0.1)  # mid = moved
    _fire_mid_timer(env, strategy)


def test_enable_trading_false_places_no_orders(env):
    # Arrange
    strategy = _make_strategy(env, enable_trading=False)
    strategy.start()

    # Act — two cycles build the EWMA variance, but trading stays off
    _seed_then_quote(env, strategy, delta=0.2)

    # Assert
    assert strategy._trade_size is None
    assert len(env.cache.orders_open(instrument_id=env.instrument.id)) == 0


def test_enable_trading_true_places_one_buy_and_one_sell(env):
    # Arrange
    strategy = _make_strategy(env, enable_trading=True)
    strategy.start()

    # Act
    _seed_then_quote(env, strategy, delta=0.2)

    # Assert
    orders = env.cache.orders_open(instrument_id=env.instrument.id)
    buys = [o for o in orders if o.side == OrderSide.BUY]
    sells = [o for o in orders if o.side == OrderSide.SELL]
    assert len(buys) == 1
    assert len(sells) == 1
    assert all(o.is_post_only for o in orders)
    assert all(o.time_in_force == TimeInForce.GTC for o in orders)


def test_quote_brackets_mid_without_self_cross(env):
    # Arrange
    strategy = _make_strategy(env, enable_trading=True)
    strategy.start()

    # Act — reservation at flat inventory equals mid (40000.2)
    _seed_then_quote(env, strategy, delta=0.2)

    # Assert
    mid = Decimal("40000.2")
    orders = env.cache.orders_open(instrument_id=env.instrument.id)
    buy = next(o for o in orders if o.side == OrderSide.BUY)
    sell = next(o for o in orders if o.side == OrderSide.SELL)
    assert buy.price.as_decimal() < mid
    assert sell.price.as_decimal() > mid


def test_at_max_position_suppresses_buy(env):
    # Arrange — max_position (5) < trade_size/lot_size (11): the worst-case long
    # (position + pending_buy + trade_size = 11) would breach the cap, so the
    # BUY leg is suppressed while the SELL leg still rests.
    strategy = _make_strategy(env, enable_trading=True, max_position=5)
    strategy.start()

    # Act
    _seed_then_quote(env, strategy, delta=0.2)

    # Assert
    orders = env.cache.orders_open(instrument_id=env.instrument.id)
    buys = [o for o in orders if o.side == OrderSide.BUY]
    sells = [o for o in orders if o.side == OrderSide.SELL]
    assert len(buys) == 0
    assert len(sells) == 1


def test_requote_cancels_old_orders_and_places_new(env):
    # Arrange
    strategy = _make_strategy(env, enable_trading=True)
    strategy.start()
    _seed_then_quote(env, strategy, delta=0.2)
    first_orders = {
        o.client_order_id for o in env.cache.orders_open(instrument_id=env.instrument.id)
    }
    assert len(first_orders) == 2

    # Act — run another quote+timer pair; the requote must cancel the 2 old
    # orders and place 2 fresh ones. delta stays at 0.2 so the moved quote's bid
    # (next_mid - 0.1) never crosses the resting ask (mid + ~0.3) and no resting
    # order fills ahead of the cancel.
    _seed_then_quote(env, strategy, delta=0.2)
    second_orders = {
        o.client_order_id for o in env.cache.orders_open(instrument_id=env.instrument.id)
    }

    # Assert
    assert len(second_orders) == 2
    assert first_orders.isdisjoint(second_orders)


def test_pending_self_cancels_drained_after_requote(env):
    # Arrange
    strategy = _make_strategy(env, enable_trading=True)
    strategy.start()
    _seed_then_quote(env, strategy, delta=0.2)
    assert strategy._pending_self_cancels == set()

    # Act — a second requote tags the resting orders as self-cancels, then
    # cancels them; the OrderCanceled acks drain the set back to empty
    _seed_then_quote(env, strategy, delta=0.2)

    # Assert
    assert strategy._pending_self_cancels == set()


def test_min_spread_guard_skips_quoting(env):
    # Arrange — a 0.1 move (smallest tick) gives sigma2 = 0.01 and a half-spread
    # of ~0.0755, below the 0.1 tick, so the min-spread guard skips quoting
    strategy = _make_strategy(env, enable_trading=True)
    strategy.start()

    # Act
    _seed_then_quote(env, strategy, delta=0.1)

    # Assert
    assert len(env.cache.orders_open(instrument_id=env.instrument.id)) == 0


# -------------------------------------------------------------------------------------------------
# Queue-position estimation (`_settle_queue_positions` and friends)
#
# These exercise the settlement/matching logic directly on the strategy's
# internal state (trackers, trade cache, pending decreases) rather than
# through real order book deltas/trade ticks — the matching math is the
# highest-risk part of the feature and is easiest to verify against
# hand-computed expectations in isolation.
# -------------------------------------------------------------------------------------------------


def test_settle_queue_positions_full_match_deducts_matched_qty(env):
    # Arrange — a decrease of 12 (50 -> 38) fully explained by a single cached
    # trade inside the match window: no cancellation remainder.
    strategy = _make_strategy(env, enable_trading=True, estimate_queue_position=True)
    strategy.start()

    price = Decimal("40000.0")
    tracker = _QueueTracker(
        price=price,
        side=OrderSide.BUY,
        client_order_id=None,
        q_ahead_point=Decimal(50),
        q_ahead_upper=Decimal(50),
        last_level_size=Decimal(50),
        last_ts_event=1_000,
    )
    strategy._queue_trackers[OrderSide.BUY] = tracker
    strategy._trade_cache[price] = deque(
        [_TradeCacheRecord(qty=Decimal(12), ts_event=1_500, remaining=Decimal(12))],
    )
    strategy._pending_decreases.append(
        _PendingDecrease(
            price=price,
            side=OrderSide.BUY,
            level_before=Decimal(50),
            level_after=Decimal(38),
            window_start_ts_event=1_000,
            window_end_ts_event=2_000,
            due_ns=env.clock.timestamp_ns(),
            retry_due_ns=None,
        ),
    )

    # Act
    strategy._settle_queue_positions()

    # Assert
    assert tracker.q_ahead_point == Decimal(38)
    assert tracker.q_ahead_upper == Decimal(38)
    assert len(strategy._pending_decreases) == 0
    assert price not in strategy._trade_cache  # fully-consumed record is dropped


def test_settle_queue_positions_partial_match_shrinks_and_caps_by_own_qty(env):
    # Arrange — a decrease of 40 (100 -> 60) where only 20 is explained by a
    # cached trade; the remaining 20 is treated as cancellations, shrinking
    # the point estimate proportionally, then both estimates are capped by
    # (level_after - own resting qty).
    strategy = _make_strategy(
        env,
        enable_trading=True,
        estimate_queue_position=True,
        trade_size=2,
    )
    strategy.start()
    _seed_then_quote(env, strategy, delta=0.2)

    tracker = strategy._queue_trackers[OrderSide.BUY]
    assert tracker is not None
    assert tracker.client_order_id is not None
    own_order = env.cache.order(tracker.client_order_id)
    assert Decimal(str(own_order.leaves_qty)) == Decimal(2)

    price = tracker.price
    tracker.q_ahead_point = Decimal(100)
    tracker.q_ahead_upper = Decimal(100)
    tracker.last_level_size = Decimal(100)
    tracker.last_ts_event = 1_000
    strategy._trade_cache[price] = deque(
        [_TradeCacheRecord(qty=Decimal(20), ts_event=3_000, remaining=Decimal(20))],
    )
    strategy._pending_decreases.append(
        _PendingDecrease(
            price=price,
            side=OrderSide.BUY,
            level_before=Decimal(100),
            level_after=Decimal(60),
            window_start_ts_event=1_000,
            window_end_ts_event=5_000,
            due_ns=env.clock.timestamp_ns(),
            retry_due_ns=None,
        ),
    )

    # Act
    strategy._settle_queue_positions()

    # Assert — matched=20 -> 80; ratio 60/80=0.75 -> 60; cap 60-2=58 -> 58
    assert tracker.q_ahead_point == Decimal(58)
    assert tracker.q_ahead_upper == Decimal(58)


def test_settle_queue_positions_ignores_trades_outside_window_and_schedules_retry(env):
    # Arrange — the only cached trade at this price is well before the match
    # window opened, so the first settlement attempt must not consume it and
    # must not finalize yet, only schedule a retry.
    strategy = _make_strategy(env, enable_trading=True, estimate_queue_position=True)
    strategy.start()

    price = Decimal("100")
    tracker = _QueueTracker(
        price=price,
        side=OrderSide.SELL,
        client_order_id=None,
        q_ahead_point=Decimal(50),
        q_ahead_upper=Decimal(50),
        last_level_size=Decimal(50),
        last_ts_event=1_000,
    )
    strategy._queue_trackers[OrderSide.SELL] = tracker
    strategy._trade_cache[price] = deque(
        [_TradeCacheRecord(qty=Decimal(12), ts_event=500, remaining=Decimal(12))],
    )
    strategy._pending_decreases.append(
        _PendingDecrease(
            price=price,
            side=OrderSide.SELL,
            level_before=Decimal(50),
            level_after=Decimal(38),
            window_start_ts_event=1_000,
            window_end_ts_event=2_000,
            due_ns=env.clock.timestamp_ns(),
            retry_due_ns=None,
        ),
    )

    # Act
    strategy._settle_queue_positions()

    # Assert — not finalized: still pending with a retry scheduled, tracker
    # untouched, and the out-of-window trade record is still cached intact.
    assert len(strategy._pending_decreases) == 1
    assert strategy._pending_decreases[0].retry_due_ns is not None
    assert tracker.q_ahead_point == Decimal(50)
    assert strategy._trade_cache[price][0].remaining == Decimal(12)


def test_settle_queue_positions_finalizes_pure_cancellation_after_retry(env):
    # Arrange — no cached trades at all; after the retry deadline passes with
    # still nothing matched, the decrease must finalize as a pure cancel.
    strategy = _make_strategy(env, enable_trading=True, estimate_queue_position=True)
    strategy.start()

    price = Decimal("100")
    tracker = _QueueTracker(
        price=price,
        side=OrderSide.SELL,
        client_order_id=None,
        q_ahead_point=Decimal(50),
        q_ahead_upper=Decimal(50),
        last_level_size=Decimal(50),
        last_ts_event=1_000,
    )
    strategy._queue_trackers[OrderSide.SELL] = tracker
    strategy._pending_decreases.append(
        _PendingDecrease(
            price=price,
            side=OrderSide.SELL,
            level_before=Decimal(50),
            level_after=Decimal(30),
            window_start_ts_event=1_000,
            window_end_ts_event=2_000,
            due_ns=env.clock.timestamp_ns(),
            retry_due_ns=None,
        ),
    )

    # Act — first pass finds nothing and schedules a retry.
    strategy._settle_queue_positions()
    retry_due_ns = strategy._pending_decreases[0].retry_due_ns
    assert retry_due_ns is not None

    # Advance past the retry deadline and settle again.
    env.clock.set_time(retry_due_ns)
    strategy._settle_queue_positions()

    # Assert — matched=0 -> ratio 30/50=0.6 applied to 50 -> 30; cap 30-0=30
    assert len(strategy._pending_decreases) == 0
    assert tracker.q_ahead_point == Decimal(30)
    assert tracker.q_ahead_upper == Decimal(30)


def test_settle_queue_positions_drops_stale_pending_when_price_changed(env):
    # Arrange — the tracker has since been requoted to a new price; a pending
    # decrease for the old price must be dropped without touching the
    # (unrelated) current tracker state.
    strategy = _make_strategy(env, enable_trading=True, estimate_queue_position=True)
    strategy.start()

    tracker = _QueueTracker(
        price=Decimal("105"),
        side=OrderSide.SELL,
        client_order_id=None,
        q_ahead_point=Decimal(10),
        q_ahead_upper=Decimal(10),
        last_level_size=Decimal(10),
        last_ts_event=5_000,
    )
    strategy._queue_trackers[OrderSide.SELL] = tracker
    strategy._pending_decreases.append(
        _PendingDecrease(
            price=Decimal("100"),
            side=OrderSide.SELL,
            level_before=Decimal(50),
            level_after=Decimal(30),
            window_start_ts_event=1_000,
            window_end_ts_event=2_000,
            due_ns=env.clock.timestamp_ns(),
            retry_due_ns=None,
        ),
    )

    # Act
    strategy._settle_queue_positions()

    # Assert
    assert len(strategy._pending_decreases) == 0
    assert tracker.q_ahead_point == Decimal(10)
    assert tracker.q_ahead_upper == Decimal(10)
