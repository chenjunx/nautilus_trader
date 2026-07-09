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
import json
from types import SimpleNamespace

import pandas as pd
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
from nautilus_trader.examples.strategies import glft_market_maker
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


def test_cross_guard_clamps_ask_when_skew_would_cross_best_bid(env):
    # Arrange — a huge long position pushes reservation far below mid (skew
    # dominates once max_g_ratio's cap on the offset is applied), which would
    # put the naive ask below the current best bid. `_update_position` is
    # stubbed so the huge position survives both sample cycles instead of
    # being recomputed from the (empty) cache.
    strategy = _make_strategy(env, enable_trading=True)
    strategy.start()
    strategy._update_position = lambda: None
    strategy._position = Decimal("1000")

    # Act
    _seed_then_quote(env, strategy, delta=0.2)

    # Assert — best_bid = moved_mid - 0.1 = 40000.1, tick = 0.1
    orders = env.cache.orders_open(instrument_id=env.instrument.id)
    sell = next(o for o in orders if o.side == OrderSide.SELL)
    buys = [o for o in orders if o.side == OrderSide.BUY]
    assert sell.price.as_decimal() == Decimal("40000.2")
    assert buys == []  # suppressed: position already far above max_position


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


# -------------------------------------------------------------------------------------------------
# Daily G* lookup-table compute + cache job (compute/cache only — not wired into
# quoting yet). Mirrors the RLS coordinator/worker pattern: `_lookup_table_recompute`
# (main thread) submits to `_lookup_table_executor`, `_lookup_table_worker` (background
# thread) does the heavy lifting. Setting the executor to None after `start()` forces
# `_lookup_table_recompute` down its synchronous fallback path so these tests don't
# need a real ThreadPoolExecutor or real order_book_deltas catalog data.
# -------------------------------------------------------------------------------------------------


def test_lookup_table_disabled_by_default_registers_no_timer(env):
    # Arrange / Act
    strategy = _make_strategy(env, enable_lookup_table=False)
    strategy.start()

    # Assert
    assert strategy.LOOKUP_TABLE_TIMER_NAME not in strategy.clock.timer_names()
    assert strategy._lookup_table is None
    assert strategy._lookup_table_executor is None


def test_lookup_table_enabled_registers_timer_and_tolerates_missing_cache(env, tmp_path):
    # Arrange / Act — no cache file exists at lookup_table_output_dir, and
    # catalog_path doesn't even exist yet; on_start must not raise.
    strategy = _make_strategy(
        env,
        enable_lookup_table=True,
        catalog_path=str(tmp_path / "catalog"),
        lookup_table_output_dir=str(tmp_path / "lookup_table"),
    )
    strategy.start()

    # Assert
    assert strategy.LOOKUP_TABLE_TIMER_NAME in strategy.clock.timer_names()
    assert strategy._lookup_table is None
    assert strategy._lookup_table_executor is not None


def test_lookup_table_loads_fresh_cache_on_start(env, tmp_path):
    # Arrange — a cache file "computed" just now (well within the default
    # 2-day staleness window) should be loaded synchronously at startup.
    # Staleness is judged against real wall-clock time (on_start doesn't use
    # the strategy's injected TestClock for this), not env.clock.
    output_dir = tmp_path / "lookup_table"
    output_dir.mkdir()
    now_ns = pd.Timestamp.now(tz="UTC").value
    cached_table = {"computed_at_ns": now_ns, "converged": True}
    (output_dir / "final_lookup_table.json").write_text(json.dumps(cached_table))

    # Act
    strategy = _make_strategy(
        env,
        enable_lookup_table=True,
        catalog_path=str(tmp_path / "catalog"),
        lookup_table_output_dir=str(output_dir),
    )
    strategy.start()

    # Assert — cache hit means no immediate recompute is scheduled.
    assert strategy._lookup_table == cached_table
    assert strategy.clock.next_time_ns(strategy.LOOKUP_TABLE_TIMER_NAME) is not None


def test_lookup_table_ignores_stale_cache_on_start(env, tmp_path):
    # Arrange — a cache file older than lookup_table_stale_after_days must be
    # ignored (logged, not loaded) so a stale table can never silently persist.
    # Staleness is judged against real wall-clock time, not env.clock.
    output_dir = tmp_path / "lookup_table"
    output_dir.mkdir()
    stale_ns = pd.Timestamp.now(tz="UTC").value - int(5 * 86400 * 1e9)  # 5 days old
    (output_dir / "final_lookup_table.json").write_text(
        json.dumps({"computed_at_ns": stale_ns, "converged": True}),
    )

    # Act
    strategy = _make_strategy(
        env,
        enable_lookup_table=True,
        catalog_path=str(tmp_path / "catalog"),
        lookup_table_output_dir=str(output_dir),
        lookup_table_stale_after_days=2.0,
    )
    strategy.start()

    # Assert
    assert strategy._lookup_table is None


def test_lookup_table_timer_dispatches_to_recompute(env, tmp_path, monkeypatch):
    # Arrange
    strategy = _make_strategy(
        env,
        enable_lookup_table=True,
        catalog_path=str(tmp_path / "catalog"),
        lookup_table_output_dir=str(tmp_path / "lookup_table"),
    )
    strategy.start()
    calls = []
    monkeypatch.setattr(strategy, "_lookup_table_recompute", lambda: calls.append(1))

    # Act
    ts = _next_ts(env)
    event = TimeEvent(strategy.LOOKUP_TABLE_TIMER_NAME, UUID4(), ts, ts)
    strategy.on_timer(event)

    # Assert
    assert calls == [1]


def test_lookup_table_worker_updates_cache_and_writes_file(env, tmp_path, monkeypatch):
    # Arrange — stub out the heavy compute_lookup_table entrypoint so this
    # test exercises only the coordinator/worker wiring, not the real
    # order-book replay/matrix math (covered separately against real catalog
    # data, not as a fast unit test).
    output_dir = tmp_path / "lookup_table"
    fake_table = {
        "computed_at_ns": 123,
        "converged": True,
        "n_iters": 3,
        "health": {"residual": 1e-12},
        "reliability_metadata": {"n_unreliable": 2},
    }
    calls = {}

    def fake_compute_lookup_table(**kwargs):
        calls.update(kwargs)
        return fake_table

    monkeypatch.setattr(
        glft_market_maker.glft_lookup_table,
        "compute_lookup_table",
        fake_compute_lookup_table,
    )

    strategy = _make_strategy(
        env,
        enable_lookup_table=True,
        catalog_path=str(tmp_path / "catalog"),
        lookup_table_output_dir=str(output_dir),
        lookup_table_window_days=7,
    )
    strategy.start()
    # Force the synchronous fallback path (no live background thread needed).
    strategy._lookup_table_executor.shutdown(wait=True)
    strategy._lookup_table_executor = None

    # Act
    strategy._lookup_table_recompute()

    # Assert
    assert strategy._lookup_table == fake_table
    assert calls["instrument_id"] == env.instrument.id.value
    assert calls["window_days"] == 7
    written = json.loads((output_dir / "final_lookup_table.json").read_text())
    assert written == fake_table


def test_lookup_table_worker_keeps_previous_cache_on_failure(env, tmp_path, monkeypatch):
    # Arrange — a recompute that raises must not clear an already-good cache;
    # it's better to keep serving yesterday's table than go blank.
    def raising_compute_lookup_table(**kwargs):
        raise ValueError("no order_book_deltas found in window")

    monkeypatch.setattr(
        glft_market_maker.glft_lookup_table,
        "compute_lookup_table",
        raising_compute_lookup_table,
    )

    strategy = _make_strategy(
        env,
        enable_lookup_table=True,
        catalog_path=str(tmp_path / "catalog"),
        lookup_table_output_dir=str(tmp_path / "lookup_table"),
    )
    strategy.start()
    strategy._lookup_table_executor.shutdown(wait=True)
    strategy._lookup_table_executor = None
    sentinel = {"converged": True}
    strategy._lookup_table = sentinel

    # Act
    strategy._lookup_table_recompute()

    # Assert
    assert strategy._lookup_table is sentinel


# -------------------------------------------------------------------------------------------------
# Micro-price (mid + G*(I_bucket, s_bucket)) — compute + record only, not read by quoting logic.
# -------------------------------------------------------------------------------------------------


def _make_table_i_by_s(default=0.0) -> list:
    return [[default, default] for _ in range(10)]


def test_micro_price_not_computed_without_lookup_table(env):
    # Arrange — no lookup table has been loaded/computed yet.
    strategy = _make_strategy(env)
    strategy.start()
    assert strategy._lookup_table is None

    ts = _next_ts(env)
    tick = TestDataStubs.quote_tick(
        instrument=env.instrument,
        bid_price=40000.0,
        ask_price=40000.1,
        ts_event=ts,
        ts_init=ts,
    )

    # Act
    strategy.on_quote_tick(tick)

    # Assert
    assert strategy._micro_price is None
    assert strategy._micro_price_i_bucket is None
    assert strategy._micro_price_s_bucket is None


def test_micro_price_computed_from_quote_tick(env):
    # Arrange — bid_qty=750, ask_qty=250 -> I=0.75 -> I_bucket=7; bid=40000.0,
    # ask=40000.1, tick=0.1 -> spread_ticks=1 -> s_bucket=0.
    strategy = _make_strategy(env)
    strategy.start()
    table = _make_table_i_by_s()
    table[7][0] = 0.05
    strategy._lookup_table = {"table_I_by_S": table}

    ts = _next_ts(env)
    tick = TestDataStubs.quote_tick(
        instrument=env.instrument,
        bid_price=40000.0,
        ask_price=40000.1,
        bid_size=750,
        ask_size=250,
        ts_event=ts,
        ts_init=ts,
    )

    # Act
    strategy.on_quote_tick(tick)

    # Assert — mid = 40000.05, G* = 0.05 -> micro_price = 40000.10
    assert strategy._micro_price == Decimal("40000.10")
    assert strategy._micro_price_i_bucket == 7
    assert strategy._micro_price_s_bucket == 0


def test_micro_price_bucket_edges(env):
    # Arrange
    strategy = _make_strategy(env)
    strategy.start()
    strategy._lookup_table = {"table_I_by_S": _make_table_i_by_s()}

    def quote(bid_size, ask_size, bid_price, ask_price):
        ts = _next_ts(env)
        tick = TestDataStubs.quote_tick(
            instrument=env.instrument,
            bid_price=bid_price,
            ask_price=ask_price,
            bid_size=bid_size,
            ask_size=ask_size,
            ts_event=ts,
            ts_init=ts,
        )
        strategy.on_quote_tick(tick)

    # Act/Assert — nearly all size on the ask side -> I near 0 -> I_bucket=0.
    quote(1, 999, 40000.0, 40000.1)
    assert strategy._micro_price_i_bucket == 0

    # nearly all size on the bid side -> I near 1 -> I_bucket=9 (clipped).
    quote(999, 1, 40000.0, 40000.1)
    assert strategy._micro_price_i_bucket == 9

    # 1-tick spread -> s_bucket=0.
    quote(500, 500, 40000.0, 40000.1)
    assert strategy._micro_price_s_bucket == 0

    # 2-tick spread -> s_bucket=1.
    quote(500, 500, 40000.0, 40000.2)
    assert strategy._micro_price_s_bucket == 1


def test_mid_price_sample_includes_micro_price(env):
    # Arrange — bid_qty=ask_qty=100_000 (TestDataStubs.quote_tick defaults) ->
    # I=0.5 -> I_bucket=5; 1-tick spread -> s_bucket=0.
    strategy = _make_strategy(env, sample_mid=True)
    strategy.start()
    table = _make_table_i_by_s()
    table[5][0] = 0.02
    strategy._lookup_table = {"table_I_by_S": table}

    # Act
    _process_quote(env, 40000.0, 40000.1)
    _fire_mid_timer(env, strategy)

    # Assert
    sample = strategy._mid_samples[-1]
    assert strategy._micro_price is not None
    assert sample.micro_price == str(strategy._micro_price)
    assert sample.micro_price_i_bucket == 5
    assert sample.micro_price_s_bucket == 0
