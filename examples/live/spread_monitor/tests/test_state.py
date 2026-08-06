from spread_monitor.state import ArbState
from spread_monitor.state import ArbStateStore
from spread_monitor.state import Phase


class FakeCache:
    def __init__(self):
        self._data: dict[str, bytes] = {}

    def add(self, key: str, value: bytes) -> None:
        self._data[key] = value

    def get(self, key: str):
        return self._data.get(key)


def test_load_missing_base_returns_idle_default():
    store = ArbStateStore(FakeCache())
    state = store.load("DOGE")
    assert state.base == "DOGE"
    assert state.phase == Phase.IDLE


def test_save_then_load_roundtrips_all_fields():
    cache = FakeCache()
    store = ArbStateStore(cache)

    state = ArbState(
        base="DOGE",
        phase=Phase.TRANSFERRING,
        spot_client_order_id="O-1",
        spot_qty="100.0",
        perp_client_order_id="O-2",
        perp_qty="100.0",
        perp_leg_attempt=1,
        withdrawal_id="W-1",
        withdrawal_chain="TRX",
        withdrawal_qty="50.0",
        transfer_started_at_ts=123.456,
        in_flight_roundtrip=True,
        roundtrip_buy_order_id="O-3",
        roundtrip_sell_order_id="O-4",
        last_error="boom",
    )
    store.save(state)

    reloaded = store.load("DOGE")
    assert reloaded == state


def test_states_for_different_bases_are_independent():
    cache = FakeCache()
    store = ArbStateStore(cache)

    doge_state = ArbState(base="DOGE", phase=Phase.ACTIVE)
    store.save(doge_state)

    ada_state = store.load("ADA")
    assert ada_state.phase == Phase.IDLE
    assert store.load("DOGE").phase == Phase.ACTIVE
