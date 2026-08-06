"""按 base 持久化的建仓/套利状态机，通过 `Strategy.cache` 的 `add`/`get` 做 pickle 读写，
保证进程重启不丢失"正在转账中"这类中间状态、不会重复建仓。

状态流转：IDLE -> BUILDING_SPOT -> BUILDING_PERP -> TRANSFERRING -> ACTIVE（套利轮转发生在
ACTIVE 内部，不改变 phase），异常路径终态 PAUSED_ERROR（对冲失败已紧急平仓，需人工处理才能恢复）。
"""

import pickle
from dataclasses import dataclass


class Phase:
    IDLE = "IDLE"
    BUILDING_SPOT = "BUILDING_SPOT"
    BUILDING_PERP = "BUILDING_PERP"
    TRANSFERRING = "TRANSFERRING"
    ACTIVE = "ACTIVE"
    PAUSED_ERROR = "PAUSED_ERROR"


@dataclass
class ArbState:
    base: str
    phase: str = Phase.IDLE

    # BUILDING_SPOT 腿
    spot_client_order_id: str | None = None
    spot_qty: str | None = None  # Decimal 存成 str，避免不同 nautilus 版本间 pickle 不兼容

    # BUILDING_PERP 腿
    perp_client_order_id: str | None = None
    perp_qty: str | None = None
    perp_leg_attempt: int = 0

    # TRANSFERRING（链上转账）
    withdrawal_id: str | None = None
    withdrawal_chain: str | None = None
    withdrawal_qty: str | None = None
    transfer_started_at_ts: float | None = None

    # ACTIVE 阶段的套利下单防重叠
    in_flight_roundtrip: bool = False
    roundtrip_buy_order_id: str | None = None
    roundtrip_sell_order_id: str | None = None

    last_error: str | None = None


class ArbStateStore:
    """`cache` 只要求有 `add(key: str, value: bytes)`/`get(key: str) -> bytes | None`
    两个方法（`Strategy.cache` 满足），不直接依赖 nautilus_trader 类型。
    """

    _KEY_PREFIX = "arb_state:"

    def __init__(self, cache) -> None:
        self._cache = cache

    def _key(self, base: str) -> str:
        return f"{self._KEY_PREFIX}{base}"

    def load(self, base: str) -> ArbState:
        raw = self._cache.get(self._key(base))
        if raw is None:
            return ArbState(base=base)
        return pickle.loads(raw)  # noqa: S301 - 只反序列化本进程自己写入的数据

    def save(self, state: ArbState) -> None:
        self._cache.add(self._key(state.base), pickle.dumps(state))
