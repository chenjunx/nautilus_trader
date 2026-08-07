"""钱包注册表：venue -> 从环境变量构建对应钱包客户端的工厂函数。

新增交易所钱包时，只需在此注册一行，不需要改动 `execution_strategy.py` 的调用逻辑。
key 与 `venues.py:VENUE_REGISTRY` 用同一套 venue 常量，保持一致。
"""

from collections.abc import Callable

from nautilus_trader.adapters.binance import BINANCE
from nautilus_trader.adapters.bitfinex import BITFINEX
from nautilus_trader.adapters.bybit import BYBIT
from nautilus_trader.adapters.gateio import GATEIO
from nautilus_trader.adapters.kraken import KRAKEN
from spread_monitor.wallet import binance
from spread_monitor.wallet import bitfinex
from spread_monitor.wallet import bybit
from spread_monitor.wallet import gateio
from spread_monitor.wallet import kraken

WALLET_REGISTRY: dict[str, Callable[[], object | None]] = {
    BINANCE: binance.from_env,
    KRAKEN: kraken.from_env,
    GATEIO: gateio.from_env,
    BITFINEX: bitfinex.from_env,
    BYBIT: bybit.from_env,
}
