"""跨所转账用的钱包客户端统一接口。

策略层不再直接 import 具体所的钱包模块，而是按 venue 从 `WALLET_REGISTRY` 里取工厂函数
构建钱包客户端。新增交易所钱包时，只需在此包内新增模块并注册到 `WALLET_REGISTRY`。
"""

from spread_monitor.wallet.base import DepositAddress
from spread_monitor.wallet.base import DepositCapableWallet
from spread_monitor.wallet.base import WithdrawCapableWallet
from spread_monitor.wallet.base import WithdrawChainInfo
from spread_monitor.wallet.registry import WALLET_REGISTRY

__all__ = [
    "WALLET_REGISTRY",
    "WithdrawCapableWallet",
    "DepositCapableWallet",
    "WithdrawChainInfo",
    "DepositAddress",
]
