"""钱包客户端接口定义（Protocol）。

拆成 `WithdrawCapableWallet`/`DepositCapableWallet` 两个 Protocol 而不是一个大接口，
是因为一个所不一定同时具备提现方/入金方两种角色（比如以后接入的所可能只作为入金目的地），
不用为了满足接口硬塞 `NotImplementedError`。
"""

from __future__ import annotations

import sys

if sys.version_info >= (3, 8):
    from typing import Protocol
    from typing import TypedDict
else:
    from typing_extensions import Protocol
    from typing_extensions import TypedDict


class WithdrawChainInfo(TypedDict):
    """提现链信息（手续费、最小提现量、是否开放）。"""

    fee: float
    min: float
    enabled: bool


class DepositAddress(TypedDict):
    """充值地址（地址本身 + tag/memo，某些链需要）。"""

    address: str
    tag: str


class WithdrawCapableWallet(Protocol):
    """能作为转账源头（提现方）的交易所钱包客户端需实现的接口。"""

    def fetch_withdraw_chains(self, base: str) -> dict[str, WithdrawChainInfo]:
        """查询指定币种支持的提现链及其手续费明细。

        返回 {归一化链名: 链信息}。
        """
        ...

    def withdraw(
        self,
        base: str,
        chain: str,
        address: str,
        amount: float,
        tag: str | None = None,
    ) -> str:
        """提交提现申请，返回提现单号。

        调用方必须把这里抛出的任何异常（超时/网络错误/HTTP 错误）都当作"提现结果未知"
        而不是"提现失败"处理——绝不能因为这里出错就自动重发一次提现，必须先人工核实
        交易所网页端是否已经提交成功，否则有双花风险。
        """
        ...


class DepositCapableWallet(Protocol):
    """能作为转账目的地（入金方）的交易所钱包客户端需实现的接口。"""

    def fetch_deposit_methods(self, base: str) -> dict[str, str]:
        """查询指定币种支持的充值方式。

        返回 {归一化链名: 交易所原始 method 名}。
        """
        ...

    def fetch_deposit_address(self, base: str, method: str) -> DepositAddress:
        """获取指定币种在指定充值方式下的充值地址。"""
        ...

    def fetch_deposit_status(self, base: str) -> list[dict]:
        """查询指定币种的充值到账记录（辅助确认链上转账是否已到账）。"""
        ...
