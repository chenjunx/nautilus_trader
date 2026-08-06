"""Gate.io 充提币业务包装，复用框架层
`nautilus_trader.adapters.gateio.spot.http.wallet` 的签名实现，不重新造轮子。
"""

import os

from nautilus_trader.adapters.gateio.spot.http.wallet import fetch_gateio_deposit_address
from nautilus_trader.adapters.gateio.spot.http.wallet import fetch_gateio_deposit_status
from nautilus_trader.adapters.gateio.spot.http.wallet import fetch_gateio_withdraw_chains
from nautilus_trader.adapters.gateio.spot.http.wallet import gateio_withdraw
from nautilus_trader.adapters.gateio.spot.http.wallet import gateio_withdraw_status
from spread_monitor.utils import _normalize_chain


class GateIoWallet:
    """满足 `wallet.base.WithdrawCapableWallet` + `DepositCapableWallet` 接口的薄包装。"""

    def __init__(self, api_key: str, api_secret: str) -> None:
        self._api_key = api_key
        self._api_secret = api_secret

    def fetch_withdraw_chains(self, base: str) -> dict[str, dict]:
        """查询提现链详情，返回时对链名做归一化处理。"""
        raw = fetch_gateio_withdraw_chains(self._api_key, self._api_secret, base)
        return {_normalize_chain(chain): info for chain, info in raw.items()}

    def withdraw(
        self, base: str, chain: str, address: str, amount: float, tag: str | None = None,
    ) -> str:
        return gateio_withdraw(
            self._api_key, self._api_secret, base, chain, address, amount, memo=tag,
        )

    def withdraw_status(self, coin: str, withdrawal_id: str) -> dict | None:
        return gateio_withdraw_status(self._api_key, self._api_secret, withdrawal_id)

    def fetch_deposit_methods(self, base: str) -> dict[str, str]:
        """Gate.io 的提现链与充值链是同一组，直接用 fetch_withdraw_chains 获取。"""
        chains = self.fetch_withdraw_chains(base)
        # 返回 {归一化链名: 原始链名}，与 Kraken 保持一致的接口
        return {norm: orig for norm, orig in chains.items()}

    def fetch_deposit_address(self, base: str, method: str) -> dict[str, str]:
        """获取 Gate.io 某币种在指定链上的充值地址。"""
        return fetch_gateio_deposit_address(self._api_key, self._api_secret, base, method)

    def fetch_deposit_status(self, base: str) -> list[dict]:
        """查询 Gate.io 充值记录（最近 30 天）。"""
        return fetch_gateio_deposit_status(self._api_key, self._api_secret, base)


def from_env() -> GateIoWallet | None:
    """缺少 `GATEIO_API_KEY`/`GATEIO_API_SECRET` 时返回 None。"""
    api_key = os.environ.get("GATEIO_API_KEY") or os.environ.get("GATEIO_SPOT_API_KEY")
    api_secret = os.environ.get("GATEIO_API_SECRET") or os.environ.get("GATEIO_SPOT_API_SECRET")
    if not api_key or not api_secret:
        return None
    return GateIoWallet(api_key, api_secret)
