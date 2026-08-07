"""Bybit 充提币业务包装，复用框架层
`nautilus_trader.adapters.bybit.http.wallet` 的签名实现，不重新造轮子。
"""

import os

from nautilus_trader.adapters.bybit.http.wallet import bybit_withdraw
from nautilus_trader.adapters.bybit.http.wallet import fetch_bybit_coin_info
from nautilus_trader.adapters.bybit.http.wallet import fetch_bybit_deposit_address
from nautilus_trader.adapters.bybit.http.wallet import fetch_bybit_deposit_records
from nautilus_trader.adapters.bybit.http.wallet import fetch_bybit_withdraw_records
from spread_monitor.utils import _normalize_chain


class BybitWallet:
    """满足 `wallet.base.WithdrawCapableWallet` + `DepositCapableWallet` 接口的薄包装。"""

    def __init__(self, api_key: str, api_secret: str) -> None:
        self._api_key = api_key
        self._api_secret = api_secret

    def fetch_withdraw_chains(self, base: str) -> dict[str, dict]:
        """查询提现链详情，返回时对链名做归一化处理。"""
        raw = fetch_bybit_coin_info(self._api_key, self._api_secret, base)
        return {_normalize_chain(chain): info for chain, info in raw.items()}

    def withdraw(
        self, base: str, chain: str, address: str, amount: float, tag: str | None = None,
    ) -> str:
        # chain 参数传入的是归一化后的链名，需要反查到 Bybit 的原始 chain 代码
        raw = fetch_bybit_coin_info(self._api_key, self._api_secret, base)
        target_chain = None
        for orig_chain in raw:
            if _normalize_chain(orig_chain) == chain.lower():
                target_chain = orig_chain
                break

        if not target_chain:
            raise ValueError(f"Bybit 不支持 {base} 在链 {chain} 上的提现")

        return bybit_withdraw(
            self._api_key,
            self._api_secret,
            coin=base,
            chain=target_chain,
            address=address,
            amount=amount,
            tag=tag,
        )

    def withdraw_status(self, coin: str, withdrawal_id: str) -> dict | None:
        records = fetch_bybit_withdraw_records(
            self._api_key, self._api_secret, coin, withdraw_id=withdrawal_id,
        )
        return records[0] if records else None

    def fetch_deposit_methods(self, base: str) -> dict[str, str]:
        """Bybit 的充值链与提现链是同一组（按 chainDeposit 标志过滤是否开放充值）。"""
        raw = fetch_bybit_coin_info(self._api_key, self._api_secret, base)
        return {
            _normalize_chain(chain): chain
            for chain, info in raw.items()
            if info.get("deposit_enabled")
        }

    def fetch_deposit_address(self, base: str, method: str) -> dict[str, str]:
        """获取 Bybit 某币种在指定链上的充值地址。method 是原始 chain 代码。"""
        return fetch_bybit_deposit_address(self._api_key, self._api_secret, base, method)

    def fetch_deposit_status(self, base: str) -> list[dict]:
        """查询 Bybit 充值记录。"""
        return fetch_bybit_deposit_records(self._api_key, self._api_secret, base)


def from_env() -> BybitWallet | None:
    """缺少 `BYBIT_API_KEY`/`BYBIT_API_SECRET` 时返回 None。"""
    api_key = os.environ.get("BYBIT_API_KEY")
    api_secret = os.environ.get("BYBIT_API_SECRET")
    if not api_key or not api_secret:
        return None
    return BybitWallet(api_key, api_secret)
