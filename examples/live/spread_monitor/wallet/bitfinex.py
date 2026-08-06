"""Bitfinex 充提币业务包装，复用框架层
`nautilus_trader.adapters.bitfinex.http.wallet` 的签名实现，不重新造轮子。
"""

import os

from nautilus_trader.adapters.bitfinex.http.wallet import bitfinex_withdraw
from nautilus_trader.adapters.bitfinex.http.wallet import bitfinex_withdraw_status
from nautilus_trader.adapters.bitfinex.http.wallet import fetch_bitfinex_currency_code
from nautilus_trader.adapters.bitfinex.http.wallet import fetch_bitfinex_deposit_address
from nautilus_trader.adapters.bitfinex.http.wallet import fetch_bitfinex_deposit_status
from nautilus_trader.adapters.bitfinex.http.wallet import fetch_bitfinex_methods_for_currency
from spread_monitor.utils import _normalize_chain


class BitfinexWallet:
    """满足 `wallet.base.WithdrawCapableWallet` + `DepositCapableWallet` 接口的薄包装。"""

    def __init__(self, api_key: str, api_secret: str, wallet: str = "exchange") -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._wallet = wallet  # "exchange" | "margin" | "funding"

    def fetch_withdraw_chains(self, base: str) -> dict[str, dict]:
        """查询提现链详情。Bitfinex 通过公开接口 pub:map:tx:method 返回该币种支持的 method 列表。

        返回 {归一化链名: {"method": 原始 method 名}}
        """
        # 先把通用符号（如 "USDT"）转换成 Bitfinex 内部代码（如 "UST"）
        currency_code = fetch_bitfinex_currency_code(base)
        methods = fetch_bitfinex_methods_for_currency(currency_code)

        chains: dict[str, dict] = {}
        for method in methods:
            norm = _normalize_chain(method)
            if norm:
                chains[norm] = {"method": method}
        return chains

    def withdraw(
        self, base: str, chain: str, address: str, amount: float, tag: str | None = None,
    ) -> str:
        # chain 参数传入的是归一化后的链名，需要反查到 Bitfinex 的 method
        # 先查询该币种支持的所有 method，找到归一化后匹配的那个
        currency_code = fetch_bitfinex_currency_code(base)
        methods = fetch_bitfinex_methods_for_currency(currency_code)

        target_method = None
        for method in methods:
            if _normalize_chain(method) == chain.lower():
                target_method = method
                break

        if not target_method:
            raise ValueError(f"Bitfinex 不支持 {base} 在链 {chain} 上的提现")

        return bitfinex_withdraw(
            self._api_key,
            self._api_secret,
            wallet=self._wallet,
            method=target_method,
            amount=amount,
            address=address,
            payment_id=tag,
        )

    def withdraw_status(self, coin: str, withdrawal_id: str) -> dict | None:
        return bitfinex_withdraw_status(self._api_key, self._api_secret, withdrawal_id)

    def fetch_deposit_methods(self, base: str) -> dict[str, str]:
        """Bitfinex 的充值链与提现链是同一组，直接用 fetch_withdraw_chains 获取。"""
        chains = self.fetch_withdraw_chains(base)
        # 返回 {归一化链名: 原始 method 名}
        return {norm: info["method"] for norm, info in chains.items()}

    def fetch_deposit_address(self, base: str, method: str) -> dict[str, str]:
        """获取 Bitfinex 充值地址。

        注意：Bitfinex 文档说明，对于需要 memo/tag 的币种（如 EOS/XRP），
        无法通过 API 获取，需要在网页端查看。
        """
        return fetch_bitfinex_deposit_address(
            self._api_key,
            self._api_secret,
            wallet=self._wallet,
            method=method.lower(),
        )

    def fetch_deposit_status(self, base: str) -> list[dict]:
        """查询 Bitfinex 充值记录。"""
        currency_code = fetch_bitfinex_currency_code(base)
        return fetch_bitfinex_deposit_status(self._api_key, self._api_secret, currency_code)


def from_env() -> BitfinexWallet | None:
    """缺少 `BITFINEX_API_KEY`/`BITFINEX_API_SECRET` 时返回 None。"""
    api_key = os.environ.get("BITFINEX_API_KEY")
    api_secret = os.environ.get("BITFINEX_API_SECRET")
    if not api_key or not api_secret:
        return None
    return BitfinexWallet(api_key, api_secret)
