"""Binance 提现业务包装，复用框架层
`nautilus_trader.adapters.binance.spot.http.wallet` 的签名实现，不重新造轮子。
"""

import os

from nautilus_trader.adapters.binance.spot.http.wallet import binance_withdraw
from nautilus_trader.adapters.binance.spot.http.wallet import binance_withdraw_status
from nautilus_trader.adapters.binance.spot.http.wallet import fetch_binance_withdraw_chain_details
from spread_monitor.utils import _normalize_chain


class BinanceWallet:
    """满足 `wallet.base.WithdrawCapableWallet` 接口的薄包装，方法体都是转调框架层裸函数。"""

    def __init__(self, api_key: str, api_secret: str) -> None:
        self._api_key = api_key
        self._api_secret = api_secret

    def fetch_withdraw_chains(self, base: str) -> dict[str, dict]:
        """查询提现链详情，返回时对链名做归一化处理（框架层不做归一化，保持职责单一）。"""
        raw = fetch_binance_withdraw_chain_details(self._api_key, self._api_secret).get(base, {})
        return {_normalize_chain(chain): info for chain, info in raw.items()}

    def withdraw(
        self, base: str, chain: str, address: str, amount: float, tag: str | None = None,
    ) -> str:
        return binance_withdraw(
            self._api_key, self._api_secret, base, chain, address, amount, address_tag=tag,
        )

    def withdraw_status(self, coin: str, withdrawal_id: str) -> dict | None:
        return binance_withdraw_status(self._api_key, self._api_secret, coin, withdrawal_id)


def from_env() -> BinanceWallet | None:
    """`BINANCE_TRADE_API_KEY`/`_SECRET` 优先，缺省回退到 `BINANCE_API_KEY`/`_SECRET`。
    两者都拿不到时返回 None（由调用方决定是否报错）。
    """
    api_key = os.environ.get("BINANCE_TRADE_API_KEY") or os.environ.get("BINANCE_API_KEY")
    api_secret = os.environ.get("BINANCE_TRADE_API_SECRET") or os.environ.get("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        return None
    return BinanceWallet(api_key, api_secret)
