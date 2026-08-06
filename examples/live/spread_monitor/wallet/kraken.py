"""Kraken 入金（充值）相关的签名 REST 调用，复用框架层
`nautilus_trader.adapters.kraken.http.wallet` 的签名实现，不重新造轮子。

关键正确性点：这里全部查询的是 Kraken **入金**能力（DepositMethods/DepositAddresses/
DepositStatus），跟 `fetch_kraken_withdraw_methods`（Kraken **出金**能力，给只读监控
判断"提现链是否匹配"用）方向相反，绝不能混用——我们是要把从 Binance 提出来的币存进 Kraken，
选链必须用 Binance 的提现链 ∩ Kraken 的**入金**链，选错方向可能选中一条 Kraken 出金
支持但入金不支持的链，转过去的钱可能卡住甚至丢失。
"""

import os

from nautilus_trader.adapters.kraken.http.wallet import fetch_kraken_deposit_addresses
from nautilus_trader.adapters.kraken.http.wallet import fetch_kraken_deposit_methods
from nautilus_trader.adapters.kraken.http.wallet import fetch_kraken_deposit_status
from spread_monitor.utils import _normalize_chain


# 进程内缓存，(asset, method) 只需要拿一次充值地址
_deposit_address_cache: dict[tuple[str, str], dict[str, str]] = {}


class KrakenWallet:
    """满足 `wallet.base.DepositCapableWallet` 接口的薄包装，方法体都是转调框架层裸函数。"""

    def __init__(self, api_key: str, api_secret: str) -> None:
        self._api_key = api_key
        self._api_secret = api_secret

    def fetch_deposit_methods(self, base: str) -> dict[str, str]:
        """拉取 Kraken 某币种支持的入金方式。

        返回 {归一化链名: Kraken method 原始名}，用于跟 Binance 的提现链取交集选链。
        """
        rows = fetch_kraken_deposit_methods(self._api_key, self._api_secret, base)
        result: dict[str, str] = {}
        for row in rows:
            method = str(row.get("method", ""))
            norm = _normalize_chain(method)
            if norm:
                result[norm] = method
        return result

    def fetch_deposit_address(self, base: str, method: str) -> dict[str, str]:
        """获取 Kraken 某币种在指定入金方式下的充值地址，没有已存在地址时才新建一个。

        进程内按 (asset, method) 缓存一次即可。
        """
        cache_key = (base.upper(), method)
        if cache_key in _deposit_address_cache:
            return _deposit_address_cache[cache_key]

        rows = fetch_kraken_deposit_addresses(self._api_key, self._api_secret, base, method)
        if not rows:
            # 该 (asset, method) 还没有过充值地址，显式请求新建一个
            rows = fetch_kraken_deposit_addresses(
                self._api_key, self._api_secret, base, method, new=True,
            )
        if not rows:
            raise RuntimeError(f"Kraken 未返回 {base} ({method}) 的入金地址")

        row = rows[0]
        result = {"address": str(row["address"]), "tag": str(row.get("tag", "") or "")}
        _deposit_address_cache[cache_key] = result
        return result

    def fetch_deposit_status(self, base: str) -> list[dict]:
        """查询 Kraken 入金到账记录。"""
        return fetch_kraken_deposit_status(self._api_key, self._api_secret, base)


def from_env() -> KrakenWallet | None:
    """缺少 `KRAKEN_SPOT_API_KEY`/`KRAKEN_SPOT_API_SECRET` 时返回 None（由调用方决定是否报错）。"""
    api_key = os.environ.get("KRAKEN_SPOT_API_KEY")
    api_secret = os.environ.get("KRAKEN_SPOT_API_SECRET")
    if not api_key or not api_secret:
        return None
    return KrakenWallet(api_key, api_secret)
