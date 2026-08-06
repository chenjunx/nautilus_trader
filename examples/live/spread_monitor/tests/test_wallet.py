"""`from_env()` 工厂函数用不到 nautilus_trader，测试也不引入这个依赖，方便单测（同
`guardrails.py` 的约定）。`WALLET_REGISTRY` 本身需要 nautilus_trader 的 venue 常量，
该测试用 `importorskip` 跳过，避免在未编译 nautilus_trader 的环境里拖垮整个测试文件的收集。
"""

import pytest
from spread_monitor.wallet import binance
from spread_monitor.wallet import kraken


def test_binance_wallet_from_env_missing_returns_none(monkeypatch):
    monkeypatch.delenv("BINANCE_TRADE_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_TRADE_API_SECRET", raising=False)
    monkeypatch.delenv("BINANCE_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_API_SECRET", raising=False)
    assert binance.from_env() is None


def test_binance_wallet_from_env_prefers_trade_key(monkeypatch):
    monkeypatch.setenv("BINANCE_TRADE_API_KEY", "trade-key")
    monkeypatch.setenv("BINANCE_TRADE_API_SECRET", "trade-secret")
    monkeypatch.setenv("BINANCE_API_KEY", "fallback-key")
    monkeypatch.setenv("BINANCE_API_SECRET", "fallback-secret")
    wallet = binance.from_env()
    assert isinstance(wallet, binance.BinanceWallet)
    assert wallet._api_key == "trade-key"
    assert wallet._api_secret == "trade-secret"


def test_binance_wallet_from_env_falls_back_to_plain_key(monkeypatch):
    monkeypatch.delenv("BINANCE_TRADE_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_TRADE_API_SECRET", raising=False)
    monkeypatch.setenv("BINANCE_API_KEY", "fallback-key")
    monkeypatch.setenv("BINANCE_API_SECRET", "fallback-secret")
    wallet = binance.from_env()
    assert isinstance(wallet, binance.BinanceWallet)
    assert wallet._api_key == "fallback-key"


def test_kraken_wallet_from_env_missing_returns_none(monkeypatch):
    monkeypatch.delenv("KRAKEN_SPOT_API_KEY", raising=False)
    monkeypatch.delenv("KRAKEN_SPOT_API_SECRET", raising=False)
    assert kraken.from_env() is None


def test_kraken_wallet_from_env_returns_wallet(monkeypatch):
    monkeypatch.setenv("KRAKEN_SPOT_API_KEY", "key")
    monkeypatch.setenv("KRAKEN_SPOT_API_SECRET", "secret")
    wallet = kraken.from_env()
    assert isinstance(wallet, kraken.KrakenWallet)


def test_wallet_registry_has_binance_and_kraken():
    pytest.importorskip("nautilus_trader", reason="nautilus_trader 未编译，跳过依赖它的 venue 常量的测试")
    from nautilus_trader.adapters.binance import BINANCE
    from nautilus_trader.adapters.kraken import KRAKEN
    from spread_monitor.wallet import WALLET_REGISTRY

    assert set(WALLET_REGISTRY) == {BINANCE, KRAKEN}
