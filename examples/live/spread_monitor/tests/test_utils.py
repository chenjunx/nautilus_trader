from spread_monitor.utils import split_leveraged_base


def test_split_leveraged_base_binance_1000shib():
    assert split_leveraged_base("BINANCE", "1000SHIB") == ("SHIB", 1000)


def test_split_leveraged_base_binance_1000000mog():
    assert split_leveraged_base("BINANCE", "1000000MOG") == ("MOG", 1000000)


def test_split_leveraged_base_binance_plain_symbol_unaffected():
    assert split_leveraged_base("BINANCE", "BTC") == ("BTC", 1)


def test_split_leveraged_base_binance_does_not_misparse_real_digit_prefix():
    """1INCH 是真实币种名，前缀 "1" 不在倍数集合里，不能被误拆。"""
    assert split_leveraged_base("BINANCE", "1INCH") == ("1INCH", 1)


def test_split_leveraged_base_bybit_uses_its_own_table():
    assert split_leveraged_base("BYBIT", "100PEPE") == ("PEPE", 100)


def test_split_leveraged_base_unconfigured_venue_returns_as_is():
    """OKX/Gate.io 等未配置前缀表的所，base_currency 本来就是纯币种名，不做拆分。"""
    assert split_leveraged_base("OKX", "1000SHIB") == ("1000SHIB", 1)
