import hashlib
import hmac
import re


def _parse_csv_set(csv_str: str) -> set[str]:
    return {part.strip().upper() for part in csv_str.split(",") if part.strip()}


# 部分交易所对单价过低的币种，会在永续合约命名里把"倍数"直接烧进 symbol/base（如 Binance
# 的 1000SHIBUSDT，baseAsset="1000SHIB"，1 张合约=1000 个真实 SHIB），而现货侧仍是纯币种名
# （SHIBUSDT，baseAsset="SHIB"）。不同所的前缀集合不通用（甚至有的所根本不这么做，如 OKX/
# Gate.io 把倍数放在独立的 multiplier 字段里），所以按 venue 定制一张前缀表。
_LEVERAGED_PREFIXES_BY_VENUE: dict[str, tuple[str, ...]] = {
    # 注意：Binance 现货/永续在本项目里是两个不同的 venue key（"BINANCE" / "BINANCE_FUT"，
    # 见 venue_config.py 的 BINANCE_FUT_KEY），放大面值命名只出现在永续侧，两个 key 都要配置。
    "BINANCE": ("10000000", "1000000", "100000", "10000", "1000"),
    "BINANCE_FUT": ("10000000", "1000000", "100000", "10000", "1000"),
    "BYBIT": ("1000000", "100000", "10000", "1000", "100", "10"),
    # OKX/GATEIO/其他所：不配置 = 不做任何拆分（这些所的 base_currency 已经是纯币种名）
}
_LEVERAGED_BASE_RE_BY_VENUE: dict[str, re.Pattern] = {
    venue: re.compile(rf"^({'|'.join(prefixes)})([A-Z][A-Z0-9]*)$")
    for venue, prefixes in _LEVERAGED_PREFIXES_BY_VENUE.items()
}


def split_leveraged_base(venue: str, symbol: str) -> tuple[str, int]:
    """按 venue 的命名规则，拆出"放大面值"合约里的真实币种和倍数。

    例：('BINANCE', '1000SHIB') -> ('SHIB', 1000)；('BINANCE', 'BTC') -> ('BTC', 1)；
    ('BINANCE', '1INCH') -> ('1INCH', 1)（前缀 "1" 不在标准倍数集合里，不误拆）；
    未在表中配置的 venue（如 OKX/GATEIO）原样返回 (symbol, 1)。
    """
    pattern = _LEVERAGED_BASE_RE_BY_VENUE.get(venue)
    if pattern is None:
        return symbol, 1
    m = pattern.match(symbol)
    if not m:
        return symbol, 1
    return m.group(2), int(m.group(1))


def _binance_sign_query(query: str, secret: str) -> str:
    """Binance 签名接口通用的 HMAC-SHA256 query 签名。"""
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()


# 各所提现/充值网络命名不统一，归一化到统一 key 后再比较是否有共同链
CHAIN_ALIASES: dict[str, str] = {
    "TRC20": "TRX", "TRON": "TRX", "TRX": "TRX",
    "ERC20": "ETH", "ETHEREUM": "ETH", "ETH": "ETH",
    "BEP20": "BSC", "BSC": "BSC",
    "BNB SMART CHAIN (BEP20)": "BSC", "BNB SMART CHAIN": "BSC",
    "BEP2": "BNB", "BNB BEACON CHAIN (BEP2)": "BNB",
    "MATIC": "MATIC", "POLYGON": "MATIC",
    "ARBITRUM": "ARBITRUM", "ARBITRUM ONE": "ARBITRUM",
    "OPTIMISM": "OPTIMISM",
    "SOL": "SOL", "SOLANA": "SOL",
    "AVAXC": "AVAXC", "AVALANCHE C-CHAIN": "AVAXC", "AVAX C-CHAIN": "AVAXC",
}


def _normalize_chain(raw: str) -> str:
    return CHAIN_ALIASES.get(raw.strip().upper(), raw.strip().upper())
