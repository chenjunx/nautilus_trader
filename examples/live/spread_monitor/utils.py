import hashlib
import hmac


def _parse_csv_set(csv_str: str) -> set[str]:
    return {part.strip().upper() for part in csv_str.split(",") if part.strip()}


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
