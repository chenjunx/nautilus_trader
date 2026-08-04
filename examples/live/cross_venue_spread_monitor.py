#!/usr/bin/env python3
"""
Cross-venue USDT spot spread monitor - main vs secondary venues.
监控主所（Binance/Bybit）与副所（Kraken 等）之间的 USDT 现货价差。

筛选规则（--mode auto，默认）:
  1. 币种在任意主所同时有 USDT 现货 + USDT 永续
  2. 币种在同一副所同时有 USDT 现货 + USDT 永续（严格同所匹配）
  3. 黑名单币种（BTC/ETH/SOL/XRP/BNB）直接排除

筛选规则（--mode manual）:
  通过 --symbols/--main/--secondary 手动指定币种和主副所，跳过上述自动发现规则；
  只要求指定币种在指定的主所、副所上各至少能找到一个 USDT 现货（不校验永续，不受黑名单限制）。

筛选规则（提现链，--require-common-chain，默认开启）:
  主所与副所必须至少有一条共同支持的提现/充值链（如都支持 TRC20），否则资金无法跨所转移，
  该币种会被剔除。需要 Binance/Kraken 的私有 API Key（Gate.io 接口为公开接口，无需 Key）：
    BINANCE_API_KEY / BINANCE_API_SECRET
    KRAKEN_SPOT_API_KEY / KRAKEN_SPOT_API_SECRET
  凭据缺失时直接报错退出；可用 --no-require-common-chain 关闭该规则（无需任何 Key）。

费用模型（单边）:
  买入成本 = ask × (1 + taker_fee + slippage)
  卖出收益 = bid × (1 - taker_fee - slippage)
  净价差   = 卖出收益 - 买入成本（仅计算主所→副所或副所→主所方向）

Usage:
    python examples/live/cross_venue_spread_monitor.py
    python examples/live/cross_venue_spread_monitor.py --alert-only
    python examples/live/cross_venue_spread_monitor.py --slippage 0.001 --fees KRAKEN=0.002
    python examples/live/cross_venue_spread_monitor.py --mode manual \
        --symbols BTC,ETH --main BINANCE --secondary KRAKEN,GATEIO
    BINANCE_API_KEY=... BINANCE_API_SECRET=... \
    KRAKEN_SPOT_API_KEY=... KRAKEN_SPOT_API_SECRET=... \
        python examples/live/cross_venue_spread_monitor.py
    python examples/live/cross_venue_spread_monitor.py --no-require-common-chain
"""

import argparse
import base64
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.parse
from collections import defaultdict
from collections import deque

import httpx

from nautilus_trader.adapters.binance import BINANCE
from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceLiveDataClientFactory
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.adapters.gateio import GATEIO
from nautilus_trader.adapters.gateio import GateIoDataClientConfig
from nautilus_trader.adapters.gateio import GateIoLiveDataClientFactory
from nautilus_trader.adapters.kraken import KRAKEN
from nautilus_trader.adapters.kraken import KrakenDataClientConfig
from nautilus_trader.adapters.kraken import KrakenEnvironment
from nautilus_trader.adapters.kraken import KrakenLiveDataClientFactory
from nautilus_trader.adapters.kraken import KrakenProductType
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import StrategyConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.venues import Venue
from nautilus_trader.trading.strategy import Strategy

# Binance futures 使用独立 venue key，便于与现货区分
BINANCE_FUT_KEY = "BINANCE_FUT"

# 交易所配置总表：新增/删除一个所，只需在此增删一条记录。
# roles: "main_spot" | "main_perp" | "secondary"
VENUE_REGISTRY: list[dict] = [
    {
        "key": BINANCE,
        "roles": {"main_spot"},
        "config": lambda: BinanceDataClientConfig(
            environment=BinanceEnvironment.LIVE,
            account_type=BinanceAccountType.SPOT,
            instrument_provider=InstrumentProviderConfig(load_all=True),
        ),
        "factory": BinanceLiveDataClientFactory,
        "default_fee": 0.00075,   # BNB 折扣后
    },
    {
        "key": BINANCE_FUT_KEY,
        "roles": {"main_perp"},
        "config": lambda: BinanceDataClientConfig(
            venue=Venue(BINANCE_FUT_KEY),
            environment=BinanceEnvironment.LIVE,
            account_type=BinanceAccountType.USDT_FUTURES,
            instrument_provider=InstrumentProviderConfig(load_all=True),
        ),
        "factory": BinanceLiveDataClientFactory,
        "default_fee": None,
    },
    {
        "key": KRAKEN,
        "roles": {"secondary", "secondary_perp"},
        "config": lambda: KrakenDataClientConfig(
            environment=KrakenEnvironment.LIVE,
            product_types=(KrakenProductType.SPOT, KrakenProductType.FUTURES),
            instrument_provider=InstrumentProviderConfig(load_all=True),
        ),
        "factory": KrakenLiveDataClientFactory,
        "default_fee": 0.00050,   # 30天量 >$50k
    },
    {
        "key": GATEIO,
        "roles": {"secondary"},
        "config": lambda: GateIoDataClientConfig(
            instrument_provider=InstrumentProviderConfig(load_all=True),
        ),
        "factory": GateIoLiveDataClientFactory,
        "default_fee": 0.00080,
    },
]

# 主所（有永续 + 现货）
MAIN_SPOT_VENUES = {str(v["key"]) for v in VENUE_REGISTRY if "main_spot" in v["roles"]}
MAIN_PERP_VENUES = {str(v["key"]) for v in VENUE_REGISTRY if "main_perp" in v["roles"]}

# 副所（现货）
SECONDARY_VENUES = {str(v["key"]) for v in VENUE_REGISTRY if "secondary" in v["roles"]}
# 副所（永续，用于要求"副所同一所同时有现货+永续"）
SECONDARY_PERP_VENUES = {str(v["key"]) for v in VENUE_REGISTRY if "secondary_perp" in v["roles"]}

# 黑名单：流动性过高，套利竞争激烈
BLACKLIST = {"BTC", "ETH", "SOL", "XRP", "BNB"}

# 各所折扣后 taker 费率默认值
DEFAULT_FEES: dict[str, float] = {
    str(v["key"]): v["default_fee"] for v in VENUE_REGISTRY if v["default_fee"] is not None
}


def _parse_csv_set(csv_str: str) -> set[str]:
    return {part.strip().upper() for part in csv_str.split(",") if part.strip()}


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


def _fetch_binance_chains(api_key: str, api_secret: str) -> dict[str, set[str]]:
    """拉取 Binance 每个币种支持的提现网络。GET /sapi/v1/capital/config/getall（签名接口）。"""
    ts = int(time.time() * 1000)
    query = f"timestamp={ts}"
    sig = hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    url = f"https://api.binance.com/sapi/v1/capital/config/getall?{query}&signature={sig}"
    resp = httpx.get(url, headers={"X-MBX-APIKEY": api_key}, timeout=10.0)
    resp.raise_for_status()

    result: dict[str, set[str]] = {}
    for coin in resp.json():
        base = str(coin.get("coin", "")).upper()
        chains = {_normalize_chain(n.get("network", "")) for n in coin.get("networkList", [])}
        chains.discard("")
        if base and chains:
            result[base] = chains
    return result


def _fetch_kraken_chains(api_key: str, api_secret: str) -> dict[str, set[str]]:
    """拉取 Kraken 每个币种支持的提现网络。POST /0/private/WithdrawMethods（签名接口）。"""
    path = "/0/private/WithdrawMethods"
    nonce = str(int(time.time() * 1000))
    postdata = urllib.parse.urlencode({"nonce": nonce})
    sha = hashlib.sha256((nonce + postdata).encode()).digest()
    sig = base64.b64encode(
        hmac.new(base64.b64decode(api_secret), path.encode() + sha, hashlib.sha512).digest(),
    ).decode()
    headers = {"API-Key": api_key, "API-Sign": sig}
    resp = httpx.post(
        f"https://api.kraken.com{path}",
        data={"nonce": nonce},
        headers=headers,
        timeout=10.0,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("error"):
        raise RuntimeError(f"Kraken WithdrawMethods 返回错误: {body['error']}")

    result: dict[str, set[str]] = defaultdict(set)
    for row in body.get("result", []):
        base = str(row.get("asset", "")).upper()
        norm = _normalize_chain(str(row.get("network") or row.get("method", "")))
        if base and norm:
            result[base].add(norm)
    return dict(result)


def _fetch_gateio_chains(currency: str) -> set[str]:
    """拉取 Gate.io 单个币种支持的提现网络。GET /api/v4/wallet/currency_chains?currency=xxx（公开接口）。

    该接口不支持一次性拉取全部币种，必须按币种查询，因此调用方需自行传入候选币种列表。
    """
    resp = httpx.get(
        "https://api.gateio.ws/api/v4/wallet/currency_chains",
        params={"currency": currency},
        timeout=10.0,
    )
    resp.raise_for_status()

    result: set[str] = set()
    for row in resp.json():
        norm = _normalize_chain(str(row.get("chain", "")))
        if norm:
            result.add(norm)
    return result


def _dump_chains(symbols_csv: str) -> None:
    """调试用：按所拉取并打印提现链原始数据（归一化后），不启动实时监控。"""
    symbols = _parse_csv_set(symbols_csv) if symbols_csv else None

    binance_key = os.environ.get("BINANCE_API_KEY")
    binance_secret = os.environ.get("BINANCE_API_SECRET")
    kraken_key = os.environ.get("KRAKEN_SPOT_API_KEY")
    kraken_secret = os.environ.get("KRAKEN_SPOT_API_SECRET")

    fetchers: dict[str, object] = {}

    if binance_key and binance_secret:
        fetchers[BINANCE] = lambda: _fetch_binance_chains(binance_key, binance_secret)
    else:
        print(f"[dump] 跳过 {BINANCE}：缺少 BINANCE_API_KEY/BINANCE_API_SECRET")

    if kraken_key and kraken_secret:
        fetchers[KRAKEN] = lambda: _fetch_kraken_chains(kraken_key, kraken_secret)
    else:
        print(f"[dump] 跳过 {KRAKEN}：缺少 KRAKEN_SPOT_API_KEY/KRAKEN_SPOT_API_SECRET")

    # Gate.io 接口按币种查询，没有一次拉全部的接口，必须配合 --dump-symbols 指定币种
    if symbols:
        fetchers[GATEIO] = lambda: {sym: _fetch_gateio_chains(sym) for sym in sorted(symbols)}
    else:
        print(f"[dump] 跳过 {GATEIO}：该所接口按币种查询，需配合 --dump-symbols 指定币种")

    for venue, fetch in fetchers.items():
        print(f"\n{'='*60}\n[{venue}] 提现链数据\n{'='*60}")
        try:
            data = fetch()
        except Exception as exc:  # noqa: BLE001 - 调试工具，任何异常都要打印出来看
            print(f"  拉取失败: {exc!r}")
            continue

        shown = 0
        for base in sorted(data):
            if symbols and base not in symbols:
                continue
            print(f"  {base:<10} {sorted(data[base])}")
            shown += 1
        suffix = f"（已按 --dump-symbols 过滤，总共 {len(data)} 个币种）" if symbols else ""
        print(f"  共显示 {shown} 个币种{suffix}")


def _load_chain_support(relevant_venues: set[str]) -> dict[str, dict[str, set[str]]]:
    """按需拉取各所的提现链数据。缺少对应所的 API Key 时直接报错退出。

    Gate.io 接口按币种查询、没有一次拉全部的接口，因此不在此处预取，
    改为在 `_filter_by_common_chain` 中按实际候选币种懒加载。
    """
    chains: dict[str, dict[str, set[str]]] = {}

    if BINANCE in relevant_venues:
        key = os.environ.get("BINANCE_API_KEY")
        secret = os.environ.get("BINANCE_API_SECRET")
        if not key or not secret:
            sys.exit(
                "[chain] 缺少 BINANCE_API_KEY/BINANCE_API_SECRET，无法校验提现链"
                "（可用 --no-require-common-chain 关闭该规则）",
            )
        chains[BINANCE] = _fetch_binance_chains(key, secret)
        print(f"[chain] {BINANCE}: {len(chains[BINANCE])} 个币种的提现链数据")

    if KRAKEN in relevant_venues:
        key = os.environ.get("KRAKEN_SPOT_API_KEY")
        secret = os.environ.get("KRAKEN_SPOT_API_SECRET")
        if not key or not secret:
            sys.exit(
                "[chain] 缺少 KRAKEN_SPOT_API_KEY/KRAKEN_SPOT_API_SECRET，无法校验提现链"
                "（可用 --no-require-common-chain 关闭该规则）",
            )
        chains[KRAKEN] = _fetch_kraken_chains(key, secret)
        print(f"[chain] {KRAKEN}: {len(chains[KRAKEN])} 个币种的提现链数据")

    return chains


def parse_fees_arg(fees_str: str) -> dict[str, float]:
    """
    Parse --fees argument like "BINANCE=0.001,KRAKEN=0.002"
    into {venue_name: fee_rate}. 未指定的所使用 DEFAULT_FEES。
    """
    result = dict(DEFAULT_FEES)  # 以各所默认值为基础
    if not fees_str:
        return result
    for part in fees_str.split(","):
        part = part.strip()
        if "=" not in part:
            continue
        venue, rate = part.split("=", 1)
        venue = venue.strip().upper()
        if venue in result:
            result[venue] = float(rate.strip())
    return result


class SpreadMonitorConfig(StrategyConfig, frozen=True):
    min_net_spread_pct: float = 0.0
    throttle_secs: float = 2.0
    summary_interval: int = 30
    slippage: float = 0.0002
    alert_only: bool = False
    venue_fees_json: str = "{}"
    # 匹配模式："auto"（自动发现，默认）或 "manual"（手动指定币种+主副所）
    mode: str = "auto"
    manual_symbols_csv: str = ""
    manual_main_csv: str = ""
    manual_secondary_csv: str = ""
    # 提现链匹配：主所与副所至少要有一条共同支持的提现/充值链
    require_common_chain: bool = True
    chain_support_json: str = "{}"
    # 链路健康度检测
    health_window_secs: float = 30.0
    health_warmup_secs: float = 60.0
    health_degrade_ratio: float = 0.2
    health_recover_ratio: float = 0.5
    health_baseline_ewma_secs: float = 300.0
    health_check_interval: float = 5.0


class SpreadMonitor(Strategy):
    def __init__(self, config: SpreadMonitorConfig) -> None:
        super().__init__(config)
        # {base: {venue: (bid, ask)}}  — 仅现货
        self._prices: dict[str, dict[str, tuple[float, float]]] = defaultdict(dict)
        # {instrument_id_str: base_currency}
        self._inst_to_base: dict[str, str] = {}
        # {instrument_id_str: "main" | "secondary"}
        self._inst_venue_type: dict[str, str] = {}
        # {base: {"main": set(venues), "secondary": set(venues)}}
        self._base_venues: dict[str, dict[str, set]] = {}
        # {instrument_id_str: taker_fee}
        self._inst_fee: dict[str, float] = {}
        self._last_print: dict[str, float] = {}
        self._last_summary: float = 0.0
        self._min_net_pct = config.min_net_spread_pct
        self._throttle = config.throttle_secs
        self._summary_interval = config.summary_interval
        self._slippage = config.slippage
        self._alert_only = config.alert_only
        self._venue_defaults: dict[str, float] = json.loads(config.venue_fees_json)

        self._mode = config.mode
        self._manual_symbols = _parse_csv_set(config.manual_symbols_csv)
        self._manual_main_venues = _parse_csv_set(config.manual_main_csv)
        self._manual_secondary_venues = _parse_csv_set(config.manual_secondary_csv)

        self._require_common_chain = config.require_common_chain
        raw_chain_support = json.loads(config.chain_support_json)
        self._chain_support: dict[str, dict[str, set[str]]] = {
            venue: {base: set(chains) for base, chains in per_base.items()}
            for venue, per_base in raw_chain_support.items()
        }
        self._gateio_chain_cache: dict[str, set[str]] = {}

        # 链路健康度检测（venue 级别，基于消息速率）
        self._health_window_secs = config.health_window_secs
        self._health_warmup_secs = config.health_warmup_secs
        self._health_degrade_ratio = config.health_degrade_ratio
        self._health_recover_ratio = config.health_recover_ratio
        self._health_baseline_ewma_secs = config.health_baseline_ewma_secs
        self._health_check_interval = config.health_check_interval
        self._venue_tick_times: dict[str, deque] = defaultdict(deque)
        self._venue_baseline_rate: dict[str, float] = {}
        self._unhealthy_venues: set[str] = set()
        self._unhealthy_since: dict[str, float] = {}
        self._all_venues: set[str] = set()
        self._start_time: float = 0.0
        self._last_health_check: float = 0.0

    def _build_auto_qualifying(self, instruments: list) -> tuple[dict, set, set]:
        """自动发现模式：主所现货+永续、同一副所现货+永续 都齐全的币种才入选。"""
        main_spot: dict[str, dict[str, object]] = defaultdict(dict)   # {base: {venue: inst}}
        main_perp: dict[str, set] = defaultdict(set)                   # {base: {venue}}
        secondary_spot: dict[str, dict[str, object]] = defaultdict(dict)
        secondary_perp: dict[str, set] = defaultdict(set)               # {base: {venue}}

        for inst in instruments:
            try:
                base = str(inst.base_currency)
                quote = str(inst.quote_currency)
            except AttributeError:
                continue

            if quote != "USDT" or base in BLACKLIST:
                continue

            venue = str(inst.id.venue)

            if isinstance(inst, CurrencyPair):
                if venue in MAIN_SPOT_VENUES:
                    main_spot[base][venue] = inst
                elif venue in SECONDARY_VENUES:
                    secondary_spot[base][venue] = inst

            elif isinstance(inst, CryptoPerpetual):
                if venue in MAIN_PERP_VENUES:
                    main_perp[base].add(venue)
                elif venue in SECONDARY_PERP_VENUES:
                    secondary_perp[base].add(venue)

        # 筛选：任意主所同时有现货+永续，且同一副所也同时有现货+永续
        qualifying: dict[str, dict] = {}
        for base in set(main_spot) & set(main_perp):
            if base not in secondary_spot:
                continue

            # 同一副所需同时有现货和永续，只保留满足条件的副所
            matched_secondary_venues = set(secondary_spot[base].keys()) & secondary_perp[base]
            if not matched_secondary_venues:
                continue
            matched_secondary_spot = {
                v: inst for v, inst in secondary_spot[base].items() if v in matched_secondary_venues
            }

            qualifying[base] = {
                "main_spot": main_spot[base],
                "secondary_spot": matched_secondary_spot,
            }

        return qualifying, MAIN_SPOT_VENUES, SECONDARY_VENUES

    def _build_manual_qualifying(self, instruments: list) -> tuple[dict, set, set]:
        """手动模式：只看 --symbols/--main/--secondary 指定的币种和所，不校验永续、不受黑名单限制。"""
        main_venues = self._manual_main_venues
        secondary_venues = self._manual_secondary_venues
        symbols = self._manual_symbols

        main_spot: dict[str, dict[str, object]] = defaultdict(dict)
        secondary_spot: dict[str, dict[str, object]] = defaultdict(dict)

        for inst in instruments:
            if not isinstance(inst, CurrencyPair):
                continue
            try:
                base = str(inst.base_currency)
                quote = str(inst.quote_currency)
            except AttributeError:
                continue

            if quote != "USDT" or base not in symbols:
                continue

            venue = str(inst.id.venue)
            if venue in main_venues:
                main_spot[base][venue] = inst
            elif venue in secondary_venues:
                secondary_spot[base][venue] = inst

        qualifying: dict[str, dict] = {}
        for base in sorted(symbols):
            found_main = main_spot.get(base, {})
            found_secondary = secondary_spot.get(base, {})
            if not found_main or not found_secondary:
                self.log.warning(
                    f"[manual] 跳过 {base}：主所现货={sorted(found_main) or '无'}  "
                    f"副所现货={sorted(found_secondary) or '无'}",
                )
                continue
            qualifying[base] = {
                "main_spot": found_main,
                "secondary_spot": found_secondary,
            }

        return qualifying, main_venues, secondary_venues

    def _get_chain_support(self, venue: str, base: str) -> set[str]:
        """返回某所某币种的提现链集合。Gate.io 接口按币种查询，此处懒加载并缓存结果。"""
        if venue == GATEIO:
            if base not in self._gateio_chain_cache:
                try:
                    self._gateio_chain_cache[base] = _fetch_gateio_chains(base)
                except Exception as exc:  # noqa: BLE001 - 单个币种拉取失败不应中断整体流程
                    self.log.warning(f"[chain] 拉取 {GATEIO} {base} 提现链失败: {exc!r}")
                    self._gateio_chain_cache[base] = set()
            return self._gateio_chain_cache[base]
        return self._chain_support.get(venue, {}).get(base, set())

    def _filter_by_common_chain(self, qualifying: dict[str, dict]) -> dict[str, dict]:
        """剔除主所与副所没有共同提现链的币种（主所整体 ∩ 副所整体，链集合取并集后比较）。"""
        if not self._require_common_chain:
            return qualifying

        result: dict[str, dict] = {}
        for base, info in qualifying.items():
            main_chains: set[str] = set()
            for venue in info["main_spot"]:
                main_chains |= self._get_chain_support(venue, base)

            sec_chains: set[str] = set()
            for venue in info["secondary_spot"]:
                sec_chains |= self._get_chain_support(venue, base)

            common = main_chains & sec_chains
            if not common:
                self.log.warning(
                    f"[chain] 跳过 {base}：主所链={sorted(main_chains) or '无'}  "
                    f"副所链={sorted(sec_chains) or '无'}，无共同提现链",
                )
                continue

            result[base] = info

        return result

    def on_start(self) -> None:
        instruments = self.cache.instruments()
        self.log.info(f"Cache contains {len(instruments)} instruments")

        if self._mode == "manual":
            qualifying, main_venues, secondary_venues = self._build_manual_qualifying(instruments)
        else:
            qualifying, main_venues, secondary_venues = self._build_auto_qualifying(instruments)

        qualifying = self._filter_by_common_chain(qualifying)

        # 配对详情及各所合约规格（较为冗长，降为 debug 级别）
        self.log.info(f"[SpreadMonitor] 配对完成（模式={self._mode}），共 {len(qualifying)} 个 USDT 交易对")
        self.log.info(f"主所: {main_venues}  副所: {secondary_venues}")
        if not self._alert_only:
            self.log.info(f"净价差阈值: {self._min_net_pct}%  滑点: {self._slippage*100:.4f}%")
        for base in sorted(qualifying):
            info = qualifying[base]
            all_insts: dict[str, object] = {**info["main_spot"], **info["secondary_spot"]}
            self.log.debug(f"{base}/USDT")
            for venue in sorted(all_insts):
                inst = all_insts[venue]
                role = "主" if venue in main_venues else "副"
                min_n = inst.min_notional
                max_q = inst.max_quantity
                min_q = inst.min_quantity
                self.log.debug(
                    f"  [{role}] {venue:<12} "
                    f"价格步长={inst.price_increment}  "
                    f"数量步长={inst.size_increment}  "
                    f"最小名义={str(min_n) if min_n is not None else 'N/A':>12}  "
                    f"最大单量={str(max_q) if max_q is not None else 'N/A':>14}  "
                    f"最小单量={str(min_q) if min_q is not None else 'N/A'}"
                )

        # 订阅现货行情
        for base, info in sorted(qualifying.items()):
            venues_for_base: dict[str, set] = {"main": set(), "secondary": set()}

            for venue, inst in info["main_spot"].items():
                inst_id_str = str(inst.id)
                self._inst_to_base[inst_id_str] = base
                self._inst_venue_type[inst_id_str] = "main"
                fee = float(inst.taker_fee) if float(inst.taker_fee) > 0 else \
                      self._venue_defaults.get(venue, 0.001)
                self._inst_fee[inst_id_str] = fee
                venues_for_base["main"].add(venue)
                self.subscribe_quote_ticks(inst.id)

            for venue, inst in info["secondary_spot"].items():
                inst_id_str = str(inst.id)
                self._inst_to_base[inst_id_str] = base
                self._inst_venue_type[inst_id_str] = "secondary"
                fee = self._venue_defaults.get(venue, 0.001)
                self._inst_fee[inst_id_str] = fee
                venues_for_base["secondary"].add(venue)
                self.subscribe_quote_ticks(inst.id)

            self._base_venues[base] = venues_for_base

        self._all_venues = {
            v for info in self._base_venues.values() for v in info["main"] | info["secondary"]
        }
        self._start_time = time.monotonic()

        self.log.info(f"已订阅 {len(self._inst_to_base)} 个行情流")

    def on_quote_tick(self, tick: QuoteTick) -> None:
        inst_id_str = str(tick.instrument_id)
        base = self._inst_to_base.get(inst_id_str)
        if base is None:
            return

        venue = str(tick.instrument_id.venue)
        now = time.monotonic()
        self._prices[base][venue] = (float(tick.bid_price), float(tick.ask_price))

        dq = self._venue_tick_times[venue]
        dq.append(now)
        cutoff = now - self._health_window_secs
        while dq and dq[0] < cutoff:
            dq.popleft()

        if now - self._last_health_check >= self._health_check_interval:
            self._last_health_check = now
            self._check_health(now)

        # 需要至少一个主所和一个副所都有健康数据
        venue_data = {
            v: p for v, p in self._prices[base].items() if v not in self._unhealthy_venues
        }
        base_info = self._base_venues.get(base, {})
        has_main = any(v in venue_data for v in base_info.get("main", set()))
        has_secondary = any(v in venue_data for v in base_info.get("secondary", set()))
        if not (has_main and has_secondary):
            return

        if not self._alert_only and now - self._last_summary >= self._summary_interval:
            self._last_summary = now
            self._print_summary()

        if now - self._last_print.get(base, 0) < self._throttle:
            return

        result = self._best_arb(base, venue_data, base_info)
        if result is None:
            return

        gross_pct, net_pct, buy_v, buy_ask, fee_b, sell_v, sell_bid, fee_s = result
        min_pct = 0.0 if self._alert_only else self._min_net_pct
        if net_pct < min_pct:
            return

        self._last_print[base] = now
        self._print_opportunity(base, venue_data, gross_pct, net_pct,
                                buy_v, buy_ask, fee_b, sell_v, sell_bid, fee_s)

    def _check_health(self, now: float) -> None:
        """
        按 venue 聚合的消息速率检测链路健康度。速率显著低于基线（含完全断流）判不健康，
        期间自动从价差计算中剔除该 venue 的数据；速率恢复后自动重新纳入。
        """
        if now - self._start_time < self._health_warmup_secs:
            return

        eps = 1e-9
        alpha = min(1.0, self._health_check_interval / self._health_baseline_ewma_secs)

        for venue in self._all_venues:
            recent_rate = len(self._venue_tick_times.get(venue, ())) / self._health_window_secs
            baseline = self._venue_baseline_rate.get(venue)
            is_unhealthy = venue in self._unhealthy_venues

            if baseline is None:
                if recent_rate <= eps:
                    # 预热期结束仍从未收到过任何 tick，视为不健康（无法确立正常基线）
                    self._unhealthy_venues.add(venue)
                    self._unhealthy_since[venue] = now
                    self.log.warning(f"[health] {venue} 预热期内未收到任何行情，判定不健康")
                else:
                    self._venue_baseline_rate[venue] = recent_rate
                continue

            if not is_unhealthy:
                if recent_rate < baseline * self._health_degrade_ratio:
                    self._unhealthy_venues.add(venue)
                    self._unhealthy_since[venue] = now
                    self.log.warning(
                        f"[health] {venue} 速率异常: 近期 {recent_rate:.2f}/s "
                        f"<< 基线 {baseline:.2f}/s，已剔除该所数据",
                    )
                else:
                    self._venue_baseline_rate[venue] = (
                        (1 - alpha) * baseline + alpha * recent_rate
                    )
            else:
                if recent_rate > baseline * self._health_recover_ratio:
                    self._unhealthy_venues.discard(venue)
                    since = self._unhealthy_since.pop(venue, now)
                    self.log.warning(
                        f"[health] {venue} 恢复正常: 近期 {recent_rate:.2f}/s "
                        f"(基线 {baseline:.2f}/s)，持续不健康 {now - since:.0f}s 后重新纳入计算",
                    )
                # 不健康期间基线冻结，避免被低速率污染

    def _fee_for_inst(self, inst_id_str: str, venue: str) -> float:
        return self._inst_fee.get(inst_id_str, self._venue_defaults.get(venue, 0.001))

    def _venue_type(self, venue: str) -> str:
        """返回 'main' 或 'secondary'，根据已订阅的 instrument 类型判断。"""
        for inst_id_str, vtype in self._inst_venue_type.items():
            if venue in inst_id_str:
                return vtype
        return "unknown"

    def _best_arb(
        self,
        base: str,
        venue_data: dict[str, tuple[float, float]],
        base_info: dict[str, set],
    ) -> tuple | None:
        """
        只比较主所→副所和副所→主所方向（不比较主所之间）。
        净价差 = 卖出收益 - 买入成本
               = bid_sell × (1 - fee_s - slip) - ask_buy × (1 + fee_b + slip)
        """
        mid = sum((b + a) / 2 for b, a in venue_data.values()) / len(venue_data)
        if mid == 0:
            return None

        slip = self._slippage
        main_venues = base_info.get("main", set()) & venue_data.keys()
        sec_venues = base_info.get("secondary", set()) & venue_data.keys()

        best_net = float("-inf")
        best: tuple | None = None

        # 遍历所有主所↔副所组合（双向）
        for buy_v, sell_v in (
            [(m, s) for m in main_venues for s in sec_venues] +
            [(s, m) for s in sec_venues for m in main_venues]
        ):
            ask = venue_data[buy_v][1]
            bid = venue_data[sell_v][0]
            # 找对应的 inst_id_str 来获取费率
            fee_b = next((self._inst_fee[k] for k in self._inst_fee
                          if self._inst_to_base.get(k) == base and buy_v in k),
                         self._venue_defaults.get(buy_v, 0.001))
            fee_s = next((self._inst_fee[k] for k in self._inst_fee
                          if self._inst_to_base.get(k) == base and sell_v in k),
                         self._venue_defaults.get(sell_v, 0.001))

            net = bid * (1 - fee_s - slip) - ask * (1 + fee_b + slip)
            if net > best_net:
                best_net = net
                gross_pct = (bid - ask) / mid * 100
                net_pct = net / mid * 100
                best = (gross_pct, net_pct, buy_v, ask, fee_b, sell_v, bid, fee_s)

        return best

    def _print_opportunity(
        self,
        base: str,
        venue_data: dict,
        gross_pct: float,
        net_pct: float,
        buy_v: str,
        buy_ask: float,
        fee_b: float,
        sell_v: str,
        sell_bid: float,
        fee_s: float,
    ) -> None:
        tag = ">> ARBI" if net_pct > 0 else "   norm"
        prices = "  ".join(
            f"{v}:{venue_data[v][0]:.6g}/{venue_data[v][1]:.6g}"
            for v in sorted(venue_data)
        )
        ts = time.strftime("%H:%M:%S")
        fee_pct = (fee_b + fee_s) * 100
        slip_pct = self._slippage * 2 * 100
        print(
            f"{ts} {tag} | {base+'/USDT':<14} | {prices}\n"
            f"         在 {buy_v} 买(ask={buy_ask:.6g}, 费={fee_b*100:.3f}%)  "
            f"在 {sell_v} 卖(bid={sell_bid:.6g}, 费={fee_s*100:.3f}%)\n"
            f"         毛价差={gross_pct:+.4f}%  手续费={fee_pct:.3f}%  "
            f"滑点={slip_pct:.3f}%  净价差={net_pct:+.4f}%"
        )
        sys.stdout.flush()

    def _print_summary(self) -> None:
        rows = []
        for base, raw_venue_data in self._prices.items():
            venue_data = {
                v: p for v, p in raw_venue_data.items() if v not in self._unhealthy_venues
            }
            base_info = self._base_venues.get(base, {})
            result = self._best_arb(base, venue_data, base_info)
            if result is None:
                continue
            gross_pct, net_pct, buy_v, _, fee_b, sell_v, _, fee_s = result
            rows.append((net_pct, gross_pct, base, venue_data, buy_v, sell_v, fee_b, fee_s))

        if rows:
            rows.sort(reverse=True)
            ts = time.strftime("%H:%M:%S")
            slip_pct = self._slippage * 2 * 100
            print(f"\n{ts} ══ TOP 20 净价差排名（主所↔副所，手续费+滑点{slip_pct:.3f}%后）══")
            fmt = "  {:<6}  {:<14}  gross={:+.5f}%  fees={:.3f}%  slip={:.3f}%  net={:+.5f}%  ({} → {})"
            for net_pct, gross_pct, base, venue_data, buy_v, sell_v, fee_b, fee_s in rows[:20]:
                tag = "ARBI" if net_pct > 0 else "norm"
                fee_pct = (fee_b + fee_s) * 100
                print(fmt.format(tag, base + "/USDT", gross_pct, fee_pct, slip_pct, net_pct, buy_v, sell_v))
            print()

        self._print_health()
        sys.stdout.flush()

    def _print_health(self) -> None:
        if not self._all_venues:
            return
        now = time.monotonic()
        print("── 链路健康 ──")
        for venue in sorted(self._all_venues):
            recent_rate = len(self._venue_tick_times.get(venue, ())) / self._health_window_secs
            baseline = self._venue_baseline_rate.get(venue)
            baseline_str = f"{baseline:.2f}/s" if baseline is not None else "建立中"
            if venue in self._unhealthy_venues:
                since = self._unhealthy_since.get(venue, now)
                print(
                    f"  {venue:<10} ✗ {recent_rate:.2f}/s  (基线 {baseline_str}, "
                    f"已持续 {now - since:.0f}s) — 已剔除",
                )
            else:
                print(f"  {venue:<10} ✓ {recent_rate:.2f}/s  (基线 {baseline_str})")
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description="跨所 USDT 现货净价差监控（主所↔副所）")
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="日志级别（默认 INFO；设为 DEBUG 可看到每个币种的合约规格明细）")
    parser.add_argument("--min-net", type=float, default=0.0,
                        help="最小净价差打印阈值（百分比，默认 0.0）")
    parser.add_argument("--throttle", type=float, default=2.0,
                        help="每对最小打印间隔秒数（默认 2）")
    parser.add_argument("--summary", type=int, default=30,
                        help="汇总排名打印间隔秒数（默认 30）")
    parser.add_argument("--fees", type=str, default="",
                        help="覆盖手续费，格式: BINANCE=0.00075,KRAKEN=0.0005（默认各所折扣后费率）")
    parser.add_argument("--slippage", type=float, default=0.0002,
                        help="单边滑点估算（默认 0.0002 = 0.02%%）")
    parser.add_argument("--alert-only", action="store_true",
                        help="只在 net>0 时输出，适合后台运行")
    parser.add_argument("--mode", choices=["auto", "manual"], default="auto",
                        help="匹配模式：auto=自动发现（默认），manual=手动指定币种和主副所")
    parser.add_argument("--symbols", type=str, default="",
                        help="manual 模式：逗号分隔的币种列表，如 BTC,ETH,DOGE")
    parser.add_argument("--main", type=str, default="",
                        help="manual 模式：逗号分隔的主所列表，如 BINANCE")
    parser.add_argument("--secondary", type=str, default="",
                        help="manual 模式：逗号分隔的副所列表，如 KRAKEN,GATEIO")
    parser.add_argument("--require-common-chain", action=argparse.BooleanOptionalAction, default=True,
                        help="主所与副所需至少有一条共同提现链才保留该币种（默认开启，需要 "
                             "BINANCE/KRAKEN 私有 API Key；用 --no-require-common-chain 关闭）")
    parser.add_argument("--dump-chains", action="store_true",
                        help="仅按已配置的 Key 拉取并打印各所提现链数据，不启动实时监控（调试用）")
    parser.add_argument("--dump-symbols", type=str, default="",
                        help="--dump-chains 时可选，逗号分隔币种列表，只打印这些币种（默认打印全部）")
    args = parser.parse_args()

    if args.dump_chains:
        _dump_chains(args.dump_symbols)
        return

    known_venues = {str(v["key"]) for v in VENUE_REGISTRY}
    if args.mode == "manual":
        if not (args.symbols and args.main and args.secondary):
            parser.error("--mode manual 需要同时指定 --symbols、--main、--secondary")
        unknown = (_parse_csv_set(args.main) | _parse_csv_set(args.secondary)) - known_venues
        if unknown:
            parser.error(f"未知交易所: {sorted(unknown)}，可选: {sorted(known_venues)}")

    venue_fees = parse_fees_arg(args.fees)
    print("[fees] 使用手续费率:")
    for v, f in venue_fees.items():
        print(f"  {v}: {f*100:.4f}%")
    print(f"[fees] 单边滑点: {args.slippage*100:.4f}%")

    chain_support_json = "{}"
    if args.require_common_chain:
        if args.mode == "manual":
            relevant_venues = _parse_csv_set(args.main) | _parse_csv_set(args.secondary)
        else:
            relevant_venues = MAIN_SPOT_VENUES | SECONDARY_VENUES
        chain_support = _load_chain_support(relevant_venues)
        chain_support_json = json.dumps(
            {v: {b: sorted(cs) for b, cs in m.items()} for v, m in chain_support.items()},
        )

    config_node = TradingNodeConfig(
        trader_id="SPREAD-MONITOR-001",
        logging=LoggingConfig(log_level=args.log_level),
        data_clients={v["key"]: v["config"]() for v in VENUE_REGISTRY},
        strategies=[],
    )

    monitor = SpreadMonitor(
        SpreadMonitorConfig(
            strategy_id="SPREAD-MONITOR-001",
            min_net_spread_pct=args.min_net,
            throttle_secs=args.throttle,
            summary_interval=args.summary,
            slippage=args.slippage,
            alert_only=args.alert_only,
            venue_fees_json=json.dumps(venue_fees),
            mode=args.mode,
            manual_symbols_csv=args.symbols,
            manual_main_csv=args.main,
            manual_secondary_csv=args.secondary,
            require_common_chain=args.require_common_chain,
            chain_support_json=chain_support_json,
        )
    )

    node = TradingNode(config=config_node)
    for v in VENUE_REGISTRY:
        node.add_data_client_factory(v["key"], v["factory"])
    node.build()
    node.trader.add_strategy(monitor)
    node.run()


if __name__ == "__main__":
    main()
