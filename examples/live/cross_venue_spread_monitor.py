#!/usr/bin/env python3
"""
Cross-venue USDT spot spread monitor - main vs secondary venues.
监控主所（Binance/Bybit）与副所（Kraken 等）之间的 USDT 现货价差。

筛选规则:
  1. 币种在任意主所同时有 USDT 现货 + USDT 永续
  2. 副所也有该币的 USDT 现货
  3. 黑名单币种（BTC/ETH/SOL/XRP/BNB）直接排除

费用模型（单边）:
  买入成本 = ask × (1 + taker_fee + slippage)
  卖出收益 = bid × (1 - taker_fee - slippage)
  净价差   = 卖出收益 - 买入成本（仅计算主所→副所或副所→主所方向）

Usage:
    python examples/live/cross_venue_spread_monitor.py
    python examples/live/cross_venue_spread_monitor.py --alert-only
    python examples/live/cross_venue_spread_monitor.py --slippage 0.001 --fees KRAKEN=0.002
"""

import argparse
import json
import sys
import time
from collections import defaultdict
from collections import deque

from nautilus_trader.adapters.binance import BINANCE
from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceLiveDataClientFactory
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.adapters.bitfinex import BITFINEX
from nautilus_trader.adapters.bitfinex import BitfinexDataClientConfig
from nautilus_trader.adapters.bitfinex import BitfinexLiveDataClientFactory
from nautilus_trader.adapters.okx import OKX
from nautilus_trader.adapters.okx import OKXDataClientConfig
from nautilus_trader.adapters.okx import OKXLiveDataClientFactory
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
from nautilus_trader.core.nautilus_pyo3 import OKXEnvironment
from nautilus_trader.core.nautilus_pyo3 import OKXInstrumentType
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

# 主所（有永续 + 现货）
MAIN_SPOT_VENUES = {str(BINANCE), str(OKX)}
MAIN_PERP_VENUES = {BINANCE_FUT_KEY, str(OKX)}

# 副所（仅现货）
SECONDARY_VENUES = {str(KRAKEN), str(GATEIO), str(BITFINEX)}

# 黑名单：流动性过高，套利竞争激烈
BLACKLIST = {"BTC", "ETH", "SOL", "XRP", "BNB"}


# 各所折扣后 taker 费率默认值
DEFAULT_FEES: dict[str, float] = {
    str(BINANCE): 0.00075,   # BNB 折扣后
    str(OKX):     0.00080,   # Lv1 折扣后
    str(KRAKEN):   0.00050,   # 30天量 >$50k
    str(GATEIO):   0.00080,
    str(BITFINEX): 0.00200,   # 标准 taker 0.2%
}


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

    def on_start(self) -> None:
        instruments = self.cache.instruments()
        self.log.info(f"Cache contains {len(instruments)} instruments")

        # 分类所有 USDT 品种
        main_spot: dict[str, dict[str, object]] = defaultdict(dict)   # {base: {venue: inst}}
        main_perp: dict[str, set] = defaultdict(set)                   # {base: {venue}}
        secondary_spot: dict[str, dict[str, object]] = defaultdict(dict)

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

        # 筛选：主所同时有现货+永续，且副所有现货
        qualifying: dict[str, dict] = {}
        for base in set(main_spot) & set(main_perp):
            if base not in secondary_spot:
                continue
            # 至少有一个主所同时有现货和永续
            main_spot_venues = set(main_spot[base].keys())
            main_perp_venues = main_perp[base]
            if not (main_spot_venues & main_perp_venues) and \
               not (main_spot_venues and main_perp_venues):
                # 宽松判断：任意主所有现货 且 任意主所（可不同）有永续
                pass

            qualifying[base] = {
                "main_spot": main_spot[base],
                "secondary_spot": secondary_spot[base],
            }

        # 打印配对详情及各所合约规格
        print(f"\n{'='*88}")
        print(f"[SpreadMonitor] 配对完成，共 {len(qualifying)} 个 USDT 交易对")
        print(f"  主所: {MAIN_SPOT_VENUES}  副所: {SECONDARY_VENUES}")
        if not self._alert_only:
            print(f"  净价差阈值: {self._min_net_pct}%  滑点: {self._slippage*100:.4f}%")
        print()
        for base in sorted(qualifying):
            info = qualifying[base]
            all_insts: dict[str, object] = {**info["main_spot"], **info["secondary_spot"]}
            print(f"  {base}/USDT")
            for venue in sorted(all_insts):
                inst = all_insts[venue]
                role = "主" if venue in MAIN_SPOT_VENUES else "副"
                min_n = inst.min_notional
                max_q = inst.max_quantity
                min_q = inst.min_quantity
                print(
                    f"    [{role}] {venue:<12} "
                    f"价格步长={inst.price_increment}  "
                    f"数量步长={inst.size_increment}  "
                    f"最小名义={min_n if min_n is not None else 'N/A':>12}  "
                    f"最大单量={max_q if max_q is not None else 'N/A':>14}  "
                    f"最小单量={min_q if min_q is not None else 'N/A'}"
                )
        print(f"{'='*88}\n")

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
    args = parser.parse_args()

    venue_fees = parse_fees_arg(args.fees)
    print("[fees] 使用手续费率:")
    for v, f in venue_fees.items():
        print(f"  {v}: {f*100:.4f}%")
    print(f"[fees] 单边滑点: {args.slippage*100:.4f}%")

    config_node = TradingNodeConfig(
        trader_id="SPREAD-MONITOR-001",
        logging=LoggingConfig(log_level="INFO"),
        data_clients={
            # 主所现货
            BINANCE: BinanceDataClientConfig(
                environment=BinanceEnvironment.LIVE,
                account_type=BinanceAccountType.SPOT,
                instrument_provider=InstrumentProviderConfig(load_all=True),
            ),
            # 主所永续（Binance 独立 venue key）
            BINANCE_FUT_KEY: BinanceDataClientConfig(
                venue=Venue(BINANCE_FUT_KEY),
                environment=BinanceEnvironment.LIVE,
                account_type=BinanceAccountType.USDT_FUTURES,
                instrument_provider=InstrumentProviderConfig(load_all=True),
            ),
            # 主所现货 + 永续
            OKX: OKXDataClientConfig(
                environment=OKXEnvironment.LIVE,
                instrument_types=frozenset({OKXInstrumentType.SPOT, OKXInstrumentType.SWAP}),
                instrument_provider=InstrumentProviderConfig(load_all=True),
            ),
            # 副所现货
            KRAKEN: KrakenDataClientConfig(
                environment=KrakenEnvironment.LIVE,
                product_types=(KrakenProductType.SPOT,),
                instrument_provider=InstrumentProviderConfig(load_all=True),
            ),
            GATEIO: GateIoDataClientConfig(
                instrument_provider=InstrumentProviderConfig(load_all=True),
            ),
            BITFINEX: BitfinexDataClientConfig(
                instrument_provider=InstrumentProviderConfig(load_all=True),
            ),
        },
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
        )
    )

    node = TradingNode(config=config_node)
    node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)
    node.add_data_client_factory(BINANCE_FUT_KEY, BinanceLiveDataClientFactory)
    node.add_data_client_factory(OKX, OKXLiveDataClientFactory)
    node.add_data_client_factory(KRAKEN, KrakenLiveDataClientFactory)
    node.add_data_client_factory(GATEIO, GateIoLiveDataClientFactory)
    node.add_data_client_factory(BITFINEX, BitfinexLiveDataClientFactory)
    node.build()
    node.trader.add_strategy(monitor)
    node.run()


if __name__ == "__main__":
    main()
