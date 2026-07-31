#!/usr/bin/env python3
"""
Cross-venue USDC spot spread monitor - with taker fee and slippage deduction.
监控 Binance、Bybit、Kraken 三所共有 USDC 现货对的实时净价差（扣手续费 + 滑点）。

费用模型（单边）:
  买入成本 = ask × (1 + taker_fee + slippage)
  卖出收益 = bid × (1 - taker_fee - slippage)
  净价差   = 卖出收益 - 买入成本

Fee sources (三所费率接口均需 API Key):
  - Binance: 若提供 API Key，框架自动从 /sapi/v1/asset/tradeFee 写入 instrument.taker_fee
  - Bybit / OKX: 使用 --fees 传入，或默认 0.1%

Usage:
    python examples/live/cross_venue_spread_monitor.py
    python examples/live/cross_venue_spread_monitor.py --min-net 0.01 --fees BYBIT=0.001,OKX=0.001
    python examples/live/cross_venue_spread_monitor.py --slippage 0.0005 --throttle 5
"""

import argparse
import json
import sys
import time
from collections import defaultdict
from decimal import Decimal

from nautilus_trader.adapters.binance import BINANCE
from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceLiveDataClientFactory
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.adapters.bybit import BYBIT
from nautilus_trader.adapters.bybit import BybitDataClientConfig
from nautilus_trader.adapters.bybit import BybitEnvironment
from nautilus_trader.adapters.bybit import BybitLiveDataClientFactory
from nautilus_trader.adapters.bybit import BybitProductType
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
from nautilus_trader.trading.strategy import Strategy

VENUE_NAMES = [str(BINANCE), str(BYBIT), str(KRAKEN)]


def parse_fees_arg(fees_str: str, default: float) -> dict[str, float]:
    """
    Parse --fees argument like "BINANCE=0.001,BYBIT=0.0008,KRAKEN=0.001"
    into {venue_name: fee_rate}.
    """
    result = {v: default for v in VENUE_NAMES}
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
    slippage: float = 0.0005          # 单边滑点，默认 0.05%
    alert_only: bool = False          # 只在 net > 0 时输出，关闭定时汇总
    # JSON: {venue_name: default_taker_fee} — per-instrument fee from cache takes priority
    venue_fees_json: str = "{}"


class SpreadMonitor(Strategy):
    def __init__(self, config: SpreadMonitorConfig) -> None:
        super().__init__(config)
        # {base: {venue: (bid, ask)}}
        self._prices: dict[str, dict[str, tuple[float, float]]] = defaultdict(dict)
        # {instrument_id_str: base_currency}
        self._inst_to_base: dict[str, str] = {}
        # {instrument_id_str: taker_fee}  — populated from instrument object or venue default
        self._inst_fee: dict[str, float] = {}
        self._last_print: dict[str, float] = {}
        self._last_summary: float = 0.0
        self._min_net_pct = config.min_net_spread_pct
        self._throttle = config.throttle_secs
        self._summary_interval = config.summary_interval
        self._slippage = config.slippage
        self._alert_only = config.alert_only
        # {venue_name: default_fee}
        self._venue_defaults: dict[str, float] = json.loads(config.venue_fees_json)

    def on_start(self) -> None:
        instruments = self.cache.instruments()
        self.log.info(f"Cache contains {len(instruments)} instruments")

        # Group by base currency, keep only USDT spot
        by_base: dict[str, dict[str, object]] = defaultdict(dict)
        inst_map: dict[str, object] = {}
        for inst in instruments:
            try:
                base = str(inst.base_currency)
                quote = str(inst.quote_currency)
            except AttributeError:
                continue
            if quote != "USDC":
                continue
            venue = str(inst.id.venue)
            if venue in VENUE_NAMES:
                by_base[base][venue] = inst.id
                inst_map[str(inst.id)] = inst

        # Keep only pairs common to all three venues
        common = {
            base: vm
            for base, vm in by_base.items()
            if set(VENUE_NAMES).issubset(vm.keys())
        }

        if self._alert_only:
            print(f"[SpreadMonitor] alert-only 模式，仅在 net>0 时输出 | "
                  f"品种数={len(common)} | 滑点={self._slippage*100:.4f}%")
        else:
            print(f"\n{'='*72}")
            print(f"[SpreadMonitor] 三所共有 USDC 现货对: {len(common)} 个")
            print(f"[SpreadMonitor] 净价差阈值: {self._min_net_pct}%  | 刷新间隔: {self._throttle}s  | 单边滑点: {self._slippage*100:.4f}%")
            for venue in VENUE_NAMES:
                default = self._venue_defaults.get(venue, 0.001)
                print(f"[SpreadMonitor] {venue} 手续费默认: {default*100:.4f}%"
                      f"{'（instrument 对象中有实际费率的品种以实际为准）' if venue == str(BINANCE) else ''}")
            print(f"{'='*72}\n")

        for base, venue_map in sorted(common.items()):
            for venue, inst_id in venue_map.items():
                inst_id_str = str(inst_id)
                self._inst_to_base[inst_id_str] = base

                # Prefer fee from instrument object (set by Binance adapter when API key provided)
                inst = inst_map.get(inst_id_str)
                fee = float(inst.taker_fee) if inst and float(inst.taker_fee) > 0 else None
                if fee is None:
                    fee = self._venue_defaults.get(venue, 0.001)
                self._inst_fee[inst_id_str] = fee

                self.subscribe_quote_ticks(inst_id)

        self.log.info(f"已订阅 {len(self._inst_to_base)} 个行情流")

    def on_quote_tick(self, tick: QuoteTick) -> None:
        inst_id_str = str(tick.instrument_id)
        base = self._inst_to_base.get(inst_id_str)
        if base is None:
            return

        venue = str(tick.instrument_id.venue)
        self._prices[base][venue] = (float(tick.bid_price), float(tick.ask_price))

        venue_data = self._prices[base]
        if len(venue_data) < len(VENUE_NAMES):
            return  # wait until all venues have data

        now = time.monotonic()
        if not self._alert_only and now - self._last_summary >= self._summary_interval:
            self._last_summary = now
            self._print_summary()

        if now - self._last_print.get(base, 0) < self._throttle:
            return

        result = self._best_arb(base, venue_data)
        if result is None:
            return

        gross_pct, net_pct, buy_v, buy_ask, fee_b, sell_v, sell_bid, fee_s = result
        min_pct = 0.0 if self._alert_only else self._min_net_pct
        if net_pct < min_pct:
            return

        self._last_print[base] = now
        self._print_opportunity(base, venue_data, gross_pct, net_pct,
                                buy_v, buy_ask, fee_b, sell_v, sell_bid, fee_s,
                                self._slippage)

    def _fee_for(self, venue: str, base: str) -> float:
        """
        Look up taker fee for a venue+base pair.
        Uses instrument-level fee when available, else venue default.
        """
        # instrument_id format varies per venue; reconstruct and look up
        # We store by inst_id_str so we need to find the right key
        # Simpler: just use venue default (instrument-level was already merged at subscribe time)
        return self._venue_defaults.get(venue, 0.001)

    def _fee_for_inst(self, venue: str, base: str) -> float:
        """Find the stored fee for a given (venue, base) combination."""
        for inst_id_str, b in self._inst_to_base.items():
            if b == base and venue in inst_id_str:
                return self._inst_fee.get(inst_id_str, self._venue_defaults.get(venue, 0.001))
        return self._venue_defaults.get(venue, 0.001)

    def _best_arb(
        self, base: str, venue_data: dict[str, tuple[float, float]]
    ) -> tuple | None:
        """
        Evaluate all buy/sell venue pairs and return the one with highest net spread.

        买入成本 = ask × (1 + fee_buy + slippage)
        卖出收益 = bid × (1 - fee_sell - slippage)
        净价差   = 卖出收益 - 买入成本
        """
        mid = sum((b + a) / 2 for b, a in venue_data.values()) / len(venue_data)
        if mid == 0:
            return None

        slip = self._slippage
        best_net = float("-inf")
        best: tuple | None = None

        venues = list(venue_data.keys())
        for buy_v in venues:
            ask = venue_data[buy_v][1]
            fee_b = self._fee_for_inst(buy_v, base)
            for sell_v in venues:
                if sell_v == buy_v:
                    continue
                bid = venue_data[sell_v][0]
                fee_s = self._fee_for_inst(sell_v, base)
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
        slippage: float,
    ) -> None:
        tag = ">> ARBI" if net_pct > 0 else "   norm"
        prices = "  ".join(
            f"{v}:{venue_data[v][0]:.6g}/{venue_data[v][1]:.6g}"
            for v in sorted(venue_data)
        )
        ts = time.strftime("%H:%M:%S")
        fee_pct = (fee_b + fee_s) * 100
        slip_pct = slippage * 2 * 100   # 双边滑点
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
        for base, venue_data in self._prices.items():
            if len(venue_data) < len(VENUE_NAMES):
                continue
            result = self._best_arb(base, venue_data)
            if result is None:
                continue
            gross_pct, net_pct, buy_v, _, fee_b, sell_v, _, fee_s = result
            rows.append((net_pct, gross_pct, base, venue_data, buy_v, sell_v, fee_b, fee_s))

        if not rows:
            return

        rows.sort(reverse=True)
        ts = time.strftime("%H:%M:%S")
        slip_pct = self._slippage * 2 * 100
        print(f"\n{ts} ══ TOP 20 净价差排名（手续费+滑点{slip_pct:.3f}%后）══")
        fmt = "  {:<6}  {:<14}  gross={:+.5f}%  fees={:.3f}%  slip={:.3f}%  net={:+.5f}%  ({} → {})"
        for net_pct, gross_pct, base, venue_data, buy_v, sell_v, fee_b, fee_s in rows[:20]:
            tag = "ARBI" if net_pct > 0 else "norm"
            fee_pct = (fee_b + fee_s) * 100
            print(fmt.format(tag, base + "/USDC", gross_pct, fee_pct, slip_pct, net_pct, buy_v, sell_v))
        print()
        sys.stdout.flush()


def main() -> None:
    parser = argparse.ArgumentParser(description="跨所 USDT 现货净价差监控")
    parser.add_argument(
        "--min-net", type=float, default=0.0,
        help="最小净价差打印阈值（百分比，默认 0.0）",
    )
    parser.add_argument(
        "--throttle", type=float, default=2.0,
        help="每对最小打印间隔秒数（默认 2）",
    )
    parser.add_argument(
        "--summary", type=int, default=30,
        help="汇总排名打印间隔秒数（默认 30）",
    )
    parser.add_argument(
        "--fees", type=str, default="",
        help="各所 taker 手续费，格式: BINANCE=0.001,BYBIT=0.001,OKX=0.001（默认全部 0.1%%）",
    )
    parser.add_argument(
        "--default-fee", type=float, default=0.001,
        help="未指定交易所的默认手续费（默认 0.001 = 0.1%%）",
    )
    parser.add_argument(
        "--slippage", type=float, default=0.0005,
        help="单边滑点估算（默认 0.0005 = 0.05%%，双边合计 0.1%%）",
    )
    parser.add_argument(
        "--alert-only", action="store_true",
        help="只在 net>0 时输出，关闭定时汇总，适合后台运行",
    )
    args = parser.parse_args()

    venue_fees = parse_fees_arg(args.fees, args.default_fee)
    print("[fees] 使用手续费率:")
    for v, f in venue_fees.items():
        print(f"  {v}: {f*100:.4f}%")
    print(f"[fees] 单边滑点: {args.slippage*100:.4f}%  双边合计: {args.slippage*2*100:.4f}%")

    config_node = TradingNodeConfig(
        trader_id="SPREAD-MONITOR-001",
        logging=LoggingConfig(log_level="WARNING"),
        data_clients={
            BINANCE: BinanceDataClientConfig(
                environment=BinanceEnvironment.LIVE,
                account_type=BinanceAccountType.SPOT,
                instrument_provider=InstrumentProviderConfig(load_all=True),
            ),
            BYBIT: BybitDataClientConfig(
                environment=BybitEnvironment.MAINNET,
                product_types=(BybitProductType.SPOT,),
                instrument_provider=InstrumentProviderConfig(load_all=True),
            ),
            KRAKEN: KrakenDataClientConfig(
                environment=KrakenEnvironment.LIVE,
                product_types=(KrakenProductType.SPOT,),
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
    node.add_data_client_factory(BYBIT, BybitLiveDataClientFactory)
    node.add_data_client_factory(KRAKEN, KrakenLiveDataClientFactory)
    node.build()
    node.trader.add_strategy(monitor)
    node.run()


if __name__ == "__main__":
    main()
