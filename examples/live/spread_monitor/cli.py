import argparse
import json

from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from spread_monitor.chains import _dump_chains
from spread_monitor.chains import _load_chain_support
from spread_monitor.strategy import SpreadMonitor
from spread_monitor.strategy import SpreadMonitorConfig
from spread_monitor.utils import _parse_csv_set
from spread_monitor.venues import MAIN_SPOT_VENUES
from spread_monitor.venues import SECONDARY_VENUES
from spread_monitor.venues import VENUE_REGISTRY
from spread_monitor.venues import parse_fees_arg


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
                        help="manual 模式：逗号分隔的副所列表，如 KRAKEN,GATEIO,BITFINEX")
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
