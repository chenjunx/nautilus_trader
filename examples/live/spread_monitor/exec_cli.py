import argparse
import json

from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from spread_monitor.execution_strategy import ArbExecutionConfig
from spread_monitor.execution_strategy import ArbExecutionStrategy
from spread_monitor.venue_config import build_venue_config
from spread_monitor.venues import DEFAULT_FEES
from spread_monitor.venues import parse_fees_arg


def main() -> None:
    parser = argparse.ArgumentParser(
        description="跨所现货 delta-neutral 建仓 + 套利执行（支持自定义主所+副所）",
    )
    parser.add_argument("--main-venue", type=str, required=True,
                        choices=["BINANCE", "GATEIO", "OKX"],
                        help="主所（现货+永续，用于建仓对冲），必填")
    parser.add_argument("--secondary-venue", type=str, required=True,
                        choices=["KRAKEN", "BITFINEX", "BINANCE", "GATEIO", "OKX"],
                        help="副所（只交易现货），必填，不可与主所相同")
    parser.add_argument("--bases", type=str, required=True,
                        help="逗号分隔的币种列表，无自动发现，如 DOGE,ADA（必填）")
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--build-notional", type=float, default=50.0,
                        help="单次建仓的现货名义金额（USDT，默认 50）")
    parser.add_argument("--build-trigger", type=float, default=0.15,
                        help="触发建仓的最小净价差百分比（默认 0.15）")
    parser.add_argument("--arb-trigger", type=float, default=0.05,
                        help="触发套利轮转的最小净价差百分比（默认 0.05）")
    parser.add_argument("--max-concurrent-builds", type=int, default=2)
    parser.add_argument("--max-active-bases", type=int, default=8)
    parser.add_argument("--global-notional-cap", type=float, default=5000.0,
                        help="所有在途建仓合计名义金额上限（USDT，默认 5000）")
    parser.add_argument("--per-trade-cap", type=float, default=200.0,
                        help="单次套利轮转下单的名义金额上限（USDT，默认 200）")
    parser.add_argument("--perp-fill-timeout", type=float, default=15.0,
                        help="永续对冲腿等待成交超时秒数（默认 15）")
    parser.add_argument("--withdrawal-poll-interval", type=float, default=30.0)
    parser.add_argument("--withdrawal-timeout", type=float, default=3600.0,
                        help="链上转账到账超时秒数（默认 3600，只告警不重发）")
    parser.add_argument("--withdrawal-fee-safety-multiple", type=float, default=3.0,
                        help="建仓预期收益需覆盖提现手续费的倍数（默认 3）")
    parser.add_argument("--fees", type=str, default="",
                        help="覆盖手续费，格式: BINANCE=0.00075,KRAKEN=0.0005")
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=True,
                        help="dry-run 模式（默认开启，不真实下单/提现）；--no-dry-run 关闭")
    parser.add_argument("--pause-flag-path", type=str, default="ARB_PAUSED",
                        help="该文件存在时暂停新建仓/新套利单（默认 ARB_PAUSED）")
    parser.add_argument("--binance-environment", type=str, default="live",
                        choices=["live", "testnet"])
    args = parser.parse_args()

    # 验证主副所不能相同
    if args.main_venue.upper() == args.secondary_venue.upper():
        print(f"[exec] 错误：主所和副所不能相同（{args.main_venue}）")
        return

    binance_env = BinanceEnvironment.TESTNET if args.binance_environment == "testnet" else BinanceEnvironment.LIVE
    if binance_env == BinanceEnvironment.TESTNET and not args.dry_run:
        print("[exec] 注意：Binance testnet 下其他交易所仍会按 dry_run 参数决定是否真实下单/提现")

    venue_fees = parse_fees_arg(args.fees) if args.fees else dict(DEFAULT_FEES)
    print(f"[exec] 主所={args.main_venue} 副所={args.secondary_venue}")
    print(f"[exec] dry_run={args.dry_run} binance_environment={args.binance_environment}")
    print(f"[exec] bases={args.bases}")

    # 动态构建交易所配置
    try:
        venue_cfg = build_venue_config(
            main_venue=args.main_venue,
            secondary_venue=args.secondary_venue,
            binance_environment=binance_env,
        )
    except ValueError as e:
        print(f"[exec] 配置错误: {e}")
        return

    config_node = TradingNodeConfig(
        trader_id="ARB-EXEC-001",
        logging=LoggingConfig(log_level=args.log_level),
        exec_engine=LiveExecEngineConfig(reconciliation=True),
        data_clients=venue_cfg["data_clients"],
        exec_clients=venue_cfg["exec_clients"],
        strategies=[],
    )

    strategy = ArbExecutionStrategy(
        ArbExecutionConfig(
            strategy_id="ARB-EXEC-001",
            bases_csv=args.bases,
            main_spot_venue=venue_cfg["main_spot_venue"],
            main_perp_venue=venue_cfg["main_perp_venue"],
            secondary_spot_venue=venue_cfg["secondary_spot_venue"],
            build_notional_usdt=args.build_notional,
            build_trigger_net_pct=args.build_trigger,
            arb_trigger_net_pct=args.arb_trigger,
            max_concurrent_builds=args.max_concurrent_builds,
            max_active_bases=args.max_active_bases,
            global_notional_cap_usdt=args.global_notional_cap,
            per_trade_notional_cap_usdt=args.per_trade_cap,
            perp_fill_timeout_secs=args.perp_fill_timeout,
            withdrawal_poll_interval_secs=args.withdrawal_poll_interval,
            withdrawal_timeout_secs=args.withdrawal_timeout,
            withdrawal_fee_safety_multiple=args.withdrawal_fee_safety_multiple,
            venue_fees_json=json.dumps(venue_fees),
            dry_run=args.dry_run,
            pause_flag_path=args.pause_flag_path,
        ),
    )

    node = TradingNode(config=config_node)
    # 动态注册所有交易所的 data/exec factories
    for venue, factory in venue_cfg["data_factories"].items():
        node.add_data_client_factory(venue, factory)
    for venue, factory in venue_cfg["exec_factories"].items():
        node.add_exec_client_factory(venue, factory)
    node.build()
    node.trader.add_strategy(strategy)

    try:
        node.run()
    finally:
        node.dispose()
