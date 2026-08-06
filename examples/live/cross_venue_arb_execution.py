#!/usr/bin/env python3
"""
Cross-venue delta-neutral arbitrage execution — build position + roundtrip arbitrage.
跨所现货 delta-neutral 建仓 + 套利执行。

与只读的 cross_venue_spread_monitor.py 完全隔离，运行在独立的、持有交易+提现权限
API Key 的 TradingNode 进程里。

支持自定义主所+副所组合：
  - 主所（必须同时有现货+永续）：BINANCE, GATEIO, OKX
  - 副所（只需要现货）：KRAKEN, BITFINEX, 以及所有主所的现货部分

建仓逻辑（IDLE -> ACTIVE）:
  1. 发现价差达到 --build-trigger 阈值时，在主所现货买入 --build-notional USDT。
  2. 现货成交后，在主所永续开等量空单对冲，永续腿失败会重试一次，
     仍失败则紧急平掉现货并转入 PAUSED_ERROR 终态，需人工处理。
  3. 永续对冲成交后，自动调用主所提现 API 把一半现货转到副所（提现地址需
     提前在主所网页端加白名单——这是操作前提，脚本不处理开白名单）。
  4. 副所确认到账后转入 ACTIVE。

套利逻辑（ACTIVE 内部循环，不改变 phase）:
  价差达到 --arb-trigger 阈值时，用两所已有库存低买高卖，不再需要转账。

安全机制: 暂停开关（--pause-flag-path）、建仓并发/活跃 base 数/总名义金额上限、
裸敞口超时+紧急平仓、提现经济性检查、副所选链用入金链而非出金链、到账超时只告警
不重发、套利下单防重叠。默认 --dry-run，详见 spread_monitor/execution_strategy.py。

Usage:
    # BINANCE + KRAKEN 组合（原默认）
    python examples/live/cross_venue_arb_execution.py \
        --main-venue BINANCE --secondary-venue KRAKEN \
        --bases DOGE,ADA --dry-run

    # OKX + BITFINEX 组合
    python examples/live/cross_venue_arb_execution.py \
        --main-venue OKX --secondary-venue BITFINEX \
        --bases DOGE --no-dry-run --build-notional 50

    # GATEIO + KRAKEN 组合（需要对应的 API Key 环境变量）
    GATEIO_API_KEY=... GATEIO_API_SECRET=... \
    KRAKEN_SPOT_API_KEY=... KRAKEN_SPOT_API_SECRET=... \
        python examples/live/cross_venue_arb_execution.py \
            --main-venue GATEIO --secondary-venue KRAKEN \
            --bases DOGE --no-dry-run

本脚本的实现在同目录下的 spread_monitor/ 包中（execution_strategy/exec_cli），
此文件只是保留统一命令行用法的入口。
"""

from spread_monitor.exec_cli import main


if __name__ == "__main__":
    main()
