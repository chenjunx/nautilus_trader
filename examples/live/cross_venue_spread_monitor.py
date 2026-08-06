#!/usr/bin/env python3
"""
Cross-venue USDT spot spread monitor - main vs secondary venues.
跨所 USDT 现货价差监控 - 主所 vs 副所。

主所（现货+永续）：同时具备现货与 USDT 永续合约，可开仓/对冲。
副所（仅现货）：只交易现货，不参与开仓对冲。

自动发现模式（默认）：
  - 发现所有支持 USDT 交易对的 base，按主所/副所角色自动分组。
  - 自动计算主所↔副所、主所↔主所的净价差排名。

手动指定模式（--mode manual）：
  - 手动指定 --symbols BTC,ETH 等币种（必填）。
  - 手动指定 --main BINANCE 主所（必填，只能指定一个）。
  - 手动指定 --secondary KRAKEN,GATEIO 副所（可选，可指定多个）。
  - 若不指定 --secondary，默认使用所有其他已注册交易所作为副所。

净价差方向：
  - 主所↔主所：所有配对都计算（两侧都能开仓对冲）
  - 主所↔副所：所有配对都计算（主所开仓对冲）
  - 副所↔副所：不计算（两侧都不能开仓对冲，无可执行路径）

链筛选（--require-common-chain，可选）:
  提现/入金链交集过滤（主所提现 ∩ 副所入金），要求主所提供 wallet API 支持
  （目前只有 Binance 与 Kraken 有完整 API），拉取失败则放行该币种。
  Gate.io 的 adapter 完全没有私有接口支持，只能使用静态费率。

费用模型（单边）:
  买入成本 = ask × (1 + taker_fee)
  卖出收益 = bid × (1 - taker_fee)
  净价差   = 卖出收益 - 买入成本（主所↔主所、主所↔副所方向；副所↔副所不计算）

Usage:
    python examples/live/cross_venue_spread_monitor.py
    python examples/live/cross_venue_spread_monitor.py --alert-only
    python examples/live/cross_venue_spread_monitor.py --fees KRAKEN=0.002
    python examples/live/cross_venue_spread_monitor.py --mode manual \
        --symbols BTC,ETH --main BINANCE --secondary KRAKEN,GATEIO
    BINANCE_API_KEY=... BINANCE_API_SECRET=... \
    KRAKEN_SPOT_API_KEY=... KRAKEN_SPOT_API_SECRET=... \
        python examples/live/cross_venue_spread_monitor.py --require-common-chain
    python examples/live/cross_venue_spread_monitor.py --no-require-common-chain

本脚本的实现按功能拆分在同目录下的 spread_monitor/ 包中
（venues/chains/kraken_api/strategy/cli），此文件只是保留原命令行用法的入口。
"""

from spread_monitor.cli import main


if __name__ == "__main__":
    main()
