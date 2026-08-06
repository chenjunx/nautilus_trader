#!/usr/bin/env python3
"""
Cross-venue USDT spot spread monitor - main vs secondary venues.
监控主所与副所之间的 USDT 现货价差。

主副所判定规则:
  一个所只要同时有 USDT 现货 + USDT 永续，就是主所（如 Binance、Gate.io、OKX）；
  否则只能是副所，只交易现货（如 Kraken，其永续以 USD 结算，不满足 USDT 永续）。
  主副所唯一的业务差异：开仓（永续对冲敞口）只能在主所做，副所只做现货、不参与开仓，
  因此主所可以两两配对算价差，副所必须至少一侧是主所才能配对（两个纯现货副所之间
  没有可执行的对冲路径，不允许配对）。
  Binance/Gate.io 的现货与永续共用同一交易所账号，通过独立 venue key 拆成两个
  client 以避免 client_id 冲突：BINANCE/BINANCE_FUT、GATEIO/GATEIO_FUT；
  OKX 的现货+永续可在同一个 client 里一起加载，无需拆分。

筛选规则（--mode auto，默认）:
  1. 币种在任意主所同时有 USDT 现货 + USDT 永续（保留对冲能力前提）
  2. 该币种在其余任意主所/副所上找到的 USDT 现货全部纳入配对候选
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

真实费率（自动，无需开关）:
  Binance/OKX 的 adapter 本身在配置了对应 API Key 时就会自动按账户实际费率（VIP 档位后）
  加载 instrument.taker_fee，脚本直接读取，无需额外处理。
  Kraken 的 adapter 不支持这一点，脚本在配置了 KRAKEN_SPOT_API_KEY/SECRET 时会在启动时
  额外调用私有 TradeVolume 接口，按账户 30 天成交量对应档位拉取真实 taker 费率覆盖
  DEFAULT_FEES/--fees 的静态值；未配置该 Key 或拉取失败时静默回退到静态值，不影响启动。
  Gate.io 的 adapter 完全没有私有接口支持，只能使用静态费率。

费用模型（单边）:
  买入成本 = ask × (1 + taker_fee + slippage)
  卖出收益 = bid × (1 - taker_fee - slippage)
  净价差   = 卖出收益 - 买入成本（主所↔主所、主所↔副所方向；副所↔副所不计算）

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

本脚本的实现按功能拆分在同目录下的 spread_monitor/ 包中
（venues/chains/kraken_api/strategy/cli），此文件只是保留原命令行用法的入口。
"""

from spread_monitor.cli import main


if __name__ == "__main__":
    main()
