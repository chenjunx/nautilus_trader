import json
import os
import sys
import time
from collections import defaultdict
from collections import deque

from nautilus_trader.adapters.gateio import GATEIO
from nautilus_trader.adapters.kraken import KRAKEN
from nautilus_trader.adapters.kraken.http.wallet import fetch_kraken_trade_fees
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.trading.strategy import Strategy
from spread_monitor.chains import _fetch_gateio_chains
from spread_monitor.pricing import best_pair_arb
from spread_monitor.utils import _parse_csv_set
from spread_monitor.venues import BLACKLIST
from spread_monitor.venues import MAIN_PERP_VENUES
from spread_monitor.venues import MAIN_SPOT_VENUES
from spread_monitor.venues import SECONDARY_VENUES


class SpreadMonitorConfig(StrategyConfig, frozen=True):
    min_net_spread_pct: float = 0.0
    throttle_secs: float = 2.0
    summary_interval: int = 30
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
        """自动发现模式：至少一个主所同时有 USDT 现货+永续（保留对冲能力）的币种才入选，
        其余主所/副所上能找到的 USDT 现货全部纳入配对候选。"""
        main_spot: dict[str, dict[str, object]] = defaultdict(dict)   # {base: {venue: inst}}
        main_perp: dict[str, set] = defaultdict(set)                   # {base: {venue}}
        secondary_spot: dict[str, dict[str, object]] = defaultdict(dict)

        for inst in instruments:
            try:
                base = str(inst.base_currency)
                quote = str(inst.quote_currency)
            except AttributeError:
                continue

            if base in BLACKLIST:
                continue

            venue = str(inst.id.venue)

            if isinstance(inst, CurrencyPair):
                if quote != "USDT":
                    continue
                if venue in MAIN_SPOT_VENUES:
                    main_spot[base][venue] = inst
                elif venue in SECONDARY_VENUES:
                    secondary_spot[base][venue] = inst

            elif isinstance(inst, CryptoPerpetual):
                if quote != "USDT" or venue not in MAIN_PERP_VENUES:
                    continue
                main_perp[base].add(venue)

        # 筛选：至少一个主所自己同时有现货+永续（同一个所才具备对冲能力）；
        # 副所现货存在即可纳入，不再要求副所有永续
        qualifying: dict[str, dict] = {}
        for base in main_spot:
            if set(main_spot[base]) & main_perp.get(base, set()):
                qualifying[base] = {
                    "main_spot": main_spot[base],
                    "secondary_spot": secondary_spot.get(base, {}),
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
            self.log.info(f"净价差阈值: {self._min_net_pct}%")
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

        # Kraken 真实费率（可选）：配置了 KRAKEN_SPOT_API_KEY/SECRET 时，按实际用到的交易对
        # 拉取账户 30 天成交量对应的真实 taker 费率，覆盖 DEFAULT_FEES/--fees 里的静态兜底值；
        # 未配置或拉取失败时静默回退，不影响启动。
        kraken_pairs = {
            str(inst.raw_symbol)
            for info in qualifying.values()
            for venue, inst in info["secondary_spot"].items()
            if venue == str(KRAKEN)
        }
        kraken_real_fees: dict[str, float] = {}
        if kraken_pairs:
            kraken_key = os.environ.get("KRAKEN_SPOT_API_KEY")
            kraken_secret = os.environ.get("KRAKEN_SPOT_API_SECRET")
            if kraken_key and kraken_secret:
                try:
                    kraken_real_fees = fetch_kraken_trade_fees(kraken_key, kraken_secret, kraken_pairs)
                    self.log.info(
                        f"[fee] {KRAKEN}: 已获取 {len(kraken_real_fees)}/{len(kraken_pairs)} "
                        "个交易对的真实费率",
                    )
                except Exception as exc:  # noqa: BLE001 - 费率仅为增强，拉取失败不应阻断启动
                    self.log.warning(f"[fee] {KRAKEN} 真实费率拉取失败，回退到默认费率: {exc!r}")
            else:
                self.log.info(f"[fee] {KRAKEN}: 未配置 KRAKEN_SPOT_API_KEY/SECRET，使用默认费率")

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
                if venue == str(KRAKEN) and str(inst.raw_symbol) in kraken_real_fees:
                    fee = kraken_real_fees[str(inst.raw_symbol)]
                else:
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

    def _fee_of(self, base: str, venue: str) -> float:
        return next(
            (self._inst_fee[k] for k in self._inst_fee
             if self._inst_to_base.get(k) == base and venue in k),
            self._venue_defaults.get(venue, 0.001),
        )

    def _best_arb(
        self,
        base: str,
        venue_data: dict[str, tuple[float, float]],
        base_info: dict[str, set],
    ) -> tuple | None:
        """
        主所↔主所、主所↔副所均可配对；副所↔副所不配对（两侧都不能开仓对冲，无可执行路径）。
        实际计算委托给 pricing.best_pair_arb，此处只负责组装副所↔副所排除对。
        """
        sec_venues = base_info.get("secondary", set()) & venue_data.keys()
        excluded_pairs = frozenset(
            frozenset((a, b)) for a in sec_venues for b in sec_venues if a != b
        )
        return best_pair_arb(
            venue_data,
            fee_of=lambda v: self._fee_of(base, v),
            excluded_pairs=excluded_pairs,
        )

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
        print(
            f"{ts} {tag} | {base+'/USDT':<14} | {prices}\n"
            f"         在 {buy_v} 买(ask={buy_ask:.6g}, 费={fee_b*100:.3f}%)  "
            f"在 {sell_v} 卖(bid={sell_bid:.6g}, 费={fee_s*100:.3f}%)\n"
            f"         毛价差={gross_pct:+.4f}%  手续费={fee_pct:.3f}%  "
            f"净价差={net_pct:+.4f}%"
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
            print(f"\n{ts} ══ TOP 20 净价差排名（主所↔副所，扣除手续费后）══")
            fmt = "  {:<6}  {:<14}  gross={:+.5f}%  fees={:.3f}%  net={:+.5f}%  ({} → {})"
            for net_pct, gross_pct, base, venue_data, buy_v, sell_v, fee_b, fee_s in rows[:20]:
                tag = "ARBI" if net_pct > 0 else "norm"
                fee_pct = (fee_b + fee_s) * 100
                print(fmt.format(tag, base + "/USDT", gross_pct, fee_pct, net_pct, buy_v, sell_v))
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
