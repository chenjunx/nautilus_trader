"""建仓 + 套利执行策略。跑在独立的、持有交易+提现权限 Key 的 TradingNode 进程里
（见 `exec_cli.py`），与只读的 `strategy.py:SpreadMonitor` 完全隔离。

状态机（按 base 持久化，见 `state.py`）：
    IDLE -[价差触发]-> BUILDING_SPOT -[现货成交]-> BUILDING_PERP -[永续成交]->
    TRANSFERRING -[Kraken 到账]-> ACTIVE -[套利轮转，phase 不变]-> ACTIVE ...
    异常终态：PAUSED_ERROR（永续腿重试后仍失败，已紧急平掉现货，需人工清空状态才恢复）。

v1 已知限制：
  - 只吃显式 --bases，没有自动发现。
  - 只做一次转账把库存铺到两所，后续套利轮转不再调整永续对冲量，两所库存会逐渐失衡。
  - 假设市价单一次性成交（`order.is_closed` 才处理），不特殊处理分笔成交的中间态。
"""

import json
import queue
import time
from collections import defaultdict
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.events import OrderDenied
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.trading.strategy import Strategy
from spread_monitor.guardrails import check_buy_side_is_main_venue
from spread_monitor.guardrails import check_global_notional_cap
from spread_monitor.guardrails import check_max_active_bases
from spread_monitor.guardrails import check_max_concurrent_builds
from spread_monitor.guardrails import check_net_pct_threshold
from spread_monitor.guardrails import check_perp_hedge_quantity
from spread_monitor.guardrails import check_withdrawal_economics
from spread_monitor.guardrails import is_paused
from spread_monitor.pricing import best_pair_arb
from spread_monitor.sizing import qty_for_notional
from spread_monitor.sizing import roundtrip_qty
from spread_monitor.state import ArbState
from spread_monitor.state import ArbStateStore
from spread_monitor.state import Phase
from spread_monitor.utils import _parse_csv_set
from spread_monitor.utils import split_leveraged_base
from spread_monitor.wallet import WALLET_REGISTRY


IN_PROGRESS_BUILD_PHASES = (Phase.BUILDING_SPOT, Phase.BUILDING_PERP, Phase.TRANSFERRING)
ACTIVE_BASE_PHASES = (*IN_PROGRESS_BUILD_PHASES, Phase.ACTIVE)

# HACK(kraken-currency-code): nautilus_trader 的 Kraken 适配器
# （crates/adapters/kraken/src/common/parse.rs::parse_spot_instrument）在构造
# CurrencyPair 时，base_currency/quote_currency 直接用了 Kraken 内部的历史资产码
# （如 "XXBT"/"XXDG"/"ZUSD"），没有像它自己拼 instrument_id 时那样做
# XBT→BTC / XDG→DOGE 的归一化——导致 inst.base_currency 在 Kraken 上可能是
# "XXDG" 而不是 "DOGE"，跟 BINANCE 等其它交易所的编码对不上，本模块按
# base_currency 字符串做跨所匹配时会误判成"这个 base 在 KRAKEN 没有现货"。
#
# 这里只是临时在策略侧补一刀：剥掉 Kraken 遗留的 X/Z 类别前缀，再套用它自己的
# 改名表。跟 Kraken 官方 Rust 代码里 normalize_currency_code() 的逻辑一致，
# 不是本模块发明的新规则。已知局限：无法区分"历史前缀"和"真的以 X/Z 开头的现代
# 币种代码"（Kraken 自己有 /0/public/Assets 接口能给出权威 altname 消除这个歧义，
# 但这个适配器目前没调用它，Python 侧也没打算为此多发一次请求）。
# 等 Kraken 适配器把 base_currency/quote_currency 也归一化后，这段可以整体删掉。
_KRAKEN_LEGACY_RENAMES = {"XBT": "BTC", "XDG": "DOGE"}


def _normalize_kraken_currency(code: str) -> str:
    stripped = code[1:] if code[:1] in ("X", "Z") else code
    return _KRAKEN_LEGACY_RENAMES.get(stripped, stripped)


class ArbExecutionConfig(StrategyConfig, frozen=True):
    bases_csv: str
    main_spot_venue: str  # 主所现货 venue key
    main_perp_venue: str  # 主所永续 venue key
    secondary_spot_venue: str  # 副所现货 venue key
    build_notional_usdt: float = 50.0
    build_trigger_net_pct: float = 0.15
    arb_trigger_net_pct: float = 0.05
    max_concurrent_builds: int = 2
    max_active_bases: int = 8
    global_notional_cap_usdt: float = 5000.0
    per_trade_notional_cap_usdt: float = 200.0
    perp_fill_timeout_secs: float = 15.0
    withdrawal_poll_interval_secs: float = 30.0
    withdrawal_timeout_secs: float = 3600.0
    withdrawal_fee_safety_multiple: float = 3.0
    venue_fees_json: str = "{}"
    # 调试模式：净价差计算时手续费按 0 处理（去掉手续费扣除），
    # 用于人为放大触发机会，跑通整套建仓/套利流程；仅应配合 dry_run=True 使用。
    debug_ignore_fees: bool = False
    dry_run: bool = True
    pause_flag_path: str = "ARB_PAUSED"


class ArbExecutionStrategy(Strategy):
    def __init__(self, config: ArbExecutionConfig) -> None:
        super().__init__(config)
        self._dry_run = config.dry_run
        self._venue_defaults: dict[str, float] = json.loads(config.venue_fees_json)
        self._debug_ignore_fees = config.debug_ignore_fees

        # 从配置读取主副所 venue keys
        self._main_spot_venue = config.main_spot_venue
        self._main_perp_venue = config.main_perp_venue
        self._secondary_spot_venue = config.secondary_spot_venue

        self._store: ArbStateStore | None = None
        self._states: dict[str, ArbState] = {}
        self._spot_inst: dict[str, dict[str, object]] = {}
        self._perp_inst: dict[str, object] = {}
        self._perp_multiplier: dict[str, int] = {}
        self._inst_to_base_venue: dict[str, tuple[str, str]] = {}
        self._prices: dict[str, dict[str, tuple[float, float]]] = defaultdict(dict)
        self._client_order_index: dict[str, tuple[str, str]] = {}
        self._chain_info: dict[str, dict] = {}
        self._chain_lookup_inflight: set[str] = set()
        self._result_queue: queue.SimpleQueue = queue.SimpleQueue()

        self._wallets: dict[str, object] = {}

    # ------------------------------------------------------------------ 启动 --

    def on_start(self) -> None:
        if self._debug_ignore_fees and not self._dry_run:
            self.log.error(
                "[exec] debug_ignore_fees=True 只能配合 dry_run 使用（会用失真的净价差触发真实下单/提现），"
                "停止启动",
            )
            self.stop()
            return
        if self._debug_ignore_fees:
            self.log.warning(
                "[DEBUG] debug_ignore_fees=True：建仓/套利触发阈值判断已去掉手续费扣除（fee=0），"
                "仅用于 dry-run 测试整套流程，不代表真实可执行收益！",
            )

        bases = sorted(_parse_csv_set(self.config.bases_csv))
        if not bases:
            self.log.error("[exec] --bases 为空，未指定任何币种，停止启动")
            self.stop()
            return

        main_spot = self._main_spot_venue
        main_perp = self._main_perp_venue
        secondary_spot = self._secondary_spot_venue

        self._wallets = {
            venue: wallet
            for venue in {main_spot, secondary_spot}
            if (wallet := WALLET_REGISTRY[venue]()) is not None
        }
        if not self._dry_run and (main_spot not in self._wallets or secondary_spot not in self._wallets):
            self.log.error(
                f"[exec] 非 dry-run 模式必须为主所 {main_spot} 与副所 {secondary_spot} 配置好对应的 API Key/Secret",
            )
            self.stop()
            return

        spot_by_base: dict[str, dict[str, object]] = defaultdict(dict)
        perp_by_base: dict[str, object] = {}
        perp_multiplier_by_base: dict[str, int] = {}
        for inst in self.cache.instruments():
            try:
                base = str(inst.base_currency)
                quote = str(inst.quote_currency)
            except AttributeError:
                continue
            venue = str(inst.id.venue)
            if venue == "KRAKEN":
                # 见文件顶部 HACK(kraken-currency-code) 说明。
                base = _normalize_kraken_currency(base)
                quote = _normalize_kraken_currency(quote)
            if quote != "USDT":
                continue
            if isinstance(inst, CurrencyPair) and venue in (main_spot, secondary_spot):
                spot_by_base[base][venue] = inst
            elif isinstance(inst, CryptoPerpetual) and venue == main_perp:
                real_base, multiplier = split_leveraged_base(main_perp, base)
                perp_by_base[real_base] = inst
                perp_multiplier_by_base[real_base] = multiplier

        self._store = ArbStateStore(self.cache)

        for base in bases:
            venues = spot_by_base.get(base, {})
            perp = perp_by_base.get(base)
            if main_spot not in venues or secondary_spot not in venues or perp is None:
                self.log.error(
                    f"[exec] 跳过 {base}：缺少 {main_spot} 现货/{secondary_spot} 现货/{main_perp} 永续 之一"
                    f"（{main_spot}={'有' if main_spot in venues else '无'} "
                    f"{secondary_spot}={'有' if secondary_spot in venues else '无'} "
                    f"PERP={'有' if perp is not None else '无'}）",
                )
                continue

            self._spot_inst[base] = venues
            self._perp_inst[base] = perp
            multiplier = perp_multiplier_by_base.get(base, 1)
            self._perp_multiplier[base] = multiplier
            if multiplier != 1:
                self.log.info(f"[exec] {base} 永续合约 {perp.id} 为放大面值合约：1 张 = {multiplier} 个真实 {base}")
            for venue, inst in venues.items():
                self._inst_to_base_venue[str(inst.id)] = (base, venue)
                self.subscribe_quote_ticks(inst.id)
            self._inst_to_base_venue[str(perp.id)] = (base, main_perp)
            self.subscribe_quote_ticks(perp.id)

            state = self._store.load(base)
            self._states[base] = state
            self._reindex_client_orders(state)
            if state.phase == Phase.PAUSED_ERROR:
                self.log.warning(
                    f"[exec] {base} 处于 PAUSED_ERROR 终态（last_error={state.last_error}），"
                    "需人工处理后清空状态才会恢复自动建仓",
                )
            elif state.phase in IN_PROGRESS_BUILD_PHASES:
                self.log.warning(f"[exec][resume] {base} 重启后恢复到中间状态 {state.phase}")
                self._resume(base, state)

        if not self._spot_inst:
            self.log.error("[exec] 没有任何 base 通过 instrument 校验，停止启动")
            self.stop()
            return

        self.clock.set_timer(
            name="exec:drain",
            interval=timedelta(seconds=1.0),
            callback=self._on_drain_timer,
        )

        self.log.info(
            f"[exec] 启动完成，dry_run={self._dry_run}，监控 {len(self._spot_inst)} 个 base: "
            f"{sorted(self._spot_inst)}",
        )

    def _reindex_client_orders(self, state: ArbState) -> None:
        if state.spot_client_order_id:
            self._client_order_index[state.spot_client_order_id] = (state.base, "spot_build")
        if state.perp_client_order_id:
            self._client_order_index[state.perp_client_order_id] = (state.base, "perp_build")
        if state.roundtrip_buy_order_id:
            self._client_order_index[state.roundtrip_buy_order_id] = (state.base, "roundtrip_buy")
        if state.roundtrip_sell_order_id:
            self._client_order_index[state.roundtrip_sell_order_id] = (state.base, "roundtrip_sell")

    def _resume(self, base: str, state: ArbState) -> None:
        """重新武装等待用的计时器，绝不重发订单——真实成交/拒绝事件靠 exec engine
        reconciliation 重放，`on_order_filled`/`on_order_rejected` 会正常触发。
        """
        if state.phase == Phase.BUILDING_PERP:
            self._arm_perp_timeout(base)
        elif state.phase == Phase.TRANSFERRING:
            self._arm_withdrawal_polling(base)

    # -------------------------------------------------------------- 状态存取 --

    def _advance_state(self, base: str, **changes) -> ArbState:
        updated = replace(self._states[base], **changes)
        self._states[base] = updated
        self._store.save(updated)
        return updated

    def _count_in_progress_builds(self) -> int:
        return sum(1 for s in self._states.values() if s.phase in IN_PROGRESS_BUILD_PHASES)

    def _count_active_bases(self) -> int:
        return sum(1 for s in self._states.values() if s.phase in ACTIVE_BASE_PHASES)

    def _committed_notional_usdt(self) -> float:
        return self._count_in_progress_builds() * self.config.build_notional_usdt

    def _fee_of(self, base: str, venue: str) -> float:
        if self._debug_ignore_fees:
            return 0.0
        inst = self._spot_inst.get(base, {}).get(venue)
        if inst is not None and float(inst.taker_fee) > 0:
            return float(inst.taker_fee)
        return self._venue_defaults.get(venue, 0.001)

    # ------------------------------------------------------------- 行情驱动 --

    def on_quote_tick(self, tick: QuoteTick) -> None:
        loc = self._inst_to_base_venue.get(str(tick.instrument_id))
        if loc is None:
            return
        base, venue = loc
        if venue == self._main_perp_venue:
            return  # 永续行情目前只在下单时按需读取最新价，不参与价差计算

        self._prices[base][venue] = (float(tick.bid_price), float(tick.ask_price))

        state = self._states.get(base)
        if state is None:
            return
        if state.phase == Phase.IDLE:
            self._maybe_start_build(base)
        elif state.phase == Phase.ACTIVE:
            self._maybe_start_roundtrip(base)

    # ------------------------------------------------------------------ 建仓 --

    def _build_guardrail_chain(
        self, base: str, net_pct: float, buy_v: str, qty: Decimal,
        multiplier: int, perp_min_quantity, withdraw_fee_usdt: float,
    ) -> list[tuple[bool, str]]:
        """建仓前风控校验链，按顺序短路检查；新增规则只需在此列表追加一项。"""
        return [
            check_max_concurrent_builds(self._count_in_progress_builds(), self.config.max_concurrent_builds),
            check_max_active_bases(self._count_active_bases(), self.config.max_active_bases),
            check_global_notional_cap(
                self._committed_notional_usdt(), self.config.build_notional_usdt,
                self.config.global_notional_cap_usdt,
            ),
            check_net_pct_threshold(net_pct, self.config.build_trigger_net_pct),
            check_buy_side_is_main_venue(buy_v, self._main_spot_venue),
            check_perp_hedge_quantity(qty, multiplier, perp_min_quantity),
            check_withdrawal_economics(
                self.config.build_notional_usdt, net_pct, withdraw_fee_usdt,
                self.config.withdrawal_fee_safety_multiple,
            ),
        ]

    def _maybe_start_build(self, base: str) -> None:
        venue_data = self._prices.get(base, {})
        if self._main_spot_venue not in venue_data or self._secondary_spot_venue not in venue_data:
            return
        if is_paused(self.config.pause_flag_path):
            return

        result = best_pair_arb(venue_data, fee_of=lambda v: self._fee_of(base, v))
        if result is None:
            return
        _gross_pct, net_pct, buy_v, buy_ask, _fee_b, _sell_v, sell_bid, _fee_s = result

        chain = self._chain_info.get(base)
        if chain is None:
            # 只有价差和买方看起来已经满足时才值得去异步查链信息，避免无意义的网络请求
            if net_pct >= self.config.build_trigger_net_pct and buy_v == self._main_spot_venue:
                self._start_chain_lookup(base)
            return
        if not chain.get("ok"):
            return

        spot_inst = self._spot_inst[base][self._main_spot_venue]
        qty = qty_for_notional(Decimal(str(self.config.build_notional_usdt)), Decimal(str(buy_ask)), spot_inst)
        if qty is None:
            self.log.debug(f"[exec] {base} 按 {self.config.build_notional_usdt} USDT 换算不出合法下单数量")
            return

        multiplier = self._perp_multiplier.get(base, 1)
        perp_min_quantity = self._perp_inst[base].min_quantity
        mid = (buy_ask + sell_bid) / 2
        withdraw_fee_usdt = chain["binance_withdraw_fee_coin"] * mid

        for ok, reason in self._build_guardrail_chain(base, net_pct, buy_v, qty, multiplier, perp_min_quantity, withdraw_fee_usdt):
            if not ok:
                self.log.debug(f"[exec] {base} 建仓被风控拦截: {reason}")
                return

        self._submit_spot_build(base, buy_ask, qty)

    def _submit_spot_build(self, base: str, ask_price: float, qty: Decimal) -> None:
        inst = self._spot_inst[base][self._main_spot_venue]

        self.log.info(f"[exec] {base} 触发建仓：{self._main_spot_venue} 现货买入约 {self.config.build_notional_usdt} USDT @ ~{ask_price}")

        if self._dry_run:
            client_order_id = f"DRYRUN-SPOT-{base}-{int(time.time() * 1000)}"
            self.log.warning(f"[DRY-RUN] {base} 建仓现货买入 {qty} @ {ask_price}（未真实下单）")
            self._client_order_index[client_order_id] = (base, "spot_build")
            self._advance_state(
                base, phase=Phase.BUILDING_SPOT,
                spot_client_order_id=client_order_id, spot_qty=str(qty),
            )
            self._on_spot_build_filled(base, qty, Decimal(str(ask_price)))
            return

        notional_qty = inst.make_qty(Decimal(str(self.config.build_notional_usdt)))
        order = self.order_factory.market(
            instrument_id=inst.id,
            order_side=OrderSide.BUY,
            quantity=notional_qty,
            quote_quantity=True,
        )
        self._client_order_index[str(order.client_order_id)] = (base, "spot_build")
        self._advance_state(
            base, phase=Phase.BUILDING_SPOT,
            spot_client_order_id=str(order.client_order_id),
        )
        self.submit_order(order, client_id=ClientId(self._main_spot_venue))

    def _on_spot_build_filled(self, base: str, qty: Decimal, _price: Decimal) -> None:
        self.log.info(f"[exec] {base} 现货建仓成交 qty={qty}，开始对冲永续空单")
        self._advance_state(base, phase=Phase.BUILDING_PERP, spot_qty=str(qty))
        self._submit_perp_hedge(base, qty)

    def _submit_perp_hedge(self, base: str, qty: Decimal) -> None:
        perp_inst = self._perp_inst[base]
        multiplier = self._perp_multiplier.get(base, 1)
        contracts = qty / multiplier

        if self._dry_run:
            client_order_id = f"DRYRUN-PERP-{base}-{int(time.time() * 1000)}"
            self.log.warning(
                f"[DRY-RUN] {base} 对冲永续空单 qty={qty}（{contracts} 张，倍数={multiplier}）（未真实下单）",
            )
            self._client_order_index[client_order_id] = (base, "perp_build")
            self._advance_state(base, perp_client_order_id=client_order_id, perp_qty=str(qty))
            price = self._prices.get(base, {}).get(self._main_spot_venue, (0.0, 0.0))[0]
            self._on_perp_build_filled(base, qty, Decimal(str(price)))
            return

        perp_qty = perp_inst.make_qty(contracts)
        order = self.order_factory.market(
            instrument_id=perp_inst.id,
            order_side=OrderSide.SELL,
            quantity=perp_qty,
        )
        self._client_order_index[str(order.client_order_id)] = (base, "perp_build")
        self._advance_state(base, perp_client_order_id=str(order.client_order_id), perp_qty=str(qty))
        self.submit_order(order, client_id=ClientId(self._main_perp_venue))
        self._arm_perp_timeout(base)

    def _arm_perp_timeout(self, base: str) -> None:
        self.clock.set_time_alert(
            name=f"exec:perp_timeout:{base}",
            alert_time=self.clock.utc_now() + timedelta(seconds=self.config.perp_fill_timeout_secs),
            callback=lambda event, b=base: self._on_perp_timeout(b),
            override=True,
        )

    def _cancel_timer_if_armed(self, name: str) -> None:
        """dry-run 分支会跳过对应的 _arm_* 调用，撤销一个从未设置过的定时器名字会
        触发 Condition.is_in 报错并杀掉整个策略，所以撤销前先判断是否存在。
        """
        if name in self.clock.timer_names:
            self.clock.cancel_timer(name)

    def _on_perp_build_filled(self, base: str, qty: Decimal, _price: Decimal) -> None:
        self._cancel_timer_if_armed(f"exec:perp_timeout:{base}")
        spot_qty = Decimal(self._states[base].spot_qty)
        self.log.info(f"[exec] {base} 永续对冲成交 perp_fill_qty={qty}（提现按现货成交量 spot_qty={spot_qty} 计算），开始转账一半现货到 {self._secondary_spot_venue}")
        self._advance_state(base, phase=Phase.TRANSFERRING, transfer_started_at_ts=time.time())
        self._start_withdrawal(base, spot_qty)

    def _on_perp_timeout(self, base: str) -> None:
        state = self._states.get(base)
        if state is None or state.phase != Phase.BUILDING_PERP:
            return  # 已经成交或已处理过，忽略迟到的定时器回调

        if state.perp_leg_attempt < 1:
            self.log.warning(f"[exec] {base} 永续腿超时，先撤单再重试一次")
            self._cancel_stale_perp_order(base, state.perp_client_order_id)
            self._advance_state(base, perp_leg_attempt=state.perp_leg_attempt + 1)
            self._submit_perp_hedge(base, Decimal(state.spot_qty))
            return

        self._cancel_stale_perp_order(base, state.perp_client_order_id)
        self._emergency_flatten(base, f"永续腿超时（重试后仍未成交），perp_client_order_id={state.perp_client_order_id}")

    def _cancel_stale_perp_order(self, base: str, client_order_id: str | None) -> None:
        """超时重试/紧急平仓前先撤掉原永续订单，避免它在重试单之后才迟到成交，
        导致两条永续腿都成交、实际对冲量翻倍。找不到订单（dry-run 或已终结）时静默跳过。
        """
        if self._dry_run or client_order_id is None:
            return
        order = self.cache.order(ClientOrderId(client_order_id))
        if order is not None and not order.is_closed:
            self.cancel_order(order, client_id=ClientId(self._main_perp_venue))

    def _on_order_failed(self, client_order_id: str, reason: str) -> None:
        loc = self._client_order_index.get(client_order_id)
        if loc is None:
            return
        base, leg = loc
        if leg != "perp_build":
            self.log.warning(f"[exec] {base} 订单 {leg} 被拒绝/拒接: {reason}")
            return

        state = self._states.get(base)
        if state is None or state.phase != Phase.BUILDING_PERP:
            return
        self._cancel_timer_if_armed(f"exec:perp_timeout:{base}")

        if state.perp_leg_attempt < 1:
            self.log.warning(f"[exec] {base} 永续腿被拒绝（{reason}），重试一次")
            self._advance_state(base, perp_leg_attempt=state.perp_leg_attempt + 1)
            self._submit_perp_hedge(base, Decimal(state.spot_qty))
            return

        self._emergency_flatten(base, f"永续腿被拒绝（重试后仍失败）: {reason}")

    def on_order_rejected(self, event: OrderRejected) -> None:
        self._on_order_failed(str(event.client_order_id), event.reason)

    def on_order_denied(self, event: OrderDenied) -> None:
        self._on_order_failed(str(event.client_order_id), event.reason)

    def _emergency_flatten(self, base: str, reason: str) -> None:
        self.log.error(f"[exec] {base} 紧急平仓：{reason}")
        state = self._states[base]

        if not self._dry_run and state.spot_client_order_id:
            position = self.cache.position_for_order(ClientOrderId(state.spot_client_order_id))
            if position is not None and position.is_open:
                self.close_position(position, client_id=ClientId(self._main_spot_venue))
            else:
                self.log.error(f"[exec] {base} 紧急平仓时找不到现货持仓（可能已被手动处理），仅标记终态")

        self._advance_state(base, phase=Phase.PAUSED_ERROR, last_error=reason)
        self.log.error(f"[exec] {base} 已转入 PAUSED_ERROR，需人工检查后清空 cache 状态才会恢复自动建仓")

    # --------------------------------------------------------- 成交事件分发 --

    def on_order_filled(self, event: OrderFilled) -> None:
        loc = self._client_order_index.get(str(event.client_order_id))
        if loc is None:
            return

        order = self.cache.order(event.client_order_id)
        if order is None or not order.is_closed:
            return  # 市价单通常一次成交；极少数分笔成交场景等到完全成交再推进状态机

        base, leg = loc
        del self._client_order_index[str(event.client_order_id)]
        qty = Decimal(str(order.filled_qty))
        price = Decimal(str(order.avg_px))

        if leg == "spot_build":
            self._on_spot_build_filled(base, qty, price)
        elif leg == "perp_build":
            self._on_perp_build_filled(base, qty, price)
        elif leg in ("roundtrip_buy", "roundtrip_sell"):
            self._on_roundtrip_leg_filled(base, leg, qty, price)

    # --------------------------------------------------------------- 提现 --

    def _kraken_address_cache_key(self, base: str, chain: str) -> str:
        return f"{base}:{chain}"

    def _start_withdrawal(self, base: str, spot_qty: Decimal) -> None:
        chain = self._chain_info.get(base)
        withdraw_qty = spot_qty / 2

        if self._dry_run or chain is None:
            self.log.warning(f"[DRY-RUN] {base} 提现 {withdraw_qty} 到 Kraken（未真实提现）")
            self._advance_state(
                base, withdrawal_id="DRYRUN-WITHDRAW", withdrawal_chain="DRYRUN",
                withdrawal_qty=str(withdraw_qty),
            )
            self._on_deposit_confirmed(base)
            return

        self.log.info(f"[exec] {base} 异步提交提现: qty={withdraw_qty} chain={chain['chain']}")
        self.run_in_executor(
            self._submit_withdrawal_blocking,
            args=(base, chain["chain"], chain["kraken_address"], chain["kraken_tag"], float(withdraw_qty)),
        )

    def _submit_withdrawal_blocking(
        self, base: str, chain: str, address: str, tag: str, amount: float,
    ) -> None:
        try:
            withdrawal_id = self._wallets[self._main_spot_venue].withdraw(base, chain, address, amount, tag=tag or None)
            result = {"base": base, "ok": True, "withdrawal_id": withdrawal_id, "chain": chain, "amount": amount}
        except Exception as exc:  # noqa: BLE001 - 提现结果未知，绝不能因此自动重发
            result = {"base": base, "ok": False, "error": repr(exc), "amount": amount}
        self._result_queue.put_nowait(("withdraw_submitted", result))

    def _handle_withdraw_submitted(self, payload: dict) -> None:
        base = payload["base"]
        if not payload["ok"]:
            self.log.error(
                f"[exec] {base} 提现调用异常，结果未知（不会自动重发）: {payload['error']}，"
                "请人工核实交易所网页端是否已提交成功",
            )
            return
        self.log.info(f"[exec] {base} 提现已提交 withdrawal_id={payload['withdrawal_id']}")
        self._advance_state(
            base, withdrawal_id=payload["withdrawal_id"], withdrawal_chain=payload["chain"],
            withdrawal_qty=str(payload["amount"]),
        )
        self._arm_withdrawal_polling(base)

    def _arm_withdrawal_polling(self, base: str) -> None:
        self.clock.set_timer(
            name=f"exec:withdrawal_poll:{base}",
            interval=timedelta(seconds=self.config.withdrawal_poll_interval_secs),
            callback=lambda event, b=base: self._poll_deposit(b),
        )
        state = self._states[base]
        started = state.transfer_started_at_ts or time.time()
        remaining = max(1.0, self.config.withdrawal_timeout_secs - (time.time() - started))
        self.clock.set_time_alert(
            name=f"exec:withdrawal_timeout:{base}",
            alert_time=self.clock.utc_now() + timedelta(seconds=remaining),
            callback=lambda event, b=base: self._on_deposit_timeout(b),
            override=True,
        )

    def _poll_deposit(self, base: str) -> None:
        state = self._states.get(base)
        if state is None or state.phase != Phase.TRANSFERRING:
            self._cancel_timer_if_armed(f"exec:withdrawal_poll:{base}")
            return
        self.run_in_executor(self._poll_deposit_blocking, args=(base,))

    def _poll_deposit_blocking(self, base: str) -> None:
        state = self._states[base]
        try:
            rows = self._wallets[self._secondary_spot_venue].fetch_deposit_status(base)
            target = Decimal(state.withdrawal_qty)
            matched = any(
                abs(Decimal(str(row.get("amount", "0"))) - target) / target < Decimal("0.02")
                for row in rows
                if target != 0
            )
            result = {"base": base, "matched": matched}
        except Exception as exc:  # noqa: BLE001 - 单次轮询失败不应中断，下一轮定时器会再试
            result = {"base": base, "matched": False, "error": repr(exc)}
        self._result_queue.put_nowait(("deposit_status", result))

    def _handle_deposit_status(self, payload: dict) -> None:
        base = payload["base"]
        if payload.get("error"):
            self.log.warning(f"[exec] {base} 查询 {self._secondary_spot_venue} 到账状态失败（下一轮重试）: {payload['error']}")
            return
        if payload["matched"]:
            self._on_deposit_confirmed(base)

    def _on_deposit_confirmed(self, base: str) -> None:
        self._cancel_timer_if_armed(f"exec:withdrawal_poll:{base}")
        self._cancel_timer_if_armed(f"exec:withdrawal_timeout:{base}")
        self.log.info(f"[exec] {base} {self._secondary_spot_venue} 到账确认，转入 ACTIVE")
        self._advance_state(base, phase=Phase.ACTIVE)

    def _on_deposit_timeout(self, base: str) -> None:
        state = self._states.get(base)
        if state is None or state.phase != Phase.TRANSFERRING:
            return
        self.log.error(
            f"[exec] {base} 转账到账超时（{self.config.withdrawal_timeout_secs}s）："
            f"withdrawal_id={state.withdrawal_id}，资金可能仍在路上，只告警不自动重发提现，"
            "停止轮询，请人工确认后手动干预状态",
        )
        self._cancel_timer_if_armed(f"exec:withdrawal_poll:{base}")

    # ---------------------------------------------------------------- 套利 --

    def _maybe_start_roundtrip(self, base: str) -> None:
        state = self._states.get(base)
        if state is None or state.in_flight_roundtrip:
            return
        venue_data = self._prices.get(base, {})
        if self._main_spot_venue not in venue_data or self._secondary_spot_venue not in venue_data:
            return
        if is_paused(self.config.pause_flag_path):
            return

        result = best_pair_arb(venue_data, fee_of=lambda v: self._fee_of(base, v))
        if result is None:
            return
        _gross_pct, net_pct, buy_v, buy_ask, _fee_b, sell_v, sell_bid, _fee_s = result
        if net_pct < self.config.arb_trigger_net_pct:
            return

        buy_inst = self._spot_inst[base][buy_v]
        sell_inst = self._spot_inst[base][sell_v]

        # 使用固定数量：两所最小下单量的较大值
        qty = roundtrip_qty(Decimal(str(buy_ask)), buy_inst, sell_inst)
        if qty is None:
            self.log.debug(f"[exec] {base} 套利轮转：无法计算合法的固定下单量，跳过")
            return

        self.log.info(
            f"[exec] {base} 套利轮转: {buy_v} 买 {qty} @ {buy_ask} / {sell_v} 卖 @ {sell_bid} "
            f"净价差={net_pct:.4f}%",
        )
        self._submit_roundtrip(base, buy_v, buy_inst, sell_v, sell_inst, qty)

    def _submit_roundtrip(self, base: str, buy_v: str, buy_inst, sell_v: str, sell_inst, qty: Decimal) -> None:
        self._advance_state(base, in_flight_roundtrip=True)

        if self._dry_run:
            buy_id = f"DRYRUN-RTBUY-{base}-{int(time.time() * 1000)}"
            sell_id = f"DRYRUN-RTSELL-{base}-{int(time.time() * 1000)}"
            self.log.warning(f"[DRY-RUN] {base} 套利买腿 {buy_v} qty={qty}（未真实下单）")
            self.log.warning(f"[DRY-RUN] {base} 套利卖腿 {sell_v} qty={qty}（未真实下单）")
            self._client_order_index[buy_id] = (base, "roundtrip_buy")
            self._client_order_index[sell_id] = (base, "roundtrip_sell")
            self._advance_state(base, roundtrip_buy_order_id=buy_id, roundtrip_sell_order_id=sell_id)
            price = self._prices[base][buy_v][1]
            self._on_roundtrip_leg_filled(base, "roundtrip_buy", qty, Decimal(str(price)))
            self._on_roundtrip_leg_filled(base, "roundtrip_sell", qty, Decimal(str(price)))
            return

        buy_qty = buy_inst.make_qty(qty)
        sell_qty = sell_inst.make_qty(qty)
        buy_order = self.order_factory.market(instrument_id=buy_inst.id, order_side=OrderSide.BUY, quantity=buy_qty)
        sell_order = self.order_factory.market(instrument_id=sell_inst.id, order_side=OrderSide.SELL, quantity=sell_qty)

        self._client_order_index[str(buy_order.client_order_id)] = (base, "roundtrip_buy")
        self._client_order_index[str(sell_order.client_order_id)] = (base, "roundtrip_sell")
        self._advance_state(
            base, roundtrip_buy_order_id=str(buy_order.client_order_id),
            roundtrip_sell_order_id=str(sell_order.client_order_id),
        )
        self.submit_order(buy_order, client_id=ClientId(buy_v))
        self.submit_order(sell_order, client_id=ClientId(sell_v))

    def _on_roundtrip_leg_filled(self, base: str, leg: str, qty: Decimal, price: Decimal) -> None:
        self.log.info(f"[exec] {base} 套利腿成交: {leg} qty={qty} price={price}")
        state = self._states[base]
        if leg == "roundtrip_buy":
            state = self._advance_state(base, roundtrip_buy_order_id=None)
        else:
            state = self._advance_state(base, roundtrip_sell_order_id=None)

        if state.roundtrip_buy_order_id is None and state.roundtrip_sell_order_id is None:
            self._advance_state(base, in_flight_roundtrip=False)

    # ------------------------------------------------------------- 链信息 --

    def _start_chain_lookup(self, base: str) -> None:
        if base in self._chain_lookup_inflight:
            return
        self._chain_lookup_inflight.add(base)
        self.log.info(
            f"[exec] {base} 异步拉取提现链信息（{self._main_spot_venue} 提现 ∩ {self._secondary_spot_venue} 入金）...",
        )

        if self._dry_run:
            self._result_queue.put_nowait((
                "chain_info",
                {
                    "base": base, "ok": True, "chain": "TRX",
                    "binance_withdraw_fee_coin": 0.0, "kraken_address": "DRYRUN", "kraken_tag": "",
                },
            ))
            return

        self.run_in_executor(self._fetch_chain_info_blocking, args=(base,))

    def _fetch_chain_info_blocking(self, base: str) -> None:
        try:
            base_chains = self._wallets[self._main_spot_venue].fetch_withdraw_chains(base)
            enabled_chains = {c for c, d in base_chains.items() if d["enabled"]}

            deposit_methods = self._wallets[self._secondary_spot_venue].fetch_deposit_methods(base)
            common = enabled_chains & deposit_methods.keys()
            if not common:
                result = {
                    "base": base, "ok": False,
                    "error": f"无共同支持的链（{self._main_spot_venue} 提现 ∩ {self._secondary_spot_venue} 入金）",
                }
            else:
                chain = min(common, key=lambda c: base_chains[c]["fee"])
                addr = self._wallets[self._secondary_spot_venue].fetch_deposit_address(base, deposit_methods[chain])
                result = {
                    "base": base, "ok": True, "chain": chain,
                    "binance_withdraw_fee_coin": base_chains[chain]["fee"],
                    "kraken_address": addr["address"], "kraken_tag": addr["tag"],
                }
        except Exception as exc:  # noqa: BLE001 - 单个 base 拉取失败不应中断整体流程
            result = {"base": base, "ok": False, "error": repr(exc)}
        self._result_queue.put_nowait(("chain_info", result))

    def _handle_chain_info_result(self, payload: dict) -> None:
        base = payload["base"]
        self._chain_lookup_inflight.discard(base)
        self._chain_info[base] = payload
        if payload.get("ok"):
            chain = payload.get("chain")
            fee_coin = payload.get("binance_withdraw_fee_coin", 0)
            address = payload.get("kraken_address", "N/A")
            tag = payload.get("kraken_tag", "")
            self.log.info(
                f"[exec] {base} 链信息就绪:\n"
                f"  链: {chain}\n"
                f"  提现手续费: {fee_coin} {base}\n"
                f"  {self._secondary_spot_venue} 充值地址: {address}\n"
                f"  {self._secondary_spot_venue} 充值tag: {tag if tag else '(无需tag)'}"
            )
        else:
            self.log.warning(f"[exec] {base} 链信息拉取失败: {payload.get('error')}，暂不建仓")

    # -------------------------------------------------------------- 异步 drain --

    def _on_drain_timer(self, _event) -> None:
        while True:
            try:
                kind, payload = self._result_queue.get_nowait()
            except queue.Empty:
                break

            try:
                if kind == "chain_info":
                    self._handle_chain_info_result(payload)
                elif kind == "withdraw_submitted":
                    self._handle_withdraw_submitted(payload)
                elif kind == "deposit_status":
                    self._handle_deposit_status(payload)
            except Exception as exc:  # noqa: BLE001 - drain 回调本身绝不能因单条数据异常而卡死
                self.log.error(f"[exec] 处理异步结果 {kind} 失败: {exc!r}")
