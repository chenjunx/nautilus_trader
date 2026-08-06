"""建仓/套利下单前置风控检查的纯函数，不依赖 nautilus_trader，方便单测。

除 `is_paused` 外均返回 `(ok, reason)`：`ok=True` 时允许继续，`False` 时 `reason`
是可直接打日志的中文说明。
"""

import os


def is_paused(pause_flag_path: str) -> bool:
    """`pause_flag_path` 指向的文件存在即暂停——只阻止新的建仓/套利下单，
    进行中的序列（BUILDING_PERP/TRANSFERRING 等）必须跑完，不受此开关影响。
    """
    return bool(pause_flag_path) and os.path.exists(pause_flag_path)


def check_global_notional_cap(committed_usdt: float, add_usdt: float, cap_usdt: float) -> tuple[bool, str]:
    total = committed_usdt + add_usdt
    if total > cap_usdt:
        return False, (
            f"总占用名义金额超限: 已占用={committed_usdt:.2f} + 新增={add_usdt:.2f} "
            f"> 上限={cap_usdt:.2f}"
        )
    return True, ""


def check_max_concurrent_builds(in_progress_builds: int, max_concurrent_builds: int) -> tuple[bool, str]:
    if in_progress_builds >= max_concurrent_builds:
        return False, f"建仓并发数已达上限: {in_progress_builds} >= {max_concurrent_builds}"
    return True, ""


def check_max_active_bases(active_bases: int, max_active_bases: int) -> tuple[bool, str]:
    if active_bases >= max_active_bases:
        return False, f"活跃 base 数已达上限: {active_bases} >= {max_active_bases}"
    return True, ""


def check_withdrawal_economics(
    build_notional_usdt: float,
    expected_edge_pct: float,
    withdraw_fee_usdt: float,
    safety_multiple: float = 3.0,
) -> tuple[bool, str]:
    """建仓前用触发价差换算的预期收益必须覆盖提现手续费的 `safety_multiple` 倍，
    否则这笔建仓"不值当"（提现手续费会吃掉大部分甚至全部套利收益）。
    """
    expected_profit_usdt = build_notional_usdt * expected_edge_pct / 100
    required_usdt = withdraw_fee_usdt * safety_multiple
    if expected_profit_usdt < required_usdt:
        return False, (
            f"提现经济性不足: 预期收益={expected_profit_usdt:.4f} USDT "
            f"< 提现手续费×{safety_multiple:g}={required_usdt:.4f} USDT"
        )
    return True, ""
