import os

from spread_monitor.guardrails import check_global_notional_cap
from spread_monitor.guardrails import check_max_active_bases
from spread_monitor.guardrails import check_max_concurrent_builds
from spread_monitor.guardrails import check_withdrawal_economics
from spread_monitor.guardrails import is_paused


def test_is_paused_false_when_file_missing(tmp_path):
    flag = tmp_path / "ARB_PAUSED"
    assert not is_paused(str(flag))


def test_is_paused_true_when_file_exists(tmp_path):
    flag = tmp_path / "ARB_PAUSED"
    flag.write_text("paused")
    assert is_paused(str(flag))


def test_is_paused_false_for_empty_path():
    assert not is_paused("")


def test_check_global_notional_cap_within_limit():
    ok, reason = check_global_notional_cap(committed_usdt=1000.0, add_usdt=500.0, cap_usdt=2000.0)
    assert ok
    assert reason == ""


def test_check_global_notional_cap_exceeds_limit():
    ok, reason = check_global_notional_cap(committed_usdt=1800.0, add_usdt=500.0, cap_usdt=2000.0)
    assert not ok
    assert reason


def test_check_max_concurrent_builds_at_limit_blocks():
    ok, _ = check_max_concurrent_builds(in_progress_builds=2, max_concurrent_builds=2)
    assert not ok


def test_check_max_concurrent_builds_below_limit_allows():
    ok, _ = check_max_concurrent_builds(in_progress_builds=1, max_concurrent_builds=2)
    assert ok


def test_check_max_active_bases_at_limit_blocks():
    ok, _ = check_max_active_bases(active_bases=8, max_active_bases=8)
    assert not ok


def test_check_max_active_bases_below_limit_allows():
    ok, _ = check_max_active_bases(active_bases=7, max_active_bases=8)
    assert ok


def test_check_withdrawal_economics_covers_fee_with_safety_multiple():
    # 500 USDT 建仓，触发价差 0.3% -> 预期收益 1.5 USDT；手续费 0.4 USDT * 3 = 1.2 USDT，够
    ok, reason = check_withdrawal_economics(
        build_notional_usdt=500.0,
        expected_edge_pct=0.3,
        withdraw_fee_usdt=0.4,
        safety_multiple=3.0,
    )
    assert ok
    assert reason == ""


def test_check_withdrawal_economics_fails_when_edge_too_thin():
    # 预期收益 0.5 USDT (500*0.1%) < 0.4*3=1.2 USDT，不划算
    ok, reason = check_withdrawal_economics(
        build_notional_usdt=500.0,
        expected_edge_pct=0.1,
        withdraw_fee_usdt=0.4,
        safety_multiple=3.0,
    )
    assert not ok
    assert reason


def test_check_withdrawal_economics_just_above_multiple_passes():
    # 预期收益略高于所需值（避免浮点边界相等导致的测试脆弱性）应放行
    ok, _ = check_withdrawal_economics(
        build_notional_usdt=1000.0,
        expected_edge_pct=0.13,  # 1000*0.13% = 1.3
        withdraw_fee_usdt=0.4,
        safety_multiple=3.0,   # 0.4*3 = 1.2
    )
    assert ok
