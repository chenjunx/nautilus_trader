from spread_monitor.pricing import best_pair_arb
from spread_monitor.pricing import net_edge


def test_net_edge_basic():
    # bid=101, ask=100, no fees/slippage -> net = 101 - 100 = 1
    assert net_edge(bid=101.0, ask=100.0, fee_buy=0.0, fee_sell=0.0, slippage=0.0) == 1.0


def test_net_edge_fees_and_slippage_reduce_edge():
    edge_no_cost = net_edge(bid=101.0, ask=100.0, fee_buy=0.0, fee_sell=0.0, slippage=0.0)
    edge_with_cost = net_edge(bid=101.0, ask=100.0, fee_buy=0.001, fee_sell=0.001, slippage=0.0005)
    assert edge_with_cost < edge_no_cost


def test_best_pair_arb_picks_cheapest_buy_and_priciest_sell():
    venue_data = {
        "A": (100.0, 100.1),  # bid, ask
        "B": (101.0, 101.2),
        "C": (99.5, 99.6),
    }
    result = best_pair_arb(venue_data, fee_of=lambda v: 0.0, slippage=0.0)
    assert result is not None
    gross_pct, net_pct, buy_v, ask, fee_b, sell_v, bid, fee_s = result
    # 应该在 C 买（最低 ask）、在 B 卖（最高 bid）
    assert buy_v == "C"
    assert sell_v == "B"


def test_best_pair_arb_excludes_pairs():
    venue_data = {
        "SEC1": (100.0, 100.1),
        "SEC2": (110.0, 110.1),
    }
    excluded = frozenset({frozenset({"SEC1", "SEC2"})})
    result = best_pair_arb(venue_data, fee_of=lambda v: 0.0, slippage=0.0, excluded_pairs=excluded)
    assert result is None


def test_best_pair_arb_empty_venue_data_returns_none():
    assert best_pair_arb({}, fee_of=lambda v: 0.0, slippage=0.0) is None


def test_best_pair_arb_uses_per_venue_fee():
    # 两所报价相同，但 EXPENSIVE_FEE 手续费高得多——net_pct 必须体现出手续费差异，
    # 而不是像 gross_pct 一样对两个方向无差别。
    venue_data = {
        "CHEAP_FEE": (100.0, 100.0),
        "EXPENSIVE_FEE": (100.0, 100.0),
    }
    high_fee_result = best_pair_arb(
        venue_data, fee_of=lambda v: 0.05 if v == "EXPENSIVE_FEE" else 0.0001, slippage=0.0,
    )
    low_fee_result = best_pair_arb(venue_data, fee_of=lambda v: 0.0001, slippage=0.0)
    assert high_fee_result is not None and low_fee_result is not None
    assert high_fee_result[1] < low_fee_result[1]  # net_pct 更低
