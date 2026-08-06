from collections.abc import Callable


def net_edge(bid: float, ask: float, fee_buy: float, fee_sell: float) -> float:
    """单边费用模型下的净价差（绝对值，非百分比）。

    净价差 = 卖出收益 - 买入成本
           = bid × (1 - fee_sell) - ask × (1 + fee_buy)
    """
    return bid * (1 - fee_sell) - ask * (1 + fee_buy)


def best_pair_arb(
    venue_data: dict[str, tuple[float, float]],
    fee_of: Callable[[str], float],
    excluded_pairs: frozenset[frozenset[str]] = frozenset(),
) -> tuple | None:
    """在给定的 {venue: (bid, ask)} 数据里找净价差最优的 (buy_venue, sell_venue) 配对。

    `excluded_pairs` 是一组 frozenset({venue_a, venue_b})，命中的配对（不区分买卖方向）
    直接跳过——用于排除两侧都不能开仓对冲的副所↔副所组合。

    返回 (gross_pct, net_pct, buy_v, ask, fee_b, sell_v, bid, fee_s)，找不到任何可配对
    组合（不足 2 个 venue）或 mid 价为 0 时返回 None。
    """
    if not venue_data:
        return None

    mid = sum((b + a) / 2 for b, a in venue_data.values()) / len(venue_data)
    if mid == 0:
        return None

    all_venues = list(venue_data.keys())
    best_net = float("-inf")
    best: tuple | None = None

    for buy_v in all_venues:
        for sell_v in all_venues:
            if buy_v == sell_v:
                continue
            if frozenset((buy_v, sell_v)) in excluded_pairs:
                continue

            ask = venue_data[buy_v][1]
            bid = venue_data[sell_v][0]
            fee_b = fee_of(buy_v)
            fee_s = fee_of(sell_v)

            net = net_edge(bid, ask, fee_b, fee_s)
            if net > best_net:
                best_net = net
                gross_pct = (bid - ask) / mid * 100
                net_pct = net / mid * 100
                best = (gross_pct, net_pct, buy_v, ask, fee_b, sell_v, bid, fee_s)

    return best
