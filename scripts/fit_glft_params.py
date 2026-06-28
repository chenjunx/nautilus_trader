#!/Users/xiachenjun/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3
"""
Fit GLFT (Guéant-Lehalle-Fernandez-Tapia) market-order arrival rate parameters
from NautilusTrader parquet market data.

Model: λ(δ) = A · exp(−k · δ)
where δ is the distance of the trade from mid-price.

Log-linearised OLS: ln(λ) = ln(A) − k·δ
"""

import glob
import math
import os
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FIXED_SCALAR = 10_000_000_000_000_000.0  # 1e16

# Action codes
ACTION_ADD = 1
ACTION_UPDATE = 2
ACTION_DELETE = 3
ACTION_CLEAR = 4

# Side codes
SIDE_BUY = 1   # bid
SIDE_SELL = 2  # ask

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def decode_val(b: bytes) -> float:
    """Decode a fixed_size_binary[16] little-endian signed int to float."""
    return int.from_bytes(b, "little", signed=True) / FIXED_SCALAR


def load_instrument_tick_size(catalog_root: str) -> float:
    """Read price_increment from the crypto_perpetual instrument parquet."""
    files = sorted(glob.glob(os.path.join(catalog_root, "crypto_perpetual", "*", "*.parquet")))
    if not files:
        raise FileNotFoundError(f"No crypto_perpetual parquet found under {catalog_root}")
    tbl = pq.read_table(files[0], columns=["price_increment"])
    val = tbl.column("price_increment")[0].as_py()
    # val is a string like "0.01"
    return float(val)


# ---------------------------------------------------------------------------
# Per-symbol processing
# ---------------------------------------------------------------------------

def process_symbol(sym_dir: str, catalog_root: str, tick_size: float) -> dict | None:
    """
    Load all trade_tick and order_book_deltas parquet files for one symbol,
    replay in time order, and collect (δ) distances.

    Returns a dict with fitting inputs, or None if insufficient data.
    """
    tt_pattern = os.path.join(catalog_root, "trade_tick", "*", "*.parquet")
    ob_pattern = os.path.join(catalog_root, "order_book_deltas", "*", "*.parquet")

    tt_files = sorted(glob.glob(tt_pattern))
    ob_files = sorted(glob.glob(ob_pattern))

    if not tt_files or not ob_files:
        print(f"  [SKIP] No data files found for {sym_dir}")
        return None

    print(f"  Loading {len(tt_files)} trade_tick files and {len(ob_files)} OB-delta files …", flush=True)

    # ------------------------------------------------------------------
    # Build a sorted list of (ts_event, kind, row_data) events.
    # kind: 0 = OB delta, 1 = trade tick  (process OB before trade at same ts)
    # We store raw bytes for price/size to defer decoding.
    # ------------------------------------------------------------------

    # We'll build two arrays: one for OB deltas, one for trades,
    # then merge-sort by ts_event (OB before trade on ties).

    # OB delta columns needed: action, side, price, size, ts_event
    ob_events = []  # list of (ts_event, action, side, price_bytes, size_bytes)

    for fpath in ob_files:
        tbl = pq.read_table(fpath, columns=["action", "side", "price", "size", "ts_event"])
        actions = tbl.column("action").to_pylist()
        sides = tbl.column("side").to_pylist()
        prices = tbl.column("price").to_pylist()
        sizes = tbl.column("size").to_pylist()
        ts_events = tbl.column("ts_event").to_pylist()
        for i in range(len(actions)):
            ob_events.append((ts_events[i], actions[i], sides[i], prices[i], sizes[i]))

    # Trade tick columns needed: price, ts_event
    trade_events = []  # list of (ts_event, price_bytes)

    for fpath in tt_files:
        tbl = pq.read_table(fpath, columns=["price", "ts_event"])
        prices = tbl.column("price").to_pylist()
        ts_events = tbl.column("ts_event").to_pylist()
        for i in range(len(prices)):
            trade_events.append((ts_events[i], prices[i]))

    print(f"  Loaded {len(ob_events):,} OB events and {len(trade_events):,} trade events. Sorting …", flush=True)

    # Sort OB events by ts_event
    ob_events.sort(key=lambda x: x[0])
    # Sort trade events by ts_event
    trade_events.sort(key=lambda x: x[0])

    # ------------------------------------------------------------------
    # Merge-replay: maintain L2 book, record δ at each trade
    # ------------------------------------------------------------------
    bids: dict[float, float] = {}  # price -> size
    asks: dict[float, float] = {}  # price -> size

    deltas = []  # list of float δ values
    all_ts = []  # ts_event for each trade used (for span calc)

    ob_idx = 0
    ob_len = len(ob_events)

    max_delta_frac = 0.10  # discard δ > 10% of mid

    for trade_ts, trade_price_bytes in trade_events:
        # Advance OB through all events with ts <= trade_ts
        while ob_idx < ob_len and ob_events[ob_idx][0] <= trade_ts:
            _, action, side, price_bytes, size_bytes = ob_events[ob_idx]
            ob_idx += 1

            if action == ACTION_CLEAR:
                if side == SIDE_BUY:
                    bids.clear()
                elif side == SIDE_SELL:
                    asks.clear()
                else:
                    # side=0 means clear all
                    bids.clear()
                    asks.clear()
                continue

            price = decode_val(price_bytes)
            if price == 0.0:
                continue

            if action == ACTION_ADD or action == ACTION_UPDATE:
                size = decode_val(size_bytes)
                if side == SIDE_BUY:
                    bids[price] = size
                elif side == SIDE_SELL:
                    asks[price] = size
            elif action == ACTION_DELETE:
                if side == SIDE_BUY:
                    bids.pop(price, None)
                elif side == SIDE_SELL:
                    asks.pop(price, None)

        # Query best bid/ask
        if not bids or not asks:
            continue

        best_bid = max(bids)
        best_ask = min(asks)

        if best_bid >= best_ask:
            # Crossed book – data glitch, skip
            continue

        mid = (best_bid + best_ask) / 2.0
        trade_price = decode_val(trade_price_bytes)
        delta = abs(trade_price - mid)

        if delta == 0.0:
            continue  # trade exactly at mid
        if mid > 0 and delta > max_delta_frac * mid:
            continue  # outlier

        deltas.append(delta)
        all_ts.append(trade_ts)

    n_trades = len(deltas)
    print(f"  Usable trades with valid δ: {n_trades:,}", flush=True)

    if n_trades < 100:
        print(f"  [SKIP] Fewer than 100 usable trades.")
        return None

    # ------------------------------------------------------------------
    # Time span in seconds
    # ------------------------------------------------------------------
    min_ts = min(all_ts)
    max_ts = max(all_ts)
    T = (max_ts - min_ts) / 1e9  # nanoseconds → seconds

    if T <= 0:
        print(f"  [SKIP] Zero time span.")
        return None

    return {
        "deltas": deltas,
        "T": T,
        "tick_size": tick_size,
        "n_trades": n_trades,
    }


def fit_glft(data: dict, symbol: str) -> dict:
    """
    Bin δ values, compute λ per bin, fit OLS ln(λ) = ln(A) − k·δ.
    Returns dict with A, k, R2, bin_count, n_trades, T.
    """
    deltas = np.array(data["deltas"], dtype=np.float64)
    T = data["T"]
    tick = data["tick_size"]
    bin_width = tick

    # Create bins from 0 to 20 ticks
    n_bins = 20
    edges = np.arange(0, n_bins + 1) * bin_width  # [0, tick, 2*tick, ..., 20*tick]
    centres = edges[:-1] + bin_width / 2.0         # bin centres

    # Count trades per bin
    counts, _ = np.histogram(deltas, bins=edges)

    # λ_i = N_i / (T * bin_width)
    lambda_vals = counts / (T * bin_width)

    # Filter bins with N_i >= 5
    mask = counts >= 5
    d_vals = centres[mask]
    lam_vals = lambda_vals[mask]

    bin_count = mask.sum()

    if bin_count < 2:
        return {"error": "Not enough bins with N >= 5 after filtering"}

    ln_lam = np.log(lam_vals)

    # OLS: ln(λ) = β0 + β1·δ  →  β1 = −k, β0 = ln(A)
    coeffs = np.polyfit(d_vals, ln_lam, 1)
    slope = coeffs[0]   # -k
    intercept = coeffs[1]  # ln(A)

    k = -slope
    A = math.exp(intercept)

    # R² of the log fit
    ln_lam_fit = np.polyval(coeffs, d_vals)
    ss_res = np.sum((ln_lam - ln_lam_fit) ** 2)
    ss_tot = np.sum((ln_lam - ln_lam.mean()) ** 2)
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")

    return {
        "A": A,
        "k": k,
        "R2": r2,
        "bin_count": bin_count,
        "n_trades": data["n_trades"],
        "T_hours": T / 3600.0,
        "tick_size": tick,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

SYMBOLS = [
    {
        "name": "ETHUSDC-PERP.BINANCE",
        "dir": "ethusdc",
    },
    {
        "name": "SOLUSDC-PERP.BINANCE",
        "dir": "solusdc",
    },
    {
        "name": "NEARUSDC-PERP.BINANCE",
        "dir": "nearusdc",
    },
]


def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_root = os.path.join(project_root, "data")

    results = []

    for sym_info in SYMBOLS:
        name = sym_info["name"]
        sym_dir = sym_info["dir"]
        catalog_root = os.path.join(data_root, sym_dir, "catalog", "data")

        print(f"\n{'='*60}")
        print(f"Processing {name}")
        print(f"{'='*60}")

        # Get tick size from instrument definition
        try:
            tick_size = load_instrument_tick_size(catalog_root)
            print(f"  Tick size: {tick_size}")
        except FileNotFoundError as e:
            print(f"  [SKIP] {e}")
            continue

        # Load and replay events
        try:
            data = process_symbol(sym_dir, catalog_root, tick_size)
        except Exception as e:
            print(f"  [ERROR] {e}")
            continue

        if data is None:
            continue

        # Fit GLFT
        fit = fit_glft(data, name)
        if "error" in fit:
            print(f"  [ERROR] {fit['error']}")
            continue

        fit["symbol"] = name
        results.append(fit)

    # ------------------------------------------------------------------
    # Print results table
    # ------------------------------------------------------------------
    if not results:
        print("\nNo symbols could be fitted.")
        sys.exit(1)

    print("\n")
    print("=" * 80)
    print("GLFT Parameter Fit Results")
    print("=" * 80)

    header = (
        f"{'Symbol':<26} {'A':>12} {'k':>10} {'R²':>8} "
        f"{'Bins':>6} {'Trades':>9} {'Span(h)':>10} {'Tick':>8}"
    )
    print(header)
    print("-" * 80)

    for r in results:
        row = (
            f"{r['symbol']:<26} "
            f"{r['A']:>12.4f} "
            f"{r['k']:>10.4f} "
            f"{r['R2']:>8.4f} "
            f"{r['bin_count']:>6d} "
            f"{r['n_trades']:>9,d} "
            f"{r['T_hours']:>10.3f} "
            f"{r['tick_size']:>8g}"
        )
        print(row)

    print("=" * 80)
    print()
    print("Interpretation:")
    print("  A  = base arrival rate (trades/sec/price-unit) at δ=0 extrapolation")
    print("  k  = exponential decay rate of arrivals with distance from mid")
    print("  R² = goodness-of-fit of log-linear regression on binned λ(δ)")
    print()


if __name__ == "__main__":
    main()
