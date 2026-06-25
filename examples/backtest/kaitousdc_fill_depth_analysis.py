# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

"""
Compute fill depth (delta = |trade_price - mid|) for KAITOUSDC-PERP.BINANCE.

Reads parquet files directly with pyarrow/pandas — no nautilus runtime needed.

Nautilus stores price/size as little-endian int128 in FixedSizeBinary(16),
with a fixed scalar of 1e15 (only the lower 64 bits are non-zero in practice).

Usage
-----
    python examples/backtest/kaitousdc_fill_depth_analysis.py \\
        --data nautilus_trader/data/kaitousdc.tar.gz

    # or with an already-extracted catalog directory:
    python examples/backtest/kaitousdc_fill_depth_analysis.py \\
        --data C:/tmp/kaitousdc/kaitousdc/catalog
"""

import argparse
import struct
import tarfile
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


PRICE_SCALAR = 1e16  # nautilus internal fixed-point scalar


def _decode_fixed_binary(series):
    """Decode nautilus FixedSizeBinary(16) little-endian i128 to float."""
    return series.apply(lambda b: struct.unpack("<q", b[:8])[0] / PRICE_SCALAR)


def _read_parquet_dir(dirpath):
    files = sorted(dirpath.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    tables = [pq.read_table(f) for f in files]
    return pa.concat_tables(tables).to_pandas()


def compute_fill_depths(catalog_path):
    delta_dir = catalog_path / "data" / "order_book_deltas" / "KAITOUSDC-PERP.BINANCE"
    trade_dir = catalog_path / "data" / "trade_tick" / "KAITOUSDC-PERP.BINANCE"

    print("  Reading order_book_deltas ...")
    deltas = _read_parquet_dir(delta_dir)
    print("    %d rows" % len(deltas))

    print("  Reading trade_ticks ...")
    trades = _read_parquet_dir(trade_dir)
    print("    %d rows" % len(trades))

    deltas["price_f"] = _decode_fixed_binary(deltas["price"])
    deltas["size_f"] = _decode_fixed_binary(deltas["size"])
    trades["price_f"] = _decode_fixed_binary(trades["price"])

    deltas = deltas.sort_values("ts_event").reset_index(drop=True)
    trades = trades.sort_values("ts_event").reset_index(drop=True)

    # action: 1=ADD, 2=UPDATE, 3=DELETE, 4=CLEAR
    # side:   1=BUY(bid), 2=SELL(ask)
    CLEAR = 4

    book_bids = {}
    book_asks = {}
    best_bid = None
    best_ask = None

    records = []
    di = 0
    ti = 0
    n_d = len(deltas)
    n_t = len(trades)

    while di < n_d or ti < n_t:
        take_delta = (ti >= n_t) or (
            di < n_d and deltas["ts_event"].iloc[di] <= trades["ts_event"].iloc[ti]
        )

        if take_delta:
            row = deltas.iloc[di]
            di += 1
            action = int(row["action"])
            side = int(row["side"])
            p = row["price_f"]
            s = row["size_f"]

            if action == CLEAR:
                book_bids.clear()
                book_asks.clear()
                best_bid = None
                best_ask = None
                continue

            book = book_bids if side == 1 else book_asks

            if action in (1, 2):  # ADD or UPDATE
                if s > 0:
                    book[p] = s
                else:
                    book.pop(p, None)
            else:  # DELETE
                book.pop(p, None)

            best_bid = max(book_bids) if book_bids else None
            best_ask = min(book_asks) if book_asks else None
        else:
            row = trades.iloc[ti]
            ti += 1
            if best_bid is None or best_ask is None:
                continue
            mid = (best_bid + best_ask) / 2.0
            price = row["price_f"]
            delta = abs(price - mid)
            side_str = "BUY" if int(row["aggressor_side"]) == 1 else "SELL"
            records.append({
                "ts_event": int(row["ts_event"]),
                "price": price,
                "aggressor_side": side_str,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "mid": mid,
                "delta": delta,
            })

    return pd.DataFrame(records)


def _print_summary(df):
    ts_min = pd.to_datetime(df["ts_event"].min(), unit="ns")
    ts_max = pd.to_datetime(df["ts_event"].max(), unit="ns")
    print("\nFill depth records : %d" % len(df))
    print("Time range         : %s  ->  %s" % (ts_min, ts_max))

    print("\n--- delta = |price - mid| overall ---")
    print(df["delta"].describe(percentiles=[0.25, 0.5, 0.75, 0.9, 0.95, 0.99]).to_string())

    print("\n--- delta by aggressor_side ---")
    print(
        df.groupby("aggressor_side")["delta"]
        .describe(percentiles=[0.5, 0.9, 0.99])
        .to_string()
    )

    _fit_k(df)
    _print_fill_probability(df)


def _fit_k(df, tick_size=0.0001, n_ticks=30):
    """
    Fit the Avellaneda-Stoikov order-arrival depth parameter k.

    Model: lambda(delta) = A * exp(-k * delta)
    => log(N(delta)) = log(T_obs * A / k) - k * delta

    Two estimators:
      1. OLS on log(survival_count) vs delta (one point per tick).
      2. MLE assuming exponential distribution: k = 1 / mean(delta).
    """
    t_obs_sec = (df["ts_event"].max() - df["ts_event"].min()) / 1e9

    # --- OLS estimator ---
    thresholds = np.arange(1, n_ticks + 1) * tick_size
    counts = np.array([(df["delta"] >= t - 1e-9).sum() for t in thresholds], dtype=float)
    mask = counts > 0
    if mask.sum() >= 2:
        x = thresholds[mask]
        y = np.log(counts[mask])
        slope, intercept = np.polyfit(x, y, 1)
        k_ols = -slope
        A_ols = np.exp(intercept) * k_ols / t_obs_sec
    else:
        k_ols = A_ols = float("nan")

    # --- MLE estimator (exponential distribution) ---
    deltas = df["delta"].values
    deltas = deltas[deltas > 0]
    k_mle = 1.0 / deltas.mean() if len(deltas) else float("nan")

    # --- by side ---
    buy = df[df["aggressor_side"] == "BUY"]["delta"].values
    sell = df[df["aggressor_side"] == "SELL"]["delta"].values
    buy = buy[buy > 0]
    sell = sell[sell > 0]
    k_mle_buy = 1.0 / buy.mean() if len(buy) else float("nan")
    k_mle_sell = 1.0 / sell.mean() if len(sell) else float("nan")

    print("\n--- Avellaneda-Stoikov k fit ---")
    print("  OLS (log-survival vs delta, %d ticks):  k = %.2f,  A = %.6f trades/sec" % (n_ticks, k_ols, A_ols))
    print("  MLE exponential (1/mean_delta):          k = %.2f" % k_mle)
    print("  MLE BUY side:                            k = %.2f" % k_mle_buy)
    print("  MLE SELL side:                           k = %.2f" % k_mle_sell)
    return {"k_ols": k_ols, "A_ols": A_ols, "k_mle": k_mle, "k_mle_buy": k_mle_buy, "k_mle_sell": k_mle_sell}


def _print_fill_probability(df, tick_size=0.0001, group_ticks=2, n_groups=10):
    """
    Arrival intensity: lambda(delta) = n(delta >= threshold) / T_obs (trades/sec).
    Groups are: [1,2], [3,4], ..., [2*n_groups-1, 2*n_groups] ticks.
    """
    t_obs_sec = (df["ts_event"].max() - df["ts_event"].min()) / 1e9
    print("\nT_obs = %.1f sec (%.2f hours)" % (t_obs_sec, t_obs_sec / 3600))

    buy = df[df["aggressor_side"] == "BUY"]
    sell = df[df["aggressor_side"] == "SELL"]

    print("\n--- arrival intensity lambda(delta) by depth (survival, %d-tick groups) ---" % group_ticks)
    print("%-20s  %8s  %10s  %10s  %10s" % ("depth_range (ticks)", "count>=", "lambda", "lam_BUY", "lam_SELL"))

    for g in range(n_groups):
        lo_tick = g * group_ticks + 1
        hi_tick = (g + 1) * group_ticks
        threshold = lo_tick * tick_size
        label = "%d-%d" % (lo_tick, hi_tick)

        n = int((df["delta"] >= threshold - 1e-9).sum())
        n_buy = int((buy["delta"] >= threshold - 1e-9).sum())
        n_sell = int((sell["delta"] >= threshold - 1e-9).sum())

        lam = n / t_obs_sec
        lam_buy = n_buy / t_obs_sec
        lam_sell = n_sell / t_obs_sec

        print("%-20s  %8d  %10.6f  %10.6f  %10.6f" % (label, n, lam, lam_buy, lam_sell))


def _run(catalog_path, output):
    print("Catalog: %s" % catalog_path)
    df = compute_fill_depths(catalog_path)

    if df.empty:
        print("No records produced -- check catalog path.")
        return

    _print_summary(df)

    out = Path(output)
    df.to_csv(str(out), index=False)
    print("\nSaved -> %s" % out.resolve())


def main():
    parser = argparse.ArgumentParser(description="KAITOUSDC fill-depth analysis")
    parser.add_argument(
        "--data",
        required=True,
        help="Path to kaitousdc.tar.gz or an already-extracted catalog directory",
    )
    parser.add_argument(
        "--output",
        default="kaitousdc_fill_depths.csv",
        help="Output CSV path (default: kaitousdc_fill_depths.csv)",
    )
    args = parser.parse_args()

    data_path = Path(args.data)

    if data_path.is_file() and tarfile.is_tarfile(str(data_path)):
        with tempfile.TemporaryDirectory() as tmpdir:
            print("Extracting %s ..." % data_path)
            with tarfile.open(str(data_path)) as tf:
                tf.extractall(tmpdir)
            catalog_path = Path(tmpdir) / "kaitousdc" / "catalog"
            _run(catalog_path, args.output)
    else:
        _run(data_path, args.output)


if __name__ == "__main__":
    main()
