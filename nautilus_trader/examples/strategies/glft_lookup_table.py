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
Parameterized, importable port of the repo's offline G* imbalance lookup-table
pipeline (``state_encoding.py`` / ``imbalance_analysis.py`` / ``transition_matrix.py``
at the repo root), for use by ``GLFTMarketMaker``'s daily in-process recompute
job. See those scripts' docstrings for the statistical method itself; this
module changes three things to make the pipeline embeddable:

1. Catalog path / instrument / tick size are parameters, not module constants.
2. The BBO tick-series cache is windowed and maintained incrementally
   (``maintain_rolling_bbo_cache``) instead of being an all-or-nothing cache
   over full history — a live strategy calling this once a day needs only
   replay the ~1 day of order_book_deltas files written since the previous
   run, not the whole catalog.
3. ``compute_lookup_table`` returns a plain dict (no stdout/disk side effects
   beyond the BBO cache file) so a background thread can call it directly.
"""
import glob
import json
import os

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


N_I_BUCKETS = 10
N_S_BUCKETS = 2
N_STATES = N_I_BUCKETS * N_S_BUCKETS

BREAK_GAP_NS = 5 * 60 * 1_000_000_000  # 5 minutes
CIRCUIT_BREAKER_HALF_TICKS = 40  # = 20 full ticks
ABSORB_COL = N_STATES  # column index 20: "escaped the model" (spread >= 3 ticks)
I_PAIRS = [(0, 9), (1, 8), (2, 7), (3, 6), (4, 5)]
UNRELIABLE_THRESHOLD = 1000
FIXED_SCALE = 1e16


def encode_state(i_bucket, s_bucket):
    return i_bucket * N_S_BUCKETS + s_bucket


def decode_state(state):
    return divmod(state, N_S_BUCKETS)


def compute_live_state(
    bid_qty: float,
    ask_qty: float,
    bid_px: float,
    ask_px: float,
    tick_size: float,
) -> tuple[int, int, int]:
    """
    Scalar version of ``prepare_columns``'s bucketing, for live per-tick use.

    Must stay bit-for-bit consistent with the vectorized bucketing in
    ``prepare_columns`` (same ``I``/``I_bucket``/``s_bucket`` formulas) since
    both feed the same ``table_I_by_S`` lookup table.
    """
    i = bid_qty / (bid_qty + ask_qty)
    i_bucket = min(9, max(0, int(i * 10)))
    spread_ticks = round((ask_px - bid_px) / tick_size)
    s_bucket = 0 if spread_ticks <= 1 else 1
    return i_bucket, s_bucket, encode_state(i_bucket, s_bucket)


def decode_fixed16(arr, signed):
    """Vectorized decode of Nautilus's 128-bit little-endian fixed-point column."""
    np_arr = arr.combine_chunks() if hasattr(arr, "combine_chunks") else arr
    buf = np_arr.buffers()[1]
    raw = np.frombuffer(buf, dtype=np.uint8)
    n = len(np_arr)
    raw = raw[: n * 16].reshape(n, 16)
    lo = raw[:, :8].copy().view("<u8").reshape(-1).astype(np.float64)
    hi = raw[:, 8:].copy().view("<i8" if signed else "<u8").reshape(-1).astype(np.float64)
    return (hi * (2.0**64) + lo) / FIXED_SCALE


def _parse_catalog_filename_start_ns(path: str) -> int | None:
    """
    Parse the leading interval-start timestamp (unix ns) out of a
    ParquetDataCatalog delta filename.

    Catalog filenames encode the covered interval as
    ``<start_iso>_<end_iso>.parquet``, with ISO8601 timestamps where ``:``
    and ``.`` are replaced by ``-`` (see ``_iso_timestamp_to_file_timestamp``
    in ``nautilus_trader.persistence.catalog.parquet``), e.g.
    ``2026-06-29T08-03-24-098892969Z_2026-06-29T08-03-29-066360245Z.parquet``.
    Filenames are lexicographically sortable in this form, but callers here
    need an actual ns value to compare against a rolling-window cutoff.
    """
    name = os.path.basename(path)
    start_token = name.split("_", 1)[0]
    try:
        date_part, time_part = start_token.split("T", 1)
        hh, mm, ss, frac = time_part.rstrip("Z").split("-")
        iso = f"{date_part}T{hh}:{mm}:{ss}.{frac}Z"
        return pd.Timestamp(iso).value
    except (ValueError, IndexError):
        return None


def build_bbo_series(
    files: list[str],
    bids: dict[float, float] | None = None,
    asks: dict[float, float] | None = None,
    prev_key: tuple | None = None,
) -> tuple[pd.DataFrame, dict[float, float], dict[float, float], tuple | None]:
    """
    Replay order_book_deltas files (in filename/chronological order) and
    emit a best-bid/best-offer tick each time the top-of-book changes.

    ``bids``/``asks``/``prev_key`` let a caller resume a replay from a
    previously reconstructed book state instead of starting from an empty
    book — required for correct incremental replay, since the raw deltas are
    absolute-size ADD/UPDATE against whatever book state preceded them; an
    empty starting book fabricates an incomplete, wrong top-of-book until
    enough messages happen to touch the true top levels again. Returns the
    resulting DataFrame plus the final ``(bids, asks, prev_key)`` so the
    caller can persist it and resume next time. ``bids``/``asks`` are
    mutated in place if passed in.
    """
    bids = {} if bids is None else bids
    asks = {} if asks is None else asks
    records = []

    for f in files:
        t = pq.read_table(f, columns=["action", "side", "price", "size", "ts_event"])
        action = t.column("action").to_numpy()
        side = t.column("side").to_numpy()
        price = decode_fixed16(t.column("price"), signed=True)
        size = decode_fixed16(t.column("size"), signed=False)
        ts = t.column("ts_event").to_numpy()

        for i in range(len(action)):
            a = action[i]
            s = side[i]
            p = round(float(price[i]), 6)
            q = float(size[i])

            if a == 4:  # CLEAR
                if s in (0, 1):
                    bids.clear()
                if s in (0, 2):
                    asks.clear()
            elif a == 1:  # ADD (absolute size)
                if s == 1:
                    bids[p] = q
                elif s == 2:
                    asks[p] = q
            elif a == 2:  # UPDATE (absolute size, per Binance depth-diff semantics)
                if s == 1:
                    if q <= 0:
                        bids.pop(p, None)
                    else:
                        bids[p] = q
                elif s == 2:
                    if q <= 0:
                        asks.pop(p, None)
                    else:
                        asks[p] = q
            elif a == 3:  # DELETE
                if s == 1:
                    bids.pop(p, None)
                elif s == 2:
                    asks.pop(p, None)

            if bids and asks:
                bb = max(bids)
                ba = min(asks)
                if bb < ba:
                    qb = bids[bb]
                    qa = asks[ba]
                    key = (bb, qb, ba, qa)
                    if key != prev_key:
                        records.append((ts[i], bb, qb, ba, qa))
                        prev_key = key

    df = pd.DataFrame(records, columns=["ts", "bid_px", "bid_qty", "ask_px", "ask_qty"])
    df.drop_duplicates("ts", keep="last", inplace=True)
    df.sort_values("ts", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df, bids, asks, prev_key


def _bbo_cache_state_path(cache_path: str) -> str:
    return cache_path + ".state.json"


def _book_state_to_json(bids, asks, prev_key):
    return dict(
        bids=[[p, q] for p, q in bids.items()],
        asks=[[p, q] for p, q in asks.items()],
        prev_key=list(prev_key) if prev_key is not None else None,
    )


def _book_state_from_json(obj):
    bids = {p: q for p, q in obj["bids"]}
    asks = {p: q for p, q in obj["asks"]}
    prev_key = tuple(obj["prev_key"]) if obj["prev_key"] is not None else None
    return bids, asks, prev_key


def maintain_rolling_bbo_cache(
    cache_path: str,
    ob_glob: str,
    window: pd.Timedelta,
    now_ns: int | None = None,
) -> pd.DataFrame:
    """
    Incrementally maintain a BBO tick-series cache trimmed to a trailing
    ``window``.

    First call (no cache on disk): replays every file matched by ``ob_glob``
    whose filename-encoded start timestamp falls within ``[now - window,
    now]``, starting from an empty order book (unavoidable warm-up transient
    at the very start of history — the same one full offline recomputation
    would pay). Subsequent calls: only replay files newer than the previous
    call's newest file (tracked in a small ``<cache_path>.state.json``
    sidecar), **resuming the in-memory book from the exact bids/asks/
    prev_key state the previous call ended with** (also persisted in the
    sidecar) so the replay is equivalent to one continuous pass over all
    history — restarting from an empty book at every batch boundary would
    fabricate an incomplete top-of-book out of whatever ADD/UPDATE messages
    happen to arrive first, silently corrupting the BBO series every day.
    Ticks are appended to the existing cache, then rows older than
    ``now - window`` are dropped.
    """
    if now_ns is None:
        now_ns = pd.Timestamp.now(tz="UTC").value
    cutoff_ns = now_ns - int(window.value)

    files_with_start = [
        (f, _parse_catalog_filename_start_ns(f)) for f in sorted(glob.glob(ob_glob))
    ]
    files_with_start = [(f, s) for f, s in files_with_start if s is not None]

    state = None
    existing = None
    if os.path.exists(_bbo_cache_state_path(cache_path)) and os.path.exists(cache_path):
        try:
            with open(_bbo_cache_state_path(cache_path)) as fh:
                state = json.load(fh)
            existing = pd.read_parquet(cache_path)
        except (OSError, ValueError):
            state, existing = None, None

    if state is not None:
        last_start_ns = state["last_file_start_ns"]
        new_files = [f for f, s in files_with_start if s > last_start_ns]
        bids, asks, prev_key = _book_state_from_json(state["book"])
    else:
        new_files = [f for f, s in files_with_start if s >= cutoff_ns]
        bids, asks, prev_key = {}, {}, None

    if new_files:
        new_df, bids, asks, prev_key = build_bbo_series(
            new_files, bids=bids, asks=asks, prev_key=prev_key,
        )
    else:
        new_df = pd.DataFrame(
            {
                "ts": pd.Series(dtype="uint64"),
                "bid_px": pd.Series(dtype="float64"),
                "bid_qty": pd.Series(dtype="float64"),
                "ask_px": pd.Series(dtype="float64"),
                "ask_qty": pd.Series(dtype="float64"),
            },
        )

    if existing is not None and len(existing):
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined.drop_duplicates("ts", keep="last", inplace=True)
    combined = combined[combined["ts"] >= cutoff_ns]
    combined.sort_values("ts", inplace=True)
    combined.reset_index(drop=True, inplace=True)

    combined.to_parquet(cache_path)
    latest_start_ns = max(
        (s for _, s in files_with_start),
        default=(state or {}).get("last_file_start_ns", cutoff_ns),
    )
    with open(_bbo_cache_state_path(cache_path), "w") as fh:
        json.dump(
            {
                "last_file_start_ns": int(latest_start_ns),
                "updated_at_ns": int(now_ns),
                "book": _book_state_to_json(bids, asks, prev_key),
            },
            fh,
        )

    return combined


def prepare_columns(df: pd.DataFrame, tick_size: float) -> pd.DataFrame:
    df = df.copy()
    df["I"] = df["bid_qty"] / (df["bid_qty"] + df["ask_qty"])
    df["I_bucket"] = np.clip(np.floor(df["I"] * 10).astype(int), 0, 9)
    df["spread_ticks"] = ((df["ask_px"] - df["bid_px"]) / tick_size).round().astype(int)
    df["s_bucket"] = np.where(df["spread_ticks"] <= 1, 0, 1)
    df["state"] = encode_state(df["I_bucket"].values, df["s_bucket"].values)
    df["mht"] = np.round((df["bid_px"] + df["ask_px"]) / tick_size).astype(np.int64)
    return df


def build_transition_matrices(df: pd.DataFrame):
    ts = df["ts"].values
    spread_ticks = df["spread_ticks"].values
    state = df["state"].values
    mht = df["mht"].values
    n = len(df)

    Q = np.zeros((N_STATES, N_STATES), dtype=np.int64)
    R_count = np.zeros((N_STATES, N_STATES + 1), dtype=np.int64)
    J_sum = np.zeros((N_STATES, N_STATES + 1), dtype=np.int64)
    row_count = np.zeros(N_STATES, dtype=np.int64)
    pos_count = np.zeros(N_STATES, dtype=np.int64)
    neg_count = np.zeros(N_STATES, dtype=np.int64)
    jump_hist = np.zeros(2 * CIRCUIT_BREAKER_HALF_TICKS + 1, dtype=np.int64)
    circuit_breaker_log = []

    diag = dict(
        n_total=n, n_break_marker=0, n_circuit_breaker=0,
        n_discarded_absorb=0, n_q=0, n_r=0, n_skipped_fresh_start=0,
        n_absorb_total=0, n_same_ts_jump=0,
    )

    prev_idx = None
    for i in range(n):
        if prev_idx is None:
            if spread_ticks[i] >= 3:
                diag["n_skipped_fresh_start"] += 1
                continue
            prev_idx = i
            continue

        gap = ts[i] - ts[prev_idx]
        if gap > BREAK_GAP_NS:
            diag["n_break_marker"] += 1
            prev_idx = i if spread_ticks[i] < 3 else None
            continue

        jump = int(mht[i]) - int(mht[prev_idx])
        if abs(jump) > CIRCUIT_BREAKER_HALF_TICKS:
            diag["n_circuit_breaker"] += 1
            circuit_breaker_log.append(dict(
                i=i, ts=int(ts[i]), prev_ts=int(ts[prev_idx]),
                jump=jump, prev_state=int(state[prev_idx]),
            ))
            prev_idx = i if spread_ticks[i] < 3 else None
            continue

        jump_hist[jump + CIRCUIT_BREAKER_HALF_TICKS] += 1
        if gap == 0 and jump != 0:
            diag["n_same_ts_jump"] += 1

        col = ABSORB_COL if spread_ticks[i] >= 3 else state[i]
        p_state = state[prev_idx]

        if col == ABSORB_COL:
            diag["n_absorb_total"] += 1

        if jump == 0 and col == ABSORB_COL:
            diag["n_discarded_absorb"] += 1
            prev_idx = None
            continue

        if jump == 0:
            Q[p_state, col] += 1
            row_count[p_state] += 1
            diag["n_q"] += 1
            prev_idx = i
            continue

        R_count[p_state, col] += 1
        J_sum[p_state, col] += jump
        row_count[p_state] += 1
        if jump > 0:
            pos_count[p_state] += 1
        else:
            neg_count[p_state] += 1
        diag["n_r"] += 1
        prev_idx = None if col == ABSORB_COL else i

    extra = dict(
        pos_count=pos_count, neg_count=neg_count, jump_hist=jump_hist,
        circuit_breaker_log=circuit_breaker_log,
    )
    return Q, R_count, J_sum, row_count, diag, extra


def paired_reliability(row_count):
    reliable = np.zeros(N_STATES, dtype=bool)
    pair_count = {}
    for s in range(N_S_BUCKETS):
        for j, k in I_PAIRS:
            sj, sk = encode_state(j, s), encode_state(k, s)
            combined = int(row_count[sj] + row_count[sk])
            ok = combined >= UNRELIABLE_THRESHOLD
            reliable[sj] = ok
            reliable[sk] = ok
            pair_count[(j, k, s)] = combined
    return reliable, pair_count


def check_matrix_health(Q_prob, j_vec, g1):
    row_sums = Q_prob.sum(axis=1)
    bad_rows = np.where((row_sums < -1e-12) | (row_sums > 1 + 1e-9))[0]
    if len(bad_rows):
        raise RuntimeError(
            f"Q_prob row sums out of [0,1] at states {bad_rows.tolist()}: {row_sums[bad_rows]}",
        )

    bad_neg = np.argwhere(Q_prob < -1e-12)
    if len(bad_neg):
        raise RuntimeError(f"Q_prob has negative elements at {bad_neg.tolist()}")

    residual = np.linalg.norm((np.eye(N_STATES) - Q_prob) @ g1 - j_vec)
    if residual >= 1e-10:
        raise RuntimeError(f"solve residual too large: {residual:.3e} (want < 1e-10)")

    return dict(row_sums=row_sums, residual=residual)


def build_Q_prob(Q, row_count, reliable):
    Q_prob = np.zeros((N_STATES, N_STATES), dtype=np.float64)
    for s_idx in range(N_STATES):
        if not reliable[s_idx] or row_count[s_idx] == 0:
            continue
        Q_prob[s_idx, :] = Q[s_idx, :] / row_count[s_idx]
    return Q_prob


def build_recursive_operator(Q, R_count, row_count, reliable, Q_prob):
    R_hat = np.zeros((N_STATES, N_STATES), dtype=np.float64)
    for s_idx in range(N_STATES):
        if not reliable[s_idx] or row_count[s_idx] == 0:
            continue
        R_hat[s_idx, :] = R_count[s_idx, :N_STATES] / row_count[s_idx]

    B = np.linalg.solve(np.eye(N_STATES) - Q_prob, R_hat)
    return B, R_hat


def symmetrize(g):
    g_sym = g.copy()
    for s in range(N_S_BUCKETS):
        for j, k in I_PAIRS:
            sj, sk = encode_state(j, s), encode_state(k, s)
            diff = 0.5 * (g[sj] - g[sk])
            g_sym[sj] = diff
            g_sym[sk] = -diff
    return g_sym


def accumulate_G_star(B, g1, max_iters=20, tol=1e-3):
    g1_sym = symmetrize(g1)
    G_star = g1_sym.copy()
    decay_log = [float(np.max(np.abs(g1_sym)))]

    g_i = g1_sym
    converged = False
    n_iters = 0
    for it in range(1, max_iters + 1):
        g_next = symmetrize(B @ g_i)
        G_star = G_star + g_next
        max_abs = float(np.max(np.abs(g_next)))
        decay_log.append(max_abs)
        n_iters = it
        g_i = g_next
        if max_abs < tol:
            converged = True
            break

    return dict(
        G_star=G_star, g1_sym=g1_sym, decay_log=decay_log,
        converged=converged, n_iters=n_iters,
    )


def normalize_and_solve(Q, R_count, J_sum, row_count):
    reliable, pair_count = paired_reliability(row_count)

    Q_prob = build_Q_prob(Q, row_count, reliable)
    j_vec = np.zeros(N_STATES, dtype=np.float64)
    for s_idx in range(N_STATES):
        if not reliable[s_idx] or row_count[s_idx] == 0:
            continue
        j_vec[s_idx] = J_sum[s_idx, :].sum() / row_count[s_idx]

    g1 = np.linalg.solve(np.eye(N_STATES) - Q_prob, j_vec)
    health = check_matrix_health(Q_prob, j_vec, g1)

    return dict(
        Q_prob=Q_prob, j_vec=j_vec, g1=g1,
        reliable=reliable, pair_count=pair_count, health=health,
    )


def apply_neighbor_extrapolation(vec, reliable):
    vec_final = vec.copy()
    extrapolation_log = []
    monotonic_log = {}
    for s in range(N_S_BUCKETS):
        low_side = [4, 3, 2, 1]
        high_side = [5, 6, 7, 8]
        low_side = [b for b in low_side if reliable[encode_state(b, s)]]
        high_side = [b for b in high_side if reliable[encode_state(b, s)]]
        low_mags = [abs(vec[encode_state(b, s)]) for b in low_side]
        high_mags = [abs(vec[encode_state(b, s)]) for b in high_side]
        low_ok = all(a <= b for a, b in zip(low_mags, low_mags[1:]))
        high_ok = all(a <= b for a, b in zip(high_mags, high_mags[1:]))
        monotonic_log[s] = dict(low_ok=low_ok, high_ok=high_ok)

        for j, k in I_PAIRS:
            sj, sk = encode_state(j, s), encode_state(k, s)
            if reliable[sj]:
                continue

            nj, nk = j + 1, k - 1
            snj, snk = encode_state(nj, s), encode_state(nk, s)

            if reliable[snj] and low_ok:
                vec_final[sj] = vec[snj]
                extrapolation_log.append((j, s, nj, "ok"))
            else:
                extrapolation_log.append((j, s, nj, "skipped: neighbor unreliable or non-monotonic"))

            if reliable[snk] and high_ok:
                vec_final[sk] = vec[snk]
                extrapolation_log.append((k, s, nk, "ok"))
            else:
                extrapolation_log.append((k, s, nk, "skipped: neighbor unreliable or non-monotonic"))

    return vec_final, extrapolation_log, monotonic_log


def clamp_G_star(vec):
    bounds = np.array([1.0 if decode_state(s)[1] == 0 else 2.0 for s in range(N_STATES)])
    clamped_states = [s for s in range(N_STATES) if abs(vec[s]) > bounds[s]]
    vec_clamped = np.clip(vec, -bounds, bounds)
    return vec_clamped, clamped_states


def build_reliability_metadata(row_count, result):
    reliable = result["reliable"]
    pair_count = result["pair_count"]

    pair_of = {}
    for (j, k, s), combined in pair_count.items():
        pair_of[encode_state(j, s)] = (k, combined)
        pair_of[encode_state(k, s)] = (j, combined)

    states = []
    unreliable_states = []
    for state in range(N_STATES):
        i_bucket, s_bucket = decode_state(state)
        paired_i, paired_combined = pair_of[state]
        entry = dict(
            state=state, I_bucket=i_bucket, s_bucket=s_bucket,
            row_count=int(row_count[state]), reliable=bool(reliable[state]),
            paired_with_I_bucket=paired_i, paired_combined_row_count=paired_combined,
        )
        states.append(entry)
        if not reliable[state]:
            unreliable_states.append(dict(entry, g1_raw=float(result["g1"][state])))

    return dict(
        threshold=UNRELIABLE_THRESHOLD,
        n_unreliable=len(unreliable_states),
        states=states,
        unreliable_states=unreliable_states,
    )


def build_final_lookup_table(
    df, row_count, result, acc, extrapolation_log,
    monotonic_log, clamped_states, G_star_price, tick_size,
):
    reliable = result["reliable"]
    ts = df["ts"].values

    extrap_of = {}
    for j, s, n, status in extrapolation_log:
        extrap_of[encode_state(j, s)] = (n, status)

    table = [[None] * N_S_BUCKETS for _ in range(10)]
    cells = []
    unreliable_states, extrapolated_states, clamped_state_list = [], [], []
    for state in range(N_STATES):
        i_bucket, s_bucket = decode_state(state)
        table[i_bucket][s_bucket] = float(G_star_price[state])
        cell = dict(
            state=state, I_bucket=i_bucket, s_bucket=s_bucket,
            row_count=int(row_count[state]), reliable=bool(reliable[state]),
            G_star_price=float(G_star_price[state]),
            clamped=state in clamped_states,
        )
        if not reliable[state]:
            unreliable_states.append(state)
        if state in extrap_of:
            borrowed_from, status = extrap_of[state]
            cell["extrapolated_from_I_bucket"] = borrowed_from
            cell["extrapolation_status"] = status
            if status == "ok":
                extrapolated_states.append(state)
        if state in clamped_states:
            clamped_state_list.append(state)
        cells.append(cell)

    return dict(
        window_start_ns=int(ts.min()), window_end_ns=int(ts.max()),
        tick_size=tick_size, unit="price (tick_size/2 per half-tick)",
        table_I_by_S=table,
        cells=cells,
        unreliable_states=unreliable_states,
        extrapolated_states=extrapolated_states,
        clamped_states=clamped_state_list,
        monotonic_log=monotonic_log,
        n_iters=acc["n_iters"], converged=acc["converged"], decay_log=acc["decay_log"],
        health=dict(
            row_sums_min=float(result["health"]["row_sums"].min()),
            row_sums_max=float(result["health"]["row_sums"].max()),
            residual=float(result["health"]["residual"]),
        ),
    )


def default_bbo_cache_path(catalog_path: str, instrument_id: str) -> str:
    safe_id = instrument_id.replace(".", "_").replace("/", "_")
    return os.path.join(catalog_path, os.pardir, f"bbo_series_{safe_id}.parquet")


def compute_lookup_table(
    catalog_path: str,
    instrument_id: str,
    tick_size: float,
    window_days: float = 7.0,
    bbo_cache_path: str | None = None,
    now_ns: int | None = None,
) -> dict:
    """
    Top-level entrypoint: rolling-window BBO reconstruction from
    ``<catalog_path>/data/order_book_deltas/<instrument_id>/*.parquet`` ->
    (I_bucket, s_bucket) transition matrices -> G* lookup table.

    Returns the same dict shape as the repo-root ``transition_matrix.py``'s
    ``data/final_lookup_table.json`` output, plus ``computed_at_ns`` and
    ``window_days``. Writes nothing to disk except the incremental BBO cache
    at ``bbo_cache_path`` — persisting the returned table is the caller's
    responsibility.

    Raises ``ValueError`` if no order_book_deltas fall within the window
    (e.g. a freshly started strategy with no history yet).
    """
    if now_ns is None:
        now_ns = pd.Timestamp.now(tz="UTC").value
    if bbo_cache_path is None:
        bbo_cache_path = default_bbo_cache_path(catalog_path, instrument_id)

    ob_glob = os.path.join(catalog_path, "data", "order_book_deltas", instrument_id, "*.parquet")
    window = pd.Timedelta(days=window_days)

    df = maintain_rolling_bbo_cache(bbo_cache_path, ob_glob, window, now_ns=now_ns)
    if df.empty:
        raise ValueError(
            f"no order_book_deltas found under {ob_glob} within the last {window_days} days",
        )

    df = prepare_columns(df, tick_size)
    Q, R_count, J_sum, row_count, diag, _extra = build_transition_matrices(df)

    result = normalize_and_solve(Q, R_count, J_sum, row_count)
    B, _R_hat = build_recursive_operator(Q, R_count, row_count, result["reliable"], result["Q_prob"])
    acc = accumulate_G_star(B, result["g1"])
    G_star_extrap, extrapolation_log, monotonic_log = apply_neighbor_extrapolation(
        acc["G_star"], result["reliable"],
    )
    G_star_clamped, clamped_states = clamp_G_star(G_star_extrap)
    G_star_price = G_star_clamped * (tick_size / 2)

    final_table = build_final_lookup_table(
        df, row_count, result, acc, extrapolation_log, monotonic_log,
        clamped_states, G_star_price, tick_size,
    )
    final_table["reliability_metadata"] = build_reliability_metadata(row_count, result)
    final_table["diagnostics"] = diag
    final_table["computed_at_ns"] = int(now_ns)
    final_table["window_days"] = window_days
    return final_table
