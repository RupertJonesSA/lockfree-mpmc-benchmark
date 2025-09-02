# Update analyze_mpmc.py to incorporate qwait_mean_ns if present.
# - Adds per-window mean of qwait_mean_ns (averaged across symbols for that window)
# - Adds run-level aggregates of qwait_mean_ns: mean/median/min/max computed over per-window means
# - Includes these in summary_runs.csv and comparison output

import argparse
import math
import os
import sys

import pandas as pd


def _find_header_and_impl(lines):
    impl = None
    header_idx = None
    for i, line in enumerate(lines):
        s = line.strip()
        if not s:
            continue
        if s.lower().startswith("impl,") and impl is None:
            parts = s.split(",", 1)
            if len(parts) == 2 and parts[1].strip():
                impl = parts[1].strip()
        if s.startswith("ts_start_ns,") and "window_id" in s and "sym" in s:
            header_idx = i
            break
    return header_idx, impl

def read_stats(path: str) -> pd.DataFrame:
    from io import StringIO
    with open(path, 'r') as f:
        lines = f.readlines()

    header_idx, impl = _find_header_and_impl(lines)
    if header_idx is None:
        if lines and lines[0].strip().lower().startswith("schema,"):
            header_idx = 1
        else:
            raise ValueError(f"Could not locate CSV header row in {path}. Expected a line starting with 'ts_start_ns,'.")

    csv_text = "".join(lines[header_idx:])
    df = pd.read_csv(StringIO(csv_text))

    if impl is None:
        lower = os.path.basename(path).lower()
        if "lockfree" in lower or "lf" in lower:
            impl = "lockfree"
        elif "lock" in lower:
            impl = "lock"
        else:
            impl = "run"

    df["impl"] = impl
    df["source_file"] = os.path.basename(path)
    return df

def ns_to_seconds(ns: int) -> float:
    return ns / 1_000_000_000.0

def summarize_run(df: pd.DataFrame) -> dict:
    df_nonpartial = df[df.get("partial", 0) == 0].copy()
    if df_nonpartial.empty:
        df_nonpartial = df.copy()
    impl = df["impl"].iloc[0]
    source = df["source_file"].iloc[0]

    t_start = df["ts_start_ns"].min()
    t_end = df["ts_end_ns"].max()
    duration_s = ns_to_seconds(t_end - t_start) if (t_end > t_start) else float("nan")

    per_window = df.groupby("window_id", as_index=False).agg({
        "ts_start_ns":"min",
        "ts_end_ns":"max",
        "qwait_p50_ns":"first" if "qwait_p50_ns" in df.columns else "first",
        "qwait_p90_ns":"first" if "qwait_p90_ns" in df.columns else "first",
        "qwait_p99_ns":"first" if "qwait_p99_ns" in df.columns else "first",
        "qwait_max_ns":"first" if "qwait_max_ns" in df.columns else "first",
        "qwait_count":"first" if "qwait_count" in df.columns else "first",
        "drops_full":"first" if "drops_full" in df.columns else "first",
        "drops_oversize":"first" if "drops_oversize" in df.columns else "first",
        "recv_count":"first" if "recv_count" in df.columns else "first",
        "parse_errs":"first" if "parse_errs" in df.columns else "first",
        "partial":"max" if "partial" in df.columns else "sum"
    })

    # If per-symbol qwait_mean_ns exists, compute per-window average across symbols
    qwait_mean_present = "qwait_mean_ns" in df.columns
    if qwait_mean_present:
        pw_mean = df.groupby("window_id", as_index=False)["qwait_mean_ns"].mean()
        pw_mean = pw_mean.rename(columns={"qwait_mean_ns":"qwait_mean_ns_win_mean"})
        per_window = per_window.merge(pw_mean, on="window_id", how="left")

    total_recv = int(per_window["recv_count"].sum()) if "recv_count" in per_window.columns else 0
    drops_full = int(per_window["drops_full"].sum()) if "drops_full" in per_window.columns else 0
    drops_oversize = int(per_window["drops_oversize"].sum()) if "drops_oversize" in per_window.columns else 0
    parse_errs = int(per_window["parse_errs"].sum()) if "parse_errs" in per_window.columns else 0
    windows = len(per_window)

    def wavg(col):
        if "qwait_count" not in per_window.columns:
            return float("nan")
        w = per_window["qwait_count"].astype(float)
        x = per_window[col].astype(float)
        denom = w.sum()
        return float((x * w).sum() / denom) if denom > 0 else float("nan")

    q50 = wavg("qwait_p50_ns") if "qwait_p50_ns" in per_window.columns else float("nan")
    q90 = wavg("qwait_p90_ns") if "qwait_p90_ns" in per_window.columns else float("nan")
    q99 = wavg("qwait_p99_ns") if "qwait_p99_ns" in per_window.columns else float("nan")
    qmax = per_window["qwait_max_ns"].max() if "qwait_max_ns" in per_window.columns and windows > 0 else float("nan")
    qcount = int(per_window["qwait_count"].sum()) if "qwait_count" in per_window.columns else 0

    # Run-level aggregates for qwait_mean_ns based on per-window means (less duplication than per-row)
    if qwait_mean_present and "qwait_mean_ns_win_mean" in per_window.columns:
        qmean_series = per_window["qwait_mean_ns_win_mean"].dropna()
        qmean_run_mean = float(qmean_series.mean()) if not qmean_series.empty else float("nan")
        qmean_run_median = float(qmean_series.median()) if not qmean_series.empty else float("nan")
        qmean_run_min = float(qmean_series.min()) if not qmean_series.empty else float("nan")
        qmean_run_max = float(qmean_series.max()) if not qmean_series.empty else float("nan")
    else:
        qmean_run_mean = qmean_run_median = qmean_run_min = qmean_run_max = float("nan")
    tput_msgs_per_s = (total_recv / duration_s) if (duration_s and duration_s > 0) else float("nan")
    drop_rate_full = (drops_full / duration_s) if (duration_s and duration_s > 0) else float("nan")
    drop_rate_oversize = (drops_oversize / duration_s) if (duration_s and duration_s > 0) else float("nan")

    group_cols = [c for c in df.columns if c in ("sym",)]
    per_symbol = df.groupby(group_cols, as_index=False).agg({
        "buy_qty":"sum" if "buy_qty" in df.columns else "sum",
        "sell_qty":"sum" if "sell_qty" in df.columns else "sum",
        "buy_notional":"sum" if "buy_notional" in df.columns else "sum",
        "sell_notional":"sum" if "sell_notional" in df.columns else "sum",
        "buy_trades":"sum" if "buy_trades" in df.columns else "sum",
        "sell_trades":"sum" if "sell_trades" in df.columns else "sum",
        **({"qwait_mean_ns":"mean"} if "qwait_mean_ns" in df.columns else {}),
    })

    if "msg_count" in df.columns:
        mcounts = df.groupby("sym", as_index=False)["msg_count"].sum()
        per_symbol = per_symbol.merge(mcounts, on="sym", how="left")
    else:
        per_symbol["msg_count"] = per_symbol.get("buy_trades", 0) + per_symbol.get("sell_trades", 0)

    if "buy_notional" in per_symbol.columns and "buy_qty" in per_symbol.columns:
        per_symbol["buy_vwap"] = per_symbol.apply(lambda r: (float(r["buy_notional"]) / max(int(r["buy_qty"]),1))
                                                  if int(r["buy_qty"]) > 0 else 0.0, axis=1)
    else:
        per_symbol["buy_vwap"] = 0.0
    if "sell_notional" in per_symbol.columns and "sell_qty" in per_symbol.columns:
        per_symbol["sell_vwap"] = per_symbol.apply(lambda r: (float(r["sell_notional"]) / max(int(r["sell_qty"]),1))
                                                   if int(r["sell_qty"]) > 0 else 0.0, axis=1)
    else:
        per_symbol["sell_vwap"] = 0.0

    return {
        "impl": impl,
        "source_file": source,
        "duration_s": duration_s,
        "windows": windows,
        "total_recv": total_recv,
        "throughput_msgs_per_s": tput_msgs_per_s,
        "drops_full": drops_full,
        "drops_oversize": drops_oversize,
        "drop_rate_full_per_s": drop_rate_full,
        "drop_rate_oversize_per_s": drop_rate_oversize,
        "parse_errs": parse_errs,
        "qwait_p50_ns_wavg": q50,
        "qwait_p90_ns_wavg": q90,
        "qwait_p99_ns_wavg": q99,
        "qwait_max_ns": qmax,
        "qwait_count": qcount,
        # New qwait_mean_ns aggregates
        "qwait_mean_ns_win_mean_mean": qmean_run_mean,
        "qwait_mean_ns_win_mean_median": qmean_run_median,
        "qwait_mean_ns_win_mean_min": qmean_run_min,
        "qwait_mean_ns_win_mean_max": qmean_run_max,
        "per_window": per_window,
        "per_symbol": per_symbol
    }

def compare_runs(a: dict, b: dict) -> pd.DataFrame:
    def pct_delta(v0, v1):
        try:
            if v0 == 0 or (isinstance(v0, float) and (math.isnan(v0) or v0 == 0.0)):
                return float("nan")
            return (v1 - v0) / v0 * 100.0
        except Exception:
            return float("nan")

    keys = [
        ("duration_s", "Duration (s)"),
        ("windows", "Windows"),
        ("total_recv", "Total Recv"),
        ("throughput_msgs_per_s", "Throughput (msg/s)"),
        ("drops_full", "Drops Full"),
        ("drop_rate_full_per_s", "Drop Rate Full (/s)"),
        ("drops_oversize", "Drops Oversize"),
        ("drop_rate_oversize_per_s", "Drop Rate Oversize (/s)"),
        ("parse_errs", "Parse Errors"),
        ("qwait_p50_ns_wavg", "Qwait p50 (ns, wavg)"),
        ("qwait_p90_ns_wavg", "Qwait p90 (ns, wavg)"),
        ("qwait_p99_ns_wavg", "Qwait p99 (ns, wavg)"),
        ("qwait_max_ns", "Qwait max (ns)"),
        ("qwait_count", "Qwait samples"),
        # New mean metrics
        ("qwait_mean_ns_win_mean_mean", "Qwait mean (ns, per-window mean of per-symbol means)"),
        ("qwait_mean_ns_win_mean_median", "Qwait mean median (ns, per-window)"),
        ("qwait_mean_ns_win_mean_min", "Qwait mean min (ns, per-window)"),
        ("qwait_mean_ns_win_mean_max", "Qwait mean max (ns, per-window)"),
    ]

    rows = []
    for k, label in keys:
        v0 = a.get(k, float("nan"))
        v1 = b.get(k, float("nan"))
        rows.append({
            "Metric": label,
            a["impl"]: v0,
            b["impl"]: v1,
            "% Δ ({}→{})".format(a["impl"], b["impl"]): pct_delta(v0, v1)
        })
    return pd.DataFrame(rows)

def main():
    ap = argparse.ArgumentParser(description="Analyze MPMC pipeline CSV outputs (lock vs lock-free).")
    ap.add_argument("csv", nargs="+", help="One or two CSV files exported by the C++ program.")
    ap.add_argument("--outdir", default=".", help="Directory to write summary CSVs.")
    args = ap.parse_args()

    if len(args.csv) > 2:
        print("Please pass at most two CSV files (e.g., lock.csv lockfree.csv).", file=sys.stderr)
        sys.exit(2)

    runs = []
    for p in args.csv:
        if not os.path.exists(p):
            print(f"File not found: {p}", file=sys.stderr); sys.exit(1)
        df = read_stats(p)
        runs.append(summarize_run(df))

    for r in runs:
        impl = r["impl"]
        per_symbol_path = os.path.join(args.outdir, f"summary_per_symbol_{impl}.csv")
        per_window_path = os.path.join(args.outdir, f"summary_per_window_{impl}.csv")
        r["per_symbol"].to_csv(per_symbol_path, index=False)
        r["per_window"].to_csv(per_window_path, index=False)
        print(f"[{impl}] Wrote {per_symbol_path} and {per_window_path}")

    top_rows = []
    for r in runs:
        top_rows.append({
            "impl": r["impl"],
            "source_file": r["source_file"],
            "duration_s": r["duration_s"],
            "windows": r["windows"],
            "total_recv": r["total_recv"],
            "throughput_msgs_per_s": r["throughput_msgs_per_s"],
            "drops_full": r["drops_full"],
            "drop_rate_full_per_s": r["drop_rate_full_per_s"],
            "drops_oversize": r["drops_oversize"],
            "drop_rate_oversize_per_s": r["drop_rate_oversize_per_s"],
            "parse_errs": r["parse_errs"],
            "qwait_p50_ns_wavg": r["qwait_p50_ns_wavg"],
            "qwait_p90_ns_wavg": r["qwait_p90_ns_wavg"],
            "qwait_p99_ns_wavg": r["qwait_p99_ns_wavg"],
            "qwait_max_ns": r["qwait_max_ns"],
            "qwait_count": r["qwait_count"],
            # New mean metrics
            "qwait_mean_ns_win_mean_mean": r["qwait_mean_ns_win_mean_mean"],
            "qwait_mean_ns_win_mean_median": r["qwait_mean_ns_win_mean_median"],
            "qwait_mean_ns_win_mean_min": r["qwait_mean_ns_win_mean_min"],
            "qwait_mean_ns_win_mean_max": r["qwait_mean_ns_win_mean_max"],
        })
    summary_df = pd.DataFrame(top_rows)
    summary_path = os.path.join(args.outdir, "summary_runs.csv")
    summary_df.to_csv(summary_path, index=False)
    print(f"Wrote {summary_path}")

    if len(runs) == 2:
        comp = compare_runs(runs[0], runs[1])
        comp_path = os.path.join(args.outdir, f"comparison_{runs[0]['impl']}_vs_{runs[1]['impl']}.csv")
        comp.to_csv(comp_path, index=False)
        print(f"Wrote {comp_path}")
        try:
            with pd.option_context('display.max_rows', None, 'display.max_colwidth', None, 'display.width', 160):
                print("\n=== COMPARISON ===")
                print(comp.to_string(index=False))
        except Exception:
            pass
    else:
        try:
            with pd.option_context('display.max_rows', None, 'display.max_colwidth', None, 'display.width', 160):
                print("\n=== SUMMARY ===")
                print(summary_df.to_string(index=False))
        except Exception:
            pass

if __name__ == "__main__":
    try:
        import pandas as _pd  # noqa: F401
    except Exception as e:
        print("This script requires pandas. Install with: pip install pandas", file=sys.stderr)
        sys.exit(1)
    main()
