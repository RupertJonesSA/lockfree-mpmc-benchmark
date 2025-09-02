#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys

import pandas as pd


def run_binary(binary, outdir, base_name, n_runs=10):
    csv_files = []
    for i in range(n_runs):
        out_csv = os.path.join(outdir, f"{base_name}_run{i+1}.csv")
        print(f"[RUN {i+1}/{n_runs}] -> {out_csv}")
        proc = subprocess.run([binary],
                              input=(out_csv + "\n").encode(),
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT)
        print(proc.stdout.decode())
        csv_files.append(out_csv)
    return csv_files

def aggregate(csv_files):
    import analyze_mpmc  # reuse your analysis code
    runs = []
    for f in csv_files:
        df = analyze_mpmc.read_stats(f)
        runs.append(analyze_mpmc.summarize_run(df))

    # Build per-run rows (keep impl for display, don't aggregate it)
    rows = []
    for r in runs:
        qn_mean = (r["qwait_count"] / r["windows"]) if r["windows"] else float("nan")
        rows.append({
            "impl": r["impl"],
            "throughput": r["throughput_msgs_per_s"],
            "p50": r["qwait_p50_ns_wavg"],
            "p90": r["qwait_p90_ns_wavg"],
            "p99": r["qwait_p99_ns_wavg"],
            "qwait_mean_ns": r.get("qwait_mean_ns_win_mean_mean", float("nan")),
            "qn_mean": qn_mean,
            "drops_full": r["drops_full"],
            "drops_oversize": r["drops_oversize"],
        })

    df = pd.DataFrame(rows)

    # Aggregate only numeric columns to avoid "could not convert string ..." errors
    numeric_df = df.select_dtypes(include="number")
    agg = numeric_df.agg(["mean", "std", "min", "max"])

    return df, agg

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("binary", help="Path to your compiled test binary")
    ap.add_argument("--outdir", default="runs", help="Directory to store CSVs")
    ap.add_argument("--base", default="lockfree", help="Base name for CSVs")
    ap.add_argument("--n", type=int, default=10, help="Number of runs")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    csvs = run_binary(args.binary, args.outdir, args.base, args.n)
    df, agg = aggregate(csvs)

    print("\n=== PER RUN ===")
    print(df.to_string(index=False))
    print("\n=== AGGREGATE (numeric cols only) ===")
    print(agg.to_string())

    df.to_csv(os.path.join(args.outdir, f"all_runs_{args.base}.csv"), index=False)
    agg.to_csv(os.path.join(args.outdir, f"aggregate_{args.base}.csv"))

if __name__ == "__main__":
    main()
