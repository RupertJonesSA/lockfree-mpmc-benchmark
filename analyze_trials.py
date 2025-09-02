#!/usr/bin/env python3
import glob
import os
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

RAW_DIR = Path("results/raw")
AGG_DIR = Path("results/agg")
PLOT_DIR = Path("plots")
AGG_DIR.mkdir(parents=True, exist_ok=True)
PLOT_DIR.mkdir(parents=True, exist_ok=True)

def parse_raw_file(path: str):
    """
    Parse a mixed raw file:
    - leading metadata lines like: key,value
    - followed by a wide CSV table with header including ts_start_ns, ts_end_ns, window_id, ...
    Returns (meta: dict, df: DataFrame or None)
    """
    meta = {}
    header_idx = None
    header_cols = None

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        raw = f.read().splitlines()

    # collect metadata until we detect a 'wide' header (>=10 comma-separated columns)
    for i, line in enumerate(raw):
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 10:
            header_idx = i
            header_cols = parts
            break
        # collect key,value style metadata (len 1 or 2)
        if 1 <= len(parts) <= 2 and len(parts[0]) > 0:
            if len(parts) == 2:
                meta[parts[0]] = parts[1]
            else:
                meta.setdefault("flags", []).append(parts[0])

    df = None
    if header_idx is not None:
        try:
            # read the table from header line onward
            from io import StringIO
            tail = "\n".join(raw[header_idx:])
            df = pd.read_csv(StringIO(tail))
        except Exception as e:
            print(f"  ! Failed parsing table in {path}: {e}")
            df = None

    return meta, df

def infer_from_filename(fname: str):
    # Example: lock_cur7_cap32768_seed5.csv
    base = os.path.basename(fname)
    impl = "lockfree" if "lockfree" in base.lower() else ("lock" if "lock" in base.lower() else "unknown")
    m_cur = re.search(r"cur(\d+)", base, re.I)
    m_cap = re.search(r"cap(\d+)", base, re.I)
    m_seed = re.search(r"seed(\d+)", base, re.I)
    cur = int(m_cur.group(1)) if m_cur else None
    cap = int(m_cap.group(1)) if m_cap else None
    seed = int(m_seed.group(1)) if m_seed else None
    return impl, cur, cap, seed

def safe_float(x, default=np.nan):
    try:
        return float(x)
    except Exception:
        return default

def infer_duration_seconds_from_table(df: pd.DataFrame) -> float:
    """
    Infer run duration from the per-window table.
    Uses min(ts_start_ns) and max(ts_end_ns) if present.
    Returns np.nan if it can't be inferred.
    """
    if df is None:
        return np.nan

    start_col = None
    end_col = None
    for c in df.columns:
        lc = c.lower()
        if lc == "ts_start_ns":
            start_col = c
        elif lc == "ts_end_ns":
            end_col = c

    if start_col is None or end_col is None:
        return np.nan

    try:
        t0 = pd.to_numeric(df[start_col], errors="coerce").min()
        t1 = pd.to_numeric(df[end_col], errors="coerce").max()
        if pd.isna(t0) or pd.isna(t1) or t1 <= t0:
            return np.nan
        return float((t1 - t0) / 1e9)  # ns -> s
    except Exception:
        return np.nan

def aggregate_trial(meta: dict, df: pd.DataFrame, srcfile: str):
    """
    Build one per-trial summary row.
    Assumptions about columns (best-effort):
      - recv_count (int per window) -> throughput = sum(recv_count) / duration_s
      - qwait_p50_ns / qwait_p90_ns / qwait_p99_ns / qwait_mean_ns / qwait_count
      - drops_full / drops_oversize per window (sum)
    We compute percentile surrogates as weighted averages using qwait_count weights (if present),
    else simple mean across windows.
    """
    impl, cur, cap, seed = infer_from_filename(srcfile)

    # duration_s from metadata if present; otherwise infer from table
    duration_s = None
    for k in ["duration_s", "duration", "duration_sec"]:
        if k in meta:
            duration_s = safe_float(meta[k])
            break
    if not duration_s or not np.isfinite(duration_s) or duration_s <= 0:
        duration_s = infer_duration_seconds_from_table(df)

    # Throughput
    total_recv = 0.0
    if df is not None and "recv_count" in df.columns:
        total_recv = float(pd.to_numeric(df["recv_count"], errors="coerce").fillna(0).sum())
    throughput = (total_recv / duration_s) if (duration_s and duration_s > 0) else np.nan

    # Weighted percentile surrogates
    weights = None
    if df is not None and "qwait_count" in df.columns:
        w = pd.to_numeric(df["qwait_count"], errors="coerce").fillna(0).astype(float)
        weights = w.where(w > 0, 0.0)
        if weights.sum() == 0:
            weights = None

    def wmean(col):
        if df is None or col not in df.columns:
            return np.nan
        vals = pd.to_numeric(df[col], errors="coerce").astype(float)
        if weights is not None:
            return float(np.average(vals, weights=weights))
        return float(vals.mean())

    p50  = wmean("qwait_p50_ns") if "qwait_p50_ns" in (df.columns if df is not None else []) else wmean("p50")
    p90  = wmean("qwait_p90_ns") if "qwait_p90_ns" in (df.columns if df is not None else []) else wmean("p90")
    p99  = wmean("qwait_p99_ns") if "qwait_p99_ns" in (df.columns if df is not None else []) else wmean("p99")
    p999 = wmean("qwait_p999_ns") if df is not None and "qwait_p999_ns" in df.columns else np.nan

    qwait_mean_ns = wmean("qwait_mean_ns")
    qn_mean       = wmean("qn_mean") if df is not None and "qn_mean" in df.columns else np.nan

    drops_full     = int(pd.to_numeric(df.get("drops_full", 0), errors="coerce").fillna(0).sum()) if df is not None else 0
    drops_oversize = int(pd.to_numeric(df.get("drops_oversize", 0), errors="coerce").fillna(0).sum()) if df is not None else 0

    row = {
        "impl": meta.get("impl", impl),
        "cur_mcgrp": safe_float(meta.get("cur_mcgrp", cur), cur),
        "capacity": safe_float(meta.get("capacity", cap), cap),
        "seed": safe_float(meta.get("seed", seed), seed),
        "duration_s": duration_s,
        "throughput": throughput,
        "p50": p50,
        "p90": p90,
        "p99": p99,
        "p999": p999,
        "qwait_mean_ns": qwait_mean_ns,
        "qn_mean": qn_mean,
        "drops_full": drops_full,
        "drops_oversize": drops_oversize,
        "source_file": os.path.basename(srcfile),
    }
    return row

def _ci95(series):
    # 95% CI assuming approx normal across seeds/runs
    s = series.dropna()
    n = len(s)
    if n <= 1:
        return 0.0
    return float(1.96 * (s.std(ddof=1) / (n ** 0.5)))

def _agg_stats(df, by_cols, metric):
    g = df.groupby(by_cols)[metric].agg(['mean', 'std', 'count'])
    # classic 1.96*sem CI (computed via helper for numerical stability/coercion)
    g['ci95'] = df.groupby(by_cols)[metric].apply(_ci95).values
    return g.reset_index()

def _savefig(path, title=None, xlabel=None, ylabel=None):
    ax = plt.gca()
    ax.grid(True, linestyle='--', alpha=0.4)
    if title: plt.title(title)
    if xlabel: plt.xlabel(xlabel)
    if ylabel: plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()

def _ensure_columns(df):
    needed = ['impl','cur_mcgrp','capacity','seed','throughput','p50','p90','p99','p999','qwait_mean_ns','drops_full','drops_oversize']
    for c in needed:
        if c not in df.columns:
            df[c] = np.nan
    return df

def _trendline_xy(x, y):
    x = np.asarray(pd.to_numeric(x, errors="coerce"), dtype=float)
    y = np.asarray(pd.to_numeric(y, errors="coerce"), dtype=float)
    mask = np.isfinite(x) & np.isfinite(y)
    if mask.sum() < 2:
        return None, None, None
    coeff = np.polyfit(x[mask], y[mask], 1)
    yhat = np.polyval(coeff, x[mask])
    ss_res = float(np.sum((y[mask] - yhat)**2))
    ss_tot = float(np.sum((y[mask] - y[mask].mean())**2))
    r2 = 1 - ss_res/ss_tot if ss_tot > 0 else np.nan
    return coeff, (x[mask], y[mask]), r2

def _matched_speedup(df):
    # compute means per impl/cur/cap, then make rows only where both impls exist
    base = df.groupby(['impl','cur_mcgrp','capacity'])['throughput'].mean().reset_index()
    lock = base[base['impl'].str.lower() == 'lock'].set_index(['cur_mcgrp','capacity'])['throughput']
    lf = base[base['impl'].str.lower() == 'lockfree'].set_index(['cur_mcgrp','capacity'])['throughput']
    common = lock.index.intersection(lf.index)
    if len(common) == 0:
        return pd.DataFrame(columns=['cur_mcgrp','capacity','speedup'])
    sp = (lf.loc[common] / lock.loc[common]).reset_index()
    sp.columns = ['cur_mcgrp','capacity','speedup']
    return sp

def main():
    files = sorted(glob.glob(str(RAW_DIR / "*.csv")))
    if not files:
        print("No raw CSVs found in results/raw/")
        sys.exit(0)

    rows = []
    for f in files:
        print(f"Parsing {f}")
        meta, df = parse_raw_file(f)

        if df is None:
            print(f"  ! No data table detected in {f}; skipping.")
            continue

        rows.append(aggregate_trial(meta, df, f))

    if not rows:
        print("No trials parsed successfully.")
        sys.exit(0)

    per_trial = pd.DataFrame(rows)
    per_trial.to_csv(AGG_DIR / "per_trial.csv", index=False)

    # Fallback impl inference if not present
    if "impl" not in per_trial.columns or per_trial["impl"].isna().any():
        per_trial["impl"] = per_trial["source_file"].str.contains("lockfree", case=False)\
            .map({True:"lockfree", False:"lock"})

    per_trial = _ensure_columns(per_trial)

    # ---------- Enhanced plotting & analysis ----------
    # 0) QC table
    qc = (per_trial
          .groupby(['impl','cur_mcgrp','capacity'])
          .agg(n_trials=('throughput','count'),
               thr_mean=('throughput','mean'),
               p99_mean=('p99','mean'))
          .reset_index())
    qc.to_csv(AGG_DIR / "qc_by_impl_cur_cap.csv", index=False)

    # 1) Bars with 95% CI: throughput by impl (overall)
    thr_by_impl = _agg_stats(per_trial, ['impl'], 'throughput')
    plt.figure()
    plt.bar(thr_by_impl['impl'], thr_by_impl['mean'], yerr=thr_by_impl['ci95'], capsize=4)
    _savefig(PLOT_DIR / "throughput_by_impl_ci.png",
             title="Throughput by Implementation (95% CI)",
             xlabel="impl", ylabel="msgs/sec")

    # 2) Scaling: throughput vs cur_mcgrp for each impl (mean ± CI), per capacity
    capacities = sorted([c for c in per_trial['capacity'].dropna().unique().tolist()])
    if not capacities:
        capacities = [None]
    for cap in capacities:
        if cap is None:
            dfc = per_trial.copy()
            cap_label = "all"
        else:
            dfc = per_trial[per_trial['capacity'] == cap]
            cap_label = str(int(cap))
        if dfc.empty:
            continue
        agg = _agg_stats(dfc, ['impl','cur_mcgrp'], 'throughput')
        for impl in sorted(agg['impl'].unique()):
            sub = agg[agg['impl'] == impl].sort_values('cur_mcgrp')
            if sub.empty:
                continue
            plt.figure()
            plt.errorbar(sub['cur_mcgrp'], sub['mean'], yerr=sub['ci95'], fmt='-o', ecolor='gray', alpha=0.4, capsize=3)
            _savefig(PLOT_DIR / f"throughput_vs_cur_{impl}_cap{cap_label}.png",
                     title=f"Throughput vs Concurrency ({impl}, cap={cap_label})",
                     xlabel="cur_mcgrp (concurrency)", ylabel="msgs/sec")

    # 3) Latency vs cur_mcgrp (p50/p90/p99) for each impl, per capacity
    for cap in capacities:
        if cap is None:
            dfc = per_trial.copy()
            cap_label = "all"
        else:
            dfc = per_trial[per_trial['capacity'] == cap]
            cap_label = str(int(cap))
        if dfc.empty:
            continue
        for impl in sorted(dfc['impl'].dropna().unique()):
            sub = dfc[dfc['impl'] == impl]
            for metric in ['p50','p90','p99']:
                if sub[metric].notna().any():
                    agg = _agg_stats(sub, ['cur_mcgrp'], metric).sort_values('cur_mcgrp')
                    plt.figure()
                    plt.errorbar(agg['cur_mcgrp'], agg['mean'], yerr=agg['ci95'], fmt='-o', ecolor='gray', alpha=0.4, capsize=3)
                    plt.yscale('log')  # latency spans can be wide
                    _savefig(PLOT_DIR / f"{metric}_vs_cur_{impl}_cap{cap_label}.png",
                             title=f"{metric} latency vs Concurrency ({impl}, cap={cap_label})",
                             xlabel="cur_mcgrp (concurrency)", ylabel=f"{metric} (ns, log scale)")

    # 4) Throughput vs p99 scatter with trend line and R^2 (per capacity)
    for cap in capacities:
        if cap is None:
            dfc = per_trial.copy()
            cap_label = "all"
        else:
            dfc = per_trial[per_trial['capacity'] == cap]
            cap_label = str(int(cap))
        if dfc.empty:
            continue
        plt.figure()
        for impl in sorted(dfc['impl'].dropna().unique()):
            sub = dfc[(dfc['impl'] == impl) & dfc['throughput'].notna() & dfc['p99'].notna()]
            if sub.empty:
                continue
            plt.scatter(sub['throughput'], sub['p99'], alpha=0.7, label=impl)
        coeff, xy, r2 = _trendline_xy(dfc['throughput'], dfc['p99'])
        if xy is not None:
            xs = np.linspace(min(xy[0]), max(xy[0]), 100)
            ys = coeff[0]*xs + coeff[1]
            plt.plot(xs, ys, linestyle='-')
            plt.legend(title=f"R²={r2:.3f}")
        else:
            plt.legend()
        plt.yscale('log')
        _savefig(PLOT_DIR / f"throughput_vs_p99_cap{cap_label}.png",
                 title=f"Throughput vs p99 latency (cap={cap_label})",
                 xlabel="Throughput (msgs/sec)", ylabel="p99 (ns, log scale)")

    # 5) Speedup (lockfree / lock) by (cur_mcgrp, capacity)
    sp = _matched_speedup(per_trial)
    if not sp.empty:
        agg_sp = sp.groupby('cur_mcgrp')['speedup'].mean().reset_index()
        plt.figure()
        plt.bar(agg_sp['cur_mcgrp'].astype(int), agg_sp['speedup'])
        _savefig(PLOT_DIR / f"speedup_lockfree_over_lock_by_cur.png",
                 title="Lockfree speedup over Lock by Concurrency",
                 xlabel="cur_mcgrp", ylabel="speedup (LF/Lock)")

        for cap, sub in sp.groupby('capacity'):
            sub = sub.sort_values('cur_mcgrp')
            plt.figure()
            plt.plot(sub['cur_mcgrp'], sub['speedup'], marker='o')
            _savefig(PLOT_DIR / f"speedup_vs_cur_cap{int(cap)}.png",
                     title=f"Speedup (LF/Lock) vs Concurrency (cap={int(cap)})",
                     xlabel="cur_mcgrp", ylabel="speedup")

    # 6) Drops vs throughput
    if ('drops_full' in per_trial.columns) or ('drops_oversize' in per_trial.columns):
        per_trial['drops_total'] = per_trial.get('drops_full', 0).fillna(0) + per_trial.get('drops_oversize', 0).fillna(0)
        plt.figure()
        for impl, sub in per_trial.groupby('impl'):
            plt.scatter(sub['drops_total'], sub['throughput'], alpha=0.7, label=impl)
        plt.legend()
        _savefig(PLOT_DIR / f"drops_vs_throughput.png",
                 title="Drops vs Throughput",
                 xlabel="Total Drops (full+oversize)", ylabel="Throughput (msgs/sec)")

    # 7) Boxplots per impl for variability in p99 and throughput
    for metric in ['p99','throughput']:
        vals = [per_trial.loc[per_trial['impl']==impl, metric].dropna().values
                for impl in sorted(per_trial['impl'].dropna().unique())]
        labels = list(sorted(per_trial['impl'].dropna().unique()))
        if all(len(v)==0 for v in vals):
            continue
        plt.figure()
        plt.boxplot(vals, labels=labels, showfliers=True)
        if metric != 'throughput':
            plt.yscale('log')
        _savefig(PLOT_DIR / f"box_{metric}_by_impl.png",
                 title=f"{metric} distribution by Implementation",
                 xlabel="impl", ylabel=f"{metric} ({'ns, log' if metric!='throughput' else 'msgs/sec'})")
    # ---------- end enhanced plotting ----------

    print("Wrote:", AGG_DIR / "per_trial.csv")
    print("Wrote:", AGG_DIR / "qc_by_impl_cur_cap.csv")
    print("Enhanced plots in:", PLOT_DIR)

if __name__ == "__main__":
    main()
