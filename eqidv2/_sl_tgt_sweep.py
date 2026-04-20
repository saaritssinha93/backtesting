"""Silent SL/TGT sweep for v17b and v17f using the quick_reresolve pattern.

Iterates grid of (short_sl, short_tgt, long_sl, long_tgt), resolves exits,
captures PF/win/sum-pnl/maxdd per combo.

Usage:
  python _sl_tgt_sweep.py --version v17b --mode coarse
  python _sl_tgt_sweep.py --version v17f --mode fine --short-sl 0.006 --long-sl 0.008
"""
from __future__ import annotations

import argparse
import contextlib
import io
import os
import sys
import time
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd

_this_dir = Path(__file__).resolve().parent
_project_root = _this_dir.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from quick_reresolve import EXIT_COLS, _get_version_config


def _pf(p):
    g = float(p[p > 0].sum())
    l = float(-p[p < 0].sum())
    return g / l if l > 0 else float("inf")


def _maxdd(p):
    if len(p) == 0:
        return 0.0
    cum = p.cumsum()
    peak = cum.cummax()
    dd = (cum - peak)
    return float(-dd.min())


def _day_win_pct(df):
    if df.empty or "entry_time_ist" not in df.columns:
        return 0.0
    d = df.copy()
    d["_day"] = pd.to_datetime(d["entry_time_ist"], errors="coerce").dt.date
    daily = d.groupby("_day")["pnl_pct"].sum()
    if len(daily) == 0:
        return 0.0
    return 100.0 * (daily > 0).sum() / len(daily)


def _metrics(df, label):
    if df.empty:
        return dict(label=label, n=0)
    wins = int((df["outcome"].str.upper() == "TARGET").sum())
    return dict(
        label=label,
        n=len(df),
        pf=round(_pf(df["pnl_pct"]), 3),
        win_pct=round(100.0 * wins / len(df), 2),
        day_win_pct=round(_day_win_pct(df), 2),
        sum_pnl=round(float(df["pnl_pct"].sum()), 2),
        maxdd=round(_maxdd(df["pnl_pct"].fillna(0)), 2),
        avg_pnl=round(float(df["pnl_pct"].mean()), 3),
    )


def _prepare_base_df(version: str):
    v = version.lower()
    if v in ("v17f", "v17e"):
        # Use v16 base resolver (same as v17b path) but pull CSV from v17f/v17e output dir
        import avwap_combined_runner_v16_5min as runner
        if v == "v17f":
            import avwap_combined_runner_v17f_5min  # noqa: F401 — trigger patches
            csv_dir = Path(r"C:\TradingData\eqidv2\outputs_v17f_5min")
        else:
            import avwap_combined_runner_v17e_5min  # noqa: F401
            csv_dir = Path(r"C:\TradingData\eqidv2\outputs_v17e_5min")
        candidates = sorted(csv_dir.glob("*trades*.csv"))
        if not candidates:
            raise FileNotFoundError(f"No trades CSV found in {csv_dir}")
        csv_path = max(candidates, key=lambda p: (p.stat().st_mtime, p.name))
    else:
        cfg = _get_version_config(version)
        runner = cfg["runner"]
        csv_path = cfg["csv_path"]
    df = pd.read_csv(csv_path)
    drop_cols = [c for c in EXIT_COLS if c in df.columns]
    df = df.drop(columns=drop_cols)
    return df, runner


def _resolve_side(
    base_df: pd.DataFrame,
    runner,
    side: str,
    sl: float,
    tgt: float,
):
    """Resolve exits for ONE side only, return DataFrame."""
    import avwap_combined_runner_v16_5min as _base
    if side == "SHORT":
        _base.TEST_SHORT_TARGET_PCT = tgt
    else:
        _base.TEST_LONG_TARGET_PCT = tgt
    _base.TEST_TARGET_OVERRIDE = True

    df = base_df.copy()
    ep = df["entry_price"].astype(float)
    is_short = df["side"].str.upper().eq("SHORT")
    if side == "SHORT":
        df["stop_price"] = ep * (1 + sl)
        df["target_price"] = ep * (1 - tgt)
    else:
        df["stop_price"] = ep * (1 - sl)
        df["target_price"] = ep * (1 + tgt)
    df["sl_price"] = df["stop_price"]

    dir_1m = runner._resolve_1min_dir()
    suffix_5m = ".parquet"
    if dir_1m.is_dir():
        for sf in list(dir_1m.glob("*"))[:5]:
            if sf.suffix:
                suffix_5m = sf.suffix
                break

    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        out = runner._resolve_exits_5min(
            df, dir_1m, suffix_5m, "pyarrow",
            eod_exit_time=runner.V15_EOD_EXIT_TIME,
        )
        out = runner._add_notional_pnl(out)
    return out


def _resolve_with(
    base_df: pd.DataFrame,
    runner,
    short_sl: float,
    short_tgt: float,
    long_sl: float,
    long_tgt: float,
):
    """Apply SL/TGT and re-resolve exits. Returns (short_df, long_df, combined_df)."""
    short_in = base_df[base_df["side"].str.upper().eq("SHORT")].copy().reset_index(drop=True)
    long_in = base_df[base_df["side"].str.upper().eq("LONG")].copy().reset_index(drop=True)
    short_df = _resolve_side(short_in, runner, "SHORT", short_sl, short_tgt) if not short_in.empty else short_in
    long_df = _resolve_side(long_in, runner, "LONG", long_sl, long_tgt) if not long_in.empty else long_in
    combined = pd.concat([short_df, long_df], ignore_index=True)
    return short_df, long_df, combined


def run_sweep(
    version: str,
    short_sls, short_tgts, long_sls, long_tgts,
    output_path: Path,
):
    print(f"\n[SWEEP {version.upper()}] "
          f"short_sls={short_sls} short_tgts={short_tgts}  "
          f"long_sls={long_sls} long_tgts={long_tgts}")

    t0 = time.time()
    base_df, runner = _prepare_base_df(version)
    print(f"[LOAD] {len(base_df)} trades loaded")

    # Split to cache the side that isn't varying
    short_in = base_df[base_df["side"].str.upper().eq("SHORT")].copy().reset_index(drop=True)
    long_in = base_df[base_df["side"].str.upper().eq("LONG")].copy().reset_index(drop=True)
    print(f"[SPLIT] {len(short_in)} SHORT, {len(long_in)} LONG")

    unique_short = list({(s, t) for s in short_sls for t in short_tgts})
    unique_long = list({(s, t) for s in long_sls for t in long_tgts})
    print(f"[CACHE] {len(unique_short)} unique SHORT combos, {len(unique_long)} unique LONG combos")

    short_cache = {}
    for i, (ssl, stg) in enumerate(unique_short, 1):
        short_cache[(ssl, stg)] = _resolve_side(short_in, runner, "SHORT", ssl, stg) if not short_in.empty else short_in
        print(f"  SHORT resolve {i}/{len(unique_short)}: sl={ssl*100:.2f}% tgt={stg*100:.2f}%", flush=True)

    long_cache = {}
    for i, (lsl, ltg) in enumerate(unique_long, 1):
        long_cache[(lsl, ltg)] = _resolve_side(long_in, runner, "LONG", lsl, ltg) if not long_in.empty else long_in
        print(f"  LONG resolve {i}/{len(unique_long)}: sl={lsl*100:.2f}% tgt={ltg*100:.2f}%", flush=True)

    rows = []
    combos = list(product(short_sls, short_tgts, long_sls, long_tgts))
    total = len(combos)
    print(f"[COMBINE] {total} combos using cached resolves")

    for idx, (ssl, stg, lsl, ltg) in enumerate(combos, 1):
        s_df = short_cache[(ssl, stg)]
        l_df = long_cache[(lsl, ltg)]
        c_df = pd.concat([s_df, l_df], ignore_index=True)
        row = dict(
            version=version,
            short_sl_pct=round(ssl * 100, 3),
            short_tgt_pct=round(stg * 100, 3),
            long_sl_pct=round(lsl * 100, 3),
            long_tgt_pct=round(ltg * 100, 3),
        )
        m_s = _metrics(s_df, "SHORT")
        m_l = _metrics(l_df, "LONG")
        m_c = _metrics(c_df, "COMB")
        for k, v in m_s.items():
            if k == "label":
                continue
            row[f"S_{k}"] = v
        for k, v in m_l.items():
            if k == "label":
                continue
            row[f"L_{k}"] = v
        for k, v in m_c.items():
            if k == "label":
                continue
            row[f"C_{k}"] = v
        rows.append(row)

    out = pd.DataFrame(rows)
    out.to_csv(output_path, index=False)
    print(f"\n[SAVED] {output_path}  ({len(out)} rows, {time.time()-t0:.1f}s)")

    print("\n==== TOP 8 by COMBINED sum_pnl ====")
    print(out.sort_values("C_sum_pnl", ascending=False).head(8).to_string(index=False))
    print("\n==== TOP 8 by COMBINED pf ====")
    print(out.sort_values("C_pf", ascending=False).head(8).to_string(index=False))
    print("\n==== TOP 8 by SHORT pf ====")
    print(out.sort_values("S_pf", ascending=False).head(8).to_string(index=False))
    print("\n==== TOP 8 by LONG pf ====")
    print(out.sort_values("L_pf", ascending=False).head(8).to_string(index=False))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True, choices=["v17b", "v17c", "v17f"])
    ap.add_argument("--mode", default="coarse",
                    choices=["coarse", "fine", "short_only", "long_only", "custom"])
    ap.add_argument("--output", default=None)
    ap.add_argument("--short-sl", type=float, default=0.0075)
    ap.add_argument("--short-tgt", type=float, default=0.008)
    ap.add_argument("--long-sl", type=float, default=0.0075)
    ap.add_argument("--long-tgt", type=float, default=0.008)
    args = ap.parse_args()

    version = args.version
    if version == "v17f":
        # v17f uses same base as v17b/c but needs explicit import so runtime paths are set
        import avwap_combined_runner_v17f_5min  # noqa: F401

    # Define grids
    if args.mode == "coarse":
        short_sls = [0.005, 0.0075, 0.010, 0.0125]
        short_tgts = [0.005, 0.008, 0.012, 0.016, 0.020]
        long_sls = [0.005, 0.0075, 0.010, 0.0125]
        long_tgts = [0.005, 0.008, 0.012, 0.016, 0.020]
    elif args.mode == "fine":
        # Narrow around defaults; caller should override via short_only / long_only sweeps
        short_sls = [args.short_sl + d for d in (-0.002, -0.001, 0, 0.001, 0.002)]
        short_tgts = [args.short_tgt + d for d in (-0.002, -0.001, 0, 0.001, 0.002)]
        long_sls = [args.long_sl + d for d in (-0.002, -0.001, 0, 0.001, 0.002)]
        long_tgts = [args.long_tgt + d for d in (-0.002, -0.001, 0, 0.001, 0.002)]
    elif args.mode == "short_only":
        # Vary only SHORT dimensions; fix LONG at args
        short_sls = [0.004, 0.005, 0.006, 0.0075, 0.009, 0.010, 0.012]
        short_tgts = [0.004, 0.006, 0.008, 0.010, 0.012, 0.015, 0.018, 0.020, 0.025]
        long_sls = [args.long_sl]
        long_tgts = [args.long_tgt]
    elif args.mode == "long_only":
        short_sls = [args.short_sl]
        short_tgts = [args.short_tgt]
        long_sls = [0.004, 0.005, 0.006, 0.0075, 0.009, 0.010, 0.012]
        long_tgts = [0.004, 0.006, 0.008, 0.010, 0.012, 0.015, 0.018, 0.020, 0.025]
    else:
        short_sls = [args.short_sl]
        short_tgts = [args.short_tgt]
        long_sls = [args.long_sl]
        long_tgts = [args.long_tgt]

    out_dir = _this_dir / "reports" / "sltgt_sweeps"
    out_dir.mkdir(parents=True, exist_ok=True)
    output = args.output or str(out_dir / f"sltgt_sweep_{version}_{args.mode}.csv")

    run_sweep(version, short_sls, short_tgts, long_sls, long_tgts, Path(output))


if __name__ == "__main__":
    main()
