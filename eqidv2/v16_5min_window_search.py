"""
Run 13 — Signal window search for v16 5min combined runner.
Tests expanded SHORT and LONG windows against Run 12 baseline.
Keeps all Run 12 parameters fixed (OR gate off, SL/TGT as-is).

Usage:
    conda run -n fin python v16_5min_window_search.py
    conda run -n fin python v16_5min_window_search.py --scenario-names baseline_r12,long_ext_1130
"""
from __future__ import annotations

import argparse
import io
import json
import math
import os
import time
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import time as dtime
from pathlib import Path
from typing import Dict, Iterator, List, Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner_v16_5min as base

ROOT   = Path(__file__).resolve().parent
OUT    = ROOT / "outputs_v16_5min_window_search"
MARKET_DAYS = 69   # backtest window size


# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------

def _scenarios() -> List[dict]:
    """
    Each entry sets SHORT and LONG signal windows to test.
    SHORT is always bounded by the 13:30 entry_cutoff in the runner.
    """
    S0 = [(dtime(9, 15), dtime(11,  0)), (dtime(12,  0), dtime(13, 30))]  # Run12 SHORT baseline
    L0 = [(dtime(9, 15), dtime(11,  0))]                                   # Run12 LONG baseline

    return [
        {
            "name": "baseline_r12",
            "short_windows": S0,
            "long_windows":  L0,
            "notes": "Run 12 baseline — no window change",
        },
        # --- LONG window expansions (SHORT unchanged) ---
        {
            "name": "long_ext_1130",
            "short_windows": S0,
            "long_windows":  [(dtime(9, 15), dtime(11, 30))],
            "notes": "LONG extended to 11:30 (+30 min morning)",
        },
        {
            "name": "long_ext_1200",
            "short_windows": S0,
            "long_windows":  [(dtime(9, 15), dtime(12,  0))],
            "notes": "LONG extended to 12:00 (+60 min morning, full pre-noon)",
        },
        {
            "name": "long_pm_1300",
            "short_windows": S0,
            "long_windows":  [(dtime(9, 15), dtime(11,  0)), (dtime(12,  0), dtime(13,  0))],
            "notes": "LONG adds early afternoon 12:00-13:00",
        },
        {
            "name": "long_pm_1330",
            "short_windows": S0,
            "long_windows":  [(dtime(9, 15), dtime(11,  0)), (dtime(12,  0), dtime(13, 30))],
            "notes": "LONG mirrors SHORT — full afternoon up to 13:30",
        },
        {
            "name": "long_ext_1130_pm_1300",
            "short_windows": S0,
            "long_windows":  [(dtime(9, 15), dtime(11, 30)), (dtime(12,  0), dtime(13,  0))],
            "notes": "LONG extended morning + early afternoon",
        },
        # --- SHORT window expansions (LONG unchanged) ---
        {
            "name": "short_ext_1130",
            "short_windows": [(dtime(9, 15), dtime(11, 30)), (dtime(12,  0), dtime(13, 30))],
            "long_windows":  L0,
            "notes": "SHORT morning extended to 11:30",
        },
        {
            "name": "short_pm_1400",
            "short_windows": [(dtime(9, 15), dtime(11,  0)), (dtime(12,  0), dtime(14,  0))],
            "long_windows":  L0,
            "notes": "SHORT afternoon extended to 14:00 (entry_cutoff=13:30 gates actual entries)",
        },
        # --- Both sides expanded ---
        {
            "name": "both_ext_1130",
            "short_windows": [(dtime(9, 15), dtime(11, 30)), (dtime(12,  0), dtime(13, 30))],
            "long_windows":  [(dtime(9, 15), dtime(11, 30))],
            "notes": "Both SHORT and LONG morning extended to 11:30",
        },
        {
            "name": "both_ext_1200",
            "short_windows": [(dtime(9, 15), dtime(12,  0)), (dtime(12,  0), dtime(13, 30))],
            "long_windows":  [(dtime(9, 15), dtime(12,  0))],
            "notes": "Both extended to 12:00 morning",
        },
        {
            "name": "both_pm",
            "short_windows": [(dtime(9, 15), dtime(11,  0)), (dtime(12,  0), dtime(14,  0))],
            "long_windows":  [(dtime(9, 15), dtime(11,  0)), (dtime(12,  0), dtime(13, 30))],
            "notes": "SHORT afternoon to 14:00, LONG adds full afternoon",
        },
    ]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pf(pnl: pd.Series) -> float:
    pos = float(pnl[pnl > 0].sum())
    neg = float(-pnl[pnl < 0].sum())
    return pos / neg if neg > 0 else (999.0 if pos > 0 else 0.0)


def _dd(pnl: pd.Series) -> float:
    if pnl.empty:
        return 0.0
    eq = pnl.cumsum()
    return float((eq.cummax() - eq).max())


def _metrics(df: pd.DataFrame, label: str = "") -> dict:
    if df.empty:
        return {"label": label, "trades": 0, "trade_days": 0,
                "tpd_market": 0.0, "pnl": 0.0, "avg_pnl": 0.0,
                "win_pct": 0.0, "day_win_pct": 0.0,
                "pf": 0.0, "max_dd": 0.0,
                "tgt_pct": 0.0, "sl_pct": 0.0, "eod_pct": 0.0}
    pnl  = df["pnl_pct"].astype(float)
    dpnl = df.groupby("trade_date")["pnl_pct"].sum()
    out  = df["outcome"].str.upper()
    n    = len(df)
    td   = int(df["trade_date"].nunique())
    return {
        "label":       label,
        "trades":      n,
        "trade_days":  td,
        "tpd_market":  round(n / MARKET_DAYS, 2),
        "pnl":         round(float(pnl.sum()), 2),
        "avg_pnl":     round(float(pnl.mean()), 4),
        "win_pct":     round(float((pnl > 0).mean() * 100), 1),
        "day_win_pct": round(float((dpnl > 0).mean() * 100), 1),
        "pf":          round(_pf(pnl), 3),
        "max_dd":      round(_dd(pnl), 2),
        "tgt_pct":     round(float((out == "TARGET").mean() * 100), 1),
        "sl_pct":      round(float((out == "SL").mean() * 100), 1),
        "eod_pct":     round(float((out == "EOD").mean() * 100), 1),
    }


@contextmanager
def _tmp(overrides: dict) -> Iterator[None]:
    saved = {k: getattr(base, k) for k in overrides}
    try:
        for k, v in overrides.items():
            setattr(base, k, v)
        yield
    finally:
        for k, v in saved.items():
            setattr(base, k, v)


def _configure(run_dir: Path, short_windows: list, long_windows: list):
    short_cfg = base.default_short_config(reports_dir=run_dir)
    long_cfg  = base.default_long_config_v9(reports_dir=run_dir)
    dir_15m   = base._resolve_15m_dir()
    for cfg in (short_cfg, long_cfg):
        cfg.dir_15m               = str(dir_15m)
        cfg.end_15m               = "_stocks_indicators_5min.parquet"
        cfg.market_regime_tickers = tuple(base.NIFTY_CONTEXT_TICKERS)
    short_cfg, long_cfg = base.apply_live_parity_profile(short_cfg, long_cfg)

    # Apply window overrides
    short_cfg.use_time_windows = True
    short_cfg.signal_windows   = list(short_windows)
    long_cfg.use_time_windows  = True
    long_cfg.signal_windows    = list(long_windows)

    # Force Run12 target
    if base.TEST_TARGET_OVERRIDE:
        short_cfg.target_pct = float(base.TEST_SHORT_TARGET_PCT)
        long_cfg.target_pct  = float(base.TEST_LONG_TARGET_PCT)

    # Parity flags
    if base.FORCE_LIVE_PARITY_MIN_BARS_LEFT:
        short_cfg.min_bars_left_after_entry = 0
        long_cfg.min_bars_left_after_entry  = 0
    if base.FORCE_LIVE_PARITY_DISABLE_TOPN:
        for cfg in (short_cfg, long_cfg):
            cfg.enable_topn_per_day = False
            cfg.topn_per_day        = 0

    # VIX scaling
    vix_map = base._load_india_vix(ROOT)
    for cfg in (short_cfg, long_cfg):
        cfg.vix_scale_enabled = base.VIX_SCALE_ENABLED
        if base.VIX_SCALE_ENABLED and vix_map:
            cfg.vix_daily       = vix_map
            cfg.vix_baseline    = base.VIX_BASELINE
            cfg.vix_scale_min   = base.VIX_SCALE_MIN
            cfg.vix_scale_max   = base.VIX_SCALE_MAX
            cfg.vix_scale_target = base.VIX_SCALE_TARGET
            cfg.vix_scale_sl    = base.VIX_SCALE_SL

    # Regime
    regime_map, _ = base.build_market_regime_map(short_cfg)
    for cfg in (short_cfg, long_cfg):
        if regime_map:
            cfg.market_regime_map          = regime_map
            cfg.enable_market_regime_filter = True
        else:
            cfg.enable_market_regime_filter = False

    return short_cfg, long_cfg


def _run_scenario(scenario: dict, workers: int, keep_csv: bool = True) -> dict:
    name          = scenario["name"]
    short_windows = scenario["short_windows"]
    long_windows  = scenario["long_windows"]
    run_dir       = OUT / name
    run_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    print(f"[{time.strftime('%H:%M:%S')}] {name}: scanning...", flush=True)

    # We need OR gate off for this run — already set in base globals as False
    short_cfg, long_cfg = _configure(run_dir, short_windows, long_windows)

    buf = io.StringIO()
    with redirect_stdout(buf), redirect_stderr(buf):
        short_df = base._run_side_parallel("SHORT", short_cfg, workers)
        long_df  = base._run_side_parallel("LONG",  long_cfg,  workers)

        # Nifty context
        if base.NIFTY_CONTEXT_ENABLED:
            mode_map, nifty_ret_map, _, _ = base._build_nifty_intraday_context(short_cfg)
            if mode_map:
                short_df, long_df = base._apply_nifty_intraday_context(
                    short_df, long_df, short_cfg, mode_map, nifty_ret_map)

        # OR-gate enrichment (needed even if gate is off — enriches or_high/or_low columns)
        if base.V16_OR_GATE_ENABLED:
            short_df, long_df = base._enrich_with_or_levels(
                short_df, long_df,
                dir_15m=short_cfg.dir_15m,
                parquet_suffix=short_cfg.end_15m,
            )

        short_df, long_df = base._replace_current_day_with_live_parity(short_df, long_df)
        short_df, long_df = base._apply_v16_post_scan_filters(short_df, long_df)

        # Resolve exits
        dir_1m = base._resolve_5min_dir()
        suffix = ".parquet"
        if dir_1m.is_dir():
            for sf in list(dir_1m.glob("*"))[:5]:
                if sf.suffix:
                    suffix = sf.suffix
                    break

        if not short_df.empty:
            short_df = base._resolve_exits_5min(
                short_df, dir_1m, suffix,
                short_cfg.parquet_engine,
                eod_exit_time=base.V15_EOD_EXIT_TIME,
            )
            short_df = base._add_notional_pnl(base._sort_trades_for_output(short_df))

        if not long_df.empty:
            long_df = base._resolve_exits_5min(
                long_df, dir_1m, suffix,
                long_cfg.parquet_engine,
                eod_exit_time=base.V15_EOD_EXIT_TIME,
            )
            long_df = base._add_notional_pnl(base._sort_trades_for_output(long_df))

    combined = pd.concat([short_df, long_df], ignore_index=True)
    combined = base._add_notional_pnl(base._sort_trades_for_output(combined))
    s_df = combined[combined["side"].str.upper() == "SHORT"].copy()
    l_df = combined[combined["side"].str.upper() == "LONG"].copy()

    elapsed = round(time.perf_counter() - t0, 1)

    s_m = _metrics(s_df, "SHORT")
    l_m = _metrics(l_df, "LONG")
    c_m = _metrics(combined, "COMBINED")

    win_fmt = lambda w: [(f"{a.strftime('%H:%M')}-{b.strftime('%H:%M')}") for a, b in w]

    result = {
        "scenario":      name,
        "notes":         scenario["notes"],
        "short_windows": str(win_fmt(short_windows)),
        "long_windows":  str(win_fmt(long_windows)),
        "elapsed_s":     elapsed,
        # combined
        "trades":        c_m["trades"],
        "tpd_market":    c_m["tpd_market"],
        "pnl":           c_m["pnl"],
        "win_pct":       c_m["win_pct"],
        "day_win_pct":   c_m["day_win_pct"],
        "pf":            c_m["pf"],
        "max_dd":        c_m["max_dd"],
        # short
        "s_trades":      s_m["trades"],
        "s_tpd":         s_m["tpd_market"],
        "s_pf":          s_m["pf"],
        "s_pnl":         s_m["pnl"],
        "s_win":         s_m["win_pct"],
        "s_day_win":     s_m["day_win_pct"],
        "s_dd":          s_m["max_dd"],
        # long
        "l_trades":      l_m["trades"],
        "l_tpd":         l_m["tpd_market"],
        "l_pf":          l_m["pf"],
        "l_pnl":         l_m["pnl"],
        "l_win":         l_m["win_pct"],
        "l_day_win":     l_m["day_win_pct"],
        "l_dd":          l_m["max_dd"],
    }

    if keep_csv:
        combined.to_csv(run_dir / f"{name}_trades.csv", index=False)

    print(
        f"[{time.strftime('%H:%M:%S')}] {name}: done in {elapsed}s | "
        f"trades={c_m['trades']} ({s_m['trades']}S+{l_m['trades']}L) | "
        f"PF={c_m['pf']:.3f} | PnL={c_m['pnl']:.1f}% | "
        f"DayWin={c_m['day_win_pct']:.1f}% | MaxDD={c_m['max_dd']:.2f}%",
        flush=True,
    )
    return result


def _rank(df: pd.DataFrame) -> pd.DataFrame:
    specs = [
        ("pf",          False, 0.28),
        ("pnl",         False, 0.22),
        ("day_win_pct", False, 0.20),
        ("win_pct",     False, 0.15),
        ("max_dd",      True,  0.15),
    ]
    out   = df.copy()
    score = np.zeros(len(out), dtype=float)
    for col, inv, w in specs:
        vals = out[col].astype(float)
        lo, hi = float(vals.min()), float(vals.max())
        if hi == lo:
            norm = np.full(len(out), 100.0)
        else:
            norm = (hi - vals.to_numpy()) / (hi - lo) * 100.0 if inv \
                   else (vals.to_numpy() - lo) / (hi - lo) * 100.0
        out[f"norm_{col}"] = np.round(norm, 2)
        score += norm * w
    out["score"] = np.round(score, 2)
    return out.sort_values(["score", "pf", "pnl"], ascending=[False, False, False]).reset_index(drop=True)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="V16 5min window search (Run 13)")
    p.add_argument("--scenario-names", default="",
                   help="Comma-separated names to run (empty = all)")
    p.add_argument("--max-workers", type=int,
                   default=max(1, int(os.getenv("EQIDV16_5MIN_MAX_WORKERS", "4"))))
    p.add_argument("--keep-top", type=int, default=15)
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    OUT.mkdir(parents=True, exist_ok=True)

    base.MAX_WORKERS           = args.max_workers
    base.ENABLE_LEGACY_CHARTS  = False
    base.ENABLE_ENHANCED_CHARTS = False

    scenarios = _scenarios()
    if args.scenario_names:
        allowed   = {n.strip() for n in args.scenario_names.split(",") if n.strip()}
        scenarios = [s for s in scenarios if s["name"] in allowed]
        print(f"[{time.strftime('%H:%M:%S')}] Running subset: {[s['name'] for s in scenarios]}")

    print(f"[{time.strftime('%H:%M:%S')}] {len(scenarios)} scenarios | workers={base.MAX_WORKERS}")

    rows = []
    for s in scenarios:
        try:
            rows.append(_run_scenario(s, base.MAX_WORKERS))
        except Exception as exc:
            print(f"ERROR in {s['name']}: {exc}", flush=True)
            rows.append({"scenario": s["name"], "notes": s["notes"],
                         "pf": 0.0, "pnl": 0.0, "day_win_pct": 0.0,
                         "win_pct": 0.0, "max_dd": 999.0, "trades": 0,
                         "error": str(exc)})

    summary = pd.DataFrame(rows)

    # Merge with existing if partial run
    summary_path = OUT / "window_summary.csv"
    if summary_path.exists() and args.scenario_names:
        existing = pd.read_csv(summary_path)
        run_names = summary["scenario"].unique().tolist()
        existing  = existing[~existing["scenario"].isin(run_names)]
        summary   = pd.concat([existing, summary], ignore_index=True)

    if not summary.empty and "pf" in summary.columns:
        ranked = _rank(summary.dropna(subset=["pf"]))
        ranked.head(args.keep_top).to_csv(summary_path, index=False)
        print(f"\n{'='*70}")
        print("WINDOW SEARCH RESULTS (ranked by composite score)")
        print(f"{'='*70}")
        cols = ["scenario", "trades", "tpd_market", "pf", "pnl",
                "win_pct", "day_win_pct", "max_dd", "l_trades", "l_pf", "l_day_win", "s_pf", "s_day_win"]
        print(ranked[[c for c in cols if c in ranked.columns]].head(args.keep_top).to_string(index=False))
        best = ranked.iloc[0]
        print(f"\nBest: {best['scenario']} | PF={best['pf']:.3f} | PnL={best['pnl']:.1f}% | "
              f"DayWin={best['day_win_pct']:.1f}% | MaxDD={best['max_dd']:.2f}% | "
              f"trades={int(best['trades'])}")
    else:
        summary.to_csv(summary_path, index=False)

    (OUT / "window_summary.json").write_text(
        json.dumps({"scenarios_run": [s["scenario"] for s in scenarios],
                    "summary_csv": str(summary_path)}, indent=2),
        encoding="utf-8",
    )
    print(f"\nSummary saved: {summary_path}")


if __name__ == "__main__":
    main()
