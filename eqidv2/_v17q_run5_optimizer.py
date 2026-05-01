# -*- coding: utf-8 -*-
"""
RUN 5 deep-optimization analysis pipeline.

Loads the Run 5 honest backtest CSV (all v17q lookahead fixes ON, full universe)
and runs a battery of post-hoc filter / ranking / cooldown experiments to
identify a deployable subset that maximizes a composite of:
   PF, win rate, daily win rate, and trade count, with bounded MaxDD.

Constraints (no full re-run needed for these):
- Filtering by setup, hour, RSI/ADX/QS/atr_pct buckets, NIFTY mode, ticker
- Day-of-week cuts
- Per-ticker/day cap, per-side/day cap, top-N per day
- Cooldown after SL
- Drop EOD outcomes
- Composite filters

Constraints that DO need a fresh backtest (deferred to validation step):
- New SL/TGT levels (Phase 2 must re-resolve)
- Trailing stop / BE rule changes (scanner must re-walk)
- Time-of-day exit cutoffs earlier than EOD (scanner must re-walk)
- New entry filters that change which signals fire (Phase 1 re-scan)

Outputs:
- run5_optimization_experiments.csv  -- one row per experiment
- run5_selected_trades.csv           -- final filtered trade set
- run5_daily_pnl_curve.csv           -- daily P&L + cumulative + drawdown
- run5_long_short_breakdown.csv      -- long/short side metrics
"""
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd
import numpy as np

OUT_DIR = Path(r"C:/TradingData/eqidv2/outputs_v17q_5min")
RUN5_CSV = OUT_DIR / "avwap_longshort_trades_v16_5min_ALL_DAYS_20260427_143701.csv"


# ============================================================================
# Metric primitives
# ============================================================================
def _pnl_cols(df: pd.DataFrame):
    pnl_p = pd.to_numeric(df.get("pnl_pct_price", df.get("pnl_pct", 0.0)), errors="coerce").fillna(0.0)
    pnl_l = pd.to_numeric(df.get("pnl_pct", 0.0), errors="coerce").fillna(0.0)
    return pnl_p, pnl_l


def metrics(df: pd.DataFrame) -> dict:
    n = len(df)
    if n == 0:
        return dict(
            n=0, target_pct=0.0, sl_pct=0.0, eod_pct=0.0,
            win_rate=0.0, pf=0.0, avg_pnl_p=0.0, sum_pnl_p=0.0, sum_pnl_lev=0.0,
            day_count=0, day_win_rate=0.0, max_dd_pct=0.0, max_dd_lev=0.0,
            sharpe=0.0, longs=0, shorts=0,
        )
    pnl_p, pnl_l = _pnl_cols(df)
    wins_p = pnl_p[pnl_p > 0].sum()
    losses_p = abs(pnl_p[pnl_p < 0].sum())
    pf = (wins_p / losses_p) if losses_p > 0 else float("inf")
    sum_pnl_p = pnl_p.sum()
    sum_pnl_lev = pnl_l.sum()

    # Daily
    d = df.copy()
    d["trade_date"] = pd.to_datetime(d["trade_date"], errors="coerce").dt.date
    daily = d.groupby("trade_date").apply(lambda g: pd.to_numeric(g["pnl_pct_price"], errors="coerce").fillna(0.0).sum())
    day_count = int(len(daily))
    day_win_rate = float((daily > 0).sum() / day_count * 100.0) if day_count else 0.0

    # Drawdown on price-return cumulative (compounded approximation: simple cumsum is fine for small per-trade %)
    daily_lev = d.groupby("trade_date").apply(lambda g: pd.to_numeric(g["pnl_pct"], errors="coerce").fillna(0.0).sum())
    cum = daily.cumsum()
    cum_lev = daily_lev.cumsum()
    max_dd_pct = float((cum.cummax() - cum).max()) if len(cum) else 0.0
    max_dd_lev = float((cum_lev.cummax() - cum_lev).max()) if len(cum_lev) else 0.0

    # Sharpe-like (per-day annualized)
    if day_count > 1:
        std = float(daily.std(ddof=1))
        sharpe = float(daily.mean() / std * np.sqrt(252)) if std > 0 else 0.0
    else:
        sharpe = 0.0

    return dict(
        n=n,
        target_pct=float((df["outcome"] == "TARGET").mean() * 100),
        sl_pct=float((df["outcome"] == "SL").mean() * 100),
        eod_pct=float((df["outcome"] == "EOD").mean() * 100),
        win_rate=float((pnl_p > 0).mean() * 100),
        pf=pf,
        avg_pnl_p=float(pnl_p.mean()),
        sum_pnl_p=float(sum_pnl_p),
        sum_pnl_lev=float(sum_pnl_lev),
        day_count=day_count,
        day_win_rate=day_win_rate,
        max_dd_pct=max_dd_pct,
        max_dd_lev=max_dd_lev,
        sharpe=sharpe,
        longs=int((df["side"] == "LONG").sum()),
        shorts=int((df["side"] == "SHORT").sum()),
    )


def composite_score(m: dict) -> float:
    """Joint score balancing PF, win rate, daily win, max DD, and trade count.

    Penalize tiny samples + tiny day counts. Reward high PF but cap pf at 3.0
    so a 200-trade outlier doesn't dominate.
    """
    if m["n"] < 50 or m["day_count"] < 30:
        return -1e6  # reject — too few trades / days for stability
    pf = min(m["pf"], 3.0)
    pf_score = (pf - 1.0) * 100  # PF 1.0 = 0, PF 1.5 = 50, PF 2.0 = 100
    win_score = (m["win_rate"] - 50) * 1.0
    day_win_score = (m["day_win_rate"] - 50) * 1.0
    dd_pen = -m["max_dd_pct"] * 0.5
    sample_bonus = min(np.log10(m["n"]) * 5, 15)
    sharpe_bonus = max(min(m["sharpe"] * 10, 30), -30)
    return pf_score + win_score + day_win_score + dd_pen + sample_bonus + sharpe_bonus


# ============================================================================
# Load + preprocess
# ============================================================================
def load_run5() -> pd.DataFrame:
    df = pd.read_csv(RUN5_CSV)
    # Normalize types
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["entry_time_ist"] = pd.to_datetime(df["entry_time_ist"], errors="coerce", utc=True)
    df["exit_time_ist"] = pd.to_datetime(df["exit_time_ist"], errors="coerce", utc=True)
    df["signal_time_ist"] = pd.to_datetime(df["signal_time_ist"], errors="coerce", utc=True)
    # Helpers
    df["entry_hour"] = df["entry_time_ist"].dt.tz_convert("Asia/Kolkata").dt.hour
    df["entry_minute"] = df["entry_time_ist"].dt.tz_convert("Asia/Kolkata").dt.minute
    df["entry_hm"] = df["entry_hour"] + df["entry_minute"] / 60.0
    df["dow"] = df["entry_time_ist"].dt.tz_convert("Asia/Kolkata").dt.day_name()
    df["holding_min"] = (df["exit_time_ist"] - df["entry_time_ist"]).dt.total_seconds() / 60.0
    return df


# ============================================================================
# Per-setup PF report
# ============================================================================
def per_setup_report(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (side, setup), g in df.groupby(["side", "setup"]):
        m = metrics(g)
        rows.append(dict(side=side, setup=setup, **m))
    out = pd.DataFrame(rows).sort_values("pf", ascending=False)
    return out


# ============================================================================
# Experiments
# ============================================================================
def run_experiment(name: str, df: pd.DataFrame, mask: pd.Series, notes: str = "") -> dict:
    sub = df.loc[mask].copy()
    m = metrics(sub)
    return dict(experiment=name, notes=notes, score=composite_score(m), **m)


def topn_per_day(df: pd.DataFrame, n: int, rank_col: str = "quality_score") -> pd.Series:
    """Return mask: keep top-N per (trade_date, side) by rank_col descending."""
    work = df.copy()
    work["_orig_idx"] = np.arange(len(work))
    work["_rank_val"] = pd.to_numeric(work[rank_col], errors="coerce").fillna(-1.0)
    work = work.sort_values(["trade_date", "side", "_rank_val"], ascending=[True, True, False], kind="mergesort")
    work["_rank_in_grp"] = work.groupby(["trade_date", "side"]).cumcount()
    keep_idx = work.loc[work["_rank_in_grp"] < n, "_orig_idx"].values
    return df.index.isin(keep_idx) if isinstance(df.index, pd.RangeIndex) else \
        df.reset_index().index.isin(keep_idx)


def cap_per_ticker_day(df: pd.DataFrame, cap: int) -> pd.Series:
    """Keep first `cap` trades per (trade_date, ticker, side) by entry_time."""
    work = df.copy()
    work["_orig_idx"] = np.arange(len(work))
    work = work.sort_values(["trade_date", "ticker", "side", "entry_time_ist"])
    work["_grp_idx"] = work.groupby(["trade_date", "ticker", "side"]).cumcount()
    keep_idx = work.loc[work["_grp_idx"] < cap, "_orig_idx"].values
    return df.reset_index(drop=True).index.isin(keep_idx)


def cooldown_after_sl(df: pd.DataFrame, cooldown_minutes: int = 60) -> pd.Series:
    """Mask: drop a trade if there was an SL on the same ticker within the
    last `cooldown_minutes` of its entry time."""
    work = df.sort_values(["ticker", "entry_time_ist"]).copy()
    keep = []
    last_sl: dict = {}
    for row in work.itertuples():
        et = row.entry_time_ist
        last = last_sl.get(row.ticker)
        if last is not None and (et - last).total_seconds() / 60.0 < cooldown_minutes:
            keep.append(False)
        else:
            keep.append(True)
        if str(row.outcome).upper() == "SL":
            last_sl[row.ticker] = row.exit_time_ist if pd.notna(row.exit_time_ist) else et
    work["_keep"] = keep
    keep_idx = work.index[work["_keep"]].tolist()
    return df.index.isin(keep_idx)


# ============================================================================
# Main pipeline
# ============================================================================
def main() -> int:
    df = load_run5()
    print(f"Loaded Run 5 CSV: {len(df)} trades")

    # Always operate on .reset_index(drop=True) to keep boolean masks aligned.
    df = df.reset_index(drop=True)

    print("\n=== Per-setup baseline ===")
    setup_report = per_setup_report(df)
    print(setup_report[["side", "setup", "n", "target_pct", "win_rate", "pf",
                         "sum_pnl_p", "max_dd_pct", "day_win_rate"]].round(2).to_string(index=False))

    # Identify "real-edge" setups by Run 5 honest PF >= 1.0
    edge_mask_long = setup_report.eval("side == 'LONG'  and pf >= 1.0 and n >= 30")
    edge_mask_short = setup_report.eval("side == 'SHORT' and pf >= 1.0 and n >= 30")
    edge_long  = set(setup_report.loc[edge_mask_long,  "setup"].tolist())
    edge_short = set(setup_report.loc[edge_mask_short, "setup"].tolist())
    print(f"\nReal-edge LONG setups  (PF>=1.0, n>=30): {edge_long}")
    print(f"Real-edge SHORT setups (PF>=1.0, n>=30): {edge_short}")

    # Also identify "marginal" setups (PF 0.85-1.0) — candidates for filter rescue
    marg_mask_long = setup_report.eval("side == 'LONG'  and pf >= 0.85 and pf < 1.0 and n >= 50")
    marg_mask_short = setup_report.eval("side == 'SHORT' and pf >= 0.85 and pf < 1.0 and n >= 50")
    marginal_long  = set(setup_report.loc[marg_mask_long,  "setup"].tolist())
    marginal_short = set(setup_report.loc[marg_mask_short, "setup"].tolist())
    print(f"Marginal LONG setups   (PF 0.85-1.0): {marginal_long}")
    print(f"Marginal SHORT setups  (PF 0.85-1.0): {marginal_short}")

    experiments = []

    # ---------- Baselines ----------
    experiments.append(run_experiment("E00_baseline_full_universe", df, df.index >= 0, "All Run 5 trades"))

    # ---------- E01: keep only PF>=1.0 setups (v17r baseline) ----------
    mask_v17r = (
        (df["side"].eq("LONG")  & df["setup"].isin(edge_long)) |
        (df["side"].eq("SHORT") & df["setup"].isin(edge_short))
    )
    experiments.append(run_experiment("E01_v17r_PF_ge_1.0_setups", df, mask_v17r,
                                      f"Setups: LONG={sorted(edge_long)}, SHORT={sorted(edge_short)}"))

    # ---------- E02: v17r + drop EOD outcomes ----------
    experiments.append(run_experiment("E02_v17r_no_EOD", df,
                                      mask_v17r & df["outcome"].ne("EOD"),
                                      "Drop EOD trades (only TARGET/SL)"))

    # ---------- E03: v17r + RSI thresholds ----------
    rsi = pd.to_numeric(df.get("rsi_signal", np.nan), errors="coerce")
    # LONG: RSI between 50 and 75; SHORT: RSI between 25 and 50
    experiments.append(run_experiment(
        "E03_v17r_rsi_window",
        df,
        mask_v17r & (
            (df["side"].eq("LONG")  & rsi.between(50, 75, inclusive="left")) |
            (df["side"].eq("SHORT") & rsi.between(25, 50, inclusive="left"))
        ),
        "LONG RSI in [50,75); SHORT RSI in [25,50)",
    ))

    # ---------- E04: v17r + ADX threshold ----------
    adx = pd.to_numeric(df.get("adx_signal", np.nan), errors="coerce")
    experiments.append(run_experiment(
        "E04_v17r_ADX_ge_25", df,
        mask_v17r & (adx >= 25.0),
        "ADX >= 25 (require trending bar)",
    ))

    # ---------- E05: v17r + ADX threshold 30 ----------
    experiments.append(run_experiment(
        "E05_v17r_ADX_ge_30", df,
        mask_v17r & (adx >= 30.0),
        "ADX >= 30 (stronger trend)",
    ))

    # ---------- E06: v17r + QS threshold ----------
    qs = pd.to_numeric(df.get("quality_score", np.nan), errors="coerce")
    experiments.append(run_experiment(
        "E06_v17r_QS_ge_5", df,
        mask_v17r & (qs >= 5.0),
        "quality_score >= 5",
    ))

    # ---------- E07: v17r + entry-hour gate (skip last hour) ----------
    experiments.append(run_experiment(
        "E07_v17r_no_late_entries", df,
        mask_v17r & (df["entry_hm"] < 14.0),
        "Entry strictly before 14:00 (avoid last 1.5 hr)",
    ))

    # ---------- E08: v17r + first-hour entries only ----------
    experiments.append(run_experiment(
        "E08_v17r_first_hour", df,
        mask_v17r & (df["entry_hm"] < 11.0),
        "Entry strictly before 11:00 (first 1h45m of session)",
    ))

    # ---------- E09: v17r + nifty context aligned ----------
    nctx = df.get("nifty_context_mode", pd.Series("BOTH", index=df.index)).astype(str)
    experiments.append(run_experiment(
        "E09_v17r_nifty_aligned", df,
        mask_v17r & (
            (df["side"].eq("LONG")  & nctx.isin(["LONG_ONLY", "BOTH"])) |
            (df["side"].eq("SHORT") & nctx.isin(["SHORT_ONLY", "BOTH"]))
        ),
        "NIFTY mode allows the side",
    ))

    # ---------- E10: v17r + nifty STRICTLY directional (drop BOTH) ----------
    experiments.append(run_experiment(
        "E10_v17r_nifty_strict_directional", df,
        mask_v17r & (
            (df["side"].eq("LONG")  & nctx.eq("LONG_ONLY")) |
            (df["side"].eq("SHORT") & nctx.eq("SHORT_ONLY"))
        ),
        "NIFTY strictly directional (LONG_ONLY for L, SHORT_ONLY for S)",
    ))

    # ---------- E11: v17r + drop Friday entries ----------
    experiments.append(run_experiment(
        "E11_v17r_no_friday", df,
        mask_v17r & df["dow"].ne("Friday"),
        "Skip Friday (weekend-overhang risk)",
    ))

    # ---------- E12: v17r + max 1 trade per ticker per day per side ----------
    experiments.append(run_experiment(
        "E12_v17r_one_per_ticker_day", df,
        mask_v17r & cap_per_ticker_day(df, 1),
        "Max 1 trade per (date,ticker,side)",
    ))

    # ---------- E13: v17r + top-N per day per side (by quality_score) ----------
    for n_top in (3, 5, 10):
        keep_top = topn_per_day(df, n_top, rank_col="quality_score")
        experiments.append(run_experiment(
            f"E13_v17r_topN={n_top}_per_day", df,
            mask_v17r & keep_top,
            f"Keep top-{n_top} per (date, side) by quality_score",
        ))

    # ---------- E14: v17r + cooldown 60 min after SL on same ticker ----------
    experiments.append(run_experiment(
        "E14_v17r_cooldown60_after_SL", df,
        mask_v17r & cooldown_after_sl(df, 60),
        "Skip trades within 60min of an SL on the same ticker",
    ))

    # ---------- E15: v17r + cooldown 120 min after SL ----------
    experiments.append(run_experiment(
        "E15_v17r_cooldown120_after_SL", df,
        mask_v17r & cooldown_after_sl(df, 120),
        "Skip trades within 120min of an SL on the same ticker",
    ))

    # ---------- E16: v17r + atr_pct threshold ----------
    atr_pct = pd.to_numeric(df.get("atr_pct_signal", np.nan), errors="coerce")
    experiments.append(run_experiment(
        "E16_v17r_atr_pct_0.003_to_0.012", df,
        mask_v17r & atr_pct.between(0.003, 0.012),
        "atr_pct in [0.003, 0.012] (mid-volatility regime)",
    ))

    # ---------- E17: v17r + nifty RS aligned (only when RS strongly favors side) ----------
    nrs = pd.to_numeric(df.get("nifty_rel_strength_pct", np.nan), errors="coerce")
    experiments.append(run_experiment(
        "E17_v17r_strong_RS", df,
        mask_v17r & (
            (df["side"].eq("LONG")  & (nrs >= 1.0)) |
            (df["side"].eq("SHORT") & (nrs <= -0.75))
        ),
        "LONG RS>=1.0%, SHORT RS<=-0.75%",
    ))

    # ---------- E18: marginal setups (PF 0.85-1.0) added with strict filters ----------
    mask_marg = (
        (df["side"].eq("LONG")  & df["setup"].isin(marginal_long)) |
        (df["side"].eq("SHORT") & df["setup"].isin(marginal_short))
    )
    experiments.append(run_experiment(
        "E18_v17r_plus_marginal_strict_QS_ADX", df,
        (mask_v17r) | (mask_marg & (qs >= 5) & (adx >= 25)),
        "v17r + marginal setups gated by QS>=5 & ADX>=25",
    ))

    # ---------- E19: combined "best filter stack" ----------
    # v17r setups + (ADX>=25 OR QS>=5) + nifty aligned + skip last 1.5hr + cooldown 60
    cool60 = cooldown_after_sl(df, 60)
    nifty_align = (
        (df["side"].eq("LONG")  & nctx.isin(["LONG_ONLY", "BOTH"])) |
        (df["side"].eq("SHORT") & nctx.isin(["SHORT_ONLY", "BOTH"]))
    )
    quality_pass = (adx >= 25) | (qs >= 5)
    experiments.append(run_experiment(
        "E19_v17r_BEST_STACK", df,
        mask_v17r & nifty_align & quality_pass & (df["entry_hm"] < 14.0) & cool60,
        "v17r + nifty aligned + (ADX>=25 OR QS>=5) + entry<14:00 + cooldown 60",
    ))

    # ---------- E20: above + drop Friday ----------
    experiments.append(run_experiment(
        "E20_v17r_BEST_STACK_no_friday", df,
        mask_v17r & nifty_align & quality_pass & (df["entry_hm"] < 14.0) & cool60 & df["dow"].ne("Friday"),
        "E19 + drop Friday",
    ))

    # ---------- E21: STRICT — only top 3 per day with all filters ----------
    keep_top3 = topn_per_day(df, 3, rank_col="quality_score")
    experiments.append(run_experiment(
        "E21_v17r_top3_BEST_STACK", df,
        mask_v17r & nifty_align & quality_pass & (df["entry_hm"] < 14.0) & cool60 & keep_top3,
        "E19 + top3 per (date,side)",
    ))

    # ---------- E22: SHORT-only (since SHORT.A_MOD is the strongest setup) ----------
    experiments.append(run_experiment(
        "E22_v17r_short_only", df,
        df["side"].eq("SHORT") & df["setup"].isin(edge_short),
        "Only SHORT.A_MOD_BREAK_C1_LOW (PF 1.53)",
    ))

    # ---------- E23: LONG-only (since LONG.B_AVWAP is one piece) ----------
    experiments.append(run_experiment(
        "E23_v17r_long_only", df,
        df["side"].eq("LONG") & df["setup"].isin(edge_long),
        "Only LONG.B_AVWAP_RECLAIM_REVERSAL (PF 1.29)",
    ))

    # ---------- Save experiment results ----------
    results = pd.DataFrame(experiments)
    cols = ["experiment", "score", "n", "longs", "shorts", "target_pct", "win_rate", "pf",
            "day_count", "day_win_rate", "max_dd_pct", "max_dd_lev",
            "sum_pnl_p", "sum_pnl_lev", "avg_pnl_p", "sharpe", "sl_pct", "eod_pct", "notes"]
    results = results[cols].sort_values("score", ascending=False).reset_index(drop=True)
    out_csv = OUT_DIR / "run5_optimization_experiments.csv"
    results.to_csv(out_csv, index=False)
    print(f"\nWrote {out_csv} ({len(results)} experiments)")

    print("\n=== Top 12 experiments by composite score ===")
    show = results.head(12).copy()
    print(show[["experiment", "n", "win_rate", "pf", "day_win_rate", "max_dd_pct",
                 "sum_pnl_lev", "score"]].round(2).to_string(index=False))

    # Best experiment selection -- additional safety constraints
    eligible = results[
        (results["n"] >= 100) &
        (results["day_count"] >= 60) &
        (results["pf"] >= 1.4) &
        (results["max_dd_pct"] < 50.0)
    ].copy()
    if eligible.empty:
        print("\nNo experiment meets strict eligibility (n>=100, days>=60, PF>=1.4, DD<50%).")
        print("Relaxing to PF>=1.3...")
        eligible = results[
            (results["n"] >= 100) &
            (results["day_count"] >= 60) &
            (results["pf"] >= 1.3) &
            (results["max_dd_pct"] < 50.0)
        ].copy()
    best = eligible.sort_values("score", ascending=False).head(1)
    if best.empty:
        print("Falling back to highest-score experiment regardless of constraints.")
        best = results.head(1)
    best_name = best.iloc[0]["experiment"]
    print(f"\n>>> Selected experiment: {best_name}")
    print(best.iloc[0].to_string())

    # Find the mask of the selected experiment by re-running it
    selected_mask = _rebuild_mask(best_name, df, edge_long, edge_short, marginal_long, marginal_short,
                                   nctx, qs, adx, rsi, atr_pct, nrs)
    selected = df.loc[selected_mask].copy()

    # Save selected trades
    sel_csv = OUT_DIR / "run5_selected_trades.csv"
    selected.to_csv(sel_csv, index=False)
    print(f"Wrote {sel_csv} ({len(selected)} trades)")

    # Daily P&L curve + drawdown
    sel = selected.copy()
    sel["trade_date"] = pd.to_datetime(sel["trade_date"]).dt.date
    daily_p = sel.groupby("trade_date")["pnl_pct_price"].sum()
    daily_l = sel.groupby("trade_date")["pnl_pct"].sum()
    daily_full = pd.DataFrame({
        "trade_date": daily_p.index,
        "daily_pnl_pct_price": daily_p.values,
        "daily_pnl_pct_levered": daily_l.values,
    })
    daily_full["cum_pnl_price"] = daily_full["daily_pnl_pct_price"].cumsum()
    daily_full["cum_pnl_lev"] = daily_full["daily_pnl_pct_levered"].cumsum()
    daily_full["high_water_lev"] = daily_full["cum_pnl_lev"].cummax()
    daily_full["drawdown_lev"] = daily_full["cum_pnl_lev"] - daily_full["high_water_lev"]
    daily_csv = OUT_DIR / "run5_daily_pnl_curve.csv"
    daily_full.to_csv(daily_csv, index=False)
    print(f"Wrote {daily_csv} ({len(daily_full)} days)")

    # Long/Short breakdown
    breakdown = []
    for side in ("LONG", "SHORT", "ALL"):
        sub = selected if side == "ALL" else selected[selected["side"] == side]
        m = metrics(sub)
        breakdown.append(dict(side=side, **m))
    breakdown_df = pd.DataFrame(breakdown)
    breakdown_csv = OUT_DIR / "run5_long_short_breakdown.csv"
    breakdown_df.to_csv(breakdown_csv, index=False)
    print(f"Wrote {breakdown_csv}")

    # Per-setup within selected
    print("\n=== Selected per-setup ===")
    print(per_setup_report(selected)[["side","setup","n","target_pct","win_rate","pf",
                                        "sum_pnl_p","max_dd_pct","day_win_rate"]].round(2).to_string(index=False))

    print("\n=== Final selected metrics ===")
    print(pd.Series(metrics(selected)).round(3).to_string())

    return 0


def _rebuild_mask(name, df, edge_long, edge_short, marginal_long, marginal_short,
                  nctx, qs, adx, rsi, atr_pct, nrs):
    """Rebuild the mask for a named experiment so we can re-extract its trade set."""
    base_v17r = (
        (df["side"].eq("LONG")  & df["setup"].isin(edge_long)) |
        (df["side"].eq("SHORT") & df["setup"].isin(edge_short))
    )
    cool60 = cooldown_after_sl(df, 60)
    cool120 = cooldown_after_sl(df, 120)
    nifty_align = (
        (df["side"].eq("LONG")  & nctx.isin(["LONG_ONLY", "BOTH"])) |
        (df["side"].eq("SHORT") & nctx.isin(["SHORT_ONLY", "BOTH"]))
    )
    quality_pass = (adx >= 25) | (qs >= 5)

    if name == "E00_baseline_full_universe":
        return df.index >= 0
    if name == "E01_v17r_PF_ge_1.0_setups":
        return base_v17r
    if name == "E02_v17r_no_EOD":
        return base_v17r & df["outcome"].ne("EOD")
    if name == "E03_v17r_rsi_window":
        return base_v17r & (
            (df["side"].eq("LONG")  & rsi.between(50, 75, inclusive="left")) |
            (df["side"].eq("SHORT") & rsi.between(25, 50, inclusive="left"))
        )
    if name == "E04_v17r_ADX_ge_25":
        return base_v17r & (adx >= 25.0)
    if name == "E05_v17r_ADX_ge_30":
        return base_v17r & (adx >= 30.0)
    if name == "E06_v17r_QS_ge_5":
        return base_v17r & (qs >= 5.0)
    if name == "E07_v17r_no_late_entries":
        return base_v17r & (df["entry_hm"] < 14.0)
    if name == "E08_v17r_first_hour":
        return base_v17r & (df["entry_hm"] < 11.0)
    if name == "E09_v17r_nifty_aligned":
        return base_v17r & nifty_align
    if name == "E10_v17r_nifty_strict_directional":
        return base_v17r & (
            (df["side"].eq("LONG")  & nctx.eq("LONG_ONLY")) |
            (df["side"].eq("SHORT") & nctx.eq("SHORT_ONLY"))
        )
    if name == "E11_v17r_no_friday":
        return base_v17r & df["dow"].ne("Friday")
    if name == "E12_v17r_one_per_ticker_day":
        return base_v17r & cap_per_ticker_day(df, 1)
    if name.startswith("E13_v17r_topN="):
        n_top = int(name.split("=")[1].split("_")[0])
        return base_v17r & topn_per_day(df, n_top, rank_col="quality_score")
    if name == "E14_v17r_cooldown60_after_SL":
        return base_v17r & cool60
    if name == "E15_v17r_cooldown120_after_SL":
        return base_v17r & cool120
    if name == "E16_v17r_atr_pct_0.003_to_0.012":
        return base_v17r & atr_pct.between(0.003, 0.012)
    if name == "E17_v17r_strong_RS":
        return base_v17r & (
            (df["side"].eq("LONG")  & (nrs >= 1.0)) |
            (df["side"].eq("SHORT") & (nrs <= -0.75))
        )
    if name == "E18_v17r_plus_marginal_strict_QS_ADX":
        mask_marg = (
            (df["side"].eq("LONG")  & df["setup"].isin(marginal_long)) |
            (df["side"].eq("SHORT") & df["setup"].isin(marginal_short))
        )
        return (base_v17r) | (mask_marg & (qs >= 5) & (adx >= 25))
    if name == "E19_v17r_BEST_STACK":
        return base_v17r & nifty_align & quality_pass & (df["entry_hm"] < 14.0) & cool60
    if name == "E20_v17r_BEST_STACK_no_friday":
        return base_v17r & nifty_align & quality_pass & (df["entry_hm"] < 14.0) & cool60 & df["dow"].ne("Friday")
    if name == "E21_v17r_top3_BEST_STACK":
        return base_v17r & nifty_align & quality_pass & (df["entry_hm"] < 14.0) & cool60 & topn_per_day(df, 3, "quality_score")
    if name == "E22_v17r_short_only":
        return df["side"].eq("SHORT") & df["setup"].isin(edge_short)
    if name == "E23_v17r_long_only":
        return df["side"].eq("LONG") & df["setup"].isin(edge_long)
    raise KeyError(name)


if __name__ == "__main__":
    sys.exit(main())
