"""
V17 research framework — experiment definitions.

All experiments are config-driven and run against the saved v17b/c/d/e/f trade CSVs
(no re-running of the 17-minute backtest per variant).

Approach:
  - SHORT-side variation uses v17d as the permissive universe (294 shorts, PF 1.648)
    and we test whether smarter filters / splits / selection can beat v17b's
    tighter universe (164 shorts, PF 2.463) on combined metrics.
  - LONG-side uses the shared 960-trade v17c-f long leg as baseline.
  - Exits are re-resolved separately via exits.py.

Combined PnL per experiment = (selected SHORT trades) + (selected LONG trades).
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .core import compute_metrics, load_all_v17, rank_variants


RESULTS_DIR = Path(__file__).resolve().parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _record(label: str, trades: pd.DataFrame) -> Dict:
    m = compute_metrics(trades)
    row = {"variant": label, **m.as_row()}
    return row


def _shorts(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["side"] == "SHORT"].copy()


def _longs(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["side"] == "LONG"].copy()


def _combine(shorts: pd.DataFrame, longs: pd.DataFrame) -> pd.DataFrame:
    out = pd.concat([shorts, longs], ignore_index=True).sort_values(
        ["entry_time_ist", "ticker"], kind="stable"
    ).reset_index(drop=True)
    return out


# ---------------------------------------------------------------------------
# EXPERIMENT SUITE
# ---------------------------------------------------------------------------
def run_all_experiments() -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    bundles = load_all_v17()
    v17b, v17c, v17d, v17e, v17f = (bundles[k] for k in ["v17b", "v17c", "v17d", "v17e", "v17f"])

    rows: List[Dict] = []
    detail: Dict[str, pd.DataFrame] = {}

    # --- Baselines ---
    for name, df in bundles.items():
        rows.append(_record(f"BASELINE_{name}", df))

    # ------------------------------------------------------------
    # EXP 1 — SHORT-side drop-one-filter on v17d (permissive short universe)
    # Each sub-variant takes v17d shorts and DROPS one specific bucket, then
    # combines with v17d longs (960 shared).
    # Goal: check which v17d filters are actually helping vs hurting.
    # ------------------------------------------------------------
    d_shorts = _shorts(v17d)
    d_longs = _longs(v17d)

    # Pockets identified in the production filter logs (v17d / v17f / v17e filters):
    drop_recipes = {
        "d1_drop_rsi_20_25": lambda s: s[~((pd.to_numeric(s["rsi_signal"], errors="coerce") >= 20) &
                                          (pd.to_numeric(s["rsi_signal"], errors="coerce") < 25))],
        "d2_drop_rsi_25_30": lambda s: s[~((pd.to_numeric(s["rsi_signal"], errors="coerce") >= 25) &
                                          (pd.to_numeric(s["rsi_signal"], errors="coerce") < 30))],
        "d3_drop_rsi_30_35": lambda s: s[~((pd.to_numeric(s["rsi_signal"], errors="coerce") >= 30) &
                                          (pd.to_numeric(s["rsi_signal"], errors="coerce") < 35))],
        "d4_drop_adx_ge_45":  lambda s: s[~(pd.to_numeric(s["adx_signal"], errors="coerce") >= 45)],
        "d5_drop_adx_lt_22":  lambda s: s[~(pd.to_numeric(s["adx_signal"], errors="coerce") < 22)],
        "d6_drop_avwap_0p5_1p0": lambda s: s[~((pd.to_numeric(s["avwap_dist_atr_signal"], errors="coerce") >= 0.5) &
                                              (pd.to_numeric(s["avwap_dist_atr_signal"], errors="coerce") < 1.0))],
        "d7_drop_avwap_ge_2p0":  lambda s: s[~(pd.to_numeric(s["avwap_dist_atr_signal"], errors="coerce") >= 2.0)],
        "d8_drop_before_0945":   lambda s: s[~(s["entry_minute"] < 9 * 60 + 45)],
        "d9_drop_after_1400":    lambda s: s[~(s["entry_minute"] >= 14 * 60)],
        "d10_drop_after_1330":   lambda s: s[~(s["entry_minute"] >= 13 * 60 + 30)],
        "d11_drop_1215_1245":    lambda s: s[~((s["entry_minute"] >= 12*60+15) & (s["entry_minute"] < 12*60+45))],
        "d12_drop_rs_gt_neg0p25":lambda s: s[~(pd.to_numeric(s["nifty_rel_strength_pct"], errors="coerce") > -0.25)],
        "d13_drop_rs_gt_neg0p40":lambda s: s[~(pd.to_numeric(s["nifty_rel_strength_pct"], errors="coerce") > -0.40)],
        "d14_drop_atr_ge_0p7":   lambda s: s[~(pd.to_numeric(s["atr_pct_signal"], errors="coerce") >= 0.7)],
        "d15_drop_pullback_setup": lambda s: s[~(s["setup"].astype(str).str.contains("PULLBACK", case=False, na=False))],
        "d16_drop_vix_gt_14":    lambda s: s[~(pd.to_numeric(s["india_vix"], errors="coerce") > 14)],
        "d17_drop_vix_gt_13":    lambda s: s[~(pd.to_numeric(s["india_vix"], errors="coerce") > 13)],
    }

    for label, fn in drop_recipes.items():
        s = fn(d_shorts)
        combined = _combine(s, d_longs)
        rows.append(_record(f"E1_{label}", combined))
        detail[f"E1_{label}"] = combined

    # ------------------------------------------------------------
    # EXP 2 — Session-split SHORT + LONG. Three bins.
    # ------------------------------------------------------------
    bins = {
        "morning_0945_1130": (9*60+45, 11*60+30),
        "midday_1130_1330":  (11*60+30, 13*60+30),
        "afternoon_1330_1500": (13*60+30, 15*60),
    }
    # use v17f (best throughput/quality balance) as the base trade set
    f_shorts = _shorts(v17f)
    f_longs  = _longs(v17f)

    for b_label, (a, b) in bins.items():
        s = f_shorts[(f_shorts["entry_minute"] >= a) & (f_shorts["entry_minute"] < b)].copy()
        l = f_longs[(f_longs["entry_minute"] >= a) & (f_longs["entry_minute"] < b)].copy()
        rows.append(_record(f"E2_short_only_{b_label}", s))
        rows.append(_record(f"E2_long_only_{b_label}", l))
        rows.append(_record(f"E2_combined_{b_label}", _combine(s, l)))

    # ------------------------------------------------------------
    # EXP 3 — Quality-score (QS) bucket contribution on v17f longs and v17d shorts
    # ------------------------------------------------------------
    for side_name, side_df in [("long", f_longs), ("short", d_shorts)]:
        qs = pd.to_numeric(side_df["quality_score"], errors="coerce")
        edges = [0, 4, 5, 6, 7, 8, 9, 10, 12, 99]
        for i in range(len(edges)-1):
            lo, hi = edges[i], edges[i+1]
            sub = side_df[(qs >= lo) & (qs < hi)]
            rows.append(_record(f"E3_QS_{side_name}_[{lo},{hi})", sub))

    # ------------------------------------------------------------
    # EXP 4 — Top-N per day by quality score (portfolio throttle)
    # Use v17f as base (1229 trades, best PnL) — we test if trimming to top-N
    # improves PF/DD without sacrificing too much throughput.
    # ------------------------------------------------------------
    for n in [3, 5, 7, 10, 14]:
        g = v17f.copy()
        g["_rk"] = g.groupby("trade_day")["quality_score"].rank(method="first", ascending=False)
        top = g[g["_rk"] <= n].drop(columns=["_rk"])
        rows.append(_record(f"E4_topN_{n}_per_day", top))
        detail[f"E4_topN_{n}_per_day"] = top

    # ------------------------------------------------------------
    # EXP 5 — VIX regime gates (using india_vix column already in CSV)
    # ------------------------------------------------------------
    for vmax in [11, 12, 13, 14, 15, 16, 99]:
        sub = v17f[pd.to_numeric(v17f["india_vix"], errors="coerce") <= vmax]
        rows.append(_record(f"E5_vix_le_{vmax}", sub))

    # ------------------------------------------------------------
    # EXP 6 — Setup-family contribution (drop each named setup from v17f)
    # ------------------------------------------------------------
    all_setups = sorted(v17f["setup"].astype(str).unique().tolist())
    for setup in all_setups:
        kept = v17f[v17f["setup"] != setup]
        rows.append(_record(f"E6_drop_setup_{setup}", kept))

    # ------------------------------------------------------------
    # EXP 7 — SHORT-dedicated recipes (custom combinations):
    #   S1: v17d shorts minus weak-PF pockets (combine several drops)
    #   S2: v17e shorts (already tight) + looser RS threshold
    #   S3: SHORT morning (09:45–11:30) + SHORT afternoon (13:30–15:00) only
    #   S4: SHORT with RS < -0.50% only (forced weakness)
    #   S5: SHORT with avwap_dist [1.0,3.0) ATR only (clean mid/far zone)
    # ------------------------------------------------------------
    def _s1(shorts):
        s = shorts.copy()
        s = s[~((pd.to_numeric(s["rsi_signal"], errors="coerce") >= 20) &
                (pd.to_numeric(s["rsi_signal"], errors="coerce") < 30))]
        s = s[~(pd.to_numeric(s["adx_signal"], errors="coerce") >= 45)]
        s = s[~((pd.to_numeric(s["avwap_dist_atr_signal"], errors="coerce") >= 0.5) &
                (pd.to_numeric(s["avwap_dist_atr_signal"], errors="coerce") < 1.0))]
        s = s[~(pd.to_numeric(s["avwap_dist_atr_signal"], errors="coerce") >= 2.5)]
        s = s[~(s["setup"].astype(str).str.contains("PULLBACK", case=False, na=False))]
        s = s[~((s["entry_minute"] >= 12*60+15) & (s["entry_minute"] < 12*60+45))]
        s = s[~(pd.to_numeric(s["nifty_rel_strength_pct"], errors="coerce") > -0.25)]
        return s

    def _s2(shorts):
        s = shorts.copy()
        # keep only shorts where relative strength < -0.40% (clear weakness)
        s = s[pd.to_numeric(s["nifty_rel_strength_pct"], errors="coerce") <= -0.40]
        return s

    def _s3(shorts):
        s = shorts.copy()
        morning = (s["entry_minute"] >= 9*60+45) & (s["entry_minute"] < 11*60+30)
        afternoon = (s["entry_minute"] >= 13*60+30) & (s["entry_minute"] < 15*60)
        return s[morning | afternoon]

    def _s4(shorts):
        s = shorts.copy()
        return s[pd.to_numeric(s["nifty_rel_strength_pct"], errors="coerce") <= -0.50]

    def _s5(shorts):
        s = shorts.copy()
        av = pd.to_numeric(s["avwap_dist_atr_signal"], errors="coerce")
        return s[(av >= 1.0) & (av < 3.0)]

    short_recipes = {
        "S1_v17d_stacked_drops": _s1(d_shorts),
        "S2_v17d_rs_le_neg0p40": _s2(d_shorts),
        "S3_v17f_session_split": _s3(f_shorts),
        "S4_v17d_rs_le_neg0p50": _s4(d_shorts),
        "S5_v17d_avwap_1p0_3p0": _s5(d_shorts),
        "S6_v17e_as_is":         _shorts(v17e),
        "S7_v17b_as_is":         _shorts(v17b),
        "S8_v17f_as_is":         _shorts(v17f),
    }

    # also save SHORT-only standalone metrics
    for label, s in short_recipes.items():
        rows.append(_record(f"E7_SHORT_{label}", s))
        detail[f"E7_SHORT_{label}"] = s

    # ------------------------------------------------------------
    # EXP 8 — LONG-dedicated recipes:
    #   L1: v17f longs (shared 960 baseline)
    #   L2: v17f longs + drop QS[10,12) (checking if highest QS is hurting)
    #   L3: v17f longs with RSI [60,65) dropped (already done in v17b; test impact on v17f universe)
    #   L4: v17f longs morning only
    #   L5: v17f longs excluding 15:00+ (late)
    #   L6: v17f longs QS >= 5 only
    # ------------------------------------------------------------
    long_recipes = {
        "L1_v17f_as_is": f_longs,
        "L2_drop_QS_10_12": f_longs[~((pd.to_numeric(f_longs["quality_score"], errors="coerce") >= 10) &
                                       (pd.to_numeric(f_longs["quality_score"], errors="coerce") < 12))],
        "L3_drop_RSI_60_65": f_longs[~((pd.to_numeric(f_longs["rsi_signal"], errors="coerce") >= 60) &
                                        (pd.to_numeric(f_longs["rsi_signal"], errors="coerce") < 65))],
        "L4_morning_only":   f_longs[(f_longs["entry_minute"] >= 9*60+45) & (f_longs["entry_minute"] < 11*60+30)],
        "L5_drop_after_1500":f_longs[f_longs["entry_minute"] < 15*60],
        "L6_QS_ge_5":        f_longs[pd.to_numeric(f_longs["quality_score"], errors="coerce") >= 5],
        "L7_QS_ge_6":        f_longs[pd.to_numeric(f_longs["quality_score"], errors="coerce") >= 6],
    }
    for label, l in long_recipes.items():
        rows.append(_record(f"E8_LONG_{label}", l))
        detail[f"E8_LONG_{label}"] = l

    # ------------------------------------------------------------
    # EXP 9 — COMPOSITE: best SHORT × best LONG
    # ------------------------------------------------------------
    # Pick top performing individual legs then combine
    # (Auto-select top 3 shorts by balance and top 3 longs, form Cartesian product)
    # For now, pre-seed with manually attractive combos.
    composite_pairs = [
        ("v17b_short + v17f_long", _shorts(v17b), f_longs),
        ("v17e_short + v17f_long", _shorts(v17e), f_longs),
        ("S1_stacked_short + v17f_long", short_recipes["S1_v17d_stacked_drops"], f_longs),
        ("S2_rs_04_short + v17f_long", short_recipes["S2_v17d_rs_le_neg0p40"], f_longs),
        ("S1_stacked_short + L5_long", short_recipes["S1_v17d_stacked_drops"], long_recipes["L5_drop_after_1500"]),
        ("S1_stacked_short + L6_long", short_recipes["S1_v17d_stacked_drops"], long_recipes["L6_QS_ge_5"]),
        ("S2_rs_04_short + L5_long", short_recipes["S2_v17d_rs_le_neg0p40"], long_recipes["L5_drop_after_1500"]),
        ("v17e_short + L6_long", _shorts(v17e), long_recipes["L6_QS_ge_5"]),
        ("v17e_short + L5_long", _shorts(v17e), long_recipes["L5_drop_after_1500"]),
        ("S2_rs_04_short + L6_long", short_recipes["S2_v17d_rs_le_neg0p40"], long_recipes["L6_QS_ge_5"]),
    ]
    for label, s, l in composite_pairs:
        combined = _combine(s, l)
        rows.append(_record(f"E9_{label}", combined))
        detail[f"E9_{label}"] = combined

    summary = pd.DataFrame(rows)
    return summary, detail


def main():
    summary, detail = run_all_experiments()
    summary_path = RESULTS_DIR / "experiments_summary.csv"
    summary.to_csv(summary_path, index=False)
    print(f"[OK] wrote {summary_path}  ({len(summary)} variants)")

    ranked = rank_variants(summary)
    ranked_path = RESULTS_DIR / "experiments_ranked.csv"
    ranked.to_csv(ranked_path, index=False)
    print(f"[OK] wrote {ranked_path}")

    # Save detailed trades for top-10 composite variants
    for k, v in list(detail.items())[:30]:
        safe = k.replace(" ", "_").replace("+", "and").replace("[", "").replace(")", "").replace(",", "_").replace("/", "_")
        v.to_csv(RESULTS_DIR / f"trades_{safe}.csv", index=False)

    # Print top-15 by balance score
    cols = ["variant", "n_trades", "n_long", "n_short", "trades_per_day", "win_rate",
            "profit_factor", "max_drawdown_pct", "sum_pnl_pct", "sharpe_ann",
            "short_pf", "short_n", "balance_score"]
    print("\n=== TOP 15 by balance score ===")
    print(ranked[cols].head(15).to_string(index=False))
    return summary, ranked


if __name__ == "__main__":
    main()
