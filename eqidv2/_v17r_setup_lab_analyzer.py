# -*- coding: utf-8 -*-
"""
v17r SETUP LAB ANALYZER -- pure pandas, no fresh backtest.

Runs Stages 0, 1, 2, 3, 4, 7, 8 of the v17r mission against the existing
v17t_live trade CSV (and v17q Run-6 cross-reference CSV). Stages 5 and 6
(target/SL sensitivity, execution stress) are deferred -- they require
fresh exit re-resolution.

Outputs all CSVs to: C:/TradingData/eqidv2/outputs_v17r_setup_lab_5min/
"""
from __future__ import annotations

import os
import math
from pathlib import Path
from typing import Dict, List, Tuple, Iterable, Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_OUT_DIR = Path("C:/TradingData/eqidv2/outputs_v17r_setup_lab_5min")
BASE_OUT_DIR.mkdir(parents=True, exist_ok=True)

V17T_LIVE_CSV = Path(
    "C:/TradingData/eqidv2/outputs_v17t_live_5min/"
    "avwap_longshort_trades_v16_5min_ALL_DAYS_20260428_105911.csv"
)
V17Q_CSV = Path(
    "C:/TradingData/eqidv2/outputs_v17q_5min/"
    "avwap_longshort_trades_v16_5min_ALL_DAYS_20260427_172331.csv"
)


# Train/OOS dates per spec (§11)
TRAIN_END = pd.Timestamp("2026-01-31")
OOS_START = pd.Timestamp("2026-02-01")


# v17t_live's existing Phase 5d AGGRESSIVE spec, copied verbatim. Used as
# the comparison baseline -- not as the answer.
V17T_DEEP_FILTER_SPEC: Dict[Tuple[str, str], List[Tuple[str, str, float]]] = {
    ("LONG",  "A_MOD_BREAK_C1_HIGH"):
        [("avwap_dist_atr_signal", ">=", 1.4933)],
    ("LONG",  "A_MOD_CLOSE_CONTINUATION_BREAK"):
        [("stochk_signal", ">=", 93.7635)],
    ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"):
        [("avwap_dist_atr_signal", "<=", 2.0839)],
    ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"):
        [("quality_score", ">=", 8.5893)],
    ("LONG",  "C_OR_BREAKOUT"):
        [("quality_score", ">=", 2.3487),
         ("atr_pct_signal", "<=", 0.0086),
         ("avwap_dist_atr_signal", ">=", 1.9958)],
    ("LONG",  "D_EMA20_BOUNCE"):
        [("quality_score", ">=", 2.1436), ("adx_signal", "<=", 40.3457)],
    ("LONG",  "G_HIGHER_HIGH_BREAK"):
        [("quality_score", ">=", 2.1540),
         ("entry_hour", "<=", 10.4167),
         ("avwap_dist_atr_signal", ">=", 1.6677)],
    ("SHORT", "A_MOD_BREAK_C1_LOW"):
        [],
    ("SHORT", "C_OR_BREAKDOWN"):
        [("avwap_dist_atr_signal", ">=", 1.5759),
         ("atr_pct_signal", ">=", 0.0041)],
    ("SHORT", "D_AVWAP_LOSE_REVERSAL"):
        [("avwap_dist_atr_signal", ">=", 1.4375)],
    ("SHORT", "D_EMA20_REJECTION"):
        [("quality_score", ">=", 0.3087), ("entry_hour", "<=", 10.0)],
    ("SHORT", "G_LOWER_LOW_BREAK"):
        [("atr_pct_signal", ">=", 0.0070)],
}


# ---------------------------------------------------------------------------
# CAUSALITY contract -- every feature used in any filter must appear here
# with verdict CAUSAL. Otherwise we refuse to use it.
# ---------------------------------------------------------------------------
CAUSALITY_TABLE: List[Dict[str, str]] = [
    {"feature_name": "rsi_signal", "source_column_in_CSV": "rsi_signal",
     "is_known_before_entry": "True",
     "comment": "Computed at signal-bar close; entry is next bar's open. Causal.",
     "verdict": "CAUSAL"},
    {"feature_name": "adx_signal", "source_column_in_CSV": "adx_signal",
     "is_known_before_entry": "True",
     "comment": "Signal-bar ADX. Causal.",
     "verdict": "CAUSAL"},
    {"feature_name": "atr_pct_signal", "source_column_in_CSV": "atr_pct_signal",
     "is_known_before_entry": "True",
     "comment": "Signal-bar ATR%. Causal.",
     "verdict": "CAUSAL"},
    {"feature_name": "avwap_dist_atr_signal",
     "source_column_in_CSV": "avwap_dist_atr_signal",
     "is_known_before_entry": "True",
     "comment": "Signal-bar AVWAP distance in ATR units. Causal.",
     "verdict": "CAUSAL"},
    {"feature_name": "ema20_gap_atr_signal",
     "source_column_in_CSV": "ema20_gap_atr_signal",
     "is_known_before_entry": "True",
     "comment": "Signal-bar signed EMA20 gap in ATR units. Causal.",
     "verdict": "CAUSAL"},
    {"feature_name": "stochk_signal", "source_column_in_CSV": "stochk_signal",
     "is_known_before_entry": "True",
     "comment": "Signal-bar Stoch %K. Causal.",
     "verdict": "CAUSAL"},
    {"feature_name": "quality_score", "source_column_in_CSV": "quality_score",
     "is_known_before_entry": "True",
     "comment": "Setup composite scored at signal time. Causal.",
     "verdict": "CAUSAL"},
    {"feature_name": "nifty_context_mode",
     "source_column_in_CSV": "nifty_context_mode",
     "is_known_before_entry": "True",
     "comment": "F7-fixed: NIFTY context evaluated at -5min from entry (one full bar prior). Causal.",
     "verdict": "CAUSAL"},
    {"feature_name": "nifty_rel_strength_pct",
     "source_column_in_CSV": "nifty_rel_strength_pct",
     "is_known_before_entry": "True",
     "comment": "F7-fixed: lagged NIFTY RS at -5min from entry. Causal.",
     "verdict": "CAUSAL"},
    {"feature_name": "entry_hour", "source_column_in_CSV": "entry_time_ist",
     "is_known_before_entry": "True",
     "comment": "Derived from entry timestamp (end of entry bar). Causal at entry.",
     "verdict": "CAUSAL"},
    {"feature_name": "gap_pct_open", "source_column_in_CSV": "gap_pct_open",
     "is_known_before_entry": "True",
     "comment": "Pre-09:30 quantity. Causal.",
     "verdict": "CAUSAL"},
    {"feature_name": "opening_range_width_pct",
     "source_column_in_CSV": "opening_range_width_pct",
     "is_known_before_entry": "True",
     "comment": "OR width measured 09:15-09:30; known before any post-09:30 entry. Causal.",
     "verdict": "CAUSAL"},
    {"feature_name": "india_vix", "source_column_in_CSV": "india_vix",
     "is_known_before_entry": "True",
     "comment": "Per-day previous-close VIX. Causal.",
     "verdict": "CAUSAL"},
]
ALLOWED_FEATURES = {row["feature_name"] for row in CAUSALITY_TABLE
                    if row["verdict"] == "CAUSAL"}


# ---------------------------------------------------------------------------
# Setup classification helpers
# ---------------------------------------------------------------------------
WEAK_LONG_SETUPS = {
    "C_OR_BREAKOUT", "G_HIGHER_HIGH_BREAK", "D_EMA20_BOUNCE",
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK",
}
WEAK_SHORT_SETUPS = {"G_LOWER_LOW_BREAK", "B_HUGE_RED_FAILED_BOUNCE"}

STRONG_LONG_SETUPS = {
    "B_AVWAP_RECLAIM_REVERSAL", "E_VWAP_BAND_FADE", "A_MOD_BREAK_C1_HIGH",
}
STRONG_SHORT_SETUPS = {
    "A_MOD_BREAK_C1_LOW", "D_AVWAP_LOSE_REVERSAL", "E_VWAP_BAND_FADE",
    "D_EMA20_REJECTION", "C_OR_BREAKDOWN",
}


# ---------------------------------------------------------------------------
# Metric helpers
# ---------------------------------------------------------------------------
PNL_COL = "pnl_pct_price"


def _safe_div(a, b):
    try:
        return a / b if b not in (0, 0.0) else float("inf")
    except Exception:
        return float("inf")


def metrics(df: pd.DataFrame) -> Dict[str, float]:
    """Aggregate scorecard metrics on a trade-row dataframe."""
    if df is None or len(df) == 0:
        return {
            "n": 0, "pf": float("nan"), "win_pct": float("nan"),
            "day_win_pct": float("nan"), "max_dd_pct": float("nan"),
            "sum_pnl_pct": 0.0, "avg_pnl_pct": float("nan"),
            "median_pnl_pct": float("nan"), "target_pct": float("nan"),
            "sl_pct": float("nan"), "eod_pct": float("nan"),
            "n_days": 0, "avg_trades_per_day": float("nan"),
        }
    pnl = pd.to_numeric(df[PNL_COL], errors="coerce").fillna(0.0)
    n = len(pnl)
    sum_pos = float(pnl[pnl > 0].sum())
    sum_neg_abs = float(-pnl[pnl < 0].sum())
    pf = _safe_div(sum_pos, sum_neg_abs)
    win_pct = float((pnl > 0).mean() * 100.0)
    sum_pnl = float(pnl.sum())
    avg_pnl = float(pnl.mean())
    median_pnl = float(pnl.median())
    outcome = df.get("outcome", pd.Series(dtype=str)).astype(str).str.upper()
    target_pct = float((outcome == "TARGET").mean() * 100.0) if len(outcome) else float("nan")
    sl_pct = float((outcome == "SL").mean() * 100.0) if len(outcome) else float("nan")
    eod_pct = float((outcome == "EOD").mean() * 100.0) if len(outcome) else float("nan")

    daily = pnl.groupby(df["trade_date"].astype(str)).sum()
    n_days = int(len(daily))
    day_win_pct = float((daily > 0).mean() * 100.0) if n_days else float("nan")
    avg_per_day = float(n / n_days) if n_days else float("nan")

    # Max drawdown on chronological cumulative pnl.
    chrono = df.sort_values(by=["trade_date", "entry_time_ist"], kind="mergesort")
    cpnl = pd.to_numeric(chrono[PNL_COL], errors="coerce").fillna(0.0).cumsum()
    if len(cpnl):
        running_max = cpnl.cummax()
        dd = cpnl - running_max
        max_dd = float(-dd.min())
    else:
        max_dd = float("nan")

    return {
        "n": n, "pf": pf, "win_pct": win_pct, "day_win_pct": day_win_pct,
        "max_dd_pct": max_dd, "sum_pnl_pct": sum_pnl, "avg_pnl_pct": avg_pnl,
        "median_pnl_pct": median_pnl, "target_pct": target_pct,
        "sl_pct": sl_pct, "eod_pct": eod_pct, "n_days": n_days,
        "avg_trades_per_day": avg_per_day,
    }


def metrics_row(df: pd.DataFrame, label: str = "") -> Dict[str, object]:
    m = metrics(df)
    m["label"] = label
    return m


# ---------------------------------------------------------------------------
# Loader: ensures derived columns and types are consistent.
# ---------------------------------------------------------------------------
def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date.astype(str)
    df["trade_date_dt"] = pd.to_datetime(df["trade_date"])
    df["setup"] = df["setup"].astype(str).str.upper().str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    et = pd.to_datetime(df["entry_time_ist"], errors="coerce", utc=True)
    try:
        et_local = et.dt.tz_convert("Asia/Kolkata")
    except Exception:
        et_local = et
    df["entry_hour"] = (et_local.dt.hour + et_local.dt.minute / 60.0)
    df["entry_minute_bin"] = (
        et_local.dt.hour.astype("Int64") * 60 + et_local.dt.minute.astype("Int64")
    )
    df["weekday"] = et_local.dt.day_name()
    df["month"] = et_local.dt.to_period("M").astype(str)
    df["week_of_month"] = (et_local.dt.day.sub(1) // 7 + 1).astype("Int64")
    # Phase split.
    df["split"] = np.where(
        df["trade_date_dt"] < OOS_START, "TRAIN", "OOS"
    )
    return df


# ---------------------------------------------------------------------------
# Filter chain application (compatible with V17T_DEEP_FILTER_SPEC format).
# ---------------------------------------------------------------------------
def apply_chain(
    df: pd.DataFrame,
    chain: List[Tuple[str, str, float]],
) -> pd.DataFrame:
    if df is None or len(df) == 0 or not chain:
        return df
    out_mask = pd.Series(True, index=df.index)
    for feat, direction, thr in chain:
        col = (df["entry_hour"] if feat == "entry_hour"
               else pd.to_numeric(df.get(feat, pd.Series(np.nan, index=df.index)),
                                  errors="coerce"))
        if direction == ">=":
            out_mask &= (col >= thr).fillna(False)
        elif direction == "<=":
            out_mask &= (col <= thr).fillna(False)
        elif direction == "==":  # categorical-eq (string)
            sval = df.get(feat, pd.Series("", index=df.index)).astype(str)
            out_mask &= sval.eq(str(thr))
    return df.loc[out_mask].copy()


def apply_full_spec(
    df: pd.DataFrame,
    spec: Dict[Tuple[str, str], List[Tuple[str, str, float]]],
) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return df
    keep_idx = []
    for (k_side, k_setup), chain in spec.items():
        sub = df[(df["side"] == k_side) & (df["setup"] == k_setup)]
        if chain:
            sub = apply_chain(sub, chain)
        keep_idx.extend(list(sub.index))
    return df.loc[df.index.isin(keep_idx)].copy()


# ---------------------------------------------------------------------------
# STAGE 0 -- baseline + ablations
# ---------------------------------------------------------------------------
def stage0_baseline(df: pd.DataFrame, df_q: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []

    rows.append(metrics_row(df, "v17t_live_unfiltered_honest"))
    rows.append(metrics_row(df_q, "v17q_run6_F7_OFF_xref"))

    # Phase 5d AGGRESSIVE applied to v17t_live CSV.
    df_p5d = apply_full_spec(df, V17T_DEEP_FILTER_SPEC)
    rows.append(metrics_row(df_p5d, "v17t_live_phase5d_aggressive"))

    # ABLATION A: drop all 4 weak LONG setups simultaneously.
    mask_a = ~((df["side"] == "LONG") & (df["setup"].isin(WEAK_LONG_SETUPS)))
    rows.append(metrics_row(df[mask_a], "ablation_A_drop_all_weak_LONG"))

    # ABLATION B: drop each weak LONG setup individually.
    for s in sorted(WEAK_LONG_SETUPS):
        mask_b = ~((df["side"] == "LONG") & (df["setup"] == s))
        rows.append(metrics_row(df[mask_b], f"ablation_B_drop_LONG_{s}"))

    # ABLATION C: drop each weak SHORT setup individually.
    for s in sorted(WEAK_SHORT_SETUPS):
        mask_c = ~((df["side"] == "SHORT") & (df["setup"] == s))
        rows.append(metrics_row(df[mask_c], f"ablation_C_drop_SHORT_{s}"))

    # ABLATION D: keep only strongest setups.
    keep_d = (
        ((df["side"] == "LONG") & (df["setup"].isin(STRONG_LONG_SETUPS))) |
        ((df["side"] == "SHORT") & (df["setup"].isin(STRONG_SHORT_SETUPS)))
    )
    rows.append(metrics_row(df[keep_d], "ablation_D_strongest_only"))

    out = pd.DataFrame(rows)
    cols_order = [
        "label", "n", "pf", "win_pct", "day_win_pct", "max_dd_pct",
        "sum_pnl_pct", "avg_pnl_pct", "median_pnl_pct", "target_pct",
        "sl_pct", "eod_pct", "n_days", "avg_trades_per_day",
    ]
    return out[[c for c in cols_order if c in out.columns]]


# ---------------------------------------------------------------------------
# STAGE 1 -- per-setup diagnostics + per-setup x feature buckets
# ---------------------------------------------------------------------------
def stage1_per_setup_diagnostics(df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for (side, setup), sub in df.groupby(["side", "setup"]):
        m = metrics(sub)
        # holding-time metrics
        st = pd.to_datetime(sub["signal_time_ist"], errors="coerce", utc=True)
        et = pd.to_datetime(sub["entry_time_ist"], errors="coerce", utc=True)
        xt = pd.to_datetime(sub["exit_time_ist"], errors="coerce", utc=True)
        hold = (xt - et).dt.total_seconds() / 60.0
        outcome = sub["outcome"].astype(str).str.upper()
        avg_hold_min = float(hold.mean()) if len(hold) else float("nan")
        avg_t2t = float(hold[outcome == "TARGET"].mean()) if (outcome == "TARGET").any() else float("nan")
        avg_t2sl = float(hold[outcome == "SL"].mean()) if (outcome == "SL").any() else float("nan")

        # ticker concentration
        ticker_pnl = sub.groupby("ticker")[PNL_COL].sum().sort_values(ascending=False)
        n_pos_t = int((ticker_pnl > 0).sum())
        n_neg_t = int((ticker_pnl < 0).sum())
        total_pnl = float(ticker_pnl.sum())
        top5_share = (float(ticker_pnl.head(5).sum()) / total_pnl * 100.0
                      if total_pnl > 0 else float("nan"))

        # monthly metrics
        monthly_pf = []
        monthly_n = []
        for mth, m_sub in sub.groupby("month"):
            mm = metrics(m_sub)
            monthly_pf.append(mm["pf"])
            monthly_n.append(mm["n"])
        monthly_pf_finite = [p for p in monthly_pf if math.isfinite(p)]

        # classification
        if m["n"] < 25:
            cls = "SMALL_SAMPLE_ONLY"
        elif (m["pf"] >= 1.4 and m["win_pct"] >= 55):
            cls = "CORE_EDGE"
        elif (m["pf"] >= 1.15 and m["win_pct"] >= 50):
            cls = "CONDITIONAL_EDGE"
        elif (m["pf"] >= 0.95):
            cls = "WEAK_EDGE"
        else:
            cls = "DEAD_SIGNAL"
        if math.isfinite(m["pf"]) and m["pf"] >= 2.5 and m["n"] < 50:
            cls = "OVERFIT_RISK"

        # recommendation
        rec = {
            "CORE_EDGE": "KEEP_AS_IS",
            "CONDITIONAL_EDGE": "FILTER_THEN_KEEP",
            "WEAK_EDGE": "FILTER_OR_DROP",
            "DEAD_SIGNAL": "DROP",
            "OVERFIT_RISK": "OOS_VALIDATE_BEFORE_KEEP",
            "SMALL_SAMPLE_ONLY": "DEFER_OR_DROP",
        }.get(cls, "REVIEW")

        rows.append({
            "side": side, "setup": setup,
            "classification": cls, "recommendation": rec,
            **m,
            "avg_holding_min": avg_hold_min,
            "avg_time_to_target_min": avg_t2t,
            "avg_time_to_sl_min": avg_t2sl,
            "n_tickers_positive": n_pos_t,
            "n_tickers_negative": n_neg_t,
            "top5_ticker_share_pct": top5_share,
            "monthly_pf_min": min(monthly_pf_finite) if monthly_pf_finite else float("nan"),
            "monthly_pf_max": max(monthly_pf_finite) if monthly_pf_finite else float("nan"),
            "monthly_n_min": min(monthly_n) if monthly_n else 0,
            "monthly_n_max": max(monthly_n) if monthly_n else 0,
            "monthly_count_pf_finite": len(monthly_pf_finite),
        })
    return pd.DataFrame(rows).sort_values(["side", "setup"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Bucketers
# ---------------------------------------------------------------------------
def _bucket_30min(hr: float) -> str:
    if pd.isna(hr):
        return "NA"
    minutes = int(round(hr * 60))
    # Bins: <=09:30, 09:30-10:00, 10:00-10:30, ..., 14:30-15:00, after-15:00
    if minutes < 9 * 60 + 30:
        return "09:15-09:30"
    if minutes >= 15 * 60:
        return "after-15:00"
    start = (minutes // 30) * 30
    end = start + 30
    return f"{start//60:02d}:{start%60:02d}-{end//60:02d}:{end%60:02d}"


def _bucket_quantile_signed(value: float, edges: List[float], labels: List[str]) -> str:
    if pd.isna(value):
        return "NA"
    for edge, label in zip(edges, labels):
        if value < edge:
            return label
    return labels[-1] if labels else "NA"


def _bucket_rsi(v: float) -> str:
    edges = [25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80]
    labels = (["<25"]
              + [f"{a}-{b}" for a, b in zip(edges[:-1], edges[1:])]
              + [">80"])
    if pd.isna(v):
        return "NA"
    if v < 25:
        return "<25"
    if v >= 80:
        return ">80"
    for a, b in zip(edges[:-1], edges[1:]):
        if a <= v < b:
            return f"{a}-{b}"
    return "NA"


def _bucket_stochk(v: float) -> str:
    if pd.isna(v):
        return "NA"
    if v < 10:
        return "<10"
    if v >= 90:
        return ">90"
    a = int(v // 10) * 10
    return f"{a}-{a+10}"


def _bucket_adx(v: float) -> str:
    edges = [15, 20, 25, 30, 35, 40, 45, 50]
    if pd.isna(v):
        return "NA"
    if v < 15:
        return "<15"
    if v >= 50:
        return ">50"
    for a, b in zip(edges[:-1], edges[1:]):
        if a <= v < b:
            return f"{a}-{b}"
    return "NA"


def _bucket_atr_pct(v: float) -> str:
    if pd.isna(v):
        return "NA"
    if v < 0.0025:
        return "<0.0025"
    if v < 0.005:
        return "0.0025-0.005"
    if v < 0.0075:
        return "0.005-0.0075"
    if v < 0.010:
        return "0.0075-0.010"
    if v < 0.015:
        return "0.010-0.015"
    return ">0.015"


def _bucket_avwap_dist(v: float) -> str:
    if pd.isna(v):
        return "NA"
    if v < 0.0:
        return "<0"
    edges = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]
    for i, e in enumerate(edges):
        if v < e:
            prev = 0.0 if i == 0 else edges[i - 1]
            return f"{prev}-{e}"
    return ">3.0"


def _bucket_ema_gap(v: float) -> str:
    if pd.isna(v):
        return "NA"
    edges = [-3.0, -2.5, -2.0, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0]
    if v < -3.0:
        return "<-3.0"
    if v >= 3.0:
        return ">3.0"
    for a, b in zip(edges[:-1], edges[1:]):
        if a <= v < b:
            return f"{a}-{b}"
    return "NA"


def _bucket_qs(v: float) -> str:
    if pd.isna(v):
        return "NA"
    if v < 2:
        return "<2"
    if v < 4:
        return "2-4"
    if v < 6:
        return "4-6"
    if v < 8:
        return "6-8"
    if v < 10:
        return "8-10"
    return ">10"


def _bucket_rs(v: float) -> str:
    if pd.isna(v):
        return "NA"
    bin_ = round(v * 4) / 4.0
    return f"{bin_:.2f}pp"


def _bucket_gap(v: float) -> str:
    if pd.isna(v):
        return "NA"
    if v < -1:
        return "<-1"
    if v < -0.5:
        return "-1to-0.5"
    if v < 0.5:
        return "-0.5to+0.5"
    if v < 1:
        return "+0.5to+1"
    return ">+1"


def _bucket_or_width(v: float) -> str:
    if pd.isna(v):
        return "NA"
    if v < 0.25:
        return "<0.25"
    if v < 0.5:
        return "0.25-0.5"
    if v < 0.75:
        return "0.5-0.75"
    if v < 1.0:
        return "0.75-1.0"
    if v < 1.5:
        return "1.0-1.5"
    return ">1.5"


def _bucket_vix(v: float) -> str:
    if pd.isna(v):
        return "NA"
    if v < 11:
        return "<11"
    if v < 13:
        return "11-13"
    if v < 15:
        return "13-15"
    return ">15"


FEATURE_BUCKETERS: Dict[str, callable] = {
    "entry_hour": _bucket_30min,
    "rsi_signal": _bucket_rsi,
    "stochk_signal": _bucket_stochk,
    "adx_signal": _bucket_adx,
    "atr_pct_signal": _bucket_atr_pct,
    "avwap_dist_atr_signal": _bucket_avwap_dist,
    "ema20_gap_atr_signal": _bucket_ema_gap,
    "quality_score": _bucket_qs,
    "nifty_rel_strength_pct": _bucket_rs,
    "gap_pct_open": _bucket_gap,
    "opening_range_width_pct": _bucket_or_width,
    "india_vix": _bucket_vix,
}


def stage1_feature_buckets(df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []

    for (side, setup), sub in df.groupby(["side", "setup"]):
        setup_total_pnl = float(pd.to_numeric(sub[PNL_COL], errors="coerce")
                                .fillna(0.0).sum())

        # numeric / mapped features
        for feat, bucketer in FEATURE_BUCKETERS.items():
            if feat not in sub.columns:
                continue
            vals = pd.to_numeric(sub[feat], errors="coerce") if feat != "entry_hour" else sub[feat]
            buckets = vals.map(bucketer)
            for b, b_sub in sub.groupby(buckets):
                m = metrics(b_sub)
                bsum = float(pd.to_numeric(b_sub[PNL_COL], errors="coerce").fillna(0.0).sum())
                rows.append({
                    "side": side, "setup": setup, "feature": feat, "bucket": b,
                    **m,
                    "contribution_pct": (bsum / setup_total_pnl * 100.0
                                         if setup_total_pnl != 0 else float("nan")),
                })

        # categorical feature
        if "nifty_context_mode" in sub.columns:
            for b, b_sub in sub.groupby(sub["nifty_context_mode"].astype(str).fillna("NA")):
                m = metrics(b_sub)
                bsum = float(pd.to_numeric(b_sub[PNL_COL], errors="coerce").fillna(0.0).sum())
                rows.append({
                    "side": side, "setup": setup,
                    "feature": "nifty_context_mode", "bucket": b,
                    **m,
                    "contribution_pct": (bsum / setup_total_pnl * 100.0
                                         if setup_total_pnl != 0 else float("nan")),
                })

        # calendar
        for cal_feat in ("weekday", "month", "week_of_month"):
            if cal_feat not in sub.columns:
                continue
            for b, b_sub in sub.groupby(sub[cal_feat].astype(str)):
                m = metrics(b_sub)
                bsum = float(pd.to_numeric(b_sub[PNL_COL], errors="coerce").fillna(0.0).sum())
                rows.append({
                    "side": side, "setup": setup,
                    "feature": cal_feat, "bucket": b,
                    **m,
                    "contribution_pct": (bsum / setup_total_pnl * 100.0
                                         if setup_total_pnl != 0 else float("nan")),
                })

    out = pd.DataFrame(rows)
    cols = ["side", "setup", "feature", "bucket", "n", "pf", "win_pct",
            "day_win_pct", "max_dd_pct", "sum_pnl_pct", "avg_pnl_pct",
            "median_pnl_pct", "contribution_pct"]
    return out[[c for c in cols if c in out.columns]]


# ---------------------------------------------------------------------------
# STAGE 3 -- Greedy per-setup filter search
# ---------------------------------------------------------------------------
GREEDY_QUANTILES = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]
GREEDY_TARGET_TIERS = [1.70, 1.55, 1.40, 1.30, 1.20]
GREEDY_FEATURES = [
    "rsi_signal", "adx_signal", "atr_pct_signal",
    "avwap_dist_atr_signal", "ema20_gap_atr_signal",
    "stochk_signal", "quality_score",
    "nifty_rel_strength_pct", "entry_hour", "gap_pct_open",
    "opening_range_width_pct", "india_vix",
]
GREEDY_N_FLOOR = 30  # min trades after a chain is applied
GREEDY_MIN_PFSQRT_GAIN = 0.02


def _scoring(pf: float, n: int) -> float:
    if not math.isfinite(pf) or n < 1:
        return -1.0
    return float(pf) * math.sqrt(n)


def stage3_greedy_search(
    df: pd.DataFrame,
    log_rows: List[Dict[str, object]],
) -> pd.DataFrame:
    """Greedy chain search per (side, setup). Returns one row per setup."""
    out_rows: List[Dict[str, object]] = []
    for (side, setup), sub in df.groupby(["side", "setup"]):
        baseline_m = metrics(sub)
        if baseline_m["n"] < 10:
            out_rows.append({
                "side": side, "setup": setup, "chain": "[]",
                "tier_target": float("nan"),
                "tier_achieved": float("nan"),
                "n_baseline": baseline_m["n"],
                "n_filtered": baseline_m["n"],
                "pf_baseline": baseline_m["pf"],
                "pf_filtered": baseline_m["pf"],
                "win_pct_filtered": baseline_m["win_pct"],
                "day_win_pct_filtered": baseline_m["day_win_pct"],
                "max_dd_filtered": baseline_m["max_dd_pct"],
                "sum_pnl_filtered": baseline_m["sum_pnl_pct"],
                "note": "TOO_SMALL_SAMPLE_NO_SEARCH",
            })
            continue

        achieved_tier = float("nan")
        for tier_target in GREEDY_TARGET_TIERS:
            chain: List[Tuple[str, str, float]] = []
            current = sub.copy()
            current_m = metrics(current)
            score_now = _scoring(current_m["pf"], current_m["n"])
            done = False
            while len(chain) < 3 and not done:
                best_gain = 0.0
                best_step = None
                best_after = None
                used_feats = {f for f, _, _ in chain}
                for feat in GREEDY_FEATURES:
                    if feat in used_feats:
                        continue
                    if feat not in current.columns and feat != "entry_hour":
                        continue
                    series = (current["entry_hour"] if feat == "entry_hour"
                              else pd.to_numeric(current.get(feat),
                                                 errors="coerce"))
                    if series.dropna().empty:
                        continue
                    qvals = series.quantile(GREEDY_QUANTILES).dropna().unique()
                    for thr in qvals:
                        for direction in (">=", "<="):
                            mask = ((series >= thr) if direction == ">="
                                    else (series <= thr))
                            mask = mask.fillna(False)
                            after = current.loc[mask]
                            mm = metrics(after)
                            log_rows.append({
                                "side": side, "setup": setup,
                                "step": len(chain) + 1,
                                "feature": feat, "direction": direction,
                                "threshold": float(thr),
                                "tier_target": tier_target,
                                "n_in": int(current_m["n"]),
                                "n_out": int(mm["n"]),
                                "pf_in": current_m["pf"],
                                "pf_out": mm["pf"],
                                "win_in": current_m["win_pct"],
                                "win_out": mm["win_pct"],
                                "score_in": score_now,
                                "score_out": _scoring(mm["pf"], mm["n"]),
                            })
                            if mm["n"] < GREEDY_N_FLOOR:
                                continue
                            score_after = _scoring(mm["pf"], mm["n"])
                            gain = score_after - score_now
                            if gain > best_gain:
                                best_gain = gain
                                best_step = (feat, direction, float(thr))
                                best_after = after
                if best_step is None or best_gain < GREEDY_MIN_PFSQRT_GAIN:
                    done = True
                    break
                chain.append(best_step)
                current = best_after
                current_m = metrics(current)
                score_now = _scoring(current_m["pf"], current_m["n"])
                if (math.isfinite(current_m["pf"])
                        and current_m["pf"] >= tier_target
                        and current_m["n"] >= GREEDY_N_FLOOR):
                    done = True
            if (math.isfinite(current_m["pf"])
                    and current_m["pf"] >= tier_target
                    and current_m["n"] >= GREEDY_N_FLOOR):
                achieved_tier = tier_target
                final_chain = chain
                final_m = current_m
                break
        else:
            # Fell through all tiers; report best chain achieved on the last
            # tier attempt (1.20).
            achieved_tier = float("nan")
            final_chain = chain
            final_m = current_m

        out_rows.append({
            "side": side, "setup": setup,
            "chain": str(final_chain),
            "chain_len": len(final_chain),
            "tier_achieved": achieved_tier,
            "n_baseline": baseline_m["n"],
            "n_filtered": final_m["n"],
            "pf_baseline": baseline_m["pf"],
            "pf_filtered": final_m["pf"],
            "win_pct_filtered": final_m["win_pct"],
            "day_win_pct_filtered": final_m["day_win_pct"],
            "max_dd_filtered": final_m["max_dd_pct"],
            "sum_pnl_filtered": final_m["sum_pnl_pct"],
            "delta_pf": (final_m["pf"] - baseline_m["pf"]
                        if math.isfinite(final_m["pf"]) and math.isfinite(baseline_m["pf"])
                        else float("nan")),
            "kept_share": (final_m["n"] / baseline_m["n"]
                           if baseline_m["n"] > 0 else float("nan")),
        })
    return pd.DataFrame(out_rows).sort_values(["side", "setup"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# STAGE 4 -- trade-count preservation contract
# ---------------------------------------------------------------------------
def stage4_trade_count_impact(
    df: pd.DataFrame,
    chains_df: pd.DataFrame,
) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for _, r in chains_df.iterrows():
        side, setup = r["side"], r["setup"]
        chain = eval(r["chain"]) if isinstance(r["chain"], str) and r["chain"] else []
        sub = df[(df["side"] == side) & (df["setup"] == setup)]
        baseline_m = metrics(sub)
        after = apply_chain(sub, chain) if chain else sub
        final_m = metrics(after)
        n_in = baseline_m["n"]
        n_out = final_m["n"]
        n_winners_in = int((sub[PNL_COL] > 0).sum())
        n_losers_in = int((sub[PNL_COL] < 0).sum())
        n_winners_out = int((after[PNL_COL] > 0).sum())
        n_losers_out = int((after[PNL_COL] < 0).sum())
        winners_removed = n_winners_in - n_winners_out
        losers_removed = n_losers_in - n_losers_out
        # delta dd (after - before): negative is good (DD reduced).
        dpf = (final_m["pf"] - baseline_m["pf"]
               if math.isfinite(final_m["pf"]) and math.isfinite(baseline_m["pf"])
               else float("nan"))
        ddd = (final_m["max_dd_pct"] - baseline_m["max_dd_pct"]
               if math.isfinite(final_m["max_dd_pct"]) and math.isfinite(baseline_m["max_dd_pct"])
               else float("nan"))
        ratio = (losers_removed / winners_removed if winners_removed > 0 else float("inf")
                 if losers_removed > 0 else float("nan"))
        verdict = "PASSTHROUGH"
        if chain:
            if n_out < 30:
                verdict = "REJECTED_TOO_FEW"
            elif n_in > 0 and (n_out / n_in) < 0.20:
                verdict = "REJECTED_KILLS_VOLUME"
            elif winners_removed > losers_removed:
                verdict = "REJECTED_REMOVES_WINNERS"
            elif (math.isfinite(ratio) and ratio >= 1.5
                  and dpf is not None and math.isfinite(dpf) and dpf >= 0.10
                  and (math.isfinite(ddd) and ddd <= 0)):
                verdict = "GREAT"
            elif (math.isfinite(ratio) and ratio >= 1.5
                  and dpf is not None and math.isfinite(dpf) and dpf >= 0.10):
                verdict = "GOOD"
            else:
                verdict = "MARGINAL"
        rows.append({
            "side": side, "setup": setup, "chain": str(chain),
            "n_baseline": n_in, "n_after_filter": n_out,
            "winners_removed": winners_removed,
            "losers_removed": losers_removed,
            "n_winners_kept": n_winners_out, "n_losers_kept": n_losers_out,
            "delta_pf": dpf, "delta_dd": ddd,
            "ratio_losers_to_winners_removed": ratio,
            "verdict": verdict,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# STAGE 7 -- OOS validation per setup
# ---------------------------------------------------------------------------
def stage7_oos_split(
    df: pd.DataFrame,
    chains_df: pd.DataFrame,
) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for _, r in chains_df.iterrows():
        side, setup = r["side"], r["setup"]
        chain = eval(r["chain"]) if isinstance(r["chain"], str) and r["chain"] else []
        sub = df[(df["side"] == side) & (df["setup"] == setup)]
        sub_after = apply_chain(sub, chain) if chain else sub
        for split_label in ("TRAIN", "OOS"):
            seg = sub_after[sub_after["split"] == split_label]
            m = metrics(seg)
            rows.append({
                "side": side, "setup": setup, "split": split_label,
                "chain": str(chain), **m,
            })

    out = pd.DataFrame(rows)
    # Add derived per-setup decay column.
    pivot = out.pivot_table(index=["side", "setup"], columns="split",
                            values=["pf", "n", "max_dd_pct", "win_pct",
                                    "day_win_pct"])
    decay_rows: List[Dict[str, object]] = []
    for (side, setup), _ in pivot.iterrows():
        try:
            train_pf = pivot.loc[(side, setup), ("pf", "TRAIN")]
            oos_pf = pivot.loc[(side, setup), ("pf", "OOS")]
            train_n = pivot.loc[(side, setup), ("n", "TRAIN")]
            oos_n = pivot.loc[(side, setup), ("n", "OOS")]
            train_dd = pivot.loc[(side, setup), ("max_dd_pct", "TRAIN")]
            oos_dd = pivot.loc[(side, setup), ("max_dd_pct", "OOS")]
            decay = (oos_pf / train_pf
                     if math.isfinite(train_pf) and train_pf > 0 else float("nan"))
            pass_oos_n = bool(oos_n >= 15)
            pass_oos_decay = bool(math.isfinite(decay) and decay >= 0.65)
            pass_oos_pf = bool(math.isfinite(oos_pf) and oos_pf >= 1.30)
            pass_dd = bool(math.isfinite(oos_dd) and math.isfinite(train_dd)
                           and oos_dd <= 1.5 * max(train_dd, 1e-6))
            verdict = "SHIP" if (pass_oos_n and pass_oos_decay and
                                  pass_oos_pf and pass_dd) else "REJECT"
            decay_rows.append({
                "side": side, "setup": setup, "split": "DECAY_SUMMARY",
                "n_train": int(train_n) if pd.notna(train_n) else 0,
                "n_oos": int(oos_n) if pd.notna(oos_n) else 0,
                "pf_train": train_pf, "pf_oos": oos_pf,
                "max_dd_train": train_dd, "max_dd_oos": oos_dd,
                "decay_ratio": decay, "verdict": verdict,
                "pass_n": pass_oos_n, "pass_decay": pass_oos_decay,
                "pass_pf": pass_oos_pf, "pass_dd": pass_dd,
            })
        except KeyError:
            continue
    return pd.concat([out, pd.DataFrame(decay_rows)], ignore_index=True, sort=False)


# ---------------------------------------------------------------------------
# Monthly stability + ticker concentration
# ---------------------------------------------------------------------------
def monthly_stability(df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for (side, setup, mth), sub in df.groupby(["side", "setup", "month"]):
        m = metrics(sub)
        rows.append({"side": side, "setup": setup, "month": mth, **m})
    return pd.DataFrame(rows)


def ticker_concentration(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for (side, setup), sub in df.groupby(["side", "setup"]):
        ticker_pnl = sub.groupby("ticker").agg(
            n=("pnl_pct_price", "size"),
            sum_pnl=("pnl_pct_price", "sum"),
            win_pct=("pnl_pct_price", lambda s: float((s > 0).mean() * 100.0)),
        ).sort_values("sum_pnl", ascending=False)
        n_total = len(sub)
        sum_total = float(ticker_pnl["sum_pnl"].sum())
        # top
        for rank, (tick, row) in enumerate(ticker_pnl.head(top_n).iterrows(), 1):
            rows.append({
                "side": side, "setup": setup, "rank_type": "TOP",
                "rank": rank, "ticker": tick, "n_trades": int(row["n"]),
                "sum_pnl_pct": float(row["sum_pnl"]),
                "win_pct": float(row["win_pct"]),
                "share_of_setup_pnl_pct": (float(row["sum_pnl"]) / sum_total * 100.0
                                            if sum_total != 0 else float("nan")),
                "share_of_setup_trades_pct": (int(row["n"]) / n_total * 100.0
                                               if n_total else float("nan")),
            })
        # bottom
        for rank, (tick, row) in enumerate(ticker_pnl.tail(top_n)
                                            .iloc[::-1].iterrows(), 1):
            rows.append({
                "side": side, "setup": setup, "rank_type": "BOTTOM",
                "rank": rank, "ticker": tick, "n_trades": int(row["n"]),
                "sum_pnl_pct": float(row["sum_pnl"]),
                "win_pct": float(row["win_pct"]),
                "share_of_setup_pnl_pct": (float(row["sum_pnl"]) / sum_total * 100.0
                                            if sum_total != 0 else float("nan")),
                "share_of_setup_trades_pct": (int(row["n"]) / n_total * 100.0
                                               if n_total else float("nan")),
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# STAGE 8 -- Candidate construction
# ---------------------------------------------------------------------------
def _build_candidate_A_spec(diag: pd.DataFrame) -> Dict[Tuple[str, str], List]:
    """Minimal cleanup: keep all setups except the four weak LONGs + two
    weak SHORTs. No filters."""
    spec: Dict[Tuple[str, str], List] = {}
    for _, r in diag.iterrows():
        side, setup = r["side"], r["setup"]
        if side == "LONG" and setup in WEAK_LONG_SETUPS:
            continue
        if side == "SHORT" and setup in WEAK_SHORT_SETUPS:
            continue
        spec[(side, setup)] = []
    return spec


def _parse_chain(chain_str: str) -> List[Tuple[str, str, float]]:
    if not isinstance(chain_str, str) or not chain_str:
        return []
    try:
        return list(eval(chain_str))
    except Exception:
        return []


def _build_candidate_B_spec(stage3_df: pd.DataFrame, diag: pd.DataFrame) -> Dict:
    """Best Stage-3 chain per setup. Drop setups whose final chain n < 30
    or final PF < 1.10."""
    spec: Dict[Tuple[str, str], List] = {}
    for _, r in stage3_df.iterrows():
        side, setup = r["side"], r["setup"]
        n_filt = int(r.get("n_filtered", 0))
        pf_filt = float(r.get("pf_filtered", float("nan")))
        chain = _parse_chain(r.get("chain", ""))
        if n_filt < 30:
            continue
        if not (math.isfinite(pf_filt) and pf_filt >= 1.10):
            continue
        spec[(side, setup)] = chain
    return spec


def _build_candidate_C_spec(B_spec: Dict) -> Dict:
    """Candidate B + lagged regime gate. Add nifty_context_mode constraint."""
    spec: Dict[Tuple[str, str], List] = {}
    for (side, setup), chain in B_spec.items():
        gate = ("nifty_context_mode", "==",
                "LONG_ONLY" if side == "LONG" else "SHORT_ONLY")
        # Permissive variant: allow BOTH too -- encoded as two-step "OR" via
        # a different filter helper. We'll instead allow {LONG_ONLY,BOTH} for
        # LONG via a custom tagged literal: the lab runtime will treat the
        # literal "LONG_OR_BOTH"/"SHORT_OR_BOTH" specially.
        gate = ("nifty_context_mode", "==",
                "LONG_OR_BOTH" if side == "LONG" else "SHORT_OR_BOTH")
        spec[(side, setup)] = list(chain) + [gate]
    return spec


def _build_candidate_D_spec(stage3_df: pd.DataFrame) -> Dict:
    """High-quality / lower count: only setups whose chain achieves PF
    >= 1.55 with n >= 25."""
    spec: Dict[Tuple[str, str], List] = {}
    for _, r in stage3_df.iterrows():
        side, setup = r["side"], r["setup"]
        n_filt = int(r.get("n_filtered", 0))
        pf_filt = float(r.get("pf_filtered", float("nan")))
        chain = _parse_chain(r.get("chain", ""))
        if n_filt < 25:
            continue
        if not (math.isfinite(pf_filt) and pf_filt >= 1.55):
            continue
        spec[(side, setup)] = chain
    return spec


def _build_candidate_E_spec(stage3_df: pd.DataFrame, diag: pd.DataFrame) -> Dict:
    """Count-preserving: keep a setup if either (a) its chain achieves PF
    >= 1.20 and n >= 50, OR (b) its baseline PF >= 1.05 with no chain."""
    spec: Dict[Tuple[str, str], List] = {}
    for _, r in stage3_df.iterrows():
        side, setup = r["side"], r["setup"]
        n_filt = int(r.get("n_filtered", 0))
        pf_filt = float(r.get("pf_filtered", float("nan")))
        chain = _parse_chain(r.get("chain", ""))
        if n_filt >= 50 and math.isfinite(pf_filt) and pf_filt >= 1.20:
            spec[(side, setup)] = chain
            continue
    # Attempt to also include setups whose baseline PF >= 1.05 even if Stage 3
    # rejected them, but only if their unfiltered count contributes volume.
    for _, r in diag.iterrows():
        side, setup = r["side"], r["setup"]
        if (side, setup) in spec:
            continue
        if r["n"] >= 50 and math.isfinite(r["pf"]) and r["pf"] >= 1.05:
            spec[(side, setup)] = []
    return spec


def _apply_candidate_spec_with_regime(
    df: pd.DataFrame,
    spec: Dict[Tuple[str, str], List],
) -> pd.DataFrame:
    """Like apply_full_spec, but understands the LONG_OR_BOTH /
    SHORT_OR_BOTH categorical literal used by candidate C."""
    if df is None or len(df) == 0:
        return df
    keep_idx: List = []
    for (k_side, k_setup), chain in spec.items():
        sub = df[(df["side"] == k_side) & (df["setup"] == k_setup)]
        if len(sub) == 0:
            continue
        rest_chain = []
        for feat, direction, thr in chain:
            if (feat == "nifty_context_mode" and direction == "=="
                    and isinstance(thr, str)
                    and thr in ("LONG_OR_BOTH", "SHORT_OR_BOTH")):
                allowed = (("LONG_ONLY", "BOTH") if thr == "LONG_OR_BOTH"
                           else ("SHORT_ONLY", "BOTH"))
                ctx = sub.get("nifty_context_mode", pd.Series("", index=sub.index))
                sub = sub[ctx.astype(str).isin(allowed)]
            else:
                rest_chain.append((feat, direction, thr))
        sub = apply_chain(sub, rest_chain) if rest_chain else sub
        keep_idx.extend(list(sub.index))
    return df.loc[df.index.isin(keep_idx)].copy()


def stage8_candidates(
    df: pd.DataFrame,
    diag: pd.DataFrame,
    stage3_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict[str, Dict]]:
    cand_specs = {
        "A_minimal_cleanup": _build_candidate_A_spec(diag),
        "B_setup_filters":   _build_candidate_B_spec(stage3_df, diag),
        "D_high_quality":    _build_candidate_D_spec(stage3_df),
        "E_count_preserving": _build_candidate_E_spec(stage3_df, diag),
    }
    cand_specs["C_regime_aware"] = _build_candidate_C_spec(
        cand_specs["B_setup_filters"]
    )

    rows: List[Dict[str, object]] = []
    # v17t_live phase 5d aggressive baseline row
    df_p5d = apply_full_spec(df, V17T_DEEP_FILTER_SPEC)
    rows.append(_eval_candidate_metrics_row(df_p5d, "v17t_live_p5d_aggressive"))
    rows.append(_eval_candidate_metrics_row(df, "v17t_live_unfiltered_honest"))

    for label, spec in cand_specs.items():
        df_c = _apply_candidate_spec_with_regime(df, spec)
        rows.append(_eval_candidate_metrics_row(df_c, f"v17r_candidate_{label}"))

    out = pd.DataFrame(rows)
    return out, cand_specs


def _eval_candidate_metrics_row(df: pd.DataFrame, label: str) -> Dict[str, object]:
    overall = metrics(df)
    train = metrics(df[df["split"] == "TRAIN"]) if len(df) else metrics(df)
    oos = metrics(df[df["split"] == "OOS"]) if len(df) else metrics(df)
    # ticker concentration
    if len(df) > 0 and df[PNL_COL].sum() != 0:
        tpnl = df.groupby("ticker")[PNL_COL].sum().sort_values(ascending=False)
        total = float(tpnl.sum())
        top1 = float(tpnl.head(1).sum() / total * 100.0) if total != 0 else float("nan")
        top5 = float(tpnl.head(5).sum() / total * 100.0) if total != 0 else float("nan")
        top10 = float(tpnl.head(10).sum() / total * 100.0) if total != 0 else float("nan")
    else:
        top1 = top5 = top10 = float("nan")
    # monthly stability
    months_pos = 0
    n_months = 0
    if len(df):
        for mth, m_sub in df.groupby("month"):
            mm = metrics(m_sub)
            n_months += 1
            if math.isfinite(mm["pf"]) and mm["pf"] > 1.0:
                months_pos += 1
    return {
        "label": label,
        "overall_n": overall["n"],
        "overall_pf": overall["pf"],
        "overall_win_pct": overall["win_pct"],
        "overall_day_win_pct": overall["day_win_pct"],
        "overall_max_dd_pct": overall["max_dd_pct"],
        "overall_sum_pnl_pct": overall["sum_pnl_pct"],
        "train_n": train["n"], "train_pf": train["pf"],
        "train_win_pct": train["win_pct"], "train_day_win_pct": train["day_win_pct"],
        "train_max_dd_pct": train["max_dd_pct"],
        "oos_n": oos["n"], "oos_pf": oos["pf"],
        "oos_win_pct": oos["win_pct"], "oos_day_win_pct": oos["day_win_pct"],
        "oos_max_dd_pct": oos["max_dd_pct"],
        "decay_pf": (oos["pf"] / train["pf"]
                     if math.isfinite(oos["pf"]) and math.isfinite(train["pf"])
                     and train["pf"] > 0 else float("nan")),
        "top1_ticker_share_pct": top1, "top5_ticker_share_pct": top5,
        "top10_ticker_share_pct": top10,
        "months_with_pf_gt_1": months_pos, "months_total": n_months,
    }


# ---------------------------------------------------------------------------
# Compare candidate vs v17t_live Phase 5d
# ---------------------------------------------------------------------------
def compare_candidate_vs_p5d(
    df: pd.DataFrame, candidate_label: str,
    candidate_spec: Dict[Tuple[str, str], List],
) -> pd.DataFrame:
    p5d_df = apply_full_spec(df, V17T_DEEP_FILTER_SPEC)
    cand_df = _apply_candidate_spec_with_regime(df, candidate_spec)

    p5d_rows = set(p5d_df.index)
    cand_rows = set(cand_df.index)

    only_p5d = p5d_rows - cand_rows
    only_cand = cand_rows - p5d_rows
    both = p5d_rows & cand_rows

    rows = []
    rows.append({
        "comparison": "summary",
        "candidate_label": candidate_label,
        "p5d_n": len(p5d_rows),
        "candidate_n": len(cand_rows),
        "shared_n": len(both),
        "only_in_p5d": len(only_p5d),
        "only_in_candidate": len(only_cand),
        **{f"p5d_{k}": v for k, v in metrics(p5d_df).items()},
        **{f"candidate_{k}": v for k, v in metrics(cand_df).items()},
    })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Causality audit CSV
# ---------------------------------------------------------------------------
def write_causality_audit() -> Path:
    out_path = BASE_OUT_DIR / "v17r_causality_audit.csv"
    pd.DataFrame(CAUSALITY_TABLE).to_csv(out_path, index=False)
    return out_path


# ---------------------------------------------------------------------------
# Main driver
# ---------------------------------------------------------------------------
def main() -> None:
    print(f"[v17r] reading v17t_live baseline: {V17T_LIVE_CSV}")
    df = load_csv(V17T_LIVE_CSV)
    print(f"[v17r] reading v17q xref:           {V17Q_CSV}")
    df_q = load_csv(V17Q_CSV)
    print(f"[v17r] v17t_live n={len(df)} | v17q xref n={len(df_q)}")

    # Causality contract -- write first.
    p = write_causality_audit()
    print(f"[v17r] wrote {p.name}")

    # Stage 0 -- baseline + ablations.
    stage0_df = stage0_baseline(df, df_q)
    p = BASE_OUT_DIR / "v17r_baseline_metrics.csv"
    stage0_df.to_csv(p, index=False)
    print(f"[v17r] wrote {p.name} ({len(stage0_df)} rows)")

    # Stage 1 -- per-setup diagnostics + per-feature buckets.
    diag = stage1_per_setup_diagnostics(df)
    p = BASE_OUT_DIR / "v17r_per_setup_diagnostics.csv"
    diag.to_csv(p, index=False)
    print(f"[v17r] wrote {p.name} ({len(diag)} rows)")

    fb = stage1_feature_buckets(df)
    p = BASE_OUT_DIR / "v17r_per_setup_feature_buckets.csv"
    fb.to_csv(p, index=False)
    print(f"[v17r] wrote {p.name} ({len(fb)} rows)")

    # Stage 3 -- greedy filter search.
    log_rows: List[Dict[str, object]] = []
    chains_df = stage3_greedy_search(df, log_rows)
    p = BASE_OUT_DIR / "v17r_per_setup_filter_search.csv"
    chains_df.to_csv(p, index=False)
    print(f"[v17r] wrote {p.name} ({len(chains_df)} rows)")

    log_df = pd.DataFrame(log_rows)
    p = BASE_OUT_DIR / "v17r_per_setup_filter_lever_log.csv"
    log_df.to_csv(p, index=False)
    print(f"[v17r] wrote {p.name} ({len(log_df)} rows)")

    # Stage 4 -- trade count impact.
    tci = stage4_trade_count_impact(df, chains_df)
    p = BASE_OUT_DIR / "v17r_trade_count_impact.csv"
    tci.to_csv(p, index=False)
    print(f"[v17r] wrote {p.name} ({len(tci)} rows)")

    # Stage 7 -- OOS + monthly + ticker.
    oos = stage7_oos_split(df, chains_df)
    p = BASE_OUT_DIR / "v17r_oos_split_metrics.csv"
    oos.to_csv(p, index=False)
    print(f"[v17r] wrote {p.name} ({len(oos)} rows)")

    monthly = monthly_stability(df)
    p = BASE_OUT_DIR / "v17r_monthly_stability.csv"
    monthly.to_csv(p, index=False)
    print(f"[v17r] wrote {p.name} ({len(monthly)} rows)")

    tk = ticker_concentration(df)
    p = BASE_OUT_DIR / "v17r_ticker_concentration.csv"
    tk.to_csv(p, index=False)
    print(f"[v17r] wrote {p.name} ({len(tk)} rows)")

    # Stage 8 -- candidates.
    cand_summary, cand_specs = stage8_candidates(df, diag, chains_df)
    p = BASE_OUT_DIR / "v17r_candidates_summary.csv"
    cand_summary.to_csv(p, index=False)
    print(f"[v17r] wrote {p.name} ({len(cand_summary)} rows)")

    # Pick the recommended candidate by composite score
    # (oos_pf * log10(oos_n) / max(1, oos_dd)).
    chosen_label = "B_setup_filters"
    best_score = -1.0
    for _, r in cand_summary.iterrows():
        if not str(r["label"]).startswith("v17r_candidate_"):
            continue
        oos_pf = r.get("oos_pf", float("nan"))
        oos_n = r.get("oos_n", 0)
        oos_dd = r.get("oos_max_dd_pct", float("nan"))
        if not (math.isfinite(oos_pf) and oos_n > 0 and math.isfinite(oos_dd)):
            continue
        score = float(oos_pf) * math.log10(max(oos_n, 10)) / max(1.0, float(oos_dd))
        if score > best_score:
            best_score = score
            chosen_label = str(r["label"]).replace("v17r_candidate_", "")

    cmp_df = compare_candidate_vs_p5d(df, chosen_label, cand_specs[chosen_label])
    p = BASE_OUT_DIR / "v17r_compare_against_v17t_p5d.csv"
    cmp_df.to_csv(p, index=False)
    print(f"[v17r] wrote {p.name} ({len(cmp_df)} rows; chosen={chosen_label})")

    # Persist candidate specs for later use by the runtime lab file.
    cand_path = BASE_OUT_DIR / "v17r_candidate_specs.py"
    with cand_path.open("w", encoding="utf-8") as fh:
        fh.write("# Auto-generated by _v17r_setup_lab_analyzer.py\n")
        fh.write("# Candidate specs dict-of-dicts; keys are (side, setup).\n\n")
        fh.write("V17R_CANDIDATE_SPECS = {\n")
        for cname, spec in cand_specs.items():
            fh.write(f"    {cname!r}: {{\n")
            for (side, setup), chain in spec.items():
                fh.write(f"        ({side!r}, {setup!r}): {list(chain)!r},\n")
            fh.write("    },\n")
        fh.write("}\n\n")
        fh.write(f"V17R_RECOMMENDED_CANDIDATE = {chosen_label!r}\n")
    print(f"[v17r] wrote {cand_path.name}")
    print("[v17r] DONE.")


if __name__ == "__main__":
    main()
