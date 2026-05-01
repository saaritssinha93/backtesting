# -*- coding: utf-8 -*-
"""
V17s 5-min combined runner -- staged MARS falsification test.

Branches from v17q. Goal: answer whether v17q LONG weakness comes from
"bad LONG setups" or "broad NIFTY/RS regime is wrong", BEFORE committing
to a full Market Adaptive Relative Strength engine.

Three staged toggles (all default to *additive* / diagnostic; behavioral
changes are off-by-default so the first run is honest baseline + audit):

  STAGE 0  -- EQIDV17S_STAGE0_DISABLE_WEAK_LONGS (default OFF)
    Drops the 4 known weak/dilutive LONG setups entirely:
      C_OR_BREAKOUT, G_HIGHER_HIGH_BREAK, D_EMA20_BOUNCE,
      B_HUGE_C1_CLOSE_RECLAIM_BREAK
    SHORT side untouched. Pure subtraction. If LONG PF jumps materially
    when this is ON, the regime work (Stage 1.x) is overkill.

  STAGE 1.1 -- EQIDV17S_STAGE1_FORCE_CAUSAL (default ON)
    Forces v17q's F7 (NIFTY/RS lookup shifted -5min so regime reflects
    only data known at the prior bar's close). Adds audit columns:
      regime_causal_mode, regime_lookup_time_ist, regime_time_lt_entry_flag

  STAGE 1.2 -- EQIDV17S_STAGE1_CLEAN_RS (default ON, additive only)
    Adds raw_rs / clean_rs_long / bearish_clean_rs / exhaustion_score_*
    / rs_quality_label and bucket columns. No rows dropped.

  STAGE 1.3 -- BREADTH (NOT BUILT in v17s -- needs offline universe
    cross-section builder. Scaffolded as a TODO; do not block on it.)

  EQIDV17S_DIAGNOSTIC_TABLES (default ON)
    On the final combined df, emit per-bucket CSVs into
    outputs_v17s_5min/diagnostics_<ts>/ for setup x rs bucket study.

  EQIDV17S_AUDIT_STRICT (default ON)
    Hard-asserts no row has regime_lookup_time >= entry_time when
    Stage 1.1 is on.

Defaults give you: v17q behavior, F7 forced ON, plus diagnostic columns
and tables. To run the falsification test, also set
EQIDV17S_STAGE0_DISABLE_WEAK_LONGS=1 and compare outputs vs default.

Outputs go to outputs_v17s_5min/.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Env helpers (need these BEFORE importing v17q so we can force F7).
# ---------------------------------------------------------------------------
def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


# ---------------------------------------------------------------------------
# V17s toggles. Read BEFORE the v17q import so STAGE1_FORCE_CAUSAL can set
# the upstream env var that v17q reads at module load.
# ---------------------------------------------------------------------------
V17S_STAGE0_DISABLE_WEAK_LONGS = _env_bool("EQIDV17S_STAGE0_DISABLE_WEAK_LONGS", False)
V17S_STAGE1_FORCE_CAUSAL       = _env_bool("EQIDV17S_STAGE1_FORCE_CAUSAL", True)
V17S_STAGE1_CLEAN_RS           = _env_bool("EQIDV17S_STAGE1_CLEAN_RS", True)
V17S_DIAGNOSTIC_TABLES         = _env_bool("EQIDV17S_DIAGNOSTIC_TABLES", True)
V17S_AUDIT_STRICT              = _env_bool("EQIDV17S_AUDIT_STRICT", True)

# F7 forcing must happen before v17q's module body runs.
if V17S_STAGE1_FORCE_CAUSAL and os.environ.get("EQIDV17Q_NIFTY_LOOKUP_PREV_BAR") is None:
    os.environ["EQIDV17Q_NIFTY_LOOKUP_PREV_BAR"] = "1"

import avwap_combined_runner_v17q_5min as _v17q  # noqa: E402  cascade
import avwap_combined_runner_v16_5min as _base  # noqa: E402


# ---------------------------------------------------------------------------
# Output dir routing -> outputs_v17s_5min/.
# ---------------------------------------------------------------------------
_orig_runtime_dir_v17s = _base.runtime_dir


def _v17s_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17s_5min", "v17r_5min", "v17q_5min", "v17p_5min", "v17o_5min",
            "v17n_5min", "v17m_5min", "v17l_5min", "v17k_5min", "v17j_5min",
            "v17i_5min", "v17h_5min", "v17g_5min", "v17f_5min", "v17d_5min",
            "v17c_5min", "v17b_5min", "v16_5min",
        ):
            text = text.replace(old, "v17s_5min")
        new_parts.append(text)
    return _orig_runtime_dir_v17s(*tuple(new_parts))


_base.runtime_dir = _v17s_runtime_dir


# ---------------------------------------------------------------------------
# Stage 0 -- drop the 4 weak LONG setups (toggle-gated).
# Hooks _apply_v16_post_scan_filters; runs AFTER any v17q-chain filters.
# ---------------------------------------------------------------------------
WEAK_LONG_SETUPS = (
    "C_OR_BREAKOUT",
    "G_HIGHER_HIGH_BREAK",
    "D_EMA20_BOUNCE",
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK",
)


def _drop_weak_longs(long_df: pd.DataFrame) -> pd.DataFrame:
    if long_df is None or long_df.empty or "setup" not in long_df.columns:
        return long_df
    setup_norm = long_df["setup"].astype(str).str.upper().str.strip()
    drop_mask = setup_norm.isin(WEAK_LONG_SETUPS)
    n_drop = int(drop_mask.sum())
    if n_drop == 0:
        return long_df
    print(f"[V17S_STAGE0] disabled {n_drop} LONG trades across {WEAK_LONG_SETUPS}")
    # Per-setup breakdown so user can verify expected drops
    for s in WEAK_LONG_SETUPS:
        n = int((setup_norm == s).sum())
        if n:
            print(f"  - {s}: -{n}")
    return long_df.loc[~drop_mask].copy()


if V17S_STAGE0_DISABLE_WEAK_LONGS:
    _v17_chain_post_scan = _base._apply_v16_post_scan_filters

    def _v17s_apply_post_scan_filters(
        short_df: pd.DataFrame,
        long_df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        short_df, long_df = _v17_chain_post_scan(short_df, long_df)
        long_df = _drop_weak_longs(long_df)
        return short_df, long_df

    _base._apply_v16_post_scan_filters = _v17s_apply_post_scan_filters


# ---------------------------------------------------------------------------
# Stage 1.2 -- clean RS + exhaustion enrichment.
#
# All purely additive on existing trade columns. Penalty thresholds are
# documented heuristics; tune via the diagnostic tables, do not blindly
# trust the starting numbers.
#
# Penalty weight is 25 per "tier" (so 4 hot conditions = score 100 = capped).
# clean_rs subtracts (penalty/25) from raw_rs, i.e. each tier eats 1.0 of RS.
# ---------------------------------------------------------------------------
EXH_W_AVWAP   = 25
EXH_W_VOL     = 25
EXH_W_RSI     = 25
EXH_W_ATRPCT  = 15
EXH_W_OPEN    = 10
EXH_W_LATE    = 10  # short-only late-day panic

# Bucket edges. Mirror the user's spec.
RS_BUCKET_EDGES = [-np.inf, -3.0, -2.0, -1.5, -1.0, -0.5, 0.5, 1.0, 1.5, 2.0, 3.0, np.inf]
RS_BUCKET_LABELS = [
    "<-3.0", "-3.0..-2.0", "-2.0..-1.5", "-1.5..-1.0", "-1.0..-0.5",
    "-0.5..0.5", "0.5..1.0", "1.0..1.5", "1.5..2.0", "2.0..3.0", ">=3.0",
]


def _num(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def _exhaustion_long(d: pd.DataFrame) -> pd.Series:
    avwap = _num(d, "avwap_dist_atr_signal")
    vol   = _num(d, "entry_bar_vol_ratio")
    rsi   = _num(d, "rsi_signal")
    atrp  = _num(d, "atr_pct_signal")
    bfo   = _num(d, "bars_from_open")

    score = pd.Series(0.0, index=d.index)
    score = score + np.where(avwap > 1.5, EXH_W_AVWAP, 0)
    score = score + np.where(vol   > 3.0, EXH_W_VOL,   0)
    score = score + np.where(rsi   > 75,  EXH_W_RSI,   0)
    score = score + np.where(atrp  > 4.0, EXH_W_ATRPCT, 0)
    score = score + np.where((bfo <= 3) & (vol > 4.0), EXH_W_OPEN, 0)
    return pd.Series(np.clip(score, 0, 100), index=d.index)


def _exhaustion_short(d: pd.DataFrame) -> pd.Series:
    avwap = _num(d, "avwap_dist_atr_signal")
    vol   = _num(d, "entry_bar_vol_ratio")
    rsi   = _num(d, "rsi_signal")
    atrp  = _num(d, "atr_pct_signal")
    bfo   = _num(d, "bars_from_open")

    score = pd.Series(0.0, index=d.index)
    score = score + np.where(avwap < -1.5, EXH_W_AVWAP, 0)  # stretched DOWN
    score = score + np.where(vol > 3.5,    EXH_W_VOL,   0)  # panic-level
    score = score + np.where(rsi < 25,     EXH_W_RSI,   0)
    score = score + np.where(atrp > 4.0,   EXH_W_ATRPCT, 0)
    score = score + np.where((bfo >= 50) & (vol > 3.5), EXH_W_LATE, 0)
    return pd.Series(np.clip(score, 0, 100), index=d.index)


def _rs_quality_label(raw_rs: pd.Series, clean_rs: pd.Series, exh: pd.Series) -> pd.Series:
    out = pd.Series("RS_WEAK", index=raw_rs.index, dtype="object")
    abs_raw   = raw_rs.abs()
    abs_clean = clean_rs.abs()
    out = out.where(~(exh >= 75), "RS_EXHAUSTED")
    mask_extended = (out == "RS_WEAK") & (exh >= 50)
    out = out.where(~mask_extended, "RS_EXTENDED")
    mask_strong = (out == "RS_WEAK") & (abs_clean >= 1.5)
    out = out.where(~mask_strong, "RS_CLEAN_STRONG")
    mask_early = (out == "RS_WEAK") & (abs_clean >= 0.5)
    out = out.where(~mask_early, "RS_CLEAN_EARLY")
    # Anything still RS_WEAK with very low |raw_rs| stays RS_WEAK.
    return out


def _bucket(series: pd.Series) -> pd.Series:
    return pd.cut(series, bins=RS_BUCKET_EDGES, labels=RS_BUCKET_LABELS,
                  include_lowest=True, right=False).astype("object").fillna("NA")


def _exh_bucket(score: pd.Series) -> pd.Series:
    out = pd.Series("LOW", index=score.index, dtype="object")
    out = out.where(score < 25, "MEDIUM")
    out = out.where(score < 50, "HIGH")
    out = out.where(score < 75, "EXTREME")
    out = out.where(score.notna(), "NA")
    return out


def _enrich_clean_rs(d: pd.DataFrame) -> pd.DataFrame:
    """Add raw_rs / exhaustion / clean_rs / quality / bucket columns."""
    if d is None or d.empty:
        return d
    if "nifty_rel_strength_pct" not in d.columns:
        print("[V17S_WARN] enrichment skipped: 'nifty_rel_strength_pct' missing")
        return d

    raw_rs = _num(d, "nifty_rel_strength_pct")
    side = d["side"].astype(str).str.upper() if "side" in d.columns else pd.Series("LONG", index=d.index)

    exh_long  = _exhaustion_long(d)
    exh_short = _exhaustion_short(d)

    # Effective exhaustion is side-specific.
    exh = np.where(side.eq("SHORT"), exh_short, exh_long)
    exh = pd.Series(exh, index=d.index)

    # clean_rs_long subtracts long exhaustion regardless of side (so we can
    # always see "what would the long-side clean RS look like for this row").
    # bearish_clean_rs is |raw_rs| - short_exhaustion (so positive = good short).
    clean_rs_long    = raw_rs - (exh_long  / 25.0)
    bearish_clean_rs = raw_rs.abs() - (exh_short / 25.0)

    # Side-aware "clean_rs" used for bucketing: long uses clean_rs_long,
    # short uses signed bearish_clean_rs (negative of raw direction).
    clean_rs = np.where(side.eq("SHORT"), -bearish_clean_rs, clean_rs_long)
    clean_rs = pd.Series(clean_rs, index=d.index)

    quality = _rs_quality_label(raw_rs, clean_rs, exh)

    out = d.copy()
    out["raw_rs"]                = raw_rs
    out["exhaustion_score_long"] = exh_long
    out["exhaustion_score_short"]= exh_short
    out["exhaustion_score"]      = exh
    out["clean_rs_long"]         = clean_rs_long
    out["bearish_clean_rs"]      = bearish_clean_rs
    out["clean_rs"]              = clean_rs
    out["rs_quality_label"]      = quality
    out["raw_rs_bucket"]         = _bucket(raw_rs)
    out["clean_rs_bucket"]       = _bucket(clean_rs)
    out["exhaustion_bucket"]     = _exh_bucket(exh)
    return out


# ---------------------------------------------------------------------------
# Stage 1.1 -- causality audit columns. Pure book-keeping; F7 itself is
# what shifts the actual lookup. We just record what regime time was used
# so the audit can prove no future leakage.
# ---------------------------------------------------------------------------
def _enrich_causality_audit(d: pd.DataFrame) -> pd.DataFrame:
    if d is None or d.empty or "entry_time_ist" not in d.columns:
        return d
    out = d.copy()
    entry_t = pd.to_datetime(out["entry_time_ist"], errors="coerce")

    # F7 active iff env was set when v17q loaded. Read the live module flag.
    f7_on = bool(getattr(_v17q, "V17Q_NIFTY_LOOKUP_PREV_BAR", False))
    out["regime_causal_mode"] = "PREV_BAR" if f7_on else "SAME_BAR"

    if f7_on:
        regime_t = entry_t - pd.Timedelta(minutes=5)
    else:
        regime_t = entry_t  # untouched -> same-bar lookup
    out["regime_lookup_time_ist"] = regime_t
    out["regime_time_lt_entry_flag"] = (regime_t < entry_t).astype("Int64")
    return out


def _audit_causality(d: pd.DataFrame) -> None:
    if not V17S_AUDIT_STRICT or d is None or d.empty:
        return
    if "regime_lookup_time_ist" not in d.columns or "entry_time_ist" not in d.columns:
        return
    rt = pd.to_datetime(d["regime_lookup_time_ist"], errors="coerce")
    et = pd.to_datetime(d["entry_time_ist"], errors="coerce")
    bad = (rt >= et) & rt.notna() & et.notna()
    n_bad = int(bad.sum())
    mode = d["regime_causal_mode"].iloc[0] if "regime_causal_mode" in d.columns else "?"
    if mode == "PREV_BAR" and n_bad > 0:
        # Hard fail when we promised prev-bar causality but observed otherwise.
        raise AssertionError(
            f"[V17S_AUDIT] {n_bad} rows have regime_lookup_time >= entry_time "
            f"under PREV_BAR mode -- causality violated"
        )
    print(f"[V17S_AUDIT] regime_causal_mode={mode}  rows={len(d)}  "
          f"future-leak rows={n_bad} (expected 0 in PREV_BAR mode)")


# ---------------------------------------------------------------------------
# Diagnostic tables. Emitted exactly once on the final combined df.
# ---------------------------------------------------------------------------
def _ensure_outcome_col(d: pd.DataFrame) -> pd.DataFrame:
    """Resolve a uniform 'outcome' column for grouping (TARGET/SL/EOD/...)."""
    if d is None or d.empty:
        return d
    if "outcome" in d.columns:
        return d
    for cand in ("exit_reason", "exit_type", "result"):
        if cand in d.columns:
            d = d.copy()
            d["outcome"] = d[cand].astype(str).str.upper()
            return d
    return d


def _agg_metrics(g: pd.DataFrame) -> pd.Series:
    pnl_col = "pnl_pct" if "pnl_pct" in g.columns else (
              "pnl_pct_price" if "pnl_pct_price" in g.columns else None)
    if pnl_col is None:
        return pd.Series({"trades": len(g)})
    pnl = pd.to_numeric(g[pnl_col], errors="coerce")
    wins = (pnl > 0).sum()
    losses = (pnl < 0).sum()
    gross_win = pnl[pnl > 0].sum()
    gross_loss = -pnl[pnl < 0].sum()
    pf = (gross_win / gross_loss) if gross_loss > 0 else np.nan

    out = {
        "trades":   int(len(g)),
        "wins":     int(wins),
        "losses":   int(losses),
        "win_pct":  round(100.0 * wins / max(1, len(g)), 2),
        "avg_pnl":  round(float(pnl.mean()), 4),
        "sum_pnl":  round(float(pnl.sum()), 4),
        "pf":       round(float(pf), 4) if pd.notna(pf) else np.nan,
    }
    if "outcome" in g.columns:
        oc = g["outcome"].astype(str).str.upper()
        n = max(1, len(g))
        out["target_pct"] = round(100.0 * (oc == "TARGET").sum() / n, 2)
        out["sl_pct"]     = round(100.0 * (oc == "SL").sum() / n, 2)
        out["eod_pct"]    = round(100.0 * (oc == "EOD").sum() / n, 2)
    return pd.Series(out)


def _emit_diagnostic_tables(d: pd.DataFrame) -> None:
    if not V17S_DIAGNOSTIC_TABLES or d is None or d.empty:
        return
    d = _ensure_outcome_col(d)

    # Output dir under outputs_v17s_5min/diagnostics_<ts>/
    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    out_dir = _v17s_runtime_dir("outputs_v17s_5min") / f"diagnostics_{ts}"
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        print(f"[V17S_WARN] could not create diagnostics dir: {exc}")
        return

    side_col = "side" if "side" in d.columns else None
    setup_col = "setup" if "setup" in d.columns else None
    if not side_col or not setup_col:
        print("[V17S_WARN] diagnostic tables skipped: side/setup column missing")
        return

    def _write(df, name):
        path = out_dir / name
        df.to_csv(path, index=False)
        print(f"[V17S_DIAG] {name}: {len(df)} rows -> {path}")

    # A. side x setup x raw_rs_bucket
    if "raw_rs_bucket" in d.columns:
        a = d.groupby([side_col, setup_col, "raw_rs_bucket"], dropna=False)\
             .apply(_agg_metrics).reset_index()
        _write(a, "A_setup_x_raw_rs_bucket.csv")

    # B. side x setup x clean_rs_bucket
    if "clean_rs_bucket" in d.columns:
        b = d.groupby([side_col, setup_col, "clean_rs_bucket"], dropna=False)\
             .apply(_agg_metrics).reset_index()
        _write(b, "B_setup_x_clean_rs_bucket.csv")

    # D. side x setup x exhaustion_bucket
    if "exhaustion_bucket" in d.columns:
        c = d.groupby([side_col, setup_col, "exhaustion_bucket"], dropna=False)\
             .apply(_agg_metrics).reset_index()
        _write(c, "D_setup_x_exhaustion_bucket.csv")

    # F. SL distribution stats
    if "outcome" in d.columns:
        feat_cols = [x for x in (
            "raw_rs", "clean_rs", "exhaustion_score",
            "avwap_dist_atr_signal", "entry_bar_vol_ratio",
            "rsi_signal", "atr_pct_signal", "bars_from_open",
        ) if x in d.columns]
        if feat_cols:
            sl_df = d[d["outcome"].astype(str).str.upper() == "SL"]
            tg_df = d[d["outcome"].astype(str).str.upper() == "TARGET"]

            def _dist(df_, label):
                if df_.empty:
                    return None
                rows = []
                for s in df_[side_col].dropna().unique():
                    for su in df_[df_[side_col] == s][setup_col].dropna().unique():
                        sub = df_[(df_[side_col] == s) & (df_[setup_col] == su)]
                        if len(sub) < 10:
                            continue
                        for f in feat_cols:
                            v = pd.to_numeric(sub[f], errors="coerce").dropna()
                            if v.empty:
                                continue
                            rows.append({
                                "side": s, "setup": su, "feature": f, "outcome": label,
                                "n": int(len(v)),
                                "mean":   round(float(v.mean()), 4),
                                "median": round(float(v.median()), 4),
                                "p25":    round(float(v.quantile(0.25)), 4),
                                "p75":    round(float(v.quantile(0.75)), 4),
                            })
                return pd.DataFrame(rows)

            sl_dist = _dist(sl_df, "SL")
            tg_dist = _dist(tg_df, "TARGET")
            if sl_dist is not None and not sl_dist.empty:
                _write(sl_dist, "F_loser_distribution_SL.csv")
            if tg_dist is not None and not tg_dist.empty:
                _write(tg_dist, "G_winner_distribution_TARGET.csv")

    # H. summary by side x setup (always useful)
    h = d.groupby([side_col, setup_col], dropna=False)\
         .apply(_agg_metrics).reset_index()
    _write(h, "H_summary_by_setup_x_side.csv")


# ---------------------------------------------------------------------------
# Master enrichment hook -- wraps _add_notional_pnl. Called 3x per main()
# (short_df, long_df, combined). Enrich on every call (idempotent); emit
# diagnostic tables only when both sides are present (final combined call).
# ---------------------------------------------------------------------------
_orig_add_notional_pnl = _base._add_notional_pnl


def _v17s_add_notional_pnl(df: pd.DataFrame) -> pd.DataFrame:
    out = _orig_add_notional_pnl(df)
    if out is None or out.empty:
        return out

    if V17S_STAGE1_FORCE_CAUSAL:
        out = _enrich_causality_audit(out)
    if V17S_STAGE1_CLEAN_RS:
        out = _enrich_clean_rs(out)

    # Final combined call has both LONG and SHORT rows. Audit + emit there.
    if "side" in out.columns and out["side"].astype(str).str.upper().nunique() >= 2:
        if V17S_STAGE1_FORCE_CAUSAL:
            _audit_causality(out)
        _emit_diagnostic_tables(out)

    return out


_base._add_notional_pnl = _v17s_add_notional_pnl


# ---------------------------------------------------------------------------
# Banner.
# ---------------------------------------------------------------------------
def _enabled_toggles() -> list[str]:
    flags = []
    flags.append(f"STAGE0_DISABLE_WEAK_LONGS={'on' if V17S_STAGE0_DISABLE_WEAK_LONGS else 'off'}")
    flags.append(f"STAGE1_FORCE_CAUSAL={'on' if V17S_STAGE1_FORCE_CAUSAL else 'off'}")
    flags.append(f"STAGE1_CLEAN_RS={'on' if V17S_STAGE1_CLEAN_RS else 'off'}")
    flags.append(f"DIAGNOSTIC_TABLES={'on' if V17S_DIAGNOSTIC_TABLES else 'off'}")
    flags.append(f"AUDIT_STRICT={'on' if V17S_AUDIT_STRICT else 'off'}")
    return flags


if __name__ == "__main__":
    print("=" * 78)
    print("V17s 5-min runner -- staged MARS falsification test")
    print("  Output dir: outputs_v17s_5min")
    print(f"  Active V17S toggles: {', '.join(_enabled_toggles())}")
    print(f"  Inherited V17Q F7 (NIFTY prev-bar): "
          f"{'ON' if getattr(_v17q, 'V17Q_NIFTY_LOOKUP_PREV_BAR', False) else 'OFF'}")
    print(f"  Stage 0 weak LONGs to drop: {WEAK_LONG_SETUPS}")
    print("  Stage 1.3 (breadth) deferred -- needs offline universe builder")
    print("=" * 78)
    _base.main()
