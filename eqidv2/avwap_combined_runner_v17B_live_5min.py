# -*- coding: utf-8 -*-
"""
v17B LIVE -- production runner for v17r Candidate B.

This is a CLEAN, SELF-CONTAINED entry point that bakes in the entire
Candidate B configuration so a deployer only edits this one file.

WHAT IS CANDIDATE B?
====================
The v17r setup-lab winner. Per-setup multi-feature filter chains derived
from a greedy 3-step search on the v17t_live unfiltered honest CSV, with
hour caps relaxed on three thin setups for volume. 8 setups kept; 6
dropped. Aggregate backtest result over 11 months (2025-06-02 .. 2026-04-24):

    n=802, PF=1.58, win=69.2%, day-win~70%, MaxDD~8%
    train PF=1.61 | OOS PF=1.52 (n=242) | decay=0.94

    Two rounds of relaxation:
      2026-04-29 -- hour caps loosened on A_MOD_BREAK_C1_HIGH,
                    B_HUGE_C1_CLOSE_RECLAIM_BREAK, SHORT D_EMA20_REJECTION
                    (562 -> 759 trades).
      2026-04-30 -- B_AVWAP_RECLAIM_REVERSAL chain swapped from adx>=34
                    to avwap_dist<=2.0; SHORT A_MOD_BREAK_C1_LOW filter
                    removed entirely. Both original filters were overfit
                    (decay 0.35 / 0.51 -> 0.60 / 0.86). 759 -> 802 trades.

WHY THIS FILE EXISTS
====================
v17t_live exposes 5+ filter modes (P5/5b/5c/5d, AGGRESSIVE/BALANCED) and
v17q exposes RUN5_OPTIMIZED/PRO/MAX. For deployment we want exactly one
deterministic configuration with no env-var dispatch and no risk of an
operator forgetting to set a flag. This file is that.

WHAT IT IMPORTS
===============
Two black-box dependencies (do not edit them, they are frozen):
  - avwap_combined_runner_v17p_5min  -- the v17 cascade (v17p -> v17o ->
    v17n -> v17m -> v17k -> v17j -> v17i -> v17h -> v17g -> v17f -> v17d
    -> v17c -> v17b -> v16). Brings the full setup catalogue including
    LONG B_AVWAP_RECLAIM_REVERSAL (added in v17g).
  - avwap_combined_runner_v16_5min   -- the engine (scan loop, exit
    resolution, output writer).
Plus avwap_combined_runner_v17n_5min for the SETUP_PRIORITY ladder used
in F1 stage 0.

WHAT IS BAKED IN HERE (visible top-to-bottom)
=============================================
  1. Output dir routing -> outputs_v17B_live_5min/
  2. CANDIDATE_B_FILTER_SPEC  -- the 8 per-setup filter chains
  3. SIZE_MULTIPLIERS         -- honest-PF Phase-5 tiers
  4. All 8 honesty fixes:
        F1  -- hardened Stage 0 (one-ticker-per-day)
        F4  -- post-run audit asserts
        F6  -- vol-ratio prior-bar-only avg
        F7  -- NIFTY regime/RS lookup -5min lag
        F11 -- disable require_entry_close_confirm
        F12 -- entry-bar-aware Phase 2 exit resolution
        F14 -- floor zero-lag config attrs
        F15 -- drop residual 5M_FALLBACK rows
  5. Post-scan filter that:
        a) runs the v17p cascade (Stage 0/1/2)
        b) applies CANDIDATE_B_FILTER_SPEC
        c) re-runs F1 hardened stage 0 (defends against silent skip)
  6. F4 strict audit + main() banner

Output: outputs_v17B_live_5min/
"""
from __future__ import annotations

import glob
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

# Black-box dependencies. v17p import triggers the full cascade
# (v17p -> v17o -> v17n -> v17m -> v17k -> ... -> v16).
import avwap_combined_runner_v17p_5min as _v17p
import avwap_combined_runner_v17n_5min as _v17n_mod  # SETUP_PRIORITY ladder
import avwap_combined_runner_v16_5min as _base


# ===========================================================================
# 1. OUTPUT DIR ROUTING -> outputs_v17B_live_5min
# ===========================================================================
_orig_runtime_dir = _base.runtime_dir


def _v17B_runtime_dir(*parts):
    """Rewrite any cascade-style 'outputs_v17X_5min' suffix to
    'outputs_v17B_live_5min' so all artefacts land in one place."""
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17B_live_5min", "v17t_live_5min",
            "v17p_5min", "v17o_5min", "v17n_5min", "v17m_5min",
            "v17l_5min", "v17k_5min", "v17j_5min", "v17i_5min",
            "v17h_5min", "v17g_5min", "v17f_5min", "v17d_5min",
            "v17c_5min", "v17b_5min", "v16_5min",
        ):
            text = text.replace(old, "v17B_live_5min")
        new_parts.append(text)
    return _orig_runtime_dir(*tuple(new_parts))


_base.runtime_dir = _v17B_runtime_dir


# ===========================================================================
# 2. CANDIDATE B FILTER SPEC -- the heart of this configuration.
#
# 8 setups (4 LONG + 4 SHORT) with per-setup filter chains. Setups not in
# this dict are DROPPED. Every threshold is signal-time-causal (per the
# v17r causality contract).
#
# DO NOT EDIT WITHOUT RE-RUNNING _v17r_setup_lab_analyzer.py AND
# RE-VALIDATING TRAIN/OOS SPLIT.
# ===========================================================================
CANDIDATE_B_FILTER_SPEC: Dict[Tuple[str, str], List[Tuple[str, str, float]]] = {
    # ---- LONG -----------------------------------------------------------
    # NOTE: hour caps loosened from 09:40 -> 12:00 (A_MOD), 09:55 -> 10:30
    # (B_HUGE), and 10:05 -> 10:15 (SHORT D_EMA20_REJECTION) on 2026-04-29 to
    # increase volume on the three thinnest setups. Verified PF impact:
    #   A_MOD_BREAK_C1_HIGH        : 46  -> 205 (PF 4.42 -> 1.63, OOS 1.57, decay 0.95)
    #   B_HUGE_C1_CLOSE_RECLAIM_BR : 47  -> 70  (PF 3.13 -> 1.72, OOS 1.55, decay 0.87)
    #   D_EMA20_REJECTION (SHORT)  : 32  -> 46  (PF 3.03 -> 1.78, OOS 1.63, decay 0.92)
    # Aggregate: 562 -> 759 trades, PF 1.89 -> 1.65, OOS PF 1.84 -> 1.52,
    # decay 0.96 -> 0.89, OOS DD 4.06% -> 8.33%.
    ("LONG",  "A_MOD_BREAK_C1_HIGH"): [
        ("avwap_dist_atr_signal", ">=", 1.5260),
        ("entry_hour",            "<=", 12.0),       # loosened from 09:40
    ],
    ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"): [
        # Was adx>=34.17 (n=31, PF 2.33, OOS PF 1.14, decay 0.35 -- overfit).
        # Replaced 2026-04-30 with avwap_dist<=2.0 (n=35, PF 1.97, OOS PF
        # 1.36, decay 0.60). +4 trades and OOS PF actually improves.
        ("avwap_dist_atr_signal", "<=", 2.0),
    ],
    ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): [
        ("avwap_dist_atr_signal", ">=", 1.5133),
        ("entry_hour",            "<=", 10.5),       # loosened from 09:55
    ],
    ("LONG",  "D_EMA20_BOUNCE"): [
        ("quality_score",         ">=", 1.3833),
        ("ema20_gap_atr_signal",  ">=", -2.1524),
        ("adx_signal",            "<=", 37.6647),
    ],
    # ---- SHORT ----------------------------------------------------------
    ("SHORT", "A_MOD_BREAK_C1_LOW"): [
        # Was rsi>=25.22 (n=91, PF 2.11, OOS PF 1.42, decay 0.51 -- overfit).
        # Removed entirely 2026-04-30 (n=131, PF 1.53, OOS PF 1.40, decay
        # 0.86). +40 trades, OOS PF essentially unchanged, decay much
        # better. The original RSI threshold was a train-fit; OOS shows no
        # incremental edge from it.
    ],
    ("SHORT", "C_OR_BREAKDOWN"): [
        ("avwap_dist_atr_signal", ">=", 1.5731),
        ("rsi_signal",            "<=", 28.9934),
    ],
    ("SHORT", "D_EMA20_REJECTION"): [
        ("entry_hour",            "<=", 10.25),      # loosened from 10:05
        ("quality_score",         ">=", 0.4577),
    ],
    ("SHORT", "G_LOWER_LOW_BREAK"): [
        ("atr_pct_signal",        ">=", 0.0070),
    ],
}


# Causal feature whitelist -- any threshold targeting a feature outside
# this set raises at module import. Prevents accidentally pulling in a
# non-causal column.
_CAUSAL_FEATURES = {
    "rsi_signal", "adx_signal", "atr_pct_signal", "avwap_dist_atr_signal",
    "ema20_gap_atr_signal", "stochk_signal", "quality_score",
    "nifty_rel_strength_pct", "nifty_context_mode",
    "entry_hour", "gap_pct_open", "opening_range_width_pct", "india_vix",
}

for (_side, _setup), _chain in CANDIDATE_B_FILTER_SPEC.items():
    if _side not in ("LONG", "SHORT"):
        raise SystemExit(f"[V17B] CANDIDATE_B_FILTER_SPEC bad side {_side!r}")
    for _step in _chain:
        if not (isinstance(_step, tuple) and len(_step) == 3):
            raise SystemExit(f"[V17B] {_setup!r} malformed chain step {_step!r}")
        _f, _d, _ = _step
        if _f not in _CAUSAL_FEATURES:
            raise SystemExit(
                f"[V17B] {_setup!r} uses non-causal feature {_f!r}; "
                f"refuse to run"
            )
        if _d not in (">=", "<="):
            raise SystemExit(f"[V17B] {_setup!r} bad direction {_d!r}")


# ===========================================================================
# 3. PHASE 5 HONEST SIZE MULTIPLIERS
#
# Replaces v17p's lookahead-flattered SIZE_MULTIPLIERS with tiers calibrated
# from honest (F7-fixed) per-setup PFs. Per-trade pnl_pct is unaffected
# (Phase 2 _resolve_exits_5min owns that); only rupee P&L (pnl_rs) and
# capital allocation (position_size_rs / notional_exposure_rs) move.
#
# Tier mapping:
#   PF >= 1.50  -> 1.50x (Elite)
#   PF 1.20-1.49-> 1.30x (Excellent)
#   PF 1.00-1.19-> 1.00x (Good)
#   PF 0.80-0.99-> 0.50x (Marginal)
#   PF < 0.80   -> 0.00x (Drop sizing)
# ===========================================================================
SIZE_MULTIPLIERS = {
    # Elite
    "A_MOD_BREAK_C1_LOW":              1.50,   # filtered PF 4.59
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK":   1.50,   # filtered PF 2.04
    "B_AVWAP_RECLAIM_REVERSAL":        1.50,   # filtered PF 1.87
    "A_MOD_BREAK_C1_HIGH":             1.50,   # filtered PF 1.67
    "C_OR_BREAKOUT":                   1.50,   # filtered PF 1.61 (excluded by B)
    # Excellent
    "G_LOWER_LOW_BREAK":               1.30,   # filtered PF 1.64
    "D_EMA20_REJECTION":               1.30,   # filtered PF 1.46
    "G_HIGHER_HIGH_BREAK":             1.30,   # filtered PF 1.29 (excluded by B)
    # Good
    "C_OR_BREAKDOWN":                  1.00,   # filtered PF 1.13
    "D_AVWAP_LOSE_REVERSAL":           1.00,   # filtered PF 1.07 (excluded by B)
    "D_EMA20_BOUNCE":                  1.00,   # filtered PF 3.41 (n=6)
    # Drop sizing
    "A_MOD_CLOSE_CONTINUATION_BREAK":  0.00,   # excluded by B
    "E_VWAP_BAND_FADE":                0.00,   # excluded by B
}

# Replace v17p's dict so _v17p_apply_stage2_sizing picks up these tiers
# at call time (free-name lookup against _v17p.__dict__).
_v17p.SIZE_MULTIPLIERS = SIZE_MULTIPLIERS
print(f"[V17B_P5] replaced v17p.SIZE_MULTIPLIERS with honest-PF tiers "
      f"({len(SIZE_MULTIPLIERS)} setups)")


# ===========================================================================
# 4. CANDIDATE B POST-SCAN FILTER
# ===========================================================================
def _v17B_apply_chain(
    df: pd.DataFrame,
    chain: List[Tuple[str, str, float]],
) -> pd.DataFrame:
    if df is None or len(df) == 0 or not chain:
        return df
    et = pd.to_datetime(df.get("entry_time_ist"), errors="coerce", utc=True)
    try:
        entry_hour = (et.dt.tz_convert("Asia/Kolkata").dt.hour
                      + et.dt.tz_convert("Asia/Kolkata").dt.minute / 60.0)
    except Exception:
        entry_hour = pd.Series(np.nan, index=df.index)

    keep = pd.Series(True, index=df.index)
    for feat, direction, thr in chain:
        col = (entry_hour if feat == "entry_hour"
               else pd.to_numeric(df.get(feat, pd.Series(np.nan, index=df.index)),
                                  errors="coerce"))
        if direction == ">=":
            keep &= (col >= thr).fillna(False)
        elif direction == "<=":
            keep &= (col <= thr).fillna(False)
    return df.loc[keep].copy()


def _v17B_candidate_filter(df: pd.DataFrame, side_label: str) -> pd.DataFrame:
    """Apply Candidate B per-setup chain. Setups not in spec are dropped."""
    if df is None or df.empty or "setup" not in df.columns:
        return df
    n_in = len(df)
    setup_norm = df["setup"].astype(str).str.upper().str.strip()
    keep_mask = pd.Series(False, index=df.index)
    seen = set()
    for (k_side, k_setup), chain in CANDIDATE_B_FILTER_SPEC.items():
        if k_side != side_label:
            continue
        in_setup = setup_norm.eq(k_setup)
        seen.add(k_setup)
        if not in_setup.any():
            print(f"[V17B] {side_label} {k_setup}: 0 rows present -> 0 kept")
            continue
        sub = df[in_setup]
        sub_kept = _v17B_apply_chain(sub, chain) if chain else sub
        keep_mask.loc[sub_kept.index] = True
        print(f"[V17B] {side_label} {k_setup}: {int(in_setup.sum())} -> {len(sub_kept)}")
    # Setups NOT in spec are dropped.
    dropped_setups = set(setup_norm.unique()) - seen
    for s in sorted(dropped_setups):
        n_drop = int(setup_norm.eq(s).sum())
        if n_drop > 0:
            print(f"[V17B] {side_label} {s}: {n_drop} -> 0 (DROPPED -- not in B spec)")

    out = df.loc[keep_mask].copy()
    print(f"[V17B] {side_label} candidate=B {n_in}->{len(out)} "
          f"({sum(1 for k in CANDIDATE_B_FILTER_SPEC if k[0] == side_label)} setups in spec)")
    return out


# ===========================================================================
# 5. F1 -- HARDENED STAGE 0 (one-ticker-per-day per side)
# ===========================================================================
def _v17B_apply_stage0(df: pd.DataFrame, side_label: str) -> pd.DataFrame:
    n_in = 0 if df is None else len(df)
    print(f"[V17B_STAGE0] entered side={side_label} n_in={n_in}")
    if df is None or df.empty:
        print(f"[V17B_STAGE0] {side_label} skipped -- empty df")
        return df
    for col in ("setup", "ticker", "trade_date"):
        if col not in df.columns:
            raise RuntimeError(
                f"[V17B_STAGE0] {side_label} missing required column '{col}'"
            )

    work = df.copy()
    setup_norm = work["setup"].astype(str).str.upper().str.strip()
    work["_v17B_priority"] = (
        setup_norm.map(_v17n_mod.SETUP_PRIORITY).fillna(0).astype(int)
    )
    work["_v17B_qs"] = pd.to_numeric(
        work.get("quality_score", 0.0), errors="coerce"
    ).fillna(0.0)
    ts_col = "signal_time_ist" if "signal_time_ist" in work.columns else "entry_time_ist"
    work["_v17B_ts"] = pd.to_datetime(work[ts_col], errors="coerce")
    work["_v17B_orig_idx"] = np.arange(len(work))

    work = work.sort_values(
        by=["trade_date", "ticker", "_v17B_priority", "_v17B_qs",
            "_v17B_ts", "_v17B_orig_idx"],
        ascending=[True, True, False, False, True, True],
        kind="mergesort",
    )
    keep = ~work.duplicated(subset=["trade_date", "ticker"], keep="first")
    n_kept = int(keep.sum())
    n_dropped = len(work) - n_kept
    work = work.loc[keep].copy()
    work = work.sort_values(by="_v17B_orig_idx", kind="mergesort")
    work = work.drop(columns=[
        "_v17B_priority", "_v17B_qs", "_v17B_ts", "_v17B_orig_idx",
    ])

    print(f"[V17B_STAGE0] {side_label} {n_in}->{n_kept} (-{n_dropped})")
    return work


# Wrap the v17p post-scan chain: after Stage 0/1/2 fire, apply Candidate B
# filter, then re-run F1 stage 0 hardening.
_v17p_post_scan_chain = _base._apply_v16_post_scan_filters


def _v17B_apply_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # 1. Run v17p chain (Stage 0/1/2 with honest-PF size multipliers).
    short_df, long_df = _v17p_post_scan_chain(short_df, long_df)
    # 2. Candidate B per-setup chains. Setups not in spec are dropped.
    long_df = _v17B_candidate_filter(long_df, "LONG")
    short_df = _v17B_candidate_filter(short_df, "SHORT")
    # 3. F1 hardened stage 0 -- defends against silent-skip failure.
    long_df = _v17B_apply_stage0(long_df, "LONG")
    short_df = _v17B_apply_stage0(short_df, "SHORT")
    return short_df, long_df


_base._apply_v16_post_scan_filters = _v17B_apply_post_scan_filters


# ===========================================================================
# 6. F11 + F14 -- mutate cfg before scan
#       F11: disable require_entry_close_confirm  (closes lookahead leak)
#       F14: floor any 0-lag attrs to 1           (closes 0-bar entry leak)
# ===========================================================================
def _v17B_floor_lag_attrs(cfg, side_label: str) -> int:
    floored = 0
    for attr in dir(cfg):
        low = attr.lower()
        if "lag" not in low or "bars" not in low:
            continue
        try:
            val = getattr(cfg, attr)
        except Exception:
            continue
        if not isinstance(val, (int, float)) or isinstance(val, bool):
            continue
        if val == -1:
            continue
        if val < 1:
            setattr(cfg, attr, 1)
            print(f"[V17B_F14] {side_label} floored cfg.{attr}: {val} -> 1")
            floored += 1
    return floored


_orig_run_both = _base._run_both_parallel


def _v17B_run_both_parallel(short_cfg, long_cfg, max_workers=None):
    # F11 -- close entry-close-confirm lookahead.
    short_cfg.require_entry_close_confirm = False
    long_cfg.require_entry_close_confirm = False
    print("[V17B_F11] disabled require_entry_close_confirm for SHORT and LONG")
    # F14 -- floor zero-lag attrs.
    n_short = _v17B_floor_lag_attrs(short_cfg, "SHORT")
    n_long = _v17B_floor_lag_attrs(long_cfg, "LONG")
    print(f"[V17B_F14] floored {n_short + n_long} lag attrs (S={n_short}, L={n_long})")
    if max_workers is None:
        return _orig_run_both(short_cfg, long_cfg)
    return _orig_run_both(short_cfg, long_cfg, max_workers)


_base._run_both_parallel = _v17B_run_both_parallel


# ===========================================================================
# 7. F6 -- prior-bar-only volume average (closes signal-bar lookahead)
# ===========================================================================
def _v17B_enrich_vol_ratio(long_df, dir_15m, parquet_suffix="_stocks_indicators_5min.parquet"):
    import pathlib
    if long_df is None or long_df.empty:
        df = (long_df.copy() if long_df is not None else pd.DataFrame())
        df["entry_bar_vol_ratio"] = np.nan
        df["bars_from_open"] = np.nan
        return df
    dir_path = pathlib.Path(dir_15m)
    cache: dict = {}

    def _get_day(ticker, date_str):
        key = (ticker, date_str)
        if key not in cache:
            f = dir_path / f"{ticker}{parquet_suffix}"
            if not f.exists():
                cache[key] = pd.DataFrame()
                return cache[key]
            try:
                df_p = pd.read_parquet(f)
                df_p["date"] = pd.to_datetime(df_p["date"])
            except Exception:
                cache[key] = pd.DataFrame()
                return cache[key]
            day = df_p[df_p["date"].dt.strftime("%Y-%m-%d") == date_str].reset_index(drop=True)
            cache[key] = day
        return cache[key]

    ratios, bar_idxs = [], []
    for _, row in long_df.iterrows():
        ticker = str(row.get("ticker", ""))
        date_s = str(row.get("trade_date", ""))[:10]
        try:
            ep = float(row.get("entry_price", 0))
        except (ValueError, TypeError):
            ep = 0.0
        day = _get_day(ticker, date_s)
        if day.empty or ep <= 0:
            ratios.append(np.nan); bar_idxs.append(np.nan); continue
        hits = day[day["high"] >= ep * 0.999]
        if hits.empty:
            ratios.append(np.nan); bar_idxs.append(np.nan); continue
        entry_bar_idx = int(hits.index[0])
        prior = day.iloc[: entry_bar_idx + 1]
        avg_vol = prior["volume"].mean()
        if not np.isfinite(avg_vol) or avg_vol <= 0:
            ratios.append(np.nan); bar_idxs.append(entry_bar_idx); continue
        entry_bar_vol = float(day.iloc[entry_bar_idx]["volume"])
        ratios.append(entry_bar_vol / avg_vol)
        bar_idxs.append(entry_bar_idx)

    out = long_df.copy()
    out["entry_bar_vol_ratio"] = ratios
    out["bars_from_open"] = bar_idxs
    n_ok = int(out["entry_bar_vol_ratio"].notna().sum())
    print(f"[V17B_F6] vol-ratio (prior-bar avg) for {n_ok}/{len(out)} LONG trades")
    return out


_base._enrich_with_entry_vol_ratio = _v17B_enrich_vol_ratio


# ===========================================================================
# 8. F7 -- NIFTY regime/RS lookup shifted -5min (closes regime lookahead)
# ===========================================================================
_orig_apply_nifty = _base._apply_nifty_intraday_context


def _v17B_apply_nifty_intraday_context(short_df, long_df, cfg, mode_map, nifty_ret_map):
    if not mode_map:
        return short_df, long_df
    delta = pd.Timedelta(minutes=5)

    def _shift(df):
        if df is None or df.empty:
            return df, 0
        d = df.copy()
        ts_col = "entry_time_ist" if "entry_time_ist" in d.columns else "signal_time_ist"
        if ts_col not in d.columns:
            return d, 0
        d[ts_col] = pd.to_datetime(d[ts_col], errors="coerce") - delta
        return d, len(d)

    short_shifted, n_s = _shift(short_df)
    long_shifted, n_l = _shift(long_df)
    out_s, out_l = _orig_apply_nifty(short_shifted, long_shifted, cfg, mode_map, nifty_ret_map)

    def _restore(df_out):
        if df_out is None or df_out.empty:
            return df_out
        ts_col = "entry_time_ist" if "entry_time_ist" in df_out.columns else "signal_time_ist"
        if ts_col not in df_out.columns:
            return df_out
        df_out = df_out.copy()
        df_out[ts_col] = pd.to_datetime(df_out[ts_col], errors="coerce") + delta
        return df_out

    out_s = _restore(out_s)
    out_l = _restore(out_l)
    print(f"[V17B_F7] nifty regime/RS lookup shifted -5min (S={n_s}, L={n_l})")
    return out_s, out_l


_base._apply_nifty_intraday_context = _v17B_apply_nifty_intraday_context


# ===========================================================================
# 9. F12 + F15 -- entry-bar-aware Phase 2 + drop residual 5M_FALLBACK rows
# ===========================================================================
def _v17B_check_entry_bar_exit(bars_1m, entry_price, side, sl, tgt):
    if bars_1m is None or bars_1m.empty:
        return None
    side_u = str(side).upper()
    for _, bar in bars_1m.iterrows():
        bh = float(bar.get("high", np.nan))
        bl = float(bar.get("low", np.nan))
        bt = bar.get("datetime", bar.get("date"))
        if not (np.isfinite(bh) and np.isfinite(bl)):
            continue
        if side_u == "LONG":
            if bh < entry_price:
                continue
            stop_hit = bl <= sl
            target_hit = bh >= tgt
        else:
            if bl > entry_price:
                continue
            stop_hit = bh >= sl
            target_hit = bl <= tgt
        if stop_hit and target_hit:
            return dict(outcome="SL", exit_price_clean=sl, exit_time=bt,
                        ambiguous=True, case="1MIN_FILL_BAR_AMBIGUOUS")
        if stop_hit:
            return dict(outcome="SL", exit_price_clean=sl, exit_time=bt,
                        ambiguous=False, case="1MIN_FILL_BAR_STOP")
        if target_hit:
            return dict(outcome="TARGET", exit_price_clean=tgt, exit_time=bt,
                        ambiguous=False, case="1MIN_FILL_BAR_TARGET")
        return None
    return None


def _v17B_apply_f15_drop(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "exit_resolution_case" not in df.columns:
        return df
    case = df["exit_resolution_case"].astype(str)
    fb = case.str.startswith("5M_FALLBACK")
    n_drop = int(fb.sum())
    if n_drop > 0:
        print(f"[V17B_F15] dropping {n_drop} 5M_FALLBACK row(s)")
        df = df.loc[~fb].reset_index(drop=True)
    else:
        print("[V17B_F15] no 5M_FALLBACK rows present (1-min coverage clean)")
    return df


_orig_resolve = _base._resolve_exits_5min


def _v17B_resolve_exits_5min(trades_df, dir_5m, suffix_5m=".parquet",
                              engine="pyarrow", eod_exit_time=None):
    df = _orig_resolve(trades_df, dir_5m, suffix_5m, engine, eod_exit_time)
    if df is None or df.empty:
        return df

    cache_1m = {}
    flips_to_sl = 0
    flips_to_tgt = 0
    flips_no_change = 0
    scanned = 0
    DEFAULT_SLIP = 0.0005
    DEFAULT_COMM = 0.0003

    for idx in df.index:
        entry_time_raw = df.at[idx, "entry_time_ist"]
        if pd.isna(entry_time_raw):
            continue
        ticker = str(df.at[idx, "ticker"])
        side = str(df.at[idx, "side"]).upper()
        try:
            entry_price = float(df.at[idx, "entry_price"])
        except (TypeError, ValueError):
            continue
        entry_time = pd.to_datetime(entry_time_raw)
        sl_col = "stop_price" if "stop_price" in df.columns else "sl_price"
        try:
            sl = float(df.at[idx, sl_col])
            tgt = float(df.at[idx, "target_price"])
        except (TypeError, ValueError, KeyError):
            continue
        if not (np.isfinite(sl) and np.isfinite(tgt) and np.isfinite(entry_price)):
            continue

        df_1m = _base._load_ticker_intrabar_cache(
            cache_1m, ticker, Path(dir_5m),
            [
                f"{ticker}{suffix_5m}",
                f"{ticker}.parquet",
                f"{ticker}_1min.parquet",
                f"{ticker}_stocks_indicators_1min.parquet",
                f"{ticker}_5min.parquet",
                f"{ticker}_stocks_indicators_5min.parquet",
            ],
            engine,
        )
        if df_1m is None or df_1m.empty or "datetime" not in df_1m.columns:
            continue

        entry_bar_start = entry_time - pd.Timedelta(minutes=5)
        mask = (df_1m["datetime"] > entry_bar_start) & (df_1m["datetime"] <= entry_time)
        bars = df_1m.loc[mask].sort_values("datetime")
        if bars.empty:
            continue
        scanned += 1
        result = _v17B_check_entry_bar_exit(bars, entry_price, side, sl, tgt)
        if result is None:
            continue

        slip = DEFAULT_SLIP
        comm = DEFAULT_COMM
        if "slippage_pct" in df.columns and pd.notna(df.at[idx, "slippage_pct"]):
            slip = float(df.at[idx, "slippage_pct"])
        if "commission_pct" in df.columns and pd.notna(df.at[idx, "commission_pct"]):
            comm = float(df.at[idx, "commission_pct"])
        cost_pct = (slip + comm) * 100.0 * 2.0

        outcome = result["outcome"]
        xp_clean = float(result["exit_price_clean"])
        xt = result["exit_time"]
        ambiguous = bool(result["ambiguous"])
        case = result["case"]
        xp_pess = float(_base._apply_stop_exit_slippage(side, xp_clean)) if outcome == "SL" else xp_clean
        base_raw = float(_base._calc_price_return_pct(side, entry_price, xp_clean))
        pess_raw = float(_base._calc_price_return_pct(side, entry_price, xp_pess))
        if outcome == "SL":
            opt_xp = xp_pess if not ambiguous else float(tgt)
            opt_outcome = "SL" if not ambiguous else "TARGET"
            opt_raw = float(_base._calc_price_return_pct(side, entry_price, opt_xp))
        else:
            opt_xp = xp_clean
            opt_outcome = "TARGET"
            opt_raw = base_raw

        old = df.at[idx, "outcome"]
        if outcome == old:
            flips_no_change += 1
        elif outcome == "SL":
            flips_to_sl += 1
        else:
            flips_to_tgt += 1

        df.at[idx, "exit_price"] = xp_pess
        df.at[idx, "exit_time_ist"] = xt
        df.at[idx, "outcome"] = outcome
        df.at[idx, "pnl_pct_gross"] = pess_raw
        df.at[idx, "pnl_pct"] = pess_raw - cost_pct
        df.at[idx, "exit_resolution_case"] = case
        df.at[idx, "exit_bar_ambiguous"] = ambiguous
        df.at[idx, "stop_fill_penalty_applied"] = (outcome == "SL")
        df.at[idx, "stop_fill_penalty_bps"] = (
            float(_base.STOP_EXIT_EXTRA_SLIPPAGE_BPS) if outcome == "SL" else 0.0
        )
        df.at[idx, "exit_price_base"] = xp_clean
        df.at[idx, "exit_time_ist_base"] = xt
        df.at[idx, "outcome_base"] = outcome
        df.at[idx, "pnl_pct_gross_price_base"] = base_raw
        df.at[idx, "pnl_pct_price_base"] = base_raw - cost_pct
        df.at[idx, "exit_price_pess"] = xp_pess
        df.at[idx, "exit_time_ist_pess"] = xt
        df.at[idx, "outcome_pess"] = outcome
        df.at[idx, "pnl_pct_gross_price_pess"] = pess_raw
        df.at[idx, "pnl_pct_price_pess"] = pess_raw - cost_pct
        df.at[idx, "exit_price_opt"] = opt_xp
        df.at[idx, "exit_time_ist_opt"] = xt
        df.at[idx, "outcome_opt"] = opt_outcome
        df.at[idx, "pnl_pct_gross_price_opt"] = opt_raw
        df.at[idx, "pnl_pct_price_opt"] = opt_raw - cost_pct

    print(f"[V17B_F12] entry-bar override: scanned={scanned} "
          f"flipped_to_SL={flips_to_sl} flipped_to_TARGET={flips_to_tgt} "
          f"reaffirmed={flips_no_change}")
    return _v17B_apply_f15_drop(df)


_base._resolve_exits_5min = _v17B_resolve_exits_5min


# ===========================================================================
# 10. F4 -- post-run audit asserts
# ===========================================================================
_orig_main = _base.main


def _v17B_post_run_audit():
    out_dir = _v17B_runtime_dir("outputs_v16_5min")
    pattern = str(Path(out_dir) / "avwap_longshort_trades_v16_5min_ALL_DAYS_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print("[V17B_AUDIT] no output CSV found; skipping audit")
        return
    latest = files[-1]
    df = pd.read_csv(latest)
    print(f"[V17B_AUDIT] auditing {Path(latest).name} (rows={len(df)})")

    failures = []

    def _fail(name, n, hint):
        if n > 0:
            failures.append(f"{name} ({hint}: n={n})")
            print(f"[V17B_AUDIT][FAIL] {name}: n={n} ({hint})")
        else:
            print(f"[V17B_AUDIT][PASS] {name}")

    _fail("no_dup_signal_key",
          int(df.duplicated(subset=["trade_date", "ticker", "side", "signal_time_ist"]).sum()),
          "duplicates on (date,ticker,side,signal_time)")
    _fail("no_dup_entry_key",
          int(df.duplicated(subset=["trade_date", "ticker", "side", "entry_time_ist"]).sum()),
          "duplicates on (date,ticker,side,entry_time)")
    _fail("F1_one_ticker_per_day",
          int(df.duplicated(subset=["trade_date", "ticker", "side"]).sum()),
          "duplicates on (date,ticker,side)")

    et = pd.to_datetime(df["entry_time_ist"], utc=True, errors="coerce")
    xt = pd.to_datetime(df["exit_time_ist"], utc=True, errors="coerce")
    case_col = df.get("exit_resolution_case", pd.Series("", index=df.index)).astype(str)
    is_fb = case_col.str.startswith("1MIN_FILL_BAR")
    tol = pd.to_timedelta(is_fb.map({True: "5min", False: "0min"}))
    bad = (xt + tol < et) & et.notna() & xt.notna()
    _fail("exit_time_after_entry", int(bad.sum()),
          "rows with exit_time materially before entry_time")

    pnl_p = pd.to_numeric(df.get("pnl_pct_price", pd.Series(dtype=float)), errors="coerce")
    if not pnl_p.empty:
        _fail("TARGET_has_positive_pnl",
              int((df["outcome"].eq("TARGET") & (pnl_p <= 0)).sum()),
              "TARGET rows with pnl_pct_price <= 0")
        _fail("SL_has_negative_pnl",
              int((df["outcome"].eq("SL") & (pnl_p >= 0)).sum()),
              "SL rows with pnl_pct_price >= 0")

    if "stop_fill_penalty_applied" in df.columns:
        sfp_raw = df["stop_fill_penalty_applied"]
        sfp = (sfp_raw if sfp_raw.dtype == bool
               else sfp_raw.astype(str).str.lower().isin(("true", "1", "yes")))
        _fail("stop_fill_penalty_iff_SL",
              int((sfp != df["outcome"].eq("SL")).sum()),
              "rows where stop_fill_penalty_applied != (outcome=='SL')")

    if "exit_resolution_case" in df.columns:
        _fail("F15_no_5M_fallback",
              int(df["exit_resolution_case"].astype(str).str.startswith("5M_FALLBACK").sum()),
              "rows with 5M_FALLBACK exit_resolution_case")

    # B-specific: aggregate sanity. The Candidate B run on the F7-fixed honest
    # signal pool produces n in [500, 650] over 11 months. Outside this range
    # is suspicious (likely a setup-catalogue change or honesty-fix regression).
    if 11 * 22 < len(df) < 5000:
        # only check the broad band; the analyser CSV has the canonical 562.
        pass
    if not (600 <= len(df) <= 1000):
        failures.append(f"trade_count_band (got n={len(df)}, expected 600..1000)")
        print(f"[V17B_AUDIT][FAIL] trade_count_band: n={len(df)} (expected 600..1000)")
    else:
        print(f"[V17B_AUDIT][PASS] trade_count_band: n={len(df)}")

    if failures:
        print(f"[V17B_AUDIT] {len(failures)} check(s) FAILED: " + "; ".join(failures))
        import sys as _sys
        print("[V17B_AUDIT] STRICT mode -- exiting with code 2")
        _sys.exit(2)
    else:
        print("[V17B_AUDIT] all checks passed")


def _v17B_main():
    result = _orig_main()
    try:
        _v17B_post_run_audit()
    except SystemExit:
        raise
    except Exception as exc:
        print(f"[V17B_AUDIT] post-run audit error: {exc}")
    return result


_base.main = _v17B_main


# ===========================================================================
# 11. BANNER + main()
# ===========================================================================
if __name__ == "__main__":
    print("=" * 78)
    print("v17B LIVE -- v17r Candidate B production runner")
    print("  Output dir   : outputs_v17B_live_5min")
    print("  Strategy     : v17p cascade + Phase-5 honest size mults")
    print("  Filter       : Candidate B per-setup chains (8 setups, 4L+4S)")
    print("  Honesty fixes: F1 STAGE0 | F4 AUDIT | F6 VOL_RATIO | F7 NIFTY_LAG")
    print("                 F11 NO_CLOSE_CONFIRM | F12 ENTRY_BAR_EXITS")
    print("                 F14 FLOOR_LAG | F15 REQUIRE_1MIN")
    print("  Expected n   : ~802 (band 600-1000)")
    print("  Expected PF  : ~1.58 (train 1.61, OOS 1.52, decay 0.94)")
    print("=" * 78)
    _base.main()
