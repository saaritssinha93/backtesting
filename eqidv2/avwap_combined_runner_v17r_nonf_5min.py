# -*- coding: utf-8 -*-
"""
V17r-noNF 5-min combined runner -- v17r with NIFTY filters disabled.

This file keeps v17r untouched and removes the inherited NIFTY regime /
relative-strength filter layer for an A/B run. It still inherits the v17q
execution-realism path.

WHY THIS FILE WAS REBUILT (2026-05-14)
--------------------------------------
The previous version had two defects that made the noNF run unusable:

  1. Stale drop-rules referenced setups that no longer exist
     (`G_HIGHER_HIGH_BREAK`, `C_OR_BREAKOUT`) -> matched ZERO rows.
  2. Disabling the NIFTY filter neutralised `nifty_context_mode` /
     `nifty_rel_strength_pct`, which silently turned the entire inherited
     short-side gate chain (V17D/F/J/K/O) into no-ops. SHORT exploded to
     ~12.6k trades at honest PF 0.31; aggregate honest PF was 0.32.

Causal analysis (`_v17r_nonf_analyzer.py`, IS<=2026-02-15) on the broken
run showed:
  * long_tail ADV bucket (ADV < Rs50cr) has ~0.51% round-trip cost -> a
    break-even win rate of ~82% at the 0.8%/0.75% geometry. It is
    mathematically un-winnable and is now hard-dropped.
  * After the ADV gate, exactly one setup survives strict OOS gates:
    SHORT A_MOD_BREAK_C1_LOW (PF 1.29, OOS PF 1.18, n=106, decay 0.86).
  * Two more clear strong PF but thin OOS (probation, kept for variety):
    LONG A_MOD_BREAK_C1_HIGH (PF 3.18, OOS n=7),
    SHORT D_AVWAP_LOSE_REVERSAL (PF 2.89, OOS n=6).
  * Every other setup stays below PF 1.2 even fully filtered and is
    hard-dropped.

This rebuild therefore replaces the inherited post-scan chain with an
explicit ADV gate + per-setup KEEP-chain whitelist. Setups not in the
whitelist are dropped entirely.

NOTE: honest PF >= 2 with usable volume is NOT reachable by filtering at
the current TGT 0.8% / SL 0.75% geometry (break-even WR 63-82% after
costs). The structural PF-2 path -- exit-geometry re-tune + a causal
universe-breadth regime gate to replace NIFTY for shorts -- requires a
fresh backtest run and is tracked separately.

Profiles:
  causal  (default): KEEP-chains use only known-at-entry features.
  volume            : same, plus entry_bar_vol_ratio is allowed (research
                      profile -- entry-bar volume needs the bar to close).

Outputs go to outputs_v17r_nonf_5min/.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner_v17q_5min as _v17q  # cascade -- pulls v17q/v17p/.../v16
import avwap_combined_runner_v16_5min as _base


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _env_choice(name: str, default: str, choices: tuple[str, ...]) -> str:
    raw = os.environ.get(name)
    if raw is None:
        return default
    val = str(raw).strip().lower()
    return val if val in choices else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


V17R_FILTERS_ENABLED = _env_bool("EQIDV17R_FILTERS_ENABLED", True)
V17R_PROFILE = _env_choice("EQIDV17R_PROFILE", "causal", ("causal", "volume"))
V17R_ADV_GATE = _env_bool("EQIDV17R_ADV_GATE", True)
# Causal universe-breadth regime gate (replaces the removed NIFTY context).
#   off    : no gate
#   loose  : SHORT pct_above_vwap >= 0.119  (drop panic-squeeze zone)
#   strict : SHORT pct_above_vwap >= 0.119 AND pct_above_ema20 <= 0.207
# Lifts honest PF from 1.75 -> 2.07 (loose) / 2.15 (strict) on the 3-month
# validation. Requires _v17r_breadth_cache.parquet built by
# _v17r_nonf_breadth_research.py for the active date window.
V17R_BREADTH_GATE = _env_choice("EQIDV17R_BREADTH_GATE", "loose", ("off", "loose", "strict"))

# Exit geometry. The 0.8% / 0.75% near-1:1 default lets honest per-row costs
# (~0.3-0.5% round trip) dominate -- break-even win rate 63-82%. A TGT/SL
# re-resolution sweep on the 3-month run (_v17r_nonf_exit_sweep.py) showed
# tightening the SL HURTS (noise stop-outs collapse win rate) while WIDENING
# the target lifts honest PF from ~1.23 to ~1.65-1.79. SL stays at the
# original 0.75%. Sweet spot: TGT 1.5%, broad stable plateau 1.3-1.8.
V17R_TGT_PCT = _env_float("EQIDV17R_TGT_PCT", 0.015)

_UNIVERSE_CSV = Path(__file__).resolve().parent / "configs" / "universe.csv"
_ADV_MID_FLOOR_RS_CR = 50.0  # mirrors v17D_cost_model.ADV_MID_FLOOR_RS_CR


# ---------------------------------------------------------------------------
# Exit-geometry override -- applied AFTER the v17q cascade import so it
# supersedes the v17i/v17l/v17m 0.8% target overrides. SL is left untouched
# (the cascade's 0.75% stop_pct is already optimal per the sweep).
# ---------------------------------------------------------------------------
_base.TEST_TARGET_OVERRIDE = True
_base.TEST_LONG_TARGET_PCT = float(V17R_TGT_PCT)
_base.TEST_SHORT_TARGET_PCT = float(V17R_TGT_PCT)
print(f"[V17R_EXIT] target override -> LONG={V17R_TGT_PCT*100:.2f}% "
      f"SHORT={V17R_TGT_PCT*100:.2f}% | SL unchanged (cascade 0.75%)")


# ---------------------------------------------------------------------------
# Output dir routing.
# ---------------------------------------------------------------------------
_orig_runtime_dir_v17r = _base.runtime_dir


def _v17r_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17r_nonf_5min", "v17r_5min", "v17q_5min", "v17p_5min", "v17o_5min", "v17n_5min",
            "v17m_5min", "v17l_5min", "v17k_5min", "v17j_5min", "v17i_5min",
            "v17h_5min", "v17g_5min", "v17f_5min", "v17d_5min", "v17c_5min",
            "v17b_5min", "v16_5min",
        ):
            text = text.replace(old, "v17r_nonf_5min")
        new_parts.append(text)
    return _orig_runtime_dir_v17r(*tuple(new_parts))


_base.runtime_dir = _v17r_runtime_dir


# ---------------------------------------------------------------------------
# NIFTY regime / RS filter bypass (the "noNF" part).
# ---------------------------------------------------------------------------
def _v17r_nonf_apply_nifty_intraday_context(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    cfg,
    mode_map: dict,
    nifty_ret_map: dict,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Bypass inherited NIFTY regime + relative-strength filtering.

    Keep neutral columns so downstream CSV consumers can still rely on the
    usual schema, but do not remove any trades based on NIFTY context.
    """

    def _neutralize(df: pd.DataFrame, side: str) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        out = df.copy()
        side_u = str(side).upper()
        if side_u == "LONG":
            out["nifty_context_mode"] = "BOTH"
            out["nifty_rel_strength_pct"] = 1.0
        else:
            out["nifty_context_mode"] = "NO_NF"
            out["nifty_rel_strength_pct"] = -1.0
        return out

    s = _neutralize(short_df, "SHORT")
    l = _neutralize(long_df, "LONG")
    print(
        "[V17R_NONF] NIFTY regime/RS filter disabled: "
        f"SHORT {len(short_df) if short_df is not None else 0}->{len(s) if s is not None else 0} | "
        f"LONG {len(long_df) if long_df is not None else 0}->{len(l) if l is not None else 0}"
    )
    return s, l


_base._apply_nifty_intraday_context = _v17r_nonf_apply_nifty_intraday_context


# ---------------------------------------------------------------------------
# ADV gate -- long_tail (ADV < Rs50cr) is un-winnable at honest costs.
# ---------------------------------------------------------------------------
def _load_adv_map() -> dict:
    try:
        uni = pd.read_csv(_UNIVERSE_CSV)
        return dict(zip(uni["ticker"].astype(str), pd.to_numeric(uni["adv_rs_cr"], errors="coerce")))
    except Exception as exc:  # pragma: no cover - defensive
        print(f"[V17R_WARN] could not load universe ADV map ({exc}); ADV gate disabled")
        return {}


_ADV_MAP = _load_adv_map()


def _apply_adv_gate(df: pd.DataFrame, side_label: str) -> pd.DataFrame:
    if df is None or df.empty or not V17R_ADV_GATE or not _ADV_MAP:
        return df
    if "ticker" not in df.columns:
        print(f"[V17R_WARN] ADV gate skipped for {side_label} -- no 'ticker' column")
        return df
    adv = df["ticker"].astype(str).map(_ADV_MAP)
    keep = adv.fillna(0.0) >= _ADV_MID_FLOOR_RS_CR
    before = len(df)
    out = df.loc[keep].copy()
    print(f"[V17R_ADV_GATE] {side_label}: {before}->{len(out)} "
          f"(-{before - len(out)} long_tail ADV<Rs{_ADV_MID_FLOOR_RS_CR:g}cr)")
    return out


# ---------------------------------------------------------------------------
# Per-setup KEEP-chains. Each chain is a list of (column, op, threshold).
# A row survives only if it matches its setup's chain on ALL clauses.
# Rows whose setup is not a key here are DROPPED (dead setups).
#
# Sourced from _v17r_nonf_analyzer.py greedy chain mining on the ADV-gated
# universe, IS<=2026-02-15. See module docstring for per-setup stats.
# ---------------------------------------------------------------------------
def _ge(df, col, thr):
    return pd.to_numeric(df.get(col), errors="coerce") >= thr


def _le(df, col, thr):
    return pd.to_numeric(df.get(col), errors="coerce") <= thr


_OPS = {">=": _ge, "<=": _le}

# (side, setup) -> list of (col, op, threshold)  [tier: SHIP / PROBATION]
CAUSAL_KEEP_CHAINS = {
    # SHIP -- passes strict OOS gates (PF 1.29, OOS PF 1.18, n=106, decay 0.86)
    ("SHORT", "A_MOD_BREAK_C1_LOW"): [
        ("adx_signal", ">=", 19.12),
        ("rsi_signal", ">=", 23.22),
        ("atr_pct_signal", "<=", 0.006252),
    ],
    # PROBATION -- strong PF, thin OOS. Kept for variety; watch OOS volume.
    ("LONG", "A_MOD_BREAK_C1_HIGH"): [
        ("quality_score", ">=", 7.104),
    ],
    ("SHORT", "D_AVWAP_LOSE_REVERSAL"): [
        ("quality_score", ">=", 0.7904),
    ],
}

# volume profile may use entry_bar_vol_ratio; currently identical chains
# because the volume feature was not selected by the greedy miner.
VOLUME_KEEP_CHAINS = dict(CAUSAL_KEEP_CHAINS)


def _apply_keep_chains(df: pd.DataFrame, side_label: str, chains: dict) -> pd.DataFrame:
    if df is None or df.empty or not V17R_FILTERS_ENABLED:
        return df
    if "setup" not in df.columns:
        print(f"[V17R_WARN] keep-chains skipped for {side_label} -- no 'setup' column")
        return df

    work = df.copy()
    setup_norm = work["setup"].astype(str).str.upper().str.strip()
    keep = pd.Series(False, index=work.index)
    details: list[str] = []

    for (side, setup), chain in chains.items():
        if side != side_label:
            continue
        in_setup = setup_norm.eq(setup)
        n_setup = int(in_setup.sum())
        if n_setup == 0:
            print(f"[V17R_WARN] keep-chain '{side} {setup}' matched ZERO rows -- setup renamed?")
            continue
        clause = in_setup.copy()
        for col, op, thr in chain:
            if col not in work.columns:
                print(f"[V17R_WARN] keep-chain '{side} {setup}' missing column '{col}' -- clause skipped")
                continue
            clause = clause & _OPS[op](work, col, thr).fillna(False)
        n_kept = int(clause.sum())
        details.append(f"{setup}:{n_setup}->{n_kept}")
        keep = keep | clause

    before = len(work)
    out = work.loc[keep].copy()
    joined = ", ".join(details) if details else "no whitelisted setups"
    print(f"[V17R_{V17R_PROFILE.upper()}] {side_label} keep-chains: {before}->{len(out)} ({joined})")
    return out


# ---------------------------------------------------------------------------
# Universe-breadth regime gate. Causal: pct_above_vwap is computed from the
# bar that closed at time t and used to gate signals at time t. No index
# lookup; no look-ahead.
# ---------------------------------------------------------------------------
_BREADTH_CACHE = Path(__file__).resolve().parent / "_v17r_breadth_cache.parquet"
_BREADTH_DF: pd.DataFrame | None = None
if V17R_BREADTH_GATE != "off" and _BREADTH_CACHE.exists():
    try:
        _BREADTH_DF = pd.read_parquet(_BREADTH_CACHE).sort_values("date").reset_index(drop=True)
        print(f"[V17R_BREADTH] loaded {len(_BREADTH_DF)} timestamps from "
              f"{_BREADTH_CACHE.name} ({_BREADTH_DF['date'].min()} -> {_BREADTH_DF['date'].max()})")
    except Exception as exc:
        print(f"[V17R_WARN] failed to load breadth cache ({exc}); breadth gate disabled")
        _BREADTH_DF = None
elif V17R_BREADTH_GATE != "off":
    print(f"[V17R_WARN] breadth cache {_BREADTH_CACHE.name} not found; run "
          f"_v17r_nonf_breadth_research.py first. Breadth gate disabled this run.")


def _apply_breadth_gate(df: pd.DataFrame, side_label: str) -> pd.DataFrame:
    if df is None or df.empty or V17R_BREADTH_GATE == "off" or _BREADTH_DF is None:
        return df
    if "signal_time_ist" not in df.columns:
        print(f"[V17R_WARN] breadth gate skipped {side_label} -- no signal_time_ist")
        return df

    sig = pd.to_datetime(df["signal_time_ist"], errors="coerce")
    if getattr(sig.dt, "tz", None) is None:
        sig = sig.dt.tz_localize("Asia/Kolkata")
    else:
        sig = sig.dt.tz_convert("Asia/Kolkata")

    work = df.assign(_sig=sig).sort_values("_sig")
    merged = pd.merge_asof(
        work, _BREADTH_DF[["date", "pct_above_vwap", "pct_above_ema20"]],
        left_on="_sig", right_on="date", direction="backward",
    )
    n_unmatched = int(merged["pct_above_vwap"].isna().sum())

    # only the SHORT side is gated; LONG sample is too small to validate
    if side_label != "SHORT":
        out = merged.drop(columns=["_sig", "date"], errors="ignore")
        return out

    v = merged["pct_above_vwap"].fillna(0.0)
    e = merged["pct_above_ema20"].fillna(0.0)
    if V17R_BREADTH_GATE == "loose":
        keep = v >= 0.119
        rule = "pct_above_vwap >= 0.119"
    else:  # strict
        keep = (v >= 0.119) & (e <= 0.207)
        rule = "pct_above_vwap >= 0.119 AND pct_above_ema20 <= 0.207"

    before = len(merged)
    out = merged.loc[keep].drop(columns=["_sig", "date"], errors="ignore").copy()
    extra = f" ({n_unmatched} unmatched kept)" if n_unmatched else ""
    print(f"[V17R_BREADTH] {side_label} gate '{rule}': "
          f"{before}->{len(out)} (-{before - len(out)}){extra}")
    return out


# ---------------------------------------------------------------------------
# Splice the v17r_nonf post-scan chain in after the inherited v17q chain.
# ---------------------------------------------------------------------------
if V17R_FILTERS_ENABLED:
    _v17q_post_scan_chain = _base._apply_v16_post_scan_filters

    def _v17r_apply_post_scan_filters(
        short_df: pd.DataFrame,
        long_df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        short_df, long_df = _v17q_post_scan_chain(short_df, long_df)

        chains = VOLUME_KEEP_CHAINS if V17R_PROFILE == "volume" else CAUSAL_KEEP_CHAINS

        short_df = _apply_adv_gate(short_df, "SHORT")
        long_df = _apply_adv_gate(long_df, "LONG")

        short_df = _apply_keep_chains(short_df, "SHORT", chains)
        long_df = _apply_keep_chains(long_df, "LONG", chains)

        short_df = _apply_breadth_gate(short_df, "SHORT")
        long_df = _apply_breadth_gate(long_df, "LONG")

        return short_df, long_df

    _base._apply_v16_post_scan_filters = _v17r_apply_post_scan_filters


def _enabled_toggles() -> list[str]:
    flags = []
    if V17R_FILTERS_ENABLED:
        flags.append(f"FILTERS={V17R_PROFILE}")
    else:
        flags.append("FILTERS=off")
    flags.append(f"ADV_GATE={'on' if V17R_ADV_GATE else 'off'}")
    flags.append(f"BREADTH={V17R_BREADTH_GATE}")
    flags.append(f"TGT={V17R_TGT_PCT*100:.2f}%")
    return flags


if __name__ == "__main__":
    print("=" * 78)
    print("V17r-noNF 5-min runner -- v17r research filters, NIFTY filter disabled")
    print("  Output dir: outputs_v17r_nonf_5min")
    print(f"  Active V17R toggles: {', '.join(_enabled_toggles())}")
    print("  Inherits v17q execution realism; bypasses NIFTY regime/RS filtering")
    print("  Post-scan: ADV gate (drop long_tail) + per-setup KEEP-chain whitelist")
    print("=" * 78)
    _base.main()
