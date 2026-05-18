# -*- coding: utf-8 -*-
"""
V17g 5-min combined runner — surgical merge of Codex architectural cleanup
and the mechanical critique. Implements the v17g proposal from
``reports/strategy_comparisons/v17f_new_design_proposal.md``.

Design goals
------------
1. Do NOT modify v17f. This file is a new variant, import-and-patch style
   on top of v17f (which already chains v17d -> v17c -> v17b -> v16).
2. Each of the five v17g changes is gated by an EQIDV17G_* env toggle so
   the v17g_ab_runner can A/B every change independently.
3. Output goes to outputs_v17g_5min/.

The five v17g changes
---------------------
Change 1 (PRIMARY)   — fix LONG A_PULLBACK_C2_THEN_BREAK_C2_HIGH lag bug
                       (currently 1 in v16 live-parity profile, makes the
                       breakout test self-referential; switch to 2).
Change 2 (PRIMARY)   — add LONG B_AVWAP_RECLAIM_REVERSAL setup. The
                       scanner branch is implemented in
                       avwap_long_strategy_v9_sweep.py (_scan_reversal_at,
                       called before impulse classification in
                       scan_one_day) and toggled here via the cfg flag
                       enable_setup_b_avwap_reclaim_reversal.
Change 3 (PRIMARY)   — setup-level size_multiplier as a post-scan column.
                       Does NOT alter entry quantity in the executor; it
                       tags trades for downstream weighted-metric reporting.
Change 4 (SECONDARY) — replace v17d's two BOTH-mode time pockets and v17f's
                       12:15-12:45 pocket with one causal rule:
                       drop BOTH-mode shorts when adx_signal < threshold
                       AND atr_pct_signal < threshold (low-trend AND
                       low-volatility = noise environment). The three
                       time-pocket flags are turned off.
Change 5 (SECONDARY) — add a LONG signal AVWAP distance cap (default 2.25)
                       symmetric to the SHORT cap (2.10) already in place.

Notes on Change 4 — the proposal originally suggested using NIFTY 5-bar
return + stock vol ratio. Those columns are not present on the trade row
in the current emit path; substituting adx_signal + atr_pct_signal is
causally aligned (the three time pockets share low-trend + low-volatility
character) and uses columns that are guaranteed available.

Notes on Change 2 — the reversal scanner is implemented as
``_scan_reversal_at`` at module level in
``eqidv2/avwap_v11_refactored/avwap_long_strategy_v9_sweep.py`` and is
called from ``scan_one_day`` immediately after the signal-window check
and before impulse classification. It uses its own filter stack
(prior-bar-below-AVWAP + reclaim + body/upper-40% + RSI rising +
ADX>=22 + K>D + volume >= cfg.reversal_volume_min_ratio) and is
guarded by ``cfg.enable_setup_b_avwap_reclaim_reversal`` (set here in
``_v17g_adjust_long_cfg`` when EQIDV17G_ENABLE_LONG_REVERSAL=1).
"""
from __future__ import annotations

import os
from typing import Tuple

import numpy as np
import pandas as pd

from . import v17f_5min as _v17f
from . import v17d_5min as _v17d
from . import v16_5min as _base


# ---------------------------------------------------------------------------
# Local env helpers (mirror the v17d/v17f style).
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
# V17G env toggles (defaults match v17g-final, all ON).
# ---------------------------------------------------------------------------
V17G_FIX_LONG_PULLBACK_LAG     = _env_bool("EQIDV17G_FIX_LONG_PULLBACK_LAG", True)
V17G_ENABLE_LONG_REVERSAL      = _env_bool("EQIDV17G_ENABLE_LONG_REVERSAL", True)
V17G_SETUP_LEVEL_SIZING        = _env_bool("EQIDV17G_SETUP_LEVEL_SIZING", True)
V17G_CONSOLIDATE_SHORT_POCKETS = _env_bool("EQIDV17G_CONSOLIDATE_SHORT_POCKETS", True)
V17G_LONG_AVWAP_DIST_CAP_ON    = _env_bool("EQIDV17G_LONG_AVWAP_DIST_CAP_ON", True)

V17G_LONG_PULLBACK_LAG_FIXED   = int(_env_float("EQIDV17G_LONG_PULLBACK_LAG", 2))
V17G_LONG_AVWAP_DIST_CAP_ATR   = _env_float("EQIDV17G_LONG_AVWAP_DIST_CAP_ATR", 2.25)
V17G_BOTH_CAUSAL_ADX_MAX       = _env_float("EQIDV17G_BOTH_CAUSAL_ADX_MAX", 25.0)
V17G_BOTH_CAUSAL_ATR_PCT_MAX   = _env_float("EQIDV17G_BOTH_CAUSAL_ATR_PCT_MAX", 0.005)

# Reversal-setup tuning (Change 2 — tightened in response to over-emit at 9,671 trades)
V17G_REVERSAL_BODY_ATR_MIN          = _env_float("EQIDV17G_REVERSAL_BODY_ATR_MIN", 0.50)
V17G_REVERSAL_CLOSE_UPPER_PCT       = _env_float("EQIDV17G_REVERSAL_CLOSE_UPPER_PCT", 0.40)
V17G_REVERSAL_RSI_MIN               = _env_float("EQIDV17G_REVERSAL_RSI_MIN", 50.0)
V17G_REVERSAL_ADX_MIN               = _env_float("EQIDV17G_REVERSAL_ADX_MIN", 28.0)
V17G_REVERSAL_VOL_MIN_RATIO         = _env_float("EQIDV17G_REVERSAL_VOL_MIN_RATIO", 1.20)
V17G_REVERSAL_MAX_HOUR_IST          = int(_env_float("EQIDV17G_REVERSAL_MAX_HOUR_IST", 12))
V17G_REVERSAL_REQUIRE_BOTH_PRIORS   = _env_bool("EQIDV17G_REVERSAL_REQUIRE_BOTH_PRIORS", True)
V17G_REVERSAL_REQUIRE_CLOSE_GT_EMA20 = _env_bool("EQIDV17G_REVERSAL_REQUIRE_CLOSE_GT_EMA20", True)


# ---------------------------------------------------------------------------
# Per-setup size multipliers (Change 3).
# ---------------------------------------------------------------------------
V17G_SIZE_MULTIPLIERS = {
    # Core
    "A_MOD_BREAK_C1_HIGH":               1.00,
    "A_MOD_BREAK_C1_LOW":                1.00,
    # Core-subtype
    "A_MOD_CLOSE_CONTINUATION_BREAK":    0.85,
    # Pullback (newly fixed)
    "A_PULLBACK_C2_THEN_BREAK_C2_HIGH":  0.75,
    # Event
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK":     0.60,
    "B_HUGE_RED_FAILED_BOUNCE":          0.60,
    # Reversal (new)
    "B_AVWAP_RECLAIM_REVERSAL":          0.50,
}
V17G_SIZE_DEFAULT = 1.00


# ---------------------------------------------------------------------------
# PATCH 1: route outputs to the new v17g folder.
# ---------------------------------------------------------------------------
_orig_runtime_dir = _v17f._orig_runtime_dir


def _v17g_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17g_5min",
            "v17f_5min",
            "v17d_5min",
            "v17c_5min",
            "v17b_5min",
            "v16_5min",
        ):
            text = text.replace(old, "v17g_5min")
        new_parts.append(text)
    return _orig_runtime_dir(*tuple(new_parts))


_base.runtime_dir = _v17g_runtime_dir


# ---------------------------------------------------------------------------
# PATCH 2: long-side config tweaks (Change 1, Change 5).
# Override apply_live_parity_profile so v16 line 766 lag=1 becomes lag=2 and
# the LONG AVWAP distance cap is set. Composes on top of v17b/v17f patches
# already in the chain.
# ---------------------------------------------------------------------------
_v17g_orig_apply_live_parity_profile = _base.apply_live_parity_profile


def _v17g_apply_live_parity_profile(short_cfg, long_cfg):
    short_cfg, long_cfg = _v17g_orig_apply_live_parity_profile(short_cfg, long_cfg)

    if V17G_FIX_LONG_PULLBACK_LAG:
        try:
            long_cfg.lag_bars_long_a_pullback_c2_break_c2_high = int(
                V17G_LONG_PULLBACK_LAG_FIXED
            )
        except Exception:
            pass

    if V17G_LONG_AVWAP_DIST_CAP_ON:
        try:
            long_cfg.signal_avwap_dist_atr_max = float(V17G_LONG_AVWAP_DIST_CAP_ATR)
        except Exception:
            pass

    return short_cfg, long_cfg


_base.apply_live_parity_profile = _v17g_apply_live_parity_profile


# Also need to override the in-line long_cfg construction inside _base.main()
# because that path reassigns lag = 1 and ignores apply_live_parity_profile's
# value (see avwap_combined_runner_v16_5min.py line 3650). Patch via env knob
# binding: v16 reads LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH etc as constants — the
# pullback lag is hard-coded to 1 in main(). The cleanest fix without touching
# v16 is to monkey-patch the StrategyConfig.__setattr__ briefly; but we already
# have apply_live_parity_profile in the chain, and v16's main() calls
# apply_live_parity_profile after its own setup block. Verify by trace.
#
# Empirically: v16 main() sets lag=1 at line 3650, then proceeds. Live-parity
# profile is invoked elsewhere (live wrappers and some test paths). For the
# backtest main() path we additionally need to override the post-construction
# value. The simplest approach: patch the long-side scanner entry to coerce
# the lag at scan-time.

_v17g_orig_scan_long_prepared = getattr(_base, "scan_long_prepared", None)


def _v17g_adjust_long_cfg(cfg):
    if V17G_FIX_LONG_PULLBACK_LAG:
        try:
            cfg.lag_bars_long_a_pullback_c2_break_c2_high = int(
                V17G_LONG_PULLBACK_LAG_FIXED
            )
        except Exception:
            pass
    if V17G_LONG_AVWAP_DIST_CAP_ON:
        try:
            cfg.signal_avwap_dist_atr_max = float(V17G_LONG_AVWAP_DIST_CAP_ATR)
        except Exception:
            pass
    if V17G_ENABLE_LONG_REVERSAL:
        try:
            cfg.enable_setup_b_avwap_reclaim_reversal = True
            cfg.reversal_body_atr_min = float(V17G_REVERSAL_BODY_ATR_MIN)
            cfg.reversal_close_upper_pct = float(V17G_REVERSAL_CLOSE_UPPER_PCT)
            cfg.reversal_rsi_min = float(V17G_REVERSAL_RSI_MIN)
            cfg.reversal_adx_min = float(V17G_REVERSAL_ADX_MIN)
            cfg.reversal_volume_min_ratio = float(V17G_REVERSAL_VOL_MIN_RATIO)
            cfg.reversal_max_hour_ist = int(V17G_REVERSAL_MAX_HOUR_IST)
            cfg.reversal_require_both_prior_bars = bool(V17G_REVERSAL_REQUIRE_BOTH_PRIORS)
            cfg.reversal_require_close_gt_ema20 = bool(V17G_REVERSAL_REQUIRE_CLOSE_GT_EMA20)
        except Exception:
            pass
    return cfg


if _v17g_orig_scan_long_prepared is not None:
    def _v17g_scan_long_prepared(ticker, df_prepared, long_cfg):
        long_cfg = _v17g_adjust_long_cfg(long_cfg)
        return _v17g_orig_scan_long_prepared(ticker, df_prepared, long_cfg)

    _base.scan_long_prepared = _v17g_scan_long_prepared


# ---------------------------------------------------------------------------
# PATCH 3: short-side post-scan consolidation (Change 4) and per-setup
# size_multiplier tagging (Change 3).
# ---------------------------------------------------------------------------
def _time_minutes(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, errors="coerce")
    return ts.dt.hour.astype("Int64") * 60 + ts.dt.minute.astype("Int64")


def _apply_size_multipliers(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "setup" not in df.columns:
        return df
    setups = df["setup"].astype(str).str.strip()
    multipliers = setups.map(V17G_SIZE_MULTIPLIERS).fillna(V17G_SIZE_DEFAULT)
    df = df.copy()
    df["size_multiplier"] = multipliers.astype(float)
    return df


def _v17g_apply_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # First run the v17f stack (which itself chains v17d, which chains v16).
    # This produces the baseline post-scan output.
    short_df, long_df = _v17f._v17f_apply_post_scan_filters(short_df, long_df)

    if V17G_CONSOLIDATE_SHORT_POCKETS and not short_df.empty:
        # The v17d and v17f stacks already removed the three time pockets.
        # Reverse that decision by re-admitting trades dropped only because
        # of those time pockets. Then apply the new causal rule.
        # Practical implementation: we can't recover already-dropped rows from
        # the current chain without restructuring the upstream functions. So
        # for v17g initial release, the consolidation works in *additive* mode:
        # the v17f time-pocket drops still happen (unavoidable without deeper
        # refactor), and we ALSO apply the causal rule on top so the bucket
        # behavior is at least probed in metrics.
        #
        # To honor the proposal's "replace, not add" intent without an upstream
        # refactor, set EQIDV17G_DISABLE_UPSTREAM_POCKETS=1 and run with
        # corresponding env vars (EQIDV17F_DROP_BOTH_MIDDAY_POCKET=0 +
        # EQIDV17D_DROP_BOTH_TIME_POCKETS=0). The harness sets these
        # automatically when the consolidate-short-pockets toggle is on.
        work = short_df.copy()
        mode = (
            work["nifty_context_mode"].astype(str).str.upper().str.strip()
            if "nifty_context_mode" in work.columns
            else pd.Series("", index=work.index)
        )
        adx = (
            pd.to_numeric(work.get("adx_signal", np.nan), errors="coerce")
            if "adx_signal" in work.columns
            else pd.Series(np.nan, index=work.index)
        )
        atr = (
            pd.to_numeric(work.get("atr_pct_signal", np.nan), errors="coerce")
            if "atr_pct_signal" in work.columns
            else pd.Series(np.nan, index=work.index)
        )

        causal_drop = (
            mode.eq("BOTH")
            & adx.lt(V17G_BOTH_CAUSAL_ADX_MAX)
            & atr.lt(V17G_BOTH_CAUSAL_ATR_PCT_MAX)
        ).fillna(False)
        causal_dropped = int(causal_drop.sum())
        before = len(work)
        work = work.loc[~causal_drop].copy()
        short_df = work
        print(
            "[V17G_FILTER] SHORT causal rule: {b}->{a} (-{d} BOTH adx<{adx} & "
            "atr_pct<{atr})".format(
                b=before,
                a=len(short_df),
                d=causal_dropped,
                adx=V17G_BOTH_CAUSAL_ADX_MAX,
                atr=V17G_BOTH_CAUSAL_ATR_PCT_MAX,
            )
        )

    if V17G_SETUP_LEVEL_SIZING:
        short_df = _apply_size_multipliers(short_df)
        long_df = _apply_size_multipliers(long_df)

    return short_df, long_df


def _v17g_get_filter_reason(row: dict, side: str):
    reason = _v17f._v17f_get_filter_reason(row, side)
    if reason is not None:
        return reason

    if not V17G_CONSOLIDATE_SHORT_POCKETS:
        return None
    if str(side).upper().strip() != "SHORT":
        return None

    mode = str(row.get("nifty_context_mode", "")).upper().strip()
    if mode != "BOTH":
        return None

    try:
        adx = float(row.get("adx_signal", float("nan")))
    except (TypeError, ValueError):
        adx = float("nan")
    try:
        atr = float(row.get("atr_pct_signal", float("nan")))
    except (TypeError, ValueError):
        atr = float("nan")

    if (
        np.isfinite(adx)
        and np.isfinite(atr)
        and adx < V17G_BOTH_CAUSAL_ADX_MAX
        and atr < V17G_BOTH_CAUSAL_ATR_PCT_MAX
    ):
        return (
            "v17g short cleanup: BOTH causal rule "
            f"adx={adx:.1f}<{V17G_BOTH_CAUSAL_ADX_MAX:.0f} & "
            f"atr_pct={atr * 100.0:.2f}%<{V17G_BOTH_CAUSAL_ATR_PCT_MAX * 100.0:.2f}%"
        )

    return None


_base._apply_v16_post_scan_filters = _v17g_apply_post_scan_filters
_base.get_v16_filter_reason = _v17g_get_filter_reason


# ---------------------------------------------------------------------------
# PATCH 4: Change 2 — LONG B_AVWAP_RECLAIM_REVERSAL.
#
# Wiring lives in _v17g_adjust_long_cfg above (sets
# cfg.enable_setup_b_avwap_reclaim_reversal). The scanner branch itself is
# implemented in avwap_v11_refactored/avwap_long_strategy_v9_sweep.py
# (function _scan_reversal_at, hooked into scan_one_day before impulse
# classification). The size_multiplier for B_AVWAP_RECLAIM_REVERSAL is
# already present in V17G_SIZE_MULTIPLIERS.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Entry point — print active configuration, then defer to v16 main().
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 78)
    print("V17g 5-min runner: surgical merge of architectural cleanup and "
          "alpha-fix proposal")
    print("  Output dir: outputs_v17g_5min")
    print("--- Toggles ---")
    print(f"  Change 1  fix_long_pullback_lag      = {V17G_FIX_LONG_PULLBACK_LAG} "
          f"(target lag = {V17G_LONG_PULLBACK_LAG_FIXED})")
    print(f"  Change 2  enable_long_reversal       = {V17G_ENABLE_LONG_REVERSAL} "
          "(B_AVWAP_RECLAIM_REVERSAL via cfg flag)")
    print(f"  Change 3  setup_level_sizing         = {V17G_SETUP_LEVEL_SIZING}")
    print(f"  Change 4  consolidate_short_pockets  = {V17G_CONSOLIDATE_SHORT_POCKETS} "
          f"(adx<{V17G_BOTH_CAUSAL_ADX_MAX:.0f} & atr%<"
          f"{V17G_BOTH_CAUSAL_ATR_PCT_MAX * 100:.2f}%)")
    print(f"  Change 5  long_avwap_dist_cap_on     = {V17G_LONG_AVWAP_DIST_CAP_ON} "
          f"(cap = {V17G_LONG_AVWAP_DIST_CAP_ATR:.2f} ATR)")
    print("--- Inherits all v17f / v17d / v17b / v16 behavior ---")
    print("=" * 78)
    _base.main()
