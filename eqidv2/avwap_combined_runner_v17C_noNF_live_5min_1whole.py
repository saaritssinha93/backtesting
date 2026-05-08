# -*- coding: utf-8 -*-
"""
v17C noNF LIVE -- 1-WHOLE variant.

Same logic as `avwap_combined_runner_v17C_noNF_live_5min_1week.py` except
the engine scans the WHOLE date range present in the parquet (no date
window).  All other diagnostic switches inherit unchanged:

  * F15 honesty filter disabled (5M_FALLBACK rows kept; audit goes
    non-strict so a 5M-fallback FAIL is logged but not fatal).
  * Optimized 1-week profile installed -- replaces production Cand-E
    allowlist with the last-week-fitted research bucket.  Running this
    profile across the whole history is effectively the OOS test of
    those thresholds: if PF stays high here, the edge generalizes; if
    PF collapses, the 1-week numbers were threshold-fitting.
  * Multiprocess workers under spawn redirect their import banners to
    /dev/null so the parent log stays clean.

Implementation: import the full v17C runner module (which triggers all
its module-level config + hot-swaps + post-resolve registration) WITHOUT
firing its __main__ block.  Then install the F15 passthrough + audit
non-strict + optimized profile in the main process only.  No date
filter is installed -- the engine processes every bar in every parquet.

Outputs land in the same dir as the full v17C run
(`outputs_v17C_noNF_live_5min`) with timestamped filenames so they
coexist with full / 1-week runs.

Tunables (edit then run):
  DISABLE_F15                     -- pass-through 5M_FALLBACK rows
                                     (default True; flip for production-
                                     quality exit precision once 1-min
                                     parquet covers the date range)
  ENABLE_1WK_OPTIMIZED_PROFILE    -- install last-week-fitted profile
                                     (default True; flip to use parent
                                     production Cand-E filters instead)

Note this runner can take significantly longer than the 1-week variant
because Phase 1 scans the full ticker x date matrix.  Phase 2 exit
resolution is also proportionally larger.  Plan for several minutes.
"""
from __future__ import annotations

import datetime as _dt
import contextlib as _contextlib
import io as _io
import multiprocessing as _mp

_IS_MAIN_PROCESS = _mp.current_process().name == "MainProcess"


def _print_main(*args, **kwargs):
    if _IS_MAIN_PROCESS:
        print(*args, **kwargs)


# ===========================================================================
# 1. RUN CONFIG
# ===========================================================================
# F15 ("require 1-min for exit resolution") is an honesty filter that drops
# trades whose exits had to use 5-min bars because the 1-min parquet didn't
# cover the date.  When 1-min coverage is uneven across a long history this
# can drop a huge chunk of trades.  Set DISABLE_F15=True to keep
# 5M_FALLBACK rows visible at degraded exit precision.
DISABLE_F15 = True

# Backtest-only optimized profile mined from the latest 7-day pool. This is
# intentionally local to this diagnostic wrapper. The parent v17C / live
# pipeline stays untouched.
ENABLE_1WK_OPTIMIZED_PROFILE = True

_print_main(f"[V17C_1WHOLE] running over WHOLE parquet date range (no date window)  "
            f"(today={_dt.date.today().isoformat()})")


# ===========================================================================
# 2. INHERIT FULL v17C RUNNER
#
# Importing the module runs all its module-level code:
#   * v17B cascade import + Cand-E filter spec swap
#   * CANDIDATE_E_SETUP_CONFIG load + per-setup validation
#   * Cand-E SIZE_MULTIPLIERS hot-swap into v17p / v17B
#   * Phase 2b FBT engine-scan hot-swap (currently ENABLED at module level)
#   * F-fix patches and post-run audit registration
#   * `_base.main = _v17C_main` rebinding
# It does NOT fire the __main__ block.
# ===========================================================================
if _IS_MAIN_PROCESS:
    import avwap_combined_runner_v17C_noNF_live_5min as _v17C_full   # noqa: F401  (intentional side-effect import)
else:
    # Windows process workers import this module under multiprocessing spawn.
    # The import side effects are required, but their banners are just noise.
    with _contextlib.redirect_stdout(_io.StringIO()):
        import avwap_combined_runner_v17C_noNF_live_5min as _v17C_full   # noqa: F401


# ===========================================================================
# 3. v16 BASE REFERENCE
#
# The 1-week variant wraps scan_short_prepared / scan_long_prepared with a
# date filter here.  The 1-whole variant intentionally skips that wrap so
# the engine scans the full date range available in the parquet.
# ===========================================================================
import avwap_combined_runner_v16_5min as _v16_base_1whole

_print_main("[V17C_1WHOLE] no date filter installed -- engine will scan the "
            "full parquet date range")


# ===========================================================================
# 3a. F15 DISABLE (diagnostic mode)
#
# Replace `_v17B_apply_f15_drop` in v17B's namespace with a passthrough that
# logs how many 5M_FALLBACK rows it would have dropped, but keeps them.
# F15 is referenced by name from inside `_v17B_resolve_exits_5min`, so the
# replacement is picked up at call time via Python's global lookup -- no need
# to re-wrap the resolver itself.  Only patch in the main process; workers
# never run F15.
#
# v17C's post-run audit will still flag F15_no_5M_fallback as FAIL when
# 5M_FALLBACK rows are present.  We additionally wrap the audit so it
# downgrades a STRICT SystemExit to a warning print -- non-fatal in this
# diagnostic mode.
# ===========================================================================
if DISABLE_F15 and _IS_MAIN_PROCESS:
    import avwap_combined_runner_v17B_live_5min as _v17B_for_1whole
    _v17B_orig_f15 = _v17B_for_1whole._v17B_apply_f15_drop

    def _v17C_1whole_f15_passthrough(df):
        if df is None or len(df) == 0:
            return df
        if "exit_resolution_case" in df.columns:
            n_5m = int(df["exit_resolution_case"].astype(str)
                       .str.startswith("5M_FALLBACK").sum())
            print(f"[V17C_1WHOLE_F15] DISABLED -- keeping {n_5m} 5M_FALLBACK row(s) "
                  f"(exit precision degraded to 5-min for those rows; "
                  f"non-production diagnostic).")
        return df

    _v17B_for_1whole._v17B_apply_f15_drop = _v17C_1whole_f15_passthrough

    _v17C_orig_audit = _v17C_full._v17C_post_run_audit

    def _v17C_1whole_audit_non_strict_for_fallback():
        try:
            return _v17C_orig_audit()
        except SystemExit as exc:
            code = getattr(exc, "code", None)
            print(f"[V17C_1WHOLE_AUDIT] non-strict diagnostic mode -- "
                  f"continuing after audit exit code {code} because F15 is "
                  f"disabled and 5M_FALLBACK rows are expected.")
            return None

    _v17C_full._v17C_post_run_audit = _v17C_1whole_audit_non_strict_for_fallback
    _print_main("[V17C_1WHOLE] F15 honesty filter DISABLED -- 5M_FALLBACK rows kept "
                "(post-run audit will mark F15 as FAIL; ignore in 1-whole mode).")


# ===========================================================================
# 3b. 1-WEEK OPTIMIZED BACKTEST PROFILE (running across whole history)
#
# Same threshold set the 1-week wrapper installs.  When this runs across
# the full date range it doubles as the OOS validation: if PF stays high
# here, the thresholds generalize; if it collapses, the 1-week PF was
# threshold-fitting.
#
# These values are LAST-WEEK-FITTED.  Do not interpret strong whole-history
# numbers as proof of edge unless they hold up on splits the threshold
# search did not see.
# ===========================================================================
if ENABLE_1WK_OPTIMIZED_PROFILE and _IS_MAIN_PROCESS:
    import avwap_combined_runner_v17B_live_5min as _v17B_profile
    import avwap_combined_runner_v17p_5min as _v17p_profile

    _v17B_profile.CANDIDATE_B_FILTER_SPEC = {
        ("LONG", "C_OR_BREAKOUT"): [
            ("entry_hour",              ">=", 9.8333),
            ("entry_hour",              "<=", 10.8334),
            ("quality_score",           ">=", 2.1068),
            ("quality_score",           "<=", 3.3895),
            ("rsi_signal",              ">=", 64.8000),
            ("rsi_signal",              "<=", 89.2100),
            ("adx_signal",              ">=", 26.7000),
            ("adx_signal",              "<=", 60.8600),
            ("stochk_signal",           ">=", 48.4000),
            ("stochk_signal",           "<=", 95.7000),
            ("avwap_dist_atr_signal",   ">=", 0.9400),
            ("avwap_dist_atr_signal",   "<=", 2.2241),
            ("ema20_gap_atr_signal",    ">=", -3.6813),
            ("ema20_gap_atr_signal",    "<=", -1.3085),
            ("atr_pct_signal",          ">=", 0.0037),
            ("atr_pct_signal",          "<=", 0.00961),
            ("entry_bar_vol_ratio",     ">=", 0.5630),
            ("entry_bar_vol_ratio",     "<=", 3.0541),
            ("bars_from_open",          ">=", 0.0),
            ("bars_from_open",          "<=", 6.0),
        ],
        ("LONG", "D_EMA20_BOUNCE"): [
            ("quality_score",           "<=", 2.2493),
        ],
        ("LONG", "G_HIGHER_HIGH_BREAK"): [
            ("entry_hour",              ">=", 10.7500),
            ("stochk_signal",           "<=", 76.8502),
        ],
        ("SHORT", "A_MOD_BREAK_C1_LOW"): [],
        ("SHORT", "C_OR_BREAKDOWN"): [
            ("quality_score",           "<=", 0.7601),
            ("avwap_dist_atr_signal",   ">=", 1.1238),
        ],
        ("SHORT", "D_EMA20_REJECTION"): [
            ("quality_score",           ">=", 0.7000),
        ],
        ("SHORT", "G_LOWER_LOW_BREAK"): [],
    }

    # Uniform 1.00x sizing for the selected research buckets.
    _profile_sizes = {
        "C_OR_BREAKOUT":                    1.00,
        "D_EMA20_BOUNCE":                   1.00,
        "G_HIGHER_HIGH_BREAK":              1.00,
        "A_MOD_BREAK_C1_LOW":               1.00,
        "C_OR_BREAKDOWN":                   1.00,
        "D_EMA20_REJECTION":                1.00,
        "G_LOWER_LOW_BREAK":                1.00,
    }
    _v17p_profile.SIZE_MULTIPLIERS = _profile_sizes
    _v17B_profile.SIZE_MULTIPLIERS = _profile_sizes

    if hasattr(_v17C_full, "CANDIDATE_E_SIZE_MULTIPLIERS"):
        _v17C_full.CANDIDATE_E_SIZE_MULTIPLIERS.clear()
        _v17C_full.CANDIDATE_E_SIZE_MULTIPLIERS.update(_profile_sizes)
    if hasattr(_v17C_full, "CANDIDATE_E_PER_SETUP_DAILY_CAP"):
        _v17C_full.CANDIDATE_E_PER_SETUP_DAILY_CAP.clear()      # no per-setup cap
    if hasattr(_v17C_full, "CANDIDATE_E_KILL_PF_THRESHOLDS"):
        _v17C_full.CANDIDATE_E_KILL_PF_THRESHOLDS.clear()       # no kill-PF gates
    if hasattr(_v17C_full, "CANDIDATE_E3_SL_TGT"):
        # Default SL=0.75 / TGT=0.80 for any setup not already specified.
        _default_sl_tgt = (0.75, 0.80)
        for _side in ("LONG", "SHORT"):
            for _setup in _profile_sizes.keys():
                _v17C_full.CANDIDATE_E3_SL_TGT.setdefault((_side, _setup), _default_sl_tgt)

    _print_main(f"[V17C_1WHOLE_OPT] installed optimized 1-week profile across whole "
                f"range: {len(_v17B_profile.CANDIDATE_B_FILTER_SPEC)} (side, setup) "
                f"entries; selected sizes 1.00x; per-setup caps disabled; "
                f"kill-PF disabled")


# ===========================================================================
# 4. BANNER + ENTRY POINT
# ===========================================================================
if __name__ == "__main__":
    print("=" * 78)
    print("v17C noNF LIVE -- 1-WHOLE variant (full parquet date range)")
    print(f"  Today          : {_dt.date.today().isoformat()}")
    print( "  Date window    : NONE (engine scans full parquet history)")
    print( "  Inherits       : avwap_combined_runner_v17C_noNF_live_5min")
    print( "  Output dir     : outputs_v17C_noNF_live_5min  (timestamped, coexists with full / 1-week runs)")
    print( "  FBT engine scan: inherited from parent (see [V17C_E] FBT engine-scan line above)")
    print(f"  F15 honesty    : {'DISABLED (5M_FALLBACK rows kept)' if DISABLE_F15 else 'enabled'}")
    print(f"  Profile        : {'OPTIMIZED 1-WEEK BACKTEST (OOS over whole history)' if ENABLE_1WK_OPTIMIZED_PROFILE else 'parent Cand-E'}")
    print( "  Note           : if the 1-week profile thresholds were threshold-fit to a")
    print( "                   narrow window, expect PF to collapse over the whole history.")
    print("=" * 78)
    _v16_base_1whole.main()
