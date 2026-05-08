# -*- coding: utf-8 -*-
"""
v17C noNF LIVE -- 1-WEEK variant.

Restricts the v17C engine scan to the last `LOOKBACK_DAYS` calendar days
from `END_DATE`.  Everything else (Cand-E filters, governors, sizing,
Phase 0 G0 strip, Phase 2a diag enrichment, Phase 2b FBT scan if enabled
in the parent runner, post-resolve summary) inherits unchanged from
`avwap_combined_runner_v17C_noNF_live_5min` -- this file just adds a date
window on top.

Implementation: import the full v17C runner as a module (which triggers
all its module-level config, hot-swaps, and overrides) WITHOUT firing its
__main__ block.  Then wrap v16's `scan_short_prepared` / `scan_long_prepared`
once more to filter the prepared bar df by date before the scan loop sees
it.  The wrap chains on top of v17C's own wrap (FBT cfg-flag injector), so
both filters fire in order.

Side effects:
  * Engine processes only bars within [START_DATE, END_DATE].  Indicators
    are pre-computed in the parquet, so the filter does not break them;
    minor warmup artifact on day 1 of the window is acceptable for a
    7-day diagnostic.
  * Output files still land in the same dir as the full v17C run
    (`outputs_v17C_noNF_live_5min`) and follow the same timestamped
    naming.  Two different runs coexist; sort by mtime to find the
    latest 1-week run.
  * `prev_close` for day-mode selection on the FIRST day of the window
    starts as None (no prior day in the filtered df); other days
    inherit prev_close normally.

Tunables (edit then run):
  LOOKBACK_DAYS  -- how many calendar days back from END_DATE.
  END_DATE       -- right edge of the window.  Default: today.

Note the live parquet date range may not extend to today; if it ends
earlier than START_DATE, the engine will scan zero bars and the run
produces zero trades.  In that case, set END_DATE to the parquet's
most recent date before re-running.
"""
from __future__ import annotations

import datetime as _dt
import contextlib as _contextlib
import io as _io
import multiprocessing as _mp
import pandas as _pd_1wk

_IS_MAIN_PROCESS = _mp.current_process().name == "MainProcess"


def _print_main(*args, **kwargs):
    if _IS_MAIN_PROCESS:
        print(*args, **kwargs)

# ===========================================================================
# 1. WINDOW CONFIG
# ===========================================================================
LOOKBACK_DAYS = 7
END_DATE: _dt.date = _dt.date.today()
START_DATE: _dt.date = END_DATE - _dt.timedelta(days=LOOKBACK_DAYS)

# F15 ("require 1-min for exit resolution") is an honesty filter that drops
# trades whose exits had to use 5-min bars because the 1-min parquet didn't
# cover the date.  In a 7-day diagnostic window the 1-min parquet is often
# behind the 5-min, so F15 silently kills every trade.  Set DISABLE_F15=True
# to keep 5M_FALLBACK rows visible (exit precision is degraded to 5-min --
# not production-quality, but lets you see signal activity in the window).
DISABLE_F15 = True

# Backtest-only optimized profile mined from the current 7-calendar-day pool.
# It is intentionally local to this diagnostic wrapper. The parent v17C/live
# runner stays untouched.
ENABLE_1WK_OPTIMIZED_PROFILE = True

_print_main(f"[V17C_1WK] window: lookback={LOOKBACK_DAYS} days  "
            f"[{START_DATE.isoformat()} .. {END_DATE.isoformat()}]  "
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
# 3. DATE-WINDOW HOT-SWAP
#
# Wrap scan_short_prepared / scan_long_prepared one more time so the
# prepared df is restricted to bars whose `date` column falls within
# [START_DATE, END_DATE] (IST, inclusive on both ends).
# ===========================================================================
import avwap_combined_runner_v16_5min as _v16_base_1wk

_orig_scan_short_1wk = getattr(_v16_base_1wk, "scan_short_prepared", None)
_orig_scan_long_1wk  = getattr(_v16_base_1wk, "scan_long_prepared",  None)


def _v17C_1wk_filter_by_date(df):
    """Return a copy of df with rows whose `date` lies in [START_DATE, END_DATE]."""
    if df is None or len(df) == 0 or "date" not in df.columns:
        return df
    d = _pd_1wk.to_datetime(df["date"], errors="coerce")
    # Normalize tz-aware to IST-naive for date() comparison
    if hasattr(d.dt, "tz") and d.dt.tz is not None:
        d = d.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    d_only = d.dt.date
    mask = (d_only >= START_DATE) & (d_only <= END_DATE)
    return df[mask].copy()


if _orig_scan_short_1wk is not None:
    def _v17C_1wk_scan_short(ticker, df_prepared, short_cfg):
        return _orig_scan_short_1wk(ticker, _v17C_1wk_filter_by_date(df_prepared), short_cfg)
    _v16_base_1wk.scan_short_prepared = _v17C_1wk_scan_short
else:
    _print_main("[V17C_1WK] WARN: v16.scan_short_prepared not found; SHORT date filter NOT installed")

if _orig_scan_long_1wk is not None:
    def _v17C_1wk_scan_long(ticker, df_prepared, long_cfg):
        return _orig_scan_long_1wk(ticker, _v17C_1wk_filter_by_date(df_prepared), long_cfg)
    _v16_base_1wk.scan_long_prepared = _v17C_1wk_scan_long
else:
    _print_main("[V17C_1WK] WARN: v16.scan_long_prepared not found; LONG date filter NOT installed")

_print_main(f"[V17C_1WK] date filter installed -- engine will scan only "
            f"[{START_DATE.isoformat()} .. {END_DATE.isoformat()}]")


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
# 5M_FALLBACK rows are present, but in non-STRICT mode that's just a print,
# not a SystemExit.
# ===========================================================================
if DISABLE_F15 and _IS_MAIN_PROCESS:
    import avwap_combined_runner_v17B_live_5min as _v17B_for_1wk
    _v17B_orig_f15 = _v17B_for_1wk._v17B_apply_f15_drop

    def _v17C_1wk_f15_passthrough(df):
        if df is None or len(df) == 0:
            return df
        if "exit_resolution_case" in df.columns:
            n_5m = int(df["exit_resolution_case"].astype(str)
                       .str.startswith("5M_FALLBACK").sum())
            print(f"[V17C_1WK_F15] DISABLED -- keeping {n_5m} 5M_FALLBACK row(s) "
                  f"(exit precision degraded to 5-min for those rows; "
                  f"non-production diagnostic).")
        return df

    _v17B_for_1wk._v17B_apply_f15_drop = _v17C_1wk_f15_passthrough

    _v17C_orig_audit = _v17C_full._v17C_post_run_audit

    def _v17C_1wk_audit_non_strict_for_fallback():
        try:
            return _v17C_orig_audit()
        except SystemExit as exc:
            code = getattr(exc, "code", None)
            print(f"[V17C_1WK_AUDIT] non-strict diagnostic mode -- "
                  f"continuing after audit exit code {code} because F15 is "
                  f"disabled and 5M_FALLBACK rows are expected.")
            return None

    _v17C_full._v17C_post_run_audit = _v17C_1wk_audit_non_strict_for_fallback
    _print_main("[V17C_1WK] F15 honesty filter DISABLED -- 5M_FALLBACK rows kept "
                "(post-run audit will mark F15 as FAIL; ignore in 1-week mode).")


# ===========================================================================
# 3b. 1-WEEK OPTIMIZED BACKTEST PROFILE
#
# This patch is intentionally installed only from the 1-week wrapper. It
# replaces Candidate-E's production allowlist with a short-term research
# allowlist:
#
#   LONG:
#     C_OR_BREAKOUT: tight pullback-breakout box mined from the broad-open
#       1-week pool. Uses only signal/entry-bar fields.
#     G_HIGHER_HIGH_BREAK: keeps the late structure-break bucket, strips the
#       open-slot NIFTY source rows and overheated structure breaks.
#     D_EMA20_BOUNCE: tiny low-QS subset.
#
#   SHORT:
#     C_OR_BREAKDOWN: upgraded from the older profile by keeping stretched
#       AVWAP breakdowns and dropping the near-threshold loser.
#     A_MOD_BREAK_C1_LOW / D_EMA20_REJECTION / G_LOWER_LOW_BREAK: tiny positive
#       buckets kept at 1.00x in this research wrapper.
#
# These thresholds are causal, but they are last-week-fitted. Treat this as
# research/paper-trade input until it survives a broader forward sample.
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

    _print_main(f"[V17C_1WK_OPT] installed optimized 1-week profile: "
                f"{len(_v17B_profile.CANDIDATE_B_FILTER_SPEC)} (side, setup) "
                f"entries; selected sizes 1.00x; per-setup caps disabled; "
                f"kill-PF disabled")


# ===========================================================================
# 4. BANNER + ENTRY POINT
# ===========================================================================
if __name__ == "__main__":
    print("=" * 78)
    print("v17C noNF LIVE -- 1-WEEK variant")
    print(f"  Lookback days  : {LOOKBACK_DAYS}")
    print(f"  Date range     : {START_DATE.isoformat()}  ->  {END_DATE.isoformat()}")
    print(f"  Today          : {_dt.date.today().isoformat()}")
    print( "  Inherits       : avwap_combined_runner_v17C_noNF_live_5min")
    print( "  Output dir     : outputs_v17C_noNF_live_5min  (timestamped, coexists with full runs)")
    print( "  FBT engine scan: inherited from parent (see [V17C_E] FBT engine-scan line above)")
    print(f"  F15 honesty    : {'DISABLED (5M_FALLBACK rows kept)' if DISABLE_F15 else 'enabled'}")
    print(f"  1-week profile : {'OPTIMIZED BACKTEST ONLY' if ENABLE_1WK_OPTIMIZED_PROFILE else 'parent Cand-E'}")
    print( "  Note           : if parquet's most recent date < START_DATE, run produces zero trades.")
    print("=" * 78)
    _v16_base_1wk.main()
