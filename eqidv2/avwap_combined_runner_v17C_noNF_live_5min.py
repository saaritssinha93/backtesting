 # -*- coding: utf-8 -*-
"""
v17C noNF LIVE -- Candidate B but with NIFTY_CONTEXT regime filter DISABLED.

WHAT THIS IS
============
Identical to v17B (Candidate B spec, all 8 honesty fixes ON) EXCEPT the
upstream `_apply_nifty_intraday_context` engine filter is replaced with a
no-op pass-through. Used to A/B test whether the NIFTY mode + RS filter
that lives in v16's signal-generation pipeline is actually pulling its
weight.

Backed-out filter (the one disabled here):
    - LONG signals where nifty_context_mode = SHORT_ONLY
    - SHORT signals where nifty_context_mode = LONG_ONLY
    - BOTH-mode LONG signals where nifty_rel_strength_pct < 0.75pp
    - BOTH-mode SHORT signals where nifty_rel_strength_pct > -0.50pp
    - V17c hybrid ATR-relative variants of the above

WHAT IT INHERITS
================
Imports avwap_combined_runner_v17B_live_5min, which already pulls the v17p
cascade + all 8 honesty fixes + Candidate B per-setup filter chains. So:

  - F1 (hardened Stage 0)        ON
  - F4 (post-run audit)          ON (relaxed trade-count band)
  - F6 (vol-ratio prior-bar)     ON
  - F7 (NIFTY lookup -5min)      ON (irrelevant since context is no-op)
  - F11 (no close-confirm)       ON
  - F12 (entry-bar exits)        ON
  - F14 (floor zero-lag)         ON
  - F15 (drop 5M_FALLBACK)       ON
  - Phase 5 honest size mults    ON
  - Candidate B per-setup chains ON

Then ONE override on top: `_base._apply_nifty_intraday_context` becomes a
pass-through that stamps explicit synthetic no-NIFTY columns. This matters
because later v17d/v17h/v17j/v17k/v17n/v17o filters expect
`nifty_context_mode` and `nifty_rel_strength_pct` to exist; leaving them
missing turns the no-NIFTY experiment into an accidental all-shorts drop.

EXPECTED RESULT (vs v17B)
=========================
The upstream filter currently culls 57-59% of raw signals (see your last
run: LONG 57,015 -> 23,548; SHORT 37,093 -> 15,724). Removing that gate
will roughly TRIPLE the candidate trade count BEFORE Candidate B's chains
fire. Aggregate is unpredictable -- two failure modes:

  Bullish failure (filter was useful):
    Candidate B chains see ~3000-4000 final trades but PF drops below 1.30
    because B's thresholds were tuned against post-regime-filter data.

  Bullish success (filter was over-aggressive):
    Candidate B chains catch most of the new volume cleanly, n grows to
    ~1500 with PF still >= 1.40.

This is an EXPERIMENTAL variant. Do NOT promote to live without comparing
to v17B's reference numbers (n=802, PF 1.58, OOS PF 1.52).

Output: outputs_v17C_noNF_live_5min/
"""
from __future__ import annotations

import pandas as pd

# Pull in v17B's full stack first (Candidate B spec + 8 honesty fixes +
# v17p cascade). All patches install during this import.
import avwap_combined_runner_v17B_live_5min as _v17B
import avwap_combined_runner_v16_5min as _base


# ---------------------------------------------------------------------------
# 1. OUTPUT DIR ROUTING -> outputs_v17C_noNF_live_5min
#
# Override v17B's runtime_dir patch so all artefacts land in a v17C-specific
# directory and don't pollute v17B output.
# ---------------------------------------------------------------------------
_orig_runtime_dir_pre_v17B = _base.runtime_dir  # this is v17B's wrapper


def _v17C_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17C_noNF_live_5min", "v17B_live_5min", "v17t_live_5min",
            "v17p_5min", "v17o_5min", "v17n_5min", "v17m_5min",
            "v17l_5min", "v17k_5min", "v17j_5min", "v17i_5min",
            "v17h_5min", "v17g_5min", "v17f_5min", "v17d_5min",
            "v17c_5min", "v17b_5min", "v16_5min",
        ):
            text = text.replace(old, "v17C_noNF_live_5min")
        new_parts.append(text)
    # Call the original (pre-patched) runtime_dir to avoid the v17B wrapper
    # rewriting our v17C path back to v17B.
    import avwap_combined_runner_v16_5min
    # We need the truly original runtime_dir, not the v17B-patched one.
    # _v17B's _orig_runtime_dir captured the un-patched original.
    return _v17B._orig_runtime_dir(*tuple(new_parts))


_base.runtime_dir = _v17C_runtime_dir


# ---------------------------------------------------------------------------
# 2. NO-OP THE NIFTY_CONTEXT FILTER (the entire point of this file)
#
# Replace _base._apply_nifty_intraday_context with a pass-through. The F7
# fix wrapped this earlier (in v17B's import) to shift the lookup -5min;
# we replace the whole thing with a function that never reads real NIFTY
# context and instead stamps synthetic side-aware columns.
#
# Why not leave NIFTY columns missing?
# Downstream inherited filters use these columns as ordinary features:
#   - v17d drops shorts with NaN nifty_rel_strength_pct
#   - v17h/v17k/v17n/v17o require positive NIFTY RS for some long setups
#   - v17j/v17k require negative NIFTY RS for some short setups
# Missing columns therefore do not mean "no NIFTY"; they mean "drop rows".
#
# The constants below make those gates permissive without using NIFTY data:
#   LONG  +1.25 passes long min-RS gates.
#   SHORT -1.25 passes short max-RS gates and stays above v17d's
#          extreme-downside cap of -2.00.
#   BOTH  satisfies both-mode setup gates without selecting LONG_ONLY or
#          SHORT_ONLY branches.
#
# The signature must match the original:
#   _apply_nifty_intraday_context(short_df, long_df, cfg, mode_map, nifty_ret_map)
# ---------------------------------------------------------------------------
_V17C_NO_NIFTY_CONTEXT_MODE = "BOTH"
_V17C_NO_NIFTY_LONG_RS = 1.25
_V17C_NO_NIFTY_SHORT_RS = -1.25


def _v17C_stamp_no_nifty_columns(df: pd.DataFrame | None, *, side: str) -> pd.DataFrame | None:
    """Stamp synthetic no-NIFTY context columns for downstream inherited filters."""
    if df is None:
        return None

    out = df.copy()
    rs_value = _V17C_NO_NIFTY_LONG_RS if side.upper() == "LONG" else _V17C_NO_NIFTY_SHORT_RS
    out["nifty_context_mode"] = _V17C_NO_NIFTY_CONTEXT_MODE
    out["nifty_rel_strength_pct"] = float(rs_value)
    out["v17C_noNF_nifty_context_disabled"] = True
    out["v17C_noNF_synthetic_nifty_rs"] = float(rs_value)
    return out


def _v17C_no_nifty_context(short_df, long_df, cfg, mode_map, nifty_ret_map):
    """No-op replacement for v16's NIFTY context filter.

    It intentionally does not read mode_map or nifty_ret_map. It stamps
    synthetic context fields so downstream inherited filters do not treat
    the no-NIFTY experiment as missing-data failure.
    """
    short_df = _v17C_stamp_no_nifty_columns(short_df, side="SHORT")
    long_df = _v17C_stamp_no_nifty_columns(long_df, side="LONG")
    n_short = 0 if short_df is None else len(short_df)
    n_long = 0 if long_df is None else len(long_df)
    print(f"[V17C_NO_NIFTY] NIFTY_CONTEXT filter DISABLED -- "
          f"passthrough SHORT={n_short}, LONG={n_long}; "
          f"stamped synthetic mode={_V17C_NO_NIFTY_CONTEXT_MODE}, "
          f"SHORT_RS={_V17C_NO_NIFTY_SHORT_RS:+.2f}%, "
          f"LONG_RS={_V17C_NO_NIFTY_LONG_RS:+.2f}%")
    return short_df, long_df


_base._apply_nifty_intraday_context = _v17C_no_nifty_context


# ---------------------------------------------------------------------------
# 3. RELAX F4 TRADE-COUNT BAND
#
# v17B's audit asserts n in [600, 1000] (Candidate B reference range).
# With the regime filter off, n will be much larger (likely 1500-4000).
# Override the audit to print a warning instead of failing on count band.
# ---------------------------------------------------------------------------
import glob
from pathlib import Path

_v17B_main = _base.main  # this is v17B's wrapped main with audit


def _v17C_post_run_audit():
    """Same as v17B's audit but with widened trade-count band and a label
    noting that NIFTY_CONTEXT was disabled."""
    out_dir = _v17C_runtime_dir("outputs_v16_5min")
    pattern = str(Path(out_dir) / "avwap_longshort_trades_v16_5min_ALL_DAYS_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print("[V17C_AUDIT] no output CSV found; skipping audit")
        return
    latest = files[-1]
    df = pd.read_csv(latest)
    print(f"[V17C_AUDIT] (NIFTY_CONTEXT DISABLED) auditing "
          f"{Path(latest).name} (rows={len(df)})")

    failures = []

    def _fail(name, n, hint):
        if n > 0:
            failures.append(f"{name} ({hint}: n={n})")
            print(f"[V17C_AUDIT][FAIL] {name}: n={n} ({hint})")
        else:
            print(f"[V17C_AUDIT][PASS] {name}")

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

    # v17C-specific: widened trade-count band. Warning only, not a failure.
    if not (1000 <= len(df) <= 5000):
        print(f"[V17C_AUDIT][WARN] trade_count_band: n={len(df)} "
              f"(expected 1000..5000 with NIFTY_CONTEXT off; outside range)")
    else:
        print(f"[V17C_AUDIT][PASS] trade_count_band: n={len(df)}")

    if failures:
        print(f"[V17C_AUDIT] {len(failures)} check(s) FAILED: " + "; ".join(failures))
        import sys as _sys
        print("[V17C_AUDIT] STRICT mode -- exiting with code 2")
        _sys.exit(2)
    else:
        print("[V17C_AUDIT] all checks passed (NIFTY_CONTEXT was DISABLED for this run)")


# Capture the un-wrapped main (the v16 base.main wrapped by v17B). We need
# the v16 original main so our wrapper runs once, not twice.
_orig_main_for_v17C = _v17B._orig_main


def _v17C_main():
    result = _orig_main_for_v17C()
    try:
        _v17C_post_run_audit()
    except SystemExit:
        raise
    except Exception as exc:
        print(f"[V17C_AUDIT] post-run audit error: {exc}")
    return result


_base.main = _v17C_main


# ---------------------------------------------------------------------------
# 4. BANNER
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 78)
    print("v17C noNF LIVE -- Candidate B with NIFTY_CONTEXT filter DISABLED")
    print("  Output dir   : outputs_v17C_noNF_live_5min")
    print("  Strategy     : v17p cascade + Phase-5 honest size mults")
    print("  Filter       : Candidate B per-setup chains (8 setups, 4L+4S)")
    print("  Honesty fixes: F1 STAGE0 | F4 AUDIT | F6 VOL_RATIO | F7 NIFTY_LAG")
    print("                 F11 NO_CLOSE_CONFIRM | F12 ENTRY_BAR_EXITS")
    print("                 F14 FLOOR_LAG | F15 REQUIRE_1MIN")
    print("  CRITICAL     : NIFTY_CONTEXT regime filter is DISABLED (no-op)")
    print("                 Expect 2-3x more raw signals than v17B")
    print("                 Expected n: 1500-4000 (audit band 1000-5000)")
    print("                 PF / OOS PF unpredictable -- experimental")
    print("=" * 78)
    _base.main()
