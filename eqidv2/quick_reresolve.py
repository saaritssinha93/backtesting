# -*- coding: utf-8 -*-
"""
quick_reresolve.py  —  shared exit-only resolver for v17b / v18c3 / v18c4 / v18c6
--------------------------------------------------------------------------
Loads an existing trade CSV (Phase 1 entries already decided), strips old
exit columns, re-resolves exits with the version's current config, and
prints metrics. Skips the ~40-min Phase 1 scan entirely.

Usage (called by version-specific launchers):
    python quick_reresolve_v18c3.py
    python quick_reresolve_v18c4.py
    python quick_reresolve_v18c6.py
    python quick_reresolve_v17b.py

Or directly:
    python quick_reresolve.py --version v18c3
    python quick_reresolve.py --version v18c4
    python quick_reresolve.py --version v18c6
    python quick_reresolve.py --version v17b

v18c6 notes
-----------
v18c6 wraps v18c5 which wraps v18c3.  The entry signals live in the
v18c6 output CSV but all execution helpers (_resolve_exits_5min, metrics,
etc.) are on avwap_combined_runner_v18c5_5min.  Importing v18c6 first
applies all its module-level patches (data-dir, RS filter, exit profile)
onto v18c5 before the re-resolve runs.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_this_dir = Path(__file__).resolve().parent
_project_root = _this_dir.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

# ---------------------------------------------------------------------------
# Exit columns to strip before re-resolving
# ---------------------------------------------------------------------------
EXIT_COLS = [
    "exit_time_ist", "exit_price", "outcome",
    "pnl_pct", "pnl_pct_gross", "pnl_pct_price", "pnl_pct_gross_price",
    "pnl_rs", "pnl_rs_gross",
    "exit_stop_pct_cfg", "exit_target_pct_cfg",
    "exit_be_trigger_pct_cfg", "exit_trail_pct_cfg",
    "exit_resolution_case", "exit_bar_ambiguous",
    "stop_fill_penalty_applied", "stop_fill_penalty_bps",
    "exit_price_base", "exit_time_ist_base", "outcome_base",
    "pnl_pct_gross_price_base", "pnl_pct_price_base",
    "exit_price_pess", "exit_time_ist_pess", "outcome_pess",
    "pnl_pct_gross_price_pess", "pnl_pct_price_pess",
    "exit_price_opt", "exit_time_ist_opt", "outcome_opt",
    "pnl_pct_gross_price_opt", "pnl_pct_price_opt",
    "leverage", "notional_exposure_rs",
    # Pre-computed SL/target prices — must strip so resolver recomputes from entry_price
    "stop_price", "sl_price", "target_price",
]

# ---------------------------------------------------------------------------
# Version registry
# ---------------------------------------------------------------------------
def _get_version_config(version: str) -> dict:
    """Return runner module and CSV path for a given version string."""
    v = version.lower().strip()

    if v == "v18c3":
        import avwap_combined_runner_v18c3_5min as runner
        csv_dir = Path(r"C:\TradingData\eqidv2\outputs_v18c3_5min")
        label = "v18c3"

    elif v == "v18c4":
        import avwap_combined_runner_v18c4_5min as runner
        csv_dir = Path(r"C:\TradingData\eqidv2\outputs_v18c4_5min")
        label = "v18c4"

    elif v == "v17b":
        # v17b functions live in _base (avwap_combined_runner_v16_5min)
        import avwap_combined_runner_v17b_5min  # noqa: F401 — patches _base
        import avwap_combined_runner_v16_5min as runner
        csv_dir = Path(r"C:\TradingData\eqidv2\outputs_v17b_5min")
        label = "v17b"

    elif v == "v18c6":
        # v18c6 wraps v18c5: import v18c6 first so its patches apply, then use
        # v18c5 as the actual runner (where all execution helpers live).
        import avwap_combined_runner_v18c6_5min  # noqa: F401 — patches v18c5
        import avwap_combined_runner_v18c5_5min as runner
        csv_dir = Path(r"C:\TradingData\eqidv2\outputs_v18c6_5min")
        label = "v18c6"

    elif v == "v19a":
        import avwap_combined_runner_v19a_5min as runner
        csv_dir = Path(r"C:\TradingData\eqidv2\outputs_v19a_5min")
        if not csv_dir.is_dir():
            csv_dir = Path(r"C:\TradingData\eqidv2\outputs_v18c6_5min")
        label = "v19a"

    else:
        raise ValueError(f"Unknown version '{version}'. Choose: v17b, v18c3, v18c4, v18c6, v19a")

    # Pick the latest trades CSV in that directory
    if v == "v19a":
        candidates = sorted(csv_dir.glob("*trades*v19a*.csv"))
        if not candidates:
            candidates = sorted(csv_dir.glob("*trades*.csv"))
    else:
        candidates = sorted(csv_dir.glob("*trades*.csv"))
    if not candidates:
        raise FileNotFoundError(f"No trades CSV found in {csv_dir}")
    csv_path = candidates[-1]

    return {"runner": runner, "csv_path": csv_path, "label": label}


# ---------------------------------------------------------------------------
# Target override — monkey-patch the exit profile function and v16 globals
# ---------------------------------------------------------------------------
def _apply_target_override(
    runner, version: str,
    short_tgt: float, long_tgt: float,
    short_sl: float = None, long_sl: float = None,
) -> None:
    """Patch the runner's exit-profile so _resolve_exits_5min uses the new targets/SLs."""
    v = version.lower()

    if v in ("v18c3", "v18c4"):
        # Patch module-level var and replace _v17c_setup_exit_profile
        runner.V17C_SHORT_TARGET_PCT = short_tgt
        _orig = runner._v17c_setup_exit_profile
        def _patched(side_val, setup_val, _o=_orig,
                     _st=short_tgt, _lt=long_tgt,
                     _ssl=short_sl, _lsl=long_sl):
            profile = _o(side_val, setup_val)
            if str(side_val).upper() == "SHORT":
                profile["target_pct"] = _st
                if _ssl is not None:
                    profile["stop_pct"] = _ssl
            else:
                profile["target_pct"] = _lt
                if _lsl is not None:
                    profile["stop_pct"] = _lsl
            return profile
        runner._v17c_setup_exit_profile = _patched

    elif v in ("v18c6", "v19a"):
        # v18c6 already patched runner._v17c_setup_exit_profile at import time.
        # Wrap that patched version to override target_pct for all setups,
        # including B_HUGE_RECLAIM which v18c6 normally gives target=1.20%.
        # SL values from v18c6's profiles are preserved unless explicitly overridden.
        _orig = runner._v17c_setup_exit_profile
        def _patched(side_val, setup_val, entry_time_ist=None,
                     _o=_orig, _st=short_tgt, _lt=long_tgt,
                     _ssl=short_sl, _lsl=long_sl):
            profile = _o(side_val, setup_val, entry_time_ist=entry_time_ist)
            if str(side_val).upper() == "SHORT":
                profile["target_pct"] = _st
                if _ssl is not None:
                    profile["stop_pct"] = _ssl
            else:
                profile["target_pct"] = _lt
                if _lsl is not None:
                    profile["stop_pct"] = _lsl
            return profile
        runner._v17c_setup_exit_profile = _patched
        # Also keep TEST_TARGET_OVERRIDE in sync so the cfg-level path agrees
        if hasattr(runner, "TEST_TARGET_OVERRIDE"):
            runner.TEST_TARGET_OVERRIDE   = True
            runner.TEST_SHORT_TARGET_PCT  = short_tgt
            runner.TEST_LONG_TARGET_PCT   = long_tgt

    elif v == "v17b":
        # v17b delegates to _base (avwap_combined_runner_v16_5min)
        import avwap_combined_runner_v16_5min as _base
        _base.TEST_TARGET_OVERRIDE  = True
        _base.TEST_SHORT_TARGET_PCT = short_tgt
        _base.TEST_LONG_TARGET_PCT  = long_tgt
        # Also patch _v17c_setup_exit_profile if it exists on _base
        if hasattr(_base, "_v17c_setup_exit_profile"):
            _orig = _base._v17c_setup_exit_profile
            def _patched(side_val, setup_val, _o=_orig,
                         _st=short_tgt, _lt=long_tgt,
                         _ssl=short_sl, _lsl=long_sl):
                profile = _o(side_val, setup_val)
                profile["target_pct"] = _st if str(side_val).upper() == "SHORT" else _lt
                if _ssl is not None and str(side_val).upper() == "SHORT":
                    profile["stop_pct"] = _ssl
                if _lsl is not None and str(side_val).upper() != "SHORT":
                    profile["stop_pct"] = _lsl
                return profile
            _base._v17c_setup_exit_profile = _patched


# ---------------------------------------------------------------------------
# Core resolve logic
# ---------------------------------------------------------------------------
def run(
    version: str,
    short_tgt: float = None, long_tgt: float = None,
    short_sl: float = None, long_sl: float = None,
) -> None:
    cfg = _get_version_config(version)
    runner = cfg["runner"]
    csv_path = cfg["csv_path"]
    label = cfg["label"]
    v = version.lower().strip()

    tgt_suffix = ""
    if short_tgt is not None and long_tgt is not None:
        _apply_target_override(runner, version, short_tgt, long_tgt, short_sl, long_sl)
        sl_part = f" SL={short_sl*100:.2f}%" if short_sl is not None else ""
        tgt_suffix = f"  [TGT override: SHORT={short_tgt*100:.2f}% LONG={long_tgt*100:.2f}%{sl_part}]"

    print(f"\n{'=' * 60}")
    print(f"  Quick exit re-resolve — {label.upper()}{tgt_suffix}")
    print(f"{'=' * 60}")
    print(f"[LOAD] {csv_path.name}")

    df = pd.read_csv(csv_path)
    side_counts = df["side"].str.upper().value_counts().to_dict()
    print(f"[LOAD] {len(df)} trades  (SHORT={side_counts.get('SHORT',0)} | LONG={side_counts.get('LONG',0)})")

    # Strip exit columns
    drop_cols = [c for c in EXIT_COLS if c in df.columns]
    df = df.drop(columns=drop_cols)
    print(f"[PREP] Stripped {len(drop_cols)} exit columns — re-resolving from entries.")

    short_df = df[df["side"].str.upper().eq("SHORT")].copy().reset_index(drop=True)
    long_df  = df[df["side"].str.upper().eq("LONG")].copy().reset_index(drop=True)

    # Recompute stop_price / target_price from entry_price.
    # v18c5/v18c6's _resolve_exits_5min reads these columns directly (unlike v18c3
    # which recomputes them internally), so they must be present after stripping.
    # For v18c6: use the setup-specific exit profile so B_HUGE_RECLAIM gets the
    # correct SL (0.85%) even when no target override is active.
    import numpy as np

    if v in ("v18c6", "v19a") and hasattr(runner, "_v17c_setup_exit_profile"):
        # Per-trade computation using the exit profile (handles setup-specific SL/TGT)
        def _compute_prices(side_df):
            if side_df.empty or "entry_price" not in side_df.columns:
                return side_df
            side_df = side_df.copy()
            stops, targets = [], []
            for row in side_df.itertuples(index=False):
                ep    = float(getattr(row, "entry_price", 0))
                side  = str(getattr(row, "side", "")).upper()
                setup = str(getattr(row, "setup", ""))
                eit   = getattr(row, "entry_time_ist", None)
                prof  = runner._v17c_setup_exit_profile(side, setup, eit)
                sl_p  = float(prof.get("stop_pct", 0.0075))
                tgt_p = float(prof.get("target_pct", 0.0095))
                if side == "SHORT":
                    stops.append(ep * (1 + sl_p))
                    targets.append(ep * (1 - tgt_p))
                else:
                    stops.append(ep * (1 - sl_p))
                    targets.append(ep * (1 + tgt_p))
            side_df["stop_price"]   = stops
            side_df["sl_price"]     = stops
            side_df["target_price"] = targets
            return side_df
        short_df = _compute_prices(short_df)
        long_df  = _compute_prices(long_df)
        print(f"[PREP] Recomputed stop_price/target_price via setup-specific exit profiles ({label}).")

    elif short_tgt is not None and long_tgt is not None:
        # Simple uniform override for v17b / v18c3 / v18c4
        for side_df, tgt, sl_pct in [
            (short_df, short_tgt, short_sl if short_sl is not None else 0.0075),
            (long_df,  long_tgt,  long_sl  if long_sl  is not None else 0.0075),
        ]:
            if side_df.empty or "entry_price" not in side_df.columns:
                continue
            ep = side_df["entry_price"].astype(float)
            is_short = side_df["side"].str.upper().eq("SHORT")
            side_df["stop_price"]   = np.where(is_short, ep * (1 + sl_pct),  ep * (1 - sl_pct))
            side_df["target_price"] = np.where(is_short, ep * (1 - tgt),     ep * (1 + tgt))
            side_df["sl_price"]     = side_df["stop_price"]
        sl_info = f" SHORT_SL={short_sl*100:.2f}% LONG_SL={long_sl*100:.2f}%" if short_sl else ""
        print(f"[PREP] Recomputed stop_price/target_price — SHORT_TGT={short_tgt*100:.2f}% LONG_TGT={long_tgt*100:.2f}%{sl_info}")

    # Resolve 1-min directory
    dir_1m = runner._resolve_1min_dir()
    suffix_5m = ".parquet"
    if dir_1m.is_dir():
        for sf in list(dir_1m.glob("*"))[:5]:
            if sf.suffix:
                suffix_5m = sf.suffix
                break

    print(f"[1MIN] {dir_1m}")
    print(f"\n[PHASE 2] Re-resolving exits...")

    if not short_df.empty:
        print(f"  [SHORT] {len(short_df)} trades...")
        short_df = runner._resolve_exits_5min(
            short_df, dir_1m, suffix_5m, "pyarrow",
            eod_exit_time=runner.V15_EOD_EXIT_TIME,
        )
    if not long_df.empty:
        print(f"  [LONG]  {len(long_df)} trades...")
        long_df = runner._resolve_exits_5min(
            long_df, dir_1m, suffix_5m, "pyarrow",
            eod_exit_time=runner.V15_EOD_EXIT_TIME,
        )

    # Add notional P&L and sort
    if not short_df.empty:
        short_df = runner._add_notional_pnl(short_df)
    if not long_df.empty:
        long_df = runner._add_notional_pnl(long_df)

    combined = pd.concat([short_df, long_df], ignore_index=True)
    combined = runner._add_notional_pnl(combined)
    combined = runner._sort_trades_for_output(combined)
    short_df = runner._sort_trades_for_output(
        combined[combined["side"].str.upper().eq("SHORT")].copy()
    )
    long_df = runner._sort_trades_for_output(
        combined[combined["side"].str.upper().eq("LONG")].copy()
    )

    # Print results
    runner._print_day_side_mix(combined)

    from avwap_v11_refactored.avwap_common_v11_v15 import (
        compute_backtest_metrics, print_metrics,
    )
    print_metrics(f"SHORT [{label}]",    compute_backtest_metrics(short_df))
    print_metrics(f"LONG  [{label}]",    compute_backtest_metrics(long_df))
    print_metrics(f"COMBINED [{label}]", compute_backtest_metrics(combined))

    runner._print_exit_realism_band("SHORT",    short_df)
    runner._print_exit_realism_band("LONG",     long_df)
    runner._print_exit_realism_band("COMBINED", combined)

    runner._print_notional_pnl(combined)
    runner._print_recent_daily_breakdown(combined, n_weeks=2)

    print(f"\n[DONE] {label.upper()}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Quick exit re-resolver")
    parser.add_argument(
        "--version", required=True,
        choices=["v17b", "v18c3", "v18c4", "v18c6", "v19a"],
        help="Which runner version to use",
    )
    parser.add_argument(
        "--short-tgt", type=float, default=None,
        help="Override SHORT target pct (e.g. 0.005 for 0.5%%)",
    )
    parser.add_argument(
        "--long-tgt", type=float, default=None,
        help="Override LONG target pct (e.g. 0.005 for 0.5%%)",
    )
    parser.add_argument(
        "--short-sl", type=float, default=None,
        help="Override SHORT stop-loss pct (e.g. 0.01 for 1.0%%)",
    )
    parser.add_argument(
        "--long-sl", type=float, default=None,
        help="Override LONG stop-loss pct (e.g. 0.01 for 1.0%%)",
    )
    args = parser.parse_args()
    run(args.version, short_tgt=args.short_tgt, long_tgt=args.long_tgt,
        short_sl=args.short_sl, long_sl=args.long_sl)


if __name__ == "__main__":
    main()
