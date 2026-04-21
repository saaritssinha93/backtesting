# -*- coding: utf-8 -*-
"""
V17g 5-min combined runner - v17b live-parity core + LONG AMCC setup drop.

Research basis
--------------
Derived from the V17 research cycle in eqidv2/research/results/FINAL_REPORT.md.
Candidate **C10** (v17b SHORT + v17f LONG minus RSI[60,65) minus AMCC) was
identified as the Pareto-optimal variant across balanced quality metrics:
  - PF 1.992 (vs v17f 1.826, +9.1%)
  - MaxDD 29.09% (vs v17f 44.80%, -35.1% relative)
  - Sharpe 10.59 (vs v17f 9.51, +11.4%)
  - Walk-forward H1 PF 1.918, H2 PF 2.077 — filter edge is OOS-stable.

Design
------
1. Inherit all v17b live-parity patches (SHORT filter stack, LONG RSI[60,65)
   block, LONG anti-chase, Run2 data-driven bundle).
2. Add one LONG post-scan filter: drop setup == A_MOD_CLOSE_CONTINUATION_BREAK.
   On v17f this removed 123 LONG trades and cut MaxDD from 44.8% -> 30.7% while
   losing only 7% PnL — the single largest-impact find.
3. Route outputs to outputs_v17g_5min/ (separate from v17b/c/d/e/f).
4. Everything else — exits (0.75/1.00), executor contract (LIMIT + SL-M, no
   BE, no trail), Nifty confirm 09:20, data dir — unchanged from v17b.

Env knobs
---------
  EQIDV17G_LONG_AMCC_DROP_ENABLED   (default: True)

Reversing the filter (EQIDV17G_LONG_AMCC_DROP_ENABLED=0) should reproduce
v17b byte-for-byte on the LONG side — use this to validate wiring before
shipping.

Outputs go to outputs_v17g_5min/.
"""
from __future__ import annotations

from typing import Tuple

import pandas as pd

import avwap_combined_runner_v17b_5min as _v17b
import avwap_combined_runner_v16_5min as _base


# ---------------------------------------------------------------------------
# Env knobs
# ---------------------------------------------------------------------------
V17G_LONG_AMCC_DROP_ENABLED = _v17b._env_bool(
    "EQIDV17G_LONG_AMCC_DROP_ENABLED", True
)
V17G_LONG_AMCC_SETUP_NAME = "A_MOD_CLOSE_CONTINUATION_BREAK"


# ---------------------------------------------------------------------------
# PATCH 1: route outputs to outputs_v17g_5min/ (override v17b's v16->v17b
# replacement so anything that already had v17b_5min in its path also
# redirects to v17g_5min).
# ---------------------------------------------------------------------------
_orig_runtime_dir = _base.runtime_dir  # already points at v17b's patched dir


def _v17g_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in ("v17b_5min", "v16_5min"):
            text = text.replace(old, "v17g_5min")
        new_parts.append(text)
    return _orig_runtime_dir(*tuple(new_parts))


_base.runtime_dir = _v17g_runtime_dir


# ---------------------------------------------------------------------------
# PATCH 2: layer the LONG AMCC drop on top of v17b's post-scan filters.
# ---------------------------------------------------------------------------
_v17b_apply_post_scan_filters = _base._apply_v16_post_scan_filters  # already v17b-patched
_v17b_get_filter_reason = _base.get_v16_filter_reason               # already v17b-patched


def _v17g_apply_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    short_df, long_df = _v17b_apply_post_scan_filters(short_df, long_df)

    long_amcc_removed = 0
    long_before = len(long_df)

    if (
        V17G_LONG_AMCC_DROP_ENABLED
        and not long_df.empty
        and "setup" in long_df.columns
    ):
        setup = long_df["setup"].astype(str).str.upper().str.strip()
        mask = setup.eq(V17G_LONG_AMCC_SETUP_NAME)
        long_amcc_removed = int(mask.sum())
        long_df = long_df.loc[~mask].copy()

    print(
        "[V17G_FILTER] LONG extra: {before}->{after} "
        "(-{n} setup={setup})".format(
            before=long_before,
            after=len(long_df),
            n=long_amcc_removed,
            setup=V17G_LONG_AMCC_SETUP_NAME,
        )
    )
    return short_df, long_df


def _v17g_get_filter_reason(row: dict, side: str):
    reason = _v17b_get_filter_reason(row, side)
    if reason is not None:
        return reason

    if not V17G_LONG_AMCC_DROP_ENABLED:
        return None

    side_u = str(side).upper().strip()
    if side_u != "LONG":
        return None

    setup = str(row.get("setup", "")).upper().strip()
    if setup == V17G_LONG_AMCC_SETUP_NAME:
        return f"v17g long cleanup: drop setup={V17G_LONG_AMCC_SETUP_NAME}"

    return None


_base._apply_v16_post_scan_filters = _v17g_apply_post_scan_filters
_base.get_v16_filter_reason = _v17g_get_filter_reason


if __name__ == "__main__":
    print("=" * 78)
    print("V17g 5-min runner: v17b live-parity core + LONG AMCC setup drop")
    print("  Inherits all v17b patches:")
    print("    - SHORT filter stack (Run1 + Run2 bundles)")
    print("    - LONG RSI [60,65) dead-zone drop")
    print("    - LONG late-A_MOD anti-chase drop")
    print("    - Exits: LIMIT target + SL-M only (no BE, no trail)")
    print("    - Nifty confirm: 09:20 (V16 live parity)")
    print("  V17g LONG extra cleanup:")
    print(f"    - drop LONG setup={V17G_LONG_AMCC_SETUP_NAME} "
          f"(enabled={V17G_LONG_AMCC_DROP_ENABLED})")
    print("  Output dir: outputs_v17g_5min")
    print("=" * 78)
    _base.main()
