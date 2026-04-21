# -*- coding: utf-8 -*-
"""
V17h 5-min combined runner - v17g (C10) with P03 exit policy (TGT 1.00% -> 1.50%).

Research basis
--------------
From eqidv2/research/results/FINAL_REPORT.md sec 5.3:
  - 10 exit policies swept on C10 and C3 via 5-min bar re-resolution.
  - P03 (SL 0.75%, TGT 1.50%, RR 2:1) ranked #1 on both candidates.
  - Relative-to-baseline: PnL +40%, DD roughly flat, PF flat.

v17h = v17g strategy (v17b SHORT + v17b LONG minus AMCC) + P03 exit widening.
This is the A/B test runner scaffolded per report sec 11.3 - not intended
for immediate live deployment since widening the target changes the live
LIMIT order, but the executor contract (LIMIT + SL-M, no BE, no trail)
remains intact.

Env knobs
---------
  EQIDV17G_LONG_AMCC_DROP_ENABLED   (inherited from v17g, default True)
  EQIDV17H_TARGET_PCT               (default 0.0150 = 1.50%, applies both sides)
  EQIDV17H_SHORT_TARGET_PCT         (optional per-side override)
  EQIDV17H_LONG_TARGET_PCT          (optional per-side override)

Outputs go to outputs_v17h_5min/.
"""
from __future__ import annotations

import os

import avwap_combined_runner_v17g_5min as _v17g  # noqa: F401 — imports cascade v17b -> v16
import avwap_combined_runner_v16_5min as _base
import avwap_combined_runner_v17b_5min as _v17b


# ---------------------------------------------------------------------------
# Env knobs
# ---------------------------------------------------------------------------
V17H_TARGET_PCT = _v17b._env_float("EQIDV17H_TARGET_PCT", 0.0150)
V17H_SHORT_TARGET_PCT = _v17b._env_float("EQIDV17H_SHORT_TARGET_PCT", V17H_TARGET_PCT)
V17H_LONG_TARGET_PCT = _v17b._env_float("EQIDV17H_LONG_TARGET_PCT", V17H_TARGET_PCT)


# ---------------------------------------------------------------------------
# PATCH 1: route outputs to outputs_v17h_5min/
# v17g already redirects v17b_5min + v16_5min -> v17g_5min in its runtime_dir
# wrapper; we add one more redirect layer to land files in v17h_5min/.
# ---------------------------------------------------------------------------
_orig_runtime_dir = _base.runtime_dir  # already v17g-patched


def _v17h_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in ("v17g_5min", "v17b_5min", "v16_5min"):
            text = text.replace(old, "v17h_5min")
        new_parts.append(text)
    return _orig_runtime_dir(*tuple(new_parts))


_base.runtime_dir = _v17h_runtime_dir


# ---------------------------------------------------------------------------
# PATCH 2: widen targets by overriding the base module constants that feed
# the TEST_TARGET_OVERRIDE block in both apply_live_parity_profile (line 786-788)
# and main() (line 3669-3671).  TEST_TARGET_OVERRIDE is already True by default.
# ---------------------------------------------------------------------------
_base.TEST_SHORT_TARGET_PCT = float(V17H_SHORT_TARGET_PCT)
_base.TEST_LONG_TARGET_PCT = float(V17H_LONG_TARGET_PCT)


# ---------------------------------------------------------------------------
# PATCH 3: also widen target inside the v17b apply_live_parity_profile wrapper,
# which runs after the base override.  v17b patched apply_live_parity_profile
# to force enable_breakeven/enable_trailing_stop=False; we append target_pct
# overrides on top so the values stick even if the live wrapper recomputes.
# ---------------------------------------------------------------------------
_v17b_apply = _base.apply_live_parity_profile  # already v17b-patched


def _v17h_apply_live_parity_profile(short_cfg, long_cfg):
    short_cfg, long_cfg = _v17b_apply(short_cfg, long_cfg)
    short_cfg.target_pct = float(V17H_SHORT_TARGET_PCT)
    long_cfg.target_pct = float(V17H_LONG_TARGET_PCT)
    return short_cfg, long_cfg


_base.apply_live_parity_profile = _v17h_apply_live_parity_profile


if __name__ == "__main__":
    print("=" * 78)
    print("V17h 5-min runner: v17g (C10) + P03 exit policy (TGT widened)")
    print("  Inherits all v17g patches:")
    print("    - v17b SHORT filter stack + live-parity core")
    print("    - LONG RSI [60,65) drop, LONG anti-chase, LONG AMCC drop")
    print("    - Exits: LIMIT + SL-M only (no BE, no trail)")
    print("  V17h exit widening:")
    print(f"    - SHORT target : {V17H_SHORT_TARGET_PCT * 100.0:.2f}% (was 1.00%)")
    print(f"    - LONG  target : {V17H_LONG_TARGET_PCT * 100.0:.2f}% (was 1.00%)")
    print("    - Stop loss    : 0.75% (UNCHANGED — RR 1.33:1 -> 2:1)")
    print("  Output dir: outputs_v17h_5min")
    print("=" * 78)
    _base.main()
