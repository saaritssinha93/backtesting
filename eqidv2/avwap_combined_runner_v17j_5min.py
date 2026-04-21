# -*- coding: utf-8 -*-
"""
V17j 5-min combined runner - v17g with measured short-side indicator relaxation.

Purpose
-------
Keep the proven v17g long leg, exits, live-parity contract, and runtime shape
intact, but admit a few more SHORT entries by relaxing only the most restrictive
short indicator filters from the inherited v17b stack.

Default short-side relaxations
------------------------------
1. SHORT_ONLY RSI cleanup narrows from [21,28) -> [20,25)
   - still blocks the deep weak-RSI pocket
   - recovers some 25-28 RSI continuation shorts
2. SHORT_ONLY high-ADX exhaustion threshold lifts from >=44 -> >=48
3. SHORT AVWAP dead zone narrows from [0.5,1.0) -> [0.75,1.0)
4. BOTH-mode short ADX chop zone narrows from [25,30) -> [25,28)

Everything else stays aligned with v17g:
  - LONG AMCC drop remains enabled by default
  - LONG RSI[60,65) drop remains inherited
  - exits remain LIMIT target + SL-M only
  - no breakeven, no trailing stop
  - Nifty confirm / live-parity timing untouched

Env knobs
---------
  EQIDV17J_SHORT_BLOCK_SHORTONLY_RSI_ENABLED   (default: True)
  EQIDV17J_SHORT_SHORTONLY_RSI_LO              (default: 20.0)
  EQIDV17J_SHORT_SHORTONLY_RSI_HI              (default: 25.0)
  EQIDV17J_SHORT_BLOCK_SHORTONLY_HIGH_ADX_ENABLED (default: True)
  EQIDV17J_SHORT_SHORTONLY_HIGH_ADX_MIN        (default: 48.0)
  EQIDV17J_SHORT_AVWAP_DEAD_ENABLED            (default: True)
  EQIDV17J_SHORT_AVWAP_DEAD_LO                 (default: 0.75)
  EQIDV17J_SHORT_AVWAP_DEAD_HI                 (default: 1.00)
  EQIDV17J_SHORT_BOTH_ADX_DEAD_ENABLED         (default: True)
  EQIDV17J_SHORT_BOTH_ADX_DEAD_LO              (default: 25.0)
  EQIDV17J_SHORT_BOTH_ADX_DEAD_HI              (default: 28.0)

Outputs go to outputs_v17j_5min/.
"""
from __future__ import annotations

import avwap_combined_runner_v17g_5min as _v17g  # noqa: F401 - import applies v17g patch stack
import avwap_combined_runner_v17b_5min as _v17b
import avwap_combined_runner_v16_5min as _base


# ---------------------------------------------------------------------------
# Env knobs - short-only relaxations layered on top of v17g/v17b.
# ---------------------------------------------------------------------------
V17J_SHORT_BLOCK_SHORTONLY_RSI_ENABLED = _v17b._env_bool(
    "EQIDV17J_SHORT_BLOCK_SHORTONLY_RSI_ENABLED", True
)
V17J_SHORT_SHORTONLY_RSI_LO = _v17b._env_float(
    "EQIDV17J_SHORT_SHORTONLY_RSI_LO", 20.0
)
V17J_SHORT_SHORTONLY_RSI_HI = _v17b._env_float(
    "EQIDV17J_SHORT_SHORTONLY_RSI_HI", 25.0
)

V17J_SHORT_BLOCK_SHORTONLY_HIGH_ADX_ENABLED = _v17b._env_bool(
    "EQIDV17J_SHORT_BLOCK_SHORTONLY_HIGH_ADX_ENABLED", True
)
V17J_SHORT_SHORTONLY_HIGH_ADX_MIN = _v17b._env_float(
    "EQIDV17J_SHORT_SHORTONLY_HIGH_ADX_MIN", 48.0
)

V17J_SHORT_AVWAP_DEAD_ENABLED = _v17b._env_bool(
    "EQIDV17J_SHORT_AVWAP_DEAD_ENABLED", True
)
V17J_SHORT_AVWAP_DEAD_LO = _v17b._env_float(
    "EQIDV17J_SHORT_AVWAP_DEAD_LO", 0.75
)
V17J_SHORT_AVWAP_DEAD_HI = _v17b._env_float(
    "EQIDV17J_SHORT_AVWAP_DEAD_HI", 1.00
)

V17J_SHORT_BOTH_ADX_DEAD_ENABLED = _v17b._env_bool(
    "EQIDV17J_SHORT_BOTH_ADX_DEAD_ENABLED", True
)
V17J_SHORT_BOTH_ADX_DEAD_LO = _v17b._env_float(
    "EQIDV17J_SHORT_BOTH_ADX_DEAD_LO", 25.0
)
V17J_SHORT_BOTH_ADX_DEAD_HI = _v17b._env_float(
    "EQIDV17J_SHORT_BOTH_ADX_DEAD_HI", 28.0
)


# ---------------------------------------------------------------------------
# PATCH 1: route outputs to outputs_v17j_5min/
# ---------------------------------------------------------------------------
_orig_runtime_dir = _base.runtime_dir  # already v17g-patched


def _v17j_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17j_5min",
            "v17h_5min",
            "v17g_5min",
            "v17f_5min",
            "v17d_5min",
            "v17c_5min",
            "v17b_5min",
            "v16_5min",
        ):
            text = text.replace(old, "v17j_5min")
        new_parts.append(text)
    return _orig_runtime_dir(*tuple(new_parts))


_base.runtime_dir = _v17j_runtime_dir


# ---------------------------------------------------------------------------
# PATCH 2: soften only the inherited v17b short indicator filters.
# v17g's long AMCC wrapper still runs on top of these updated globals.
# ---------------------------------------------------------------------------
_v17b.V17B_SHORT_BLOCK_SHORTONLY_RSI_ENABLED = bool(
    V17J_SHORT_BLOCK_SHORTONLY_RSI_ENABLED
)
_v17b.V17B_SHORT_SHORTONLY_RSI_LO = float(V17J_SHORT_SHORTONLY_RSI_LO)
_v17b.V17B_SHORT_SHORTONLY_RSI_HI = float(V17J_SHORT_SHORTONLY_RSI_HI)

_v17b.V17B_SHORT_BLOCK_SHORTONLY_HIGH_ADX_ENABLED = bool(
    V17J_SHORT_BLOCK_SHORTONLY_HIGH_ADX_ENABLED
)
_v17b.V17B_SHORT_SHORTONLY_HIGH_ADX_MIN = float(
    V17J_SHORT_SHORTONLY_HIGH_ADX_MIN
)

_v17b.V17B_SHORT_AVWAP_DEAD_ENABLED = bool(V17J_SHORT_AVWAP_DEAD_ENABLED)
_v17b.V17B_SHORT_AVWAP_DEAD_LO = float(V17J_SHORT_AVWAP_DEAD_LO)
_v17b.V17B_SHORT_AVWAP_DEAD_HI = float(V17J_SHORT_AVWAP_DEAD_HI)

_v17b.V17B_SHORT_BOTH_ADX_DEAD_ENABLED = bool(V17J_SHORT_BOTH_ADX_DEAD_ENABLED)
_v17b.V17B_SHORT_BOTH_ADX_DEAD_LO = float(V17J_SHORT_BOTH_ADX_DEAD_LO)
_v17b.V17B_SHORT_BOTH_ADX_DEAD_HI = float(V17J_SHORT_BOTH_ADX_DEAD_HI)


if __name__ == "__main__":
    print("=" * 78)
    print("V17j 5-min runner: v17g + measured short-side indicator relaxation")
    print("  Inherits all v17g patches:")
    print("    - v17b live-parity short stack as the base")
    print("    - LONG RSI [60,65) drop")
    print("    - LONG AMCC drop")
    print("    - Exits: LIMIT + SL-M only (no BE, no trail)")
    print("  V17j short relaxations:")
    print(
        f"    - SHORT_ONLY RSI block: enabled={V17J_SHORT_BLOCK_SHORTONLY_RSI_ENABLED} "
        f"range=[{V17J_SHORT_SHORTONLY_RSI_LO:.1f},{V17J_SHORT_SHORTONLY_RSI_HI:.1f})"
    )
    print(
        f"    - SHORT_ONLY high ADX block: enabled={V17J_SHORT_BLOCK_SHORTONLY_HIGH_ADX_ENABLED} "
        f"min={V17J_SHORT_SHORTONLY_HIGH_ADX_MIN:.1f}"
    )
    print(
        f"    - SHORT AVWAP dead zone: enabled={V17J_SHORT_AVWAP_DEAD_ENABLED} "
        f"range=[{V17J_SHORT_AVWAP_DEAD_LO:.2f},{V17J_SHORT_AVWAP_DEAD_HI:.2f})"
    )
    print(
        f"    - SHORT BOTH-mode ADX dead zone: enabled={V17J_SHORT_BOTH_ADX_DEAD_ENABLED} "
        f"range=[{V17J_SHORT_BOTH_ADX_DEAD_LO:.1f},{V17J_SHORT_BOTH_ADX_DEAD_HI:.1f})"
    )
    print("  Output dir: outputs_v17j_5min")
    print("=" * 78)
    _base.main()
