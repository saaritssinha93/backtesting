# -*- coding: utf-8 -*-
"""
V17i 5-min combined runner - v17g core + narrowed indicator dead zones.

Motivation
----------
Round-4 diagnostic (eqidv2/research/diag_round4.py) showed v17g is running at
5.18 trades/day, mostly LONG-dominated (783 L / 165 S). User target is 10/day
with simultaneous quality gains.

v17i narrows each of v17b's indicator dead zones by one "step" - this releases
boundary trades that were conservatively rejected. The filter stack itself
remains intact; only the bounds are tightened:

  SHORT:
    RSI SHORT_ONLY dead       : [21, 28) -> [22, 27)    (releases RSI 21 and 27)
    AVWAP dist dead           : [0.5, 1.0) -> [0.6, 0.9) (releases boundary bins)
    ADX BOTH-mode dead        : [25, 30) -> [26, 29)    (releases boundary bins)
    Open-window cutoff        : 9:45 -> 9:40            (releases 9:40-9:44)
    Base RSI dead-zone LO     : 30.0 -> 28.0            (releases RSI 28-30)

  LONG:
    RSI dead                  : [60, 65) -> [61, 64)    (releases RSI 60 and 64)
    Late anti-chase min bars  : 13 -> 16                (releases early-late trades)
    Base QS dead              : [7.6, 7.9) -> [7.65, 7.85)

v17g's existing patches (AMCC drop, SHORT filter stack, output routing) all
remain in force. v17i layers ON TOP of v17g by mutating the v17b env vars
BEFORE v17b's module-level evaluation picks them up -- but since we import
v17g after setting env vars here, we instead mutate the v17b module constants
directly after import.

Env knobs
---------
  All narrowing can be reversed via env vars:
    EQIDV17B_SHORT_SHORTONLY_RSI_LO    (default 22.0, v17b=21.0)
    EQIDV17B_SHORT_SHORTONLY_RSI_HI    (default 27.0, v17b=28.0)
    EQIDV17B_SHORT_AVWAP_DEAD_LO       (default 0.6,  v17b=0.5)
    EQIDV17B_SHORT_AVWAP_DEAD_HI       (default 0.9,  v17b=1.0)
    EQIDV17B_SHORT_BOTH_ADX_DEAD_LO    (default 26.0, v17b=25.0)
    EQIDV17B_SHORT_BOTH_ADX_DEAD_HI    (default 29.0, v17b=30.0)
    EQIDV17B_SHORT_OPEN_CUTOFF_MIN     (default 580,  v17b=585=9:45)
    EQIDV17B_BASE_SHORT_RSI_DEAD_LO    (default 28.0, v17b=30.0)
    EQIDV17B_LONG_RSI_DEAD_LO          (default 61.0, v17b=60.0)
    EQIDV17B_LONG_RSI_DEAD_HI          (default 64.0, v17b=65.0)
    EQIDV17B_LONG_LATE_MIN_BARS_FROM_OPEN (default 16, v17b=13)
    EQIDV17B_BASE_LONG_QS_DEAD_LO      (default 7.65, v17b=7.6)
    EQIDV17B_BASE_LONG_QS_DEAD_HI      (default 7.85, v17b=7.9)

Outputs go to outputs_v17i_5min/.
"""
from __future__ import annotations

import os

# Set v17b-level env vars BEFORE importing v17g (which imports v17b).
# This ensures v17b's module-level _env_float/_env_bool picks up v17i defaults.
_V17I_DEFAULTS = {
    "EQIDV17B_SHORT_SHORTONLY_RSI_LO": "22.0",
    "EQIDV17B_SHORT_SHORTONLY_RSI_HI": "27.0",
    "EQIDV17B_SHORT_AVWAP_DEAD_LO": "0.6",
    "EQIDV17B_SHORT_AVWAP_DEAD_HI": "0.9",
    "EQIDV17B_SHORT_BOTH_ADX_DEAD_LO": "26.0",
    "EQIDV17B_SHORT_BOTH_ADX_DEAD_HI": "29.0",
    "EQIDV17B_SHORT_OPEN_CUTOFF_MIN": str(9 * 60 + 40),
    "EQIDV17B_BASE_SHORT_RSI_DEAD_LO": "28.0",
    "EQIDV17B_LONG_RSI_DEAD_LO": "61.0",
    "EQIDV17B_LONG_RSI_DEAD_HI": "64.0",
    "EQIDV17B_LONG_LATE_MIN_BARS_FROM_OPEN": "16",
    "EQIDV17B_BASE_LONG_QS_DEAD_LO": "7.65",
    "EQIDV17B_BASE_LONG_QS_DEAD_HI": "7.85",
}
for _k, _v in _V17I_DEFAULTS.items():
    os.environ.setdefault(_k, _v)

import avwap_combined_runner_v17g_5min as _v17g  # noqa: F401 — pulls v17b -> v16
import avwap_combined_runner_v16_5min as _base
import avwap_combined_runner_v17b_5min as _v17b


# ---------------------------------------------------------------------------
# PATCH 1: route outputs to outputs_v17i_5min/
# v17g already redirects v17g_5min/v17b_5min/v16_5min -> v17g_5min; we add
# one more redirect to land in v17i_5min.
# ---------------------------------------------------------------------------
_orig_runtime_dir = _base.runtime_dir  # already v17g-patched


def _v17i_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in ("v17g_5min", "v17b_5min", "v16_5min"):
            text = text.replace(old, "v17i_5min")
        new_parts.append(text)
    return _orig_runtime_dir(*tuple(new_parts))


_base.runtime_dir = _v17i_runtime_dir


# ---------------------------------------------------------------------------
# PATCH 2: force the already-imported v17b module constants to reflect the
# narrowed defaults. Since _V17I_DEFAULTS was applied before import, the
# v17b module should have picked them up at import time. This block is a
# belt-and-suspenders sanity check + allows the printed config to reflect
# v17i-specific values.
# ---------------------------------------------------------------------------
_v17b.V17B_SHORT_SHORTONLY_RSI_LO = float(os.environ["EQIDV17B_SHORT_SHORTONLY_RSI_LO"])
_v17b.V17B_SHORT_SHORTONLY_RSI_HI = float(os.environ["EQIDV17B_SHORT_SHORTONLY_RSI_HI"])
_v17b.V17B_SHORT_AVWAP_DEAD_LO = float(os.environ["EQIDV17B_SHORT_AVWAP_DEAD_LO"])
_v17b.V17B_SHORT_AVWAP_DEAD_HI = float(os.environ["EQIDV17B_SHORT_AVWAP_DEAD_HI"])
_v17b.V17B_SHORT_BOTH_ADX_DEAD_LO = float(os.environ["EQIDV17B_SHORT_BOTH_ADX_DEAD_LO"])
_v17b.V17B_SHORT_BOTH_ADX_DEAD_HI = float(os.environ["EQIDV17B_SHORT_BOTH_ADX_DEAD_HI"])
_v17b.V17B_SHORT_OPEN_CUTOFF_MIN = int(os.environ["EQIDV17B_SHORT_OPEN_CUTOFF_MIN"])
_v17b.V17B_LONG_RSI_DEAD_LO = float(os.environ["EQIDV17B_LONG_RSI_DEAD_LO"])
_v17b.V17B_LONG_RSI_DEAD_HI = float(os.environ["EQIDV17B_LONG_RSI_DEAD_HI"])
_v17b.V17B_LONG_LATE_MIN_BARS_FROM_OPEN = int(os.environ["EQIDV17B_LONG_LATE_MIN_BARS_FROM_OPEN"])

# Base-module overrides (v17b's PATCH 5 already set these at import time; we
# re-apply here in case env vars changed between v17b import and now).
_base.V16_SHORT_RSI_DEAD_ZONE_LO = float(os.environ["EQIDV17B_BASE_SHORT_RSI_DEAD_LO"])
_base.V16_LONG_QS_DEAD_LO = float(os.environ["EQIDV17B_BASE_LONG_QS_DEAD_LO"])
_base.V16_LONG_QS_DEAD_HI = float(os.environ["EQIDV17B_BASE_LONG_QS_DEAD_HI"])


if __name__ == "__main__":
    print("=" * 78)
    print("V17i 5-min runner: v17g (C10) core + narrowed indicator dead zones")
    print("  Inherits all v17g patches:")
    print("    - v17b SHORT filter stack + live-parity core")
    print("    - LONG AMCC drop, LONG RSI[60,65) drop, LONG anti-chase")
    print("    - Exits: LIMIT + SL-M only (no BE, no trail)")
    print("  V17i dead-zone narrowing:")
    print(f"    SHORT RSI dead            : [{_v17b.V17B_SHORT_SHORTONLY_RSI_LO:.1f}, "
          f"{_v17b.V17B_SHORT_SHORTONLY_RSI_HI:.1f})  (was [21.0, 28.0))")
    print(f"    SHORT AVWAP dead          : [{_v17b.V17B_SHORT_AVWAP_DEAD_LO:.2f}, "
          f"{_v17b.V17B_SHORT_AVWAP_DEAD_HI:.2f})  (was [0.50, 1.00))")
    print(f"    SHORT ADX BOTH dead       : [{_v17b.V17B_SHORT_BOTH_ADX_DEAD_LO:.1f}, "
          f"{_v17b.V17B_SHORT_BOTH_ADX_DEAD_HI:.1f})  (was [25.0, 30.0))")
    print(f"    SHORT open cutoff         : {_v17b.V17B_SHORT_OPEN_CUTOFF_MIN} min  "
          f"(was 585 = 9:45)")
    print(f"    SHORT base RSI dead LO    : {_base.V16_SHORT_RSI_DEAD_ZONE_LO:.1f}  "
          f"(was 30.0)")
    print(f"    LONG RSI dead             : [{_v17b.V17B_LONG_RSI_DEAD_LO:.1f}, "
          f"{_v17b.V17B_LONG_RSI_DEAD_HI:.1f})  (was [60.0, 65.0))")
    print(f"    LONG late anti-chase min  : {_v17b.V17B_LONG_LATE_MIN_BARS_FROM_OPEN} bars  "
          f"(was 13)")
    print(f"    LONG base QS dead         : [{_base.V16_LONG_QS_DEAD_LO:.2f}, "
          f"{_base.V16_LONG_QS_DEAD_HI:.2f})  (was [7.60, 7.90))")
    print("  Output dir: outputs_v17i_5min")
    print("=" * 78)
    _base.main()
