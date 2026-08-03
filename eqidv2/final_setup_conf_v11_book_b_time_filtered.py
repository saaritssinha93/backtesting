"""V11 research Book B: current book with setup-specific time hypotheses.

The underlying setup definitions and exits come from the frozen working V11
book.  Only entry-time guards are changed.  This is deliberately a separate
module so the production baseline remains untouched.
"""

from __future__ import annotations

from copy import deepcopy

import final_setup_conf_v11_working as _base


BOOK_ID = "V11_BOOK_B_TIME_FILTERED_20260722"
BOOK_STATUS = "RESEARCH_ONLY"

COST_BASIS = _base.COST_BASIS
ACCEPT_GATE = deepcopy(_base.ACCEPT_GATE)
RESEARCH_WATCH_CONF = deepcopy(_base.RESEARCH_WATCH_CONF)
FINAL_SETUP_CONF = deepcopy(_base.FINAL_SETUP_CONF)

# These are hypotheses identified from session-level diagnostics.  They are not
# promotion rules and must be evaluated on untouched validation/holdout dates.
TIME_GUARDS = {
    "E_ORB_BREAKOUT_LONG": {
        "min_slot": "10:30",
        "max_slot": "11:30",
    },
    "C_OR_BREAKDOWN": {
        "min_slot": "12:30",
    },
    "A_MOD_BREAK_C1_LOW": {
        "min_slot": "13:30",
    },
    "B_HUGE_RED_FAILED_BOUNCE": {
        "max_slot": "13:30",
    },
}

# L_DOUBLE_BOTTOM_VWAP already inherits the working book's stricter 10:00-11:30
# window.  Do not loosen that existing protection merely to reproduce the wider
# diagnostic window used by older runs.

for _name, _guards in TIME_GUARDS.items():
    if _name not in FINAL_SETUP_CONF:
        raise RuntimeError(f"Book B base config is missing required setup: {_name}")
    _cfg = FINAL_SETUP_CONF[_name]
    _cfg["entry_guards"] = {**deepcopy(_cfg.get("entry_guards", {})), **deepcopy(_guards)}
    _provenance = deepcopy(_cfg.get("provenance", {}))
    _provenance["v11_book_b_2026_07_22"] = {
        "book": BOOK_ID,
        "status": BOOK_STATUS,
        "change": f"entry_guards={_guards!r}",
        "validation": "time-window hypothesis; never promote from the discovery sample",
    }
    _cfg["provenance"] = _provenance
