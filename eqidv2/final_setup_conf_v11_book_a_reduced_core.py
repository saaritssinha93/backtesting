"""V11 research Book A: reduced core setup book.

This module is shadow/backtest-only.  It derives from the frozen working V11
book and keeps only the setups selected for the reduced-core experiment.
Nothing here is imported by V7 or by the production V11 baseline unless the
module is explicitly selected through EQIDV2_V11_FINAL_SETUP_CONF_MODULE.
"""

from __future__ import annotations

from copy import deepcopy

import final_setup_conf_v11_working as _base


BOOK_ID = "V11_BOOK_A_REDUCED_CORE_20260722"
BOOK_STATUS = "RESEARCH_ONLY"
CORE_SETUPS = (
    "E_ORB_BREAKOUT_LONG",
    "C_OR_BREAKDOWN",
    "G_HIGHER_HIGH_BREAK",
    "G_LOWER_LOW_BREAK",
)

COST_BASIS = _base.COST_BASIS
ACCEPT_GATE = deepcopy(_base.ACCEPT_GATE)
RESEARCH_WATCH_CONF = deepcopy(_base.RESEARCH_WATCH_CONF)
FINAL_SETUP_CONF = {
    name: deepcopy(_base.FINAL_SETUP_CONF[name])
    for name in CORE_SETUPS
    if name in _base.FINAL_SETUP_CONF
}

if set(FINAL_SETUP_CONF) != set(CORE_SETUPS):
    missing = sorted(set(CORE_SETUPS) - set(FINAL_SETUP_CONF))
    raise RuntimeError(f"Book A base config is missing required setups: {missing}")

for _name, _cfg in FINAL_SETUP_CONF.items():
    _provenance = deepcopy(_cfg.get("provenance", {}))
    _provenance["v11_book_a_2026_07_22"] = {
        "book": BOOK_ID,
        "status": BOOK_STATUS,
        "change": "reduced-core membership only; setup gates and exits remain unchanged",
        "validation": "must pass fixed train/validation/holdout and forward-shadow gates",
    }
    _cfg["provenance"] = _provenance

