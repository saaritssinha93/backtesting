"""
V11 lab CONF_B: balanced expansion candidate.

Research-only module. Starts from prune2 and adds back only the cleanest
trade-count expansion: unrestricted B_HUGE_RED_FAILED_BOUNCE timing. The weak
longs and A_PULLBACK remain shadow-only.
"""

from __future__ import annotations

from copy import deepcopy

import final_setup_conf_v11_lab_prune2 as _base


COST_BASIS = _base.COST_BASIS
ACCEPT_GATE = deepcopy(_base.ACCEPT_GATE)
FINAL_SETUP_CONF = deepcopy(_base.FINAL_SETUP_CONF)
RESEARCH_WATCH_CONF = deepcopy(_base.RESEARCH_WATCH_CONF)

LAB_CONF_ID = "CONF_B_BALANCED_B_HUGE_EXPANSION"
LAB_CONF_STATUS = "ACTIVE_LAB"
LAB_CONF_NOTES = [
    "Start from CONF_A/prune2.",
    "Expand B_HUGE_RED_FAILED_BOUNCE by removing the R&D 11:30 cap.",
    "Keep A_MOD, C_OR, and E_ORB prune2 protections unchanged.",
]

if "B_HUGE_RED_FAILED_BOUNCE" in FINAL_SETUP_CONF:
    _cfg = deepcopy(FINAL_SETUP_CONF["B_HUGE_RED_FAILED_BOUNCE"])
    _cfg["mask_terms"] = []
    _prov = dict(_cfg.get("provenance", {}))
    _prov["v11_lab_conf_b_2026_07_10"] = {
        "change": "remove signal_minute <= 690 from B_HUGE_RED_FAILED_BOUNCE",
        "reason": (
            "B_HUGE had positive contribution in current V11 reruns; prune2's "
            "global time cap removed observed winning B_HUGE shorts while the "
            "setup's own pre-momentum gate remains load-bearing."
        ),
        "validation_requirement": (
            "must improve trades/day and net P&L without worsening drawdown or "
            "best-day dependency across fixed windows"
        ),
    }
    _cfg["provenance"] = _prov
    FINAL_SETUP_CONF["B_HUGE_RED_FAILED_BOUNCE"] = _cfg
