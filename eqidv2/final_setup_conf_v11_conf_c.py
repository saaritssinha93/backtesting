"""
V11 lab CONF_C: aggressive reference candidate.

Research-only module. This intentionally tests wider trade-count expansion than
CONF_B. It is not live-eligible by design; it exists to show where extra trades
start damaging expectancy.
"""

from __future__ import annotations

from copy import deepcopy

import final_setup_conf_v11_lab_prune2 as _base


COST_BASIS = _base.COST_BASIS
ACCEPT_GATE = deepcopy(_base.ACCEPT_GATE)
FINAL_SETUP_CONF = deepcopy(_base.FINAL_SETUP_CONF)
RESEARCH_WATCH_CONF = deepcopy(_base.RESEARCH_WATCH_CONF)

LAB_CONF_ID = "CONF_C_AGGRESSIVE_REFERENCE"
LAB_CONF_STATUS = "REFERENCE_ONLY"
LAB_CONF_NOTES = [
    "Start from CONF_A/prune2.",
    "Remove B_HUGE timing cap.",
    "Slightly widen C_OR body cap and A_MOD time cap as a controlled overtrade reference.",
    "Not live-eligible without beating CONF_A/CONF_B on robustness.",
]

if "B_HUGE_RED_FAILED_BOUNCE" in FINAL_SETUP_CONF:
    _cfg = deepcopy(FINAL_SETUP_CONF["B_HUGE_RED_FAILED_BOUNCE"])
    _cfg["mask_terms"] = []
    _cfg["provenance"] = {
        **dict(_cfg.get("provenance", {})),
        "v11_lab_conf_c_2026_07_10": {"change": "remove B_HUGE signal_minute cap"},
    }
    FINAL_SETUP_CONF["B_HUGE_RED_FAILED_BOUNCE"] = _cfg

if "C_OR_BREAKDOWN" in FINAL_SETUP_CONF:
    _cfg = deepcopy(FINAL_SETUP_CONF["C_OR_BREAKDOWN"])
    _cfg["mask_terms"] = [
        ["signal_minute", "<=", 720],
        ["body_pct", "<=", 0.75],
    ]
    _cfg["provenance"] = {
        **dict(_cfg.get("provenance", {})),
        "v11_lab_conf_c_2026_07_10": {
            "change": "widen C_OR to signal_minute<=720 and body_pct<=0.75",
            "warning": "aggressive reference; prune2 body/time filter was protective",
        },
    }
    FINAL_SETUP_CONF["C_OR_BREAKDOWN"] = _cfg

if "A_MOD_BREAK_C1_LOW" in FINAL_SETUP_CONF:
    _cfg = deepcopy(FINAL_SETUP_CONF["A_MOD_BREAK_C1_LOW"])
    _cfg["mask_terms"] = [
        ["vol_ratio", ">=", 1.955814],
        ["signal_minute", "<=", 720],
    ]
    _cfg["provenance"] = {
        **dict(_cfg.get("provenance", {})),
        "v11_lab_conf_c_2026_07_10": {
            "change": "widen A_MOD from signal_minute<=690 to <=720",
            "warning": "aggressive reference; late A_MOD losses are a known risk",
        },
    }
    FINAL_SETUP_CONF["A_MOD_BREAK_C1_LOW"] = _cfg
