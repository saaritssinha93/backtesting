"""
V11 lab CONF_D: filtered B_HUGE expansion candidate.

Research-only module. Starts from CONF_A/prune2 and replaces the blunt B_HUGE
time cap with a movement-quality gate: only take B_HUGE shorts when the signal
bar has enough ATR-normalized movement and enough actual 5-minute range.
"""

from __future__ import annotations

from copy import deepcopy

import final_setup_conf_v11_lab_prune2 as _base


COST_BASIS = _base.COST_BASIS
ACCEPT_GATE = deepcopy(_base.ACCEPT_GATE)
FINAL_SETUP_CONF = deepcopy(_base.FINAL_SETUP_CONF)
RESEARCH_WATCH_CONF = deepcopy(_base.RESEARCH_WATCH_CONF)

LAB_CONF_ID = "CONF_D_FILTERED_B_HUGE_EXPANSION"
LAB_CONF_STATUS = "ACTIVE_LAB_CANDIDATE"
LAB_CONF_NOTES = [
    "Start from CONF_A/prune2.",
    "Keep A_MOD, C_OR, and E_ORB prune2 protections unchanged.",
    "Replace B_HUGE signal_minute<=690 with ATR/range quality requirements.",
    "Intent: more B_HUGE trades than CONF_A, but avoid the weak late drift shorts seen in CONF_B.",
]

if "B_HUGE_RED_FAILED_BOUNCE" in FINAL_SETUP_CONF:
    _cfg = deepcopy(FINAL_SETUP_CONF["B_HUGE_RED_FAILED_BOUNCE"])
    _cfg["mask_terms"] = [
        ["atr_pct", ">=", 0.00185],
        ["signal_range_pct", ">=", 0.279],
    ]
    _cfg["provenance"] = {
        **dict(_cfg.get("provenance", {})),
        "v11_lab_conf_d_2026_07_10": {
            "change": (
                "replace B_HUGE signal_minute<=690 with atr_pct>=0.00185 "
                "and signal_range_pct>=0.279"
            ),
            "reason": (
                "CONF_B's unrestricted B_HUGE expansion added trades but was "
                "near breakeven. The retained trades showed materially better "
                "follow-through when both ATR% and signal-bar range were high."
            ),
            "validation_requirement": (
                "must beat CONF_A on net P&L and trades/day while preserving "
                "PF, drawdown, and last-5-day behavior across fixed live-parity windows"
            ),
            "status": "research-only; not live-eligible without forward shadow",
        },
    }
    FINAL_SETUP_CONF["B_HUGE_RED_FAILED_BOUNCE"] = _cfg
