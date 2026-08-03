"""
V11 lab CONF_A: conservative candidate.

Research-only module. Starts from final_setup_conf_v11_lab_prune2, the current
do-less/lose-less lab book. Do not import this from V7 live/paper.
"""

from __future__ import annotations

from copy import deepcopy

import final_setup_conf_v11_lab_prune2 as _base


COST_BASIS = _base.COST_BASIS
ACCEPT_GATE = deepcopy(_base.ACCEPT_GATE)
FINAL_SETUP_CONF = deepcopy(_base.FINAL_SETUP_CONF)
RESEARCH_WATCH_CONF = deepcopy(_base.RESEARCH_WATCH_CONF)

LAB_CONF_ID = "CONF_A_CONSERVATIVE_PRUNE2"
LAB_CONF_STATUS = "ACTIVE_LAB"
LAB_CONF_NOTES = [
    "Conservative control book: prune2 exactly.",
    "Weak L_DOUBLE/G_HIGHER/A_PULLBACK remain shadow-only.",
    "Use as benchmark for CONF_B/CONF_C expansions.",
]
