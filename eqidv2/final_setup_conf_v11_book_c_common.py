"""Factory for executable V11 research Book C exit variants."""
from __future__ import annotations
from copy import deepcopy
import final_setup_conf_v11_book_a_reduced_core as _base

COST_BASIS = _base.COST_BASIS
ACCEPT_GATE = deepcopy(_base.ACCEPT_GATE)
RESEARCH_WATCH_CONF = deepcopy(_base.RESEARCH_WATCH_CONF)


def build_book(exit_policy: dict, variant: str) -> dict:
    conf = deepcopy(_base.FINAL_SETUP_CONF)
    for name, cfg in conf.items():
        cfg["exit_policy"] = deepcopy(exit_policy)
        provenance = deepcopy(cfg.get("provenance", {}))
        provenance[f"v11_book_c_{variant.lower()}_2026_07_22"] = {
            "book": f"V11_BOOK_C_{variant}_20260722",
            "status": "RESEARCH_ONLY",
            "change": f"exit_policy={exit_policy!r}",
            "validation": "compare with Book A using identical entries and fixed windows",
        }
        cfg["provenance"] = provenance
    return conf

