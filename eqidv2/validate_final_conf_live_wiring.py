"""Phase-1 sign-off check for the final_setup_conf -> v7 live wiring.

Validates the CURRENT EQIDV2_USE_FINAL_SETUP_CONF flag state:
  flag UNSET  -> asserts the wiring is a pure no-op (live behaviour unchanged).
  flag SET    -> asserts the conf (16-setup book) is installed across the live
                 scanner + 1-minute entry engine + v6 exit levels.

Run BOTH ways to fully sign off Phase 1:
    py -3.12 validate_final_conf_live_wiring.py
    EQIDV2_USE_FINAL_SETUP_CONF=1 py -3.12 validate_final_conf_live_wiring.py

Exit code 0 = PASS. Non-zero = FAIL. Does NOT connect to a broker or run a slot.
"""
from __future__ import annotations

import sys

import pandas as pd

import eqidv2_final_conf_live_bootstrap as boot
import eqidv2_signal_discovery_v7_5min_id_persistent as scn
import eqidv2_entry_engine_1min_v5_id as eng
import avwap_5min_ID_v7_candidate_scan as cs
import avwap_5min_ID_v6_backtesting as v6


def _check(label: str, cond: bool) -> bool:
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    return cond


def main() -> int:
    enabled = boot.is_enabled()
    keys = set(boot.conf_keys())
    ukeys = set(boot.conf_keys_upper())
    allow_before = set(cs.ALLOWED_SETUPS)
    cor_shadow_before = "C_OR_BREAKDOWN" in eng.PRE_ENTRY_MOMENTUM_SHADOW_SETUPS

    a = scn._ensure_conf_scanner()
    b = eng._ensure_conf_engine()
    ok = True

    if not enabled:
        print("== FLAG OFF: expect pure no-op ==")
        ok &= _check("scanner _ensure returns False", a is False)
        ok &= _check("engine _ensure returns False", b is False)
        ok &= _check("candidate_scan.ALLOWED_SETUPS unchanged", set(cs.ALLOWED_SETUPS) == allow_before)
        ok &= _check("C_OR_BREAKDOWN still shadowed", cor_shadow_before and "C_OR_BREAKDOWN" in eng.PRE_ENTRY_MOMENTUM_SHADOW_SETUPS)
    else:
        print(f"== FLAG ON: expect conf installed ({boot.GATE_VERSION}) ==")
        ok &= _check("scanner active", a is True)
        ok &= _check("engine active", b is True)
        ok &= _check("ALLOWED_SETUPS == 16 conf keys", set(cs.ALLOWED_SETUPS) == keys and len(keys) == 16)
        ok &= _check("no conf setup in EARLY_BLOCKED", not (set(map(str.upper, cs.EARLY_BLOCKED_SETUPS)) & ukeys))
        ok &= _check("no conf setup in RESEARCH_PROBATION", not (set(map(str.upper, cs.RESEARCH_PROBATION_SETUPS)) & ukeys))
        ok &= _check("C_OR_BREAKDOWN un-shadowed", "C_OR_BREAKDOWN" not in eng.PRE_ENTRY_MOMENTUM_SHADOW_SETUPS)
        ok &= _check("pre-momentum gates == conf", eng.PRE_ENTRY_MOMENTUM_SETUP_GATES == boot.pre_momentum_gates_from_conf())
        ok &= _check("pre-momentum gates enabled", bool(eng.PRE_ENTRY_MOMENTUM_GATES_ENABLED))
        ok &= _check("missing-feature action == block", eng.PRE_ENTRY_MOMENTUM_MISSING_ACTION == "block")
        er = boot.exit_rules_from_conf()
        ok &= _check("v6 exit levels == conf for all 16", all(v6.SETUP_EXIT_RULES.get(k) == er[k] for k in er))

        # gate behaviour smoke
        df = pd.DataFrame([
            {"setup": "C_OR_BREAKDOWN", "regime": "BEAR", "signal_time_ist": "2026-06-12 10:00:00",
             "vol_ratio": 2.0, "quality_score": 80, "vwap_dist_atr": -1.0, "rs_pct": 0.1, "body_pct": 0.5},
            {"setup": "L_PRESSURE_BURST_VWAP", "regime": "BULL", "signal_time_ist": "2026-06-12 10:00:00",
             "vol_ratio": 2.0, "quality_score": 20, "vwap_dist_atr": 0.5, "rs_pct": 0.1, "body_pct": 0.5},
            {"setup": "L_PRESSURE_BURST_VWAP", "regime": "BULL", "signal_time_ist": "2026-06-12 10:00:00",
             "vol_ratio": 2.0, "quality_score": 40, "vwap_dist_atr": 0.5, "rs_pct": 0.1, "body_pct": 0.5},
            {"setup": "E_VWAP_LOSE_EARLY_SHORT", "regime": "BEAR", "signal_time_ist": "2026-06-12 09:30:00",
             "vol_ratio": 2.0, "quality_score": 80, "vwap_dist_atr": -1.0, "rs_pct": 0.1, "body_pct": 0.5},
            {"setup": "NOT_A_CONF_SETUP", "regime": "BULL", "signal_time_ist": "2026-06-12 10:00:00",
             "vol_ratio": 2.0, "quality_score": 80, "vwap_dist_atr": 0.5, "rs_pct": 0.1, "body_pct": 0.5},
        ])
        acc, _, st = boot.apply_conf_gate(df)
        ok &= _check("conf gate keeps exactly the 2 valid rows", st["final_setup_conf_accepted"] == 2
                     and set(acc["setup"]) == {"C_OR_BREAKDOWN", "L_PRESSURE_BURST_VWAP"})

    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
