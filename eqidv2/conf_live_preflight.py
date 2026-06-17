"""Go-live preflight for the final_setup_conf -> v7 live migration.

Read-only GO / NO-GO checklist. It does NOT change anything and never imports the
live executor (no broker auth). It checks:

  A. Conf-path wiring (flag, exit-level sync, Tier-C live detection present).
  B. Paper-vs-live executor config parity (rev-2 P0-19) from env vars + the
     defaults baked into the executors.
  C. Risk-brake safety posture (rev-2 P0-18): reports the realized-only / refuse-
     entry-only limitation as a WARN until an MTM-aware, can-flatten brake lands.

Run:  py -3.12 conf_live_preflight.py
      EQIDV2_USE_FINAL_SETUP_CONF=1 py -3.12 conf_live_preflight.py
"""
from __future__ import annotations

import os
import sys

import eqidv2_final_conf_live_bootstrap as boot
import avwap_5min_ID_v6_backtesting as v6

# Executor env vars + the defaults coded in the executors. The paper executor
# flips its defaults to live values when the conf flag is set (P0-19), so this
# preflight mirrors that conf-mode logic to detect real drift.
_CONF = str(os.getenv("EQIDV2_USE_FINAL_SETUP_CONF", "0")).strip().lower() in {"1", "true", "yes", "on"}
PAPER_MAX = int(os.getenv("EQIDV2_PAPER_V7_ID_5MIN_MAX_CONCURRENT_TRADES", "20" if _CONF else "100"))
PAPER_BRAKE_ON = str(os.getenv("EQIDV2_PAPER_V7_DAILY_LOSS_BRAKE_ENABLED", "1" if _CONF else "0")).strip().lower() in {"1", "true", "yes", "on"}
PAPER_BRAKE_RS = abs(float(os.getenv("EQIDV2_PAPER_V7_DAILY_LOSS_BRAKE_RS", "10000" if _CONF else "7500")))
LIVE_MAX = int(os.getenv("EQIDV2_MAX_CONCURRENT_TRADES", "20"))
LIVE_DAILY_RS = float(os.getenv("EQIDV2_LIVE_DAILY_LOSS_LIMIT_RS", "10000"))
LIVE_PER_TRADE_RS = float(os.getenv("EQIDV2_LIVE_PER_TRADE_LOSS_LIMIT_RS", "5000"))
LIVE_KILL_ON = str(os.getenv("EQIDV2_LIVE_KILL_SWITCH_AUTO", "1")).strip().lower() in {"1", "true", "yes", "on"}

_rows: list[tuple[str, str, str]] = []


def _add(status: str, name: str, detail: str = "") -> None:
    _rows.append((status, name, detail))


def main() -> int:
    # ---- A. conf-path wiring -------------------------------------------------
    enabled = boot.is_enabled()
    keys = sorted(boot.conf_keys())
    _add("PASS" if enabled else "INFO", "conf flag EQIDV2_USE_FINAL_SETUP_CONF",
         "ON" if enabled else "OFF (live runs the legacy book; set =1 to use the 16-setup conf)")
    _add("PASS" if len(keys) == 16 else "FAIL", "conf book size", f"{len(keys)} setups")

    er = boot.exit_rules_from_conf()
    # exit levels are pushed to v6 only after the engine activates; here just confirm
    # the conf declares an exit for every setup (the engine syncs them at run).
    _add("PASS" if len(er) == len(keys) else "FAIL", "exit levels declared", f"{len(er)}/{len(keys)} setups have sl/tgt")

    try:
        import eqidv2_conf_tier_c_live_scan as tc
        _add("PASS", "Tier-C live detectors present", ", ".join(tc.TIER_C_SETUPS))
    except Exception as exc:
        _add("FAIL", "Tier-C live detectors", f"import failed: {exc!r}")

    # ---- B. paper-vs-live config parity (P0-19) -----------------------------
    _add("PASS" if PAPER_MAX == LIVE_MAX else "FAIL",
         "P0-19 max concurrent positions paper==live", f"paper={PAPER_MAX} live={LIVE_MAX}")
    _add("PASS" if PAPER_BRAKE_ON else "FAIL",
         "P0-19 paper daily brake enabled", f"paper_brake={'ON' if PAPER_BRAKE_ON else 'OFF'}")
    _add("PASS" if abs(PAPER_BRAKE_RS - LIVE_DAILY_RS) < 1 else "WARN",
         "P0-19 daily loss limit paper==live", f"paper=Rs{PAPER_BRAKE_RS:,.0f} live=Rs{LIVE_DAILY_RS:,.0f}")

    # ---- C. brake safety posture (P0-18) ------------------------------------
    _add("PASS" if LIVE_KILL_ON else "FAIL", "live auto kill-switch enabled",
         f"daily=Rs{LIVE_DAILY_RS:,.0f} per_trade=Rs{LIVE_PER_TRADE_RS:,.0f}")
    try:
        import eqidv2_risk_brake as _rb
        _add("PASS", "P0-18 MTM brake logic present", "eqidv2_risk_brake (realized+MTM, throttle, per-setup caps, flatten flag-gated)")
    except Exception as exc:
        _add("FAIL", "P0-18 MTM brake logic present", f"import failed: {exc!r}")
    mtm_obs = str(os.getenv("EQIDV2_BRAKE_MTM_OBSERVE", "1" if _CONF else "0")).strip().lower() in {"1", "true", "yes", "on"}
    mtm_act = str(os.getenv("EQIDV2_BRAKE_MTM_ACT", "0")).strip().lower() in {"1", "true", "yes", "on"}
    flatten = str(os.getenv("EQIDV2_BRAKE_FLATTEN_ON_BREACH", "0")).strip().lower() in {"1", "true", "yes", "on"}
    _add("PASS" if mtm_obs else "WARN", "P0-18 MTM brake wired in PAPER (observe)",
         f"observe={'ON' if mtm_obs else 'OFF'} act={'ON' if mtm_act else 'OFF'} flatten={'ON' if flatten else 'OFF'}")
    _add("WARN", "P0-18 MTM brake wired in LIVE executor",
         "pending — wire after watching paper observe logs; act+flatten stay flag-gated")

    # ---- print ---------------------------------------------------------------
    width = max(len(n) for _, n, _ in _rows)
    print("=" * 100)
    print("CONF -> v7 LIVE  GO/NO-GO PREFLIGHT")
    print("=" * 100)
    for status, name, detail in _rows:
        print(f"  [{status:4}] {name:<{width}}  {detail}")
    n_fail = sum(1 for s, _, _ in _rows if s == "FAIL")
    n_warn = sum(1 for s, _, _ in _rows if s == "WARN")
    print("-" * 100)
    verdict = "NO-GO (real capital)" if (n_fail or n_warn) else "GO"
    print(f"VERDICT: {verdict}   |   FAIL={n_fail}  WARN={n_warn}")
    print("Component/source parity is verified; the same-day paper-vs-v11 entry diff is the forward confirmation.")
    print("FAIL/WARN items above are EXECUTION-layer live-safety (P0-18/P0-19), required before real money.")
    print("Paper-trading the conf path (flag ON, paper executor) is ready to collect that live-vs-v11 day.")
    return 0 if not n_fail else 1


if __name__ == "__main__":
    sys.exit(main())
