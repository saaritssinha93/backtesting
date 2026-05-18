# -*- coding: utf-8 -*-
"""
V17r-noNF 5-min runner — EXTENDED variety (does NOT alter the live runner).

Imports avwap_combined_runner_v17r_nonf_5min unchanged and adds two more
SHORT setups to its keep-chain whitelist. Source of the additions:
_v17r_nonf_setup_lab.py, which mines causal chains on the 11-month broken-
run CSV (re-resolved at TGT 1.5%/SL 0.75%, breadth-gate applied).

NEW SETUPS  (gates met: kept PF >= 1.15, OOS PF >= 1.15, decay >= 0.65)

  ("SHORT", "C_OR_BREAKDOWN"):
      n=43, kept PF=1.16, IS PF=1.02 (n32), OOS PF=1.67 (n11), decay=1.64.
      Chain: rsi_signal <= 28.12 AND entry_hour <= 10.08
             AND quality_score >= 0.6075 AND adx_signal <= 45.26.

  ("SHORT", "E_VWAP_BAND_FADE"):
      Greedy gave n=583 PF 1.16 (very loose). Tightened by hand to a
      higher-PF subset at the cost of volume:
      n=174 (over 11mo), kept PF=1.49, IS PF=1.54 (n140), OOS PF=1.30 (n34),
      decay=0.84.
      Chain: avwap_dist_atr_signal >= 0.5 AND atr_pct_signal >= 0.005.

LONG-side variety expansion was attempted (A_MOD_CLOSE_CONTINUATION_BREAK,
B_HUGE_C1_CLOSE_RECLAIM_BREAK) -- both failed strict OOS gates with the
new geometry; not added. To bring back dormant longs like D_EMA20_BOUNCE
or G_HIGHER_HIGH_BREAK, the cascade's V17M low-win-cleanup filter must be
disabled -- separate exercise.

Run with:
    set EQIDV_DATE_FROM=2026-02-05   # last 3 months
    python avwap_combined_runner_v17r_nonf_5min_more.py
"""
from __future__ import annotations

import avwap_combined_runner_v17r_nonf_5min as _v17r


# Extend the live whitelist (the live runner's existing 3 chains are NOT
# touched — only new (side, setup) keys are added).
_NEW_CHAINS = {
    ("SHORT", "C_OR_BREAKDOWN"): [
        ("rsi_signal", "<=", 28.1209),
        ("entry_hour", "<=", 10.0833),
        ("quality_score", ">=", 0.607518),
        ("adx_signal", "<=", 45.2624),
    ],
    ("SHORT", "E_VWAP_BAND_FADE"): [
        ("avwap_dist_atr_signal", ">=", 0.5),
        ("atr_pct_signal", ">=", 0.005),
    ],
}

for key, chain in _NEW_CHAINS.items():
    if key in _v17r.CAUSAL_KEEP_CHAINS:
        print(f"[V17R_MORE] WARN: {key} already in live whitelist -- skipping (live chain wins)")
        continue
    _v17r.CAUSAL_KEEP_CHAINS[key] = chain
    _v17r.VOLUME_KEEP_CHAINS[key] = chain
    print(f"[V17R_MORE] added {key[0]} {key[1]}: {chain}")

print(f"[V17R_MORE] total whitelist size: {len(_v17r.CAUSAL_KEEP_CHAINS)} setups "
      f"({sum(1 for k in _v17r.CAUSAL_KEEP_CHAINS if k[0]=='SHORT')} SHORT, "
      f"{sum(1 for k in _v17r.CAUSAL_KEEP_CHAINS if k[0]=='LONG')} LONG)")


if __name__ == "__main__":
    print("=" * 78)
    print("V17r-noNF +MORE — extended setup variety atop the live runner")
    print(f"  New setups: {list(_NEW_CHAINS.keys())}")
    print(f"  Inherits all live config (TGT 1.5%/SL 0.75%/ADV gate/breadth gate)")
    print("=" * 78)
    _v17r._base.main()
