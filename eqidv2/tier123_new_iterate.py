"""
tier123_new_iterate.py — run the SAME T_iterate aggressive + anti-overfit battery on the NEW tier123
research setups (those that fired on the CORRECTED probe, beyond T_TREND/MR which are already done).
Drives T_iterate by overriding its SETUPS + CACHE. NET of cost; train Nov-Apr / test May-Jun.

Run AFTER 15:30 IST. Usage: py -3.12 tier123_new_iterate.py
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd
import T_iterate as T

CAP = 1000  # train sample cap (bounds path-building on the big 2500-candidate setups)
NEW = {
    # Tier 1
    "E_ORB_RETEST_HOLD_LONG": ("LONG", (0.70, 1.00), CAP),
    "E_ORB_RETEST_HOLD_SHORT": ("SHORT", (0.70, 1.00), CAP),
    "E_FAILED_OR_BREAKDOWN_TRAP_LONG": ("LONG", (0.75, 1.00), CAP),
    "E_FAILED_OR_BREAKOUT_TRAP_SHORT": ("SHORT", (0.75, 1.00), CAP),
    "V_RECLAIM_PULLBACK_LONG": ("LONG", (0.70, 1.00), CAP),
    "V_REJECTION_PULLBACK_SHORT": ("SHORT", (0.70, 1.00), CAP),
    "M_EXPANSION_FIRST_PULLBACK_LONG": ("LONG", (0.80, 1.20), CAP),
    "M_EXPANSION_FIRST_PULLBACK_SHORT": ("SHORT", (0.80, 1.20), CAP),
    "C_LATE_MORNING_COMPRESSION_BREAK_LONG": ("LONG", (0.75, 1.10), CAP),
    "C_LATE_MORNING_COMPRESSION_BREAK_SHORT": ("SHORT", (0.75, 1.10), CAP),
    # Tier 2
    "G_GAP_HOLD_CONTINUATION_LONG": ("LONG", (0.80, 1.20), CAP),
    "G_GAP_HOLD_CONTINUATION_SHORT": ("SHORT", (0.80, 1.20), CAP),
    "A_HVN_ABSORPTION_BREAK_LONG": ("LONG", (0.75, 1.00), CAP),
    "A_HVN_ABSORPTION_BREAK_SHORT": ("SHORT", (0.75, 1.00), CAP),
    # Tier 3
    "P_PDH_BREAK_RETEST_LONG": ("LONG", (0.75, 1.00), CAP),
    "P_PDL_BREAK_RETEST_SHORT": ("SHORT", (0.75, 1.00), CAP),
}


def main():
    T.SETUPS = NEW
    T.CACHE = Path(r"C:\TradingData\eqidv2\v11_Tnew_paths_cache.pkl")
    out = T.PROP / "tier123_new_setups_results.csv"
    T.PROP.mkdir(parents=True, exist_ok=True)
    allp = T.load_or_build()
    print(f"[Tnew] path counts: { {k: len(v) for k, v in allp.items()} }", flush=True)
    outs = []
    for setup in T.SETUPS:
        if len(allp.get(setup, [])) < T.MIN_TRADES:
            print(f"\n{setup}: insufficient paths ({len(allp.get(setup, []))})"); continue
        _, res = T.search(setup, allp[setup])
        if len(res):
            res["setup"] = setup
            outs.append(res)
    if outs:
        pd.concat(outs, ignore_index=True).to_csv(out, index=False)
    print(f"\n[Tnew] done -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
