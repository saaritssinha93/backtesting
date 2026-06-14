"""
tier123_new_validate.py — full anti-overfit battery on the 4 standout NEW tier123 setups (from
tier123_new_iterate). Same bar that caught S_UPTHRUST/T_TREND as artifacts and confirmed L_RS_LEADER.
Uses the Tnew path cache. Run: py -3.12 tier123_new_validate.py
"""
from __future__ import annotations
from pathlib import Path
import pickle
import new_setups_validate_passers as V

CACHE = Path(r"C:\TradingData\eqidv2\v11_Tnew_paths_cache.pkl")
CFGS = {
    "V_RECLAIM_PULLBACK_LONG": {
        "side": "LONG",
        "gate": [("sig5_adx_calc", "<=", 16.911132), ("pre5_dir_count", "<=", 2.0)],
        "exits": [(0.7, 0.6), (0.5, 0.8), (0.9, 0.8), (0.7, 0.8), (0.5, 0.6)],
        "primary_exit": (0.7, 0.6),
        "sens": {("sig5_adx_calc", "<="): [12.0, 15.0, 16.911132, 20.0, 25.0],
                 ("pre5_dir_count", "<="): [0.0, 1.0, 2.0, 3.0, 4.0]},
    },
    "P_PDH_BREAK_RETEST_LONG": {
        "side": "LONG",
        "gate": [("pre_entry_momentum_score", ">=", 75.071712), ("body_pct", "<=", 0.749993), ("pre3_range_r", ">=", 0.499787)],
        "exits": [(0.5, 0.6), (0.7, 0.6), (0.7, 0.8), (0.5, 0.8), (0.5, 1.0)],
        "primary_exit": (0.5, 0.6),
        "sens": {("pre_entry_momentum_score", ">="): [65.0, 70.0, 75.071712, 80.0, 85.0],
                 ("body_pct", "<="): [0.55, 0.65, 0.749993, 0.85, 0.95],
                 ("pre3_range_r", ">="): [0.30, 0.40, 0.499787, 0.60, 0.70]},
    },
    "E_ORB_RETEST_HOLD_LONG": {
        "side": "LONG",
        "gate": [("sig5_adx_calc", ">=", 42.41646), ("vol_ratio", ">=", 2.423842), ("quality_score", ">=", 86.575268), ("signal_minute", ">=", 605.0)],
        "exits": [(0.9, 1.25), (0.5, 1.25), (0.7, 1.0), (1.1, 1.0), (0.9, 1.0)],
        "primary_exit": (0.9, 1.25),
        "sens": {("sig5_adx_calc", ">="): [30.0, 38.0, 42.41646, 48.0, 55.0],
                 ("vol_ratio", ">="): [1.8, 2.1, 2.423842, 2.8, 3.4],
                 ("quality_score", ">="): [70.0, 80.0, 86.575268, 95.0, 105.0]},
    },
    "E_ORB_RETEST_HOLD_SHORT": {
        "side": "SHORT",
        "gate": [("pre5_dir_count", ">=", 4.0), ("pre3_close_pos", "<=", 0.583333), ("sig5_adx_calc", ">=", 23.273922), ("body_pct", ">=", 0.6)],
        "exits": [(0.7, 0.6), (0.9, 0.6), (0.7, 0.8), (0.9, 2.0), (0.5, 0.8)],
        "primary_exit": (0.7, 0.6),
        "sens": {("pre5_dir_count", ">="): [2.0, 3.0, 4.0, 5.0],
                 ("pre3_close_pos", "<="): [0.40, 0.50, 0.583333, 0.70, 0.85],
                 ("sig5_adx_calc", ">="): [18.0, 21.0, 23.273922, 28.0, 34.0],
                 ("body_pct", ">="): [0.40, 0.50, 0.6, 0.75, 0.90]},
    },
}


def main():
    with open(CACHE, "rb") as fh:
        allp = pickle.load(fh)
    for setup, cfg in CFGS.items():
        V.validate(setup, cfg, {setup: allp[setup]})
    print("\n[verdict] accept only if PF survives sensitivity, no single term carries it on drop-out, "
          "both halves >1.3, >=70% months +, top1day<=~55%, p<0.10 at >=2 exits, n_test>=8, NOT market_ret-conditioned.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
