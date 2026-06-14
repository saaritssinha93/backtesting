"""
lrs_revalidate.py — full anti-overfit battery on L_RS_LEADER_VWAP_HOLD using the CORRECTED-VWAP/regime
cache (v11_newsetups_paths_cache.pkl, rebuilt 2026-06-13). It was rejected before for test-starvation
(5 test); corrected data gives 204 test. Two candidate gates from the corrected search:
  A (2-term):  lower_wick_pct>=0.148 & pre10_mom_r>=0.631            (exit 1.1/1.0)  [top1d 31%, p0.003]
  B (4-term):  quality_score>=97.1 & signal_minute<=660 & vol_ratio>=2.16 & vwap_dist_atr<=1.49 (exit 0.5/1.25)
Run: py -3.12 lrs_revalidate.py
"""
from __future__ import annotations
from pathlib import Path
import pickle
import new_setups_validate_passers as V

CACHE = Path(r"C:\TradingData\eqidv2\v11_newsetups_paths_cache.pkl")
CFGS = {
    "L_RS_LEADER_VWAP_HOLD__A2": {
        "_setup": "L_RS_LEADER_VWAP_HOLD", "side": "LONG",
        "gate": [("lower_wick_pct", ">=", 0.148148), ("pre10_mom_r", ">=", 0.630842)],
        "exits": [(1.1, 1.0), (0.9, 1.0), (1.1, 0.8), (0.9, 1.5), (1.1, 1.5)],
        "primary_exit": (1.1, 1.0),
        "sens": {
            ("lower_wick_pct", ">="): [0.05, 0.10, 0.148148, 0.22, 0.32],
            ("pre10_mom_r", ">="): [0.30, 0.45, 0.630842, 0.80, 1.0],
        },
    },
    "L_RS_LEADER_VWAP_HOLD__B4": {
        "_setup": "L_RS_LEADER_VWAP_HOLD", "side": "LONG",
        "gate": [("quality_score", ">=", 97.121022), ("signal_minute", "<=", 660.0),
                 ("vol_ratio", ">=", 2.164331), ("vwap_dist_atr", "<=", 1.49336)],
        "exits": [(0.5, 1.25), (0.7, 1.25), (0.5, 1.5), (0.9, 1.25), (0.5, 1.0)],
        "primary_exit": (0.5, 1.25),
        "sens": {
            ("quality_score", ">="): [85.0, 92.0, 97.121022, 105.0, 115.0],
            ("vol_ratio", ">="): [1.5, 1.8, 2.164331, 2.6, 3.2],
            ("vwap_dist_atr", "<="): [0.5, 1.0, 1.49336, 2.2, 3.5],
        },
    },
}


def main():
    with open(CACHE, "rb") as fh:
        allp = pickle.load(fh)
    print(f"corrected cache L_RS_LEADER paths: {len(allp.get('L_RS_LEADER_VWAP_HOLD', []))}")
    for name, cfg in CFGS.items():
        V.validate(name, cfg, {name: allp[cfg["_setup"]]})
    print("\n[verdict] honest accept only if PF survives sensitivity, no single term carries it on drop-out, "
          "both halves >1.3, >=70% months +, top1day<=~55%, p<0.10 at >=2 exits, n_test>=8.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
