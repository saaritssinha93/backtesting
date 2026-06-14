"""
new_setups_validate_salvage.py — full anti-overfit battery on the SALVAGE passers (same standard as
new_setups_validate_passers.py). Reuses validate() + the salvage path caches. No goalpost move.

Candidates that produced PASS-tagged configs in the salvage search:
  N_HIGH_RS_EMA_BOUNCE_LONG_SALV (full D_EMA20_BOUNCE pool):
      sig5_adx_calc<=14.23 & close_loc<=0.95 & rs_pct>=0.20   (exit 0.7/1.5)   [LOW-ADX gate -> scrutinise]
  N_MORNING_ZERO_WICK_SHORT_NOORB_WIDE (non-E_ORB, wide):
      sig5_adx_calc>=21.5 & signal_minute>=710 & atr_pct>=0.0021  (exit 0.9/2.0) [signal_minute>=710 = LATE morning]
The strict NOORB pool (n=112) yielded NO honest config (all top1day>100%) -> reject, not validated here.

Run: py -3.12 new_setups_validate_salvage.py
"""
from __future__ import annotations
from pathlib import Path
import pickle
import new_setups_validate_passers as V

CACHE_N = Path(r"C:\TradingData\eqidv2\v11_salvage_N_paths_cache.pkl")
CACHE_L = Path(r"C:\TradingData\eqidv2\v11_salvage_L_paths_cache.pkl")

CFGS = {
    "L_RS_LEADER_VWAP_HOLD_LOOSE": {
        "_cache": "L",
        "side": "LONG",
        "gate": [("stock_ret", "<=", 0.96988), ("atr_pct", ">=", 0.002184), ("lower_wick_pct", ">=", 0.058506)],
        "exits": [(1.1, 2.0), (0.9, 2.0), (1.1, 1.5), (0.7, 1.5), (0.5, 2.0)],
        "primary_exit": (1.1, 2.0),
        "sens": {
            ("stock_ret", "<="): [0.5, 0.75, 0.96988, 1.3, 2.0],
            ("atr_pct", ">="): [0.0012, 0.0017, 0.002184, 0.0028, 0.0040],
            ("lower_wick_pct", ">="): [0.0, 0.03, 0.058506, 0.10, 0.18],
        },
    },
    "N_HIGH_RS_EMA_BOUNCE_LONG_SALV": {
        "_cache": "N",
        "side": "LONG",
        "gate": [("sig5_adx_calc", "<=", 14.232527), ("close_loc", "<=", 0.947359), ("rs_pct", ">=", 0.201432)],
        "exits": [(0.7, 1.5), (0.5, 1.25), (0.5, 1.0), (0.9, 1.5), (0.7, 1.0)],
        "primary_exit": (0.7, 1.5),
        "sens": {
            ("sig5_adx_calc", "<="): [11.0, 13.0, 14.232527, 17.0, 21.0],
            ("close_loc", "<="): [0.85, 0.90, 0.947359, 0.98, 1.01],
            ("rs_pct", ">="): [0.0, 0.10, 0.201432, 0.35, 0.55],
        },
    },
    "N_MORNING_ZERO_WICK_SHORT_NOORB_WIDE": {
        "_cache": "N",
        "side": "SHORT",
        "gate": [("sig5_adx_calc", ">=", 21.496426), ("signal_minute", ">=", 710.0), ("atr_pct", ">=", 0.002124)],
        "exits": [(0.9, 2.0), (1.1, 1.5), (0.7, 1.5), (1.1, 2.0), (0.7, 1.25)],
        "primary_exit": (0.9, 2.0),
        "sens": {
            ("sig5_adx_calc", ">="): [16.0, 19.0, 21.496426, 25.0, 29.0],
            ("signal_minute", ">="): [600.0, 660.0, 710.0, 720.0, 690.0],
            ("atr_pct", ">="): [0.0010, 0.0015, 0.002124, 0.0030, 0.0045],
        },
    },
}


def main():
    caches = {}
    with open(CACHE_N, "rb") as fh:
        caches["N"] = pickle.load(fh)
    if CACHE_L.exists():
        with open(CACHE_L, "rb") as fh:
            caches["L"] = pickle.load(fh)
    print(f"salvage N cache: { {k: len(v) for k, v in caches['N'].items()} }")
    if "L" in caches:
        print(f"salvage L cache: { {k: len(v) for k, v in caches['L'].items()} }")
    for setup, cfg in CFGS.items():
        allp = caches.get(cfg.get("_cache", "N"), {})
        V.validate(setup, cfg, allp)
    print("\n[verdict] honest accept only if: PF survives the sensitivity neighbourhood, no single term carries "
          "it on drop-out, both halves >1.3, >=70% months +, top1day<=~55%, p<0.10 at >=2 exits, n_test>=8.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
