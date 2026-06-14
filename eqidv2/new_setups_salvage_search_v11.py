"""
new_setups_salvage_search_v11.py — run the SAME aggressive + anti-overfit search (new_setups_search_v11)
on the HONEST salvage pools for the 3 rejects. Same acceptance bar; no goalpost move.

Sources:
  phase n (clean-pool overlays, fast):  new_setups_salvage_candidates.csv
      N_HIGH_RS_EMA_BOUNCE_LONG_SALV (663) | N_MORNING_ZERO_WICK_SHORT_NOORB (112) | ..._NOORB_WIDE (340)
  phase l (loosened structural re-scan): l_rs_leader_loose_trades.csv
      L_RS_LEADER_VWAP_HOLD_LOOSE

Usage: py -3.12 new_setups_salvage_search_v11.py --phase n|l
"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import new_setups_search_v11 as S

PROBE = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_new_setups_probe")
PROP = S.PROP

PHASES = {
    "n": {
        "src": PROBE / "_none_.csv",
        "overlay": PROBE / "new_setups_salvage_candidates.csv",
        "scanner_setups": set(),
        "overlay_setups": {"N_HIGH_RS_EMA_BOUNCE_LONG_SALV", "N_MORNING_ZERO_WICK_SHORT_NOORB",
                           "N_MORNING_ZERO_WICK_SHORT_NOORB_WIDE"},
        "setups": {
            "N_HIGH_RS_EMA_BOUNCE_LONG_SALV": ("LONG", (0.90, 1.50), 2000),
            "N_MORNING_ZERO_WICK_SHORT_NOORB": ("SHORT", (1.00, 2.00), 2000),
            "N_MORNING_ZERO_WICK_SHORT_NOORB_WIDE": ("SHORT", (1.00, 2.00), 2000),
        },
        "cache": Path(r"C:\TradingData\eqidv2\v11_salvage_N_paths_cache.pkl"),
        "out": PROP / "new_setups_salvage_search_results_N.csv",
    },
    "l": {
        "src": PROBE / "l_rs_leader_loose_trades.csv",
        "overlay": PROBE / "_none_.csv",
        "scanner_setups": {"L_RS_LEADER_VWAP_HOLD_LOOSE"},
        "overlay_setups": set(),
        "setups": {"L_RS_LEADER_VWAP_HOLD_LOOSE": ("LONG", (0.80, 1.50), 2000)},
        "cache": Path(r"C:\TradingData\eqidv2\v11_salvage_L_paths_cache.pkl"),
        "out": PROP / "new_setups_salvage_search_results_L.csv",
    },
    "new2": {  # the two brand-new setups (gap-and-go long, power-hour laggard short)
        "src": PROBE / "new2_setups_standalone_trades.csv",
        "overlay": PROBE / "_none_.csv",
        "scanner_setups": {"GAP_UP_HOLD_BREAK", "POWER_HOUR_LAGGARD_BREAKDOWN"},
        "overlay_setups": set(),
        "setups": {
            "GAP_UP_HOLD_BREAK": ("LONG", (0.90, 1.50), 2000),
            "POWER_HOUR_LAGGARD_BREAKDOWN": ("SHORT", (0.80, 1.00), 2000),
        },
        "cache": Path(r"C:\TradingData\eqidv2\v11_new2_paths_cache.pkl"),
        "out": PROP / "new2_setups_search_results.csv",
    },
    "short": {  # reverse-engineering: mine the big unexplored clean-pool short pools for simple gates
        "src": PROBE / "_none_.csv",
        "overlay": PROBE / "short_mine_candidates.csv",
        "scanner_setups": set(),
        "overlay_setups": {"A_MOD_BREAK_C1_LOW", "C_OR_BREAKDOWN", "D_AVWAP_LOSE_REVERSAL",
                           "B_HUGE_RED_FAILED_BOUNCE", "G_LOWER_LOW_BREAK"},
        "setups": {
            "A_MOD_BREAK_C1_LOW": ("SHORT", (0.70, 1.00), 2000),
            "C_OR_BREAKDOWN": ("SHORT", (0.70, 1.00), 2000),
            "D_AVWAP_LOSE_REVERSAL": ("SHORT", (0.70, 1.00), 2000),
            "B_HUGE_RED_FAILED_BOUNCE": ("SHORT", (0.70, 1.00), 2000),
            "G_LOWER_LOW_BREAK": ("SHORT", (0.70, 1.00), 2000),
        },
        "cache": Path(r"C:\TradingData\eqidv2\v11_shortmine_paths_cache.pkl"),
        "out": PROP / "short_mine_search_results.csv",
    },
    "amod": {  # deeper mine of the biggest short pool A_MOD_BREAK_C1_LOW (3000/3000 sample)
        "src": PROBE / "_none_.csv",
        "overlay": PROBE / "amod_mine_candidates.csv",
        "scanner_setups": set(),
        "overlay_setups": {"A_MOD_BREAK_C1_LOW"},
        "setups": {"A_MOD_BREAK_C1_LOW": ("SHORT", (0.70, 1.00), 3000)},
        "cache": Path(r"C:\TradingData\eqidv2\v11_amod_paths_cache.pkl"),
        "out": PROP / "amod_mine_search_results.csv",
    },
    "short2": {  # deeper mine D_AVWAP_LOSE_REVERSAL + G_LOWER_LOW_BREAK
        "src": PROBE / "_none_.csv",
        "overlay": PROBE / "short2_mine_candidates.csv",
        "scanner_setups": set(),
        "overlay_setups": {"D_AVWAP_LOSE_REVERSAL", "G_LOWER_LOW_BREAK"},
        "setups": {"D_AVWAP_LOSE_REVERSAL": ("SHORT", (0.70, 1.00), 3500),
                   "G_LOWER_LOW_BREAK": ("SHORT", (0.70, 1.00), 5000)},
        "cache": Path(r"C:\TradingData\eqidv2\v11_short2_paths_cache.pkl"),
        "out": PROP / "short2_mine_search_results.csv",
    },
    "new3": {  # mean-reversion / trap-reversal trio (chosen for day-spread)
        "src": PROBE / "new3_setups_standalone_trades.csv",
        "overlay": PROBE / "_none_.csv",
        "scanner_setups": {"GAP_DOWN_FADE_RECLAIM", "FIRST_HOUR_LOW_RECLAIM", "FIRST_HOUR_HIGH_FAIL"},
        "overlay_setups": set(),
        "setups": {
            "GAP_DOWN_FADE_RECLAIM": ("LONG", (0.90, 1.50), 2000),
            "FIRST_HOUR_LOW_RECLAIM": ("LONG", (0.90, 1.50), 2000),
            "FIRST_HOUR_HIGH_FAIL": ("SHORT", (0.80, 1.00), 2000),
        },
        "cache": Path(r"C:\TradingData\eqidv2\v11_new3_paths_cache.pkl"),
        "out": PROP / "new3_setups_search_results.csv",
    },
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["n", "l", "new2", "new3", "short", "amod", "short2"], required=True)
    a = ap.parse_args()
    cfg = PHASES[a.phase]
    # drive the proven engine by overriding its module globals
    S.SRC = cfg["src"]
    S.OVERLAY_SRC = cfg["overlay"]
    S.SCANNER_SETUPS = cfg["scanner_setups"]
    S.OVERLAY_SETUPS = cfg["overlay_setups"]
    S.SETUPS = cfg["setups"]
    S.CACHE = cfg["cache"]
    PROP.mkdir(parents=True, exist_ok=True)
    allp = S.load_or_build()
    outs = []
    for setup in S.SETUPS:
        if len(allp.get(setup, [])) < S.MIN_TRADES:
            print(f"\n{setup}: insufficient paths ({len(allp.get(setup, []))})"); continue
        _, res = S.search(setup, allp[setup])
        if len(res):
            outs.append(res)
    if outs:
        pd.concat(outs, ignore_index=True).to_csv(cfg["out"], index=False)
        print(f"\n[salvage] wrote {cfg['out']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
