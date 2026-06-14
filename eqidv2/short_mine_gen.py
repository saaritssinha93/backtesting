"""
short_mine_gen.py — REVERSE-ENGINEERING short setups: pull the big UNEXPLORED short pools from the clean
pool (fire on ~140 days each -> day-spread), sample to a tractable size (preserving day distribution),
write a candidate CSV for the search to mine SIMPLE gates (PF>2 train, good test, NOT day-concentrated).

Setups mined (not in book; not the E_ORB churn-sink / S_MACD crash-artifact):
  A_MOD_BREAK_C1_LOW, C_OR_BREAKDOWN, D_AVWAP_LOSE_REVERSAL, B_HUGE_RED_FAILED_BOUNCE, G_LOWER_LOW_BREAK
Run: py -3.12 short_mine_gen.py
"""
from __future__ import annotations
from pathlib import Path
import glob
import numpy as np
import pandas as pd

POOL = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_cleanpool")
OUT = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_new_setups_probe\short_mine_candidates.csv")
SETUPS = ["A_MOD_BREAK_C1_LOW", "C_OR_BREAKDOWN", "D_AVWAP_LOSE_REVERSAL",
          "B_HUGE_RED_FAILED_BOUNCE", "G_LOWER_LOW_BREAK"]
CAP_TRAIN, CAP_TEST = 1300, 1300
KEEP = ["ticker", "setup", "side", "signal_time_ist", "quality_score", "rs_pct", "market_ret_pct",
        "regime", "vol_ratio", "atr_pct", "body_pct", "close_loc", "vwap_dist_atr"]


def main():
    files = sorted(glob.glob(str(POOL / "chunk_*" / "historical_all_available_raw_candidates.csv")))
    df = pd.concat([pd.read_csv(f, low_memory=False) for f in files], ignore_index=True, sort=False)
    df["sig"] = pd.to_datetime(df["signal_time_ist"], errors="coerce")
    df = df.dropna(subset=["sig"])
    df = df[df["side"].astype(str).str.upper().eq("SHORT") & df["setup"].astype(str).isin(SETUPS)].copy()
    df["d"] = df["sig"].dt.strftime("%Y-%m-%d")
    out = []
    rng = 7
    for s in SETUPS:
        e = df[df["setup"].astype(str) == s]
        tr = e[e["d"] <= "2026-04-30"]; te = e[e["d"] >= "2026-05-01"]
        if len(tr) > CAP_TRAIN:
            tr = tr.sample(CAP_TRAIN, random_state=rng)
        if len(te) > CAP_TEST:
            te = te.sample(CAP_TEST, random_state=rng)
        sub = pd.concat([tr, te], ignore_index=True)
        out.append(sub)
        print(f"  {s}: train={len(tr)} test={len(te)} (of {(e['d']<='2026-04-30').sum()}/{(e['d']>='2026-05-01').sum()})")
    res = pd.concat(out, ignore_index=True)
    res = res[[c for c in KEEP if c in res.columns]]
    res.to_csv(OUT, index=False)
    print(f"[short_mine] wrote {len(res)} -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
