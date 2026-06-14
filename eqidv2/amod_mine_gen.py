"""amod_mine_gen.py — deeper mine of A_MOD_BREAK_C1_LOW (biggest short pool, 43808). Bigger sample for power."""
from __future__ import annotations
from pathlib import Path
import glob
import pandas as pd

POOL = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_cleanpool")
OUT = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_new_setups_probe\amod_mine_candidates.csv")
CAP_TRAIN, CAP_TEST = 3000, 3000
KEEP = ["ticker", "setup", "side", "signal_time_ist", "quality_score", "rs_pct", "market_ret_pct",
        "regime", "vol_ratio", "atr_pct", "body_pct", "close_loc", "vwap_dist_atr"]


def main():
    files = sorted(glob.glob(str(POOL / "chunk_*" / "historical_all_available_raw_candidates.csv")))
    df = pd.concat([pd.read_csv(f, low_memory=False) for f in files], ignore_index=True, sort=False)
    df["sig"] = pd.to_datetime(df["signal_time_ist"], errors="coerce")
    df = df.dropna(subset=["sig"])
    df = df[df["side"].astype(str).str.upper().eq("SHORT") & df["setup"].astype(str).eq("A_MOD_BREAK_C1_LOW")].copy()
    df["d"] = df["sig"].dt.strftime("%Y-%m-%d")
    tr = df[df["d"] <= "2026-04-30"]; te = df[df["d"] >= "2026-05-01"]
    if len(tr) > CAP_TRAIN:
        tr = tr.sample(CAP_TRAIN, random_state=7)
    if len(te) > CAP_TEST:
        te = te.sample(CAP_TEST, random_state=7)
    res = pd.concat([tr, te], ignore_index=True)[[c for c in KEEP if c in df.columns]]
    res.to_csv(OUT, index=False)
    print(f"[amod_mine] A_MOD_BREAK_C1_LOW: train={len(tr)} test={len(te)} -> wrote {len(res)} to {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
