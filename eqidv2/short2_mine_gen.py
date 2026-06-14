"""short2_mine_gen.py — deeper mine D_AVWAP_LOSE_REVERSAL (resolve h1-thin) + G_LOWER_LOW_BREAK (full)."""
from __future__ import annotations
from pathlib import Path
import glob
import pandas as pd

POOL = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_cleanpool")
OUT = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_new_setups_probe\short2_mine_candidates.csv")
# setup -> (cap_train, cap_test)
PLAN = {"D_AVWAP_LOSE_REVERSAL": (3500, 3000), "G_LOWER_LOW_BREAK": (5000, 5000)}  # G_LOWER: take all
KEEP = ["ticker", "setup", "side", "signal_time_ist", "quality_score", "rs_pct", "market_ret_pct",
        "regime", "vol_ratio", "atr_pct", "body_pct", "close_loc", "vwap_dist_atr"]


def main():
    files = sorted(glob.glob(str(POOL / "chunk_*" / "historical_all_available_raw_candidates.csv")))
    df = pd.concat([pd.read_csv(f, low_memory=False) for f in files], ignore_index=True, sort=False)
    df["sig"] = pd.to_datetime(df["signal_time_ist"], errors="coerce")
    df = df.dropna(subset=["sig"])
    df = df[df["side"].astype(str).str.upper().eq("SHORT") & df["setup"].astype(str).isin(PLAN)].copy()
    df["d"] = df["sig"].dt.strftime("%Y-%m-%d")
    out = []
    for s, (ctr, cte) in PLAN.items():
        e = df[df["setup"].astype(str) == s]
        tr = e[e["d"] <= "2026-04-30"]; te = e[e["d"] >= "2026-05-01"]
        if len(tr) > ctr:
            tr = tr.sample(ctr, random_state=7)
        if len(te) > cte:
            te = te.sample(cte, random_state=7)
        out.append(pd.concat([tr, te], ignore_index=True))
        print(f"  {s}: train={len(tr)} test={len(te)}")
    res = pd.concat(out, ignore_index=True)[[c for c in KEEP if c in df.columns]]
    res.to_csv(OUT, index=False)
    print(f"[short2_mine] wrote {len(res)} -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
