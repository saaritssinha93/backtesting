"""
new_setups_salvage_gen_v11.py — HONEST salvage candidate pools for the two N_ rejects, from the clean pool.

Why the originals failed and the honest fix (SAME acceptance bar, no goalpost move):
  * N_HIGH_RS_EMA_BOUNCE_LONG: rs_pct>=4.0 was pre-imposed at the ~99th pct (median D_EMA20_BOUNCE rs is
    0.77) -> only 12 candidates. FIX: emit the FULL D_EMA20_BOUNCE long pool and let the robustness-first
    search FIND the rs cut (+ body/quality/PM gate) with anti-overfit validation, instead of hand-fixing it.
      -> N_HIGH_RS_EMA_BOUNCE_LONG_SALV  (all D_EMA20_BOUNCE longs, ~663)
  * N_MORNING_ZERO_WICK_SHORT: 90% from E_ORB_BREAKOUT_SHORT (documented churn/cost sink) -> the apparent
    edge was churn. FIX: EXCLUDE E_ORB and test the morning-zero-wick idea on the NON-churn sources, relaxing
    the zero-wick tolerance only as far as needed for a >=8-trade test sample.
      -> N_MORNING_ZERO_WICK_SHORT_NOORB       (non-E_ORB, lower_wick<=0.05%, 10:01-11:30, ~112)
      -> N_MORNING_ZERO_WICK_SHORT_NOORB_WIDE  (non-E_ORB, lower_wick<=0.10%, 09:30-12:00, ~340)

Writes all salvage candidates (tier123/overlay schema) to new_setups_salvage_candidates.csv.
Run AFTER 15:30 IST. Usage: py -3.12 new_setups_salvage_gen_v11.py
"""
from __future__ import annotations
from pathlib import Path
import glob
import numpy as np
import pandas as pd

POOL = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_cleanpool")
OUT = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_new_setups_probe\new_setups_salvage_candidates.csv")
NON_ORB_SRC = {"S_BB_SQUEEZE_SHORT", "D_EMA20_REJECTION", "E_VWAP_BAND_FADE"}
CARRY = ["quality_score", "rs_pct", "market_ret_pct", "regime", "vol_ratio", "atr_pct",
         "body_pct", "close_loc", "vwap_dist_atr"]


def _load():
    files = sorted(glob.glob(str(POOL / "chunk_*" / "historical_all_available_raw_candidates.csv")))
    df = pd.concat([pd.read_csv(f, low_memory=False) for f in files], ignore_index=True, sort=False)
    df["signal_time_ist"] = pd.to_datetime(df["signal_time_ist"], errors="coerce")
    df = df.dropna(subset=["signal_time_ist", "ticker", "side", "setup"]).copy()
    df["minute"] = df["signal_time_ist"].dt.hour * 60 + df["signal_time_ist"].dt.minute
    for c in ("signal_open", "signal_close", "signal_low", "body_pct", "rs_pct", "quality_score"):
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    close = df["signal_close"].replace(0, np.nan)
    bb = pd.concat([df["signal_open"], df["signal_close"]], axis=1).min(axis=1)
    df["lower_wick_price_pct"] = (bb - df["signal_low"]) / close * 100.0
    return df


def _relabel(df, mask, setup, side, reason):
    out = df.loc[mask].copy()
    if out.empty:
        return out
    out["source_setup"] = out["setup"].astype(str)
    out["setup"] = setup
    out["side"] = side
    out["reason"] = reason
    out["_day"] = out["signal_time_ist"].dt.strftime("%Y-%m-%d")
    out["_score"] = pd.to_numeric(out.get("quality_score"), errors="coerce").fillna(0.0)
    out = (out.sort_values(["ticker", "_day", "_score"], ascending=[True, True, False])
              .drop_duplicates(["ticker", "_day"], keep="first")
              .drop(columns=["_day", "_score"]).reset_index(drop=True))
    keep = ["ticker", "setup", "side", "signal_time_ist", "source_setup", "reason"] + [c for c in CARRY if c in out.columns]
    return out[keep]


def _report(name, sub):
    if len(sub):
        d = sub["signal_time_ist"].dt.strftime("%Y-%m-%d")
        print(f"  {name}: n={len(sub)} train={int((d<='2026-04-30').sum())} test={int((d>='2026-05-01').sum())} "
              f"src={dict(sub['source_setup'].value_counts())}")
    else:
        print(f"  {name}: n=0")


def main():
    df = _load()
    isL = df["side"].astype(str).str.upper().eq("LONG")
    isS = df["side"].astype(str).str.upper().eq("SHORT")

    salv_long = _relabel(df, df["setup"].astype(str).eq("D_EMA20_BOUNCE") & isL,
                         "N_HIGH_RS_EMA_BOUNCE_LONG_SALV", "LONG", "d_ema20_bounce_full_pool_search_rs_gate")
    noorb = _relabel(df, df["setup"].astype(str).isin(NON_ORB_SRC) & isS & df["minute"].between(601, 690)
                     & (df["lower_wick_price_pct"] <= 0.05) & (df["quality_score"] <= 100.0),
                     "N_MORNING_ZERO_WICK_SHORT_NOORB", "SHORT", "morning_zero_wick_nonchurn_sources")
    noorb_wide = _relabel(df, df["setup"].astype(str).isin(NON_ORB_SRC) & isS & df["minute"].between(570, 720)
                          & (df["lower_wick_price_pct"] <= 0.10) & (df["quality_score"] <= 100.0),
                          "N_MORNING_ZERO_WICK_SHORT_NOORB_WIDE", "SHORT", "morning_zero_wick_nonchurn_wide")

    out = pd.concat([salv_long, noorb, noorb_wide], ignore_index=True, sort=False)
    out.to_csv(OUT, index=False)
    print(f"[salvage_gen] pool rows={len(df):,}")
    _report("N_HIGH_RS_EMA_BOUNCE_LONG_SALV", salv_long)
    _report("N_MORNING_ZERO_WICK_SHORT_NOORB", noorb)
    _report("N_MORNING_ZERO_WICK_SHORT_NOORB_WIDE", noorb_wide)
    print(f"[salvage_gen] wrote {len(out)} -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
