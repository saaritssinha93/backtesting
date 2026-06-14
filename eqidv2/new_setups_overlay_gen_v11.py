"""
new_setups_overlay_gen_v11.py — generate the TWO N_ OVERLAY setups' candidates from the CLEAN POOL
so they can be run through the SAME aggressive-iteration + anti-overfit search as the L/S structural
setups (new_setups_search_v11.py), on the SAME train(Nov-Apr)/test(May-Jun) split.

The N_ setups are RELABELS of existing-setup candidates (definitions frozen from
research_v11_new_overlay_forward_test.py / V11_NEW_SETUP_RESEARCH_2026-06-12.md):
  - N_HIGH_RS_EMA_BOUNCE_LONG  = D_EMA20_BOUNCE LONG  & body_pct>=0.60 & rs_pct>=4.0
  - N_MORNING_ZERO_WICK_SHORT  = {S_BB_SQUEEZE_SHORT,E_ORB_BREAKOUT_SHORT,D_EMA20_REJECTION,
                                   E_VWAP_BAND_FADE} SHORT & 10:01-11:30 & lower_wick<=0.01% & qs<=100
One candidate per ticker per day (keep highest quality_score). Source = the clean-pool RAW candidate
CSVs (raw scan, pre-gate — matches L/S being raw structural candidates and the original N_ source).

Output schema = the existing candidate schema the search's build_paths() consumes (ticker, setup, side,
signal_time_ist, regime + POOL_FEATS that are present). The enriched indicator columns (rsi, macd, ...)
that the L/S scanner adds are absent here -> the search filters them out gracefully (notna<0.6).

Run AFTER 15:30 IST. Usage: py -3.12 new_setups_overlay_gen_v11.py
"""
from __future__ import annotations
from pathlib import Path
import glob
import numpy as np
import pandas as pd

POOL = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_cleanpool")
OUT_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_new_setups_probe")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUT_DIR / "new_overlay_candidates_traintest.csv"

LONG_SETUP = "N_HIGH_RS_EMA_BOUNCE_LONG"
SHORT_SETUP = "N_MORNING_ZERO_WICK_SHORT"
SHORT_SOURCE_SETUPS = {"S_BB_SQUEEZE_SHORT", "E_ORB_BREAKOUT_SHORT", "D_EMA20_REJECTION", "E_VWAP_BAND_FADE"}

# carry-through feature columns the search mines as POOL_FEATS (present in raw_candidates schema)
CARRY = ["quality_score", "rs_pct", "market_ret_pct", "regime", "vol_ratio", "atr_pct",
         "body_pct", "close_loc", "vwap_dist_atr", "signal_open", "signal_high", "signal_low",
         "signal_close", "signal_volume"]


def _load_pool() -> pd.DataFrame:
    files = sorted(glob.glob(str(POOL / "chunk_*" / "historical_all_available_raw_candidates.csv")))
    if not files:
        raise SystemExit(f"!! no raw_candidates under {POOL}")
    df = pd.concat([pd.read_csv(f, low_memory=False) for f in files], ignore_index=True, sort=False)
    df["signal_time_ist"] = pd.to_datetime(df["signal_time_ist"], errors="coerce")
    df = df.dropna(subset=["signal_time_ist", "ticker", "side", "setup"]).copy()
    df["signal_minute"] = df["signal_time_ist"].dt.hour * 60 + df["signal_time_ist"].dt.minute
    for c in ("signal_open", "signal_close", "signal_low", "body_pct", "rs_pct", "quality_score"):
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    close = df["signal_close"].replace(0, np.nan)
    body_bottom = pd.concat([df["signal_open"], df["signal_close"]], axis=1).min(axis=1)
    df["lower_wick_price_pct"] = (body_bottom - df["signal_low"]) / close * 100.0
    return df


def _relabel(df: pd.DataFrame, mask: pd.Series, setup: str, side: str, reason: str) -> pd.DataFrame:
    out = df.loc[mask].copy()
    if out.empty:
        return out
    out["source_setup"] = out["setup"].astype(str)
    out["setup"] = setup
    out["side"] = side
    out["reason"] = reason
    out["candidate_family"] = "new_overlay_forward_test"
    out["selection_mode"] = "new_overlay_forward_test"
    out["_day"] = out["signal_time_ist"].dt.strftime("%Y-%m-%d")
    out["_score"] = pd.to_numeric(out.get("quality_score"), errors="coerce").fillna(0.0)
    out = (out.sort_values(["ticker", "_day", "_score"], ascending=[True, True, False])
              .drop_duplicates(["ticker", "_day"], keep="first")
              .drop(columns=["_day", "_score"]).reset_index(drop=True))
    keep = ["ticker", "setup", "side", "signal_time_ist", "source_setup", "reason",
            "candidate_family", "selection_mode"] + [c for c in CARRY if c in out.columns]
    return out[keep]


def main() -> int:
    df = _load_pool()
    long_mask = (df["setup"].astype(str).eq("D_EMA20_BOUNCE")
                 & df["side"].astype(str).str.upper().eq("LONG")
                 & (df["body_pct"] >= 0.60) & (df["rs_pct"] >= 4.0))
    short_mask = (df["setup"].astype(str).isin(SHORT_SOURCE_SETUPS)
                  & df["side"].astype(str).str.upper().eq("SHORT")
                  & df["signal_minute"].between(601, 690)
                  & (df["lower_wick_price_pct"] <= 0.01) & (df["quality_score"] <= 100.0))
    longs = _relabel(df, long_mask, LONG_SETUP, "LONG",
                     "high_relative_strength_ema20_bounce_with_directional_body")
    shorts = _relabel(df, short_mask, SHORT_SETUP, "SHORT",
                      "morning_bearish_signal_closes_at_low_without_lower_wick")
    out = pd.concat([longs, shorts], ignore_index=True, sort=False)
    out.to_csv(OUT_CSV, index=False)
    print(f"[overlay_gen] pool rows={len(df):,}")
    for s, sub in (("N_HIGH_RS_EMA_BOUNCE_LONG", longs), ("N_MORNING_ZERO_WICK_SHORT", shorts)):
        if len(sub):
            d = sub["signal_time_ist"].dt.strftime("%Y-%m-%d")
            tr = int((d <= "2026-04-30").sum()); te = int((d >= "2026-05-01").sum())
            src = dict(sub["source_setup"].value_counts())
            print(f"  {s}: n={len(sub)} train={tr} test={te} src={src}")
        else:
            print(f"  {s}: n=0")
    print(f"[overlay_gen] wrote {len(out)} -> {OUT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
