"""Phase-3: prove the live entry engine's pre-momentum FEATURE computation matches
v11's, so a candidate that passes the conf premom gate in v11 also passes live.

For a sample of real signals, compute the gate features two ways:
  v11    : avwap_5min_ID_v11_backtesting._pre_entry_momentum_features_v11
  engine : eqidv2_entry_engine_1min_v5_id._pre_entry_momentum_features
feeding both the SAME (ticker, side, entry_price, stop, entry_ts, signal_ts), and
compare the features the 16-setup conf gates actually use.
"""
from __future__ import annotations

import os
# Force the engine's indicator loader to use the historical feed (_eq_live2) for
# these past dates instead of picking the rolling _eq_live file by mtime.
os.environ["EQIDV2_DATA_5M_DIR"] = r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2"

import sys
import numpy as np
import pandas as pd

import avwap_5min_ID_v11_backtesting as v11
import eqidv2_entry_engine_1min_v5_id as eng

# Point the engine's indicator loaders at the SAME historical feed v11 reads
# (_eq_live2) so sig5_* (5m) resolves for these past dates. In live, DATA_5M_DIR
# already holds today's bars, so this is a test-harness alignment only.
from pathlib import Path
eng.DATA_5M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
eng.DATA_1MIN_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq_live2")

CLEANPOOL = r"C:\TradingData\eqidv2\outputs_ID_v11_cleanpool\chunk_202606\historical_all_available_raw_candidates.csv"
GATE_FEATS = ["pre2_mom_r", "pre5_mom_r", "pre10_mom_r", "pre3_range_r", "pre3_close_pos",
              "pre1_adx", "sig5_adx_calc", "sig5_rsi_dir", "pre_entry_momentum_score"]
# setups with a conf premom gate (features are decision-relevant)
GATED = ["D_EMA20_REJECTION", "C_OR_BREAKDOWN", "G_HIGHER_HIGH_BREAK", "A_MOD_BREAK_C1_LOW",
         "B_HUGE_RED_FAILED_BOUNCE", "G_LOWER_LOW_BREAK"]
N = 40
TOL = 1e-6


def main() -> int:
    d = pd.read_csv(CLEANPOOL, low_memory=False)
    d = d[d["setup"].astype(str).isin(GATED)].copy()
    d["signal_time_ist"] = pd.to_datetime(d["signal_time_ist"], errors="coerce")
    d = d.dropna(subset=["signal_time_ist"]).sample(min(N, len(d)), random_state=11)

    compared = 0
    feat_checks = 0
    feat_mismatch = 0
    both_nan = 0
    for _, r in d.iterrows():
        tk = str(r["ticker"]).upper()
        side = str(r["side"]).upper()
        sig_ts = pd.Timestamp(r["signal_time_ist"])
        entry_ts = (sig_ts + pd.Timedelta(minutes=1)).floor("min")
        entry_price = float(r.get("signal_close", np.nan))
        if not np.isfinite(entry_price) or entry_price <= 0:
            continue
        stop = entry_price * (0.99 if side == "LONG" else 1.01)  # identical risk for both

        fv, _ = v11._pre_entry_momentum_features_v11(tk, side, entry_price, stop, entry_ts, sig_ts)
        entry_row = {
            "ticker": tk, "side": side, "entry_price": entry_price, "sl_price": stop,
            "pre_momentum_cutoff_ist": eng._fmt_ist(entry_ts),
            "signal_time_ist": eng._fmt_ist(sig_ts),
        }
        # v11 loads 1m via _load_1m_with_open; hand the engine the same bars.
        bars = v11._load_1m_with_open(tk)
        raw_by = {}
        if bars is not None and not bars.empty:
            b = bars.reset_index().rename(columns={"index": "date"})
            if "date" not in b.columns:
                b = b.rename(columns={b.columns[0]: "date"})
            raw_by = {tk: b}
        fe, _ = eng._pre_entry_momentum_features(entry_row, raw_by)
        if not fv or not fe:
            continue
        compared += 1
        for k in GATE_FEATS:
            a, bb = fv.get(k), fe.get(k)
            an = (a is None) or (isinstance(a, float) and not np.isfinite(a))
            bn = (bb is None) or (isinstance(bb, float) and not np.isfinite(bb))
            if an and bn:
                both_nan += 1
                continue
            feat_checks += 1
            if an != bn or abs(float(a) - float(bb)) > TOL:
                feat_mismatch += 1
                if feat_mismatch <= 8:
                    print(f"  MISMATCH {tk} {side} {k}: v11={a} engine={bb}")

    print(f"\nsignals compared: {compared}")
    print(f"finite feature checks: {feat_checks} | mismatches: {feat_mismatch} | both-nan skips: {both_nan}")
    ok = compared >= 10 and feat_mismatch == 0
    print("RESULT:", "PASS (engine premom features == v11)" if ok else "REVIEW")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
