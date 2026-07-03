r"""mfe_mae_study.py — TRAIN-ONLY winner/loser + MFE/MAE study for A_MOD_BREAK_C1_LOW.

Measures what the trades ACTUALLY do on the 1-minute path (max favorable / max adverse
excursion to 15:20 IST) so SL/target can be set from measured movement instead of guesses.
Runs on the DEDUPED baseline TRAIN book only (TEST is never touched here — exit design
stays leak-free). Also compares winner vs loser feature distributions using the causal
enriched features.

Outputs (all in the campaign folder):
  mfe_mae_train.csv        per-trade: entry, MFE%, MAE%, t_MFE(min), eod_ret%, outcome@baseline
  winner_loser_stats.json  feature medians for winners vs losers + MFE/MAE percentiles
                           + suggested SL/TGT candidates

Run:  py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_BREAK_C1_LOW\scripts\mfe_mae_study.py
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
WORK = _HERE.parent
TT_DIR = WORK.parent.parent
REPO = TT_DIR.parent
ENGINE_DIR = TT_DIR / "setup_pf_1_4_approval_loop" / "_engine"
for _p in (str(REPO), str(TT_DIR), str(ENGINE_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt                       # noqa: E402
import avwap_5min_ID_v11_backtesting as v11         # noqa: E402
import pf_band_fitval_loop as eng                   # noqa: E402

SETUP = "A_MOD_BREAK_C1_LOW"
TRAIN = ("2026-03-01", "2026-05-30")
ENRICHED_CSV = TT_DIR / "setup_pf_1_4_full_loop" / SETUP / "pools" / SETUP / "enriched_features.csv"
FEATS = ["rsi", "adx5", "ema20_dist_atr", "ema_stack_atr", "macd_hist_atr", "bb_pos",
         "stoch_k", "mfi14", "obv_slope6", "vol_z", "sess_vwap_dist_atr", "below_vwap_streak6",
         "day_pos", "day_low_dist_atr", "bars_since_day_low", "gap_pct", "day_ret_pct",
         "c1_break_depth_atr", "ret6_atr", "red_streak", "body_sum6_atr", "range6_atr",
         "range_expansion", "vol_ratio", "atr_pct", "close_loc", "quality_score"]


def main() -> int:
    tt.POOL_DIRS = [WORK / "pools" / SETUP]
    tt.POOL_DIR = tt.POOL_DIRS[0]
    pool = tt.load_pool()
    pool = pool[pool["setup"] == SETUP].copy()
    d = pool["_day"]
    tr = pool[(d >= pd.Timestamp(TRAIN[0])) & (d <= pd.Timestamp(TRAIN[1]))].copy()
    print(f"[mfe] TRAIN rows {len(tr)}")
    tt.SLIPPAGE_BPS = 15.0
    tr = tt.attach_entries(tr)
    book = tt.dedupe_family(tr)          # 1 per (slot,ticker) then 1 per ticker/day
    print(f"[mfe] deduped TRAIN book {len(book)}")

    recs = []
    t0 = time.time()
    for j, r in enumerate(book.itertuples(), 1):
        bars = v11._load_1m_with_open(r.ticker)
        if bars is None or bars.empty:
            continue
        ets = pd.Timestamp(r.tt_entry_iso)
        day_end = ets.normalize() + pd.Timedelta(hours=15, minutes=20)
        path = bars.loc[(bars.index >= ets) & (bars.index <= day_end)]
        if path.empty:
            continue
        fill = float(r.tt_fill)
        lows = path["low"].to_numpy(float)
        highs = path["high"].to_numpy(float)
        closes = path["close"].to_numpy(float)
        # SHORT: favorable = down move, adverse = up move
        mfe = (fill - np.minimum.accumulate(lows).min()) / fill * 100.0
        mae = (np.maximum.accumulate(highs).max() - fill) / fill * 100.0
        i_mfe = int(np.argmin(lows))
        i_mae = int(np.argmax(highs))
        # net at baseline exits for the winner/loser label
        net = tt._resolve_net(r.ticker, r.side, r.tt_entry_iso, fill, int(r.tt_qty), 1.10, 1.00)
        full = tt._resolve_full(r.ticker, r.side, r.tt_entry_iso, fill, int(r.tt_qty), 1.10, 1.00)
        recs.append({
            "ticker": r.ticker, "day": str(pd.Timestamp(r._day).date() if hasattr(r, "_day") else r.tt_sig_ts.date()),
            "sig": r.tt_sig_ts.isoformat(), "entry": fill,
            "mfe_pct": round(float(mfe), 3), "mae_pct": round(float(mae), 3),
            "t_mfe_min": i_mfe, "t_mae_min": i_mae,
            "mfe_first": bool(i_mfe < i_mae),
            "eod_ret_pct": round(float((fill - closes[-1]) / fill * 100.0), 3),
            "net_baseline": (None if net is None else round(float(net), 1)),
            "outcome_baseline": (full[1] if full else None),
        })
        if j % 1000 == 0:
            print(f"[mfe] {j}/{len(book)} ({time.time()-t0:.0f}s)", flush=True)
    det = pd.DataFrame(recs)
    det.to_csv(WORK / "mfe_mae_train.csv", index=False)

    # ---- winner/loser feature comparison -----------------------------------------
    enr = pd.read_csv(ENRICHED_CSV, low_memory=False)
    enr["_sig"] = pd.to_datetime(enr["_sig"])
    book2 = book.copy()
    book2["_signaive"] = book2["tt_sig_ts"].dt.tz_localize(None)
    det["_signaive"] = pd.to_datetime(det["sig"]).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    m = det.merge(book2[["ticker", "_signaive", "quality_score", "vol_ratio", "atr_pct", "close_loc"]],
                  on=["ticker", "_signaive"], how="left")
    m = m.merge(enr.rename(columns={"_sig": "_signaive"}), on=["ticker", "_signaive"], how="left",
                suffixes=("", "_e"))
    m["is_winner"] = pd.to_numeric(m["net_baseline"], errors="coerce") > 0
    stats = {"n": len(m), "n_winners": int(m["is_winner"].sum()),
             "win_rate": round(float(m["is_winner"].mean()) * 100, 1)}
    fcomp = {}
    for f in FEATS:
        if f in m.columns:
            x = pd.to_numeric(m[f], errors="coerce")
            w, l = x[m["is_winner"]], x[~m["is_winner"]]
            if w.notna().sum() > 20 and l.notna().sum() > 20:
                fcomp[f] = {"winner_med": round(float(w.median()), 4),
                            "loser_med": round(float(l.median()), 4),
                            "winner_p25": round(float(w.quantile(.25)), 4),
                            "winner_p75": round(float(w.quantile(.75)), 4)}
    stats["feature_medians"] = fcomp
    for lbl, g in (("winners", m[m["is_winner"]]), ("losers", m[~m["is_winner"]]), ("all", m)):
        stats[f"mfe_{lbl}"] = {p: round(float(g["mfe_pct"].quantile(p / 100)), 3) for p in (25, 40, 50, 60, 75, 90)}
        stats[f"mae_{lbl}"] = {p: round(float(g["mae_pct"].quantile(p / 100)), 3) for p in (25, 40, 50, 60, 75, 90)}
    stats["mfe_first_share"] = round(float(m["mfe_first"].mean()) * 100, 1)
    stats["median_eod_ret"] = round(float(m["eod_ret_pct"].median()), 3)
    # exit suggestions: target must be commonly reachable; SL beyond typical adverse noise
    stats["suggested_exits"] = {
        "tgt_candidates_pct": [stats["mfe_all"][40], stats["mfe_all"][50], stats["mfe_all"][60]],
        "sl_candidates_pct": [stats["mae_all"][50], stats["mae_all"][60], stats["mae_all"][75]],
        "note": "TRAIN-only percentiles of 1-min MFE/MAE from entry to 15:20 (SHORT).",
    }
    (WORK / "winner_loser_stats.json").write_text(json.dumps(stats, indent=2), encoding="utf-8")
    print(f"[mfe] n={stats['n']} win%={stats['win_rate']} mfe_first%={stats['mfe_first_share']} "
          f"med_eod={stats['median_eod_ret']}")
    print(f"[mfe] MFE all p40/50/60: {stats['mfe_all'][40]}/{stats['mfe_all'][50]}/{stats['mfe_all'][60]} | "
          f"MAE all p50/60/75: {stats['mae_all'][50]}/{stats['mae_all'][60]}/{stats['mae_all'][75]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
