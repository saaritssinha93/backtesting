r"""mfe_mae_study.py — 1-minute MFE/MAE study for the redesigned pool (research-only).

For the TRAIN book (post family-dedupe, baseline exit irrelevant here), walks the 1-min
bars from entry fill to 15:20 IST and records:
  * MFE%  — max favorable excursion (high vs fill, LONG)
  * MAE%  — max adverse excursion (low vs fill)
  * MAE-before-MFE% — worst adverse excursion BEFORE the bar of maximum favorable
    excursion (what a stop must survive to collect the move)
  * minutes-to-MFE
Grouped overall / by regime / by hour, plus a bracket table: for each candidate SL, the
share of trades whose MAE-before-MFE stays inside it AND whose MFE reaches each target.

Usage: py -3.12 mfe_mae_study.py [--pool <dir>] [--sample 4000]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
TT_DIR = HERE.parents[3]
REPO = TT_DIR.parent
for p in (str(REPO), str(TT_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

import setup_train_test as tt  # noqa: E402
import avwap_5min_ID_v11_backtesting as v11  # noqa: E402

SETUP = "A_MOD_CLOSE_CONTINUATION_BREAK"
TRAIN_START = pd.Timestamp("2026-03-01")
TRAIN_END = pd.Timestamp("2026-05-30")
EOD = "15:20"

SL_CAND = [0.4, 0.5, 0.6, 0.7, 0.85, 1.0, 1.2]
TGT_CAND = [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5]


def excursions(ticker, entry_iso, fill):
    bars = v11._load_1m_with_open(ticker)
    if bars is None or bars.empty:
        return None
    et = pd.Timestamp(entry_iso)
    eod = et.normalize() + pd.Timedelta(hours=15, minutes=20)
    w = bars[(bars.index >= et) & (bars.index <= eod)]
    if len(w) < 3:
        return None
    hi = w["high"].to_numpy(float)
    lo = w["low"].to_numpy(float)
    mfe_path = (hi / fill - 1.0) * 100.0
    mae_path = (lo / fill - 1.0) * 100.0
    i_mfe = int(np.argmax(mfe_path))
    mfe = float(mfe_path[i_mfe])
    mae = float(mae_path.min())
    mae_before = float(mae_path[:i_mfe + 1].min()) if i_mfe >= 0 else mae
    return {"mfe": mfe, "mae": mae, "mae_before_mfe": mae_before,
            "min_to_mfe": float(i_mfe), "close_ret": float(w["close"].iloc[-1] / fill - 1) * 100}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=str(WORK / "pools" / "pool_enriched"))
    ap.add_argument("--sample", type=int, default=4000)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    tt.POOL_DIRS = [str(Path(args.pool).resolve())]
    tt.SLIPPAGE_BPS = 15.0
    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).eq(SETUP)]
    train = pool[(pool["_day"] >= TRAIN_START) & (pool["_day"] <= TRAIN_END)].reset_index(drop=True)
    train = tt.attach_entries(train)
    book = tt.dedupe_family(train)          # one per ticker/day, best per slot — the tradeable book
    print(f"[mfe] TRAIN book rows {len(book)}", flush=True)
    if len(book) > args.sample:
        book = book.sample(n=args.sample, random_state=args.seed).reset_index(drop=True)

    recs = []
    for j, r in enumerate(book.itertuples(), 1):
        e = excursions(r.ticker, r.tt_entry_iso, float(r.tt_fill))
        if e is None:
            continue
        e.update({"regime": str(getattr(r, "regime", "?")).upper(),
                  "hour": pd.Timestamp(r.tt_entry_iso).hour,
                  "ticker": r.ticker, "day": str(r._asdict().get('_day', ''))[:10]})
        recs.append(e)
        if j % 1000 == 0:
            print(f"[mfe] {j}/{len(book)}", flush=True)
    df = pd.DataFrame(recs)
    df.to_csv(WORK / "mfe_mae_train.csv", index=False)

    def qtable(x):
        return {f"q{int(q*100)}": round(float(x.quantile(q)), 3) for q in (0.1, 0.25, 0.5, 0.75, 0.9)}

    out = {"n": len(df),
           "mfe_pct": qtable(df["mfe"]), "mae_pct": qtable(df["mae"]),
           "mae_before_mfe_pct": qtable(df["mae_before_mfe"]),
           "minutes_to_mfe": qtable(df["min_to_mfe"]),
           "close_ret_pct": qtable(df["close_ret"]),
           "by_regime": {}, "by_hour": {}, "bracket_feasibility": {}}
    for rg, g in df.groupby("regime"):
        if len(g) >= 30:
            out["by_regime"][rg] = {"n": len(g), "mfe_med": round(float(g["mfe"].median()), 3),
                                    "mae_med": round(float(g["mae"].median()), 3),
                                    "maebm_med": round(float(g["mae_before_mfe"].median()), 3),
                                    "close_ret_med": round(float(g["close_ret"].median()), 3)}
    for hh, g in df.groupby("hour"):
        if len(g) >= 30:
            out["by_hour"][int(hh)] = {"n": len(g), "mfe_med": round(float(g["mfe"].median()), 3),
                                       "mae_med": round(float(g["mae"].median()), 3)}
    # bracket feasibility: P(MAE_before_MFE > -SL and MFE >= TGT) — the win-rate ceiling
    for sl in SL_CAND:
        row = {}
        for tg in TGT_CAND:
            ok = ((df["mae_before_mfe"] > -sl) & (df["mfe"] >= tg)).mean()
            # required win rate for PF=1.3 at this RR with ~0.10% round-trip cost drag
            cost = 0.10
            win_pnl, loss_pnl = tg - cost, sl + cost
            wr_needed = 1.3 * loss_pnl / (win_pnl + 1.3 * loss_pnl) if win_pnl > 0 else 1.0
            row[f"T{tg}"] = {"hit": round(float(ok) * 100, 1), "wr_needed_pf1.3": round(wr_needed * 100, 1)}
        out["bracket_feasibility"][f"SL{sl}"] = row
    (WORK / "mfe_mae_study.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps({k: out[k] for k in ("n", "mfe_pct", "mae_before_mfe_pct")}, indent=1))
    print(f"[mfe] wrote {WORK / 'mfe_mae_study.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
