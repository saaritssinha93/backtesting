"""
validate_B_setups_filters.py
============================

Data-grounded diagnosis + filter-experiment harness for the B* setup family
(B_AVWAP_RECLAIM_REVERSAL, B_HUGE_C1_CLOSE_RECLAIM_BREAK, B_HUGE_RED_FAILED_BOUNCE),
Nov 2025 -> now, across the full ~1200-ticker universe.

It does NOT optimise thresholds blindly. It:
  1. Loads the actual B* candidate pool (clean consistent pool, post v8-gate/overlay,
     pre-dedupe -> the population the strategy considers).
  2. Resolves every candidate at the FIXED production exit (SL 0.70 / Tgt 1.50) on
     1-minute data, NET of statutory NSE cost.
  3. Computes MAE/MFE (in R and %) + immediate-fail vs slow-fade.
  4. Splits TRAIN (2025-11-01..2026-04-30) / TEST (2026-05-01..2026-06-10).
  5. Bucket analysis of winners vs losers (time, regime, rs_pct, vwap_dist_atr,
     vol_ratio, atr_pct, close_loc, body_pct).
  6. Applies candidate filters one-by-one, before/after TRAIN+TEST PF / count /
     win-rate / avg-win / avg-loss / net.

No lookahead: entry = first 1-min open strictly after the 5-min signal; exits walk
forward only. Outputs CSVs alongside the proposals dir.

Run:  py -3.12 validate_B_setups_filters.py
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

import setup_train_test as stt
import avwap_5min_ID_v11_backtesting as v11
import walkforward_gate as wfg
from nse_intraday_costs import CostConfig

B_SETUPS = ["B_AVWAP_RECLAIM_REVERSAL", "B_HUGE_C1_CLOSE_RECLAIM_BREAK", "B_HUGE_RED_FAILED_BOUNCE"]
FIXED_EXIT = {s: (0.70, 1.50) for s in B_SETUPS}        # production default SETUP_EXIT_RULES
CLEAN_POOL = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_cleanpool")
OUT_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_traintest_pool\proposals")
TRAIN = ("2025-11-01", "2026-04-30")
TEST = ("2026-05-01", "2026-06-10")
CFG = CostConfig()


# ---------------------------------------------------------------------------
# 1. Build the B* trade dataset (entry + fixed-exit resolution + MAE/MFE)
# ---------------------------------------------------------------------------
def _mae_mfe(bars1, side, entry_px, entry_ts, exit_ts):
    if bars1 is None or bars1.empty:
        return np.nan, np.nan
    sub = bars1[(bars1.index >= entry_ts) & (bars1.index <= exit_ts)]
    if sub.empty:
        return np.nan, np.nan
    hi, lo = float(sub["high"].max()), float(sub["low"].min())
    if side == "LONG":
        mfe = (hi - entry_px) / entry_px * 100.0
        mae = (lo - entry_px) / entry_px * 100.0
    else:
        mfe = (entry_px - lo) / entry_px * 100.0
        mae = (entry_px - hi) / entry_px * 100.0
    return mfe, mae


def build_trades() -> pd.DataFrame:
    stt.POOL_DIRS = [CLEAN_POOL]
    pool = stt.load_pool()
    b = pool[pool["setup"].isin(B_SETUPS)].copy()
    b = stt.attach_entries(b)
    print(f"[validate_B] B* candidates with 1m entry: {len(b)}")
    recs = []
    for r in b.itertuples():
        sl, tgt = FIXED_EXIT[r.setup]
        bars1 = v11._load_1m_with_open(r.ticker)
        res = None
        if bars1 is not None and not bars1.empty:
            res = v11.er.resolve(bars=bars1, side=r.side, entry_price=float(r.tt_fill),
                                 entry_time_ist=pd.Timestamp(r.tt_entry_iso), sl_pct=sl, tgt_pct=tgt)
        if res is None:
            continue
        net = float(wfg.net_pnl_vectorized(np.array([r.tt_fill]), np.array([res.exit_price]),
                                           np.array([r.tt_qty]), np.array([r.side]), CFG)[0])
        gross = ((res.exit_price - r.tt_fill) if r.side == "LONG" else (r.tt_fill - res.exit_price)) * r.tt_qty
        mfe, mae = _mae_mfe(bars1, r.side, float(r.tt_fill), pd.Timestamp(r.tt_entry_iso), res.exit_time_ist)
        risk_pct = sl
        d = r.tt_sig_ts
        recs.append({
            "setup": r.setup, "side": r.side, "ticker": r.ticker,
            "date": d.date().isoformat(), "signal_minute": int(d.hour * 60 + d.minute),
            "regime": getattr(r, "regime", ""),
            "rs_pct": float(getattr(r, "rs_pct", np.nan)),
            "market_ret_pct": float(getattr(r, "market_ret_pct", np.nan)),
            "vol_ratio": float(getattr(r, "vol_ratio", np.nan)),
            "atr_pct": float(getattr(r, "atr_pct", np.nan)),
            "close_loc": float(getattr(r, "close_loc", np.nan)),
            "body_pct": float(getattr(r, "body_pct", np.nan)),
            "vwap_dist_atr": float(getattr(r, "vwap_dist_atr", np.nan)),
            "quality_score": float(getattr(r, "quality_score", np.nan)),
            "entry_price": round(float(r.tt_fill), 2), "exit_price": round(float(res.exit_price), 2),
            "outcome": res.outcome, "bars_held": int(res.bars_held),
            "gross_pnl_rs": round(float(gross), 2), "net_pnl_rs": round(net, 2),
            "mfe_pct": round(mfe, 3), "mae_pct": round(mae, 3),
            "mfe_R": round(mfe / risk_pct, 2) if risk_pct else np.nan,
            "mae_R": round(mae / risk_pct, 2) if risk_pct else np.nan,
        })
    df = pd.DataFrame(recs)
    df["period"] = np.where(df["date"] <= TRAIN[1], "TRAIN", np.where(df["date"] >= TEST[0], "TEST", "OTHER"))
    df["win"] = df["net_pnl_rs"] > 0
    df["immediate_fail"] = (df["outcome"] == "SL") & (df["bars_held"] <= 5)
    df["moved_favourable_first"] = df["mfe_R"] >= 0.5
    return df


# ---------------------------------------------------------------------------
# 2. Bucket analysis
# ---------------------------------------------------------------------------
def _pf(net):
    net = np.asarray(net, float)
    g = net[net > 0].sum()
    l = -net[net < 0].sum()
    return float(g / l) if l > 0 else (float("inf") if g > 0 else 0.0)


BUCKETS = {
    "time_of_day": lambda d: pd.cut(d["signal_minute"], [0, 600, 660, 720, 780, 900],
                                    labels=["<10:00", "10:00-11:00", "11:00-12:00", "12:00-13:00", ">13:00"]),
    "regime": lambda d: d["regime"].astype(str),
    "rs_pct": lambda d: pd.cut(d["rs_pct"], [-100, -1, 0, 0.5, 1, 2, 100],
                               labels=["<-1", "-1..0", "0..0.5", "0.5..1", "1..2", ">2"]),
    "vwap_dist_atr": lambda d: pd.cut(d["vwap_dist_atr"], [-1e9, -1, 0, 0.6, 1.5, 3, 1e9],
                                      labels=["<-1", "-1..0", "0..0.6", "0.6..1.5", "1.5..3", ">3"]),
    "vol_ratio": lambda d: pd.cut(d["vol_ratio"], [0, 1.3, 1.6, 2.0, 3.0, 1e9],
                                  labels=["<1.3", "1.3..1.6", "1.6..2", "2..3", ">3"]),
    "atr_pct": lambda d: pd.cut(d["atr_pct"], [0, 0.004, 0.006, 0.008, 0.012, 1],
                                labels=["<0.4%", "0.4-0.6%", "0.6-0.8%", "0.8-1.2%", ">1.2%"]),
    "close_loc": lambda d: pd.cut(d["close_loc"], [0, 0.2, 0.4, 0.6, 0.8, 1.01],
                                  labels=["0-0.2", "0.2-0.4", "0.4-0.6", "0.6-0.8", "0.8-1.0"]),
    "body_pct": lambda d: pd.cut(d["body_pct"], [0, 0.3, 0.5, 0.7, 1.01],
                                 labels=["<0.3", "0.3-0.5", "0.5-0.7", ">0.7"]),
}


def bucket_table(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for setup, g0 in df.groupby("setup"):
        for bname, fn in BUCKETS.items():
            g = g0.copy()
            try:
                g["_b"] = fn(g)
            except Exception:
                continue
            for bval, gg in g.groupby("_b", observed=True):
                if len(gg) == 0:
                    continue
                rows.append({
                    "setup": setup, "bucket_dim": bname, "bucket": str(bval),
                    "n": len(gg), "win_pct": round(gg["win"].mean() * 100, 1),
                    "net_pf": round(_pf(gg["net_pnl_rs"]), 2),
                    "net_rs": round(gg["net_pnl_rs"].sum(), 0),
                    "avg_mfe_R": round(gg["mfe_R"].mean(), 2), "avg_mae_R": round(gg["mae_R"].mean(), 2),
                    "immediate_fail_pct": round(gg["immediate_fail"].mean() * 100, 1),
                })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 3. Candidate filter experiments
# ---------------------------------------------------------------------------
# (setup, experiment_id, predicate) — predicate operates on a row dict.
CANDIDATE_FILTERS = {
    "B_AVWAP_RECLAIM_REVERSAL": [
        ("BAR-C1", "reclaim_near_vwap", lambda r: -0.20 <= r["vwap_dist_atr"] <= 0.75),
        ("BAR-C2", "rs_positive", lambda r: r["rs_pct"] > 0.10),
        ("BAR-B1", "near_vwap_and_rs", lambda r: (-0.20 <= r["vwap_dist_atr"] <= 0.75) and r["rs_pct"] > 0.0),
        ("BAR-B2", "avoid_open_15m", lambda r: r["signal_minute"] >= 570 + 20),
        ("BAR-A1", "bull_or_trend", lambda r: str(r["regime"]) in ("BULL", "TREND")),
    ],
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK": [
        ("BHC-C1", "atr_band", lambda r: r["atr_pct"] <= 0.009),
        ("BHC-C2", "rs_positive", lambda r: r["rs_pct"] > 0.0),
        ("BHC-B1", "not_extended", lambda r: r["vwap_dist_atr"] <= 3.0),
        ("BHC-B2", "vol_strong", lambda r: r["vol_ratio"] >= 1.6),
        ("BHC-A1", "morning_only", lambda r: r["signal_minute"] <= 720),
    ],
    "B_HUGE_RED_FAILED_BOUNCE": [
        ("BHR-C1", "rs_negative", lambda r: r["rs_pct"] < -0.30),
        ("BHR-C2", "below_vwap_dist", lambda r: r["vwap_dist_atr"] <= -0.50),
        ("BHR-B1", "weak_close_and_rs", lambda r: r["close_loc"] <= 0.30 and r["rs_pct"] < -0.20),
        ("BHR-B2", "regime_bear_trend", lambda r: str(r["regime"]) in ("BEAR", "TREND")),
        ("BHR-A1", "market_falling", lambda r: r["market_ret_pct"] <= -0.20),
    ],
}


def _stats(g):
    net = g["net_pnl_rs"].to_numpy()
    wins = net[net > 0]
    losses = net[net < 0]
    return dict(n=len(g), win_pct=round((net > 0).mean() * 100, 1) if len(g) else 0,
                net_pf=round(_pf(net), 2), net_rs=round(float(net.sum()), 0),
                avg_win=round(float(wins.mean()), 0) if len(wins) else 0,
                avg_loss=round(float(losses.mean()), 0) if len(losses) else 0)


def filter_experiments(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for setup, exps in CANDIDATE_FILTERS.items():
        base = df[df["setup"] == setup]
        for period in ("TRAIN", "TEST"):
            bp = base[base["period"] == period]
            b = _stats(bp)
            rows.append({"setup": setup, "experiment_id": "BASE", "filter": "none", "period": period, **b})
            for eid, name, pred in exps:
                keep = bp[bp.apply(lambda r: bool(pred(r)) if pd.notna(r["vwap_dist_atr"]) else False, axis=1)]
                rows.append({"setup": setup, "experiment_id": eid, "filter": name, "period": period, **_stats(keep)})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = build_trades()
    if df.empty:
        raise SystemExit("[validate_B] no B* trades built — check clean pool")
    df.to_csv(OUT_DIR / "B_setups_trades_nov_to_now.csv", index=False)

    print("\n=== B* per-setup, per-period (fixed exit 0.70/1.50, NET) ===")
    for (setup, period), g in df.groupby(["setup", "period"]):
        if period == "OTHER":
            continue
        print(f"  {setup:<32} {period:<6} n={len(g):>3} win={g['win'].mean()*100:4.1f}% "
              f"PF={_pf(g['net_pnl_rs']):4.2f} net=Rs {g['net_pnl_rs'].sum():>8,.0f} "
              f"mfeR={g['mfe_R'].mean():4.2f} maeR={g['mae_R'].mean():5.2f} immFail={g['immediate_fail'].mean()*100:4.1f}%")

    bt = bucket_table(df)
    bt.to_csv(OUT_DIR / "B_setups_bucket_analysis.csv", index=False)
    fx = filter_experiments(df)
    fx.to_csv(OUT_DIR / "B_setups_filter_experiment_results.csv", index=False)

    print("\n=== filter experiments (TRAIN -> TEST PF / n) ===")
    for setup in B_SETUPS:
        print(f"  {setup}")
        sub = fx[fx["setup"] == setup]
        for eid in ["BASE"] + [e[0] for e in CANDIDATE_FILTERS[setup]]:
            tr = sub[(sub["experiment_id"] == eid) & (sub["period"] == "TRAIN")]
            te = sub[(sub["experiment_id"] == eid) & (sub["period"] == "TEST")]
            if tr.empty or te.empty:
                continue
            tr, te = tr.iloc[0], te.iloc[0]
            print(f"     {eid:<10} {te['filter'][:22]:<22} TRAIN n={tr['n']:>3} PF={tr['net_pf']:>4} | "
                  f"TEST n={te['n']:>3} PF={te['net_pf']:>4} win={te['win_pct']:>4}%")

    print(f"\n[validate_B] wrote: B_setups_trades_nov_to_now.csv, B_setups_bucket_analysis.csv, "
          f"B_setups_filter_experiment_results.csv  -> {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
