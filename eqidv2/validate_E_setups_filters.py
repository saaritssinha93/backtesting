"""
validate_E_setups_filters.py — deep diagnosis of the tradeable E* setups, Nov->now, ~1200 tickers,
pre-momentum ON/OFF + bucket analysis. Only 4 E setups have a population (the other 11 E setups
are EARLY-mode/blocked -> zero candidates):
  E_ORB_BREAKOUT_LONG   (LONG)  exit 0.80/1.20  PM: pre15_vol_ratio20<=1.083 & pre1_adx>=42.31
  E_ORB_BREAKOUT_SHORT  (SHORT) exit 0.80/1.20  PM: pre10_dir_count>=5 & pre5_vol_ratio20>=1.656  (prod exit override 0.80/1.50)
  E_VWAP_BAND_FADE      (SHORT) exit 0.70/0.60  PM: none
  E_VWAP_LOSE_EARLY_SHORT(SHORT) exit 0.70/1.00 PM: sig5_vol_ratio20>=1.564 & pre3_body_sum_r<=0.797 ; entry guard >=09:45
All from the admitted pre-dedupe clean pool; E_ORB_BREAKOUT_SHORT sampled to 1500 to bound compute.
Fixed exits, NET of cost, no lookahead.  Run:  py -3.12 validate_E_setups_filters.py
"""
from __future__ import annotations
from pathlib import Path
import numpy as np
import pandas as pd

import setup_train_test as stt
import avwap_5min_ID_v11_backtesting as v11
import walkforward_gate as wfg
from nse_intraday_costs import CostConfig

CLEAN = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_cleanpool")
PROP = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_traintest_pool\proposals")
TRAIN_END, TEST_START = "2026-04-30", "2026-05-01"
CFG = CostConfig()
E_SETUPS = ["E_ORB_BREAKOUT_LONG", "E_ORB_BREAKOUT_SHORT", "E_VWAP_BAND_FADE", "E_VWAP_LOSE_EARLY_SHORT"]
FIXED_EXIT = {"E_ORB_BREAKOUT_LONG": (0.80, 1.20), "E_ORB_BREAKOUT_SHORT": (0.80, 1.20),
              "E_VWAP_BAND_FADE": (0.70, 0.60), "E_VWAP_LOSE_EARLY_SHORT": (0.70, 1.00)}
PM_TERMS = {
    "E_ORB_BREAKOUT_LONG": (("pre15_vol_ratio20", "<=", 1.08301), ("pre1_adx", ">=", 42.3138)),
    "E_ORB_BREAKOUT_SHORT": (("pre10_dir_count", ">=", 5.0), ("pre5_vol_ratio20", ">=", 1.65561)),
    "E_VWAP_LOSE_EARLY_SHORT": (("sig5_vol_ratio20", ">=", 1.5643), ("pre3_body_sum_r", "<=", 0.797498)),
}
SAMPLE = {"E_ORB_BREAKOUT_SHORT": 1500}
FEATS = ["rs_pct", "market_ret_pct", "vol_ratio", "atr_pct", "close_loc", "body_pct",
         "vwap_dist_atr", "upper_wick_pct", "ranker_score", "quality_score", "v7_signal_notional_rs"]


def load_E():
    stt.POOL_DIRS = [CLEAN]
    pool = stt.load_pool().rename(columns={"tt_sig_ts": "tt_sig"})
    parts = []
    for s in E_SETUPS:
        e = pool[pool["setup"] == s]
        if s in SAMPLE and len(e) > SAMPLE[s]:
            e = e.sample(SAMPLE[s], random_state=7)
        parts.append(e)
    df = pd.concat(parts, ignore_index=True)
    return df


def _mae_mfe(b, side, e, ets, xts):
    sub = b[(b.index >= ets) & (b.index <= xts)]
    if sub.empty:
        return np.nan, np.nan
    hi, lo = float(sub["high"].max()), float(sub["low"].min())
    return ((hi - e) / e * 100, (lo - e) / e * 100) if side == "LONG" else ((e - lo) / e * 100, (e - hi) / e * 100)


def build_E(df):
    recs = []
    for r in df.itertuples():
        setup = r.setup
        sl, tgt = FIXED_EXIT[setup]
        side = str(getattr(r, "side", "")).upper()
        bars1 = v11._load_1m_with_open(r.ticker)
        if bars1 is None or bars1.empty:
            continue
        e = v11._first_1m_entry(bars1, r.tt_sig, max_delay_minutes=3)
        if e is None:
            continue
        ets, raw_open = e
        fill = round(raw_open * (1 + 0.0005), 2) if side == "LONG" else round(raw_open * (1 - 0.0005), 2)
        if fill <= 0:
            continue
        qty = max(1, int(100000.0 / raw_open))
        stop = fill * (1 - sl / 100.0) if side == "LONG" else fill * (1 + sl / 100.0)
        res = v11.er.resolve(bars=bars1, side=side, entry_price=fill, entry_time_ist=ets, sl_pct=sl, tgt_pct=tgt)
        if res is None:
            continue
        net = float(wfg.net_pnl_vectorized(np.array([fill]), np.array([res.exit_price]), np.array([qty]), np.array([side]), CFG)[0])
        mfe, mae = _mae_mfe(bars1, side, fill, ets, res.exit_time_ist)
        d = r.tt_sig
        rec = {"setup": setup, "side": side, "ticker": r.ticker, "date": d.strftime("%Y-%m-%d"),
               "signal_minute": int(d.hour * 60 + d.minute), "regime": str(getattr(r, "regime", "")),
               "rs_pct": float(getattr(r, "rs_pct", np.nan)), "market_ret_pct": float(getattr(r, "market_ret_pct", np.nan)),
               "vol_ratio": float(getattr(r, "vol_ratio", np.nan)), "atr_pct": float(getattr(r, "atr_pct", np.nan)),
               "close_loc": float(getattr(r, "close_loc", np.nan)), "body_pct": float(getattr(r, "body_pct", np.nan)),
               "vwap_dist_atr": float(getattr(r, "vwap_dist_atr", np.nan)), "upper_wick_pct": float(getattr(r, "upper_wick_pct", np.nan)),
               "outcome": res.outcome, "bars_held": int(res.bars_held), "net_pnl_rs": round(net, 2),
               "mfe_R": round(mfe / sl, 2), "mae_R": round(mae / sl, 2), "premom_pass": True}
        if setup in PM_TERMS:
            feats, reason = v11._pre_entry_momentum_features_v11(r.ticker, side, fill, stop, ets, d)
            rec["premom_pass"] = (not reason) and v11._eval_pre_momentum_terms(feats, PM_TERMS[setup])[0]
        recs.append(rec)
    out = pd.DataFrame(recs)
    out["period"] = np.where(out["date"] <= TRAIN_END, "TRAIN", np.where(out["date"] >= TEST_START, "TEST", "OTHER"))
    out["win"] = out["net_pnl_rs"] > 0
    out["immediate_fail"] = (out["outcome"] == "SL") & (out["bars_held"] <= 5)
    return out


def _pf(net):
    net = np.asarray(net, float); g, l = net[net > 0].sum(), -net[net < 0].sum()
    return float(g / l) if l > 0 else (float("inf") if g > 0 else 0.0)


def _dbp(df, nb=10000, seed=7):
    s = df.groupby("date")["net_pnl_rs"].sum().to_numpy()
    if len(s) < 3:
        return float("nan")
    r = np.random.default_rng(seed)
    return float((s[r.integers(0, len(s), size=(nb, len(s)))].mean(axis=1) <= 0).mean())


def _line(df, label):
    tr, te = df[df.period == "TRAIN"], df[df.period == "TEST"]
    return (f"  {label:<28} TRAIN n={len(tr):>4} PF={_pf(tr['net_pnl_rs']):>5.2f} | "
            f"TEST n={len(te):>4} PF={_pf(te['net_pnl_rs']):>5.2f} win={te['win'].mean()*100 if len(te) else 0:>4.0f}% net=Rs {df['net_pnl_rs'].sum():>9,.0f}")


BUCKETS = {
    "regime": lambda d: d["regime"].astype(str),
    "rs_pct": lambda d: pd.cut(d["rs_pct"], [-100, -1, 0, 1, 2, 100], labels=["<-1", "-1..0", "0..1", "1..2", ">2"]),
    "market_ret_pct": lambda d: pd.cut(d["market_ret_pct"], [-100, -0.5, -0.2, 0, 0.5, 100], labels=["<-0.5", "-0.5..-0.2", "-0.2..0", "0..0.5", ">0.5"]),
    "vwap_dist_atr": lambda d: pd.cut(d["vwap_dist_atr"], [-1e9, -3, -1, 0, 2, 1e9], labels=["<-3", "-3..-1", "-1..0", "0..2", ">2"]),
    "time_of_day": lambda d: pd.cut(d["signal_minute"], [0, 600, 660, 720, 810, 900], labels=["<10:00", "10:00-11:00", "11:00-12:00", "12:00-13:30", ">13:30"]),
    "atr_pct": lambda d: pd.cut(d["atr_pct"], [0, 0.004, 0.006, 0.008, 0.012, 1], labels=["<0.4%", "0.4-0.6%", "0.6-0.8%", "0.8-1.2%", ">1.2%"]),
    "vol_ratio": lambda d: pd.cut(d["vol_ratio"], [0, 1.5, 2, 3, 5, 1e9], labels=["<1.5", "1.5..2", "2..3", "3..5", ">5"]),
}


def bucket_table(df):
    rows = []
    for setup, g0 in df.groupby("setup"):
        for bn, fn in BUCKETS.items():
            g = g0.copy()
            try:
                g["_b"] = fn(g)
            except Exception:
                continue
            for bv, gg in g.groupby("_b", observed=True):
                if len(gg) == 0:
                    continue
                rows.append({"setup": setup, "bucket_dim": bn, "bucket": str(bv), "n": len(gg),
                             "win_pct": round(gg["win"].mean()*100, 1), "net_pf": round(_pf(gg["net_pnl_rs"]), 2),
                             "net_rs": round(gg["net_pnl_rs"].sum(), 0), "immfail_pct": round(gg["immediate_fail"].mean()*100, 1)})
    return pd.DataFrame(rows)


def main():
    PROP.mkdir(parents=True, exist_ok=True)
    df0 = load_E()
    print(f"[validate_E] candidates: {df0['setup'].value_counts().to_dict()}")
    df = build_E(df0)
    df.to_csv(PROP / "E_setups_trades_nov_to_now.csv", index=False)
    print("\n=== E* per-setup, per-period (fixed exit, NET) ===")
    for (setup, period), g in df.groupby(["setup", "period"]):
        if period == "OTHER":
            continue
        print(f"  {setup:<24} {period:<6} n={len(g):>4} win={g['win'].mean()*100:4.1f}% PF={_pf(g['net_pnl_rs']):4.2f} "
              f"net=Rs {g['net_pnl_rs'].sum():>9,.0f} mfeR={g['mfe_R'].mean():4.2f} immF={g['immediate_fail'].mean()*100:4.0f}% dbp={_dbp(g):.2f}")
    print("\n=== pre-momentum ON vs OFF (where a PM gate exists) ===")
    for setup in PM_TERMS:
        g = df[df.setup == setup]
        if g.empty:
            continue
        print(f"  {setup}:")
        print(_line(g, "  premom OFF (all)"))
        print(_line(g[g.premom_pass], "  premom ON (pass)"))
        print(_line(g[~g.premom_pass], "  premom REJECTED"))
    bt = bucket_table(df)
    bt.to_csv(PROP / "E_setups_bucket_analysis.csv", index=False)
    print("\n=== key buckets (per setup) ===")
    for setup in E_SETUPS:
        s = bt[bt.setup == setup]
        if s.empty:
            continue
        print(f"  --- {setup} ---")
        for dim in ["regime", "market_ret_pct", "rs_pct", "vwap_dist_atr", "time_of_day", "vol_ratio"]:
            for _, r in s[s.bucket_dim == dim].iterrows():
                print(f"     {dim:<14}{str(r.bucket):<12} n={int(r.n):>4} win={r.win_pct:>5}% PF={r.net_pf:>5} net=Rs {r.net_rs:>9,.0f}")
    print(f"\n[validate_E] wrote E_setups_trades_nov_to_now.csv, E_setups_bucket_analysis.csv -> {PROP}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
