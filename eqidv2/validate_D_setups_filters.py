"""
validate_D_setups_filters.py — deep diagnosis of the D* trend family, Nov->now, ~1200 tickers,
testing each gate WITH/WITHOUT incl. the pre-entry momentum filter.

D setups (avwap_5min_ID_v2_backtesting.py):
  D_EMA20_BOUNCE   (LONG)  near EMA20 in an uptrend stack, bounce. exit 0.70/1.50.
                   PM gate: pre3_range_r>=0.2923, pre_entry_momentum_score<=78.3448.
                   prod mask: (vol_ratio<=1.60 OR vwap_dist_atr>=-0.39) AND signal_minute<=705.
  D_EMA20_REJECTION(SHORT) near EMA20 in a downtrend stack, rejection. exit 0.75/1.30.
                   PM gate: pre10_mom_r<=0.1566, pre5_mom_r>=0.1249, sig5_adx_calc>=20.
                   prod mask: body_pct>=0.89 AND ranker_score>=0.39 (+ residual late overlay).
  D_AVWAP_LOSE_REVERSAL (SHORT) loses session VWAP from above (bearish mirror of B_AVWAP). exit 1.00/1.50.

Data: D_EMA20_* are well-represented in the pre_dedupe clean pool (admitted population);
D_AVWAP has only ~18 admitted (mostly v8-gate-rejected) so it is read from RAW detections.
Fixed exits, NET of cost, no lookahead. Run:  py -3.12 validate_D_setups_filters.py
"""
from __future__ import annotations
import glob
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
FIXED_EXIT = {"D_EMA20_BOUNCE": (0.70, 1.50), "D_EMA20_REJECTION": (0.75, 1.30), "D_AVWAP_LOSE_REVERSAL": (1.00, 1.50)}
PM_TERMS = {
    "D_EMA20_BOUNCE": (("pre3_range_r", ">=", 0.292349), ("pre_entry_momentum_score", "<=", 78.3448)),
    "D_EMA20_REJECTION": (("pre10_mom_r", "<=", 0.156614), ("pre5_mom_r", ">=", 0.12493), ("sig5_adx_calc", ">=", 20.0)),
}
FEATS = ["rs_pct", "market_ret_pct", "vol_ratio", "atr_pct", "close_loc", "body_pct", "vwap_dist_atr", "ranker_score", "quality_score"]
SAMPLE_AVWAP = 1500


def load_D() -> pd.DataFrame:
    stt.POOL_DIRS = [CLEAN]
    pool = stt.load_pool()
    ema = pool[pool["setup"].isin(["D_EMA20_BOUNCE", "D_EMA20_REJECTION"])].copy()
    ema = ema.rename(columns={"tt_sig_ts": "tt_sig"})
    keep = ["ticker", "side", "setup", "tt_sig"] + [c for c in FEATS if c in ema.columns]
    ema = ema[keep]
    # D_AVWAP from raw (only ~18 admitted)
    fs = glob.glob(str(CLEAN / "chunk_*" / "historical_all_available_days" / "*" / "raw_candidates.csv"))
    raw = pd.concat([pd.read_csv(f) for f in fs if Path(f).exists()], ignore_index=True)
    av = raw[raw["setup"] == "D_AVWAP_LOSE_REVERSAL"].copy()
    av["ticker"] = av["ticker"].astype(str).str.upper().str.strip()
    av["side"] = av["side"].astype(str).str.upper().str.strip()
    av["tt_sig"] = av["signal_time_ist"].map(v11._normalise_ts)
    av = av.dropna(subset=["tt_sig"])
    for c in FEATS:
        if c in av.columns:
            av[c] = pd.to_numeric(av[c], errors="coerce")
    av = av.drop_duplicates(subset=["ticker", "tt_sig"])
    if len(av) > SAMPLE_AVWAP:
        av = av.sample(SAMPLE_AVWAP, random_state=7)
    av = av[["ticker", "side", "setup", "tt_sig"] + [c for c in FEATS if c in av.columns]]
    df = pd.concat([ema, av], ignore_index=True)
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["regime"] = ""  # attached in main() from the pool keyed by (ticker, setup, tt_sig)
    return df


def _regime_col(df, pool):
    # regime is on the pool rows for EMA; for raw D_AVWAP pull from raw frame already merged via FEATS? add separately
    return df


def _mae_mfe(bars1, side, e, ets, xts):
    sub = bars1[(bars1.index >= ets) & (bars1.index <= xts)]
    if sub.empty:
        return np.nan, np.nan
    hi, lo = float(sub["high"].max()), float(sub["low"].min())
    return ((hi - e) / e * 100, (lo - e) / e * 100) if side == "LONG" else ((e - lo) / e * 100, (e - hi) / e * 100)


def build_D(df: pd.DataFrame) -> pd.DataFrame:
    recs = []
    for r in df.itertuples():
        setup = r.setup
        sl, tgt = FIXED_EXIT[setup]
        bars1 = v11._load_1m_with_open(r.ticker)
        if bars1 is None or bars1.empty:
            continue
        e = v11._first_1m_entry(bars1, r.tt_sig, max_delay_minutes=3)
        if e is None:
            continue
        ets, raw_open = e
        fill = round(raw_open * (1 + 0.0005), 2) if r.side == "LONG" else round(raw_open * (1 - 0.0005), 2)
        if fill <= 0:
            continue
        qty = max(1, int(100000.0 / raw_open))
        stop = fill * (1 - sl / 100.0) if r.side == "LONG" else fill * (1 + sl / 100.0)
        res = v11.er.resolve(bars=bars1, side=r.side, entry_price=fill, entry_time_ist=ets, sl_pct=sl, tgt_pct=tgt)
        if res is None:
            continue
        net = float(wfg.net_pnl_vectorized(np.array([fill]), np.array([res.exit_price]), np.array([qty]), np.array([r.side]), CFG)[0])
        mfe, mae = _mae_mfe(bars1, r.side, fill, ets, res.exit_time_ist)
        d = r.tt_sig
        rec = {"setup": setup, "side": r.side, "ticker": r.ticker, "date": d.strftime("%Y-%m-%d"),
               "signal_minute": int(d.hour * 60 + d.minute), "regime": str(getattr(r, "regime", "")),
               "rs_pct": float(getattr(r, "rs_pct", np.nan)), "market_ret_pct": float(getattr(r, "market_ret_pct", np.nan)),
               "vol_ratio": float(getattr(r, "vol_ratio", np.nan)), "atr_pct": float(getattr(r, "atr_pct", np.nan)),
               "close_loc": float(getattr(r, "close_loc", np.nan)), "body_pct": float(getattr(r, "body_pct", np.nan)),
               "vwap_dist_atr": float(getattr(r, "vwap_dist_atr", np.nan)), "ranker_score": float(getattr(r, "ranker_score", np.nan)),
               "outcome": res.outcome, "bars_held": int(res.bars_held), "net_pnl_rs": round(net, 2),
               "mfe_R": round(mfe / sl, 2), "mae_R": round(mae / sl, 2), "premom_pass": True}
        if setup in PM_TERMS:
            feats, reason = v11._pre_entry_momentum_features_v11(r.ticker, r.side, fill, stop, ets, d)
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


BUCKETS = {
    "regime": lambda d: d["regime"].astype(str),
    "rs_pct": lambda d: pd.cut(d["rs_pct"], [-100, -1, 0, 0.5, 1, 2, 100], labels=["<-1", "-1..0", "0..0.5", "0.5..1", "1..2", ">2"]),
    "vwap_dist_atr": lambda d: pd.cut(d["vwap_dist_atr"], [-1e9, -1, -0.4, 0, 0.5, 1e9], labels=["<-1", "-1..-0.4", "-0.4..0", "0..0.5", ">0.5"]),
    "time_of_day": lambda d: pd.cut(d["signal_minute"], [0, 660, 705, 780, 825, 900], labels=["<11:00", "11:00-11:45", "11:45-13:00", "13:00-13:45", ">13:45"]),
    "vol_ratio": lambda d: pd.cut(d["vol_ratio"], [0, 1.3, 1.6, 2, 3, 1e9], labels=["<1.3", "1.3..1.6", "1.6..2", "2..3", ">3"]),
    "atr_pct": lambda d: pd.cut(d["atr_pct"], [0, 0.004, 0.006, 0.008, 0.012, 1], labels=["<0.4%", "0.4-0.6%", "0.6-0.8%", "0.8-1.2%", ">1.2%"]),
    "body_pct": lambda d: pd.cut(d["body_pct"], [0, 0.5, 0.7, 0.85, 1.01], labels=["<0.5", "0.5-0.7", "0.7-0.85", ">0.85"]),
}


def bucket_table(df):
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
                rows.append({"setup": setup, "bucket_dim": bname, "bucket": str(bval), "n": len(gg),
                             "win_pct": round(gg["win"].mean() * 100, 1), "net_pf": round(_pf(gg["net_pnl_rs"]), 2),
                             "net_rs": round(gg["net_pnl_rs"].sum(), 0), "immfail_pct": round(gg["immediate_fail"].mean() * 100, 1)})
    return pd.DataFrame(rows)


def _line(df, label):
    tr, te = df[df.period == "TRAIN"], df[df.period == "TEST"]
    return (f"  {label:<34} TRAIN n={len(tr):>4} PF={_pf(tr['net_pnl_rs']):>5.2f} | "
            f"TEST n={len(te):>4} PF={_pf(te['net_pnl_rs']):>5.2f} win={te['win'].mean()*100 if len(te) else 0:>4.0f}% net=Rs {df['net_pnl_rs'].sum():>8,.0f}")


def main():
    PROP.mkdir(parents=True, exist_ok=True)
    # regime needs to survive into build; reload pool to attach regime to EMA rows
    stt.POOL_DIRS = [CLEAN]
    pool = stt.load_pool()
    reg = pool.set_index(["ticker", "setup", pool["tt_sig_ts"].astype(str)])["regime"].to_dict() if "regime" in pool.columns else {}
    df0 = load_D()
    # attach regime where available (EMA rows from pool); raw D_AVWAP regime from raw merged in FEATS? add now
    df0["regime"] = [reg.get((t, s, str(ts)), "") for t, s, ts in zip(df0["ticker"], df0["setup"], df0["tt_sig"])]
    print(f"[validate_D] candidates: {df0['setup'].value_counts().to_dict()}")
    df = build_D(df0)
    df.to_csv(PROP / "D_setups_trades_nov_to_now.csv", index=False)
    print("\n=== D* per-setup, per-period (fixed exit, NET) ===")
    for (setup, period), g in df.groupby(["setup", "period"]):
        if period == "OTHER":
            continue
        print(f"  {setup:<22} {period:<6} n={len(g):>4} win={g['win'].mean()*100:4.1f}% PF={_pf(g['net_pnl_rs']):4.2f} "
              f"net=Rs {g['net_pnl_rs'].sum():>8,.0f} mfeR={g['mfe_R'].mean():4.2f} immFail={g['immediate_fail'].mean()*100:4.1f}%")
    print("\n=== pre-momentum ON vs OFF (where a PM gate exists) ===")
    for setup in PM_TERMS:
        g = df[df.setup == setup]
        print(f"  {setup}:")
        print(_line(g, "  premom OFF (all)"))
        print(_line(g[g.premom_pass], "  premom ON (pass)"))
        print(_line(g[~g.premom_pass], "  premom REJECTED"))
    bucket_table(df).to_csv(PROP / "D_setups_bucket_analysis.csv", index=False)
    bt = bucket_table(df)
    print("\n=== key buckets ===")
    for setup in FIXED_EXIT:
        s = bt[bt.setup == setup]
        if s.empty:
            continue
        print(f"  --- {setup} ---")
        for dim in ["regime", "rs_pct", "vwap_dist_atr", "time_of_day", "vol_ratio", "body_pct"]:
            for _, r in s[s.bucket_dim == dim].iterrows():
                print(f"     {dim:<14}{str(r.bucket):<12} n={int(r.n):>4} win={r.win_pct:>5}% PF={r.net_pf:>5} net=Rs {r.net_rs:>8,.0f} immF={r.immfail_pct:>4}%")
    print(f"\n[validate_D] wrote D_setups_trades_nov_to_now.csv, D_setups_bucket_analysis.csv -> {PROP}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
