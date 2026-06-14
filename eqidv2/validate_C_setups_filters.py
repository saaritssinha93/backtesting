"""
validate_C_setups_filters.py
Data-grounded diagnosis for the C* opening-range family, Nov 2025 -> now, ~1200 tickers,
testing each gate WITH and WITHOUT (gate ladder), incl. the pre-entry momentum filter.

KEY FINDING (root-caused here): C_OR_BREAKOUT raw detections only start at ~10:55 (the
v2 scan loop starts at bar index 20 = VWAP_LOOKBACK), but the LIVE entry guard requires
signal time 09:55-10:40. The windows are DISJOINT -> C_OR_BREAKOUT produces ZERO live
entries (the unresolved "C_OR_BREAKOUT zero-entry-row" issue). This script therefore
diagnoses C_OR_BREAKOUT on its REAL firing window with the *vwap/atr* guards applied but
the broken time window removed, and tests a CORRECTED window + pre-momentum on/off.

C_OR_BREAKOUT (LONG) is overlay-admitted (absent from the profile=none clean pool), so we
load RAW 5-min detections from the clean-pool per-day raw_candidates.csv, apply the vwap/atr
guard (vwap_dist_atr>=2.0, atr_pct<=0.010), SAMPLE to bound compute, resolve at the fixed
exit (1.20/1.50) on 1-minute data NET of cost, compute MAE/MFE + the pre-entry momentum gate.
C_OR_BREAKDOWN (SHORT) is a SHADOW/blocked setup; mirror guard (vwap_dist_atr<=-2.0).

No lookahead. Run:  py -3.12 validate_C_setups_filters.py
"""
from __future__ import annotations
import glob
from pathlib import Path
import numpy as np
import pandas as pd

import avwap_5min_ID_v11_backtesting as v11
import walkforward_gate as wfg
from nse_intraday_costs import CostConfig

CLEAN = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_cleanpool")
PROP = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_traintest_pool\proposals")
TRAIN_END, TEST_START = "2026-04-30", "2026-05-01"
CFG = CostConfig()
FIXED_EXIT = {"C_OR_BREAKOUT": (1.20, 1.50), "C_OR_BREAKDOWN": (0.70, 1.30)}
C_OR_PM_TERMS = (("sig5_adx_calc", ">=", 25.0), ("sig5_rsi_dir", ">=", 60.0),
                 ("sig5_vol_ratio20", ">=", 1.5), ("pre2_mom_r", ">=", -0.050))
SAMPLE_N = 1500
LIVE_WIN = (595, 640)   # 09:55-10:40 (the live guard — provably disjoint from detections)


def load_raw_C() -> pd.DataFrame:
    fs = glob.glob(str(CLEAN / "chunk_*" / "historical_all_available_days" / "*" / "raw_candidates.csv"))
    df = pd.concat([pd.read_csv(f) for f in fs if Path(f).exists()], ignore_index=True)
    df = df[df["setup"].astype(str).isin(FIXED_EXIT)].copy()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["tt_sig"] = df["signal_time_ist"].map(v11._normalise_ts)
    df = df.dropna(subset=["tt_sig"])
    df["signal_minute"] = df["tt_sig"].dt.hour * 60 + df["tt_sig"].dt.minute
    df["date"] = df["tt_sig"].dt.strftime("%Y-%m-%d")
    for c in ("rs_pct", "vwap_dist_atr", "atr_pct", "vol_ratio", "close_loc", "body_pct", "market_ret_pct", "quality_score"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.drop_duplicates(subset=["ticker", "setup", "tt_sig"]).reset_index(drop=True)
    keep = []
    for setup in FIXED_EXIT:
        s = df[df["setup"] == setup]
        if setup == "C_OR_BREAKOUT":
            s = s[(s["vwap_dist_atr"] >= 2.0) & (s["atr_pct"] <= 0.010)]
        else:
            s = s[(s["vwap_dist_atr"] <= -2.0) & (s["atr_pct"] <= 0.010)]
        if len(s) > SAMPLE_N:
            s = s.sample(SAMPLE_N, random_state=7)
        keep.append(s)
    return pd.concat(keep, ignore_index=True)


def _mae_mfe(bars1, side, e, ets, xts):
    sub = bars1[(bars1.index >= ets) & (bars1.index <= xts)]
    if sub.empty:
        return np.nan, np.nan
    hi, lo = float(sub["high"].max()), float(sub["low"].min())
    return ((hi - e) / e * 100, (lo - e) / e * 100) if side == "LONG" else ((e - lo) / e * 100, (e - hi) / e * 100)


def build_C(raw: pd.DataFrame) -> pd.DataFrame:
    recs = []
    for r in raw.itertuples():
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
        rec = {"setup": setup, "side": r.side, "ticker": r.ticker, "date": r.date,
               "signal_minute": int(r.signal_minute), "regime": str(getattr(r, "regime", "")),
               "rs_pct": float(getattr(r, "rs_pct", np.nan)), "market_ret_pct": float(getattr(r, "market_ret_pct", np.nan)),
               "vol_ratio": float(getattr(r, "vol_ratio", np.nan)), "atr_pct": float(getattr(r, "atr_pct", np.nan)),
               "close_loc": float(getattr(r, "close_loc", np.nan)), "body_pct": float(getattr(r, "body_pct", np.nan)),
               "vwap_dist_atr": float(getattr(r, "vwap_dist_atr", np.nan)),
               "outcome": res.outcome, "bars_held": int(res.bars_held), "net_pnl_rs": round(net, 2),
               "mfe_R": round(mfe / sl, 2), "mae_R": round(mae / sl, 2), "premom_pass": True}
        if setup == "C_OR_BREAKOUT":
            feats, reason = v11._pre_entry_momentum_features_v11(r.ticker, r.side, fill, stop, ets, r.tt_sig)
            rec["premom_pass"] = (not reason) and v11._eval_pre_momentum_terms(feats, C_OR_PM_TERMS)[0]
        recs.append(rec)
    out = pd.DataFrame(recs)
    out["period"] = np.where(out["date"] <= TRAIN_END, "TRAIN", np.where(out["date"] >= TEST_START, "TEST", "OTHER"))
    out["win"] = out["net_pnl_rs"] > 0
    out["immediate_fail"] = (out["outcome"] == "SL") & (out["bars_held"] <= 5)
    return out


def _pf(net):
    net = np.asarray(net, float); g, l = net[net > 0].sum(), -net[net < 0].sum()
    return float(g / l) if l > 0 else (float("inf") if g > 0 else 0.0)


def _line(df, label):
    tr, te = df[df.period == "TRAIN"], df[df.period == "TEST"]
    return (f"  {label:<40} TRAIN n={len(tr):>4} PF={_pf(tr['net_pnl_rs']):>5.2f} | "
            f"TEST n={len(te):>4} PF={_pf(te['net_pnl_rs']):>5.2f} win={te['win'].mean()*100 if len(te) else 0:>4.0f}% "
            f"net=Rs {df['net_pnl_rs'].sum():>9,.0f} immFail={df['immediate_fail'].mean()*100:>4.0f}%")


def ladder(df: pd.DataFrame):
    b = df[df.setup == "C_OR_BREAKOUT"].copy()
    print("\n=== C_OR_BREAKOUT GATE LADDER (guard=vwap>=2.0 & atr<=0.010 already applied) ===")
    print(_line(b, "GUARD pop (all-day, the real window)"))
    print(_line(b[(b.signal_minute >= LIVE_WIN[0]) & (b.signal_minute <= LIVE_WIN[1])], "+ LIVE window 09:55-10:40 (THE BUG)"))
    print(_line(b[(b.signal_minute >= 655) & (b.signal_minute <= 720)], "+ corrected 10:55-12:00"))
    print(_line(b[(b.signal_minute >= 720) & (b.signal_minute <= 810)], "+ corrected 12:00-13:30"))
    print(_line(b[b.signal_minute >= 810], "+ late >13:30"))
    print("  -- pre-momentum ON vs OFF (within the all-day guard population) --")
    print(_line(b, "premom OFF (all guard)"))
    print(_line(b[b.premom_pass], "premom ON (pass)"))
    print(_line(b[~b.premom_pass], "premom REJECTED (what PM drops)"))
    rows = [{"step": "guard_all_day", **_d(b)}, {"step": "live_window_0955_1040", **_d(b[(b.signal_minute >= LIVE_WIN[0]) & (b.signal_minute <= LIVE_WIN[1])])},
            {"step": "corrected_1055_1200", **_d(b[(b.signal_minute >= 655) & (b.signal_minute <= 720)])},
            {"step": "premom_on", **_d(b[b.premom_pass])}, {"step": "premom_off", **_d(b)}]
    pd.DataFrame(rows).to_csv(PROP / "C_OR_BREAKOUT_gate_ladder.csv", index=False)

    print("\n=== C_OR_BREAKDOWN (SHADOW/blocked) — mirror-guard assessment ===")
    bd = df[df.setup == "C_OR_BREAKDOWN"].copy()
    print(_line(bd, "mirror-guard pop (all-day)"))
    print(_line(bd[bd.market_ret_pct <= -0.20], "+ market falling (mkt_ret<=-0.20)"))
    print(_line(bd[bd.regime.astype(str) == "BEAR"], "+ regime BEAR"))


def _d(df):
    tr, te = df[df.period == "TRAIN"], df[df.period == "TEST"]
    return {"train_n": len(tr), "train_pf": round(_pf(tr["net_pnl_rs"]), 2),
            "test_n": len(te), "test_pf": round(_pf(te["net_pnl_rs"]), 2),
            "test_win": round(te["win"].mean() * 100, 1) if len(te) else 0, "net_total": round(df["net_pnl_rs"].sum(), 0)}


def main():
    PROP.mkdir(parents=True, exist_ok=True)
    raw = load_raw_C()
    print(f"[validate_C] sampled guard pops: {raw['setup'].value_counts().to_dict()}")
    df = build_C(raw)
    df.to_csv(PROP / "C_setups_trades_nov_to_now.csv", index=False)
    print("\n=== C* per-setup, per-period (guard pop, fixed exit, NET) ===")
    for (setup, period), g in df.groupby(["setup", "period"]):
        if period == "OTHER":
            continue
        print(f"  {setup:<18} {period:<6} n={len(g):>4} win={g['win'].mean()*100:4.1f}% PF={_pf(g['net_pnl_rs']):4.2f} "
              f"net=Rs {g['net_pnl_rs'].sum():>9,.0f} mfeR={g['mfe_R'].mean():4.2f} immFail={g['immediate_fail'].mean()*100:4.1f}%")
    ladder(df)
    # time-of-day edge map for C_OR_BREAKOUT (guard pop)
    b = df[df.setup == "C_OR_BREAKOUT"].copy()
    b["tod"] = pd.cut(b["signal_minute"], [654, 690, 720, 750, 810, 870], labels=["10:55-11:30", "11:30-12:00", "12:00-12:30", "12:30-13:30", ">13:30"])
    print("\n=== C_OR_BREAKOUT time-of-day edge map (guard pop) ===")
    for tod, g in b.groupby("tod", observed=True):
        print(f"  {str(tod):<14} n={len(g):>4} win={g['win'].mean()*100:4.1f}% PF={_pf(g['net_pnl_rs']):4.2f} net=Rs {g['net_pnl_rs'].sum():>9,.0f}")
    print(f"\n[validate_C] wrote C_setups_trades_nov_to_now.csv, C_OR_BREAKOUT_gate_ladder.csv -> {PROP}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
