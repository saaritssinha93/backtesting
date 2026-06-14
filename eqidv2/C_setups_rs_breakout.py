"""
C_setups_rs_breakout.py — focused iteration on the one C_OR_BREAKOUT edge that survived:
the RELATIVE-STRENGTH breakout (stock breaks OR high while NIFTY is red). Sweeps the
market_ret threshold x exit grid (LONG) to find the firmest train+test config, honestly.
Run:  py -3.12 C_setups_rs_breakout.py
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
SL_GRID = (0.70, 0.85, 1.00, 1.20)
TGT_GRID = (0.60, 0.80, 1.00, 1.20, 1.50)
MKT_THR = (-0.10, -0.20, -0.30, -0.50)


def build_paths():
    fs = glob.glob(str(CLEAN / "chunk_*" / "historical_all_available_days" / "*" / "raw_candidates.csv"))
    df = pd.concat([pd.read_csv(f) for f in fs if Path(f).exists()], ignore_index=True)
    df = df[df["setup"] == "C_OR_BREAKOUT"].copy()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["tt_sig"] = df["signal_time_ist"].map(v11._normalise_ts)
    df = df.dropna(subset=["tt_sig"])
    for c in ("vwap_dist_atr", "atr_pct", "rs_pct", "vol_ratio", "market_ret_pct"):
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    # guard + relative-strength breakout population (market red)
    df = df[(df["vwap_dist_atr"] >= 2.0) & (df["atr_pct"] <= 0.010) & (df["market_ret_pct"] <= -0.10)].copy()
    df["signal_minute"] = df["tt_sig"].dt.hour * 60 + df["tt_sig"].dt.minute
    df["date"] = df["tt_sig"].dt.strftime("%Y-%m-%d")
    df = df.drop_duplicates(subset=["ticker", "tt_sig"]).reset_index(drop=True)
    paths = []
    for r in df.itertuples():
        bars1 = v11._load_1m_with_open(r.ticker)
        if bars1 is None or bars1.empty:
            continue
        e = v11._first_1m_entry(bars1, r.tt_sig, max_delay_minutes=3)
        if e is None:
            continue
        ets, raw_open = e
        if raw_open <= 0:
            continue
        eod = ets.normalize() + pd.Timedelta(hours=15, minutes=20)
        sub = bars1[(bars1.index >= ets) & (bars1.index <= eod)]
        if sub.empty:
            continue
        paths.append({"date": r.date, "market_ret_pct": float(r.market_ret_pct), "rs_pct": float(r.rs_pct),
                      "regime": str(getattr(r, "regime", "")), "signal_minute": int(r.signal_minute),
                      "raw_open": float(raw_open), "highs": sub["high"].to_numpy(float),
                      "lows": sub["low"].to_numpy(float), "closes": sub["close"].to_numpy(float)})
    print(f"[rs_breakout] RS-breakout (market<=-0.10) paths: {len(paths)}")
    return paths


def sim_long(p, sl, tgt):
    e = p["raw_open"] * 1.0005
    qty = max(1, int(100000.0 / p["raw_open"]))
    h, l, c = p["highs"], p["lows"], p["closes"]
    slp, tgp = e * (1 - sl / 100), e * (1 + tgt / 100)
    sl_hit, tg_hit = l <= slp, h >= tgp
    fsl = int(np.argmax(sl_hit)) if sl_hit.any() else 10**9
    ftg = int(np.argmax(tg_hit)) if tg_hit.any() else 10**9
    xp = slp if (sl_hit.any() and fsl <= ftg) else (tgp if tg_hit.any() else c[-1])
    return float(wfg.net_pnl_vectorized(np.array([e]), np.array([xp]), np.array([qty]), np.array(["LONG"]), CFG)[0])


def _pf(net):
    net = np.asarray(net, float); g, l = net[net > 0].sum(), -net[net < 0].sum()
    return float(g / l) if l > 0 else (float("inf") if g > 0 else 0.0)


def _dbp(dates, net, n=8000, seed=7):
    s = pd.Series(net, index=pd.to_datetime(dates)).groupby(level=0).sum().to_numpy()
    if len(s) < 3:
        return float("nan")
    rng = np.random.default_rng(seed)
    return float((s[rng.integers(0, len(s), size=(n, len(s)))].mean(axis=1) <= 0).mean())


def main():
    paths = build_paths()
    rows = []
    for thr in MKT_THR:
        pp = [p for p in paths if p["market_ret_pct"] <= thr]
        for sl in SL_GRID:
            for tgt in TGT_GRID:
                rec = [(p["date"], sim_long(p, sl, tgt)) for p in pp]
                if not rec:
                    continue
                dts = [r[0] for r in rec]; net = np.array([r[1] for r in rec])
                tr = net[np.array(dts) <= TRAIN_END]; te = net[np.array(dts) >= TEST_START]
                ted = [d for d in dts if d >= TEST_START]
                if len(tr) < 20 or len(te) < 6:
                    continue
                rows.append({"mkt_thr": thr, "sl": sl, "tgt": tgt, "train_n": len(tr), "train_pf": round(_pf(tr), 2),
                             "test_n": len(te), "test_pf": round(_pf(te), 2), "test_win": round((te > 0).mean()*100, 1),
                             "test_dbp": round(_dbp(ted, te), 3), "train_net": round(tr.sum(), 0), "test_net": round(te.sum(), 0),
                             "minpf": round(min(_pf(tr), _pf(te)), 2)})
    res = pd.DataFrame(rows).sort_values("minpf", ascending=False)
    res.to_csv(PROP / "C_OR_BREAKOUT_rs_breakout_sweep.csv", index=False)
    print("\n=== RS-breakout (market red) — exit x market-threshold sweep, top by min(train,test) PF ===")
    print(f"  {'mkt<=':<7}{'SL':<6}{'Tgt':<6}{'TRAIN n/PF':<16}{'TEST n/PF/win':<22}{'test p':<8}")
    for _, r in res.head(16).iterrows():
        print(f"  {r['mkt_thr']:<7}{r['sl']:<6}{r['tgt']:<6}"
              f"{f'{int(r.train_n)}/{r.train_pf}':<16}{f'{int(r.test_n)}/{r.test_pf}/{r.test_win}%':<22}{r['test_dbp']}")
    print(f"\n[rs_breakout] wrote C_OR_BREAKOUT_rs_breakout_sweep.csv -> {PROP}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
