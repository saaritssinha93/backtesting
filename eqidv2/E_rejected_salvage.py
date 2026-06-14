"""
E_rejected_salvage.py — deep salvage attempt on the 3 rejected E setups
(E_ORB_BREAKOUT_SHORT, E_ORB_BREAKOUT_LONG, E_VWAP_BAND_FADE).
For each: build entry->EOD 1-min PATHS, then sweep (SL x Tgt) exits x sub-populations
x pre-momentum on/off, honestly split train/test, day-block bootstrap. Report any
config with edge in BOTH periods. NET of cost, no lookahead.
Run:  py -3.12 E_rejected_salvage.py
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
SETUPS = {"E_ORB_BREAKOUT_SHORT": "SHORT", "E_ORB_BREAKOUT_LONG": "LONG", "E_VWAP_BAND_FADE": "SHORT"}
SAMPLE = {"E_ORB_BREAKOUT_SHORT": 1200}
PM_TERMS = {
    "E_ORB_BREAKOUT_LONG": (("pre15_vol_ratio20", "<=", 1.08301), ("pre1_adx", ">=", 42.3138)),
    "E_ORB_BREAKOUT_SHORT": (("pre10_dir_count", ">=", 5.0), ("pre5_vol_ratio20", ">=", 1.65561)),
}
SL_GRID = (0.50, 0.70, 0.85, 1.00)
TGT_GRID = (0.40, 0.60, 0.80, 1.00, 1.20, 1.50)
# sub-population masks per setup (operate on a path dict)
SUBPOPS = {
    "E_ORB_BREAKOUT_SHORT": {
        "all": None, "market<=-0.2": lambda p: p["market_ret_pct"] <= -0.2,
        "vol2-3": lambda p: 2 <= p["vol_ratio"] <= 3, "time10-11": lambda p: 600 <= p["signal_minute"] <= 660,
        "rs<-1": lambda p: p["rs_pct"] < -1, "regimeBEAR": lambda p: p["regime"] == "BEAR",
        "premom_on": lambda p: p["premom_pass"], "premom_off": lambda p: not p["premom_pass"],
    },
    "E_ORB_BREAKOUT_LONG": {
        "all": None, "regimeTREND": lambda p: p["regime"] == "TREND", "market>0.2": lambda p: p["market_ret_pct"] >= 0.2,
        "rs>2": lambda p: p["rs_pct"] > 2, "less_extended_vwap<1": lambda p: p["vwap_dist_atr"] < 1.0,
        "premom_on": lambda p: p["premom_pass"], "premom_off": lambda p: not p["premom_pass"],
    },
    "E_VWAP_BAND_FADE": {
        "all": None, "market>0.5": lambda p: p["market_ret_pct"] > 0.5, "regimeBULL": lambda p: p["regime"] == "BULL",
        "near_vwap_-1..0": lambda p: -1 <= p["vwap_dist_atr"] <= 0, "rs<-1": lambda p: p["rs_pct"] < -1,
        "vol>5": lambda p: p["vol_ratio"] > 5,
    },
}


def build_paths():
    stt.POOL_DIRS = [CLEAN]
    pool = stt.load_pool().rename(columns={"tt_sig_ts": "tt_sig"})
    out = {}
    for setup, side in SETUPS.items():
        e = pool[pool["setup"] == setup]
        if setup in SAMPLE and len(e) > SAMPLE[setup]:
            e = e.sample(SAMPLE[setup], random_state=7)
        paths = []
        for r in e.itertuples():
            bars1 = v11._load_1m_with_open(r.ticker)
            if bars1 is None or bars1.empty:
                continue
            ent = v11._first_1m_entry(bars1, r.tt_sig, max_delay_minutes=3)
            if ent is None:
                continue
            ets, raw_open = ent
            if raw_open <= 0:
                continue
            eod = ets.normalize() + pd.Timedelta(hours=15, minutes=20)
            sub = bars1[(bars1.index >= ets) & (bars1.index <= eod)]
            if sub.empty:
                continue
            d = r.tt_sig
            p = {"date": d.strftime("%Y-%m-%d"), "signal_minute": int(d.hour*60+d.minute),
                 "regime": str(getattr(r, "regime", "")), "rs_pct": float(getattr(r, "rs_pct", np.nan)),
                 "market_ret_pct": float(getattr(r, "market_ret_pct", np.nan)), "vol_ratio": float(getattr(r, "vol_ratio", np.nan)),
                 "vwap_dist_atr": float(getattr(r, "vwap_dist_atr", np.nan)), "raw_open": float(raw_open), "side": side,
                 "highs": sub["high"].to_numpy(float), "lows": sub["low"].to_numpy(float), "closes": sub["close"].to_numpy(float),
                 "premom_pass": True}
            if setup in PM_TERMS:
                fill = raw_open * (1.0005 if side == "LONG" else 0.9995)
                stop = fill * (1 - 0.8/100) if side == "LONG" else fill * (1 + 0.8/100)
                feats, reason = v11._pre_entry_momentum_features_v11(r.ticker, side, fill, stop, ets, d)
                p["premom_pass"] = (not reason) and v11._eval_pre_momentum_terms(feats, PM_TERMS[setup])[0]
            paths.append(p)
        out[setup] = paths
        print(f"[salvage] {setup}: {len(paths)} paths")
    return out


def sim(p, sl, tgt):
    side = p["side"]; e = p["raw_open"] * (1.0005 if side == "LONG" else 0.9995)
    qty = max(1, int(100000.0 / p["raw_open"])); h, l, c = p["highs"], p["lows"], p["closes"]
    if side == "LONG":
        slp, tgp = e*(1-sl/100), e*(1+tgt/100); slh, tgh = l <= slp, h >= tgp
    else:
        slp, tgp = e*(1+sl/100), e*(1-tgt/100); slh, tgh = h >= slp, l <= tgp
    fsl = int(np.argmax(slh)) if slh.any() else 10**9
    ftg = int(np.argmax(tgh)) if tgh.any() else 10**9
    xp = slp if (slh.any() and fsl <= ftg) else (tgp if tgh.any() else c[-1])
    return float(wfg.net_pnl_vectorized(np.array([e]), np.array([xp]), np.array([qty]), np.array([side]), CFG)[0])


def _pf(n): n = np.asarray(n, float); a, b = n[n > 0].sum(), -n[n < 0].sum(); return float(a/b) if b > 0 else (float('inf') if a > 0 else 0.0)


def _dbp(dts, net, nb=6000, seed=7):
    s = pd.Series(net, index=pd.to_datetime(dts)).groupby(level=0).sum().to_numpy()
    if len(s) < 3: return float('nan')
    r = np.random.default_rng(seed); return float((s[r.integers(0, len(s), size=(nb, len(s)))].mean(axis=1) <= 0).mean())


def main():
    PROP.mkdir(parents=True, exist_ok=True)
    allp = build_paths()
    rows = []
    for setup, paths in allp.items():
        for sub, mask in SUBPOPS[setup].items():
            pp = [p for p in paths if (mask is None or mask(p))]
            if len(pp) < 25:
                continue
            for sl in SL_GRID:
                for tgt in TGT_GRID:
                    rec = [(p["date"], sim(p, sl, tgt)) for p in pp]
                    dts = [r[0] for r in rec]; net = np.array([r[1] for r in rec])
                    tr = net[np.array(dts) <= TRAIN_END]; te = net[np.array(dts) >= TEST_START]
                    ted = [d for d in dts if d >= TEST_START]
                    if len(tr) < 20 or len(te) < 6:
                        continue
                    rows.append({"setup": setup, "subpop": sub, "sl": sl, "tgt": tgt,
                                 "train_n": len(tr), "train_pf": round(_pf(tr), 2), "test_n": len(te),
                                 "test_pf": round(_pf(te), 2), "test_win": round((te > 0).mean()*100, 1),
                                 "minpf": round(min(_pf(tr), _pf(te)), 2), "test_dbp": round(_dbp(ted, te), 3),
                                 "net": round(float(net.sum()), 0)})
    res = pd.DataFrame(rows)
    res.to_csv(PROP / "E_rejected_salvage.csv", index=False)
    print("\n=== best config per rejected setup (by min(train,test) PF) ===")
    for setup in SETUPS:
        s = res[res.setup == setup].sort_values("minpf", ascending=False)
        print(f"\n  --- {setup} ---")
        for _, r in s.head(6).iterrows():
            print(f"    {r['subpop']:<18} SL={r['sl']} Tgt={r['tgt']} | TRAIN n={int(r['train_n']):>3} PF={r['train_pf']:>4} | "
                  f"TEST n={int(r['test_n']):>3} PF={r['test_pf']:>4} win={r['test_win']:>4}% p={r['test_dbp']}")
        surv = s[(s.train_pf >= 1.5) & (s.test_pf >= 1.3) & (s.test_dbp < 0.10)]
        print(f"    SURVIVORS (train>=1.5 & test>=1.3 & p<0.10): {len(surv)}")
    print(f"\n[salvage] wrote E_rejected_salvage.csv -> {PROP}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
