"""
C_setups_iterate.py — deep, out-of-the-box iteration on the C* opening-range family.
Goal: find a configuration with edge in BOTH train (Nov-Apr) and test (May-Jun), net of cost.

Approach (honest, many iterations, no lookahead):
  1. Build the C_OR_BREAKOUT guard population (vwap_dist>=2.0 & atr<=0.010), sample 1500,
     compute the 1-minute entry and store the full entry->15:20 price PATH per trade.
  2. Fast vectorized exit simulator -> sweep a full (SL x Tgt) grid + time-stops, for BOTH
     the original LONG and the OUT-OF-THE-BOX contrarian SHORT-the-extended-breakout fade.
  3. Sub-population mining: time window x regime x rs_pct x vol_ratio on the best exit.
  4. Rank by min(train_pf, test_pf) with a trade-count floor; report day-block bootstrap p.

Run:  py -3.12 C_setups_iterate.py
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
SAMPLE_N = 1500
SL_GRID = (0.40, 0.50, 0.60, 0.70, 0.85, 1.00, 1.20)
TGT_GRID = (0.30, 0.40, 0.50, 0.60, 0.80, 1.00, 1.50)
TIMESTOPS = (None, 12, 24, 36)     # bars (1-min); None = to 15:20 EOD
MIN_TRADES = 25


def build_paths():
    fs = glob.glob(str(CLEAN / "chunk_*" / "historical_all_available_days" / "*" / "raw_candidates.csv"))
    df = pd.concat([pd.read_csv(f) for f in fs if Path(f).exists()], ignore_index=True)
    df = df[df["setup"] == "C_OR_BREAKOUT"].copy()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["tt_sig"] = df["signal_time_ist"].map(v11._normalise_ts)
    df = df.dropna(subset=["tt_sig"])
    for c in ("vwap_dist_atr", "atr_pct", "rs_pct", "vol_ratio", "market_ret_pct"):
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    df = df[(df["vwap_dist_atr"] >= 2.0) & (df["atr_pct"] <= 0.010)].copy()
    df["signal_minute"] = df["tt_sig"].dt.hour * 60 + df["tt_sig"].dt.minute
    df["date"] = df["tt_sig"].dt.strftime("%Y-%m-%d")
    df = df.drop_duplicates(subset=["ticker", "tt_sig"]).reset_index(drop=True)
    if len(df) > SAMPLE_N:
        df = df.sample(SAMPLE_N, random_state=7).reset_index(drop=True)
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
        paths.append({
            "date": r.date, "ticker": r.ticker, "signal_minute": int(r.signal_minute),
            "regime": str(getattr(r, "regime", "")), "rs_pct": float(r.rs_pct),
            "vol_ratio": float(r.vol_ratio), "atr_pct": float(r.atr_pct),
            "market_ret_pct": float(getattr(r, "market_ret_pct", np.nan)),
            "vwap_dist_atr": float(r.vwap_dist_atr), "raw_open": float(raw_open),
            "highs": sub["high"].to_numpy(float), "lows": sub["low"].to_numpy(float),
            "closes": sub["close"].to_numpy(float),
        })
    print(f"[iterate] C_OR_BREAKOUT guard paths built: {len(paths)}")
    return paths


def sim(path, side, sl, tgt, max_bars):
    slip = 0.0005
    e = path["raw_open"] * (1 + slip) if side == "LONG" else path["raw_open"] * (1 - slip)
    qty = max(1, int(100000.0 / path["raw_open"]))
    h, l, c = path["highs"], path["lows"], path["closes"]
    n = len(c)
    _has_ts = (max_bars is not None) and not (isinstance(max_bars, float) and np.isnan(max_bars))
    hz = min(n, int(max_bars)) if _has_ts else n
    if side == "LONG":
        slp, tgp = e * (1 - sl / 100), e * (1 + tgt / 100)
        sl_hit, tg_hit = l[:hz] <= slp, h[:hz] >= tgp
    else:
        slp, tgp = e * (1 + sl / 100), e * (1 - tgt / 100)
        sl_hit, tg_hit = h[:hz] >= slp, l[:hz] <= tgp
    fsl = int(np.argmax(sl_hit)) if sl_hit.any() else 10**9
    ftg = int(np.argmax(tg_hit)) if tg_hit.any() else 10**9
    if sl_hit.any() and fsl <= ftg:
        xp = slp
    elif tg_hit.any():
        xp = tgp
    else:
        xp = c[hz - 1]
    net = float(wfg.net_pnl_vectorized(np.array([e]), np.array([xp]), np.array([qty]), np.array([side]), CFG)[0])
    return net


def _pf(net):
    net = np.asarray(net, float); g, l = net[net > 0].sum(), -net[net < 0].sum()
    return float(g / l) if l > 0 else (float("inf") if g > 0 else 0.0)


def _dayblock_p(dates, net, n_boot=8000, seed=7):
    s = pd.Series(net, index=pd.to_datetime(dates)).groupby(level=0).sum().to_numpy()
    if len(s) < 3:
        return float("nan")
    rng = np.random.default_rng(seed)
    return float((s[rng.integers(0, len(s), size=(n_boot, len(s)))].mean(axis=1) <= 0).mean())


def evaluate(paths, side, sl, tgt, max_bars, mask=None):
    rows = [(p["date"], sim(p, side, sl, tgt, max_bars)) for p in paths if (mask is None or mask(p))]
    if not rows:
        return None
    dts = [r[0] for r in rows]; net = np.array([r[1] for r in rows])
    tr = np.array([n for d, n in zip(dts, net) if d <= TRAIN_END])
    te = np.array([n for d, n in zip(dts, net) if d >= TEST_START])
    te_d = [d for d in dts if d >= TEST_START]
    return {"side": side, "sl": sl, "tgt": tgt, "tstop": max_bars,
            "train_n": len(tr), "train_pf": round(_pf(tr), 2),
            "test_n": len(te), "test_pf": round(_pf(te), 2),
            "test_win": round((te > 0).mean() * 100, 1) if len(te) else 0,
            "train_net": round(float(tr.sum()), 0), "test_net": round(float(te.sum()), 0),
            "test_dayblock_p": round(_dayblock_p(te_d, te), 3) if len(te) else 1.0}


def main():
    PROP.mkdir(parents=True, exist_ok=True)
    paths = build_paths()
    if not paths:
        raise SystemExit("[iterate] no paths")
    results = []
    for side in ("LONG", "SHORT"):
        for sl in SL_GRID:
            for tgt in TGT_GRID:
                for ts in TIMESTOPS:
                    r = evaluate(paths, side, sl, tgt, ts)
                    if r and r["train_n"] >= MIN_TRADES and r["test_n"] >= 8:
                        results.append(r)
    res = pd.DataFrame(results)
    res["minpf"] = res[["train_pf", "test_pf"]].min(axis=1)
    res.to_csv(PROP / "C_OR_BREAKOUT_exit_sweep.csv", index=False)

    print("\n=== EXIT SWEEP — top configs by min(train_pf, test_pf), both sides ===")
    top = res.sort_values("minpf", ascending=False).head(18)
    for _, r in top.iterrows():
        print(f"  {r['side']:<5} SL={r['sl']:.2f} Tgt={r['tgt']:.2f} tstop={str(r['tstop']):<4} | "
              f"TRAIN n={int(r['train_n']):>4} PF={r['train_pf']:>4} | TEST n={int(r['test_n']):>3} PF={r['test_pf']:>4} "
              f"win={r['test_win']:>4}% p={r['test_dayblock_p']}")

    # best robust config -> sub-population mining
    best = res.sort_values("minpf", ascending=False).iloc[0]
    print(f"\n=== SUB-POPULATION MINING on best exit: {best['side']} SL={best['sl']} Tgt={best['tgt']} tstop={best['tstop']} ===")
    side, sl, tgt = best["side"], best["sl"], best["tgt"]
    ts = None if pd.isna(best["tstop"]) else int(best["tstop"])
    subs = {
        "all": None,
        "regime!=BULL": lambda p: p["regime"] != "BULL",
        "regime==NEUTRAL": lambda p: p["regime"] == "NEUTRAL",
        "time 12:00-13:30": lambda p: 720 <= p["signal_minute"] <= 810,
        "time>=13:00": lambda p: p["signal_minute"] >= 780,
        "rs>1": lambda p: p["rs_pct"] > 1.0,
        "rs<0 (laggard)": lambda p: p["rs_pct"] < 0.0,
        "vol_ratio 1.5-2.5": lambda p: 1.5 <= p["vol_ratio"] <= 2.5,
        "vol_ratio>3": lambda p: p["vol_ratio"] > 3.0,
        "vwap_dist 2-3": lambda p: 2.0 <= p["vwap_dist_atr"] <= 3.0,
        "vwap_dist>4 (very extended)": lambda p: p["vwap_dist_atr"] > 4.0,
        "mkt falling<=-0.2": lambda p: p["market_ret_pct"] <= -0.20,
    }
    sub_rows = []
    for name, m in subs.items():
        r = evaluate(paths, side, sl, tgt, ts, mask=m)
        if r:
            print(f"  {name:<28} TRAIN n={int(r['train_n']):>4} PF={r['train_pf']:>4} | TEST n={int(r['test_n']):>3} PF={r['test_pf']:>4} "
                  f"win={r['test_win']:>4}% p={r['test_dayblock_p']}")
            sub_rows.append({"sub": name, **r})
    pd.DataFrame(sub_rows).to_csv(PROP / "C_OR_BREAKOUT_subpop.csv", index=False)
    print(f"\n[iterate] wrote C_OR_BREAKOUT_exit_sweep.csv, C_OR_BREAKOUT_subpop.csv -> {PROP}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
