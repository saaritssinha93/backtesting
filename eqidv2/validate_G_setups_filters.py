"""
validate_G_setups_filters.py — deep diagnosis of the G* family (v11).
G_HIGHER_HIGH_BREAK (LONG, ~750) is the only G setup with a real population;
G_LOWER_LOW_BREAK (SHORT) has n=5 (insufficient); G_DRIVE_CONTINUATION_* have 0.

For G_HIGHER_HIGH_BREAK: build entry->EOD 1-min PATHS, then
  1. baseline fixed production exit (0.90/1.50), train/test, pre-momentum ON/OFF
  2. one-variable bucket analysis (regime, market_ret, vol_ratio, vwap_dist, rs, time, close_loc)
  3. sub-population x exit sweep (SL x Tgt) to find any train+test-positive cell
NET of NSE cost, no lookahead, day-block bootstrap.
Run:  py -3.12 validate_G_setups_filters.py
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
SETUPS = {"G_HIGHER_HIGH_BREAK": "LONG", "G_LOWER_LOW_BREAK": "SHORT"}
PROD_EXIT = {"G_HIGHER_HIGH_BREAK": (0.90, 1.50), "G_LOWER_LOW_BREAK": (0.85, 0.90)}
# production pre-momentum gate (v11 PRE_ENTRY_MOMENTUM_SETUP_GATES)
PM_TERMS = {"G_HIGHER_HIGH_BREAK": (("pre3_close_pos", "<=", 0.985417), ("sig5_rsi_dir", "<=", 67.878))}
SL_GRID = (0.50, 0.70, 0.90, 1.10)
TGT_GRID = (0.80, 1.00, 1.25, 1.50, 2.00)


def _band(v, edges):
    for i in range(len(edges) - 1):
        if edges[i] <= v < edges[i + 1]:
            return f"[{edges[i]},{edges[i+1]})"
    return f">={edges[-1]}" if v >= edges[-1] else f"<{edges[0]}"


def build_paths():
    stt.POOL_DIRS = [CLEAN]
    pool = stt.load_pool().rename(columns={"tt_sig_ts": "tt_sig"})
    out = {}
    for setup, side in SETUPS.items():
        e = pool[pool["setup"].astype(str) == setup]
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
            p = {"date": d.strftime("%Y-%m-%d"), "signal_minute": int(d.hour * 60 + d.minute),
                 "regime": str(getattr(r, "regime", "")), "rs_pct": float(getattr(r, "rs_pct", np.nan)),
                 "market_ret_pct": float(getattr(r, "market_ret_pct", np.nan)), "vol_ratio": float(getattr(r, "vol_ratio", np.nan)),
                 "vwap_dist_atr": float(getattr(r, "vwap_dist_atr", np.nan)), "close_loc": float(getattr(r, "close_loc", np.nan)),
                 "raw_open": float(raw_open), "side": side,
                 "highs": sub["high"].to_numpy(float), "lows": sub["low"].to_numpy(float), "closes": sub["close"].to_numpy(float),
                 "premom_pass": True}
            if setup in PM_TERMS:
                fill = raw_open * (1.0005 if side == "LONG" else 0.9995)
                stop = fill * (1 - 0.9 / 100) if side == "LONG" else fill * (1 + 0.9 / 100)
                feats, reason = v11._pre_entry_momentum_features_v11(r.ticker, side, fill, stop, ets, d)
                p["premom_pass"] = (not reason) and v11._eval_pre_momentum_terms(feats, PM_TERMS[setup])[0]
            paths.append(p)
        out[setup] = paths
        print(f"[G] {setup}: {len(paths)} paths built")
    return out


def sim(p, sl, tgt):
    side = p["side"]; e = p["raw_open"] * (1.0005 if side == "LONG" else 0.9995)
    qty = max(1, int(100000.0 / p["raw_open"])); h, l, c = p["highs"], p["lows"], p["closes"]
    if side == "LONG":
        slp, tgp = e * (1 - sl / 100), e * (1 + tgt / 100); slh, tgh = l <= slp, h >= tgp
    else:
        slp, tgp = e * (1 + sl / 100), e * (1 - tgt / 100); slh, tgh = h >= slp, l <= tgp
    fsl = int(np.argmax(slh)) if slh.any() else 10 ** 9
    ftg = int(np.argmax(tgh)) if tgh.any() else 10 ** 9
    xp = slp if (slh.any() and fsl <= ftg) else (tgp if tgh.any() else c[-1])
    return float(wfg.net_pnl_vectorized(np.array([e]), np.array([xp]), np.array([qty]), np.array([side]), CFG)[0])


def _pf(n):
    n = np.asarray(n, float); a, b = n[n > 0].sum(), -n[n < 0].sum()
    return float(a / b) if b > 0 else (float('inf') if a > 0 else 0.0)


def _dbp(dts, net, nb=6000, seed=7):
    s = pd.Series(net, index=pd.to_datetime(dts)).groupby(level=0).sum().to_numpy()
    if len(s) < 3:
        return float('nan')
    r = np.random.default_rng(seed)
    return float((s[r.integers(0, len(s), size=(nb, len(s)))].mean(axis=1) <= 0).mean())


def _split(pp, sl, tgt):
    rec = [(p["date"], sim(p, sl, tgt)) for p in pp]
    dts = np.array([r[0] for r in rec]); net = np.array([r[1] for r in rec])
    tr, te = net[dts <= TRAIN_END], net[dts >= TEST_START]
    ted = dts[dts >= TEST_START]
    return tr, te, ted, net, dts


def main():
    PROP.mkdir(parents=True, exist_ok=True)
    allp = build_paths()
    rows_bucket, rows_sweep, summary = [], [], []

    for setup, side in SETUPS.items():
        paths = allp[setup]
        sl0, tgt0 = PROD_EXIT[setup]
        if len(paths) < 25:
            summary.append(f"{setup}: n={len(paths)} INSUFFICIENT (need >=25) -> cannot validate")
            continue

        # --- 1. baseline + premom on/off at production exit ---
        for label, pp in [("ALL", paths),
                           ("premom_ON", [p for p in paths if p["premom_pass"]]),
                           ("premom_OFF", [p for p in paths if not p["premom_pass"]])]:
            if len(pp) < 10:
                continue
            tr, te, ted, net, _ = _split(pp, sl0, tgt0)
            summary.append(f"{setup:20} {label:11} exit {sl0}/{tgt0} | TRAIN n={len(tr):>3} PF={_pf(tr):.2f} "
                           f"| TEST n={len(te):>3} PF={_pf(te):.2f} win={(te>0).mean()*100:.0f}% p={_dbp(ted,te):.3f} "
                           f"| net Rs{net.sum():,.0f}")

        # --- 2. one-variable bucket analysis (fixed production exit) ---
        BUCKETS = {
            "regime": lambda p: p["regime"],
            "market_ret": lambda p: _band(p["market_ret_pct"], [-1.0, -0.3, 0.0, 0.3]),
            "vol_ratio": lambda p: _band(p["vol_ratio"], [1.5, 2.5, 3.5, 5.0]),
            "vwap_dist_atr": lambda p: _band(p["vwap_dist_atr"], [0.5, 1.5, 2.5, 3.5]),
            "rs_pct": lambda p: _band(p["rs_pct"], [0.0, 0.5, 1.0, 2.0]),
            "close_loc": lambda p: _band(p["close_loc"], [0.6, 0.75, 0.9]),
            "time": lambda p: _band(p["signal_minute"], [570, 615, 660, 750]),
        }
        for var, fn in BUCKETS.items():
            keys = sorted({fn(p) for p in paths})
            for k in keys:
                pp = [p for p in paths if fn(p) == k]
                if len(pp) < 20:
                    continue
                tr, te, ted, net, _ = _split(pp, sl0, tgt0)
                if len(tr) < 12:
                    continue
                rows_bucket.append({"setup": setup, "variable": var, "bucket": k, "n": len(pp),
                                    "train_n": len(tr), "train_pf": round(_pf(tr), 2),
                                    "test_n": len(te), "test_pf": round(_pf(te), 2) if len(te) else np.nan,
                                    "full_pf": round(_pf(net), 2), "net_rs": round(float(net.sum()), 0),
                                    "day_block_p": round(_dbp(np.array([p["date"] for p in pp]), net), 3)})

        # --- 3. sub-population x exit sweep ---
        SUBPOPS = {
            "all": None,
            "less_ext_vwap<2.5": lambda p: p["vwap_dist_atr"] < 2.5,
            "near_vwap<1.5": lambda p: p["vwap_dist_atr"] < 1.5,
            "regimeNEUTRAL": lambda p: p["regime"] == "NEUTRAL",
            "regimeTREND": lambda p: p["regime"] == "TREND",
            "market>=0": lambda p: p["market_ret_pct"] >= 0,
            "rs>=1": lambda p: p["rs_pct"] >= 1.0,
            "vol2.5-5": lambda p: 2.5 <= p["vol_ratio"] <= 5.0,
            "premom_ON": lambda p: p["premom_pass"],
            "premom_OFF": lambda p: not p["premom_pass"],
            "less_ext+premomOFF": lambda p: p["vwap_dist_atr"] < 2.5 and not p["premom_pass"],
        }
        for sub, mask in SUBPOPS.items():
            pp = [p for p in paths if (mask is None or mask(p))]
            if len(pp) < 25:
                continue
            for sl in SL_GRID:
                for tgt in TGT_GRID:
                    tr, te, ted, net, _ = _split(pp, sl, tgt)
                    if len(tr) < 20 or len(te) < 6:
                        continue
                    rows_sweep.append({"setup": setup, "subpop": sub, "sl": sl, "tgt": tgt,
                                       "train_n": len(tr), "train_pf": round(_pf(tr), 2),
                                       "test_n": len(te), "test_pf": round(_pf(te), 2),
                                       "test_win": round((te > 0).mean() * 100, 1),
                                       "minpf": round(min(_pf(tr), _pf(te)), 2),
                                       "test_dbp": round(_dbp(ted, te), 3), "net": round(float(net.sum()), 0)})

    pd.DataFrame(rows_bucket).to_csv(PROP / "G_setups_bucket_analysis.csv", index=False)
    sweep = pd.DataFrame(rows_sweep)
    sweep.to_csv(PROP / "G_setups_exit_subpop_sweep.csv", index=False)

    print("\n=== BASELINE (production exit) + premom on/off ===")
    for s in summary:
        print("  " + s)

    print("\n=== bucket analysis (production exit) — cells with full_pf>=1.2 (sorted) ===")
    if rows_bucket:
        bdf = pd.DataFrame(rows_bucket).sort_values("full_pf", ascending=False)
        for _, r in bdf[bdf.full_pf >= 1.2].head(20).iterrows():
            print(f"  {r['setup']:20} {r['variable']:13} {str(r['bucket']):14} n={int(r['n']):>3} "
                  f"train_pf={r['train_pf']:>4} test_pf={r['test_pf']:>5} full={r['full_pf']:>4} p={r['day_block_p']}")

    print("\n=== exit x subpop sweep — best by min(train,test) PF per setup ===")
    for setup in SETUPS:
        s = sweep[sweep.setup == setup].sort_values("minpf", ascending=False) if len(sweep) else sweep
        if not len(s):
            continue
        print(f"\n  --- {setup} ---")
        for _, r in s.head(8).iterrows():
            print(f"    {r['subpop']:<18} SL={r['sl']} Tgt={r['tgt']} | TRAIN n={int(r['train_n']):>3} PF={r['train_pf']:>4} "
                  f"| TEST n={int(r['test_n']):>3} PF={r['test_pf']:>4} win={r['test_win']:>4}% p={r['test_dbp']}")
        surv = s[(s.train_pf >= 1.5) & (s.test_pf >= 1.3) & (s.test_dbp < 0.10)]
        print(f"    SURVIVORS (train>=1.5 & test>=1.3 & p<0.10): {len(surv)}")
    print(f"\n[G] wrote G_setups_bucket_analysis.csv + G_setups_exit_subpop_sweep.csv -> {PROP}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
