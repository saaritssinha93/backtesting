"""
new3_validate.py — battery on the new3 finalists. The only candidate with spread (top1day~40%) is
FIRST_HOUR_HIGH_FAIL (SHORT). Its strongest gates use vwap_dist_atr<=-15 (extreme; normal ~+-3) ->
SUSPECTED feature-scaling artifact. This script: (1) prints the vwap_dist_atr distribution, (2) validates
the gate WITH drop-out + vwap_dist sensitivity (is the weird term load-bearing?), (3) validates a CLEAN
gate with no vwap_dist term, (4) confirms GAP_DOWN_FADE / FIRST_HOUR_LOW reject.
Run: py -3.12 new3_validate.py
"""
from __future__ import annotations
from pathlib import Path
import pickle
import numpy as np
import pandas as pd
import new_setups_validate_passers as V

CACHE = Path(r"C:\TradingData\eqidv2\v11_new3_paths_cache.pkl")

CFGS = {
    # the cleanest 3-term core from the search (test 38/d11, top1day 40%, p 0.088)
    "FIRST_HOUR_HIGH_FAIL": {
        "side": "SHORT",
        "gate": [("vwap_dist_atr", "<=", -15.199692), ("pre5_mom_r", "<=", 0.066522), ("adx", ">=", 25.889635)],
        "exits": [(0.9, 0.8), (0.7, 1.25), (1.1, 1.0), (0.9, 1.5), (0.7, 1.0)],
        "primary_exit": (0.9, 0.8),
        "sens": {
            ("vwap_dist_atr", "<="): [-40.0, -25.0, -15.199692, -8.0, -2.0],   # is -15 a cliff/artifact?
            ("pre5_mom_r", "<="): [0.0, 0.033, 0.066522, 0.12, 0.20],
            ("adx", ">="): [18.0, 22.0, 25.889635, 30.0, 35.0],
        },
    },
    # CLEAN alternative gate (no vwap_dist_atr) from the search (test 16/d7, top1day 43%, p 0.033)
    "FIRST_HOUR_HIGH_FAIL_CLEAN": {
        "side": "SHORT", "_setup": "FIRST_HOUR_HIGH_FAIL",
        "gate": [("ema20_slope", ">=", 0.373456), ("pre1_adx", ">=", 33.567288),
                 ("pre3_close_pos", "<=", 0.666726), ("signal_minute", ">=", 720.0)],
        "exits": [(0.9, 2.0), (0.9, 1.5), (0.7, 1.5), (1.1, 2.0), (0.9, 0.8)],
        "primary_exit": (0.9, 2.0),
        "sens": {
            ("ema20_slope", ">="): [0.0, 0.2, 0.373456, 0.6, 1.0],
            ("pre1_adx", ">="): [25.0, 30.0, 33.567288, 38.0, 44.0],
            ("signal_minute", ">="): [630.0, 690.0, 720.0, 780.0, 840.0],
        },
    },
}


def main():
    with open(CACHE, "rb") as fh:
        allp = pickle.load(fh)
    print(f"new3 cache: { {k: len(v) for k, v in allp.items()} }")

    # vwap_dist_atr distribution for FIRST_HOUR_HIGH_FAIL — is -15 a real population or a tail artifact?
    recs = allp["FIRST_HOUR_HIGH_FAIL"]
    df = pd.DataFrame([{k: v for k, v in r.items() if k not in ("highs", "lows", "closes")} for r in recs])
    v = pd.to_numeric(df["vwap_dist_atr"], errors="coerce")
    print("\n=== vwap_dist_atr distribution (FIRST_HOUR_HIGH_FAIL) ===")
    print("  quantiles:", {q: round(float(v.quantile(q)), 2) for q in (0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99)})
    print(f"  frac <= -15.2: {float((v <= -15.2).mean()):.3f}  | frac in [-3,3]: {float(v.between(-3,3).mean()):.3f}")
    print("  >> if <=-15.2 is a tiny tail (<5%) with normal mass in [-3,3], the gate is an ATR-scaling artifact.")

    for name, cfg in CFGS.items():
        setup = cfg.get("_setup", name)
        V.validate(name, cfg, {name: allp[setup]})

    # quick reject-confirm for the two longs: best-gate top1day
    print("\n=== reject-confirm (longs): best robustness gate day-concentration ===")
    import walkforward_gate as wfg
    from nse_intraday_costs import CostConfig
    CFG = CostConfig()
    def net_for_exit(recs, sl, tgt, side):
        out = np.empty(len(recs))
        for i, p in enumerate(recs):
            e = p["raw_open"] * (1.0005 if side == "LONG" else 0.9995); qty = max(1, int(100000.0 / p["raw_open"]))
            h, l, c = p["highs"], p["lows"], p["closes"]
            if side == "LONG":
                slp, tgp = e*(1-sl/100), e*(1+tgt/100); slh, tgh = l <= slp, h >= tgp
            else:
                slp, tgp = e*(1+sl/100), e*(1-tgt/100); slh, tgh = h >= slp, l <= tgp
            fsl = int(np.argmax(slh)) if slh.any() else 10**9; ftg = int(np.argmax(tgh)) if tgh.any() else 10**9
            xp = slp if (slh.any() and fsl <= ftg) else (tgp if tgh.any() else c[-1])
            out[i] = wfg.net_pnl_vectorized(np.array([e]), np.array([xp]), np.array([qty]), np.array([side]), CFG)[0]
        return out
    for setup, gate, ex in [
        ("GAP_DOWN_FADE_RECLAIM", lambda d: (d["rs_pct"] >= 2.819) & (d["pre1_adx"] >= 28.17) & (d["sig5_rsi_dir"] <= 73.75), (0.9, 1.5)),
        ("FIRST_HOUR_LOW_RECLAIM", lambda d: (d["rs_pct"] <= -0.9377) & (d["signal_minute"] <= 735) & (d["quality_score"] >= 55.99), (1.1, 1.25)),
    ]:
        rc = allp[setup]; d = pd.DataFrame([{k: v for k, v in r.items() if k not in ("highs", "lows", "closes")} for r in rc])
        is_te = d["date"].to_numpy() >= "2026-05-01"
        net = net_for_exit(rc, ex[0], ex[1], "LONG"); m = gate(d).to_numpy() & is_te
        g = d[m].copy(); g["net"] = net[m]; byday = g.groupby("date")["net"].sum().sort_values(ascending=False); tot = byday.sum()
        print(f"  {setup}: test n={int(m.sum())} d{g['date'].nunique()} net Rs{tot:,.0f} top1day={round(byday.iloc[0]/tot*100) if tot>0 else 'na'}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
