r"""search_aprmay_jun_5bps.py — reinvented-DOC5C approval search on the WIDE split.
============================================================================
Split (user-specified):
  TRAIN 2026-04-01 .. 2026-05-30   (FIT = first half sessions, VAL = second half)
  TEST  2026-06-01 .. 2026-07-01   (all of June — a proper OOS month)
Cost: 5 bps/leg, statutory NSE, next-open fill. Repo pipeline (eval_family).

Search ONLY on FIT/VAL, confirm on full TRAIN, read TEST ONCE per config whose
full-TRAIN PF is in [1.30,1.70]. A PASS = TRAIN PF in band AND TEST PF>1.40 AND
>=20 TRAIN / >=12 TEST trades AND no trade/day/symbol dominates (<=0.50).

Research-only. NO conf edits. NO live trades.
"""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path
import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve()
OUTDIR = _HERE.parent.parent
POOL = OUTDIR / "reinvent_pool"
TT_DIR = None
for p in _HERE.parents:
    if (p / "setup_train_test.py").exists():
        TT_DIR = p
        break
REPO = TT_DIR.parent
ENGINE_DIR = OUTDIR.parent / "_engine"
for _p in (str(REPO), str(TT_DIR), str(ENGINE_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
import setup_train_test as tt          # noqa: E402
import pf_band_fitval_loop as eng      # noqa: E402

TRAIN_START, TRAIN_END = pd.Timestamp("2026-04-01"), pd.Timestamp("2026-05-30")
TEST_START, TEST_END = pd.Timestamp("2026-06-01"), pd.Timestamp("2026-07-01")
PF_LO, PF_HI = 1.30, 1.70
TEST_PF_MIN = 1.40
FV_FLOOR = 6
DOM_CAP = 0.50
SLIP = 5.0
ALL = ["DOC5C_GAP_RETEST_HOLD_LONG", "DOC5C_GAP_RECLAIM_LONG", "DOC5C_GAP_PULLBACK_HOLD_LONG"]


def ev(setup, cfg, df):
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    return eng.full_metrics(setup, cfg, df)


def mk(sl, tgt, mask=None, premom=None, guard=None):
    return {"sl": float(sl), "tgt": float(tgt),
            "mask_terms": [tuple(t) for t in (mask or [])],
            "premom_terms": [tuple(t) for t in (premom or [])],
            "guard": (dict(guard) if guard else None), "status": "OK",
            "max_positions": 20, "daily_loss_rs": 0.0}


def dom_ok(m):
    return (m["trade_dom_gross"] is not None and m["trade_dom_gross"] <= DOM_CAP
            and m["day_dom"] is not None and m["day_dom"] <= DOM_CAP
            and m["sym_dom"] is not None and m["sym_dom"] <= DOM_CAP)


def L(m):
    return (f"n={m['n']:>3} PF={m['net_pf']:>6.3f} net=Rs{m['net_pnl']:>7,.0f} win%={m['win_rate']:>5} "
            f"tgt%={m['target_rate']:>5} SL/T/E={m['sl_cnt']}/{m['tgt_cnt']}/{m['eod_cnt']} "
            f"dbp={m['day_block_p']} dom={m['trade_dom_gross']}/{m['day_dom']}/{m['sym_dom']}")


def load_setup(setup, FIT_s, VAL_s, TEST_s):
    tt.POOL_DIRS = [POOL]; tt.POOL_DIR = POOL
    pool = tt.load_pool()
    pool = pool[pool["setup"] == setup].copy()
    span = set(map(pd.Timestamp, list(FIT_s) + list(VAL_s) + list(TEST_s)))
    sub = pool[pool["_day"].isin(span)].copy()
    tt.SLIPPAGE_BPS = SLIP
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()
    sub = tt.attach_entries(sub)
    def sl_(ss):
        return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    return sl_(FIT_s), sl_(VAL_s), sl_(list(FIT_s) + list(VAL_s)), sl_(TEST_s)


EXITS = [(0.70, 1.00), (0.70, 1.25), (0.85, 1.00), (0.85, 1.25), (0.85, 1.50),
         (1.00, 1.00), (1.00, 1.25), (1.00, 1.50), (1.20, 1.25), (1.20, 1.50),
         (1.20, 2.00), (1.00, 0.80), (1.20, 0.80), (1.20, 0.60), (1.50, 1.00), (0.70, 0.80)]

KNOBS = {
    "retest_depth_atr>=": ("retest_depth_atr", ">=", [0.0, 0.25, 0.35, 0.4, 0.5, 0.7]),
    "orh_dist_atr<=": ("orh_dist_atr", "<=", [1.25, 1.0, 0.75, 0.5]),
    "gap_pct<=": ("gap_pct", "<=", [4.0, 3.0, 2.0, 1.5]),
    "gap_pct>=": ("gap_pct", ">=", [0.5, 0.8, 1.2]),
    "vwap_slope_atr>=": ("vwap_slope_atr", ">=", [0.0, 0.2, 0.4]),
    "vol_ratio>=": ("vol_ratio", ">=", [1.1, 1.4, 1.8, 2.2]),
    "close_loc>=": ("close_loc", ">=", [0.55, 0.65, 0.75]),
    "rs_pct>=": ("rs_pct", ">=", [0.0, 0.3, 0.6, 1.0]),
    "adx>=": ("adx", ">=", [15, 20, 25]),
    "rsi<=": ("rsi", "<=", [80, 72, 65]),
    "vwap_dist_atr<=": ("vwap_dist_atr", "<=", [3.0, 2.5, 2.0, 1.5]),
    "atr_pct<=": ("atr_pct", "<=", [0.008, 0.006, 0.005]),
    "quality_score>=": ("quality_score", ">=", [70, 85, 100]),
    "signal_minute<=": ("signal_minute", "<=", [660, 690, 720]),
}
PM = {"pre3_range_r>=": ("pre3_range_r", ">=", [0.2, 0.3]),
      "sig5_adx_calc>=": ("sig5_adx_calc", ">=", [20, 26]),
      "pre_entry_momentum_score>=": ("pre_entry_momentum_score", ">=", [55, 70])}


def best_exit(setup, FIT, VAL, TRAIN):
    rows = []
    for sl, tgt in EXITS:
        c = mk(sl, tgt)
        mf, mv, mt = ev(setup, c, FIT), ev(setup, c, VAL), ev(setup, c, TRAIN)
        s = -9 if (mf["n"] < FV_FLOOR or mv["n"] < FV_FLOOR) else min(mf["net_pf"], mv["net_pf"])
        rows.append({"sl": sl, "tgt": tgt, "fit_pf": mf["net_pf"], "val_pf": mv["net_pf"],
                     "train_n": mt["n"], "train_pf": mt["net_pf"], "_s": round(s, 3)})
    return pd.DataFrame(rows).sort_values("_s", ascending=False)


def deep(setup, FIT, VAL, TRAIN, TEST, results):
    print(f"\n########## DEEP SEARCH {setup} ##########")
    for lbl, d in (("FIT", FIT), ("VAL", VAL), ("TRAIN", TRAIN), ("TEST", TEST)):
        print(f" raw 0.70/1.25 {lbl:5s} {L(ev(setup, mk(0.7,1.25), d))}")
    bx = best_exit(setup, FIT, VAL, TRAIN)
    print("\n exit grid top8:"); print(bx.head(8).to_string(index=False))
    RSL, RTG = float(bx.iloc[0]["sl"]), float(bx.iloc[0]["tgt"])

    # knob sweep -> collect stable terms (both halves >=1.0, n>=FV_FLOOR)
    stable = []
    print("\n knob sweep (ref exit {:.2f}/{:.2f}); '*' = both halves >=1.15:".format(RSL, RTG))
    for name, (feat, op, vals) in KNOBS.items():
        for v in vals:
            c = mk(RSL, RTG, mask=[(feat, op, float(v))])
            mf, mv = ev(setup, c, FIT), ev(setup, c, VAL)
            mn = min(mf["net_pf"], mv["net_pf"])
            ok = mf["n"] >= FV_FLOOR and mv["n"] >= FV_FLOOR
            if ok and mn >= 1.15:
                print(f"   {feat}{op}{v:<7} FIT {mf['n']}/{mf['net_pf']:.2f} VAL {mv['n']}/{mv['net_pf']:.2f} *")
            if ok and mn >= 1.0:
                stable.append((feat, op, float(v)))
    for name, (feat, op, vals) in PM.items():
        for v in vals:
            c = mk(RSL, RTG, premom=[(feat, op, float(v))])
            mf, mv = ev(setup, c, FIT), ev(setup, c, VAL)
            if mf["n"] >= FV_FLOOR and mv["n"] >= FV_FLOOR and min(mf["net_pf"], mv["net_pf"]) >= 1.0:
                stable.append((feat, op, float(v)))
    # de-dup, keep at most 14 strongest structural terms
    seen = set(); uniq = []
    for t in stable:
        k = (t[0], t[1], round(t[2], 4))
        if k not in seen:
            seen.add(k); uniq.append(t)
    stable = uniq
    print(f"\n {len(stable)} FIT/VAL-stable terms feed combos")

    pmset = [None, ("pre3_range_r", ">=", 0.3), ("sig5_adx_calc", ">=", 26)]
    guards = [None, {"max_slot": "12:00"}, {"top_n": 2}, {"max_slot": "12:30", "top_n": 3}]
    combos = []; seen = set()
    terms = [None] + stable
    for i in range(len(terms)):
        for j in range(i, len(terms)):
            mask = [t for t in {terms[i], terms[j]} if t is not None]
            if len(mask) > 2:
                continue
            for pmt in pmset:
                pml = [pmt] if pmt else []
                for guard in guards:
                    for sl, tgt in EXITS:
                        key = (tuple(sorted(mask)), tuple(pml), json.dumps(guard, sort_keys=True), sl, tgt)
                        if key in seen:
                            continue
                        seen.add(key)
                        c = mk(sl, tgt, mask=mask, premom=pml, guard=guard)
                        mf, mv = ev(setup, c, FIT), ev(setup, c, VAL)
                        if mf["n"] < FV_FLOOR or mv["n"] < FV_FLOOR:
                            continue
                        combos.append((min(mf["net_pf"], mv["net_pf"]), mf, mv, c, mask, pml, guard, sl, tgt))
    combos.sort(key=lambda x: -x[0])
    print(f" {len(combos)} FIT/VAL combos (n>= {FV_FLOOR} both halves); confirming top-40 on TRAIN, TEST if in band...")

    passes = []
    inband = []
    checked = 0
    for mn, mf, mv, c, mask, pml, guard, sl, tgt in combos:
        if checked >= 40:
            break
        mt = ev(setup, c, TRAIN)
        checked += 1
        if not (PF_LO <= mt["net_pf"] <= PF_HI and mt["n"] >= 20 and dom_ok(mt)):
            continue
        mte = ev(setup, c, TEST)
        block = eng.cfg_to_conf_block(setup, "LONG", c)
        rec = {"setup": setup, "cfg": block,
               "fit": {"n": mf["n"], "pf": mf["net_pf"]}, "val": {"n": mv["n"], "pf": mv["net_pf"]},
               "train": {k: v for k, v in mt.items() if k != "detail"},
               "test": {k: v for k, v in mte.items() if k != "detail"}}
        inband.append(rec)
        ispass = (mte["net_pf"] > TEST_PF_MIN and mte["n"] >= 12 and dom_ok(mte))
        tag = "  <<< PASS" if ispass else ""
        print(f"  IN-BAND mask=[{';'.join(f'{a}{o}{b}' for a,o,b in mask) or '-'}] pm=[{';'.join(f'{a}{o}{b}' for a,o,b in pml) or '-'}] "
              f"guard={guard or '-'} exit={sl}/{tgt}")
        print(f"     FIT {mf['n']}/{mf['net_pf']:.2f} VAL {mv['n']}/{mv['net_pf']:.2f} | TRAIN {L(mt)}")
        print(f"     TEST {L(mte)}{tag}")
        if ispass:
            passes.append(rec)
    results["deep"][setup] = {"inband": inband, "passes": passes}
    return passes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--setups", default="DOC5C_GAP_RETEST_HOLD_LONG,DOC5C_GAP_RECLAIM_LONG")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    tt.POOL_DIRS = [POOL]; tt.POOL_DIR = POOL
    pool_all = tt.load_pool()
    sessions = sorted(pd.Series(pool_all["_day"].dropna().unique()))
    TRAIN_s = [s for s in sessions if TRAIN_START <= s <= TRAIN_END]
    TEST_s = [s for s in sessions if TEST_START <= s <= TEST_END]
    half = len(TRAIN_s) // 2
    FIT_s, VAL_s = TRAIN_s[:half], TRAIN_s[half:]
    def rng(ss):
        return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}" if ss else "(empty)"
    print(f"[search-v2] @ {SLIP}bps  TRAIN {rng(TRAIN_s)} ({len(TRAIN_s)}) = FIT {rng(FIT_s)}({len(FIT_s)}) + VAL {rng(VAL_s)}({len(VAL_s)})")
    print(f"[search-v2] TEST {rng(TEST_s)} ({len(TEST_s)})")
    print(f"[search-v2] pool rows/setup:\n{pool_all['setup'].value_counts().to_string()}")

    results = {"windows": {"TRAIN": rng(TRAIN_s), "FIT": rng(FIT_s), "VAL": rng(VAL_s), "TEST": rng(TEST_s),
                           "TEST_sessions": [str(pd.Timestamp(s).date()) for s in TEST_s]},
               "slippage_bps": SLIP, "deep": {}}

    # quick screen of all three
    print("\n===== SCREEN (raw @5bps) =====")
    for setup in ALL:
        FIT, VAL, TRAIN, TEST = load_setup(setup, FIT_s, VAL_s, TEST_s)
        rt, te = ev(setup, mk(0.7, 1.25), TRAIN), ev(setup, mk(0.7, 1.25), TEST)
        print(f"  {setup:<30} TRAIN n={rt['n']} PF={rt['net_pf']:.3f} | TEST n={te['n']} PF={te['net_pf']:.3f}")

    all_pass = []
    for setup in [s.strip() for s in args.setups.split(",") if s.strip()]:
        FIT, VAL, TRAIN, TEST = load_setup(setup, FIT_s, VAL_s, TEST_s)
        all_pass += deep(setup, FIT, VAL, TRAIN, TEST, results)

    results["n_pass"] = len(all_pass)
    results["passes"] = all_pass
    (OUTDIR / "scripts" / "search_aprmay_jun_5bps.json").write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    print("\n" + "=" * 70)
    print(f"TOTAL PASS candidates (TRAIN[1.30,1.70] & TEST>1.40 & meaningful & non-dominated): {len(all_pass)}")
    for r in all_pass:
        print(f"  {r['setup']} TRAIN {r['train']['n']}/{r['train']['net_pf']} TEST {r['test']['n']}/{r['test']['net_pf']}  {json.dumps(r['cfg']['mask_terms'])} exit {r['cfg']['exit']}")
    print("wrote scripts/search_aprmay_jun_5bps.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
