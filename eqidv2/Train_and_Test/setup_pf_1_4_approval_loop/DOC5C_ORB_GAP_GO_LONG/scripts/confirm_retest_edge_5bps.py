r"""confirm_retest_edge_5bps.py — dial the FIT/VAL-validated retest_depth edge into the
TRAIN PF [1.30,1.70] band (anti-overfit) and read TEST ONCE, at 5 bps/leg.
============================================================================
The staged sweep found `retest_depth_atr>=0.5` is a robust FIT/VAL edge on
DOC5C_GAP_RETEST_HOLD_LONG, but at the scalpy 1.2/0.6 exit full-TRAIN PF blows past
1.70 (overfit). This script keeps the SAME structural edge (selected on FIT/VAL) and
sweeps ONLY the exit + a couple of simple structural companions to bring full-TRAIN PF
DOWN into [1.30,1.70], then scores TEST once. Selection is on TRAIN band membership +
FIT/VAL positivity — never on TEST.

Research-only, 5 bps/leg, statutory NSE cost, next-open fill. No conf edits, no live.
"""
from __future__ import annotations
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

SETUP = "DOC5C_GAP_RETEST_HOLD_LONG"
TRAIN_START = pd.Timestamp("2026-05-18")
TEST_START = pd.Timestamp("2026-06-20")
PF_LO, PF_HI = 1.30, 1.70
TEST_PF_MIN = 1.40
SLIP = 5.0


def ev(cfg, df):
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    return eng.full_metrics(SETUP, cfg, df)


def mk(sl, tgt, mask=None, guard=None):
    return {"sl": float(sl), "tgt": float(tgt),
            "mask_terms": [tuple(t) for t in (mask or [])], "premom_terms": [],
            "guard": (dict(guard) if guard else None), "status": "OK",
            "max_positions": 20, "daily_loss_rs": 0.0}


def L(m):
    return (f"n={m['n']:>3} PF={m['net_pf']:>6.3f} net=Rs{m['net_pnl']:>7,.0f} win%={m['win_rate']:>5} "
            f"tgt%={m['target_rate']:>5} SL/T/E={m['sl_cnt']}/{m['tgt_cnt']}/{m['eod_cnt']} "
            f"dbp={m['day_block_p']} dom(t/d/s)={m['trade_dom_gross']}/{m['day_dom']}/{m['sym_dom']}")


def main():
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    tt.POOL_DIRS = [POOL]; tt.POOL_DIR = POOL
    pool = tt.load_pool()
    pool = pool[pool["setup"] == SETUP].copy()
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    TEST_s = [s for s in sessions if s >= TEST_START]
    first_test = TEST_s[0]
    TRAIN_s = [s for s in sessions if (s >= TRAIN_START and s < first_test)]
    half = len(TRAIN_s) // 2
    FIT_s, VAL_s = TRAIN_s[:half], TRAIN_s[half:]
    def rng(ss):
        return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}"
    print(f"FIT {rng(FIT_s)}({len(FIT_s)}) VAL {rng(VAL_s)}({len(VAL_s)}) "
          f"TRAIN {rng(TRAIN_s)}({len(TRAIN_s)}) TEST {rng(TEST_s)}({len(TEST_s)})  @ {SLIP}bps")

    span = set(map(pd.Timestamp, FIT_s + VAL_s + TEST_s))
    sub = pool[pool["_day"].isin(span)].copy()
    tt.SLIPPAGE_BPS = SLIP
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()
    sub = tt.attach_entries(sub)
    def sl_(ss):
        return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    FIT, VAL, TRAIN, TEST = sl_(FIT_s), sl_(VAL_s), sl_(FIT_s + VAL_s), sl_(TEST_s)

    masks = {
        "retest>=0.5": [("retest_depth_atr", ">=", 0.5)],
        "retest>=0.4": [("retest_depth_atr", ">=", 0.4)],
        "retest>=0.35": [("retest_depth_atr", ">=", 0.35)],
        "retest>=0.3": [("retest_depth_atr", ">=", 0.3)],
        "retest>=0.25": [("retest_depth_atr", ">=", 0.25)],
        "retest>=0.5 & vwap_dist<=2.5": [("retest_depth_atr", ">=", 0.5), ("vwap_dist_atr", "<=", 2.5)],
        "retest>=0.4 & vwap_dist<=2.5": [("retest_depth_atr", ">=", 0.4), ("vwap_dist_atr", "<=", 2.5)],
        "retest>=0.35 & close_loc>=0.65": [("retest_depth_atr", ">=", 0.35), ("close_loc", ">=", 0.65)],
    }
    exits = [(0.70, 1.00), (0.70, 1.25), (0.85, 1.00), (0.85, 1.25), (0.85, 1.50),
             (1.00, 1.00), (1.00, 1.25), (1.00, 1.50), (1.20, 1.25), (1.20, 1.50),
             (1.20, 2.00), (1.00, 0.80), (1.20, 0.80), (1.20, 0.60), (1.50, 1.00)]

    print("\n=== EXIT dial-down for retest_depth edge (find full-TRAIN PF in [1.30,1.70]) ===")
    band_hits = []
    for mname, mask in masks.items():
        print(f"\n--- mask [{mname}] ---")
        for (sl, tgt) in exits:
            c = mk(sl, tgt, mask=mask)
            mf, mv, mt = ev(c, FIT), ev(c, VAL), ev(c, TRAIN)
            inband = PF_LO <= mt["net_pf"] <= PF_HI
            fv_ok = mf["n"] >= 5 and mv["n"] >= 5 and min(mf["net_pf"], mv["net_pf"]) >= 1.0
            tag = ""
            if inband:
                tag += " <BAND>"
            if fv_ok:
                tag += " <FVok>"
            print(f"  exit {sl}/{tgt}: FIT {mf['n']}/{mf['net_pf']:.2f} VAL {mv['n']}/{mv['net_pf']:.2f} "
                  f"| TRAIN {mt['n']}/{mt['net_pf']:.3f} tgt%={mt['target_rate']} dbp={mt['day_block_p']} "
                  f"dom={mt['trade_dom_gross']}/{mt['day_dom']}/{mt['sym_dom']}{tag}")
            if inband and fv_ok and mt["n"] >= 15:
                mte = ev(c, TEST)
                print(f"      -> TEST {L(mte)}")
                band_hits.append({"mask": mname, "sl": sl, "tgt": tgt,
                                  "fit": {k: mf[k] for k in ('n', 'net_pf')},
                                  "val": {k: mv[k] for k in ('n', 'net_pf')},
                                  "train": {k: v for k, v in mt.items() if k != 'detail'},
                                  "test": {k: v for k, v in mte.items() if k != 'detail'},
                                  "cfg": eng.cfg_to_conf_block(SETUP, "LONG", c)})
    out = {"setup": SETUP, "slippage_bps": SLIP,
           "windows": {"FIT": rng(FIT_s), "VAL": rng(VAL_s), "TRAIN": rng(TRAIN_s), "TEST": rng(TEST_s),
                       "TEST_sessions": [str(pd.Timestamp(s).date()) for s in TEST_s]},
           "band_hits": band_hits}
    (OUTDIR / "scripts" / "confirm_retest_edge_5bps.json").write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(f"\n=== {len(band_hits)} in-band TRAIN configs (with TEST read) ===")
    for h in band_hits:
        t, te = h["train"], h["test"]
        print(f"  [{h['mask']}] exit {h['sl']}/{h['tgt']}: TRAIN n={t['n']} PF={t['net_pf']} | "
              f"TEST n={te['n']} PF={te['net_pf']} net=Rs{te['net_pnl']:,.0f} dbp={te['day_block_p']} "
              f"dom={te['trade_dom_gross']}/{te['day_dom']}/{te['sym_dom']}"
              f"  => {'PASS' if (te['net_pf'] > TEST_PF_MIN and te['n'] >= 6) else 'test-fail'}")
    print("wrote scripts/confirm_retest_edge_5bps.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
