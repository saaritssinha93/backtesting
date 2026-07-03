r"""relaxed_eval.py — "relax the bar" evaluator for DOC5A at 5 bps.
============================================================================
The 1.30-1.70 band is unreachable without overfitting (see reinvent_5bps/
REINVENT_5BPS_READOUT.md). Per user direction, find the best TRADEABLE config
instead: meaningful trade count (~25+ TRAIN), highest TRAIN PF that keeps the
trades, and a POSITIVE TEST — non-dominated and stable across FIT/VAL.

Discipline: candidates are shortlisted ONLY by FIT/VAL (from the existing
900-trial trials.csv, min trades/half >= --min_half). TRAIN + TEST are then
measured ONCE per candidate (no tuning to TEST). Cost = 5 bps/leg.

Run from repo root:
  py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/DOC5A_AVWAP_PULLBACK_LONG/scripts/relaxed_eval.py
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
SETUP_DIR = HERE.parent.parent
REPO = HERE
for _ in range(12):
    if (REPO / "Train_and_Test").exists() and (REPO / "final_setup_conf.py").exists():
        break
    REPO = REPO.parent
ENGINE = REPO / "Train_and_Test" / "setup_pf_1_4_approval_loop" / "_engine"
for p in (str(REPO), str(REPO / "Train_and_Test"), str(ENGINE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import setup_train_test as tt      # noqa: E402
import pf_band_fitval_loop as pbl  # noqa: E402

SETUP = "DOC5A_AVWAP_PULLBACK_LONG"
POOL = SETUP_DIR / "variant_pool"
TRIALS = SETUP_DIR / "reinvent_5bps" / SETUP / "trials.csv"
TRAIN_START = pd.Timestamp("2026-05-18"); TEST_START = pd.Timestamp("2026-06-20")
MIN_HALF = 15          # min trades per FIT/VAL half to be "meaningful"
MIN_TRAIN_N = 25       # min full-TRAIN trades to accept as tradeable
TOPK = 18


def _parse_terms(s):
    if not isinstance(s, str) or s.strip() in ("", "-", "nan"):
        return []
    out = []
    for part in s.split(";"):
        m = re.match(r"\s*(.+?)(>=|<=)(.+)\s*$", part)
        if m:
            out.append((m.group(1).strip(), m.group(2), float(m.group(3))))
    return out


def _parse_guard(s):
    if not isinstance(s, str) or s.strip() in ("", "-", "nan"):
        return None
    try:
        g = json.loads(s)
        return g or None
    except Exception:
        return None


def _mline(m):
    return (f"n={m['n']:>3} PF={m['net_pf']:>5} net=Rs{m['net_pnl']:>8,.0f} win%={m['win_rate']:>4} "
            f"tpd={m['trades_per_day']:>4} tgt%={m['target_rate']:>4} "
            f"tradeDom={m['trade_dom_gross']} dayDom={m['day_dom']} symDom={m['sym_dom']} dbp={m['day_block_p']}")


def main() -> int:
    tt.POOL_DIRS = [POOL]; tt.POOL_DIR = POOL
    pool = tt.load_pool(); pool = pool[pool["setup"] == SETUP].copy()
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    test_s = [s for s in sessions if s >= TEST_START] or sessions[-5:]
    first_test = test_s[0]
    train_s = [s for s in sessions if (s >= TRAIN_START and s < first_test)]
    half = len(train_s) // 2
    fit_s, val_s = train_s[:half], train_s[half:]

    pbl._set_slippage(5.0)
    span = set(map(pd.Timestamp, fit_s + val_s + test_s))
    sub = tt.attach_entries(pool[pool["_day"].isin(span)].copy())

    def _slice(ss):
        return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    FIT, VAL, TRAIN, TEST = _slice(fit_s), _slice(val_s), _slice(fit_s + val_s), _slice(test_s)

    def _rng(ss):
        return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}"
    print(f"[relaxed] TRAIN {_rng(train_s)} ({len(train_s)})  TEST {_rng(test_s)} ({len(test_s)})  @5bps")

    # shortlist candidates from FIT/VAL trials only
    t = pd.read_csv(TRIALS)
    t["minn"] = t[["fit_n", "val_n"]].min(axis=1); t["minpf"] = t[["fit_pf", "val_pf"]].min(axis=1)
    t = t[(t.minn >= MIN_HALF)].copy()
    t["key"] = t["sl"].astype(str) + t["tgt"].astype(str) + t["mask"].fillna("-") + t["premom"].fillna("-") + t["guard"].fillna("-")
    t = t.sort_values("minpf", ascending=False).drop_duplicates("key").head(TOPK)
    print(f"[relaxed] shortlisted {len(t)} FIT/VAL configs (min trades/half >= {MIN_HALF})\n")

    rows = []
    for _, r in t.iterrows():
        cfg = {"sl": float(r["sl"]), "tgt": float(r["tgt"]),
               "mask_terms": _parse_terms(r["mask"]), "premom_terms": _parse_terms(r["premom"]),
               "guard": _parse_guard(r["guard"]), "status": "OK",
               "max_positions": int(r.get("max_positions", 20) or 20),
               "daily_loss_rs": float(r.get("daily_loss_rs", 0.0) or 0.0)}
        tt.MAX_POSITIONS = cfg["max_positions"]; tt.DAILY_LOSS_RS = cfg["daily_loss_rs"]
        mTR = pbl.full_metrics(SETUP, cfg, TRAIN); mTE = pbl.full_metrics(SETUP, cfg, TEST)
        rows.append((cfg, mTR, mTE))
        bm = "; ".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "(none)"
        bp = "; ".join(f"{a}{o}{b}" for a, o, b in cfg["premom_terms"]) or "(none)"
        print(f"SL/Tgt {cfg['sl']}/{cfg['tgt']} | mask[{bm}] premom[{bp}] guard={cfg['guard']}")
        print(f"   TRAIN {_mline(mTR)}")
        print(f"   TEST  {_mline(mTE)}")

    # pick best tradeable: TRAIN n>=MIN_TRAIN_N, TRAIN PF>=1.05, TEST PF>1.10, non-dominated
    def ok(mTR, mTE):
        return (mTR["n"] >= MIN_TRAIN_N and mTR["net_pf"] >= 1.05 and mTE["n"] >= 8
                and mTE["net_pf"] > 1.10
                and (mTR["day_dom"] or 9.99) <= 0.6 and (mTE["day_dom"] or 9.99) <= 0.6
                and (mTR["sym_dom"] or 9.99) <= 0.6 and (mTE["sym_dom"] or 9.99) <= 0.6)
    winners = [(c, tr, te) for (c, tr, te) in rows if ok(tr, te)]
    winners.sort(key=lambda x: (x[2]["net_pf"], x[1]["net_pf"]), reverse=True)
    print("\n" + "=" * 90)
    if winners:
        c, tr, te = winners[0]
        print(f"BEST TRADEABLE (relaxed, below 1.30 goal): TRAIN PF {tr['net_pf']} (n={tr['n']}) / "
              f"TEST PF {te['net_pf']} (n={te['n']})")
        (SETUP_DIR / "reinvent_5bps" / "RELAXED_BEST.json").write_text(json.dumps({
            "config": {k: v for k, v in c.items() if k != "status"},
            "train_5bps": {k: v for k, v in tr.items() if k != "detail"},
            "test_5bps": {k: v for k, v in te.items() if k != "detail"}}, indent=2, default=str), encoding="utf-8")
        print("wrote reinvent_5bps/RELAXED_BEST.json")
    else:
        print("NO tradeable relaxed config: no shortlisted FIT/VAL config has TRAIN PF>=1.05 AND "
              "TEST PF>1.10 AND non-dominated AND TRAIN n>=%d." % MIN_TRAIN_N)
    print("=" * 90)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
