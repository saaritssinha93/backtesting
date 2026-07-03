r"""iterate2.py — focused, structural, explainable iteration sweep for P_PDH.
Each entry changes ONE logical group at a time, on top of explicit hypotheses
grounded in the diagnostic. TRAIN + TEST + dominance always computed and logged.

Research-only. Reuses optimize_ppdh + setup_train_test pipeline."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_SETUP_DIR = _HERE.parent
sys.path.insert(0, str(_HERE))
import optimize_ppdh as O  # noqa: E402
import setup_train_test as tt  # noqa: E402

G = [["pre_entry_momentum_score", ">=", 75.071712], ["pre3_range_r", ">=", 0.499787]]  # baseline premom gate
BODY = [["body_pct", "<=", 0.749993]]  # baseline mask


def classify(tr, te, in_band):
    if tr["trades"] < 25:
        return "too few trades (train)"
    if tr["net_pf"] < 1.30:
        return "TRAIN PF too low"
    if tr["net_pf"] > 1.70:
        return "TRAIN PF too high / overfit risk"
    if te["trades"] < 15:
        return "too few trades (test)"
    if te["net_pf"] <= 1.40:
        return "TEST PF below 1.40"
    for k in ("top_day_net_share", "top_symbol_net_share", "top_trade_gross_share"):
        if (te.get(k) or 0) > 0.45:
            return f"one {k.split('_')[1]} dominated (test)"
    return "PASS"


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)
    tt.SLIPPAGE_BPS = 15.0
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()
    tt.POOL_DIRS = [O.POOL]; tt.POOL_DIR = O.POOL
    pool = tt.load_pool(); pool = pool[pool["setup"] == O.SETUP].copy()
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    TEST_s = sessions[-9:]; TRAIN_s = sessions[-36:-9]
    span = set(map(pd.Timestamp, list(TRAIN_s) + list(TEST_s)))
    sub = tt.attach_entries(pool[pool["_day"].isin(span)].copy())

    def sl_(ss):
        return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    TRAIN, TEST = sl_(TRAIN_s), sl_(TEST_s)

    # quantiles for structural thresholds (TRAIN only)
    q = {f: {x: float(pd.to_numeric(TRAIN[f], errors="coerce").dropna().quantile(x)) for x in (0.3, 0.5, 0.7)}
         for f in ("quality_score", "vol_ratio", "rs_pct", "close_loc") if f in TRAIN.columns}

    ITER = []
    # group A: exit on baseline gate (widen target / fix R:R)
    ITER += [
        ("A1 G+exit 0.70/2.00", "exit/SL/target", O.mk(0.70, 2.00, BODY, G)),
        ("A2 G+exit 0.70/2.50", "exit/SL/target", O.mk(0.70, 2.50, BODY, G)),
        ("A3 G+exit 0.85/2.50", "exit/SL/target", O.mk(0.85, 2.50, BODY, G)),
        ("A4 G+exit 1.00/2.50", "exit/SL/target", O.mk(1.00, 2.50, BODY, G)),
        ("A5 G+exit 0.50/0.80 (file alt)", "exit/SL/target", O.mk(0.50, 0.80, BODY, G)),
    ]
    # group B: add ONE structural selectivity term to G + 0.70/2.00
    base = (0.70, 2.00)
    ITER += [
        ("B1 +rs_pct>=0 (RS leader)", "filter/mask", O.mk(*base, BODY + [["rs_pct", ">=", 0.0]], G)),
        ("B2 +close_loc>=0.6 (close strong)", "filter/mask", O.mk(*base, BODY + [["close_loc", ">=", 0.6]], G)),
        ("B3 +vol_ratio>=2.0 (conviction)", "filter/mask", O.mk(*base, BODY + [["vol_ratio", ">=", 2.0]], G)),
        ("B4 +vol_ratio<=4.0 (avoid climax)", "filter/mask", O.mk(*base, BODY + [["vol_ratio", "<=", 4.0]], G)),
    ]
    if "quality_score" in q:
        ITER.append(("B5 +quality_score>=median", "filter/mask",
                     O.mk(*base, BODY + [["quality_score", ">=", round(q["quality_score"][0.5], 4)]], G)))
    # group C: time-of-day on G + 0.70/2.00
    ITER += [
        ("C1 +min_slot 09:45", "guard/time", O.mk(*base, BODY, G, {"min_slot": "09:45"})),
        ("C2 +max_slot 11:30 (morning only)", "guard/time", O.mk(*base, BODY, G, {"max_slot": "11:30"})),
        ("C3 +window 09:45-12:30", "guard/time", O.mk(*base, BODY, G, {"min_slot": "09:45", "max_slot": "12:30"})),
        ("C4 +top_n 1 (best/slot)", "guard/top_n", O.mk(*base, BODY, G, {"top_n": 1})),
    ]
    # group D: regime / overlay on G + 0.70/2.00
    ITER += [
        ("D1 +regime_align (don't fight tape)", "regime", O.mk(*base, BODY, G, regime_align=True)),
        ("D2 +daily_loss_rs 2500", "overlay", O.mk(*base, BODY, G, daily_loss_rs=2500.0)),
    ]
    # group E: alternative simpler structural gates (drop premom)
    ITER += [
        ("E1 mask rs>=0 & close_loc>=0.6", "filter/mask", O.mk(*base, [["rs_pct", ">=", 0.0], ["close_loc", ">=", 0.6]])),
        ("E2 mask vol>=2 & rs>=0", "filter/mask", O.mk(*base, [["vol_ratio", ">=", 2.0], ["rs_pct", ">=", 0.0]])),
        ("E3 pm sig5_adx>=25 only", "gate/pre_momentum", O.mk(*base, None, [["sig5_adx_calc", ">=", 25.0]])),
        ("E4 pm score>=85 & range_r>=0.50", "gate/pre_momentum", O.mk(*base, None,
            [["pre_entry_momentum_score", ">=", 85.0], ["pre3_range_r", ">=", 0.499787]])),
    ]

    rows = []
    for i, (name, group, cfg) in enumerate(ITER, 1):
        tr = O._m(cfg, TRAIN); te = O._m(cfg, TEST)
        in_band = 1.30 <= tr["net_pf"] <= 1.70 and tr["trades"] >= 25
        verdict = classify(tr, te, in_band)
        rows.append({
            "iter": i, "name": name, "group": group, "sl": cfg["sl"], "tgt": cfg["tgt"],
            "train_n": tr["trades"], "train_pf": tr["net_pf"], "train_net": tr["net_pnl"],
            "train_win%": tr["win_pct"], "train_sl": tr["sl_cnt"], "train_tgt": tr["tgt_cnt"], "train_eod": tr["eod_cnt"],
            "test_n": te["trades"], "test_pf": te["net_pf"], "test_net": te["net_pnl"],
            "test_win%": te["win_pct"], "test_domday": te["top_day_net_share"], "test_domsym": te["top_symbol_net_share"],
            "in_band": in_band, "verdict": verdict,
        })
        print(f"{i:>2} {name:<38} TRAIN n={tr['trades']:>3} PF={tr['net_pf']:<5} "
              f"TEST n={te['trades']:>2} PF={te['net_pf']:<5} domday={te['top_day_net_share']} -> {verdict}")
    df = pd.DataFrame(rows)
    df.to_csv(_SETUP_DIR / "iterations2.csv", index=False)
    n_pass = int((df.verdict == "PASS").sum())
    print(f"\nwrote iterations2.csv | PASS={n_pass} of {len(df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
