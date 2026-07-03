r"""analyze.py — diagnostic: confirm the top FIT/VAL Optuna trials on TRAIN+TEST
UNCONDITIONALLY (not only when in band), so we can see the full PF landscape and
classify why P_PDH does/doesn't reach TRAIN[1.30,1.70] with TEST>1.40.

Research-only. Reuses optimize_ppdh helpers + setup_train_test pipeline."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_SETUP_DIR = _HERE.parent
sys.path.insert(0, str(_HERE))
import optimize_ppdh as O  # noqa: E402
import setup_train_test as tt  # noqa: E402


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

    def reparse(s):
        out = []
        if isinstance(s, str) and s != "-":
            for tk in s.split(";"):
                for op in (">=", "<=", "==", "!="):
                    if op in tk:
                        a, b = tk.split(op); out.append([a, op, float(b)]); break
        return out

    tr = pd.read_csv(_SETUP_DIR / "optuna_trials.csv")
    tr = tr.sort_values("score", ascending=False).head(120)
    rows = []
    for _, r in tr.iterrows():
        guard = None if (not isinstance(r["guard"], str) or r["guard"] == "-") else json.loads(r["guard"])
        cfg = O.mk(r["sl"], r["tgt"], reparse(r["mask"]), reparse(r["premom"]), guard)
        m_tr = O._m(cfg, TRAIN); m_te = O._m(cfg, TEST)
        rows.append({
            "sl": r["sl"], "tgt": r["tgt"], "mask": r["mask"], "premom": r["premom"], "guard": r["guard"],
            "fit_pf": r["fit_pf"], "val_pf": r["val_pf"], "score": r["score"],
            "train_n": m_tr["trades"], "train_pf": m_tr["net_pf"], "train_net": m_tr["net_pnl"],
            "train_dom_day": m_tr["top_day_net_share"], "train_dom_sym": m_tr["top_symbol_net_share"],
            "test_n": m_te["trades"], "test_pf": m_te["net_pf"], "test_net": m_te["net_pnl"],
            "test_dom_day": m_te["top_day_net_share"],
        })
    out = pd.DataFrame(rows)
    out.to_csv(_SETUP_DIR / "optuna_confirmed.csv", index=False)
    print(f"confirmed {len(out)} top trials -> optuna_confirmed.csv")
    print("\n=== distribution of TRAIN PF among confirmed ===")
    print(out["train_pf"].describe().round(3).to_string())
    print("\n=== configs with TRAIN PF >= 1.30 (any) ===")
    hi = out[out.train_pf >= 1.30].sort_values("test_pf", ascending=False)
    cols = ["sl", "tgt", "mask", "premom", "train_n", "train_pf", "test_n", "test_pf", "test_net", "test_dom_day"]
    print(f"count={len(hi)}")
    print(hi[cols].head(25).to_string(index=False))
    print("\n=== configs with TRAIN PF in [1.30,1.70] AND train_n>=25 ===")
    band = out[(out.train_pf >= 1.30) & (out.train_pf <= 1.70) & (out.train_n >= 25)].sort_values("test_pf", ascending=False)
    print(f"count={len(band)}")
    print(band[cols].head(25).to_string(index=False))
    print("\n=== top by TEST PF among train_n>=20 (regardless of train band) ===")
    t2 = out[out.train_n >= 20].sort_values("test_pf", ascending=False)
    print(t2[cols].head(20).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
