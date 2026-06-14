"""
B_setups_validate_recs.py
Recommendation 1: push B_HUGE_C1_CLOSE_RECLAIM_BREAK (regime != BULL) through the
   purged walk-forward gate + day-clustered bootstrap.
Recommendation 2: sweep B_AVWAP_RECLAIM_REVERSAL vwap_dist_atr cuts (0.75..1.5,
   with/without vol_ratio<=2.5) to find a cut that keeps enough TEST trades.
Reads B_setups_trades_nov_to_now.csv (clean pool, fixed 0.70/1.50, net of cost).
"""
from __future__ import annotations
from pathlib import Path
import numpy as np
import pandas as pd

import walkforward_gate as wfg
from nse_intraday_costs import CostConfig

PROP = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_traintest_pool\proposals")
TRAIN_END = "2026-04-30"
TEST_START = "2026-05-01"
CFG = CostConfig()


def _pf(net):
    net = np.asarray(net, float)
    g, l = net[net > 0].sum(), -net[net < 0].sum()
    return float(g / l) if l > 0 else (float("inf") if g > 0 else 0.0)


def _day_block_p(df, n_boot=10000, seed=7):
    daily = df.groupby("date")["net_pnl_rs"].sum().to_numpy()
    if len(daily) < 3:
        return float("nan")
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(daily), size=(n_boot, len(daily)))
    return float((daily[idx].mean(axis=1) <= 0).mean())


def _qty(row):
    ps = (row["exit_price"] - row["entry_price"]) if row["side"] == "LONG" else (row["entry_price"] - row["exit_price"])
    if abs(ps) > 1e-9 and abs(row["gross_pnl_rs"]) > 1e-9:
        return max(1, int(round(row["gross_pnl_rs"] / ps)))
    return max(1, int(round(100000.0 / row["entry_price"])))


def _split(df):
    tr = df[df["date"] <= TRAIN_END]
    te = df[df["date"] >= TEST_START]
    return tr, te


def main() -> int:
    df = pd.read_csv(PROP / "B_setups_trades_nov_to_now.csv")

    # ===== REC 1: B_HUGE_C1 regime != BULL -> walk-forward gate + day-block bootstrap =====
    print("=" * 96)
    print("REC 1 — B_HUGE_C1_CLOSE_RECLAIM_BREAK (regime != BULL) through the strict gate")
    print("=" * 96)
    b = df[(df["setup"] == "B_HUGE_C1_CLOSE_RECLAIM_BREAK") & (df["regime"].astype(str) != "BULL")].copy()
    b["qty"] = b.apply(_qty, axis=1)
    gate_in = pd.DataFrame({"date": pd.to_datetime(b["date"]), "setup": b["setup"], "side": b["side"],
                            "entry_price": b["entry_price"], "exit_price": b["exit_price"], "qty": b["qty"]})
    tr, te = _split(b)
    print(f"  trades: total={len(b)}  train={len(tr)} (PF {_pf(tr['net_pnl_rs']):.2f})  "
          f"test={len(te)} (PF {_pf(te['net_pnl_rs']):.2f}, win {te['win'].mean()*100:.0f}%)")
    print(f"  full-sample day-clustered bootstrap p(net>0) = {_day_block_p(b):.4f}  "
          f"(days={b['date'].nunique()}, net=Rs {b['net_pnl_rs'].sum():,.0f})")
    for mn in (40, 20, 12):
        wf = wfg.WalkForwardConfig(train_days=60, test_days=20, embargo_days=1,
                                   global_calendar_folds=True, min_oos_trades=mn, n_bootstrap=5000)
        rep = wfg.run_gate(gate_in, wf, CFG)
        r = rep.iloc[0]
        print(f"  [gate min_oos={mn:>2}] n_oos={r['n_oos']:>3} net_pf_oos={r['net_pf_oos']} "
              f"fold_consistency={r['fold_consistency']} p={r['p_value']} overfit={r['overfit_flag']} -> {r['decision']}")

    # ===== REC 2: B_AVWAP vwap_dist_atr cut sweep =====
    print("\n" + "=" * 96)
    print("REC 2 — B_AVWAP_RECLAIM_REVERSAL vwap_dist_atr cut sweep (find a cut that keeps TEST trades)")
    print("=" * 96)
    a0 = df[df["setup"] == "B_AVWAP_RECLAIM_REVERSAL"].copy()
    print(f"  {'cut':<26}{'TRAIN n/PF':<16}{'TEST n/PF/win':<22}{'day-block p':<12}{'net Rs'}")
    sweeps = []
    for cap in (0.60, 0.75, 1.00, 1.25, 1.50, 99.0):
        for vol_cap, vlabel in ((2.5, " & vol<=2.5"), (1e9, "")):
            cond = (a0["vwap_dist_atr"] <= cap) & (a0["vol_ratio"] <= vol_cap)
            s = a0[cond.fillna(False)]
            if s.empty:
                continue
            tr, te = _split(s)
            label = f"vwap<= {cap if cap < 90 else 'inf'}{vlabel}"
            print(f"  {label:<26}"
                  f"{f'{len(tr)}/{_pf(tr['net_pnl_rs']):.2f}':<16}"
                  f"{f'{len(te)}/{_pf(te['net_pnl_rs']):.2f}/{te['win'].mean()*100 if len(te) else 0:.0f}%':<22}"
                  f"{_day_block_p(s):<12.4f}{s['net_pnl_rs'].sum():,.0f}")
            sweeps.append({"cut": label, "train_n": len(tr), "train_pf": round(_pf(tr['net_pnl_rs']), 2),
                           "test_n": len(te), "test_pf": round(_pf(te['net_pnl_rs']), 2),
                           "test_win": round(te['win'].mean()*100, 1) if len(te) else 0,
                           "day_block_p": round(_day_block_p(s), 4), "net_rs": round(s['net_pnl_rs'].sum(), 0)})
    pd.DataFrame(sweeps).to_csv(PROP / "B_AVWAP_cut_sweep.csv", index=False)
    print(f"\n  wrote {PROP / 'B_AVWAP_cut_sweep.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
