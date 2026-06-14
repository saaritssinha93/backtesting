"""
B_setups_apply_suggested.py
Apply the diagnosis-recommended B* filters to the actual Nov->now trade data
(clean pool, fixed exit 0.70/1.50, NET of cost) and report TRAIN/TEST before vs after.
Reads B_setups_trades_nov_to_now.csv (produced by validate_B_setups_filters.py).
"""
from __future__ import annotations
from pathlib import Path
import numpy as np
import pandas as pd

PROP = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_traintest_pool\proposals")
TRAIN_END = "2026-04-30"
TEST_START = "2026-05-01"

# Diagnosis-recommended (must-test) filters, per setup.
SUGGESTED = {
    "B_AVWAP_RECLAIM_REVERSAL":
        lambda d: (d["vwap_dist_atr"] <= 0.75) & (d["vol_ratio"] <= 2.5),
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK":
        lambda d: d["regime"].astype(str) != "BULL",
    "B_HUGE_RED_FAILED_BOUNCE":
        lambda d: (d["vol_ratio"] >= 1.4) & (d["vol_ratio"] <= 2.2)
                  & (d["atr_pct"] <= 0.012) & (d["market_ret_pct"] <= -0.20),
}


def _pf(net):
    net = np.asarray(net, float)
    g, l = net[net > 0].sum(), -net[net < 0].sum()
    return float(g / l) if l > 0 else (float("inf") if g > 0 else 0.0)


def _stats(g):
    net = g["net_pnl_rs"].to_numpy()
    w, l = net[net > 0], net[net < 0]
    return dict(n=len(g), win=round((net > 0).mean() * 100, 1) if len(g) else 0.0,
                pf=round(_pf(net), 2), net=round(float(net.sum()), 0),
                avg_win=round(float(w.mean()), 0) if len(w) else 0.0,
                avg_loss=round(float(l.mean()), 0) if len(l) else 0.0,
                immfail=round(g["immediate_fail"].mean() * 100, 1) if len(g) else 0.0)


def main() -> int:
    df = pd.read_csv(PROP / "B_setups_trades_nov_to_now.csv")
    df["period"] = np.where(df["date"] <= TRAIN_END, "TRAIN",
                            np.where(df["date"] >= TEST_START, "TEST", "OTHER"))
    rows = []
    print("=" * 104)
    print("B* SUGGESTED-FILTER TRAIN/TEST (clean pool, fixed 0.70/1.50, NET of cost)")
    print("=" * 104)
    for setup, pred in SUGGESTED.items():
        s = df[df["setup"] == setup].copy()
        keep = s[pred(s).fillna(False)]
        print(f"\n### {setup}")
        for period in ("TRAIN", "TEST"):
            b = _stats(s[s["period"] == period])
            a = _stats(keep[keep["period"] == period])
            print(f"  {period:<6} BEFORE n={b['n']:>3} win={b['win']:>5}% PF={b['pf']:>5} net=Rs {b['net']:>8,.0f}"
                  f"   AFTER n={a['n']:>3} win={a['win']:>5}% PF={a['pf']:>5} net=Rs {a['net']:>8,.0f}")
            rows.append({"setup": setup, "period": period, "stage": "before", **b})
            rows.append({"setup": setup, "period": period, "stage": "after", **a})

    # combined family
    keep_all = pd.concat([df[(df["setup"] == s) & SUGGESTED[s](df[df["setup"] == s]).reindex(df.index).fillna(False)]
                          for s in SUGGESTED], ignore_index=True) if False else None
    # simpler combined: apply per-setup mask then concat
    parts = []
    for setup, pred in SUGGESTED.items():
        s = df[df["setup"] == setup].copy()
        parts.append(s[pred(s).fillna(False)])
    comb = pd.concat(parts, ignore_index=True)
    print("\n### FAMILY B* (combined, suggested filters)")
    for period in ("TRAIN", "TEST"):
        b = _stats(df[df["period"] == period])
        a = _stats(comb[comb["period"] == period])
        print(f"  {period:<6} BEFORE n={b['n']:>3} win={b['win']:>5}% PF={b['pf']:>5} net=Rs {b['net']:>8,.0f}"
              f"   AFTER n={a['n']:>3} win={a['win']:>5}% PF={a['pf']:>5} net=Rs {a['net']:>8,.0f}")
        rows.append({"setup": "FAMILY_B", "period": period, "stage": "before", **b})
        rows.append({"setup": "FAMILY_B", "period": period, "stage": "after", **a})

    out = PROP / "B_setups_suggested_result.csv"
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"\nwrote {out}")
    print("NOTE: pre-dedupe per-trade view (consistent with the diagnosis). One-ticker-per-day")
    print("dedupe + the live pipeline would trim counts slightly; small test n -> directional only.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
