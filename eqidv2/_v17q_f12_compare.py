"""
Compare a baseline run (F12 OFF) vs an F12 ON run.
Identifies trades whose outcome flipped between the two runs.
"""
from __future__ import annotations
import sys
import pandas as pd

KEY = ["trade_date", "ticker", "side", "setup", "signal_time_ist", "entry_time_ist"]

def load(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["signal_time_ist"] = pd.to_datetime(df["signal_time_ist"], utc=True)
    df["entry_time_ist"] = pd.to_datetime(df["entry_time_ist"], utc=True)
    return df

def metrics(df: pd.DataFrame) -> dict:
    n = len(df)
    pnl = pd.to_numeric(df["pnl_pct"], errors="coerce").fillna(0.0)
    pnl_price = pd.to_numeric(df.get("pnl_pct_price", df["pnl_pct"]), errors="coerce").fillna(0.0)
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    target = (df["outcome"] == "TARGET").sum()
    sl = (df["outcome"] == "SL").sum()
    eod = (df["outcome"] == "EOD").sum()
    pf_num = wins.sum()
    pf_den = abs(losses.sum()) if (losses < 0).any() else 0
    pf = (pf_num / pf_den) if pf_den > 0 else float("inf")
    return dict(
        n=n,
        target=int(target), sl=int(sl), eod=int(eod),
        win_rate_target=target / n * 100 if n else 0,
        win_rate_pnl=int((pnl > 0).sum()) / n * 100 if n else 0,
        sum_pnl_pct=float(pnl.sum()),
        sum_pnl_price=float(pnl_price.sum()),
        pf=pf,
    )

def main(off_path: str, on_path: str) -> int:
    off = load(off_path)
    on = load(on_path)

    print(f"OFF rows: {len(off)}")
    print(f"ON  rows: {len(on)}")
    if len(off) != len(on):
        print("WARN: F12 changed row count -- expected only outcome flips, not adds/removes")

    # Inner join on trade key
    off_idx = off.set_index(off[KEY].astype(str).apply(tuple, axis=1))
    on_idx = on.set_index(on[KEY].astype(str).apply(tuple, axis=1))
    common = off_idx.index.intersection(on_idx.index)
    print(f"Trades present in both: {len(common)}")

    a = off_idx.loc[common].sort_index()
    b = on_idx.loc[common].sort_index()

    # Outcome flip table
    cross = pd.crosstab(a["outcome"], b["outcome"], rownames=["OFF"], colnames=["ON"])
    print("\nOutcome flip matrix (rows = OFF outcome, cols = ON outcome):")
    print(cross)
    n_flipped = int(((a["outcome"].values != b["outcome"].values)).sum())
    print(f"\nTotal outcome flips: {n_flipped} / {len(common)} ({n_flipped/len(common)*100:.1f}%)")

    print("\n=== Metrics OFF (F12 disabled, baseline) ===")
    m_off = metrics(off)
    for k, v in m_off.items():
        print(f"  {k}: {v}")
    print("\n=== Metrics ON (F12 enabled) ===")
    m_on = metrics(on)
    for k, v in m_on.items():
        print(f"  {k}: {v}")

    print("\n=== Delta (ON - OFF) ===")
    for k in m_off:
        if isinstance(m_off[k], (int, float)):
            print(f"  {k:20s}: {m_on[k] - m_off[k]:+.4f}")

    # Sample of trades flipped TARGET -> SL
    flipped = a[a["outcome"].values != b["outcome"].values].copy()
    flipped["new_outcome"] = b["outcome"].values
    print("\n=== First 10 flipped trades ===")
    cols = ["trade_date", "ticker", "side", "setup", "entry_time_ist", "outcome", "new_outcome"]
    if not flipped.empty:
        print(flipped.reset_index(drop=True).head(10)[cols].to_string(index=False))

    # By setup
    if not flipped.empty:
        print("\n=== Flips by setup ===")
        sb = flipped.groupby(["setup", "outcome", "new_outcome"]).size()
        print(sb.head(30))

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
