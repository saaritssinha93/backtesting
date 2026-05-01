"""
Compare two v17q backtest output CSVs (toggle OFF vs toggle ON) and report
duplicate-row removals + any unexpected differences.
"""
from __future__ import annotations
import sys
import pandas as pd
from pathlib import Path

KEY = ["trade_date", "ticker", "side", "setup", "signal_time_ist", "entry_time_ist"]

def load(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["signal_time_ist"] = pd.to_datetime(df["signal_time_ist"], utc=True)
    df["entry_time_ist"] = pd.to_datetime(df["entry_time_ist"], utc=True)
    return df

def main(off_path: str, on_path: str) -> int:
    off = load(off_path)
    on = load(on_path)

    print(f"OFF rows: {len(off)}")
    print(f"ON  rows: {len(on)}")
    print(f"delta:    {len(on) - len(off):+d}")
    print()

    # Build keysets
    off_keys = set(map(tuple, off[KEY].astype(str).values))
    on_keys = set(map(tuple, on[KEY].astype(str).values))
    only_off = off_keys - on_keys  # rows that exist in OFF but not ON = dropped by F1
    only_on = on_keys - off_keys   # should be 0 — F1 is supposed to only remove rows
    common = off_keys & on_keys

    print(f"Rows in OFF only (dropped by F1): {len(only_off)}")
    print(f"Rows in ON only (NEW — should be 0): {len(only_on)}")
    print(f"Rows in both: {len(common)}")
    print()

    if only_on:
        print("UNEXPECTED: F1 introduced new rows. Sample:")
        sample = on[on[KEY].astype(str).apply(tuple, axis=1).isin(only_on)].head(5)
        print(sample[KEY].to_string(index=False))
        return 1

    # Verify: every dropped row has a same-(date,ticker,side) sibling in ON
    if only_off:
        dropped = off[off[KEY].astype(str).apply(tuple, axis=1).isin(only_off)].copy()
        on_sib_keys = set(map(tuple, on[["trade_date","ticker","side"]].astype(str).values))
        dropped["has_sibling"] = dropped[["trade_date","ticker","side"]].astype(str).apply(tuple, axis=1).isin(on_sib_keys)
        n_orphan = int((~dropped["has_sibling"]).sum())
        print(f"Of {len(dropped)} dropped rows, {len(dropped) - n_orphan} have a kept sibling on same (date,ticker,side).")
        if n_orphan:
            print(f"WARN: {n_orphan} dropped rows have NO kept sibling — would mean F1 dropped a unique row.")
            print((dropped[~dropped['has_sibling']][KEY]).to_string(index=False))
            return 2

        # Audit cross-check: are these the (date,ticker,side) that v17p had >=2 of?
        offdup = off.groupby(["trade_date","ticker","side"]).size()
        offdup = offdup[offdup > 1]
        print(f"\n(date,ticker,side) groups with >=2 rows in OFF baseline: {len(offdup)}")
        print(f"Total extra rows beyond first: {int(offdup.sum() - len(offdup))}")
        print(f"Rows F1 actually dropped: {len(only_off)}")
        match = int(offdup.sum() - len(offdup)) == len(only_off)
        print(f"Match: {match}")

        print("\nSample dropped rows (first 8):")
        cols_show = KEY + ["entry_price", "outcome", "pnl_pct"]
        cols_show = [c for c in cols_show if c in dropped.columns]
        print(dropped[cols_show].head(8).to_string(index=False))

        # And the sibling each one collapsed into
        print("\nSurvivors for those (date,ticker,side):")
        keys3 = dropped[["trade_date","ticker","side"]].drop_duplicates().values.tolist()
        for d, t, s in keys3[:8]:
            survivors = on[(on["trade_date"].astype(str) == d) & (on["ticker"] == t) & (on["side"] == s)]
            for _, r in survivors.iterrows():
                print(f"  KEPT  {d} {t} {s} setup={r['setup']:35s} sig={r['signal_time_ist']} qs={float(r.get('quality_score',0)):.2f}")
            for _, r in dropped[(dropped["trade_date"].astype(str) == d) & (dropped["ticker"] == t) & (dropped["side"] == s)].iterrows():
                print(f"  drop  {d} {t} {s} setup={r['setup']:35s} sig={r['signal_time_ist']} qs={float(r.get('quality_score',0)):.2f}")

    print("\nF1 validation: PASS" if not only_on else "FAIL")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
