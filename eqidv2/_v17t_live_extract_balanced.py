# -*- coding: utf-8 -*-
"""Extract the BALANCED (per-setup PF floor 1.55) filter chains for v17t_live.

Reuses the search machinery from `_v17t_live_relax_aggressive.py` but pins
the per-setup PF target to 1.55 and prints the resulting filter dict.
"""
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from _v17t_live_relax_aggressive import (
    OUT_DIR, RUN5_CSV, CURRENT_CHAINS, N_FLOOR,
    apply_chain, metrics, search_setup,
)


PER_SETUP_FLOOR = 1.55


def main():
    df = pd.read_csv(RUN5_CSV)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.reset_index(drop=True)
    print(f"Loaded {len(df)} trades. Extracting BALANCED chains @ PF>={PER_SETUP_FLOOR}")
    print()

    new_chains = {}
    for (side, setup), original_chain in CURRENT_CHAINS.items():
        result = search_setup(df, side, setup, original_chain, [PER_SETUP_FLOOR])
        if result is None:
            new_chains[(side, setup)] = original_chain
            sub = df[(df["side"] == side) & (df["setup"] == setup)]
            mask = apply_chain(sub, original_chain)
            m = metrics(sub.loc[mask])
            print(f"  KEEP-ORIG {side:6s} {setup:35s} n={m['n']:>4d} PF={m['pf']:.2f}")
        else:
            new_chains[(side, setup)] = result["chain"]
            chain_str = " | ".join([f"{f} {d} {t:.4f}" for f, d, t in result["chain"]]) or "(no constraints)"
            print(f"  RELAX     {side:6s} {setup:35s} n={result['n']:>4d} PF={result['pf']:.2f} "
                  f"win={result['win_rate']:.1f}%")
            print(f"            {chain_str}")

    agg_keep = pd.Series(False, index=df.index)
    for (side, setup), chain in new_chains.items():
        sub_idx = df.index[(df["side"] == side) & (df["setup"] == setup)]
        sub = df.loc[sub_idx]
        mask = apply_chain(sub, chain)
        agg_keep.loc[sub.index[mask]] = True
    selected = df.loc[agg_keep].copy()
    m = metrics(selected)
    print()
    print(f"=== AGGREGATE BALANCED (per-setup floor {PER_SETUP_FLOOR}) ===")
    print(f"  trades       : {m['n']}")
    print(f"  long/short   : {(selected['side']=='LONG').sum()} / {(selected['side']=='SHORT').sum()}")
    print(f"  win rate     : {m['win_rate']:.2f}%")
    print(f"  PF           : {m['pf']:.3f}")
    print(f"  sum PnL %    : {m['sum_pnl_p']:+.2f}")
    print(f"  max DD %     : {m['max_dd']:.2f}")
    print(f"  day count    : {m['day_count']}")
    print(f"  day-win rate : {m['day_win']:.2f}%")
    print()

    print("=== V17T_DEEP_FILTER_SPEC_BALANCED (paste into v17t_live) ===")
    print("V17T_DEEP_FILTER_SPEC_BALANCED = {")
    for (side, setup), chain in new_chains.items():
        chain_d = [(f, d, round(t, 4)) for f, d, t in chain]
        print(f"    ({side!r:8s}, {setup!r:42s}): {chain_d!r},")
    print("}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
