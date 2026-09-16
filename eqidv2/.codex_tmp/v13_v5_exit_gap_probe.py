"""Audit whether any selected one-minute exit gaps through a stop level."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import fno_v13_corrected_v3_backtest as v3
import fno_v13_corrected_v5_backtest as v5


signals, _, days, _, _, _ = v5.load_market(
    date(2026, 9, 3), rebuild_cache=False, refresh_eligibility=False
)
orders = {"native": v5.select_orders(signals, v3.active_setups())}
orders.update(
    {name: v5.select_orders(signals, v5.profile_setups(profile)) for name, profile in v5.PROFILES.items()}
)
union = pd.concat(orders.values(), ignore_index=True).drop_duplicates("sid")
paths, _ = v5.materialize_raw_paths(union, cutoff=v5.OFFICIAL_CUTOFF)

for name, current_orders in orders.items():
    if name == "native":
        audit = v5.simulate_native(current_orders, paths, cost_bps=5.0)
    else:
        audit = v5.simulate_scaleout(current_orders, paths, v5.PROFILES[name].exit, cost_bps=5.0)
    found = []
    for row in audit.loc[audit["filled"]].itertuples(index=False):
        if row.exit_reason not in {"STOP", "FULL_STOP", "T1_THEN_BREAKEVEN"}:
            continue
        is_long = row.side == "LONG"
        level = (
            row.entry_price
            if row.exit_reason == "T1_THEN_BREAKEVEN"
            else row.entry_price * (1 - row.initial_stop_pct / 100 if is_long else 1 + row.initial_stop_pct / 100)
        )
        bar_open = float(paths[int(row.sid)]["open"][int(row.exit_path_index)])
        adverse = bar_open < level if is_long else bar_open > level
        if adverse:
            found.append(
                {
                    "sid": row.sid,
                    "day": row.day,
                    "symbol": row.tradingsymbol,
                    "side": row.side,
                    "reason": row.exit_reason,
                    "level": level,
                    "open": bar_open,
                    "adverse_bps": abs(bar_open / level - 1) * 10000,
                }
            )
    print(name, "gap_stop_exits", len(found))
    if found:
        print(pd.DataFrame(found).to_string(index=False))
