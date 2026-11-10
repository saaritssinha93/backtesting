"""Check V14 price-action slot reporting without rewarding tiny/empty samples."""
from __future__ import annotations

import numpy as np
import pandas as pd

from research import g14_price_action_backtest as study


def test_slot_70_requires_more_than_70_percent_and_ten_trades() -> None:
    day = "2026-10-23"
    rows = []
    for slot, side, wins in (("09:20", "LONG", 7), ("09:25", "SHORT", 8)):
        for i in range(10):
            rows.append(dict(arm="v3_control", cost_bps=9., day=day,
                             signal_ts=pd.Timestamp(f"{day} {slot}", tz="Asia/Kolkata"),
                             side=side, portfolio_net_profit_rupees=100. if i < wins else -100.))
    slots, times, coverage = study._slot_result(pd.DataFrame(rows), [day])
    at = slots.loc[slots.arm.eq("v3_control") & slots.cost_bps.eq(9.) & slots.period.eq("assessment")]
    first = at.loc[at.slot.eq("09:20") & at.side.eq("LONG")].iloc[0]
    second = at.loc[at.slot.eq("09:25") & at.side.eq("SHORT")].iloc[0]
    empty = at.loc[at.slot.eq("09:35") & at.side.eq("SHORT")].iloc[0]
    assert first.trades == 10 and first.win_rate_pct == 70 and not first.observed_gt70_with10
    assert second.trades == 10 and second.win_rate_pct == 80 and second.observed_gt70_with10
    assert empty.trades == 0 and np.isnan(empty.win_rate_pct)
    summary = coverage.loc[coverage.arm.eq("v3_control") & coverage.cost_bps.eq(9.)
                           & coverage.period.eq("assessment")].iloc[0]
    assert summary.cells_gt70_with10 == 1
    assert not summary.all_12_gt70_with10
    assert times.loc[times.arm.eq("v3_control") & times.cost_bps.eq(9.)
                     & times.period.eq("assessment") & times.slot.eq("09:20"),
                     "win_rate_pct"].iloc[0] == 70
