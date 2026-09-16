"""Read-only development-first probe for a defensible conservative V13-v5 profile."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import fno_v13_corrected_v3_backtest as v3
import fno_v13_corrected_v5_backtest as v5


through_day = date(2026, 9, 3)
signals, _, days, _, _, _ = v5.load_market(
    through_day, rebuild_cache=False, refresh_eligibility=False
)
setups = v3.active_setups()
orders = v5.select_orders(signals, setups)
paths, _ = v5.materialize_raw_paths(orders, cutoff=v5.OFFICIAL_CUTOFF)
splits = v5.split_days(days)


def flattened_metrics(audit: pd.DataFrame) -> dict[str, float]:
    row: dict[str, float] = {}
    for split in ("TRAIN", "VALIDATION", "PSEUDO_TEST", "ALL"):
        metrics = v5.metrics(audit, splits[split], label=split)
        for key in (
            "executed_trades",
            "win_rate_pct",
            "target_hit_rate_pct",
            "profit_factor",
            "net_profit_pct",
            "expectancy_pct",
            "maximum_drawdown_pct",
        ):
            row[f"{split.lower()}_{key}"] = metrics[key]
    return row


rows: list[dict[str, object]] = []
exclusions: list[tuple[str, ...]] = [()]
exclusions.extend((setup.setup_id,) for setup in setups)
exclusions.extend(
    [
        ("0936_LONG", "0946_LONG"),
        ("0936_LONG", "0941_LONG"),
        ("0941_LONG", "0946_LONG"),
    ]
)
for excluded in exclusions:
    current_orders = orders.loc[~orders["setup_id"].isin(excluded)].copy()
    for t1 in (1.05, 1.075, 1.10):
        for maximum_holding in (None, 180, 210):
            spec = v5.ExitSpec(1.50, t1, 0.20, 2.60, maximum_holding_minutes=maximum_holding)
            audit = v5.simulate_scaleout(current_orders, paths, spec, cost_bps=5.0)
            rows.append(
                {
                    "excluded": "+".join(excluded) if excluded else "NONE",
                    "t1_pct": t1,
                    "maximum_holding_minutes": maximum_holding or "EOD_1515",
                    **flattened_metrics(audit),
                }
            )

out = pd.DataFrame(rows)
out.to_csv(Path(__file__).with_name("conservative_probe.csv"), index=False)

# Rank using TRAIN+VALIDATION only. Pseudo-test fields are printed only after ranking.
candidate = out.loc[
    out["train_profit_factor"].gt(1.0)
    & out["validation_profit_factor"].gt(1.0)
    & out["train_net_profit_pct"].gt(0.0)
    & out["validation_net_profit_pct"].gt(0.0)
    & out["all_executed_trades"].lt(83)
]
candidate = candidate.sort_values(
    [
        "validation_maximum_drawdown_pct",
        "validation_profit_factor",
        "train_profit_factor",
        "validation_net_profit_pct",
    ],
    ascending=[False, False, False, False],
)
columns = [
    "excluded",
    "t1_pct",
    "maximum_holding_minutes",
    "train_executed_trades",
    "train_profit_factor",
    "train_net_profit_pct",
    "train_maximum_drawdown_pct",
    "validation_executed_trades",
    "validation_profit_factor",
    "validation_net_profit_pct",
    "validation_maximum_drawdown_pct",
    "pseudo_test_executed_trades",
    "pseudo_test_profit_factor",
    "pseudo_test_net_profit_pct",
    "pseudo_test_maximum_drawdown_pct",
    "all_executed_trades",
    "all_win_rate_pct",
    "all_target_hit_rate_pct",
    "all_profit_factor",
    "all_net_profit_pct",
    "all_maximum_drawdown_pct",
]
print(candidate[columns].head(25).to_string(index=False))
