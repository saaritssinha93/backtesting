"""Verify published baseline parity and that every eligible new order was admitted."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

PACKAGE = Path(__file__).resolve().parent
OUTPUT = PACKAGE / "results_full_20261103"
PUBLISHED = PACKAGE.parent / "g3_v18_20261102/results_20261102_complete"


def yes(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin(("true", "1", "1.0"))


def verify() -> dict:
    summary = json.loads((OUTPUT / "summary.json").read_text())
    assert summary["published_control_reproduction"]["state"] == "PASS"
    daily = pd.read_csv(OUTPUT / "daily_results.csv")
    reference = pd.read_csv(PUBLISHED / "daily_results.csv")
    reference = reference.loc[reference.variant.eq("control"),
                              ["day", "selected_orders", "trades", "net_pnl_rs"]]
    merged = daily.loc[daily.day.le("2026-10-30")].merge(reference, on="day", how="outer", indicator=True)
    assert len(merged) == 62 and merged._merge.eq("both").all()
    assert (merged.baseline_selected == merged.selected_orders).all()
    assert (merged.baseline_trades == merged.trades).all()
    max_difference = float((merged.baseline_pnl_rs - merged.net_pnl_rs).abs().max())
    assert max_difference < .01
    audit = pd.read_csv(OUTPUT / "no_slot_limit_selection_audit.csv", low_memory=False)
    selected, eligible, original = map(yes, (audit.v9_selected, audit.v9_filter_pass, audit.baseline_selected))
    assert selected.equals(eligible)
    assert selected[original].all()
    assert not audit.loc[selected, "signal_id"].duplicated().any()
    group = audit.loc[selected].groupby(["day", "setup_id"])
    assert group.size().equals(group.max_entries.first())
    baseline = pd.read_csv(OUTPUT / "control_trades.csv", low_memory=False)
    uncapped = pd.read_csv(OUTPUT / "no_slot_limit_trades.csv", low_memory=False)
    base_executed = set(baseline.loc[yes(baseline.portfolio_executed), "sid"].astype(int))
    new_executed = set(uncapped.loc[yes(uncapped.portfolio_executed), "sid"].astype(int))
    assert len(base_executed) == 392 and len(new_executed) == 503
    return dict(status="PASS", published_daily_days_matched=62,
        maximum_published_daily_pnl_difference_rs=max_difference,
        full_history_sessions=len(daily), eligible_selected=int(selected.sum()),
        additional_orders_selected=int((selected & ~original).sum()),
        original_orders_retained_in_selection=bool(selected[original].all()),
        baseline_executed=len(base_executed), no_slot_limit_executed=len(new_executed),
        baseline_executed_displaced_by_portfolio=len(base_executed-new_executed),
        additional_executed=len(new_executed-base_executed),
        published_reference_sha256=hashlib.sha256((PUBLISHED / "daily_results.csv").read_bytes()).hexdigest())


if __name__ == "__main__":
    result = verify()
    (OUTPUT / "verification.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
