"""Independent accounting and artifact checks for a completed V18 result."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v18_20261102.run import PACKAGE, dump, sha


def true(series):
    return series.astype(str).str.lower().isin(("true", "1", "1.0"))


def verify(folder: Path) -> dict:
    folder = folder.resolve()
    if not folder.is_relative_to(PACKAGE):
        raise ValueError("Only this new package's results may be verified")
    manifest = json.loads((folder / "manifest.json").read_text())
    for name, digest in manifest["files"].items():
        path = folder / name
        if path.parent != folder or sha(path) != digest:
            raise ValueError(f"Artifact hash differs: {name}")
    summary = json.loads((folder / "summary.json").read_text())
    overall = pd.read_csv(folder / "overall_comparison.csv")
    daily = pd.read_csv(folder / "daily_results.csv")
    monthly = pd.read_csv(folder / "monthly_results.csv")
    if summary["days"] != sorted(set(summary["days"])) or summary["sessions"] != len(summary["days"]):
        raise ValueError("Calendar is not unique and chronological")
    if summary["last_date"] != summary["days"][-1]:
        raise ValueError("Last result date differs from calendar")
    checks = []
    for variant in summary["variants"]:
        trades = pd.read_csv(folder / f"{variant}_trades.csv", low_memory=False)
        audit = pd.read_csv(folder / f"{variant}_selection_audit.csv", low_memory=False)
        if trades.sid.duplicated().any() or trades.signal_id.duplicated().any():
            raise ValueError(f"Duplicate trade identity: {variant}")
        if set(trades.sid) != set(audit.loc[true(audit.v9_selected), "sid"]):
            raise ValueError(f"Selection and execution inventories differ: {variant}")
        filled = true(trades.filled)
        executed = true(trades.portfolio_executed)
        if (executed & ~filled).any():
            raise ValueError(f"Unfilled trade executed: {variant}")
        if not set(trades.day.astype(str)).issubset(summary["days"]):
            raise ValueError(f"Trade outside calendar: {variant}")
        signal, confirmation, entry = [pd.to_datetime(trades[col], utc=True) for col in ("signal_ts", "confirmation_ts", "entry_ts")]
        if not confirmation.eq(signal + pd.Timedelta(minutes=1)).all():
            raise ValueError(f"Confirmation chronology mismatch: {variant}")
        if not (entry.loc[filled] > confirmation.loc[filled]).all():
            raise ValueError(f"Entry used confirmation candle: {variant}")
        exit_times = pd.to_datetime(trades.exit_ts, utc=True)
        if not (entry.loc[filled].dt.date.eq(exit_times.loc[filled].dt.date)).all():
            raise ValueError(f"Position carried across a session: {variant}")
        actual = trades.loc[executed, "portfolio_net_profit_rupees"].sum()
        own = overall.loc[overall.variant.eq(variant) & overall.period.eq("all")].set_index("cost_bps")
        own_daily = daily.loc[daily.variant.eq(variant)]
        if own_daily.day.tolist() != summary["days"]:
            raise ValueError(f"Missing zero-trade calendar day: {variant}")
        totals = [actual, own.loc[5, "net_pnl_rs"], own_daily.net_pnl_rs.sum(), monthly.loc[monthly.variant.eq(variant), "net_pnl_rs"].sum()]
        if not np.allclose(totals, actual, atol=1e-6, rtol=0):
            raise ValueError(f"P&L accounting does not reconcile: {variant} {totals}")
        if not own.trades.eq(executed.sum()).all():
            raise ValueError(f"Cost stress unexpectedly changed admission: {variant}")
        for cost in (9, 15):
            # Original G3 uses fixed 100,000 capital *5 exposure per executed trade.
            expected = actual - int(executed.sum()) * 500_000 * (cost - 5) / 10_000
            if not np.isclose(own.loc[cost, "net_pnl_rs"], expected, atol=1e-6, rtol=0):
                raise ValueError(f"Cost stress reconciliation differs: {variant}/{cost}")
        checks.append(dict(variant=variant, selected=len(trades), executed=int(executed.sum()),
                           net_pnl_rs=float(actual), chronology_pass=True, selection_pass=True,
                           pnl_reconciliation_pass=True, cost_sensitivity_pass=True))
    # Other work may edit unrelated files in the shared workspace. Do not
    # revert those changes or claim global immutability. All runner writes
    # are constrained to this package; independently check consumed engines.
    provenance = json.loads((folder / "source_provenance.json").read_text())
    consumed = set(provenance.get("legacy_code", {}))
    changes = summary["preservation_check"].get("changes", [])
    critical = [row for row in changes if row["path"] in consumed or "g3" in Path(row["path"]).name.lower()]
    if critical:
        raise ValueError(f"Consumed G3 source changed during task: {critical}")
    return dict(result=str(folder), artifact_hashes_verified=len(manifest["files"]),
                sessions=len(summary["days"]), passed=True, checks=checks,
                g3_inputs_unchanged=True, observed_unrelated_workspace_changes=changes)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=Path)
    parser.add_argument("--receipt", type=Path, default=PACKAGE / "verification_20261102.json")
    args = parser.parse_args()
    if not args.receipt.resolve().is_relative_to(PACKAGE):
        raise ValueError("Receipt must stay in the new directory")
    result = verify(args.folder)
    if args.receipt.exists():
        raise FileExistsError(args.receipt)
    dump(args.receipt, result)
    print(json.dumps(result, indent=2))
