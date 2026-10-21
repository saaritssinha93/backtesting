"""Audit saved Nifty decisions, source hashes, trade accounting and G4 totals."""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g4_nifty_direction_backtest as study


def verify(directory):
    directory = Path(directory).resolve()
    manifest = study.base.read(directory / "manifest.json")
    for name, digest in manifest["files"].items():
        if study.base.sha256(directory / name) != digest:
            raise ValueError("Artifact hash mismatch: " + name)
    for name, digest in manifest["code_sha256"].items():
        if study.base.sha256(Path(name)) != digest:
            raise ValueError("Implementation changed: " + name)
    summary = study.base.read(directory / "summary.json")
    audit = pd.read_csv(directory / "nifty_order_audit.csv")
    if audit.sid.duplicated().any() or len(audit) != summary["order_audit_count"]:
        raise ValueError("Incomplete order audit")
    checked = 0
    for day, group in audit.groupby("day"):
        frame = pd.read_parquet(directory / "nifty_inputs" / (day + ".parquet"))
        for row in group.itertuples():
            values = study.direction_at(frame, row.confirmation_ts)
            if pd.Timestamp(values["nifty_bar_ts"]) > pd.Timestamp(row.confirmation_ts):
                raise ValueError("Future Nifty candle used")
            for mode in summary["enabled_modes"]:
                if mode == "control":
                    continue
                np.testing.assert_allclose(getattr(row, mode), values[mode], atol=1e-10, rtol=0)
                if getattr(row, mode + "_allowed") != study.allow_direction(row.side, values[mode]):
                    raise ValueError("Side decision differs")
            checked += 1
    baseline = study.base.read_frozen_csv(directory / "baseline_native.csv")
    comparison = pd.read_csv(directory / "comparison.csv")
    daily = pd.read_csv(directory / "daily_comparison.csv")
    setups = pd.read_csv(directory / "setup_results.csv")
    counts = {}
    for mode in summary["enabled_modes"]:
        book = study.base.read_frozen_csv(directory / mode / "portfolio_trades.csv")
        if set(book.sid) != set(baseline.sid):
            raise ValueError("Missing orders")
        if mode == "control":
            study.sealed.parity(book, baseline)
        else:
            allowed = book.sid.map(audit.set_index("sid")[mode + "_allowed"])
            if (book.portfolio_executed.eq(True) & ~allowed).any():
                raise ValueError("Blocked trade received capital")
        expected = np.where(book.portfolio_executed,
            book.exposure_per_entry_rupees * (book.gross_return_pct - .05) / 100, 0.)
        np.testing.assert_allclose(book.portfolio_net_profit_rupees, expected, atol=1e-6, rtol=0)
        for row in comparison.loc[comparison["mode"].eq(mode)].itertuples():
            days = summary["days"] if row.period == "all" else [
                summary["last_date"] if row.period == "last_day" else "2026-10-15" if row.period == "october_15" else "2026-10-16"]
            stats = study.bounded.stats(book, days)
            for key, value in stats.items():
                if not np.isclose(getattr(row, key), value, atol=1e-6, rtol=0):
                    raise ValueError("Summary accounting mismatch")
        by_day = book.groupby("day").portfolio_net_profit_rupees.sum().reindex(summary["days"], fill_value=0.)
        recorded = daily.loc[daily["mode"].eq(mode)].set_index("day").reindex(summary["days"])
        np.testing.assert_allclose(by_day, recorded.net_pnl_rs, atol=1e-6, rtol=0)
        setup_rows = setups.loc[setups["mode"].eq(mode)]
        if len(setup_rows) != 51 or not np.isclose(setup_rows.net_pnl_rs.sum(), by_day.sum(), atol=1e-6, rtol=0):
            raise ValueError("Setup coverage/accounting mismatch")
        counts[mode] = int(book.portfolio_executed.sum())
    return dict(audit_status="PASS", verified_artifacts=len(manifest["files"]),
        verified_code_files=len(manifest["code_sha256"]), verified_order_decisions=checked,
        sessions=summary["sessions"], setups=51, trades_by_mode=counts,
        control_parity=True, confirmation_time_causality=True, monetary_daily_setup_accounting=True,
        omitted_secondary_modes=summary["omitted_modes_missing_context"],
        run_manifest_sha256=study.base.sha256(directory / "manifest.json"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.output and args.output.exists():
        raise ValueError("Require a fresh verification file")
    result = verify(args.directory)
    if args.output:
        study.dump(args.output, result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
