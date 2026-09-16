"""Scenario report for the V13-v6 capital-constrained portfolio engine.

This runner compares the frozen V13-v5 ledger with 1/2/3/5-slot V13-v6
portfolio scenarios.  Slot counts are capacity diagnostics, not optimized
strategy parameters.  A separate immutable run directory is created each time.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v13_v6_portfolio_backtest as portfolio


SCHEMA_VERSION = "FNO_V13_V6_RESEARCH_COMPARISON_V1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _profit_factor(pnl: pd.Series) -> float:
    values = pd.to_numeric(pnl, errors="coerce").dropna()
    gains = float(values.loc[values > 0].sum())
    losses = float(-values.loc[values < 0].sum())
    return gains / losses if losses else (float("inf") if gains else float("nan"))


def _period_metrics(
    ledger: pd.DataFrame,
    scenario: str,
    period: str,
    session_days: list[str] | None = None,
) -> dict:
    frame = ledger.copy()
    days = pd.to_datetime(frame["day"], errors="coerce")
    if period != "ALL":
        frame = frame.loc[days.dt.strftime("%Y-%m").eq(period)].copy()
    executed = frame.loc[frame["portfolio_executed"].astype(bool)]
    pnl = pd.to_numeric(executed["portfolio_net_profit_rupees"], errors="coerce")
    daily = frame.groupby("day", sort=True)["portfolio_net_profit_rupees"].sum()
    if session_days is not None:
        wanted_days = [
            day for day in session_days if period == "ALL" or day.startswith(period)
        ]
        daily = daily.reindex(wanted_days, fill_value=0.0)
    curve = daily.cumsum()
    drawdown = curve - np.maximum.accumulate(np.r_[0.0, curve.to_numpy(float)])[1:]
    peak_positions = 0
    peak_capital = 0.0
    peak_exposure = 0.0
    peak_risk = 0.0
    if not executed.empty:
        peak_positions = int(
            (
                pd.to_numeric(
                    executed["portfolio_open_positions_before"], errors="coerce"
                )
                + 1
            ).max()
        )
        peak_capital = float(
            (
                pd.to_numeric(
                    executed["portfolio_reserved_capital_before_rupees"], errors="coerce"
                )
                + pd.to_numeric(
                    executed["portfolio_trade_capital_rupees"], errors="coerce"
                )
            ).max()
        )
        peak_exposure = float(
            (
                pd.to_numeric(
                    executed["portfolio_gross_exposure_before_rupees"], errors="coerce"
                )
                + pd.to_numeric(
                    executed["portfolio_trade_exposure_rupees"], errors="coerce"
                )
            ).max()
        )
        peak_risk = float(
            (
                pd.to_numeric(
                    executed["portfolio_open_risk_before_rupees"], errors="coerce"
                )
                + pd.to_numeric(
                    executed["portfolio_trade_initial_risk_rupees"], errors="coerce"
                )
            ).max()
        )
    return {
        "scenario": scenario,
        "period": period,
        "sessions": int(len(daily)),
        "source_filled_trades": int(frame["filled"].astype(bool).sum()),
        "executed_trades": int(len(executed)),
        "rejected_trades": int(frame["portfolio_status"].eq("REJECTED").sum()),
        "average_trades_per_session": (
            float(len(executed) / len(daily))
            if len(daily)
            else np.nan
        ),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl < 0).sum()),
        "net_profit_rupees": float(pnl.sum()),
        "profit_factor": _profit_factor(pnl),
        "maximum_drawdown_rupees": float(drawdown.min()) if len(drawdown) else 0.0,
        "peak_concurrent_positions": peak_positions,
        "peak_reserved_capital_rupees": peak_capital,
        "peak_gross_exposure_rupees": peak_exposure,
        "peak_open_initial_risk_rupees": peak_risk,
    }


def unconstrained_ledger(source: pd.DataFrame) -> pd.DataFrame:
    prepared = portfolio.prepare_source_ledger(source)
    filled_capital = pd.to_numeric(
        prepared.loc[prepared["filled"], "capital_per_entry_rupees"], errors="coerce"
    ).sum()
    ledger, summary = portfolio.apply_portfolio_constraints(
        source,
        portfolio.PortfolioConfig(portfolio_capital_rupees=float(filled_capital)),
    )
    if summary["portfolio_rejected_trades"]:
        raise RuntimeError("Unconstrained reference unexpectedly rejected source fills")
    return ledger


def build_comparison(
    source: pd.DataFrame,
    slot_counts: list[int],
    capital_per_slot: float,
    session_days: list[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    if not slot_counts or any(slots <= 0 for slots in slot_counts):
        raise ValueError("slot_counts must contain positive integers")
    scenarios: dict[str, pd.DataFrame] = {"V13_V5_UNCONSTRAINED": unconstrained_ledger(source)}
    for slots in slot_counts:
        name = f"V13_V6_{slots}_SLOT"
        scenarios[name], _ = portfolio.apply_portfolio_constraints(
            source,
            portfolio.PortfolioConfig(
                portfolio_capital_rupees=capital_per_slot * slots,
                max_positions=slots,
            ),
        )
    months = sorted(pd.to_datetime(source["day"], errors="coerce").dt.strftime("%Y-%m").dropna().unique())
    rows = [
        _period_metrics(ledger, name, period, session_days)
        for name, ledger in scenarios.items()
        for period in [*months, "ALL"]
    ]
    return pd.DataFrame(rows), scenarios


def render_report(comparison: pd.DataFrame, *, source: Path, capital_per_slot: float) -> str:
    headline = comparison.loc[comparison["period"].isin(["2026-08", "2026-09"])]
    return "\n".join(
        [
            "# V13-V6 Portfolio Capacity Research",
            "",
            "V13-V5 signal selection and exits are frozen. V13-V6 changes only portfolio acceptance and capital reservation.",
            "",
            f"- Source: `{source}`",
            f"- Capital per slot: INR {capital_per_slot:,.2f}",
            "- Slot scenarios are capacity diagnostics, not optimized production parameters.",
            "- Same-time orders use confirmation/setup order and the setup's documented picker value, followed by symbol/SID tie-breaks.",
            "",
            "## August and September",
            "",
            headline.to_markdown(index=False, floatfmt=".3f"),
            "",
            "## All Periods",
            "",
            comparison.to_markdown(index=False, floatfmt=".3f"),
            "",
        ]
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=portfolio.DEFAULT_SOURCE)
    parser.add_argument("--output-root", type=Path, default=portfolio.DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--capital-per-slot-rupees", type=float)
    parser.add_argument(
        "--session-calendar",
        type=Path,
        help="Eligibility CSV used to retain eligible zero-trade sessions.",
    )
    parser.add_argument("--slot-counts", default="1,2,3,5")
    parser.add_argument("--run-id")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    source_path = args.source.resolve()
    source = pd.read_csv(source_path)
    if args.capital_per_slot_rupees is None:
        values = pd.to_numeric(
            source.loc[source["filled"].astype(str).str.lower().eq("true"), "capital_per_entry_rupees"],
            errors="coerce",
        ).dropna().unique()
        if len(values) != 1:
            raise ValueError("Cannot infer one capital-per-slot value; configure it explicitly")
        capital_per_slot = float(values[0])
    else:
        capital_per_slot = float(args.capital_per_slot_rupees)
    if not np.isfinite(capital_per_slot) or capital_per_slot <= 0:
        raise ValueError("capital-per-slot must be positive and finite")
    slot_counts = sorted({int(value) for value in args.slot_counts.split(",") if value.strip()})
    calendar_path = (
        args.session_calendar.resolve()
        if args.session_calendar is not None
        else source_path.parent.parent / "fno_v13_corrected_v5_session_eligibility.csv"
    )
    session_days: list[str] | None = None
    if calendar_path.is_file():
        calendar = pd.read_csv(calendar_path)
        eligible = calendar["eligible"].astype(str).str.lower().eq("true")
        maximum_source_day = pd.to_datetime(source["day"], errors="coerce").max()
        session_days = sorted(
            pd.to_datetime(calendar.loc[eligible, "day"], errors="coerce")
            .loc[lambda values: values.le(maximum_source_day)]
            .dt.strftime("%Y-%m-%d")
            .dropna()
            .unique()
        )
    comparison, scenarios = build_comparison(
        source, slot_counts, capital_per_slot, session_days
    )

    generated = common.now_ist()
    run_id = args.run_id or generated.strftime("v13_v6_research_%Y%m%dT%H%M%S_IST")
    if any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for character in run_id):
        raise ValueError("run-id may contain only letters, digits, underscore and hyphen")
    run_dir = args.output_root.resolve() / run_id
    if run_dir.exists():
        raise FileExistsError(f"Refusing to overwrite V13-v6 research run: {run_dir}")
    run_dir.mkdir(parents=True)
    comparison_path = run_dir / "fno_v13_v6_capacity_comparison.csv"
    report_path = run_dir / "V13_V6_CAPACITY_REPORT.md"
    common.atomic_write_csv(comparison, comparison_path)
    common.atomic_write_text(
        report_path,
        render_report(comparison, source=source_path, capital_per_slot=capital_per_slot),
    )
    scenario_hashes = {}
    for name, ledger in scenarios.items():
        path = run_dir / f"{name.lower()}_trades.csv"
        common.atomic_write_csv(ledger, path)
        scenario_hashes[name] = {"path": str(path), "sha256": _sha256(path)}
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "run_id": run_id,
        "generated_at_ist": generated.isoformat(timespec="seconds"),
        "source": str(source_path),
        "source_sha256": _sha256(source_path),
        "session_calendar": (
            {"path": str(calendar_path), "sha256": _sha256(calendar_path)}
            if calendar_path.is_file()
            else None
        ),
        "capital_per_slot_rupees": capital_per_slot,
        "slot_counts": slot_counts,
        "comparison": {"path": str(comparison_path), "sha256": _sha256(comparison_path)},
        "report": {"path": str(report_path), "sha256": _sha256(report_path)},
        "scenario_ledgers": scenario_hashes,
    }
    common.atomic_write_json(run_dir / "manifest.json", manifest)
    print(comparison.to_string(index=False))
    print(f"[V13-v6 research][RUN] {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
