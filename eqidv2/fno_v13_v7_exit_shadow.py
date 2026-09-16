"""Forward-test scaffold for the V13-v7 180-minute exit shadow.

The runner consumes the frozen V13-v5 higher-frequency order ledger and
rematerializes exact one-minute paths through the V13-v5 correctness layer. It
compares the unchanged end-of-day exit with one pre-registered 180-minute cap.
No signal, volume, OI, ranking, stop, target, or partial-allocation parameter is
changed. Optional portfolio capital is applied independently and causally to
each exit arm through the V13-v6 reservation engine.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v13_corrected_v5_backtest as v5
import fno_v13_v6_portfolio_backtest as v6_portfolio


SCHEMA_VERSION = "FNO_V13_V7_EXIT_SHADOW_V1"
DEFAULT_SOURCE = v6_portfolio.DEFAULT_SOURCE
DEFAULT_OUTPUT_ROOT = common.FNO_ROOT / "strategy_research" / "v13_corrected_v7"
CONTROL_EXIT = v5.ExitSpec(1.50, 1.075, 0.10, 2.60)
TIME180_EXIT = v5.ExitSpec(
    1.50, 1.075, 0.10, 2.60, maximum_holding_minutes=180
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def simulate_exit_arms(
    orders: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    *,
    cost_bps: float,
    capital_per_entry_rupees: float,
    leverage_factor: float,
) -> dict[str, pd.DataFrame]:
    arms: dict[str, pd.DataFrame] = {}
    for name, exit_spec in (
        ("V13_V5_CONTROL_EOD", CONTROL_EXIT),
        ("V13_V7_TIME180_SHADOW", TIME180_EXIT),
    ):
        audit = v5.simulate_scaleout(orders, paths, exit_spec, cost_bps=cost_bps)
        audit = v5.apply_fixed_capital_model(
            audit, capital_per_entry_rupees, leverage_factor
        )
        audit["exit_shadow_arm"] = name
        audit["evidence_status"] = (
            "FROZEN_CONTROL"
            if name == "V13_V5_CONTROL_EOD"
            else "PRE_REGISTERED_FORWARD_SHADOW_NOT_PROMOTED"
        )
        arms[name] = audit
    return arms


def validate_control_parity(
    source: pd.DataFrame, control: pd.DataFrame, *, tolerance: float = 1e-7
) -> dict[str, Any]:
    required = {"sid", "filled", "net_profit_rupees", "exit_reason"}
    missing = required.difference(source.columns)
    if missing:
        raise ValueError(f"Control parity source missing {sorted(missing)}")
    expected = source.loc[:, list(required)].copy()
    observed = control.loc[:, list(required)].copy()
    expected["filled"] = expected["filled"].astype(str).str.lower().eq("true") | expected[
        "filled"
    ].eq(True)
    observed["filled"] = observed["filled"].astype(bool)
    merged = expected.merge(observed, on="sid", how="outer", suffixes=("_source", "_replay"), indicator=True)
    if not merged["_merge"].eq("both").all():
        raise RuntimeError("V13-v7 control parity failed: SID set differs")
    if not merged["filled_source"].eq(merged["filled_replay"]).all():
        raise RuntimeError("V13-v7 control parity failed: fill flags differ")
    filled = merged.loc[merged["filled_source"]]
    delta = (
        pd.to_numeric(filled["net_profit_rupees_replay"], errors="coerce")
        - pd.to_numeric(filled["net_profit_rupees_source"], errors="coerce")
    ).abs()
    maximum_delta = float(delta.max()) if len(delta) else 0.0
    if not np.isfinite(maximum_delta) or maximum_delta > tolerance:
        raise RuntimeError(
            f"V13-v7 control parity failed: max P&L delta {maximum_delta}"
        )
    reason_match = filled["exit_reason_source"].fillna("").eq(
        filled["exit_reason_replay"].fillna("")
    )
    if not reason_match.all():
        raise RuntimeError("V13-v7 control parity failed: exit reasons differ")
    return {
        "passed": True,
        "source_rows": int(len(source)),
        "filled_rows": int(len(filled)),
        "maximum_absolute_pnl_delta_rupees": maximum_delta,
        "tolerance_rupees": tolerance,
    }


def load_session_days(calendar_path: Path, maximum_day: Any) -> list[str]:
    if not calendar_path.is_file():
        return []
    calendar = pd.read_csv(calendar_path)
    required = {"day", "eligible"}
    if not required.issubset(calendar.columns):
        raise ValueError(f"Session calendar missing {sorted(required - set(calendar.columns))}")
    days = pd.to_datetime(calendar["day"], errors="coerce")
    eligible = calendar["eligible"].astype(str).str.lower().eq("true")
    maximum = pd.Timestamp(maximum_day).normalize()
    return sorted(days.loc[eligible & days.le(maximum)].dt.strftime("%Y-%m-%d").dropna().unique())


def _period_frame(
    audit: pd.DataFrame,
    *,
    arm: str,
    evaluation: str,
    session_days: list[str],
) -> list[dict[str, Any]]:
    frame = audit.copy()
    frame["day"] = pd.to_datetime(frame["day"], errors="coerce").dt.strftime("%Y-%m-%d")
    periods = sorted({day[:7] for day in session_days}) or sorted(
        frame["day"].dropna().str[:7].unique()
    )
    rows = []
    executed_column = (
        "portfolio_executed" if evaluation == "CAPITAL_CONSTRAINED" else "filled"
    )
    pnl_column = (
        "portfolio_net_profit_rupees"
        if evaluation == "CAPITAL_CONSTRAINED"
        else "net_profit_rupees"
    )
    for period in [*periods, "ALL"]:
        mask = pd.Series(True, index=frame.index)
        wanted_days = session_days
        if period != "ALL":
            mask &= frame["day"].str.startswith(period)
            wanted_days = [day for day in session_days if day.startswith(period)]
        part = frame.loc[mask]
        executed = part.loc[part[executed_column].astype(bool)]
        pnl = pd.to_numeric(executed[pnl_column], errors="coerce").dropna()
        gains = float(pnl.loc[pnl > 0].sum())
        losses = float(-pnl.loc[pnl < 0].sum())
        daily = part.groupby("day", sort=True)[pnl_column].sum()
        if wanted_days:
            daily = daily.reindex(wanted_days, fill_value=0.0)
        curve = daily.cumsum()
        drawdown = curve - np.maximum.accumulate(np.r_[0.0, curve.to_numpy(float)])[1:]
        rows.append(
            {
                "arm": arm,
                "evaluation": evaluation,
                "period": period,
                "sessions": int(len(daily)),
                "selected_orders": int(len(part)),
                "executed_trades": int(len(executed)),
                "portfolio_rejected_trades": (
                    int(part["portfolio_status"].eq("REJECTED").sum())
                    if evaluation == "CAPITAL_CONSTRAINED"
                    else 0
                ),
                "wins": int((pnl > 0).sum()),
                "losses": int((pnl < 0).sum()),
                "net_profit_rupees": float(pnl.sum()),
                "profit_factor": (
                    gains / losses
                    if losses
                    else (float("inf") if gains else float("nan"))
                ),
                "maximum_drawdown_rupees": (
                    float(drawdown.min()) if len(drawdown) else 0.0
                ),
                "average_holding_minutes": (
                    float(pd.to_numeric(executed["holding_minutes"], errors="coerce").mean())
                    if len(executed)
                    else np.nan
                ),
                "first_target_hits": (
                    int(executed["first_target_hit"].astype(bool).sum())
                    if len(executed)
                    else 0
                ),
                "runner_target_hits": (
                    int(executed["runner_target_hit"].astype(bool).sum())
                    if len(executed)
                    else 0
                ),
                "full_stops": (
                    int(executed["exit_reason"].eq("FULL_STOP").sum())
                    if len(executed)
                    else 0
                ),
            }
        )
    return rows


def build_comparison(
    arms: dict[str, pd.DataFrame],
    *,
    session_days: list[str],
    portfolio_config: v6_portfolio.PortfolioConfig | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    rows: list[dict[str, Any]] = []
    outputs: dict[str, pd.DataFrame] = {}
    for arm, audit in arms.items():
        rows.extend(
            _period_frame(
                audit, arm=arm, evaluation="UNCONSTRAINED_PAIRED", session_days=session_days
            )
        )
        outputs[f"{arm}__UNCONSTRAINED"] = audit
        if portfolio_config is not None:
            constrained, _ = v6_portfolio.apply_portfolio_constraints(
                audit, portfolio_config
            )
            rows.extend(
                _period_frame(
                    constrained,
                    arm=arm,
                    evaluation="CAPITAL_CONSTRAINED",
                    session_days=session_days,
                )
            )
            outputs[f"{arm}__PORTFOLIO"] = constrained
    return pd.DataFrame(rows), outputs


def paired_trade_delta(arms: dict[str, pd.DataFrame]) -> pd.DataFrame:
    control = arms["V13_V5_CONTROL_EOD"].loc[
        lambda frame: frame["filled"].astype(bool),
        ["sid", "day", "tradingsymbol", "net_profit_rupees", "exit_reason", "holding_minutes"],
    ].copy()
    shadow = arms["V13_V7_TIME180_SHADOW"].loc[
        lambda frame: frame["filled"].astype(bool),
        ["sid", "net_profit_rupees", "exit_reason", "holding_minutes"],
    ].copy()
    paired = control.merge(shadow, on="sid", how="inner", suffixes=("_control", "_time180"))
    paired["pnl_delta_time180_minus_control_rupees"] = (
        paired["net_profit_rupees_time180"] - paired["net_profit_rupees_control"]
    )
    paired["holding_delta_minutes"] = (
        paired["holding_minutes_time180"] - paired["holding_minutes_control"]
    )
    return paired


def render_report(comparison: pd.DataFrame, paired: pd.DataFrame, source: Path) -> str:
    focus = comparison.loc[comparison["period"].isin(["2026-08", "2026-09", "ALL"])]
    changed = paired.loc[paired["pnl_delta_time180_minus_control_rupees"].abs().gt(1e-9)]
    return "\n".join(
        [
            "# V13-V7 180-Minute Exit Shadow",
            "",
            "Research shadow only. Entry selection and all non-time exit parameters remain frozen to V13-V5.",
            "",
            f"- Source ledger: `{source}`",
            "- Control: current V13-V5 end-of-day exit.",
            "- Candidate: maximum 180 minutes after actual fill.",
            "- Volume relaxation and all signal changes are explicitly excluded.",
            f"- Paired fills: {len(paired)}; trades whose P&L changes: {len(changed)}.",
            "",
            "## Comparison",
            "",
            focus.to_markdown(index=False, floatfmt=".3f"),
            "",
            "## Largest paired changes",
            "",
            changed.reindex(
                changed["pnl_delta_time180_minus_control_rupees"].abs().sort_values(ascending=False).index
            ).head(20).to_markdown(index=False, floatfmt=".3f"),
            "",
            "Promotion remains blocked until the pre-registered forward sample gates are met.",
            "",
        ]
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--session-calendar", type=Path)
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--capital-per-entry-rupees", type=float, default=100_000.0)
    parser.add_argument("--leverage-factor", type=float, default=5.0)
    parser.add_argument("--portfolio-capital-rupees", type=float)
    parser.add_argument("--max-positions", type=int)
    parser.add_argument("--max-positions-per-symbol", type=int)
    parser.add_argument("--max-gross-exposure-rupees", type=float)
    parser.add_argument("--max-open-risk-rupees", type=float)
    parser.add_argument("--run-id")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    source_path = args.source.resolve()
    orders = pd.read_csv(source_path)
    paths, quality = v5.materialize_raw_paths(orders, cutoff=v5.OFFICIAL_CUTOFF)
    arms = simulate_exit_arms(
        orders,
        paths,
        cost_bps=float(args.cost_bps),
        capital_per_entry_rupees=float(args.capital_per_entry_rupees),
        leverage_factor=float(args.leverage_factor),
    )
    control_parity = validate_control_parity(
        orders, arms["V13_V5_CONTROL_EOD"]
    )
    calendar_path = (
        args.session_calendar.resolve()
        if args.session_calendar is not None
        else source_path.parent.parent / "fno_v13_corrected_v5_session_eligibility.csv"
    )
    maximum_day = pd.to_datetime(orders["day"], errors="coerce").max()
    session_days = load_session_days(calendar_path, maximum_day)
    portfolio_config = None
    if args.portfolio_capital_rupees is not None:
        portfolio_config = v6_portfolio.PortfolioConfig(
            portfolio_capital_rupees=float(args.portfolio_capital_rupees),
            max_positions=args.max_positions,
            max_positions_per_symbol=args.max_positions_per_symbol,
            max_gross_exposure_rupees=args.max_gross_exposure_rupees,
            max_open_risk_rupees=args.max_open_risk_rupees,
        )
    comparison, output_ledgers = build_comparison(
        arms, session_days=session_days, portfolio_config=portfolio_config
    )
    paired = paired_trade_delta(arms)

    generated = common.now_ist()
    run_id = args.run_id or generated.strftime("v13_v7_exit_shadow_%Y%m%dT%H%M%S_IST")
    if any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for character in run_id):
        raise ValueError("run-id may contain only letters, digits, underscore and hyphen")
    run_dir = args.output_root.resolve() / run_id
    if run_dir.exists():
        raise FileExistsError(f"Refusing to overwrite V13-v7 shadow run: {run_dir}")
    run_dir.mkdir(parents=True)
    comparison_path = run_dir / "fno_v13_v7_exit_comparison.csv"
    paired_path = run_dir / "fno_v13_v7_paired_trade_delta.csv"
    quality_path = run_dir / "fno_v13_v7_path_quality.csv"
    report_path = run_dir / "V13_V7_EXIT_SHADOW_REPORT.md"
    common.atomic_write_csv(comparison, comparison_path)
    common.atomic_write_csv(paired, paired_path)
    common.atomic_write_csv(quality, quality_path)
    common.atomic_write_text(report_path, render_report(comparison, paired, source_path))
    ledgers = {}
    for name, ledger in output_ledgers.items():
        path = run_dir / f"{name.lower()}_trades.csv"
        common.atomic_write_csv(ledger, path)
        ledgers[name] = {"path": str(path), "sha256": _sha256(path)}
    # `arms` is the mapping of named exit ledgers, not the source trade frame.
    # Derive the run-data boundary from the immutable source ledger that both
    # arms replay, so manifest publication cannot fail after a valid replay.
    arm_days = pd.to_datetime(orders.get("day"), errors="coerce")
    data_through_date = arm_days.max().date().isoformat() if arm_days.notna().any() else None
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "promotion_status": "BLOCKED_PENDING_FORWARD_SAMPLE_GATES",
        "run_id": run_id,
        "generated_at_ist": generated.isoformat(timespec="seconds"),
        "data_through_date": data_through_date,
        "source": str(source_path),
        "source_sha256": _sha256(source_path),
        "session_calendar": (
            {"path": str(calendar_path), "sha256": _sha256(calendar_path)}
            if calendar_path.is_file()
            else None
        ),
        "cost_bps": float(args.cost_bps),
        "capital_per_entry_rupees": float(args.capital_per_entry_rupees),
        "leverage_factor": float(args.leverage_factor),
        "portfolio_config": (
            portfolio_config.__dict__ if portfolio_config is not None else None
        ),
        "exit_arms": {
            "control": CONTROL_EXIT.__dict__,
            "time180": TIME180_EXIT.__dict__,
        },
        "control_parity": control_parity,
        "outputs": {
            "comparison": {"path": str(comparison_path), "sha256": _sha256(comparison_path)},
            "paired": {"path": str(paired_path), "sha256": _sha256(paired_path)},
            "quality": {"path": str(quality_path), "sha256": _sha256(quality_path)},
            "report": {"path": str(report_path), "sha256": _sha256(report_path)},
            "ledgers": ledgers,
        },
    }
    common.atomic_write_json(run_dir / "manifest.json", manifest)
    print(comparison.to_string(index=False))
    print(f"[V13-v7 exit shadow][RUN] {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
