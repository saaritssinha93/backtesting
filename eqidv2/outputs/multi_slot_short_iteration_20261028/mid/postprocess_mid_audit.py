"""Build the final honesty audit for the frozen 12:31/13:01 SHORT grid.

This script does not select a new rule and does not simulate anything.  It
independently recomputes the five development gates from the saved result
tables, summarizes the retained baselines, and refreshes the output manifest.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


OUT = Path(__file__).resolve().parent
SETUPS = ("1231_SHORT", "1301_SHORT")
STRATEGIES = ("G3", "G7_V4_LOCK2")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def money(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}Rs {abs(value):,.2f}"


def main() -> None:
    protocol = json.loads((OUT.parent / "grid_protocol.json").read_text(encoding="utf-8"))
    selection = pd.read_csv(OUT / "development_selection.csv")
    setup = pd.read_csv(OUT / "variant_setup_metrics.csv")
    portfolio = pd.read_csv(OUT / "variant_portfolio_metrics.csv")
    reported = pd.read_csv(OUT / "selected_variant_results.csv")
    parity = json.loads((OUT / "baseline_reproduction.json").read_text(encoding="utf-8"))

    if not parity or not all(bool(row["passed"]) for row in parity):
        raise ValueError("Baseline parity is not clean")
    if selection["eligible"].astype(bool).any():
        raise ValueError("Expected no development-qualified mid-slot variant")
    if selection["selected_from_development"].astype(bool).any():
        raise ValueError("A mid-slot variant was unexpectedly selected")
    if set(reported["reported_variant"]) != {"baseline"}:
        raise ValueError("Reported result is not the retained baseline")

    development_setup = setup.loc[(setup.cost_bps == 5.0) & (setup.period == "development")]
    development_portfolio = portfolio.loc[
        (portfolio.cost_bps == 5.0) & (portfolio.period == "development")
    ]
    gate_rows: list[dict] = []
    for row in selection.itertuples(index=False):
        setup_row = development_setup.loc[
            (development_setup.setup_id == row.setup_id)
            & (development_setup.strategy == row.strategy)
            & (development_setup.variant == row.variant)
        ].iloc[0]
        portfolio_row = development_portfolio.loc[
            (development_portfolio.setup_id == row.setup_id)
            & (development_portfolio.strategy == row.strategy)
            & (development_portfolio.variant == row.variant)
        ].iloc[0]
        base_setup = development_setup.loc[
            (development_setup.setup_id == row.setup_id)
            & (development_setup.strategy == row.strategy)
            & (development_setup.variant == "baseline")
        ].iloc[0]
        base_portfolio = development_portfolio.loc[
            (development_portfolio.setup_id == row.setup_id)
            & (development_portfolio.strategy == row.strategy)
            & (development_portfolio.variant == "baseline")
        ].iloc[0]
        gates = {
            "gate_minimum_8_executed": bool(setup_row.trades >= 8),
            "gate_point_win_rate_strictly_gt_75": bool(setup_row.win_rate_pct > 75.0),
            "gate_setup_net_not_below_baseline": bool(
                setup_row.net_pnl_rs >= base_setup.net_pnl_rs - 1e-6
            ),
            "gate_losing_amount_strictly_below_baseline": bool(
                setup_row.negative_trade_sum_abs_rs
                < base_setup.negative_trade_sum_abs_rs - 1e-6
            ),
            "gate_portfolio_net_not_below_baseline": bool(
                portfolio_row.net_pnl_rs >= base_portfolio.net_pnl_rs - 1e-6
            ),
        }
        recomputed = all(gates.values())
        if recomputed != bool(row.eligible):
            raise ValueError(f"Gate recomputation mismatch: {row.setup_id}/{row.strategy}/{row.variant}")
        gate_rows.append(
            {
                "setup_id": row.setup_id,
                "strategy": row.strategy,
                "variant": row.variant,
                **gates,
                "gates_passed": sum(gates.values()),
                "eligible_all_five_gates": recomputed,
                "development_trades": int(setup_row.trades),
                "development_wins": int(setup_row.wins),
                "development_losses": int(setup_row.losses),
                "development_win_rate_pct": float(setup_row.win_rate_pct),
                "development_wilson_95_lower_pct": float(setup_row.wilson_95_lower_pct),
                "development_setup_net_pnl_rs": float(setup_row.net_pnl_rs),
                "development_setup_net_change_rs": float(
                    setup_row.net_pnl_rs - base_setup.net_pnl_rs
                ),
                "development_loss_amount_reduction_rs": float(
                    base_setup.negative_trade_sum_abs_rs
                    - setup_row.negative_trade_sum_abs_rs
                ),
                "development_portfolio_net_change_rs": float(
                    portfolio_row.net_pnl_rs - base_portfolio.net_pnl_rs
                ),
                "failure_reasons": row.failure_reasons,
                "variant_order": int(row.variant_order),
            }
        )
    gate_matrix = pd.DataFrame(gate_rows)
    gate_matrix.to_csv(OUT / "honesty_gate_matrix.csv", index=False)

    # Descriptive matrix only. It is deliberately not used to replace the
    # development choice after viewing later/all-history outcomes.
    period_names = ["development", "validation", "assessment", "later", "october", "all"]
    diagnostic = setup.loc[(setup.cost_bps == 5.0) & setup.period.isin(period_names)].copy()
    values = [
        "trades", "wins", "losses", "win_rate_pct", "wilson_95_lower_pct",
        "net_pnl_rs", "net_change_vs_baseline_rs", "negative_trade_sum_abs_rs",
    ]
    matrix = diagnostic.pivot_table(
        index=["setup_id", "strategy", "variant"], columns="period", values=values, aggfunc="first"
    )
    matrix.columns = [f"{metric}_{period}" for metric, period in matrix.columns]
    matrix.reset_index().to_csv(OUT / "diagnostic_period_matrix.csv", index=False)

    outcome_rows: list[dict] = []
    for setup_id in SETUPS:
        for strategy in STRATEGIES:
            group = gate_matrix.loc[
                (gate_matrix.setup_id == setup_id) & (gate_matrix.strategy == strategy)
            ].copy()
            closest = group.sort_values(
                ["gates_passed", "development_setup_net_pnl_rs", "development_losses", "variant_order"],
                ascending=[False, False, True, True],
                kind="stable",
            ).iloc[0]
            base_rows = reported.loc[
                (reported.setup_id == setup_id)
                & (reported.strategy == strategy)
                & (reported.cost_bps == 5.0)
            ].set_index("period")
            all_history = setup.loc[
                (setup.setup_id == setup_id)
                & (setup.strategy == strategy)
                & (setup.cost_bps == 5.0)
                & (setup.period == "all")
            ]
            max_wilson = all_history.sort_values(
                ["wilson_95_lower_pct", "trades"], ascending=False, kind="stable"
            ).iloc[0]
            outcome_rows.append(
                {
                    "setup_id": setup_id,
                    "strategy": strategy,
                    "development_selected_variant": None,
                    "reported_variant": "baseline",
                    "honest_gt75_qualifier_found": False,
                    "closest_failed_variant_diagnostic_only": closest.variant,
                    "closest_failed_gates_passed_of_5": int(closest.gates_passed),
                    "closest_failed_development_trades": int(closest.development_trades),
                    "closest_failed_development_win_rate_pct": float(
                        closest.development_win_rate_pct
                    ),
                    "closest_failed_development_wilson_lower_pct": float(
                        closest.development_wilson_95_lower_pct
                    ),
                    "closest_failed_development_setup_net_change_rs": float(
                        closest.development_setup_net_change_rs
                    ),
                    "closest_failed_development_portfolio_net_change_rs": float(
                        closest.development_portfolio_net_change_rs
                    ),
                    "closest_failed_failure_reasons": closest.failure_reasons,
                    "baseline_development_trades": int(base_rows.loc["development", "setup_trades"]),
                    "baseline_development_win_rate_pct": float(
                        base_rows.loc["development", "setup_win_rate_pct"]
                    ),
                    "baseline_all_trades": int(base_rows.loc["all", "setup_trades"]),
                    "baseline_all_win_rate_pct": float(base_rows.loc["all", "setup_win_rate_pct"]),
                    "baseline_all_wilson_lower_pct": float(
                        base_rows.loc["all", "setup_wilson_95_lower_pct"]
                    ),
                    "baseline_all_net_pnl_rs": float(base_rows.loc["all", "setup_net_pnl_rs"]),
                    "baseline_october_trades": int(base_rows.loc["october", "setup_trades"]),
                    "baseline_october_win_rate_pct": float(
                        base_rows.loc["october", "setup_win_rate_pct"]
                    ),
                    "baseline_october_net_pnl_rs": float(
                        base_rows.loc["october", "setup_net_pnl_rs"]
                    ),
                    "maximum_all_history_wilson_variant_diagnostic_only": max_wilson.variant,
                    "maximum_all_history_wilson_lower_pct": float(max_wilson.wilson_95_lower_pct),
                }
            )
    outcomes = pd.DataFrame(outcome_rows)
    outcomes.to_csv(OUT / "honesty_assessment.csv", index=False)

    cost_stress = reported.loc[
        reported.period.isin(["all", "october"]),
        [
            "setup_id", "strategy", "reported_variant", "cost_bps", "period",
            "setup_trades", "setup_wins", "setup_losses", "setup_win_rate_pct",
            "setup_wilson_95_lower_pct", "setup_net_pnl_rs",
            "setup_negative_trade_sum_abs_rs", "portfolio_net_pnl_rs",
            "portfolio_daily_close_drawdown_rs",
        ],
    ].copy()
    cost_stress.to_csv(OUT / "cost_stress_summary.csv", index=False)

    # Add the audit conclusions to the concise human report.
    report = [
        "# 12:31 and 13:01 SHORT replacement-aware audit",
        "",
        "The shared frozen grid was applied before native ranking and quota. Every replacement was simulated from one-minute paths. G7 replacements received freshly computed Nifty direction permission and the current V4 low-efficiency veto, then +0.6/+0.1 protection and the causal two-loss lock.",
        "",
        "All six baseline parity checks passed against the accepted 09:26-adjusted G3 and G7 ledgers. No active config or core file was changed.",
        "",
        "## Honest result",
        "",
        "No predeclared variant passed all five development gates for either slot in either strategy. The retained result is the current baseline in all four cases. No point estimate above 75% is claimed as a qualified improvement.",
        "",
        "| Slot | Strategy | Baseline all history (5 bps) | Baseline October (5 bps) | Closest failed development rule |",
        "|---|---|---:|---:|---|",
    ]
    for row in outcomes.itertuples(index=False):
        report.append(
            f"| {row.setup_id} | {row.strategy} | {row.baseline_all_trades} trades, "
            f"{row.baseline_all_win_rate_pct:.2f}%, {money(row.baseline_all_net_pnl_rs)} | "
            f"{row.baseline_october_trades} trades, {row.baseline_october_win_rate_pct:.2f}%, "
            f"{money(row.baseline_october_net_pnl_rs)} | {row.closest_failed_variant_diagnostic_only}: "
            f"{row.closest_failed_development_trades} trades, "
            f"{row.closest_failed_development_win_rate_pct:.2f}%, "
            f"{int(row.closest_failed_gates_passed_of_5)}/5 gates |"
        )
    report += [
        "",
        "## What failed and why",
        "",
        "- 12:31 G3 filters that printed 80-100% in development retained only 3-6 trades, below the fixed minimum of 8, and reduced development PnL. The best all-history Wilson lower bound among every tested rule was only 56.55%.",
        "- 12:31 G7 had only 3 executed development trades at baseline and zero development losing-trade amount. Because the shared loss gate requires a strict reduction, no rule can improve below zero; the small 2-3 trade 100% results therefore cannot qualify.",
        "- 13:01 G3 `extension_le_3p5` was the genuine near miss: 8/11 wins (72.73%), setup PnL +Rs 4,375.41 and full-portfolio PnL +Rs 7,925.41 versus baseline in development. It failed the strict >75% gate. Across all history it was exactly 75.00% (18/24), with a 55.10% Wilson lower bound.",
        "- 13:01 G7 did not exceed 66.67% in development under any rule. Its existing configuration already applies the 3.5 ATR extension cap, so that grid row correctly reproduces baseline rather than creating a false improvement.",
        "",
        "## Cost stress",
        "",
        "The retained baselines were rerun with cost-dependent chronology at 5, 9, and 15 bps. Win rates and PnL deteriorated materially at higher costs, especially 13:01 G7 (71.43% at 5 bps to 38.46% at 15 bps). See `cost_stress_summary.csv` for exact all-history and October values.",
        "",
        "## Statistical limit",
        "",
        "This history was repeatedly inspected and is not an independent holdout. Every 95% Wilson lower bound remained below 75%, including the descriptive 100% results with very small samples.",
        "",
        "Full evidence is in `honesty_gate_matrix.csv`, `variant_setup_metrics.csv`, `variant_portfolio_metrics.csv`, `variant_trade_details.csv`, `selected_variant_daywise.csv`, and `diagnostic_period_matrix.csv`.",
        "",
    ]
    (OUT / "report.md").write_text("\n".join(report), encoding="utf-8")

    generated = [path for path in OUT.iterdir() if path.is_file() and path.name != "artifact_manifest.json"]
    manifest = {
        "created_at_ist": datetime.now().astimezone().isoformat(),
        "scope": "BACKTEST_ONLY",
        "execution_authority": False,
        "selection_changed": False,
        "files": {path.name: sha256(path) for path in sorted(generated)},
    }
    (OUT / "artifact_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
