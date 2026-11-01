"""Consolidate the ten requested slot audits into one sealed evidence package.

The final locked decision is intentionally distinct from a development-only
selection.  A development rule that fails the later chronology stays visible
as a diagnostic, while accepted-ledger baseline remains the final result.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
OUT = Path(__file__).resolve().parent
ACCEPTED = Path(
    r"C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_lock_backtest/"
    r"runs/2026-10-28/20261028T143059782924_f86f1197_0926_short_wick020_lock2"
)

SETUPS = (
    "0931_LONG",
    "0931_SHORT",
    "1001_LONG",
    "1051_LONG",
    "1111_SHORT",
    "1146_SHORT",
    "1226_SHORT",
    "1231_SHORT",
    "1301_SHORT",
    "1321_SHORT",
)
STRATEGIES = ("G3", "G7_V4_LOCK2")
BATCHES = {
    "early": ("0931_SHORT", "1226_SHORT"),
    "missing_early_long": ("0931_LONG", "1001_LONG"),
    "missing_midday": ("1051_LONG", "1111_SHORT", "1146_SHORT"),
    "mid": ("1231_SHORT", "1301_SHORT"),
    "late": ("1321_SHORT",),
}
STANDARD_BATCHES = tuple(name for name in BATCHES if name != "late")
PERIOD_ORDER = {
    "all": 0,
    "development": 1,
    "validation": 2,
    "assessment": 3,
    "final_sessions": 4,
    "later": 5,
    "october": 6,
}
VARIANT_ORDER = {
    "baseline": 0,
    "wick_ge_0p10": 1,
    "wick_ge_0p20": 2,
    "wick_ge_0p30": 3,
    "extension_le_3p5": 4,
    "extension_le_3p0": 5,
    "extension_le_2p5": 6,
    "body_le_0p90": 7,
    "body_le_0p80": 8,
    "down_move_le_0p75": 9,
    "move_magnitude_le_0p75": 9,
    "down_move_le_0p50": 10,
    "move_magnitude_le_0p50": 10,
    "wick_ge_0p20_and_extension_le_3p0": 11,
}

CANONICAL_METRICS = (
    "sessions",
    "selected_opportunities",
    "trades",
    "wins",
    "losses",
    "flats",
    "win_rate_pct",
    "wilson_95_lower_pct",
    "wilson_95_upper_pct",
    "net_pnl_rs",
    "negative_trade_sum_abs_rs",
    "daily_close_drawdown_rs",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def bools(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False).astype(bool)
    return values.fillna(False).astype(str).str.strip().str.lower().isin(
        {"1", "true", "t", "yes", "y"}
    )


def wilson(wins: int, trades: int) -> tuple[float, float]:
    if trades <= 0:
        return math.nan, math.nan
    z = 1.959963984540054
    p = wins / trades
    denominator = 1 + z * z / trades
    centre = p + z * z / (2 * trades)
    margin = z * math.sqrt(p * (1 - p) / trades + z * z / (4 * trades * trades))
    return 100 * (centre - margin) / denominator, 100 * (centre + margin) / denominator


def drawdown(daily_values: pd.Series) -> float:
    if len(daily_values) == 0:
        return 0.0
    cumulative = daily_values.astype(float).cumsum().to_numpy()
    running_peak = np.maximum.accumulate(np.r_[0.0, cumulative])[1:]
    return float(np.max(running_peak - cumulative))


def validate_batch(batch: str) -> dict:
    directory = ROOT / batch
    manifest_path = directory / "artifact_manifest.json"
    parity_path = directory / "baseline_reproduction.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing final batch manifest: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    declared = manifest.get("files")
    if not isinstance(declared, dict) or not declared:
        raise ValueError(f"Invalid files map in {manifest_path}")
    checked = []
    for relative, expected in sorted(declared.items()):
        path = directory / relative
        if not path.is_file():
            raise FileNotFoundError(f"Manifest member is missing: {path}")
        actual = sha256(path)
        if actual != expected:
            raise ValueError(f"Manifest hash mismatch: {path}")
        checked.append(relative)
    parity = json.loads(parity_path.read_text(encoding="utf-8"))
    if not isinstance(parity, list) or not parity:
        raise ValueError(f"Invalid baseline parity file: {parity_path}")
    failed = [row for row in parity if not bool(row.get("passed"))]
    if failed:
        raise ValueError(f"Baseline parity failed in {batch}: {failed}")
    unlisted = sorted(
        path.name
        for path in directory.iterdir()
        if path.is_file() and path.name != "artifact_manifest.json" and path.name not in declared
    )
    return {
        "batch": batch,
        "directory": str(directory),
        "manifest": str(manifest_path),
        "manifest_sha256": sha256(manifest_path),
        "declared_files_verified": len(checked),
        "baseline_parity_checks_passed": len(parity),
        "unlisted_files": unlisted,
    }


def canonicalize(source: pd.DataFrame, batch: str, source_file: str) -> pd.DataFrame:
    frame = source.copy()
    if "cost_bps_round_trip" in frame and "cost_bps" not in frame:
        frame = frame.rename(columns={"cost_bps_round_trip": "cost_bps"})
    if "wilson_95_lower_gt_75" in frame and "wilson_lower_gt_75" not in frame:
        frame = frame.rename(columns={"wilson_95_lower_gt_75": "wilson_lower_gt_75"})
    for column in ("setup_id", "strategy", "variant", "scope", "period", "cost_bps"):
        if column not in frame:
            raise ValueError(f"{batch}/{source_file} lacks {column}")
    for column in CANONICAL_METRICS:
        if column not in frame:
            frame[column] = np.nan
    frame["scope"] = frame.scope.replace({"1321_SHORT": "SETUP"})
    frame["source_batch"] = batch
    frame["source_file"] = source_file
    frame["source_kind"] = "direct_simulation"
    keep = [
        "setup_id", "strategy", "variant", "scope", "period", "cost_bps",
        *CANONICAL_METRICS, "source_batch", "source_file", "source_kind",
    ]
    return frame[keep]


def load_standard_variants(batch: str) -> pd.DataFrame:
    directory = ROOT / batch
    setup = pd.read_csv(directory / "variant_setup_metrics.csv")
    setup["scope"] = "SETUP"
    portfolio = pd.read_csv(directory / "variant_portfolio_metrics.csv")
    portfolio["scope"] = "FULL_PORTFOLIO"
    return pd.concat(
        [
            canonicalize(setup, batch, "variant_setup_metrics.csv"),
            canonicalize(portfolio, batch, "variant_portfolio_metrics.csv"),
        ],
        ignore_index=True,
    )


def load_late_variants() -> pd.DataFrame:
    directory = ROOT / "late"
    fixed = pd.read_csv(directory / "fixed_grid_metrics.csv")
    fixed["setup_id"] = "1321_SHORT"
    direct = canonicalize(fixed, "late", "fixed_grid_metrics.csv")
    stress = pd.read_csv(directory / "selected_cost_stress.csv")
    stress["setup_id"] = "1321_SHORT"
    extra = canonicalize(stress, "late", "selected_cost_stress.csv")
    # The fixed grid already supplies the selected 5 bps rows.  Cost stress
    # supplies the locked rule at 9 and 15 bps.
    extra = extra.loc[extra.cost_bps.ne(5.0)].copy()
    return pd.concat([direct, extra], ignore_index=True, sort=False)


def add_later_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Fill an absent `later` row from validation+assessment+final blocks."""
    key = ["setup_id", "strategy", "variant", "scope", "cost_bps"]
    additions = []
    for values, group in frame.groupby(key, dropna=False, sort=False):
        if (group.period == "later").any():
            continue
        parts = group.loc[group.period.isin(["validation", "assessment", "final_sessions"])].copy()
        if set(parts.period) != {"validation", "assessment", "final_sessions"}:
            continue
        record = dict(zip(key, values))
        record.update(
            {
                "period": "later",
                "sessions": int(pd.to_numeric(parts.sessions).sum()),
                "selected_opportunities": (
                    math.nan
                    if parts.selected_opportunities.isna().all()
                    else float(pd.to_numeric(parts.selected_opportunities).sum())
                ),
                "trades": int(pd.to_numeric(parts.trades).sum()),
                "wins": int(pd.to_numeric(parts.wins).sum()),
                "losses": int(pd.to_numeric(parts.losses).sum()),
                "flats": int(pd.to_numeric(parts.flats).sum()),
                "net_pnl_rs": float(pd.to_numeric(parts.net_pnl_rs).sum()),
                "negative_trade_sum_abs_rs": float(
                    pd.to_numeric(parts.negative_trade_sum_abs_rs).sum()
                ),
                "daily_close_drawdown_rs": math.nan,
                "source_batch": str(parts.source_batch.iloc[0]),
                "source_file": "+".join(sorted(set(parts.source_file))),
                "source_kind": "additive_later_from_fixed_blocks_no_drawdown",
            }
        )
        trades = record["trades"]
        wins = record["wins"]
        lower, upper = wilson(wins, trades)
        record["win_rate_pct"] = 100 * wins / trades if trades else math.nan
        record["wilson_95_lower_pct"] = lower
        record["wilson_95_upper_pct"] = upper
        additions.append(record)
    if additions:
        frame = pd.concat([frame, pd.DataFrame(additions)], ignore_index=True, sort=False)
    return frame


def add_baseline_comparisons(frame: pd.DataFrame) -> pd.DataFrame:
    keys = ["setup_id", "strategy", "scope", "period", "cost_bps"]
    baseline = frame.loc[frame.variant.eq("baseline"), keys + [
        "trades", "net_pnl_rs", "negative_trade_sum_abs_rs"
    ]].rename(
        columns={
            "trades": "baseline_trades",
            "net_pnl_rs": "baseline_net_pnl_rs",
            "negative_trade_sum_abs_rs": "baseline_negative_trade_sum_abs_rs",
        }
    )
    if baseline.duplicated(keys).any():
        raise ValueError("Duplicate normalized baseline metric key")
    result = frame.merge(baseline, on=keys, how="left", validate="many_to_one")
    if result.baseline_net_pnl_rs.isna().any():
        raise ValueError("Missing normalized baseline comparison")
    result["net_change_vs_baseline_rs"] = result.net_pnl_rs - result.baseline_net_pnl_rs
    result["loss_amount_reduction_vs_baseline_rs"] = (
        result.baseline_negative_trade_sum_abs_rs - result.negative_trade_sum_abs_rs
    )
    result["trade_count_ratio_vs_baseline_pct"] = np.where(
        result.baseline_trades.gt(0), 100 * result.trades / result.baseline_trades, np.nan
    )
    result["point_win_rate_gt_75"] = result.win_rate_pct.gt(75.0)
    result["wilson_95_lower_gt_75"] = result.wilson_95_lower_pct.gt(75.0)
    result["variant_order"] = result.variant.map(VARIANT_ORDER)
    if result.variant_order.isna().any():
        unknown = sorted(result.loc[result.variant_order.isna(), "variant"].unique())
        raise ValueError(f"Unknown variants: {unknown}")
    result["period_order"] = result.period.map(PERIOD_ORDER)
    return result.sort_values(
        ["setup_id", "strategy", "variant_order", "scope", "cost_bps", "period_order"],
        kind="stable",
    ).reset_index(drop=True)


def load_periods() -> tuple[dict[str, list[str]], list[dict]]:
    period_proofs = []
    reference = None
    for batch in BATCHES:
        source = json.loads((ROOT / batch / "source_provenance.json").read_text(encoding="utf-8"))
        periods = source["periods"]
        period_proofs.append({
            "batch": batch,
            "source_provenance_sha256": sha256(ROOT / batch / "source_provenance.json"),
            "periods": periods,
        })
        common = {name: values for name, values in periods.items() if name != "later"}
        if reference is None:
            reference = common
        elif common != reference:
            raise ValueError(f"Period calendar mismatch in {batch}")
    if reference is None:
        raise ValueError("No period calendar")
    reference["later"] = (
        reference["validation"] + reference["assessment"] + reference["final_sessions"]
    )
    reference = {name: reference[name] for name in PERIOD_ORDER}
    return reference, period_proofs


def accepted_ledger(path: Path, strategy: str) -> pd.DataFrame:
    frame = pd.read_csv(
        path,
        usecols=["day", "setup_id", "portfolio_executed", "portfolio_net_profit_rupees"],
    )
    frame["strategy"] = strategy
    frame["day"] = frame.day.astype(str).str[:10]
    frame["executed"] = bools(frame.portfolio_executed)
    frame["pnl"] = pd.to_numeric(frame.portfolio_net_profit_rupees, errors="coerce").fillna(0.0)
    return frame


def ledger_metrics(frame: pd.DataFrame, days: list[str]) -> dict:
    selected = frame.loc[frame.day.isin(days)]
    executed = selected.loc[selected.executed]
    pnl = executed.pnl.astype(float)
    trades = int(len(executed))
    wins = int(pnl.gt(1e-9).sum())
    losses = int(pnl.lt(-1e-9).sum())
    flats = trades - wins - losses
    lower, upper = wilson(wins, trades)
    daily = executed.groupby("day").pnl.sum().reindex(days, fill_value=0.0)
    return {
        "sessions": len(days),
        "selected_opportunities": int(len(selected)),
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "flats": flats,
        "win_rate_pct": 100 * wins / trades if trades else math.nan,
        "wilson_95_lower_pct": lower,
        "wilson_95_upper_pct": upper,
        "net_pnl_rs": float(pnl.sum()),
        "negative_trade_sum_abs_rs": float(-pnl.loc[pnl.lt(0)].sum()),
        "daily_close_drawdown_rs": drawdown(daily),
    }


def accepted_period_metrics(ledgers: dict[str, pd.DataFrame], periods: dict) -> pd.DataFrame:
    rows = []
    for strategy, ledger in ledgers.items():
        for setup_id in SETUPS:
            slot = ledger.loc[ledger.setup_id.eq(setup_id)]
            for period, days in periods.items():
                rows.append({
                    "setup_id": setup_id,
                    "strategy": strategy,
                    "scope": "SETUP",
                    "period": period,
                    **ledger_metrics(slot, days),
                })
        for period, days in periods.items():
            rows.append({
                "setup_id": "ALL_SETUPS",
                "strategy": strategy,
                "scope": "FULL_PORTFOLIO",
                "period": period,
                **ledger_metrics(ledger, days),
            })
    return pd.DataFrame(rows)


def load_reported_development_choices() -> dict[tuple[str, str], str]:
    choices = {}
    for batch in STANDARD_BATCHES:
        table = pd.read_csv(ROOT / batch / "selected_variant_results.csv")
        for (setup_id, strategy), group in table.groupby(["setup_id", "strategy"]):
            values = group.development_selected_variant.dropna().astype(str).unique().tolist()
            if len(values) > 1:
                raise ValueError(f"Multiple development choices in {batch}/{setup_id}/{strategy}")
            choices[(setup_id, strategy)] = values[0] if values else "baseline"
    late = pd.read_csv(ROOT / "late" / "selected_chronology_results.csv")
    for strategy, group in late.groupby("strategy"):
        values = group.selected_from_development.dropna().astype(str).unique().tolist()
        if len(values) != 1:
            raise ValueError(f"Ambiguous late development choice: {strategy}")
        choices[("1321_SHORT", strategy)] = values[0]
    return choices


def load_reported_gate_rows() -> pd.DataFrame:
    parts = []
    for batch in BATCHES:
        table = pd.read_csv(ROOT / batch / "development_selection.csv")
        if "setup_id" not in table:
            table["setup_id"] = BATCHES[batch][0]
        table["reported_eligible"] = bools(table.eligible)
        table["source_batch"] = batch
        parts.append(table[[
            "setup_id", "strategy", "variant", "reported_eligible", "failure_reasons", "source_batch"
        ]])
    return pd.concat(parts, ignore_index=True)


def development_gates(
    variants: pd.DataFrame, reported: pd.DataFrame, reported_choices: dict
) -> tuple[pd.DataFrame, dict]:
    dev = variants.loc[(variants.cost_bps == 5.0) & (variants.period == "development")]
    rows = []
    computed_choices = {}
    for setup_id in SETUPS:
        for strategy in STRATEGIES:
            setup_scope = dev.loc[
                (dev.setup_id == setup_id) & (dev.strategy == strategy) & (dev.scope == "SETUP")
            ].set_index("variant")
            portfolio_scope = dev.loc[
                (dev.setup_id == setup_id)
                & (dev.strategy == strategy)
                & (dev.scope == "FULL_PORTFOLIO")
            ].set_index("variant")
            if len(setup_scope) != 12 or len(portfolio_scope) != 12:
                raise ValueError(f"Incomplete development grid: {setup_id}/{strategy}")
            base_setup = setup_scope.loc["baseline"]
            base_portfolio = portfolio_scope.loc["baseline"]
            eligible = []
            for variant, result in setup_scope.drop(index="baseline").iterrows():
                portfolio_result = portfolio_scope.loc[variant]
                gates = {
                    "gate_minimum_8_executed": bool(result.trades >= 8),
                    "gate_point_win_rate_strictly_gt_75": bool(result.win_rate_pct > 75.0),
                    "gate_setup_net_not_below_baseline": bool(
                        result.net_pnl_rs >= base_setup.net_pnl_rs - 1e-6
                    ),
                    "gate_losing_amount_strictly_below_baseline": bool(
                        result.negative_trade_sum_abs_rs
                        < base_setup.negative_trade_sum_abs_rs - 1e-6
                    ),
                    "gate_portfolio_net_not_below_baseline": bool(
                        portfolio_result.net_pnl_rs >= base_portfolio.net_pnl_rs - 1e-6
                    ),
                }
                is_eligible = all(gates.values())
                record = {
                    "setup_id": setup_id,
                    "strategy": strategy,
                    "variant": variant,
                    "variant_order": int(VARIANT_ORDER[variant]),
                    **gates,
                    "gates_passed": sum(gates.values()),
                    "eligible_all_five_development_gates": is_eligible,
                    "development_trades": int(result.trades),
                    "development_wins": int(result.wins),
                    "development_losses": int(result.losses),
                    "development_win_rate_pct": float(result.win_rate_pct),
                    "development_wilson_95_lower_pct": float(result.wilson_95_lower_pct),
                    "development_setup_net_pnl_rs": float(result.net_pnl_rs),
                    "development_setup_net_change_rs": float(
                        result.net_pnl_rs - base_setup.net_pnl_rs
                    ),
                    "development_loss_amount_reduction_rs": float(
                        base_setup.negative_trade_sum_abs_rs
                        - result.negative_trade_sum_abs_rs
                    ),
                    "development_portfolio_net_change_rs": float(
                        portfolio_result.net_pnl_rs - base_portfolio.net_pnl_rs
                    ),
                }
                rows.append(record)
                if is_eligible:
                    eligible.append(
                        (
                            float(result.net_pnl_rs),
                            -int(result.losses),
                            -int(VARIANT_ORDER[variant]),
                            variant,
                        )
                    )
            computed_choices[(setup_id, strategy)] = max(eligible)[3] if eligible else "baseline"
    gates = pd.DataFrame(rows)
    merged = gates.merge(
        reported,
        on=["setup_id", "strategy", "variant"],
        how="left",
        validate="one_to_one",
    )
    if merged.reported_eligible.isna().any():
        raise ValueError("Missing batch gate result during normalization")
    mismatch = merged.reported_eligible.ne(merged.eligible_all_five_development_gates)
    if mismatch.any():
        raise ValueError(
            "Development gate mismatch: "
            + str(merged.loc[mismatch, ["setup_id", "strategy", "variant"]].to_dict("records"))
        )
    if computed_choices != reported_choices:
        differences = {
            str(key): (computed_choices.get(key), reported_choices.get(key))
            for key in set(computed_choices) | set(reported_choices)
            if computed_choices.get(key) != reported_choices.get(key)
        }
        raise ValueError(f"Development choice mismatch: {differences}")
    merged["selected_from_development"] = [
        computed_choices[(row.setup_id, row.strategy)] == row.variant
        for row in merged.itertuples(index=False)
    ]
    return merged, computed_choices


def promotion_decisions(variants: pd.DataFrame, development_choices: dict) -> pd.DataFrame:
    later = variants.loc[(variants.cost_bps == 5.0) & (variants.period == "later")]
    all_period = variants.loc[(variants.cost_bps == 5.0) & (variants.period == "all")]
    rows = []
    for setup_id in SETUPS:
        for strategy in STRATEGIES:
            chosen = development_choices[(setup_id, strategy)]
            setup_later = later.loc[
                (later.setup_id == setup_id) & (later.strategy == strategy) & (later.scope == "SETUP")
            ].set_index("variant")
            portfolio_later = later.loc[
                (later.setup_id == setup_id)
                & (later.strategy == strategy)
                & (later.scope == "FULL_PORTFOLIO")
            ].set_index("variant")
            selected = setup_later.loc[chosen]
            baseline = setup_later.loc["baseline"]
            selected_portfolio = portfolio_later.loc[chosen]
            baseline_portfolio = portfolio_later.loc["baseline"]
            gates = {
                "later_gate_minimum_8_executed": bool(selected.trades >= 8),
                "later_gate_point_win_rate_strictly_gt_75": bool(selected.win_rate_pct > 75.0),
                "later_gate_setup_net_not_below_baseline": bool(
                    selected.net_pnl_rs >= baseline.net_pnl_rs - 1e-6
                ),
                "later_gate_losing_amount_strictly_below_baseline": bool(
                    selected.negative_trade_sum_abs_rs
                    < baseline.negative_trade_sum_abs_rs - 1e-6
                ),
                "later_gate_portfolio_net_not_below_baseline": bool(
                    selected_portfolio.net_pnl_rs >= baseline_portfolio.net_pnl_rs - 1e-6
                ),
            }
            if chosen == "baseline":
                status = "NO_DEVELOPMENT_QUALIFIER"
                later_pass = False
            else:
                later_pass = all(gates.values())
                status = "PENDING_COMBINED_REPLAY" if later_pass else "REJECTED_LATER"
            # No combined replay promoted a rule. Accepted baseline remains the
            # final locked truth for all ten requested setups.
            final_variant = "baseline"
            selected_all = all_period.loc[
                (all_period.setup_id == setup_id)
                & (all_period.strategy == strategy)
                & (all_period.scope == "SETUP")
                & (all_period.variant == chosen)
            ].iloc[0]
            rows.append(
                {
                    "setup_id": setup_id,
                    "strategy": strategy,
                    "development_selected_variant": chosen if chosen != "baseline" else None,
                    "development_outcome": (
                        "SELECTED_DIAGNOSTIC_ONLY" if chosen != "baseline" else "NO_QUALIFIER"
                    ),
                    **gates,
                    "later_all_honesty_gates_pass": later_pass,
                    "later_trades": int(selected.trades),
                    "later_wins": int(selected.wins),
                    "later_losses": int(selected.losses),
                    "later_win_rate_pct": float(selected.win_rate_pct),
                    "later_wilson_95_lower_pct": float(selected.wilson_95_lower_pct),
                    "later_setup_net_change_rs": float(
                        selected.net_pnl_rs - baseline.net_pnl_rs
                    ),
                    "later_loss_amount_reduction_rs": float(
                        baseline.negative_trade_sum_abs_rs
                        - selected.negative_trade_sum_abs_rs
                    ),
                    "later_portfolio_net_change_rs": float(
                        selected_portfolio.net_pnl_rs - baseline_portfolio.net_pnl_rs
                    ),
                    "all_history_trades": int(selected_all.trades),
                    "all_history_wins": int(selected_all.wins),
                    "all_history_losses": int(selected_all.losses),
                    "all_history_win_rate_pct": float(selected_all.win_rate_pct),
                    "all_history_wilson_95_lower_pct": float(selected_all.wilson_95_lower_pct),
                    "all_history_net_change_rs": float(selected_all.net_change_vs_baseline_rs),
                    "promotion_status": status,
                    "combined_replay_performed": False,
                    "final_locked_variant": final_variant,
                }
            )
    return pd.DataFrame(rows)


def october_setup_daywise(
    ledgers: dict[str, pd.DataFrame], october_days: list[str], final_variants: dict
) -> pd.DataFrame:
    if set(final_variants.values()) != {"baseline"}:
        raise ValueError("Accepted-ledger daywise is only valid when all final variants are baseline")
    rows = []
    for strategy, ledger in ledgers.items():
        for setup_id in SETUPS:
            slot = ledger.loc[ledger.setup_id.eq(setup_id)]
            for day in october_days:
                selected = slot.loc[slot.day.eq(day)]
                executed = selected.loc[selected.executed]
                pnl = executed.pnl.astype(float)
                trades = int(len(executed))
                wins = int(pnl.gt(1e-9).sum())
                losses = int(pnl.lt(-1e-9).sum())
                lower, upper = wilson(wins, trades)
                rows.append(
                    {
                        "day": day,
                        "setup_id": setup_id,
                        "strategy": strategy,
                        "final_locked_variant": "baseline",
                        "source": "accepted_0926_adjusted_ledger",
                        "sessions": 1,
                        "selected_opportunities": int(len(selected)),
                        "trades": trades,
                        "wins": wins,
                        "losses": losses,
                        "flats": trades - wins - losses,
                        "win_rate_pct": 100 * wins / trades if trades else math.nan,
                        "wilson_95_lower_pct": lower,
                        "wilson_95_upper_pct": upper,
                        "net_pnl_rs": float(pnl.sum()),
                        "negative_trade_sum_abs_rs": float(-pnl.loc[pnl.lt(0)].sum()),
                    }
                )
    result = pd.DataFrame(rows)
    expected = len(october_days) * len(SETUPS) * len(STRATEGIES)
    if len(result) != expected:
        raise ValueError(f"October setup/day row count mismatch: {len(result)} != {expected}")
    return result.sort_values(["day", "setup_id", "strategy"], kind="stable").reset_index(drop=True)


def aggregate_daywise(setup_daywise: pd.DataFrame) -> pd.DataFrame:
    grouped = setup_daywise.groupby(["day", "strategy"], sort=True).agg(
        requested_setups=("setup_id", "nunique"),
        selected_opportunities=("selected_opportunities", "sum"),
        trades=("trades", "sum"),
        wins=("wins", "sum"),
        losses=("losses", "sum"),
        flats=("flats", "sum"),
        net_pnl_rs=("net_pnl_rs", "sum"),
        negative_trade_sum_abs_rs=("negative_trade_sum_abs_rs", "sum"),
    ).reset_index()
    grouped["win_rate_pct"] = np.where(
        grouped.trades.gt(0), 100 * grouped.wins / grouped.trades, np.nan
    )
    bounds = [wilson(int(row.wins), int(row.trades)) for row in grouped.itertuples()]
    grouped["wilson_95_lower_pct"] = [value[0] for value in bounds]
    grouped["wilson_95_upper_pct"] = [value[1] for value in bounds]
    grouped["october_cumulative_net_pnl_rs"] = grouped.groupby("strategy").net_pnl_rs.cumsum()
    grouped["october_running_peak_rs"] = grouped.groupby("strategy")[
        "october_cumulative_net_pnl_rs"
    ].cummax().clip(lower=0.0)
    grouped["october_drawdown_from_peak_rs"] = (
        grouped.october_running_peak_rs - grouped.october_cumulative_net_pnl_rs
    )
    return grouped


def full_portfolio_daywise(ledgers: dict[str, pd.DataFrame], october_days: list[str]) -> pd.DataFrame:
    rows = []
    for strategy, ledger in ledgers.items():
        for day in october_days:
            selected = ledger.loc[ledger.day.eq(day)]
            executed = selected.loc[selected.executed]
            pnl = executed.pnl.astype(float)
            trades = int(len(executed))
            wins = int(pnl.gt(1e-9).sum())
            losses = int(pnl.lt(-1e-9).sum())
            lower, upper = wilson(wins, trades)
            rows.append(
                {
                    "day": day,
                    "strategy": strategy,
                    "source": "accepted_0926_adjusted_ledger",
                    "selected_opportunities": int(len(selected)),
                    "trades": trades,
                    "wins": wins,
                    "losses": losses,
                    "flats": trades - wins - losses,
                    "win_rate_pct": 100 * wins / trades if trades else math.nan,
                    "wilson_95_lower_pct": lower,
                    "wilson_95_upper_pct": upper,
                    "net_pnl_rs": float(pnl.sum()),
                    "negative_trade_sum_abs_rs": float(-pnl.loc[pnl.lt(0)].sum()),
                }
            )
    result = pd.DataFrame(rows).sort_values(["day", "strategy"], kind="stable")
    result["october_cumulative_net_pnl_rs"] = result.groupby("strategy").net_pnl_rs.cumsum()
    result["october_running_peak_rs"] = result.groupby("strategy")[
        "october_cumulative_net_pnl_rs"
    ].cummax().clip(lower=0.0)
    result["october_drawdown_from_peak_rs"] = (
        result.october_running_peak_rs - result.october_cumulative_net_pnl_rs
    )
    return result.reset_index(drop=True)


def validate_accepted_baseline(
    variants: pd.DataFrame, accepted: pd.DataFrame, setup_daywise: pd.DataFrame
) -> list[dict]:
    proofs = []
    direct = variants.loc[
        (variants.variant == "baseline") & (variants.cost_bps == 5.0) & (variants.scope == "SETUP")
    ]
    for setup_id in SETUPS:
        for strategy in STRATEGIES:
            for period in PERIOD_ORDER:
                row = direct.loc[
                    (direct.setup_id == setup_id)
                    & (direct.strategy == strategy)
                    & (direct.period == period)
                ].iloc[0]
                truth = accepted.loc[
                    (accepted.setup_id == setup_id)
                    & (accepted.strategy == strategy)
                    & (accepted.scope == "SETUP")
                    & (accepted.period == period)
                ].iloc[0]
                differences = []
                for column in ("trades", "wins", "losses"):
                    if int(row[column]) != int(truth[column]):
                        differences.append(f"{column}:{row[column]}!={truth[column]}")
                if not math.isclose(float(row.net_pnl_rs), float(truth.net_pnl_rs), abs_tol=1e-5):
                    differences.append(f"net:{row.net_pnl_rs}!={truth.net_pnl_rs}")
                if differences:
                    raise ValueError(
                        f"Accepted baseline normalization mismatch {setup_id}/{strategy}/{period}: {differences}"
                    )
            proofs.append({
                "setup_id": setup_id,
                "strategy": strategy,
                "all_seven_periods_match_accepted_ledger": True,
            })

    # Standard batches also persisted setup/day results.  Validate every row
    # for the final baseline against the accepted ledger truth.
    for batch in STANDARD_BATCHES:
        daywise = pd.read_csv(ROOT / batch / "selected_variant_daywise.csv")
        for setup_id in BATCHES[batch]:
            for strategy in STRATEGIES:
                final = daywise.loc[
                    (daywise.setup_id == setup_id) & (daywise.strategy == strategy)
                ].copy()
                # Development-selected diagnostics are not final locked truth.
                if set(final.variant.astype(str)) != {"baseline"}:
                    continue
                truth = setup_daywise.loc[
                    (setup_daywise.setup_id == setup_id)
                    & (setup_daywise.strategy == strategy)
                ]
                final = final.loc[final.day.astype(str).isin(truth.day.astype(str))].copy()
                joined = final.merge(truth, on=["setup_id", "strategy", "day"], suffixes=("_batch", "_truth"))
                if len(joined) != len(final):
                    raise ValueError(f"Daywise baseline coverage mismatch: {batch}/{setup_id}/{strategy}")
                for column in ("trades", "wins", "losses"):
                    if not (joined[f"{column}_batch"].astype(int) == joined[f"{column}_truth"].astype(int)).all():
                        raise ValueError(f"Daywise {column} mismatch: {batch}/{setup_id}/{strategy}")
                if not np.allclose(joined.net_pnl_rs_batch, joined.net_pnl_rs_truth, atol=1e-6):
                    raise ValueError(f"Daywise net mismatch: {batch}/{setup_id}/{strategy}")
    return proofs


def money(value: float) -> str:
    return ("-" if value < 0 else "") + f"Rs {abs(value):,.2f}"


def percent(value: float) -> str:
    return "n/a" if pd.isna(value) else f"{value:.2f}%"


def build_report(
    promotions: pd.DataFrame,
    gates: pd.DataFrame,
    locked: pd.DataFrame,
    portfolio_summary: pd.DataFrame,
    validation: dict,
) -> str:
    report = [
        "# Ten-setup G3 and G7 V4 lock audit",
        "",
        "This consolidated package covers the ten unique requested setup/direction pairs in G3 and G7 V4 two-loss-lock. Evidence ends on 2026-10-27. The 2026-10-28 session is incomplete and is excluded.",
        "",
        "All batch manifest hashes and baseline parity checks passed. Final daywise truth comes from the accepted portfolios that already contain the 09:26 SHORT wick>=0.20 change. No active strategy or config file was changed.",
        "",
        "## Final decision",
        "",
        "No tested rule is promoted. Three G3 variant rows passed the development gates across two setups. The frozen tie-break selected one rule for each setup; both selected rules failed the later chronology. They remain visible as diagnostics and the final locked result stays at accepted baseline for every setup and strategy.",
        "",
        "| Setup | Strategy | Development choice | Later result | Final locked |",
        "|---|---|---|---:|---|",
    ]
    for row in promotions.itertuples(index=False):
        choice = row.development_selected_variant if isinstance(row.development_selected_variant, str) else "none"
        later = (
            f"{row.later_wins}/{row.later_trades} wins, {row.later_win_rate_pct:.2f}%"
            if choice != "none"
            else "no development qualifier"
        )
        report.append(
            f"| {row.setup_id} | {row.strategy} | {choice} | {later} | {row.final_locked_variant} |"
        )
    report += [
        "",
        "Development rows that passed all five gates were:",
        "",
    ]
    for row in gates.loc[gates.eligible_all_five_development_gates].sort_values(
        ["setup_id", "strategy", "variant_order"]
    ).itertuples(index=False):
        selection = "selected by tie-break" if row.selected_from_development else "eligible, not selected"
        report.append(
            f"- {row.setup_id} {row.strategy} `{row.variant}`: "
            f"{row.development_wins}/{row.development_trades} "
            f"({row.development_win_rate_pct:.2f}%), setup net change "
            f"{money(row.development_setup_net_change_rs)}; {selection}."
        )
    report += [
        "",
        "The two tie-break selections were rejected after development:",
        "",
    ]
    selected = promotions.loc[promotions.development_selected_variant.notna()]
    for row in selected.itertuples(index=False):
        report.append(
            f"- {row.setup_id} {row.strategy} `{row.development_selected_variant}`: later "
            f"{row.later_wins}/{row.later_trades} ({row.later_win_rate_pct:.2f}%), "
            f"all history {row.all_history_wins}/{row.all_history_trades} "
            f"({row.all_history_win_rate_pct:.2f}%). It failed the later >75% requirement."
        )
    report += [
        "",
        "## Accepted baseline at 5 bps",
        "",
        "| Setup | Strategy | Overall W-L | Overall win rate | Overall net | October W-L | October win rate | October net |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    table = locked.loc[
        (locked.scope == "SETUP") & (locked.cost_bps == 5.0) & locked.period.isin(["all", "october"])
    ].pivot(index=["setup_id", "strategy"], columns="period")
    for setup_id in SETUPS:
        for strategy in STRATEGIES:
            row = table.loc[(setup_id, strategy)]
            report.append(
                f"| {setup_id} | {strategy} | {int(row[('wins', 'all')])}-{int(row[('losses', 'all')])} | "
                f"{percent(row[('win_rate_pct', 'all')])} | {money(row[('net_pnl_rs', 'all')])} | "
                f"{int(row[('wins', 'october')])}-{int(row[('losses', 'october')])} | "
                f"{percent(row[('win_rate_pct', 'october')])} | {money(row[('net_pnl_rs', 'october')])} |"
            )
    report += [
        "",
        "## Full-portfolio cost stress",
        "",
        "| Strategy | Cost | Overall W-L | Overall win rate | Overall net | October net |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in portfolio_summary.loc[portfolio_summary.period.isin(["all", "october"])].sort_values(
        ["strategy", "cost_bps", "period"]
    ).itertuples(index=False):
        if row.period != "all":
            continue
        october = portfolio_summary.loc[
            (portfolio_summary.strategy == row.strategy)
            & (portfolio_summary.cost_bps == row.cost_bps)
            & (portfolio_summary.period == "october")
        ].iloc[0]
        report.append(
            f"| {row.strategy} | {row.cost_bps:.0f} bps | {int(row.wins)}-{int(row.losses)} | "
            f"{row.win_rate_pct:.2f}% | {money(row.net_pnl_rs)} | {money(october.net_pnl_rs)} |"
        )
    report += [
        "",
        "The 5, 9, and 15 bps rows rerun cost-dependent G7 loss-lock chronology. Higher costs materially lower both win rate and PnL.",
        "",
        "## Evidence and limits",
        "",
        f"- {validation['submanifest_files_verified']} declared batch files were hash-verified.",
        f"- {validation['baseline_parity_checks_passed']} batch baseline-parity checks passed, plus accepted-ledger normalization for all 20 setup/strategy pairs.",
        "- `october_setup_daywise.csv` has all 320 setup/strategy/session rows, including zero-trade sessions. `october_requested_setups_aggregate_daywise.csv` and `october_full_portfolio_daywise.csv` provide both aggregate views.",
        "- Setup-level point win rates are descriptive. No setup-level Wilson 95% lower bound validates a >75% claim, and this repeatedly inspected 59-session history is not an independent holdout.",
        "",
        "See `promotion_decisions.csv`, `development_gate_summary.csv`, `final_locked_results.csv`, `all_normalized_variant_results.csv`, and `artifact_manifest.json` for the complete audit.",
        "",
    ]
    return "\n".join(report)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    batch_validation = [validate_batch(batch) for batch in BATCHES]
    periods, period_proofs = load_periods()
    if periods["all"][-1] != "2026-10-27" or len(periods["all"]) != 59:
        raise ValueError("Unexpected data cutoff/session count")
    if len(periods["october"]) != 16:
        raise ValueError("Unexpected October session count")

    variant_parts = [load_standard_variants(batch) for batch in STANDARD_BATCHES]
    variant_parts.append(load_late_variants())
    variants = add_later_rows(pd.concat(variant_parts, ignore_index=True, sort=False))
    key = ["setup_id", "strategy", "variant", "scope", "period", "cost_bps"]
    duplicates = variants.duplicated(key, keep=False)
    if duplicates.any():
        raise ValueError("Duplicate normalized variant key: " + str(variants.loc[duplicates, key].head().to_dict("records")))
    if set(variants.setup_id) != set(SETUPS) or set(variants.strategy) != set(STRATEGIES):
        raise ValueError("Normalized setup/strategy coverage mismatch")
    variants = add_baseline_comparisons(variants)

    reported_choices = load_reported_development_choices()
    reported_gates = load_reported_gate_rows()
    gates, development_choices = development_gates(variants, reported_gates, reported_choices)
    promotions = promotion_decisions(variants, development_choices)
    final_variants = {
        (row.setup_id, row.strategy): row.final_locked_variant
        for row in promotions.itertuples(index=False)
    }

    # Final locked output is the accepted baseline after later rejection.
    locked_parts = []
    for (setup_id, strategy), final_variant in final_variants.items():
        part = variants.loc[
            (variants.setup_id == setup_id)
            & (variants.strategy == strategy)
            & (variants.variant == final_variant)
        ].copy()
        part["development_selected_variant"] = development_choices[(setup_id, strategy)]
        part["final_locked_variant"] = final_variant
        part["promotion_status"] = promotions.loc[
            (promotions.setup_id == setup_id) & (promotions.strategy == strategy), "promotion_status"
        ].iloc[0]
        locked_parts.append(part)
    locked = pd.concat(locked_parts, ignore_index=True, sort=False)
    expected_locked = len(SETUPS) * len(STRATEGIES) * 2 * 3 * len(PERIOD_ORDER)
    if len(locked) != expected_locked:
        raise ValueError(f"Final locked coverage mismatch: {len(locked)} != {expected_locked}")

    ledgers = {
        "G3": accepted_ledger(ACCEPTED / "g3_reference_portfolio_trades.csv", "G3"),
        "G7_V4_LOCK2": accepted_ledger(ACCEPTED / "portfolio_trades.csv", "G7_V4_LOCK2"),
    }
    accepted_metrics = accepted_period_metrics(ledgers, periods)
    setup_daywise = october_setup_daywise(ledgers, periods["october"], final_variants)
    accepted_proofs = validate_accepted_baseline(variants, accepted_metrics, setup_daywise)
    requested_daywise = aggregate_daywise(setup_daywise)
    portfolio_daywise = full_portfolio_daywise(ledgers, periods["october"])

    # Unique portfolio baseline rows: every final setup-specific replay must
    # agree when the final variant is baseline.
    portfolio_locked = locked.loc[locked.scope.eq("FULL_PORTFOLIO")].copy()
    portfolio_value_columns = [
        "sessions", "trades", "wins", "losses", "flats", "win_rate_pct",
        "wilson_95_lower_pct", "wilson_95_upper_pct", "net_pnl_rs",
        "negative_trade_sum_abs_rs", "daily_close_drawdown_rs",
    ]
    portfolio_rows = []
    for (strategy, cost, period), group in portfolio_locked.groupby(
        ["strategy", "cost_bps", "period"], sort=True
    ):
        for column in portfolio_value_columns:
            values = pd.to_numeric(group[column], errors="coerce").dropna().to_numpy(float)
            if len(values) and not np.allclose(values, values[0], atol=1e-5):
                raise ValueError(f"Portfolio baseline disagreement {strategy}/{cost}/{period}/{column}")
        row = group.iloc[0].copy()
        row["setup_id"] = "ALL_SETUPS"
        row["source_batch"] = "consensus_across_all_final_baseline_replays"
        portfolio_rows.append(row)
    portfolio_summary = pd.DataFrame(portfolio_rows)

    # Accepted ledger is the primary 5 bps portfolio truth.
    for strategy in STRATEGIES:
        for period in PERIOD_ORDER:
            row = portfolio_summary.loc[
                (portfolio_summary.strategy == strategy)
                & (portfolio_summary.cost_bps == 5.0)
                & (portfolio_summary.period == period)
            ].iloc[0]
            truth = accepted_metrics.loc[
                (accepted_metrics.strategy == strategy)
                & (accepted_metrics.scope == "FULL_PORTFOLIO")
                & (accepted_metrics.period == period)
            ].iloc[0]
            if int(row.trades) != int(truth.trades) or not math.isclose(
                float(row.net_pnl_rs), float(truth.net_pnl_rs), abs_tol=1e-5
            ):
                raise ValueError(f"Full portfolio accepted baseline mismatch: {strategy}/{period}")

    diagnostic_parts = []
    for row in promotions.loc[promotions.development_selected_variant.notna()].itertuples(index=False):
        diagnostic_parts.append(
            variants.loc[
                (variants.setup_id == row.setup_id)
                & (variants.strategy == row.strategy)
                & (variants.variant == row.development_selected_variant)
            ]
        )
    development_diagnostics = (
        pd.concat(diagnostic_parts, ignore_index=True, sort=False)
        if diagnostic_parts
        else variants.iloc[0:0].copy()
    )

    # Stable output ordering and schemas.
    variants.to_csv(OUT / "all_normalized_variant_results.csv", index=False)
    gates.to_csv(OUT / "development_gate_summary.csv", index=False)
    promotions.to_csv(OUT / "promotion_decisions.csv", index=False)
    development_diagnostics.to_csv(OUT / "development_selected_diagnostics.csv", index=False)
    locked.to_csv(OUT / "final_locked_results.csv", index=False)
    locked.loc[locked.period.eq("all")].to_csv(OUT / "final_locked_overall.csv", index=False)
    locked.loc[(locked.cost_bps == 5.0) & locked.period.ne("all")].to_csv(
        OUT / "final_locked_chronology.csv", index=False
    )
    locked.loc[locked.period.isin(["all", "october"])].to_csv(
        OUT / "final_locked_cost_stress.csv", index=False
    )
    portfolio_summary.to_csv(OUT / "portfolio_baseline_results.csv", index=False)
    accepted_metrics.to_csv(OUT / "accepted_5bps_period_truth.csv", index=False)
    setup_daywise.to_csv(OUT / "october_setup_daywise.csv", index=False)
    requested_daywise.to_csv(OUT / "october_requested_setups_aggregate_daywise.csv", index=False)
    portfolio_daywise.to_csv(OUT / "october_full_portfolio_daywise.csv", index=False)

    validation_summary = {
        "status": "PASS",
        "data_end": "2026-10-27",
        "as_of_session": "2026-10-28_INCOMPLETE_EXCLUDED",
        "expected_setups": list(SETUPS),
        "strategies": list(STRATEGIES),
        "submanifest_files_verified": sum(row["declared_files_verified"] for row in batch_validation),
        "baseline_parity_checks_passed": sum(
            row["baseline_parity_checks_passed"] for row in batch_validation
        ),
        "accepted_setup_strategy_period_checks": len(accepted_proofs) * len(PERIOD_ORDER),
        "development_gate_rows_recomputed": len(gates),
        "development_selected_diagnostics": int(
            promotions.development_selected_variant.notna().sum()
        ),
        "final_nonbaseline_promotions": int(promotions.final_locked_variant.ne("baseline").sum()),
        "october_setup_day_rows": len(setup_daywise),
        "october_requested_aggregate_rows": len(requested_daywise),
        "october_full_portfolio_rows": len(portfolio_daywise),
        "batch_validation": batch_validation,
        "accepted_normalization": accepted_proofs,
    }
    write_json(OUT / "validation_summary.json", validation_summary)
    write_json(
        OUT / "source_provenance.json",
        {
            "scope": "BACKTEST_ONLY",
            "execution_authority": False,
            "active_configs_modified": False,
            "independent_holdout": False,
            "data_end": "2026-10-27",
            "session_2026_10_28": "INCOMPLETE_EXCLUDED",
            "accepted_baseline": str(ACCEPTED),
            "accepted_files": {
                "g3_reference_portfolio_trades.csv": sha256(
                    ACCEPTED / "g3_reference_portfolio_trades.csv"
                ),
                "portfolio_trades.csv": sha256(ACCEPTED / "portfolio_trades.csv"),
                "artifact_manifest.json": sha256(ACCEPTED / "artifact_manifest.json"),
            },
            "batch_manifests": batch_validation,
            "period_proofs": period_proofs,
            "build_script_sha256": sha256(Path(__file__)),
        },
    )
    (OUT / "report.md").write_text(
        build_report(promotions, gates, locked, portfolio_summary, validation_summary),
        encoding="utf-8",
    )

    generated = [path for path in OUT.iterdir() if path.is_file() and path.name != "artifact_manifest.json"]
    write_json(
        OUT / "artifact_manifest.json",
        {
            "created_at_ist": datetime.now().astimezone().isoformat(),
            "scope": "BACKTEST_ONLY",
            "execution_authority": False,
            "active_configs_modified": False,
            "data_end": "2026-10-27",
            "session_2026_10_28": "INCOMPLETE_EXCLUDED",
            "files": {path.name: sha256(path) for path in sorted(generated)},
        },
    )
    print(promotions[[
        "setup_id", "strategy", "development_selected_variant", "later_win_rate_pct",
        "promotion_status", "final_locked_variant",
    ]].to_string(index=False))
    print("OUTPUT", OUT)


if __name__ == "__main__":
    main()
