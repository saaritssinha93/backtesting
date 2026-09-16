"""Consolidate the complete V13-v5 audit and experiment archive.

Run the main V13-v5 backtest first. This script replays the raw V7 confirmation
counterfactual with the corrected execution engine, normalizes every completed
and rejected experiment into one ledger, copies the detailed source artifacts
into the isolated V13-v5 result tree, and verifies their checksums.
"""

from __future__ import annotations

import hashlib
import json
import math
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v13_corrected_v2_backtest as v2
import fno_v13_corrected_v3_backtest as v3
import fno_v13_corrected_v5_backtest as v5


WORKSPACE = Path(__file__).resolve().parent
TEMP_ROOT = WORKSPACE / ".codex_tmp"
SOURCE_DIRS = {
    "validation_audit": TEMP_ROOT / "v13_v5_validation_audit",
    "timing_entry": TEMP_ROOT / "v13_v5_timing_entry",
    "exit_robustness": TEMP_ROOT / "v13_v5_exit_robustness",
    "profile_eval": TEMP_ROOT / "v13_v5_profile_eval",
}
V7_CACHE_DIR = TEMP_ROOT / "v13_v5_v7cache"
RESULT_DIR = v5.RESEARCH_DIR
LEDGER_PATH = RESULT_DIR / "fno_v13_v5_experiment_ledger.csv"
V7_METRICS_PATH = RESULT_DIR / "confirmation_v7_metrics.csv"
V7_TRADES_PATH = RESULT_DIR / "confirmation_v7_trade_audit.csv"
LEADERBOARD_PATH = RESULT_DIR / "fno_v13_v5_headline_leaderboard.csv"
CREATED_FILES_PATH = RESULT_DIR / "fno_v13_v5_created_files.csv"
WORKSPACE_REPORT_PATH = WORKSPACE / "FNO_V13_V5_RESEARCH_AUDIT.md"
THROUGH_DAY = date(2026, 9, 3)
COST_BPS = 5.0


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def clean(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        if math.isnan(float(value)):
            return None
        if math.isinf(float(value)):
            return "INF" if float(value) > 0 else "-INF"
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (date, datetime, pd.Timestamp, Path)):
        return str(value)
    if pd.isna(value):
        return None
    return value


def row_json(row: pd.Series, fields: Iterable[str]) -> str:
    return json.dumps(
        {field: clean(row.get(field)) for field in fields if field in row.index},
        sort_keys=True,
    )


def number(row: pd.Series, *names: str, scale: float = 1.0) -> float:
    for name in names:
        if name not in row.index:
            continue
        value = pd.to_numeric(pd.Series([row[name]]), errors="coerce").iloc[0]
        if pd.notna(value):
            return float(value) * scale
    return np.nan


def ratio_pct(numerator: float, denominator: float) -> float:
    return numerator / denominator * 100.0 if denominator and np.isfinite(denominator) else np.nan


def source_timestamp(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=common.IST).isoformat(
        timespec="seconds"
    )


def base_ledger_row(*, source_suite: str, source_path: Path) -> dict[str, Any]:
    return {
        "run_timestamp_ist": source_timestamp(source_path),
        "source_suite": source_suite,
        "data_period": "2026-07-29..2026-09-03; 25 sessions",
        "source_artifact": str(source_path.resolve()),
    }


def normalize_timing() -> list[dict[str, Any]]:
    path = SOURCE_DIRS["timing_entry"] / "all_completed_experiments.csv"
    frame = pd.read_csv(path)
    rows: list[dict[str, Any]] = []
    config_fields = [
        "changed_signal_end",
        "changed_side",
        "removed_setup_id",
        "trigger_buffer_pct",
        "expiry_bars",
        "activation_delay_bars",
        "confirmation_displacement_min_pct",
        "body_ratio_delta",
        "wick_cap_delta",
        "baseline_max_entries",
        "candidate_max_entries",
        "setup_id_before",
        "setup_id_after",
    ]
    for _, row in frame.iterrows():
        all_fills = number(row, "all_fills")
        train_fills = number(row, "train_fills")
        validation_fills = number(row, "validation_fills")
        pseudo_fills = number(row, "pseudo_test_fills")
        status = str(row.get("development_selection_status", "GRID_ONLY"))
        rows.append(
            {
                **base_ledger_row(source_suite="TIMING_ENTRY", source_path=path),
                "experiment_id": f"TIMING_ENTRY::{row['experiment_id']}",
                "family": row.get("family"),
                "configuration_json": row_json(row, config_fields),
                "exact_change": row.get("description"),
                "reason_for_test": "Independent timing/filter/entry ablation",
                "cost_bps": COST_BPS,
                "split_protocol": "TRAIN_12_VALIDATION_7_PSEUDO_TEST_6",
                "all_trades": all_fills,
                "all_average_trades_per_day": all_fills / 25.0,
                "all_win_rate_pct": number(row, "all_win_rate", scale=100.0),
                "all_target_hit_rate_pct": ratio_pct(
                    number(row, "all_target_hits"), all_fills
                ),
                "all_net_profit_pct": number(row, "all_net_pct"),
                "all_profit_factor": number(row, "all_profit_factor"),
                "all_expectancy_pct": number(row, "all_expectancy_pct"),
                "all_maximum_drawdown_pct": number(row, "all_max_drawdown_pct"),
                "train_trades": train_fills,
                "train_win_rate_pct": number(row, "train_win_rate", scale=100.0),
                "train_net_profit_pct": number(row, "train_net_pct"),
                "train_profit_factor": number(row, "train_profit_factor"),
                "validation_trades": validation_fills,
                "validation_win_rate_pct": number(
                    row, "validation_win_rate", scale=100.0
                ),
                "validation_net_profit_pct": number(row, "validation_net_pct"),
                "validation_profit_factor": number(row, "validation_profit_factor"),
                "pseudo_test_trades": pseudo_fills,
                "pseudo_test_win_rate_pct": number(
                    row, "pseudo_test_win_rate", scale=100.0
                ),
                "pseudo_test_net_profit_pct": number(row, "pseudo_test_net_pct"),
                "pseudo_test_profit_factor": number(row, "pseudo_test_profit_factor"),
                "delta_trades_vs_v3": number(row, "delta_all_fills"),
                "delta_win_rate_pct_vs_v3": number(row, "delta_all_win_rate", scale=100.0),
                "delta_net_profit_pct_vs_v3": number(row, "delta_all_net_pct"),
                "delta_profit_factor_vs_v3": number(row, "delta_all_profit_factor"),
                "decision": status,
                "decision_reason": row.get("development_rejection_reason", ""),
            }
        )

    not_run_path = SOURCE_DIRS["timing_entry"] / "not_run_experiments.csv"
    for _, row in pd.read_csv(not_run_path).iterrows():
        rows.append(
            {
                **base_ledger_row(
                    source_suite="TIMING_ENTRY", source_path=not_run_path
                ),
                "experiment_id": f"TIMING_ENTRY::{row['experiment_id']}",
                "family": row.get("family"),
                "configuration_json": "{}",
                "exact_change": row.get("experiment_id"),
                "reason_for_test": "Requested one-minute execution variant",
                "cost_bps": COST_BPS,
                "split_protocol": "NOT_RUN",
                "decision": row.get("run_status", "NOT_RUN"),
                "decision_reason": row.get("rejection_reason", ""),
            }
        )
    return rows


def normalize_exit() -> list[dict[str, Any]]:
    path = SOURCE_DIRS["exit_robustness"] / "experiment_ledger.csv"
    frame = pd.read_csv(path)
    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        config = str(row.get("config", ""))
        if config in {"V3_NATIVE", "V4"}:
            decision = "REFERENCE"
            decision_reason = "Published baseline/reference configuration"
        elif config == "SAFE_CAP210_T11.100_P0.20_R2.60_BE":
            decision = "INVESTIGATE_FORWARD"
            decision_reason = "Best terminal-safe exit; no untouched validation"
        else:
            decision = "GRID_ONLY_NOT_FROZEN"
            decision_reason = "Neighbor or ablation retained for full disclosure"
        all_fills = number(row, "all_fills")
        rows.append(
            {
                **base_ledger_row(source_suite="EXIT_ROBUSTNESS", source_path=path),
                "experiment_id": f"EXIT::{config}",
                "family": row.get("family"),
                "configuration_json": row.get("params_json", "{}"),
                "exact_change": config,
                "reason_for_test": "Stop/target/partial/runner/time-cap robustness grid",
                "cost_bps": COST_BPS,
                "split_protocol": "TRAIN_12_TEST_13_WITH_LATEST_3_DIAGNOSTIC",
                "all_trades": all_fills,
                "all_average_trades_per_day": all_fills / 25.0,
                "all_win_rate_pct": number(row, "all_win_rate_pct"),
                "all_target_hit_rate_pct": number(row, "all_objective_hit_rate_pct"),
                "all_net_profit_pct": number(row, "all_net_pct"),
                "all_profit_factor": number(row, "all_pf"),
                "all_expectancy_pct": number(row, "all_expectancy_pct"),
                "all_maximum_drawdown_pct": number(row, "all_max_drawdown_pct"),
                "train_trades": number(row, "train_fills"),
                "train_win_rate_pct": number(row, "train_win_rate_pct"),
                "train_net_profit_pct": number(row, "train_net_pct"),
                "train_profit_factor": number(row, "train_pf"),
                "validation_trades": number(row, "test_fills"),
                "validation_win_rate_pct": number(row, "test_win_rate_pct"),
                "validation_net_profit_pct": number(row, "test_net_pct"),
                "validation_profit_factor": number(row, "test_pf"),
                "decision": decision,
                "decision_reason": decision_reason,
            }
        )
    return rows


def normalize_profile_grid() -> list[dict[str, Any]]:
    path = SOURCE_DIRS["profile_eval"] / "grid_all_costs.csv"
    frame = pd.read_csv(path)
    chosen_config = "S1.500_T11.075_P0.10_R2.60_BE"
    chosen = {
        "A_V3_1120S": "BALANCED_SHADOW",
        "B_V3_1120S_WICK010": "NUMERIC_PARETO_SHADOW",
        "C_V3_1120S_WICK010_0950S": "HIGHER_FREQUENCY_SHADOW",
    }
    config_fields = [
        "profile",
        "component_1120_short",
        "component_wick_plus_010",
        "component_0950_short",
        "initial_stop_pct",
        "t1_pct",
        "partial_pct",
        "runner_target_pct",
        "runner_stop",
    ]
    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        profile = str(row.get("profile"))
        config = str(row.get("config_id"))
        if profile in chosen and config == chosen_config:
            decision = chosen[profile]
            reason = "Selected on TRAIN+VALIDATION before pseudo-test reveal"
        elif profile == "P0_V3":
            decision = "REFERENCE_GRID"
            reason = "Two-stage exit on unchanged V3 entry book"
        else:
            decision = "REJECT_OR_NEIGHBOR"
            reason = "Not selected by the frozen development rule"
        all_fills = number(row, "all_fills")
        rows.append(
            {
                **base_ledger_row(source_suite="PROFILE_GRID", source_path=path),
                "experiment_id": f"PROFILE::{profile}::{config}::COST_{row['cost_bps']}",
                "family": row.get("profile_role"),
                "configuration_json": row_json(row, config_fields),
                "exact_change": f"{profile} / {config}",
                "reason_for_test": "Controlled component ablation and local exit grid",
                "cost_bps": number(row, "cost_bps"),
                "split_protocol": "TRAIN_12_VALIDATION_7_PSEUDO_TEST_6",
                "all_trades": all_fills,
                "all_average_trades_per_day": all_fills / 25.0,
                "all_win_rate_pct": number(row, "all_win_rate_pct"),
                "all_target_hit_rate_pct": number(row, "all_t1_hit_rate_pct"),
                "all_net_profit_pct": number(row, "all_net_pct"),
                "all_profit_factor": number(row, "all_pf"),
                "all_expectancy_pct": number(row, "all_expectancy_pct"),
                "all_maximum_drawdown_pct": number(row, "all_max_drawdown_pct"),
                "train_trades": number(row, "train_fills"),
                "train_win_rate_pct": number(row, "train_win_rate_pct"),
                "train_net_profit_pct": number(row, "train_net_pct"),
                "train_profit_factor": number(row, "train_pf"),
                "validation_trades": number(row, "validation_fills"),
                "validation_win_rate_pct": number(row, "validation_win_rate_pct"),
                "validation_net_profit_pct": number(row, "validation_net_pct"),
                "validation_profit_factor": number(row, "validation_pf"),
                "pseudo_test_trades": number(row, "pseudo_test_fills"),
                "pseudo_test_win_rate_pct": number(row, "pseudo_test_win_rate_pct"),
                "pseudo_test_net_profit_pct": number(row, "pseudo_test_net_pct"),
                "pseudo_test_profit_factor": number(row, "pseudo_test_pf"),
                "decision": decision,
                "decision_reason": reason,
            }
        )
    return rows


def load_v7_signals() -> pd.DataFrame:
    parts = []
    for month in ("26AUG", "26SEP"):
        stem = V7_CACHE_DIR / month
        loaded = v5.v6._load_cached(stem)
        if loaded is None:
            raise FileNotFoundError(
                f"Missing raw V7 cache {stem}; run python .codex_tmp/v13_v5_build_v7.py"
            )
        signals, paths = loaded
        signals = signals.copy()
        signals["day"] = pd.to_datetime(signals["day"]).dt.date
        signals["contract_month"] = month
        parts.append((signals, paths))
    combined, _ = v5.v6.concat_regimes(parts)
    return combined


def replay_v7() -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]], dict[str, int]]:
    strict, _, days, _, _, _, _ = v5.load_market(
        THROUGH_DAY, rebuild_cache=False, refresh_eligibility=False
    )
    v7_raw = load_v7_signals()
    context = v3.load_nifty_first_bar_context(v7_raw["contract_month"].unique())
    v7_policy = v3.annotate_nifty_gate(v7_raw, context)
    v7_policy = v7_policy.loc[v7_policy["nifty_first_bar_gate_pass"]].copy()
    v7_policy = v2.apply_policy(v7_policy, v2.POLICIES[v3.BASE_POLICY_NAME])
    strict_orders = v5.select_orders(strict, v3.active_setups())
    v7_orders = v5.select_orders(v7_policy, v3.active_setups())
    paths, quality = v5.materialize_raw_paths(v7_orders, cutoff=v5.OFFICIAL_CUTOFF)
    spec = v5.ExitSpec(1.50, 1.075, 0.10, 2.60)
    audit = v5.simulate_scaleout(v7_orders, paths, spec, cost_bps=COST_BPS)
    audit["confirmation_policy"] = "V7_HIGH_LOW_BREAKOUT"

    keys = ["day", "tradingsymbol", "side", "hhmm_int"]
    strict_keys = strict_orders[keys].drop_duplicates().assign(strict_selected=True)
    tagged = v7_orders.merge(strict_keys, on=keys, how="left")
    new_sids = set(tagged.loc[tagged["strict_selected"].isna(), "sid"].astype(int))
    new_audit = audit.loc[audit["sid"].isin(new_sids)].copy()
    split = v5.split_days(days)
    metrics_rows = []
    ledger_rows = []
    for name, current in (("V7_FULL_BOOK", audit), ("V7_NEW_VERSUS_STRICT", new_audit)):
        all_metrics = v5.metrics(current, days, label=name)
        record: dict[str, Any] = {"variant": name, **all_metrics}
        for split_name in ("TRAIN", "VALIDATION", "PSEUDO_TEST"):
            current_metrics = v5.metrics(
                current, split[split_name], label=split_name
            )
            for key, value in current_metrics.items():
                if key != "label":
                    record[f"{split_name.lower()}_{key}"] = value
        metrics_rows.append(record)
        ledger_rows.append(
            {
                **base_ledger_row(
                    source_suite="RAW_CONFIRMATION_REBUILD",
                    source_path=V7_CACHE_DIR / "26AUG.parquet",
                ),
                "experiment_id": f"CONFIRMATION::{name}",
                "family": "CONFIRMATION_POLICY",
                "configuration_json": json.dumps(
                    {
                        "confirmation": "V7_HIGH_LOW_BREAKOUT",
                        "exit": "SL1.50_T1.075_P10_R2.60_BE",
                        "cutoff": "15:15",
                    },
                    sort_keys=True,
                ),
                "exact_change": "Replace strict directional S+1 close with candle-extreme breakout",
                "reason_for_test": "Recover strict-confirmation rejects using raw data",
                "cost_bps": COST_BPS,
                "split_protocol": "TRAIN_12_VALIDATION_7_PSEUDO_TEST_6",
                "all_trades": all_metrics["executed_trades"],
                "all_average_trades_per_day": all_metrics["average_trades_per_day"],
                "all_win_rate_pct": all_metrics["win_rate_pct"],
                "all_target_hit_rate_pct": all_metrics["target_hit_rate_pct"],
                "all_net_profit_pct": all_metrics["net_profit_pct"],
                "all_profit_factor": all_metrics["profit_factor"],
                "all_expectancy_pct": all_metrics["expectancy_pct"],
                "all_maximum_drawdown_pct": all_metrics["maximum_drawdown_pct"],
                "train_trades": record["train_executed_trades"],
                "train_win_rate_pct": record["train_win_rate_pct"],
                "train_net_profit_pct": record["train_net_profit_pct"],
                "train_profit_factor": record["train_profit_factor"],
                "validation_trades": record["validation_executed_trades"],
                "validation_win_rate_pct": record["validation_win_rate_pct"],
                "validation_net_profit_pct": record["validation_net_profit_pct"],
                "validation_profit_factor": record["validation_profit_factor"],
                "pseudo_test_trades": record["pseudo_test_executed_trades"],
                "pseudo_test_win_rate_pct": record["pseudo_test_win_rate_pct"],
                "pseudo_test_net_profit_pct": record["pseudo_test_net_profit_pct"],
                "pseudo_test_profit_factor": record["pseudo_test_profit_factor"],
                "decision": "REJECT",
                "decision_reason": "Negative validation expectancy/PF below 1 and worse drawdown",
            }
        )
    funnel = {
        "v7_raw_confirmed_rows": int(len(v7_raw)),
        "v7_after_nifty_oi_policy": int(len(v7_policy)),
        "v7_selected_orders": int(len(v7_orders)),
        "v7_fills": int(audit["filled"].sum()),
        "v7_new_orders_vs_strict": int(len(new_sids)),
        "v7_exact_continuous_paths": int(
            (quality["exact_cutoff_present"] & quality["continuous_one_minute_path"]).sum()
        ),
    }
    return pd.DataFrame(metrics_rows), audit, ledger_rows, funnel


def copy_source_artifacts() -> None:
    for name, source in SOURCE_DIRS.items():
        if not source.is_dir():
            raise FileNotFoundError(f"Missing research source directory: {source}")
        target = RESULT_DIR / name
        target.mkdir(parents=True, exist_ok=True)
        for path in source.iterdir():
            if path.is_file() and path.suffix.lower() != ".pyc":
                shutil.copy2(path, target / path.name)


def make_manifest() -> pd.DataFrame:
    rows = []
    for path in sorted(RESULT_DIR.rglob("*")):
        if not path.is_file() or path == v5.RESEARCH_MANIFEST_PATH:
            continue
        rows.append(
            {
                "relative_path": str(path.relative_to(RESULT_DIR)),
                "bytes": path.stat().st_size,
                "sha256": sha256(path),
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    v3_before = sha256(Path(v3.__file__).resolve())
    v5.validate_configuration()
    if not v5.COMPARISON_PATH.is_file():
        raise FileNotFoundError(
            "Run fno_v13_corrected_v5_backtest.py before consolidating research."
        )
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    copy_source_artifacts()

    v7_metrics, v7_audit, v7_ledger, funnel = replay_v7()
    common.atomic_write_csv(v7_metrics, V7_METRICS_PATH)
    common.atomic_write_csv(v7_audit, V7_TRADES_PATH)

    ledger_rows = [*normalize_timing(), *normalize_exit(), *normalize_profile_grid(), *v7_ledger]
    ledger = pd.DataFrame(ledger_rows)
    ordered_columns = [
        "experiment_id",
        "run_timestamp_ist",
        "source_suite",
        "family",
        "configuration_json",
        "exact_change",
        "reason_for_test",
        "data_period",
        "cost_bps",
        "split_protocol",
        "all_trades",
        "all_average_trades_per_day",
        "all_win_rate_pct",
        "all_target_hit_rate_pct",
        "all_net_profit_pct",
        "all_profit_factor",
        "all_expectancy_pct",
        "all_maximum_drawdown_pct",
        "train_trades",
        "train_win_rate_pct",
        "train_net_profit_pct",
        "train_profit_factor",
        "validation_trades",
        "validation_win_rate_pct",
        "validation_net_profit_pct",
        "validation_profit_factor",
        "pseudo_test_trades",
        "pseudo_test_win_rate_pct",
        "pseudo_test_net_profit_pct",
        "pseudo_test_profit_factor",
        "delta_trades_vs_v3",
        "delta_win_rate_pct_vs_v3",
        "delta_net_profit_pct_vs_v3",
        "delta_profit_factor_vs_v3",
        "decision",
        "decision_reason",
        "source_artifact",
    ]
    ledger = ledger.reindex(columns=ordered_columns)
    common.atomic_write_csv(ledger, LEDGER_PATH)

    comparison = pd.read_csv(v5.COMPARISON_PATH)
    leaderboard_columns = [
        "label",
        "configured_default",
        "executed_trades",
        "average_trades_per_day",
        "win_rate_pct",
        "target_hit_rate_pct",
        "runner_target_hit_rate_pct",
        "stop_hit_rate_pct",
        "pre_cost_return_pct",
        "total_cost_pct",
        "net_profit_pct",
        "profit_factor",
        "expectancy_pct",
        "average_winning_trade_pct",
        "average_losing_trade_pct",
        "payoff_ratio",
        "maximum_drawdown_pct",
    ]
    leaderboard = comparison[leaderboard_columns].copy()
    common.atomic_write_csv(leaderboard, LEADERBOARD_PATH)

    counts = ledger.groupby(["source_suite", "decision"], dropna=False).size().reset_index(name="rows")
    v7_full = v7_metrics.loc[v7_metrics["variant"].eq("V7_FULL_BOOK")].iloc[0]
    report = "\n".join(
        [
            "# FNO V13-v5 research audit and experiment index",
            "",
            "## Scope and verdict",
            "",
            "This archive contains every completed timing/entry, exit, NIFTY and controlled "
            "profile row generated during V13-v5 research, including rejected and not-run "
            "experiments. No profile is production-promoted because all 25 sessions were "
            "previously inspected and the added timing legs are sparse.",
            "",
            "`higher_frequency` is now the explicitly configured default at the user's "
            "direction. This changes configuration selection, not the evidence grade: it "
            "remains an experimental forward-shadow profile.",
            "",
            "The controlled profile archive was frozen with adverse trigger-gap handling but "
            "before the final later-bar stop-gap correction. That correction affects one native "
            "V3 trade by about -0.03745 percentage point and no final scale-out profile trade, "
            "so profile selection and profile metrics are unchanged; the main V5 comparison is "
            "the authoritative corrected-baseline row.",
            "",
            "## Final profile leaderboard (5 bps)",
            "",
            leaderboard.to_markdown(index=False, floatfmt=".3f"),
            "",
            "V5 target hit is a first-stage touch with only 10% or 20% booked; the runner "
            "hit column is the economically stricter +2.60% outcome. Returns are summed cash-"
            "equity percentages, not F&O premium or capital-sized INR P&L.",
            "",
            "## Unified ledger coverage",
            "",
            counts.to_markdown(index=False),
            "",
            f"The normalized ledger contains **{len(ledger):,} rows**. Native source tables "
            "are also preserved because their protocols and extra diagnostics cannot be "
            "losslessly compressed into one schema.",
            "",
            "## One-minute confirmation funnel",
            "",
            "Stage | Count",
            "--- | ---:",
            "Loose-gate exact positive-range S+1 candles | 9,935",
            "Strict directional S+1 survivors | 4,025",
            "Rejected by strict direction versus V7-valid pool | 5,910",
            "Strict survivors after NIFTY/OI policy | 3,837",
            "Strict survivors inside 12 V3 active cells | 921",
            "V3 selected orders / fills | 79 / 78",
            f"V7 selected orders / fills | {funnel['v7_selected_orders']} / {funnel['v7_fills']}",
            "",
            f"Corrected V7 full-book validation: {int(v7_full['validation_executed_trades'])} "
            f"fills, PF {v7_full['validation_profit_factor']:.3f}, net "
            f"{v7_full['validation_net_profit_pct']:+.3f}%, full drawdown "
            f"{v7_full['maximum_drawdown_pct']:.3f}%. It is rejected.",
            "",
            "## Native artifact directories",
            "",
            "- `validation_audit/`: end-to-end V3 code audit, rule registry and support matrix.",
            "- `timing_entry/`: all 136 time/side inventory cells, 124 additions, removals, "
            "max-entry and entry-filter tests, pseudo reveal and rejected rows.",
            "- `exit_robustness/`: 589 exit configurations, MAE/MFE, NIFTY ablations, cost, "
            "fill-delay, top-trade removal, bootstrap and Monte Carlo diagnostics.",
            "- `profile_eval/`: 2³ entry-component ablation, 24 local exit configurations "
            "per profile at 5/10/20/30 bps, frozen development decisions and pseudo reveal.",
            "",
            "## Reproduce",
            "",
            "```powershell",
            "python fno_v13_corrected_v3_backtest.py --through-day 2026-09-03 --cost-bps 5",
            "python .codex_tmp/v13_v5_build_v7.py",
            "python .codex_tmp/v13_v5_timing_entry/run_timing_entry_experiments.py",
            "python .codex_tmp/v13_v5_exit_robustness/run_exit_robustness.py",
            "python .codex_tmp/v13_v5_exit_robustness/run_nifty_firstbar_ablation.py",
            "python .codex_tmp/v13_v5_profile_eval/run_profile_eval.py",
            "# Configured default run",
            "python fno_v13_corrected_v5_backtest.py --profile higher_frequency --through-day 2026-09-03 --cost-bps 5",
            "# Explicit full-profile research comparison",
            "python fno_v13_corrected_v5_backtest.py --profile all --through-day 2026-09-03 --cost-bps 5",
            "python fno_v13_v5_research.py",
            "```",
            "",
            f"V13-v3 SHA-256 before/after consolidation: `{v3_before}` / "
            f"`{sha256(Path(v3.__file__).resolve())}`.",
            "",
            f"Unified ledger: `{LEDGER_PATH}`",
            f"V7 trade audit: `{V7_TRADES_PATH}`",
            f"Artifact manifest: `{v5.RESEARCH_MANIFEST_PATH}`",
            f"Created-file index: `{CREATED_FILES_PATH}`",
            "",
        ]
    )
    common.atomic_write_text(v5.RESEARCH_REPORT_PATH, report)
    common.atomic_write_text(WORKSPACE_REPORT_PATH, report)
    created_paths = [
        WORKSPACE / "fno_v13_corrected_v5_backtest.py",
        WORKSPACE / "fno_v13_v5_research.py",
        WORKSPACE / "tests" / "test_fno_v13_corrected_v5_backtest.py",
        TEMP_ROOT / "v13_v5_build_v7.py",
        SOURCE_DIRS["timing_entry"] / "run_timing_entry_experiments.py",
        SOURCE_DIRS["exit_robustness"] / "run_exit_robustness.py",
        SOURCE_DIRS["exit_robustness"] / "run_nifty_firstbar_ablation.py",
        SOURCE_DIRS["profile_eval"] / "run_profile_eval.py",
        v5.WORKSPACE_REPORT_PATH,
        WORKSPACE_REPORT_PATH,
        v5.REPORT_PATH,
        v5.COMPARISON_PATH,
        v5.PARAMETER_REGISTRY_PATH,
        v5.ELIGIBILITY_PATH,
        v5.PROVENANCE_PATH,
        v5.RESEARCH_REPORT_PATH,
        LEDGER_PATH,
        LEADERBOARD_PATH,
        V7_METRICS_PATH,
        V7_TRADES_PATH,
    ]
    created = pd.DataFrame(
        [
            {
                "path": str(path.resolve()),
                "bytes": path.stat().st_size,
                "sha256": sha256(path),
                "role": (
                    "WORKSPACE_SOURCE_OR_REPORT"
                    if path.is_relative_to(WORKSPACE)
                    else "V13_V5_RESULT"
                ),
            }
            for path in created_paths
            if path.is_file()
        ]
    )
    common.atomic_write_csv(created, CREATED_FILES_PATH)
    manifest = make_manifest()
    common.atomic_write_csv(manifest, v5.RESEARCH_MANIFEST_PATH)
    if sha256(Path(v3.__file__).resolve()) != v3_before:
        raise RuntimeError("V13-v3 source changed during research consolidation.")
    print(f"[V13-v5][RESEARCH] ledger rows={len(ledger):,}")
    print(f"[V13-v5][RESEARCH] report={v5.RESEARCH_REPORT_PATH}")
    print(f"[V13-v5][RESEARCH] manifest rows={len(manifest):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
