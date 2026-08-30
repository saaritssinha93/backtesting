"""Run the complete research-only FNO V11 staged comparison.

Stage 0 is the hash-bound V10 Stage-7 + 09:35 LONG max-0.50 + Gap2
current-mixed control.  Every executable V11 challenger is replayed in its own
directory through the same 65-session candidate cache, one-minute state
machine, chronological portfolio ledger, and three cost scenarios.  Stages
that require unavailable point-in-time or futures-execution data fail closed
and are recorded as blocked rather than being approximated silently.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import sys
from contextlib import nullcontext
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v10_backtest as v10_backtest
import fno_v10_backtest_config as locked_config
import fno_v10_experiment_backtest as experiment
import fno_v10_followup_challenger_research as filters
import fno_v10_gap_guard_research as gaps
import fno_v11_execution_runtime as execution_runtime
import fno_v11_selection_runtime as selection_runtime
import fno_v11_variant_registry as registry
import fno_v8_windowed_1m_entry_backtest as engine


SCHEMA_VERSION = "fno_v11_full_staged_comparison_v1"
OUTPUT_ROOT = common.FNO_ROOT / "strategy_research" / "v11_fno_staged_research_v1"
FROZEN_V10_REFERENCE_RUN = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\v10_max050_gap2_full_history_v1"
    r"\run_20260830T163837220506+0530"
)
BASE_GAP_VARIANT = "MAX_2_BPS"
BOOTSTRAP_REPLICATES = 4_000


@dataclass(frozen=True)
class ExperimentDefinition:
    variant_id: str
    stage_id: str
    family: str
    description: str
    registry_variant_id: str | None = None
    runtime_spec: execution_runtime.RuntimeSpec = execution_runtime.RuntimeSpec()
    gap_variant: str = BASE_GAP_VARIANT
    disabled_setup_id: str | None = None
    component_variant_ids: tuple[str, ...] = ()
    post_hoc: bool = False

    @property
    def is_control(self) -> bool:
        return self.variant_id == "V10_STAGE0_FROZEN_CONTROL"

    def payload(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "runtime_spec": asdict(self.runtime_spec),
            "research_only": True,
            "promotion_eligible": False,
            "live_or_paper_authority": False,
        }


@dataclass(frozen=True)
class PreparedExperiment:
    definition: ExperimentDefinition
    candidates: pd.DataFrame
    decisions: pd.DataFrame
    setups: tuple[engine.V8Setup, ...]
    setup_patch: Mapping[str, Any]


def _selection_experiments() -> tuple[ExperimentDefinition, ...]:
    definitions: list[ExperimentDefinition] = []
    for spec in registry.VARIANT_SPECS:
        if spec.variant_id == registry.CONTROL_VARIANT_ID:
            continue
        if spec.selection_rule is not None:
            definitions.append(
                ExperimentDefinition(
                    variant_id=spec.variant_id,
                    stage_id="STAGE_05_SELECTION",
                    family="FIVE_MINUTE_SELECTION",
                    description=spec.description,
                    registry_variant_id=spec.variant_id,
                )
            )
    return tuple(definitions)


def _setup_experiments() -> tuple[ExperimentDefinition, ...]:
    definitions: list[ExperimentDefinition] = []
    for spec in registry.VARIANT_SPECS:
        if not any(
            (
                spec.disabled_setup_id is not None,
                spec.picker_override is not None,
                spec.cap_override is not None,
            )
        ):
            continue
        definitions.append(
            ExperimentDefinition(
                variant_id=spec.variant_id,
                stage_id="STAGE_06_SETUP_PICKER_CAP",
                family=spec.mechanism_type,
                description=spec.description,
                registry_variant_id=spec.variant_id,
            )
        )
    definitions.extend(
        (
            ExperimentDefinition(
                "V11_S6_DISABLE_0930_SHORT",
                "STAGE_06_SETUP_PICKER_CAP",
                "DISABLED_SETUP",
                "Disable only 09:30 SHORT as an isolated global-ledger ablation",
                disabled_setup_id="09:30_SHORT",
            ),
            ExperimentDefinition(
                "V11_S6_DISABLE_0940_SHORT",
                "STAGE_06_SETUP_PICKER_CAP",
                "DISABLED_SETUP",
                "Disable only 09:40 SHORT as an isolated global-ledger ablation",
                disabled_setup_id="09:40_SHORT",
            ),
        )
    )
    return tuple(definitions)


CONTROL = ExperimentDefinition(
    "V10_STAGE0_FROZEN_CONTROL",
    "STAGE_00_FROZEN_V10",
    "CONTROL",
    "Frozen V10 Stage7 + 09:35 LONG max-0.50 + Gap2 mixed-cap control",
    registry_variant_id=registry.CONTROL_VARIANT_ID,
)

ENTRY_EXPERIMENTS = (
    ExperimentDefinition(
        "V11_S4_0930_SHORT_ENTRY_NOT_BEFORE_S3",
        "STAGE_04_ENTRY_TIMING",
        "ENTRY_TIMING",
        "09:30 SHORT pending stop cannot fill before S+3",
        runtime_spec=execution_runtime.RuntimeSpec(
            entry_setup_id="09:30_SHORT", entry_not_before_minute=3
        ),
    ),
    ExperimentDefinition(
        "V11_S4_0940_SHORT_ENTRY_NOT_BEFORE_S3",
        "STAGE_04_ENTRY_TIMING",
        "ENTRY_TIMING",
        "09:40 SHORT pending stop cannot fill before S+3",
        runtime_spec=execution_runtime.RuntimeSpec(
            entry_setup_id="09:40_SHORT", entry_not_before_minute=3
        ),
    ),
)

EXIT_EXPERIMENTS = (
    ExperimentDefinition(
        "V11_S7_BREAK_EVEN_AFTER_050R_NEXT_BAR",
        "STAGE_07_EXIT_AND_GAP",
        "EXIT_RULE",
        "Arm entry-price stop after a completed +0.50R bar; effective next bar",
        runtime_spec=execution_runtime.RuntimeSpec(
            exit_rule="BREAK_EVEN_NEXT_BAR", exit_activation_r=0.50
        ),
    ),
    ExperimentDefinition(
        "V11_S7_BREAK_EVEN_AFTER_075R_NEXT_BAR",
        "STAGE_07_EXIT_AND_GAP",
        "EXIT_RULE",
        "Arm entry-price stop after a completed +0.75R bar; effective next bar",
        runtime_spec=execution_runtime.RuntimeSpec(
            exit_rule="BREAK_EVEN_NEXT_BAR", exit_activation_r=0.75
        ),
    ),
    ExperimentDefinition(
        "V11_S7_BREAK_EVEN_AFTER_100R_NEXT_BAR",
        "STAGE_07_EXIT_AND_GAP",
        "EXIT_RULE",
        "Arm entry-price stop after a completed +1.00R bar; effective next bar",
        runtime_spec=execution_runtime.RuntimeSpec(
            exit_rule="BREAK_EVEN_NEXT_BAR", exit_activation_r=1.00
        ),
    ),
    ExperimentDefinition(
        "V11_S7_LATE_1430_BE_AFTER_100R_NEXT_BAR",
        "STAGE_07_EXIT_AND_GAP",
        "EXIT_RULE",
        "At/after completed 14:30 bar, arm break-even after +1R; next bar only",
        runtime_spec=execution_runtime.RuntimeSpec(
            exit_rule="LATE_1430_BREAK_EVEN_NEXT_BAR", exit_activation_r=1.00
        ),
    ),
    ExperimentDefinition(
        "V11_S7_TRAIL_1R_AFTER_2R_NEXT_BAR",
        "STAGE_07_EXIT_AND_GAP",
        "EXIT_RULE",
        "After completed +2R bar, trail one initial R behind best price next bar",
        runtime_spec=execution_runtime.RuntimeSpec(
            exit_rule="TRAIL_1R_AFTER_2R_NEXT_BAR", exit_activation_r=2.00
        ),
    ),
    ExperimentDefinition(
        "V11_S7_REJECT_ALL_GAP_FILLS",
        "STAGE_07_EXIT_AND_GAP",
        "GAP_POLICY",
        "Reject every gap-through-trigger fill instead of the frozen Gap2 guard",
        gap_variant="REJECT_ALL_GAP_FILLS",
    ),
)

PORTFOLIO_EXPERIMENTS = (
    ExperimentDefinition(
        "V11_S9_SAME_SYMBOL_SAME_SIDE_MAX_2",
        "STAGE_09_PORTFOLIO",
        "PORTFOLIO_SYMBOL_LIMIT",
        "Allow two concurrent same-side reservations per symbol; opposite side remains prohibited",
        runtime_spec=execution_runtime.RuntimeSpec(same_side_symbol_limit=2),
    ),
)

INDIVIDUAL_EXPERIMENTS = (
    ENTRY_EXPERIMENTS
    + _selection_experiments()
    + _setup_experiments()
    + EXIT_EXPERIMENTS
    + PORTFOLIO_EXPERIMENTS
)
ALL_PREDECLARED_EXPERIMENTS = (CONTROL,) + INDIVIDUAL_EXPERIMENTS


BLOCKED_TESTS: tuple[dict[str, str], ...] = (
    {
        "stage_id": "STAGE_01_DATA_VALIDITY",
        "test_id": "POINT_IN_TIME_UNIVERSE_FULL_HISTORY",
        "status": "BLOCKED_VALIDITY",
        "reason": "Dated masters/universes exist for only 13 recent sessions; the 59-session core reuses an Aug-11 universe backward.",
    },
    {
        "stage_id": "STAGE_01_DATA_VALIDITY",
        "test_id": "AUG_26_EXACT_1530_REPLAY",
        "status": "BLOCKED_VALIDITY",
        "reason": "Aug-26 equity 1-minute paths end at 15:15; no exact 15:30 current-strategy replay is available.",
    },
    {
        "stage_id": "STAGE_01_DATA_VALIDITY",
        "test_id": "FULL_EXACT_1530_PATHS",
        "status": "BLOCKED_VALIDITY",
        "reason": "246 of 1,134 selected candidate paths stop at 15:15; only 888 reach 15:30.",
    },
    {
        "stage_id": "STAGE_02_FUTURES_EXECUTION",
        "test_id": "ROLLING_FRONT_MONTH_FUTURES_1M",
        "status": "BLOCKED_VALIDITY",
        "reason": "Complete actual front-month futures 1-minute histories for MAY through SEP are absent.",
    },
    {
        "stage_id": "STAGE_02_FUTURES_EXECUTION",
        "test_id": "DATED_LOT_TICK_MARGIN_COSTS",
        "status": "BLOCKED_VALIDITY",
        "reason": "Full-history dated masters and historical SPAN/exposure margin snapshots are absent.",
    },
    {
        "stage_id": "STAGE_08_STRUCTURAL_FILTERS",
        "test_id": "FUTURES_PRICE_OI_PERSISTENCE",
        "status": "BLOCKED_VALIDITY",
        "reason": "Current immutable cache has futures OI selection but no complete rolling futures 1-minute price/execution path or causal OI-persistence sidecar.",
    },
    {
        "stage_id": "STAGE_08_STRUCTURAL_FILTERS",
        "test_id": "INDEX_SECTOR_VWAP_ALIGNMENT",
        "status": "BLOCKED_VALIDITY",
        "reason": "No snapshot-bound point-in-time index/sector histories and dated membership mappings cover all 65 sessions.",
    },
    {
        "stage_id": "STAGE_08_STRUCTURAL_FILTERS",
        "test_id": "ATR_NORMALIZED_RISK",
        "status": "BLOCKED_VALIDITY",
        "reason": "The frozen candidate cache lacks a bound prior-session ATR history; deriving it from partial paths would change the data contract.",
    },
    {
        "stage_id": "STAGE_09_PORTFOLIO",
        "test_id": "ACTUAL_FUTURES_RISK_SIZING",
        "status": "BLOCKED_VALIDITY",
        "reason": "Actual contract prices, dated lot sizes, and historical margins are not complete for full history.",
    },
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _gap_spec(variant: str) -> gaps.GapGuardSpec:
    return next(spec for spec in gaps.GAP_GUARDS if spec.variant == variant)


def _passthrough_decisions(
    candidates: pd.DataFrame, definition: ExperimentDefinition
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "candidate_id": candidates["candidate_id"].astype(str),
            "variant_id": definition.variant_id,
            "stage_id": definition.stage_id,
            "setup_id": candidates["setup_id"].astype(str),
            "kept": True,
            "reason": "V11_RUNTIME_OR_GAP_PASSTHROUGH",
            "metric": "",
            "measured_value": np.nan,
            "threshold": np.nan,
            "comparator": "",
        }
    )


def _prepare_experiment(
    definition: ExperimentDefinition,
    baseline_selected: pd.DataFrame,
) -> PreparedExperiment:
    experiment.configure_engine(locked_config.ACTIVE_VARIANT)
    engine._confirmation_check = experiment._NEUTRAL_CONFIRMATION_CHECK
    base_setups = tuple(engine.ACTIVE_SETUPS)
    candidates = baseline_selected.copy(deep=True)
    decisions = _passthrough_decisions(candidates, definition)
    setups = base_setups
    patch_payload: dict[str, Any] = {
        "registry_variant_id": definition.registry_variant_id,
        "field_overrides": [],
        "disabled_setup_ids": [],
        "requires_runner_picker_hook": False,
    }

    if definition.registry_variant_id is not None:
        candidates, decisions = selection_runtime.apply_variant_to_selected_candidates(
            candidates, definition.registry_variant_id
        )
        setups, metadata = selection_runtime.derive_patched_engine_setups(
            base_setups, definition.registry_variant_id
        )
        patch_payload = {
            "registry_variant_id": metadata.variant_id,
            "registry_micro_stage_id": metadata.stage_id,
            "field_overrides": [asdict(item) for item in metadata.field_overrides],
            "disabled_setup_ids": list(metadata.disabled_setup_ids),
            "requires_runner_picker_hook": metadata.requires_runner_picker_hook,
            "picker_hook": (
                asdict(metadata.picker_hook) if metadata.picker_hook else None
            ),
        }

    if definition.disabled_setup_id is not None:
        target = candidates["setup_id"].astype(str).eq(definition.disabled_setup_id)
        decisions = _passthrough_decisions(candidates, definition)
        decisions.loc[target, "kept"] = False
        decisions.loc[target, "reason"] = "V11_DISABLED_SETUP_REJECTED"
        candidates = candidates.loc[~target].copy().reset_index(drop=True)
        setups = tuple(
            setup for setup in setups if setup.setup_id != definition.disabled_setup_id
        )
        patch_payload["disabled_setup_ids"] = [definition.disabled_setup_id]

    engine.ACTIVE_SETUPS = tuple(setups)
    if any(setup.picker == "min_volume" for setup in setups):
        if not patch_payload.get("requires_runner_picker_hook"):
            raise AssertionError("min_volume setup is missing its mandatory V11 hook")
    return PreparedExperiment(
        definition=definition,
        candidates=candidates,
        decisions=decisions,
        setups=tuple(setups),
        setup_patch=patch_payload,
    )


def _periods(
    sessions: Sequence[date], segments: Sequence[Mapping[str, Any]]
) -> list[tuple[str, tuple[date, ...]]]:
    ordered = tuple(sorted(set(sessions)))
    midpoint = len(ordered) // 2
    periods: list[tuple[str, tuple[date, ...]]] = [
        ("FULL_USABLE", ordered),
        (
            "CORE_59",
            tuple(
                day
                for day in ordered
                if day < v10_backtest.MAX050_GAP2_EXTENSION_DAY
            ),
        ),
        (
            "FORWARD_EXTENSION",
            tuple(
                day
                for day in ordered
                if day >= v10_backtest.MAX050_GAP2_EXTENSION_DAY
            ),
        ),
        ("FIRST_HALF", ordered[:midpoint]),
        ("SECOND_HALF", ordered[midpoint:]),
        ("LAST_14", ordered[-14:]),
    ]
    for year, month in sorted({(day.year, day.month) for day in ordered}):
        periods.append(
            (
                f"MONTH_{year:04d}_{month:02d}",
                tuple(day for day in ordered if (day.year, day.month) == (year, month)),
            )
        )
    for segment in segments:
        segment_id = str(dict(segment["segment"])["segment_id"])
        periods.append(
            (
                f"SEGMENT_{segment_id}",
                tuple(date.fromisoformat(value) for value in segment["sessions"]),
            )
        )
    return [(name, days) for name, days in periods if days]


def _metric_rows(
    audit: pd.DataFrame,
    sessions: Sequence[date],
    segments: Sequence[Mapping[str, Any]],
    *,
    definition: ExperimentDefinition,
    scenario: str,
    cost_bps: float,
    slippage_bps: float,
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    audit_days = audit["session_date"].map(engine._parse_day)
    metric_rows: list[dict[str, Any]] = []
    full_daily = pd.DataFrame()
    spec = _gap_spec(definition.gap_variant)
    for period, days in _periods(sessions, segments):
        subset = audit.loc[audit_days.isin(set(days))].copy()
        row, daily = gaps.metric_row(
            subset,
            days,
            dataset="ALL_USABLE_HISTORY",
            period=period,
            scenario=scenario,
            spec=spec,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
        )
        row.update(
            {
                "variant_id": definition.variant_id,
                "stage_id": definition.stage_id,
                "family": definition.family,
                "description": definition.description,
                "gap_variant": definition.gap_variant,
                "post_hoc": definition.post_hoc,
            }
        )
        metric_rows.append(row)
        if period == "FULL_USABLE":
            full_daily = daily.copy()
            full_daily["variant_id"] = definition.variant_id
            full_daily["stage_id"] = definition.stage_id
            full_daily["family"] = definition.family
    return metric_rows, full_daily


def _bootstrap_row(
    audit: pd.DataFrame,
    sessions: Sequence[date],
    *,
    definition: ExperimentDefinition,
    scenario: str,
) -> dict[str, Any]:
    ordered = tuple(sorted(set(sessions)))
    closed = audit.loc[gaps._closed_mask(audit)].copy()
    closed["_day"] = closed["session_date"].map(engine._parse_day)
    closed["_return"] = pd.to_numeric(closed["net_return_pct"], errors="coerce")
    closed["_pnl"] = pd.to_numeric(closed["net_pnl_rs"], errors="coerce")
    by_day = closed.groupby("_day", sort=False).agg(
        positive_points=("_return", lambda values: float(values[values > 0].sum())),
        loss_points=("_return", lambda values: float(-values[values < 0].sum())),
        net_points=("_return", "sum"),
        net_pnl_rs=("_pnl", "sum"),
    )
    positive = np.asarray(
        [float(by_day.at[day, "positive_points"]) if day in by_day.index else 0.0 for day in ordered]
    )
    losses = np.asarray(
        [float(by_day.at[day, "loss_points"]) if day in by_day.index else 0.0 for day in ordered]
    )
    points = np.asarray(
        [float(by_day.at[day, "net_points"]) if day in by_day.index else 0.0 for day in ordered]
    )
    pnl = np.asarray(
        [float(by_day.at[day, "net_pnl_rs"]) if day in by_day.index else 0.0 for day in ordered]
    )
    seed_material = f"{SCHEMA_VERSION}|{definition.variant_id}|{scenario}".encode()
    seed = int.from_bytes(hashlib.sha256(seed_material).digest()[:8], "big")
    rng = np.random.default_rng(seed)
    sampled = rng.integers(0, len(ordered), size=(BOOTSTRAP_REPLICATES, len(ordered)))
    sampled_profit = positive[sampled].sum(axis=1)
    sampled_loss = losses[sampled].sum(axis=1)
    sampled_pf = np.divide(
        sampled_profit,
        sampled_loss,
        out=np.full_like(sampled_profit, np.inf),
        where=sampled_loss > 0,
    )
    finite_pf = sampled_pf[np.isfinite(sampled_pf)]
    daily_pnl = pnl[sampled].mean(axis=1)
    net_points = points[sampled].sum(axis=1)
    daily_total = pd.Series(pnl, index=ordered)
    total_pnl = float(daily_total.sum())
    positive_days = daily_total.loc[daily_total > 0].sort_values(ascending=False)
    best10_share = (
        float(positive_days.head(10).sum() / total_pnl * 100.0)
        if total_pnl > 0
        else math.nan
    )
    monthly = daily_total.groupby([day.strftime("%Y-%m") for day in ordered]).sum()
    best_month_share = (
        float(monthly.max() / total_pnl * 100.0) if total_pnl > 0 else math.nan
    )
    return {
        "variant_id": definition.variant_id,
        "stage_id": definition.stage_id,
        "scenario": scenario,
        "bootstrap_replicates": BOOTSTRAP_REPLICATES,
        "bootstrap_unit": "SESSION",
        "pf_p025": (
            float(np.quantile(finite_pf, 0.025)) if len(finite_pf) else math.nan
        ),
        "pf_median": (
            float(np.quantile(finite_pf, 0.50)) if len(finite_pf) else math.nan
        ),
        "mean_daily_pnl_rs_p025": float(np.quantile(daily_pnl, 0.025)),
        "mean_daily_pnl_rs_median": float(np.quantile(daily_pnl, 0.50)),
        "net_points_p025": float(np.quantile(net_points, 0.025)),
        "net_points_median": float(np.quantile(net_points, 0.50)),
        "best_10_positive_days_share_pct": best10_share,
        "best_month_share_pct": best_month_share,
        "research_only": True,
        "promotion_eligible": False,
    }


def _scenario_output(
    run_dir: Path,
    prepared: PreparedExperiment,
    audit: pd.DataFrame,
    daily: pd.DataFrame,
    summary: Mapping[str, Any],
    scenario: str,
) -> dict[str, str]:
    scenario_dir = (
        run_dir
        / "stages"
        / prepared.definition.stage_id
        / prepared.definition.variant_id
        / "scenarios"
        / scenario.lower()
    )
    scenario_dir.mkdir(parents=True, exist_ok=True)
    audit_path = scenario_dir / "candidate_order_audit.csv"
    trades_path = scenario_dir / "closed_trades.csv"
    daily_path = scenario_dir / "daywise.csv"
    summary_path = scenario_dir / "summary.json"
    common.atomic_write_csv(audit, audit_path)
    common.atomic_write_csv(audit.loc[gaps._closed_mask(audit)], trades_path)
    common.atomic_write_csv(daily, daily_path)
    common.atomic_write_json(summary_path, gaps._json_ready(dict(summary)))
    return {
        "audit": str(audit_path.resolve()),
        "closed_trades": str(trades_path.resolve()),
        "daywise": str(daily_path.resolve()),
        "summary": str(summary_path.resolve()),
    }


def _run_experiment(
    definition: ExperimentDefinition,
    baseline_selected: pd.DataFrame,
    minute_paths: pd.DataFrame,
    sessions: Sequence[date],
    segments: Sequence[Mapping[str, Any]],
    run_dir: Path,
    *,
    scenarios: Sequence[tuple[str, float, float]],
) -> tuple[list[dict[str, Any]], list[pd.DataFrame], list[dict[str, Any]], dict[str, Any]]:
    prepared = _prepare_experiment(definition, baseline_selected)
    variant_dir = run_dir / "stages" / definition.stage_id / definition.variant_id
    variant_dir.mkdir(parents=True, exist_ok=True)
    common.atomic_write_csv(prepared.candidates, variant_dir / "selected_candidates.csv")
    common.atomic_write_csv(prepared.decisions, variant_dir / "selection_decisions.csv")
    common.atomic_write_json(
        variant_dir / "resolved_experiment.json",
        gaps._json_ready(
            {
                "schema_version": SCHEMA_VERSION,
                "experiment": definition.payload(),
                "setup_patch": dict(prepared.setup_patch),
                "setups": [asdict(setup) for setup in prepared.setups],
                "selected_candidate_count": len(prepared.candidates),
            }
        ),
    )

    metric_rows: list[dict[str, Any]] = []
    daily_parts: list[pd.DataFrame] = []
    robustness_rows: list[dict[str, Any]] = []
    artifacts: dict[str, Any] = {}
    parity: dict[str, Any] = {}
    gap_spec = _gap_spec(definition.gap_variant)
    for scenario, cost_bps, slippage_bps in scenarios:
        print(
            f"[FNO-V11] {definition.stage_id} {definition.variant_id} "
            f"scenario={scenario} candidates={len(prepared.candidates)}",
            flush=True,
        )
        policy = experiment._entry_policy_for_variant(
            locked_config.ACTIVE_VARIANT,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
            square_off="15:30",
            eod_policy="LAST_REAL_BAR_SENSITIVITY",
        )
        hook_context = (
            nullcontext()
            if definition.is_control
            else execution_runtime.installed_runtime_hooks(definition.runtime_spec)
        )
        with hook_context:
            with gaps.installed_gap_guard(gap_spec):
                audit = experiment._NEUTRAL_RUN_BACKTEST(
                    prepared.candidates,
                    minute_paths,
                    variant=definition.variant_id,
                    policy=policy,
                    target_exposure_per_entry_rs=50_000.0,
                )
        audit = audit.copy()
        audit["v11_variant_id"] = definition.variant_id
        audit["v11_stage_id"] = definition.stage_id
        audit["v11_family"] = definition.family
        audit["v11_scenario"] = scenario
        audit["research_only"] = True
        audit["promotion_eligible"] = False
        rows, daily = _metric_rows(
            audit,
            sessions,
            segments,
            definition=definition,
            scenario=scenario,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
        )
        metric_rows.extend(rows)
        daily_parts.append(daily)
        robustness_rows.append(
            _bootstrap_row(
                audit,
                sessions,
                definition=definition,
                scenario=scenario,
            )
        )
        full_summary = next(row for row in rows if row["period"] == "FULL_USABLE")
        artifacts[scenario] = _scenario_output(
            run_dir, prepared, audit, daily, full_summary, scenario
        )
        if definition.is_control:
            reference_path = (
                FROZEN_V10_REFERENCE_RUN
                / "scenarios"
                / scenario.lower()
                / "candidate_order_audit.csv"
            )
            if reference_path.is_file():
                parity[scenario] = gaps.validate_control_parity(audit, reference_path)
            if scenario == "REFERENCE_15_0":
                parity["benchmark"] = v10_backtest.validate_current_mixed_benchmark(
                    full_summary
                )
    common.atomic_write_json(variant_dir / "control_parity.json", parity)
    return metric_rows, daily_parts, robustness_rows, {
        "definition": definition.payload(),
        "artifacts": artifacts,
        "control_parity": parity,
    }


def _safe_ratio(value: Any, baseline: Any) -> float:
    try:
        numerator = float(value)
        denominator = float(baseline)
    except (TypeError, ValueError):
        return math.nan
    if not math.isfinite(numerator) or not math.isfinite(denominator) or denominator == 0:
        return math.nan
    return numerator / denominator


def _development_gates(metrics: pd.DataFrame) -> pd.DataFrame:
    full = metrics.loc[metrics["period"].eq("FULL_USABLE")].copy()
    forward = metrics.loc[metrics["period"].eq("FORWARD_EXTENSION")].copy()
    control_full = full.loc[full["variant_id"].eq(CONTROL.variant_id)].set_index(
        "scenario"
    )
    control_forward = forward.loc[
        forward["variant_id"].eq(CONTROL.variant_id)
    ].set_index("scenario")
    rows: list[dict[str, Any]] = []
    for variant_id, variant_rows in full.groupby("variant_id", sort=False):
        definition_row = variant_rows.iloc[0]
        indexed = variant_rows.set_index("scenario")
        forward_indexed = forward.loc[forward["variant_id"].eq(variant_id)].set_index(
            "scenario"
        )
        checks: dict[str, bool] = {}
        net_ratios: list[float] = []
        for scenario in ("REFERENCE_15_0", "STRESS_20_2", "STRESS_25_5"):
            observed = indexed.loc[scenario]
            baseline = control_full.loc[scenario]
            checks[f"net_at_least_control_{scenario}"] = float(
                observed["net_return_points"]
            ) >= float(baseline["net_return_points"]) - 1e-12
            checks[f"pf_at_least_control_{scenario}"] = float(
                observed["profit_factor"]
            ) >= float(baseline["profit_factor"]) - 1e-12
            net_ratios.append(
                _safe_ratio(observed["net_return_points"], baseline["net_return_points"])
            )
            checks[f"forward_nonnegative_{scenario}"] = float(
                forward_indexed.loc[scenario, "net_return_points"]
            ) >= -1e-12
        observed_mdd = float(indexed.loc["REFERENCE_15_0", "max_daily_drawdown_points"])
        baseline_mdd = float(
            control_full.loc["REFERENCE_15_0", "max_daily_drawdown_points"]
        )
        checks["mdd_within_105pct_control"] = observed_mdd <= baseline_mdd * 1.05
        passed = bool(all(checks.values()))
        failures = [name for name, value in checks.items() if not value]
        rows.append(
            {
                "variant_id": variant_id,
                "stage_id": definition_row["stage_id"],
                "family": definition_row["family"],
                "is_control": variant_id == CONTROL.variant_id,
                "development_gate_passed": passed,
                "failed_check_count": len(failures),
                "failed_checks": ";".join(failures),
                "worst_case_net_ratio_vs_control": float(np.nanmin(net_ratios)),
                "reference_mdd_ratio_vs_control": observed_mdd / baseline_mdd,
                **checks,
                "promotion_gate_passed": False,
                "promotion_blocker": "NO_UNTOUCHED_PROSPECTIVE_SAMPLE_AND_EXECUTION_DATA_INVALID",
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["development_gate_passed", "worst_case_net_ratio_vs_control", "reference_mdd_ratio_vs_control"],
        ascending=[False, False, True],
        kind="stable",
    ).reset_index(drop=True)


def _compatible_for_combination(
    first: ExperimentDefinition, second: ExperimentDefinition
) -> bool:
    if first.family == second.family:
        return False
    if first.registry_variant_id and second.registry_variant_id:
        return False
    if not first.runtime_spec.is_neutral and not second.runtime_spec.is_neutral:
        return False
    if first.gap_variant != BASE_GAP_VARIANT and second.gap_variant != BASE_GAP_VARIANT:
        return False
    if first.disabled_setup_id and second.disabled_setup_id:
        return False
    return True


def _combined_definition(
    gates_frame: pd.DataFrame,
    definitions: Mapping[str, ExperimentDefinition],
) -> ExperimentDefinition | None:
    eligible = gates_frame.loc[
        gates_frame["development_gate_passed"]
        & ~gates_frame["is_control"]
    ]
    ids = eligible["variant_id"].astype(str).tolist()
    for index, first_id in enumerate(ids):
        for second_id in ids[index + 1 :]:
            first = definitions[first_id]
            second = definitions[second_id]
            if not _compatible_for_combination(first, second):
                continue
            registry_variant = first.registry_variant_id or second.registry_variant_id
            runtime_spec = (
                first.runtime_spec
                if not first.runtime_spec.is_neutral
                else second.runtime_spec
            )
            gap_variant = (
                first.gap_variant
                if first.gap_variant != BASE_GAP_VARIANT
                else second.gap_variant
            )
            disabled = first.disabled_setup_id or second.disabled_setup_id
            short_hash = hashlib.sha256(
                f"{first_id}|{second_id}".encode()
            ).hexdigest()[:10].upper()
            return ExperimentDefinition(
                variant_id=f"V11_S10_POST_HOC_TOP2_{short_hash}",
                stage_id="STAGE_10_POST_HOC_COMBINATION",
                family="POST_HOC_COMBINATION",
                description=f"Post-hoc combination of {first_id} and {second_id}",
                registry_variant_id=registry_variant,
                runtime_spec=runtime_spec,
                gap_variant=gap_variant,
                disabled_setup_id=disabled,
                component_variant_ids=(first_id, second_id),
                post_hoc=True,
            )
    return None


def _stage_status_frame(
    executed: Sequence[ExperimentDefinition],
    *,
    stage10_status: Mapping[str, str],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = [dict(record) for record in BLOCKED_TESTS]
    counts = pd.Series([item.stage_id for item in executed]).value_counts()
    for stage_id, count in counts.items():
        rows.append(
            {
                "stage_id": stage_id,
                "test_id": "ALL_PREDECLARED_EXECUTABLE_VARIANTS",
                "status": "EXECUTED_RESEARCH_ONLY",
                "reason": f"{int(count)} isolated variant(s) completed under all selected cost scenarios.",
            }
        )
    rows.append(
        {
            "stage_id": "STAGE_03_REBASELINE",
            "test_id": "V10_STAGE0_EXACT_PARITY",
            "status": "VERIFIED_BY_STAGE0",
            "reason": "The Stage0 replay is benchmark- and row-parity checked against the frozen V10 run.",
        }
    )
    rows.append(dict(stage10_status))
    return pd.DataFrame(rows).sort_values(
        ["stage_id", "test_id"], kind="stable"
    ).reset_index(drop=True)


def _fmt(value: Any, decimals: int = 4) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if not math.isfinite(number):
        return "n/a"
    return f"{number:.{decimals}f}"


def _report(
    metrics: pd.DataFrame,
    gates_frame: pd.DataFrame,
    stage_status: pd.DataFrame,
    robustness: pd.DataFrame,
    *,
    sessions: Sequence[date],
    missing_sessions: Sequence[date],
) -> str:
    reference = metrics.loc[
        metrics["period"].eq("FULL_USABLE")
        & metrics["scenario"].eq("REFERENCE_15_0")
    ].copy()
    harsh = metrics.loc[
        metrics["period"].eq("FULL_USABLE")
        & metrics["scenario"].eq("STRESS_25_5")
    ][["variant_id", "profit_factor", "net_return_points"]].rename(
        columns={
            "profit_factor": "harsh_pf",
            "net_return_points": "harsh_net_points",
        }
    )
    forward = metrics.loc[
        metrics["period"].eq("FORWARD_EXTENSION")
        & metrics["scenario"].eq("STRESS_25_5")
    ][["variant_id", "net_return_points"]].rename(
        columns={"net_return_points": "forward_harsh_net"}
    )
    table = (
        reference.merge(harsh, on="variant_id", how="left")
        .merge(forward, on="variant_id", how="left")
        .merge(
            gates_frame[
                [
                    "variant_id",
                    "development_gate_passed",
                    "worst_case_net_ratio_vs_control",
                ]
            ],
            on="variant_id",
            how="left",
        )
        .sort_values(
            ["development_gate_passed", "worst_case_net_ratio_vs_control"],
            ascending=[False, False],
            kind="stable",
        )
    )
    eligible = gates_frame.loc[
        gates_frame["development_gate_passed"] & ~gates_frame["is_control"]
    ]
    best_id = (
        str(eligible.iloc[0]["variant_id"])
        if not eligible.empty
        else CONTROL.variant_id
    )
    observed_challengers = gates_frame.loc[~gates_frame["is_control"]]
    best_observed = (
        str(observed_challengers.iloc[0]["variant_id"])
        if not observed_challengers.empty
        else "none"
    )
    lines = [
        "# FNO V11 staged full-history research comparison",
        "",
        f"Usable sessions: **{len(sessions)}** ({min(sessions)} through {max(sessions)}).",
        f"Missing regular sessions inside the span: **{', '.join(map(str, missing_sessions)) or 'none'}**.",
        "",
        f"**Best under the predeclared development gates: `{best_id}`.**",
        f"Best observed challenger by worst cost-scenario net ratio: `{best_observed}`.",
        "",
        "No result in this report is promotion-eligible: the execution remains cash-bar based, most of the historical universe is not point-in-time, 26-Aug is absent from the comparable cache, and an untouched prospective sample does not exist.",
        "",
        "## Full-period comparison",
        "",
        "| Stage | Variant | Fills | WR | PF | Net pts | Net P&L Rs | MDD | Harsh PF | Harsh net | Forward harsh net | Gate |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in table.to_dict("records"):
        lines.append(
            "| {stage} | `{variant}` | {fills} | {wr}% | {pf} | {net} | {pnl} | {mdd} | {hpf} | {hnet} | {fwd} | {gate} |".format(
                stage=row["stage_id"],
                variant=row["variant_id"],
                fills=int(row["fills"]),
                wr=_fmt(row["win_rate_pct"], 2),
                pf=_fmt(row["profit_factor"]),
                net=_fmt(row["net_return_points"]),
                pnl=_fmt(row["net_pnl_rs"], 2),
                mdd=_fmt(row["max_daily_drawdown_points"]),
                hpf=_fmt(row["harsh_pf"]),
                hnet=_fmt(row["harsh_net_points"]),
                fwd=_fmt(row["forward_harsh_net"]),
                gate="PASS" if bool(row["development_gate_passed"]) else "FAIL",
            )
        )
    lines.extend(
        [
            "",
            "## Stage execution and validity gates",
            "",
            "| Stage | Test | Status | Reason |",
            "|---|---|---|---|",
        ]
    )
    for row in stage_status.to_dict("records"):
        lines.append(
            f"| {row['stage_id']} | {row['test_id']} | {row['status']} | {row['reason']} |"
        )
    control_bootstrap = robustness.loc[
        robustness["variant_id"].eq(CONTROL.variant_id)
        & robustness["scenario"].eq("REFERENCE_15_0")
    ]
    if not control_bootstrap.empty:
        row = control_bootstrap.iloc[0]
        lines.extend(
            [
                "",
                "## Statistical context",
                "",
                f"Stage0 session-bootstrap PF 2.5th percentile: **{_fmt(row['pf_p025'])}**; mean daily P&L 2.5th percentile: **Rs {_fmt(row['mean_daily_pnl_rs_p025'], 2)}**.",
                f"Best month share of Stage0 net P&L: **{_fmt(row['best_month_share_pct'], 2)}%**; best ten positive days share: **{_fmt(row['best_10_positive_days_share_pct'], 2)}%**.",
                "",
                "Bootstrap intervals are diagnostics on the same development sample, not an out-of-sample guarantee.",
            ]
        )
    lines.extend(
        [
            "",
            "## Decision rule",
            "",
            "A challenger passes only if full-period net points and PF are at least Stage0 in all three cost scenarios, reference drawdown is no worse than 105% of Stage0, and every forward-extension cost case remains non-negative. Improving win rate alone is never sufficient.",
            "",
            "Stage10 is post-hoc and is run only when at least two compatible isolated challengers pass. Even a passing Stage10 combination remains development-only.",
        ]
    )
    return "\n".join(lines) + "\n"


def _validate_catalog() -> None:
    registry.validate_registry()
    identifiers = [definition.variant_id for definition in ALL_PREDECLARED_EXPERIMENTS]
    if len(identifiers) != len(set(identifiers)):
        raise AssertionError("FNO V11 experiment IDs must be unique")
    for definition in ALL_PREDECLARED_EXPERIMENTS:
        definition.runtime_spec.validate()
        _gap_spec(definition.gap_variant)
        if definition.registry_variant_id is not None:
            registry.get_spec(definition.registry_variant_id)
        if definition.disabled_setup_id is not None and (
            definition.disabled_setup_id not in registry.VALID_SETUP_IDS
        ):
            raise AssertionError(f"unknown disabled setup: {definition.disabled_setup_id}")


def run_all(argv: Sequence[str]) -> Path:
    parser = argparse.ArgumentParser(
        prog="fno_v11_staged_backtest.py run-all",
        description="Run frozen Stage0 plus every executable isolated FNO V11 stage.",
    )
    parser.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    parser.add_argument("--reference-only", action="store_true")
    args = parser.parse_args(list(argv))

    _validate_catalog()
    (
        all_candidates,
        minute_paths,
        segment_records,
        sessions,
        expected_span,
        missing_sessions,
    ) = v10_backtest._load_all_usable_max050_gap2_history()
    selection_spec = filters.SPEC_BY_NAME[
        v10_backtest.MAX050_GAP2_SELECTION_VARIANT
    ]
    baseline_selected, baseline_decisions = filters.selection_overlay(
        all_candidates, selection_spec
    )
    if len(baseline_selected) != int(
        v10_backtest.MAX050_GAP2_CURRENT_MIXED_BENCHMARK["candidates"]
    ):
        raise AssertionError("V11 Stage0 selected-candidate count drifted")

    output_root = args.output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(gaps.IST).strftime("%Y%m%dT%H%M%S%f%z")
    run_dir = output_root / f"run_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=False)
    source_dir = run_dir / "source"
    source_dir.mkdir()
    sources = (
        Path(__file__).resolve(),
        Path(registry.__file__).resolve(),
        Path(selection_runtime.__file__).resolve(),
        Path(execution_runtime.__file__).resolve(),
        Path(v10_backtest.__file__).resolve(),
        Path(locked_config.__file__).resolve(),
        Path(filters.__file__).resolve(),
        Path(gaps.__file__).resolve(),
        Path(experiment.__file__).resolve(),
        Path(engine.__file__).resolve(),
    )
    for source in sources:
        shutil.copy2(source, source_dir / source.name)
    common.atomic_write_csv(all_candidates, run_dir / "all_input_candidates.csv")
    common.atomic_write_csv(
        baseline_selected, run_dir / "v10_stage0_selected_candidates.csv"
    )
    common.atomic_write_csv(
        baseline_decisions, run_dir / "v10_stage0_selection_decisions.csv"
    )
    common.atomic_write_json(
        run_dir / "source_segments.json",
        {"schema_version": SCHEMA_VERSION, "segments": segment_records},
    )

    scenarios = (
        gaps.COST_SCENARIOS[:1] if args.reference_only else gaps.COST_SCENARIOS
    )
    all_metrics: list[dict[str, Any]] = []
    all_daily: list[pd.DataFrame] = []
    all_robustness: list[dict[str, Any]] = []
    run_records: dict[str, Any] = {}
    executed: list[ExperimentDefinition] = []
    definition_by_id = {
        definition.variant_id: definition for definition in ALL_PREDECLARED_EXPERIMENTS
    }

    for definition in ALL_PREDECLARED_EXPERIMENTS:
        rows, daily, robustness_rows, record = _run_experiment(
            definition,
            baseline_selected,
            minute_paths,
            sessions,
            segment_records,
            run_dir,
            scenarios=scenarios,
        )
        all_metrics.extend(rows)
        all_daily.extend(daily)
        all_robustness.extend(robustness_rows)
        run_records[definition.variant_id] = record
        executed.append(definition)

    metrics = pd.DataFrame(all_metrics)
    gates_frame = _development_gates(metrics)
    stage10 = _combined_definition(gates_frame, definition_by_id)
    if stage10 is None:
        stage10_status = {
            "stage_id": "STAGE_10_POST_HOC_COMBINATION",
            "test_id": "TOP_TWO_COMPATIBLE_PASSERS",
            "status": "NOT_RUN_GATE_FAILED",
            "reason": "Fewer than two compatible isolated challengers passed every predeclared development gate.",
        }
    else:
        rows, daily, robustness_rows, record = _run_experiment(
            stage10,
            baseline_selected,
            minute_paths,
            sessions,
            segment_records,
            run_dir,
            scenarios=scenarios,
        )
        all_metrics.extend(rows)
        all_daily.extend(daily)
        all_robustness.extend(robustness_rows)
        run_records[stage10.variant_id] = record
        executed.append(stage10)
        metrics = pd.DataFrame(all_metrics)
        gates_frame = _development_gates(metrics)
        stage10_status = {
            "stage_id": "STAGE_10_POST_HOC_COMBINATION",
            "test_id": stage10.variant_id,
            "status": "EXECUTED_POST_HOC_RESEARCH_ONLY",
            "reason": "Two compatible isolated gate-passers were combined; this mined result cannot be promoted without new data.",
        }

    daywise = pd.concat(all_daily, ignore_index=True)
    robustness = pd.DataFrame(all_robustness)
    stage_status = _stage_status_frame(
        executed, stage10_status=stage10_status
    )
    metrics_path = run_dir / "all_period_metrics.csv"
    daily_path = run_dir / "all_daywise.csv"
    robustness_path = run_dir / "bootstrap_and_concentration.csv"
    gates_path = run_dir / "development_gates.csv"
    status_path = run_dir / "stage_status.csv"
    report_path = run_dir / "FNO_V11_ALL_STAGES_COMPARISON.md"
    common.atomic_write_csv(metrics, metrics_path)
    common.atomic_write_csv(daywise, daily_path)
    common.atomic_write_csv(robustness, robustness_path)
    common.atomic_write_csv(gates_frame, gates_path)
    common.atomic_write_csv(stage_status, status_path)
    common.atomic_write_text(
        report_path,
        _report(
            metrics,
            gates_frame,
            stage_status,
            robustness,
            sessions=sessions,
            missing_sessions=missing_sessions,
        ),
    )

    provenance_path = run_dir / "provenance.json"
    inventory_path = run_dir / "artifact_inventory.json"
    source_hashes = {
        source.name: {"path": str(source), "sha256": _sha256_file(source)}
        for source in sources
    }
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "created_at_ist": datetime.now(gaps.IST),
        "command": [
            "python",
            "-u",
            str(Path(__file__).resolve()),
            "run-all",
            *argv,
        ],
        "registry_sha256": registry.registry_sha256(),
        "v10_stage0_binding": asdict(registry.BASELINE_BINDING),
        "source_hashes": source_hashes,
        "usable_session_dates": [day.isoformat() for day in sessions],
        "usable_session_count": len(sessions),
        "calendar_span_session_count": len(expected_span),
        "missing_regular_session_dates": [day.isoformat() for day in missing_sessions],
        "source_segments": segment_records,
        "experiments": run_records,
        "blocked_tests": list(BLOCKED_TESTS),
        "outputs": {
            "metrics": str(metrics_path.resolve()),
            "daywise": str(daily_path.resolve()),
            "robustness": str(robustness_path.resolve()),
            "development_gates": str(gates_path.resolve()),
            "stage_status": str(status_path.resolve()),
            "report": str(report_path.resolve()),
        },
        "limitations": [
            "RESEARCH_ONLY_POST_HOC_DEVELOPMENT_SAMPLE",
            "POINT_IN_TIME_UNIVERSE_INCOMPLETE",
            "STATIC_26AUG_CONTRACT_USED_BACKWARD_IN_CORE",
            "CASH_EQUITY_1M_EXECUTION_NOT_FUTURES_EXECUTION",
            "LAST_REAL_BAR_SENSITIVITY_FOR_246_PATHS_ENDING_1515",
            "2026_08_26_NOT_IN_COMPARABLE_65_SESSION_CACHE",
            "NO_HISTORICAL_MARGIN_SPREAD_DEPTH_OR_PARTIAL_FILL_DATA",
            "NO_UNTOUCHED_PROSPECTIVE_VALIDATION_SAMPLE",
        ],
        "headline_valid": False,
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }
    common.atomic_write_json(provenance_path, gaps._json_ready(provenance))
    common.atomic_write_json(
        inventory_path,
        {
            "schema_version": SCHEMA_VERSION,
            "artifacts": gaps._inventory_files(
                run_dir, exclude={provenance_path, inventory_path}
            ),
        },
    )
    provenance["artifact_inventory"] = {
        "path": str(inventory_path.resolve()),
        "sha256": _sha256_file(inventory_path),
    }
    common.atomic_write_json(provenance_path, gaps._json_ready(provenance))
    common.atomic_write_json(
        output_root / "latest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_dir": str(run_dir.resolve()),
            "provenance_sha256": _sha256_file(provenance_path),
            "usable_session_count": len(sessions),
            "research_only": True,
        },
    )
    print(f"[FNO-V11] complete: {run_dir}", flush=True)
    return run_dir


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        raise SystemExit("command required: validate or run-all")
    command = args[0]
    if command == "validate":
        _validate_catalog()
        print(
            json.dumps(
                {
                    "schema_version": SCHEMA_VERSION,
                    "registry_sha256": registry.registry_sha256(),
                    "predeclared_experiments": len(ALL_PREDECLARED_EXPERIMENTS),
                    "blocked_tests": len(BLOCKED_TESTS),
                    "valid": True,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if command == "run-all":
        run_all(args[1:])
        return 0
    raise SystemExit(f"unknown command: {command!r}")


if __name__ == "__main__":
    raise SystemExit(main())

