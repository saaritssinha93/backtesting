"""Full-history staged FNO V12 research runner.

V11 Stage 10 is an immutable Stage-0 control.  Every V12 challenger is
rebuilt from the complete 1,241-candidate input frame, replayed through the
same frozen V11 execution and strong-identity Gap2 stack, and isolated in a
fresh spawned process.  The runner is deliberately research-only: missing
point-in-time futures, market-context, margin, and path data are recorded as
blocked tests rather than silently approximated.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import itertools
import json
import math
import multiprocessing
import os
import shutil
import warnings
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v10_backtest as v10_backtest
import fno_v10_backtest_config as locked_config
import fno_v10_experiment_backtest as experiment
import fno_v10_gap_guard_research as gaps
import fno_v11_backtest as v11_backtest
import fno_v11_execution_runtime as v11_execution
import fno_v11_gap_runtime as v11_gap
import fno_v12_analysis as v12_analysis
import fno_v12_execution_runtime as v12_execution
import fno_v12_resources as resources
import fno_v12_selection_runtime as selection
import fno_v12_variant_registry as registry
import fno_v8_windowed_1m_entry_backtest as engine


warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    module=r"fno_v8_windowed_1m_entry_backtest",
)


SCHEMA_VERSION = "fno_v12_all_stages_full_history_v1"
RESEARCH_DESIGN = "PREDECLARED_ISOLATED_THEN_GATED_POST_HOC_COMBINATION"
OUTPUT_ROOT = (
    common.FNO_ROOT / "strategy_research" / "v12_fno_staged_research_v1"
)
TARGET_EXPOSURE_PER_ENTRY_RS = 50_000.0
SQUARE_OFF = "15:30"
EOD_POLICY = "LAST_REAL_BAR_SENSITIVITY"
SCENARIOS = tuple(v11_backtest.EXPECTED_SCENARIOS)
BOOTSTRAP_REPLICATES = 2_000

SEALED_V11_RUN = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\v11_stage10_fixed_full_history_v1"
    r"\run_20260830T213455896360+0530"
)
SEALED_V11_PROVENANCE_SHA256 = (
    "85ce11a0a3d531b73cc753f3013ba5b68d72061cd722e979feb2d5f8bbcf8c33"
)
SEALED_V11_INVENTORY_SHA256 = (
    "833ce5d88b2df86819a2106eb438814a09cee2a3e5d9639d4dec73139068aee7"
)

SEALED_V10_RUN = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\v10_max050_gap2_full_history_v1"
    r"\run_20260830T155702622623+0530"
)
SEALED_V10_PROVENANCE_SHA256 = (
    "5416827802c1c3184d6d273d86e40ee1c126582142cbd33bec97eecdcd074334"
)
SEALED_V10_INVENTORY_SHA256 = (
    "da57d338ae3ea4a05b53b107b4be009084b053ae2e7379425e2261c7c5b90b9e"
)
V10_COMPARATOR_ID = "V10_MAX050_GAP2_FROZEN_COMPARATOR"

_WORKER_ALL_CANDIDATES: pd.DataFrame | None = None
_WORKER_MINUTE_PATHS: pd.DataFrame | None = None
_WORKER_SEGMENTS: list[dict[str, Any]] | None = None
_WORKER_SESSIONS: list[date] | None = None
_WORKER_RAW_BINDING: str | None = None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        gaps._json_ready(value),
        allow_nan=False,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _validate_inventory(run_dir: Path, inventory_path: Path) -> dict[str, Any]:
    payload = json.loads(inventory_path.read_text(encoding="utf-8"))
    records = list(payload.get("artifacts", []))
    if not records:
        raise AssertionError(f"empty artifact inventory: {inventory_path}")
    checked = 0
    for record in records:
        relative = Path(str(record["relative_path"]))
        if relative.is_absolute() or ".." in relative.parts:
            raise AssertionError(f"unsafe inventory path: {relative}")
        target = (run_dir / relative).resolve()
        if not target.is_file():
            raise FileNotFoundError(f"inventoried artifact missing: {target}")
        if int(record["bytes"]) != target.stat().st_size:
            raise AssertionError(f"inventoried size changed: {target}")
        if str(record["sha256"]) != _sha256_file(target):
            raise AssertionError(f"inventoried SHA-256 changed: {target}")
        checked += 1
    return {"validated": True, "artifact_count": checked}


def _validate_sealed_comparators() -> dict[str, Any]:
    v11_provenance = SEALED_V11_RUN / "provenance.json"
    v11_inventory = SEALED_V11_RUN / "artifact_inventory.json"
    if _sha256_file(v11_provenance) != SEALED_V11_PROVENANCE_SHA256:
        raise AssertionError("sealed V11 provenance SHA-256 changed")
    if _sha256_file(v11_inventory) != SEALED_V11_INVENTORY_SHA256:
        raise AssertionError("sealed V11 inventory SHA-256 changed")
    v11_validation = v11_backtest.validate_run_provenance(v11_provenance)

    v10_provenance = SEALED_V10_RUN / "provenance.json"
    v10_inventory = SEALED_V10_RUN / "artifact_inventory.json"
    if _sha256_file(v10_provenance) != SEALED_V10_PROVENANCE_SHA256:
        raise AssertionError("sealed V10 provenance SHA-256 changed")
    if _sha256_file(v10_inventory) != SEALED_V10_INVENTORY_SHA256:
        raise AssertionError("sealed V10 inventory SHA-256 changed")
    v10_payload = json.loads(v10_provenance.read_text(encoding="utf-8"))
    if v10_payload.get("complete") is not True:
        raise AssertionError("sealed V10 run is not complete")
    profile = dict(v10_payload.get("profile", {}))
    if profile.get("profile_id") != "V10_STAGE7_0935_LONG_MAX_050_GAP2":
        raise AssertionError("sealed V10 profile changed")
    observed_v10_scenarios = tuple(dict(v10_payload.get("scenarios", {})))
    if observed_v10_scenarios != tuple(name for name, _, _ in SCENARIOS):
        raise AssertionError("sealed V10 archive does not contain all scenarios")
    v10_validation = _validate_inventory(SEALED_V10_RUN, v10_inventory)
    return {
        "v11": {
            "run_dir": str(SEALED_V11_RUN),
            "provenance_sha256": SEALED_V11_PROVENANCE_SHA256,
            "inventory_sha256": SEALED_V11_INVENTORY_SHA256,
            "validation": v11_validation,
        },
        "v10": {
            "run_dir": str(SEALED_V10_RUN),
            "provenance_sha256": SEALED_V10_PROVENANCE_SHA256,
            "inventory_sha256": SEALED_V10_INVENTORY_SHA256,
            "validation": v10_validation,
        },
    }


def _raw_input_binding(
    all_candidates: pd.DataFrame,
    minute_paths: pd.DataFrame,
    sessions: Sequence[date],
    segments: Sequence[Mapping[str, Any]],
) -> str:
    return _canonical_sha256(
        {
            "sessions": [day.isoformat() for day in sessions],
            "segments": list(segments),
            "all_candidates_sha256": v11_backtest._frame_content_sha256(
                all_candidates
            ),
            "minute_paths_sha256": v11_backtest._frame_content_sha256(
                minute_paths
            ),
        }
    )


def _load_inputs() -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    list[dict[str, Any]],
    list[date],
    list[date],
    list[date],
    str,
]:
    (
        all_candidates,
        minute_paths,
        segments,
        sessions,
        expected_span,
        missing_sessions,
    ) = v10_backtest._load_all_usable_max050_gap2_history()
    binding = _raw_input_binding(
        all_candidates, minute_paths, sessions, segments
    )
    return (
        all_candidates,
        minute_paths,
        list(segments),
        list(sessions),
        list(expected_span),
        list(missing_sessions),
        binding,
    )


def _configure_base_engine() -> tuple[engine.V8Setup, ...]:
    experiment.configure_engine(locked_config.ACTIVE_VARIANT)
    engine._confirmation_check = experiment._NEUTRAL_CONFIRMATION_CHECK
    return tuple(engine.ACTIVE_SETUPS)


def _gap_spec(config: registry.ResolvedConfig) -> gaps.GapGuardSpec:
    threshold = float(config.gap.max_adverse_gap_bps)
    suffix = str(int(threshold)) if threshold.is_integer() else str(threshold)
    return gaps.GapGuardSpec(f"MAX_{suffix}_BPS", threshold)


def _period_metric_rows(
    audit: pd.DataFrame,
    sessions: Sequence[date],
    segments: Sequence[Mapping[str, Any]],
    *,
    config: registry.ResolvedConfig,
    scenario: str,
    cost_bps: float,
    slippage_bps: float,
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    audit_days = audit["session_date"].map(engine._parse_day)
    rows: list[dict[str, Any]] = []
    full_daily = pd.DataFrame()
    spec = _gap_spec(config)
    for period, days in v11_backtest._periods(sessions, segments):
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
                "variant_id": config.variant_id,
                "stage_id": config.stage_id,
                "family": config.family,
                "description": config.description,
                "resolved_config_sha256": registry.resolved_config_sha256(
                    config
                ),
                "post_hoc": config.post_hoc,
                "component_variant_ids": ";".join(
                    config.component_variant_ids
                ),
            }
        )
        rows.append(row)
        if period == "FULL_USABLE":
            full_daily = daily.copy()
            full_daily["variant_id"] = config.variant_id
            full_daily["stage_id"] = config.stage_id
            full_daily["family"] = config.family
            full_daily["post_hoc"] = config.post_hoc
    return rows, full_daily


def _profit_factor(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    profit = float(numeric.loc[numeric > 0].sum())
    loss = float(-numeric.loc[numeric < 0].sum())
    if loss == 0:
        return math.inf if profit > 0 else math.nan
    return profit / loss


def _side_setup_rows(
    audit: pd.DataFrame,
    *,
    config: registry.ResolvedConfig,
    scenario: str,
) -> list[dict[str, Any]]:
    closed = audit.loc[gaps._closed_mask(audit)].copy()
    groups: list[tuple[str, pd.DataFrame]] = [("ALL", closed)]
    groups.extend(
        (f"SIDE_{side}", closed.loc[closed["side"].astype(str).eq(side)])
        for side in ("LONG", "SHORT")
    )
    groups.extend(
        (
            f"SETUP_{setup_id}",
            closed.loc[closed["setup_id"].astype(str).eq(setup_id)],
        )
        for setup_id in sorted(registry.VALID_SETUP_IDS)
    )
    rows: list[dict[str, Any]] = []
    for group_id, frame in groups:
        returns = pd.to_numeric(frame.get("net_return_pct"), errors="coerce")
        pnl = pd.to_numeric(frame.get("net_pnl_rs"), errors="coerce")
        rows.append(
            {
                "variant_id": config.variant_id,
                "stage_id": config.stage_id,
                "scenario": scenario,
                "group_id": group_id,
                "fills": len(frame),
                "wins": int(returns.gt(0).sum()),
                "losses": int(returns.lt(0).sum()),
                "win_rate_pct": (
                    float(returns.gt(0).mean() * 100.0)
                    if len(frame)
                    else math.nan
                ),
                "profit_factor": _profit_factor(returns),
                "net_return_points": float(returns.sum()) if len(frame) else 0.0,
                "net_pnl_rs": float(pnl.sum()) if len(frame) else 0.0,
            }
        )
    return rows


def _bootstrap_and_concentration_row(
    audit: pd.DataFrame,
    sessions: Sequence[date],
    *,
    config: registry.ResolvedConfig,
    scenario: str,
) -> dict[str, Any]:
    ordered = tuple(sorted(set(sessions)))
    closed = audit.loc[gaps._closed_mask(audit)].copy()
    closed["_day"] = closed["session_date"].map(engine._parse_day)
    by_day = closed.groupby("_day", sort=False).agg(
        positive=("net_return_pct", lambda values: float(values[values > 0].sum())),
        loss=("net_return_pct", lambda values: float(-values[values < 0].sum())),
        points=("net_return_pct", "sum"),
        pnl=("net_pnl_rs", "sum"),
    )
    positive = np.asarray(
        [float(by_day.at[d, "positive"]) if d in by_day.index else 0.0 for d in ordered]
    )
    loss = np.asarray(
        [float(by_day.at[d, "loss"]) if d in by_day.index else 0.0 for d in ordered]
    )
    points = np.asarray(
        [float(by_day.at[d, "points"]) if d in by_day.index else 0.0 for d in ordered]
    )
    pnl = np.asarray(
        [float(by_day.at[d, "pnl"]) if d in by_day.index else 0.0 for d in ordered]
    )
    seed = int.from_bytes(
        hashlib.sha256(
            f"{SCHEMA_VERSION}|{config.variant_id}|{scenario}".encode()
        ).digest()[:8],
        "big",
    )
    rng = np.random.default_rng(seed)
    sampled = rng.integers(0, len(ordered), size=(BOOTSTRAP_REPLICATES, len(ordered)))
    sampled_positive = positive[sampled].sum(axis=1)
    sampled_loss = loss[sampled].sum(axis=1)
    sampled_pf = np.divide(
        sampled_positive,
        sampled_loss,
        out=np.full_like(sampled_positive, np.inf),
        where=sampled_loss > 0,
    )
    finite_pf = sampled_pf[np.isfinite(sampled_pf)]
    daily_pnl = pd.Series(pnl, index=ordered)
    positive_days = daily_pnl.loc[daily_pnl > 0].sort_values(ascending=False)
    total_pnl = float(daily_pnl.sum())
    months = daily_pnl.groupby([day.strftime("%Y-%m") for day in ordered]).sum()
    return {
        "variant_id": config.variant_id,
        "stage_id": config.stage_id,
        "scenario": scenario,
        "bootstrap_replicates": BOOTSTRAP_REPLICATES,
        "pf_p025": (
            float(np.quantile(finite_pf, 0.025)) if len(finite_pf) else math.nan
        ),
        "pf_median": (
            float(np.quantile(finite_pf, 0.5)) if len(finite_pf) else math.nan
        ),
        "net_points_p025": float(np.quantile(points[sampled].sum(axis=1), 0.025)),
        "net_points_median": float(np.quantile(points[sampled].sum(axis=1), 0.5)),
        "best_5_positive_days_share_pct": (
            float(positive_days.head(5).sum() / total_pnl * 100.0)
            if total_pnl > 0
            else math.nan
        ),
        "best_month_share_pct": (
            float(months.max() / total_pnl * 100.0)
            if total_pnl > 0
            else math.nan
        ),
    }


def _normalize_for_compare(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    result = frame.reindex(columns=["candidate_id", *columns]).copy()
    result["candidate_id"] = result["candidate_id"].astype(str)
    return result.set_index("candidate_id", drop=True)


def _affected_decisions(control: pd.DataFrame, challenger: pd.DataFrame) -> dict[str, Any]:
    columns = [
        column
        for column in (
            "status",
            "reason",
            "filled",
            "confirmation_minute",
            "confirmation_time",
            "entry_time",
            "entry_price",
            "exit_time",
            "exit_price",
            "exit_reason",
            "net_return_pct",
            "net_pnl_rs",
        )
        if column in control.columns or column in challenger.columns
    ]
    left = _normalize_for_compare(control, columns)
    right = _normalize_for_compare(challenger, columns)
    ids = left.index.union(right.index)
    left = left.reindex(ids)
    right = right.reindex(ids)
    changed = pd.Series(False, index=ids)
    numeric = {
        "confirmation_minute",
        "entry_price",
        "exit_price",
        "net_return_pct",
        "net_pnl_rs",
    }
    for column in columns:
        if column in numeric:
            lval = pd.to_numeric(left[column], errors="coerce").to_numpy(float)
            rval = pd.to_numeric(right[column], errors="coerce").to_numpy(float)
            equal = np.isclose(lval, rval, rtol=0.0, atol=1e-9, equal_nan=True)
        else:
            lval = left[column].fillna("<NA>").astype(str).to_numpy()
            rval = right[column].fillna("<NA>").astype(str).to_numpy()
            equal = lval == rval
        changed |= ~equal
    return {
        "control_rows": len(control),
        "challenger_rows": len(challenger),
        "union_rows": len(ids),
        "affected_decisions": int(changed.sum()),
        "affected_candidate_ids_sha256": hashlib.sha256(
            "\n".join(sorted(ids[changed].astype(str))).encode("utf-8")
        ).hexdigest(),
    }


def _scenario_dir(run_dir: Path, config: registry.ResolvedConfig, scenario: str) -> Path:
    return (
        run_dir
        / "stages"
        / config.stage_id
        / config.variant_id
        / "scenarios"
        / scenario.lower()
    )


def _write_scenario(
    run_dir: Path,
    config: registry.ResolvedConfig,
    scenario: str,
    audit: pd.DataFrame,
    daily: pd.DataFrame,
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    scenario_dir = _scenario_dir(run_dir, config, scenario)
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
        "closed_trade_economic_fingerprint_sha256": (
            v11_backtest._closed_trade_economic_fingerprint(
                pd.read_csv(trades_path)
            )
        ),
    }


def _run_variant(
    config: registry.ResolvedConfig,
    all_candidates: pd.DataFrame,
    minute_paths: pd.DataFrame,
    sessions: Sequence[date],
    segments: Sequence[Mapping[str, Any]],
    run_dir: Path,
) -> dict[str, Any]:
    base_setups = _configure_base_engine()
    prepared = selection.prepare_variant_selection(
        all_candidates, base_setups, config
    )
    engine.ACTIVE_SETUPS = tuple(prepared.setups)
    runtime_spec = v12_execution.runtime_spec_from_rule(
        config.runtime,
        equal_rank_picker_scores=(
            prepared.selection_metadata.equal_rank_picker_scores
        ),
    )
    variant_dir = run_dir / "stages" / config.stage_id / config.variant_id
    variant_dir.mkdir(parents=True, exist_ok=True)
    common.atomic_write_csv(
        prepared.candidates, variant_dir / "selected_candidates.csv"
    )
    common.atomic_write_csv(
        prepared.decisions, variant_dir / "selection_decisions.csv"
    )
    common.atomic_write_json(
        variant_dir / "resolved_experiment.json",
        gaps._json_ready(
            {
                "schema_version": SCHEMA_VERSION,
                "registry_sha256": registry.registry_sha256(),
                "resolved_config_sha256": registry.resolved_config_sha256(config),
                "resolved_config": config.payload(),
                "selection_metadata": prepared.selection_metadata.payload(),
                "setup_patch_metadata": asdict(prepared.setup_patch_metadata),
                "runtime_spec": runtime_spec.payload(),
                "setups": [asdict(setup) for setup in prepared.setups],
                "research_only": True,
                "live_or_paper_authority": False,
            }
        ),
    )

    metric_rows: list[dict[str, Any]] = []
    daywise_parts: list[pd.DataFrame] = []
    side_rows: list[dict[str, Any]] = []
    robustness_rows: list[dict[str, Any]] = []
    artifacts: dict[str, Any] = {}
    affected: dict[str, Any] = {}
    parity: dict[str, Any] = {}
    gap_spec = _gap_spec(config)
    for scenario, cost_bps, slippage_bps in SCENARIOS:
        policy = experiment._entry_policy_for_variant(
            locked_config.ACTIVE_VARIANT,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
            square_off=SQUARE_OFF,
            eod_policy=EOD_POLICY,
        )
        with v11_execution.installed_runtime_hooks(
            v11_backtest.FIXED_RUNTIME_SPEC, allow_composite=True
        ):
            with v12_execution.installed_runtime_hooks(runtime_spec):
                with v11_gap.installed_gap_guard(gap_spec):
                    audit = experiment._NEUTRAL_RUN_BACKTEST(
                        prepared.candidates,
                        minute_paths,
                        variant=config.variant_id,
                        policy=policy,
                        target_exposure_per_entry_rs=(
                            TARGET_EXPOSURE_PER_ENTRY_RS
                        ),
                    )
        audit = audit.copy()
        audit["v12_variant_id"] = config.variant_id
        audit["v12_stage_id"] = config.stage_id
        audit["v12_scenario"] = scenario
        audit["research_only"] = True
        audit["promotion_eligible"] = False
        rows, daily = _period_metric_rows(
            audit,
            sessions,
            segments,
            config=config,
            scenario=scenario,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
        )
        metric_rows.extend(rows)
        daywise_parts.append(daily)
        side_rows.extend(
            _side_setup_rows(audit, config=config, scenario=scenario)
        )
        robustness_rows.append(
            _bootstrap_and_concentration_row(
                audit, sessions, config=config, scenario=scenario
            )
        )
        full_summary = next(row for row in rows if row["period"] == "FULL_USABLE")
        artifacts[scenario] = _write_scenario(
            run_dir, config, scenario, audit, daily, full_summary
        )
        control_audit_path = _scenario_dir(
            run_dir,
            registry.resolve_variant(registry.CONTROL_VARIANT_ID),
            scenario,
        ) / "candidate_order_audit.csv"
        if config.variant_id == registry.CONTROL_VARIANT_ID:
            benchmark = v11_backtest.validate_full_usable_benchmark(
                full_summary, scenario
            )
            fingerprint = artifacts[scenario][
                "closed_trade_economic_fingerprint_sha256"
            ]
            expected_fingerprint = v11_backtest.EXPECTED_CLOSED_TRADE_FINGERPRINTS[
                scenario
            ]
            if fingerprint != expected_fingerprint:
                raise AssertionError(
                    f"V12 Stage0 {scenario} fingerprint drifted: "
                    f"expected={expected_fingerprint} observed={fingerprint}"
                )
            sealed_audit = (
                SEALED_V11_RUN
                / "scenarios"
                / scenario.lower()
                / "candidate_order_audit.csv"
            )
            parity[scenario] = {
                "benchmark": benchmark,
                "sealed_audit": gaps.validate_control_parity(audit, sealed_audit),
            }
            affected[scenario] = {
                "control_rows": len(audit),
                "challenger_rows": len(audit),
                "union_rows": len(audit),
                "affected_decisions": 0,
            }
        else:
            if not control_audit_path.is_file():
                raise FileNotFoundError(
                    f"V12 Stage0 audit missing before challenger: {control_audit_path}"
                )
            # Compare the two persisted audit artifacts, not a CSV-loaded
            # control against an in-memory challenger.  Pandas timestamp
            # stringification differs between those representations and can
            # otherwise inflate a narrow filter into hundreds of false
            # "affected" decisions.
            affected[scenario] = _affected_decisions(
                pd.read_csv(control_audit_path),
                pd.read_csv(Path(artifacts[scenario]["audit"])),
            )

    common.atomic_write_json(
        variant_dir / "run_record.json",
        gaps._json_ready(
            {
                "schema_version": SCHEMA_VERSION,
                "variant_id": config.variant_id,
                "artifacts": artifacts,
                "affected_decisions": affected,
                "control_parity": parity,
                "research_only": True,
            }
        ),
    )
    return {
        "variant_id": config.variant_id,
        "config": config.payload(),
        "metric_rows": metric_rows,
        "daywise": pd.concat(daywise_parts, ignore_index=True),
        "side_setup_rows": side_rows,
        "robustness_rows": robustness_rows,
        "artifacts": artifacts,
        "affected_decisions": affected,
        "control_parity": parity,
        "selected_candidate_count": len(prepared.candidates),
    }


def _worker_initialize(expected_raw_binding: str) -> None:
    global _WORKER_ALL_CANDIDATES
    global _WORKER_MINUTE_PATHS
    global _WORKER_SEGMENTS
    global _WORKER_SESSIONS
    global _WORKER_RAW_BINDING
    resources.apply_single_thread_environment()
    (
        all_candidates,
        minute_paths,
        segments,
        sessions,
        _,
        _,
        binding,
    ) = _load_inputs()
    if binding != expected_raw_binding:
        raise AssertionError("V12 worker raw input binding differs from parent")
    _WORKER_ALL_CANDIDATES = all_candidates
    _WORKER_MINUTE_PATHS = minute_paths
    _WORKER_SEGMENTS = segments
    _WORKER_SESSIONS = sessions
    _WORKER_RAW_BINDING = binding


def _worker_run(task: tuple[registry.ResolvedConfig, str, str]) -> dict[str, Any]:
    config, run_dir_text, expected_raw_binding = task
    if any(
        value is None
        for value in (
            _WORKER_ALL_CANDIDATES,
            _WORKER_MINUTE_PATHS,
            _WORKER_SEGMENTS,
            _WORKER_SESSIONS,
            _WORKER_RAW_BINDING,
        )
    ):
        raise AssertionError("V12 worker dataset was not initialized")
    if _WORKER_RAW_BINDING != expected_raw_binding:
        raise AssertionError("V12 worker binding changed")
    print(
        f"[FNO-V12] START {config.stage_id} {config.variant_id}", flush=True
    )
    result = _run_variant(
        config,
        _WORKER_ALL_CANDIDATES,
        _WORKER_MINUTE_PATHS,
        _WORKER_SESSIONS,
        _WORKER_SEGMENTS,
        Path(run_dir_text),
    )
    reference = next(
        row
        for row in result["metric_rows"]
        if row["period"] == "FULL_USABLE"
        and row["scenario"] == "REFERENCE_15_0"
    )
    print(
        f"[FNO-V12] DONE {config.variant_id} fills={int(reference['fills'])} "
        f"PF={float(reference['profit_factor']):.4f} "
        f"net={float(reference['net_return_points']):+.4f}",
        flush=True,
    )
    return result


def _pool_kwargs(workers: int, raw_binding: str) -> dict[str, Any]:
    if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
        raise ValueError("workers must be a positive integer")
    return {
        "max_workers": workers,
        "mp_context": multiprocessing.get_context("spawn"),
        "initializer": _worker_initialize,
        "initargs": (raw_binding,),
        "max_tasks_per_child": 1,
    }


def _execute_parallel(
    configs: Sequence[registry.ResolvedConfig],
    *,
    run_dir: Path,
    raw_binding: str,
    workers: int,
) -> list[dict[str, Any]]:
    if not configs:
        return []
    results: list[dict[str, Any]] = []
    tasks = [(config, str(run_dir), raw_binding) for config in configs]
    with concurrent.futures.ProcessPoolExecutor(
        **_pool_kwargs(min(workers, len(tasks)), raw_binding)
    ) as pool:
        future_to_id = {
            pool.submit(_worker_run, task): task[0].variant_id for task in tasks
        }
        completed = 0
        for future in concurrent.futures.as_completed(future_to_id):
            variant_id = future_to_id[future]
            result = future.result()
            if result["variant_id"] != variant_id:
                raise AssertionError("V12 worker returned the wrong variant")
            results.append(result)
            completed += 1
            print(
                f"[FNO-V12] progress {completed}/{len(tasks)}: {variant_id}",
                flush=True,
            )
    order = {config.variant_id: index for index, config in enumerate(configs)}
    return sorted(results, key=lambda item: order[item["variant_id"]])


def _collect_result_frames(
    results: Sequence[Mapping[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    metrics = pd.DataFrame(
        [row for result in results for row in result["metric_rows"]]
    )
    daywise_parts = [result["daywise"] for result in results]
    daywise = (
        pd.concat(daywise_parts, ignore_index=True)
        if daywise_parts
        else pd.DataFrame()
    )
    side_setup = pd.DataFrame(
        [row for result in results for row in result["side_setup_rows"]]
    )
    robustness = pd.DataFrame(
        [row for result in results for row in result["robustness_rows"]]
    )
    return metrics, daywise, side_setup, robustness


def _import_v10_comparator() -> tuple[pd.DataFrame, pd.DataFrame]:
    metrics = pd.read_csv(SEALED_V10_RUN / "all_period_metrics.csv")
    daywise = pd.read_csv(SEALED_V10_RUN / "all_daywise.csv")
    for frame in (metrics, daywise):
        frame["variant_id"] = V10_COMPARATOR_ID
        frame["stage_id"] = "COMPARATOR_V10"
        frame["family"] = "FROZEN_V10_COMPARATOR"
        frame["post_hoc"] = True
        frame["comparator_only"] = True
    metrics["description"] = "Sealed V10 max .50 + strong Gap2 comparator"
    metrics["resolved_config_sha256"] = ""
    metrics["component_variant_ids"] = ""
    return metrics, daywise


def _pairwise_daywise_deltas(
    daywise: pd.DataFrame,
    *,
    baseline_id: str = registry.CONTROL_VARIANT_ID,
) -> pd.DataFrame:
    required = {
        "variant_id",
        "scenario",
        "session_date",
        "net_return_pct",
        "net_pnl_rs",
        "fills",
    }
    missing = sorted(required - set(daywise.columns))
    if missing:
        raise ValueError(f"daywise comparison columns missing: {missing}")
    normalized = daywise.copy()
    normalized["session_date"] = normalized["session_date"].map(
        lambda value: engine._parse_day(value).isoformat()
    )
    base = normalized.loc[normalized["variant_id"].eq(baseline_id)].copy()
    if base.duplicated(["scenario", "session_date"]).any():
        raise AssertionError("V11 Stage0 daywise rows are not unique")
    base = base[
        [
            "scenario",
            "session_date",
            "net_return_pct",
            "net_pnl_rs",
            "fills",
        ]
    ].rename(
        columns={
            "net_return_pct": "control_net_return_pct",
            "net_pnl_rs": "control_net_pnl_rs",
            "fills": "control_fills",
        }
    )
    parts: list[pd.DataFrame] = []
    for variant_id, frame in normalized.loc[
        ~normalized["variant_id"].eq(baseline_id)
    ].groupby("variant_id", sort=False):
        merged = frame.merge(
            base,
            on=["scenario", "session_date"],
            how="left",
            validate="one_to_one",
        )
        if merged["control_net_return_pct"].isna().any():
            raise AssertionError(f"missing V11 daywise comparator for {variant_id}")
        merged["delta_net_return_points"] = (
            pd.to_numeric(merged["net_return_pct"], errors="raise")
            - pd.to_numeric(merged["control_net_return_pct"], errors="raise")
        )
        merged["delta_net_pnl_rs"] = (
            pd.to_numeric(merged["net_pnl_rs"], errors="raise")
            - pd.to_numeric(merged["control_net_pnl_rs"], errors="raise")
        )
        merged["delta_fills"] = (
            pd.to_numeric(merged["fills"], errors="raise")
            - pd.to_numeric(merged["control_fills"], errors="raise")
        )
        merged["control_variant_id"] = baseline_id
        parts.append(merged)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _paired_bootstrap_rows(deltas: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if deltas.empty:
        return pd.DataFrame(rows)
    for (variant_id, scenario), frame in deltas.groupby(
        ["variant_id", "scenario"], sort=False
    ):
        values = pd.to_numeric(
            frame["delta_net_return_points"], errors="raise"
        ).to_numpy(float)
        seed = int.from_bytes(
            hashlib.sha256(
                f"{SCHEMA_VERSION}|PAIRED|{variant_id}|{scenario}".encode()
            ).digest()[:8],
            "big",
        )
        rng = np.random.default_rng(seed)
        sampled = rng.integers(
            0, len(values), size=(BOOTSTRAP_REPLICATES, len(values))
        )
        sums = values[sampled].sum(axis=1)
        positive = np.sort(values[values > 0])[::-1]
        total_positive = float(positive.sum())
        rows.append(
            {
                "variant_id": variant_id,
                "scenario": scenario,
                "paired_session_count": len(values),
                "bootstrap_replicates": BOOTSTRAP_REPLICATES,
                "delta_net_points_p025": float(np.quantile(sums, 0.025)),
                "delta_net_points_median": float(np.quantile(sums, 0.5)),
                "delta_net_points_p975": float(np.quantile(sums, 0.975)),
                "probability_delta_positive": float(np.mean(sums > 0)),
                "top_5_positive_delta_days_share_pct": (
                    float(positive[:5].sum() / total_positive * 100.0)
                    if total_positive > 0
                    else math.nan
                ),
            }
        )
    return pd.DataFrame(rows)


def _safe_ratio(value: Any, baseline: Any) -> float:
    try:
        numerator = float(value)
        denominator = float(baseline)
    except (TypeError, ValueError):
        return math.nan
    if not math.isfinite(numerator) or not math.isfinite(denominator):
        return math.nan
    if denominator == 0:
        return math.nan
    return numerator / denominator


def _development_gates(
    metrics: pd.DataFrame,
    daywise: pd.DataFrame,
    side_setup: pd.DataFrame,
    results: Sequence[Mapping[str, Any]],
) -> pd.DataFrame:
    scenario_ids = tuple(name for name, _, _ in SCENARIOS)
    full = metrics.loc[
        metrics["period"].eq("FULL_USABLE")
        & metrics["variant_id"].ne(V10_COMPARATOR_ID)
    ].copy()
    control = full.loc[
        full["variant_id"].eq(registry.CONTROL_VARIANT_ID)
    ].set_index("scenario")
    if set(control.index) != set(scenario_ids):
        raise AssertionError("V12 gate baseline lacks one or more scenarios")
    result_by_id = {str(item["variant_id"]): item for item in results}
    rows: list[dict[str, Any]] = []
    for variant_id, frame in full.loc[
        ~full["variant_id"].eq(registry.CONTROL_VARIANT_ID)
    ].groupby("variant_id", sort=False):
        by_scenario = frame.set_index("scenario")
        if set(by_scenario.index) != set(scenario_ids):
            rows.append(
                {
                    "variant_id": variant_id,
                    "gate_status": "INSUFFICIENT",
                    "gate_reason": "MISSING_COST_SCENARIO",
                }
            )
            continue
        config_post_hoc = bool(frame["post_hoc"].iloc[0])
        net_ratios = {
            scenario: _safe_ratio(
                by_scenario.at[scenario, "net_return_points"],
                control.at[scenario, "net_return_points"],
            )
            for scenario in scenario_ids
        }
        net_deltas = {
            scenario: float(by_scenario.at[scenario, "net_return_points"])
            - float(control.at[scenario, "net_return_points"])
            for scenario in scenario_ids
        }
        pf_deltas = {
            scenario: float(by_scenario.at[scenario, "profit_factor"])
            - float(control.at[scenario, "profit_factor"])
            for scenario in scenario_ids
        }
        ref = "REFERENCE_15_0"
        harsh = "STRESS_25_5"
        ref_fill_retention = _safe_ratio(
            by_scenario.at[ref, "fills"], control.at[ref, "fills"]
        )
        ref_mdd_ratio = _safe_ratio(
            by_scenario.at[ref, "max_daily_drawdown_points"],
            control.at[ref, "max_daily_drawdown_points"],
        )
        record = result_by_id.get(str(variant_id), {})
        affected = int(
            dict(record.get("affected_decisions", {}))
            .get(ref, {})
            .get("affected_decisions", 0)
        )

        variant_daily = daywise.loc[daywise["variant_id"].eq(variant_id)].copy()
        control_daily = daywise.loc[
            daywise["variant_id"].eq(registry.CONTROL_VARIANT_ID)
        ].copy()
        paired = variant_daily.merge(
            control_daily[
                ["scenario", "session_date", "net_return_pct"]
            ].rename(columns={"net_return_pct": "control_net"}),
            on=["scenario", "session_date"],
            how="left",
            validate="one_to_one",
        )
        paired["delta"] = (
            pd.to_numeric(paired["net_return_pct"], errors="raise")
            - pd.to_numeric(paired["control_net"], errors="raise")
        )
        paired["_day"] = paired["session_date"].map(engine._parse_day)
        ref_paired = paired.loc[paired["scenario"].eq(ref)]
        forward_delta = float(
            ref_paired.loc[
                ref_paired["_day"] >= v10_backtest.MAX050_GAP2_EXTENSION_DAY,
                "delta",
            ].sum()
        )
        ex_july_delta = float(
            ref_paired.loc[ref_paired["_day"].map(lambda day: day.month != 7), "delta"].sum()
        )
        sides = side_setup.loc[
            side_setup["variant_id"].eq(variant_id)
            & side_setup["scenario"].eq(harsh)
            & side_setup["group_id"].isin(["SIDE_LONG", "SIDE_SHORT"])
        ]
        both_sides_positive = bool(
            set(sides["group_id"]) == {"SIDE_LONG", "SIDE_SHORT"}
            and sides["net_return_points"].gt(0).all()
        )

        checks = {
            "affected_decisions_ge_30": affected >= 30,
            "all_scenario_net_not_below_v11": all(
                value >= -1e-12 for value in net_deltas.values()
            ),
            "harsh_net_ge_105pct_v11": (
                net_ratios[harsh] >= 1.05 - 1e-12
            ),
            "all_scenario_pf_delta_ge_minus_005": all(
                value >= -0.05 - 1e-12 for value in pf_deltas.values()
            ),
            "reference_mdd_le_105pct_v11": ref_mdd_ratio <= 1.05 + 1e-12,
            "reference_fill_retention_ge_070": ref_fill_retention >= 0.70 - 1e-12,
            "forward_extension_delta_nonnegative": forward_delta >= -1e-12,
            "ex_july_delta_nonnegative": ex_july_delta >= -1e-12,
            "both_sides_harsh_positive": both_sides_positive,
        }
        failed = [name for name, passed in checks.items() if not passed]
        status = "PASS" if not failed else "FAIL"
        if affected < 30:
            status = "INSUFFICIENT"
        rows.append(
            {
                "variant_id": variant_id,
                "stage_id": frame["stage_id"].iloc[0],
                "post_hoc": config_post_hoc,
                "gate_status": status,
                "gate_reason": "PASS" if not failed else ";".join(failed),
                "affected_decisions": affected,
                "reference_fill_retention": ref_fill_retention,
                "reference_mdd_ratio": ref_mdd_ratio,
                "forward_extension_delta_points": forward_delta,
                "ex_july_delta_points": ex_july_delta,
                "both_sides_harsh_positive": both_sides_positive,
                "reference_net_ratio": net_ratios[ref],
                "stress20_net_ratio": net_ratios["STRESS_20_2"],
                "harsh_net_ratio": net_ratios[harsh],
                "worst_scenario_net_ratio": min(net_ratios.values()),
                "harsh_net_return_points": float(
                    by_scenario.at[harsh, "net_return_points"]
                ),
                "reference_mdd_points": float(
                    by_scenario.at[ref, "max_daily_drawdown_points"]
                ),
                **{f"check_{name}": passed for name, passed in checks.items()},
            }
        )
    gates = pd.DataFrame(rows)
    if not gates.empty:
        gates = gates.sort_values(
            [
                "worst_scenario_net_ratio",
                "harsh_net_return_points",
                "reference_mdd_points",
                "variant_id",
            ],
            ascending=[False, False, True, True],
            kind="stable",
        ).reset_index(drop=True)
        gates["observed_rank"] = np.arange(1, len(gates) + 1)
    return gates


def _choose_stage12_configs(gates: pd.DataFrame) -> list[registry.ResolvedConfig]:
    if gates.empty:
        return []
    passing = gates.loc[
        gates["gate_status"].eq("PASS") & ~gates["post_hoc"].astype(bool)
    ].head(4)
    ids = passing["variant_id"].astype(str).tolist()
    combinations: list[tuple[str, ...]] = []
    for left, right in itertools.combinations(ids, 2):
        if registry.compatible_for_merge(left, right):
            combinations.append((left, right))
    if len(ids) >= 3:
        top_three = tuple(ids[:3])
        fields = [
            set(registry.get_spec(value).changed_fields) for value in top_three
        ]
        if sum(len(value) for value in fields) == len(set().union(*fields)):
            combinations.append(top_three)
    resolved: list[registry.ResolvedConfig] = []
    seen: set[str] = set()
    for component_ids in combinations:
        config = registry.merge_resolved_configs(component_ids)
        if config.variant_id not in seen:
            seen.add(config.variant_id)
            resolved.append(config)
    return resolved


def _best_payload(
    gates: pd.DataFrame, metrics: pd.DataFrame
) -> dict[str, Any]:
    full_ref = metrics.loc[
        metrics["period"].eq("FULL_USABLE")
        & metrics["scenario"].eq("REFERENCE_15_0")
    ].set_index("variant_id")
    best_observed_id = (
        str(gates.iloc[0]["variant_id"]) if not gates.empty else None
    )
    isolated_pass = gates.loc[
        gates["gate_status"].eq("PASS") & ~gates["post_hoc"].astype(bool)
    ]
    post_hoc_pass = gates.loc[
        gates["gate_status"].eq("PASS") & gates["post_hoc"].astype(bool)
    ]
    best_isolated_id = (
        str(isolated_pass.iloc[0]["variant_id"])
        if not isolated_pass.empty
        else None
    )
    best_post_hoc_id = (
        str(post_hoc_pass.iloc[0]["variant_id"])
        if not post_hoc_pass.empty
        else None
    )
    decision_id = best_isolated_id or registry.CONTROL_VARIANT_ID

    def metric_payload(variant_id: str | None) -> dict[str, Any] | None:
        if variant_id is None or variant_id not in full_ref.index:
            return None
        row = full_ref.loc[variant_id]
        return {
            "variant_id": variant_id,
            "reference_fills": int(row["fills"]),
            "reference_win_rate_pct": float(row["win_rate_pct"]),
            "reference_profit_factor": float(row["profit_factor"]),
            "reference_net_return_points": float(row["net_return_points"]),
            "reference_net_pnl_rs": float(row["net_pnl_rs"]),
            "reference_max_daily_drawdown_points": float(
                row["max_daily_drawdown_points"]
            ),
        }

    return {
        "label": "BEST_OBSERVED_V12_CASH_PROXY_RESEARCH_VARIANT",
        "best_observed": metric_payload(best_observed_id),
        "best_gate_passing_isolated": metric_payload(best_isolated_id),
        "best_gate_passing_post_hoc": metric_payload(best_post_hoc_id),
        "decision_best": metric_payload(decision_id),
        "decision_rule": (
            "Use best gate-passing isolated V12; otherwise retain frozen V11. "
            "Post-hoc combinations never receive promotion authority."
        ),
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }


def _catalog_frame() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for spec in registry.VARIANT_SPECS:
        config = registry.resolve_variant(spec)
        rows.append(
            {
                "variant_id": spec.variant_id,
                "stage_id": spec.stage_id,
                "family": spec.family,
                "description": spec.description,
                "changed_fields": ";".join(spec.changed_fields),
                "resolved_config_sha256": registry.resolved_config_sha256(config),
                "resolved_config_json": json.dumps(
                    config.payload(), sort_keys=True, separators=(",", ":")
                ),
                "status": "EXECUTABLE",
            }
        )
    return pd.DataFrame(rows)


def _stage_status_frame(
    results: Sequence[Mapping[str, Any]],
    *,
    stage12_status: str,
) -> pd.DataFrame:
    rows = [
        {
            "stage_id": str(result["config"]["stage_id"]),
            "test_id": str(result["variant_id"]),
            "status": "COMPLETE",
            "reason": "All three cost scenarios completed",
            "selected_candidate_count": int(result["selected_candidate_count"]),
        }
        for result in results
    ]
    rows.extend(
        {
            "stage_id": record.stage_id,
            "test_id": record.test_id,
            "status": record.status,
            "reason": record.reason,
            "selected_candidate_count": math.nan,
        }
        for record in registry.BLOCKED_TESTS
    )
    rows.append(
        {
            "stage_id": "STAGE_12_POST_HOC_COMBINATION",
            "test_id": "GATED_COMPATIBLE_COMBINATIONS",
            "status": stage12_status,
            "reason": (
                "Only compatible isolated gate passers may be combined"
            ),
            "selected_candidate_count": math.nan,
        }
    )
    return pd.DataFrame(rows)


def _source_files() -> tuple[Path, ...]:
    files = (
        Path(__file__).resolve(),
        Path(registry.__file__).resolve(),
        Path(selection.__file__).resolve(),
        Path(v12_analysis.__file__).resolve(),
        Path(v12_execution.__file__).resolve(),
        Path(resources.__file__).resolve(),
        Path(v11_backtest.__file__).resolve(),
        Path(v11_execution.__file__).resolve(),
        Path(v11_gap.__file__).resolve(),
        Path(v10_backtest.__file__).resolve(),
        Path(locked_config.__file__).resolve(),
        Path(experiment.__file__).resolve(),
        Path(gaps.__file__).resolve(),
        Path(engine.__file__).resolve(),
        Path(common.__file__).resolve(),
    )
    if len({path.name for path in files}) != len(files):
        raise AssertionError("V12 source snapshot names must be unique")
    missing = [path for path in files if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"V12 source files missing: {missing}")
    return files


def _snapshot_sources(run_dir: Path) -> dict[str, dict[str, Any]]:
    source_dir = run_dir / "source"
    source_dir.mkdir(parents=True, exist_ok=False)
    records: dict[str, dict[str, Any]] = {}
    for source in _source_files():
        target = source_dir / source.name
        before = _sha256_file(source)
        shutil.copy2(source, target)
        copied = _sha256_file(target)
        if before != copied:
            raise AssertionError(f"V12 source snapshot changed while copying: {source}")
        records[source.name] = {
            "source_path": str(source),
            "snapshot_path": str(target.resolve()),
            "bytes": source.stat().st_size,
            "sha256": before,
        }
    return records


def _validate_sources_unchanged(records: Mapping[str, Mapping[str, Any]]) -> None:
    for name, record in records.items():
        source = Path(str(record["source_path"]))
        if not source.is_file() or _sha256_file(source) != record["sha256"]:
            raise AssertionError(f"V12 source changed during run: {name}")


def _fmt(value: Any, decimals: int = 4) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if not math.isfinite(numeric):
        return "n/a"
    return f"{numeric:.{decimals}f}"


def _comparison_table(metrics: pd.DataFrame, gates: pd.DataFrame) -> str:
    full = metrics.loc[metrics["period"].eq("FULL_USABLE")].copy()
    gate_map = (
        gates.set_index("variant_id")["gate_status"].to_dict()
        if not gates.empty
        else {}
    )
    stage_map = full.groupby("variant_id", sort=False)["stage_id"].first().to_dict()
    ids = full["variant_id"].drop_duplicates().astype(str).tolist()
    preferred = [V10_COMPARATOR_ID, registry.CONTROL_VARIANT_ID]
    ordered = [value for value in preferred if value in ids]
    remaining = [value for value in ids if value not in ordered]
    if not gates.empty:
        rank = gates.set_index("variant_id")["observed_rank"].to_dict()
        remaining.sort(key=lambda value: (rank.get(value, 10**9), value))
    ordered.extend(remaining)
    lines = [
        "| Strategy | Stage | Gate | Ref fills | Ref WR | Ref PF | Ref net pts | Ref P&L | Ref MDD | Harsh PF | Harsh net pts |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for variant_id in ordered:
        frame = full.loc[full["variant_id"].eq(variant_id)].set_index("scenario")
        if "REFERENCE_15_0" not in frame.index or "STRESS_25_5" not in frame.index:
            continue
        ref = frame.loc["REFERENCE_15_0"]
        harsh = frame.loc["STRESS_25_5"]
        gate = (
            "COMPARATOR"
            if variant_id == V10_COMPARATOR_ID
            else "CONTROL"
            if variant_id == registry.CONTROL_VARIANT_ID
            else str(gate_map.get(variant_id, "n/a"))
        )
        lines.append(
            "| "
            + " | ".join(
                (
                    variant_id,
                    str(stage_map.get(variant_id, "")),
                    gate,
                    str(int(ref["fills"])),
                    _fmt(ref["win_rate_pct"], 2),
                    _fmt(ref["profit_factor"], 4),
                    _fmt(ref["net_return_points"], 4),
                    "₹" + _fmt(ref["net_pnl_rs"], 2),
                    _fmt(ref["max_daily_drawdown_points"], 4),
                    _fmt(harsh["profit_factor"], 4),
                    _fmt(harsh["net_return_points"], 4),
                )
            )
            + " |"
        )
    return "\n".join(lines)


def _report(
    metrics: pd.DataFrame,
    gates: pd.DataFrame,
    stage_status: pd.DataFrame,
    best: Mapping[str, Any],
    resource_plan: Mapping[str, Any],
    *,
    sessions: Sequence[date],
    missing_sessions: Sequence[date],
    stage12_configs: Sequence[registry.ResolvedConfig],
) -> str:
    observed = dict(best.get("best_observed") or {})
    decision = dict(best.get("decision_best") or {})
    blocked = stage_status.loc[stage_status["status"].str.startswith("BLOCKED")]
    lines = [
        "# FNO V12 All-Stages Full-History Comparison",
        "",
        "> Research-only cash-equity proxy replay. It is not validated futures execution and has no paper/live authority.",
        "",
        "## Outcome",
        "",
        f"- Best observed V12: `{observed.get('variant_id', 'none')}`.",
        f"- Gate-based decision: `{decision.get('variant_id', 'none')}`.",
        f"- Isolated gate-passing V12: `{(best.get('best_gate_passing_isolated') or {}).get('variant_id', 'none')}`.",
        f"- Gate-passing post-hoc V12: `{(best.get('best_gate_passing_post_hoc') or {}).get('variant_id', 'none')}`.",
        "- A post-hoc winner is descriptive only; it cannot displace frozen V11 without prospective evidence.",
        "",
        "## Comparable full-history results",
        "",
        _comparison_table(metrics, gates),
        "",
        "## Research design",
        "",
        f"- Usable sessions: {len(sessions)} ({sessions[0]} through {sessions[-1]}).",
        f"- Missing regular sessions in span: {', '.join(day.isoformat() for day in missing_sessions)}.",
        f"- Predeclared configs: {len(registry.VARIANT_SPECS)} including frozen V11 Stage0.",
        f"- Stage12 combinations executed: {len(stage12_configs)}.",
        f"- Cost scenarios: {', '.join(name for name, _, _ in SCENARIOS)}.",
        "- Each challenger starts from all 1,241 candidates, filters once, reranks causally, and runs in a fresh spawned process.",
        "- Development gates require >=30 affected decisions, no net loss in any cost scenario, >=5% harsh-cost improvement, PF tolerance of -0.05, MDD/fill-retention limits, positive forward and ex-July deltas, and positive harsh LONG and SHORT totals.",
        "",
        "## Resource execution",
        "",
        f"- Logical CPUs: {resource_plan['measured_hardware']['logical_cpu_count']}.",
        f"- Available RAM at planning: {_fmt(resource_plan['measured_hardware'].get('available_memory_gib'), 2)} GiB.",
        f"- Fresh spawned workers used: {resource_plan['recommended_workers']}.",
        "- BLAS/OpenMP thread pools were fixed at one thread per worker; each task received a fresh process.",
        "",
        "## Blocked validity tests",
        "",
        "| Stage | Test | Status | Reason |",
        "|---|---|---|---|",
    ]
    for row in blocked.to_dict("records"):
        lines.append(
            f"| {row['stage_id']} | {row['test_id']} | {row['status']} | {row['reason']} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation limits",
            "",
            "- The source universe is not fully point-in-time for the whole history.",
            "- The replay uses cash-equity minute paths, not complete rolling futures paths with historical spreads, depth, margins, and dated lots.",
            "- Some paths terminate at 15:15 while others reach 15:30; exact exit optimization is therefore blocked.",
            "- Searching many variants creates selection bias. The best observed result must be validated prospectively on genuinely new sessions.",
            "",
            "All detailed selections, decisions, trades, daywise deltas, side/setup metrics, bootstrap diagnostics, gates, configs, hashes, and provenance are stored beside this report.",
            "",
        ]
    )
    return "\n".join(lines)


def _resolve_worker_count(args: argparse.Namespace, plan: dict[str, Any]) -> int:
    automatic = int(plan["recommended_workers"])
    value = str(args.workers).strip().lower()
    if value == "auto":
        return automatic
    try:
        requested = int(value)
    except ValueError as exc:
        raise ValueError("--workers must be 'auto' or a positive integer") from exc
    if requested < 1:
        raise ValueError("--workers must be positive")
    resource_limit = int(plan["limits"]["resource_worker_limit"])
    if requested > resource_limit:
        raise ValueError(
            f"requested workers={requested} exceeds current safe resource limit={resource_limit}"
        )
    plan["recommended_workers"] = requested
    plan["reason"] = f"Explicit user worker override within resource limit: {requested}"
    return requested


def _run(args: argparse.Namespace) -> Path:
    if not args.all_usable_history:
        raise ValueError("V12 supports only --all-usable-history")
    registry.validate_registry(require_pinned_hash=True, require_parent_contract=True)
    fixed_contract = v11_backtest.validate_fixed_contract(require_files=True)
    comparator_validation = _validate_sealed_comparators()
    (
        all_candidates,
        minute_paths,
        segments,
        sessions,
        expected_span,
        missing_sessions,
        raw_binding,
    ) = _load_inputs()
    if len(all_candidates) != 1241 or len(sessions) != 65:
        raise AssertionError("V12 full-history input dimensions drifted")

    base_setups = _configure_base_engine()
    stage0_prepared = selection.prepare_variant_selection(
        all_candidates, base_setups, registry.CONTROL_VARIANT_ID
    )
    stage0_binding = v11_backtest._input_binding_sha256(
        stage0_prepared.candidates, minute_paths, sessions, segments
    )
    if stage0_binding != v11_backtest.EXPECTED_INPUT_BINDING_SHA256:
        raise AssertionError(
            "V12 frozen Stage0 selection differs from V11 input binding"
        )

    output_root = args.output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(gaps.IST).strftime("%Y%m%dT%H%M%S%f%z")
    run_dir = (output_root / f"run_{stamp}").resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    source_records = _snapshot_sources(run_dir)
    common.atomic_write_csv(all_candidates, run_dir / "all_input_candidates.csv")
    common.atomic_write_csv(_catalog_frame(), run_dir / "experiment_catalog.csv")
    common.atomic_write_csv(
        pd.DataFrame([record.payload() for record in registry.BLOCKED_TESTS]),
        run_dir / "blocked_tests.csv",
    )
    common.atomic_write_json(
        run_dir / "source_segments.json",
        {"schema_version": SCHEMA_VERSION, "segments": segments},
    )
    common.atomic_write_json(
        run_dir / "registry.json",
        {
            "registry_sha256": registry.registry_sha256(),
            "registry": registry.registry_payload(),
        },
    )

    print(
        "[FNO-V12] Running frozen V11 Stage0 parity across all three costs",
        flush=True,
    )
    stage0_result = _run_variant(
        registry.resolve_variant(registry.CONTROL_VARIANT_ID),
        all_candidates,
        minute_paths,
        sessions,
        segments,
        run_dir,
    )
    isolated_configs = [
        registry.resolve_variant(spec)
        for spec in registry.VARIANT_SPECS
        if not spec.is_control
    ]
    resource_plan = resources.plan_fresh_spawn_workers(
        task_count=len(isolated_configs), apply_thread_limits=True
    )
    workers = _resolve_worker_count(args, resource_plan)
    common.atomic_write_json(run_dir / "resource_plan.json", resource_plan)
    print(
        f"[FNO-V12] Stage0 exact. Running {len(isolated_configs)} isolated "
        f"variants with {workers} fresh spawned workers",
        flush=True,
    )
    isolated_results = _execute_parallel(
        isolated_configs,
        run_dir=run_dir,
        raw_binding=raw_binding,
        workers=workers,
    )
    results: list[dict[str, Any]] = [stage0_result, *isolated_results]
    metrics, daywise, side_setup, robustness = _collect_result_frames(results)
    affected_map = {
        result["variant_id"]: result["affected_decisions"] for result in results
    }
    isolated_gates = v12_analysis.isolated_development_gates(
        metrics,
        daywise,
        side_setup,
        registry.CONTROL_VARIANT_ID,
        affected_map,
        candidate_variant_ids=[config.variant_id for config in isolated_configs],
        forward_session_dates=[
            day
            for day in sessions
            if day >= v10_backtest.MAX050_GAP2_EXTENSION_DAY
        ],
    )

    stage12_configs = (
        [] if args.skip_stage12 else _choose_stage12_configs(isolated_gates)
    )
    isolated_passer_count = int(
        isolated_gates["gate_status"].eq("PASS").sum()
        if not isolated_gates.empty
        else 0
    )
    if args.skip_stage12:
        stage12_plan_status = "SKIPPED_BY_OPTION"
    elif stage12_configs:
        stage12_plan_status = "EXECUTABLE_POST_HOC"
    elif isolated_passer_count < 2:
        stage12_plan_status = "NOT_RUN_FEWER_THAN_TWO_ISOLATED_PASSERS"
    else:
        stage12_plan_status = "NOT_RUN_NO_COMPATIBLE_PASSER_COMBINATION"
    common.atomic_write_json(
        run_dir / "stage12_plan.json",
        {
            "schema_version": SCHEMA_VERSION,
            "status": stage12_plan_status,
            "isolated_gate_passer_count": isolated_passer_count,
            "configs": [config.payload() for config in stage12_configs],
        },
    )
    if stage12_configs:
        print(
            f"[FNO-V12] Running {len(stage12_configs)} gated Stage12 "
            "post-hoc combinations",
            flush=True,
        )
        stage12_results = _execute_parallel(
            stage12_configs,
            run_dir=run_dir,
            raw_binding=raw_binding,
            workers=min(workers, len(stage12_configs)),
        )
        results.extend(stage12_results)
        metrics, daywise, side_setup, robustness = _collect_result_frames(results)
    affected_map = {
        result["variant_id"]: result["affected_decisions"] for result in results
    }
    gates = v12_analysis.isolated_development_gates(
        metrics,
        daywise,
        side_setup,
        registry.CONTROL_VARIANT_ID,
        affected_map,
        candidate_variant_ids=[
            result["variant_id"]
            for result in results
            if result["variant_id"] != registry.CONTROL_VARIANT_ID
        ],
        forward_session_dates=[
            day
            for day in sessions
            if day >= v10_backtest.MAX050_GAP2_EXTENSION_DAY
        ],
    )

    v10_metrics, v10_daywise = _import_v10_comparator()
    all_metrics = pd.concat([v10_metrics, metrics], ignore_index=True, sort=False)
    all_daywise = pd.concat([v10_daywise, daywise], ignore_index=True, sort=False)
    delta_variant_ids = [
        value
        for value in all_daywise["variant_id"].drop_duplicates().astype(str)
        if value != registry.CONTROL_VARIANT_ID
    ]
    deltas = v12_analysis.pairwise_daywise_deltas(
        all_daywise,
        registry.CONTROL_VARIANT_ID,
        variant_ids=delta_variant_ids,
    )
    paired_bootstrap = v12_analysis.paired_bootstrap_and_concentration(deltas)
    robustness = robustness.merge(
        paired_bootstrap,
        on=["variant_id", "scenario"],
        how="left",
        validate="one_to_one",
    )
    best = _best_payload(gates, metrics)
    stage12_status = (
        "COMPLETE" if stage12_configs else stage12_plan_status
    )
    stage_status = _stage_status_frame(results, stage12_status=stage12_status)

    outputs = {
        "metrics": run_dir / "all_period_metrics.csv",
        "daywise": run_dir / "all_daywise.csv",
        "side_setup": run_dir / "side_setup_metrics.csv",
        "deltas": run_dir / "pairwise_daywise_deltas.csv",
        "gates": run_dir / "development_gates.csv",
        "robustness": run_dir / "bootstrap_and_concentration.csv",
        "status": run_dir / "stage_status.csv",
        "best": run_dir / "best_observed.json",
        "report": run_dir / "FNO_V12_ALL_STAGES_COMPARISON.md",
    }
    common.atomic_write_csv(all_metrics, outputs["metrics"])
    common.atomic_write_csv(all_daywise, outputs["daywise"])
    common.atomic_write_csv(side_setup, outputs["side_setup"])
    common.atomic_write_csv(deltas, outputs["deltas"])
    common.atomic_write_csv(gates, outputs["gates"])
    common.atomic_write_csv(robustness, outputs["robustness"])
    common.atomic_write_csv(stage_status, outputs["status"])
    common.atomic_write_json(outputs["best"], gaps._json_ready(best))
    common.atomic_write_text(
        outputs["report"],
        _report(
            all_metrics,
            gates,
            stage_status,
            best,
            resource_plan,
            sessions=sessions,
            missing_sessions=missing_sessions,
            stage12_configs=stage12_configs,
        ),
    )

    end_raw_binding = _raw_input_binding(
        all_candidates, minute_paths, sessions, segments
    )
    if end_raw_binding != raw_binding:
        raise AssertionError("V12 inputs mutated during execution")
    _validate_sources_unchanged(source_records)
    provenance_path = run_dir / "provenance.json"
    inventory_path = run_dir / "artifact_inventory.json"
    common.atomic_write_json(
        inventory_path,
        {
            "schema_version": SCHEMA_VERSION,
            "artifacts": gaps._inventory_files(
                run_dir, exclude={provenance_path, inventory_path}
            ),
        },
    )
    inventory_validation = _validate_inventory(run_dir, inventory_path)
    command = [
        "python",
        "-u",
        "fno_v12_staged_backtest.py",
        "run",
        "--all-usable-history",
        "--max-system-resources",
    ]
    if str(args.workers).lower() != "auto":
        command.extend(["--workers", str(args.workers)])
    if args.skip_stage12:
        command.append("--skip-stage12")
    if output_root != OUTPUT_ROOT.resolve():
        command.extend(["--output-root", str(output_root)])
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "created_at_ist": datetime.now(gaps.IST),
        "run_dir": str(run_dir),
        "command": command,
        "research_design": RESEARCH_DESIGN,
        "registry_sha256": registry.registry_sha256(),
        "frozen_v11_contract": fixed_contract,
        "sealed_comparator_validation": comparator_validation,
        "raw_input_binding_sha256": raw_binding,
        "stage0_input_binding_sha256": stage0_binding,
        "raw_input_binding_rechecked_at_end": True,
        "usable_session_dates": [day.isoformat() for day in sessions],
        "usable_session_count": len(sessions),
        "calendar_span_session_count": len(expected_span),
        "missing_regular_session_dates": [
            day.isoformat() for day in missing_sessions
        ],
        "source_segments": segments,
        "source_snapshots": source_records,
        "sources_rechecked_at_end": True,
        "resource_plan": resource_plan,
        "executed_variant_count": len(results),
        "isolated_variant_count": len(isolated_results),
        "stage12_variant_count": len(stage12_configs),
        "executed_scenarios": [name for name, _, _ in SCENARIOS],
        "result_records": [
            {
                "variant_id": result["variant_id"],
                "config": result["config"],
                "selected_candidate_count": result["selected_candidate_count"],
                "artifacts": result["artifacts"],
                "affected_decisions": result["affected_decisions"],
                "control_parity": result["control_parity"],
            }
            for result in results
        ],
        "best": best,
        "outputs": {key: str(value.resolve()) for key, value in outputs.items()},
        "artifact_inventory": {
            "path": str(inventory_path.resolve()),
            "sha256": _sha256_file(inventory_path),
            **inventory_validation,
        },
        "limitations": [
            "POST_HOC_SEARCH_REQUIRES_PROSPECTIVE_VALIDATION",
            "STATIC_NON_POINT_IN_TIME_UNIVERSE_IN_CORE_HISTORY",
            "SOURCE_SLOT_COVERAGE_INCOMPLETE",
            "MIXED_1515_1530_PATH_BOUNDARY",
            "2026_08_26_HAS_NO_VALIDATED_CACHE",
            "CASH_EQUITY_PATHS_NOT_ACTUAL_ROLLING_FUTURES_EXECUTION",
            "HISTORICAL_SPREAD_DEPTH_MARGIN_AND_DATED_LOT_DATA_ABSENT",
        ],
        "headline_valid": False,
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }
    common.atomic_write_json(provenance_path, gaps._json_ready(provenance))
    common.atomic_write_json(
        output_root / "latest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_dir": str(run_dir),
            "provenance_sha256": _sha256_file(provenance_path),
            "inventory_sha256": _sha256_file(inventory_path),
            "registry_sha256": registry.registry_sha256(),
            "best": best,
            "research_only": True,
        },
    )
    print(f"[FNO-V12] complete: {run_dir}", flush=True)
    return run_dir


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fno_v12_staged_backtest.py",
        description="Frozen V11 Stage0 plus all predeclared V12 research stages.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    catalog = commands.add_parser("catalog", help="Print immutable V12 catalog")
    catalog.set_defaults(handler=lambda args: print(json.dumps(
        registry.registry_payload(), indent=2, sort_keys=True
    )))
    run = commands.add_parser("run", help="Run all V12 stages")
    run.add_argument("--all-usable-history", action="store_true", required=True)
    run.add_argument(
        "--max-system-resources",
        action="store_true",
        help="Use the automatic maximum-practical fresh-process resource plan.",
    )
    run.add_argument(
        "--workers",
        default="auto",
        help="Fresh spawned workers: auto (default) or an integer within the resource limit.",
    )
    run.add_argument("--skip-stage12", action="store_true")
    run.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    run.set_defaults(handler=_run)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = args.handler(args)
    if isinstance(result, Path):
        print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
