"""Locked standalone FNO V12 full-history backtester.

This file is the fixed, single-strategy entry point for the strongest
gate-passing isolated V12 research variant:
``V12_S06_LATE_SHORT_VOLUME_MIN_150``.  It deliberately does not discover,
rank, or combine variants at runtime.  The strategy retains the frozen V11
Stage-10 execution stack and adds only inclusive five-minute volume-ratio
minimums of 1.50 for the 09:40 SHORT and 09:45 SHORT selections.

Every replay starts from the complete 1,241-candidate input frame, rebuilds
the fixed 1,017-candidate V12 selection, and verifies the pinned 65-session
economics and trade fingerprints.  This module has backtest/research
authority only.  It has no live- or paper-trading authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import sys
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

import fno_oi_common as common
import fno_v10_backtest as v10_backtest
import fno_v10_backtest_config as locked_config
import fno_v10_experiment_backtest as experiment
import fno_v10_gap_guard_research as gaps
import fno_v11_backtest as v11_backtest
import fno_v11_execution_runtime as v11_execution
import fno_v11_gap_runtime as v11_gap
import fno_v12_execution_runtime as v12_execution
import fno_v12_selection_runtime as v12_selection
import fno_v12_variant_registry as registry
import fno_v8_windowed_1m_entry_backtest as engine


SCHEMA_VERSION = "fno_v12_late_short_volume_150_locked_backtest_v1"
PROFILE_ID = "V12_S06_LATE_SHORT_VOLUME_MIN_150"
STAGE_ID = "STAGE_06D_LATE_SHORT_VOLUME"
FAMILY = "SELECTION_FIVE_MINUTE_VOLUME_MIN"
DESCRIPTION = (
    "09:40 and 09:45 SHORT five-minute volume ratio minimum 1.50 inclusive"
)
OUTPUT_ROOT = (
    common.FNO_ROOT
    / "strategy_research"
    / "v12_s06_late_short_volume_min150_full_history_v1"
)
TARGET_EXPOSURE_PER_ENTRY_RS = 50_000.0
SQUARE_OFF = "15:30"
EOD_POLICY = "LAST_REAL_BAR_SENSITIVITY"
GAP_VARIANT = "MAX_2_BPS"

EXPECTED_REGISTRY_SHA256 = (
    "4948ba186095a5baea6b538a64255bc7304e96720ba98da512d6d21490328c35"
)
EXPECTED_RESOLVED_CONFIG_SHA256 = (
    "660ab5d2d06290d23e6b39593ddbb5afe03f51e3b6bb714099134eff7481ca4f"
)
EXPECTED_PROFILE_SHA256 = (
    "067c5f1c14b7f626b0c112524c2a0c63bc9f379f6d081547bfc747e1c8fa7cbe"
)
# Public compatibility name used by the frozen V10/V11 standalone runners.
LOCKED_PROFILE_SHA256 = EXPECTED_PROFILE_SHA256
EXPECTED_SESSION_COUNT = 65
EXPECTED_ALL_CANDIDATES = 1241
EXPECTED_SELECTED_CANDIDATES = 1017
EXPECTED_INPUT_BINDING_SHA256 = (
    "78c4d7088f7cf500ec8da587a200314c43cf669a56e2df2aca52b74ec025e62c"
)
EXPECTED_SCENARIOS: tuple[tuple[str, float, float], ...] = tuple(
    v11_backtest.EXPECTED_SCENARIOS
)

FIXED_CONFIG = registry.resolve_variant(PROFILE_ID)

EXPECTED_FULL_USABLE: dict[str, dict[str, object]] = {
    "REFERENCE_15_0": {
        "sessions": 65,
        "candidates": 1017,
        "fills": 229,
        "wins": 120,
        "losses": 109,
        "flat_trades": 0,
        "win_rate_pct": 52.40174672489083,
        "profit_factor": 2.235608588019062,
        "net_return_points": 96.44436250687984,
        "net_pnl_rs": 47_503.83646266349,
        "max_daily_drawdown_points": 5.269268744424497,
        "positive_days": 37,
        "negative_days": 24,
        "flat_days": 4,
        "remaining_gap_fills": 23,
        "guard_rejections": 22,
        "data_incomplete_candidates": 0,
    },
    "STRESS_20_2": {
        "sessions": 65,
        "candidates": 1017,
        "fills": 229,
        "wins": 116,
        "losses": 113,
        "flat_trades": 0,
        "win_rate_pct": 50.65502183406113,
        "profit_factor": 1.942252656412753,
        "net_return_points": 80.33885933960273,
        "net_pnl_rs": 39_710.98746792726,
        "max_daily_drawdown_points": 6.266063217303861,
        "positive_days": 34,
        "negative_days": 27,
        "flat_days": 4,
        "remaining_gap_fills": 23,
        "guard_rejections": 22,
        "data_incomplete_candidates": 0,
    },
    "STRESS_25_5": {
        "sessions": 65,
        "candidates": 1017,
        "fills": 229,
        "wins": 111,
        "losses": 118,
        "flat_trades": 0,
        "win_rate_pct": 48.47161572052402,
        "profit_factor": 1.6285926256965166,
        "net_return_points": 59.8044590382387,
        "net_pnl_rs": 29_759.080444006366,
        "max_daily_drawdown_points": 7.278721516986862,
        "positive_days": 32,
        "negative_days": 29,
        "flat_days": 4,
        "remaining_gap_fills": 23,
        "guard_rejections": 22,
        "data_incomplete_candidates": 0,
    },
}

EXPECTED_CLOSED_TRADE_FINGERPRINTS = {
    "REFERENCE_15_0": (
        "b200e6b5ce29044462a6b3edc43ac09736643b9a04129ff431cdffe08c612428"
    ),
    "STRESS_20_2": (
        "a452e295d15aed4eeeccaa97d79efb9814b638296140cb63c521b6f81db58816"
    ),
    "STRESS_25_5": (
        "1befedf2c49f8af4f7647b31cf6ba69e061216097233b8d25fc1f055fdc46c63"
    ),
}

_FLOAT_BENCHMARK_FIELDS = frozenset(
    {
        "win_rate_pct",
        "profit_factor",
        "net_return_points",
        "net_pnl_rs",
        "max_daily_drawdown_points",
    }
)
_BENCHMARK_ABS_TOLERANCE = 1e-9
_CLOSED_TRADE_FINGERPRINT_COLUMNS = (
    "candidate_id",
    "setup_id",
    "symbol",
    "side",
    "entry_time",
    "entry_price",
    "stop_price",
    "target_price",
    "exit_time",
    "exit_price",
    "exit_reason",
    "gross_return_pct",
    "net_return_pct",
    "quantity",
    "gross_pnl_rs",
    "estimated_cost_rs",
    "net_pnl_rs",
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _profile_payload() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "profile_id": PROFILE_ID,
        "stage_id": STAGE_ID,
        "family": FAMILY,
        "description": DESCRIPTION,
        "selection_origin": {
            "isolated_predeclared_variant": True,
            "gate_passing_observed": True,
            "winner_selected_after_v12_research": True,
            "stage12_combination": False,
        },
        "v12_registry": {
            "registry_id": registry.REGISTRY_ID,
            "registry_sha256": EXPECTED_REGISTRY_SHA256,
            "resolved_config_sha256": EXPECTED_RESOLVED_CONFIG_SHA256,
        },
        "resolved_config": FIXED_CONFIG.payload(),
        "parent_v11": {
            "profile_id": v11_backtest.PROFILE_ID,
            "profile_sha256": v11_backtest.LOCKED_PROFILE_SHA256,
            "runtime_spec": asdict(v11_backtest.FIXED_RUNTIME_SPEC),
        },
        "execution_stack": [
            "V11_FIXED_RUNTIME_OUTER",
            "V12_NEUTRAL_RUNTIME_INNER",
            "V11_STRONG_IDENTITY_GAP2_INNERMOST",
        ],
        "selection_contract": {
            "start_from": "ALL_1241_INPUT_CANDIDATES",
            "move_0935_long_max_pct": 0.50,
            "move_0940_long_min_pct": 0.40,
            "volume_0940_short_min": 1.50,
            "volume_0945_short_min": 1.50,
            "bounds": "INCLUSIVE",
            "rerank_after_selection": True,
        },
        "gap_guard": {
            "variant": GAP_VARIANT,
            "max_adverse_gap_bps": 2.0,
            "identity_policy": v11_gap.IDENTITY_POLICY,
        },
        "target_exposure_per_entry_rs": TARGET_EXPOSURE_PER_ENTRY_RS,
        "square_off": SQUARE_OFF,
        "eod_policy": EOD_POLICY,
        "cost_scenarios": [
            {"scenario": name, "cost_bps": cost, "slippage_bps": slippage}
            for name, cost, slippage in EXPECTED_SCENARIOS
        ],
        "historical_contract": {
            "usable_sessions": EXPECTED_SESSION_COUNT,
            "all_input_candidates": EXPECTED_ALL_CANDIDATES,
            "selected_candidates": EXPECTED_SELECTED_CANDIDATES,
            "input_binding_sha256": EXPECTED_INPUT_BINDING_SHA256,
            "missing_regular_session_dates": ["2026-08-26"],
        },
        "limitations": [
            "WINNER_SELECTION_REQUIRES_PROSPECTIVE_VALIDATION",
            "CASH_EQUITY_PATHS_NOT_ACTUAL_ROLLING_FUTURES_EXECUTION",
            "SOURCE_SLOT_COVERAGE_INCOMPLETE",
            "STATIC_CONTRACT_UNIVERSES_BY_SEGMENT",
        ],
        "headline_valid": False,
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }


def profile_sha256() -> str:
    return common.canonical_json_sha256(_profile_payload())


def _frame_content_sha256(frame: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    digest.update(
        json.dumps(
            {
                "columns": [str(column) for column in frame.columns],
                "dtypes": [str(dtype) for dtype in frame.dtypes],
                "rows": len(frame),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    try:
        row_hashes = pd.util.hash_pandas_object(
            frame, index=True, categorize=False
        ).to_numpy(dtype="uint64", copy=False)
    except TypeError:
        normalized = frame.copy()
        for column in normalized.columns:
            if normalized[column].dtype == "object":
                normalized[column] = normalized[column].map(repr)
        row_hashes = pd.util.hash_pandas_object(
            normalized, index=True, categorize=False
        ).to_numpy(dtype="uint64", copy=False)
    digest.update(row_hashes.tobytes())
    return digest.hexdigest()


def _input_binding_sha256(
    selected: pd.DataFrame,
    minute_paths: pd.DataFrame,
    sessions: Sequence[date],
    segments: Sequence[Mapping[str, Any]],
) -> str:
    payload = {
        "sessions": [day.isoformat() for day in sessions],
        "segments": gaps._json_ready(list(segments)),
        "selected_sha256": _frame_content_sha256(selected),
        "minute_paths_sha256": _frame_content_sha256(minute_paths),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _gap_spec() -> gaps.GapGuardSpec:
    return gaps.GapGuardSpec(GAP_VARIANT, 2.0)


def _runtime_spec(
    prepared: v12_selection.PreparedSelection | None = None,
) -> v12_execution.RuntimeSpec:
    scores = (
        prepared.selection_metadata.equal_rank_picker_scores
        if prepared is not None
        else None
    )
    return v12_execution.runtime_spec_from_rule(
        FIXED_CONFIG.runtime, equal_rank_picker_scores=scores
    )


def validate_fixed_contract(*, require_files: bool = True) -> dict[str, Any]:
    """Fail closed if any fixed V12 or inherited V11 rule drifts."""

    parent = v11_backtest.validate_fixed_contract(require_files=require_files)
    registry.validate_registry(
        require_pinned_hash=True, require_parent_contract=True
    )
    if registry.registry_sha256() != EXPECTED_REGISTRY_SHA256:
        raise AssertionError("locked V12 registry SHA-256 drifted")
    if registry.resolved_config_sha256(FIXED_CONFIG) != (
        EXPECTED_RESOLVED_CONFIG_SHA256
    ):
        raise AssertionError("locked V12 resolved-config SHA-256 drifted")
    if (FIXED_CONFIG.variant_id, FIXED_CONFIG.stage_id, FIXED_CONFIG.family) != (
        PROFILE_ID,
        STAGE_ID,
        FAMILY,
    ):
        raise AssertionError("locked V12 variant identity drifted")
    if FIXED_CONFIG.description != DESCRIPTION:
        raise AssertionError("locked V12 description drifted")
    if asdict(FIXED_CONFIG.selection) != {
        "move_0935_long_max_pct": 0.50,
        "move_0940_long_min_pct": 0.40,
        "move_0925_long_max_pct": None,
        "move_0925_short_max_pct": None,
        "volume_0935_long_min": 1.00,
        "volume_0940_short_min": 1.50,
        "volume_0945_short_min": 1.50,
        "ema_gap_0925_short_persistence_min_ratio": None,
        "picker_0940_short": "max_move",
    }:
        raise AssertionError("locked V12 selection rules drifted")
    if asdict(FIXED_CONFIG.runtime) != {
        "m2_short_mode": None,
        "m2_short_setup_ids": ("09:25_SHORT", "09:30_SHORT"),
        "long_entry_expiry_minute": None,
    }:
        raise AssertionError("locked V12 runtime overlay drifted")
    if asdict(FIXED_CONFIG.gap) != {"max_adverse_gap_bps": 2.0}:
        raise AssertionError("locked V12 gap rule drifted")
    runtime = _runtime_spec()
    if not runtime.is_neutral:
        raise AssertionError("locked V12 runtime overlay must remain neutral")
    gap = _gap_spec()
    gap.validate()
    if gap.max_adverse_gap_bps != 2.0 or gap.reject_all_gap_fills:
        raise AssertionError("locked V12 requires the maximum 2 bps gap guard")
    if v11_gap.IDENTITY_POLICY != "STRONG_REFERENCE_AND_IS_CHECK":
        raise AssertionError("locked V12 requires the V11 strong-identity guard")
    observed_scenarios = tuple(
        (str(name), float(cost), float(slippage))
        for name, cost, slippage in gaps.COST_SCENARIOS
    )
    if observed_scenarios != EXPECTED_SCENARIOS:
        raise AssertionError("locked V12 cost scenarios drifted")
    observed_profile_sha256 = profile_sha256()
    if observed_profile_sha256 != EXPECTED_PROFILE_SHA256:
        raise AssertionError(
            "locked standalone V12 profile SHA-256 drifted: "
            f"expected={EXPECTED_PROFILE_SHA256} observed={observed_profile_sha256}"
        )
    return {
        "validated": True,
        "profile_id": PROFILE_ID,
        "profile_sha256": observed_profile_sha256,
        "registry_sha256": EXPECTED_REGISTRY_SHA256,
        "resolved_config_sha256": EXPECTED_RESOLVED_CONFIG_SHA256,
        "parent_v11": parent,
        "gap_identity_policy": v11_gap.IDENTITY_POLICY,
    }


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
    scenario: str,
    cost_bps: float,
    slippage_bps: float,
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    audit_days = audit["session_date"].map(engine._parse_day)
    rows: list[dict[str, Any]] = []
    full_daily = pd.DataFrame()
    gap = _gap_spec()
    for period, days in _periods(sessions, segments):
        subset = audit.loc[audit_days.isin(set(days))].copy()
        row, daily = gaps.metric_row(
            subset,
            days,
            dataset="ALL_USABLE_HISTORY",
            period=period,
            scenario=scenario,
            spec=gap,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
        )
        row.update(
            {
                "profile_id": PROFILE_ID,
                "variant_id": PROFILE_ID,
                "stage_id": STAGE_ID,
                "family": FAMILY,
                "description": DESCRIPTION,
                "registry_sha256": EXPECTED_REGISTRY_SHA256,
                "resolved_config_sha256": EXPECTED_RESOLVED_CONFIG_SHA256,
                "post_hoc": False,
            }
        )
        rows.append(row)
        if period == "FULL_USABLE":
            full_daily = daily.copy()
            full_daily["profile_id"] = PROFILE_ID
            full_daily["variant_id"] = PROFILE_ID
            full_daily["stage_id"] = STAGE_ID
            full_daily["family"] = FAMILY
    return rows, full_daily


def validate_full_usable_benchmark(
    row: Mapping[str, Any], scenario: str
) -> dict[str, Any]:
    if scenario not in EXPECTED_FULL_USABLE:
        raise KeyError(f"unknown locked V12 scenario: {scenario}")
    expected = EXPECTED_FULL_USABLE[scenario]
    observed: dict[str, Any] = {}
    mismatches: list[str] = []
    for field, expected_value in expected.items():
        if field not in row:
            mismatches.append(f"{field}=MISSING")
            continue
        observed_value = row[field]
        observed[field] = gaps._json_ready(observed_value)
        if field in _FLOAT_BENCHMARK_FIELDS:
            try:
                matches = math.isclose(
                    float(observed_value),
                    float(expected_value),
                    rel_tol=0.0,
                    abs_tol=_BENCHMARK_ABS_TOLERANCE,
                )
            except (TypeError, ValueError):
                matches = False
        else:
            matches = observed_value == expected_value
        if not matches:
            mismatches.append(
                f"{field}={observed_value!r} expected={expected_value!r}"
            )
    if mismatches:
        raise AssertionError(
            f"locked V12 {PROFILE_ID} {scenario} benchmark drifted: "
            + "; ".join(mismatches)
        )
    return {
        "verified": True,
        "scenario": scenario,
        "float_abs_tolerance": _BENCHMARK_ABS_TOLERANCE,
        "expected": gaps._json_ready(expected),
        "observed": observed,
    }


def _closed_trade_economic_fingerprint(trades: pd.DataFrame) -> str:
    missing = sorted(set(_CLOSED_TRADE_FINGERPRINT_COLUMNS) - set(trades.columns))
    if missing:
        raise AssertionError(f"closed-trade fingerprint columns missing: {missing}")
    canonical = trades.loc[:, _CLOSED_TRADE_FINGERPRINT_COLUMNS].copy()
    canonical["candidate_id"] = canonical["candidate_id"].astype(str)
    canonical = canonical.sort_values("candidate_id", kind="stable").reset_index(
        drop=True
    )
    payload = canonical.to_csv(
        index=False, lineterminator="\n", float_format="%.17g"
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _source_files() -> tuple[Path, ...]:
    module_dir = Path(__file__).resolve().parent
    sources = (
        Path(__file__).resolve(),
        Path(registry.__file__).resolve(),
        Path(v12_selection.__file__).resolve(),
        Path(v12_execution.__file__).resolve(),
        Path(v11_backtest.__file__).resolve(),
        Path(v11_execution.__file__).resolve(),
        Path(v11_gap.__file__).resolve(),
        Path(v10_backtest.__file__).resolve(),
        Path(locked_config.__file__).resolve(),
        Path(experiment.__file__).resolve(),
        Path(gaps.__file__).resolve(),
        Path(engine.__file__).resolve(),
        Path(common.__file__).resolve(),
        module_dir / "fno_v10_followup_challenger_research.py",
        module_dir / "fno_v10_experiment_config.py",
        module_dir / "fno_v10_unified_5m_1m_backtest.py",
        module_dir / "fno_oi_backtest_provenance.py",
        module_dir / "fno_oi_hybrid_data.py",
        module_dir / "eqidv2_runtime_paths.py",
    )
    if len({path.name for path in sources}) != len(sources):
        raise AssertionError("standalone V12 source snapshot names are not unique")
    missing = [path for path in sources if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"standalone V12 source files are missing: {missing}")
    return sources


def _snapshot_sources(run_dir: Path) -> dict[str, dict[str, str]]:
    source_dir = run_dir / "source"
    source_dir.mkdir()
    records: dict[str, dict[str, str]] = {}
    for source in _source_files():
        live_sha256 = _sha256_file(source)
        snapshot = source_dir / source.name
        shutil.copy2(source, snapshot)
        snapshot_sha256 = _sha256_file(snapshot)
        if snapshot_sha256 != live_sha256:
            raise AssertionError(f"source snapshot drifted while copying {source.name}")
        records[source.name] = {
            "path": str(source),
            "snapshot_path": str(snapshot.resolve()),
            "sha256": live_sha256,
        }
    return records


def _validate_sources_unchanged(records: Mapping[str, Mapping[str, str]]) -> None:
    for name, record in records.items():
        path = Path(str(record["path"])).resolve()
        if _sha256_file(path) != record["sha256"]:
            raise AssertionError(f"source changed during standalone V12 run: {name}")


def _write_scenario_outputs(
    run_dir: Path,
    audit: pd.DataFrame,
    daily: pd.DataFrame,
    summary: Mapping[str, Any],
    scenario: str,
) -> dict[str, str]:
    scenario_dir = run_dir / "scenarios" / scenario.lower()
    scenario_dir.mkdir(parents=True)
    audit_path = scenario_dir / "candidate_order_audit.csv"
    closed_path = scenario_dir / "closed_trades.csv"
    daily_path = scenario_dir / "daywise.csv"
    summary_path = scenario_dir / "summary.json"
    common.atomic_write_csv(audit, audit_path)
    common.atomic_write_csv(audit.loc[gaps._closed_mask(audit)], closed_path)
    common.atomic_write_csv(daily, daily_path)
    common.atomic_write_json(summary_path, gaps._json_ready(dict(summary)))
    return {
        "audit": str(audit_path.resolve()),
        "closed_trades": str(closed_path.resolve()),
        "daywise": str(daily_path.resolve()),
        "summary": str(summary_path.resolve()),
    }


def _report(
    metrics: pd.DataFrame,
    *,
    sessions: Sequence[date],
    missing_sessions: Sequence[date],
) -> str:
    full = metrics.loc[metrics["period"].eq("FULL_USABLE")].set_index("scenario")
    reference = full.loc["REFERENCE_15_0"]
    v11_reference = float(
        v11_backtest.EXPECTED_FULL_USABLE["REFERENCE_15_0"]["net_return_points"]
    )
    v10_reference = float(
        v10_backtest.MAX050_GAP2_CURRENT_MIXED_BENCHMARK["net_return_points"]
    )
    versus_v11 = (float(reference["net_return_points"]) / v11_reference - 1.0) * 100
    versus_v10 = (float(reference["net_return_points"]) / v10_reference - 1.0) * 100
    lines = [
        "# Locked standalone FNO V12 backtest",
        "",
        f"Profile: `{PROFILE_ID}`",
        f"Profile SHA-256: `{profile_sha256()}`",
        f"Resolved config SHA-256: `{EXPECTED_RESOLVED_CONFIG_SHA256}`",
        f"Usable sessions: {len(sessions)} ({min(sessions)} through {max(sessions)})",
        "Missing regular sessions inside the span: "
        + (", ".join(map(str, missing_sessions)) or "none"),
        "",
        "Fixed mechanisms:",
        "",
        "- Frozen V11 Stage-10 execution and same-side reservation rules.",
        "- 09:35 LONG move <= 0.50% and 09:40 LONG move >= 0.40%.",
        "- 09:40 SHORT and 09:45 SHORT volume ratio >= 1.50.",
        "- Strong-reference maximum 2 bps adverse-gap guard.",
        "",
        "| Scenario | Fills | W-L | WR | PF | Net points | Net P&L | MDD |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for scenario, _, _ in EXPECTED_SCENARIOS:
        row = full.loc[scenario]
        lines.append(
            "| {scenario} | {fills} | {wins}-{losses} | {wr:.2f}% | "
            "{pf:.4f} | {points:+.4f} | Rs {pnl:+,.2f} | {mdd:.4f} |".format(
                scenario=scenario,
                fills=int(row["fills"]),
                wins=int(row["wins"]),
                losses=int(row["losses"]),
                wr=float(row["win_rate_pct"]),
                pf=float(row["profit_factor"]),
                points=float(row["net_return_points"]),
                pnl=float(row["net_pnl_rs"]),
                mdd=float(row["max_daily_drawdown_points"]),
            )
        )
    lines.extend(
        [
            "",
            f"Reference net-points change versus frozen V11: **{versus_v11:+.2f}%**.",
            f"Reference net-points change versus frozen V10: **{versus_v10:+.2f}%**.",
            "",
            "This is the best observed gate-passing isolated V12 cash-proxy "
            "research variant. Winner selection remains post-sample, the paths "
            "are not rolling futures executions, and the profile is not approved "
            "for paper or live trading.",
            "",
        ]
    )
    return "\n".join(lines)


def _validate_inventory(run_dir: Path, inventory_path: Path) -> dict[str, Any]:
    payload = json.loads(inventory_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise AssertionError("standalone V12 artifact inventory schema drifted")
    records = list(payload.get("artifacts", ()))
    if not records:
        raise AssertionError("standalone V12 artifact inventory is empty")
    listed: set[str] = set()
    for record in records:
        relative = Path(str(record["relative_path"]))
        relative_text = str(relative).replace("\\", "/")
        if relative.is_absolute() or ".." in relative.parts or relative_text in listed:
            raise AssertionError(f"invalid inventory path: {relative_text}")
        path = (run_dir / relative).resolve()
        if not path.is_relative_to(run_dir) or not path.is_file():
            raise FileNotFoundError(f"inventoried artifact is missing: {path}")
        if path.stat().st_size != int(record["bytes"]):
            raise AssertionError(f"inventoried artifact size drifted: {relative_text}")
        if _sha256_file(path) != str(record["sha256"]):
            raise AssertionError(f"inventoried artifact hash drifted: {relative_text}")
        listed.add(relative_text)
    actual = {
        str(path.relative_to(run_dir)).replace("\\", "/")
        for path in run_dir.rglob("*")
        if path.is_file()
        and str(path.relative_to(run_dir)).replace("\\", "/")
        not in {"artifact_inventory.json", "provenance.json"}
    }
    if listed != actual:
        raise AssertionError(
            "standalone V12 artifact inventory set is incomplete: "
            f"missing={sorted(actual - listed)} extra={sorted(listed - actual)}"
        )
    return {
        "validated": True,
        "artifact_count": len(records),
        "all_sizes_and_hashes_valid": True,
        "listed_set_matches_run_files": True,
    }


def validate_run_provenance(path: Path) -> dict[str, Any]:
    provenance_path = path.expanduser().resolve()
    if not provenance_path.is_file():
        raise FileNotFoundError(provenance_path)
    payload = json.loads(provenance_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise AssertionError("standalone V12 provenance schema drifted")
    if payload.get("complete") is not True:
        raise AssertionError("standalone V12 run is incomplete")
    if payload.get("profile_id") != PROFILE_ID:
        raise AssertionError("standalone V12 provenance profile ID drifted")
    if payload.get("profile_sha256") != EXPECTED_PROFILE_SHA256:
        raise AssertionError("standalone V12 provenance profile SHA-256 drifted")
    if common.canonical_json_sha256(payload.get("profile")) != EXPECTED_PROFILE_SHA256:
        raise AssertionError("standalone V12 resolved profile payload drifted")
    if payload.get("registry_sha256") != EXPECTED_REGISTRY_SHA256:
        raise AssertionError("standalone V12 provenance registry binding drifted")
    if payload.get("resolved_config_sha256") != EXPECTED_RESOLVED_CONFIG_SHA256:
        raise AssertionError("standalone V12 provenance config binding drifted")
    if payload.get("input_binding_sha256") != EXPECTED_INPUT_BINDING_SHA256:
        raise AssertionError("standalone V12 provenance input binding drifted")
    if int(payload.get("selected_candidate_count", -1)) != EXPECTED_SELECTED_CANDIDATES:
        raise AssertionError("standalone V12 selected-candidate count drifted")
    if payload.get("research_only") is not True:
        raise AssertionError("standalone V12 provenance lost research-only status")
    if payload.get("promotion_eligible") is not False:
        raise AssertionError("standalone V12 provenance gained promotion eligibility")
    if payload.get("live_or_paper_authority") is not False:
        raise AssertionError("standalone V12 provenance gained live/paper authority")
    run_dir = Path(str(payload.get("run_dir", ""))).expanduser().resolve()
    if provenance_path.parent != run_dir:
        raise AssertionError("standalone V12 provenance run-directory binding drifted")
    inventory_binding = dict(payload.get("artifact_inventory", {}))
    inventory_path = Path(str(inventory_binding.get("path", ""))).resolve()
    if inventory_path != run_dir / "artifact_inventory.json":
        raise AssertionError("standalone V12 provenance inventory path drifted")
    if _sha256_file(inventory_path) != inventory_binding.get("sha256"):
        raise AssertionError("standalone V12 provenance inventory SHA-256 drifted")
    inventory_validation = _validate_inventory(run_dir, inventory_path)
    benchmark = dict(payload.get("benchmark_verification", {}))
    fingerprints = dict(payload.get("closed_trade_economic_fingerprints", {}))
    executed = tuple(payload.get("executed_scenarios", ()))
    allowed_scopes = {
        (EXPECTED_SCENARIOS[0][0],),
        tuple(scenario for scenario, _, _ in EXPECTED_SCENARIOS),
    }
    if executed not in allowed_scopes:
        raise AssertionError(
            f"standalone V12 executed-scenario scope drifted: {list(executed)}"
        )
    scenario_artifacts = dict(payload.get("scenario_artifacts", {}))
    for scenario in executed:
        if not bool(dict(benchmark.get(scenario, {})).get("verified")):
            raise AssertionError(f"standalone V12 benchmark is not verified: {scenario}")
        if fingerprints.get(scenario) != EXPECTED_CLOSED_TRADE_FINGERPRINTS[scenario]:
            raise AssertionError(f"standalone V12 trade fingerprint drifted: {scenario}")
        artifacts = dict(scenario_artifacts.get(scenario, {}))
        for key in ("closed_trades", "summary"):
            artifact = Path(str(artifacts.get(key, ""))).expanduser().resolve()
            if not artifact.is_relative_to(run_dir) or not artifact.is_file():
                raise FileNotFoundError(
                    f"standalone V12 {scenario} {key} artifact is missing: {artifact}"
                )
        observed_fingerprint = _closed_trade_economic_fingerprint(
            pd.read_csv(artifacts["closed_trades"])
        )
        if observed_fingerprint != EXPECTED_CLOSED_TRADE_FINGERPRINTS[scenario]:
            raise AssertionError(
                f"standalone V12 closed-trade economics changed: {scenario}"
            )
        summary = json.loads(Path(artifacts["summary"]).read_text(encoding="utf-8"))
        validate_full_usable_benchmark(summary, scenario)
    return {
        "validated": True,
        "run_dir": str(run_dir),
        "executed_scenarios": list(executed),
        "profile_sha256": EXPECTED_PROFILE_SHA256,
        "registry_sha256": EXPECTED_REGISTRY_SHA256,
        "resolved_config_sha256": EXPECTED_RESOLVED_CONFIG_SHA256,
        "input_binding_sha256": EXPECTED_INPUT_BINDING_SHA256,
        "artifact_inventory": inventory_validation,
    }


def _run(args: argparse.Namespace) -> Path:
    contract_validation = validate_fixed_contract(require_files=True)
    (
        all_candidates,
        minute_paths,
        segment_records,
        sessions,
        expected_span,
        missing_sessions,
    ) = v10_backtest._load_all_usable_max050_gap2_history()
    if len(all_candidates) != EXPECTED_ALL_CANDIDATES:
        raise AssertionError("standalone V12 all-candidate count drifted")
    if len(sessions) != EXPECTED_SESSION_COUNT:
        raise AssertionError("standalone V12 usable-session count drifted")

    experiment.configure_engine(locked_config.ACTIVE_VARIANT)
    engine._confirmation_check = experiment._NEUTRAL_CONFIRMATION_CHECK
    base_setups = tuple(engine.ACTIVE_SETUPS)
    prepared = v12_selection.prepare_variant_selection(
        all_candidates, base_setups, FIXED_CONFIG
    )
    if len(prepared.candidates) != EXPECTED_SELECTED_CANDIDATES:
        raise AssertionError("standalone V12 selected-candidate count drifted")
    if prepared.selection_metadata.input_candidate_count != EXPECTED_ALL_CANDIDATES:
        raise AssertionError("standalone V12 selection did not start from all candidates")
    if prepared.selection_metadata.resolved_config_sha256 != (
        EXPECTED_RESOLVED_CONFIG_SHA256
    ):
        raise AssertionError("standalone V12 selection config binding drifted")
    patch_fields = {
        (item.setup_id, item.field_name, float(item.new_value))
        for item in prepared.setup_patch_metadata.field_overrides
    }
    if patch_fields != {
        ("09:40_SHORT", "volume_ratio", 1.50),
        ("09:45_SHORT", "volume_ratio", 1.50),
    }:
        raise AssertionError(f"standalone V12 setup patches drifted: {patch_fields}")
    engine.ACTIVE_SETUPS = tuple(prepared.setups)
    runtime_spec = _runtime_spec(prepared)
    if not runtime_spec.is_neutral:
        raise AssertionError("standalone V12 execution overlay must remain neutral")

    input_binding = _input_binding_sha256(
        prepared.candidates, minute_paths, sessions, segment_records
    )
    if input_binding != EXPECTED_INPUT_BINDING_SHA256:
        raise AssertionError(
            "standalone V12 historical input binding drifted: "
            f"expected={EXPECTED_INPUT_BINDING_SHA256} observed={input_binding}"
        )

    output_root = args.output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(gaps.IST).strftime("%Y%m%dT%H%M%S%f%z")
    run_dir = (output_root / f"run_{stamp}").resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    source_records = _snapshot_sources(run_dir)

    common.atomic_write_csv(all_candidates, run_dir / "all_input_candidates.csv")
    common.atomic_write_csv(prepared.candidates, run_dir / "selected_candidates.csv")
    common.atomic_write_csv(prepared.decisions, run_dir / "selection_decisions.csv")
    common.atomic_write_json(
        run_dir / "resolved_profile.json",
        {
            "profile_sha256": EXPECTED_PROFILE_SHA256,
            "profile": _profile_payload(),
            "registry_sha256": EXPECTED_REGISTRY_SHA256,
            "resolved_config_sha256": EXPECTED_RESOLVED_CONFIG_SHA256,
            "selection_metadata": prepared.selection_metadata.payload(),
            "setup_patch_metadata": asdict(prepared.setup_patch_metadata),
            "runtime_spec": runtime_spec.payload(),
            "setups": [asdict(setup) for setup in prepared.setups],
        },
    )
    common.atomic_write_json(
        run_dir / "source_segments.json",
        {"schema_version": SCHEMA_VERSION, "segments": segment_records},
    )

    scenarios = EXPECTED_SCENARIOS[:1] if args.reference_only else EXPECTED_SCENARIOS
    metric_rows: list[dict[str, Any]] = []
    daywise_parts: list[pd.DataFrame] = []
    scenario_artifacts: dict[str, dict[str, str]] = {}
    benchmark_verification: dict[str, dict[str, Any]] = {}
    fingerprints: dict[str, str] = {}
    gap = _gap_spec()

    for scenario, cost_bps, slippage_bps in scenarios:
        print(
            f"[FNO-V12-LOCKED] scenario={scenario} "
            f"sessions={len(sessions)} candidates={len(prepared.candidates)}",
            flush=True,
        )
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
                with v11_gap.installed_gap_guard(gap):
                    audit = experiment._NEUTRAL_RUN_BACKTEST(
                        prepared.candidates,
                        minute_paths,
                        variant=PROFILE_ID,
                        policy=policy,
                        target_exposure_per_entry_rs=(
                            TARGET_EXPOSURE_PER_ENTRY_RS
                        ),
                    )
        audit = audit.copy()
        audit["v12_variant_id"] = PROFILE_ID
        audit["v12_stage_id"] = STAGE_ID
        audit["v12_family"] = FAMILY
        audit["v12_scenario"] = scenario
        audit["research_only"] = True
        audit["promotion_eligible"] = False

        rows, daily = _metric_rows(
            audit,
            sessions,
            segment_records,
            scenario=scenario,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
        )
        full_summary = next(row for row in rows if row["period"] == "FULL_USABLE")
        benchmark_verification[scenario] = validate_full_usable_benchmark(
            full_summary, scenario
        )
        metric_rows.extend(rows)
        daywise_parts.append(daily)
        artifacts = _write_scenario_outputs(
            run_dir, audit, daily, full_summary, scenario
        )
        fingerprint = _closed_trade_economic_fingerprint(
            pd.read_csv(artifacts["closed_trades"])
        )
        expected_fingerprint = EXPECTED_CLOSED_TRADE_FINGERPRINTS[scenario]
        if fingerprint != expected_fingerprint:
            raise AssertionError(
                f"locked V12 {scenario} trade fingerprint drifted: "
                f"expected={expected_fingerprint} observed={fingerprint}"
            )
        fingerprints[scenario] = fingerprint
        artifacts["closed_trade_economic_fingerprint_sha256"] = fingerprint
        scenario_artifacts[scenario] = artifacts
        print(
            "[FNO-V12-LOCKED] "
            f"{scenario}: fills={int(full_summary['fills'])} "
            f"WR={float(full_summary['win_rate_pct']):.2f}% "
            f"PF={float(full_summary['profit_factor']):.4f} "
            f"net={float(full_summary['net_return_points']):+.4f} "
            f"P&L=Rs {float(full_summary['net_pnl_rs']):+,.2f} "
            f"MDD={float(full_summary['max_daily_drawdown_points']):.4f}",
            flush=True,
        )

    metrics = pd.DataFrame(metric_rows)
    daywise = pd.concat(daywise_parts, ignore_index=True)
    metrics_path = run_dir / "all_period_metrics.csv"
    daywise_path = run_dir / "all_daywise.csv"
    benchmark_path = run_dir / "benchmark_verification.json"
    report_path = run_dir / "report.md"
    common.atomic_write_csv(metrics, metrics_path)
    common.atomic_write_csv(daywise, daywise_path)
    common.atomic_write_json(
        benchmark_path,
        {
            "schema_version": SCHEMA_VERSION,
            "verified": True,
            "benchmarks": benchmark_verification,
            "closed_trade_economic_fingerprints": fingerprints,
        },
    )
    common.atomic_write_text(
        report_path,
        _report(metrics, sessions=sessions, missing_sessions=missing_sessions),
    )

    end_binding = _input_binding_sha256(
        prepared.candidates, minute_paths, sessions, segment_records
    )
    if end_binding != input_binding:
        raise AssertionError("standalone V12 inputs mutated during execution")
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
        "fno_v12_backtest.py",
        "run",
        "--all-usable-history",
    ]
    if args.reference_only:
        command.append("--reference-only")
    if output_root != OUTPUT_ROOT.resolve():
        command.extend(["--output-root", str(output_root)])
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "created_at_ist": datetime.now(gaps.IST),
        "run_dir": str(run_dir),
        "command": command,
        "profile_id": PROFILE_ID,
        "profile_sha256": EXPECTED_PROFILE_SHA256,
        "profile": _profile_payload(),
        "registry_sha256": EXPECTED_REGISTRY_SHA256,
        "resolved_config_sha256": EXPECTED_RESOLVED_CONFIG_SHA256,
        "fixed_contract_validation": contract_validation,
        "input_binding_sha256": input_binding,
        "input_binding_rechecked_at_end": True,
        "all_input_candidate_count": len(all_candidates),
        "selected_candidate_count": len(prepared.candidates),
        "selection_metadata": prepared.selection_metadata.payload(),
        "setup_patch_metadata": asdict(prepared.setup_patch_metadata),
        "runtime_spec": runtime_spec.payload(),
        "usable_session_dates": [day.isoformat() for day in sessions],
        "usable_session_count": len(sessions),
        "calendar_span_session_count": len(expected_span),
        "missing_regular_session_dates": [day.isoformat() for day in missing_sessions],
        "source_segments": segment_records,
        "source_snapshots": source_records,
        "sources_rechecked_at_end": True,
        "executed_scenarios": [scenario for scenario, _, _ in scenarios],
        "scenario_artifacts": scenario_artifacts,
        "benchmark_verification": benchmark_verification,
        "closed_trade_economic_fingerprints": fingerprints,
        "outputs": {
            "metrics": str(metrics_path.resolve()),
            "daywise": str(daywise_path.resolve()),
            "benchmark_verification": str(benchmark_path.resolve()),
            "report": str(report_path.resolve()),
        },
        "artifact_inventory": {
            "path": str(inventory_path.resolve()),
            "sha256": _sha256_file(inventory_path),
            **inventory_validation,
        },
        "limitations": _profile_payload()["limitations"],
        "headline_valid": False,
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }
    common.atomic_write_json(provenance_path, gaps._json_ready(provenance))
    validation = validate_run_provenance(provenance_path)
    common.atomic_write_json(
        output_root / "latest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_dir": str(run_dir),
            "provenance_sha256": _sha256_file(provenance_path),
            "profile_sha256": EXPECTED_PROFILE_SHA256,
            "resolved_config_sha256": EXPECTED_RESOLVED_CONFIG_SHA256,
            "input_binding_sha256": EXPECTED_INPUT_BINDING_SHA256,
            "executed_scenarios": validation["executed_scenarios"],
            "research_only": True,
        },
    )
    print(f"[FNO-V12-LOCKED] complete: {run_dir}", flush=True)
    return run_dir


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fno_v12_backtest.py",
        description=(
            "Locked standalone full-history FNO V12 late-SHORT-volume backtester."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("profile", help="Print the immutable V12 profile.")
    run = commands.add_parser("run", help="Run the locked V12 backtest.")
    run.add_argument(
        "--all-usable-history",
        action="store_true",
        required=True,
        help="Acknowledge the pinned 65-session research-only history contract.",
    )
    run.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    run.add_argument(
        "--reference-only",
        action="store_true",
        help="Run only REFERENCE_15_0; default runs reference plus both stresses.",
    )
    validate = commands.add_parser(
        "validate", help="Validate a completed standalone V12 provenance."
    )
    validate.add_argument("--provenance", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.command == "profile":
        print(
            json.dumps(
                {
                    "profile_sha256": profile_sha256(),
                    "profile": _profile_payload(),
                    "expected_full_usable": EXPECTED_FULL_USABLE,
                    "expected_closed_trade_fingerprints": (
                        EXPECTED_CLOSED_TRADE_FINGERPRINTS
                    ),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "run":
        _run(args)
        return 0
    if args.command == "validate":
        print(
            json.dumps(
                validate_run_provenance(args.provenance),
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
