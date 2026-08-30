"""Locked standalone FNO V11 Stage-10 full-history backtester.

This is the direct, single-strategy entry point for the best observed V11
research configuration.  It does not import the staged experiment runner and
does not discover or rank variants at runtime.  The strategy is fixed to:

* frozen V10 Stage-7 mixed setup caps;
* 09:35 LONG five-minute move <= 0.50%;
* V11 strong-identity maximum 2 bps adverse-gap guard;
* 09:30 SHORT pending stop cannot fill before setup-relative minute S+3; and
* at most two concurrent same-side reservations for one symbol, while an
  opposite-side reservation for that symbol remains prohibited.

The historical inputs and expected economics are pinned to the attested
65-session research contract.  This file has backtest/research authority only.
It has no live- or paper-trading authority.
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

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v10_backtest as v10_backtest
import fno_v10_backtest_config as locked_config
import fno_v10_experiment_backtest as experiment
import fno_v10_followup_challenger_research as filters
import fno_v10_gap_guard_research as gaps
import fno_v11_execution_runtime as execution_runtime
import fno_v11_gap_runtime as gap_runtime
import fno_v8_windowed_1m_entry_backtest as engine


SCHEMA_VERSION = "fno_v11_stage10_locked_backtest_v1"
PROFILE_ID = "V11_S10_POST_HOC_TOP2_1436C7D363"
STAGE_ID = "STAGE_10_FIXED_STANDALONE"
FAMILY = "LOCKED_STAGE10_COMPOSITE"
DESCRIPTION = (
    "Locked standalone replay of V11 Stage10: 09:30 SHORT entry not before "
    "S+3 plus same-symbol/same-side maximum 2"
)
COMPONENT_VARIANT_IDS = (
    "V11_S9_SAME_SYMBOL_SAME_SIDE_MAX_2",
    "V11_S4_0930_SHORT_ENTRY_NOT_BEFORE_S3",
)
OUTPUT_ROOT = (
    common.FNO_ROOT / "strategy_research" / "v11_stage10_fixed_full_history_v1"
)
SELECTION_VARIANT = "0935_LONG_MOVE_MAX_050"
GAP_VARIANT = "MAX_2_BPS"
TARGET_EXPOSURE_PER_ENTRY_RS = 50_000.0
SQUARE_OFF = "15:30"
EOD_POLICY = "LAST_REAL_BAR_SENSITIVITY"
EXPECTED_SESSION_COUNT = 65
EXPECTED_SELECTED_CANDIDATES = 1134
EXPECTED_INPUT_BINDING_SHA256 = (
    "24e4da6c580693637bd7ce9c50c618b07d2e8a6a8dfded4498658d8eab113f2b"
)
EXPECTED_V10_PROFILE_ID = "V10_STAGE7_LOCKED_BACKTEST_20260827"
EXPECTED_V10_PROFILE_SHA256 = (
    "f2b3291903dfb1f2c95f1d24b63285d527dc7a9a6aa3d6334caed03d0834e59c"
)
EXPECTED_SETUP_BOOK_SHA256 = (
    "ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675"
)
ORIGIN_EXPERIMENT_PAYLOAD_SHA256 = (
    "62386e073c7a758d93b654aee881a68f334336747c528010fac1395072a79bcb"
)
LOCKED_PROFILE_SHA256 = (
    "8dfc162701705c0daa89d7ba2faa8dd7ddd3ff8eb6605370d96de1fdaa1f6fe1"
)

FIXED_RUNTIME_SPEC = execution_runtime.RuntimeSpec(
    entry_setup_id="09:30_SHORT",
    entry_not_before_minute=3,
    exit_rule=None,
    exit_activation_r=None,
    same_side_symbol_limit=2,
)

EXPECTED_SCENARIOS: tuple[tuple[str, float, float], ...] = (
    ("REFERENCE_15_0", 15.0, 0.0),
    ("STRESS_20_2", 20.0, 2.0),
    ("STRESS_25_5", 25.0, 5.0),
)

EXPECTED_FULL_USABLE: dict[str, dict[str, object]] = {
    "REFERENCE_15_0": {
        "sessions": 65,
        "candidates": 1134,
        "fills": 237,
        "wins": 123,
        "losses": 114,
        "flat_trades": 0,
        "win_rate_pct": 51.89873417721519,
        "profit_factor": 2.1451722486639957,
        "net_return_points": 94.63085984175197,
        "net_pnl_rs": 46_783.22802111076,
        "max_daily_drawdown_points": 8.56737490149464,
        "positive_days": 37,
        "negative_days": 25,
        "flat_days": 3,
        "remaining_gap_fills": 24,
        "guard_rejections": 24,
        "data_incomplete_candidates": 0,
    },
    "STRESS_20_2": {
        "sessions": 65,
        "candidates": 1134,
        "fills": 237,
        "wins": 119,
        "losses": 118,
        "flat_trades": 0,
        "win_rate_pct": 50.210970464135016,
        "profit_factor": 1.8627004050003626,
        "net_return_points": 77.87761983466613,
        "net_pnl_rs": 38_681.91875137453,
        "max_daily_drawdown_points": 9.870817960114385,
        "positive_days": 34,
        "negative_days": 28,
        "flat_days": 3,
        "remaining_gap_fills": 24,
        "guard_rejections": 24,
        "data_incomplete_candidates": 0,
    },
    "STRESS_25_5": {
        "sessions": 65,
        "candidates": 1134,
        "fills": 237,
        "wins": 114,
        "losses": 123,
        "flat_trades": 0,
        "win_rate_pct": 48.10126582278481,
        "profit_factor": 1.5657168272703743,
        "net_return_points": 56.81711568642697,
        "net_pnl_rs": 28_481.757727453645,
        "max_daily_drawdown_points": 11.034340487111521,
        "positive_days": 32,
        "negative_days": 30,
        "flat_days": 3,
        "remaining_gap_fills": 24,
        "guard_rejections": 24,
        "data_incomplete_candidates": 0,
    },
}

EXPECTED_CLOSED_TRADE_FINGERPRINTS = {
    "REFERENCE_15_0": (
        "f171f7741aad48b7b50d634a8a63ef5b3c070669ea910818a6a569ed143d9833"
    ),
    "STRESS_20_2": (
        "cc85008deeefcf9085daa0aa16743367662bf0b8999c586a11bfd89ca656eabe"
    ),
    "STRESS_25_5": (
        "c352beb835ae4035bee53a6d1c6854108055a726cce0ca83802dda885ad21afb"
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
        "component_variant_ids": list(COMPONENT_VARIANT_IDS),
        "post_hoc_origin": True,
        "origin_experiment_payload_sha256": ORIGIN_EXPERIMENT_PAYLOAD_SHA256,
        "base_v10_profile": {
            "profile_id": EXPECTED_V10_PROFILE_ID,
            "profile_sha256": EXPECTED_V10_PROFILE_SHA256,
            "active_variant": locked_config.ACTIVE_VARIANT,
            "setup_book_sha256": EXPECTED_SETUP_BOOK_SHA256,
            "selection_variant": SELECTION_VARIANT,
        },
        "runtime_spec": asdict(FIXED_RUNTIME_SPEC),
        "gap_guard": {
            "variant": GAP_VARIANT,
            "identity_policy": gap_runtime.IDENTITY_POLICY,
            "runtime_schema_version": gap_runtime.RUNTIME_SCHEMA_VERSION,
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
            "selected_candidates": EXPECTED_SELECTED_CANDIDATES,
            "input_binding_sha256": EXPECTED_INPUT_BINDING_SHA256,
            "missing_regular_session_dates": ["2026-08-26"],
        },
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
    return next(spec for spec in gaps.GAP_GUARDS if spec.variant == GAP_VARIANT)


def validate_fixed_contract(*, require_files: bool = True) -> dict[str, Any]:
    """Fail closed if any fixed Stage-10 rule or historical binding drifts."""

    locked_config.validate_locked_profile()
    v10_backtest.validate_max050_gap2_contract(require_files=require_files)
    filters.validate_registry()
    if locked_config.PROFILE_ID != EXPECTED_V10_PROFILE_ID:
        raise AssertionError("locked V10 profile ID drifted")
    if locked_config.profile_sha256() != EXPECTED_V10_PROFILE_SHA256:
        raise AssertionError("locked V10 profile SHA-256 drifted")
    if v10_backtest.EXPECTED_V10_SETUP_BOOK_SHA256 != EXPECTED_SETUP_BOOK_SHA256:
        raise AssertionError("locked V10 setup-book SHA-256 drifted")
    if locked_config.ACTIVE_VARIANT != "0940_LONG_MOVE_040":
        raise AssertionError("locked V10 active variant drifted")

    selection = filters.SPEC_BY_NAME[SELECTION_VARIANT]
    if selection.move_0935_long_max != 0.50:
        raise AssertionError("V11 Stage10 must retain 09:35 LONG move <= 0.50%")
    gap = _gap_spec()
    if gap.max_adverse_gap_bps != 2.0 or gap.reject_all_gap_fills:
        raise AssertionError("V11 Stage10 must retain the maximum 2 bps gap guard")
    if gap_runtime.IDENTITY_POLICY != "STRONG_REFERENCE_AND_IS_CHECK":
        raise AssertionError("V11 Stage10 requires the strong-reference gap guard")

    FIXED_RUNTIME_SPEC.validate(allow_composite=True)
    if asdict(FIXED_RUNTIME_SPEC) != {
        "entry_setup_id": "09:30_SHORT",
        "entry_not_before_minute": 3,
        "exit_rule": None,
        "exit_activation_r": None,
        "same_side_symbol_limit": 2,
    }:
        raise AssertionError("fixed V11 Stage10 runtime spec drifted")
    observed_scenarios = tuple(
        (str(name), float(cost), float(slippage))
        for name, cost, slippage in gaps.COST_SCENARIOS
    )
    if observed_scenarios != EXPECTED_SCENARIOS:
        raise AssertionError("V11 Stage10 cost scenarios drifted")
    observed_profile_sha256 = profile_sha256()
    if observed_profile_sha256 != LOCKED_PROFILE_SHA256:
        raise AssertionError(
            "locked standalone V11 profile SHA-256 drifted: "
            f"expected={LOCKED_PROFILE_SHA256} observed={observed_profile_sha256}"
        )
    return {
        "validated": True,
        "profile_id": PROFILE_ID,
        "profile_sha256": observed_profile_sha256,
        "v10_profile_sha256": locked_config.profile_sha256(),
        "setup_book_sha256": EXPECTED_SETUP_BOOK_SHA256,
        "gap_identity_policy": gap_runtime.IDENTITY_POLICY,
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
                "selection_variant": SELECTION_VARIANT,
                "gap_variant": GAP_VARIANT,
                "post_hoc_origin": True,
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
            f"locked V11 Stage10 {scenario} benchmark drifted: "
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
        Path(execution_runtime.__file__).resolve(),
        Path(gap_runtime.__file__).resolve(),
        Path(v10_backtest.__file__).resolve(),
        Path(locked_config.__file__).resolve(),
        Path(filters.__file__).resolve(),
        Path(gaps.__file__).resolve(),
        Path(experiment.__file__).resolve(),
        Path(engine.__file__).resolve(),
        Path(common.__file__).resolve(),
        module_dir / "fno_v10_experiment_config.py",
        module_dir / "fno_v10_unified_5m_1m_backtest.py",
        module_dir / "fno_oi_backtest_provenance.py",
        module_dir / "fno_oi_hybrid_data.py",
        module_dir / "eqidv2_runtime_paths.py",
    )
    if len({path.name for path in sources}) != len(sources):
        raise AssertionError("standalone source snapshot names are not unique")
    missing = [path for path in sources if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"standalone source files are missing: {missing}")
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
        observed = _sha256_file(path)
        if observed != record["sha256"]:
            raise AssertionError(f"source changed during standalone run: {name}")


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
    full = metrics.loc[metrics["period"].eq("FULL_USABLE")].copy()
    full = full.set_index("scenario")
    reference = full.loc["REFERENCE_15_0"]
    v10_reference_points = float(
        v10_backtest.MAX050_GAP2_CURRENT_MIXED_BENCHMARK["net_return_points"]
    )
    improvement_pct = (
        float(reference["net_return_points"]) / v10_reference_points - 1.0
    ) * 100.0
    lines = [
        "# Locked standalone FNO V11 Stage-10 backtest",
        "",
        f"Profile: `{PROFILE_ID}`",
        f"Profile SHA-256: `{profile_sha256()}`",
        f"Usable sessions: {len(sessions)} ({min(sessions)} through {max(sessions)})",
        "Missing regular sessions inside the span: "
        + (", ".join(map(str, missing_sessions)) or "none"),
        "",
        "Fixed mechanisms:",
        "",
        "- V10 Stage-7 mixed-cap base with 09:35 LONG move <= 0.50%.",
        "- Strong-reference V11 maximum 2 bps adverse-gap guard.",
        "- 09:30 SHORT cannot fill before setup-relative minute S+3.",
        "- Same symbol/same side maximum 2; opposite side remains prohibited.",
        "",
        "| Scenario | Fills | W-L | WR | PF | Net points | Net P&L | MDD |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for scenario, _, _ in EXPECTED_SCENARIOS:
        if scenario not in full.index:
            continue
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
            "Reference net-points improvement versus frozen V10 Stage0: "
            f"**{improvement_pct:+.2f}%**.",
            "",
            "This is the locked reproduction of the strongest observed post-hoc "
            "research result. It remains research-only and is not independently "
            "validated for live promotion.",
            "",
        ]
    )
    return "\n".join(lines)


def _validate_inventory(
    run_dir: Path, inventory_path: Path
) -> dict[str, Any]:
    payload = json.loads(inventory_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise AssertionError("standalone artifact inventory schema drifted")
    records = list(payload.get("artifacts", ()))
    if not records:
        raise AssertionError("standalone artifact inventory is empty")
    listed: set[str] = set()
    for record in records:
        relative = Path(str(record["relative_path"]))
        relative_text = str(relative).replace("\\", "/")
        if relative.is_absolute() or relative_text in listed:
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
            "standalone artifact inventory set is incomplete: "
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
        raise AssertionError("standalone provenance schema drifted")
    if payload.get("complete") is not True:
        raise AssertionError("standalone run is incomplete")
    if payload.get("profile_id") != PROFILE_ID:
        raise AssertionError("standalone provenance profile ID drifted")
    if payload.get("profile_sha256") != LOCKED_PROFILE_SHA256:
        raise AssertionError("standalone provenance profile SHA-256 drifted")
    if common.canonical_json_sha256(payload.get("profile")) != LOCKED_PROFILE_SHA256:
        raise AssertionError("standalone resolved profile payload drifted")
    if payload.get("input_binding_sha256") != EXPECTED_INPUT_BINDING_SHA256:
        raise AssertionError("standalone provenance input binding drifted")
    if payload.get("research_only") is not True:
        raise AssertionError("standalone provenance lost research-only status")
    if payload.get("promotion_eligible") is not False:
        raise AssertionError("standalone provenance gained promotion eligibility")
    if payload.get("live_or_paper_authority") is not False:
        raise AssertionError("standalone provenance gained live/paper authority")
    run_dir = Path(str(payload.get("run_dir", ""))).expanduser().resolve()
    if provenance_path.parent != run_dir:
        raise AssertionError("standalone provenance run-directory binding drifted")
    inventory_binding = dict(payload.get("artifact_inventory", {}))
    inventory_path = Path(str(inventory_binding.get("path", ""))).resolve()
    if inventory_path != run_dir / "artifact_inventory.json":
        raise AssertionError("standalone provenance inventory path drifted")
    if _sha256_file(inventory_path) != inventory_binding.get("sha256"):
        raise AssertionError("standalone provenance inventory SHA-256 drifted")
    inventory_validation = _validate_inventory(run_dir, inventory_path)
    benchmark = dict(payload.get("benchmark_verification", {}))
    fingerprints = dict(payload.get("closed_trade_economic_fingerprints", {}))
    executed = tuple(payload.get("executed_scenarios", ()))
    allowed_execution_scopes = {
        (EXPECTED_SCENARIOS[0][0],),
        tuple(scenario for scenario, _, _ in EXPECTED_SCENARIOS),
    }
    if executed not in allowed_execution_scopes:
        raise AssertionError(
            f"standalone executed-scenario scope drifted: {list(executed)}"
        )
    scenario_artifacts = dict(payload.get("scenario_artifacts", {}))
    for scenario in executed:
        if not bool(dict(benchmark.get(scenario, {})).get("verified")):
            raise AssertionError(f"standalone benchmark is not verified: {scenario}")
        if fingerprints.get(scenario) != EXPECTED_CLOSED_TRADE_FINGERPRINTS[scenario]:
            raise AssertionError(f"standalone trade fingerprint drifted: {scenario}")
        artifacts = dict(scenario_artifacts.get(scenario, {}))
        for key in ("closed_trades", "summary"):
            artifact_path = Path(str(artifacts.get(key, ""))).expanduser().resolve()
            if not artifact_path.is_relative_to(run_dir) or not artifact_path.is_file():
                raise FileNotFoundError(
                    f"standalone {scenario} {key} artifact is missing: {artifact_path}"
                )
        observed_fingerprint = _closed_trade_economic_fingerprint(
            pd.read_csv(artifacts["closed_trades"])
        )
        if observed_fingerprint != EXPECTED_CLOSED_TRADE_FINGERPRINTS[scenario]:
            raise AssertionError(
                f"standalone closed-trade economics changed: {scenario}"
            )
        observed_summary = json.loads(
            Path(artifacts["summary"]).read_text(encoding="utf-8")
        )
        validate_full_usable_benchmark(observed_summary, scenario)
    return {
        "validated": True,
        "run_dir": str(run_dir),
        "executed_scenarios": list(executed),
        "profile_sha256": LOCKED_PROFILE_SHA256,
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
    selection_spec = filters.SPEC_BY_NAME[SELECTION_VARIANT]
    selected, decisions = filters.selection_overlay(all_candidates, selection_spec)
    if len(sessions) != EXPECTED_SESSION_COUNT:
        raise AssertionError("standalone V11 usable-session count drifted")
    if len(selected) != EXPECTED_SELECTED_CANDIDATES:
        raise AssertionError("standalone V11 selected-candidate count drifted")
    input_binding = _input_binding_sha256(
        selected, minute_paths, sessions, segment_records
    )
    if input_binding != EXPECTED_INPUT_BINDING_SHA256:
        raise AssertionError(
            "standalone V11 historical input binding drifted: "
            f"expected={EXPECTED_INPUT_BINDING_SHA256} observed={input_binding}"
        )

    output_root = args.output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(gaps.IST).strftime("%Y%m%dT%H%M%S%f%z")
    run_dir = (output_root / f"run_{stamp}").resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    source_records = _snapshot_sources(run_dir)

    common.atomic_write_csv(all_candidates, run_dir / "all_input_candidates.csv")
    common.atomic_write_csv(selected, run_dir / "selected_candidates.csv")
    common.atomic_write_csv(decisions, run_dir / "selection_decisions.csv")
    common.atomic_write_json(
        run_dir / "resolved_profile.json",
        {"profile_sha256": LOCKED_PROFILE_SHA256, "profile": _profile_payload()},
    )
    common.atomic_write_json(
        run_dir / "source_segments.json",
        {"schema_version": SCHEMA_VERSION, "segments": segment_records},
    )

    experiment.configure_engine(locked_config.ACTIVE_VARIANT)
    engine._confirmation_check = experiment._NEUTRAL_CONFIRMATION_CHECK
    gap = _gap_spec()
    scenarios = EXPECTED_SCENARIOS[:1] if args.reference_only else EXPECTED_SCENARIOS
    metric_rows: list[dict[str, Any]] = []
    daywise_parts: list[pd.DataFrame] = []
    scenario_artifacts: dict[str, dict[str, str]] = {}
    benchmark_verification: dict[str, dict[str, Any]] = {}
    fingerprints: dict[str, str] = {}

    for scenario, cost_bps, slippage_bps in scenarios:
        print(
            f"[FNO-V11-STAGE10] scenario={scenario} "
            f"sessions={len(sessions)} candidates={len(selected)}",
            flush=True,
        )
        policy = experiment._entry_policy_for_variant(
            locked_config.ACTIVE_VARIANT,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
            square_off=SQUARE_OFF,
            eod_policy=EOD_POLICY,
        )
        with execution_runtime.installed_runtime_hooks(
            FIXED_RUNTIME_SPEC, allow_composite=True
        ):
            with gap_runtime.installed_gap_guard(gap):
                audit = experiment._NEUTRAL_RUN_BACKTEST(
                    selected,
                    minute_paths,
                    variant=PROFILE_ID,
                    policy=policy,
                    target_exposure_per_entry_rs=TARGET_EXPOSURE_PER_ENTRY_RS,
                )
        audit = audit.copy()
        audit["v11_variant_id"] = PROFILE_ID
        audit["v11_stage_id"] = STAGE_ID
        audit["v11_family"] = FAMILY
        audit["v11_scenario"] = scenario
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
        trades = pd.read_csv(artifacts["closed_trades"])
        fingerprint = _closed_trade_economic_fingerprint(trades)
        expected_fingerprint = EXPECTED_CLOSED_TRADE_FINGERPRINTS[scenario]
        if fingerprint != expected_fingerprint:
            raise AssertionError(
                f"locked V11 Stage10 {scenario} trade fingerprint drifted: "
                f"expected={expected_fingerprint} observed={fingerprint}"
            )
        fingerprints[scenario] = fingerprint
        artifacts["closed_trade_economic_fingerprint_sha256"] = fingerprint
        scenario_artifacts[scenario] = artifacts
        print(
            "[FNO-V11-STAGE10] "
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
        selected, minute_paths, sessions, segment_records
    )
    if end_binding != input_binding:
        raise AssertionError("standalone V11 inputs mutated during execution")
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
        str(Path(__file__).resolve()),
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
        "profile_sha256": LOCKED_PROFILE_SHA256,
        "profile": _profile_payload(),
        "fixed_contract_validation": contract_validation,
        "input_binding_sha256": input_binding,
        "input_binding_rechecked_at_end": True,
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
        "limitations": [
            "POST_HOC_CONFIGURATION_REQUIRES_PROSPECTIVE_VALIDATION",
            "SOURCE_SLOT_COVERAGE_INCOMPLETE",
            "LAST_REAL_BAR_SENSITIVITY_NOT_HEADLINE",
            "STATIC_CONTRACT_UNIVERSES_BY_SEGMENT",
            "2026_08_26_HAS_NO_VALIDATED_CACHE",
            "CASH_EQUITY_PATHS_NOT_ACTUAL_ROLLING_FUTURES_EXECUTION",
        ],
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
            "profile_sha256": LOCKED_PROFILE_SHA256,
            "input_binding_sha256": EXPECTED_INPUT_BINDING_SHA256,
            "executed_scenarios": validation["executed_scenarios"],
            "research_only": True,
        },
    )
    print(f"[FNO-V11-STAGE10] complete: {run_dir}", flush=True)
    return run_dir


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fno_v11_backtest.py",
        description="Locked standalone full-history FNO V11 Stage-10 backtester.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("profile", help="Print the immutable Stage-10 profile.")
    run = commands.add_parser("run", help="Run the locked Stage-10 backtest.")
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
        "validate", help="Validate a completed standalone run provenance."
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
