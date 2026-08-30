"""Canonical full-history backtest launcher for FNO V10 research profiles.

``run`` remains the immutable Stage-7 baseline. ``max050-gap2`` is the
standalone full-history front door for Stage 7 plus the 09:35 LONG <= 0.50%
selection ceiling and the maximum 2 bps adverse entry-gap guard.  The latter
replays every non-overlapping, source-bound historical segment currently
available and records calendar gaps instead of treating missing sessions as
flat days.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import sys
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v10_backtest_config as locked_config
import fno_v10_experiment_backtest as experiment
import fno_v10_followup_challenger_research as filters
import fno_v10_gap_guard_research as gaps
import fno_v8_windowed_1m_entry_backtest as engine


REPLAY_COMMANDS = frozenset({"run", "smoke"})
PASSTHROUGH_COMMANDS = frozenset({"build-cache"})
LOCKED_RUN_SCHEMA_VERSION = "fno_v10_stage7_locked_backtest_run_v1"
MAX050_GAP2_SCHEMA_VERSION = "fno_v10_max050_gap2_full_history_v1"
MAX050_GAP2_PROFILE_ID = "V10_STAGE7_0935_LONG_MAX_050_GAP2"
MAX050_GAP2_SELECTION_VARIANT = "0935_LONG_MOVE_MAX_050"
MAX050_GAP2_GAP_VARIANT = "MAX_2_BPS"
MAX050_GAP2_EXTENSION_DAY = date(2026, 8, 20)
MAX050_GAP2_OUTPUT_ROOT = (
    common.FNO_ROOT
    / "strategy_research"
    / "v10_max050_gap2_full_history_v1"
)
MAX050_GAP2_CAP_SWEEP_SCHEMA_VERSION = "fno_v10_max050_gap2_cap_sweep_v1"
MAX050_GAP2_CAP_SWEEP_OUTPUT_ROOT = (
    common.FNO_ROOT
    / "strategy_research"
    / "v10_max050_gap2_cap_sweep_v1"
)
MAX050_GAP2_CAP_SWEEP_VALUES = (1, 2, 3, 4, 5)
MAX050_GAP2_BASELINE_PROFILE_ID = "CURRENT_MIXED_BASELINE"
EXPECTED_V10_SETUP_BOOK_SHA256 = (
    "ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675"
)
MAX050_GAP2_CURRENT_MIXED_BENCHMARK: dict[str, object] = {
    "dataset": "ALL_USABLE_HISTORY",
    "period": "FULL_USABLE",
    "scenario": "REFERENCE_15_0",
    "sessions": 65,
    "candidates": 1134,
    "fills": 232,
    "wins": 116,
    "losses": 116,
    "flat_trades": 0,
    "win_rate_pct": 50.0,
    "profit_factor": 1.8327310411717306,
    "net_return_points": 73.05442256172977,
    "net_pnl_rs": 36_312.05263290276,
    "max_daily_drawdown_points": 9.351281246312235,
    "positive_days": 37,
    "negative_days": 25,
    "flat_days": 3,
    "remaining_gap_fills": 24,
    "guard_rejections": 14,
    "data_incomplete_candidates": 0,
}
MAX050_GAP2_BENCHMARK_FLOAT_FIELDS = frozenset(
    {
        "win_rate_pct",
        "profit_factor",
        "net_return_points",
        "net_pnl_rs",
        "max_daily_drawdown_points",
    }
)
MAX050_GAP2_BENCHMARK_ABS_TOLERANCE = 1e-9


@dataclass(frozen=True)
class HistoricalSegment:
    segment_id: str
    cache_manifest: Path
    from_day: date
    through_day: date
    contract_month_filter: str

    def payload(self) -> dict[str, Any]:
        return asdict(self)


MAX050_GAP2_USABLE_SEGMENTS: tuple[HistoricalSegment, ...] = (
    HistoricalSegment(
        "AUG_CORE_59",
        Path(
            r"C:\TradingData\eqidv2\fno_oi\strategy_research"
            r"\v10_repaired_snapshot_reruns_20260827_v1\caches"
            r"\historical_59_sessions\64744f54dbfb5f1a\manifest.json"
        ),
        date(2026, 5, 27),
        date(2026, 8, 19),
        "26AUG",
    ),
    HistoricalSegment(
        "AUG_EXTENSION_20_21",
        Path(
            r"C:\TradingData\eqidv2\fno_oi\strategy_research"
            r"\v10_unified_5m_1m_v1\cache\ad5d9c3c1c68751c\manifest.json"
        ),
        date(2026, 8, 20),
        date(2026, 8, 21),
        "26AUG",
    ),
    HistoricalSegment(
        "SEP_ROLLOVER_24_25",
        Path(
            r"C:\TradingData\eqidv2\fno_oi\strategy_research"
            r"\v10_unified_5m_1m_v1\rollover_diagnostic\cache"
            r"\586e53c8cdd53098\manifest.json"
        ),
        date(2026, 8, 24),
        date(2026, 8, 25),
        "26SEP",
    ),
    HistoricalSegment(
        "SEP_DIAGNOSTIC_27",
        Path(
            r"C:\TradingData\eqidv2\fno_oi\strategy_research"
            r"\v10_repaired_snapshot_reruns_20260827_v1\caches"
            r"\today_2026_08_27_sep\4f6678c068fa1bfb\manifest.json"
        ),
        date(2026, 8, 27),
        date(2026, 8, 27),
        "26SEP",
    ),
    HistoricalSegment(
        "SEP_DIAGNOSTIC_28",
        Path(
            r"C:\TradingData\eqidv2\fno_oi\strategy_research"
            r"\today_six_strategy_replays_v1"
            r"\today_2026-08-28_20260828T180729448015+0530"
            r"\v10_cache\75ae1eb8013c86f0\manifest.json"
        ),
        date(2026, 8, 28),
        date(2026, 8, 28),
        "26SEP",
    ),
)

_ORIGINAL_EXPERIMENT_PROVENANCE_BUILDER = experiment._build_run_provenance
_ORIGINAL_EXPERIMENT_PROVENANCE_VALIDATOR = (
    experiment.validate_experiment_run_provenance
)


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def launcher_source_sha256() -> str:
    return _sha256_file(Path(__file__))


def config_source_sha256() -> str:
    return _sha256_file(Path(locked_config.__file__))


def _option_values(args: Sequence[str], option: str) -> list[str]:
    values: list[str] = []
    index = 0
    while index < len(args):
        value = str(args[index])
        if value == option:
            if index + 1 >= len(args) or str(args[index + 1]).startswith("--"):
                raise ValueError(f"{option} requires exactly one value")
            values.append(str(args[index + 1]).strip())
            index += 2
            continue
        if value.startswith(f"{option}="):
            selected = value.split("=", 1)[1].strip()
            if not selected:
                raise ValueError(f"{option} requires exactly one value")
            values.append(selected)
        index += 1
    return values


def _has_option(args: Sequence[str], option: str) -> bool:
    return any(
        str(value) == option or str(value).startswith(f"{option}=")
        for value in args
    )


def _same_path(observed: str, expected: object) -> bool:
    return Path(observed).expanduser().resolve() == Path(str(expected)).resolve()


def _same_float(observed: str, expected: object) -> bool:
    try:
        return math.isclose(
            float(observed), float(expected), rel_tol=0.0, abs_tol=1e-12
        )
    except (TypeError, ValueError):
        return False


def _same_text(observed: str, expected: object) -> bool:
    return str(observed).strip() == str(expected)


def _lock_option(
    args: list[str],
    option: str,
    expected: object,
    comparator: Callable[[str, object], bool] = _same_text,
) -> None:
    values = _option_values(args, option)
    if len(values) > 1:
        raise ValueError(f"The locked V10 backtester accepts one {option} only")
    if values and not comparator(values[0], expected):
        raise ValueError(
            f"The locked V10 contract requires {option}={expected}; "
            f"received {values[0]}"
        )
    if not values:
        args.extend([option, str(expected)])


def _explicit_variants(args: Sequence[str]) -> list[str]:
    return [value.upper().strip() for value in _option_values(args, "--variant")]


def _inject_locked_variant(argv: Sequence[str]) -> list[str]:
    args = list(argv)
    if not args or args[0] not in REPLAY_COMMANDS:
        return args
    if {"-h", "--help"} & set(args[1:]):
        return args
    variants = _explicit_variants(args[1:])
    if len(variants) > 1:
        raise ValueError("The locked V10 backtester accepts one --variant only")
    if variants and variants[0] != locked_config.ACTIVE_VARIANT:
        raise ValueError(
            "The canonical V10 backtester is locked to "
            f"{locked_config.ACTIVE_VARIANT}; received {variants[0]}"
        )
    if not variants:
        args.extend(["--variant", locked_config.ACTIVE_VARIANT])
    return args


def _inject_locked_run_contract(argv: Sequence[str]) -> list[str]:
    args = _inject_locked_variant(argv)
    if not args or {"-h", "--help"} & set(args[1:]):
        return args
    command = args[0]
    if command not in REPLAY_COMMANDS | PASSTHROUGH_COMMANDS:
        return args
    for forbidden in ("--symbols", "--no-write"):
        if _has_option(args[1:], forbidden):
            raise ValueError(
                f"{forbidden} is not allowed by the full-universe locked profile"
            )
    contract = locked_config.locked_profile_payload()[
        "extended_stored_history_replay"
    ]
    _lock_option(
        args,
        "--source-snapshot",
        contract["source_snapshot_manifest"],
        _same_path,
    )
    if command in {"run", "build-cache"}:
        _lock_option(args, "--from-day", contract["from_day"])
        _lock_option(args, "--through-day", contract["through_day"])
    if command in REPLAY_COMMANDS:
        _lock_option(args, "--cost-bps", contract["cost_bps"], _same_float)
        _lock_option(
            args, "--slippage-bps", contract["slippage_bps"], _same_float
        )
        _lock_option(args, "--square-off", contract["square_off"])
        _lock_option(args, "--eod-policy", contract["eod_policy"])
        _lock_option(args, "--split-day", contract["split_day"])
    return args


def _provenance_argument(args: Sequence[str]) -> Path:
    values = _option_values(args, "--provenance")
    if len(values) != 1:
        raise ValueError("validate requires exactly one --provenance PATH")
    return Path(values[0]).expanduser().resolve()


def _validate_locked_provenance_target(args: Sequence[str]) -> dict[str, Any]:
    path = _provenance_argument(args)
    if not path.is_file():
        raise FileNotFoundError(f"V10 provenance does not exist: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    variant = str(
        payload.get("v10_experiment_variant")
        or dict(payload.get("parameters", {})).get("variant", "")
    ).upper().strip()
    if variant != locked_config.ACTIVE_VARIANT:
        raise ValueError(
            "The canonical V10 validator accepts only locked Stage 7 "
            f"provenance; observed {variant or 'MISSING'}"
        )
    observed_hash = str(
        payload.get("v10_experiment_variant_config_sha256", "")
    )
    if observed_hash != locked_config.EXPECTED_VARIANT_CONFIG_SHA256:
        raise ValueError("Locked Stage 7 provenance has a different config hash")
    if payload.get("v10_locked_backtest_profile_sha256") != (
        locked_config.EXPECTED_PROFILE_SHA256
    ):
        raise ValueError("Stage 7 provenance is not bound to the locked profile")
    return payload


def _profile_document() -> dict[str, Any]:
    return {
        "profile_sha256": locked_config.profile_sha256(),
        "profile": locked_config.locked_profile_payload(),
    }


def _build_locked_run_provenance(**kwargs: Any) -> dict[str, Any]:
    output_paths = dict(kwargs.get("output_paths", {}))
    engine_archive = Path(str(output_paths["strategy_source_archive"]))
    run_dir = engine_archive.parent
    launcher_archive = run_dir / Path(__file__).name
    config_archive = run_dir / Path(locked_config.__file__).name
    profile_archive = run_dir / "resolved_v10_stage7_locked_profile.json"
    launcher_hash = launcher_source_sha256()
    config_hash = config_source_sha256()
    experiment.engine.provenance.publish_immutable_copy(
        Path(__file__), launcher_archive, expected_sha256=launcher_hash
    )
    experiment.engine.provenance.publish_immutable_copy(
        Path(locked_config.__file__),
        config_archive,
        expected_sha256=config_hash,
    )
    common.atomic_write_json(profile_archive, locked_config.locked_profile_payload())
    output_paths.update(
        {
            "locked_backtest_launcher_source_archive": launcher_archive,
            "locked_backtest_config_source_archive": config_archive,
            "resolved_locked_backtest_profile": profile_archive,
        }
    )
    forwarded = dict(kwargs)
    forwarded["output_paths"] = output_paths
    payload = _ORIGINAL_EXPERIMENT_PROVENANCE_BUILDER(**forwarded)
    payload.update(
        {
            "v10_locked_backtest_run_schema_version": LOCKED_RUN_SCHEMA_VERSION,
            "v10_locked_backtest_profile_id": locked_config.PROFILE_ID,
            "v10_locked_backtest_profile_sha256": locked_config.profile_sha256(),
            "v10_locked_backtest_profile": locked_config.locked_profile_payload(),
            "v10_locked_backtest_authority": "BACKTEST_ONLY",
            "locked_backtest_launcher_source_sha256": launcher_hash,
            "locked_backtest_config_source_sha256": config_hash,
            "research_only": True,
            "promotion_eligible": False,
        }
    )
    return payload


def _require_equal(observed: object, expected: object, label: str) -> None:
    if observed != expected:
        raise AssertionError(
            f"Locked V10 provenance {label} changed: "
            f"expected {expected!r}, observed {observed!r}"
        )


def _require_locked_profile_payload(observed: object, label: str) -> None:
    """Compare a profile after JSON normalization (tuples serialize as lists)."""

    _require_equal(
        common.canonical_json_sha256(observed),
        locked_config.profile_sha256(),
        label,
    )


def _validate_full_history_contract(payload: Mapping[str, Any]) -> None:
    contract = locked_config.locked_profile_payload()[
        "extended_stored_history_replay"
    ]
    parameters = dict(payload.get("parameters", {}))
    entry = dict(parameters.get("entry_policy", {}))
    window = dict(payload.get("backtest_window", {}))
    source = dict(payload.get("source_snapshot", {}))
    universe = dict(payload.get("universe", {}))
    results = dict(payload.get("results", {}))
    inventory = dict(payload.get("source_inventory", {}))

    _require_equal(window.get("from_day"), contract["from_day"], "from_day")
    _require_equal(
        window.get("through_day"), contract["through_day"], "through_day"
    )
    _require_equal(window.get("split_day"), contract["split_day"], "split_day")
    _require_equal(parameters.get("variant"), locked_config.ACTIVE_VARIANT, "variant")
    _require_equal(float(entry.get("cost_bps")), contract["cost_bps"], "cost_bps")
    _require_equal(
        float(entry.get("slippage_bps")),
        contract["slippage_bps"],
        "slippage_bps",
    )
    _require_equal(entry.get("square_off"), contract["square_off"], "square_off")
    _require_equal(entry.get("eod_policy"), contract["eod_policy"], "eod_policy")
    _require_equal(
        parameters.get("portfolio_mode"), contract["portfolio_mode"], "portfolio"
    )
    _require_equal(
        float(parameters.get("target_exposure_per_entry_rs")),
        contract["target_exposure_per_entry_rs"],
        "target exposure",
    )
    _require_equal(
        source.get("snapshot_fingerprint"),
        contract["source_snapshot_fingerprint"],
        "snapshot fingerprint",
    )
    if not _same_path(
        str(source.get("manifest_path", "")), contract["source_snapshot_manifest"]
    ):
        raise AssertionError("Locked V10 provenance source snapshot path changed")
    manifest_path = Path(str(source["manifest_path"]))
    _require_equal(
        _sha256_file(manifest_path),
        contract["source_snapshot_manifest_sha256"],
        "source manifest hash",
    )
    for key, expected in dict(contract["universe"]).items():
        _require_equal(universe.get(key), expected, f"universe.{key}")
    _require_equal(
        int(results.get("sessions", -1)),
        contract["expected_official_sessions"],
        "session count",
    )
    _require_equal(
        int(inventory.get("existing_count", -1)),
        contract["source_capture_count"],
        "source capture count",
    )
    _require_equal(
        inventory.get("inventory_sha256"),
        contract["source_inventory_sha256"],
        "source inventory hash",
    )
    _require_equal(
        inventory.get("source_fingerprint"),
        contract["source_inventory_fingerprint"],
        "source inventory fingerprint",
    )
    _require_equal(
        int(inventory.get("total_bytes", -1)),
        contract["source_total_bytes"],
        "source total bytes",
    )
    expected_symbol_sessions = (
        int(results.get("sessions", -1))
        * int(universe.get("mapped_stock_futures", -1))
    )
    _require_equal(
        expected_symbol_sessions,
        contract["expected_symbol_sessions"],
        "expected symbol sessions",
    )
    incomplete = int(results.get("source_incomplete_symbol_sessions", -1))
    _require_equal(
        incomplete,
        contract["expected_incomplete_symbol_sessions"],
        "incomplete symbol sessions",
    )
    _require_equal(
        expected_symbol_sessions - incomplete,
        contract["expected_complete_symbol_sessions"],
        "complete symbol sessions",
    )
    _require_equal(
        int(results.get("unexpected_source_symbol_sessions", -1)),
        0,
        "unexpected source symbol sessions",
    )


def validate_locked_run_provenance(path: Path | str) -> dict[str, Any]:
    payload = _ORIGINAL_EXPERIMENT_PROVENANCE_VALIDATOR(path)
    _require_equal(
        payload.get("v10_locked_backtest_run_schema_version"),
        LOCKED_RUN_SCHEMA_VERSION,
        "locked run schema",
    )
    _require_equal(
        payload.get("v10_locked_backtest_profile_id"),
        locked_config.PROFILE_ID,
        "profile id",
    )
    _require_equal(
        payload.get("v10_locked_backtest_profile_sha256"),
        locked_config.profile_sha256(),
        "profile hash",
    )
    _require_locked_profile_payload(
        payload.get("v10_locked_backtest_profile"), "profile payload hash"
    )
    _require_equal(
        payload.get("v10_locked_backtest_authority"),
        "BACKTEST_ONLY",
        "authority",
    )
    _require_equal(payload.get("research_only"), True, "research status")
    _require_equal(payload.get("promotion_eligible"), False, "promotion status")

    outputs = dict(payload.get("outputs", {}))
    required = {
        "locked_backtest_launcher_source_archive",
        "locked_backtest_config_source_archive",
        "resolved_locked_backtest_profile",
    }
    missing = sorted(required - set(outputs))
    if missing:
        raise ValueError(f"Locked V10 provenance misses source artifacts: {missing}")
    launcher_hash = experiment._artifact_hash(
        dict(outputs["locked_backtest_launcher_source_archive"]),
        "locked launcher source",
    )
    config_hash = experiment._artifact_hash(
        dict(outputs["locked_backtest_config_source_archive"]),
        "locked config source",
    )
    _require_equal(
        launcher_hash,
        payload.get("locked_backtest_launcher_source_sha256"),
        "launcher source hash",
    )
    _require_equal(
        config_hash,
        payload.get("locked_backtest_config_source_sha256"),
        "config source hash",
    )
    profile_record = dict(outputs["resolved_locked_backtest_profile"])
    profile_hash = experiment._artifact_hash(profile_record, "locked profile")
    profile_path = Path(str(profile_record["path"]))
    archived_profile = json.loads(profile_path.read_text(encoding="utf-8"))
    _require_locked_profile_payload(archived_profile, "profile archive hash")
    if not profile_hash:
        raise AssertionError("Locked profile archive has no hash")
    _validate_full_history_contract(payload)
    payload["current_locked_launcher_matches_archive"] = (
        launcher_source_sha256() == launcher_hash
    )
    payload["current_locked_config_matches_archive"] = (
        config_source_sha256() == config_hash
    )
    return payload


def _install_locked_provenance_adapters() -> None:
    experiment._build_run_provenance = _build_locked_run_provenance
    experiment.validate_experiment_run_provenance = validate_locked_run_provenance


def max050_gap2_profile_payload() -> dict[str, Any]:
    selection = filters.SPEC_BY_NAME[MAX050_GAP2_SELECTION_VARIANT]
    gap = next(
        spec for spec in gaps.GAP_GUARDS if spec.variant == MAX050_GAP2_GAP_VARIANT
    )
    return {
        "schema_version": MAX050_GAP2_SCHEMA_VERSION,
        "profile_id": MAX050_GAP2_PROFILE_ID,
        "base_profile": locked_config.PROFILE_ID,
        "base_variant": locked_config.ACTIVE_VARIANT,
        "five_minute_selection": {
            "stage7_0940_long_move_min_pct": 0.40,
            "selection_variant": selection.variant,
            "selection": selection.payload(),
            "application": "FILTER_THEN_RERANK_WITHIN_SESSION_AND_SETUP",
        },
        "one_minute_entry": {
            "gap_variant": gap.variant,
            "max_adverse_gap_bps": gap.max_adverse_gap_bps,
            "reject_all_gap_fills": gap.reject_all_gap_fills,
            "application": "COMPLETED_1M_BAR_OPEN_THROUGH_PENDING_STOP",
        },
        "economics": {
            "target_exposure_per_entry_rs": 50_000.0,
            "square_off": "15:30",
            "eod_policy": "LAST_REAL_BAR_SENSITIVITY",
            "cost_scenarios": [
                {
                    "scenario": name,
                    "cost_bps": cost_bps,
                    "slippage_bps": slippage_bps,
                }
                for name, cost_bps, slippage_bps in gaps.COST_SCENARIOS
            ],
        },
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }


def validate_max050_gap2_contract(*, require_files: bool = True) -> None:
    filters.validate_registry()
    selection = filters.SPEC_BY_NAME[MAX050_GAP2_SELECTION_VARIANT]
    if selection.move_0935_long_max != 0.50:
        raise AssertionError("V10 max050 profile must retain the 0.50% ceiling")
    gap = next(
        spec for spec in gaps.GAP_GUARDS if spec.variant == MAX050_GAP2_GAP_VARIANT
    )
    if gap.max_adverse_gap_bps != 2.0 or gap.reject_all_gap_fills:
        raise AssertionError("V10 Gap2 profile must retain the 2 bps adverse-gap cap")
    observed: set[date] = set()
    for segment in MAX050_GAP2_USABLE_SEGMENTS:
        if segment.from_day > segment.through_day:
            raise AssertionError(f"Invalid segment range: {segment.segment_id}")
        sessions = set(
            engine.expected_regular_session_dates(
                segment.from_day, segment.through_day
            )
        )
        duplicate = observed & sessions
        if duplicate:
            raise AssertionError(
                f"Historical segments overlap on {sorted(duplicate)}"
            )
        observed.update(sessions)
        if require_files and not segment.cache_manifest.is_file():
            raise FileNotFoundError(segment.cache_manifest)
    if len(observed) != 65:
        raise AssertionError(
            f"Expected 65 usable sessions in the pinned segment registry, got {len(observed)}"
        )


def _segment_session_series(frame: pd.DataFrame) -> pd.Series:
    if "session_date" not in frame.columns:
        raise ValueError("Cache artifact has no session_date column")
    return pd.to_datetime(frame["session_date"], errors="raise").dt.date


def _coverage_counts_for_sessions(
    coverage: pd.DataFrame, sessions: set[date]
) -> tuple[int, int]:
    incomplete = 0
    unexpected = 0
    session_text = {value.isoformat() for value in sessions}
    for row in coverage.to_dict("records"):
        incomplete_values = json.loads(
            str(row.get("source_incomplete_session_dates_json", "[]"))
        )
        unexpected_values = json.loads(
            str(row.get("unexpected_session_dates_json", "[]"))
        )
        incomplete += len(session_text & set(map(str, incomplete_values)))
        unexpected += len(session_text & set(map(str, unexpected_values)))
    return incomplete, unexpected


def _load_max050_gap2_segment(
    segment: HistoricalSegment,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    manifest_path = segment.cache_manifest.resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    artifacts = dict(manifest.get("artifacts", {}))
    candidates_path = gaps._artifact_from_manifest(
        manifest, "candidates", manifest_path
    )
    paths_path = gaps._artifact_from_manifest(manifest, "paths", manifest_path)
    coverage_path = gaps._artifact_from_manifest(
        manifest, "coverage", manifest_path
    )
    input_contract = dict(manifest.get("input_contract", {}))
    setup_book_sha256 = str(
        manifest.get("setup_book_sha256")
        or input_contract.get("setup_book_sha256", "")
    )
    if setup_book_sha256 != EXPECTED_V10_SETUP_BOOK_SHA256:
        raise AssertionError(f"Unexpected V10 setup book: {manifest_path}")
    universe = dict(input_contract.get("universe", {}))
    observed_contract = str(universe.get("contract_month_filter", "")).upper()
    if observed_contract != segment.contract_month_filter:
        raise AssertionError(
            f"{segment.segment_id} contract changed: {observed_contract}"
        )
    available_sessions = {
        engine._parse_day(value) for value in manifest.get("session_dates", [])
    }
    requested_sessions = set(
        engine.expected_regular_session_dates(segment.from_day, segment.through_day)
    )
    missing = sorted(requested_sessions - available_sessions)
    if missing:
        raise AssertionError(
            f"{segment.segment_id} cache misses requested sessions: {missing}"
        )
    candidates = pd.read_parquet(candidates_path)
    minute_paths = pd.read_parquet(paths_path)
    coverage = pd.read_parquet(coverage_path)
    if len(candidates) != int(manifest.get("candidate_count", -1)):
        raise AssertionError(f"{segment.segment_id} candidate count changed")
    if len(minute_paths) != int(manifest.get("path_row_count", -1)):
        raise AssertionError(f"{segment.segment_id} path count changed")
    candidate_days = _segment_session_series(candidates)
    selected_candidates = candidates.loc[candidate_days.isin(requested_sessions)].copy()
    selected_ids = set(selected_candidates["candidate_id"].astype(str))
    selected_paths = minute_paths.loc[
        minute_paths["candidate_id"].astype(str).isin(selected_ids)
    ].copy()
    if selected_candidates["candidate_id"].astype(str).duplicated().any():
        raise AssertionError(f"{segment.segment_id} contains duplicate candidates")
    incomplete, unexpected = _coverage_counts_for_sessions(
        coverage, requested_sessions
    )
    mapped = int(universe.get("mapped_stock_futures", len(coverage)))
    return selected_candidates, selected_paths, {
        "segment": gaps._json_ready(segment.payload()),
        "cache_manifest": {
            "path": str(manifest_path),
            "size": manifest_path.stat().st_size,
            "sha256": _sha256_file(manifest_path),
        },
        "cache_schema_version": manifest.get("schema_version"),
        "cache_input_fingerprint": manifest.get("input_fingerprint"),
        "snapshot_fingerprint": input_contract.get("snapshot_fingerprint"),
        "universe": universe,
        "sessions": [value.isoformat() for value in sorted(requested_sessions)],
        "session_count": len(requested_sessions),
        "candidate_count": len(selected_candidates),
        "minute_path_rows": len(selected_paths),
        "expected_symbol_sessions": mapped * len(requested_sessions),
        "source_incomplete_symbol_sessions": incomplete,
        "unexpected_source_symbol_sessions": unexpected,
        "headline_source_complete": incomplete == 0 and unexpected == 0,
    }


def _load_all_usable_max050_gap2_history() -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    list[dict[str, Any]],
    list[date],
    list[date],
    list[date],
]:
    candidate_parts: list[pd.DataFrame] = []
    path_parts: list[pd.DataFrame] = []
    segment_records: list[dict[str, Any]] = []
    sessions: list[date] = []
    for segment in MAX050_GAP2_USABLE_SEGMENTS:
        candidates, minute_paths, record = _load_max050_gap2_segment(segment)
        candidate_parts.append(candidates)
        path_parts.append(minute_paths)
        segment_records.append(record)
        sessions.extend(date.fromisoformat(value) for value in record["sessions"])
    if len(sessions) != len(set(sessions)):
        raise AssertionError("Usable-history segments overlap")
    ordered_sessions = sorted(set(sessions))
    expected_span = engine.expected_regular_session_dates(
        min(ordered_sessions), max(ordered_sessions)
    )
    missing_sessions = sorted(set(expected_span) - set(ordered_sessions))
    candidates = pd.concat(candidate_parts, ignore_index=True)
    minute_paths = pd.concat(path_parts, ignore_index=True)
    if candidates["candidate_id"].astype(str).duplicated().any():
        raise AssertionError("Combined usable history contains duplicate candidates")
    if minute_paths.duplicated(["candidate_id", "bar_ts"]).any():
        raise AssertionError("Combined usable history contains duplicate minute bars")
    return (
        candidates,
        minute_paths,
        segment_records,
        ordered_sessions,
        expected_span,
        missing_sessions,
    )


def _profile_metric_rows(
    audit: pd.DataFrame,
    sessions: Sequence[date],
    segments: Sequence[Mapping[str, Any]],
    *,
    scenario: str,
    cost_bps: float,
    slippage_bps: float,
    gap_spec: gaps.GapGuardSpec,
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    ordered = tuple(sorted(set(sessions)))
    periods: list[tuple[str, tuple[date, ...]]] = [
        ("FULL_USABLE", ordered),
        (
            "CORE_59",
            tuple(day for day in ordered if day < MAX050_GAP2_EXTENSION_DAY),
        ),
        (
            "FORWARD_EXTENSION",
            tuple(day for day in ordered if day >= MAX050_GAP2_EXTENSION_DAY),
        ),
    ]
    for segment in segments:
        periods.append(
            (
                f"SEGMENT_{dict(segment['segment'])['segment_id']}",
                tuple(date.fromisoformat(value) for value in segment["sessions"]),
            )
        )
    rows: list[dict[str, Any]] = []
    full_daily = pd.DataFrame()
    audit_days = _segment_session_series(audit)
    for period, period_sessions in periods:
        subset = audit.loc[audit_days.isin(set(period_sessions))].copy()
        row, daily = gaps.metric_row(
            subset,
            period_sessions,
            dataset="ALL_USABLE_HISTORY",
            period=period,
            scenario=scenario,
            spec=gap_spec,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
        )
        row.update(
            {
                "profile_id": MAX050_GAP2_PROFILE_ID,
                "selection_variant": MAX050_GAP2_SELECTION_VARIANT,
                "gap_variant": MAX050_GAP2_GAP_VARIANT,
            }
        )
        rows.append(row)
        if period == "FULL_USABLE":
            full_daily = daily.copy()
            full_daily["profile_id"] = MAX050_GAP2_PROFILE_ID
            full_daily["selection_variant"] = MAX050_GAP2_SELECTION_VARIANT
            full_daily["gap_variant"] = MAX050_GAP2_GAP_VARIANT
    return rows, full_daily


def _max050_gap2_report(
    metrics: pd.DataFrame,
    *,
    sessions: Sequence[date],
    missing_sessions: Sequence[date],
) -> str:
    reference = metrics.loc[
        metrics["scenario"].eq("REFERENCE_15_0")
        & metrics["period"].isin(
            ["FULL_USABLE", "CORE_59", "FORWARD_EXTENSION"]
        )
    ].copy()
    lines = [
        "# V10 .50 + Gap2 full usable-history backtest",
        "",
        f"Usable sessions: {len(sessions)} ({min(sessions)} through {max(sessions)})",
        f"Missing regular sessions inside the span: {', '.join(map(str, missing_sessions)) or 'none'}",
        "",
        "| Period | Sessions | Fills | WR | PF | Net points | Net P&L |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in reference.to_dict("records"):
        pf = row.get("profit_factor")
        pf_text = "n/a" if pd.isna(pf) else f"{float(pf):.4f}"
        lines.append(
            "| {period} | {sessions} | {fills} | {wr:.2f}% | {pf} | "
            "{points:+.4f} | {pnl:+.2f} |".format(
                period=row["period"],
                sessions=int(row["sessions"]),
                fills=int(row["fills"]),
                wr=float(row["win_rate_pct"]),
                pf=pf_text,
                points=float(row["net_return_points"]),
                pnl=float(row["net_pnl_rs"]),
            )
        )
    lines.extend(
        [
            "",
            "Research-only sensitivity: source coverage is incomplete and the "
            "last-real-bar EOD policy is not headline-valid.",
            "",
        ]
    )
    return "\n".join(lines)


def _max050_gap2_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fno_v10_backtest.py max050-gap2",
        description=(
            "Replay V10 Stage7 + 09:35 LONG max 0.50% + adverse Gap2 on all "
            "validated, non-overlapping historical cache segments."
        ),
    )
    parser.add_argument(
        "--all-usable-history",
        action="store_true",
        required=True,
        help="Acknowledge the pinned 65-session, rollover-aware research contract.",
    )
    parser.add_argument(
        "--output-root", type=Path, default=MAX050_GAP2_OUTPUT_ROOT
    )
    parser.add_argument(
        "--reference-only",
        action="store_true",
        help="Run only 15 bps cost and zero slippage instead of all cost stresses.",
    )
    return parser


def run_max050_gap2(argv: Sequence[str]) -> Path:
    args = _max050_gap2_parser().parse_args(list(argv))
    validate_max050_gap2_contract()
    (
        candidates,
        minute_paths,
        segment_records,
        sessions,
        expected_span,
        missing_sessions,
    ) = _load_all_usable_max050_gap2_history()

    selection_spec = filters.SPEC_BY_NAME[MAX050_GAP2_SELECTION_VARIANT]
    gap_spec = next(
        spec for spec in gaps.GAP_GUARDS if spec.variant == MAX050_GAP2_GAP_VARIANT
    )
    selected, decisions = filters.selection_overlay(candidates, selection_spec)
    experiment.configure_engine(locked_config.ACTIVE_VARIANT)
    engine._confirmation_check = experiment._NEUTRAL_CONFIRMATION_CHECK

    output_root = args.output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(gaps.IST).strftime("%Y%m%dT%H%M%S%f%z")
    run_dir = output_root / f"run_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=False)
    source_dir = run_dir / "source"
    source_dir.mkdir()
    sources = (
        Path(__file__).resolve(),
        Path(locked_config.__file__).resolve(),
        Path(filters.__file__).resolve(),
        Path(gaps.__file__).resolve(),
        Path(experiment.__file__).resolve(),
        Path(engine.__file__).resolve(),
    )
    for source in sources:
        shutil.copy2(source, source_dir / source.name)

    common.atomic_write_csv(candidates, run_dir / "all_input_candidates.csv")
    common.atomic_write_csv(selected, run_dir / "selected_candidates.csv")
    common.atomic_write_csv(decisions, run_dir / "selection_decisions.csv")
    common.atomic_write_json(
        run_dir / "resolved_profile.json", max050_gap2_profile_payload()
    )
    common.atomic_write_json(
        run_dir / "source_segments.json",
        {"schema_version": MAX050_GAP2_SCHEMA_VERSION, "segments": segment_records},
    )

    scenarios = (
        gaps.COST_SCENARIOS[:1] if args.reference_only else gaps.COST_SCENARIOS
    )
    metric_rows: list[dict[str, Any]] = []
    daily_parts: list[pd.DataFrame] = []
    scenario_artifacts: dict[str, Any] = {}
    benchmark_verification: dict[str, Any] | None = None
    for scenario, cost_bps, slippage_bps in scenarios:
        print(
            f"[V10-MAX050-GAP2] scenario={scenario} sessions={len(sessions)}",
            flush=True,
        )
        policy = experiment._entry_policy_for_variant(
            locked_config.ACTIVE_VARIANT,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
            square_off="15:30",
            eod_policy="LAST_REAL_BAR_SENSITIVITY",
        )
        with gaps.installed_gap_guard(gap_spec):
            audit = experiment._NEUTRAL_RUN_BACKTEST(
                selected,
                minute_paths,
                variant=MAX050_GAP2_PROFILE_ID,
                policy=policy,
                target_exposure_per_entry_rs=50_000.0,
            )
        audit = audit.copy()
        audit["profile_id"] = MAX050_GAP2_PROFILE_ID
        audit["selection_variant"] = MAX050_GAP2_SELECTION_VARIANT
        audit["gap_variant"] = MAX050_GAP2_GAP_VARIANT
        audit["scenario"] = scenario
        audit["research_only"] = True
        audit["promotion_eligible"] = False
        rows, daily = _profile_metric_rows(
            audit,
            sessions,
            segment_records,
            scenario=scenario,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
            gap_spec=gap_spec,
        )
        if scenario == "REFERENCE_15_0":
            reference_full = next(
                row for row in rows if row["period"] == "FULL_USABLE"
            )
            benchmark_verification = validate_current_mixed_benchmark(
                reference_full
            )
        metric_rows.extend(rows)
        daily_parts.append(daily)
        scenario_dir = run_dir / "scenarios" / scenario.lower()
        scenario_dir.mkdir(parents=True)
        audit_path = scenario_dir / "candidate_order_audit.csv"
        closed_path = scenario_dir / "closed_trades.csv"
        daily_path = scenario_dir / "daywise.csv"
        summary_path = scenario_dir / "summary.json"
        common.atomic_write_csv(audit, audit_path)
        common.atomic_write_csv(audit.loc[gaps._closed_mask(audit)], closed_path)
        common.atomic_write_csv(daily, daily_path)
        common.atomic_write_json(
            summary_path,
            next(row for row in rows if row["period"] == "FULL_USABLE"),
        )
        scenario_artifacts[scenario] = {
            "audit": str(audit_path.resolve()),
            "closed_trades": str(closed_path.resolve()),
            "daywise": str(daily_path.resolve()),
            "summary": str(summary_path.resolve()),
        }

    metrics = pd.DataFrame(metric_rows)
    daywise = pd.concat(daily_parts, ignore_index=True)
    metrics_path = run_dir / "all_period_metrics.csv"
    daywise_path = run_dir / "all_daywise.csv"
    report_path = run_dir / "report.md"
    benchmark_path = run_dir / "current_mixed_benchmark_verification.json"
    common.atomic_write_csv(metrics, metrics_path)
    common.atomic_write_csv(daywise, daywise_path)
    common.atomic_write_text(
        report_path,
        _max050_gap2_report(
            metrics, sessions=sessions, missing_sessions=missing_sessions
        ),
    )
    if benchmark_verification is None:
        raise AssertionError("Reference current-mixed benchmark was not evaluated")
    common.atomic_write_json(benchmark_path, benchmark_verification)

    provenance_path = run_dir / "provenance.json"
    inventory_path = run_dir / "artifact_inventory.json"
    provenance = {
        "schema_version": MAX050_GAP2_SCHEMA_VERSION,
        "complete": True,
        "created_at_ist": datetime.now(gaps.IST),
        "command": ["python", "-u", str(Path(__file__).resolve()), "max050-gap2", *argv],
        "profile": max050_gap2_profile_payload(),
        "usable_session_dates": [value.isoformat() for value in sessions],
        "usable_session_count": len(sessions),
        "calendar_span_session_count": len(expected_span),
        "missing_regular_session_dates": [
            value.isoformat() for value in missing_sessions
        ],
        "source_segments": segment_records,
        "scenarios": scenario_artifacts,
        "outputs": {
            "metrics": str(metrics_path.resolve()),
            "daywise": str(daywise_path.resolve()),
            "report": str(report_path.resolve()),
            "current_mixed_benchmark_verification": str(
                benchmark_path.resolve()
            ),
        },
        "limitations": [
            "SOURCE_SLOT_COVERAGE_INCOMPLETE",
            "LAST_REAL_BAR_SENSITIVITY_NOT_HEADLINE",
            "STATIC_CONTRACT_UNIVERSES_BY_SEGMENT",
            "2026_08_26_HAS_NO_VALIDATED_CACHE",
            "POST_SELECTION_COMBINATION_REQUIRES_FORWARD_VALIDATION",
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
            "schema_version": MAX050_GAP2_SCHEMA_VERSION,
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
            "schema_version": MAX050_GAP2_SCHEMA_VERSION,
            "run_dir": str(run_dir.resolve()),
            "provenance_sha256": _sha256_file(provenance_path),
            "usable_session_count": len(sessions),
            "research_only": True,
        },
    )
    print(f"[V10-MAX050-GAP2] complete: {run_dir}", flush=True)
    return run_dir


def _setup_cap_map(setups: Sequence[engine.V8Setup]) -> dict[str, int]:
    return {setup.setup_id: int(setup.max_entries) for setup in setups}


def validate_current_mixed_benchmark(
    row: Mapping[str, Any],
) -> dict[str, Any]:
    """Fail closed if the pinned 65-session current-mixed result drifts."""

    observed: dict[str, Any] = {}
    mismatches: list[str] = []
    for field, expected in MAX050_GAP2_CURRENT_MIXED_BENCHMARK.items():
        if field not in row:
            mismatches.append(f"{field}=MISSING")
            continue
        value = row[field]
        observed[field] = gaps._json_ready(value)
        if field in MAX050_GAP2_BENCHMARK_FLOAT_FIELDS:
            try:
                matches = math.isclose(
                    float(value),
                    float(expected),
                    rel_tol=0.0,
                    abs_tol=MAX050_GAP2_BENCHMARK_ABS_TOLERANCE,
                )
            except (TypeError, ValueError):
                matches = False
        else:
            matches = value == expected
        if not matches:
            mismatches.append(f"{field}={value!r} expected={expected!r}")
    if mismatches:
        raise AssertionError(
            "Pinned V10 .50 + Gap2 current-mixed benchmark changed: "
            + "; ".join(mismatches)
        )
    return {
        "verified": True,
        "profile_id": MAX050_GAP2_BASELINE_PROFILE_ID,
        "contract": "PINNED_65_SESSION_REFERENCE_15_0",
        "float_abs_tolerance": MAX050_GAP2_BENCHMARK_ABS_TOLERANCE,
        "expected": gaps._json_ready(MAX050_GAP2_CURRENT_MIXED_BENCHMARK),
        "observed": observed,
        "display": (
            "Current mixed limits | 232 fills | 116-116 | WR 50.00% | "
            "PF 1.8327 | +73.0544 points | +Rs 36,312.05 | MDD 9.3513"
        ),
    }


def _uniform_max_entry_setups(
    setups: Sequence[engine.V8Setup], max_entries: int
) -> tuple[engine.V8Setup, ...]:
    if isinstance(max_entries, bool) or max_entries not in MAX050_GAP2_CAP_SWEEP_VALUES:
        raise ValueError(
            f"max_entries must be one of {MAX050_GAP2_CAP_SWEEP_VALUES}"
        )
    return tuple(replace(setup, max_entries=int(max_entries)) for setup in setups)


def _max050_gap2_cap_sweep_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fno_v10_backtest.py max050-gap2-cap-sweep",
        description=(
            "Compare the current mixed per-setup caps with uniform max_entries "
            "1, 2, 3, 4 and 5 on all usable V10 .50 + Gap2 history."
        ),
    )
    parser.add_argument(
        "--all-usable-history",
        action="store_true",
        required=True,
        help="Acknowledge the pinned 65-session, rollover-aware research contract.",
    )
    parser.add_argument(
        "--output-root", type=Path, default=MAX050_GAP2_CAP_SWEEP_OUTPUT_ROOT
    )
    return parser


def _cap_sweep_report(comparison: pd.DataFrame) -> str:
    lines = [
        "# V10 .50 + Gap2 uniform max-entries sweep",
        "",
        "Reference economics: 15 bps cost, zero slippage, ₹50,000 exposure per entry.",
        "",
        "| Profile | Fills | W-L | WR | PF | Net points | Net P&L | Max DD |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in comparison.to_dict("records"):
        lines.append(
            "| {profile} | {fills} | {wins}-{losses} | {wr:.2f}% | {pf:.4f} | "
            "{points:+.4f} | {pnl:+.2f} | {drawdown:.4f} |".format(
                profile=row["cap_profile"],
                fills=int(row["fills"]),
                wins=int(row["wins"]),
                losses=int(row["losses"]),
                wr=float(row["win_rate_pct"]),
                pf=float(row["profit_factor"]),
                points=float(row["net_return_points"]),
                pnl=float(row["net_pnl_rs"]),
                drawdown=float(row["max_daily_drawdown_points"]),
            )
        )
    lines.extend(
        [
            "",
            "The global 12-position cap, ₹120,000 capital, ₹10,000 reserved margin, "
            "one-position-per-symbol rule, filters and Gap2 remain unchanged.",
            "",
        ]
    )
    return "\n".join(lines)


def run_max050_gap2_cap_sweep(argv: Sequence[str]) -> Path:
    args = _max050_gap2_cap_sweep_parser().parse_args(list(argv))
    validate_max050_gap2_contract()
    (
        candidates,
        minute_paths,
        segment_records,
        sessions,
        expected_span,
        missing_sessions,
    ) = _load_all_usable_max050_gap2_history()
    selection_spec = filters.SPEC_BY_NAME[MAX050_GAP2_SELECTION_VARIANT]
    gap_spec = next(
        spec for spec in gaps.GAP_GUARDS if spec.variant == MAX050_GAP2_GAP_VARIANT
    )
    selected, decisions = filters.selection_overlay(candidates, selection_spec)
    experiment.configure_engine(locked_config.ACTIVE_VARIANT)
    engine._confirmation_check = experiment._NEUTRAL_CONFIRMATION_CHECK
    baseline_setups = tuple(engine.ACTIVE_SETUPS)
    expected_baseline_caps = {
        "09:25_LONG": 4,
        "09:25_SHORT": 4,
        "09:30_LONG": 1,
        "09:30_SHORT": 4,
        "09:35_LONG": 1,
        "09:35_SHORT": 2,
        "09:40_LONG": 1,
        "09:40_SHORT": 1,
        "09:45_LONG": 1,
        "09:45_SHORT": 1,
    }
    if _setup_cap_map(baseline_setups) != expected_baseline_caps:
        raise AssertionError("Current mixed V10 cap baseline changed")
    profiles: list[tuple[str, int | None, tuple[engine.V8Setup, ...]]] = [
        (MAX050_GAP2_BASELINE_PROFILE_ID, None, baseline_setups)
    ]
    profiles.extend(
        (
            f"UNIFORM_MAX_ENTRIES_{value}",
            value,
            _uniform_max_entry_setups(baseline_setups, value),
        )
        for value in MAX050_GAP2_CAP_SWEEP_VALUES
    )

    output_root = args.output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(gaps.IST).strftime("%Y%m%dT%H%M%S%f%z")
    run_dir = output_root / f"sweep_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=False)
    source_dir = run_dir / "source"
    source_dir.mkdir()
    sources = (
        Path(__file__).resolve(),
        Path(locked_config.__file__).resolve(),
        Path(filters.__file__).resolve(),
        Path(gaps.__file__).resolve(),
        Path(experiment.__file__).resolve(),
        Path(engine.__file__).resolve(),
    )
    for source in sources:
        shutil.copy2(source, source_dir / source.name)
    common.atomic_write_csv(selected, run_dir / "selected_candidates.csv")
    common.atomic_write_csv(decisions, run_dir / "selection_decisions.csv")
    common.atomic_write_json(
        run_dir / "source_segments.json",
        {
            "schema_version": MAX050_GAP2_CAP_SWEEP_SCHEMA_VERSION,
            "segments": segment_records,
        },
    )

    policy = experiment._entry_policy_for_variant(
        locked_config.ACTIVE_VARIANT,
        cost_bps=15.0,
        slippage_bps=0.0,
        square_off="15:30",
        eod_policy="LAST_REAL_BAR_SENSITIVITY",
    )
    metric_rows: list[dict[str, Any]] = []
    daily_parts: list[pd.DataFrame] = []
    profile_artifacts: dict[str, Any] = {}
    profile_contracts: dict[str, Any] = {}
    try:
        for profile_id, uniform_value, setups in profiles:
            print(
                f"[V10-CAP-SWEEP] profile={profile_id} sessions={len(sessions)}",
                flush=True,
            )
            engine.ACTIVE_SETUPS = setups
            with gaps.installed_gap_guard(gap_spec):
                audit = experiment._NEUTRAL_RUN_BACKTEST(
                    selected,
                    minute_paths,
                    variant=profile_id,
                    policy=policy,
                    target_exposure_per_entry_rs=50_000.0,
                )
            audit = audit.copy()
            audit["profile_id"] = profile_id
            audit["cap_profile"] = profile_id
            audit["uniform_max_entries"] = uniform_value
            audit["selection_variant"] = MAX050_GAP2_SELECTION_VARIANT
            audit["gap_variant"] = MAX050_GAP2_GAP_VARIANT
            audit["research_only"] = True
            audit["promotion_eligible"] = False
            rows, daily = _profile_metric_rows(
                audit,
                sessions,
                segment_records,
                scenario="REFERENCE_15_0",
                cost_bps=15.0,
                slippage_bps=0.0,
                gap_spec=gap_spec,
            )
            setup_caps = _setup_cap_map(setups)
            setup_payload = [asdict(setup) for setup in setups]
            for row in rows:
                row.update(
                    {
                        "profile_id": profile_id,
                        "cap_profile": profile_id,
                        "uniform_max_entries": uniform_value,
                        "setup_cap_map_json": json.dumps(setup_caps, sort_keys=True),
                        "setup_book_sha256": filters.canonical_sha256(setup_payload),
                    }
                )
            daily = daily.copy()
            daily["profile_id"] = profile_id
            daily["cap_profile"] = profile_id
            daily["uniform_max_entries"] = uniform_value
            metric_rows.extend(rows)
            daily_parts.append(daily)
            profile_dir = run_dir / "profiles" / profile_id.lower()
            profile_dir.mkdir(parents=True)
            audit_path = profile_dir / "candidate_order_audit.csv"
            closed_path = profile_dir / "closed_trades.csv"
            daily_path = profile_dir / "daywise.csv"
            summary_path = profile_dir / "summary.json"
            common.atomic_write_csv(audit, audit_path)
            common.atomic_write_csv(audit.loc[gaps._closed_mask(audit)], closed_path)
            common.atomic_write_csv(daily, daily_path)
            full_row = next(row for row in rows if row["period"] == "FULL_USABLE")
            common.atomic_write_json(summary_path, gaps._json_ready(full_row))
            profile_artifacts[profile_id] = {
                "audit": str(audit_path.resolve()),
                "closed_trades": str(closed_path.resolve()),
                "daywise": str(daily_path.resolve()),
                "summary": str(summary_path.resolve()),
            }
            profile_contracts[profile_id] = {
                "uniform_max_entries": uniform_value,
                "setup_cap_map": setup_caps,
                "setup_book_sha256": filters.canonical_sha256(setup_payload),
            }
    finally:
        engine.ACTIVE_SETUPS = baseline_setups

    metrics = pd.DataFrame(metric_rows)
    daywise = pd.concat(daily_parts, ignore_index=True)
    profile_order = [profile[0] for profile in profiles]
    comparison = metrics.loc[metrics["period"].eq("FULL_USABLE")].copy()
    comparison["profile_order"] = comparison["cap_profile"].map(
        {value: index for index, value in enumerate(profile_order)}
    )
    comparison = comparison.sort_values("profile_order", kind="stable").drop(
        columns="profile_order"
    )
    baseline = comparison.loc[
        comparison["cap_profile"].eq(MAX050_GAP2_BASELINE_PROFILE_ID)
    ].iloc[0]
    benchmark_verification = validate_current_mixed_benchmark(baseline)
    for field in (
        "fills",
        "wins",
        "losses",
        "win_rate_pct",
        "profit_factor",
        "net_return_points",
        "net_pnl_rs",
        "max_daily_drawdown_points",
        "positive_days",
        "negative_days",
        "flat_days",
        "guard_rejections",
    ):
        comparison[f"delta_{field}_vs_baseline"] = (
            pd.to_numeric(comparison[field], errors="coerce") - float(baseline[field])
        )

    metrics_path = run_dir / "all_period_metrics.csv"
    daywise_path = run_dir / "all_daywise.csv"
    comparison_path = run_dir / "comparison_vs_current_mixed.csv"
    report_path = run_dir / "report.md"
    benchmark_path = run_dir / "current_mixed_benchmark_verification.json"
    common.atomic_write_csv(metrics, metrics_path)
    common.atomic_write_csv(daywise, daywise_path)
    common.atomic_write_csv(comparison, comparison_path)
    common.atomic_write_text(report_path, _cap_sweep_report(comparison))
    common.atomic_write_json(benchmark_path, benchmark_verification)

    provenance_path = run_dir / "provenance.json"
    inventory_path = run_dir / "artifact_inventory.json"
    provenance = {
        "schema_version": MAX050_GAP2_CAP_SWEEP_SCHEMA_VERSION,
        "complete": True,
        "created_at_ist": datetime.now(gaps.IST),
        "command": [
            "python",
            "-u",
            str(Path(__file__).resolve()),
            "max050-gap2-cap-sweep",
            *argv,
        ],
        "base_profile": max050_gap2_profile_payload(),
        "experiment": "UNIFORM_PER_SETUP_MAX_ENTRIES_ONLY",
        "profile_contracts": profile_contracts,
        "unchanged_controls": {
            "capital_rs": 120_000.0,
            "margin_per_entry_rs": 10_000.0,
            "target_exposure_per_entry_rs": 50_000.0,
            "max_concurrent_positions": 12,
            "pending_reserves_margin": True,
            "one_position_per_symbol": True,
            "cost_bps": 15.0,
            "slippage_bps": 0.0,
        },
        "usable_session_dates": [value.isoformat() for value in sessions],
        "usable_session_count": len(sessions),
        "calendar_span_session_count": len(expected_span),
        "missing_regular_session_dates": [
            value.isoformat() for value in missing_sessions
        ],
        "source_segments": segment_records,
        "profiles": profile_artifacts,
        "outputs": {
            "metrics": str(metrics_path.resolve()),
            "daywise": str(daywise_path.resolve()),
            "comparison": str(comparison_path.resolve()),
            "report": str(report_path.resolve()),
            "current_mixed_benchmark_verification": str(
                benchmark_path.resolve()
            ),
        },
        "limitations": [
            "SOURCE_SLOT_COVERAGE_INCOMPLETE",
            "LAST_REAL_BAR_SENSITIVITY_NOT_HEADLINE",
            "STATIC_CONTRACT_UNIVERSES_BY_SEGMENT",
            "2026_08_26_HAS_NO_VALIDATED_CACHE",
            "CAP_SWEEP_MULTIPLE_TESTING_REQUIRES_FORWARD_VALIDATION",
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
            "schema_version": MAX050_GAP2_CAP_SWEEP_SCHEMA_VERSION,
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
            "schema_version": MAX050_GAP2_CAP_SWEEP_SCHEMA_VERSION,
            "run_dir": str(run_dir.resolve()),
            "provenance_sha256": _sha256_file(provenance_path),
            "usable_session_count": len(sessions),
            "research_only": True,
        },
    )
    print(f"[V10-CAP-SWEEP] complete: {run_dir}", flush=True)
    return run_dir


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    locked_config.validate_locked_profile()
    if not args:
        raise ValueError(
            "A command is required: profile, run, max050-gap2, "
            "max050-gap2-cap-sweep, smoke, build-cache, or validate"
        )
    command = args[0]
    if command == "profile":
        print(json.dumps(_profile_document(), indent=2, sort_keys=True))
        return 0
    if command in {"-h", "--help"}:
        print(__doc__)
        return 0
    if command == "max050-gap2":
        run_max050_gap2(args[1:])
        return 0
    if command == "max050-gap2-cap-sweep":
        run_max050_gap2_cap_sweep(args[1:])
        return 0
    allowed = REPLAY_COMMANDS | PASSTHROUGH_COMMANDS | {"validate"}
    if command not in allowed:
        raise ValueError(
            f"Unsupported locked V10 command {command!r}; allowed={sorted(allowed)}"
        )
    if command == "validate" and not ({"-h", "--help"} & set(args[1:])):
        _validate_locked_provenance_target(args[1:])
    delegated_args = _inject_locked_run_contract(args)
    _install_locked_provenance_adapters()
    return experiment.main(delegated_args)


if __name__ == "__main__":
    raise SystemExit(main())
