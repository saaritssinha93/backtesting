"""Package the frozen 59-session V6/V8/V10-Stage7 comparison.

This utility is deliberately report-only.  It validates three completed run
provenances and their recorded artifacts, then writes a new non-overwritable
research package.  It never changes a strategy, cache, snapshot, or run.

The accepted comparison contract is intentionally narrow:

* V6's literal rules executed by the V8-Strict launcher (variant ``VS``);
* the V8-Combined parent strategy (variant ``VC``); and
* locked V10 Stage 7 (variant ``0940_LONG_MOVE_040``).

All runs must cover 2026-05-27 through 2026-08-19, use the frozen August
snapshot and the 2026-08-06 TRAIN/TEST split, and retain the same economics
and conservative portfolio policy.  The result is research-only and is not a
promotion decision.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

TOOLS_ROOT = Path(__file__).resolve().parent
WORKSPACE_ROOT = TOOLS_ROOT.parent
for import_root in (str(TOOLS_ROOT), str(WORKSPACE_ROOT)):
    if import_root not in sys.path:
        sys.path.insert(0, import_root)

import fno_oi_common as common
import fno_v10_backtest as locked_launcher
import fno_v10_backtest_config as locked_config
import fno_v10_experiment_backtest as experiment
import fno_v8_combined_best_per_leg_backtest as combined_launcher
import fno_v8_strict_v6_logic_backtest as strict_launcher
import fno_v8_windowed_1m_entry_backtest as engine


SCHEMA_VERSION = "fno_v10_full_history_trio_package_v1"
TEST_SCHEMA_VERSION = "fno_v10_full_history_trio_test_evidence_v1"
DECISION = "COMPLETED_59_SESSION_TRIO_RESEARCH_ONLY_NOT_PROMOTION"
PROVENANCE_ARCHIVE_NAMES = {
    "V6_RULES_ON_V8_STRICT": "v6_rules_on_v8_strict_provenance.json",
    "V8_COMBINED": "v8_combined_provenance.json",
    "V10_STAGE7": "v10_stage7_locked_provenance.json",
}

FROM_DAY = "2026-05-27"
THROUGH_DAY = "2026-08-19"
SPLIT_DAY = "2026-08-06"
EXPECTED_SESSIONS = 59
EXPECTED_TRAIN_SESSIONS = 49
EXPECTED_TEST_SESSIONS = 10
EXPECTED_SYMBOLS = 208
EXPECTED_SYMBOL_SESSIONS = 12_272
EXPECTED_COMPLETE_SYMBOL_SESSIONS = 6_350
EXPECTED_INCOMPLETE_SYMBOL_SESSIONS = 5_922

COST_BPS = 15.0
SLIPPAGE_BPS = 0.0
SQUARE_OFF = "15:30"
EOD_POLICY = "LAST_REAL_BAR_SENSITIVITY"
TARGET_EXPOSURE_RS = 50_000.0
PORTFOLIO_MODE = (
    "GLOBAL_PENDING_MARGIN_AND_DUPLICATE_RESERVATION_"
    "CONSERVATIVE_NO_BACKFILL_V1"
)

SNAPSHOT_MANIFEST = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research\v8_windowed_strict_v1"
    r"\snapshots\snapshot_20260820T124734626995+0530_mnofor_c\manifest.json"
)
SNAPSHOT_MANIFEST_SHA256 = (
    "579e8673fb96644bc2e4b348c9d98486ee2c26291def72702fe8da0e6a55324d"
)
SNAPSHOT_FINGERPRINT = (
    "6734204d53315d386a2c3949f01b272d4399a8a1d3b44b9cfd556a1b859190cc"
)
SOURCE_INVENTORY_SHA256 = (
    "03407e713ebae80270268733f1549bc54e1c6d384dafd67d67734ebaed2c2711"
)
SOURCE_FINGERPRINT = (
    "85f7404d4f026d3564280f30bc317c13bc5ecdac47748ebaf307c4aa1b2676d3"
)
SOURCE_TOTAL_BYTES = 3_116_245_273
UNIVERSE_FILE_SHA256 = (
    "24170f39c7cf99021553396e40e0d88a435f857364b2423dcfbe9312539dbf09"
)
UNIVERSE_SHA256 = (
    "18c496bbf9e09b6914d073cba21c4c6c56305da1ed5759f4f91cc8cb66c19ad5"
)
MAPPED_UNIVERSE_SHA256 = (
    "2cc160189f87bff4eb987a15a4684d95619ee9c810db3cd37276b114ad5824bf"
)
MAPPED_SYMBOL_SET_SHA256 = (
    "d42f87a9c5fc8ab1710b09b6c4c9832c9d19ecc440ef92b84cad6981499a05a3"
)
SESSION_CALENDAR_SHA256 = (
    "bbbd6306c532bc2cdfd2c8dab6880bd7df2eed81e5f8a5c3cf51df40b4e55bd4"
)
EXPECTED_SOURCE_LIMITATIONS = [
    "STATIC_2026_08_11_UNIVERSE_SURVIVORSHIP_RESEARCH",
    "STATIC_26AUG_FUTURES_OI_NOT_POINT_IN_TIME_ROLLING",
    "LEGACY_EQUITY_1M_HAS_NO_ROW_LINEAGE_FLAGS",
    "SOURCE_SNAPSHOT_IS_PER_FILE_STABLE_NOT_GLOBAL_TRANSACTION",
]

NEUTRAL_ENGINE_SHA256 = (
    "40788d0e7c97de4527f0a8565fa5ead32302487e5b17b60c831282597d77a895"
)
STRICT_LAUNCHER_SHA256 = (
    "7aa3e165f83ae4252e6d3913a929d7554aed048de63389e31fc1efa0e2f0c363"
)
COMBINED_LAUNCHER_SHA256 = (
    "9e75b0a05e28e5a712c919f6f524f38e0bd8c17450f071a6467498c0874fdfd4"
)
LOCKED_V10_LAUNCHER_SHA256 = (
    "7b5e5fc6c606039b8c2e91648dbffa8f245844f0a557acedcc382d4201bac0b5"
)
LOCKED_V10_CONFIG_FILE_SHA256 = (
    "f0664d2035ce13d89ab209f68ab8c6bf91f8181dad3101f8a19371200fc80a59"
)
EXPERIMENT_RUNNER_SHA256 = (
    "1c9e4c2c3d64c48c1aa12a0906aa392e262fe0000469a404e7b0eb27372cec4c"
)
EXPERIMENT_CONFIG_FILE_SHA256 = (
    "c2211c4161440989adf603d05f1fbf542ce9cc6675759950ca5f58afe97299dc"
)
UNIFIED_V10_LAUNCHER_SHA256 = (
    "ceb27c4ae8ecf6953504d0c393bf1babf37bf5c44cf14f454e5a5363b4b916e0"
)
LOCKED_PROFILE_SHA256 = (
    "f2b3291903dfb1f2c95f1d24b63285d527dc7a9a6aa3d6334caed03d0834e59c"
)
EXPERIMENT_REGISTRY_SHA256 = (
    "105935648a67ff126b73b98233efd6c10f40a5706f971a75dc22540251cc843b"
)
STAGE7_VARIANT_CONFIG_SHA256 = (
    "f3a54e5fddbfd8445923f9df52a68207f47b57bc43ccbd7eb83b2aad10a9bc18"
)

STRICT_SETUP_BOOK_SHA256 = (
    "5de61f611ad30b52d303b2075ee169421f1208c5026789a78ce4907f35c16919"
)
COMBINED_SETUP_BOOK_SHA256 = (
    "ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675"
)
STRICT_CACHE_INPUT_FINGERPRINT = (
    "22d1a91819134bbe4355eed3deadb90c85e1fe04b7dee5e810ae26750c86e828"
)
COMBINED_CACHE_INPUT_FINGERPRINT = (
    "dac2aa2d7f82b4e7a115b725f082960dd21acf2c54ada1b13fd70f9d010cd3d5"
)
STAGE7_CACHE_INPUT_FINGERPRINT = (
    "7a5fbbe68381a0fe805e47c2712c55d1e44f31f82224b2ebc54a975f6c2aaada"
)

EXPECTED_PROMOTION_BLOCKERS = frozenset(
    {
        "STATIC_LATER_DATED_UNIVERSE",
        "STATIC_AUGUST_FUTURES_OI_NOT_ROLLING_POINT_IN_TIME",
        "LEGACY_EQUITY_ROW_LINEAGE_UNPROVEN",
        "GLOBAL_PORTFOLIO_LEDGER_USES_CONSERVATIVE_NO_BACKFILL_OVERLAY",
        "PROSPECTIVE_20_SESSIONS_AND_100_FILLS_NOT_COMPLETED",
        "UPSTREAM_SOURCE_SLOT_COVERAGE_INCOMPLETE",
        "LAST_REAL_BAR_SENSITIVITY_NOT_HEADLINE",
    }
)

RUN_SPECS: dict[str, dict[str, Any]] = {
    "V6_RULES_ON_V8_STRICT": {
        "variant": "VS",
        "objective": "V6_STRICT_SIGNAL_QUALITY_ON_V8_CAUSAL_EXECUTION",
        "strategy_prefix": "FNO_V8_STRICT_V6_LOGIC_20260820_7aa3e165f83a",
        "setup_book_sha256": STRICT_SETUP_BOOK_SHA256,
        "configuration_source": (
            "LITERAL_V6_STRICT_BOOK_ON_V8_ENGINE;"
            f"LAUNCHER_SHA256={STRICT_LAUNCHER_SHA256}"
        ),
        "cache_schema_version": "fno_v8_strict_cache_manifest_v1",
        "path_policy_version": "fno_v8_strict_same_session_exact_grid_ohlcvt_v1",
        "cache_fingerprint": STRICT_CACHE_INPUT_FINGERPRINT,
    },
    "V8_COMBINED": {
        "variant": "VC",
        "objective": "TRAIN_SELECTED_PER_LEG_CONFIGURATION_ON_V8_EXECUTION",
        "strategy_prefix": "FNO_V8_COMBINED_BEST_PER_LEG_20260820_9e75b0a05e28",
        "setup_book_sha256": COMBINED_SETUP_BOOK_SHA256,
        "configuration_source": (
            "LITERAL_TRAIN_SELECTED_STRICT_RETUNED_PER_LEG_BOOK;"
            f"LAUNCHER_SHA256={COMBINED_LAUNCHER_SHA256}"
        ),
        "cache_schema_version": "fno_v8_combined_best_per_leg_cache_manifest_v1",
        "path_policy_version": "fno_v8_combined_same_session_exact_grid_ohlcvt_v1",
        "cache_fingerprint": COMBINED_CACHE_INPUT_FINGERPRINT,
    },
    "V10_STAGE7": {
        "variant": "0940_LONG_MOVE_040",
        "objective": (
            "PREDECLARED_ISOLATED_5M_SELECTION_AND_1M_ENTRY_EXPERIMENTS;"
            "FULL_CHRONOLOGICAL_V10_STATE_MACHINE_REPLAY"
        ),
        "strategy_prefix": (
            "FNO_V10_STAGE1_ISOLATED_EXPERIMENTS_20260826_1c9e4c2c3d64"
        ),
        "setup_book_sha256": COMBINED_SETUP_BOOK_SHA256,
        "configuration_source": (
            "FROZEN_V10B_BASE;PREDECLARED_STAGE1_REGISTRY;"
            f"REGISTRY_SHA256={EXPERIMENT_REGISTRY_SHA256};"
            f"RUNNER_SHA256={EXPERIMENT_RUNNER_SHA256}"
        ),
        "cache_schema_version": "fno_v10_stage1_base_candidate_cache_v1",
        "path_policy_version": "fno_v10_stage1_same_session_path_v1",
        "cache_fingerprint": STAGE7_CACHE_INPUT_FINGERPRINT,
    },
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError) as exc:
        raise ValueError(f"Unreadable JSON artifact: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object: {path}")
    return value


def atomic_write_text(path: Path, value: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(value, encoding="utf-8", newline="\n")
    os.replace(temporary, path)


def atomic_copy_verified(source: Path, destination: Path, expected_sha256: str) -> None:
    data = source.read_bytes()
    observed = hashlib.sha256(data).hexdigest()
    _require_equal(observed, expected_sha256, f"source copy {source} SHA256")
    temporary = destination.with_name(f".{destination.name}.tmp")
    temporary.write_bytes(data)
    os.replace(temporary, destination)
    _require_equal(
        sha256_file(destination),
        expected_sha256,
        f"packaged copy {destination} SHA256",
    )


def atomic_write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    materialized = [dict(row) for row in rows]
    if not materialized:
        raise ValueError(f"Refusing to write an empty CSV: {path}")
    fields = list(materialized[0])
    if any(list(row) != fields for row in materialized):
        raise ValueError(f"CSV rows have inconsistent fields: {path}")
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(materialized)
    os.replace(temporary, path)


def _same_number(left: Any, right: Any, *, tolerance: float = 1e-9) -> bool:
    try:
        left_value = float(left)
        right_value = float(right)
    except (TypeError, ValueError):
        return left == right
    if math.isnan(left_value) or math.isnan(right_value):
        return math.isnan(left_value) and math.isnan(right_value)
    if math.isinf(left_value) or math.isinf(right_value):
        return left_value == right_value
    return math.isclose(left_value, right_value, rel_tol=tolerance, abs_tol=tolerance)


def _require_equal(observed: Any, expected: Any, label: str) -> None:
    if observed != expected:
        raise AssertionError(f"{label}: expected {expected!r}, observed {observed!r}")


def _require_number(observed: Any, expected: float, label: str) -> None:
    if not _same_number(observed, expected):
        raise AssertionError(f"{label}: expected {expected!r}, observed {observed!r}")


def _resolved(path: Path | str) -> Path:
    return Path(path).expanduser().resolve()


def _require_same_path(observed: Path | str, expected: Path, label: str) -> None:
    if _resolved(observed) != expected.resolve():
        raise AssertionError(
            f"{label}: expected {expected.resolve()}, observed {_resolved(observed)}"
        )


def _source_bindings() -> dict[str, dict[str, Any]]:
    root = Path(__file__).resolve().parents[1]
    expected = {
        "v8_neutral_engine": (
            root / "fno_v8_windowed_1m_entry_backtest.py",
            NEUTRAL_ENGINE_SHA256,
        ),
        "v6_strict_launcher": (
            root / "fno_v8_strict_v6_logic_backtest.py",
            STRICT_LAUNCHER_SHA256,
        ),
        "v8_combined_launcher": (
            root / "fno_v8_combined_best_per_leg_backtest.py",
            COMBINED_LAUNCHER_SHA256,
        ),
        "v10_locked_launcher": (
            root / "fno_v10_backtest.py",
            LOCKED_V10_LAUNCHER_SHA256,
        ),
        "v10_locked_profile_config": (
            root / "fno_v10_backtest_config.py",
            LOCKED_V10_CONFIG_FILE_SHA256,
        ),
        "v10_experiment_runner": (
            root / "fno_v10_experiment_backtest.py",
            EXPERIMENT_RUNNER_SHA256,
        ),
        "v10_experiment_config": (
            root / "fno_v10_experiment_config.py",
            EXPERIMENT_CONFIG_FILE_SHA256,
        ),
        "v10_unified_launcher": (
            root / "fno_v10_unified_5m_1m_backtest.py",
            UNIFIED_V10_LAUNCHER_SHA256,
        ),
    }
    rows: dict[str, dict[str, Any]] = {}
    for label, (path, expected_hash) in expected.items():
        if not path.is_file():
            raise FileNotFoundError(f"Required frozen source is missing: {path}")
        observed_hash = sha256_file(path)
        _require_equal(observed_hash, expected_hash, f"{label} SHA256")
        rows[label] = {
            "path": str(path.resolve()),
            "sha256": observed_hash,
            "bytes": path.stat().st_size,
        }
    locked_config.validate_locked_profile()
    _require_equal(
        locked_config.profile_sha256(),
        LOCKED_PROFILE_SHA256,
        "locked V10 profile SHA256",
    )
    _require_equal(
        locked_config.EXPECTED_PROFILE_SHA256,
        LOCKED_PROFILE_SHA256,
        "pinned V10 profile SHA256",
    )
    return rows


def _validate_snapshot_manifest() -> dict[str, Any]:
    if not SNAPSHOT_MANIFEST.is_file():
        raise FileNotFoundError(SNAPSHOT_MANIFEST)
    _require_equal(
        sha256_file(SNAPSHOT_MANIFEST),
        SNAPSHOT_MANIFEST_SHA256,
        "source snapshot manifest SHA256",
    )
    snapshot = load_json(SNAPSHOT_MANIFEST)
    _require_equal(snapshot.get("complete"), True, "snapshot complete")
    _require_equal(snapshot.get("physical_copy"), True, "snapshot physical_copy")
    _require_equal(
        snapshot.get("capture_scope"),
        "PER_FILE_STABLE_PHYSICAL_COPY_NOT_GLOBAL_FILESYSTEM_TRANSACTION",
        "snapshot capture_scope",
    )
    _require_equal(
        snapshot.get("snapshot_fingerprint"),
        SNAPSHOT_FINGERPRINT,
        "snapshot fingerprint",
    )
    _require_equal(len(snapshot.get("captures", [])), 416, "snapshot capture count")
    inventory = dict(snapshot.get("source_inventory", {}))
    _require_equal(inventory.get("entry_count"), 416, "snapshot inventory entries")
    _require_equal(inventory.get("existing_count"), 416, "existing source entries")
    _require_equal(inventory.get("missing_count"), 0, "missing source entries")
    _require_equal(
        inventory.get("inventory_sha256"),
        SOURCE_INVENTORY_SHA256,
        "source inventory SHA256",
    )
    _require_equal(
        inventory.get("source_fingerprint"),
        SOURCE_FINGERPRINT,
        "source fingerprint",
    )
    _require_equal(inventory.get("total_bytes"), SOURCE_TOTAL_BYTES, "source bytes")
    universe = dict(snapshot.get("universe", {}))
    _require_equal(universe.get("master_date"), "2026-08-11", "universe date")
    _require_equal(universe.get("contract_month_filter"), "26AUG", "contract month")
    _require_equal(universe.get("mapped_stock_futures"), 208, "mapped stocks")
    _require_equal(
        universe.get("file_sha256"), UNIVERSE_FILE_SHA256, "universe file SHA256"
    )
    return snapshot


def _output_record(payload: Mapping[str, Any], key: str) -> tuple[Path, dict[str, Any]]:
    record = dict(dict(payload.get("outputs", {})).get(key, {}))
    if not record:
        raise ValueError(f"Provenance is missing output record {key!r}")
    path = _resolved(str(record.get("path", "")))
    if not path.is_file():
        raise FileNotFoundError(f"Recorded output is missing: {path}")
    _require_equal(path.stat().st_size, int(record.get("size", -1)), f"{key} size")
    _require_equal(sha256_file(path), str(record.get("sha256", "")), f"{key} SHA256")
    return path, record


def _validate_run_outputs(
    submitted_provenance: Path,
    payload: Mapping[str, Any],
    *,
    stage7: bool,
) -> tuple[Path, dict[str, Path]]:
    keys = [
        "cache_manifest_archive",
        "candidate_order_audit",
        "coverage",
        "daily",
        "diagnostic_breakdowns",
        "report",
        "setups",
        "state_events",
        "strategy_source_archive",
    ]
    if stage7:
        keys.extend(
            [
                "experiment_config_source_archive",
                "experiment_runner_source_archive",
                "launcher_source_archive",
                "locked_backtest_config_source_archive",
                "locked_backtest_launcher_source_archive",
                "neutral_engine_source_archive",
                "resolved_experiment_config",
                "resolved_locked_backtest_profile",
                "selection_decisions",
            ]
        )
    observed_keys = set(dict(payload.get("outputs", {})))
    _require_equal(
        observed_keys,
        set(keys),
        "Stage7 output registry" if stage7 else "V8 output registry",
    )
    paths = {key: _output_record(payload, key)[0] for key in keys}
    run_dirs = {path.parent.resolve() for path in paths.values()}
    if len(run_dirs) != 1:
        raise AssertionError("A run's archived outputs do not share one directory")
    run_dir = next(iter(run_dirs))
    archived_provenance = run_dir / "provenance.json"
    if not archived_provenance.is_file():
        raise FileNotFoundError(archived_provenance)
    if sha256_file(archived_provenance) != sha256_file(submitted_provenance):
        raise AssertionError(
            "Submitted provenance is not byte-identical to the run archive: "
            f"{submitted_provenance}"
        )
    return run_dir, paths


def _validate_common_run_contract(
    label: str,
    payload: Mapping[str, Any],
    spec: Mapping[str, Any],
) -> None:
    _require_equal(payload.get("schema_version"), "fno_backtest_run_provenance_v1", f"{label} provenance schema")
    _require_equal(payload.get("provenance_claim"), "RECREATED_CURRENT_SOURCE_REPLAY_NOT_ORIGINAL_SELECTION_PROVENANCE", f"{label} provenance claim")
    _require_equal(payload.get("objective"), spec["objective"], f"{label} objective")
    _require_equal(payload.get("strategy_version"), spec["strategy_prefix"], f"{label} strategy version")
    _require_equal(payload.get("promotion_eligible"), False, f"{label} promotion eligibility")
    _require_equal(
        payload.get("original_selection_source_provenance_available"),
        False,
        f"{label} original selection provenance availability",
    )
    _require_equal(payload.get("strategy_source_sha256"), NEUTRAL_ENGINE_SHA256, f"{label} neutral engine SHA256")
    _require_equal(
        payload.get("current_strategy_source_matches_archive"),
        True,
        f"{label} current strategy/archive binding",
    )

    window = dict(payload.get("backtest_window", {}))
    expected_window = {
        "from_day": FROM_DAY,
        "split_day": SPLIT_DAY,
        "through_day": THROUGH_DAY,
    }
    _require_equal(window, expected_window, f"{label} backtest window")

    parameters = dict(payload.get("parameters", {}))
    for key, expected in expected_window.items():
        _require_equal(parameters.get(key), expected, f"{label} parameters.{key}")
    _require_equal(parameters.get("variant"), spec["variant"], f"{label} variant")
    _require_equal(parameters.get("portfolio_mode"), PORTFOLIO_MODE, f"{label} portfolio mode")
    _require_number(parameters.get("target_exposure_per_entry_rs"), TARGET_EXPOSURE_RS, f"{label} target exposure")
    entry = dict(parameters.get("entry_policy", {}))
    _require_number(entry.get("cost_bps"), COST_BPS, f"{label} cost bps")
    _require_number(entry.get("slippage_bps"), SLIPPAGE_BPS, f"{label} slippage bps")
    _require_equal(entry.get("square_off"), SQUARE_OFF, f"{label} square off")
    _require_equal(entry.get("eod_policy"), EOD_POLICY, f"{label} EOD policy")
    _require_equal(entry.get("entry_expiry_minute"), 5, f"{label} entry expiry")
    _require_equal(entry.get("same_bar_policy"), "STOP_FIRST", f"{label} same-bar policy")
    _require_equal(entry.get("post_confirmation_cancel"), True, f"{label} post-confirmation cancel")
    _require_equal(entry.get("allow_cap_reassignment"), True, f"{label} cap reassignment")
    _require_number(entry.get("buffer_bps"), 0.0, f"{label} buffer bps")
    _require_equal(entry.get("close_location_min"), None, f"{label} close location")
    _require_equal(entry.get("max_confirmation_minute"), 1, f"{label} max confirmation")
    _require_equal(entry.get("midpoint_invalidation"), False, f"{label} midpoint invalidation")
    _require_equal(entry.get("confirmation_volume_ratio_min"), None, f"{label} RV1 entry filter")

    snapshot = dict(payload.get("source_snapshot", {}))
    _require_same_path(snapshot.get("manifest_path", ""), SNAPSHOT_MANIFEST, f"{label} snapshot path")
    _require_equal(snapshot.get("snapshot_fingerprint"), SNAPSHOT_FINGERPRINT, f"{label} snapshot fingerprint")
    _require_equal(snapshot.get("physical_copy"), True, f"{label} physical snapshot")

    inventory = dict(payload.get("source_inventory", {}))
    _require_equal(inventory.get("entry_count"), 416, f"{label} inventory entries")
    _require_equal(inventory.get("existing_count"), 416, f"{label} existing inventory")
    _require_equal(inventory.get("missing_count"), 0, f"{label} missing inventory")
    _require_equal(inventory.get("inventory_sha256"), SOURCE_INVENTORY_SHA256, f"{label} inventory SHA256")
    _require_equal(inventory.get("source_fingerprint"), SOURCE_FINGERPRINT, f"{label} source fingerprint")
    _require_equal(inventory.get("total_bytes"), SOURCE_TOTAL_BYTES, f"{label} source bytes")

    universe = dict(payload.get("universe", {}))
    _require_equal(universe.get("master_date"), "2026-08-11", f"{label} universe date")
    _require_equal(universe.get("contract_month_filter"), "26AUG", f"{label} contract month")
    _require_equal(universe.get("mapped_stock_futures"), EXPECTED_SYMBOLS, f"{label} mapped symbols")
    _require_equal(universe.get("file_sha256"), UNIVERSE_FILE_SHA256, f"{label} universe SHA256")
    _require_equal(universe.get("universe_sha256"), UNIVERSE_SHA256, f"{label} universe contract SHA256")
    _require_equal(universe.get("mapped_universe_sha256"), MAPPED_UNIVERSE_SHA256, f"{label} mapped universe SHA256")
    _require_equal(universe.get("mapped_symbol_set_sha256"), MAPPED_SYMBOL_SET_SHA256, f"{label} mapped symbol set SHA256")

    _require_equal(payload.get("cache_input_fingerprint"), spec["cache_fingerprint"], f"{label} cache input fingerprint")
    strategy = dict(payload.get("strategy_payload", {}))
    _require_equal(strategy.get("setup_book_sha256"), spec["setup_book_sha256"], f"{label} setup book SHA256")
    _require_equal(strategy.get("configuration_source"), spec["configuration_source"], f"{label} configuration source")

    results = dict(payload.get("results", {}))
    _require_equal(results.get("sessions"), EXPECTED_SESSIONS, f"{label} sessions")
    _require_equal(results.get("split_day"), SPLIT_DAY, f"{label} result split")
    _require_equal(results.get("headline_valid"), False, f"{label} headline validity")
    _require_equal(results.get("promotion_eligible"), False, f"{label} result promotion eligibility")
    _require_equal(results.get("source_complete"), False, f"{label} source completeness")
    _require_equal(results.get("source_incomplete_symbol_sessions"), EXPECTED_INCOMPLETE_SYMBOL_SESSIONS, f"{label} incomplete symbol-sessions")
    _require_equal(results.get("unexpected_source_symbol_sessions"), 0, f"{label} unexpected symbol-sessions")
    _require_equal(results.get("unresolved_filled_trades"), 0, f"{label} unresolved fills")
    blockers = frozenset(str(item) for item in results.get("promotion_blockers", []))
    _require_equal(blockers, EXPECTED_PROMOTION_BLOCKERS, f"{label} promotion blockers")
    periods = dict(results.get("period_metrics", {}))
    _require_equal(dict(periods.get("TRAIN", {})).get("sessions"), EXPECTED_TRAIN_SESSIONS, f"{label} TRAIN sessions")
    _require_equal(dict(periods.get("TEST", {})).get("sessions"), EXPECTED_TEST_SESSIONS, f"{label} TEST sessions")


def _validate_cache_and_coverage(label: str, payload: Mapping[str, Any], paths: Mapping[str, Path]) -> None:
    manifest = load_json(paths["cache_manifest_archive"])
    _require_equal(manifest.get("complete"), True, f"{label} cache complete")
    _require_equal(manifest.get("schema_version"), RUN_SPECS[label]["cache_schema_version"], f"{label} cache schema")
    _require_equal(manifest.get("input_fingerprint"), RUN_SPECS[label]["cache_fingerprint"], f"{label} archived cache fingerprint")
    contract = dict(manifest.get("input_contract", {}))
    _require_equal(contract.get("schema_version"), RUN_SPECS[label]["cache_schema_version"], f"{label} input schema")
    _require_equal(contract.get("path_policy_version"), RUN_SPECS[label]["path_policy_version"], f"{label} path policy")
    _require_equal(contract.get("strategy_version"), payload.get("strategy_version"), f"{label} cache strategy version")
    _require_equal(contract.get("strategy_code_sha256"), NEUTRAL_ENGINE_SHA256, f"{label} cache source SHA256")
    _require_equal(contract.get("setup_book_sha256"), RUN_SPECS[label]["setup_book_sha256"], f"{label} cache setup SHA256")
    _require_equal(contract.get("from_day"), FROM_DAY, f"{label} cache from_day")
    _require_equal(contract.get("through_day"), THROUGH_DAY, f"{label} cache through_day")
    _require_equal(contract.get("snapshot_fingerprint"), SNAPSHOT_FINGERPRINT, f"{label} cache snapshot")
    _require_equal(contract.get("source_inventory_sha256"), SOURCE_INVENTORY_SHA256, f"{label} cache inventory SHA256")
    _require_equal(contract.get("source_fingerprint"), SOURCE_FINGERPRINT, f"{label} cache source fingerprint")
    _require_equal(contract.get("execution_instrument"), "NSE_CASH_EQUITY", f"{label} execution instrument")
    _require_equal(contract.get("oi_instrument"), "STATIC_26AUG_NFO_FUTURE_RESEARCH_ONLY", f"{label} OI instrument")
    _require_equal(contract.get("five_minute_construction"), "FIVE_EXACT_REAL_END_LABELLED_NSE_1M_BARS", f"{label} five-minute construction")
    _require_equal(contract.get("timestamp_convention"), "CANDLE_END_ASIA_KOLKATA", f"{label} timestamp convention")
    _require_equal(contract.get("source_limitations"), EXPECTED_SOURCE_LIMITATIONS, f"{label} source limitations")
    _require_equal(dict(contract.get("universe", {})), dict(payload.get("universe", {})), f"{label} cache universe")
    _require_equal(len(contract.get("symbols", [])), EXPECTED_SYMBOLS, f"{label} cache symbols")
    calendar = dict(contract.get("session_calendar", {}))
    _require_equal(calendar.get("calendar_sha256"), SESSION_CALENDAR_SHA256, f"{label} calendar SHA256")
    _require_equal(calendar.get("expected_session_count"), EXPECTED_SESSIONS, f"{label} cache sessions")
    _require_equal(
        calendar.get("expected_session_dates"),
        [value.isoformat() for value in engine.expected_regular_session_dates(FROM_DAY, THROUGH_DAY)],
        f"{label} cache session dates",
    )
    _require_equal(manifest.get("complete_symbol_sessions"), EXPECTED_COMPLETE_SYMBOL_SESSIONS, f"{label} complete symbol-sessions")
    _require_equal(manifest.get("source_incomplete_symbol_sessions"), EXPECTED_INCOMPLETE_SYMBOL_SESSIONS, f"{label} cache incomplete symbol-sessions")
    _require_equal(manifest.get("expected_symbol_sessions"), EXPECTED_SYMBOL_SESSIONS, f"{label} expected symbol-sessions")
    _require_equal(manifest.get("headline_source_complete"), False, f"{label} cache headline completeness")

    coverage = pd.read_csv(paths["coverage"], keep_default_na=False)
    # The exported coverage artifact is one row per symbol.  Its JSON date
    # partitions, rather than one physical row per symbol-session, are the
    # authoritative source for the 12,272-symbol-session headline counts.
    _require_equal(len(coverage), EXPECTED_SYMBOLS, f"{label} coverage rows")
    derived = engine.derive_coverage_completeness(
        coverage,
        selected_symbols=contract.get("symbols", []),
        expected_session_dates=calendar.get("expected_session_dates", []),
    )
    expected_coverage = {
        "coverage_symbol_count": EXPECTED_SYMBOLS,
        "expected_symbol_sessions": EXPECTED_SYMBOL_SESSIONS,
        "complete_symbol_sessions": EXPECTED_COMPLETE_SYMBOL_SESSIONS,
        "source_incomplete_symbol_sessions": EXPECTED_INCOMPLETE_SYMBOL_SESSIONS,
        "unexpected_source_symbol_sessions": 0,
        "headline_source_complete": False,
    }
    _require_equal(derived, expected_coverage, f"{label} derived coverage")
    for key, expected in expected_coverage.items():
        _require_equal(manifest.get(key), expected, f"{label} manifest {key}")


def _validate_stage7_identity(payload: Mapping[str, Any]) -> None:
    _require_equal(payload.get("research_only"), True, "Stage7 research_only")
    _require_equal(payload.get("promotion_eligible"), False, "Stage7 promotion_eligible")
    _require_equal(
        payload.get("v10_locked_backtest_run_schema_version"),
        locked_launcher.LOCKED_RUN_SCHEMA_VERSION,
        "Stage7 locked run schema",
    )
    _require_equal(
        payload.get("v10_locked_backtest_profile_id"),
        locked_config.PROFILE_ID,
        "Stage7 locked profile id",
    )
    _require_equal(
        payload.get("v10_locked_backtest_profile_sha256"),
        LOCKED_PROFILE_SHA256,
        "Stage7 locked profile SHA256",
    )
    _require_equal(
        common.canonical_json_sha256(
            payload.get("v10_locked_backtest_profile")
        ),
        LOCKED_PROFILE_SHA256,
        "Stage7 locked profile payload hash",
    )
    _require_equal(
        payload.get("v10_locked_backtest_authority"),
        "BACKTEST_ONLY",
        "Stage7 locked authority",
    )
    _require_equal(
        payload.get("locked_backtest_launcher_source_sha256"),
        LOCKED_V10_LAUNCHER_SHA256,
        "Stage7 locked launcher SHA256",
    )
    _require_equal(
        payload.get("locked_backtest_config_source_sha256"),
        LOCKED_V10_CONFIG_FILE_SHA256,
        "Stage7 locked config SHA256",
    )
    _require_equal(
        payload.get("current_locked_launcher_matches_archive"),
        True,
        "Stage7 current locked launcher/archive binding",
    )
    _require_equal(
        payload.get("current_locked_config_matches_archive"),
        True,
        "Stage7 current locked config/archive binding",
    )
    _require_equal(payload.get("v10_experiment_variant"), "0940_LONG_MOVE_040", "Stage7 variant identity")
    _require_equal(payload.get("v10_experiment_registry_sha256"), EXPERIMENT_REGISTRY_SHA256, "Stage7 registry SHA256")
    _require_equal(payload.get("v10_experiment_variant_config_sha256"), STAGE7_VARIANT_CONFIG_SHA256, "Stage7 variant config SHA256")
    _require_equal(payload.get("experiment_runner_source_sha256"), EXPERIMENT_RUNNER_SHA256, "Stage7 experiment runner SHA256")
    _require_equal(payload.get("experiment_config_source_sha256"), EXPERIMENT_CONFIG_FILE_SHA256, "Stage7 experiment config SHA256")
    _require_equal(payload.get("neutral_engine_source_sha256"), NEUTRAL_ENGINE_SHA256, "Stage7 neutral engine SHA256")
    _require_equal(payload.get("launcher_source_sha256"), UNIFIED_V10_LAUNCHER_SHA256, "Stage7 unified launcher SHA256")
    _require_equal(payload.get("current_strategy_source_matches_archive"), True, "Stage7 strategy source/archive binding")
    _require_equal(payload.get("current_launcher_source_matches_archive"), True, "Stage7 unified launcher/archive binding")
    _require_equal(payload.get("current_neutral_engine_source_matches_archive"), True, "Stage7 neutral engine/archive binding")
    _require_equal(payload.get("current_experiment_runner_matches_archive"), True, "Stage7 experiment runner/archive binding")
    _require_equal(payload.get("current_experiment_config_matches_archive"), True, "Stage7 experiment config/archive binding")
    _require_equal(payload.get("current_experiment_registry_matches_archive"), True, "Stage7 experiment registry/archive binding")
    _require_equal(payload.get("current_experiment_variant_matches_archive"), True, "Stage7 experiment variant/archive binding")
    _require_equal(payload.get("current_experiment_contract_matches_archive"), True, "Stage7 experiment contract/archive binding")
    resolved_path, _ = _output_record(payload, "resolved_experiment_config")
    resolved = load_json(resolved_path)
    selected = dict(resolved.get("selected_variant", {}))
    _require_equal(selected.get("variant"), "0940_LONG_MOVE_040", "resolved Stage7 variant")
    _require_equal(
        selected.get("price_threshold_overrides"),
        [{"setup_id": "09:40_LONG", "price_change_pct": 0.4}],
        "resolved Stage7 threshold",
    )
    _require_equal(selected.get("entry_expiry_minute"), 5, "resolved Stage7 expiry")
    _require_equal(selected.get("disabled_setup_ids"), [], "resolved Stage7 disabled setups")
    _require_equal(selected.get("slot_rvol20_min"), None, "resolved Stage7 RVOL")


def _validate_native_provenance(label: str, path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(path)
    if label == "V6_RULES_ON_V8_STRICT":
        strict_launcher.configure_engine()
        payload = engine.validate_v8_run_provenance(path)
    elif label == "V8_COMBINED":
        combined_launcher.configure_engine()
        payload = engine.validate_v8_run_provenance(path)
    elif label == "V10_STAGE7":
        experiment.configure_engine("0940_LONG_MOVE_040")
        payload = locked_launcher.validate_locked_run_provenance(path)
    else:  # pragma: no cover - internal programming guard
        raise ValueError(f"Unknown run label: {label}")
    if not isinstance(payload, dict):
        raise AssertionError(f"{label} native validator did not return provenance")
    return payload


def _bool_series(series: pd.Series, label: str) -> pd.Series:
    normalized = series.astype(str).str.strip().str.lower()
    allowed = {"true", "false"}
    observed = set(normalized.unique())
    if not observed.issubset(allowed):
        raise AssertionError(f"{label} contains non-Boolean values: {sorted(observed)}")
    return normalized.eq("true")


def _load_audit(path: Path, label: str) -> pd.DataFrame:
    frame = pd.read_csv(path, low_memory=False)
    required = {
        "candidate_id",
        "session_date",
        "signal_time",
        "setup_id",
        "side",
        "symbol",
        "futures_symbol",
        "frozen_rank",
        "picker",
        "picker_value",
        "price_change_pct",
        "traded_value",
        "status",
        "filled",
        "entry_time",
        "exit_time",
        "exit_reason",
        "net_return_pct",
        "net_pnl_rs",
    }
    if not required.issubset(frame.columns):
        raise AssertionError(f"{label} audit is missing {sorted(required - set(frame.columns))}")
    if (
        frame.empty
        or frame["candidate_id"].isna().any()
        or frame["candidate_id"].astype(str).str.strip().eq("").any()
        or frame["candidate_id"].astype(str).duplicated().any()
    ):
        raise AssertionError(f"{label} audit must contain unique candidates")
    if "schema_version" not in frame.columns or not frame["schema_version"].astype(str).eq(
        "fno_v8_windowed_1m_trade_v3"
    ).all():
        raise AssertionError(f"{label} audit schema is not frozen V8 trade v3")
    frame = frame.copy()
    frame["candidate_id"] = frame["candidate_id"].astype(str)
    frame["session_date"] = pd.to_datetime(frame["session_date"], errors="raise").dt.strftime("%Y-%m-%d")
    official_dates = {
        value.isoformat()
        for value in engine.expected_regular_session_dates(FROM_DAY, THROUGH_DAY)
    }
    unexpected_dates = sorted(set(frame["session_date"]) - official_dates)
    if unexpected_dates:
        raise AssertionError(
            f"{label} audit contains non-official session dates: {unexpected_dates}"
        )
    frame["filled_bool"] = _bool_series(frame["filled"], f"{label} filled")
    for column in ("net_return_pct", "net_pnl_rs"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    unresolved = frame["filled_bool"] & (
        ~np.isfinite(frame["net_return_pct"]) | ~np.isfinite(frame["net_pnl_rs"])
    )
    if unresolved.any():
        raise AssertionError(f"{label} contains unresolved filled trades")
    return frame


def _load_daily(path: Path, label: str) -> pd.DataFrame:
    frame = pd.read_csv(path)
    required = {"session_date", "period", "candidates", "fills", "net_return_pct", "net_pnl_rs"}
    if not required.issubset(frame.columns):
        raise AssertionError(f"{label} daily is missing {sorted(required - set(frame.columns))}")
    if len(frame) != EXPECTED_SESSIONS:
        raise AssertionError(f"{label} daily requires {EXPECTED_SESSIONS} rows")
    frame = frame.copy()
    frame["session_date"] = pd.to_datetime(frame["session_date"], errors="raise").dt.strftime("%Y-%m-%d")
    if frame["session_date"].duplicated().any() or not frame["session_date"].is_monotonic_increasing:
        raise AssertionError(f"{label} daily dates must be unique and ordered")
    expected_dates = [value.isoformat() for value in engine.expected_regular_session_dates(FROM_DAY, THROUGH_DAY)]
    _require_equal(frame["session_date"].tolist(), expected_dates, f"{label} daily calendar")
    expected_period = np.where(frame["session_date"].lt(SPLIT_DAY), "TRAIN", "TEST")
    _require_equal(frame["period"].astype(str).tolist(), expected_period.tolist(), f"{label} daily periods")
    _require_equal(int((frame["period"] == "TRAIN").sum()), EXPECTED_TRAIN_SESSIONS, f"{label} TRAIN row count")
    _require_equal(int((frame["period"] == "TEST").sum()), EXPECTED_TEST_SESSIONS, f"{label} TEST row count")
    for column in ("candidates", "fills", "net_return_pct", "net_pnl_rs"):
        frame[column] = pd.to_numeric(frame[column], errors="raise")
        if not np.isfinite(frame[column]).all():
            raise AssertionError(f"{label} daily {column} contains non-finite values")
    for column in ("candidates", "fills"):
        if frame[column].lt(0).any() or not np.equal(frame[column], np.floor(frame[column])).all():
            raise AssertionError(f"{label} daily {column} must be non-negative integers")
    if frame["fills"].gt(frame["candidates"]).any():
        raise AssertionError(f"{label} daily fills exceed candidates")
    return frame


def _period_mask(frame: pd.DataFrame, period: str) -> pd.Series:
    if period == "FULL":
        return pd.Series(True, index=frame.index, dtype=bool)
    if period == "TRAIN":
        return frame["session_date"].lt(SPLIT_DAY)
    if period == "TEST":
        return frame["session_date"].ge(SPLIT_DAY)
    raise ValueError(f"Unknown period {period!r}")


def _metric_values(audit: pd.DataFrame, daily: pd.DataFrame) -> dict[str, Any]:
    filled = audit.loc[audit["filled_bool"]].copy()
    returns = filled["net_return_pct"].astype(float)
    pnl = filled["net_pnl_rs"].astype(float)
    wins = int(returns.gt(0).sum())
    losses = int(returns.lt(0).sum())
    flats = int(returns.eq(0).sum())
    profits = float(returns.loc[returns.gt(0)].sum())
    loss_points = float(-returns.loc[returns.lt(0)].sum())
    profit_factor: float | None
    if loss_points > 0:
        profit_factor = profits / loss_points
    elif profits > 0:
        profit_factor = math.inf
    else:
        profit_factor = None
    day_returns = daily["net_return_pct"].astype(float).to_numpy()
    cumulative = np.concatenate(([0.0], np.cumsum(day_returns)))
    drawdown = cumulative - np.maximum.accumulate(cumulative)
    max_drawdown = float(-drawdown.min()) if len(drawdown) else 0.0
    return {
        "sessions": int(len(daily)),
        "active_sessions": int((daily["fills"].astype(int) > 0).sum()),
        "candidates": int(len(audit)),
        "fills": int(len(filled)),
        "wins": wins,
        "losses": losses,
        "flat_trades": flats,
        "win_rate_pct": (100.0 * wins / len(filled)) if len(filled) else None,
        "profit_factor": profit_factor,
        "net_return_points": float(returns.sum()),
        "net_pnl_rs": float(pnl.sum()),
        "max_daily_drawdown_points": max_drawdown,
        "positive_days": int((daily["net_return_pct"] > 0).sum()),
        "negative_days": int((daily["net_return_pct"] < 0).sum()),
        "flat_days": int((daily["net_return_pct"] == 0).sum()),
    }


def _period_metrics(audit: pd.DataFrame, daily: pd.DataFrame, period: str) -> dict[str, Any]:
    audit_part = audit.loc[_period_mask(audit, period)].copy()
    daily_part = daily.loc[_period_mask(daily, period)].copy()
    return _metric_values(audit_part, daily_part)


def _validate_recomputed_metrics(label: str, payload: Mapping[str, Any], audit: pd.DataFrame, daily: pd.DataFrame) -> None:
    result = dict(payload.get("results", {}))
    full = _period_metrics(audit, daily, "FULL")
    _require_equal(full["candidates"], result.get("candidates"), f"{label} recomputed candidates")
    _require_equal(full["fills"], result.get("fills"), f"{label} recomputed fills")
    _require_equal(full["fills"], result.get("closed_fills"), f"{label} recomputed closed fills")
    observed_status = {
        str(key): int(value)
        for key, value in audit["status"].astype(str).value_counts().to_dict().items()
    }
    recorded_status = {
        str(key): int(value)
        for key, value in dict(result.get("status_counts", {})).items()
    }
    _require_equal(observed_status, recorded_status, f"{label} status counts")

    audit_daily = (
        audit.assign(
            filled_count=audit["filled_bool"].astype(int),
            filled_return=np.where(audit["filled_bool"], audit["net_return_pct"], 0.0),
            filled_pnl=np.where(audit["filled_bool"], audit["net_pnl_rs"], 0.0),
        )
        .groupby("session_date", as_index=False)
        .agg(
            candidates=("candidate_id", "size"),
            fills=("filled_count", "sum"),
            net_return_pct=("filled_return", "sum"),
            net_pnl_rs=("filled_pnl", "sum"),
        )
    )
    reconciled = daily.merge(
        audit_daily,
        on="session_date",
        how="left",
        suffixes=("_daily", "_audit"),
        validate="one_to_one",
    ).fillna(
        {
            "candidates_audit": 0,
            "fills_audit": 0,
            "net_return_pct_audit": 0.0,
            "net_pnl_rs_audit": 0.0,
        }
    )
    for column in ("candidates", "fills"):
        if not np.array_equal(
            reconciled[f"{column}_daily"].astype(int).to_numpy(),
            reconciled[f"{column}_audit"].astype(int).to_numpy(),
        ):
            raise AssertionError(f"{label} daily {column} does not reconcile")
    for column in ("net_return_pct", "net_pnl_rs"):
        if not np.allclose(
            reconciled[f"{column}_daily"].astype(float).to_numpy(),
            reconciled[f"{column}_audit"].astype(float).to_numpy(),
            rtol=1e-12,
            atol=1e-12,
        ):
            raise AssertionError(f"{label} daily {column} does not reconcile")
    diagnostic = dict(result.get("diagnostic_closed_trade_metrics", {}))
    for metric, source_key in (
        ("profit_factor", "profit_factor"),
        ("net_return_points", "net_return_percentage_points"),
        ("net_pnl_rs", "net_pnl_rs"),
        ("max_daily_drawdown_points", "max_daily_drawdown_percentage_points"),
    ):
        if not _same_number(full[metric], diagnostic.get(source_key)):
            raise AssertionError(f"{label} recomputed {metric} differs from provenance")
    for period in ("TRAIN", "TEST"):
        metrics = _period_metrics(audit, daily, period)
        recorded = dict(dict(result.get("period_metrics", {})).get(period, {}))
        for metric, source_key in (
            ("sessions", "sessions"),
            ("fills", "fills"),
            ("profit_factor", "profit_factor"),
            ("net_return_points", "net_return_percentage_points"),
            ("positive_days", "positive_days"),
        ):
            if not _same_number(metrics[metric], recorded.get(source_key)):
                raise AssertionError(f"{label} recomputed {period} {metric} differs from provenance")


def _summary_rows(run_data: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    metrics = {
        label: _period_metrics(data["audit"], data["daily"], "FULL")
        for label, data in run_data.items()
    }
    parent = metrics["V8_COMBINED"]
    rows: list[dict[str, Any]] = []
    for label in RUN_SPECS:
        payload = run_data[label]["payload"]
        values = metrics[label]
        row = {
            "engine": label,
            "variant": RUN_SPECS[label]["variant"],
            "from_day": FROM_DAY,
            "through_day": THROUGH_DAY,
            "split_day": SPLIT_DAY,
            **values,
            "delta_fills_vs_v8": values["fills"] - parent["fills"],
            "delta_win_rate_pct_vs_v8": (
                None
                if values["win_rate_pct"] is None or parent["win_rate_pct"] is None
                else values["win_rate_pct"] - parent["win_rate_pct"]
            ),
            "delta_profit_factor_vs_v8": (
                None
                if values["profit_factor"] is None or parent["profit_factor"] is None
                else values["profit_factor"] - parent["profit_factor"]
            ),
            "delta_net_return_points_vs_v8": values["net_return_points"] - parent["net_return_points"],
            "delta_net_pnl_rs_vs_v8": values["net_pnl_rs"] - parent["net_pnl_rs"],
            "delta_max_drawdown_points_vs_v8": values["max_daily_drawdown_points"] - parent["max_daily_drawdown_points"],
            "headline_valid": dict(payload["results"])["headline_valid"],
            "promotion_eligible": dict(payload["results"])["promotion_eligible"],
            "research_only_package": True,
        }
        rows.append(row)
    return rows


def _daywise_rows(run_data: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for label in RUN_SPECS:
        audit = run_data[label]["audit"]
        daily = run_data[label]["daily"].copy()
        daily["cumulative_net_return_points"] = daily["net_return_pct"].cumsum()
        for record in daily.to_dict("records"):
            day = str(record["session_date"])
            day_audit = audit.loc[audit["session_date"].eq(day)]
            filled = day_audit.loc[day_audit["filled_bool"]]
            returns = filled["net_return_pct"]
            profits = float(returns.loc[returns.gt(0)].sum())
            losses = float(-returns.loc[returns.lt(0)].sum())
            pf = profits / losses if losses > 0 else (math.inf if profits > 0 else None)
            rows.append(
                {
                    "engine": label,
                    "variant": RUN_SPECS[label]["variant"],
                    "session_date": day,
                    "period": record["period"],
                    "candidates": int(record["candidates"]),
                    "fills": int(record["fills"]),
                    "wins": int(returns.gt(0).sum()),
                    "losses": int(returns.lt(0).sum()),
                    "flat_trades": int(returns.eq(0).sum()),
                    "profit_factor": pf,
                    "net_return_points": float(record["net_return_pct"]),
                    "net_pnl_rs": float(record["net_pnl_rs"]),
                    "cumulative_net_return_points": float(record["cumulative_net_return_points"]),
                }
            )
    return rows


def _train_test_rows(run_data: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    metrics: dict[tuple[str, str], dict[str, Any]] = {}
    for label in RUN_SPECS:
        for period in ("FULL", "TRAIN", "TEST"):
            metrics[(label, period)] = _period_metrics(
                run_data[label]["audit"], run_data[label]["daily"], period
            )
    rows: list[dict[str, Any]] = []
    for label in RUN_SPECS:
        for period in ("FULL", "TRAIN", "TEST"):
            values = metrics[(label, period)]
            parent = metrics[("V8_COMBINED", period)]
            rows.append(
                {
                    "engine": label,
                    "variant": RUN_SPECS[label]["variant"],
                    "period": period,
                    **values,
                    "delta_fills_vs_v8": values["fills"] - parent["fills"],
                    "delta_win_rate_pct_vs_v8": (
                        None
                        if values["win_rate_pct"] is None or parent["win_rate_pct"] is None
                        else values["win_rate_pct"] - parent["win_rate_pct"]
                    ),
                    "delta_profit_factor_vs_v8": (
                        None
                        if values["profit_factor"] is None or parent["profit_factor"] is None
                        else values["profit_factor"] - parent["profit_factor"]
                    ),
                    "delta_net_return_points_vs_v8": values["net_return_points"] - parent["net_return_points"],
                    "delta_net_pnl_rs_vs_v8": values["net_pnl_rs"] - parent["net_pnl_rs"],
                }
            )
    return rows


def _group_daily(audit: pd.DataFrame, calendar: pd.DataFrame) -> pd.DataFrame:
    filled = audit.loc[audit["filled_bool"]]
    trade_daily = (
        filled.groupby("session_date", as_index=False)
        .agg(
            fills=("candidate_id", "size"),
            net_return_pct=("net_return_pct", "sum"),
            net_pnl_rs=("net_pnl_rs", "sum"),
        )
    )
    candidate_daily = audit.groupby("session_date", as_index=False).agg(candidates=("candidate_id", "size"))
    daily = calendar[["session_date", "period"]].copy()
    daily = daily.merge(candidate_daily, on="session_date", how="left")
    daily = daily.merge(trade_daily, on="session_date", how="left")
    for column in ("candidates", "fills", "net_return_pct", "net_pnl_rs"):
        daily[column] = pd.to_numeric(daily[column], errors="coerce").fillna(0)
    return daily


def _side_setup_rows(run_data: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for label in RUN_SPECS:
        audit = run_data[label]["audit"]
        calendar = run_data[label]["daily"]
        dimensions = (
            ("side", ["LONG", "SHORT"]),
            ("setup_id", sorted(audit["setup_id"].astype(str).unique())),
        )
        for dimension, values in dimensions:
            for value in values:
                group = audit.loc[audit[dimension].astype(str).eq(value)].copy()
                group_daily = _group_daily(group, calendar)
                for period in ("FULL", "TRAIN", "TEST"):
                    metrics = _period_metrics(group, group_daily, period)
                    rows.append(
                        {
                            "engine": label,
                            "variant": RUN_SPECS[label]["variant"],
                            "dimension": dimension,
                            "value": value,
                            "period": period,
                            **metrics,
                        }
                    )
    return rows


def _validate_selection_decisions(
    decisions_path: Path,
    v8_audit: pd.DataFrame,
    stage7_audit: pd.DataFrame,
) -> pd.DataFrame:
    decisions = pd.read_csv(decisions_path, low_memory=False)
    required = {
        "candidate_id",
        "session_date",
        "signal_time",
        "setup_id",
        "side",
        "symbol",
        "price_change_pct",
        "picker",
        "picker_value",
        "traded_value",
        "original_frozen_rank",
        "recalculated_frozen_rank",
        "selection_passed",
        "selection_reason",
        "experiment_variant",
        "selection_overlay_id",
        "entry_expiry_minute",
        "variant_config_sha256",
        "schema_version",
    }
    if not required.issubset(decisions.columns):
        raise AssertionError(f"Stage7 selection decisions are missing {sorted(required - set(decisions.columns))}")
    if decisions.empty or decisions["candidate_id"].astype(str).duplicated().any():
        raise AssertionError("Stage7 selection decisions must contain unique candidates")
    decisions = decisions.copy()
    decisions["candidate_id"] = decisions["candidate_id"].astype(str)
    decisions["selection_passed_bool"] = _bool_series(decisions["selection_passed"], "Stage7 selection_passed")
    _require_equal(decisions["candidate_id"].tolist(), v8_audit["candidate_id"].tolist(), "Stage7 decision/V8 candidate identity and order")
    passed_ids = set(decisions.loc[decisions["selection_passed_bool"], "candidate_id"])
    _require_equal(passed_ids, set(stage7_audit["candidate_id"]), "Stage7 passed-decision/audit identity")
    _require_equal(
        decisions.loc[decisions["selection_passed_bool"], "candidate_id"].tolist(),
        stage7_audit["candidate_id"].tolist(),
        "Stage7 passed-decision/audit order",
    )
    passed_decisions = decisions.loc[decisions["selection_passed_bool"]].set_index(
        "candidate_id", drop=False
    )
    stage7_by_id = stage7_audit.set_index("candidate_id", drop=False).loc[
        passed_decisions.index
    ]
    recalculated_rank = pd.to_numeric(
        passed_decisions["recalculated_frozen_rank"], errors="raise"
    ).to_numpy()
    replay_rank = pd.to_numeric(
        stage7_by_id["frozen_rank"], errors="raise"
    ).to_numpy()
    if not np.allclose(
        recalculated_rank,
        replay_rank,
        rtol=0.0,
        atol=0.0,
        equal_nan=False,
    ):
        raise AssertionError(
            "Stage7 recalculated selection ranks differ from replay audit ranks"
        )
    _require_equal(set(decisions["experiment_variant"].astype(str)), {"0940_LONG_MOVE_040"}, "Stage7 decision variant")
    _require_equal(set(decisions["selection_overlay_id"].astype(str)), {"0940_LONG_MOVE_040"}, "Stage7 decision overlay")
    _require_equal(set(decisions["variant_config_sha256"].astype(str)), {STAGE7_VARIANT_CONFIG_SHA256}, "Stage7 decision config SHA256")
    _require_equal(set(pd.to_numeric(decisions["entry_expiry_minute"], errors="raise")), {5}, "Stage7 decision expiry")
    _require_equal(set(decisions["schema_version"].astype(str)), {"fno_v10_selection_decision_v1"}, "Stage7 decision schema")
    parent = v8_audit.set_index("candidate_id", drop=False).loc[decisions["candidate_id"]]
    for column in ("session_date", "signal_time", "setup_id", "side", "symbol", "picker"):
        if not decisions[column].astype(str).reset_index(drop=True).equals(
            parent[column].astype(str).reset_index(drop=True)
        ):
            raise AssertionError(f"Stage7 decision/V8 {column} lineage changed")
    for column in ("price_change_pct", "picker_value", "traded_value", "original_frozen_rank"):
        parent_column = "frozen_rank" if column == "original_frozen_rank" else column
        observed = pd.to_numeric(decisions[column], errors="raise").to_numpy()
        expected = pd.to_numeric(parent[parent_column], errors="raise").to_numpy()
        if not np.allclose(observed, expected, rtol=1e-12, atol=1e-12, equal_nan=True):
            raise AssertionError(f"Stage7 decision/V8 {column} lineage changed")
    prices = pd.to_numeric(decisions["price_change_pct"], errors="raise")
    expected_pass = ~decisions["setup_id"].astype(str).eq("09:40_LONG") | prices.ge(0.40)
    if not expected_pass.equals(decisions["selection_passed_bool"]):
        raise AssertionError("Stage7 decisions do not implement only the inclusive 09:40 LONG >= 0.40 gate")
    rejected = decisions.loc[~decisions["selection_passed_bool"]]
    if not decisions.loc[decisions["selection_passed_bool"], "selection_reason"].astype(str).eq("PASSED").all():
        raise AssertionError("Stage7 passed candidates have an unexpected reason")
    if not rejected.empty and not rejected["selection_reason"].astype(str).eq("PRICE_CHANGE_BELOW_VARIANT_MINIMUM").all():
        raise AssertionError("Stage7 rejected candidates have an unexpected reason")
    return decisions


def _clean_scalar(value: Any) -> Any:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return value


def _paired_rows(
    v8_audit: pd.DataFrame,
    stage7_audit: pd.DataFrame,
    decisions: pd.DataFrame,
) -> list[dict[str, Any]]:
    v8 = v8_audit.set_index("candidate_id", drop=False)
    stage7 = stage7_audit.set_index("candidate_id", drop=False)
    decision = decisions.set_index("candidate_id", drop=False)
    unexpected = set(stage7.index) - set(v8.index)
    if unexpected:
        raise AssertionError(f"Stage7 contains candidates absent from V8: {sorted(unexpected)[:5]}")
    rows: list[dict[str, Any]] = []
    identity_columns = ("session_date", "signal_time", "setup_id", "side", "symbol", "futures_symbol")
    for candidate_id in v8_audit["candidate_id"].tolist():
        parent = v8.loc[candidate_id]
        selected = candidate_id in stage7.index
        child = stage7.loc[candidate_id] if selected else None
        choice = decision.loc[candidate_id]
        _require_equal(bool(choice["selection_passed_bool"]), selected, f"paired selection {candidate_id}")
        if selected:
            for column in identity_columns:
                _require_equal(str(child[column]), str(parent[column]), f"paired {candidate_id} {column}")
        parent_filled = bool(parent["filled_bool"])
        child_filled = bool(child["filled_bool"]) if selected else False
        if parent_filled and child_filled:
            fill_pair = "BOTH_FILLED"
        elif parent_filled:
            fill_pair = "V8_ONLY_FILL"
        elif child_filled:
            fill_pair = "STAGE7_ONLY_FILL"
        else:
            fill_pair = "NEITHER_FILLED"
        parent_return = float(parent["net_return_pct"]) if parent_filled else 0.0
        child_return = float(child["net_return_pct"]) if child_filled else 0.0
        parent_pnl = float(parent["net_pnl_rs"]) if parent_filled else 0.0
        child_pnl = float(child["net_pnl_rs"]) if child_filled else 0.0
        rows.append(
            {
                "candidate_id": candidate_id,
                "session_date": str(parent["session_date"]),
                "period": "TRAIN" if str(parent["session_date"]) < SPLIT_DAY else "TEST",
                "signal_time": parent["signal_time"],
                "setup_id": parent["setup_id"],
                "side": parent["side"],
                "symbol": parent["symbol"],
                "price_change_pct": float(choice["price_change_pct"]),
                "selection_passed": selected,
                "selection_reason": choice["selection_reason"],
                "pair_class": "COMMON_CANDIDATE" if selected else "DROPPED_BY_STAGE7_FILTER",
                "fill_pair": fill_pair,
                "v8_status": parent["status"],
                "stage7_status": child["status"] if selected else None,
                "v8_filled": parent_filled,
                "stage7_filled": child_filled,
                "v8_entry_time": _clean_scalar(parent["entry_time"]),
                "stage7_entry_time": _clean_scalar(child["entry_time"]) if selected else None,
                "v8_exit_time": _clean_scalar(parent["exit_time"]),
                "stage7_exit_time": _clean_scalar(child["exit_time"]) if selected else None,
                "v8_exit_reason": _clean_scalar(parent["exit_reason"]),
                "stage7_exit_reason": _clean_scalar(child["exit_reason"]) if selected else None,
                "v8_net_return_pct": parent_return,
                "stage7_net_return_pct": child_return,
                "delta_net_return_pct": child_return - parent_return,
                "v8_net_pnl_rs": parent_pnl,
                "stage7_net_pnl_rs": child_pnl,
                "delta_net_pnl_rs": child_pnl - parent_pnl,
                "same_status": bool(selected and str(child["status"]) == str(parent["status"])),
                "same_filled_state": bool(selected and child_filled == parent_filled),
            }
        )
    return rows


def _paired_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "v8_candidates": len(rows),
        "common_candidates": sum(row["pair_class"] == "COMMON_CANDIDATE" for row in rows),
        "dropped_by_stage7_filter": sum(row["pair_class"] == "DROPPED_BY_STAGE7_FILTER" for row in rows),
        "both_filled": sum(row["fill_pair"] == "BOTH_FILLED" for row in rows),
        "v8_only_fills": sum(row["fill_pair"] == "V8_ONLY_FILL" for row in rows),
        "stage7_only_fills": sum(row["fill_pair"] == "STAGE7_ONLY_FILL" for row in rows),
        "neither_filled": sum(row["fill_pair"] == "NEITHER_FILLED" for row in rows),
        "delta_net_return_points": float(sum(float(row["delta_net_return_pct"]) for row in rows)),
        "delta_net_pnl_rs": float(sum(float(row["delta_net_pnl_rs"]) for row in rows)),
    }


def _test_results_payload(
    *,
    tests_passed: int | None,
    tests_failed: int | None,
    test_commands: Sequence[str],
) -> dict[str, Any]:
    if (tests_passed is None) != (tests_failed is None):
        raise ValueError("--tests-passed and --tests-failed must be supplied together")
    if tests_passed is not None and tests_passed < 0:
        raise ValueError("--tests-passed cannot be negative")
    if tests_failed is not None and tests_failed < 0:
        raise ValueError("--tests-failed cannot be negative")
    if tests_failed:
        raise AssertionError("Cannot publish a frozen package with failing tests")
    commands = [str(command).strip() for command in test_commands]
    if any(not command for command in commands):
        raise ValueError("Recorded test commands cannot be empty")
    if tests_passed is not None and tests_passed <= 0:
        raise ValueError("PASS test evidence requires a positive passed-test count")
    if tests_passed is not None and not commands:
        raise ValueError("PASS test evidence requires at least one test command")
    return {
        "schema_version": TEST_SCHEMA_VERSION,
        "status": "COUNTS_NOT_RECORDED" if tests_passed is None else "PASS",
        "tests_passed": tests_passed,
        "tests_failed": tests_failed,
        "tests_total": None if tests_passed is None else tests_passed + int(tests_failed or 0),
        "commands": commands,
    }


def _artifact_inventory(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*"), key=lambda value: value.relative_to(root).as_posix()):
        if not path.is_file() or path.resolve() == (root / "stage_manifest.json").resolve():
            continue
        rows.append(
            {
                "path": path.relative_to(root).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    return rows


def _decision_text(
    summary_rows: Sequence[Mapping[str, Any]],
    paired: Mapping[str, Any],
    test_results: Mapping[str, Any],
    run_data: Mapping[str, Mapping[str, Any]],
) -> str:
    by_label = {str(row["engine"]): row for row in summary_rows}
    lines = [
        "# FNO V6 / V8 / V10 Stage 7 - frozen 59-session comparison",
        "",
        f"- Decision: **{DECISION}**",
        f"- Window: {FROM_DAY} through {THROUGH_DAY} ({EXPECTED_SESSIONS} sessions)",
        f"- Split: {SPLIT_DAY} ({EXPECTED_TRAIN_SESSIONS} TRAIN / {EXPECTED_TEST_SESSIONS} TEST)",
        f"- Economics: {COST_BPS:g} bps cost, {SLIPPAGE_BPS:g} bps slippage, {SQUARE_OFF} requested square-off",
        f"- EOD policy: `{EOD_POLICY}`",
        f"- Locked V10 profile SHA256: `{LOCKED_PROFILE_SHA256}`",
        f"- Source coverage: {EXPECTED_COMPLETE_SYMBOL_SESSIONS:,} complete / {EXPECTED_INCOMPLETE_SYMBOL_SESSIONS:,} incomplete of {EXPECTED_SYMBOL_SESSIONS:,} symbol-sessions",
        "",
        "| Engine | Fills | W/L | Win rate | PF | Net points | Net P&L proxy | Daily DD |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label in RUN_SPECS:
        row = by_label[label]
        lines.append(
            "| {engine} | {fills} | {wins}/{losses} | {win_rate:.4f}% | "
            "{pf:.6f} | {net:.6f} | Rs {pnl:.2f} | {dd:.6f} |".format(
                engine=label,
                fills=row["fills"],
                wins=row["wins"],
                losses=row["losses"],
                win_rate=float(row["win_rate_pct"]),
                pf=float(row["profit_factor"]),
                net=float(row["net_return_points"]),
                pnl=float(row["net_pnl_rs"]),
                dd=float(row["max_daily_drawdown_points"]),
            )
        )
    tests = (
        "not recorded"
        if test_results["tests_total"] is None
        else f"{test_results['tests_passed']} passed / {test_results['tests_failed']} failed"
    )
    lines.extend(
        [
            "",
            "## Paired Stage7 versus V8-Combined",
            "",
            f"- Common candidates: {paired['common_candidates']}",
            f"- Candidates removed by the Stage7 gate: {paired['dropped_by_stage7_filter']}",
            f"- Both-filled / V8-only / Stage7-only: {paired['both_filled']} / {paired['v8_only_fills']} / {paired['stage7_only_fills']}",
            f"- Stage7 minus V8 net return: {paired['delta_net_return_points']:.12f} points",
            f"- Stage7 minus V8 net P&L proxy: Rs {paired['delta_net_pnl_rs']:.2f}",
            "",
            "## Provenance and authority",
            "",
            f"- V6 rules on V8-Strict: `{run_data['V6_RULES_ON_V8_STRICT']['provenance_path']}`",
            f"- V8-Combined: `{run_data['V8_COMBINED']['provenance_path']}`",
            f"- V10 Stage7: `{run_data['V10_STAGE7']['provenance_path']}`",
            f"- Frozen snapshot: `{SNAPSHOT_MANIFEST.resolve()}`",
            f"- Test evidence: {tests}",
            "- The Stage7 run natively archives and hash-binds the locked V10 launcher, locked profile config, and resolved locked profile; the package revalidates those bindings against the frozen current sources.",
            "",
            "## Retained limitations",
            "",
            "- The universe is the later-dated static 2026-08-11 stock-futures universe.",
            "- OI uses static 26AUG contracts; it is not a point-in-time rolling near-month series for May-July.",
            "- Equity one-minute row lineage is unproven, source-slot coverage is incomplete, and exact 15:30 bars are unavailable for part of the window.",
            "- Stage7 was selected in prior retrospective research; this 59-session extension is descriptive, not a fresh untouched holdout.",
            "",
            "This package compares frozen research runs only. It grants no live or paper authority and does not promote any engine.",
            "",
        ]
    )
    return "\n".join(lines)


def build_package(
    *,
    v6_provenance: Path,
    v8_provenance: Path,
    stage7_provenance: Path,
    output_dir: Path,
    tests_passed: int | None = None,
    tests_failed: int | None = None,
    test_commands: Sequence[str] = (),
) -> Path:
    target = _resolved(output_dir)
    if target.exists():
        raise FileExistsError(f"Output directory already exists: {target}")

    source_bindings = _source_bindings()
    snapshot = _validate_snapshot_manifest()
    test_results = _test_results_payload(
        tests_passed=tests_passed,
        tests_failed=tests_failed,
        test_commands=test_commands,
    )

    submitted = {
        "V6_RULES_ON_V8_STRICT": _resolved(v6_provenance),
        "V8_COMBINED": _resolved(v8_provenance),
        "V10_STAGE7": _resolved(stage7_provenance),
    }
    if len(set(submitted.values())) != 3:
        raise AssertionError("The trio requires three distinct provenances")
    run_data: dict[str, dict[str, Any]] = {}
    for label in RUN_SPECS:
        path = submitted[label]
        payload = _validate_native_provenance(label, path)
        _validate_common_run_contract(label, payload, RUN_SPECS[label])
        if label == "V10_STAGE7":
            _validate_stage7_identity(payload)
        run_dir, paths = _validate_run_outputs(path, payload, stage7=label == "V10_STAGE7")
        _validate_cache_and_coverage(label, payload, paths)
        audit = _load_audit(paths["candidate_order_audit"], label)
        daily = _load_daily(paths["daily"], label)
        _validate_recomputed_metrics(label, payload, audit, daily)
        run_data[label] = {
            "payload": payload,
            "provenance_path": path,
            "provenance_sha256": sha256_file(path),
            "run_dir": run_dir,
            "paths": paths,
            "audit": audit,
            "daily": daily,
        }

    coverage_hashes = {
        label: sha256_file(data["paths"]["coverage"])
        for label, data in run_data.items()
    }
    if len(set(coverage_hashes.values())) != 1:
        raise AssertionError(
            f"The trio does not share byte-identical source coverage: {coverage_hashes}"
        )
    _require_equal(
        sha256_file(run_data["V8_COMBINED"]["paths"]["setups"]),
        sha256_file(run_data["V10_STAGE7"]["paths"]["setups"]),
        "V8-Combined/Stage7 setup artifact parity",
    )

    # V8-Combined is Stage7's exact parent selection stream before the one
    # frozen 09:40 LONG threshold is applied.
    decisions = _validate_selection_decisions(
        run_data["V10_STAGE7"]["paths"]["selection_decisions"],
        run_data["V8_COMBINED"]["audit"],
        run_data["V10_STAGE7"]["audit"],
    )
    paired_rows = _paired_rows(
        run_data["V8_COMBINED"]["audit"],
        run_data["V10_STAGE7"]["audit"],
        decisions,
    )
    paired_summary = _paired_summary(paired_rows)
    summary_rows = _summary_rows(run_data)
    daywise_rows = _daywise_rows(run_data)
    train_test_rows = _train_test_rows(run_data)
    side_setup_rows = _side_setup_rows(run_data)

    target.mkdir(parents=True, exist_ok=False)
    provenance_archive_dir = target / "provenance"
    provenance_archive_dir.mkdir(exist_ok=False)
    packaged_provenances: dict[str, dict[str, Any]] = {}
    for label, data in run_data.items():
        destination = provenance_archive_dir / PROVENANCE_ARCHIVE_NAMES[label]
        atomic_copy_verified(
            data["provenance_path"],
            destination,
            data["provenance_sha256"],
        )
        packaged_provenances[label] = {
            "path": destination.relative_to(target).as_posix(),
            "bytes": destination.stat().st_size,
            "sha256": data["provenance_sha256"],
        }
    atomic_write_csv(target / "summary.csv", summary_rows)
    atomic_write_csv(target / "daywise.csv", daywise_rows)
    atomic_write_csv(target / "train_test.csv", train_test_rows)
    atomic_write_csv(target / "side_setup.csv", side_setup_rows)
    atomic_write_csv(target / "paired_stage7_vs_v8.csv", paired_rows)
    atomic_write_text(
        target / "decision.md",
        _decision_text(summary_rows, paired_summary, test_results, run_data),
    )
    atomic_write_text(
        target / "test_results.json",
        json.dumps(test_results, indent=2, sort_keys=True) + "\n",
    )

    artifact_inventory = _artifact_inventory(target)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": DECISION,
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
        "comparison_contract": {
            "from_day": FROM_DAY,
            "through_day": THROUGH_DAY,
            "split_day": SPLIT_DAY,
            "sessions": EXPECTED_SESSIONS,
            "train_sessions": EXPECTED_TRAIN_SESSIONS,
            "test_sessions": EXPECTED_TEST_SESSIONS,
            "cost_bps": COST_BPS,
            "slippage_bps": SLIPPAGE_BPS,
            "square_off": SQUARE_OFF,
            "eod_policy": EOD_POLICY,
            "target_exposure_per_entry_rs": TARGET_EXPOSURE_RS,
            "portfolio_mode": PORTFOLIO_MODE,
            "expected_symbol_sessions": EXPECTED_SYMBOL_SESSIONS,
            "complete_symbol_sessions": EXPECTED_COMPLETE_SYMBOL_SESSIONS,
            "source_incomplete_symbol_sessions": EXPECTED_INCOMPLETE_SYMBOL_SESSIONS,
        },
        "snapshot_binding": {
            "manifest_path": str(SNAPSHOT_MANIFEST.resolve()),
            "manifest_sha256": SNAPSHOT_MANIFEST_SHA256,
            "snapshot_fingerprint": SNAPSHOT_FINGERPRINT,
            "source_inventory_sha256": SOURCE_INVENTORY_SHA256,
            "source_fingerprint": SOURCE_FINGERPRINT,
            "capture_scope": snapshot["capture_scope"],
        },
        "locked_profile_binding": {
            "profile_id": locked_config.PROFILE_ID,
            "profile_sha256": LOCKED_PROFILE_SHA256,
            "profile": locked_config.locked_profile_payload(),
            "native_run_provenance_archives_outer_locked_front_door": True,
            "package_time_source_attestation": True,
        },
        "source_bindings": source_bindings,
        "runs": {
            label: {
                "engine_label": label,
                "variant": RUN_SPECS[label]["variant"],
                "submitted_provenance": str(data["provenance_path"]),
                "provenance_sha256": data["provenance_sha256"],
                "packaged_provenance": packaged_provenances[label],
                "run_dir": str(data["run_dir"]),
                "backtest_input_fingerprint": data["payload"].get("backtest_input_fingerprint"),
                "cache_input_fingerprint": data["payload"].get("cache_input_fingerprint"),
                "strategy_version": data["payload"].get("strategy_version"),
                "objective": data["payload"].get("objective"),
                "native_provenance_validation": "PASS",
            }
            for label, data in run_data.items()
        },
        "paired_stage7_vs_v8": paired_summary,
        "test_evidence": test_results,
        "manifest_scope": "ALL_REGULAR_FILES_RECURSIVELY_EXCEPT_STAGE_MANIFEST_ITSELF",
        "artifact_count": len(artifact_inventory),
        "artifacts": artifact_inventory,
        "packager": {
            "path": str(Path(__file__).resolve()),
            "sha256": sha256_file(Path(__file__).resolve()),
        },
    }
    atomic_write_text(
        target / "stage_manifest.json",
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
    )

    # Final fail-closed readback: every pre-manifest artifact must still match
    # the bytes recorded in the manifest, and no unexpected file may exist.
    readback = load_json(target / "stage_manifest.json")
    recorded = {row["path"]: row for row in readback["artifacts"]}
    current = {row["path"]: row for row in _artifact_inventory(target)}
    _require_equal(current, recorded, "recursive package artifact inventory")
    _require_equal(len(current), 10, "package artifact count excluding manifest")
    return target


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v6-provenance", type=Path, required=True)
    parser.add_argument("--v8-provenance", type=Path, required=True)
    parser.add_argument("--stage7-provenance", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--tests-passed", type=int)
    parser.add_argument("--tests-failed", type=int)
    parser.add_argument(
        "--test-command",
        action="append",
        default=[],
        help="Repeat to record each executed test command.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    output = build_package(
        v6_provenance=args.v6_provenance,
        v8_provenance=args.v8_provenance,
        stage7_provenance=args.stage7_provenance,
        output_dir=args.output_dir,
        tests_passed=args.tests_passed,
        tests_failed=args.tests_failed,
        test_commands=args.test_command,
    )
    print(output.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
