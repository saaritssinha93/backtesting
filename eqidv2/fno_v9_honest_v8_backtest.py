"""Independent V9-Honest launcher on the neutral V8 execution engine.

This launcher freezes the ten active V8-Combined legs as literals.  The
separate 09:50/09:55 TRAIN search selected no qualifying LONG or SHORT leg,
so those four attempted legs are retained only as disabled research lineage.
They are deliberately absent from ``ACTIVE_SETUPS`` and cannot produce an
order in this launcher.

The strategy remains research-only.  In particular, freezing an honest
negative selection result does not cure the static historical universe,
static 26AUG futures/OI lineage, or legacy one-minute row-lineage limits of
the source data.  Execution, portfolio, fill, cost and square-off semantics
come exclusively from :mod:`fno_v8_windowed_1m_entry_backtest`.
"""

from __future__ import annotations

import hashlib
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import fno_oi_common as common
import fno_v8_windowed_1m_entry_backtest as engine


STRATEGY_FAMILY = "FNO_V9_HONEST_V8_COMBINED_20260820"
V9_RUN_SCHEMA_VERSION = "fno_v9_honest_v8_combined_run_v1"
ACTIVE_SETUP_BOOK_SHA256 = (
    "ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675"
)

# Frozen TRAIN-only optimizer lineage.  These are records, not runtime
# dependencies: the launcher does not import or read the optimizer or repair
# programs.  Absolute paths identify the original immutable research run.
OPTIMIZER_SOURCE_SHA256 = (
    "baac01902807ffd6beb465cd890c2a1cae50213719f49421372d56600a5f99e2"
)
OPTIMIZER_SEARCH_RUN_FINGERPRINT = (
    "335057a1588c66762c1d20fceb8690ae132db7308aa100174e8ad0554f02b0c7"
)
OPTIMIZER_SEARCH_BACKTEST_FINGERPRINT = (
    "d94b3f15c6440f4394b1fb1d1717e2ff1680b3f9ebb391b4ca884d99595c4f73"
)
OPTIMIZER_SEARCH_RUN_DIR = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\v9_0950_0955_honest_v1\optimizer_runs"
    r"\search_20260820T235156342242+0530_335057a1588c"
)
OPTIMIZER_SELECTION_ARTIFACT_PATH = OPTIMIZER_SEARCH_RUN_DIR / "selection.json"
OPTIMIZER_SELECTION_ARTIFACT_SHA256 = (
    "b67b28ac2eaf223b762dbeaac83b579821169359a22a5850465da383d0c8181b"
)
OPTIMIZER_PROVENANCE_ARTIFACT_PATH = OPTIMIZER_SEARCH_RUN_DIR / "provenance.json"
OPTIMIZER_PROVENANCE_ARTIFACT_SHA256 = (
    "a1f091dd0de6f0f4332209f15be52e187de1958dfd3c6c6ba11998c205eb4ed9"
)

# Every output hash published by the frozen search provenance is repeated
# here so the negative selection cannot silently be rebound to another run.
OPTIMIZER_SEARCH_ARTIFACT_SHA256: dict[str, str] = {
    "optimizer_source_archive": OPTIMIZER_SOURCE_SHA256,
    "report": "39e83a51c95e1e871816672fd8a8a6b4d14a6514f321487dbf4df1249245fe5e",
    "selection": OPTIMIZER_SELECTION_ARTIFACT_SHA256,
    "trials_09:50_LONG": "75c99e9415997e49b158242e21eb42bf6fe0eae749d25afe5f4a97c14a20eb5b",
    "trials_09:50_SHORT": "3cecb0a31168c26bc76c1cd57a38d766988c997776e690291d3b83f2ea47f5a7",
    "trials_09:55_LONG": "aa1976329f99d00fbbef0836f1c5d728a675916407a660a8c829a47756206dc2",
    "trials_09:55_SHORT": "1b00f1aa1386c4cd976bf341f422982c4fcc636f76c5ad6eca5538da38dc286b",
    "v8_source_archive": "731473f37da02a5f65b31d800478bce714a277e492290839b63704adcb6c70fd",
}


S = engine.V8Setup
ACTIVE_SETUPS: tuple[engine.V8Setup, ...] = (
    # Retuned 09:25 LONG.
    S(
        "09:25", "LONG", 4, "max_move", 0.30, 0.10, 3.0, 0.00, 0.50,
        0.0, 0.40, 1.0, entry_conf_minute=3, entry_buffer_bps=0.0,
        entry_midpoint=False, entry_clv=None,
    ),
    # Retuned 09:25 SHORT.
    S(
        "09:25", "SHORT", 4, "max_move", 0.20, 0.10, 1.5, 0.60, 0.60,
        25_000_000.0, 0.50, 3.0, entry_conf_minute=3,
        entry_buffer_bps=2.0, entry_midpoint=False, entry_clv=None,
    ),
    # Common 09:30 LONG; retuned 09:30 SHORT.
    S("09:30", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.50, 0.50, 0.0, 1.00, 2.5),
    S(
        "09:30", "SHORT", 4, "max_volume", 0.20, 1.00, 1.0, 0.45,
        0.30, 25_000_000.0, 1.00, 4.0, entry_conf_minute=3,
        entry_buffer_bps=0.0, entry_midpoint=True, entry_clv=0.50,
    ),
    # Common 09:35 LONG/SHORT.
    S("09:35", "LONG", 1, "max_liquidity", 0.20, 0.10, 1.0, 0.60, 0.50, 0.0, 1.00, 2.5),
    S("09:35", "SHORT", 2, "max_liquidity", 0.50, 1.00, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    # Common 09:40 LONG; strict/less-weak 09:40 SHORT.
    S("09:40", "LONG", 1, "max_liquidity", 0.20, 0.10, 2.0, 0.50, 0.50, 0.0, 0.50, 2.5),
    S("09:40", "SHORT", 1, "max_move", 0.20, 0.10, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    # Common 09:45 LONG/SHORT.
    S("09:45", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    S("09:45", "SHORT", 1, "max_volume", 0.20, 0.75, 1.0, 0.40, 0.30, 0.0, 1.00, 2.0),
)

# Literal fail-closed registry.  There is intentionally no V8Setup object for
# any of these legs because no configuration passed the independent TRAIN
# guards.  Validation and TEST outcomes were never accessed.
DISABLED_SHADOW_LEGS: dict[str, dict[str, Any]] = {
    "09:50_LONG": {
        "signal_end": "09:50",
        "side": "LONG",
        "status": "DISABLED_SHADOW",
        "stage": "TRAIN",
        "reason": "NO_CONFIG_PASSED_INDEPENDENT_TRAIN_LEG_GUARDS",
        "attempted_configurations": 48,
        "permanently_disabled_for_search_run": True,
        "validation_outcomes_accessed": False,
        "test_outcomes_accessed": False,
        "selection_run_fingerprint": OPTIMIZER_SEARCH_RUN_FINGERPRINT,
        "selection_artifact_path": str(OPTIMIZER_SELECTION_ARTIFACT_PATH),
        "selection_artifact_fingerprint": OPTIMIZER_SELECTION_ARTIFACT_SHA256,
        "selection_artifact_sha256": OPTIMIZER_SELECTION_ARTIFACT_SHA256,
    },
    "09:50_SHORT": {
        "signal_end": "09:50",
        "side": "SHORT",
        "status": "DISABLED_SHADOW",
        "stage": "TRAIN",
        "reason": "NO_CONFIG_PASSED_INDEPENDENT_TRAIN_LEG_GUARDS",
        "attempted_configurations": 48,
        "permanently_disabled_for_search_run": True,
        "validation_outcomes_accessed": False,
        "test_outcomes_accessed": False,
        "selection_run_fingerprint": OPTIMIZER_SEARCH_RUN_FINGERPRINT,
        "selection_artifact_path": str(OPTIMIZER_SELECTION_ARTIFACT_PATH),
        "selection_artifact_fingerprint": OPTIMIZER_SELECTION_ARTIFACT_SHA256,
        "selection_artifact_sha256": OPTIMIZER_SELECTION_ARTIFACT_SHA256,
    },
    "09:55_LONG": {
        "signal_end": "09:55",
        "side": "LONG",
        "status": "DISABLED_SHADOW",
        "stage": "TRAIN",
        "reason": "NO_CONFIG_PASSED_INDEPENDENT_TRAIN_LEG_GUARDS",
        "attempted_configurations": 48,
        "permanently_disabled_for_search_run": True,
        "validation_outcomes_accessed": False,
        "test_outcomes_accessed": False,
        "selection_run_fingerprint": OPTIMIZER_SEARCH_RUN_FINGERPRINT,
        "selection_artifact_path": str(OPTIMIZER_SELECTION_ARTIFACT_PATH),
        "selection_artifact_fingerprint": OPTIMIZER_SELECTION_ARTIFACT_SHA256,
        "selection_artifact_sha256": OPTIMIZER_SELECTION_ARTIFACT_SHA256,
    },
    "09:55_SHORT": {
        "signal_end": "09:55",
        "side": "SHORT",
        "status": "DISABLED_SHADOW",
        "stage": "TRAIN",
        "reason": "NO_CONFIG_PASSED_INDEPENDENT_TRAIN_LEG_GUARDS",
        "attempted_configurations": 48,
        "permanently_disabled_for_search_run": True,
        "validation_outcomes_accessed": False,
        "test_outcomes_accessed": False,
        "selection_run_fingerprint": OPTIMIZER_SEARCH_RUN_FINGERPRINT,
        "selection_artifact_path": str(OPTIMIZER_SELECTION_ARTIFACT_PATH),
        "selection_artifact_fingerprint": OPTIMIZER_SELECTION_ARTIFACT_SHA256,
        "selection_artifact_sha256": OPTIMIZER_SELECTION_ARTIFACT_SHA256,
    },
}


ROOT = common.FNO_ROOT / "strategy_research" / "v9_honest_v8_combined_v1"

# Capture the neutral seams before ``configure_engine`` installs this
# launcher's adapters.  The adapters augment provenance without changing any
# candidate, entry, fill, portfolio or P&L logic.
_ORIGINAL_STRATEGY_PAYLOAD = engine.strategy_payload
_ORIGINAL_PROVENANCE_BUILDER = engine.provenance.build_run_provenance
_ORIGINAL_ENGINE_PROVENANCE_VALIDATOR = engine.validate_v8_run_provenance


def launcher_sha256() -> str:
    """Return the exact bytes hash of this independent launcher."""

    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def active_setup_payload() -> list[dict[str, Any]]:
    return [asdict(setup) for setup in ACTIVE_SETUPS]


def selection_lineage_payload() -> dict[str, Any]:
    """Return a fresh JSON-safe copy of the frozen negative selection record."""

    return {
        "optimizer_source_sha256": OPTIMIZER_SOURCE_SHA256,
        "search_run_fingerprint": OPTIMIZER_SEARCH_RUN_FINGERPRINT,
        "search_backtest_input_fingerprint": OPTIMIZER_SEARCH_BACKTEST_FINGERPRINT,
        "search_run_dir": str(OPTIMIZER_SEARCH_RUN_DIR),
        "selection_artifact_path": str(OPTIMIZER_SELECTION_ARTIFACT_PATH),
        "selection_artifact_fingerprint": OPTIMIZER_SELECTION_ARTIFACT_SHA256,
        "selection_artifact_sha256": OPTIMIZER_SELECTION_ARTIFACT_SHA256,
        "provenance_artifact_path": str(OPTIMIZER_PROVENANCE_ARTIFACT_PATH),
        "provenance_artifact_sha256": OPTIMIZER_PROVENANCE_ARTIFACT_SHA256,
        "search_output_sha256": dict(OPTIMIZER_SEARCH_ARTIFACT_SHA256),
        "selection_status": "NO_QUALIFYING_TRAIN_LEGS",
        "outcomes_accessed": ["TRAIN"],
        "coverage_mode": "RECTANGULAR_PANEL",
        "diagnostic_only": True,
        "lineage_certified": False,
        "pf_claim_eligible": False,
        "eligible_for_validation": False,
        "promotion_eligible": False,
    }


def disabled_shadow_payload() -> dict[str, dict[str, Any]]:
    return {key: dict(value) for key, value in DISABLED_SHADOW_LEGS.items()}


def validate_launcher_configuration() -> None:
    observed = common.canonical_json_sha256(active_setup_payload())
    if observed != ACTIVE_SETUP_BOOK_SHA256:
        raise AssertionError(
            "V9-Honest active setup book changed: "
            f"expected {ACTIVE_SETUP_BOOK_SHA256}, observed {observed}"
        )
    if len(ACTIVE_SETUPS) != 10 or len(
        {setup.setup_id for setup in ACTIVE_SETUPS}
    ) != 10:
        raise AssertionError("V9-Honest requires ten unique active setup legs")
    expected_active = {
        (slot, side)
        for slot in ("09:25", "09:30", "09:35", "09:40", "09:45")
        for side in ("LONG", "SHORT")
    }
    observed_active = {(setup.signal_end, setup.side) for setup in ACTIVE_SETUPS}
    if observed_active != expected_active:
        raise AssertionError("V9-Honest active legs differ from V8-Combined")
    expected_disabled = {
        "09:50_LONG",
        "09:50_SHORT",
        "09:55_LONG",
        "09:55_SHORT",
    }
    if set(DISABLED_SHADOW_LEGS) != expected_disabled:
        raise AssertionError("V9-Honest disabled later-leg registry changed")
    if expected_disabled & {setup.setup_id for setup in ACTIVE_SETUPS}:
        raise AssertionError("A disabled later leg leaked into ACTIVE_SETUPS")
    for leg_id, record in DISABLED_SHADOW_LEGS.items():
        if record.get("status") != "DISABLED_SHADOW" or record.get("stage") != "TRAIN":
            raise AssertionError(f"Disabled leg status changed: {leg_id}")
        if record.get("reason") != "NO_CONFIG_PASSED_INDEPENDENT_TRAIN_LEG_GUARDS":
            raise AssertionError(f"Disabled leg reason changed: {leg_id}")
        if record.get("selection_run_fingerprint") != OPTIMIZER_SEARCH_RUN_FINGERPRINT:
            raise AssertionError(f"Disabled leg search lineage changed: {leg_id}")
        if record.get("selection_artifact_sha256") != OPTIMIZER_SELECTION_ARTIFACT_SHA256:
            raise AssertionError(f"Disabled leg selection hash changed: {leg_id}")
        if record.get("selection_artifact_fingerprint") != OPTIMIZER_SELECTION_ARTIFACT_SHA256:
            raise AssertionError(f"Disabled leg selection fingerprint changed: {leg_id}")
    bound_hashes = {
        OPTIMIZER_SOURCE_SHA256,
        OPTIMIZER_SEARCH_RUN_FINGERPRINT,
        OPTIMIZER_SEARCH_BACKTEST_FINGERPRINT,
        OPTIMIZER_SELECTION_ARTIFACT_SHA256,
        OPTIMIZER_PROVENANCE_ARTIFACT_SHA256,
        *OPTIMIZER_SEARCH_ARTIFACT_SHA256.values(),
    }
    if any(len(value) != 64 or any(c not in "0123456789abcdef" for c in value) for value in bound_hashes):
        raise AssertionError("V9-Honest optimizer lineage contains an invalid SHA-256")


def _v9_strategy_payload() -> dict[str, Any]:
    payload = _ORIGINAL_STRATEGY_PAYLOAD()
    payload["v9_honest_launcher"] = {
        "schema_version": V9_RUN_SCHEMA_VERSION,
        "launcher_source_sha256": launcher_sha256(),
        "neutral_engine_source_sha256": engine._module_source_sha256(),
        "active_setup_book_sha256": ACTIVE_SETUP_BOOK_SHA256,
        "selection_lineage": selection_lineage_payload(),
        "disabled_shadow_legs": disabled_shadow_payload(),
        "active_later_legs": [],
        "research_only": True,
        "promotion_eligible": False,
    }
    return payload


def _build_v9_run_provenance(**kwargs: Any) -> dict[str, Any]:
    """Archive and bind both launcher and neutral-engine source bytes."""

    output_paths = dict(kwargs.get("output_paths", {}))
    engine_archive = Path(str(output_paths["strategy_source_archive"]))
    launcher_archive = engine_archive.parent / Path(__file__).name
    launcher_hash = launcher_sha256()
    engine.provenance.publish_immutable_copy(
        Path(__file__),
        launcher_archive,
        expected_sha256=launcher_hash,
    )
    # The engine already archived itself under strategy_source_archive.  The
    # explicit alias makes the two-source contract unambiguous to consumers.
    output_paths["launcher_source_archive"] = launcher_archive
    output_paths["neutral_engine_source_archive"] = engine_archive
    forwarded = dict(kwargs)
    forwarded["output_paths"] = output_paths
    payload = _ORIGINAL_PROVENANCE_BUILDER(**forwarded)
    payload["v9_honest_run_schema_version"] = V9_RUN_SCHEMA_VERSION
    payload["launcher_source_sha256"] = launcher_hash
    payload["neutral_engine_source_sha256"] = engine._module_source_sha256()
    payload["optimizer_selection_lineage"] = selection_lineage_payload()
    payload["disabled_shadow_legs"] = disabled_shadow_payload()
    payload["research_only"] = True
    payload["promotion_eligible"] = False
    return payload


def validate_v9_run_provenance(path: Path | str) -> dict[str, Any]:
    """Validate the neutral V8 record plus V9's two-source/selection binding."""

    payload = _ORIGINAL_ENGINE_PROVENANCE_VALIDATOR(path)
    if payload.get("v9_honest_run_schema_version") != V9_RUN_SCHEMA_VERSION:
        raise ValueError("Not a supported V9-Honest run provenance artifact")
    if payload.get("research_only") is not True or payload.get("promotion_eligible") is not False:
        raise AssertionError("V9-Honest research-only status changed")

    outputs = dict(payload.get("outputs", {}))
    required = {"launcher_source_archive", "neutral_engine_source_archive"}
    missing = sorted(required - set(outputs))
    if missing:
        raise ValueError(f"V9-Honest provenance is missing source archives: {missing}")
    for name in sorted(required):
        record = dict(outputs[name])
        if not engine.provenance.artifact_matches(record.get("path", ""), record):
            raise AssertionError(f"V9-Honest source artifact changed: {name}")

    launcher_record = dict(outputs["launcher_source_archive"])
    engine_record = dict(outputs["neutral_engine_source_archive"])
    launcher_archive_hash = engine.provenance.sha256_file(
        Path(str(launcher_record.get("path", "")))
    )
    engine_archive_hash = engine.provenance.sha256_file(
        Path(str(engine_record.get("path", "")))
    )
    if launcher_archive_hash != str(payload.get("launcher_source_sha256", "")):
        raise AssertionError("Archived V9-Honest launcher source hash is invalid")
    if engine_archive_hash != str(payload.get("neutral_engine_source_sha256", "")):
        raise AssertionError("Archived neutral V8 engine source hash is invalid")
    if engine_archive_hash != str(payload.get("strategy_source_sha256", "")):
        raise AssertionError("V9-Honest neutral engine alias disagrees with V8 provenance")

    expected_lineage = selection_lineage_payload()
    if common.canonical_json_sha256(payload.get("optimizer_selection_lineage", {})) != (
        common.canonical_json_sha256(expected_lineage)
    ):
        raise AssertionError("V9-Honest optimizer selection lineage changed")
    strategy_contract = dict(payload.get("strategy_payload", {})).get(
        "v9_honest_launcher", {}
    )
    strategy_contract = dict(strategy_contract)
    if common.canonical_json_sha256(
        strategy_contract.get("selection_lineage", {})
    ) != common.canonical_json_sha256(expected_lineage):
        raise AssertionError("V9-Honest strategy payload selection lineage changed")
    if str(strategy_contract.get("launcher_source_sha256", "")) != launcher_archive_hash:
        raise AssertionError("V9-Honest strategy payload launcher hash is invalid")
    if str(strategy_contract.get("neutral_engine_source_sha256", "")) != engine_archive_hash:
        raise AssertionError("V9-Honest strategy payload neutral-engine hash is invalid")
    if str(strategy_contract.get("active_setup_book_sha256", "")) != (
        ACTIVE_SETUP_BOOK_SHA256
    ):
        raise AssertionError("V9-Honest strategy payload active-book hash is invalid")
    if strategy_contract.get("active_later_legs") != []:
        raise AssertionError("A disabled V9-Honest later leg became active")
    if common.canonical_json_sha256(
        strategy_contract.get("disabled_shadow_legs", {})
    ) != common.canonical_json_sha256(disabled_shadow_payload()):
        raise AssertionError("V9-Honest strategy payload disabled registry changed")
    if common.canonical_json_sha256(payload.get("disabled_shadow_legs", {})) != (
        common.canonical_json_sha256(disabled_shadow_payload())
    ):
        raise AssertionError("V9-Honest disabled later-leg registry changed")

    payload["current_launcher_source_matches_archive"] = (
        launcher_sha256() == launcher_archive_hash
    )
    payload["current_neutral_engine_source_matches_archive"] = (
        engine._module_source_sha256() == engine_archive_hash
    )
    return payload


def configure_engine() -> None:
    """Install the immutable active book and provenance adapters."""

    validate_launcher_configuration()
    launcher_hash = launcher_sha256()
    engine.STRATEGY_VERSION = f"{STRATEGY_FAMILY}_{launcher_hash[:12]}"
    engine.OBJECTIVE = (
        "V8_COMBINED_ACTIVE_BOOK_WITH_FAIL_CLOSED_0950_0955_TRAIN_SELECTION"
    )
    engine.CONFIG_SOURCE = (
        "LITERAL_V8_COMBINED_TEN_LEG_BOOK;"
        "0950_0955_NO_QUALIFYING_TRAIN_LEGS_DISABLED;"
        f"OPTIMIZER_SHA256={OPTIMIZER_SOURCE_SHA256};"
        f"SEARCH_RUN_FINGERPRINT={OPTIMIZER_SEARCH_RUN_FINGERPRINT};"
        f"SELECTION_SHA256={OPTIMIZER_SELECTION_ARTIFACT_SHA256};"
        f"LAUNCHER_SHA256={launcher_hash}"
    )
    engine.CACHE_SCHEMA_VERSION = "fno_v9_honest_v8_combined_cache_manifest_v1"
    engine.RUN_SCHEMA_VERSION = V9_RUN_SCHEMA_VERSION
    engine.PATH_POLICY_VERSION = "fno_v9_honest_same_session_exact_grid_ohlcvt_v1"
    engine.ACTIVE_SETUPS = ACTIVE_SETUPS
    engine.V8_SETUP_BOOK_SHA256 = ACTIVE_SETUP_BOOK_SHA256
    engine.VARIANT_REGISTRY = {
        "VH": {
            "description": (
                "V9-Honest active V8-Combined book; 09:50/09:55 remain "
                "disabled after no independent TRAIN leg qualified"
            ),
            "max_confirmation_minute": 1,
            "buffer_bps": 0.0,
            "midpoint_invalidation": False,
            "close_location_min": None,
        }
    }
    engine.V8_ROOT = ROOT
    engine.CACHE_DIR = ROOT / "cache"
    engine.SNAPSHOT_ROOT = ROOT / "snapshots"
    engine.RUN_ROOT = ROOT / "runs"
    engine.PROVENANCE_ROOT = ROOT / "provenance"
    engine.REPORT_PATH = common.LATEST_DIR / "latest_fno_v9_honest_v8_combined.md"
    engine.CACHE_MANIFEST_PATH = engine.CACHE_DIR / "manifest.json"
    engine.CANDIDATE_CACHE_PATH = engine.CACHE_DIR / "five_minute_candidates.parquet"
    engine.PATH_CACHE_PATH = engine.CACHE_DIR / "same_session_minute_paths.parquet"
    engine.DEFAULT_SOURCE_SNAPSHOT = None

    engine.strategy_payload = _v9_strategy_payload
    engine.provenance.build_run_provenance = _build_v9_run_provenance
    engine.validate_v8_run_provenance = validate_v9_run_provenance
    engine.validate_configuration()


def _inject_v9_variant(argv: Sequence[str]) -> list[str]:
    args = list(argv)
    if args and args[0] in {"run", "smoke"} and "--variant" not in args:
        args.extend(["--variant", "VH"])
    return args


def main(argv: Sequence[str] | None = None) -> int:
    configure_engine()
    args = _inject_v9_variant(sys.argv[1:] if argv is None else argv)
    return engine.main(args)


if __name__ == "__main__":
    raise SystemExit(main())
