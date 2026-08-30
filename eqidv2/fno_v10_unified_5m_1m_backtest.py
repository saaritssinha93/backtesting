"""Unified FNO V10 five-minute selection and one-minute execution backtester.

V10 starts from the frozen V8-Combined ten-leg book and the neutral V8
state-machine implementation.  It intentionally changes no trading economics:
the first V10 baseline must reproduce V8 before any proposed filters are
introduced.  The difference is the strategy contract.  V10 records every
five-minute selection field and every resolved one-minute entry/exit field in
one hash-pinned, per-leg payload so later experiments cannot silently change
one side of the two-timeframe strategy.

The 09:50 and 09:55 LONG/SHORT searches from V9 remain disabled.  They are
recorded as negative research lineage, not instantiated as runtime setups.
All cache, run, provenance, snapshot and latest-report paths are isolated from
V8 and V9.  This module is a research backtester, never a live-order launcher.
"""

from __future__ import annotations

import hashlib
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import fno_oi_common as common
import fno_v8_windowed_1m_entry_backtest as engine


STRATEGY_FAMILY = "FNO_V10_UNIFIED_5M_SELECTION_1M_EXECUTION_20260826"
V10_CONFIG_SCHEMA_VERSION = "fno_v10_unified_5m_1m_config_v1"
V10_RUN_SCHEMA_VERSION = "fno_v10_unified_5m_1m_run_v1"
V10_VARIANT = "V10B"

# The setup hash deliberately equals the selected V8-Combined book.  V10 is a
# parity baseline until a separately named, predeclared experiment is added.
ACTIVE_SETUP_BOOK_SHA256 = (
    "ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675"
)
# Hash of ``unified_contract_payload``.  It binds the complete resolved
# two-timeframe contract rather than only the V8Setup dataclass rows.
UNIFIED_CONTRACT_SHA256 = (
    "e0784567b0f0f34834331775c676ed6977a86679a1877babad37f69ad7282bed"
)

S = engine.V8Setup
ACTIVE_SETUPS: tuple[engine.V8Setup, ...] = (
    S(
        "09:25", "LONG", 4, "max_move", 0.30, 0.10, 3.0, 0.00, 0.50,
        0.0, 0.40, 1.0, entry_conf_minute=3, entry_buffer_bps=0.0,
        entry_midpoint=False, entry_clv=None,
    ),
    S(
        "09:25", "SHORT", 4, "max_move", 0.20, 0.10, 1.5, 0.60, 0.60,
        25_000_000.0, 0.50, 3.0, entry_conf_minute=3,
        entry_buffer_bps=2.0, entry_midpoint=False, entry_clv=None,
    ),
    S("09:30", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.50, 0.50, 0.0, 1.00, 2.5),
    S(
        "09:30", "SHORT", 4, "max_volume", 0.20, 1.00, 1.0, 0.45,
        0.30, 25_000_000.0, 1.00, 4.0, entry_conf_minute=3,
        entry_buffer_bps=0.0, entry_midpoint=True, entry_clv=0.50,
    ),
    S("09:35", "LONG", 1, "max_liquidity", 0.20, 0.10, 1.0, 0.60, 0.50, 0.0, 1.00, 2.5),
    S("09:35", "SHORT", 2, "max_liquidity", 0.50, 1.00, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    S("09:40", "LONG", 1, "max_liquidity", 0.20, 0.10, 2.0, 0.50, 0.50, 0.0, 0.50, 2.5),
    S("09:40", "SHORT", 1, "max_move", 0.20, 0.10, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    S("09:45", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    S("09:45", "SHORT", 1, "max_volume", 0.20, 0.75, 1.0, 0.40, 0.30, 0.0, 1.00, 2.0),
)

# These are V9's honest negative late-slot conclusions expressed as a compact
# fail-closed registry.  No entry in this mapping is an executable V8Setup.
DISABLED_RESEARCH_LEGS: dict[str, dict[str, Any]] = {
    f"{slot}_{side}": {
        "signal_end": slot,
        "side": side,
        "status": "DISABLED_RESEARCH",
        "reason": "NO_CONFIG_PASSED_INDEPENDENT_TRAIN_LEG_GUARDS",
        "active": False,
    }
    for slot in ("09:50", "09:55")
    for side in ("LONG", "SHORT")
}

ROOT = common.FNO_ROOT / "strategy_research" / "v10_unified_5m_1m_v1"

# Capture neutral seams once.  The adapters add V10 identity and source
# archives without modifying candidate, fill, exit, cost or portfolio logic.
_ORIGINAL_STRATEGY_PAYLOAD = engine.strategy_payload
_ORIGINAL_PROVENANCE_BUILDER = engine.provenance.build_run_provenance
_ORIGINAL_ENGINE_PROVENANCE_VALIDATOR = engine.validate_v8_run_provenance


def launcher_sha256() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def active_setup_payload() -> list[dict[str, Any]]:
    return [asdict(setup) for setup in ACTIVE_SETUPS]


def _base_entry_policy() -> engine.EntryPolicy:
    """Return the V10 baseline policy before per-leg overrides."""

    return engine.EntryPolicy(
        buffer_bps=0.0,
        max_confirmation_minute=1,
        entry_expiry_minute=5,
        close_location_min=None,
        cost_bps=15.0,
        slippage_bps=0.0,
        midpoint_invalidation=False,
        post_confirmation_cancel=True,
        allow_cap_reassignment=True,
        same_bar_policy="STOP_FIRST",
        square_off="15:30",
        eod_policy="EXACT_SQUARE_OFF",
    )


def resolved_leg_payload(setup: engine.V8Setup) -> dict[str, Any]:
    """Resolve both timeframes and risk controls for one executable leg."""

    policy = engine.policy_for_setup(setup, _base_entry_policy())
    return {
        "setup_id": setup.setup_id,
        "signal_end": setup.signal_end,
        "side": setup.side,
        "five_minute_selection": {
            "max_entries": setup.max_entries,
            "picker": setup.picker,
            "price_change_pct": setup.price_change_pct,
            "oi_change_pct": setup.oi_change_pct,
            "volume_ratio": setup.volume_ratio,
            "ema_structure": "EMA9_GT_EMA20_GT_EMA50"
            if setup.side == "LONG"
            else "EMA9_LT_EMA20_LT_EMA50",
            "min_traded_value": setup.min_traded_value,
            "cash_bar_source": "FIVE_EXACT_REAL_END_LABELLED_NSE_1M_BARS",
            "oi_bar_source": "MATCHED_NFO_FUTURE_COMPLETED_5M_OI",
        },
        "one_minute_confirmation_and_entry": {
            "confirmation_window_end_minute": policy.max_confirmation_minute,
            "entry_expiry_minute": policy.entry_expiry_minute,
            "body_ratio_min": setup.body_ratio,
            "adverse_wick_ratio_max": setup.max_wick_ratio,
            "close_location_min": policy.close_location_min,
            "trigger_buffer_bps": policy.buffer_bps,
            "midpoint_invalidation": policy.midpoint_invalidation,
            "post_confirmation_cancel": policy.post_confirmation_cancel,
            "same_confirmation_bar_fill": False,
            "same_bar_exit_policy": policy.same_bar_policy,
        },
        "risk_and_exit": {
            "stop_pct": setup.stop_pct,
            "target_pct": setup.target_pct,
            "square_off": policy.square_off,
            "eod_policy": policy.eod_policy,
        },
    }


def unified_contract_payload() -> dict[str, Any]:
    """Return the complete, JSON-safe V10 baseline strategy contract."""

    portfolio = engine.PortfolioPolicy()
    return {
        "schema_version": V10_CONFIG_SCHEMA_VERSION,
        "strategy_family": STRATEGY_FAMILY,
        "baseline_lineage": "V8_COMBINED_BEST_PER_LEG_ECONOMIC_PARITY",
        "active_setup_book_sha256": ACTIVE_SETUP_BOOK_SHA256,
        "active_legs": [resolved_leg_payload(setup) for setup in ACTIVE_SETUPS],
        "disabled_research_legs": {
            key: dict(value) for key, value in DISABLED_RESEARCH_LEGS.items()
        },
        "portfolio": asdict(portfolio),
        "run_economics_defaults": {
            "cost_bps": 15.0,
            "slippage_bps": 0.0,
            "target_exposure_per_entry_rs": portfolio.target_exposure_per_entry_rs,
        },
        "research_only": True,
        "promotion_eligible": False,
    }


def validate_launcher_configuration() -> None:
    setup_hash = common.canonical_json_sha256(active_setup_payload())
    if setup_hash != ACTIVE_SETUP_BOOK_SHA256:
        raise AssertionError(
            "V10 active setup book changed: "
            f"expected {ACTIVE_SETUP_BOOK_SHA256}, observed {setup_hash}"
        )
    if common.canonical_json_sha256(unified_contract_payload()) != (
        UNIFIED_CONTRACT_SHA256
    ):
        raise AssertionError("V10 unified 5m/1m strategy contract changed")
    expected = {
        (slot, side)
        for slot in ("09:25", "09:30", "09:35", "09:40", "09:45")
        for side in ("LONG", "SHORT")
    }
    if {(setup.signal_end, setup.side) for setup in ACTIVE_SETUPS} != expected:
        raise AssertionError("V10 requires all ten V8-Combined setup legs")
    if len({setup.setup_id for setup in ACTIVE_SETUPS}) != len(ACTIVE_SETUPS):
        raise AssertionError("V10 active setup IDs must be unique")
    if set(DISABLED_RESEARCH_LEGS) != {
        "09:50_LONG", "09:50_SHORT", "09:55_LONG", "09:55_SHORT"
    }:
        raise AssertionError("V10 disabled late-slot registry changed")
    if set(DISABLED_RESEARCH_LEGS) & {
        setup.setup_id for setup in ACTIVE_SETUPS
    }:
        raise AssertionError("A disabled V10 research leg became executable")


def _v10_strategy_payload() -> dict[str, Any]:
    payload = _ORIGINAL_STRATEGY_PAYLOAD()
    payload["v10_unified_launcher"] = {
        "schema_version": V10_CONFIG_SCHEMA_VERSION,
        "launcher_source_sha256": launcher_sha256(),
        "neutral_engine_source_sha256": engine._module_source_sha256(),
        "unified_contract_sha256": UNIFIED_CONTRACT_SHA256,
        "contract": unified_contract_payload(),
    }
    return payload


def _build_v10_run_provenance(**kwargs: Any) -> dict[str, Any]:
    """Archive and bind the V10 launcher in addition to the neutral engine."""

    output_paths = dict(kwargs.get("output_paths", {}))
    engine_archive = Path(str(output_paths["strategy_source_archive"]))
    launcher_archive = engine_archive.parent / Path(__file__).name
    launcher_hash = launcher_sha256()
    engine.provenance.publish_immutable_copy(
        Path(__file__), launcher_archive, expected_sha256=launcher_hash
    )
    output_paths["launcher_source_archive"] = launcher_archive
    output_paths["neutral_engine_source_archive"] = engine_archive
    forwarded = dict(kwargs)
    forwarded["output_paths"] = output_paths
    payload = _ORIGINAL_PROVENANCE_BUILDER(**forwarded)
    payload["v10_run_schema_version"] = V10_RUN_SCHEMA_VERSION
    payload["v10_unified_contract_sha256"] = UNIFIED_CONTRACT_SHA256
    payload["launcher_source_sha256"] = launcher_hash
    payload["neutral_engine_source_sha256"] = engine._module_source_sha256()
    payload["research_only"] = True
    payload["promotion_eligible"] = False
    return payload


def _artifact_hash(record: Mapping[str, Any], label: str) -> str:
    if not engine.provenance.artifact_matches(record.get("path", ""), record):
        raise AssertionError(f"V10 source artifact changed: {label}")
    return engine.provenance.sha256_file(Path(str(record.get("path", ""))))


def validate_v10_run_provenance(path: Path | str) -> dict[str, Any]:
    """Validate the neutral-engine record and V10's complete contract binding."""

    payload = _ORIGINAL_ENGINE_PROVENANCE_VALIDATOR(path)
    if payload.get("v10_run_schema_version") != V10_RUN_SCHEMA_VERSION:
        raise ValueError("Not a supported V10 unified run provenance artifact")
    if payload.get("research_only") is not True:
        raise AssertionError("V10 research-only status changed")
    if payload.get("promotion_eligible") is not False:
        raise AssertionError("V10 promotion status changed")
    if payload.get("v10_unified_contract_sha256") != UNIFIED_CONTRACT_SHA256:
        raise AssertionError("V10 provenance contract hash changed")

    outputs = dict(payload.get("outputs", {}))
    required = {"launcher_source_archive", "neutral_engine_source_archive"}
    missing = sorted(required - set(outputs))
    if missing:
        raise ValueError(f"V10 provenance is missing source archives: {missing}")
    launcher_hash = _artifact_hash(
        dict(outputs["launcher_source_archive"]), "launcher_source_archive"
    )
    engine_hash = _artifact_hash(
        dict(outputs["neutral_engine_source_archive"]),
        "neutral_engine_source_archive",
    )
    if launcher_hash != str(payload.get("launcher_source_sha256", "")):
        raise AssertionError("Archived V10 launcher hash is invalid")
    if engine_hash != str(payload.get("neutral_engine_source_sha256", "")):
        raise AssertionError("Archived V10 neutral-engine hash is invalid")
    if engine_hash != str(payload.get("strategy_source_sha256", "")):
        raise AssertionError("V10 neutral-engine alias disagrees with base provenance")

    contract = dict(payload.get("strategy_payload", {})).get(
        "v10_unified_launcher", {}
    )
    contract = dict(contract)
    if contract.get("unified_contract_sha256") != UNIFIED_CONTRACT_SHA256:
        raise AssertionError("V10 strategy payload contract hash changed")
    if common.canonical_json_sha256(contract.get("contract", {})) != (
        UNIFIED_CONTRACT_SHA256
    ):
        raise AssertionError("V10 archived unified strategy contract changed")
    if contract.get("launcher_source_sha256") != launcher_hash:
        raise AssertionError("V10 strategy payload launcher hash is invalid")
    if contract.get("neutral_engine_source_sha256") != engine_hash:
        raise AssertionError("V10 strategy payload engine hash is invalid")

    payload["current_launcher_source_matches_archive"] = (
        launcher_sha256() == launcher_hash
    )
    payload["current_neutral_engine_source_matches_archive"] = (
        engine._module_source_sha256() == engine_hash
    )
    return payload


def configure_engine() -> None:
    """Install the immutable V10 baseline into the neutral V8 engine."""

    validate_launcher_configuration()
    launcher_hash = launcher_sha256()
    engine.STRATEGY_VERSION = f"{STRATEGY_FAMILY}_{launcher_hash[:12]}"
    engine.OBJECTIVE = (
        "UNIFIED_COMPLETED_5M_SELECTION_AND_EXACT_1M_CONFIRMATION_EXECUTION;"
        "V8_COMBINED_ECONOMIC_PARITY_BASELINE"
    )
    engine.CONFIG_SOURCE = (
        "LITERAL_V8_COMBINED_TEN_LEG_BOOK;"
        "RESOLVED_5M_AND_1M_V10_CONTRACT;"
        f"UNIFIED_CONTRACT_SHA256={UNIFIED_CONTRACT_SHA256};"
        f"LAUNCHER_SHA256={launcher_hash}"
    )
    engine.CACHE_SCHEMA_VERSION = "fno_v10_unified_5m_1m_cache_manifest_v1"
    engine.RUN_SCHEMA_VERSION = V10_RUN_SCHEMA_VERSION
    engine.PATH_POLICY_VERSION = "fno_v10_same_session_exact_5m_1m_ohlcvt_v1"
    engine.ACTIVE_SETUPS = ACTIVE_SETUPS
    engine.V8_SETUP_BOOK_SHA256 = ACTIVE_SETUP_BOOK_SHA256
    engine.VARIANT_REGISTRY = {
        V10_VARIANT: {
            "description": (
                "V10 unified baseline: all V8-Combined 5m selectors and "
                "resolved 1m entry policies"
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
    engine.REPORT_PATH = common.LATEST_DIR / "latest_fno_v10_unified_5m_1m.md"
    engine.CACHE_MANIFEST_PATH = engine.CACHE_DIR / "manifest.json"
    engine.CANDIDATE_CACHE_PATH = engine.CACHE_DIR / "five_minute_candidates.parquet"
    engine.PATH_CACHE_PATH = engine.CACHE_DIR / "same_session_minute_paths.parquet"
    engine.DEFAULT_SOURCE_SNAPSHOT = None
    engine.strategy_payload = _v10_strategy_payload
    engine.provenance.build_run_provenance = _build_v10_run_provenance
    engine.validate_v8_run_provenance = validate_v10_run_provenance
    engine.validate_configuration()


def _inject_v10_variant(argv: Sequence[str]) -> list[str]:
    args = list(argv)
    if args and args[0] in {"run", "smoke"} and "--variant" not in args:
        args.extend(["--variant", V10_VARIANT])
    return args


def main(argv: Sequence[str] | None = None) -> int:
    configure_engine()
    args = _inject_v10_variant(sys.argv[1:] if argv is None else argv)
    return engine.main(args)


if __name__ == "__main__":
    raise SystemExit(main())
