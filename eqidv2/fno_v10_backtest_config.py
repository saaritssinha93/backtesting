"""Frozen backtest-only profile for the selected FNO V10 Stage 7 strategy."""

from __future__ import annotations

from typing import Any

import fno_v10_experiment_config as experiment_config


PROFILE_SCHEMA_VERSION = "fno_v10_stage7_locked_backtest_profile_v1"
PROFILE_ID = "V10_STAGE7_LOCKED_BACKTEST_20260827"
ACTIVE_VARIANT = "0940_LONG_MOVE_040"
AUTHORITY = "BACKTEST_ONLY"

EXPECTED_REGISTRY_SHA256 = (
    "105935648a67ff126b73b98233efd6c10f40a5706f971a75dc22540251cc843b"
)
EXPECTED_VARIANT_CONFIG_SHA256 = (
    "f3a54e5fddbfd8445923f9df52a68207f47b57bc43ccbd7eb83b2aad10a9bc18"
)

# Immutable research lineage proving why Stage 7, rather than another Stage 1
# experiment, is the canonical V10 backtest profile.
STAGE_SEQUENCE_SHA256 = (
    "e8381c478b6843de26f74d61ef569495b739fe10f17743e447d0f771e7fd88c2"
)
STAGE7_REFERENCE_PROVENANCE_SHA256 = (
    "336d8b4bef026d5b02ed3aa71ff6cb27b515c8bfb4b77a83066d5dbf3f54198d"
)
STAGE7_REFERENCE_INPUT_FINGERPRINT = (
    "a863048ae1f084cf37a9a9a3471df069ad664bf6d437110748f7917316a5d3de"
)
STAGE7_SELECTION_DECISIONS_SHA256 = (
    "948949240ee2680ead6f49c730f3ea1802a8e03dd8e5622213117312349e76ac"
)
STAGE7_CANDIDATE_AUDIT_SHA256 = (
    "26c22cd1ed0fd64f0983d7aed015669df91221d9a2b7aa57a30628bc82b6dbeb"
)
STAGE10_COST_STRESS_PLAN_SHA256 = (
    "bf05b088e96d0e5259012a6d76cc837b5ed0941075f76ac31ccccb518191a928"
)
STAGE10_COST_STRESS_MANIFEST_SHA256 = (
    "39b595a0e58d1980a38049da85414d496deae0b103392904df7fe6293ff03b41"
)

SOURCE_SNAPSHOT_MANIFEST = (
    "C:\\TradingData\\eqidv2\\fno_oi\\strategy_research\\"
    "v8_windowed_strict_v1\\snapshots\\"
    "snapshot_20260820T124734626995+0530_mnofor_c\\manifest.json"
)
SOURCE_SNAPSHOT_MANIFEST_SHA256 = (
    "579e8673fb96644bc2e4b348c9d98486ee2c26291def72702fe8da0e6a55324d"
)
SOURCE_SNAPSHOT_FINGERPRINT = (
    "6734204d53315d386a2c3949f01b272d4399a8a1d3b44b9cfd556a1b859190cc"
)

EXPECTED_PROFILE_SHA256 = (
    "f2b3291903dfb1f2c95f1d24b63285d527dc7a9a6aa3d6334caed03d0834e59c"
)


def locked_profile_payload() -> dict[str, Any]:
    """Return the complete immutable Stage 7 backtest profile."""

    spec = experiment_config.get_spec(ACTIVE_VARIANT)
    return {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "profile_id": PROFILE_ID,
        "authority": AUTHORITY,
        "active_variant": ACTIVE_VARIANT,
        "experiment_registry_sha256": EXPECTED_REGISTRY_SHA256,
        "variant_config_sha256": EXPECTED_VARIANT_CONFIG_SHA256,
        "variant": spec.payload(),
        "selection_contract": {
            "changed_setup_id": "09:40_LONG",
            "price_change_pct_min": 0.40,
            "comparison_base": "V10B",
            "other_selection_and_entry_parameters_changed": False,
        },
        "selection_lineage": {
            "source_stage": "STAGE_07",
            "stage_sequence_sha256": STAGE_SEQUENCE_SHA256,
            "reference_provenance_sha256": (
                STAGE7_REFERENCE_PROVENANCE_SHA256
            ),
            "reference_backtest_input_fingerprint": (
                STAGE7_REFERENCE_INPUT_FINGERPRINT
            ),
            "selection_decisions_sha256": STAGE7_SELECTION_DECISIONS_SHA256,
            "candidate_order_audit_sha256": STAGE7_CANDIDATE_AUDIT_SHA256,
            "stage10_cost_stress_plan_sha256": (
                STAGE10_COST_STRESS_PLAN_SHA256
            ),
            "stage10_cost_stress_manifest_sha256": (
                STAGE10_COST_STRESS_MANIFEST_SHA256
            ),
        },
        "extended_stored_history_replay": {
            "source_snapshot_manifest": SOURCE_SNAPSHOT_MANIFEST,
            "source_snapshot_manifest_sha256": (
                SOURCE_SNAPSHOT_MANIFEST_SHA256
            ),
            "source_snapshot_fingerprint": SOURCE_SNAPSHOT_FINGERPRINT,
            "source_inventory_sha256": (
                "03407e713ebae80270268733f1549bc54e1c6d384dafd67d67734ebaed2c2711"
            ),
            "source_inventory_fingerprint": (
                "85f7404d4f026d3564280f30bc317c13bc5ecdac47748ebaf307c4aa1b2676d3"
            ),
            "source_capture_count": 416,
            "source_total_bytes": 3_116_245_273,
            "from_day": "2026-05-27",
            "through_day": "2026-08-19",
            "expected_official_sessions": 59,
            "split_day": "2026-08-06",
            "cost_bps": 15.0,
            "slippage_bps": 0.0,
            "square_off": "15:30",
            "eod_policy": "LAST_REAL_BAR_SENSITIVITY",
            "target_exposure_per_entry_rs": 50_000.0,
            "portfolio_mode": (
                "GLOBAL_PENDING_MARGIN_AND_DUPLICATE_RESERVATION_"
                "CONSERVATIVE_NO_BACKFILL_V1"
            ),
            "universe": {
                "master_date": "2026-08-11",
                "contract_month_filter": "26AUG",
                "mapped_stock_futures": 208,
                "mapped_symbol_set_sha256": (
                    "d42f87a9c5fc8ab1710b09b6c4c9832c9d19ecc440ef92b84cad6981499a05a3"
                ),
                "mapped_universe_sha256": (
                    "2cc160189f87bff4eb987a15a4684d95619ee9c810db3cd37276b114ad5824bf"
                ),
                "universe_sha256": (
                    "18c496bbf9e09b6914d073cba21c4c6c56305da1ed5759f4f91cc8cb66c19ad5"
                ),
            },
            "expected_symbol_sessions": 12_272,
            "expected_complete_symbol_sessions": 6_350,
            "expected_incomplete_symbol_sessions": 5_922,
            "coverage_authority": (
                "EXPECTED_EXCHANGE_SESSIONS_ONLY;PER_SYMBOL_SOURCE_COVERAGE_"
                "REMAINS_INCOMPLETE"
            ),
        },
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }


def profile_sha256() -> str:
    return experiment_config.canonical_json_sha256(locked_profile_payload())


def validate_locked_profile(*, require_pinned_hash: bool = True) -> None:
    """Fail closed if Stage 7 or its backtest-only authority drifts."""

    experiment_config.validate_registry()
    if experiment_config.registry_sha256() != EXPECTED_REGISTRY_SHA256:
        raise AssertionError("V10 experiment registry changed")
    spec = experiment_config.get_spec(ACTIVE_VARIANT)
    if experiment_config.variant_config_sha256(spec) != (
        EXPECTED_VARIANT_CONFIG_SHA256
    ):
        raise AssertionError("Locked V10 Stage 7 variant config changed")
    if spec.price_threshold_overrides != (("09:40_LONG", 0.40),):
        raise AssertionError("Stage 7 must contain only the 09:40 LONG 0.40 gate")
    if (
        spec.confirmation_volume_ratio_min is not None
        or spec.entry_expiry_minute != 5
        or spec.disabled_setup_ids
        or spec.slot_rvol20_min is not None
    ):
        raise AssertionError("A non-Stage-7 mechanism entered the locked profile")
    payload = locked_profile_payload()
    if payload["authority"] != "BACKTEST_ONLY":
        raise AssertionError("V10 Stage 7 backtest authority changed")
    if payload["research_only"] is not True:
        raise AssertionError("V10 Stage 7 research-only status changed")
    if payload["promotion_eligible"] is not False:
        raise AssertionError("V10 Stage 7 promotion status changed")
    if payload["live_or_paper_authority"] is not False:
        raise AssertionError("V10 Stage 7 acquired live or paper authority")
    if require_pinned_hash and profile_sha256() != EXPECTED_PROFILE_SHA256:
        raise AssertionError(
            "Locked V10 Stage 7 profile changed: "
            f"expected {EXPECTED_PROFILE_SHA256}, observed {profile_sha256()}"
        )


if __name__ == "__main__":
    validate_locked_profile()
    print(profile_sha256())
