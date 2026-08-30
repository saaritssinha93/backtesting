from __future__ import annotations

import importlib
from dataclasses import asdict

import fno_oi_common as common
import fno_v10_unified_5m_1m_backtest as v10
import fno_v8_windowed_1m_entry_backtest as engine


def test_v10_book_is_hash_pinned_complete_and_v8_combined_equivalent() -> None:
    assert common.canonical_json_sha256(
        [asdict(setup) for setup in v10.ACTIVE_SETUPS]
    ) == v10.ACTIVE_SETUP_BOOK_SHA256
    assert len(v10.ACTIVE_SETUPS) == 10
    assert {(setup.signal_end, setup.side) for setup in v10.ACTIVE_SETUPS} == {
        (slot, side)
        for slot in ("09:25", "09:30", "09:35", "09:40", "09:45")
        for side in ("LONG", "SHORT")
    }
    assert common.canonical_json_sha256(v10.unified_contract_payload()) == (
        v10.UNIFIED_CONTRACT_SHA256
    )


def test_v10_resolves_five_minute_and_one_minute_fields_for_every_leg() -> None:
    contract = v10.unified_contract_payload()
    assert len(contract["active_legs"]) == 10
    for leg in contract["active_legs"]:
        assert set(leg) == {
            "setup_id",
            "signal_end",
            "side",
            "five_minute_selection",
            "one_minute_confirmation_and_entry",
            "risk_and_exit",
        }
        five = leg["five_minute_selection"]
        one = leg["one_minute_confirmation_and_entry"]
        risk = leg["risk_and_exit"]
        assert five["cash_bar_source"].startswith("FIVE_EXACT_REAL")
        assert five["oi_bar_source"].endswith("COMPLETED_5M_OI")
        assert one["confirmation_window_end_minute"] in {1, 3}
        assert one["entry_expiry_minute"] == 5
        assert risk["square_off"] == "15:30"
    by_id = {leg["setup_id"]: leg for leg in contract["active_legs"]}
    assert by_id["09:25_LONG"]["one_minute_confirmation_and_entry"][
        "confirmation_window_end_minute"
    ] == 3
    assert by_id["09:30_SHORT"]["one_minute_confirmation_and_entry"][
        "midpoint_invalidation"
    ] is True
    assert by_id["09:40_LONG"]["risk_and_exit"]["stop_pct"] == 0.50


def test_v10_late_research_legs_are_fail_closed_records() -> None:
    assert set(v10.DISABLED_RESEARCH_LEGS) == {
        "09:50_LONG",
        "09:50_SHORT",
        "09:55_LONG",
        "09:55_SHORT",
    }
    active_ids = {setup.setup_id for setup in v10.ACTIVE_SETUPS}
    assert active_ids.isdisjoint(v10.DISABLED_RESEARCH_LEGS)
    assert all(
        record["active"] is False
        for record in v10.DISABLED_RESEARCH_LEGS.values()
    )


def test_v10_configures_an_isolated_parity_baseline() -> None:
    original_builder = engine.provenance.build_run_provenance
    try:
        v10.configure_engine()
        assert engine.ACTIVE_SETUPS == v10.ACTIVE_SETUPS
        assert engine.V8_SETUP_BOOK_SHA256 == v10.ACTIVE_SETUP_BOOK_SHA256
        assert engine.RUN_SCHEMA_VERSION == v10.V10_RUN_SCHEMA_VERSION
        assert set(engine.VARIANT_REGISTRY) == {v10.V10_VARIANT}
        assert engine.CACHE_DIR.is_relative_to(v10.ROOT)
        assert engine.RUN_ROOT.is_relative_to(v10.ROOT)
        assert engine.PROVENANCE_ROOT.is_relative_to(v10.ROOT)
        policy = engine.entry_policy_for_variant(
            v10.V10_VARIANT,
            cost_bps=15.0,
            slippage_bps=0.0,
            square_off="15:30",
            eod_policy="EXACT_SQUARE_OFF",
        )
        assert policy.max_confirmation_minute == 1
        assert policy.entry_expiry_minute == 5
        assert policy.buffer_bps == 0.0
        payload = engine.strategy_payload()["v10_unified_launcher"]
        assert payload["unified_contract_sha256"] == v10.UNIFIED_CONTRACT_SHA256
    finally:
        engine.provenance.build_run_provenance = original_builder
        importlib.reload(engine)


def test_v10_cli_injects_baseline_variant_only_for_replays() -> None:
    assert v10._inject_v10_variant(["run", "--from-day", "2026-08-12"]) == [
        "run",
        "--from-day",
        "2026-08-12",
        "--variant",
        v10.V10_VARIANT,
    ]
    assert v10._inject_v10_variant(
        ["validate", "--provenance", "run/provenance.json"]
    ) == ["validate", "--provenance", "run/provenance.json"]
