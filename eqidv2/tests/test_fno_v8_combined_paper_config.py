from __future__ import annotations

import ast
from dataclasses import asdict
from pathlib import Path

import pytest

import fno_v8_combined_paper_config as config


EXPECTED_SETUP_HASH = (
    "ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675"
)


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    return imported


def test_literal_combined_book_is_hash_pinned_complete_and_independent() -> None:
    config.validate_configuration()
    assert config.COMBINED_SETUP_BOOK_SHA256 == EXPECTED_SETUP_HASH
    assert config.canonical_json_sha256(config.setup_payload()) == EXPECTED_SETUP_HASH
    assert len(config.ACTIVE_SETUPS) == 10
    assert len({setup.setup_id for setup in config.ACTIVE_SETUPS}) == 10
    assert {(setup.signal_end, setup.side) for setup in config.ACTIVE_SETUPS} == {
        (slot, side)
        for slot in ("09:25", "09:30", "09:35", "09:40", "09:45")
        for side in ("LONG", "SHORT")
    }


def test_literal_book_has_exact_selected_per_leg_values() -> None:
    by_id = {setup.setup_id: setup for setup in config.ACTIVE_SETUPS}
    assert asdict(by_id["09:25_LONG"]) == {
        "signal_end": "09:25",
        "side": "LONG",
        "max_entries": 4,
        "picker": "max_move",
        "price_change_pct": 0.30,
        "oi_change_pct": 0.10,
        "volume_ratio": 3.0,
        "body_ratio": 0.0,
        "max_wick_ratio": 0.50,
        "min_traded_value": 0.0,
        "stop_pct": 0.40,
        "target_pct": 1.0,
        "entry_conf_minute": 3,
        "entry_buffer_bps": 0.0,
        "entry_midpoint": False,
        "entry_clv": None,
    }
    assert by_id["09:25_SHORT"].entry_buffer_bps == 2.0
    assert by_id["09:30_SHORT"].entry_conf_minute == 3
    assert by_id["09:30_SHORT"].entry_midpoint is True
    assert by_id["09:30_SHORT"].entry_clv == 0.50
    assert (by_id["09:40_SHORT"].max_entries, by_id["09:40_SHORT"].picker) == (
        1,
        "max_move",
    )
    assert not by_id["09:40_SHORT"].overrides_entry_policy


def test_resolved_entry_policy_has_exact_global_and_per_leg_seams() -> None:
    common = config.entry_policy_for_setup("09:35", "LONG")
    assert common.max_confirmation_minute == 1
    assert common.entry_expiry_minute == 5
    assert common.buffer_bps == 0.0
    assert common.close_location_min is None
    assert common.midpoint_invalidation is False
    assert common.cost_bps == 15.0
    assert common.slippage_bps == 0.0
    assert common.same_bar_policy == "STOP_FIRST"
    assert common.square_off == "15:30"
    assert common.eod_policy == "EXACT_SQUARE_OFF"

    long_0925 = config.entry_policy_for_setup("09:25", "LONG")
    short_0925 = config.entry_policy_for_setup("09:25", "SHORT")
    short_0930 = config.entry_policy_for_setup("09:30", "SHORT")
    assert long_0925.max_confirmation_minute == 3
    assert long_0925.close_location_min is None
    assert short_0925.max_confirmation_minute == 3
    assert short_0925.buffer_bps == 2.0
    assert short_0930.max_confirmation_minute == 3
    assert short_0930.midpoint_invalidation is True
    assert short_0930.close_location_min == 0.50


def test_paper_economics_portfolio_and_eight_app_contract_are_frozen() -> None:
    assert config.MODE == "PAPER"
    assert config.PAPER_ONLY is True
    assert config.COST_BPS == 15.0
    assert config.SLIPPAGE_BPS == 0.0
    assert config.REQUIRED_KITE_APPS == 8
    assert asdict(config.PORTFOLIO_POLICY) == {
        "capital_rs": 120_000.0,
        "margin_per_entry_rs": 10_000.0,
        "target_exposure_per_entry_rs": 50_000.0,
        "max_concurrent_positions": 12,
        "pending_reserves_margin": True,
        "one_position_per_symbol": True,
    }
    payload = config.strategy_payload()
    assert payload["data_contract"]["completed_bars_only"] is True
    assert payload["data_contract"]["ltp_fill_fallback"] is False
    assert payload["data_contract"]["synthetic_or_gap_filled_bars"] is False


def test_strategy_fingerprint_covers_economics_data_and_app_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = config.strategy_fingerprint()
    monkeypatch.setattr(config, "REQUIRED_KITE_APPS", 7)
    assert config.strategy_fingerprint() != original
    monkeypatch.undo()

    original = config.strategy_fingerprint()
    monkeypatch.setattr(config, "BAR_SOURCE_POLICY", "DRIFTED")
    assert config.strategy_fingerprint() != original


def test_runtime_paths_are_disjoint_from_v6_and_research() -> None:
    paths = config.source_paths()
    root = paths["root"]
    for name, path in paths.items():
        lowered = str(path).replace("\\", "/").lower()
        assert "/v6_live" not in lowered, name
        assert "/strategy_research/" not in lowered, name
    for name in (
        "control_root",
        "permit_archive_root",
        "activation",
        "kill_switch",
        "session_root",
        "evidence_root",
        "checkpoint_root",
        "lock",
    ):
        assert paths[name].is_relative_to(root), name
    assert root.name == "v8_combined_paper_v1"
    assert config.LATEST_REPORT_PATH.name == "latest_fno_v8_combined_paper.md"


def test_config_imports_no_legacy_optimizer_or_parent_launcher() -> None:
    imported = _imports(Path(config.__file__))
    forbidden_prefixes = (
        "fno_v5",
        "fno_v6",
        "fno_v7",
        "fno_oi_ema_confirm",
        "fno_v8_combined_best_per_leg_backtest",
        "fno_v8_windowed_1m_entry_backtest",
        "fno_v8_windowed_1m_entry_optimize",
        "fno_v8_setup_param_sweep",
    )
    assert not any(
        name.startswith(prefix)
        for name in imported
        for prefix in forbidden_prefixes
    )


def test_unknown_setup_cannot_silently_inherit_a_policy() -> None:
    assert config.setup_for("09:50", "LONG") is None
    with pytest.raises(KeyError):
        config.entry_policy_for_setup("09:50", "LONG")
