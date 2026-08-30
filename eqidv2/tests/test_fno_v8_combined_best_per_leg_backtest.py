from __future__ import annotations

import ast
import importlib
from dataclasses import asdict
from pathlib import Path

import fno_oi_common as common
import fno_v8_combined_best_per_leg_backtest as combined
import fno_v8_windowed_1m_entry_backtest as engine


def test_combined_book_is_literal_hash_pinned_and_complete() -> None:
    payload = [asdict(setup) for setup in combined.COMBINED_SETUPS]
    assert common.canonical_json_sha256(payload) == combined.COMBINED_SETUP_BOOK_SHA256
    assert len(combined.COMBINED_SETUPS) == 10
    assert len({setup.setup_id for setup in combined.COMBINED_SETUPS}) == 10


def test_combined_book_has_the_selected_per_leg_sources() -> None:
    by_id = {setup.setup_id: setup for setup in combined.COMBINED_SETUPS}

    # Retuned legs.
    assert (by_id["09:25_LONG"].max_entries, by_id["09:25_LONG"].picker) == (
        4,
        "max_move",
    )
    assert by_id["09:25_LONG"].entry_conf_minute == 3
    assert by_id["09:25_SHORT"].entry_buffer_bps == 2.0
    assert by_id["09:30_SHORT"].entry_midpoint is True
    assert by_id["09:30_SHORT"].entry_clv == 0.50

    # Strict/less-weak 09:40 SHORT.
    assert (by_id["09:40_SHORT"].max_entries, by_id["09:40_SHORT"].picker) == (
        1,
        "max_move",
    )
    assert by_id["09:40_SHORT"].oi_change_pct == 0.10
    assert not by_id["09:40_SHORT"].overrides_entry_policy

    # Six common legs retain the shared S+1 policy.
    common_ids = {
        "09:30_LONG",
        "09:35_LONG",
        "09:35_SHORT",
        "09:40_LONG",
        "09:45_LONG",
        "09:45_SHORT",
    }
    assert not any(by_id[setup_id].overrides_entry_policy for setup_id in common_ids)


def test_combined_launcher_has_no_parent_strategy_import() -> None:
    source = Path(combined.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    assert "fno_v8_strict_v6_logic_backtest" not in imports
    assert not any(name.startswith("fno_oi_ema_confirm") for name in imports)


def test_configured_engine_uses_combined_isolated_namespace() -> None:
    try:
        combined.configure_engine()
        assert engine.ACTIVE_SETUPS == combined.COMBINED_SETUPS
        assert engine.V8_SETUP_BOOK_SHA256 == combined.COMBINED_SETUP_BOOK_SHA256
        assert set(engine.VARIANT_REGISTRY) == {"VC"}
        policy = engine.entry_policy_for_variant(
            "VC",
            cost_bps=15.0,
            slippage_bps=0.0,
            square_off="15:30",
            eod_policy="LAST_REAL_BAR_SENSITIVITY",
        )
        assert policy.max_confirmation_minute == 1
        assert policy.buffer_bps == 0.0
        assert engine.CACHE_DIR.is_relative_to(combined.ROOT)
        assert engine.RUN_ROOT.is_relative_to(combined.ROOT)
        assert "v8_windowed_strict_v1" not in str(engine.CACHE_DIR)
        assert "v8_strict_v6_logic_v1" not in str(engine.CACHE_DIR)
    finally:
        importlib.reload(engine)


def test_cli_injects_only_combined_variant() -> None:
    assert combined._inject_combined_variant(["run", "--from-day", "2026-06-24"]) == [
        "run",
        "--from-day",
        "2026-06-24",
        "--variant",
        "VC",
    ]
    assert combined._inject_combined_variant(
        ["validate", "--provenance", "x.json"]
    ) == ["validate", "--provenance", "x.json"]
