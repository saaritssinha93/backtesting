from __future__ import annotations

import ast
import importlib
from dataclasses import asdict
from pathlib import Path

import fno_oi_common as common
import fno_v8_strict_v6_logic_backtest as strict
import fno_v8_windowed_1m_entry_backtest as engine


def test_strict_book_is_literal_hash_pinned_and_has_no_entry_overrides() -> None:
    payload = [asdict(setup) for setup in strict.STRICT_SETUPS]
    assert common.canonical_json_sha256(payload) == strict.STRICT_SETUP_BOOK_SHA256
    assert len(strict.STRICT_SETUPS) == 10
    assert len({setup.setup_id for setup in strict.STRICT_SETUPS}) == 10
    assert not any(setup.overrides_entry_policy for setup in strict.STRICT_SETUPS)


def test_strict_book_restores_original_caps_pickers_and_brackets() -> None:
    by_id = {setup.setup_id: setup for setup in strict.STRICT_SETUPS}
    assert (by_id["09:25_SHORT"].max_entries, by_id["09:25_SHORT"].picker) == (
        2,
        "max_volume",
    )
    assert (by_id["09:25_SHORT"].stop_pct, by_id["09:25_SHORT"].target_pct) == (
        0.75,
        3.0,
    )
    assert (by_id["09:40_SHORT"].max_entries, by_id["09:40_SHORT"].picker) == (
        1,
        "max_move",
    )
    assert (by_id["09:40_SHORT"].oi_change_pct, by_id["09:40_SHORT"].target_pct) == (
        0.10,
        3.0,
    )


def test_launcher_imports_no_v6_v7_or_legacy_replay_modules() -> None:
    source = Path(strict.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    forbidden = {
        name
        for name in imports
        if name.startswith(
            (
                "fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6",
                "fno_oi_ema_confirm_0925_0930_0935_0940_0945_v7",
                "fno_oi_ema_confirm_sweep",
                "fno_v5_hybrid_backtest",
                "fno_oi_ema_confirm_optimize",
                "fno_oi_ema_confirm_v7_signal_cache",
            )
        )
    }
    assert forbidden == set()


def test_configured_engine_is_strict_and_artifact_isolated() -> None:
    try:
        strict.configure_engine()
        assert engine.ACTIVE_SETUPS == strict.STRICT_SETUPS
        assert engine.V8_SETUP_BOOK_SHA256 == strict.STRICT_SETUP_BOOK_SHA256
        assert set(engine.VARIANT_REGISTRY) == {"VS"}
        policy = engine.entry_policy_for_variant(
            "VS",
            cost_bps=15.0,
            slippage_bps=0.0,
            square_off="15:30",
            eod_policy="LAST_REAL_BAR_SENSITIVITY",
        )
        assert policy.max_confirmation_minute == 1
        assert policy.buffer_bps == 0.0
        assert not policy.midpoint_invalidation
        assert engine.CACHE_DIR.is_relative_to(strict.ROOT)
        assert engine.RUN_ROOT.is_relative_to(strict.ROOT)
        assert "v8_windowed_strict_v1" not in str(engine.CACHE_DIR)
    finally:
        importlib.reload(engine)


def test_cli_injects_only_the_strict_variant() -> None:
    assert strict._inject_strict_variant(["run", "--from-day", "2026-07-28"]) == [
        "run",
        "--from-day",
        "2026-07-28",
        "--variant",
        "VS",
    ]
    assert strict._inject_strict_variant(["validate", "--provenance", "x.json"]) == [
        "validate",
        "--provenance",
        "x.json",
    ]
