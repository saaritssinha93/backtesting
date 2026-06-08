"""
Phase 0 characterization tests: pre-entry momentum gate.

Confirms:
  - Default PRE_ENTRY_MOMENTUM_MISSING_ACTION is "block" (not "allow").
  - When missing_action=block, a row with missing features is rejected.
  - When missing_action=allow, a row with missing features passes.
  - A row that passes all feature checks is not affected by missing_action.

Never writes to C:\\TradingData.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import pytest


def test_missing_action_default_is_block():
    """The Python default must be 'block' after the fix."""
    import importlib, eqidv2_entry_engine_1min_v5_id as ee
    # The default is set at import time from the env var.
    # In a clean environment without the env var the code reads the fallback.
    assert ee.PRE_ENTRY_MOMENTUM_MISSING_ACTION in {"block"}, (
        f"Expected 'block' default, got {ee.PRE_ENTRY_MOMENTUM_MISSING_ACTION!r}. "
        "The bat file or code default may not have been updated."
    )


def test_bat_file_sets_block():
    """Bat file must explicitly set BLOCK so accidental env-var absence can't revert."""
    import pathlib
    bat = pathlib.Path(__file__).parent.parent / "bat" / "run_eqidv2_entry_engine_1min_v5_id.bat"
    if not bat.exists():
        pytest.skip("Bat file not found — skipping")
    content = bat.read_text(encoding="utf-8", errors="ignore")
    assert "PRE_MOMENTUM_MISSING_ACTION=block" in content, (
        "Bat file must set EQIDV2_ENTRY_ENGINE_PRE_MOMENTUM_MISSING_ACTION=block"
    )


def test_missing_features_blocked_when_action_block(monkeypatch):
    import eqidv2_entry_engine_1min_v5_id as ee
    monkeypatch.setattr(ee, "PRE_ENTRY_MOMENTUM_MISSING_ACTION", "block")
    monkeypatch.setattr(ee, "PRE_ENTRY_MOMENTUM_GATES_ENABLED", True)

    # Row for a gated setup with no pre-momentum features in raw_by_ticker
    row = {
        "ticker": "RELIANCE",
        "side": "LONG",
        "setup": "C_OR_BREAKOUT",
        "bar_time_ist": "2026-06-07 10:00:00+05:30",
        "signal_time_ist": "2026-06-07 10:00:00+05:30",
        "entry_price": 2500.0,
    }
    df = pd.DataFrame([row])
    raw_by_ticker: dict = {}  # empty — no 1-min bars → missing features

    kept, rejected, stats = ee._apply_pre_entry_momentum_gate(df, raw_by_ticker)
    assert len(kept) == 0, "Missing features must block when action=block"
    assert len(rejected) == 1
    assert "missing" in str(rejected.iloc[0].get("reject_reason", "")).lower()


def test_missing_features_pass_when_action_allow(monkeypatch):
    import eqidv2_entry_engine_1min_v5_id as ee
    monkeypatch.setattr(ee, "PRE_ENTRY_MOMENTUM_MISSING_ACTION", "allow")
    monkeypatch.setattr(ee, "PRE_ENTRY_MOMENTUM_GATES_ENABLED", True)

    row = {
        "ticker": "RELIANCE",
        "side": "LONG",
        "setup": "C_OR_BREAKOUT",
        "bar_time_ist": "2026-06-07 10:00:00+05:30",
        "signal_time_ist": "2026-06-07 10:00:00+05:30",
        "entry_price": 2500.0,
    }
    df = pd.DataFrame([row])
    raw_by_ticker: dict = {}

    kept, rejected, stats = ee._apply_pre_entry_momentum_gate(df, raw_by_ticker)
    assert len(kept) == 1, "Missing features should PASS when action=allow (backward compat)"


def test_ungated_setup_always_passes(monkeypatch):
    """A setup with no gate entry in PRE_ENTRY_MOMENTUM_SETUP_GATES always passes."""
    import eqidv2_entry_engine_1min_v5_id as ee
    monkeypatch.setattr(ee, "PRE_ENTRY_MOMENTUM_MISSING_ACTION", "block")
    monkeypatch.setattr(ee, "PRE_ENTRY_MOMENTUM_GATES_ENABLED", True)

    row = {
        "ticker": "RELIANCE",
        "side": "LONG",
        "setup": "UNLISTED_TEST_SETUP",
        "bar_time_ist": "2026-06-07 10:00:00+05:30",
        "signal_time_ist": "2026-06-07 10:00:00+05:30",
        "entry_price": 2500.0,
    }
    df = pd.DataFrame([row])
    raw_by_ticker: dict = {}

    kept, rejected, stats = ee._apply_pre_entry_momentum_gate(df, raw_by_ticker)
    assert len(kept) == 1, "Ungated setup must always pass regardless of missing_action"
