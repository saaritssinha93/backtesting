"""New opening-side gates use the same evidence in scanner and dashboard."""
from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

import fno_eq_id_monitor_detail as detail
import fno_oi_common as common
import fno_v13_v10_g_3_live_config as config
import fno_v13_v10_g_3_opening_inputs as opening
from fno_strategy_registry import PROFILES


DAY = date(2026, 10, 15)


@pytest.fixture
def opening_scanner(tmp_path, monkeypatch):
    monkeypatch.setenv("FNO_LIVE_GENERATION", "v6")
    monkeypatch.setenv("FNO_V6_STRATEGY_PROFILE", "V13_V10_G_3")
    monkeypatch.setattr(common, "FNO_ROOT", tmp_path / "fno_oi")
    monkeypatch.setattr(common, "LATEST_DIR", tmp_path / "latest")
    path = Path(__file__).resolve().parents[1] / "fno_v5_live.py"
    spec = importlib.util.spec_from_file_location("g3_two_slot_runtime_fixture", path)
    runtime = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, runtime)
    spec.loader.exec_module(runtime)
    stamp = pd.Timestamp(config.slot_datetime(DAY, "09:20"))
    monkeypatch.setattr(common, "now_ist", lambda: stamp.to_pydatetime() + timedelta(seconds=10))
    future = pd.DataFrame([dict(ts=stamp, timestamp=stamp, tradingsymbol="TEST26OCTFUT",
        expiry="2026-10-27", oi=100500., fetch_timestamp=f"{DAY}T09:20:04+05:30")])
    cash = pd.DataFrame([dict(ts=stamp, open=100., high=100.1, low=99., close=99.5, volume=3000.,
        source_1m_count=5, ema9=99., ema20=100., ema50=101., volume_ratio=3., traded_value=298500.,
        oi=100500., prev_oi=100000., oi_change_pct=.5, price_change_pct=-.5)])
    universe = pd.DataFrame([dict(equity_symbol="TEST", futures_tradingsymbol="TEST26OCTFUT",
        expiry="2026-10-27", equity_instrument_token=1, futures_instrument_token=2)])
    evidence = dict(opening_0920_status="VALID", price_change_basis="09:15_CASH_OPEN_TO_09:20_CASH_CLOSE",
        oi_change_source="FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M", opening_evidence_sha256="a" * 64,
        price_change_pct=-.5, oi_change_pct=.5, oi=100500., prev_oi=100000.)
    monkeypatch.setattr(opening, "load_futures_frame", lambda _path: future)
    monkeypatch.setattr(opening, "build_opening_features", lambda *args, **kwargs: dict(evidence))
    monkeypatch.setattr(runtime.hybrid, "load_equity_five_minute", lambda *args, **kwargs: cash)
    monkeypatch.setattr(runtime.hybrid, "join_equity_price_with_futures_oi", lambda *args: cash.copy())
    monkeypatch.setattr(runtime.backtest, "load_five_minute", lambda *args: pytest.fail("Opening receipt metadata was dropped"))
    return runtime, universe, evidence


@pytest.mark.parametrize("nifty_return,expected", [(-.05, True), (0., False), (None, False)])
def test_0920_short_fetches_index_context_and_records_its_guard(opening_scanner, monkeypatch, nifty_return, expected):
    runtime, universe, _ = opening_scanner
    requested = []

    def index_context(day):
        requested.append(day)
        return dict(nifty_first_bar_return_pct=nifty_return,
                    nifty_context_state="READY" if nifty_return is not None else "MISSING")

    monkeypatch.setattr(runtime, "_g_nifty_first_bar_context", index_context)
    snapshot = runtime.scan_five_minute_slot(universe, DAY, "09:20")
    assert requested == [DAY]
    assert snapshot["short_candidates"] == int(expected)
    row = snapshot["feature_evaluations"][0]
    assert row["gate_nifty_0925_short"] is expected
    assert row["nifty_short_guard_applies"] is True
    if not expected:
        assert row["first_failed_gate"] == "gate_nifty_0925_short"
    monkeypatch.setattr(detail, "ACTIVE_PROFILE", PROFILES["V13_V10_G_3"])
    gate = detail._gate(row, "gate_nifty_0925_short", "5m", pinned=True)
    assert gate["status"] == ("PASS" if expected else "FAIL")
    assert "Opening SHORT NIFTY guard" == gate["label"]


def test_opening_exclusion_discloses_both_sides_without_doubling_stock_count(opening_scanner, monkeypatch, tmp_path):
    runtime, universe, evidence = opening_scanner
    evidence["opening_0920_status"] = "MISSING_BASELINE"
    monkeypatch.setattr(runtime, "_g_nifty_first_bar_context", lambda _day: dict(nifty_first_bar_return_pct=-.1))
    snapshot = runtime.scan_five_minute_slot(universe, DAY, "09:20")
    assert len(snapshot["opening_0920_exclusions"]) == 1
    exclusion = snapshot["opening_0920_exclusions"][0]
    assert set(exclusion["setup_ids"]) == {"0921_LONG", "0921_SHORT"}
    assert set(exclusion["sides"]) == {"LONG", "SHORT"}
    profile = PROFILES["V13_V10_G_3"]
    monkeypatch.setattr(detail, "ACTIVE_PROFILE", profile)
    monkeypatch.setattr(detail, "STRATEGY_VERSION", profile.version)
    root = tmp_path / profile.live_dir
    path = root / "scanner_5m" / str(DAY) / "slot_0920.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(snapshot), encoding="utf-8")
    rows, coverage, _ = detail._read_phase(root, str(DAY), "09:20", "5m", config.slot_datetime(DAY, "10:00"), [])
    affected = [row for row in rows if row["decision"] == "OPENING_OI_EXCLUDED"]
    assert {row["setup_id"] for row in affected} == {"0921_LONG", "0921_SHORT"}
    assert len({row["id"] for row in affected}) == 2
    assert coverage["opening_excluded"] == 1


def test_other_profile_keeps_0925_only_nifty_display(monkeypatch):
    monkeypatch.setattr(detail, "ACTIVE_PROFILE", PROFILES["V13_V10_G"])
    row = dict(base_side="SHORT", signal_end="09:20", gate_nifty_0925_short=True)
    assert detail._gate(row, "gate_nifty_0925_short", "5m", pinned=True)["status"] == "NOT_APPLICABLE"


def test_recorded_nifty_applicability_preserves_prior_revision_display(monkeypatch):
    monkeypatch.setattr(detail, "ACTIVE_PROFILE", PROFILES["V13_V10_G_3"])
    row = dict(base_side="SHORT", signal_end="09:20", gate_nifty_0925_short=True,
               nifty_short_guard_applies=False)
    assert detail._gate(row, "gate_nifty_0925_short", "5m", pinned=False)["status"] == "NOT_APPLICABLE"
