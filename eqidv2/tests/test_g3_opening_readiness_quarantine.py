"""Offline opening-data guard tests; no market, credential or broker access."""
from __future__ import annotations

import copy
import importlib.util
import sys
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import fno_oi_common as common
import fno_live_entry_guard as guard

DAY = date(2026, 11, 2)
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def runtime(monkeypatch, tmp_path):
    monkeypatch.setenv("FNO_LIVE_GENERATION", "v6")
    monkeypatch.setenv("FNO_V6_STRATEGY_PROFILE", "V13_V10_G_3")
    monkeypatch.setattr(common, "FNO_ROOT", tmp_path / "fno_oi")
    monkeypatch.setattr(common, "LATEST_DIR", tmp_path / "latest")
    monkeypatch.setattr(guard, "deployment_state", lambda: {
        "profile": "V13_V10_G_3", "deployment_stage": "PAPER_VALIDATION",
        "live_activation_authorized": False})
    name = "g3_opening_quarantine_fixture"
    spec = importlib.util.spec_from_file_location(name, ROOT / "fno_v5_live.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    monkeypatch.setattr(module.config, "OPENING_VERIFIED_NO_CANDLE_QUARANTINE_ENABLED", True)
    return module


def universe(count=218):
    return pd.DataFrame([dict(exchange="NFO", tradingsymbol=f"STOCK{i}26NOVFUT",
        underlying=f"STOCK{i}", instrument_token=10000+i, expiry=pd.Timestamp("2026-11-24"),
        lot_size=100, tick_size=.05, futures_tradingsymbol=f"STOCK{i}26NOVFUT",
        futures_instrument_token=10000+i, futures_lot_size=100, futures_tick_size=.05,
        equity_symbol=f"STOCK{i}", equity_instrument_token=20000+i, equity_tick_size=.05)
        for i in range(count)])


def marker(runtime, frame, missing=3, clock="09:20", partial=True):
    names = set(frame.futures_tradingsymbol)
    skipped = set(sorted(names)[:missing])
    written = names - skipped
    return dict(schema_version=runtime.config.FNO_FETCH_SLOT_SCHEMA_VERSION,
        slot_ist=runtime.config.slot_datetime(DAY, clock).isoformat(), source="final",
        published_at_ist=runtime.config.slot_datetime(DAY, clock).isoformat(),
        state="PARTIAL" if partial else "SUCCESS", complete=not partial,
        stock_complete=not partial, attempt_complete=True,
        outcome_symbol_set_complete=True, stock_outcome_symbol_set_complete=True,
        contracts_expected=len(names), contracts_written=len(written),
        no_candle_count=len(skipped), no_candle_symbols=sorted(skipped),
        verified_no_candle_symbols=sorted(skipped), unverified_no_candle_symbols=[],
        failed_count=0, invalid_data_count=0, stock_failed_count=0, stock_invalid_data_count=0,
        stock_contracts_expected=len(names), stock_contracts_written=len(written),
        stock_written_symbols=sorted(written), stock_no_candle_count=len(skipped),
        stock_no_candle_symbols=sorted(skipped), stock_verified_no_candle_count=len(skipped),
        stock_verified_no_candle_symbols=sorted(skipped), stock_unverified_no_candle_symbols=[],
        stock_coverage_ratio=len(written)/len(names),
        stock_universe_sha256=common.universe_sha256(frame),
        stock_symbol_set_sha256=common.symbol_set_sha256(names),
        minimum_stock_coverage=.99, minimum_coverage=.99, maximum_verified_no_candle_stocks=2,
        minimum_no_candle_fetch_attempts=3, readiness_policy="verified_stock_no_candle_skip_v1",
        no_candle_observations={s:3 for s in skipped}, no_candle_fetch_attempts={s:3 for s in skipped})


def cash_marker(runtime, frame):
    count = len(frame)
    return dict(slot_ist=runtime.config.slot_datetime(DAY, "09:20").isoformat(),
        source="final", complete=True, tickers_expected=count, tickers_written=count,
        tickers_complete=count, tickers_failed=0, fno_equity_quality_complete=True,
        fno_equity_expected=count, fno_equity_ready=count, fno_equity_failed=0,
        fno_equity_universe_sha256=runtime._equity_universe_sha256(frame))


def wire_markers(runtime, monkeypatch, frame, futures, cash):
    slot = runtime.config.slot_datetime(DAY, "09:20")
    mapping = {common.fetch_slot_path(slot): futures, common.cash_slot_path(slot): cash}
    monkeypatch.setattr(runtime, "_read_json", lambda path: copy.deepcopy(mapping.get(path, {})))
    monkeypatch.setattr(runtime, "_archive_json_evidence", lambda *a, **k: None)
    monkeypatch.setattr(runtime, "_archive_mapped_universe_evidence", lambda *a, **k: None)
    return mapping


def test_approved_opening_quarantine_preserves_producer_marker(runtime):
    frame = universe(); source = marker(runtime, frame); before = copy.deepcopy(source)
    error, evidence = runtime._fno_marker_readiness_decision(source, frame, DAY, "09:20")
    assert not error
    assert evidence["degraded_readiness"] is True
    assert evidence["producer_marker_state"] == "PARTIAL"
    assert evidence["producer_marker_complete"] is False
    assert evidence["stock_contracts_written"] == 215
    assert evidence["quarantined_futures_symbols"] == source["stock_verified_no_candle_symbols"]
    assert evidence["producer_marker_sha256"] == runtime.canonical_payload_sha256(source)
    assert source == before


@pytest.mark.parametrize("scope", ["disabled", "later_slot", "other_profile"])
def test_exception_never_leaks_into_strict_default_or_other_scopes(runtime, monkeypatch, scope):
    frame = universe(); clock = "09:25" if scope == "later_slot" else "09:20"
    source = marker(runtime, frame, clock=clock)
    if scope == "disabled":
        monkeypatch.setattr(runtime.config, "OPENING_VERIFIED_NO_CANDLE_QUARANTINE_ENABLED", False)
    if scope == "other_profile":
        monkeypatch.setattr(runtime, "is_g3_config", lambda config: False)
    error, evidence = runtime._fno_marker_readiness_decision(source, frame, DAY, clock)
    assert error and not evidence


@pytest.mark.parametrize("count,missing,accepted", [(218,3,True),(200,3,True),(199,3,False),
    (218,4,False),(218,218,False),(218,0,False),(400,3,True)])
def test_quarantine_count_and_coverage_boundaries(runtime, count, missing, accepted):
    frame = universe(count); source = marker(runtime, frame, missing)
    error, _ = runtime._fno_marker_readiness_decision(source, frame, DAY, "09:20")
    assert (not error) is accepted


@pytest.mark.parametrize("field,value", [
    ("failed_count",1),("invalid_data_count",1),("stock_failed_count",1),("stock_invalid_data_count",1),
    ("complete",True),("stock_complete",True),("attempt_complete",False),
    ("stock_outcome_symbol_set_complete",False),("outcome_symbol_set_complete",False),
    ("stock_symbol_set_sha256","bad"),("stock_universe_sha256","bad"),
    ("stock_written_symbols",[]),("stock_contracts_written",216),("stock_coverage_ratio",1.),
    ("stock_verified_no_candle_symbols",[]),("stock_unverified_no_candle_symbols",["STOCK026NOVFUT"]),
    ("verified_no_candle_symbols",[]),("unverified_no_candle_symbols",["INDEX26NOVFUT"]),
    ("no_candle_observations",{}),("no_candle_fetch_attempts",{}),
    ("minimum_stock_coverage",.985),("minimum_coverage",.985),
    ("maximum_verified_no_candle_stocks",3),("minimum_no_candle_fetch_attempts",2),
    ("schema_version","untrusted"),("source","provisional"),
    ("slot_ist","2026-10-30T09:20:00+05:30"),
])
def test_malformed_or_unhealthy_evidence_never_qualifies(runtime, field, value):
    frame = universe(); source = marker(runtime, frame); source[field] = value
    error, evidence = runtime._fno_marker_readiness_decision(source, frame, DAY, "09:20")
    assert error and not evidence


def test_missing_error_fields_cannot_look_like_zero(runtime):
    frame = universe(); source = marker(runtime, frame); del source["failed_count"]
    assert runtime._fno_marker_readiness_decision(source, frame, DAY, "09:20")[0]
    assert runtime._fno_marker_readiness_decision({}, frame, DAY, "09:20")[0]
    assert runtime._fno_marker_readiness_decision(source, pd.DataFrame(), DAY, "09:20")[0]


def test_strict_success_has_no_quarantine_label(runtime):
    frame = universe(); source = marker(runtime, frame, missing=1, partial=False)
    assert runtime._fno_marker_readiness_decision(source, frame, DAY, "09:20") == ("", {})


def test_config_cannot_widen_quarantine_past_reviewed_hard_bounds(runtime, monkeypatch):
    monkeypatch.setattr(runtime.config,"OPENING_QUARANTINE_MAX_STOCKS",100)
    monkeypatch.setattr(runtime.config,"OPENING_QUARANTINE_MIN_STOCK_COVERAGE",.50)
    for count, missing in ((218,4),(199,3)):
        frame=universe(count); source=marker(runtime,frame,missing)
        assert runtime._fno_marker_readiness_decision(source,frame,DAY,"09:20")[0]


def test_readiness_and_skips_share_policy_and_cash_checks(runtime, monkeypatch):
    frame = universe(); source = marker(runtime, frame); cash = cash_marker(runtime, frame)
    mapping = wire_markers(runtime, monkeypatch, frame, source, cash)
    assert runtime._slot_marker_ready(DAY,"09:20",frame) == (True,"ready")
    assert runtime._verified_no_candle_symbols_for_slot(DAY,"09:20",frame) == set(source["stock_verified_no_candle_symbols"])
    assert runtime._opening_readiness_evidence_for_slot(DAY,"09:20",frame)["degraded_readiness"]
    cash_path = common.cash_slot_path(runtime.config.slot_datetime(DAY,"09:20"))
    mapping[cash_path] = {}
    assert runtime._slot_marker_ready(DAY,"09:20",frame) == (False,"cash_5m_marker_missing")
    mapping[cash_path] = {**cash,"complete":False}
    assert runtime._slot_marker_ready(DAY,"09:20",frame) == (True,"ready")


def test_missing_confirmation_still_blocks_all_entries(runtime, monkeypatch):
    snapshot = dict(state="SUCCESS", strategy_version=runtime.config.STRATEGY_VERSION,
        strategy_fingerprint=runtime.config.strategy_fingerprint(), session_date=str(DAY),
        signal_end="09:20", data_contract=runtime.hybrid.DATA_CONTRACT_VERSION,
        candidates=[{"tradingsymbol":"HEALTHY"}], opening_readiness={"degraded_readiness":True})
    monkeypatch.setattr(runtime,"_load_completed_confirmation_feed",lambda *a: ({},{"_feed":"missing"},{}))
    monkeypatch.setattr(runtime,"select_entry_signals",lambda *a,**k:pytest.fail("Missing confirmation reached entry selection"))
    result = runtime.process_confirmation_slot(snapshot,DAY,"09:20",None,SimpleNamespace(capital=100000,leverage=5))
    assert result["state"] == "BLOCKED_INCOMPLETE_DATA"
    assert result["_selected_signals"] == []


def scaffold_scanner(runtime, monkeypatch, frame):
    monkeypatch.setattr(runtime.config,"SIGNAL_TO_CONFIRMATION",{"09:20":"09:21"})
    monkeypatch.setattr(runtime,"_load_universe",lambda *a:frame)
    monkeypatch.setattr(runtime,"_render_scanner_report",lambda *a:"fixture")
    monkeypatch.setattr(runtime,"_current_slot_snapshot",lambda *a:False)
    monkeypatch.setattr(runtime.common,"atomic_write_text",lambda *a:None)
    monkeypatch.setattr(runtime,"_heartbeat",lambda *a,**k:None)
    monkeypatch.setattr(runtime,"_publish",lambda *a,**k:None)
    saved=[]
    monkeypatch.setattr(runtime,"_write_scanner_snapshot",lambda day,slot,snapshot:saved.append(copy.deepcopy(snapshot)))
    return saved


def test_scanner_success_records_degraded_readiness_and_exact_skips(runtime, monkeypatch):
    frame=universe(); source=marker(runtime,frame)
    wire_markers(runtime,monkeypatch,frame,source,cash_marker(runtime,frame))
    saved=scaffold_scanner(runtime,monkeypatch,frame)
    monkeypatch.setattr(runtime.common,"now_ist",lambda:runtime.config.slot_datetime(DAY,"09:20")+timedelta(seconds=30))
    observed=[]
    def scan(*a,**kwargs):
        observed.append(kwargs["verified_no_candle_symbols"])
        return dict(state="SUCCESS",contracts_evaluated=215,long_candidates=0,short_candidates=0,candidates=[],raw_data_quality=[])
    monkeypatch.setattr(runtime,"scan_five_minute_slot",scan)
    args=SimpleNamespace(once=True,boundary_buffer_sec=3.,poll_sec=1.,ignore_fetch_marker=False)
    assert runtime.run_scanner(args,DAY)==0
    assert observed == [set(source["stock_verified_no_candle_symbols"])]
    assert saved[0]["state"] == "SUCCESS"
    assert saved[0]["opening_readiness"]["producer_marker_state"] == "PARTIAL"


def test_waiting_input_deadline_is_not_misreported_as_restart(runtime, monkeypatch):
    frame=universe(); saved=scaffold_scanner(runtime,monkeypatch,frame)
    current=[runtime.config.slot_datetime(DAY,"09:20")+timedelta(seconds=30)]
    monkeypatch.setattr(runtime.common,"now_ist",lambda:current[0])
    monkeypatch.setattr(runtime,"_slot_marker_ready",lambda *a:(False,"fno_fetch_marker_PARTIAL"))
    monkeypatch.setattr(runtime,"scan_five_minute_slot",lambda *a,**k:pytest.fail("Unready input was scanned"))
    def advance(*a):
        current[0]=runtime.config.slot_datetime(DAY,"09:21")+timedelta(seconds=91)
    monkeypatch.setattr(runtime.time,"sleep",advance)
    args=SimpleNamespace(once=False,boundary_buffer_sec=3.,poll_sec=1.,ignore_fetch_marker=False)
    assert runtime.run_scanner(args,DAY)==2
    assert saved[0]["state"] == "BLOCKED_STALE_ACTIVATION"
    assert "restart" not in saved[0]["stale_reason"].lower()
    assert saved[0]["last_readiness_wait"]["reason"] == "fno_fetch_marker_PARTIAL"
    assert saved[0]["candidates"] == []
