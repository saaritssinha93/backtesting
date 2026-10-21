"""Offline integration fixtures: no credentials, worker starts or broker traffic."""
from __future__ import annotations

import importlib.util
import json
import sys
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import fno_oi_common as common
import fno_v13_v10_g_3_live_config as config
import fno_live_entry_guard as guard

ROOT = Path(__file__).resolve().parents[1]
DAY = date(2026, 10, 12)
MIDDAY_PROMOTED_CASES = [
    ("11:35", "LONG", 7.15, 1.0), ("11:35", "SHORT", .87, .4),
    ("11:40", "LONG", 4.03, 0.), ("11:40", "SHORT", 1.19, 1.2),
    ("11:45", "SHORT", 4.95, 0.), ("12:05", "LONG", 1.4, .6),
    ("12:10", "SHORT", .79, 0.), ("12:15", "LONG", 1.24, 1.0),
    ("12:15", "SHORT", 3.49, 0.), ("12:25", "SHORT", 2.13, 0.),
    ("12:30", "LONG", 2.53, .6), ("12:30", "SHORT", 1.85, .4),
]
AFTERNOON_PROMOTED_CASES = [
    ("12:35", "LONG", .37, .2), ("12:35", "SHORT", 1.24, 0.),
    ("12:45", "LONG", .23, 0.), ("12:45", "SHORT", .49, .8),
    ("12:50", "LONG", .26, .6), ("12:50", "SHORT", .14, .8),
    ("12:55", "SHORT", .14, .4), ("13:00", "SHORT", .88, .8),
    ("13:05", "LONG", .18, 0.), ("13:15", "LONG", .17, .6),
    ("13:15", "SHORT", .33, 1.3), ("13:20", "SHORT", 1.01, 0.),
    ("13:25", "LONG", .41, .2), ("13:25", "SHORT", .76, 1.2),
    ("13:30", "LONG", 1.17, .8),
]


def isolated(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("FNO_LIVE_GENERATION", "v6")
    monkeypatch.setenv("FNO_V6_STRATEGY_PROFILE", "V13_V10_G_3")
    monkeypatch.setenv("FNO_V6_EXECUTION_SESSION_NAMESPACE", "live_kite_qty1")
    monkeypatch.setattr(common, "FNO_ROOT", tmp_path / "fno_oi")
    monkeypatch.setattr(common, "LATEST_DIR", tmp_path / "latest")
    monkeypatch.setattr(guard, "deployment_state", lambda: {
        "profile": "V13_V10_G_3", "deployment_stage": "PAPER_VALIDATION", "live_activation_authorized": False})
    return isolated("g3_fixture_runtime", "fno_v5_live.py")


def test_g3_paper_order_snapshot_survives_a_full_windows_replace_retry_round(runtime, tmp_path, monkeypatch):
    path = tmp_path / "order.json"
    path.write_text('{"status":"PENDING_ENTRY"}', encoding="utf-8")
    replace = common.os.replace
    calls = []

    def temporarily_locked(source, target):
        calls.append((source, target))
        if len(calls) <= common.ATOMIC_REPLACE_ATTEMPTS:
            raise PermissionError(13, "Access is denied", str(source), 5, str(target))
        return replace(source, target)

    monkeypatch.setattr(common.os, "replace", temporarily_locked)
    monkeypatch.setattr(common.time_module, "sleep", lambda _seconds: None)
    runtime._persist_order_snapshot(path, {"mode": "PAPER", "status": "OPEN"})

    assert len(calls) == common.ATOMIC_REPLACE_ATTEMPTS + 1
    assert json.loads(path.read_text(encoding="utf-8"))["status"] == "OPEN"
    assert not list(tmp_path.glob("*.tmp"))


def test_g3_live_order_write_keeps_original_failure_behavior(runtime, tmp_path, monkeypatch):
    path = tmp_path / "order.json"
    calls = []

    def denied(target, _payload):
        calls.append(target)
        raise PermissionError(13, "Access is denied", "temp", 5, str(target))

    monkeypatch.setattr(common, "atomic_write_json", denied)
    with pytest.raises(PermissionError):
        runtime._persist_order_snapshot(path, {"mode": "LIVE", "status": "OPEN"})
    assert calls == [path]


def candidate(side="LONG", ratio=1.1, clock="09:30"):
    stamp = config.slot_datetime(DAY, clock)
    long = side == "LONG"
    return dict(tradingsymbol="TEST", instrument_token=1, futures_tradingsymbol="TEST26OCTFUT",
        futures_instrument_token=2, exchange="NSE", lot_size=1, tick_size=.05,
        data_contract="test", side=side, signal_timestamp=stamp.isoformat(), signal_ts=stamp.isoformat(),
        signal_end=clock, confirmation_timestamp=(stamp+timedelta(minutes=1)).isoformat(),
        v9_1m_feature_ts=(stamp+timedelta(minutes=1)).isoformat(), signal_close=100.,
        ema9=101. if long else 99., ema20=100., ema50=99. if long else 101.,
        price_change_pct=.8 if long else -.8, oi=1005., prev_oi=1000., oi_change_pct=.5,
        volume_ratio=2., traded_value=2_000_000., confirmed=True, body_ratio=.6, wick_ratio=.1,
        trigger=102. if long else 98., v9_1m_volume_ratio=ratio, nifty_first_bar_return_pct=-.1)


def test_runtime_profile_selection_identity_and_quantity(runtime):
    assert runtime.LIVE_ROOT.name == "v13_v10_g_3_live"
    assert runtime.CONTROL_ROOT.name == "v13_v10_g_3_live"
    assert runtime.ORDER_TAG_PREFIX == "FVG3"
    rows = runtime.select_entry_signals([candidate()], DAY, "09:30", capital_rs=100000., leverage=5.)
    assert len(rows) == 1
    signal = rows[0]
    assert signal["stop_pct"] == 1.25
    assert signal["signal_id_schema_version"] == config.SIGNAL_ID_SCHEMA_VERSION
    assert signal["strategy_policy"]["relaxed_0925_long"] is False
    assert signal["live_sizing"]["quantity"] == 490
    assert signal["paper_sizing"]["quantity"] == 4901
    assert runtime.create_order_state(signal, "LIVE", live_quantity=100)["quantity"] == 100
    assert runtime.create_order_state(signal, "LIVE", live_quantity=10000)["quantity"] == 490
    assert signal["signal_id"] == config.canonical_signal_id(config.STRATEGY_VERSION, DAY, "09:30", "09:31", "LONG", "TEST")


def test_late_live_activation_blocks_recent_pre_start_signal_before_broker_pool(
    runtime, monkeypatch
):
    now = datetime.fromisoformat("2026-10-12T09:32:00+05:30")
    signal = runtime.select_entry_signals(
        [candidate()], DAY, "09:30", capital_rs=100000.0, leverage=5.0
    )[0]
    # This signal is still inside its normal ten-minute trigger window, but it
    # was committed before both the approval and this coordinator start.
    signal["published_at_ist"] = "2026-10-12T09:31:20+05:30"
    common.atomic_write_json(
        runtime.LIVE_ARM_PATH,
        {
            "enabled": True,
            "session_date": DAY.isoformat(),
            "entry_not_before_ist": "2026-10-12T09:31:30+05:30",
        },
    )
    monkeypatch.setenv(
        runtime.G3_LIVE_ACTIVATION_EPOCH_ENV,
        "2026-10-12T09:31:45+05:30",
    )
    monkeypatch.setattr(runtime.common, "now_ist", lambda: now)
    monkeypatch.setattr(runtime, "load_signals", lambda *_args, **_kwargs: [signal])
    monkeypatch.setattr(runtime, "_blocking_pipeline_issue", lambda *_args: None)
    monkeypatch.setattr(runtime, "_render_worker_report", lambda *_args: "offline")
    monkeypatch.setattr(runtime, "_publish", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "KitePool",
        lambda *_args, **_kwargs: pytest.fail(
            "Pre-activation signal constructed a broker pool"
        ),
    )
    args = SimpleNamespace(
        execution_mode="LIVE",
        live_quantity=None,
        max_apps=1,
        timeout_sec=0.1,
        once=True,
    )

    assert runtime.run_worker(args, DAY, "LONG") == 0
    stored = json.loads(
        runtime._order_path(DAY, "LIVE", signal["signal_id"]).read_text(
            encoding="utf-8"
        )
    )
    assert stored["status"] == "CANCELLED"
    assert stored["status_reason"] == "PRE_ACTIVATION_SIGNAL_BLOCKED"
    assert stored["entry_order_id"] == ""
    assert stored["entry_submission_attempt_count"] == 0


def test_live_activation_epoch_allows_only_signal_published_after_start(
    runtime, monkeypatch
):
    now = datetime.fromisoformat("2026-10-12T09:32:00+05:30")
    signal = runtime.select_entry_signals(
        [candidate()], DAY, "09:30", capital_rs=100000.0, leverage=5.0
    )[0]
    signal["published_at_ist"] = "2026-10-12T09:31:50+05:30"
    common.atomic_write_json(
        runtime.LIVE_ARM_PATH,
        {
            "enabled": True,
            "session_date": DAY.isoformat(),
            "entry_not_before_ist": "2026-10-12T09:31:30+05:30",
        },
    )
    monkeypatch.setenv(
        runtime.G3_LIVE_ACTIVATION_EPOCH_ENV,
        "2026-10-12T09:31:45+05:30",
    )
    state = runtime.create_order_state(signal, "LIVE")

    assert runtime._block_pre_activation_g3_entry(signal, state, now) is False
    assert state["status"] == "PENDING_ENTRY"
    assert state["live_entry_not_before_ist"] == "2026-10-12T09:31:45+05:30"


def test_runtime_side_thresholds_and_strict_0925(runtime):
    assert runtime.select_entry_signals([candidate(ratio=1.099)], DAY, "09:30", capital_rs=100000., leverage=5.) == []
    assert runtime.select_entry_signals([candidate("SHORT", 1.199)], DAY, "09:30", capital_rs=100000., leverage=5.) == []
    row = candidate(clock="09:25")
    row.update(ema9=99., ema20=101., ema50=100.)
    assert runtime.select_entry_signals([row], DAY, "09:25", capital_rs=100000., leverage=5.) == []


@pytest.mark.parametrize("clock,side", [("09:25","LONG"),("09:25","SHORT"),("09:30","LONG"),("09:30","SHORT"),("09:50","LONG"),("09:50","SHORT"), ("10:35","LONG"), ("10:35","SHORT"), ("10:40","LONG"),
    ("10:45","LONG"), ("10:45","SHORT"), ("10:50","LONG"), ("10:55","SHORT"),
    ("11:10","LONG"), ("11:10","SHORT"), ("11:25","SHORT")]
    + [(clock, side) for clock, side, _, _ in MIDDAY_PROMOTED_CASES + AFTERNOON_PROMOTED_CASES])
def test_scanner_adapts_exact_join_timestamp_to_g3_contract(runtime,monkeypatch,clock,side):
    # The real equity/futures join returns `ts`, not either of the decision
    # contract's signal timestamp keys. A missing adapter silently rejects all
    # candidates while the scanner still reports SUCCESS.
    raw = candidate(side=side,clock=clock)
    for key in ("signal_timestamp","signal_ts","confirmation_timestamp","v9_1m_feature_ts"):
        raw.pop(key,None)
    raw.update(ts=pd.Timestamp(config.slot_datetime(DAY,clock)),open=99.,high=102.,low=98.,close=100.,volume=2000.)
    frame = pd.DataFrame([raw])
    universe = pd.DataFrame([dict(futures_tradingsymbol="TEST26OCTFUT",equity_symbol="TEST",
        equity_instrument_token=1,futures_instrument_token=2,equity_tick_size=.05)])
    monkeypatch.setattr(runtime.backtest,"load_five_minute",lambda *a,**k:frame.copy())
    monkeypatch.setattr(runtime.hybrid,"load_equity_five_minute",lambda *a,**k:frame.copy())
    monkeypatch.setattr(runtime.hybrid,"join_equity_price_with_futures_oi",lambda *a,**k:frame.copy())
    monkeypatch.setattr(runtime,"_g_nifty_first_bar_context",lambda *a:{"nifty_first_bar_return_pct":-.1})
    snapshot = runtime.scan_five_minute_slot(universe,DAY,clock)
    assert snapshot["state"] == "SUCCESS"
    assert len(snapshot["candidates"]) == 1
    row = snapshot["candidates"][0]
    assert row["side"] == side
    assert row["signal_timestamp"] == config.slot_datetime(DAY,clock).isoformat()
    assert snapshot["strategy_fingerprint"] == config.strategy_fingerprint()


def test_scanner_restart_preserves_completed_slots_and_waits_for_future(runtime,monkeypatch):
    saved = {}
    for clock in ("09:20","09:25","09:30"):
        path = runtime.scanner_slot_path(DAY,clock)
        common.atomic_write_json(path,dict(state="SUCCESS",strategy_version=config.STRATEGY_VERSION,
            strategy_fingerprint=config.strategy_fingerprint(),session_date=DAY.isoformat(),signal_end=clock,
            long_candidates=0,short_candidates=0,candidates=[]))
        saved[path] = path.read_bytes()
        assert runtime._current_slot_snapshot(path,DAY,clock)
    monkeypatch.setattr(runtime,"_load_universe",lambda *a:pd.DataFrame())
    monkeypatch.setattr(runtime,"_render_scanner_report",lambda *a:"offline fixture")
    monkeypatch.setattr(runtime.common,"now_ist",lambda:config.slot_datetime(DAY,"09:34"))
    monkeypatch.setattr(runtime,"_slot_marker_ready",lambda *a:pytest.fail("Completed/future slot re-entered readiness"))
    monkeypatch.setattr(runtime,"scan_five_minute_slot",lambda *a,**k:pytest.fail("Completed slot recomputed"))
    heartbeats = []
    monkeypatch.setattr(runtime,"_heartbeat",lambda *a,**k:heartbeats.append(k))
    class StopPolling(Exception):
        pass
    def stop(*a):
        raise StopPolling()
    monkeypatch.setattr(runtime.time,"sleep",stop)
    with pytest.raises(StopPolling):
        runtime.run_scanner(SimpleNamespace(once=False,boundary_buffer_sec=3.,poll_sec=1.,ignore_fetch_marker=False),DAY)
    assert heartbeats[-1]["next_slot"] == "09:35"
    assert heartbeats[-1]["processed_slots"] == 3
    assert all(path.read_bytes() == before for path,before in saved.items())


def test_scanner_restart_prioritizes_live_slot_and_tombstones_expired_backlog(
    runtime, monkeypatch
):
    monkeypatch.setattr(
        runtime.config,
        "SIGNAL_TO_CONFIRMATION",
        {"09:20": "09:21", "09:25": "09:26", "09:30": "09:31"},
    )
    universe = pd.DataFrame([{"futures_tradingsymbol": "TEST26OCTFUT"}])
    monkeypatch.setattr(runtime, "_load_universe", lambda *_a: universe)
    monkeypatch.setattr(runtime, "_render_scanner_report", lambda *_a: "fixture")
    monkeypatch.setattr(runtime, "_slot_marker_ready", lambda *_a: (True, "ready"))
    monkeypatch.setattr(
        runtime, "_verified_no_candle_symbols_for_slot", lambda *_a: set()
    )
    now = config.slot_datetime(DAY, "09:26") + timedelta(seconds=30)
    monkeypatch.setattr(runtime.common, "now_ist", lambda: now)
    scanned: list[str] = []

    def scan(_universe, _day, signal_end, **_kwargs):
        scanned.append(signal_end)
        return {
            "state": "SUCCESS",
            "long_candidates": 0,
            "short_candidates": 0,
            "contracts_evaluated": 1,
            "candidates": [],
            "raw_data_quality": [],
        }

    monkeypatch.setattr(runtime, "scan_five_minute_slot", scan)
    monkeypatch.setattr(runtime, "_publish", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "_heartbeat", lambda *_a, **_k: None)

    class StopPolling(Exception):
        pass

    monkeypatch.setattr(
        runtime.time, "sleep", lambda *_a: (_ for _ in ()).throw(StopPolling())
    )
    args = SimpleNamespace(
        once=False,
        boundary_buffer_sec=3.0,
        confirmation_max_wait_sec=90.0,
        poll_sec=1.0,
        ignore_fetch_marker=False,
    )

    with pytest.raises(StopPolling):
        runtime.run_scanner(args, DAY)

    assert scanned == ["09:25"]
    stale = json.loads(runtime.scanner_slot_path(DAY, "09:20").read_text())
    current = json.loads(runtime.scanner_slot_path(DAY, "09:25").read_text())
    assert stale["state"] == "BLOCKED_STALE_ACTIVATION"
    assert stale["candidates"] == []
    assert stale["scanner_complete"] is False
    assert current["state"] == "SUCCESS"


def test_scanner_discards_candidates_when_scan_finishes_after_deadline(
    runtime, monkeypatch
):
    monkeypatch.setattr(
        runtime.config, "SIGNAL_TO_CONFIRMATION", {"09:20": "09:21"}
    )
    universe = pd.DataFrame([{"futures_tradingsymbol": "TEST26OCTFUT"}])
    monkeypatch.setattr(runtime, "_load_universe", lambda *_a: universe)
    monkeypatch.setattr(runtime, "_render_scanner_report", lambda *_a: "fixture")
    monkeypatch.setattr(runtime, "_slot_marker_ready", lambda *_a: (True, "ready"))
    monkeypatch.setattr(
        runtime, "_verified_no_candle_symbols_for_slot", lambda *_a: set()
    )
    before = config.slot_datetime(DAY, "09:21") + timedelta(seconds=90)
    after = before + timedelta(microseconds=1)
    calls = 0

    def clock():
        nonlocal calls
        calls += 1
        return before if calls <= 3 else after

    monkeypatch.setattr(runtime.common, "now_ist", clock)
    monkeypatch.setattr(
        runtime,
        "scan_five_minute_slot",
        lambda *_a, **_k: {
            "state": "SUCCESS",
            "long_candidates": 1,
            "short_candidates": 0,
            "contracts_evaluated": 1,
            "candidates": [{"side": "LONG"}],
            "raw_data_quality": [],
        },
    )
    monkeypatch.setattr(runtime, "_publish", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "_heartbeat", lambda *_a, **_k: None)
    args = SimpleNamespace(
        once=False,
        boundary_buffer_sec=3.0,
        confirmation_max_wait_sec=90.0,
        poll_sec=1.0,
        ignore_fetch_marker=False,
    )

    assert runtime.run_scanner(args, DAY) == 2
    stored = json.loads(runtime.scanner_slot_path(DAY, "09:20").read_text())
    assert stored["state"] == "BLOCKED_STALE_ACTIVATION"
    assert stored["candidates"] == []
    assert "completed after" in stored["stale_reason"]


@pytest.mark.parametrize("clock,expected_count,usable", [("09:20", 1, True), ("09:25", 2, False)])
def test_scanner_opening_quality_audits_only_the_completed_futures_bar(runtime, monkeypatch, clock, expected_count, usable):
    import fno_v13_v10_g_3_opening_inputs as opening_inputs
    raw = candidate(clock=clock)
    raw.update(ts=pd.Timestamp(config.slot_datetime(DAY, clock)), open=99., high=102., low=98., close=100., volume=2000.)
    frame = pd.DataFrame([raw])
    universe = pd.DataFrame([dict(futures_tradingsymbol="TEST26OCTFUT", equity_symbol="TEST",
        equity_instrument_token=1, futures_instrument_token=2, equity_tick_size=.05, expiry="2026-10-27")])
    monkeypatch.setattr(runtime.backtest, "load_five_minute", lambda *a, **k: frame.copy())
    monkeypatch.setattr(opening_inputs, "load_futures_frame", lambda *a, **k: frame.copy())
    monkeypatch.setattr(opening_inputs, "build_opening_features", lambda *a, **k: dict(
        opening_0920_status="VALID", price_change_pct=.5, oi_change_pct=.2, prev_oi=10000.))
    monkeypatch.setattr(runtime.hybrid, "load_equity_five_minute", lambda *a, **k: frame.copy())
    monkeypatch.setattr(runtime.hybrid, "join_equity_price_with_futures_oi", lambda *a, **k: frame.copy())
    monkeypatch.setattr(runtime, "_g_nifty_first_bar_context", lambda *a: {"nifty_first_bar_return_pct": -.1})
    snapshot = runtime.scan_five_minute_slot(universe, DAY, clock)
    audit = next(row for row in snapshot["raw_data_quality"] if row["source"] == "NFO_FUTURE_5M_LIVE")
    assert audit["expected_count"] == expected_count and audit["usable"] is usable
    assert audit["missing_timestamp_count"] == (0 if clock == "09:20" else 1)


def test_g3_staged_stop_for_all_dates_and_quote_free_expiry(runtime):
    assert runtime._g_stop_policy_metadata(date(2026, 8, 1))["tighten_after_minutes"] == 120
    signal = runtime.select_entry_signals([candidate()], DAY, "09:30", capital_rs=100000., leverage=5.)[0]
    state = runtime.create_order_state(signal, "PAPER")
    expired = runtime.advance_paper_order(state, None, config.slot_datetime(DAY, "09:42"))
    assert expired["status"] == "CANCELLED"
    state = runtime.create_order_state(signal, "PAPER")
    opened = runtime.advance_paper_order(state, 102., config.slot_datetime(DAY, "09:32"))
    assert opened["status"] == "OPEN"
    assert runtime._staged_stop_target(opened, config.slot_datetime(DAY, "11:31")) is None
    assert runtime._staged_stop_target(opened, config.slot_datetime(DAY, "11:32")) == 101.


def test_promoted_0950_long_passes_confirmation_and_sizes_both_execution_modes(runtime):
    row = candidate(clock="09:50", ratio=.6)
    row.update(price_change_pct=.1, oi_change_pct=.05, volume_ratio=1.3)
    bar = dict(timestamp=row["confirmation_timestamp"], open=100., high=102., low=99.5, close=101.5,
               volume=60., v9_1m_volume_ratio=.6, v9_1m_feature_ts=row["v9_1m_feature_ts"])
    confirmed = runtime.confirmation_metrics(row, bar)
    assert confirmed["confirmed"] is True
    assert confirmed["minimum_confirmation_volume_ratio"] == .6
    selected = runtime.select_entry_signals([confirmed], DAY, "09:50", capital_rs=100000., leverage=5.)
    assert len(selected) == 1
    signal = selected[0]
    assert signal["setup_id"] == "0951_LONG"
    assert signal["confirmation_end"] == "09:51"
    assert signal["target_pct"] == .75 and signal["stop_pct"] == 1.25
    assert signal["trigger"] == 102.
    assert signal["paper_sizing"]["quantity"] == 4901
    assert signal["live_sizing"]["quantity"] == 490
    assert config.entry_expiry(DAY, "09:51") == config.slot_datetime(DAY, "10:01")
    # Constructing either mode is offline; actual LIVE execution still uses its
    # independent deployment, acknowledgement, arm and portfolio gates.
    assert runtime.create_order_state(signal, "LIVE")["quantity"] == 490
    pending = runtime.create_order_state(signal, "PAPER")
    opened = runtime.advance_paper_order(pending, 102., config.slot_datetime(DAY, "09:52"))
    assert opened["status"] == "OPEN"
    assert runtime._staged_stop_target(opened, config.slot_datetime(DAY, "11:51")) is None
    assert runtime._staged_stop_target(opened, config.slot_datetime(DAY, "11:52")) == 101.


def test_0920_long_runtime_confirmation_entry_and_live_sizing_are_shared(runtime):
    row = candidate(clock="09:20", ratio=1.8)
    row.update(volume_ratio=3., opening_0920_status="VALID",
        price_change_basis="09:15_CASH_OPEN_TO_09:20_CASH_CLOSE", oi_change_source="PROVISIONAL_FULL_QUOTE",
        availability="LIVE_AS_OF_VERIFIED", oi_change_interval_seconds=237)
    bar = dict(timestamp=row["confirmation_timestamp"], open=100., high=102., low=99.9, close=101.8,
               volume=180., v9_1m_volume_ratio=1.8, v9_1m_feature_ts=row["v9_1m_feature_ts"])
    confirmed = runtime.confirmation_metrics(row, bar)
    selected = runtime.select_entry_signals([confirmed], DAY, "09:20", capital_rs=100000., leverage=5.)
    assert len(selected) == 1
    signal = selected[0]
    assert signal["setup_id"] == "0921_LONG"
    assert (signal["target_pct"], signal["stop_pct"]) == (3., 1.25)
    assert signal["oi_change_source"] == "PROVISIONAL_FULL_QUOTE"
    assert signal["paper_sizing"]["quantity"] == 4901
    assert signal["live_sizing"]["quantity"] == 490
    assert config.entry_expiry(DAY, "09:21") == config.slot_datetime(DAY, "09:31")
    opened = runtime.advance_paper_order(runtime.create_order_state(signal, "PAPER"), 102.,
                                         config.slot_datetime(DAY, "09:22"))
    assert opened["status"] == "OPEN"
    assert runtime.create_order_state(signal, "LIVE")["quantity"] == 490
    # Late/retrospective evidence is never substituted by the scanner; missing
    # evidence also fails at selection rather than accepting an old candidate.
    confirmed.pop("opening_0920_status")
    assert runtime.select_entry_signals([confirmed], DAY, "09:20", capital_rs=100000., leverage=5.) == []


@pytest.mark.parametrize("clock,side,target,ratio", [
    ("09:20", "SHORT", 1.5, 1.2), ("10:10", "LONG", 1.5, .6),
    ("10:05", "LONG", .56, .6), ("10:10", "SHORT", 1.56, 1.2),
    ("10:15", "LONG", .96, .6), ("10:15", "SHORT", .68, 1.2),
    ("10:25", "SHORT", 2.85, 1.2), ("10:30", "SHORT", 1.66, 1.2),
    ("10:35", "LONG", 2.12, .6), ("10:35", "SHORT", 2.86, .4),
    ("10:40", "LONG", 1.71, .6), ("10:45", "LONG", .79, .6),
    ("10:45", "SHORT", 1.01, .8), ("10:50", "LONG", .77, .4),
    ("10:55", "SHORT", 1.53, .8), ("11:10", "LONG", 1.76, .6),
    ("11:10", "SHORT", 1.84, .8), ("11:25", "SHORT", 4.20, .4)] + MIDDAY_PROMOTED_CASES + AFTERNOON_PROMOTED_CASES)
def test_promoted_confirmation_paper_fill_and_live_order_configuration(runtime, clock, side, target, ratio):
    row = candidate(side=side, clock=clock, ratio=ratio)
    if clock == "09:20":
        row.update(opening_0920_status="VALID", price_change_basis="09:15_CASH_OPEN_TO_09:20_CASH_CLOSE",
                   oi_change_source="FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M")
    setup = config.setup_for(clock, side)
    row["body_ratio"] = max(row["body_ratio"], setup.body_ratio)
    if setup.setup_id in config.contract.midday_setups.SETUP_IDS | config.contract.afternoon_setups.SETUP_IDS:
        row["price_change_pct"] = setup.price_change_pct * (1 if side == "LONG" else -1)
        row["volume_ratio"] = setup.volume_ratio
    bar = dict(timestamp=row["confirmation_timestamp"], open=100., high=102. if side == "LONG" else 100.1,
        low=99.9 if side == "LONG" else 98., close=101.8 if side == "LONG" else 98.2,
        volume=ratio*100, v9_1m_volume_ratio=ratio, v9_1m_feature_ts=row["v9_1m_feature_ts"])
    if clock == "10:35":
        bar.update(v9_1m_ema20=100., v9_1m_atr14=1.)
    confirmed = runtime.confirmation_metrics(row, bar)
    assert confirmed["confirmed"] and confirmed["minimum_confirmation_volume_ratio"] == ratio
    signals = runtime.select_entry_signals([confirmed], DAY, clock, capital_rs=100000., leverage=5.)
    assert len(signals) == 1
    signal = signals[0]
    assert signal["target_pct"] == target and signal["stop_pct"] == 1.25
    assert signal["confirmation_end"] == config.SIGNAL_TO_CONFIRMATION[clock]
    paper = runtime.create_order_state(signal, "PAPER")
    live = runtime.create_order_state(signal, "LIVE")
    assert live["target_pct"] == paper["target_pct"] == target
    assert live["quantity"] == int(50000 / signal["trigger"])
    assert paper["quantity"] == int(500000 / signal["trigger"])
    entered = config.slot_datetime(DAY, signal["confirmation_end"]) + timedelta(minutes=1)
    opened = runtime.advance_paper_order(paper, signal["trigger"], entered)
    assert opened["status"] == "OPEN"
    assert opened["target_price"] == config.bracket_levels(signal["trigger"], side, 1.25, target, .05)[1]
    assert not runtime.confirmation_metrics(row, {**bar, "v9_1m_volume_ratio": ratio - .000001})["confirmed"]
    assert not runtime.confirmation_metrics(row, {**bar, "v9_1m_volume_ratio": None})["confirmed"]


@pytest.mark.parametrize("clock,side", [(clock, side) for clock, side, _, ratio in MIDDAY_PROMOTED_CASES + AFTERNOON_PROMOTED_CASES if ratio == 0.])
@pytest.mark.parametrize("prior_count", [19, 20])
def test_zero_volume_minimum_keeps_live_feed_warmup(runtime, monkeypatch, clock, side, prior_count):
    import fno_equity_fetch_1min as feed
    row = candidate(side=side, clock=clock, ratio=0.)
    row["strategy_profile"] = config.STRATEGY_PROFILE
    start = config.slot_datetime(DAY, clock)
    end = start + timedelta(minutes=1)
    records = [dict(date=stamp, open=100., high=101., low=99., close=100., volume=100.)
        for stamp in pd.date_range(start-timedelta(minutes=prior_count), periods=prior_count, freq="min")]
    records.append(dict(date=start, open=100., high=102. if side == "LONG" else 100.1,
        low=99.9 if side == "LONG" else 98., close=101.8 if side == "LONG" else 98.2, volume=0.))
    provider = SimpleNamespace(client=SimpleNamespace(historical_data=lambda *a, **kw: records), pace=lambda: None)
    monkeypatch.setattr(feed, "_persist_bar", lambda candidate, bar, *args: bar)
    result = feed._fetch_one(provider, row, start, end)
    assert result["state"] == "WRITTEN"
    bar = result["bar"]
    assert bar["confirmation_history_complete"] is (prior_count == 20)
    assert bar["v9_1m_volume_ratio"] == (0. if prior_count == 20 else None)
    assert feed._g_volume_snapshot_valid(bar, end, config)
    confirmed = runtime.confirmation_metrics(row, bar)
    assert confirmed["minimum_confirmation_volume_ratio"] == 0.
    assert confirmed["confirmed"] is (prior_count == 20)


def test_0920_scanner_keeps_receipt_metadata_and_fails_closed_without_baseline(runtime, monkeypatch):
    import fno_v13_v10_g_3_opening_inputs as opening
    stamp = pd.Timestamp(config.slot_datetime(DAY, "09:20"))
    future = pd.DataFrame([dict(ts=stamp, timestamp=stamp, tradingsymbol="TEST26OCTFUT",
        expiry="2026-10-27", oi=100500., fetch_timestamp=f"{DAY}T09:20:04+05:30")])
    cash = pd.DataFrame([dict(ts=stamp, open=100., high=101., low=99.9, close=100.5, volume=3000.,
        source_1m_count=5, ema9=100., ema20=99., ema50=98., volume_ratio=3., traded_value=301500.,
        oi=100500., prev_oi=1., oi_change_pct=999., price_change_pct=999.)])
    universe = pd.DataFrame([dict(equity_symbol="TEST", futures_tradingsymbol="TEST26OCTFUT",
        expiry="2026-10-27", equity_instrument_token=1, futures_instrument_token=2)])
    monkeypatch.setattr(runtime.common, "now_ist", lambda: config.slot_datetime(DAY,"09:20") + timedelta(seconds=10))
    monkeypatch.setattr(opening, "load_futures_frame", lambda path: future)
    monkeypatch.setattr(runtime.backtest, "load_five_minute", lambda *a: pytest.fail("Receipt-dropping legacy loader used"))
    monkeypatch.setattr(runtime.hybrid, "load_equity_five_minute", lambda *a, **kw: cash)
    monkeypatch.setattr(runtime.hybrid, "join_equity_price_with_futures_oi", lambda *a: cash.copy())
    monkeypatch.setattr(opening, "read_first_minute", lambda *a: (None, None))
    quoted = dict(symbol="TEST", contract="TEST26OCTFUT", oi=100000., source="KITE_FULL_QUOTE",
        baseline_kind="PROVISIONAL_FULL_QUOTE", status="QUOTE_NEAR_0916_NOT_CANDLE",
        exact_first_minute_candle=False, exchange_timestamp=f"{DAY}T09:16:04+05:30",
        received_at=f"{DAY}T09:16:05+05:30")
    monkeypatch.setattr(opening, "read_quote", lambda *a: (quoted, "fixture-evidence-sha"))
    snapshot = runtime.scan_five_minute_slot(universe, DAY, "09:20")
    assert snapshot["state"] == "SUCCESS"
    assert snapshot["long_candidates"] == 1 and snapshot["short_candidates"] == 0
    candidate = snapshot["candidates"][0]
    assert candidate["oi_change_pct"] == pytest.approx(.5)
    assert candidate["price_change_pct"] == pytest.approx(.5)
    assert candidate["prev_oi"] == 100000.
    assert candidate["oi_change_source"] == "PROVISIONAL_FULL_QUOTE"
    assert "09:20:10" in candidate["feature_available_at_ist"]
    assert snapshot["feature_evaluations"][0]["oi_change_source"] == "PROVISIONAL_FULL_QUOTE"
    other = universe.iloc[0].to_dict()
    other.update(equity_symbol="MISSING", futures_tradingsymbol="MISSING26OCTFUT")
    mixed = runtime.scan_five_minute_slot(pd.concat([universe, pd.DataFrame([other])]), DAY, "09:20")
    assert mixed["state"] == "SUCCESS" and mixed["long_candidates"] == 1
    assert len(mixed["opening_0920_exclusions"]) == 1
    quoted["received_at"] = f"{DAY}T09:20:02+05:30"
    missing = runtime.scan_five_minute_slot(universe, DAY, "09:20")
    assert missing["state"] == "PARTIAL" and missing["candidates"] == []
    assert missing["opening_0920_exclusions"][0]["reason"] == "missing_or_late_0916_opening_oi_evidence"


@pytest.mark.parametrize("clock,side,ratio,minimum,expected", [
    ("09:50", "LONG", .6, .6, True),
    ("09:50", "LONG", .599999, .6, False),
    ("09:30", "LONG", .6, 1.1, False),
    ("09:50", "SHORT", .6, 1.2, False),
    ("09:50", "SHORT", 1.2, 1.2, True),
])
def test_confirmation_volume_exception_is_only_0950_long(runtime, clock, side, ratio, minimum, expected):
    row = candidate(side=side, ratio=ratio, clock=clock)
    bar = dict(timestamp=row["confirmation_timestamp"], open=100., high=102., low=98.,
               close=101.5 if side == "LONG" else 98.5, volume=100.*ratio,
               v9_1m_volume_ratio=ratio, v9_1m_feature_ts=row["v9_1m_feature_ts"])
    result = runtime.confirmation_metrics(row, bar)
    assert result["confirmed"] is expected
    assert result["minimum_confirmation_volume_ratio"] == minimum


def test_promoted_slot_ranks_liquidity_after_confirmation_and_keeps_one_entry(runtime):
    rows = [candidate(clock="09:50", ratio=.6) for _ in range(3)]
    for row, symbol, liquidity, ratio in zip(rows, ("LOW", "WINNER", "REJECT"), (1e6, 2e6, 1e9), (.6, .6, .59999)):
        row.update(tradingsymbol=symbol, traded_value=liquidity, v9_1m_volume_ratio=ratio)
    selected = runtime.select_entry_signals(rows, DAY, "09:50", capital_rs=100000., leverage=5.)
    assert [row["tradingsymbol"] for row in selected] == ["WINNER"]
    assert selected[0]["target_pct"] == .75


def test_g3_capacity_book_uses_g3_fingerprint(runtime):
    signal = runtime.select_entry_signals([candidate()], DAY, "09:30", capital_rs=100000., leverage=5.)[0]
    states = []
    for i in range(11):
        state = runtime.create_order_state({**signal, "signal_id": str(i)}, "PAPER")
        states.append(runtime._advance_g_paper_state(state, 102., config.slot_datetime(DAY, "09:32")))
    assert sum(s["status"] == "OPEN" for s in states) == 10
    assert states[-1]["status"] == "BLOCKED_PORTFOLIO"


def test_profile_gate_cannot_be_overridden_by_stale_arm(runtime, monkeypatch):
    monkeypatch.setenv(config.LIVE_ACK_ENV, config.LIVE_ACK)
    common.atomic_write_json(runtime.LIVE_ARM_PATH, {"enabled": True, "session_date": DAY.isoformat(),
        "strategy_fingerprint": config.strategy_fingerprint()})
    assert runtime._live_arm_state(DAY) == (False, "G3_LIVE_ACTIVATION_NOT_AUTHORIZED")
    assert guard.entry_authority("V13_V10_G") == (False, "INACTIVE_STRATEGY_ENTRY_BLOCKED")
    monkeypatch.setattr(guard, "deployment_state", lambda: {"profile": "V13_V10_G_3",
        "deployment_stage": "LIVE_APPROVED", "live_activation_authorized": True})
    assert runtime._live_arm_state(DAY) == (False, "LIVE_ARM_EXECUTION_POLICY_MISMATCH")
    common.atomic_write_json(runtime.LIVE_ARM_PATH, {"enabled": True, "session_date": DAY.isoformat(),
        "strategy_fingerprint": config.strategy_fingerprint(),
        "live_execution_policy_fingerprint": config.live_execution_policy.policy_fingerprint()})
    assert runtime._live_arm_state(DAY) == (True, "LIVE_ARMED")
    common.atomic_write_json(common.FNO_ROOT / "v6_live/kill_switch.json", {"enabled": True})
    assert runtime._live_arm_state(DAY) == (False, "KILL_SWITCH_ENABLED")


def test_shared_owner_lock_rechecks_authority_before_fake_submission(runtime, monkeypatch):
    with pytest.raises(RuntimeError, match="ACTIVATION_NOT_AUTHORIZED"):
        with guard.entry_owner_lock(common.FNO_ROOT / "control", config.STRATEGY_PROFILE):
            pytest.fail("Disarmed entry got ownership")
    monkeypatch.setattr(guard, "deployment_state", lambda: {"profile": "V13_V10_G"})
    with guard.entry_owner_lock(common.FNO_ROOT / "control", "V13_V10_G"):
        pass


def test_coordinator_g3_names_pins_and_no_autoarm(runtime, monkeypatch):
    coordinator = isolated("g3_fixture_coordinator", "fno_v6_live_kite_session.py")
    assert coordinator.SESSION_ID == "fno_v13_v10_g_3_live_kite_qty1"
    assert "fno_v13_v10_g_3_live.py" in coordinator.worker_command(DAY, "LONG")[2]
    assert "--live-quantity" not in coordinator.worker_command(DAY, "LONG")
    assert coordinator.worker_environment()["FNO_V6_STRATEGY_PROFILE"] == config.STRATEGY_PROFILE
    activation_epoch = "2026-10-12T09:15:00.123456+05:30"
    assert (
        coordinator.worker_environment(activation_epoch)[
            coordinator.G3_LIVE_ACTIVATION_EPOCH_ENV
        ]
        == activation_epoch
    )
    assert coordinator.worker_session_id("SHORT") == "fno_v13_v10_g_3_live_kite_qty1_short"
    with pytest.raises(ValueError, match="auto-arm is disabled"):
        coordinator._auto_arm_session(DAY)
    assert not (runtime.CONTROL_ROOT / "live_arm.json").exists()
    monkeypatch.setattr(config, "attest_selected_backtest", lambda: {})
    monkeypatch.setattr(coordinator.subprocess, "Popen", lambda *a, **k: pytest.fail("Preflight spawned a worker"))
    assert coordinator.main(["--readiness-only", "--session-date", DAY.isoformat()]) == 0
    assert json.loads(coordinator.STATUS_PATH.read_text())["execution_enabled"] is False
    for path in (coordinator.STATUS_PATH, coordinator.HEARTBEAT_PATH):
        config.live_execution_policy.validate_execution_metadata(json.loads(path.read_text()))


def test_importing_g3_wrappers_is_inert(monkeypatch):
    monkeypatch.setenv("FNO_V6_STRATEGY_PROFILE", "SENTINEL")
    for name in ("live", "paper", "live_kite_session", "options_paper"):
        isolated("inert_g3_" + name, "fno_v13_v10_g_3_" + name + ".py")
    import os
    assert os.environ["FNO_V6_STRATEGY_PROFILE"] == "SENTINEL"


def test_g3_options_source_and_output_namespace(runtime, monkeypatch):
    monkeypatch.setenv("FNO_OPTIONS_EQUITY_PROFILE", config.STRATEGY_PROFILE)
    options = isolated("g3_fixture_options", "fno_v13_v10_g_options_paper.py")
    assert options.EQUITY_ORDER_ROOT == runtime.LIVE_ROOT / "orders"
    assert options.OPTIONS_ROOT.name == "v13_v10_g_3_options_paper"
    assert all("g_3_options" in name for name in options.ROLE_SESSIONS.values())
    assert options.STOP_PCT > 0 and options.TARGET_PCT > 0


def test_feature_ledger_g3_threshold_and_strict_branch(runtime):
    from ai_platform.observability.feature_ledger import evaluate_v13_v10_g_base_row
    row = candidate()
    row.update(confirmation_open=100., confirmation_high=102., confirmation_low=99.5,
               confirmation_close=101.5, confirmation_volume=1000., session_date=DAY.isoformat())
    result = evaluate_v13_v10_g_base_row(row, strategy_config=config)
    assert result["minimum_confirmation_volume_ratio"] == 1.1
    assert result["gate_confirmation_volume"] is True
    assert result["relaxed_0925_long_base_branch"] is False
    assert result["strategy_profile"] == config.STRATEGY_PROFILE
    assert result["setup_filter_pass"] is True


def test_feature_ledger_infers_v2_and_explains_promoted_threshold(runtime):
    from ai_platform.observability.feature_ledger import evaluate_v13_v10_g_base_row
    row = candidate(clock="09:50", ratio=.6)
    row.update(confirmation_open=100., confirmation_high=102., confirmation_low=99.5,
               confirmation_close=101.5, confirmation_volume=60., session_date=DAY.isoformat())
    result = evaluate_v13_v10_g_base_row(row, strategy_version=config.STRATEGY_VERSION)
    assert result["strategy_profile"] == config.STRATEGY_PROFILE
    assert result["minimum_confirmation_volume_ratio"] == .6
    assert result["gate_confirmation_volume"] is True
    assert result["setup_filter_pass"] is True


def test_preflight_reports_promoted_configuration_without_runtime_mutations():
    from fno_v13_v10_g_3_preflight import inspect_deployment
    report = inspect_deployment(verify_frozen=False)
    assert report["ready"] is True
    assert report["strategy_version"] == "V13-V10-G-3-PRODUCTION-V11"
    assert report["active_setup_count"] == 61
    assert len(report["promoted_afternoon_fifteen"]) == 15
    assert {s["setup_id"] for s in report["promoted_afternoon_fifteen"]} == config.contract.afternoon_setups.SETUP_IDS
    assert len(report["promoted_midday_twelve"]) == 12
    assert {s["setup_id"] for s in report["promoted_midday_twelve"]} == config.contract.midday_setups.SETUP_IDS
    assert report["promoted_0950_long_setup"]["target_pct"] == .75
    assert report["promoted_0950_long_setup"]["minimum_confirmation_volume_ratio"] == .6
    assert report["workers_started"] is report["broker_requests"] is report["arm_files_written"] is False


def test_g3_volume_snapshot_requires_20_causal_prior_bars(runtime):
    import fno_equity_fetch_1min as feed
    end = config.slot_datetime(DAY, "09:31")
    history = pd.DataFrame({"timestamp": pd.date_range(end-timedelta(minutes=20), periods=20, freq="min"), "volume": [100.] * 20})
    bar = config.annotate_confirmation_volume({"timestamp": end.isoformat(), "volume": 110.}, history)
    bar["confirmation_history_complete"] = True
    assert feed._g_volume_snapshot_valid(bar, end, config)
    bar["confirmation_prior_volume_count"] = 19
    assert not feed._g_volume_snapshot_valid(bar, end, config)


def test_launcher_preflight_is_disarmed_and_daily_is_production():
    bat = (ROOT / "bat/run_fno_v13_v10_g_3_live_kite_qty1.bat").read_text()
    assert "--readiness-only" in bat and "--auto-arm" not in bat and "LIVE_ACK=" not in bat
    activate = (ROOT / "bat/activate_fno_v13_v10_g_3_live_kite_qty1.bat").read_text()
    assert "--activate-live" in activate and "--auto-arm" not in activate and "LIVE_ACK=" not in activate
    daily = (ROOT / "bat/run_backtesting_result_v13_v10_g_3_1620.bat").read_text()
    assert "backtesting_result_v13_v10_g_3_daily.py" in daily
    assert 'set "FNO_ACTIVE_STRATEGY_PROFILE=V13_V10_G_3"' in daily
    assert daily.index('if not "%EXIT_CODE%"=="0" goto FINALIZE') < daily.index('tools\\v13_execution_research.py')
    assert daily.index('tools\\v13_execution_research.py') < daily.index('tools\\v13_strategy_research.py')
    assert 'set "EXIT_CODE=%EXECUTION_ANALYSIS_EXIT%"' in daily
    assert 'set "EXIT_CODE=%STRATEGY_ANALYSIS_EXIT%"' in daily
    assert 'shadow_automation' not in daily


def test_pipeline_covers_last_exact_confirmation_and_publication_grace(runtime):
    assert config.SIGNAL_TO_CONFIRMATION["13:30"] == "13:31"
    assert config.PIPELINE_DEADLINE == "13:33"
    assert runtime.PIPELINE_DEADLINE.isoformat() == "13:33:00"
    deadline = config.slot_datetime(DAY, config.PIPELINE_DEADLINE)
    assert deadline > config.confirmation_deadline(DAY, "13:31")
    assert config.SESSION_END > "15:15"
