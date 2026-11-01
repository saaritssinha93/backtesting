"""Offline entry-boundary and G7 NOT_SUPPORTED regression checks."""
from __future__ import annotations

import importlib.util
import json
from datetime import date, datetime
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def runtime(monkeypatch, tmp_path):
    import fno_oi_common as common
    monkeypatch.setenv("FNO_LIVE_GENERATION", "v6")
    monkeypatch.setenv("FNO_V6_STRATEGY_PROFILE", "V13_V10_G_3")
    monkeypatch.setattr(common, "FNO_ROOT", tmp_path / "fno")
    monkeypatch.setattr(common, "LATEST_DIR", tmp_path / "latest")
    name = "slot70_test_runtime"
    spec = importlib.util.spec_from_file_location(name, ROOT / "fno_v5_live.py")
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


def test_disabled_slot_blocked_at_every_new_entry_boundary(runtime):
    disabled = {"setup_id": "0931_LONG"}
    assert not runtime._slot_entry_allowed(disabled)
    for mode in ("LIVE", "PAPER"):
        with pytest.raises(ValueError, match="SLOT_WIN_RATE_POLICY_DISABLED"):
            runtime.create_order_state(disabled, mode)
    with pytest.raises(ValueError, match="SLOT_WIN_RATE_POLICY_DISABLED"):
        runtime._validate_signal(disabled, date(2026, 10, 29))
    class NoBroker:
        def __getattr__(self, name):
            pytest.fail("Disabled entry touched broker: " + name)
    with pytest.raises(ValueError, match="SLOT_WIN_RATE_POLICY_DISABLED"):
        runtime._place_live_entry_order(disabled, NoBroker(),
            datetime.fromisoformat("2026-10-29T09:32:00+05:30"),
            transaction_type="BUY", tag="test", order_type="SL-M", reason="test")


def test_disabled_pending_paper_cancelled_without_quote_but_open_is_managed(runtime):
    now = datetime.fromisoformat("2026-10-29T09:33:00+05:30")
    pending = dict(setup_id="0931_LONG", status="PENDING_ENTRY")
    result = runtime.advance_paper_order(pending, None, now)
    assert result["status"] == "CANCELLED"
    assert result["status_reason"] == "SLOT_WIN_RATE_POLICY_DISABLED"
    position = dict(setup_id="0931_LONG", status="OPEN", mode="PAPER", side="LONG",
        session_date="2026-10-29", entry_at_ist="2026-10-29T09:32:00+05:30",
        entry_price=100., stop_price=98.75, target_price=102., stop_pct=1.25,
        target_pct=2., tick_size=.05, quantity=10, capital_rs=100000.,
        round_trip_cost_bps=5.)
    closed = runtime.advance_paper_order(position, 98.5, now)
    assert closed["status"] == "CLOSED"
    assert closed["exit_reason"] == "STOP"
    assert closed["net_pnl_rs"] < 0


def test_slot_guard_retains_strategy_specific_side(runtime):
    assert runtime._slot_entry_allowed({"setup_id": "0931_SHORT"})
    assert not runtime._slot_entry_allowed({"setup_id": "0931_LONG"})
    assert runtime._slot_entry_allowed({"confirmation_end": "09:31", "side": "SHORT"})
    assert not runtime._slot_entry_allowed({"setup_id": "unknown"})
    with pytest.raises(ValueError, match="SLOT_WIN_RATE_POLICY_DISABLED"):
        runtime.scan_five_minute_slot(pd.DataFrame(), date(2026, 10, 29), "11:45")
    with pytest.raises(ValueError, match="SLOT_WIN_RATE_POLICY_DISABLED"):
        runtime.process_confirmation_slot({}, date(2026, 10, 29), "12:25", None, None)


def test_disabled_live_pending_reconciles_then_cancels_existing_broker_order(runtime, monkeypatch):
    now = datetime.fromisoformat("2026-10-29T09:33:00+05:30")
    state = dict(status="PENDING_ENTRY", setup_id="0931_LONG", quantity=10,
        side="LONG", signal_id="old", tradingsymbol="ABC", entry_order_id="E1",
        entry_activation_deadline_ist="2026-10-29T09:41:00+05:30")
    monkeypatch.setattr(runtime, "_live_arm_state", lambda day: (True, "ARMED"))
    monkeypatch.setattr(runtime, "_kill_switch_enabled", lambda: False)
    responses = iter([dict(status="TRIGGER PENDING", filled_quantity=0),
                      dict(status="CANCELLED", filled_quantity=0)])
    monkeypatch.setattr(runtime, "_broker_order", lambda client, order_id: next(responses))
    cancelled = []
    monkeypatch.setattr(runtime, "_broker_cancel", lambda client, order_id: cancelled.append(order_id))
    result = runtime.advance_live_order(state, object(), now)
    assert cancelled == ["E1"]
    assert result["status"] == "CANCELLED"
    assert result["status_reason"] == "SLOT_WIN_RATE_POLICY_DISABLED"


def test_disabled_live_pending_does_not_discard_a_racing_broker_fill(runtime, monkeypatch):
    now = datetime.fromisoformat("2026-10-29T09:33:00+05:30")
    state = dict(status="PENDING_ENTRY", setup_id="0931_LONG", quantity=10,
        side="LONG", signal_id="old", tradingsymbol="ABC", entry_order_id="E1",
        entry_activation_deadline_ist="2026-10-29T09:41:00+05:30")
    monkeypatch.setattr(runtime, "_live_arm_state", lambda day: (True, "ARMED"))
    monkeypatch.setattr(runtime, "_kill_switch_enabled", lambda: False)
    monkeypatch.setattr(runtime, "_broker_order", lambda client, order_id:
        dict(status="COMPLETE", filled_quantity=10, average_price=100.))
    observed = []
    def reconcile_fill(s, order, current):
        observed.append(order["filled_quantity"])
        s["status"] = "OPEN"
        return False  # Pause after verifying the protective fill path is called.
    monkeypatch.setattr(runtime, "_apply_live_entry_fill", reconcile_fill)
    result = runtime.advance_live_order(state, object(), now)
    assert observed == [10]
    assert result["status"] == "OPEN"


def prior_paper_position():
    from research import archived_g3_production_v13 as archived
    day = date(2026, 10, 29)
    setup = archived.setup_for("09:30", "LONG")
    return dict(strategy_version=archived.STRATEGY_VERSION,
        strategy_fingerprint=archived.strategy_fingerprint(),
        signal_id=archived.canonical_signal_id(archived.STRATEGY_VERSION, day,
            setup.signal_end, setup.confirmation_end, "LONG", "ABC"),
        session_date=str(day), setup_id=setup.setup_id, signal_end=setup.signal_end,
        confirmation_end=setup.confirmation_end, side="LONG", tradingsymbol="ABC",
        mode="PAPER", status="OPEN", quantity=5000, capital_rs=100000., leverage=5.,
        target_exposure_rs=500000., trigger_price=100., entry_price=100.,
        entry_at_ist="2026-10-29T09:32:00+05:30", stop_price=98.75,
        target_price=100*(1+setup.target_pct/100), stop_pct=setup.stop_pct,
        target_pct=setup.target_pct, tick_size=.05, exchange="NSE", lot_size=1,
        round_trip_cost_bps=5.)


def test_old_v14_open_position_keeps_protection_after_all_slot_cutover():
    from g3_legacy_state_recovery import validate_protective_state
    from research import archived_g3_production_v14 as archived
    day = date(2026, 10, 29)
    setup = archived.setup_for("09:40", "LONG")
    assert setup is not None  # V14 admitted this pair; V15 excludes it.
    old = prior_paper_position()
    old.update(strategy_version=archived.STRATEGY_VERSION,
        strategy_fingerprint=archived.strategy_fingerprint(), setup_id=setup.setup_id,
        signal_end=setup.signal_end, confirmation_end=setup.confirmation_end,
        signal_id=archived.canonical_signal_id(archived.STRATEGY_VERSION, day,
            setup.signal_end, setup.confirmation_end, setup.side, "ABC"),
        stop_pct=setup.stop_pct, target_pct=setup.target_pct,
        target_price=100*(1+setup.target_pct/100))
    assert validate_protective_state(old, day, "PAPER") is old
    old["status"] = "PENDING_ENTRY"
    with pytest.raises(ValueError, match="cannot create"):
        validate_protective_state(old, day, "PAPER")


def test_prior_profile_position_recovered_without_authoritative_new_signals(runtime, monkeypatch):
    old = prior_paper_position()
    day = date(2026, 10, 29)
    path = runtime._order_path(day, "PAPER", old["signal_id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(old), encoding="utf-8")
    recovered, issues = runtime._g3_archived_recovery_states(day, "PAPER", "LONG")
    assert len(recovered) == 1 and not issues
    assert not runtime._slot_entry_allowed(recovered[0])
    monkeypatch.setattr(runtime, "load_signals", lambda *_: [])
    monkeypatch.setattr(runtime, "_blocking_pipeline_issue", lambda *_: {"state": "BLOCKED"})
    monkeypatch.setattr(runtime.common, "now_ist", lambda:
        datetime.fromisoformat("2026-10-29T09:33:00+05:30"))
    monkeypatch.setattr(runtime, "_render_worker_report", lambda *_: "offline recovery test")
    monkeypatch.setattr(runtime, "_publish", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "_heartbeat", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "_write_order_state", lambda state:
        path.write_text(json.dumps(state), encoding="utf-8"))
    class QuotesOnly:
        last_operation = "quote"
        last_operation_app = "fixture"
        last_operation_failures = []
        credential_reload_count = 0
        def __init__(self, *_):
            pass
        def quote_prices(self, symbols):
            return {symbol: 98. for symbol in symbols}, "fixture", []
    monkeypatch.setattr(runtime, "KitePool", QuotesOnly)
    args = SimpleNamespace(execution_mode="PAPER", live_quantity=None,
        max_apps=1, timeout_sec=.1, once=True)
    # Protection continues while the failed upstream remains visibly degraded.
    assert runtime.run_worker(args, day, "LONG") == 2
    closed = json.loads(path.read_text(encoding="utf-8"))
    assert closed["status"] == "CLOSED" and closed["exit_reason"] == "STOP"
    assert closed["strategy_fingerprint"] == old["strategy_fingerprint"]
    assert closed["strategy_version"] == old["strategy_version"]


def test_prior_profile_cannot_be_adopted_for_new_entry(runtime):
    from g3_legacy_state_recovery import validate_protective_state
    old = prior_paper_position() | {"status": "PENDING_ENTRY"}
    with pytest.raises(ValueError, match="cannot create"):
        validate_protective_state(old, date(2026, 10, 29), "PAPER")
    with pytest.raises(ValueError, match="SLOT_WIN_RATE_POLICY_DISABLED"):
        runtime.create_order_state(old, "PAPER")
    with pytest.raises(ValueError, match="identity"):
        validate_protective_state(prior_paper_position() | {"strategy_fingerprint": "tampered"},
            date(2026, 10, 29), "PAPER")
    # Even a retained slot cannot turn an archived identity into a fresh fill.
    retained = dict(old, setup_id="0931_SHORT", side="SHORT")
    with pytest.raises(ValueError, match="ARCHIVED_STRATEGY"):
        runtime.create_order_state(retained, "PAPER")
    cancelled = runtime.advance_paper_order(retained, 98.,
        datetime.fromisoformat("2026-10-29T09:33:00+05:30"))
    assert cancelled["status"] == "CANCELLED"
    assert cancelled["status_reason"] == "ARCHIVED_STRATEGY_CANNOT_CREATE_NEW_ENTRY"


def test_unreviewed_0926_keeps_prior_production_filters_and_ranking(runtime):
    from research import archived_g3_production_v13 as previous
    day = date(2026, 10, 29)
    stamp = runtime.config.slot_datetime(day, "09:25")
    confirmation = runtime.config.slot_datetime(day, "09:26")
    rows = []
    for symbol, wick, volume in (("FIRST", .19, 4.), ("SECOND", .20, 3.), ("NEXT", .30, 2.)):
        rows.append(dict(tradingsymbol=symbol, instrument_token=1,
            futures_tradingsymbol=symbol + "26OCTFUT", futures_instrument_token=2,
            exchange="NSE", lot_size=1, tick_size=.05, data_contract="test", side="SHORT",
            signal_timestamp=stamp.isoformat(), signal_ts=stamp.isoformat(), signal_end="09:25",
            confirmation_timestamp=confirmation.isoformat(), v9_1m_feature_ts=confirmation.isoformat(),
            signal_close=100., ema9=99., ema20=100., ema50=101., price_change_pct=-.8,
            oi=1005., prev_oi=1000., oi_change_pct=.5, volume_ratio=volume,
            traded_value=2_000_000., confirmed=True, body_ratio=.6, wick_ratio=wick,
            trigger=98., v9_1m_volume_ratio=1.2, nifty_first_bar_return_pct=-.1))
    selected = runtime.select_entry_signals(rows, day, "09:25")
    previous_setup = previous.setup_for("09:25", "SHORT", session_date=day)
    expected = previous.rank_candidates(rows, previous_setup, session_date=day)
    assert {row["tradingsymbol"] for row in selected} == {
        row["tradingsymbol"] for row in expected}
    assert {row["tradingsymbol"] for row in selected} == {"FIRST", "SECOND"}
    assert all(row["setup_id"] == "0926_SHORT" for row in selected)


def test_g3_options_new_entries_filtered_old_open_position_remains_manageable(monkeypatch):
    monkeypatch.setenv("FNO_OPTIONS_EQUITY_PROFILE", "V13_V10_G_3")
    spec = importlib.util.spec_from_file_location("slot70_options", ROOT / "fno_v13_v10_g_options_paper.py")
    options = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "slot70_options", options)
    spec.loader.exec_module(options)
    old = prior_paper_position()
    now = datetime.fromisoformat("2026-10-29T09:33:00+05:30")
    with pytest.raises(ValueError, match="SLOT_WIN_RATE_POLICY_DISABLED"):
        options.create_state({"setup_id": "0931_LONG"}, {}, now, "test")
    state = dict(schema_version=options.SCHEMA_VERSION, strategy_version=options.OPTIONS_STRATEGY_VERSION,
        strategy_fingerprint=old["strategy_fingerprint"], equity_strategy_version=old["strategy_version"],
        mode="PAPER", session_date="2026-10-29", status="OPEN", side="LONG", setup_id="0931_LONG",
        signal_id="existing", lots=1, quantity=10, lot_size=10, option_type="CE", position_side="BUY",
        tick_size=.05, entry_price=100., entry_costs_rs=0., stop_price=90., target_price=130.)
    monkeypatch.setattr(options, "validate_quote", lambda *_: dict(last=80., bid=79., ask=81., spread_pct=1., volume=1000.))
    options.advance_quote_state(state, {}, now, SimpleNamespace(max_quote_age_sec=10., max_spread_pct=5.))
    assert state["status"] == "CLOSED" and state["exit_reason"] == "STOP"
    pending = dict(state, status="WAITING_ENTRY_QUOTE")
    assert options.expire_without_quote(pending, now, SimpleNamespace())
    assert pending["status"] == "SKIPPED"
    assert pending["status_reason"] == "SLOT_WIN_RATE_POLICY_DISABLED"


def test_g7_named_configuration_and_entrypoints_are_explicitly_unsupported(monkeypatch, capsys):
    from fno_v13_v10_g_7_v4_lock_live_config import readiness, configuration
    from fno_v13_v10_g_7_v4_lock_live import main as live
    from fno_v13_v10_g_7_v4_lock_paper import main as paper
    from strategy_slot_selection import enabled_setup_ids
    from fno_live_entry_guard import entry_authority
    from fno_live_profile import config_for_generation
    monkeypatch.setenv("FNO_V6_STRATEGY_PROFILE", "V13_V10_G_7_V4_LOCK")
    assert config_for_generation("v6").STRATEGY_PROFILE == "V13_V10_G_7_V4_LOCK"
    for mode, entrypoint in (("LIVE", live), ("PAPER", paper)):
        status = readiness(mode)
        assert status["status"] == "NOT_SUPPORTED"
        assert set(status["selected_setup_ids"]) == enabled_setup_ids("G7_V4_LOCK2")
        assert status["broker_calls"] == 0 and not status["workers_started"]
        assert entrypoint(["--readiness-only"]) == 2
        assert json.loads(capsys.readouterr().out)["status"] == "NOT_SUPPORTED"
    assert entry_authority("V13_V10_G_7_V4_LOCK") == (
        False, "G7_V4_LOCK_EXECUTION_ADAPTER_NOT_SUPPORTED")
    assert not configuration()["execution_authority"]


def test_direct_shared_transport_cannot_bypass_g7_block(monkeypatch):
    monkeypatch.setenv("FNO_LIVE_GENERATION", "v6")
    monkeypatch.setenv("FNO_V6_STRATEGY_PROFILE", "V13_V10_G_7_V4_LOCK")
    spec = importlib.util.spec_from_file_location("blocked_g7_transport", ROOT / "fno_v5_live.py")
    module = importlib.util.module_from_spec(spec)
    with pytest.raises(RuntimeError, match="NOT_SUPPORTED"):
        spec.loader.exec_module(module)


def test_g7_loss_lock_is_causal_resets_daily_and_does_not_mutate_positions():
    from g7_v4_lock_runtime_rules import admission
    from strategy_slot_selection import enabled_setup_ids
    setup = sorted(enabled_setup_ids("G7_V4_LOCK2"))[0]
    rows = [dict(signal_id=str(i), session_date="2026-10-29", status="CLOSED",
        net_pnl_rs=-1., exit_at_ist=f"2026-10-29T10:0{i}:00+05:30") for i in (1, 2)]
    at = lambda t: admission(rows, setup_id=setup, symbol="ABC", prospective_entry_ts=t)
    assert at("2026-10-29T10:02:00+05:30")["allowed"]  # Exact tie retained.
    assert at("2026-10-29T10:02:01+05:30")["reason"] == "DAILY_NET_LOSS_COUNT_LIMIT"
    assert at("2026-10-30T10:03:00+05:30")["allowed"]
    rows[1]["net_pnl_rs"] = float("nan")
    assert at("2026-10-29T10:01:30+05:30")["allowed"]  # No future P&L read.
    with pytest.raises(ValueError, match="unavailable"):
        at("2026-10-29T10:03:00+05:30")
    opened = dict(signal_id="open", session_date="2026-10-29", status="OPEN",
        entry_at_ist="2026-10-29T09:32:00+05:30", tradingsymbol="ABC")
    assert admission([opened], setup_id=setup, symbol="ABC",
        prospective_entry_ts="2026-10-29T10:03:00+05:30")["reason"] == "ONE_ACTIVE_POSITION_PER_SYMBOL"
    assert opened["status"] == "OPEN"
    assert not admission([], setup_id="0931_LONG", symbol="ABC",
        prospective_entry_ts="2026-10-29T10:03:00+05:30")["allowed"]


@pytest.mark.parametrize("side,close,initial,protected", [
    ("LONG", 100.6, 98.8, 100.1), ("SHORT", 99.4, 101.2, 99.9)])
def test_g7_completed_close_protection_boundaries_and_never_loosen(side, close, initial, protected):
    from g7_v4_lock_runtime_rules import observe_completed_close, protected_stop_for_next_bar
    position = dict(status="OPEN", side=side, entry_price=100., stop_price=initial,
        entry_at_ist="2026-10-29T09:31:30+05:30", entry_bar_end="2026-10-29T09:32:00+05:30")
    same = observe_completed_close(position, close=close, bar_end="2026-10-29T09:32:00+05:30",
        observed_at="2026-10-29T09:32:01+05:30")
    assert "protection_armed_at" not in same
    armed = observe_completed_close(position, close=close, bar_end="2026-10-29T09:33:00+05:30",
        observed_at="2026-10-29T09:33:01+05:30")
    assert "protection_armed_at" not in position
    assert protected_stop_for_next_bar(armed, bar_start="2026-10-29T09:32:00+05:30") == initial
    assert protected_stop_for_next_bar(armed, bar_start="2026-10-29T09:33:00+05:30") == pytest.approx(protected)
    tighter = 100.3 if side == "LONG" else 99.7
    armed["stop_price"] = tighter
    assert protected_stop_for_next_bar(armed, bar_start="2026-10-29T09:34:00+05:30") == tighter
    with pytest.raises(ValueError, match="Uncompleted"):
        observe_completed_close(position, close=close, bar_end="2026-10-29T09:35:00+05:30",
            observed_at="2026-10-29T09:34:00+05:30")


def test_g7_nifty_permission_requires_exact_candle_and_ignores_future():
    from g7_v4_lock_runtime_rules import nifty_permission
    frame = pd.DataFrame([dict(ts="2026-10-29T09:20:00+05:30", open=100., close=99.),
                          dict(ts="2026-10-29T09:25:00+05:30", open=99., close=98.),
                          dict(ts="2026-10-29T09:30:00+05:30", open=0., close=0.)])
    assert not nifty_permission(frame, "2026-10-29T09:26:00+05:30", "LONG")["allowed"]
    assert nifty_permission(frame, "2026-10-29T09:26:00+05:30", "SHORT")["allowed"]
    with pytest.raises(ValueError, match="Missing or invalid"):
        nifty_permission(frame.iloc[[0]], "2026-10-29T09:26:00+05:30", "LONG")


def test_g7_low_efficiency_prefix_invariance_and_invalid_input():
    from g7_v4_lock_runtime_rules import low_efficiency_veto
    ts = pd.date_range("2026-10-29T09:16:00+05:30", periods=50, freq="min")
    values = [100 + i * .01 for i in range(50)]
    bars = pd.DataFrame(dict(ts=ts, open=values, high=[v + .02 for v in values],
        low=[v - .02 for v in values], close=[v + .01 for v in values], volume=100))
    last = bars.iloc[35]
    row = dict(sid=1, tradingsymbol="TEST", side="LONG", confirmation_ts=last.ts,
        trigger=last.high, **{"confirmation_" + k: last[k] for k in ("open", "high", "low", "close")})
    expected = low_efficiency_veto(bars.iloc[:36], row, historical_inventory_complete=True)
    bars.loc[36:, "close"] = -999
    actual = low_efficiency_veto(bars, row, historical_inventory_complete=True)
    assert actual["veto"] == expected["veto"]
    assert actual["features"]["efficiency10"] == expected["features"]["efficiency10"]
    with pytest.raises(ValueError, match="inventory"):
        low_efficiency_veto(bars, row, historical_inventory_complete=False)
    with pytest.raises(ValueError, match="Missing or duplicate"):
        low_efficiency_veto(bars.drop(index=4), row, historical_inventory_complete=True)
