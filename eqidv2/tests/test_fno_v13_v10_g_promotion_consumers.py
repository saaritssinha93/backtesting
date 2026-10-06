"""Dated G contracts in the coordinator and diagnostic feature ledger."""
from __future__ import annotations

import json
from datetime import date

import pandas as pd
import pytest

import fno_v13_v10_g_live_config as config
import fno_v6_live_kite_session as coordinator
from ai_platform.observability.feature_ledger import evaluate_v13_v10_g_base_row


def feature(day="2026-10-06", clock="09:25", **overrides):
    signal = pd.Timestamp(f"{day} {clock}", tz="Asia/Kolkata")
    row = dict(
        day=day, signal_ts=signal, confirmation_ts=signal + pd.Timedelta(minutes=1),
        v9_1m_feature_ts=signal + pd.Timedelta(minutes=1),
        signal_end=clock, confirmation_end=(signal + pd.Timedelta(minutes=1)).strftime("%H:%M"),
        tradingsymbol="TEST", futures_tradingsymbol="TEST26OCTFUT",
        signal_close=100., ema9=99., ema20=100., ema50=101.,
        price_change_pct=.3, oi=1012., prev_oi=1000., oi_change_pct=1.2,
        volume_ratio=1.75, traded_value=2_000_000.,
        confirmation_open=100., confirmation_high=102., confirmation_low=99.5,
        confirmation_close=101.5, confirmation_volume=10000., body_ratio=.54,
        v9_1m_upper_wick_ratio=.2, v9_1m_lower_wick_ratio=.2,
        v9_1m_volume_ratio=1.2, v9_exact_confirmation_present=True,
    )
    return {**row, **overrides}


@pytest.mark.parametrize("emas", [(99., 100., 101.), (None, None, None)])
def test_feature_ledger_explains_relaxed_long_without_false_rejections(emas):
    row = feature(ema9=emas[0], ema20=emas[1], ema50=emas[2])
    result = evaluate_v13_v10_g_base_row(row)
    assert result["base_side"] == "LONG"
    assert result["setup_filter_pass"] is True
    assert result["ema_alignment_bypassed"] is True
    assert result["maximum_base_oi_change_pct"] == 1.2
    assert result["required_volume_ratio"] == 1.75
    assert result["required_body_ratio"] == .54
    assert json.loads(result["failed_gates"]) == []


@pytest.mark.parametrize("day,clock", [("2026-10-05", "09:25"), ("2026-10-06", "09:30")])
def test_feature_ledger_does_not_relax_historical_or_other_slots(day, clock):
    result = evaluate_v13_v10_g_base_row(feature(day, clock))
    assert result["base_side"] == ""
    assert result["setup_filter_pass"] is False
    assert result["ema_alignment_bypassed"] is False
    assert result["maximum_base_oi_change_pct"] == 1.


@pytest.mark.parametrize("change,failed_gate", [
    ({"volume_ratio": 1.749}, "gate_base_volume"),
    ({"oi_change_pct": 1.201}, "gate_base_oi_max"),
    ({"body_ratio": .539}, "gate_setup_body"),
    ({"day": "2026-10-05"}, "gate_session_date"),
])
def test_feature_ledger_explains_relaxed_boundaries_and_wrong_day(change, failed_gate):
    row = feature()
    row.update(change)
    result = evaluate_v13_v10_g_base_row(row)
    assert result["setup_filter_pass"] is False
    assert failed_gate in json.loads(result["failed_gates"])


def test_feature_ledger_keeps_0925_short_nifty_gate():
    row = feature(price_change_pct=-.5, oi_change_pct=.5, body_ratio=.6,
                  confirmation_open=100., confirmation_high=100.5,
                  confirmation_low=98., confirmation_close=98.5)
    passing = evaluate_v13_v10_g_base_row(row, nifty_return=-.05)
    rejected = evaluate_v13_v10_g_base_row(row, nifty_return=.20)
    assert passing["base_side"] == "SHORT"
    assert passing["setup_filter_pass"] is True
    assert rejected["base_side"] == ""
    assert rejected["gate_nifty_0925_short"] is False
    assert passing["ema_alignment_bypassed"] is False


@pytest.mark.parametrize("day,stop_pct", [(date(2026, 10, 5), .6), (date(2026, 10, 6), 1.25)])
def test_coordinator_accepts_only_session_dated_execution_terms(tmp_path, monkeypatch, day, stop_pct):
    monkeypatch.setattr(coordinator, "config", config)
    monkeypatch.setattr(coordinator, "CONFIRMATION_ROOT", tmp_path / "confirmation")
    monkeypatch.setattr(coordinator, "SIGNAL_ROOT", tmp_path / "signals")
    setup = config.setup_for("09:25", "LONG", session_date=day)
    assert setup.stop_pct == stop_pct
    row = dict(
        signal_id="dated-signal", strategy_version=config.STRATEGY_VERSION,
        strategy_fingerprint=config.strategy_fingerprint(), session_date=day.isoformat(),
        signal_end="09:25", side="LONG", setup_id=setup.setup_id,
        confirmation_end=setup.confirmation_end,
        entry_activation_deadline_ist=config.activation_deadline(day, setup.confirmation_end).isoformat(timespec="seconds"),
        stop_pct=stop_pct, target_pct=setup.target_pct, live_sizing={"quantity": 1},
    )
    snapshot = {key: row[key] for key in ("strategy_version", "strategy_fingerprint", "session_date")}
    snapshot.update(state="SUCCESS", selected_signal_ids=[row["signal_id"]])
    snapshot_path = coordinator._confirmation_path(day, "09:25")
    snapshot_path.parent.mkdir(parents=True)
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    signal_path = coordinator.SIGNAL_ROOT / day.isoformat() / "dated-signal.json"
    signal_path.parent.mkdir(parents=True)
    signal_path.write_text(json.dumps(row), encoding="utf-8")
    assert coordinator.load_authoritative_signals(day) == [row]
    row["stop_pct"] = 1.25 if stop_pct == .6 else .6
    signal_path.write_text(json.dumps(row), encoding="utf-8")
    with pytest.raises(RuntimeError, match="stop_pct"):
        coordinator.load_authoritative_signals(day)
