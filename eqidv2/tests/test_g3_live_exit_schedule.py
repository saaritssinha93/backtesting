"""Offline exit-clock and read-only operator-plan regression tests."""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import socket

import pytest

import g3_live_exit_schedule as exits
from tools import g3_live_operator as op

DAY = date(2026, 11, 11)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Exit schedule tests cannot access a broker or network")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)


def at(clock, day=DAY):
    return datetime.fromisoformat(f"{day.isoformat()}T{clock}+05:30")


def test_regular_exit_timing_and_earlier_strategy_exit():
    schedule = exits.build_schedule(DAY, "15:15")
    assert schedule["scope"] == "G3_LIVE_REGULAR_NSE"
    assert schedule["strategy_square_off"] == "15:15"
    assert schedule["flatten_at_ist"] == at("15:05:00").isoformat()
    assert schedule["verify_flat_at_ist"] == at("15:10:00").isoformat()
    assert schedule["broker_mis_cutoff_at_ist"] == at("15:12:00").isoformat()
    assert exits.effective_square_off(at("10:00:00"), "14:45", session_date=DAY) == at("14:45:00")
    assert exits.effective_square_off(at("15:05:00"), "15:15", session_date=DAY) == at("15:05:00")


def test_verification_boundary_and_timezone_conversion():
    assert not exits.is_verification_due(at("15:09:59"), "15:15", session_date=DAY)
    assert exits.is_verification_due(at("15:10:00"), "15:15", session_date=DAY)
    assert exits.is_verification_due(at("15:12:00").astimezone(timezone.utc), "15:15", session_date=DAY)
    utc = at("00:05:00").astimezone(timezone.utc)
    assert utc.date() != DAY
    assert exits.effective_square_off(utc, "15:15", session_date=DAY) == at("15:05:00")


@pytest.mark.parametrize("clock", ["9:15", "25:00", "15:60", "15:05:00", "15:05+05:30", "", None])
def test_malformed_strategy_clock_rejected(clock):
    with pytest.raises(ValueError, match="clock"):
        exits.build_schedule(DAY, clock)


def test_wrong_day_and_naive_clock_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        exits.effective_square_off(datetime(2026, 11, 11, 15), "15:15")
    with pytest.raises(ValueError, match="session date"):
        exits.effective_square_off(at("15:00:00"), "15:15", session_date=DAY - timedelta(days=1))
    with pytest.raises(ValueError, match="session date"):
        exits.build_schedule(at("15:00:00"), "15:15")


@pytest.fixture
def profile():
    return dict(profile=op.config.STRATEGY_PROFILE,
                strategy_version=op.config.STRATEGY_VERSION,
                strategy_fingerprint=op.config.strategy_fingerprint(),
                live_execution_policy=op.config.live_execution_policy.execution_metadata())


def test_operator_plan_reports_separate_live_schedule_without_mutating_identity(profile, tmp_path):
    before = deepcopy(profile)
    strategy_fingerprint = op.config.strategy_fingerprint()
    sizing_fingerprint = op.config.live_execution_policy.policy_fingerprint()
    absent = tmp_path / "uncreated"
    result = op.plan(DAY, profile, now=at("15:04:59"), runtime_root=absent)
    assert result["exit_policy"]["square_off"] == "15:15"
    assert result["live_exit_schedule"] == exits.build_schedule(DAY, "15:15")
    assert profile == before and not absent.exists()
    assert op.config.strategy_fingerprint() == strategy_fingerprint
    assert op.config.live_execution_policy.policy_fingerprint() == sizing_fingerprint
    assert not result["mutates_controls"] and not result["starts_workers"] and not result["submits_orders"]


@pytest.mark.parametrize("clock", ["15:05:00", "15:10:00", "15:14:59"])
def test_operator_cannot_approve_after_live_flatten_start(profile, tmp_path, clock):
    with pytest.raises(ValueError, match="square-off cutoff"):
        op.plan(DAY, profile, now=at(clock), runtime_root=tmp_path)


def test_operator_keeps_earlier_strategy_cutoff(profile, tmp_path, monkeypatch):
    monkeypatch.setattr(op, "validate_identity", lambda state: None)
    monkeypatch.setattr(op.config, "SQUARE_OFF", "14:45")
    with pytest.raises(ValueError, match="square-off cutoff"):
        op.plan(DAY, profile, now=at("14:45:00"), runtime_root=tmp_path)


@pytest.mark.parametrize("day", [date(2026, 11, 8), date(2026, 11, 10)])
def test_operator_does_not_apply_regular_schedule_to_muhurat_or_holiday(profile, tmp_path, day):
    with pytest.raises(ValueError, match="verified regular trading session"):
        op.plan(day, profile, now=at("14:00:00", day), runtime_root=tmp_path)


def test_operator_clock_is_aware_and_future_plan_uses_future_session(profile, tmp_path):
    with pytest.raises(ValueError, match="timezone-aware"):
        op.plan(DAY, profile, now=datetime(2026, 11, 11, 8), runtime_root=tmp_path)
    future = DAY + timedelta(days=1)
    result = op.plan(future, profile, now=at("16:00:00").astimezone(timezone.utc), runtime_root=tmp_path)
    assert result["live_exit_schedule"]["flatten_at_ist"] == at("15:05:00", future).isoformat()
