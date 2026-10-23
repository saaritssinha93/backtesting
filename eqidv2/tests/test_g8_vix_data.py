"""Causal timing and independent source-integrity checks for G-8 VIX inputs."""
from pathlib import Path

import pandas as pd
import pytest

from research import g8_vix_data as vix


def _candles(day="2026-10-22"):
    dates = pd.date_range(f"{day} 09:15", periods=75, freq="5min", tz=vix.IST)
    return pd.DataFrame({
        "date": dates,
        "open": [20 + i / 10 for i in range(75)],
        "high": [20.3 + i / 10 for i in range(75)],
        "low": [19.9 + i / 10 for i in range(75)],
        "close": [20.2 + i / 10 for i in range(75)],
        "volume": 0,
    })


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "vix_5m.csv"
    _candles().to_csv(path, index=False)
    return path


def test_close_unavailable_until_completed_candle_boundary(source):
    data = vix.VixData(source)
    assert data.features("2026-10-22 09:19:59") is None
    boundary = data.features("2026-10-22 09:20:00")
    assert boundary["vix_level"] == pytest.approx(20.2)
    assert boundary["vix_available_at"] == "2026-10-22T09:20:00+05:30"
    assert boundary["vix_session_pct"] == pytest.approx(1.0)
    assert data.features("2026-10-22 09:24:59")["vix_level"] == pytest.approx(20.2)
    assert data.features("2026-10-22 09:25:00")["vix_level"] == pytest.approx(20.3)


def test_later_price_path_cannot_change_earlier_features(source):
    data = vix.VixData(source)
    decision = pd.Timestamp("2026-10-22 10:11", tz=vix.IST)
    before = data.features(decision, "2026-10-22 10:00")
    frame = data.days["2026-10-22"]
    frame.loc[frame.available_at > decision, ["open", "high", "low", "close"]] = 9999.0
    assert data.features(decision, "2026-10-22 10:00") == before
    assert before["vix_bar_start"] == "2026-10-22T10:05:00+05:30"


def test_early_slot_does_not_invent_15_or_30_minute_history(source):
    data = vix.VixData(source)
    early = data.features("2026-10-22 09:21")
    assert early["vix_15m_pct"] is None
    assert early["vix_30m_pct"] is None
    assert early["vix_previous_close_pct"] is None
    assert data.features("2026-10-22 09:30")["vix_15m_pct"] is not None
    assert data.features("2026-10-22 09:44:59")["vix_30m_pct"] is None
    assert data.features("2026-10-22 09:45")["vix_30m_pct"] is not None


def test_stale_and_missing_session_are_unavailable(source):
    data = vix.VixData(source)
    assert data.features("2026-10-23 10:11") is None
    assert data.features("2026-10-22 15:36") is None
    frame = data.days["2026-10-22"]
    data.days["2026-10-22"] = frame[frame.available_at <= pd.Timestamp("2026-10-22 10:10", tz=vix.IST)]
    assert data.features("2026-10-22 10:16") is None


def test_missing_opening_candle_cannot_establish_session_direction(source):
    data = vix.VixData(source)
    data.days["2026-10-22"] = data.days["2026-10-22"].iloc[1:].reset_index(drop=True)
    assert data.features("2026-10-22 10:11") is None


def test_previous_close_is_prior_session_only(tmp_path):
    path = tmp_path / "vix.csv"
    pd.concat([_candles("2026-10-21"), _candles("2026-10-22")]).to_csv(path, index=False)
    data = vix.VixData(path)
    current = data.features("2026-10-22 09:20")
    assert current["vix_previous_close_pct"] == pytest.approx((20.2 / 27.6 - 1) * 100)


def _pair(tmp_path):
    frame = _candles()
    five_path = tmp_path / "vix_5m.csv"
    fifteen_path = tmp_path / "vix_15m.csv"
    frame.to_csv(five_path, index=False)
    aggregate = frame.set_index("date").resample("15min").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
    aggregate.reset_index().to_csv(fifteen_path, index=False)
    return five_path, fifteen_path


def test_coverage_audit_records_complete_consistent_intervals(tmp_path, monkeypatch):
    monkeypatch.setattr(vix, "INPUT_ROOT", tmp_path)
    five, fifteen = _pair(tmp_path)
    report = vix.write_audit(five, fifteen)
    assert report["all_sessions_complete"] is True
    assert report["rows_5m"] == 75
    assert report["rows_15m"] == 25
    assert report["max_5m_to_15m_ohlc_difference"] == pytest.approx(0)
    assert set(report["files"]) == {five.name, fifteen.name, "india_vix_coverage.csv"}
    assert (tmp_path / "india_vix_provenance.json").exists()


def test_coverage_audit_rejects_missing_candle(tmp_path, monkeypatch):
    monkeypatch.setattr(vix, "INPUT_ROOT", tmp_path)
    five, fifteen = _pair(tmp_path)
    frame = pd.read_csv(five).drop(index=20)
    frame.to_csv(five, index=False)
    with pytest.raises(ValueError, match="incomplete"):
        vix.write_audit(five, fifteen)


def test_coverage_audit_rejects_inconsistent_interval_prices(tmp_path, monkeypatch):
    monkeypatch.setattr(vix, "INPUT_ROOT", tmp_path)
    five, fifteen = _pair(tmp_path)
    frame = pd.read_csv(fifteen)
    frame.loc[3, "close"] += 0.05
    frame.to_csv(fifteen, index=False)
    with pytest.raises(ValueError, match="disagree"):
        vix.write_audit(five, fifteen)


def test_invalid_future_or_cross_session_slot_rejected(source):
    data = vix.VixData(source)
    with pytest.raises(ValueError, match="slot start"):
        data.features("2026-10-22 10:11", "2026-10-22 10:15")
    with pytest.raises(ValueError, match="slot start"):
        data.features("2026-10-22 10:11", "2026-10-21 10:00")
