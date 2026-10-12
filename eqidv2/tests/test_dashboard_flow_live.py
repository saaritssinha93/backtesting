"""Session integration checks use isolated market/evidence fixtures."""
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path

import pytest

import dashboard_flow_live as live


@pytest.fixture
def inputs(monkeypatch):
    monkeypatch.setattr(live, "list_available_dates", lambda root: ["2026-10-09", "2026-10-12", "2026-10-13"])
    monkeypatch.setattr(live, "load_session_market", lambda *args, **kwargs: {
        "slots": [{"time": "09:25", "rows": [{"symbol": "AAA", "price": 102, "price_pct": 2, "oi_change5m_pct": 4, "oi_change_pct": 999}]},
                  {"time": "10:00", "rows": [{"symbol": "FUTURE", "price": 200}]}],
        "warnings": ["Baseline gap"], "sources": [{"label": "market", "path": "saved"}],
    })
    monkeypatch.setattr(live, "build_live_evidence", lambda *args, **kwargs: {
        "strategy_version": "G3", "slots": {"09:25": {"state": "RECORDED", "counts": {"base_pass": 1, "published": 0},
            "rows": {"AAA": {"decision": "BASE_NOT_PUBLISHED", "base_pass": True, "published": False, "reasons": ["Not published"]}},
            "notes": ["Not proof of a missed trade"]}}, "warnings": [], "sources": [],
    })


def test_join_preserves_as_recorded_miss_and_five_minute_oi(inputs):
    payload = live.load_live_flow(Path("unused"), "2026-10-12", now=datetime(2026, 10, 12, 9, 31))
    assert len(payload["slots"]) == 76
    assert payload["slots"][0]["time"] == "09:15"
    assert payload["slots"][-1]["time"] == "15:30"
    slot = payload["slots"][2]
    assert slot["rows"][0]["oi_change_pct"] == 4
    assert slot["rows"][0]["evidence"]["status"] == "BASE_NOT_PUBLISHED"
    assert slot["evidence"]["counts"]["published"] == 0
    assert slot["strategy_rows"][0]["symbol"] == "AAA"
    assert payload["read_only"] is True
    assert payload["available_dates"] == ["2026-10-09", "2026-10-12"]


def test_future_source_rows_never_exposed(inputs):
    payload = live.load_live_flow(Path("unused"), now=datetime(2026, 10, 12, 9, 31))
    future = next(slot for slot in payload["slots"] if slot["time"] == "10:00")
    assert future["state"] == "future"
    assert future["rows"] == []
    assert future["strategy_rows"] == []


@pytest.mark.parametrize("value", ["../2026-10-12", "2026-02-30", "2026-1-2", "2026-10-13", "2026-10-12x"])
def test_invalid_dates_rejected_before_reading(inputs, monkeypatch, value):
    def forbidden(*args):
        raise AssertionError("Invalid input reached data discovery")
    monkeypatch.setattr(live, "list_available_dates", forbidden)
    with pytest.raises(ValueError):
        live.load_live_flow(Path("unused"), value, now=datetime(2026, 10, 12, 9, 31))


def test_clock_is_converted_to_ist(inputs):
    payload = live.load_live_flow(Path("unused"), now=datetime(2026, 10, 12, 4, 1, tzinfo=timezone.utc))
    assert payload["slots"][2]["state"] == "available"
    assert payload["slots"][4]["state"] == "future"
    assert payload["generated_at"].endswith("+05:30")
    json.dumps(payload, allow_nan=False)


def test_nonfinite_values_become_missing():
    assert live._json_safe({"rows": [{"price": float("nan"), "oi": float("inf")}]}) == {"rows": [{"price": None, "oi": None}]}
