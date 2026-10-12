"""Offline option-PAPER source checks for the separate G3 LIVE sizing policy."""
from __future__ import annotations

import json
from datetime import date

import pytest

from fno_v13_v10_g_3_contract import STRATEGY_VERSION, strategy_fingerprint
from fno_v13_v10_g_3_live_execution_policy import execution_metadata

DAY = date(2026, 10, 12)


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def order(**changes):
    return dict(schema_version="fno_v6_equity_order_state_v2", mode="LIVE", session_date=str(DAY),
        strategy_version=STRATEGY_VERSION, strategy_fingerprint=strategy_fingerprint(),
        signal_id="g3-example", tradingsymbol="EXAMPLE", side="LONG", status="OPEN",
        quantity=50, requested_quantity=50, strategy_sized_quantity=50, lot_size=1,
        sizing_reference_price=1000., trigger_price=1000., entry_price=1001.,
        capital_rs=10000., execution_profile="live_kite_qty1", **execution_metadata()) | changes


@pytest.fixture
def options_source(tmp_path, monkeypatch):
    import fno_v13_v10_g_options_paper as options
    import fno_v13_v10_g_3_live_config as config
    monkeypatch.setattr(options, "equity_config", config)
    monkeypatch.setattr(options, "EQUITY_ORDER_ROOT", tmp_path / "source-orders")
    return options


def option_equity(**changes):
    return order(entry_at_ist=f"{DAY}T09:27:05+05:30") | changes


def save_option_source(options, filename, row):
    mode = row["mode"]
    root = options.EQUITY_ORDER_ROOT / mode
    if mode == "LIVE":
        root /= "live_kite_qty1"
    write(root / str(DAY) / filename, row)


@pytest.mark.parametrize("quantity", [50, 17])
def test_g3_options_source_accepts_variable_actual_live_fills(options_source, quantity):
    row = option_equity(quantity=quantity)
    save_option_source(options_source, "live.json", row)
    save_option_source(options_source, "paper.json", row | {"mode": "PAPER", "quantity": 500})
    issues = []
    rows = options_source.load_equity_entries(DAY, source="AUTO", issues=issues)
    assert len(rows) == 1 and rows[0]["source_equity_mode"] == "LIVE"
    assert rows[0]["quantity"] == quantity and not issues


@pytest.mark.parametrize("changes", [dict(live_execution_policy_fingerprint="old-qty1"),
    dict(quantity_policy="FIXED_ONE_SHARE"), dict(requested_quantity=500),
    dict(quantity=51), dict(quantity=0)])
def test_invalid_g3_live_options_source_is_quarantined_without_paper_fallback(options_source, changes):
    row = option_equity(**changes)
    save_option_source(options_source, "different-live-filename.json", row)
    paper = option_equity(mode="PAPER", quantity=500)
    for key in execution_metadata():
        paper.pop(key, None)
    save_option_source(options_source, "different-paper-filename.json", paper)
    save_option_source(options_source, "unrelated-paper.json", paper | {"signal_id": "paper-only"})
    issues = []
    rows = options_source.load_equity_entries(DAY, source="AUTO", issues=issues)
    assert [row["signal_id"] for row in rows] == ["paper-only"]
    assert issues[0]["reason"] == "SOURCE_EQUITY_G3_LIVE_SIZING_INVALID"
    assert issues[0]["signal_id_hint"] == "g3-example"
    assert options_source.load_equity_entries(DAY, source="LIVE") == []


def test_future_options_status_publishes_equity_fingerprint_without_order_authority(options_source, monkeypatch):
    published = []
    monkeypatch.setattr(options_source.common, "publish_status", lambda *args, **kwargs: published.append((args, kwargs)))
    options_source._publish("long-entry", "RUNNING", session_date_ist=str(DAY))
    args, payload = published[0]
    assert args[1] == "RUNNING"
    assert payload["execution_mode"] == "PAPER"
    assert payload["strategy_version"] == STRATEGY_VERSION
    assert payload["strategy_fingerprint"] == strategy_fingerprint()
    assert payload["stop_pct"] == 30 and payload["target_pct"] == 40.4
