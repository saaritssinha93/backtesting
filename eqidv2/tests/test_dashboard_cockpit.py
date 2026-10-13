"""Cockpit projections must not turn absent/mixed/stale evidence into money or health."""
from __future__ import annotations

import json
from datetime import datetime

import pytest

import dashboard_cockpit as cockpit
import fno_eq_id_monitor_detail as monitor_detail
from fno_strategy_registry import PROFILES


DAY = "2026-10-08"
NOW = datetime.fromisoformat(f"{DAY}T10:00:00+05:30")


@pytest.fixture(autouse=True)
def explicit_test_profile(monkeypatch):
    # Test fixtures have an explicit namespace, independent of deployment edits.
    profile = PROFILES["V13_V10_G"]
    monkeypatch.setattr(cockpit, "ACTIVE_PROFILE", profile)
    monkeypatch.setattr(cockpit, "STRATEGY_VERSION", profile.version)
    monkeypatch.setattr(monitor_detail, "ACTIVE_PROFILE", profile)
    monkeypatch.setattr(monitor_detail, "STRATEGY_VERSION", profile.version)


@pytest.fixture
def empty_detail(monkeypatch):
    monkeypatch.setattr(cockpit, "build_monitor_detail", lambda *a, **k: dict(
        warnings=[], coverage=[], rows_5m=[], rows_1m=[], state="NO_EVIDENCE"))


def state(**changes):
    result = dict(mode="PAPER", session_date=DAY, strategy_version=cockpit.STRATEGY_VERSION,
                  strategy_fingerprint="fingerprint", signal_id="signal-1", tradingsymbol="TEST",
                  side="LONG", status="CLOSED", status_reason="TARGET", quantity=10,
                  updated_at_ist=f"{DAY}T09:59:30+05:30", entry_at_ist=f"{DAY}T09:30:00+05:30",
                  exit_at_ist=f"{DAY}T09:59:30+05:30", entry_price=100, exit_price=110,
                  last_price=110, estimated_cost_rs=1, net_pnl_rs=99)
    result.update(changes)
    return result


def write_order(tmp_path, row, *, filename="order.json", namespace=""):
    directory = tmp_path / "v13_v10_g_live" / "orders" / row["mode"]
    if namespace:
        directory /= namespace
    directory /= DAY
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / filename
    path.write_text(json.dumps(row), encoding="utf-8")
    return path


def load(tmp_path, **kwargs):
    return cockpit.load_cockpit_data(tmp_path, DAY, now_ist=NOW, **kwargs)


def test_empty_sources_never_mean_profitable_or_healthy(tmp_path):
    result = load(tmp_path)
    assert result["evidence"]["state"] == "NO_EVIDENCE"
    assert result["summary"]["realized_pnl_rs"] is None
    assert result["summary"]["unrealized_pnl_rs"] is None
    assert result["summary"]["total_pnl_rs"] is None
    assert result["summary"]["win_rate_pct"] is None
    assert result["summary"]["open"] is result["summary"]["pending"] is result["summary"]["closed"] is None
    assert result["summary"]["recorded_counts"]["orders"] == 0
    assert result["orders"] == result["chart"]["candles"] == []
    assert result["read_only"] is True


def test_live_namespace_and_paper_are_never_pooled(tmp_path, empty_detail):
    write_order(tmp_path, state())
    write_order(tmp_path, state(mode="LIVE", quantity=1, net_pnl_rs=8), namespace="live_kite_qty1")
    paper, live = load(tmp_path), load(tmp_path, mode="LIVE")
    assert paper["summary"]["realized_pnl_rs"] == 99
    assert live["summary"]["realized_pnl_rs"] == 8
    assert paper["summary"]["closed"] == live["summary"]["closed"] == 1
    assert live["orders"][0]["quantity"] == 1
    assert DAY in live["available_dates"]


def test_wrong_mode_snapshot_is_omitted_and_aggregate_withheld(tmp_path, empty_detail):
    path = write_order(tmp_path, state())
    path.write_text(json.dumps(state(mode="LIVE")), encoding="utf-8")
    result = load(tmp_path)
    assert result["orders"] == []
    assert result["evidence"]["orders_incomplete"]
    assert result["summary"]["total_pnl_rs"] is None


def test_snapshot_timestamp_does_not_prove_quote_freshness(tmp_path, empty_detail):
    write_order(tmp_path, state(status="OPEN", exit_at_ist="", exit_price=0))
    result = load(tmp_path)
    assert result["positions"][0]["recorded_net_pnl_rs"] == 99
    assert result["positions"][0]["quote_state"] == "NO_TIMESTAMPED_QUOTE"
    assert result["summary"]["unrealized_pnl_rs"] is None
    assert result["summary"]["total_pnl_rs"] is None
    assert result["chart"]["observations"] == [dict(timestamp_ist=f"{DAY}T09:30:00+05:30", price=100., label="Entry fill")]


@pytest.mark.parametrize("quote,expected", [("09:59:30", 99), ("09:50:00", None)])
def test_mark_to_market_requires_recent_timestamped_quote(tmp_path, empty_detail, quote, expected):
    write_order(tmp_path, state(status="OPEN", quote_at_ist=f"{DAY}T{quote}+05:30"))
    result = load(tmp_path)
    assert result["summary"]["unrealized_pnl_rs"] == expected


def test_same_signal_duplicates_do_not_double_count(tmp_path, empty_detail):
    raw = state(mode="LIVE")
    write_order(tmp_path, raw)
    write_order(tmp_path, raw, namespace="live_kite_qty1")
    result = load(tmp_path, mode="LIVE")
    assert result["summary"]["closed"] == 1
    assert result["summary"]["realized_pnl_rs"] == 99


def test_conflicting_duplicate_snapshots_withhold_pnl(tmp_path, empty_detail):
    write_order(tmp_path, state())
    write_order(tmp_path, state(net_pnl_rs=200), filename="duplicate.json")
    result = load(tmp_path)
    assert len(result["orders"]) == 1
    assert result["orders"][0]["evidence_state"] == "CONFLICT"
    assert result["summary"]["realized_pnl_rs"] is None


def test_mixed_fingerprints_withhold_aggregate(tmp_path, empty_detail):
    write_order(tmp_path, state())
    write_order(tmp_path, state(signal_id="signal-2", strategy_fingerprint="other"), filename="second.json")
    result = load(tmp_path)
    assert result["identity"]["state"] == result["evidence"]["state"] == "MISMATCH"
    assert result["summary"]["total_pnl_rs"] is None


def test_historical_open_quote_is_never_current(tmp_path, empty_detail):
    write_order(tmp_path, state(status="OPEN", quote_at_ist=f"{DAY}T09:59:30+05:30"))
    result = cockpit.load_cockpit_data(tmp_path, DAY, now_ist=datetime.fromisoformat("2026-10-12T00:01:00+05:30"))
    assert result["evidence"]["state"] == "HISTORICAL"
    assert result["summary"]["unrealized_pnl_rs"] is None


@pytest.mark.parametrize("kwargs", [dict(session_date="../../secret"), dict(session_date="2026-02-31"),
                                    dict(mode="BACKTEST"), dict(symbol="../TEST"), dict(symbol="test"),
                                    dict(now_ist=datetime(2026, 10, 8, 10))])
def test_invalid_query_rejected_before_sources(tmp_path, kwargs):
    params = dict(session_date=DAY, now_ist=NOW)
    params.update(kwargs)
    with pytest.raises(ValueError):
        cockpit.load_cockpit_data(tmp_path, **params)


def test_actual_candles_keep_gaps_and_exclude_wrong_dates(tmp_path, empty_detail):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    directory = tmp_path / "raw_equity_1m" / DAY
    directory.mkdir(parents=True)
    rows = [dict(timestamp=f"{DAY}T09:{minute}:00+05:30", tradingsymbol="TEST", exchange="NSE",
                 open=100., high=102., low=99., close=101., volume=10.) for minute in ("26", "31")]
    rows.append({**rows[0], "timestamp": "2026-10-07T09:26:00+05:30"})
    pq.write_table(pa.Table.from_pylist(rows), directory / "TEST_1minute.parquet")
    result = load(tmp_path, symbol="TEST")
    assert result["chart"]["kind"] == "candles"
    assert [row["timestamp_ist"] for row in result["chart"]["candles"]] == [f"{DAY}T09:26:00+05:30", f"{DAY}T09:31:00+05:30"]
    assert result["summary"]["total_pnl_rs"] is None


def test_event_stream_validates_mode_and_deduplicates(tmp_path, empty_detail):
    directory = tmp_path / "v13_v10_g_live" / "order_events" / "PAPER"
    directory.mkdir(parents=True)
    event = dict(timestamp_utc=f"{DAY}T04:29:30+00:00", event_type="order_state_transition",
                 context=dict(mode="PAPER", session_date=DAY, strategy_version=cockpit.STRATEGY_VERSION,
                              strategy_fingerprint="fingerprint", signal_id="signal-1"),
                 data=dict(tradingsymbol="TEST", state_before="PENDING_ENTRY", state_after="OPEN", quantity=10))
    other_mode = {**event, "context": {**event["context"], "mode": "LIVE"}}
    (directory / f"{DAY}.jsonl").write_text("\n".join(json.dumps(row) for row in [event, event, other_mode]), encoding="utf-8")
    result = load(tmp_path)
    assert len(result["events"]) == 1
    assert result["events"][0]["timestamp_ist"] == f"{DAY}T09:59:30+05:30"
    assert result["summary"]["realized_pnl_rs"] is None


def test_clock_distinguishes_weekend_and_preopen(tmp_path, empty_detail):
    now = datetime.fromisoformat("2026-10-12T00:01:00+05:30")
    assert cockpit.load_cockpit_data(tmp_path, "2026-10-12", now_ist=now)["session_clock"]["phase"] == "PREOPEN"
    sunday = cockpit.load_cockpit_data(tmp_path, "2026-10-11", now_ist=now)
    assert sunday["session_clock"]["phase"] == "WEEKEND"
    assert sunday["evidence"]["state"] == "NO_EVIDENCE"


def test_active_g3_does_not_fall_back_to_legacy_g_orders(tmp_path, empty_detail, monkeypatch):
    write_order(tmp_path, state())
    profile = PROFILES["V13_V10_G_3"]
    monkeypatch.setattr(cockpit, "ACTIVE_PROFILE", profile)
    monkeypatch.setattr(cockpit, "STRATEGY_VERSION", profile.version)
    result = load(tmp_path)
    assert result["identity"]["profile"] == "V13_V10_G_3"
    assert result["identity"]["label"] == "V13-V10-G-3"
    assert result["orders"] == []
    assert result["evidence"]["state"] == "NO_EVIDENCE"


def test_g3_fingerprint_is_evaluated_once_per_request_and_stale_evidence_is_rejected(tmp_path, empty_detail, monkeypatch):
    profile = PROFILES["V13_V10_G_3"]
    monkeypatch.setattr(cockpit, "ACTIVE_PROFILE", profile)
    monkeypatch.setattr(cockpit, "STRATEGY_VERSION", profile.version)
    current = ["current-fingerprint"]
    calls = []

    def fingerprint(profile_name):
        calls.append(profile_name)
        return current[0]

    monkeypatch.setattr(cockpit, "expected_fingerprint", fingerprint)
    root = tmp_path / profile.live_dir
    orders = root / "orders" / "PAPER" / DAY
    orders.mkdir(parents=True)
    (orders / "valid.json").write_text(json.dumps(state(strategy_version=profile.version,
        strategy_fingerprint=current[0])), encoding="utf-8")
    (orders / "stale.json").write_text(json.dumps(state(strategy_version=profile.version,
        strategy_fingerprint="old-fingerprint", signal_id="stale-signal")), encoding="utf-8")
    events = root / "order_events" / "PAPER"
    events.mkdir(parents=True)
    context = dict(mode="PAPER", session_date=DAY, strategy_version=profile.version,
                   strategy_fingerprint=current[0], signal_id="signal-1")
    rows = [dict(timestamp_utc=f"{DAY}T04:29:30+00:00", event_type="transition",
                 context=context, data=dict(tradingsymbol="TEST", reason=str(index))) for index in range(6)]
    rows.append({**rows[0], "context": {**context, "strategy_fingerprint": "old-fingerprint"}})
    (events / f"{DAY}.jsonl").write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
    (root / "strategy_manifest.json").write_text(json.dumps(dict(strategy_version=profile.version,
        strategy_fingerprint=current[0])), encoding="utf-8")

    first = load(tmp_path)
    assert calls == [profile.name]
    assert len(first["orders"]) == 1
    assert len(first["events"]) == 6
    assert first["identity"]["state"] == "RECORDED"

    current[0] = "new-fingerprint"
    second = load(tmp_path)
    assert calls == [profile.name, profile.name]
    assert second["orders"] == second["events"] == []
    assert second["identity"]["state"] == "NO_EVIDENCE"


def test_large_event_archive_reads_bounded_tail(tmp_path, empty_detail, monkeypatch):
    monkeypatch.setattr(cockpit, "MAX_EVENT_BYTES", 2048)
    directory = tmp_path / "v13_v10_g_live" / "order_events" / "PAPER"
    directory.mkdir(parents=True)
    event = dict(timestamp_utc=f"{DAY}T04:29:30+00:00", event_type="order_state_transition",
                 context=dict(mode="PAPER", session_date=DAY, strategy_version=cockpit.STRATEGY_VERSION,
                              strategy_fingerprint="fingerprint", signal_id="signal-1"),
                 data=dict(tradingsymbol="TEST", state_after="OPEN"))
    lines = []
    for index in range(100):
        lines.append(json.dumps({**event, "data": {**event["data"], "reason": f"event-{index}"}}))
    (directory / f"{DAY}.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    result = load(tmp_path)
    assert 0 < len(result["events"]) < 100
    assert any(row["reason"] == "event-99" for row in result["events"])
    assert any("bounded archive tail" in warning for warning in result["evidence"]["warnings"])


def test_manifest_alone_is_configuration_not_runtime_evidence(tmp_path, empty_detail):
    root = tmp_path / "v13_v10_g_live"
    root.mkdir()
    (root / "strategy_manifest.json").write_text(json.dumps(dict(strategy_version=cockpit.STRATEGY_VERSION, strategy_fingerprint="expected")), encoding="utf-8")
    result = load(tmp_path)
    assert result["identity"]["state"] == "CONFIGURED"
    assert result["evidence"]["state"] == "NO_EVIDENCE"


@pytest.mark.parametrize("changes", [dict(status=["OPEN"]), dict(side={"direction": "LONG"}),
                                    dict(strategy_fingerprint=["hash"]), dict(quantity=-1)])
def test_malformed_order_fields_are_unknown_not_server_errors(tmp_path, empty_detail, changes):
    write_order(tmp_path, state(**changes))
    result = load(tmp_path)
    assert result["orders"] == []
    assert result["summary"]["total_pnl_rs"] is None


def test_comparison_price_returns_ignore_quantity_and_keep_mode_totals(tmp_path, empty_detail):
    write_order(tmp_path, state(quantity=100, net_pnl_rs=999))
    write_order(tmp_path, state(mode="LIVE", quantity=1, entry_price=101, exit_price=109, net_pnl_rs=7), namespace="live_kite_qty1")
    result = load(tmp_path)
    row = result["comparison"][0]
    assert row["state"] == "PAIRED"
    assert row["paper_qty"] == 100 and row["live_qty"] == 1
    assert row["paper_return_pct"] == 10
    assert row["live_return_pct"] == pytest.approx((109 / 101 - 1) * 100, abs=0.000001)
    assert row["entry_difference_bps"] == 100
    assert result["summary"]["realized_pnl_rs"] == 999
    assert load(tmp_path, mode="LIVE")["summary"]["realized_pnl_rs"] == 7


def test_comparison_missing_and_unfilled_pairs_remain_unknown(tmp_path, empty_detail):
    write_order(tmp_path, state())
    missing = load(tmp_path)["comparison"][0]
    assert missing["state"] == "MISSING_LIVE"
    assert missing["live_entry"] is missing["live_return_pct"] is missing["entry_difference_bps"] is None
    write_order(tmp_path, state(mode="LIVE", quantity=1, status="ENTRY_REJECTED"), namespace="live_kite_qty1")
    unfilled = load(tmp_path)["comparison"][0]
    assert unfilled["state"] == "INCOMPLETE_FILLS"
    assert unfilled["live_entry"] is unfilled["live_exit"] is unfilled["live_return_pct"] is None
    assert unfilled["entry_difference_bps"] is None


def test_comparison_short_returns_and_conflicting_identity(tmp_path, empty_detail):
    write_order(tmp_path, state(side="SHORT", entry_price=100, exit_price=90))
    path = write_order(tmp_path, state(mode="LIVE", side="SHORT", entry_price=100, exit_price=90), namespace="live_kite_qty1")
    row = load(tmp_path)["comparison"][0]
    assert row["paper_return_pct"] == row["live_return_pct"] == 10
    path.write_text(json.dumps(state(mode="LIVE", side="LONG", entry_price=100, exit_price=90)), encoding="utf-8")
    conflict = load(tmp_path)["comparison"][0]
    assert conflict["state"] == "IDENTITY_MISMATCH"
    assert conflict["paper_return_pct"] is conflict["live_return_pct"] is conflict["entry_difference_bps"] is None


def test_invalid_record_withholds_exposure_counts_even_when_other_rows_are_valid(tmp_path, empty_detail):
    write_order(tmp_path, state(status="OPEN"))
    path = write_order(tmp_path, state(signal_id="invalid-signal"), filename="invalid.json")
    path.write_text("invalid json", encoding="utf-8")
    result = load(tmp_path)
    assert result["evidence"]["orders_incomplete"] is True
    assert result["summary"]["open"] is result["summary"]["pending"] is result["summary"]["closed"] is None
    assert result["summary"]["recorded_counts"]["open"] == 1


def test_valid_nonempty_book_can_report_zero_recorded_open_positions(tmp_path, empty_detail):
    write_order(tmp_path, state())
    result = load(tmp_path)
    assert result["summary"]["open"] == result["summary"]["pending"] == 0
    assert result["summary"]["closed"] == 1
