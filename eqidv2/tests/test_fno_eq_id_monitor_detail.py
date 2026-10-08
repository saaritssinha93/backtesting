"""The monitoring reader never treats absence or later-phase placeholders as pass."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime

import pytest

import fno_eq_id_monitor_detail as detail


DAY = "2026-10-07"
NOW = datetime.fromisoformat("2026-10-07T10:00:00+05:30")


def feature(**changes):
    row = dict(session_date=DAY, strategy_version=detail.STRATEGY_VERSION,
               strategy_fingerprint="pinned", signal_ts=f"{DAY}T09:25:00+05:30",
               confirmation_ts=f"{DAY}T09:26:00+05:30", signal_end="09:25", confirmation_end="09:26",
               tradingsymbol="TEST", base_side="LONG", base_long_pass=True, base_short_pass=False,
               setup_filter_pass=True, setup_id="0926_LONG", gate_base_volume=True,
               gate_ema_long=True, gate_ema_short=False, gate_confirmation_direction=True,
               gate_confirmation_volume=True, volume_ratio=2.5, v9_1m_volume_ratio=1.3,
               required_volume_ratio=1.75, margin_volume_ratio=0.75,
               gate_setup_volume=True, failed_gates="[]")
    row.update(changes)
    return row


def payload(rows=None, **changes):
    rows = [feature()] if rows is None else rows
    value = dict(session_date=DAY, strategy_version=detail.STRATEGY_VERSION, strategy_fingerprint="pinned",
                 signal_end="09:25", confirmation_end="09:26", published_at_ist=f"{DAY}T09:26:10+05:30",
                 state="SUCCESS", feature_evaluations=rows, feature_evaluation_count=len(rows),
                 selected_signal_ids=[], candidate_count=len(rows), scanner_complete=True)
    value.update(changes)
    return value


def write_phase(tmp_path, value, phase="5m"):
    directory = tmp_path / "v13_v10_g_live" / ("scanner_5m" if phase == "5m" else "confirmation_1m") / DAY
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / ("slot_0925.json" if phase == "5m" else "slot_0926.json")
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def build(tmp_path, **kwargs):
    return detail.build_monitor_detail(tmp_path, DAY, now_ist=kwargs.get("now", NOW))


def check(row, name):
    return next(item for item in row["checks"] if item["name"] == name)


def test_scanner_does_not_claim_confirmation_rejection_or_selection(tmp_path):
    write_phase(tmp_path, payload([feature(gate_confirmation_present=False,
                                          failed_gates='["gate_confirmation_present"]')]))
    row = build(tmp_path)["rows_5m"][0]
    assert row["decision"] == "BASE_PASS"
    assert row["first_failed_gate"] == ""
    assert row["failed_gates"] == []
    assert all(item["name"] != "gate_confirmation_present" for item in row["checks"])
    assert check(row, "confirmation_stage")["status"] == "NOT_EVALUATED"
    assert check(row, "gate_ema_short")["status"] == "NOT_APPLICABLE"


def test_explicit_false_and_missing_gate_differ(tmp_path):
    write_phase(tmp_path, payload([feature(gate_base_volume=False)]))
    row = build(tmp_path)["rows_5m"][0]
    assert check(row, "gate_base_volume")["status"] == "FAIL"
    assert check(row, "gate_base_oi_min")["status"] == "UNKNOWN"


@pytest.mark.parametrize("value, expected", [(False, "FAIL"), ("false", "FAIL"), (None, "UNKNOWN"), (0, "UNKNOWN"), ("yes", "UNKNOWN"), (True, "PASS")])
def test_bool_parsing_is_strict(tmp_path, value, expected):
    write_phase(tmp_path, payload([feature(gate_base_volume=value)]))
    assert check(build(tmp_path)["rows_5m"][0], "gate_base_volume")["status"] == expected


def test_recorded_threshold_and_margin_not_recalculated(tmp_path):
    write_phase(tmp_path, payload(), "1m")
    row = build(tmp_path)["rows_1m"][0]
    gate = check(row, "gate_setup_volume")
    assert gate["actual"] == 2.5 and gate["margin"] == 0.75
    assert "1.75" in gate["rule"]
    assert row["decision"] == "FILTER_PASS_NOT_SELECTED"
    assert "exact exclusion reason is not recorded" in check(row, "final_selection")["reason"]


def test_selected_id_does_not_turn_failed_indicator_into_pass(tmp_path):
    write_phase(tmp_path, payload([feature(gate_confirmation_volume=False)],
                                 selected_signal_ids=["20261007_0926_LONG_TEST_abcdef"]), "1m")
    row = build(tmp_path)["rows_1m"][0]
    assert row["decision"] == "SELECTED"
    assert check(row, "gate_confirmation_volume")["status"] == "FAIL"


def test_blocked_slot_never_claims_selection(tmp_path):
    write_phase(tmp_path, payload(state="BLOCKED_INCOMPLETE_DATA", selected_signal_ids=["20261007_0926_LONG_TEST_hash"]), "1m")
    row = build(tmp_path)["rows_1m"][0]
    assert row["decision"] == "BLOCKED_INCOMPLETE_SLOT"
    assert check(row, "final_selection")["status"] == "NOT_EVALUATED"


@pytest.mark.parametrize("changes,state", [
    ({"session_date": "2026-10-06"}, "STALE_SESSION"),
    ({"strategy_version": "another_strategy"}, "INVALID_STRATEGY"),
    ({"signal_end": "09:30"}, "INVALID_CLOCK"),
    ({"published_at_ist": f"{DAY}T11:00:00+05:30"}, "FUTURE_EVIDENCE"),
    ({"published_at_ist": "2026-10-06T09:26:00+05:30"}, "INVALID_TIMESTAMP"),
    ({"feature_evaluation_count": 8}, "LEDGER_COUNT_MISMATCH"),
    ({"feature_evaluations_sha256": "wrong"}, "CHECKSUM_MISMATCH"),
])
def test_invalid_parent_cannot_supply_pass(tmp_path, changes, state):
    write_phase(tmp_path, payload(**changes))
    result = build(tmp_path)
    assert result["coverage"][0]["scanner_state"] == state
    row = result["rows_5m"][0]
    assert row["evidence_state"] == state
    assert row["decision"] == "UNKNOWN"
    assert all(item["status"] == "UNKNOWN" for item in row["checks"])


def test_ledger_hash_verifies(tmp_path):
    value = payload()
    value["feature_evaluations_sha256"] = hashlib.sha256(json.dumps(value["feature_evaluations"], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    write_phase(tmp_path, value)
    assert build(tmp_path)["coverage"][0]["scanner_state"] == "RECORDED"


@pytest.mark.parametrize("changes,state", [
    ({"session_date": "2026-10-06"}, "STALE_SESSION"),
    ({"strategy_fingerprint": "different"}, "INVALID_STRATEGY"),
    ({"confirmation_ts": f"{DAY}T09:27:00+05:30"}, "INVALID_CLOCK"),
    ({"evaluation_state": "TELEMETRY_ERROR"}, "TELEMETRY_ERROR"),
    ({"volume_ratio": float("nan")}, "INVALID_LEDGER_VALUES"),
    ({"volume_ratio": float("inf")}, "INVALID_LEDGER_VALUES"),
])
def test_invalid_row_is_unknown_and_json_safe(tmp_path, changes, state):
    write_phase(tmp_path, payload([feature(**changes)]))
    result = build(tmp_path)
    row = result["rows_5m"][0]
    assert row["evidence_state"] == state
    assert row["decision"] == "UNKNOWN"
    assert check(row, "gate_base_volume")["status"] == "UNKNOWN"
    json.dumps(result, allow_nan=False)


def test_missing_evidence_never_uses_prior_day(tmp_path):
    directory = tmp_path / "v13_v10_g_live" / "scanner_5m" / "2026-10-06"
    directory.mkdir(parents=True)
    (directory / "slot_0925.json").write_text(json.dumps(payload()), encoding="utf-8")
    result = build(tmp_path)
    assert result["rows_5m"] == []
    assert result["rows_1m"] == []
    assert result["coverage"][0]["scanner_state"] == "MISSING"


def test_confirmation_is_not_due_until_its_clock(tmp_path):
    result = build(tmp_path, now=datetime.fromisoformat(f"{DAY}T09:25:30+05:30"))
    assert result["coverage"][0]["scanner_state"] == "MISSING"
    assert result["coverage"][0]["confirmation_state"] == "NOT_DUE"


def test_ema_policy_bypass_not_labelled_indicator_pass(tmp_path):
    write_phase(tmp_path, payload([feature(ema_alignment_bypassed=True)]))
    gate = check(build(tmp_path)["rows_5m"][0], "gate_ema_long")
    assert gate["status"] == "NOT_APPLICABLE"
    assert "bypass" in gate["reason"]


def test_symbol_strings_remain_data_not_markup(tmp_path):
    symbol = '<script>alert("X")</script>|TEST'
    write_phase(tmp_path, payload([feature(tradingsymbol=symbol)]))
    result = build(tmp_path)
    assert result["rows_5m"][0]["symbol"] == symbol
    assert "<script>" in json.dumps(result)  # API data; frontend must use textContent.


def test_missing_candle_has_own_row_not_failed_indicator(tmp_path):
    write_phase(tmp_path, payload([], ineligible_no_candle_symbols=["MISSING"]), "1m")
    row = build(tmp_path)["rows_1m"][0]
    assert row["symbol"] == "MISSING" and row["stage"] == "DATA_COVERAGE"
    assert row["decision"] == "NO_CANDLE"


def test_parent_strategy_mismatch_suppresses_confirmation(tmp_path):
    write_phase(tmp_path, payload())
    write_phase(tmp_path, payload([feature(strategy_fingerprint="new")], strategy_fingerprint="new"), "1m")
    row = build(tmp_path)["rows_1m"][0]
    assert row["evidence_state"] == "INVALID_STRATEGY"
    assert check(row, "gate_setup_volume")["status"] == "UNKNOWN"


def test_oversized_files_are_bounded(tmp_path, monkeypatch):
    monkeypatch.setattr(detail, "MAX_FILE_BYTES", 10)
    write_phase(tmp_path, payload())
    result = build(tmp_path)
    assert result["rows_5m"] == []
    assert any("safe read limit" in warning for warning in result["warnings"])


def test_feature_row_limit_reports_truncation(tmp_path, monkeypatch):
    monkeypatch.setattr(detail, "MAX_FEATURE_ROWS", 1)
    write_phase(tmp_path, payload([feature(), feature(tradingsymbol="SECOND")]))
    result = build(tmp_path)
    assert len(result["rows_5m"]) == 1
    assert any("truncated" in warning for warning in result["warnings"])


def test_no_guard_pass_is_inferred_from_order_fill(tmp_path):
    directory = tmp_path / "v13_v10_g_live" / "orders" / "PAPER" / DAY
    directory.mkdir(parents=True)
    row = dict(session_date=DAY, strategy_version=detail.STRATEGY_VERSION, strategy_fingerprint="pinned",
               updated_at_ist=f"{DAY}T09:27:00+05:30", tradingsymbol="TEST", side="LONG",
               status="OPEN", status_reason="PAPER_STOP_ENTRY_TOUCHED", entry_price=100,
               paper_portfolio_admission={"allowed": True, "available_capital_rs": 1000000, "required_capital_rs": 100000},
               first_entry_blocker_reason="", last_entry_blocker_reason="")
    (directory / "order.json").write_text(json.dumps(row), encoding="utf-8")
    result = build(tmp_path)["rows_1m"][0]
    assert result["stage"] == "PAPER_ORDER_SNAPSHOT"
    assert check(result, "paper_capital_admission")["status"] == "PASS"
    assert check(result, "recorded_entry_blocker")["status"] == "UNKNOWN"
    assert check(result, "entry_deadline")["status"] == "UNKNOWN"


def test_event_uses_actual_timestamp_and_no_synthetic_minutes(tmp_path):
    directory = tmp_path / "v13_v10_g_live" / "order_events" / "PAPER"
    directory.mkdir(parents=True)
    event = dict(context={"session_date": DAY, "strategy_version": detail.STRATEGY_VERSION, "strategy_fingerprint": "pinned"},
                 data={"tradingsymbol": "TEST", "side": "LONG", "state_after": "OPEN", "reason": "PAPER_STOP_ENTRY_TOUCHED"}, timestamp_utc=f"{DAY}T03:56:10Z")
    (directory / f"{DAY}.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")
    rows = build(tmp_path)["rows_1m"]
    assert len(rows) == 1 and rows[0]["minute"] == "09:26"
    assert check(rows[0], "entry_guard_audit")["status"] == "UNKNOWN"


@pytest.mark.parametrize("day", ["../2026-10-07", "2026-02-30", "20261007", "", None])
def test_date_path_validation(tmp_path, day):
    with pytest.raises(ValueError):
        detail.build_monitor_detail(tmp_path, day, now_ist=NOW)


def test_naive_clock_rejected(tmp_path):
    with pytest.raises(ValueError):
        detail.build_monitor_detail(tmp_path, DAY, now_ist=datetime(2026, 10, 7, 10))


def test_source_quality_cannot_pass_with_wrong_latest_clock(tmp_path):
    entry = dict(symbol="TEST", source="NSE_EQUITY_5M_LIVE", session_date=DAY,
                 signal_end="09:25", usable=True, status="GOOD", timestamp_max=f"{DAY}T09:20:00+05:30")
    write_phase(tmp_path, payload(raw_data_quality=[entry]))
    row = build(tmp_path)["rows_5m"][0]
    assert check(row, "equity_source_quality")["status"] == "UNKNOWN"
    entry["timestamp_max"] = f"{DAY}T09:25:00+05:30"
    write_phase(tmp_path, payload(raw_data_quality=[entry]))
    assert check(build(tmp_path)["rows_5m"][0], "equity_source_quality")["status"] == "PASS"


def test_verified_missing_futures_stock_is_not_silently_lost(tmp_path):
    write_phase(tmp_path, payload(skipped_no_candle_contracts=[{
        "equity_symbol": "SAIL", "futures_tradingsymbol": "SAIL26OCTFUT",
        "reason": "repeatedly_verified_exact_slot_no_candle",
    }]))
    rows = build(tmp_path)["rows_5m"]
    assert len(rows) == 2
    assert rows[1]["symbol"] == "SAIL" and rows[1]["decision"] == "VERIFIED_NO_FUTURES_CANDLE"


def write_feed(tmp_path, scan, **changes):
    digest = hashlib.sha256(json.dumps(scan, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode()).hexdigest()
    directory = tmp_path / "equity_1m_slot_ready" / "v6" / DAY
    directory.mkdir(parents=True, exist_ok=True)
    marker = payload([], complete=True, within_deadline=False, written_count=1, candidate_count=1,
                     scanner_snapshot_sha256=digest, deadline_ist=f"{DAY}T09:27:30+05:30",
                     observation_history={"TEST": [{"observed_at_ist": f"{DAY}T09:26:08+05:30", "state": "WRITTEN"}]})
    marker.update(changes)
    (directory / f"slot_0926_{digest[:16]}.json").write_text(json.dumps(marker), encoding="utf-8")
    return marker


def test_durable_feed_recorded_flags_and_stock_observation(tmp_path):
    scan = payload()
    write_phase(tmp_path, scan)
    write_phase(tmp_path, payload(), "1m")
    write_feed(tmp_path, scan)
    result = build(tmp_path)
    row = result["rows_1m"][0]
    assert check(row, "durable_feed_complete")["status"] == "PASS"
    assert check(row, "feed_within_deadline")["status"] == "FAIL"
    assert row["indicators"]["bar_observation_state"] == "WRITTEN"
    assert result["coverage"][0]["confirmation"]["durable_feed"]["written_count"] == 1


def test_feed_marker_hash_mismatch_cannot_pass(tmp_path):
    scan = payload()
    write_phase(tmp_path, scan)
    write_phase(tmp_path, payload(confirmation_feed_marker_sha256="wrong"), "1m")
    write_feed(tmp_path, scan)
    row = build(tmp_path)["rows_1m"][0]
    assert check(row, "durable_feed_complete")["status"] == "UNKNOWN"
    assert row["indicators"]["durable_feed_state"] == "CHECKSUM_MISMATCH"


def test_invalid_feature_row_cannot_borrow_valid_feed_pass(tmp_path):
    scan = payload()
    write_phase(tmp_path, scan)
    write_phase(tmp_path, payload([feature(strategy_fingerprint="wrong")]), "1m")
    write_feed(tmp_path, scan)
    row = build(tmp_path)["rows_1m"][0]
    assert all(item["status"] == "UNKNOWN" for item in row["checks"])


def test_blocked_slot_completeness_guard_is_visible(tmp_path):
    write_phase(tmp_path, payload(state="BLOCKED_INCOMPLETE_DATA"))
    assert check(build(tmp_path)["rows_5m"][0], "slot_complete")["status"] == "FAIL"
