from copy import deepcopy
from datetime import datetime
import json

import pandas as pd
import pytest

from dashboard_flow_observer_store import append_events, observer_root, write_json
from dashboard_flow_opening_data import apply_opening_observations, opening_enabled
from fno_v13_v10_g_3_opening_oi import compare_first_minute_oi_to_0920
from dashboard_flow_live_data import load_session_market

DAY = "2026-10-12"
CONTRACT = "TEST26OCTFUT"


def event(clock="09:15:00", *, receipt=None, kind="future", **changes):
    return {"session_date": DAY, "symbol": "TEST", "contract": CONTRACT if kind == "future" else "", "kind": kind,
            "exchange_timestamp": f"{DAY}T{clock}+05:30", "received_at": f"{DAY}T{receipt or clock}+05:30", "price": 100, "session_open": 99, "oi": 1000 if kind == "future" else None, "source": "KITE_WEBSOCKET_FULL", **changes}


def market():
    rows = []
    for clock, oi, change in [("09:15", None, None), ("09:20", 1100, None), ("09:25", 1210, 10)]:
        rows.append({"time": clock, "state": "OPENING_REFERENCE" if clock == "09:15" else "COMPLETE", "coverage": {}, "rows": [{"symbol": "TEST", "contract": CONTRACT, "price": 100, "oi": oi, "oi_source": "NEAR_MONTH_FUTURES_5M" if oi is not None else None, "previous_oi": 900, "previous_close": 99, "price_pct": 1, "change5m_pct": None if clock == "09:15" else 2, "oi_change_pct": 12, "oi_day_change_pct": 12, "oi_change5m_pct": change}]})
    return {"session_date": DAY, "slots": rows, "warnings": [], "sources": []}


def apply(root, base=None, at="09:30:00"):
    return apply_opening_observations(root, base or market(), now=datetime.fromisoformat(f"{DAY}T{at}+05:30"))


def row(result, index=0):
    return result["slots"][index]["rows"][0]


def enable(root):
    write_json(root, "config.json", {"opening_enabled": True})


def history(root, **changes):
    write_json(root, f"historical/{DAY}/{CONTRACT}.json", {"session_date": DAY, "symbol": "TEST", "contract": CONTRACT, "source": "KITE_HISTORICAL_MINUTE", "acquired_at": f"{DAY}T19:00:00+05:30", "candles": [{"candle_start": f"{DAY}T09:15:00+05:30", "candle_end": f"{DAY}T09:16:00+05:30", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 20, "oi": 1050}], **changes})


def live_quote(root, **changes):
    quote = {"symbol": "TEST", "contract": CONTRACT, "status": "QUOTE_NEAR_0916_NOT_CANDLE",
             "baseline_kind": "PROVISIONAL_FULL_QUOTE", "oi": 1050, "source": "KITE_FULL_QUOTE",
             "exchange_timestamp": f"{DAY}T09:16:05+05:30", "received_at": f"{DAY}T09:16:06+05:30",
             "exact_first_minute_candle": False, "quote_baseline_available_by_0920": True, **changes}
    write_json(root, f"coverage/live_opening_oi_{DAY}.json", {"schema": "dashboard_flow_live_opening_oi_v1", "session_date": DAY, "universe_source": "DATED_UNIVERSE", "rows": [quote]})


def test_disabled_default_never_changes_base_rows_or_creates_files(tmp_path):
    base = market()
    result = apply(tmp_path, base)
    assert result["slots"] == base["slots"]
    assert result["opening_observer"]["state"] == "DISABLED"
    assert not observer_root(tmp_path).exists()


def test_exact_opening_supplies_day_and_0920_not_0915_five_minute(tmp_path):
    enable(tmp_path)
    append_events(tmp_path, DAY, [event("09:10:00", oi=950), event(), event("09:10:00", kind="cash", price=98), event(kind="cash")])
    base = market()
    unchanged = deepcopy(base)
    result = apply(tmp_path, base)
    assert base == unchanged
    assert row(result)["oi"] == 1000
    assert row(result)["oi_day_change_pct"] == pytest.approx((1000 / 900 - 1) * 100)
    assert row(result)["oi_change5m_pct"] is None
    assert row(result)["change5m_pct"] is None
    assert row(result)["opening_context"]["oi_preopen_change_pct"] == pytest.approx((1000 / 950 - 1) * 100)
    assert row(result)["opening_context"]["price_preopen_change_pct"] == pytest.approx((100 / 98 - 1) * 100)
    assert row(result, 1)["oi_change5m_pct"] == pytest.approx(10)
    assert row(result, 1)["oi_change5m_source"] == "EXACT_OPENING_OBSERVER_BASELINE"
    assert {key: value for key, value in row(result, 2).items() if key != "opening_context"} == row(base, 2)
    assert result["opening_observer"]["verified_opening_oi"] == 1
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("record", [event(receipt="09:15:01"), event("09:15:20"), event("09:13:59"), event(contract="TEST26NOVFUT")])
def test_late_stale_and_wrong_contract_cannot_fill_opening(tmp_path, record):
    enable(tmp_path)
    append_events(tmp_path, DAY, [record])
    result = apply(tmp_path)
    assert row(result)["oi"] is None
    assert row(result, 1)["oi_change5m_pct"] is None
    assert result["opening_observer"]["verified_opening_oi"] == 0


def test_recent_asof_snapshot_has_separate_actual_interval(tmp_path):
    enable(tmp_path)
    append_events(tmp_path, DAY, [event("09:14:30", receipt="09:14:31")])
    result = apply(tmp_path)
    assert row(result)["oi"] == 1000
    assert row(result, 1)["oi_change5m_pct"] is None
    context = row(result, 1)["opening_context"]
    assert context["comparison_seconds"] == 330
    assert context["oi_opening_change_pct"] == pytest.approx(10)


@pytest.mark.parametrize("changes", [{"gap_filled": True}, {"quality_state": "INVALID"}, {"source": "KITE_HISTORICAL_MINUTE"}, {"source": "GAP_FILLED_SNAPSHOT"}])
def test_synthetic_or_candle_evidence_cannot_become_opening_snapshot(tmp_path, changes):
    enable(tmp_path)
    append_events(tmp_path, DAY, [event(**changes)])
    assert row(apply(tmp_path))["oi"] is None


def test_incomplete_raw_write_is_visible_in_audit_without_blocking_market(tmp_path):
    enable(tmp_path)
    append_events(tmp_path, DAY, [event()])
    with (observer_root(tmp_path) / "raw" / DAY / "events.jsonl").open("ab") as handle:
        handle.write(b'{"not_complete":')
    result = apply(tmp_path)
    assert row(result)["oi"] == 1000
    assert result["opening_observer"]["record_audit"]["partial_records"] == 1
    assert "INVALID_OR_PARTIAL_OBSERVATIONS_IGNORED" in result["opening_observer"]["audit_flags"]


def test_first_minute_never_backdated_and_keeps_exact_open_unknown(tmp_path):
    enable(tmp_path)
    history(tmp_path)
    result = apply(tmp_path)
    assert row(result)["oi"] is None
    assert row(result)["opening_context"]["first_minute_oi"] is None
    assert "first_minute" not in row(result)["opening_context"]
    assert row(result, 1)["oi_change5m_pct"] == pytest.approx((1100 / 1050 - 1) * 100)
    assert row(result, 1)["opening_context"]["first_minute_oi"] == 1050
    assert row(result, 1)["opening_context"]["first_minute_retrospective"] is True
    first = row(result, 1)["opening_context"]["first_minute"]
    assert first == {
        "candle_start": f"{DAY}T09:15:00+05:30",
        "candle_end": f"{DAY}T09:16:00+05:30",
        "instrument": "FUTURES", "price_basis": "DATED_NEAR_MONTH_FUTURES", "contract": CONTRACT,
        "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0,
        "volume": 20.0, "oi": 1050.0, "previous_oi": 900,
        "oi_day_change_pct": pytest.approx((1050 / 900 - 1) * 100),
        "source": "KITE_HISTORICAL_MINUTE", "acquired_at": f"{DAY}T19:00:00+05:30",
        "retrospective": True,
    }
    assert row(result, 1)["price"] == 100  # Cash price is unchanged by futures OHLC.
    assert row(result, 1)["oi"] == 1100  # Regular five-minute futures OI is unchanged.
    assert row(result, 1)["opening_context"]["oi_change_since_first_minute_pct"] == pytest.approx((1100 / 1050 - 1) * 100)
    assert row(result, 1)["oi_change_pct"] == row(result, 1)["oi_change5m_pct"] == row(result, 1)["opening_context"]["oi_change_since_first_minute_pct"]
    assert row(result, 1)["oi_change5m_source"] == "FIRST_MINUTE_0916_BOOTSTRAP_4M"
    assert row(result, 1)["oi_change5m_interval_minutes"] == 4
    assert row(result, 1)["oi_change5m_available_by_0920"] is False
    assert row(result, 1)["opening_context"]["oi_change_since_first_minute_interval_minutes"] == 4
    assert row(result, 1)["opening_context"]["available_by_0920"] is False
    assert "oi_change_since_first_minute_pct" not in row(result)["opening_context"]
    assert "oi_change_since_first_minute_pct" not in row(result, 2)["opening_context"]
    assert result["opening_first_minutes"] == [{"symbol": "TEST", "contract": CONTRACT, "first_minute": first}]
    assert result["opening_observer"]["state"] == "HISTORICAL_CONTEXT_ONLY"
    before_end = apply(tmp_path, at="09:15:00")
    assert before_end["opening_observer"]["first_minute_symbols"] == 0
    assert before_end["opening_first_minutes"] == []
    assert "first_minute" not in row(before_end)["opening_context"]
    assert apply(tmp_path, at="09:16:00")["opening_first_minutes"][0]["first_minute"]["oi"] == 1050


def test_timely_live_quote_bootstraps_first_bar_with_explicit_provisional_source(tmp_path):
    enable(tmp_path)
    live_quote(tmp_path)
    result = apply(tmp_path)
    opening, at_0920, later = (row(result, index) for index in range(3))
    expected = (1100 / 1050 - 1) * 100
    assert opening["oi"] is None
    assert at_0920["oi_change_pct"] == at_0920["oi_change5m_pct"] == pytest.approx(expected)
    assert at_0920["opening_context"]["oi_change_since_0916_pct"] == pytest.approx(expected)
    assert at_0920["oi_change5m_source"] == "LIVE_QUOTE_NEAR_0916_BOOTSTRAP_PROVISIONAL"
    assert at_0920["oi_change5m_interval_minutes"] == pytest.approx(235 / 60)
    assert at_0920["opening_context"]["provisional_quote_baseline"]["exchange_timestamp"] == f"{DAY}T09:16:05+05:30"
    assert at_0920["opening_context"]["provisional_quote_baseline"]["baseline_kind"] == "PROVISIONAL_FULL_QUOTE"
    assert later["oi_change5m_pct"] == 10
    assert result["opening_observer"]["provisional_quote_symbols"] == 1
    assert result["opening_observer"]["state"] == "PROVISIONAL_QUOTE_AVAILABLE"


@pytest.mark.parametrize("changes", [
    {"contract": "TEST26NOVFUT"}, {"exchange_timestamp": f"{DAY}T09:16:16+05:30"},
    {"received_at": f"{DAY}T09:20:00+05:30"}, {"received_at": f"{DAY}T09:16:25+05:30"},
    {"oi": 0}, {"source": "KITE_HISTORICAL_MINUTE"}, {"exact_first_minute_candle": True},
])
def test_invalid_provisional_quote_never_bootstraps(tmp_path, changes):
    enable(tmp_path)
    live_quote(tmp_path, **changes)
    result = apply(tmp_path)
    assert row(result, 1)["oi_change5m_pct"] is None
    assert row(result, 1)["opening_context"].get("oi_change_since_0916_pct") is None


def test_recovered_minute_is_preferred_to_provisional_quote_for_history(tmp_path):
    enable(tmp_path)
    live_quote(tmp_path, oi=1000)
    history(tmp_path)
    result = apply(tmp_path)
    at_0920 = row(result, 1)
    assert at_0920["oi_change_pct"] == pytest.approx((1100 / 1050 - 1) * 100)
    assert at_0920["oi_change5m_source"] == "FIRST_MINUTE_0916_BOOTSTRAP_4M"
    assert at_0920["opening_context"]["provisional_quote_baseline"]["oi"] == 1000


@pytest.mark.parametrize("changes", [{"oi_source": "GAP_FILLED"}, {"gap_filled": True}, {"oi": 0}, {"contract": "TEST26NOVFUT"}])
def test_provisional_quote_rejects_unverified_completed_bar(tmp_path, changes):
    enable(tmp_path)
    live_quote(tmp_path)
    base = market()
    row(base, 1).update(changes)
    result = apply(tmp_path, base)
    assert row(result, 1).get("oi_change5m_source") != "LIVE_QUOTE_NEAR_0916_BOOTSTRAP_PROVISIONAL"


def test_archived_first_minute_is_exposed_without_regular_0920_row(tmp_path):
    enable(tmp_path)
    history(tmp_path)
    base = market()
    base["slots"][1]["rows"] = []
    result = apply(tmp_path, base)
    assert result["slots"][1]["rows"] == []
    assert result["opening_first_minutes"][0]["first_minute"]["oi"] == 1050
    assert result["opening_first_minutes"][0]["first_minute"]["oi_day_change_pct"] == pytest.approx((1050 / 900 - 1) * 100)


def test_first_minute_day_change_requires_same_contract_previous_oi(tmp_path):
    enable(tmp_path)
    history(tmp_path)
    base = market()
    row(base, 1)["previous_oi"] = None
    result = apply(tmp_path, base)
    first = row(result, 1)["opening_context"]["first_minute"]
    assert first["oi"] == 1050
    assert first["previous_oi"] is None
    assert first["oi_day_change_pct"] is None


def test_first_minute_oi_comparison_rejects_wrong_contract_missing_oi_and_wrong_boundary(tmp_path):
    enable(tmp_path)
    history(tmp_path)
    result = apply(tmp_path)
    first = result["opening_first_minutes"][0]["first_minute"]
    completed = row(result, 1)
    boundary = f"{DAY}T09:20:00+05:30"
    assert compare_first_minute_oi_to_0920(first, completed, boundary=boundary)["oi_change_since_first_minute_pct"] == pytest.approx((1100 / 1050 - 1) * 100)
    assert compare_first_minute_oi_to_0920(first, {**completed, "contract": "TEST26NOVFUT"}, boundary=boundary) is None
    assert compare_first_minute_oi_to_0920(first, {**completed, "oi": None}, boundary=boundary) is None
    assert compare_first_minute_oi_to_0920(first, {**completed, "oi_source": "GAP_FILLED"}, boundary=boundary) is None
    assert compare_first_minute_oi_to_0920(first, completed, boundary=f"{DAY}T09:25:00+05:30") is None
    assert compare_first_minute_oi_to_0920({**first, "gap_filled": True}, completed, boundary=boundary) is None
    realtime_first = {**first, "acquired_at": f"{DAY}T09:16:10+05:30"}
    assert compare_first_minute_oi_to_0920(realtime_first, completed, boundary=boundary)["available_by_0920"] is True
    assert compare_first_minute_oi_to_0920(first, completed, boundary=boundary)["available_by_0920"] is False


def test_wrong_contract_history_cannot_supply_context(tmp_path):
    enable(tmp_path)
    history(tmp_path, contract="TEST26NOVFUT")
    assert row(apply(tmp_path), 1)["opening_context"]["first_minute_oi"] is None


def test_dated_ltm_future_maps_to_known_ltim_cash_symbol_without_generic_aliasing(tmp_path):
    enable(tmp_path)
    contract = "LTM26AUGFUT"
    write_json(tmp_path, f"historical/{DAY}/{contract}.json", {
        "session_date": DAY, "symbol": "LTM", "contract": contract, "source": "KITE_HISTORICAL_MINUTE",
        "acquired_at": f"{DAY}T19:00:00+05:30",
        "candles": [{"candle_start": f"{DAY}T09:15:00+05:30", "candle_end": f"{DAY}T09:16:00+05:30",
                     "open": 100, "high": 101, "low": 99, "close": 100, "volume": 20, "oi": 1050}]})
    base = market()
    for slot in base["slots"]:
        slot["rows"][0].update(symbol="LTIM", contract=contract)
    result = apply(tmp_path, base)
    assert row(result, 1)["oi_change5m_pct"] == pytest.approx((1100 / 1050 - 1) * 100)
    assert result["opening_first_minutes"][0]["symbol"] == "LTIM"
    for slot in base["slots"]:
        slot["rows"][0]["symbol"] = "OTHER"
    assert apply(tmp_path, base)["opening_first_minutes"] == []


def test_history_collector_filename_roundtrips_ampersand_contract(tmp_path):
    from dashboard_flow_observer_history import history_path
    enable(tmp_path)
    contract = "M&M26OCTFUT"
    payload = {"session_date": DAY, "symbol": "M&M", "contract": contract, "source": "KITE_HISTORICAL_MINUTE", "acquired_at": f"{DAY}T19:00:00+05:30", "candles": [{"candle_start": f"{DAY}T09:15:00+05:30", "candle_end": f"{DAY}T09:16:00+05:30", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 20, "oi": 1050}]}
    write_json(tmp_path, history_path(DAY, contract), payload)
    base = market()
    for slot in base["slots"]:
        slot["rows"][0].update(symbol="M&M", contract=contract)
    result = apply(tmp_path, base)
    assert row(result)["opening_context"]["first_minute_oi"] is None
    assert row(result, 1)["opening_context"]["first_minute_oi"] == 1050


def test_zero_baseline_is_not_infinity_and_missing_previous_oi_not_fabricated(tmp_path):
    enable(tmp_path)
    append_events(tmp_path, DAY, [event("09:10:00", oi=0), event()])
    base = market()
    row(base)["previous_oi"] = None
    result = apply(tmp_path, base)
    assert row(result)["oi_day_change_pct"] is None
    assert row(result)["opening_context"]["oi_preopen_change_pct"] is None


def test_config_malformed_and_nonboolean_are_disabled(tmp_path):
    write_json(tmp_path, "config.json", {"opening_enabled": "true"})
    assert not opening_enabled(tmp_path)
    (observer_root(tmp_path) / "config.json").write_text("{broken", encoding="utf-8")
    assert not opening_enabled(tmp_path)


def test_collector_status_uses_actual_named_path_and_filters_requested_session(tmp_path):
    enable(tmp_path)
    write_json(tmp_path, "status/opening_observer.json", {"session_date": DAY, "state": "COLLECTING", "event_count": 20, "api_key": "never expose"})
    write_json(tmp_path, "status/opening_history.json", {"session_date": DAY, "state": "COMPLETE", "message": "Recovered minute context"})
    summary = apply(tmp_path)["opening_observer"]
    assert summary["collector"]["state"] == "COLLECTING"
    assert summary["collector"]["event_count"] == 20
    assert "api_key" not in summary["collector"]
    assert summary["history_collector"]["state"] == "COMPLETE"
    write_json(tmp_path, "status/opening_observer.json", {"session_date": "2026-10-09", "state": "OLD"})
    write_json(tmp_path, "status/opening_history.json", {"session_date": "2026-10-09", "state": "OLD"})
    summary = apply(tmp_path)["opening_observer"]
    assert "collector" not in summary
    assert "history_collector" not in summary


def test_dated_status_is_safe_fallback_for_historical_session(tmp_path):
    enable(tmp_path)
    write_json(tmp_path, "status/opening_observer.json", {"session_date": "2026-10-13", "state": "NEWER"})
    write_json(tmp_path, f"status/{DAY}.json", {"session_date": DAY, "state": "COMPLETE"})
    assert apply(tmp_path)["opening_observer"]["collector"]["state"] == "COMPLETE"


def test_malformed_status_cannot_suppress_valid_observations(tmp_path):
    enable(tmp_path)
    append_events(tmp_path, DAY, [event()])
    write_json(tmp_path, "status/opening_observer.json", {})
    (observer_root(tmp_path) / "status/opening_observer.json").write_text("{bad", encoding="utf-8")
    result = apply(tmp_path)
    assert row(result)["oi"] == 1000
    assert "OBSERVER_STATUS_READ_FAILED" in result["opening_observer"]["audit_flags"]


def test_base_cache_is_not_mutated_and_observer_updates_are_immediate(tmp_path):
    universe = tmp_path / f"fno_oi/universe/near_month_{DAY}.parquet"
    universe.parent.mkdir(parents=True)
    pd.DataFrame([{"underlying": "TEST", "tradingsymbol": CONTRACT, "equity_symbol": "TEST", "is_index_future": False}]).to_parquet(universe, index=False)
    future = tmp_path / f"fno_oi/raw_contracts_5m/{CONTRACT}_5minute.parquet"
    future.parent.mkdir(parents=True)
    pd.DataFrame([{"timestamp": pd.Timestamp("2026-10-09 15:30", tz="Asia/Kolkata"), "candle_start": pd.Timestamp("2026-10-09 15:25", tz="Asia/Kolkata"), "open": 100, "high": 101, "low": 99, "close": 100, "volume": 20, "oi": 900, "quality_state": "VALID"}]).to_parquet(future, index=False)
    before = future.read_bytes()
    now = datetime.fromisoformat(f"{DAY}T09:15:01+05:30")
    enable(tmp_path)
    assert load_session_market(tmp_path, DAY, now=now)["slots"][0]["rows"] == []
    append_events(tmp_path, DAY, [event()])
    result = load_session_market(tmp_path, DAY, now=now)
    assert row(result)["oi"] == 1000
    assert row(result)["oi_day_change_pct"] == pytest.approx((1000 / 900 - 1) * 100)
    write_json(tmp_path, "config.json", {"opening_enabled": False})
    assert load_session_market(tmp_path, DAY, now=now)["slots"][0]["rows"] == []
    assert future.read_bytes() == before
