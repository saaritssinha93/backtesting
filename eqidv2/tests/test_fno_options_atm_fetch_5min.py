from __future__ import annotations

from argparse import Namespace
from datetime import date

import pandas as pd
import pytest

import fno_options_atm_fetch_5min as opt


def master_frame() -> pd.DataFrame:
    rows = []
    token = 100
    for symbol in ("RELIANCE", "INFY", "NIFTY"):
        for expiry in ("2026-09-29", "2026-10-06"):
            for leg in ("CE", "PE"):
                for strike in (90.0, 100.0, 110.0):
                    token += 1
                    rows.append(
                        {
                            "instrument_token": token,
                            "exchange_token": token + 1000,
                            "tradingsymbol": f"{symbol}{expiry.replace('-', '')}{int(strike)}{leg}",
                            "name": symbol,
                            "last_price": 0.0,
                            "expiry": pd.Timestamp(expiry),
                            "strike": strike,
                            "tick_size": 0.05,
                            "lot_size": 250,
                            "instrument_type": leg,
                            "segment": "NFO-OPT",
                            "exchange": "NFO",
                            "underlying": symbol,
                            "master_date": pd.Timestamp("2026-09-04"),
                            "unique_key": f"NFO:{symbol}:{token}",
                        }
                    )
    return pd.DataFrame(rows)


def contract() -> dict:
    return {
        "underlying": "RELIANCE",
        "tradingsymbol": "RELIANCE20260929100CE",
        "instrument_token": 101,
        "exchange_token": 1101,
        "expiry": pd.Timestamp("2026-09-29"),
        "strike": 100.0,
        "instrument_type": "CE",
        "lot_size": 250,
        "tick_size": 0.05,
        "strike_offset": 0,
        "spot_price": 104.0,
        "spot_source": "EQUITY_5M",
        "spot_slot": pd.Timestamp("2026-09-04 09:20", tz=opt.common.IST),
        "atm_distance": 4.0,
        "expiry_policy": "monthly",
        "mapping_status": "MAPPED_ATM",
    }


def candles(interval: str) -> list[dict]:
    frequency = "5min" if interval == "5minute" else "1min"
    periods = 1 if interval == "5minute" else 5
    start = "2026-09-04 09:15" if interval == "5minute" else "2026-09-04 09:15"
    return [
        {"date": stamp, "open": 10, "high": 12, "low": 9, "close": 11, "volume": 5, "oi": 100}
        for stamp in pd.date_range(start, periods=periods, freq=frequency, tz=opt.common.IST)
    ]


def args(**updates) -> Namespace:
    values = dict(
        mode="historical", slot="", session_date="2026-09-04", from_date="", through_date="",
        underlyings="RELIANCE,INFY", include_index_options=False, expiry_policy="monthly",
        strike_window=0, spot_source="auto", intervals=("5minute", "minute"), workers_per_app=2,
        writer_workers=8, allow_high_writer_count=False, request_interval_sec=0.34,
        timeout_sec=8.0, max_retries=1, slot_retry_attempts=2, max_apps=8,
        boundary_buffer_sec=3.0, min_coverage=0.99,
        allow_non_trading_day=False, dry_run=True,
    )
    values.update(updates)
    return Namespace(**values)


def test_intervals_default_to_both():
    assert opt.build_parser().parse_args([]).intervals == ("5minute", "minute")


@pytest.mark.parametrize("text,expected", [("5m", ("5minute",)), ("1m", ("minute",)), ("both", ("5minute", "minute"))])
def test_parse_intervals(text, expected):
    assert opt.parse_intervals(text) == expected


def test_atm_tie_breaks_to_lower_strike(monkeypatch):
    monkeypatch.setattr(opt, "load_spot", lambda *unused: (105.0, "EQUITY_5M"))
    result = opt.resolve_atm_contracts(master_frame(), ["RELIANCE"], "2026-09-04 09:20+05:30")
    assert set(result["strike"]) == {100.0}
    assert set(result["instrument_type"]) == {"CE", "PE"}


def test_strike_window_is_symmetric(monkeypatch):
    monkeypatch.setattr(opt, "load_spot", lambda *unused: (100.0, "EQUITY_5M"))
    result = opt.resolve_atm_contracts(master_frame(), ["RELIANCE"], "2026-09-04 09:20+05:30", strike_window=1)
    assert len(result) == 6
    assert set(result["strike_offset"]) == {-1, 0, 1}


def test_no_spot_fails_closed(monkeypatch):
    def fail(*unused):
        raise ValueError("missing")
    monkeypatch.setattr(opt, "load_spot", fail)
    result = opt.resolve_atm_contracts(master_frame(), ["INFY"], "2026-09-04 09:20+05:30")
    assert len(result) == 2
    assert result["mapping_status"].eq("NO_SPOT_PRICE_FOR_SLOT").all()


def test_missing_expiry_is_not_substituted(monkeypatch):
    monkeypatch.setattr(opt, "load_spot", lambda *unused: (100.0, "EQUITY_5M"))
    old = master_frame().assign(expiry=pd.Timestamp("2026-08-25"))
    result = opt.resolve_atm_contracts(old, ["RELIANCE"], "2026-09-04 09:20+05:30")
    assert result["mapping_status"].str.contains("OPTION").all()
    assert result["instrument_token"].isna().all()


def test_5minute_normalization_has_one_exact_end():
    result = opt.normalize_option_candles(
        candles("5minute"), contract(), "5minute", "2026-09-04 09:20+05:30",
        fetched_at=opt.common.now_ist(), master_date=date(2026, 9, 4), master_sha256="abc",
    )
    assert len(result) == 1
    assert opt._slot(result.iloc[0]["timestamp"]) == opt._slot("2026-09-04 09:20+05:30")
    assert tuple(result.columns) == opt.OPTIONS_RAW_COLUMNS


def test_1minute_normalization_has_five_exact_ends():
    result = opt.normalize_option_candles(
        candles("minute"), contract(), "minute", "2026-09-04 09:20+05:30",
        fetched_at=opt.common.now_ist(), master_date=date(2026, 9, 4), master_sha256="abc",
    )
    assert len(result) == 5
    assert result["candle_interval"].eq("minute").all()
    assert result["timestamp"].nunique() == 5


def test_invalid_ohlc_is_rejected():
    records = candles("5minute")
    records[0]["high"] = 8
    result = opt.normalize_option_candles(
        records, contract(), "5minute", "2026-09-04 09:20+05:30",
        fetched_at=opt.common.now_ist(), master_date=date(2026, 9, 4), master_sha256="abc",
    )
    assert result.iloc[0]["quality_state"] == "INVALID_OHLC"


def test_projection_counts_both_intervals():
    result = opt.project_requests(12, 2, args(), 8)
    assert result["initial_requests"] == 24
    assert result["max_requests"] == 72
    assert result["accepted"] is True


def test_projection_rejects_over_budget():
    result = opt.project_requests(1260, 2, args(), 8)
    assert result["accepted"] is False


def test_map_is_idempotent_and_detects_drift(tmp_path, monkeypatch):
    monkeypatch.setattr(opt, "OPTIONS_MAP_DIR", tmp_path)
    frame = pd.DataFrame([contract()])
    path, first = opt.persist_contract_map(frame, "2026-09-04 09:20+05:30", dry_run=False)
    assert path.exists()
    assert opt.persist_contract_map(frame, "2026-09-04 09:20+05:30", dry_run=False)[1] == first
    changed = frame.copy()
    changed.loc[0, "instrument_token"] = 999
    with pytest.raises(opt.ContractSetDriftError):
        opt.persist_contract_map(changed, "2026-09-04 09:20+05:30", dry_run=False)


def test_unresolved_map_can_upgrade_without_weakening_mapped_drift_guard(tmp_path, monkeypatch):
    monkeypatch.setattr(opt, "OPTIONS_MAP_DIR", tmp_path)
    mapped = pd.DataFrame([contract()])
    unresolved = mapped.copy()
    unresolved.loc[0, "mapping_status"] = "NO_SPOT_PRICE_FOR_SLOT"
    unresolved.loc[0, "tradingsymbol"] = ""
    unresolved.loc[0, "instrument_token"] = pd.NA
    unresolved.loc[0, "exchange_token"] = pd.NA
    unresolved.loc[0, "expiry"] = pd.NaT
    unresolved.loc[0, "strike"] = float("nan")

    path, old_digest = opt.persist_contract_map(
        unresolved, "2026-09-04 09:20+05:30", dry_run=False
    )
    _, new_digest = opt.persist_contract_map(
        mapped, "2026-09-04 09:20+05:30", dry_run=False
    )

    assert new_digest != old_digest
    repaired = pd.read_parquet(path)
    assert repaired.iloc[0]["mapping_status"] == "MAPPED_ATM"
    assert repaired.iloc[0]["tradingsymbol"] == contract()["tradingsymbol"]


def test_dry_run_makes_no_historical_call_or_write(monkeypatch):
    monkeypatch.setattr(opt, "load_cash_marker", lambda unused: {"source": "final", "complete": True})
    monkeypatch.setattr(opt, "load_spot", lambda *unused: (100.0, "EQUITY_5M"))
    monkeypatch.setattr(opt, "persist_contract_map", lambda frame, slot, dry_run: (opt.Path("map.parquet"), "maphash"))
    monkeypatch.setattr(opt, "fetch_contracts", lambda *a, **k: pytest.fail("historical fetch called"))
    marker = opt.run_slot("2026-09-04 09:20+05:30", master_frame(), "masterhash", [], args())
    assert marker["historical_calls"] == 0
    assert marker["writes"] == 0
    assert marker["intervals"] == ["5minute", "minute"]


def test_index_options_are_excluded_by_default():
    selected = opt._selected_underlyings(master_frame(), args(underlyings=""))
    assert "NIFTY" not in selected
    assert {"RELIANCE", "INFY"}.issubset(selected)


def test_writer_cap_requires_explicit_override():
    candidate = args(writer_workers=9)
    with pytest.raises(ValueError, match="capped"):
        opt.validate_args(candidate)
    candidate.allow_high_writer_count = True
    opt.validate_args(candidate)


def test_contract_paths_separate_intervals(tmp_path, monkeypatch):
    monkeypatch.setattr(opt, "RAW_OPTIONS_5M_DIR", tmp_path / "five")
    monkeypatch.setattr(opt, "RAW_OPTIONS_1M_DIR", tmp_path / "one")
    assert "five" in str(opt.option_contract_path("5minute", "ABC"))
    assert "one" in str(opt.option_contract_path("minute", "ABC"))


def test_readback_corruption_fails_loudly(tmp_path, monkeypatch):
    incoming = opt.normalize_option_candles(
        candles("5minute"), contract(), "5minute", "2026-09-04 09:20+05:30",
        fetched_at=opt.common.now_ist(), master_date=date(2026, 9, 4), master_sha256="abc",
    )

    def corrupt(frame, path):
        broken = frame.copy()
        broken.loc[0, "close"] = 999.0
        broken.to_parquet(path, index=False)

    monkeypatch.setattr(opt.common, "atomic_write_parquet", corrupt)
    with pytest.raises(IOError, match="readback"):
        opt.persist_option_rows(tmp_path / "contract.parquet", incoming)


def test_archive_readback_treats_equivalent_numeric_dtypes_as_equal():
    left = opt.normalize_option_candles(
        candles("5minute"), contract(), "5minute", "2026-09-04 09:20+05:30",
        fetched_at=opt.common.now_ist(), master_date=date(2026, 9, 4), master_sha256="abc",
    )
    right = left.copy()
    for column in opt.OPTIONS_NUMERIC_COLUMNS:
        right[column] = pd.to_numeric(right[column], errors="coerce").astype(float)

    assert opt._archive_records(left) == opt._archive_records(right)


def test_auth_failure_fails_over_and_completes(monkeypatch):
    bad_client, good_client = object(), object()
    bad = opt.AppLane("app1", [bad_client], 0.34)
    good = opt.AppLane("app2", [], 0.34)
    good.next_client = lambda: good_client
    monkeypatch.setattr(opt.common, "publish_heartbeat", lambda *a, **k: None)

    def history(lane, client, *unused, **kwargs):
        if lane.app_name == "app1":
            raise RuntimeError("TokenException: session expired")
        return candles(kwargs["interval"])

    monkeypatch.setattr(opt, "_historical_call", history)
    monkeypatch.setattr(opt, "persist_option_rows", lambda path, frame: len(frame))
    result = opt.fetch_contracts(
        pd.DataFrame([contract()]), ("5minute",), "2026-09-04 09:20+05:30",
        [bad, good], args(dry_run=False), master_date=date(2026, 9, 4), master_sha256="abc",
    )
    assert result[0]["state"] == "VERIFIED"
    assert result[0]["apps_attempted"] == "app1|app2"


def test_429_retries_same_lane_with_backoff(monkeypatch):
    client = object()
    lane = opt.AppLane("app1", [client], 0.34)
    calls, sleeps = [], []
    monkeypatch.setattr(opt.common, "publish_heartbeat", lambda *a, **k: None)

    def history(*unused, **kwargs):
        calls.append(kwargs["interval"])
        if len(calls) == 1:
            raise RuntimeError("429 too many requests")
        return candles(kwargs["interval"])

    monkeypatch.setattr(opt, "_historical_call", history)
    monkeypatch.setattr(opt.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(opt, "persist_option_rows", lambda path, frame: len(frame))
    result = opt.fetch_contracts(
        pd.DataFrame([contract()]), ("5minute",), "2026-09-04 09:20+05:30",
        [lane], args(dry_run=False), master_date=date(2026, 9, 4), master_sha256="abc",
    )
    assert result[0]["state"] == "VERIFIED"
    assert result[0]["apps_attempted"] == "app1|app1"
    assert sleeps == [2.0]


def test_marker_incomplete_below_minimum_coverage(tmp_path, monkeypatch):
    plan = pd.DataFrame([contract()])
    monkeypatch.setattr(opt, "load_cash_marker", lambda unused: {})
    monkeypatch.setattr(opt, "_selected_underlyings", lambda *unused: ["RELIANCE"])
    monkeypatch.setattr(opt, "resolve_atm_contracts", lambda *a, **k: plan)
    monkeypatch.setattr(opt, "persist_contract_map", lambda *a, **k: (tmp_path / "map.parquet", "maphash"))
    monkeypatch.setattr(opt, "fetch_contracts", lambda *a, **k: [{
        "tradingsymbol": "X", "underlying": "RELIANCE", "interval": "5minute",
        "state": "FAILED", "rows_written": 0, "apps_attempted": "app1", "error": "empty",
    }])
    monkeypatch.setattr(opt, "option_marker_path", lambda unused: tmp_path / "marker.json")
    monkeypatch.setattr(opt, "write_latest_report", lambda unused: tmp_path / "latest.md")
    monkeypatch.setattr(opt.common, "publish_status", lambda *a, **k: None)
    monkeypatch.setattr(opt.common, "publish_heartbeat", lambda *a, **k: None)
    marker = opt.run_slot(
        "2026-09-04 09:20+05:30", master_frame(), "abc", [opt.AppLane("app1", [object()], 0.34)],
        args(dry_run=False, intervals=("5minute",), min_coverage=0.99),
    )
    assert marker["complete"] is False
    assert marker["coverage"] == 0.0


def test_module_contains_no_order_api_calls():
    source = opt.Path(opt.__file__).read_text(encoding="utf-8")
    assert "place_order" not in source
    assert "modify_order" not in source
    assert "cancel_order" not in source
