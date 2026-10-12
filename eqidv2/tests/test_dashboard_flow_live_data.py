from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

import dashboard_flow_live_data as data


def save(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def cash(day, slot, close, *, open=None, **extra):
    return {"date": pd.Timestamp(f"{day} {slot}", tz=data.IST), "open": open or close, "high": max(open or close, close) + 1, "low": min(open or close, close) - 1, "close": close, "volume": 10, "source_1m_count": 5, "gap_filled": 0, "opening_snapshot": False, "provisional_stale": 0, **extra}


def oi(day, slot, value, **extra):
    stamp = pd.Timestamp(f"{day} {slot}", tz=data.IST)
    return {"timestamp": stamp, "candle_start": stamp - pd.Timedelta(minutes=5), "oi": value, "quality_state": "VALID", **extra}


@pytest.fixture
def market(tmp_path):
    universe = [{"underlying": "TEST", "tradingsymbol": "TEST26OCTFUT", "equity_symbol": "TEST", "is_index_future": False}]
    for day in ("2026-10-08", "2026-10-09"):
        save(tmp_path / f"fno_oi/universe/near_month_{day}.parquet", universe)
    cash_path = tmp_path / "stocks_indicators_5min_eq_live/TEST_stocks_indicators_5min.parquet"
    oi_path = tmp_path / "fno_oi/raw_contracts_5m/TEST26OCTFUT_5minute.parquet"
    save(cash_path, [cash("2026-10-08", "15:30", 100), cash("2026-10-09", "09:15", 999, opening_snapshot=True), cash("2026-10-09", "09:20", 105, open=102), cash("2026-10-09", "09:25", 110)])
    save(oi_path, [oi("2026-10-08", "15:30", 1000), oi("2026-10-09", "09:20", 1100), oi("2026-10-09", "09:25", 1210)])
    return tmp_path, cash_path, oi_path


def load(root, at="10:00", day="2026-10-09"):
    return data.load_session_market(root, day, now=pd.Timestamp(f"2026-10-09 {at}", tz=data.IST))


def row(payload, time):
    return next(slot for slot in payload["slots"] if slot["time"] == time)["rows"][0]


def test_price_and_oi_use_distinct_daily_and_exact_adjacent_baselines(market):
    root, _, _ = market
    payload = load(root)
    point = row(payload, "09:25")
    assert point["price_pct"] == pytest.approx(10)
    assert point["change5m_pct"] == pytest.approx((110 / 105 - 1) * 100)
    assert point["oi_day_change_pct"] == pytest.approx(21)
    assert point["oi_change5m_pct"] == pytest.approx(10)
    assert point["contract"] == "TEST26OCTFUT"
    assert payload["previous_session"] == "2026-10-08"
    assert len(payload["slots"]) == 76


def test_open_is_reconstructed_after_bar_completion_not_snapshot_close(market):
    root, _, _ = market
    early = load(root, "09:19")
    assert early["slots"][0]["rows"] == []
    assert early["slots"][1]["state"] == "FUTURE"
    payload = load(root, "09:20")
    point = row(payload, "09:15")
    assert point["price"] == 102
    assert point["price_pct"] == pytest.approx(2)
    assert point["bar_kind"] == "RECONSTRUCTED_SESSION_OPEN"
    assert point["oi"] is None
    assert point["change5m_pct"] is None
    assert row(payload, "09:20")["change5m_pct"] == pytest.approx((105 / 102 - 1) * 100)
    assert row(payload, "09:20")["oi_change5m_pct"] is None


def test_0915_snapshot_copy_does_not_reject_genuine_first_completed_bar(market):
    root, cash_path, _ = market
    save(cash_path, [cash("2026-10-08", "15:30", 100), cash("2026-10-09", "09:15", 105, open=102, opening_snapshot=True, source_1m_count=0), cash("2026-10-09", "09:20", 105, open=102)])
    payload = load(root)
    assert row(payload, "09:15")["price"] == 102
    assert row(payload, "09:20")["price"] == 105


def test_gap_filled_previous_close_is_only_a_labelled_proxy(market):
    root, cash_path, _ = market
    save(cash_path, [cash("2026-10-08", "15:15", 99), cash("2026-10-08", "15:30", 99, gap_filled=1), cash("2026-10-09", "09:20", 105, open=102)])
    point = row(load(root), "09:20")
    assert point["previous_close"] is None
    assert point["price_pct"] is None
    assert point["previous_close_proxy"] == 99
    assert "15:15:00" in point["previous_close_proxy_time"]
    assert point["price_proxy_pct"] == pytest.approx((105 / 99 - 1) * 100)


def test_exact_daily_close_replaces_missing_intraday_baseline_not_stale_daily(market):
    root, cash_path, _ = market
    save(cash_path, [cash("2026-10-09", "09:20", 105, open=102)])
    daily = root / "stocks_indicators_daily_eq/TEST_stocks_indicators_daily.parquet"
    save(daily, [cash("2026-10-07", "00:00", 500)])
    assert row(load(root), "09:20")["previous_close"] is None
    save(daily, [cash("2026-10-08", "00:00", 101)])
    point = row(load(root), "09:20")
    assert point["previous_close"] == 101
    assert point["previous_close_source"] == "DAILY_CASH_CLOSE"


def test_missing_adjacent_bar_never_forward_fills_either_delta(market):
    root, cash_path, oi_path = market
    save(cash_path, [cash("2026-10-08", "15:30", 100), cash("2026-10-09", "09:20", 105), cash("2026-10-09", "09:30", 110)])
    save(oi_path, [oi("2026-10-08", "15:30", 1000), oi("2026-10-09", "09:20", 1100), oi("2026-10-09", "09:30", 1210)])
    payload = load(root)
    assert payload["slots"][2]["rows"] == []
    point = row(payload, "09:30")
    assert point["change5m_pct"] is None
    assert point["oi_change5m_pct"] is None
    assert point["price_pct"] == pytest.approx(10)


def test_repaired_file_invalidates_cached_symbol_without_waiting(market):
    root, cash_path, _ = market
    assert row(load(root), "09:25")["price"] == 110
    save(cash_path, [cash("2026-10-08", "15:30", 100), cash("2026-10-09", "09:20", 105), cash("2026-10-09", "09:25", 120)])
    assert row(load(root), "09:25")["price"] == 120


def test_historical_minute_source_requires_five_exact_minutes_and_has_priority(market):
    root, _, _ = market
    path = root / "stocks_indicators_1min_eq/TEST_stocks_indicators_1min.parquet"
    rows = [cash("2026-10-09", f"09:{minute}", 200 + minute) for minute in range(16, 25)]
    save(path, rows)
    payload = data.load_session_market(root, "2026-10-09", now=pd.Timestamp("2026-10-12 10:00", tz=data.IST))
    assert row(payload, "09:20")["price"] == 220
    assert row(payload, "09:20")["price_source"] == "HISTORICAL_CASH_1M_AGGREGATED"
    # Only four minutes for 09:25: the genuine saved five-minute bar supplies it.
    assert row(payload, "09:25")["price"] == 110


def test_bad_cash_quality_and_bad_future_timestamp_are_not_rankable(market):
    root, cash_path, oi_path = market
    save(cash_path, [cash("2026-10-08", "15:30", 100), cash("2026-10-09", "09:20", 105, provisional_stale=1)])
    save(oi_path, [oi("2026-10-09", "09:20", 1100, candle_start=pd.Timestamp("2026-10-09 09:10", tz=data.IST))])
    payload = load(root)
    assert payload["slots"][1]["rows"] == []


def test_zero_oi_is_observed_but_not_a_valid_percentage_denominator(market):
    root, _, oi_path = market
    save(oi_path, [oi("2026-10-09", "09:20", 0), oi("2026-10-09", "09:25", 100)])
    payload = load(root)
    assert row(payload, "09:20")["oi"] == 0
    assert row(payload, "09:25")["oi_change5m_pct"] is None


def test_registry_fallback_is_disclosed_and_never_uses_expired_or_index_contract(tmp_path):
    save(tmp_path / "fno_oi/universe/contract_registry.parquet", [
        {"underlying": "TEST", "tradingsymbol": "TEST26SEPFUT", "expiry": "2026-09-29", "is_index_future": False},
        {"underlying": "TEST", "tradingsymbol": "TEST26OCTFUT", "expiry": "2026-10-27", "is_index_future": False},
        {"underlying": "TEST", "tradingsymbol": "TEST26NOVFUT", "expiry": "2026-11-23", "is_index_future": False},
        {"underlying": "NIFTY", "tradingsymbol": "NIFTY26OCTFUT", "expiry": "2026-10-27", "is_index_future": True},
    ])
    save(tmp_path / "stocks_indicators_5min_eq_live/TEST_stocks_indicators_5min.parquet", [cash("2026-10-09", "09:20", 100)])
    payload = load(tmp_path)
    assert payload["universe_count"] == 1
    assert payload["universe_state"] == "RECONSTRUCTED_NEAR_MONTH_REGISTRY"
    assert row(payload, "09:20")["contract"] == "TEST26OCTFUT"
    assert "historical membership may differ" in payload["warnings"][0]


def test_invalid_date_cannot_become_filesystem_path(tmp_path):
    with pytest.raises(ValueError):
        data.load_session_market(tmp_path, "../2026-10-09")


def test_special_character_contract_uses_persisted_safe_filename(tmp_path):
    save(tmp_path / "fno_oi/universe/near_month_2026-10-09.parquet", [{"underlying": "M&M", "equity_symbol": "M&M", "tradingsymbol": "M&M26OCTFUT", "is_index_future": False}])
    save(tmp_path / "stocks_indicators_5min_eq_live/M&M_stocks_indicators_5min.parquet", [cash("2026-10-09", "09:20", 100)])
    save(tmp_path / f"fno_oi/raw_contracts_5m/{data._contract_stem('M&M26OCTFUT')}_5minute.parquet", [oi("2026-10-09", "09:20", 1000)])
    point = row(load(tmp_path), "09:20")
    assert point["symbol"] == "M&M"
    assert point["oi"] == 1000
