"""The optional 09:20 research export must not turn opening evidence into a setup."""
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from dashboard_flow_observer_store import write_json
from fno_v13_v10_g_3_contract import SIGNAL_TO_CONFIRMATION
from research import g3_0920_feature_export as export


DAY = date(2026, 10, 12)
CONTRACT = "SUZLON26OCTFUT"
EXPIRY = "2026-10-29"


def _minute(day, clock, price, fetched):
    return {"date": f"{day}T{clock}:00+05:30", "open": price, "high": price + .10,
            "low": price - .10, "close": price + .05, "volume": 1000,
            "quality_state": "VALID", "source": "KITE_HISTORICAL_1M", "fetched_at": fetched}


def _fixture(root: Path, *, opening_oi=1000, first_acquired="2026-10-12T09:16:10+05:30",
             cash_fetch="2026-10-12T09:20:12+05:30", future_contract=CONTRACT,
             prior_quality="VALID"):
    universe_dir = root / "fno_oi/universe"
    universe_dir.mkdir(parents=True)
    universe = pd.DataFrame([
        {"master_date": str(DAY), "exchange": "NFO", "underlying": "SUZLON",
         "tradingsymbol": CONTRACT, "futures_tradingsymbol": CONTRACT,
         "instrument_token": 10, "futures_instrument_token": 10,
         "equity_symbol": "SUZLON", "equity_instrument_token": 20,
         "expiry": EXPIRY, "is_index_future": False},
        {"master_date": str(DAY), "exchange": "NFO", "underlying": "NIFTY",
         "tradingsymbol": "NIFTY26OCTFUT", "futures_tradingsymbol": "NIFTY26OCTFUT",
         "instrument_token": 11, "futures_instrument_token": 11,
         "equity_symbol": "", "equity_instrument_token": 0,
         "expiry": EXPIRY, "is_index_future": True},
    ])
    universe.to_parquet(universe_dir / f"near_month_{DAY}.parquet", index=False)
    prior = datetime.fromisoformat("2026-10-09T14:56:00+05:30")
    minutes = [_minute("2026-10-09", (prior + timedelta(minutes=n)).strftime("%H:%M"),
                       99, "2026-10-09T15:30:00+05:30") for n in range(25)]
    for minute in minutes:
        minute["quality_state"] = prior_quality
    prices = [(100, 100.1, 99.9, 100.05), (100.1, 100.3, 100, 100.2),
              (100.2, 100.4, 100.1, 100.3), (100.3, 100.5, 100.2, 100.4),
              (100.4, 101.1, 100.3, 101), (101, 101.2, 100.9, 101.15)]
    for n, (opened, high, low, closed) in enumerate(prices):
        clock = (datetime.fromisoformat("2026-10-12T09:16:00+05:30") + timedelta(minutes=n)).strftime("%H:%M")
        minute = _minute(str(DAY), clock, opened, cash_fetch if n < 5 else "2026-10-12T09:21:12+05:30")
        minute.update(open=opened, high=high, low=low, close=closed)
        minutes.append(minute)
    cash = root / "stocks_indicators_1min_eq/SUZLON_stocks_indicators_1min.parquet"
    cash.parent.mkdir(parents=True)
    pd.DataFrame(minutes).to_parquet(cash, index=False)
    future_dir = root / "fno_oi/raw_contracts_5m"
    future_dir.mkdir(parents=True)
    pd.DataFrame([{"timestamp": "2026-10-12T09:20:00+05:30",
                   "candle_start": "2026-10-12T09:15:00+05:30",
                   "tradingsymbol": future_contract, "expiry": EXPIRY,
                   "open": 100, "high": 101, "low": 99, "close": 100.8,
                   "volume": 10000, "oi": 1100, "quality_state": "VALID",
                   "source": "kite_historical", "fetch_timestamp": "2026-10-12T09:20:20+05:30"}]).to_parquet(
                       future_dir / f"{CONTRACT}_5minute.parquet", index=False)
    pd.DataFrame([{"timestamp": "2026-10-12T09:20:00+05:30",
                   "candle_start": "2026-10-12T09:15:00+05:30",
                   "tradingsymbol": "NIFTY26OCTFUT", "expiry": EXPIRY,
                   "open": 20000, "high": 20050, "low": 19980, "close": 20020,
                   "volume": 50000, "oi": 10000, "quality_state": "VALID",
                   "source": "kite_historical", "fetch_timestamp": "2026-10-12T09:20:20+05:30"}]).to_parquet(
                       future_dir / "NIFTY26OCTFUT_5minute.parquet", index=False)
    write_json(root, f"historical/{DAY}/{CONTRACT}.json", {
        "symbol": "SUZLON", "session_date": str(DAY), "contract": CONTRACT,
        "source": "KITE_HISTORICAL_MINUTE", "acquired_at": first_acquired,
        "candles": [{"candle_start": "2026-10-12T09:15:00+05:30",
                     "candle_end": "2026-10-12T09:16:00+05:30",
                     "open": 100, "high": 100.2, "low": 99.9, "close": 100.05,
                     "volume": 1000, "oi": opening_oi}]})


def test_research_first_bar_and_oi_are_distinct_from_prior_close(tmp_path):
    _fixture(tmp_path)
    frame, coverage = export.build_day(DAY, data_root=tmp_path)
    assert len(frame) == coverage["stock_rows"] == 1
    row = frame.iloc[0]
    assert row.cash_0920_status == "VALID_COMPLETED_FIRST_BAR"
    assert row.cash_open == 100 and row.cash_close == 101
    assert row.price_first_5m_pct == pytest.approx(1)
    assert row.price_vs_previous_completed_5m_pct != pytest.approx(row.price_first_5m_pct)
    assert row.price_vs_verified_pdc_pct is None or pd.isna(row.price_vs_verified_pdc_pct)
    assert all(pd.notna(row[key]) for key in ("ema9", "ema20", "ema50", "volume_ratio",
                                               "signal_body_ratio", "signal_upper_wick_ratio", "traded_value"))
    assert row.oi_change_pct == pytest.approx(10)
    assert row.oi_change_interval_minutes == 4
    assert row.prev_oi_research_0916 == 1000 and row.oi_research_0920 == 1100
    assert row.confirmation_0921_status == "VALID_REAL_MINUTE"
    assert row.confirmation_asof_0920 == False
    assert row.v9_1m_volume_ratio == pytest.approx(1)
    assert row.nifty_first_bar_return_pct == pytest.approx(.1)
    assert row.signal_inputs_received_by_0921 == True
    assert row.complete_decision_evidence_by_0922 == True
    assert row.signal_end == "09:20" and row.active_g3_setup == False
    assert "09:20" not in SIGNAL_TO_CONFIRMATION
    assert min(SIGNAL_TO_CONFIRMATION) == "09:25"
    assert coverage["trading_logic_changed"] is False


@pytest.mark.parametrize(("change", "reason"), [
    ({"opening_oi": 0}, "INVALID_ZERO_FIRST_MINUTE_OI"),
    ({"future_contract": "OTHER26OCTFUT"}, "FUTURES_0920_DATED_CONTRACT_MISMATCH"),
])
def test_missing_or_mismatched_oi_remains_null_with_reason(tmp_path, change, reason):
    _fixture(tmp_path, **change)
    frame, coverage = export.build_day(DAY, data_root=tmp_path)
    row = frame.iloc[0]
    assert pd.isna(row.oi_change_pct)
    assert reason in row.missing_reasons
    assert coverage["valid_0916_oi_changes"] == 0


def test_late_historical_acquisition_is_not_live_equivalent(tmp_path):
    _fixture(tmp_path, first_acquired="2026-10-12T16:00:00+05:30",
             cash_fetch="2026-10-12T16:39:00+05:30")
    row = export.build_day(DAY, data_root=tmp_path)[0].iloc[0]
    assert row.oi_change_pct == pytest.approx(10)
    assert row.oi_baseline_acquired_by_0920 == False
    assert row.cash_first_bar_received_by_0921 == False
    assert row.signal_inputs_received_by_0921 == False
    assert row.complete_decision_evidence_by_0922 == False


def test_unspecified_historical_quality_preserves_warmup_indicators(tmp_path):
    valid_root, legacy_root = tmp_path / "valid", tmp_path / "legacy"
    _fixture(valid_root)
    _fixture(legacy_root, prior_quality=None)
    valid = export.build_day(DAY, data_root=valid_root)[0].iloc[0]
    legacy = export.build_day(DAY, data_root=legacy_root)[0].iloc[0]
    assert legacy.cash_0920_status == "VALID_COMPLETED_FIRST_BAR"
    for key in ("ema9", "ema20", "ema50", "volume_ratio"):
        assert legacy[key] == pytest.approx(valid[key])


def test_nullable_quality_flags_preserve_real_minutes_but_reject_true_flags(tmp_path):
    _fixture(tmp_path)
    cash = tmp_path / "stocks_indicators_1min_eq/SUZLON_stocks_indicators_1min.parquet"
    minute = pd.read_parquet(cash)
    minute["gap_filled"] = pd.Series([pd.NA] * len(minute), dtype="boolean")
    minute["is_synthetic"] = pd.Series([pd.NA] * len(minute), dtype="boolean")
    minute.to_parquet(cash, index=False)
    row = export.build_day(DAY, data_root=tmp_path)[0].iloc[0]
    assert row.cash_0920_status == "VALID_COMPLETED_FIRST_BAR"
    assert row.confirmation_0921_status == "VALID_REAL_MINUTE"
    assert row.price_first_5m_pct == pytest.approx(1)
    minute.loc[minute.date.astype(str).str.contains("2026-10-12T09:18"), "gap_filled"] = True
    minute.to_parquet(cash, index=False)
    flagged = export.build_day(DAY, data_root=tmp_path)[0].iloc[0]
    assert flagged.cash_0920_status == "MISSING_OR_FLAGGED_CASH_FIRST_BAR"


def test_impossible_precandle_fetch_cannot_prove_live_availability(tmp_path):
    _fixture(tmp_path, cash_fetch="2026-10-12T09:15:00+05:30")
    row = export.build_day(DAY, data_root=tmp_path)[0].iloc[0]
    assert row.price_first_5m_pct == pytest.approx(1)
    assert row.cash_first_bar_received_by_0921 == False
    assert row.signal_inputs_received_by_0921 == False
    assert row.complete_decision_evidence_by_0922 == False


def test_explicit_root_missing_dated_universe_fails_closed(tmp_path):
    with pytest.raises(FileNotFoundError, match="Dated universe is missing"):
        export.build_day(DAY, data_root=tmp_path)
