"""Batch first-slot research must match the dated single-session adapter."""
from datetime import date, datetime, timedelta
import json

import pandas as pd
import pytest

from dashboard_flow_observer_store import write_json
from research import g3_0920_feature_export as single
from research import g3_0920_history as batch


DAYS = (date(2026, 10, 9), date(2026, 10, 12))
CONTRACT = "SUZLON26OCTFUT"
EXPIRY = "2026-10-29"


def _fixture(root):
    universe_root = root / "fno_oi/universe"
    universe_root.mkdir(parents=True)
    for day in DAYS:
        pd.DataFrame([
            {"master_date": str(day), "exchange": "NFO", "underlying": "SUZLON",
             "tradingsymbol": CONTRACT, "futures_tradingsymbol": CONTRACT,
             "instrument_token": 10, "futures_instrument_token": 10,
             "equity_symbol": "SUZLON", "equity_instrument_token": 20,
             "expiry": EXPIRY, "is_index_future": False},
            {"master_date": str(day), "exchange": "NFO", "underlying": "NIFTY",
             "tradingsymbol": "NIFTY26OCTFUT", "futures_tradingsymbol": "NIFTY26OCTFUT",
             "instrument_token": 11, "futures_instrument_token": 11,
             "equity_symbol": "", "equity_instrument_token": 0,
             "expiry": EXPIRY, "is_index_future": True},
        ]).to_parquet(universe_root / f"near_month_{day}.parquet", index=False)
    previous = datetime.fromisoformat("2026-10-08T14:56:00+05:30")
    minutes = []
    for n in range(25):
        ts = previous + timedelta(minutes=n)
        minutes.append({"date": ts.isoformat(), "open": 99, "high": 99.1, "low": 98.9,
                        "close": 99.05, "volume": 1000, "quality_state": None,
                        "source": "KITE_HISTORICAL_1M", "fetched_at": "2026-10-08T15:30:00+05:30"})
    for day in DAYS:
        for n in range(6):
            ts = datetime.fromisoformat(f"{day}T09:16:00+05:30") + timedelta(minutes=n)
            price = 100 + n / 5 + (0 if day == DAYS[0] else 1)
            minutes.append({"date": ts.isoformat(), "open": price, "high": price + .1,
                            "low": price - .1, "close": price + .05, "volume": 1000 + n * 100,
                            "quality_state": "VALID", "source": "KITE_HISTORICAL_1M",
                            "fetched_at": f"{day}T09:{21 if n == 5 else 20}:12+05:30"})
    cash = root / "stocks_indicators_1min_eq/SUZLON_stocks_indicators_1min.parquet"
    cash.parent.mkdir(parents=True)
    pd.DataFrame(minutes).to_parquet(cash, index=False)
    future_root = root / "fno_oi/raw_contracts_5m"
    future_root.mkdir(parents=True)
    for symbol, start_price in ((CONTRACT, 100), ("NIFTY26OCTFUT", 20000)):
        records = []
        for day in DAYS:
            records.append({"timestamp": f"{day}T09:20:00+05:30",
                            "candle_start": f"{day}T09:15:00+05:30",
                            "tradingsymbol": symbol, "expiry": EXPIRY,
                            "open": start_price, "high": start_price + 1,
                            "low": start_price - 1, "close": start_price + .5,
                            "volume": 10000, "oi": 1100,
                            "quality_state": "VALID", "source": "kite_historical",
                            "fetch_timestamp": f"{day}T09:20:20+05:30"})
        pd.DataFrame(records).to_parquet(future_root / f"{symbol}_5minute.parquet", index=False)
    day = DAYS[0]
    write_json(root, f"historical/{day}/{CONTRACT}.json", {
        "symbol": "SUZLON", "session_date": str(day), "contract": CONTRACT,
        "source": "KITE_HISTORICAL_MINUTE", "acquired_at": f"{day}T09:16:10+05:30",
        "candles": [{"candle_start": f"{day}T09:15:00+05:30",
                     "candle_end": f"{day}T09:16:00+05:30",
                     "open": 100, "high": 100.2, "low": 99.9, "close": 100.05,
                     "volume": 1000, "oi": 1000}]})


def test_batch_parity_one_cash_read_and_missing_oi(tmp_path, monkeypatch):
    _fixture(tmp_path)
    expected = {day: single.build_day(day, data_root=tmp_path)[0].iloc[0] for day in DAYS}
    count = 0
    original = single._read_cash

    def counted(path):
        nonlocal count
        count += 1
        return original(path)

    monkeypatch.setattr(single, "_read_cash", counted)
    frame, coverage = batch.build_days(DAYS, data_root=tmp_path)
    assert count == 1
    assert len(frame) == coverage["stock_rows"] == 2
    assert coverage["valid_0916_oi_changes"] == 1
    assert coverage["trading_sessions"] == 2
    for day in DAYS:
        row = frame.loc[frame.session_date.eq(str(day))].iloc[0]
        reference = expected[day]
        for key in ("cash_open", "cash_high", "cash_low", "cash_close", "cash_volume",
                    "price_first_5m_pct", "ema9", "ema20", "ema50", "volume_ratio",
                    "confirmation_volume_ratio", "v9_1m_volume_ratio", "oi_change_pct"):
            if pd.isna(reference.get(key)):
                assert pd.isna(row.get(key))
            else:
                assert row.get(key) == pytest.approx(reference.get(key))
        for key in ("cash_0920_status", "confirmation_0921_status", "opening_0916_status",
                    "futures_0920_status", "missing_reasons", "signal_inputs_received_by_0921",
                    "complete_decision_evidence_by_0922"):
            assert row.get(key) == reference.get(key)
    assert frame.iloc[0].oi_change_pct == pytest.approx(10)
    assert pd.isna(frame.iloc[1].oi_change_pct)
    assert "MISSING_OR_INVALID_FIRST_MINUTE_OI" in frame.iloc[1].missing_reasons


def test_closed_dates_require_explicit_skip_and_are_reported(tmp_path):
    _fixture(tmp_path)
    holiday = date(2026, 9, 14)
    with pytest.raises(ValueError, match="closed sessions"):
        batch.build_days([DAYS[0], holiday], data_root=tmp_path)
    frame, coverage = batch.build_days([DAYS[0], holiday], data_root=tmp_path, skip_closed=True)
    assert len(frame) == 1
    assert coverage["skipped_closed_days"][0]["day"] == str(holiday)
    assert "Ganesh Chaturthi" in coverage["skipped_closed_days"][0]["reason"]


def test_missing_eligible_dated_universe_fails_closed(tmp_path):
    with pytest.raises(FileNotFoundError, match="Dated universe is missing"):
        batch.build_days([DAYS[0]], data_root=tmp_path)


def test_calendar_cli_discloses_missing_trading_universe(tmp_path):
    _fixture(tmp_path)
    args = ["--start", "2026-10-08", "--through", "2026-10-12",
            "--data-root", str(tmp_path), "--output-root", str(tmp_path / "out"),
            "--skip-closed"]
    with pytest.raises(SystemExit) as error:
        batch.main(args)
    assert error.value.code == 2
    assert batch.main([*args, "--skip-missing-universe"]) == 0
    report = tmp_path / "out/2026-10-08_through_2026-10-12/coverage.json"
    coverage = json.loads(report.read_text())
    assert coverage["skipped_missing_universe_days"] == ["2026-10-08"]
    assert coverage["included_days"] == ["2026-10-09", "2026-10-12"]
    assert {row["day"] for row in coverage["skipped_closed_days"]} == {"2026-10-10", "2026-10-11"}


def _sealed_fixture(root):
    """Small bundle with a real hash chain and one research-only mapping."""
    day = date(2026, 7, 29)
    monthly = date(2026, 8, 21)
    expiry = "2026-08-25"
    universe = root / f"fno_oi/universe/near_month_{monthly}.parquet"
    universe.parent.mkdir(parents=True)
    rows = [
        {"master_date": str(monthly), "exchange": "NFO", "underlying": "SUZLON",
         "tradingsymbol": "SUZLON26AUGFUT", "futures_tradingsymbol": "SUZLON26AUGFUT",
         "instrument_token": 10, "futures_instrument_token": 10,
         "equity_symbol": "SUZLON", "equity_instrument_token": 20,
         "expiry": expiry, "is_index_future": False},
        {"master_date": str(monthly), "exchange": "NFO", "underlying": "NIFTY",
         "tradingsymbol": "NIFTY26AUGFUT", "futures_tradingsymbol": "NIFTY26AUGFUT",
         "instrument_token": 11, "futures_instrument_token": 11,
         "equity_symbol": "", "equity_instrument_token": 0,
         "expiry": expiry, "is_index_future": True},
    ]
    pd.DataFrame(rows).to_parquet(universe, index=False)
    future = root / "fno_oi/raw_contracts_5m/NIFTY26AUGFUT_5minute.parquet"
    future.parent.mkdir(parents=True)
    pd.DataFrame([{"timestamp": f"{day}T09:20:00+05:30",
                   "candle_start": f"{day}T09:15:00+05:30",
                   "tradingsymbol": "NIFTY26AUGFUT", "expiry": expiry,
                   "open": 20000, "high": 20001, "low": 19999, "close": 20000.5,
                   "volume": 100, "oi": 1000, "quality_state": "VALID"}]).to_parquet(future, index=False)
    bundle = root / batch._SEALED_RUN
    dataset = bundle / "dataset"
    dataset.mkdir(parents=True)
    pd.DataFrame([{"day": str(day), "hhmm_int": 920, "tradingsymbol": "SUZLON",
                   "futures_tradingsymbol": "SUZLON26AUGFUT", "contract_month": "26AUG",
                   "instrument_token": 20, "futures_instrument_token": 10,
                   "exchange": "NSE"}]).to_parquet(dataset / "all_5m_features.parquet", index=False)
    pd.DataFrame([{"day": str(day), "required_contract": "26AUG", "expiry": expiry,
                   "universe_size": 1, "contracts_with_data": 1, "eligible": True}]).to_csv(
                       dataset / "source_session_eligibility.csv", index=False)
    pd.DataFrame([{"path": str(future.resolve()), "exists": True,
                   "sha256": batch._sha256(future)}]).to_csv(dataset / "source_manifest.csv", index=False)
    products = ("all_5m_features.parquet", "source_session_eligibility.csv", "source_manifest.csv")
    manifest = {"days": [str(day)], "calendar": {"26AUG": expiry},
                "sources": [{"path": str(universe.resolve()), "exists": True,
                             "sha256": batch._sha256(universe)}],
                "output_sha256": {name: batch._sha256(dataset / name) for name in products}}
    (dataset / "dataset_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    artifacts = {}
    for name in ("dataset/dataset_manifest.json", *(f"dataset/{name}" for name in products)):
        path = bundle / name
        artifacts[name] = {"bytes": path.stat().st_size, "sha256": batch._sha256(path)}
    (bundle / "bundle_manifest.json").write_text(json.dumps({"state": "COMPLETE",
                                                               "execution_authority": False,
                                                               "artifacts": artifacts}), encoding="utf-8")
    return day, bundle


def test_sealed_monthly_fallback_requires_opt_in_and_discloses_proof(tmp_path):
    day, _ = _sealed_fixture(tmp_path)
    with pytest.raises(FileNotFoundError, match="Dated universe is missing"):
        batch.build_days([day], data_root=tmp_path)
    mapped, proof, nifty = batch.load_sealed_mapping(day, tmp_path)
    assert len(mapped) == proof["sealed_session_mapping_count"] == 1
    assert proof["mapping_source"] == "SEALED_MONTHLY_DATASET"
    assert proof["monthly_universe_date"] == "2026-08-21"
    assert proof["bundle_manifest_sha256"] and proof["all_5m_features_sha256"]
    assert nifty["nifty_context_status"] == "VALID_COMPLETED_NIFTY_FIRST_BAR"
    frame, coverage = batch.build_days([day], data_root=tmp_path, allow_sealed_fallback=True)
    assert len(frame) == 1
    assert coverage["per_day"][0]["universe"]["mapping_source"] == "SEALED_MONTHLY_DATASET"


def test_sealed_monthly_fallback_rejects_tamper_and_conflicting_legacy_dated_file(tmp_path):
    day, bundle = _sealed_fixture(tmp_path)
    dated = tmp_path / f"fno_oi/universe/near_month_{day}.parquet"
    pd.DataFrame([{"master_date": str(day), "tradingsymbol": "SUZLON26AUGFUT",
                   "instrument_token": 999, "is_index_future": False}]).to_parquet(dated, index=False)
    with pytest.raises(ValueError, match="conflicts with sealed monthly futures"):
        batch.load_sealed_mapping(day, tmp_path)
    with pytest.raises(ValueError, match="conflicts with sealed monthly futures"):
        batch.main(["--start", str(day), "--through", str(day), "--data-root", str(tmp_path),
                    "--output-root", str(tmp_path / "out"), "--allow-sealed-fallback",
                    "--skip-missing-universe"])
    dated.unlink()
    features = bundle / "dataset/all_5m_features.parquet"
    with features.open("ab") as stream:
        stream.write(b"tampered")
    with pytest.raises(ValueError, match="artifact hash/size mismatch"):
        batch.load_sealed_mapping(day, tmp_path)
