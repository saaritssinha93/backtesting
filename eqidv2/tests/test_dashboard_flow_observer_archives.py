from pathlib import Path

import pandas as pd
import pytest

from dashboard_flow_observer_archives import load_contract_archives
from dashboard_flow_observer_history import valid_candles


CONTRACT = "ABC26SEPFUT"


def candle(**changes):
    return {"date": "2026-09-01T09:15:00+05:30", "open": 100, "high": 102,
            "low": 99, "close": 101, "volume": 20, "oi": 300, **changes}


def save(root, relative, records):
    path = root / "fno_oi" / relative / f"{CONTRACT}_1minute.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(records).to_parquet(path)
    return path


def test_known_archives_normalized_and_unrelated_sources_excluded(tmp_path):
    a = save(tmp_path, "raw_contracts_1m_hist", [candle()])
    b = save(tmp_path, "strategy_research/v13_corrected_v5/derivative_market_data/raw_futures_1m",
             [dict(candle(), timestamp="2026-09-01T09:15:00+05:30", tradingsymbol=CONTRACT,
                   expiry="2026-09-29", instrument_type="FUT")])
    c = save(tmp_path, "strategy_research/v6_derivative_backtests/market_cache/2026-09-01",
             [dict(candle(), bar_start="2026-09-01T09:15:00+05:30", bar_end="2026-09-01T09:16:00+05:30")])
    save(tmp_path, "unreviewed/raw_futures_1m", [candle()])
    result = load_contract_archives(tmp_path, CONTRACT)
    assert {item["path"] for item in result} == {str(a), str(b), str(c)}
    assert all(item["records"][0]["candle_end"] == "2026-09-01T09:16:00+05:30" for item in result)


@pytest.mark.parametrize("changes", [
    {"tradingsymbol": "XYZ26SEPFUT"}, {"expiry": "2026-10-27"}, {"instrument_type": "CE"},
    {"gap_filled": True}, {"source": "SYNTHETIC_REPAIR"}, {"quality_state": "PROVISIONAL"},
    {"timestamp": "2026-09-01T09:16:00+05:30"}, {"bar_end": "2026-09-01T09:20:00+05:30"},
    {"date": "2026-09-01T09:14:00+05:30"},
])
def test_identity_quality_and_interval_conflicts_rejected(tmp_path, changes):
    save(tmp_path, "raw_contracts_1m_hist", [candle(**changes)])
    assert load_contract_archives(tmp_path, CONTRACT)[0]["records"] == []


def test_registry_requires_exact_expiry(tmp_path):
    registry = tmp_path / "fno_oi/universe/contract_registry.parquet"
    registry.parent.mkdir(parents=True)
    pd.DataFrame([{"tradingsymbol": CONTRACT, "expiry": "2026-09-29"}]).to_parquet(registry)
    save(tmp_path, "raw_contracts_1m_hist", [candle(expiry="2026-09-28")])
    assert load_contract_archives(tmp_path, CONTRACT)[0]["records"] == []


def test_cross_source_conflicts_preserved_for_validation(tmp_path):
    save(tmp_path, "raw_contracts_1m_hist", [candle()])
    save(tmp_path, "raw_contracts_1m/2026-09-01", [candle(oi=301)])
    result = load_contract_archives(tmp_path, CONTRACT)
    records = [record for source in result for record in source["records"]]
    assert len(records) == 2
    assert valid_candles(records, pd.Timestamp("2026-09-01").date()) == []


def test_timestamp_only_producer_and_utc_alias(tmp_path):
    row = candle()
    row["timestamp"] = row.pop("date")
    row["ts"] = "2026-09-01T03:45:00+00:00"
    save(tmp_path, "strategy_research/v13_corrected_v5/daily_options/2026-09-01/derivative_market_data/raw_futures_1m", [row])
    result = load_contract_archives(tmp_path, CONTRACT)
    assert result[0]["records"][0]["candle_start"] == "2026-09-01T09:15:00+05:30"


def test_no_write_or_source_change(tmp_path):
    path = save(tmp_path, "raw_contracts_1m_hist", [candle()])
    before = path.read_bytes()
    load_contract_archives(tmp_path, CONTRACT)
    assert path.read_bytes() == before
    assert not (tmp_path / "dashboard_flow_observer").exists()


def test_rejects_nonmonthly_or_traversal_names(tmp_path):
    for contract in ("../ABC", "ABC", "ABC26XXXFUT"):
        with pytest.raises(ValueError):
            load_contract_archives(tmp_path, contract)
