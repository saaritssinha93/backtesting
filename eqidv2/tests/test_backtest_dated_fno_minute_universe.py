from datetime import date
import logging

import pandas as pd

import fno_oi_hybrid_data as hybrid
import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min as fetcher


def test_mis_filter_cannot_drop_required_fno_equities(tmp_path, monkeypatch):
    day = date(2026, 9, 15)
    path = tmp_path / "fno_oi/universe/near_month_2026-09-15.parquet"
    path.parent.mkdir(parents=True)
    pd.DataFrame([
        {"underlying": "IDEA", "tradingsymbol": "IDEA26SEPFUT", "instrument_token": 100,
         "equity_symbol": "IDEA", "equity_instrument_token": 3677697},
        {"underlying": "LTM", "tradingsymbol": "LTM26SEPFUT", "instrument_token": 200,
         "equity_symbol": "LTM", "equity_instrument_token": 4561409},
        {"underlying": "NIFTY", "tradingsymbol": "NIFTY26SEPFUT", "instrument_token": 300},
    ]).fillna({"equity_symbol": ""}).to_parquet(path, index=False)
    monkeypatch.setattr(hybrid, "load_equity_token_map", lambda: {})
    symbols, tokens = fetcher.include_dated_fno_equities(
        ["RELIANCE", "LTM"], {"RELIANCE": 99, "LTM": 1}, logging.getLogger(__name__),
        session_date=day, runtime_root=tmp_path,
    )
    assert symbols == ["IDEA", "LTM", "RELIANCE"]
    assert tokens == {"RELIANCE": 99, "LTM": 4561409, "IDEA": 3677697}


def test_missing_dated_universe_does_not_substitute_other_day(tmp_path):
    symbols, tokens = fetcher.include_dated_fno_equities(
        ["AAA"], {"AAA": 1}, logging.getLogger(__name__),
        session_date=date(2026, 9, 15), runtime_root=tmp_path,
    )
    assert symbols == ["AAA"] and tokens == {"AAA": 1}
