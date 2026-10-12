"""Paired 09:20 G-3 research replay must preserve dated evidence and P&L math."""
from __future__ import annotations

from datetime import date
from pathlib import Path
import json

import pandas as pd
import pytest

import fno_oi_hybrid_data as hybrid
import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
from research import g3_0920_backtest as backtest
from research.g3_0920_strategy import OI_SOURCE, select_orders_0920


DAY = date(2026, 10, 12)


def _opening_feature(*, symbol="AAA", **changes):
    row = dict(
        session_date=DAY.isoformat(), symbol=symbol, contract=f"{symbol}26OCTFUT",
        research_feature_status="COMPLETE", cash_0920_status="VALID_COMPLETED_FIRST_BAR",
        confirmation_0921_status="VALID_REAL_MINUTE",
        futures_0920_status="VALID_COMPLETED_FUTURES_0920",
        opening_0916_status="VALID_FIRST_MINUTE_EVIDENCE",
        price_first_5m_pct=.5, oi_change_pct=.3,
        oi_change_source=OI_SOURCE, oi_change_interval_minutes=4,
        oi_change_availability="RETROSPECTIVE_RESEARCH",
        futures_0920_oi=100_300., opening_0916_oi=100_000.,
        cash_open=100., cash_high=100.7, cash_low=99.9, cash_close=100.5,
        cash_volume=500.,
        ema9=101., ema20=100., ema50=99., volume_ratio=4.,
        traded_value=1_000_000., confirmation_open=100.5,
        confirmation_high=101., confirmation_low=100.4, confirmation_close=100.8,
        confirmation_volume=100.,
        confirmation_body_ratio=.6, confirmation_upper_wick_ratio=.25,
        confirmation_lower_wick_ratio=.25, v9_1m_volume_ratio=1.2,
        nifty_context_status="VALID_COMPLETED_NIFTY_FIRST_BAR",
        nifty_first_bar_return_pct=-.1,
    )
    row.update(changes)
    return row


def _cash_path(root: Path, symbol="AAA") -> Path:
    folder = root / "stocks_indicators_1min_eq"
    folder.mkdir(parents=True, exist_ok=True)
    times = pd.date_range(f"{DAY} 09:16", f"{DAY} 15:15", freq="min", tz="Asia/Kolkata")
    rows = []
    for stamp in times:
        clock = stamp.strftime("%H:%M")
        if clock < "09:20":
            o, h, low, c, v = 100., 100.2, 99.9, 100., 100.
        elif clock == "09:20":
            o, h, low, c, v = 100., 100.7, 100., 100.5, 100.
        elif clock == "09:21":
            o, h, low, c, v = 100.5, 101., 100.4, 100.8, 100.
        else:
            o, h, low, c, v = 100.8, 101., 100.4, 100.8, 100.
        rows.append(dict(date=stamp, open=o, high=h, low=low, close=c, volume=v))
    path = hybrid.equity_one_minute_path(symbol, folder)
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def _dated_universe(root: Path, *, symbols=("AAA", "BBB")) -> Path:
    path = root / "fno_oi" / "universe" / f"near_month_{DAY}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [dict(master_date=str(DAY), exchange="NFO", tradingsymbol=f"{symbol}26OCTFUT",
                 futures_tradingsymbol=f"{symbol}26OCTFUT", underlying=symbol,
                 equity_symbol=symbol, is_index_future=False, expiry="2026-10-29",
                 instrument_token=1000 + n, futures_instrument_token=1000 + n,
                 equity_instrument_token=2000 + n, lot_size=1, tick_size=.05)
            for n, symbol in enumerate(symbols)]
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def test_dated_universe_requires_every_stock_and_contract(tmp_path, monkeypatch):
    monkeypatch.setattr(hybrid, "load_equity_token_map", lambda: {})
    path = _dated_universe(tmp_path)
    full = pd.DataFrame([_opening_feature(symbol="AAA"), _opening_feature(symbol="BBB")])
    _, proof = provenance.load_backtest_universe(
        universe_path=path, universe_date=DAY, contract_month_contains="26OCT",
        require_persisted_mapping=True)
    coverage = dict(session_date=str(DAY), stock_rows=len(full), universe=proof)
    backtest.validate_feature_universe(full, tmp_path, coverage=coverage)
    with pytest.raises(ValueError, match="(?i)(missing|mismatch|incomplete)"):
        backtest.validate_feature_universe(full.loc[full.symbol.eq("AAA")], tmp_path, coverage=coverage)
    wrong_contract = full.copy()
    wrong_contract.loc[wrong_contract.symbol.eq("BBB"), "contract"] = "BBB26NOVFUT"
    with pytest.raises(ValueError, match="(?i)(contract|mismatch|incomplete)"):
        backtest.validate_feature_universe(wrong_contract, tmp_path, coverage=coverage)
    path.unlink()
    with pytest.raises((ValueError, FileNotFoundError), match="(?i)(universe|missing)"):
        backtest.validate_feature_universe(full, tmp_path, coverage=coverage)


def test_sealed_feature_export_rejects_artifact_or_coverage_drift(tmp_path):
    feature_path = tmp_path / "features.parquet"
    pd.DataFrame([_opening_feature()]).to_parquet(feature_path, index=False)
    coverage_path = tmp_path / "coverage.json"
    coverage_path.write_text(json.dumps(dict(stock_rows=1, included_days=[str(DAY)],
                                             requested_calendar_start=str(DAY),
                                             requested_calendar_through=str(DAY))))
    manifest_path = tmp_path / "run_manifest.json"
    manifest_path.write_text(json.dumps(dict(schema="g3_0920_research_history_run_v1",
                                             requested_calendar_start=str(DAY),
                                             requested_calendar_through=str(DAY),
                                             artifacts_sha256={feature_path.name: backtest.sha256(feature_path),
                                                               coverage_path.name: backtest.sha256(coverage_path)})))
    features, coverage, proof = backtest.load_feature_evidence(feature_path)
    assert len(features) == coverage["stock_rows"] == 1
    assert proof["sealed"] is True
    coverage_path.write_text(coverage_path.read_text() + " ")
    with pytest.raises(ValueError, match="artifact hash changed"):
        backtest.load_feature_evidence(feature_path)


def test_execution_path_rejects_changed_opening_ohlc(tmp_path, monkeypatch):
    _cash_path(tmp_path)
    monkeypatch.setattr(backtest, "_first_minute", lambda *args: dict(contract="AAA26OCTFUT", oi=100_000.))
    futures_path = tmp_path / "fno_oi" / "raw_contracts_5m" / "AAA26OCTFUT_5minute.parquet"
    futures_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([dict(timestamp=pd.Timestamp(f"{DAY} 09:20", tz="Asia/Kolkata"),
                       tradingsymbol="AAA26OCTFUT", oi=100_300.)]).to_parquet(futures_path, index=False)
    good = select_orders_0920(pd.DataFrame([_opening_feature()]))
    assert len(good) == 1
    paths, evidence = backtest.new_execution_paths(good, tmp_path)
    assert set(paths) == set(good.sid)
    assert evidence[0]["bars"] == 354
    altered = good.copy()
    altered.loc[:, "cash_close"] = 100.6
    with pytest.raises(ValueError, match="(?i)(drift|mismatch|opening|cash)"):
        backtest.new_execution_paths(altered, tmp_path)
    altered = good.copy()
    altered.loc[:, "confirmation_high"] = 102.
    with pytest.raises(ValueError, match="(?i)(drift|mismatch|confirmation|cash)"):
        backtest.new_execution_paths(altered, tmp_path)


def test_opening_source_parity_uses_safe_stem_for_ampersand_contract(tmp_path, monkeypatch):
    symbol, futures_contract = "M&M", "M&M26AUGFUT"
    minute = backtest._read_cash(_cash_path(tmp_path, symbol=symbol))
    order = select_orders_0920(pd.DataFrame([
        _opening_feature(symbol=symbol, contract=futures_contract),
    ])).iloc[0]
    monkeypatch.setattr(backtest, "_first_minute", lambda *args: dict(
        contract=futures_contract, oi=100_000.))
    safe_stem = common.safe_contract_stem(futures_contract)
    assert safe_stem != futures_contract
    futures_path = (tmp_path / "fno_oi" / "raw_contracts_5m" /
                    f"{safe_stem}_5minute.parquet")
    futures_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([dict(timestamp=pd.Timestamp(f"{DAY} 09:20", tz="Asia/Kolkata"),
                       tradingsymbol=futures_contract, oi=100_300.)]).to_parquet(
                           futures_path, index=False)
    proof = backtest._opening_source_parity(order, minute, DAY, tmp_path)
    assert proof["contract"] == futures_contract
    assert proof["path"] == str(futures_path.resolve())
    assert proof["opening_0916_oi"] == 100_000.
    assert proof["completed_0920_oi"] == 100_300.


def _trade(sid: int, *, executed: bool, pnl: float, side="LONG") -> dict:
    stamp = pd.Timestamp(f"{DAY} 09:30", tz="Asia/Kolkata")
    return dict(
        sid=sid, signal_id=f"SIGNAL_{sid}", day=str(DAY), side=side,
        tradingsymbol=f"STOCK_{sid}", filled=True,
        entry_ts=stamp, exit_ts=stamp + pd.Timedelta(minutes=5),
        confirmation_ts=stamp - pd.Timedelta(minutes=1),
        entry_price=100., exit_price=101., net_return_pct=.95,
        portfolio_executed=executed,
        portfolio_net_profit_rupees=pnl if executed else 0.,
        portfolio_gross_profit_rupees=(pnl + 25.) if executed else 0.,
        portfolio_cost_rupees=25. if executed else 0.,
    )


def test_paired_pnl_accounts_for_displaced_and_newly_accepted_old_trades():
    baseline = pd.DataFrame([
        _trade(1, executed=True, pnl=100.),
        _trade(2, executed=False, pnl=0.),
    ])
    variant = pd.DataFrame([
        _trade(1, executed=False, pnl=0.),
        _trade(2, executed=True, pnl=200.),
        _trade(3, executed=True, pnl=-50., side="SHORT"),
    ])
    summary, daily = backtest.compare_ledgers(baseline, variant, [DAY], [DAY], {3})
    assert summary["baseline"]["net_pnl_rs"] == 100.
    assert summary["with_0920"]["net_pnl_rs"] == 150.
    assert summary["delta_net_pnl_rs"] == 50.
    assert summary["added_0920"]["net_pnl_rs"] == -50.
    assert summary["old_trade_effect"]["net_pnl_change_rs"] == 100.
    assert summary["old_trade_effect"]["displaced_signal_ids"] == ["SIGNAL_1"]
    assert summary["old_trade_effect"]["newly_accepted_signal_ids"] == ["SIGNAL_2"]
    assert daily.loc[0, "delta_net_pnl_rs"] == 50.
