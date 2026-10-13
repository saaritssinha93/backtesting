"""Full-history extension preserves the base, rejects drift and retains flat days."""
from datetime import date
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from research import g3_current_history as history
import backtesting_result_v13_v10_g_3_daily as publisher
import dashboard_flow as flow


@pytest.fixture
def cohort(tmp_path, monkeypatch):
    root = tmp_path / "backtesting_result_v13_v10_g_3"
    base = root / "backtests/runs/2026-10-12/base"
    base.mkdir(parents=True)
    universe = tmp_path / "universe"
    universe.mkdir()
    (universe / "near_month_2026-10-13.parquet").touch()
    def trade(day, identity, pnl):
        return dict(day=day, signal_id=identity, setup_id="0931_LONG", filled=True, portfolio_executed=True,
            strategy_version=history.contract.STRATEGY_VERSION,
            strategy_fingerprint=history.contract.strategy_fingerprint(),
            portfolio_net_profit_rupees=pnl, portfolio_gross_profit_rupees=pnl+5,
            portfolio_cost_rupees=5)
    pd.DataFrame([trade("2026-07-29", "base", 100)]).to_csv(base / "variant_trades.csv", index=False)
    pd.DataFrame(dict(day=["2026-07-29", "2026-10-12"])).to_csv(base / "daily_results.csv", index=False)
    (base / "manifest.json").write_text('{}')
    base_summary = dict(last_date="2026-10-12", strategy_fingerprint=history.contract.strategy_fingerprint())
    monkeypatch.setattr(history, "verify_base", lambda path: (base_summary, {}))
    daily = root / "production/runs/2026-10-13/daily"
    daily.mkdir(parents=True)
    ledger = pd.DataFrame([trade("2026-10-13", "extension", -20)])
    ledger.to_csv(daily / "portfolio_trades.csv", index=False)
    history._daily_table(ledger, ["2026-10-13"]).to_csv(daily / "daily_results.csv", index=False)
    payload = dict(schema_version=history.replay.SCHEMA_VERSION, strategy=history.replay.STRATEGY,
        strategy_version=history.contract.STRATEGY_VERSION, strategy_fingerprint=history.contract.strategy_fingerprint(),
        execution_authority=False, role="production_daily", complete=True, state="SUCCESS",
        session_date="2026-10-13", days=["2026-10-13"], run_id="daily", coverage={},
        artifacts={name:dict(path=name+".csv", sha256=history.replay.sha256(daily / (name+".csv")))
                   for name in ("portfolio_trades", "daily_results")})
    history.replay.atomic_json(daily / "run_manifest.json", payload)
    monkeypatch.setattr(publisher, "_verified_daily", lambda root, day: (daily, payload))
    monkeypatch.setattr(history.replay, "require_closed_session", lambda day: None)
    return SimpleNamespace(root=root, base=base, daily=daily, payload=payload,
                           roots=SimpleNamespace(universe=universe), through=date(2026, 10, 13))


def test_full_history_includes_new_day_once_and_keeps_flat_base_day(cohort, monkeypatch):
    c = cohort
    output = c.root / "backtests/runs/2026-10-13/full"
    summary = history.build(c.root, output, c.through, base=c.base, roots=c.roots)
    assert summary["sessions"] == 3
    assert summary["current_g3"]["net_pnl_rs"] == 80
    assert summary["current_g3"]["max_drawdown_rs"] == 20
    daily = pd.read_csv(output / "daily_results.csv")
    assert daily.trades.tolist() == [1, 0, 1]
    assert daily.cumulative_net_pnl.tolist() == [100, 100, 80]
    history.publish(c.root, output)
    assert history.current_base(c.root) == c.base
    rerun = c.root / "backtests/runs/2026-10-13/rerun"
    repeated = history.build(c.root, rerun, c.through, roots=c.roots)
    assert repeated["current_g3"] == summary["current_g3"]
    monkeypatch.setattr(flow, "DATA_ROOT", c.root.parent)
    loaded = flow.load_flow_data(strategy="V13-V10-G-3")
    assert loaded["selected_run"]["current_full_strategy"] is True
    assert loaded["summary"]["period_start"] == "2026-07-29"
    assert loaded["summary"]["period_end"] == "2026-10-13"
    assert loaded["summary"]["net_pnl"] == 80


@pytest.mark.parametrize("corruption", ["revision", "duplicate", "accounting"])
def test_invalid_extension_cannot_publish(cohort, corruption):
    c = cohort
    path = c.daily / "portfolio_trades.csv"
    ledger = pd.read_csv(path)
    if corruption == "revision":
        ledger["strategy_fingerprint"] = "wrong"
    elif corruption == "duplicate":
        ledger["signal_id"] = "base"
    else:
        ledger["portfolio_net_profit_rupees"] = 999
    ledger.to_csv(path, index=False)
    with pytest.raises(ValueError):
        history.build(c.root, c.root / "backtests/runs/2026-10-13/invalid", c.through, base=c.base, roots=c.roots)
    assert not (c.root / "latest/current_backtest.json").exists()


def test_source_drift_and_missing_cutoff_leave_current_pointer_intact(cohort):
    c = cohort
    output = c.root / "backtests/runs/2026-10-13/full"
    history.build(c.root, output, c.through, base=c.base, roots=c.roots)
    history.publish(c.root, output)
    pointer = c.root / "latest/current_backtest.json"
    original = pointer.read_bytes()
    with pytest.raises(ValueError, match="no dated market universe"):
        history.build(c.root, c.root / "backtests/runs/2026-10-14/missing", date(2026, 10, 14), roots=c.roots)
    (c.daily / "portfolio_trades.csv").write_text("changed")
    with pytest.raises(ValueError, match="artifact changed"):
        history.publish(c.root, output)
    assert pointer.read_bytes() == original


def test_zero_trade_extension_remains_a_verified_session(cohort):
    c = cohort
    path = c.daily / "portfolio_trades.csv"
    pd.read_csv(path).iloc[:0].to_csv(path, index=False)
    c.payload["artifacts"]["portfolio_trades"]["sha256"] = history.replay.sha256(path)
    daily_path = c.daily / "daily_results.csv"
    history._daily_table(pd.read_csv(path), ["2026-10-13"]).to_csv(daily_path, index=False)
    c.payload["artifacts"]["daily_results"]["sha256"] = history.replay.sha256(daily_path)
    history.replay.atomic_json(c.daily / "run_manifest.json", c.payload)
    result = history.build(c.root, c.root / "backtests/runs/2026-10-13/zero", c.through, base=c.base, roots=c.roots)
    assert result["sessions"] == 3
    assert result["current_g3"]["executed_trades"] == 1
    assert result["current_g3"]["net_pnl_rs"] == 100
