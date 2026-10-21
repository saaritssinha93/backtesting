"""The backtest cutoff must be visible without changing execution authority."""
from datetime import date
import hashlib
import json
from pathlib import Path
import sys
from types import ModuleType

import pandas as pd
import pytest

import dashboard_flow as flow
import fno_v13_v10_g_3_backtest as cli
import fno_v13_v10_g_3_contract as production
from research import g3_backtest_report as report
from research.archived_g3_v8 import contract as archived_v8
from research.g3_0920_backtest import _stats


VERSION = report.contract.STRATEGY_VERSION
SCHEMA = "fno_g3_target_cutoff_backtest_v1"


@pytest.fixture(params=["current", "archived_v8"])
def cutoff_run(tmp_path, monkeypatch, request):
    profile = report.contract if request.param == "current" else archived_v8
    monkeypatch.setattr(flow, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(flow, "active_profile_name", lambda: "V13_V10_G_3")
    root = tmp_path / "backtesting_result_v13_v10_g_3"
    relative = Path("backtests/runs/2026-10-13/cutoff")
    directory = root / relative
    directory.mkdir(parents=True)
    (directory / "manifest.json").write_text("{}")
    (directory / "daily_results.csv").write_text("day,trades,wins,losses,net_pnl\n2026-10-13,0,0,0,0\n")
    summary = dict(strategy_version=profile.STRATEGY_VERSION, strategy_fingerprint=profile.strategy_fingerprint(),
        active_setup_count=len(profile.ACTIVE_SETUPS),
        coverage={}, first_date="2026-10-13", last_date="2026-10-13", sessions=1,
        created_at_ist="2026-10-13T22:00:00+05:30")
    pointer = dict(schema_version=SCHEMA, role="backtest_history", execution_authority=False,
        run_dir=relative.as_posix(), session_date="2026-10-13", strategy_version=profile.STRATEGY_VERSION,
        strategy_fingerprint=profile.strategy_fingerprint(),
        manifest_sha256=hashlib.sha256((directory / "manifest.json").read_bytes()).hexdigest())
    (root / "latest").mkdir()
    pointer_path = root / "latest/current_backtest.json"
    pointer_path.write_text(json.dumps(pointer))
    verifier = ModuleType("research.g3_target_cutoff_backtest")
    verifier.verified_package = lambda run, result_root: (summary, {})
    monkeypatch.setitem(sys.modules, verifier.__name__, verifier)
    for name in ("_verified_g3_runs", "_verified_g3_0920_long_backtest", "_verified_g3_0920_daily",
                 "_verified_g3_research_runs", "_verified_g4_current_backtest"):
        monkeypatch.setattr(flow, name, lambda: [])
    return directory, pointer_path, pointer, summary


def test_cutoff_reader_and_user_copy_distinguish_paper_live(cutoff_run):
    summary = cutoff_run[3]
    current = summary['strategy_version'] == VERSION
    records = flow._verified_g3_current_backtest()
    assert len(records) == 1
    metadata, _ = records[0]
    assert metadata["strategy_matches_backtest"] is current
    assert metadata["strategy_matches_active"] is False
    assert metadata["active_setup_count"] == (51 if current else 46)
    assert metadata["backtest_only_target_cutoff"] is True
    assert "target >0.75%" in metadata["label"]
    payload = flow.load_flow_data(strategy="V13-V10-G-3")
    copy = next(item for item in payload["warnings"] if item.startswith("Current G-3 historical backtest:"))
    assert f"{51 if current else 46} setups" in copy and "targets <=0.75%" in copy
    if current:
        assert "09:50 LONG (0.75%)" in copy and "10:05 LONG (0.56%)" in copy and "10:15 SHORT (0.68%)" in copy
        assert "10 other setups" in copy
        assert "09:20 SHORT and 10:10 LONG setups each use a 1.50% target" in copy
    else:
        assert "all 13 setups" in copy and "restored" not in copy
    assert "PAPER/LIVE retain their separate 61-setup" in copy
    assert "share the same" not in copy


@pytest.mark.parametrize("corruption", ["authority", "fingerprint", "manifest"])
def test_cutoff_reader_rejects_invalid_pointer(cutoff_run, corruption):
    directory, pointer_path, pointer, _ = cutoff_run
    if corruption == "authority":
        pointer["execution_authority"] = True
    elif corruption == "fingerprint":
        pointer["strategy_fingerprint"] = "wrong"
    else:
        (directory / "manifest.json").write_text('{"changed":true}')
    pointer_path.write_text(json.dumps(pointer))
    assert flow._verified_g3_current_backtest() == []


def _report_input(profile):
    ledger = pd.DataFrame([dict(day="2026-10-13", setup_id="0931_LONG", filled=True,
        portfolio_executed=True, portfolio_net_profit_rupees=100.,
        portfolio_gross_profit_rupees=105., portfolio_cost_rupees=5.)])
    summary = dict(days=["2026-10-13"], first_date="2026-10-13", last_date="2026-10-13", sessions=1,
        active_setup_count=len(profile.ACTIVE_SETUPS), strategy_version=profile.STRATEGY_VERSION,
        strategy_fingerprint=profile.strategy_fingerprint(), current_g3=_stats(ledger, [date(2026, 10, 13)]))
    daily = pd.DataFrame([dict(day="2026-10-13", trades=1, wins=1, losses=0, net_pnl=100.)])
    return summary, ledger, daily


def test_current_report_has_51_rows_and_explicit_restored_exceptions(tmp_path):
    assert len(report.contract.ACTIVE_SETUPS) == 51
    report.write(tmp_path, *_report_input(report.contract))
    setups = pd.read_csv(tmp_path / "setup_results.csv")
    assert len(setups) == 51
    assert set(setups.loc[setups.target_pct.le(.75), 'setup_id']) == {'0951_LONG', '1006_LONG', '1016_SHORT'}
    assert int(setups.added.sum()) == 5
    page = (tmp_path / "report.html").read_text()
    assert "G3 historical backtest: 51 setups" in page
    assert "10 remaining setups with target <=0.75% are disabled" in page
    for restored in ("09:50 LONG (0.75%)", "10:05 LONG (0.56%)", "10:15 SHORT (0.68%)"):
        assert restored in page
    assert "PAPER/LIVE retain their 61-setup" in page
    assert "share the same" not in page


def test_archived_v8_report_retains_46_rows_without_restored_exceptions(tmp_path):
    report.write(tmp_path, *_report_input(archived_v8))
    setups = pd.read_csv(tmp_path / "setup_results.csv")
    assert len(setups) == 46 and setups.target_pct.gt(.75).all()
    page = (tmp_path / "report.html").read_text()
    assert "G3 historical backtest: 46 setups" in page
    assert "all 13 setups with target <=0.75% are disabled" in page
    assert "restored exceptions" not in page


def test_report_rejects_unknown_identity_instead_of_relabeling_current(tmp_path):
    summary, ledger, daily = _report_input(archived_v8)
    summary['strategy_fingerprint'] = 'wrong'
    with pytest.raises(ValueError, match='unrecognized strategy identity'):
        report.write(tmp_path, summary, ledger, daily)


def test_current_production_report_contains_all_61_rows(tmp_path):
    report.write(tmp_path, *_report_input(production))
    setups = pd.read_csv(tmp_path / "setup_results.csv")
    assert len(setups) == 61
    assert int(setups.added.sum()) == 15
    assert len(setups.loc[setups.target_pct.le(.75)]) == 13
    page = (tmp_path / "report.html").read_text()
    assert "G3 historical backtest: 61 setups" in page
    assert f"This saved {production.STRATEGY_VERSION} report" in page
    assert "all 13 setups with target <=0.75% are disabled" not in page


def test_normal_cli_routes_to_cutoff_build_and_publish(tmp_path, monkeypatch):
    root = tmp_path / "result"
    output = root / "backtests/runs/2026-10-13/normal"
    calls = []
    backend = ModuleType("research.g3_target_cutoff_backtest")
    def build(result_root, directory, through):
        calls.append(("build", result_root, directory, through))
        return dict(sessions=50, current_g3=dict(executed_trades=428, win_rate_pct=75., net_pnl_rs=1.))
    def publish(result_root, directory):
        calls.append(("publish", result_root, directory))
        return {}
    backend.build, backend.publish = build, publish
    monkeypatch.setitem(sys.modules, backend.__name__, backend)
    monkeypatch.setattr(cli, "BACKTEST_ROOT", root)
    monkeypatch.setattr(sys, "argv", ["g3-backtest", "--through", "2026-10-13", "--output-dir", str(output)])
    cli.main()
    assert calls == [("build", root, output, date(2026, 10, 13)), ("publish", root, output)]
    assert len(production.ACTIVE_SETUPS) == 61
    assert production.STRATEGY_VERSION == "V13-V10-G-3-PRODUCTION-V11"
