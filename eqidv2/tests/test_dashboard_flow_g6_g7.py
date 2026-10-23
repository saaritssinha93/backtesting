"""Verified G-6/G-7 Dashboard Flow discovery and selection contracts."""

import hashlib
import json

import pytest

import dashboard_flow as flow
from tests.test_dashboard_flow import write_csv


CASES = (
    pytest.param("g6", "V13-V10-G-6", id="g6"),
    pytest.param("g7", "V13-V10-G-7", id="g7-v3"),
)


def _modules(family):
    if family == "g6":
        from research import g6_daily_backtest as daily
        from research import g6_g4_replica_backtest as history
    else:
        from research import g7_daily_backtest as daily
        from research import g7_g6_replica_backtest as history
    return history, daily


def _json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _manifest(run, *, schema_version=None):
    files = {
        path.relative_to(run).as_posix(): _digest(path)
        for path in run.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }
    payload = {"files": files}
    if schema_version:
        payload["schema_version"] = schema_version
    _json(run / "manifest.json", payload)
    return _digest(run / "manifest.json")


def _trade(day, pnl, *, symbol="SELECTED"):
    return dict(
        sid=f"{day}-{symbol}", day=day, tradingsymbol=symbol, side="LONG",
        setup_id="1011_LONG", filled=True, portfolio_executed=True,
        entry_price=100, exit_price=101, entry_ts=f"{day} 10:12:00+05:30",
        exit_ts=f"{day} 10:20:00+05:30", exit_reason="TARGET",
        portfolio_net_profit_rupees=pnl,
        portfolio_gross_profit_rupees=pnl + 5,
        portfolio_cost_rupees=5,
    )


def _install_family(tmp_path, monkeypatch, family, strategy):
    history, daily = _modules(family)
    root = tmp_path / f"backtesting_result_v13_v10_g_{family[-1]}"
    selected_policy = history.NIFTY_POLICY
    unfiltered_policy = f"{family}_unfiltered"
    history_day, daily_day = "2026-10-21", "2026-10-22"
    history_run = root / "runs" / history_day / "history"
    daily_run = root / "daily" / "runs" / daily_day / "daily"
    config = history.configuration_payload()

    history_summary = dict(
        variant_id=history.VERSION, strategy=strategy, execution_authority=False,
        active_setups=51, sessions=1, first_date=history_day, last_date=history_day,
        days=[history_day], selected_policy=selected_policy,
        direction_filter_mode="nifty_both_counter_skip",
    )
    _json(history_run / "summary.json", history_summary)
    _json(history_run / "config.json", config)
    write_csv(history_run / selected_policy / "daily_results.csv", [
        dict(day=history_day, trades=1, wins=1, losses=0,
             net_pnl=100, gross_pnl=105, cost=5),
    ])
    write_csv(history_run / selected_policy / "portfolio_trades.csv", [_trade(history_day, 100)])
    # A saved control is valid evidence but must never be added to the selected policy.
    write_csv(history_run / unfiltered_policy / "daily_results.csv", [
        dict(day=history_day, trades=1, wins=1, losses=0,
             net_pnl=999_999, gross_pnl=1_000_004, cost=5),
    ])
    write_csv(history_run / unfiltered_policy / "portfolio_trades.csv", [
        _trade(history_day, 999_999, symbol="CONTROL"),
    ])
    history_manifest = _manifest(history_run)

    selected_result = dict(
        sessions=1, selected_orders=1, native_fills=1, trades=1, wins=1,
        losses=0, win_rate_pct=100, profitable_days=1, losing_days=0,
        net_pnl_rs=200, gross_pnl_rs=205, costs_rs=5,
    )
    daily_summary = dict(
        schema_version=daily.SCHEMA, variant_id=history.VERSION,
        scope="BACKTEST_ONLY", execution_authority=False, complete=True,
        sessions=1, session_date=daily_day, first_date=daily_day,
        last_date=daily_day, days=[daily_day], active_setups=51,
        selected_policy=selected_policy,
        direction_filter_mode="nifty_both_counter_skip",
        selected_result=selected_result, history_pointer_changed=False,
    )
    _json(daily_run / "summary.json", daily_summary)
    _json(daily_run / "config.json", config)
    write_csv(daily_run / selected_policy / "portfolio_trades.csv", [_trade(daily_day, 200)])
    write_csv(daily_run / unfiltered_policy / "portfolio_trades.csv", [
        _trade(daily_day, 888_888, symbol="CONTROL"),
    ])
    daily_manifest = _manifest(daily_run, schema_version=daily.SCHEMA)

    _json(root / "latest" / "bounded_exit.json", dict(
        variant_id=history.VERSION,
        run_dir=history_run.relative_to(root).as_posix(),
        manifest_sha256=history_manifest, session_date=history_day,
        direction_filter_mode="nifty_both_counter_skip",
        selected_policy=selected_policy, execution_authority=False,
    ))
    _json(root / "latest" / "daily_backtest.json", dict(
        schema_version=daily.SCHEMA, variant_id=history.VERSION,
        run_dir=daily_run.relative_to(root).as_posix(),
        manifest_sha256=daily_manifest, session_date=daily_day,
        direction_filter_mode="nifty_both_counter_skip",
        selected_policy=selected_policy, execution_authority=False,
    ))

    monkeypatch.setattr(flow, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(flow, "active_profile_name", lambda: "V13_V10_G")
    monkeypatch.setattr(history, "verify", lambda run: json.loads((run / "summary.json").read_text()))
    monkeypatch.setattr(daily, "verify", lambda run: json.loads((run / "summary.json").read_text()))
    return dict(
        root=root, history_run=history_run, daily_run=daily_run,
        history_pointer=root / "latest" / "bounded_exit.json",
        strategy=strategy, version=history.VERSION, selected_policy=selected_policy,
    )


@pytest.mark.parametrize("family,strategy", CASES)
def test_verified_history_and_daily_replay_are_selectable_without_mixing_controls(
        tmp_path, monkeypatch, family, strategy):
    package = _install_family(tmp_path, monkeypatch, family, strategy)

    payload = flow.load_flow_data(strategy=strategy)
    family_runs = [run for run in payload["runs"] if run["strategy"] == strategy]
    assert [run["kind"] for run in family_runs] == ["backtest", "daily_replay"]
    assert payload["selected_run"]["id"] == family_runs[0]["id"]
    assert payload["selected_run"]["strategy_version"] == package["version"]
    assert payload["summary"]["trades"] == 1
    assert payload["summary"]["net_pnl"] == 100
    assert payload["trades"][0]["symbol"] == "SELECTED"
    assert payload["data_mode"] == f"Verified {strategy.removeprefix('V13-V10-')} backtest"

    daily = flow.load_flow_data(run_id=family_runs[1]["id"])
    assert daily["selected_run"]["kind"] == "daily_replay"
    assert daily["summary"]["sessions"] == 1
    assert daily["summary"]["period_start"] == "2026-10-22"
    assert daily["summary"]["trades"] == 1
    assert daily["summary"]["net_pnl"] == 200
    assert daily["trades"][0]["symbol"] == "SELECTED"
    assert str(tmp_path) not in json.dumps(payload)
    assert str(tmp_path) not in json.dumps(daily)
    if family == "g7":
        assert "V3" in payload["selected_run"]["source_label"]
        assert "V3" in daily["selected_run"]["source_label"]


@pytest.mark.parametrize("family,strategy", CASES)
def test_tampered_history_member_is_removed_while_verified_daily_remains(
        tmp_path, monkeypatch, family, strategy):
    package = _install_family(tmp_path, monkeypatch, family, strategy)
    assert flow.load_flow_data(strategy=strategy)["selected_run"]["kind"] == "backtest"

    selected_daily = package["history_run"] / package["selected_policy"] / "daily_results.csv"
    selected_daily.write_text("day,net_pnl\n2026-10-21,999999\n", encoding="utf-8")

    payload = flow.load_flow_data(strategy=strategy)
    family_runs = [run for run in payload["runs"] if run["strategy"] == strategy]
    assert [run["kind"] for run in family_runs] == ["daily_replay"]
    assert payload["selected_run"]["kind"] == "daily_replay"
    assert payload["summary"]["net_pnl"] == 200


@pytest.mark.parametrize("family,strategy", CASES)
def test_tampered_daily_ledger_is_removed_without_displacing_history(
        tmp_path, monkeypatch, family, strategy):
    package = _install_family(tmp_path, monkeypatch, family, strategy)
    selected_ledger = package["daily_run"] / package["selected_policy"] / "portfolio_trades.csv"
    selected_ledger.write_text("day,portfolio_net_profit_rupees\n2026-10-22,999999\n", encoding="utf-8")

    payload = flow.load_flow_data(strategy=strategy)
    family_runs = [run for run in payload["runs"] if run["strategy"] == strategy]
    assert [run["kind"] for run in family_runs] == ["backtest"]
    assert payload["selected_run"]["kind"] == "backtest"
    assert payload["summary"]["net_pnl"] == 100


@pytest.mark.parametrize("family,strategy", CASES)
@pytest.mark.parametrize("field,value", [
    ("execution_authority", True),
    ("run_dir", "../outside"),
    ("session_date", "2026-10-22"),
    ("selected_policy", "unfiltered-control"),
])
def test_invalid_history_pointer_never_inherits_verified_status(
        tmp_path, monkeypatch, family, strategy, field, value):
    package = _install_family(tmp_path, monkeypatch, family, strategy)
    pointer = json.loads(package["history_pointer"].read_text())
    pointer[field] = value
    _json(package["history_pointer"], pointer)

    payload = flow.load_flow_data(strategy=strategy)
    family_runs = [run for run in payload["runs"] if run["strategy"] == strategy]
    assert [run["kind"] for run in family_runs] == ["daily_replay"]
    assert payload["selected_run"]["kind"] == "daily_replay"

