"""Historical aggregation and allowlisted discovery contracts for Dashboard Flow."""

import csv
import json

import pytest

import dashboard_flow as flow


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    monkeypatch.setattr(flow, "DATA_ROOT", tmp_path)
    return tmp_path


def make_run(root, name="20261008T164833"):
    return root / "backtesting_result_v13_v10_g" / "runs" / "2026-10-08" / name


def test_aggregates_real_daily_totals_and_net_trade_profit_factor(data_root):
    run = make_run(data_root)
    write_csv(run / "daily_results.csv", [
        dict(day="2026-10-01", trades=2, wins=1, losses=1, net_pnl=70, gross_pnl=80, cost=10),
        dict(day="2026-10-02", trades=1, wins=0, losses=1, net_pnl=-20, gross_pnl=-15, cost=5),
    ])
    write_csv(run / "trades_with_pullbacks.csv", [
        dict(trade_id="one", day="2026-10-01", tradingsymbol="AAA", side="LONG", net_pnl=100, gross_pnl=105, cost=5),
        dict(trade_id="two", day="2026-10-01", tradingsymbol="BBB", side="SHORT", net_pnl=-30, gross_pnl=-25, cost=5),
        dict(trade_id="three", day="2026-10-02", tradingsymbol="CCC", side="LONG", net_pnl=-20, gross_pnl=-15, cost=5),
    ])
    payload = flow.load_flow_data()
    summary = payload["summary"]
    assert payload["state"] == "ready"
    assert summary["net_pnl"] == 50
    assert summary["gross_pnl"] - summary["cost"] == summary["net_pnl"]
    assert summary["profit_factor"] == 2  # 100 / (30 + 20), not a daily PF.
    assert summary["max_drawdown"] == 20
    assert summary["trades"] == 3 and summary["wins"] == 1
    assert summary["win_rate_pct"] == pytest.approx(100 / 3)
    assert summary["long_trades"] == 2 and summary["short_trades"] == 1
    assert [row["cumulative_net_pnl"] for row in payload["daily"]] == [70, 50]
    assert not payload["warnings"]
    assert str(data_root) not in json.dumps(payload)


def test_portfolio_executed_rows_and_portfolio_values_take_precedence(data_root):
    run = make_run(data_root)
    write_csv(run / "daily_results.csv", [dict(day="2026-10-01", trades=1, wins=0, losses=1, net_pnl_rupees=-10)])
    write_csv(run / "portfolio_trades.csv", [
        dict(day="2026-10-01", tradingsymbol="AAA", filled="True", portfolio_executed="True", net_profit_rupees=500, portfolio_net_profit_rupees=-10),
        dict(day="2026-10-01", tradingsymbol="BBB", filled="True", portfolio_executed="False", net_profit_rupees=1000, portfolio_net_profit_rupees=0),
        dict(day="2026-10-01", tradingsymbol="CCC", filled="False", portfolio_executed="False", net_profit_rupees=0, portfolio_net_profit_rupees=0),
    ])
    payload = flow.load_flow_data()
    assert len(payload["trades"]) == 1
    assert payload["trades"][0]["net_pnl"] == -10
    assert payload["summary"]["profit_factor"] == 0


def test_all_three_families_are_selectable_without_mixing_results(data_root):
    for name in ("20261008T100000", "20261008T160000"):
        write_csv(make_run(data_root, name) / "daily_results.csv", [dict(day="2026-10-01", trades=0, wins=0, losses=0, net_pnl=0)])
    for relative, filename, pnl in (
        ("backtesting_result_v13_v10_g_3/runs/2026-10-01/20261001_older", "trades_with_pullbacks.csv", 100),
        ("backtesting_result_v13_v10_g_3/runs/2099-12-31/20991231_future", "trades_with_pullbacks.csv", 300),
        ("fno_oi/strategy_research/v13_corrected_v10_g_2/run_20991231_future", "portfolio_trades.csv", 200),
    ):
        write_csv(data_root / relative / "daily_results.csv", [dict(day="2026-10-01", trades=1, wins=1, losses=0, net_pnl=pnl)])
        write_csv(data_root / relative / filename, [dict(day="2026-10-01", tradingsymbol="AAA", net_pnl=pnl)])
    write_csv(data_root / "unrelated/daily_results.csv", [dict(day="2026-10-01", net_pnl=99999)])
    payload = flow.load_flow_data()
    assert len(payload["runs"]) == 5
    assert payload["selected_run"]["strategy"] == "V13-V10-G-3"
    assert payload["selected_run"]["run_name"] == "20991231_future"
    assert [run["strategy"] for run in payload["runs"]] == [
        "V13-V10-G-3", "V13-V10-G-3", "V13-V10-G-2", "V13-V10-G", "V13-V10-G",
    ]
    assert [run["run_name"] for run in payload["runs"][-2:]] == ["20261008T160000", "20261008T100000"]
    for strategy, expected in (("V13-V10-G-2", 200), ("V13-V10-G-3", 300)):
        run = next(run for run in payload["runs"] if run["strategy"] == strategy)
        selected = flow.load_flow_data(run["id"])
        assert run["kind"] == "backtest"
        assert selected["summary"]["net_pnl"] == expected
        assert len(selected["trades"]) == 1
    other = payload["runs"][1]["id"]
    assert flow.load_flow_data(other)["selected_run"]["id"] == other
    for value in ("../unrelated", str(make_run(data_root)), "missing-run"):
        with pytest.raises(ValueError, match="Unknown dashboard run"):
            flow.load_flow_data(value)


def make_full_run(root, name, trades):
    run = root / "fno_oi/strategy_research/v13_v10_g_full_history" / name
    write_csv(run / "g_backtest/portfolio_trades.csv", trades)
    (run / "g_backtest/run_metadata.json").write_text(json.dumps({
        "first_session": "2026-10-01", "last_session": "2026-10-05", "session_count": 3,
    }), encoding="utf-8")
    write_csv(run / "dataset/source_session_eligibility.csv", [
        dict(day="2026-10-01", eligible=True),
        dict(day="2026-10-02", eligible=True),
        dict(day="2026-10-03", eligible=False),
        dict(day="2026-10-05", eligible=True),
    ])
    return run


def test_latest_full_history_is_default_when_only_g_exists_and_keeps_zero_trade_sessions(data_root):
    trades = [
        dict(day="2026-10-01", tradingsymbol="AAA", side="LONG", portfolio_executed=True, portfolio_net_profit_rupees=100, portfolio_gross_profit_rupees=105, portfolio_cost_rupees=5),
        dict(day="2026-10-05", tradingsymbol="BBB", side="SHORT", portfolio_executed=True, portfolio_net_profit_rupees=-25, portfolio_gross_profit_rupees=-20, portfolio_cost_rupees=5),
        dict(day="2026-10-05", tradingsymbol="CCC", side="LONG", portfolio_executed=False, portfolio_net_profit_rupees=500, portfolio_gross_profit_rupees=505, portfolio_cost_rupees=5),
    ]
    for name in ("run_20261001_full", "run_20261008_full"):
        make_full_run(data_root, name, trades)
    write_csv(make_run(data_root, "20261009T160000") / "daily_results.csv", [
        dict(day="2026-10-09", trades=0, wins=0, losses=0, net_pnl=0),
    ])
    payload = flow.load_flow_data()
    assert payload["selected_run"]["run_name"] == "run_20261008_full"
    assert payload["selected_run"]["kind"] == "full_history"
    assert len(payload["runs"]) == 3
    assert [row["date"] for row in payload["daily"]] == ["2026-10-01", "2026-10-02", "2026-10-05"]
    assert [row["net_pnl"] for row in payload["daily"]] == [100, 0, -25]
    assert [row["cumulative_net_pnl"] for row in payload["daily"]] == [100, 100, 75]
    assert payload["summary"]["sessions"] == 3
    assert payload["summary"]["flat_days"] == 1
    assert payload["summary"]["trades"] == 2
    assert payload["summary"]["net_pnl"] == 75
    assert payload["summary"]["gross_pnl"] == 85
    assert payload["summary"]["cost"] == 10
    assert payload["summary"]["profit_factor"] == 4
    assert payload["summary"]["max_drawdown"] == 25
    assert not payload["warnings"]
    daily_run = next(run for run in payload["runs"] if run["kind"] == "daily")
    assert flow.load_flow_data(daily_run["id"])["summary"]["period_end"] == "2026-10-09"
    assert str(data_root) not in json.dumps(payload)


def test_incomplete_full_history_session_export_is_not_discovered(data_root):
    run = make_full_run(data_root, "run_20261008_full", [dict(day="2026-10-01", net_pnl=100)])
    write_csv(run / "dataset/source_session_eligibility.csv", [dict(day="2026-10-01", eligible=True)])
    assert flow.load_flow_data()["state"] == "empty"
    (run / "g_backtest/run_metadata.json").unlink()
    assert flow.load_flow_data()["runs"] == []


def test_full_history_missing_pnl_is_not_replaced_by_zero(data_root):
    make_full_run(data_root, "run_20261008_full", [
        dict(day="2026-10-01", tradingsymbol="AAA", portfolio_executed=True, portfolio_net_profit_rupees="NaN"),
    ])
    payload = flow.load_flow_data()
    assert payload["daily"][0]["net_pnl"] is None
    assert payload["daily"][1]["net_pnl"] == 0
    assert payload["summary"]["net_pnl"] is None
    assert payload["summary"]["profit_factor"] is None
    json.dumps(payload, allow_nan=False)


def test_empty_data_is_explicit_and_json_serializable(data_root):
    payload = flow.load_flow_data()
    assert payload["state"] == "empty"
    assert payload["selected_run"] is None and payload["runs"] == []
    assert payload["daily"] == [] and payload["trades"] == []
    assert payload["summary"]["profit_factor"] is None
    assert payload["warnings"]
    json.dumps(payload, allow_nan=False)


def test_non_finite_evidence_is_never_fabricated_as_zero(data_root):
    run = make_run(data_root)
    write_csv(run / "daily_results.csv", [
        dict(day="2026-10-01", trades=1, wins=1, losses=0, net_pnl="NaN", gross_pnl="Infinity", cost=5),
        dict(day="2026-10-02", trades=1, wins=0, losses=1, net_pnl=-20, gross_pnl=-15, cost=5),
    ])
    write_csv(run / "trades_with_pullbacks.csv", [dict(day="2026-10-01", net_pnl="-Infinity", entry_price="NaN")])
    payload = flow.load_flow_data()
    assert payload["summary"]["net_pnl"] is None
    assert payload["summary"]["max_drawdown"] is None
    assert payload["summary"]["profit_factor"] is None
    assert payload["daily"][0]["net_pnl"] is None
    assert payload["daily"][1]["cumulative_net_pnl"] is None
    assert payload["trades"][0]["entry_price"] is None
    json.dumps(payload, allow_nan=False)


def test_unreconciled_trade_ledger_does_not_claim_complete_profit_factor(data_root):
    run = make_run(data_root)
    write_csv(run / "daily_results.csv", [dict(day="2026-10-01", trades=2, wins=1, losses=1, net_pnl=10)])
    write_csv(run / "trades_with_pullbacks.csv", [dict(day="2026-10-01", net_pnl=20)])
    payload = flow.load_flow_data()
    assert payload["summary"]["net_pnl"] == 10
    assert payload["summary"]["profit_factor"] is None
    assert any("reconcile" in warning for warning in payload["warnings"])


def test_launcher_uses_loopback_and_keeps_ephemeral_token_out_of_console(monkeypatch, capsys):
    from urllib.parse import parse_qs, urlparse
    from tools import run_dashboard_flow as launcher

    observed = {}

    class Server:
        def __init__(self, address, handler):
            observed["address"] = address
            observed["handler"] = handler
            observed["server"] = self

        def serve_forever(self, **kwargs):
            raise KeyboardInterrupt

        def server_close(self):
            observed["closed"] = True

    monkeypatch.setattr(launcher, "ThreadingHTTPServer", Server)
    monkeypatch.setattr(launcher.webbrowser, "open", lambda url, **kwargs: observed.setdefault("url", url))
    monkeypatch.setattr(launcher.sys, "argv", ["run_dashboard_flow.py"])
    for name in ("LOG_DASH_TOKEN", "LOG_DASH_USER", "LOG_DASH_PASS"):
        monkeypatch.delenv(name, raising=False)
    assert launcher.main() == 0
    token = parse_qs(urlparse(observed["url"]).query)["token"][0]
    assert len(token) >= 32
    assert observed["server"].api_token == token
    assert token not in capsys.readouterr().out
    assert observed["address"] == ("127.0.0.1", 8790)
    assert observed["closed"]
