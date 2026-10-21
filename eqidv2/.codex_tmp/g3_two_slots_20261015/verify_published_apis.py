"""Verify newly published G3 history through authenticated read-only APIs.

Run only after the V12 full-history and dependent monitor reports are published.
This helper performs no refresh, worker action, regeneration, or polling.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import sys

from refresh_and_verify_dashboard import ROOT, fetch, server, worker_pids

sys.path.insert(0, str(ROOT))
from eqidv2_runtime_paths import runtime_dir
from research import g3_target_cutoff_contract as contract


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def same_money(left, right, label):
    assert math.isclose(float(left), float(right), abs_tol=.02, rel_tol=1e-9), label


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--through", default="2026-10-14")
    parser.add_argument("--receipt", type=Path,
        default=Path(__file__).with_name("published_api_verification.json"))
    args = parser.parse_args()
    expected_version = "V13-V10-G-3-BACKTEST-TARGET-GT075-RESTORED3-V12"
    assert contract.STRATEGY_VERSION == expected_version, "Current backtest is not V12"
    expected_fingerprint = contract.strategy_fingerprint()
    result_root = runtime_dir("backtesting_result_v13_v10_g_3").resolve()
    pointer = read(result_root / "latest/current_backtest.json")
    source = (result_root / pointer["run_dir"]).resolve()
    assert source.is_relative_to(result_root), "Published source escaped expected root"
    assert pointer["strategy_version"] == expected_version, "V12 history has not been published"
    assert pointer["strategy_fingerprint"] == expected_fingerprint
    assert pointer["session_date"] == args.through
    assert sha256(source / "manifest.json") == pointer["manifest_sha256"]
    manifest = read(source / "manifest.json")
    for name in ("summary.json", "config.json", "portfolio_trades.csv", "daily_results.csv"):
        assert sha256(source / name) == manifest["files"][name], "Source artifact hash mismatch: " + name
    summary = read(source / "summary.json")
    config = read(source / "config.json")
    assert config == contract.strategy_payload(), "Published setup rules differ from current V12"
    assert summary["strategy_version"] == expected_version
    assert summary["strategy_fingerprint"] == expected_fingerprint
    assert summary["active_setup_count"] == len(config["active_setups"]) == 51
    assert summary["last_date"] == args.through
    specifications = {}
    for clock, side, confirm in (("09:20", "SHORT", "09:21"), ("10:10", "LONG", "10:11")):
        matches = [row for row in config["active_setups"] if row["signal_end"] == clock and row["side"] == side]
        assert len(matches) == 1, "New setup is missing or duplicated"
        setup = matches[0]
        assert setup["target_pct"] == 1.5 and setup["confirmation_end"] == confirm
        specifications[clock + " " + side] = dict(target_pct=setup["target_pct"], confirmation_end=confirm,
            max_entries=setup["max_entries"], picker=setup["picker"])

    workers_before = worker_pids()
    dashboard = server()
    assert dashboard is not None, "Dashboard is not running"
    dashboard_pid = dashboard.pid
    token = dashboard.environ().get("LOG_DASH_TOKEN")
    assert token, "Dashboard authentication unavailable"
    print("Verifying published V12 dashboard flow", flush=True)
    flow = fetch(token, "/api/dashboard-flow", strategy="V13-V10-G-3")
    selected = flow["selected_run"]
    assert flow["state"] == "ready"
    assert selected["strategy_version"] == expected_version
    assert selected["strategy_fingerprint"] == expected_fingerprint
    assert selected["active_setup_count"] == 51
    assert selected["run_name"] == source.name, "Dashboard selected a different historical package"
    assert selected["verified"] and selected["integrity_verified"] and selected["strategy_matches_backtest"]
    assert selected["current_full_strategy"]
    assert "09:20 SHORT" in selected["backtest_scope_text"]
    assert "10:10 LONG" in selected["backtest_scope_text"] and "1.50%" in selected["backtest_scope_text"]
    totals = summary["current_g3"]
    flow_totals = flow["summary"]
    assert flow_totals["sessions"] == summary["sessions"] == len(flow["daily"])
    assert flow_totals["period_end"] == args.through
    for output_key, source_key in (("trades", "executed_trades"), ("wins", "wins"), ("losses", "losses")):
        assert flow_totals[output_key] == totals[source_key], "Flow count mismatch: " + output_key
    assert len(flow["trades"]) == totals["executed_trades"]
    same_money(flow_totals["net_pnl"], totals["net_pnl_rs"], "Flow net P&L differs from verified source")

    print("Verifying dependent monitor report uses the same V12 source", flush=True)
    monitor = fetch(token, "/api/g3-trade-monitor/backtest")
    assert monitor["state"] == "VERIFIED", "Dependent monitor research is not verified"
    assert monitor["read_only"] is True and monitor["execution_authority"] is False
    monitor_summary = monitor["summary"]
    assert monitor_summary["source_strategy_version"] == expected_version
    assert monitor_summary["source_strategy_fingerprint"] == expected_fingerprint
    assert monitor_summary["source_active_setup_count"] == 51
    # The dashboard intentionally redacts absolute local paths. Bind the API
    # by its manifest hash and check the path in the sealed local artifact.
    monitor_root = runtime_dir("v13_v10_g_3_trade_monitor_backtest").resolve()
    monitor_pointer = read(monitor_root / "latest/current_run.json")
    monitor_run = (monitor_root / monitor_pointer["run_dir"]).resolve()
    assert monitor_run.is_relative_to(monitor_root)
    assert sha256(monitor_run / "manifest.json") == monitor_pointer["manifest_sha256"]
    monitor_manifest = read(monitor_run / "manifest.json")
    assert sha256(monitor_run / "summary.json") == monitor_manifest["files"]["summary.json"]
    saved_monitor_summary = read(monitor_run / "summary.json")
    assert Path(saved_monitor_summary["source"]["run_dir"]).resolve() == source
    assert monitor_summary["source"]["manifest_sha256"] == pointer["manifest_sha256"]
    assert monitor_summary["sessions"] == summary["sessions"]
    assert monitor_summary["days"] == summary["days"]
    assert monitor_summary["baseline_native_parity"] is True
    assert monitor_summary["baseline_allocation_parity"] is True
    baseline = next(row for row in monitor["policy_results"] if row["period"] == "all" and row["policy"] == "baseline")
    assert int(baseline["executed_trades"]) == totals["executed_trades"]
    assert int(baseline["selected_orders"]) == totals["selected_orders"]
    same_money(baseline["net_pnl_rs"], totals["net_pnl_rs"], "Monitor baseline net P&L differs")
    exported_summary = fetch(token, "/g3-trade-monitor-backtest/summary.json")
    assert exported_summary == monitor_summary, "Monitor report artifact differs from API summary"
    after = server()
    assert after is not None and after.pid == dashboard_pid, "Dashboard process changed during verification"
    assert worker_pids() == workers_before, "G3 worker inventory changed during verification"
    receipt = dict(status="PASS", read_only=True, dashboard_pid=dashboard_pid,
        workers_untouched=True, strategy_version=expected_version, strategy_fingerprint=expected_fingerprint,
        active_setup_count=51, published_run=str(source), session_count=summary["sessions"],
        period_start=summary["first_date"], period_end=args.through, specifications=specifications,
        flow_totals=flow_totals, monitor_baseline=dict(selected_orders=int(baseline["selected_orders"]),
            executed_trades=int(baseline["executed_trades"]), net_pnl_rs=float(baseline["net_pnl_rs"])),
        monitor_report_url=monitor["report_url"], monitor_policy_rows=len(monitor["policy_results"]),
        monitor_daily_rows=len(monitor["daily_comparison"]))
    args.receipt.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(dict(status="PASS", receipt=str(args.receipt), active_setup_count=51,
        sessions=summary["sessions"], executed_trades=totals["executed_trades"])), flush=True)


if __name__ == "__main__":
    main()
