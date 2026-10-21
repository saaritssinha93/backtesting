"""Read-only check of the newly published G3 Dashboard Flow package."""
import json
from pathlib import Path
import sys

from refresh_and_verify_dashboard import ROOT, fetch, server, worker_pids
from verify_published_apis import read, sha256, same_money

sys.path.insert(0, str(ROOT))
from eqidv2_runtime_paths import runtime_dir
from research import g3_target_cutoff_contract as contract


def main():
    root = runtime_dir("backtesting_result_v13_v10_g_3")
    pointer = read(root / "latest/current_backtest.json")
    run = (root / pointer["run_dir"]).resolve()
    assert run.is_relative_to(root.resolve())
    assert pointer["strategy_version"] == contract.STRATEGY_VERSION
    assert pointer["strategy_fingerprint"] == contract.strategy_fingerprint()
    assert sha256(run / "manifest.json") == pointer["manifest_sha256"]
    summary = read(run / "summary.json")
    config = read(run / "config.json")
    assert config == contract.strategy_payload()
    assert summary["active_setup_count"] == 51
    setup_targets = {}
    for clock, side in (("09:20", "SHORT"), ("10:10", "LONG")):
        setup = next(item for item in config["active_setups"] if item["signal_end"] == clock and item["side"] == side)
        assert setup["target_pct"] == 1.5
        setup_targets[clock + " " + side] = setup["target_pct"]
    dashboard = server()
    assert dashboard is not None
    workers = worker_pids()
    token = dashboard.environ().get("LOG_DASH_TOKEN")
    assert token
    flow = fetch(token, "/api/dashboard-flow", strategy="V13-V10-G-3")
    selected, totals = flow["selected_run"], flow["summary"]
    assert flow["state"] == "ready"
    assert selected["strategy_version"] == contract.STRATEGY_VERSION
    assert selected["strategy_fingerprint"] == contract.strategy_fingerprint()
    assert selected["run_name"] == run.name
    assert selected["active_setup_count"] == 51
    assert selected["verified"] and selected["integrity_verified"] and selected["strategy_matches_backtest"]
    assert selected["current_full_strategy"]
    for expected in ("09:20 SHORT", "10:10 LONG", "1.50%"):
        assert expected in selected["backtest_scope_text"]
    assert totals["sessions"] == len(flow["daily"]) == summary["sessions"]
    assert totals["period_end"] == summary["last_date"] == pointer["session_date"]
    for public, saved in (("trades", "executed_trades"), ("wins", "wins"), ("losses", "losses")):
        assert totals[public] == summary["current_g3"][saved]
    assert len(flow["trades"]) == totals["trades"]
    same_money(totals["net_pnl"], summary["current_g3"]["net_pnl_rs"], "Flow P&L mismatch")
    assert server().pid == dashboard.pid and worker_pids() == workers
    receipt = dict(status="PASS", read_only=True, workers_untouched=True, dashboard_pid=dashboard.pid,
        strategy_version=selected["strategy_version"], strategy_fingerprint=selected["strategy_fingerprint"],
        active_setup_count=51, run_name=selected["run_name"], setup_targets_pct=setup_targets,
        summary=totals, report_path=str(run / "report.html"),
        selection_audit_path=str(run / "enabled_two_slots_selection.csv"),
        report_access="Existing filesystem report and relative CSV links; no Dashboard Flow artifact route exists.")
    output = Path(__file__).with_name("flow_api_verification.json")
    output.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(dict(status="PASS", receipt=str(output), sessions=totals["sessions"],
        trades=totals["trades"], net_pnl=totals["net_pnl"], active_setup_count=51)))


if __name__ == "__main__":
    main()
