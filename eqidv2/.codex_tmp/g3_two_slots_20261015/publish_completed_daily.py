"""Publish the already verified daily member used by the new full history."""
import json
from pathlib import Path
import sys

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT))
from research import g3_target_cutoff_backtest as history
from research import g3_target_cutoff_contract as contract
import backtesting_result_v13_v10_g_3_daily as publisher


def main():
    root = publisher.DEFAULT_OUTPUT_ROOT.resolve()
    pointer = history._read(root / "latest/current_backtest.json")
    directory = history._child(root, pointer["run_dir"])
    assert history.sha256(directory / "manifest.json") == pointer["manifest_sha256"]
    summary, _ = history.verified_package(directory, root)
    assert summary["strategy_fingerprint"] == contract.strategy_fingerprint()
    assert summary["last_date"] == "2026-10-14"
    member, payload = history._checked_reference(root, summary["daily_sources"][-1])
    assert payload["session_date"] == "2026-10-14" and payload["complete"] is True
    with publisher._lock(root):
        daily = publisher.publish_result(root, member, payload)
        cumulative = publisher.refresh_history(root, member, payload)
    receipt = dict(status="PASS", reused_sealed_daily_source=True,
        session_date=payload["session_date"], strategy_version=payload["strategy_version"],
        strategy_fingerprint=payload["strategy_fingerprint"], daily_run=str(member),
        daily_report=daily["report_markdown"], daily_metrics=payload["metrics"],
        cumulative_history_run=cumulative["run_dir"] if cumulative else None,
        historical_run=str(directory), historical_report=str(directory / "report.html"),
        historical_sessions=summary["sessions"], historical_setup_count=summary["active_setup_count"],
        new_slots=summary["enabled_two_slots"]["selected_by_setup"])
    output = Path(__file__).parent / "backtest_verification.json"
    output.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(receipt), flush=True)


if __name__ == "__main__":
    main()
