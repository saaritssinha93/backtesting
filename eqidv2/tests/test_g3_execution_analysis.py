"""G3 execution diagnostics never inherit G calibration or claim readiness."""
from __future__ import annotations

import csv
import hashlib
import json
from datetime import date, datetime
from pathlib import Path

import pytest

from ai_platform.observability.g3_execution_analysis import (
    REPORT_FILENAME, collect_g3_paper_calibration, generate_g3_execution_analysis,
)
from fno_v13_v10_g_3_contract import IST, STRATEGY_VERSION, canonical_signal_id, strategy_fingerprint

DAY=date(2026,10,9)
NOW=datetime(2026,10,9,16,30,tzinfo=IST)


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value),encoding="utf-8")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def identity(symbol="AAA",side="LONG"):
    return canonical_signal_id(STRATEGY_VERSION,DAY,"09:30","09:31",side,symbol)


def order(symbol="AAA",side="LONG",**overrides):
    row=dict(schema_version="fno_v6_equity_order_state_v2",mode="PAPER",status="CLOSED",
        strategy_version=STRATEGY_VERSION,strategy_fingerprint=strategy_fingerprint(),
        session_date=str(DAY),signal_end="09:30",confirmation_end="09:31",tradingsymbol=symbol,side=side,
        signal_id=identity(symbol,side),entry_at_ist="2026-10-09T09:32:25+05:30",
        entry_order_activated_at_ist="2026-10-09T09:31:30+05:30",trigger_price=100.,
        entry_price=100.2 if side=="LONG" else 99.8)
    row.update(overrides)
    return row


@pytest.fixture
def package(tmp_path):
    source=tmp_path/"backtesting_result_v13_v10_g_3"
    run=source/"history/runs"/str(DAY)/"r1"
    run.mkdir(parents=True)
    row=dict(day=str(DAY),signal_id=identity(),side="LONG",setup_id="0931_LONG",tradingsymbol="AAA",
        strategy_version=STRATEGY_VERSION,strategy_fingerprint=strategy_fingerprint(),filled=True,
        portfolio_executed=True,portfolio_net_profit_rupees=125.)
    ledger=run/"portfolio_trades.csv"
    with ledger.open("w",newline="",encoding="utf-8") as stream:
        writer=csv.DictWriter(stream,fieldnames=list(row))
        writer.writeheader(); writer.writerow(row)
    daily=run/"daily_results.csv"; daily.write_text("day,trades,net_pnl_rupees\n2026-10-09,1,125\n",encoding="utf-8")
    original=run/"source_manifest.json"; write(original,{"strategy_contract":"fixture"})
    artifacts={key:dict(path=path.name,sha256=sha(path)) for key,path in
        (("portfolio_trades",ledger),("daily_results",daily),("source_manifest",original))}
    payload=dict(schema_version="fno_g3_production_run_v1",execution_authority=False,role="cumulative_history",
        strategy_version=STRATEGY_VERSION,strategy_fingerprint=strategy_fingerprint(),session_date=str(DAY),
        complete=True,state="SUCCESS",coverage={},metrics=dict(trades=1,net_pnl_rupees=125.),artifacts=artifacts)
    manifest=run/"run_manifest.json"; write(manifest,payload)
    write(source/"latest/current_history.json",dict(run_dir=run.relative_to(source).as_posix(),
        manifest_sha256=sha(manifest),role="cumulative_history",strategy_version=STRATEGY_VERSION,
        strategy_fingerprint=strategy_fingerprint(),session_date=str(DAY)))
    return source


def test_missing_paper_produces_expected_report_with_pending_state(package,tmp_path):
    output=tmp_path/"execution"
    value=generate_g3_execution_analysis(source_root=package,paper_root=tmp_path/"absent",output_root=output,now=NOW)
    assert value["state"] == "AWAITING_G3_PAPER_CALIBRATION"
    assert value["paper_calibration"]["fills"] == 0
    assert not value["calibrated"] and not value["safe_for_live_selection"] and not value["execution_authority"]
    assert value["path_stress"]["state"] == "NOT_RUN" and value["scenarios"] == []
    manifest=json.loads((output/"latest/manifest.json").read_text())
    assert manifest["reports"][REPORT_FILENAME]["sha256"] == sha(output/"latest"/REPORT_FILENAME)
    assert not list((output/"latest").glob("*research_baseline*"))


def test_real_g3_paper_measurements_use_supplied_root_and_preserve_sources(package,tmp_path):
    paper=tmp_path/"custom_paper"
    paths=[]
    for name,row in (("long",order()),("short",order("BBB","SHORT")),
        ("cancelled",order("CCC",status="CANCELLED",entry_at_ist=None,entry_price=None))):
        path=paper/str(DAY)/(name+".json"); write(path,row); paths.append(path)
    hashes={p:sha(p) for p in paths}
    value=generate_g3_execution_analysis(source_root=package,paper_root=paper,output_root=tmp_path/"analysis",now=NOW)
    stats=value["paper_calibration"]
    assert stats["terminal_orders"] == 3 and stats["fills"] == 2
    assert stats["activation_latency_seconds"]["mean"] == 30.
    assert stats["activation_to_fill_seconds"]["mean"] == 55.
    assert stats["trigger_to_fill_distance_bps"]["mean"] == pytest.approx(20.)
    assert value["source_overlap"]["filled_paper_signals_in_historical_ledger"] == 1
    assert value["state"] == "INSUFFICIENT_G3_PATH_STRESS_EVIDENCE"
    assert not stats["calibrated"]
    assert hashes == {p:sha(p) for p in paths}


@pytest.mark.parametrize("changes,reason",[
    ({"strategy_version":"G"},"WRONG_SCHEMA_PROFILE_FINGERPRINT_OR_MODE"),
    ({"strategy_fingerprint":"old-G"},"WRONG_SCHEMA_PROFILE_FINGERPRINT_OR_MODE"),
    ({"mode":"LIVE"},"WRONG_SCHEMA_PROFILE_FINGERPRINT_OR_MODE"),
    ({"status":"OPEN"},"NONTERMINAL_STATE"),
    ({"signal_id":"old_G_ID"},"INVALID_OR_CHANGING_ORDER_STATE"),
    ({"entry_price":0},"INVALID_OR_CHANGING_ORDER_STATE"),
    ({"entry_at_ist":"2026-10-09T09:30:00+05:30"},"INVALID_OR_CHANGING_ORDER_STATE"),
])
def test_mixed_or_invalid_paper_never_establishes_calibration(tmp_path,changes,reason):
    write(tmp_path/"order.json",order(**changes))
    value=collect_g3_paper_calibration(tmp_path,now=NOW)
    assert value["fills"] == 0 and value["terminal_orders"] == 0
    assert value["excluded_artifact_counts"][reason] == 1
    assert value["activation_latency_seconds"]["mean"] is None


def test_current_partial_session_does_not_enter_calibration(tmp_path):
    write(tmp_path/"order.json",order())
    value=collect_g3_paper_calibration(tmp_path,now=datetime(2026,10,9,12,tzinfo=IST))
    assert value["fills"] == 0
    assert value["excluded_artifact_counts"]["SESSION_NOT_CLOSED"] == 1


def test_contradictory_duplicate_identity_fails_closed(package,tmp_path):
    paper=tmp_path/"paper"
    write(paper/"one.json",order())
    write(paper/"two.json",order(entry_price=102.))
    output=tmp_path/"out"
    with pytest.raises(RuntimeError,match="Conflicting"):
        generate_g3_execution_analysis(source_root=package,paper_root=paper,output_root=output,now=NOW)
    assert not output.exists()


def test_bad_historical_package_never_publishes_pending_success(package,tmp_path):
    (package/"history/runs"/str(DAY)/"r1/portfolio_trades.csv").write_text("tampered")
    output=tmp_path/"out"
    with pytest.raises(ValueError,match="integrity"):
        generate_g3_execution_analysis(source_root=package,paper_root=None,output_root=output,now=NOW)
    assert not output.exists()


def _strategy_analysis(package, tmp_path, execution):
    from ai_platform.observability.g3_analysis import generate_g3_analysis
    return generate_g3_analysis(source_root=package, output_root=tmp_path / "strategy",
                                execution_research_root=execution)


def test_strategy_reports_link_verified_g3_paper_counts_but_never_claim_calibration(package, tmp_path):
    paper, execution = tmp_path / "paper", tmp_path / "execution"
    write(paper / "filled.json", order())
    write(paper / "cancelled.json", order("BBB", status="CANCELLED", entry_at_ist=None, entry_price=None))
    diagnostics = generate_g3_execution_analysis(source_root=package, paper_root=paper, output_root=execution, now=NOW)
    result = _strategy_analysis(package, tmp_path, execution)
    linked = result["execution_diagnostics"]
    assert linked["verified"] and linked["state"] == "INSUFFICIENT_G3_PATH_STRESS_EVIDENCE"
    assert linked["paper_terminal_orders"] == 2 and linked["paper_fills"] == 1
    assert linked["paper_session_dates"] == [str(DAY)]
    assert linked["source_manifest_sha256"] == diagnostics["source_manifest_sha256"]
    assert linked["calibrated"] is False and linked["execution_authority"] is False
    for suffix in ("observability_entry_execution", "research_improvements"):
        report = (Path(result["run_dir"]) / f"latest_fno_v13_v10_g_3_{suffix}.md").read_text()
        assert "State: **INSUFFICIENT_G3_PATH_STRESS_EVIDENCE**" in report
        assert "| 2 | 1 | 1 |" in report
        assert "not reconciled complete sessions" in report


def test_strategy_reports_distinguish_verified_zero_paper_from_missing_diagnostics(package, tmp_path):
    execution = tmp_path / "execution"
    missing = _strategy_analysis(package, tmp_path, execution)["execution_diagnostics"]
    assert missing["state"] == "AWAITING_G3_EXECUTION_DIAGNOSTICS" and missing["paper_fills"] is None
    generate_g3_execution_analysis(source_root=package, paper_root=None, output_root=execution, now=NOW)
    pending = _strategy_analysis(package, tmp_path, execution)["execution_diagnostics"]
    assert pending["state"] == "AWAITING_G3_PAPER_CALIBRATION" and pending["paper_fills"] == 0
    assert pending["verified"] is True and pending["calibrated"] is False


@pytest.mark.parametrize("corruption,expected", [
    ("stale_history", "STALE_G3_EXECUTION_EVIDENCE"),
    ("other_source_run", "STALE_G3_EXECUTION_EVIDENCE"),
    ("old_g_fingerprint", "INVALID_G3_EXECUTION_EVIDENCE"),
    ("old_g_schema", "INVALID_G3_EXECUTION_EVIDENCE"),
    ("authority", "INVALID_G3_EXECUTION_EVIDENCE"),
    ("ready", "INVALID_G3_EXECUTION_EVIDENCE"),
    ("calibrated", "INVALID_G3_EXECUTION_EVIDENCE"),
    ("count_mismatch", "INVALID_G3_EXECUTION_EVIDENCE"),
    ("immutable_manifest", "INVALID_G3_EXECUTION_EVIDENCE"),
    ("report", "INVALID_G3_EXECUTION_EVIDENCE"),
    ("latest_report", "INVALID_G3_EXECUTION_EVIDENCE"),
    ("report_escape", "INVALID_G3_EXECUTION_EVIDENCE"),
    ("run_escape", "INVALID_G3_EXECUTION_EVIDENCE"),
])
def test_strategy_rejects_stale_or_invalid_execution_without_old_g_fallback(package, tmp_path, corruption, expected):
    execution, paper = tmp_path / "execution", tmp_path / "paper"
    write(paper / "filled.json", order())
    value = generate_g3_execution_analysis(source_root=package, paper_root=paper, output_root=execution, now=NOW)
    run = Path(value["run_dir"])
    if corruption == "stale_history": value["source_manifest_sha256"] = "0" * 64
    if corruption == "other_source_run": value["source_run"] = str(tmp_path / "different_history")
    if corruption == "old_g_fingerprint": value["strategy_fingerprint"] = "old-G"
    if corruption == "old_g_schema": value["schema_version"] = "v13_v10_g_execution_research_v1"
    if corruption == "authority": value["execution_authority"] = True
    if corruption == "ready": value["state"] = "READY"
    if corruption == "calibrated": value["paper_calibration"]["calibrated"] = True
    if corruption == "count_mismatch": value["paper_calibration"]["fills"] = 999
    if corruption == "report_escape": value["reports"][REPORT_FILENAME]["path"] = "../../outside.md"
    if corruption == "run_escape": value["run_dir"] = str(tmp_path / "outside")
    write(run / "manifest.json", value)
    write(execution / "latest/manifest.json", value)
    if corruption == "immutable_manifest": write(run / "manifest.json", {})
    if corruption == "report": (run / REPORT_FILENAME).write_text("tampered")
    if corruption == "latest_report": (execution / "latest" / REPORT_FILENAME).write_text("tampered")
    old = tmp_path / "v13_v10_g_execution_research/latest/manifest.json"
    write(old, {"state": "READY", "calibrated": True, "paper_calibration": {"fills": 9999}})
    result = _strategy_analysis(package, tmp_path, execution)
    linked = result["execution_diagnostics"]
    assert linked["state"] == expected and linked["verified"] is False
    assert linked["paper_fills"] is None and linked["calibrated"] is False
    assert linked["execution_authority"] is False
    report = (Path(result["run_dir"]) / "latest_fno_v13_v10_g_3_observability_entry_execution.md").read_text()
    assert "9999" not in report and "| UNAVAILABLE | UNAVAILABLE | UNAVAILABLE |" in report
