"""G3 profile isolation, committed production evidence and shadow safeguards."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from ai_platform.adapters.common import ArtifactError, file_sha256
from ai_platform.adapters.daily_replay import read_daily_replay
from ai_platform.api.registry import SourceRegistry
from ai_platform.api.repository import PlatformRepository
from ai_platform.observability.g3_shadow_gate import STATE, pending_g3_shadow
from ai_platform.observability.runtime_collector import RuntimeMetricsCollector
from fno_v13_v10_g_3_contract import STRATEGY_VERSION, config_fingerprint

ROOT=Path(__file__).resolve().parents[1]
DAY="2026-10-09"
NOW=datetime(2026,10,9,16,30,tzinfo=ZoneInfo("Asia/Kolkata"))


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value),encoding="utf-8")


def production(runtime, *, role="production_daily"):
    root=runtime/"backtesting_result_v13_v10_g_3"
    run=root/"production/runs"/DAY/"run1"
    run.mkdir(parents=True)
    ledger=run/"portfolio_trades.csv"
    ledger.write_text("day,net_pnl\n",encoding="utf-8")
    value=dict(schema_version="fno_g3_production_run_v1",strategy_version=STRATEGY_VERSION,
        strategy_fingerprint=config_fingerprint(),session_date=DAY,role=role,state="SUCCESS",complete=True,
        execution_authority=False,created_at=NOW.isoformat(),metrics={"trades":0,"net_pnl_rupees":0.},coverage={},
        artifacts={"portfolio_trades":{"path":ledger.name,"sha256":file_sha256(ledger)}})
    manifest=run/"run_manifest.json"
    write(manifest,value)
    latest=root/"latest/latest_backtesting_result_v13_v10_g_3.json"
    digest=file_sha256(manifest)
    write(latest,{**value,"run_manifest":str(manifest.resolve()),"manifest_sha256":digest})
    write(latest.parent/"current_run.json",dict(run_dir=run.relative_to(root).as_posix(),manifest_sha256=digest,
        role=role,strategy_version=STRATEGY_VERSION,strategy_fingerprint=config_fingerprint(),session_date=DAY))
    return latest,manifest,ledger


def read(path):
    return read_daily_replay(path,expected_strategy_version=STRATEGY_VERSION,expected_strategy_fingerprint=config_fingerprint())


def test_g3_zero_trade_production_manifest_is_valid(tmp_path):
    latest,_,_=production(tmp_path)
    result=read(latest)
    assert result.complete and result.metrics["trades"] == 0
    assert result.strategy_version == STRATEGY_VERSION


@pytest.mark.parametrize("role",["cumulative_history","FROZEN_RESEARCH_BASELINE","observer"])
def test_g3_daily_reader_rejects_nonproduction_roles(tmp_path,role):
    latest,_,_=production(tmp_path,role=role)
    with pytest.raises(ArtifactError,match="role"):
        read(latest)


def test_g3_requires_committed_identity_and_artifact_integrity(tmp_path):
    latest,_,ledger=production(tmp_path)
    original=ledger.read_bytes()
    ledger.write_text("changed",encoding="utf-8")
    with pytest.raises(ArtifactError,match="sha256"):
        read(latest)
    ledger.write_bytes(original)
    pointer=latest.parent/"current_run.json"
    value=json.loads(pointer.read_text())
    del value["strategy_fingerprint"]
    write(pointer,value)
    with pytest.raises(ArtifactError,match="fingerprint"):
        read(latest)


def test_g3_current_pointer_cannot_escape_its_production_root(tmp_path):
    latest,_,_=production(tmp_path)
    pointer=latest.parent/"current_run.json"
    value=json.loads(pointer.read_text())
    value["run_dir"]="../legacy_g"
    write(pointer,value)
    with pytest.raises(ArtifactError,match="leaves production"):
        read(latest)


def test_g3_rejects_legacy_schema_even_if_identity_is_relabelled(tmp_path):
    path=tmp_path/"legacy.json"
    write(path,{"session_date":DAY,"strategy_version":STRATEGY_VERSION,"strategy_fingerprint":config_fingerprint(),
        "status":"SUCCESS","result":{"schema_version":"fno_v13_v10_g_daily_replay_v1"}})
    with pytest.raises(ArtifactError,match="production-run schema"):
        read(path)


def test_active_g3_sources_and_repository_identity_have_no_g_fallback(tmp_path):
    sources=SourceRegistry(ROOT/"docs/ai_platform/source_registry.json",tmp_path,profile="V13_V10_G_3")
    legacy=SourceRegistry(ROOT/"docs/ai_platform/source_registry.json",tmp_path,profile="V13_V10_G")
    assert "v13_v10_g_3_live" in sources.path("equity_paper_orders").as_posix()
    assert "v13_v10_g_live" in legacy.path("equity_paper_orders").as_posix()
    write(legacy.path("daily_replay_latest_json"),{"status":"SUCCESS"})
    assert not sources.path("daily_replay_latest_json").exists()
    repo=PlatformRepository(sources,ROOT/"docs/ai_platform/profile_registry.json")
    assert repo._equity_identity() == (STRATEGY_VERSION,config_fingerprint())
    assert repo._option_identity()[1:] == (STRATEGY_VERSION,config_fingerprint())
    with pytest.raises((ArtifactError,FileNotFoundError)):
        repo.result_summary(date.fromisoformat(DAY))


def test_g3_metrics_use_g3_generation_and_do_not_accept_legacy_success(tmp_path):
    registry=ROOT/"docs/ai_platform/profile_registry.json"
    old=tmp_path/"backtesting_result_v13_v10_g/latest/latest_backtesting_result_v13_v10_g.json"
    write(old,{"status":"SUCCESS","session_date":DAY,"result":{"state":"SUCCESS","complete":True,"session_date":DAY}})
    collector=RuntimeMetricsCollector(tmp_path,profile="V13_V10_G_3",profile_registry_path=registry,
                                      now=lambda:NOW,cache_ttl_seconds=0)
    assert 'trading_replay_due{profile="v13-v10-g-3",replay_kind="daily"} 1' in collector.render_prometheus_samples()
    production(tmp_path)
    # Generated files use the actual wall-clock mtime, so semantic completion
    # timestamps provide the fixed test clock for both latest and manifests.
    for path in (tmp_path/"backtesting_result_v13_v10_g_3").rglob("*.json"):
        os.utime(path,(NOW.timestamp(),NOW.timestamp()))
    output=collector.render_prometheus_samples()
    assert 'trading_replay_due{profile="v13-v10-g-3",replay_kind="daily"} 0' in output
    assert 'profile="v13-v10-g"' not in output


def test_g3_pending_shadow_cannot_reuse_old_model_or_claim_prepared(tmp_path):
    legacy=tmp_path/"shadow_sessions"/DAY/"prepared_manifest.json"
    write(legacy,{"strategy_version":"G","completed":True})
    original=legacy.read_bytes()
    for operation in ("prepare","seal","finalize","verify"):
        result=pending_g3_shadow(session_date=date.fromisoformat(DAY),command=operation,output_root=tmp_path)
        assert result["state"] == STATE and not result["complete"]
        assert not result["model_loaded"] and not result["execution_authority"]
        assert "g3_prospective" in result["status_path"]
    assert legacy.read_bytes() == original


@pytest.mark.parametrize("tool,command",[("v13_shadow_automation.py","prepare"),("v13_prospective_shadow.py","prepare")])
def test_shadow_cli_active_g3_is_blocked_without_reading_legacy_inputs(tmp_path,tool,command):
    # Explicit profile works even before active deployment changes. Nonexistent
    # legacy model paths must never be opened by this branch.
    args=[sys.executable,str(ROOT/"tools"/tool),"--profile","V13_V10_G_3","--output-root",str(tmp_path/"shadow")]
    if tool == "v13_shadow_automation.py":args += ["--session-date",DAY,command]
    else:args += [command,"--session-date",DAY]
    result=subprocess.run(args,cwd=ROOT,capture_output=True,text=True)
    assert result.returncode == 2,result.stderr
    value=json.loads(result.stdout)
    assert value["state"] == STATE and value["strategy_version"] == STRATEGY_VERSION
