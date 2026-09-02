from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "bat" / "run_fno_v10_v11_v12_paper_session.bat"
INSTALLER = ROOT / "bat" / "schedule_fno_v10_v11_v12_paper_weekday.ps1"


def _source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_runner_is_frozen_to_one_paper_session() -> None:
    source = _source(RUNNER)

    assert 'set "FNO_MULTI_PAPER_EXECUTION_MODE=PAPER"' in source
    assert 'set "FNO_MULTI_PAPER_SESSION_ID=fno_v10_v11_v12_paper"' in source
    assert 'set "EQIDV2_RUNTIME_ROOT=C:\\TradingData\\eqidv2"' in source
    assert 'set "SESSION_SCRIPT=%BASE_DIR%\\fno_multi_paper_session.py"' in source
    assert '"%PYTHON_EXE%" -u "%SESSION_SCRIPT%" run' in source
    assert "FNO_MULTI_PAPER_EXECUTION_MODE=LIVE" not in source
    assert "%*" not in source
    assert 'if not "%~1"==""' in source
    assert 'set "LOG_DIR=%BASE_DIR%\\logs"' in source
    assert 'set "LOG_FILE=%LOG_DIR%\\fno_v10_v11_v12_paper.log"' in source


def test_installer_defines_one_future_weekday_task_without_running_it() -> None:
    source = _source(INSTALLER)

    assert '$taskName = "EQIDV2_fno_v10_v11_v12_paper_0915"' in source
    assert '$startTime = "09:15"' in source
    assert '"bat\\run_fno_v10_v11_v12_paper_session.bat"' in source
    assert "Monday,Tuesday,Wednesday,Thursday,Friday" in source
    assert "New-ScheduledTaskTrigger" in source
    assert "Register-ScheduledTask" in source
    assert "Enable-ScheduledTask -TaskName $taskName" in source
    assert "StartWhenAvailable" in source
    assert "WakeToRun must be true" in source
    assert "-WakeToRun" in source
    assert "Start-ScheduledTask" not in source
    assert "Stop-ScheduledTask" not in source
    assert "Demand starts must be enabled for the dashboard restart contract." in source
    assert source.count("Register-ScheduledTask") == 1


def test_installer_stages_disabled_and_fails_closed() -> None:
    source = _source(INSTALLER)

    register_at = source.index("Register-ScheduledTask")
    enable_at = source.index("Enable-ScheduledTask -TaskName $taskName")
    assert "New-ScheduledTaskSettingsSet" in source
    assert "-Disable" in source
    assert "-DisallowDemandStart" not in source
    assert "AllowDemandStart=True" in source
    assert "-MultipleInstances IgnoreNew" in source
    assert '$restartCount = 5' in source
    assert '$restartInterval = [TimeSpan]::FromMinutes(1)' in source
    assert "-RestartCount $restartCount" in source
    assert "-RestartInterval $restartInterval" in source
    assert register_at < enable_at
    assert "Disable-ScheduledTask -TaskName $taskName" in source
    assert "FNO_MULTI_PAPER_EXECUTION_MODE=LIVE" in source
    assert "throw \"Runner contains a forbidden LIVE execution mode.\"" in source
