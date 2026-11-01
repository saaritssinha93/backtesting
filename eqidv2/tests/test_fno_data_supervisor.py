"""Real isolated child/lock tests; never launch production workers or tasks."""
from dataclasses import replace
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from tools import fno_data_supervisor as sup


def row(pid, ppid, name, command):
    return dict(pid=pid, ppid=ppid, name=name, command=command, inspect=True)


@pytest.mark.parametrize("command", [
    "python fno_oi_fetch_5min.py", "python fno_oi_fetch_5min_fast_shadow.py",
    "python fno_oi_fetch_5min_fast_production.py",
    "powershell -File assert_fno_oi_fast_production_trial_exclusive.ps1",
    "cmd /c run_fno_oi_fetch_5min_fast_production.bat",
])
def test_exclusivity_blocks_all_existing_writers_and_stalled_launchers(command):
    rows = [row(10, 9, "python.exe", "supervisor"), row(9, 1, "cmd.exe", command),
            row(20, 1, "python.exe", command)]
    assert sup.conflicting_processes(sup.Config("fetch"), rows, 10) == [
        dict(pid=20, name="python.exe", reason="existing_data_owner")]


def test_unreadable_process_fails_closed_but_unrelated_worker_is_allowed():
    rows = [row(10, 1, "python.exe", "supervisor"), row(20, 1, "python.exe", ""),
            row(30, 1, "python.exe", "paper_entry.py")]
    assert sup.conflicting_processes(sup.Config("ranker"), rows, 10) == [
        dict(pid=20, name="python.exe", reason="unreadable_process")]


def test_legacy_scheduler_state_must_be_explicitly_disabled(monkeypatch):
    enabled = False

    def query(*args, **kwargs):
        xml = f'<Task xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task"><Settings><Enabled>{str(enabled).lower()}</Enabled></Settings></Task>'
        return subprocess.CompletedProcess(args, 0, xml.encode(), b"")
    monkeypatch.setattr(subprocess, "run", query)
    sup.assert_legacy_tasks_disabled()
    enabled = True
    with pytest.raises(RuntimeError, match="not disabled"):
        sup.assert_legacy_tasks_disabled()


def test_cutoff_uses_ist_and_exact_boundary():
    fetch, ranker = sup.Config("fetch"), sup.Config("ranker")
    assert not sup.after_cutoff(fetch, datetime(2026, 10, 30, 10, 3, 59, tzinfo=timezone.utc))
    assert sup.after_cutoff(fetch, datetime(2026, 10, 30, 10, 4, tzinfo=timezone.utc))
    assert not sup.after_cutoff(ranker, datetime(2026, 10, 30, 10, 5, 59, tzinfo=timezone.utc))
    assert sup.after_cutoff(ranker, datetime(2026, 10, 30, 10, 6, tzinfo=timezone.utc))


@pytest.fixture
def config(tmp_path, monkeypatch):
    cfg = sup.Config("ranker", root=tmp_path, runtime=tmp_path / "runtime",
                     interval=0.01, grace=0.10, freshness=0.10,
                     max_restarts=1, restart_delay=0, cooldown_delay=0)
    cfg.log.parent.mkdir()
    cfg.heartbeat.parent.mkdir(parents=True)
    monkeypatch.setattr(sup, "preflight", lambda _: None)
    monkeypatch.setattr(sup, "ntp_offset", lambda: 0)
    monkeypatch.setattr(sup, "after_cutoff", lambda _: False)
    return cfg


def worker(config, source):
    (config.root / f"{config.name}.py").write_text(source, encoding="utf-8")


def test_missing_stale_and_wrong_run_heartbeat_are_distinct(config):
    now = time.time()
    assert sup.worker_failure(config, "mine", now, now + 0.01) == ""
    assert sup.worker_failure(config, "mine", now - 1, now) == "freshness_missing"
    config.heartbeat.write_text("run_id=mine\n")
    old = now - 2
    os.utime(config.heartbeat, (old, old))
    assert sup.worker_failure(config, "mine", now - 1, now) == "freshness_stale"
    config.heartbeat.write_text("run_id=someone_else\n")
    assert sup.worker_failure(config, "mine", now - 1, time.time()) == "worker_run_id_mismatch"


@pytest.mark.parametrize("transient", ["", "\n", "run_id=mi", PermissionError("sharing violation"), FileNotFoundError()])
def test_transient_heartbeat_write_overlap_is_retried_before_restarting(config, monkeypatch, transient):
    config.heartbeat.write_text("run_id=mine\n")
    now = time.time()
    original = Path.read_text
    reads, sleeps = [], []

    def read(path, *args, **kwargs):
        if path == config.heartbeat:
            reads.append(path)
            if len(reads) == 1:
                if isinstance(transient, Exception):
                    raise transient
                return transient
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", read)
    monkeypatch.setattr(sup.time, "sleep", sleeps.append)
    assert sup.worker_failure(config, "mine", now - 1, now) == ""
    assert len(reads) == 2 and sleeps == [0.1]


@pytest.mark.parametrize("content, expected", [
    ("", "freshness_missing"), ("run_id=mine", "freshness_missing"),
    ("run_id=other\n", "worker_run_id_mismatch"),
    (PermissionError("sharing violation"), "freshness_missing"),
    (FileNotFoundError(), "freshness_missing"),
])
def test_persistent_invalid_heartbeat_remains_fail_closed_and_bounded(config, monkeypatch, content, expected):
    config.heartbeat.write_text("run_id=mine\n" if isinstance(content, Exception) else content)
    now = time.time()
    original = Path.read_text
    reads, sleeps = [], []

    def read(path, *args, **kwargs):
        if path == config.heartbeat:
            reads.append(path)
            if isinstance(content, Exception):
                raise content
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", read)
    monkeypatch.setattr(sup.time, "sleep", sleeps.append)
    assert sup.worker_failure(config, "mine", now - 1, now) == expected
    assert len(reads) == 3 and sleeps == [0.1, 0.1]


def test_changed_file_during_read_is_retried_even_if_old_content_parses(config, monkeypatch):
    config.heartbeat.write_text("run_id=mine\n")
    now = time.time()
    original = Path.read_text
    calls = []

    def read(path, *args, **kwargs):
        raw = original(path, *args, **kwargs)
        if path == config.heartbeat:
            calls.append(path)
            if len(calls) == 1:
                path.write_text("run_id=mine\nstate=WAITING\n")
        return raw
    monkeypatch.setattr(Path, "read_text", read)
    monkeypatch.setattr(sup.time, "sleep", lambda _: None)
    assert sup.worker_failure(config, "mine", now - 1, now) == ""
    assert len(calls) == 2


def test_persistent_stale_heartbeat_cannot_be_rescued_by_retry(config, monkeypatch):
    config.heartbeat.write_text("run_id=mine\n")
    now = time.time()
    os.utime(config.heartbeat, (now - 2, now - 2))
    sleeps = []
    monkeypatch.setattr(sup.time, "sleep", sleeps.append)
    assert sup.worker_failure(config, "mine", now - 1, now) == "freshness_stale"
    assert sleeps == [0.1, 0.1]


def test_cash_partial_json_is_retried_without_requiring_run_id(config, monkeypatch):
    cfg = replace(config, service="cash5m")
    cfg.heartbeat.write_text('{"overall_state":"OK"}')
    now = time.time()
    original = Path.read_text
    calls = []

    def read(path, *args, **kwargs):
        if path == cfg.heartbeat:
            calls.append(path)
            if len(calls) == 1:
                return '{"overall_state":'
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", read)
    monkeypatch.setattr(sup.time, "sleep", lambda _: None)
    assert sup.worker_failure(cfg, "new_run", now - 1, now) == ""
    assert len(calls) == 2


@pytest.mark.skipif(os.name != "nt", reason="Windows share-mode lock")
def test_singleton_lock_cannot_be_stolen_and_releases(config):
    path = Path(str(config.status) + ".lock")
    with sup.SingletonLock(path):
        with pytest.raises(OSError):
            with sup.SingletonLock(path):
                pytest.fail("contending lock acquired")
    with sup.SingletonLock(path):
        pass


@pytest.mark.skipif(os.name != "nt", reason="Windows data supervisor integration")
def test_real_child_success_and_exact_fixed_args(config):
    worker(config, "import sys\nprint('ARGS', sys.argv[1:])\n")
    assert sup.run(config) == 0
    assert "status=SUCCESS" in config.status.read_text()
    assert "--workers" in config.log.read_text()
    assert "--min-rank-coverage" in config.log.read_text()
    assert "--slot" not in config.command()


@pytest.mark.skipif(os.name != "nt", reason="Windows data supervisor integration")
def test_real_crashing_child_has_bounded_restarts(config):
    worker(config, "print('CRASH')\nraise SystemExit(7)\n")
    assert sup.run(config) == 7
    assert config.log.read_text().count("CRASH") == 2
    text = config.status.read_text()
    assert "status=FAILED" in text and "reason=max_restarts_exceeded" in text


@pytest.mark.skipif(os.name != "nt", reason="Windows data supervisor integration")
def test_restart_uses_new_run_identity_and_accepts_only_new_heartbeat(config):
    worker(config, """import os, time
from pathlib import Path
ids = Path('seen_run_ids.txt')
first = not ids.exists()
run_id = os.environ['EQIDV2_OBS_RUN_ID']
with ids.open('a') as stream: stream.write(run_id + '\\n')
if first: raise SystemExit(7)
heartbeat = Path(os.environ['EQIDV2_RUNTIME_ROOT']) / 'runtime_status/fno_oi_feature_ranker.heartbeat'
for _ in range(10):
    heartbeat.write_text('run_id=' + run_id + '\\n')
    time.sleep(0.03)
""")
    assert sup.run(config) == 0
    run_ids = (config.root / "seen_run_ids.txt").read_text().splitlines()
    assert len(run_ids) == 2 and len(set(run_ids)) == 2
    status = config.status.read_text()
    assert "status=SUCCESS" in status
    assert f"run_id={run_ids[1]}" in status
    assert "restart_count=1" in status


@pytest.mark.skipif(os.name != "nt", reason="Windows data supervisor integration")
def test_real_stalled_child_is_killed_by_freshness_and_never_retried_without_budget(config):
    config = replace(config, max_restarts=0)
    worker(config, "import time\nprint('STALLED', flush=True)\ntime.sleep(30)\n")
    started = time.monotonic()
    assert sup.run(config) == 124
    assert time.monotonic() - started < 5
    assert "reason=freshness_missing" in Path(str(config.log) + ".supervisor.log").read_text()


@pytest.mark.skipif(os.name != "nt", reason="Windows job object integration")
def test_job_close_kills_only_its_owned_child():
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"],
                             creationflags=subprocess.CREATE_NO_WINDOW)
    job = sup.ChildJob()
    try:
        job.assign(child)
        started = time.monotonic()
        job.close()
        child.wait(timeout=5)
        # Windows job close may report exit code zero; the 30-second worker
        # must nevertheless end immediately when its owner releases the job.
        assert time.monotonic() - started < 5
    finally:
        sup.stop_owned_child(child)
        job.close()


@pytest.mark.skipif(os.name != "nt", reason="Windows lock")
def test_ntp_drift_or_cutoff_never_launches_child(config, monkeypatch):
    worker(config, "raise RuntimeError('MUST NOT RUN')\n")
    monkeypatch.setattr(sup, "ntp_offset", lambda: 31)
    assert sup.run(config) == 2
    assert "reason=ntp_drift_exceeded" in config.status.read_text()
    monkeypatch.setattr(sup, "ntp_offset", lambda: None)
    monkeypatch.setattr(sup, "after_cutoff", lambda _: True)
    assert sup.run(config) == 0
    assert "status=SKIPPED_CUTOFF" in config.status.read_text()
    assert not config.log.exists()


def test_only_fixed_data_services_have_launchers_and_arguments():
    assert set(sup.SPECS) == {"fetch", "ranker", "cash5m", "options"}
    fetch = sup.Config("fetch")
    assert fetch.command()[3:] == [
        "--boundary-buffer-sec", "3", "--request-interval-sec", "0.36",
        "--workers-per-app", "2", "--writer-workers", "8", "--min-coverage", "0.99",
        "--bootstrap-days", "60", "--slot-retry-attempts", "2", "--slot-retry-delay-sec", "2"]
    for service in sup.SPECS:
        text = (sup.ROOT / "bat" / f"run_{sup.Config(service).name}.bat").read_text()
        assert "powershell" not in text.lower()
        assert f"--service {service}" in text
        assert "fno_data_supervisor.py" in text
        assert "%*" not in text


def test_cash_preserves_worker_limits_refresh_flag_and_independent_grace(monkeypatch):
    cfg = sup.Config("cash5m")
    assert cfg.spec[1:3] == ("1531", 720)
    assert cfg.grace_seconds == 1500 and cfg.freshness_seconds == 780
    monkeypatch.delenv("EQIDV2_5M_REFRESH_TOKENS", raising=False)
    assert cfg.command()[3:] == ["--max-workers", "128", "--max-workers-per-app", "16",
                                 "--buffer-sec", "2", "--quarter-hour-buffer-sec", "2",
                                 "--enable-opening-slot-fetch"]
    monkeypatch.setenv("EQIDV2_5M_REFRESH_TOKENS", "true")
    assert cfg.command()[-1] == "--refresh-tokens"


def test_cash_json_freshness_uses_current_run_mtime_without_requiring_run_id(config):
    cfg = replace(config, service="cash5m")
    now = time.time()
    cfg.heartbeat.write_text('{"overall_state":"OK"}')
    assert sup.worker_failure(cfg, "new_run", now - 1, now) == ""
    old = now - 2
    os.utime(cfg.heartbeat, (old, old))
    assert sup.worker_failure(cfg, "new_run", now - 1, now) == "freshness_stale"
    cfg.heartbeat.write_text('not JSON')
    assert sup.worker_failure(cfg, "new_run", now - 1, now) == "freshness_missing"
    cfg = replace(cfg, grace=1500)
    assert sup.worker_failure(cfg, "new_run", now - 721, now) == "hung_idle"


def test_cash_rotation_is_reversible_and_keeps_small_log(config):
    cfg = replace(config, service="cash5m")
    cfg.log.write_bytes(b"complete old log")
    assert sup.rotate_cash_log(cfg, max_bytes=100) is None
    archive = sup.rotate_cash_log(cfg, max_bytes=5)
    assert archive.parent == cfg.log.parent.resolve()
    assert archive.read_bytes() == b"complete old log"
    assert archive.name.endswith(".bak")
    assert not cfg.log.exists()


def test_cash_stalled_rotation_and_supervisor_both_block(config):
    cfg = replace(config, service="cash5m")
    rows = [row(10, 1, "python.exe", "supervisor"),
            row(20, 1, "powershell.exe", f"rotate_log_if_large.ps1 -Path {cfg.name}.log"),
            row(30, 1, "powershell.exe", f"supervise_command.ps1 -Name {cfg.name}.py")]
    assert [item["pid"] for item in sup.conflicting_processes(cfg, rows, 10)] == [20, 30]


def test_options_preserves_original_arguments_watchdog_and_cutoff():
    cfg = sup.Config("options")
    assert cfg.command()[3:] == [
        "--mode", "live", "--intervals", "both", "--max-apps", "8",
        "--boundary-buffer-sec", "3", "--request-interval-sec", "0.34",
        "--workers-per-app", "2", "--writer-workers", "8", "--min-coverage", "0.99",
        "--slot-retry-attempts", "2"]
    assert cfg.spec[1:3] == ("1534", 0)
    assert cfg.grace_seconds == cfg.freshness_seconds == 180
    assert cfg.max_restarts == 20 and cfg.restart_delay == 15
    assert not sup.after_cutoff(cfg, datetime(2026, 11, 2, 10, 3, 59, tzinfo=timezone.utc))
    assert sup.after_cutoff(cfg, datetime(2026, 11, 2, 10, 4, tzinfo=timezone.utc))


@pytest.mark.parametrize("command", [
    "python fno_options_atm_fetch_5min.py --mode historical",
    "powershell -File supervise_command.ps1 -Name fno_options_atm_fetch_5min.py",
    "cmd /c run_fno_options_atm_fetch_5min.bat",
])
def test_options_blocks_old_shell_or_competing_writer_but_not_paper(command):
    rows = [row(10, 9, "python.exe", "supervisor"), row(9, 1, "cmd.exe", command),
            row(20, 1, "python.exe", command),
            row(30, 1, "python.exe", "fno_v13_v10_g_options_paper.py")]
    assert sup.conflicting_processes(sup.Config("options"), rows, 10) == [
        dict(pid=20, name="python.exe", reason="existing_data_owner")]


@pytest.mark.skipif(os.name != "nt", reason="Windows isolated BAT launcher")
def test_options_bat_reaches_only_fixed_supervisor_in_isolated_directory(tmp_path):
    (tmp_path / "bat").mkdir()
    (tmp_path / "tools").mkdir()
    source = (sup.ROOT / "bat/run_fno_options_atm_fetch_5min.bat").read_text()
    source = source.replace(r"C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe", sys.executable)
    source = source.replace(r"C:\TradingData\eqidv2", str(tmp_path / "runtime"))
    launcher = tmp_path / "bat/run_fno_options_atm_fetch_5min.bat"
    launcher.write_text(source)
    (tmp_path / "fno_options_atm_fetch_5min.py").write_text("raise RuntimeError('must not launch directly')")
    (tmp_path / "tools/fno_data_supervisor.py").write_text(
        "import sys\nassert sys.argv[1:] == ['--service', 'options']\nprint('ISOLATED_LAUNCH_OK')\n")
    result = subprocess.run(["cmd.exe", "/d", "/c", str(launcher)], cwd=tmp_path,
                            capture_output=True, timeout=10, creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode == 0, result.stderr.decode(errors="replace")
    assert "ISOLATED_LAUNCH_OK" in (tmp_path / "logs/fno_options_atm_fetch_5min.launcher.log").read_text()
