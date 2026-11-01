"""Offline safety checks for the fixed G3 LIVE supervisor path."""
from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys

import pytest

from tools import g3_live_supervisor as live


def config(tmp_path: Path) -> live.Config:
    return live.Config(root=tmp_path, runtime=tmp_path / 'runtime', python=sys.executable)


def test_open_state_only_positively_open_overrides_cutoff(tmp_path, monkeypatch):
    day = datetime(2026, 11, 2, tzinfo=live.IST)
    monkeypatch.setattr(live, 'datetime', type('Clock', (), {'now': staticmethod(lambda tz: day)}))
    path = tmp_path / 'open.json'
    assert live.open_position_state(path) == 'unknown'
    path.write_text('{', encoding='utf-8')
    assert live.open_position_state(path) == 'unknown'
    path.write_text(json.dumps({'session_date': '2026-11-01', 'open_trades': [{}]}), encoding='utf-8')
    assert live.open_position_state(path) == 'unknown'
    path.write_text(json.dumps({'session_date': '2026-11-02', 'open_trades': []}), encoding='utf-8')
    assert live.open_position_state(path) == 'flat'
    path.write_text(json.dumps({'session_date': '2026-11-02', 'open_trades': [None, {}]}), encoding='utf-8')
    assert live.open_position_state(path) == 'open'
    path.write_text(json.dumps({'session_date': '2026-11-02', 'open_trades': {'a': None}}), encoding='utf-8')
    assert live.open_position_state(path) == 'flat'
    path.write_text(json.dumps({'session_date': '2026-11-02', 'open_trades': {'a': {'symbol': 'X'}}}), encoding='utf-8')
    assert live.open_position_state(path) == 'open'


def test_existing_worker_and_unreadable_shell_block_without_termination():
    rows = [
        {'pid': 1, 'ppid': 0, 'name': 'cmd.exe', 'inspect': True,
         'command': 'call activate_fno_v13_v10_g_3_live_kite_qty1.bat'},
        {'pid': 2, 'ppid': 1, 'name': 'python.exe', 'inspect': True,
         'command': 'python tools/g3_live_supervisor.py'},
        {'pid': 3, 'ppid': 0, 'name': 'python.exe', 'inspect': True,
         'command': 'python fno_v13_v10_g_3_live_kite_session.py --activate-live'},
        {'pid': 4, 'ppid': 0, 'name': 'powershell.exe', 'inspect': True, 'command': ''},
    ]
    assert live.existing_workers(rows, 2) == [
        {'pid': 3, 'reason': 'existing_g3_live_worker'},
        {'pid': 4, 'reason': 'unreadable_process'},
    ]


def test_preflight_requires_inherited_operator_ack(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    cfg.worker.touch()
    monkeypatch.setattr(live.os, 'name', 'nt')
    monkeypatch.setattr(live, 'native_processes', lambda: [])
    monkeypatch.setenv('FNO_V6_STRATEGY_PROFILE', 'V13_V10_G_3')
    monkeypatch.setenv('FNO_V6_EXECUTION_MODE', 'LIVE')
    monkeypatch.delenv(live.ACK_ENV, raising=False)
    with pytest.raises(RuntimeError, match='acknowledgement'):
        live.preflight(cfg)
    monkeypatch.setenv(live.ACK_ENV, live.ACK)
    live.preflight(cfg)


def test_worker_run_id_and_freshness_are_required(tmp_path):
    cfg = config(tmp_path)
    cfg.worker_dir.mkdir(parents=True)
    now = datetime.now(timezone.utc).timestamp()
    payload = {'run_id': 'old', 'heartbeat_ist': datetime.fromtimestamp(now, live.IST).isoformat()}
    cfg.worker_heartbeat.write_text(json.dumps(payload), encoding='utf-8')
    # No liveness action during startup grace.
    assert live.worker_failure(cfg, 'new', 123, now - 10, now - 10, now)[0] == ''
    assert live.worker_failure(cfg, 'new', 123, now - 200, now - 200, now)[0] == 'worker_run_id_mismatch'
    payload['run_id'] = 'new'
    payload['heartbeat_ist'] = datetime.fromtimestamp(now - 200, live.IST).isoformat()
    cfg.worker_heartbeat.write_text(json.dumps(payload), encoding='utf-8')
    assert live.worker_failure(cfg, 'new', 123, now - 200, now - 200, now)[0] == 'worker_heartbeat_stale'
    payload['heartbeat_ist'] = datetime.fromtimestamp(now, live.IST).isoformat()
    payload['pid'] = 999
    cfg.worker_heartbeat.write_text(json.dumps(payload), encoding='utf-8')
    assert live.worker_failure(cfg, 'new', 123, now - 200, now - 200, now)[0] == 'worker_pid_mismatch'


def test_after_cutoff_unknown_state_never_launches(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    monkeypatch.setattr(live, 'SingletonLock', lambda path: nullcontext())
    monkeypatch.setattr(live, 'preflight', lambda *a, **k: None)
    monkeypatch.setattr(live, 'ntp_offset', lambda: 0)
    monkeypatch.setattr(live, 'after_cutoff', lambda *a: True)
    monkeypatch.setattr(live.subprocess, 'Popen', lambda *a, **k: pytest.fail('worker launched'))
    assert live.run(cfg) == 0
    assert 'SKIPPED_CUTOFF' in cfg.status.read_text(encoding='utf-8')


def test_known_open_cutoff_recovery_uses_fixed_args_and_owned_job(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    launches, jobs = [], []

    class Child:
        def __init__(self, code):
            self.pid = 123 + len(launches)
            self.returncode = code
            self.calls = 0

        def poll(self):
            self.calls += 1
            return None if self.calls == 1 else self.returncode

    class Job:
        def __init__(self):
            self.assigned = None
            self.closed = False
            jobs.append(self)

        def assign(self, child):
            self.assigned = child

        def close(self):
            self.closed = True

    def fake_popen(args, **kwargs):
        launches.append((args, kwargs))
        return Child(1 if len(launches) == 1 else 0)

    monkeypatch.setattr(live, 'SingletonLock', lambda path: nullcontext())
    monkeypatch.setattr(live, 'preflight', lambda *a, **k: None)
    monkeypatch.setattr(live, 'ntp_offset', lambda: 0)
    monkeypatch.setattr(live, 'after_cutoff', lambda *a: True)
    monkeypatch.setattr(live, 'open_position_state', lambda path: 'open')
    monkeypatch.setattr(live, '_otlp_environment', lambda env: env)
    monkeypatch.setattr(live, 'ChildJob', Job)
    monkeypatch.setattr(live.subprocess, 'Popen', fake_popen)
    monkeypatch.setattr(live, 'worker_failure', lambda *a: ('', a[4]))
    monkeypatch.setattr(live, 'stop_owned_child', lambda child: None)
    monkeypatch.setattr(live.time, 'sleep', lambda delay: None)
    assert live.run(cfg) == 0
    assert len(launches) == 2
    assert all(args == [sys.executable, '-u', str(cfg.worker), '--activate-live'] for args, _ in launches)
    assert all(job.assigned is not None and job.closed for job in jobs)
    assert launches[0][1]['env']['EQIDV2_OBS_RUN_ID'] != launches[1][1]['env']['EQIDV2_OBS_RUN_ID']
    assert 'SUCCESS' in cfg.status.read_text(encoding='utf-8')


def test_bat_has_no_powershell_or_ack_assignment():
    bat = (live.ROOT / 'bat/activate_fno_v13_v10_g_3_live_kite_qty1.bat').read_text(encoding='utf-8')
    assert 'powershell' not in bat.lower()
    assert '"%PYTHON_EXE%" -u "%SUPERVISOR_PY%"' in bat
    assert '--activate-live' in Path(live.__file__).read_text(encoding='utf-8')
    assert 'set "FNO_V13_V10_G_3_LIVE_ACK=' not in bat
