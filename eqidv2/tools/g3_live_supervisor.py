"""G3 LIVE-only Windows supervisor, without a PowerShell dependency.

This does not authorize trading. The approved operator supplies the existing
acknowledgement and arm checks before calling the fixed G3 launcher. A direct
invocation without that inherited acknowledgement fails closed.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
import uuid

try:
    from .fno_data_supervisor import ChildJob, SingletonLock, atomic_kv, native_processes, ntp_offset, stop_owned_child
except ImportError:  # Direct invocation: python tools/g3_live_supervisor.py
    from fno_data_supervisor import ChildJob, SingletonLock, atomic_kv, native_processes, ntp_offset, stop_owned_child


IST = timezone(timedelta(hours=5, minutes=30))
ROOT = Path(__file__).resolve().parents[1]
RUNTIME = Path('C:/TradingData/eqidv2')
NAME = 'fno_v13_v10_g_3_live_kite_qty1'
WORKER_LEAF = 'fno_v13_v10_g_3_live_kite_session.py'
ACK_ENV = 'FNO_V13_V10_G_3_LIVE_ACK'
ACK = 'I_UNDERSTAND_REAL_FNO_V13_V10_G_3_EQUITY_ORDERS'


@dataclass(frozen=True)
class Config:
    root: Path = ROOT
    runtime: Path = RUNTIME
    python: str = sys.executable
    max_restarts: int = 20
    restart_delay: float = 15
    monitor_interval: float = 5
    worker_stale: float = 180
    worker_grace: float = 180
    hung_timeout: float = 900
    cooldown_window: float = 300
    cooldown_count: int = 6
    cooldown_delay: float = 120
    cutoff_hhmm: str = '1545'

    @property
    def worker(self) -> Path:
        return self.root / WORKER_LEAF

    @property
    def status_dir(self) -> Path:
        return self.runtime / 'runtime_status'

    @property
    def status(self) -> Path:
        return self.status_dir / f'{NAME}.status'

    @property
    def heartbeat(self) -> Path:
        return self.status_dir / f'{NAME}.heartbeat'

    @property
    def lock(self) -> Path:
        return self.status_dir / f'{NAME}.supervisor.lock'

    @property
    def spawn(self) -> Path:
        return self.status_dir / f'{NAME}.supervisor.spawn'

    @property
    def worker_dir(self) -> Path:
        return self.runtime / 'fno_oi/v13_v10_g_3_live/live_kite'

    @property
    def worker_status(self) -> Path:
        return self.worker_dir / 'status.json'

    @property
    def worker_heartbeat(self) -> Path:
        return self.worker_dir / 'heartbeat.json'

    @property
    def open_positions(self) -> Path:
        return self.worker_dir / f'open_positions_{datetime.now(IST):%Y-%m-%d}.json'

    @property
    def log(self) -> Path:
        return self.root / 'logs' / f'{NAME}_{datetime.now(IST):%Y-%m-%d}.log'


def after_cutoff(config: Config, now: datetime | None = None) -> bool:
    return (now or datetime.now(IST)).astimezone(IST).strftime('%H%M') >= config.cutoff_hhmm


def open_position_state(path: Path) -> str:
    """Return open, flat or unknown; only positively open can override cutoff."""
    try:
        before = path.stat()
        payload = json.loads(path.read_text(encoding='utf-8-sig'))
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            return 'unknown'
    except (OSError, ValueError, UnicodeError):
        return 'unknown'
    if not isinstance(payload, dict) or payload.get('session_date') != datetime.now(IST).strftime('%Y-%m-%d'):
        return 'unknown'
    rows = payload.get('open_trades')
    if isinstance(rows, list):
        return 'open' if any(row is not None for row in rows) else 'flat'
    if isinstance(rows, dict):
        return 'open' if any(row is not None for row in rows.values()) else 'flat'
    return 'unknown'


def _ancestors(rows: list[dict], pid: int) -> set[int]:
    by_pid = {int(row['pid']): int(row.get('ppid', 0)) for row in rows}
    result = {pid}
    while pid in by_pid:
        pid = by_pid[pid]
        if pid == 0 or pid in result:
            break
        result.add(pid)
    return result


def existing_workers(rows: list[dict], own_pid: int) -> list[dict]:
    """Block a second owner; never terminate a worker found during preflight."""
    ancestors = _ancestors(rows, own_pid)
    conflicts = []
    for row in rows:
        if row['pid'] in ancestors or not row.get('inspect'):
            continue
        command = row.get('command', '').lower()
        if not command:
            conflicts.append({'pid': row['pid'], 'reason': 'unreadable_process'})
            continue
        # The coordinator starts LONG, SHORT and broker-reconciliation children
        # as fno_v13_v10_g_3_live.py. A surviving child can still place or
        # manage orders even when its original coordinator is gone.
        live_child = ('fno_v13_v10_g_3_live.py' in command and
                      re.search(r'--execution-mode(?:\s+|=)["\']?live\b', command))
        legacy_supervisor = ('supervise_command.ps1' in command and NAME in command)
        another_supervisor = bool(re.search(r'(?:^|[\s/\\"\'])g3_live_supervisor\.py(?=$|[\s"\'])', command))
        another_launcher = f'activate_{NAME}.bat' in command
        if WORKER_LEAF in command or live_child or legacy_supervisor or another_supervisor or another_launcher:
            conflicts.append({'pid': row['pid'], 'reason': 'existing_g3_live_worker'})
    return conflicts


def preflight(config: Config, *, require_ack: bool = True) -> None:
    if os.name != 'nt':
        raise RuntimeError('G3 LIVE supervisor requires Windows')
    if not config.worker.is_file():
        raise RuntimeError('Fixed G3 LIVE worker is missing')
    if not Path(config.python).is_file():
        raise RuntimeError('Frozen Python executable is missing')
    if require_ack and (os.getenv('FNO_V6_STRATEGY_PROFILE') != 'V13_V10_G_3'
                        or os.getenv('FNO_V6_EXECUTION_MODE') != 'LIVE'
                        or os.getenv(ACK_ENV) != ACK):
        raise RuntimeError('G3 LIVE operator acknowledgement/profile is missing')
    conflicts = existing_workers(native_processes(), os.getpid())
    if conflicts:
        raise RuntimeError('G3 LIVE worker ownership conflict: ' + json.dumps(conflicts))


def _snapshot(path: Path) -> tuple[dict, float] | None:
    try:
        before = path.stat()
        raw = path.read_text(encoding='utf-8-sig')
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            return None
        if raw.lstrip().startswith('{'):
            data = json.loads(raw)
        else:
            if not raw.endswith('\n'):
                return None
            data = dict(line.split('=', 1) for line in raw.splitlines() if '=' in line)
        if not isinstance(data, dict) or not data:
            return None
        stamp = after.st_mtime
        for key in ('ts_utc', 'ts', 'heartbeat_ist', 'updated_at_ist'):
            value = data.get(key)
            if isinstance(value, str) and value:
                try:
                    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    if parsed.tzinfo is not None:
                        stamp = parsed.timestamp()
                        break
                except ValueError:
                    pass
        return data, stamp
    except (OSError, ValueError, UnicodeError):
        return None


def worker_failure(config: Config, run_id: str, worker_pid: int, started: float,
                   last_valid: float, now: float) -> tuple[str, float]:
    """A liveness failure must persist beyond the worker's startup grace."""
    log_mtime = config.log.stat().st_mtime if config.log.exists() else started
    if now - max(started, log_mtime) >= config.hung_timeout:
        return 'hung_idle', last_valid
    if now - started < config.worker_grace:
        return '', last_valid
    candidates = [x for x in (_snapshot(config.worker_heartbeat), _snapshot(config.worker_status)) if x]
    if not candidates:
        if now - last_valid >= max(15, config.worker_stale, 3 * config.monitor_interval):
            return 'worker_heartbeat_missing', last_valid
        return '', last_valid
    data, stamp = max(candidates, key=lambda x: x[1])
    last_valid = now
    signal_run_id = str(data.get('run_id') or '').strip()
    if not signal_run_id:
        return 'worker_run_id_missing', last_valid
    if signal_run_id != run_id:
        return 'worker_run_id_mismatch', last_valid
    signal_pid = data.get('pid')
    if signal_pid not in (None, ''):
        try:
            if int(signal_pid) != worker_pid:
                return 'worker_pid_mismatch', last_valid
        except (TypeError, ValueError):
            return 'worker_pid_invalid', last_valid
    if now - stamp >= config.worker_stale:
        return 'worker_heartbeat_stale', last_valid
    if stamp > now + 5:
        return 'worker_timestamp_future', last_valid
    return '', last_valid


def _otlp_environment(env: dict[str, str]) -> dict[str, str]:
    if any(env.get(key) for key in ('AI_PLATFORM_OTLP_TRACES_ENDPOINT',
                                    'OTEL_EXPORTER_OTLP_TRACES_ENDPOINT', 'OTEL_EXPORTER_OTLP_ENDPOINT')):
        return env
    try:
        with socket.create_connection(('127.0.0.1', 4318), timeout=0.75):
            pass
    except OSError:
        env['OTEL_SDK_DISABLED'] = 'true'
    else:
        env.setdefault('OTEL_EXPORTER_OTLP_TRACES_ENDPOINT', 'http://127.0.0.1:4318/v1/traces')
        env.setdefault('OTEL_EXPORTER_OTLP_TRACES_PROTOCOL', 'http/protobuf')
    return env


def diagnostic(config: Config, message: str) -> None:
    line = f'{datetime.now(IST):%Y-%m-%d %H:%M:%S} | {message}'
    print(line, flush=True)
    with Path(str(config.log) + '.supervisor.log').open('a', encoding='utf-8') as stream:
        stream.write(line + '\n')


def write_state(config: Config, state: str, restart: int, run_id: str = '',
                child=None, started: str = '', reason: str = '', exit_code: int | str = '') -> None:
    now = datetime.now(IST)
    pid = child.pid if child else ''
    common = dict(name=NAME, state=state, status=state, ts=now.strftime('%Y-%m-%d_%H:%M:%S'),
                  ts_utc=now.astimezone(timezone.utc).isoformat(), updated_utc=now.astimezone(timezone.utc).isoformat(),
                  pid=pid, supervisor_pid=os.getpid(), launcher_pid=pid, worker_pid=pid,
                  launcher_start_utc=started, worker_start_utc=started, run_id=run_id,
                  restart_count=restart, run_no=restart + 1, reason=reason, note=reason,
                  exit_code=exit_code, cutoff_hhmm=config.cutoff_hhmm,
                  worker_status_file=str(config.worker_status), worker_heartbeat_file=str(config.worker_heartbeat),
                  worker_stale_s=config.worker_stale, worker_grace_s=config.worker_grace,
                  log_file=str(config.log), supervisor_log_file=str(config.log) + '.supervisor.log',
                  status_file=str(config.status), heartbeat_file=str(config.heartbeat),
                  lock_file=str(config.lock), spawn_record_file=str(config.spawn),
                  max_restarts=config.max_restarts, restart_delay_s=config.restart_delay,
                  hung_timeout_s=config.hung_timeout, monitor_every_s=config.monitor_interval,
                  cooldown_window_s=config.cooldown_window, cooldown_limit=config.cooldown_count,
                  cooldown_delay_s=config.cooldown_delay)
    for path in (config.status, config.heartbeat, config.spawn):
        atomic_kv(path, common)


def run(config: Config) -> int:
    """Called only from the fixed BAT after the approved operator gate."""
    config.status_dir.mkdir(parents=True, exist_ok=True)
    config.log.parent.mkdir(parents=True, exist_ok=True)
    with SingletonLock(config.lock):
        preflight(config)
        offset = ntp_offset()
        if offset is not None and abs(offset) > 30:
            write_state(config, 'FAILED', 0, reason='ntp_drift_exceeded', exit_code=2)
            diagnostic(config, f'ERROR NTP drift {offset:.3f}s exceeds 30s')
            return 2
        diagnostic(config, 'WARN NTP unreachable' if offset is None else f'INFO NTP drift {offset:.3f}s')
        restart, events = 0, []
        while True:
            if after_cutoff(config):
                state = open_position_state(config.open_positions)
                if state != 'open':
                    write_state(config, 'SKIPPED_CUTOFF' if restart == 0 else 'STOPPED_AFTER_CUTOFF',
                                restart, reason=f'after_cutoff_{state}', exit_code=0)
                    diagnostic(config, f'INFO After cutoff, {state} book; no new worker')
                    return 0
                diagnostic(config, 'WARN After cutoff, positively open positions require exit-management recovery')
            # Keep the lock while checking and spawning; an orphaned healthy
            # worker is never killed just to make another launch possible.
            preflight(config)
            run_id = uuid.uuid4().hex
            env = _otlp_environment(dict(os.environ))
            env.update(EQIDV2_RUNTIME_ROOT=str(config.runtime), PYTHONUNBUFFERED='1',
                       PYTHONIOENCODING='utf-8', EQIDV2_OBSERVABILITY_ENABLED='1',
                       EQIDV2_OBS_RUN_ID=run_id, EQIDV2_OBS_SERVICE=NAME)
            job, child = ChildJob(), None
            started_at = time.time()
            started = datetime.fromtimestamp(started_at, timezone.utc).isoformat()
            reason = ''
            try:
                with config.log.open('ab', buffering=0) as log:
                    child = subprocess.Popen([config.python, '-u', str(config.worker), '--activate-live'],
                                             cwd=config.root, env=env, stdout=log, stderr=subprocess.STDOUT,
                                             creationflags=subprocess.CREATE_NO_WINDOW)
                    job.assign(child)
                    diagnostic(config, f'INFO START {NAME} worker_pid={child.pid} run_id={run_id} restart={restart}')
                    last_valid = started_at
                    while child.poll() is None:
                        write_state(config, 'RUNNING', restart, run_id, child, started, 'monitoring')
                        # Retry transient in-flight worker writes before taking
                        # a destructive liveness action against the owned child.
                        for attempt in range(3):
                            reason, last_valid = worker_failure(config, run_id, child.pid, started_at,
                                                                last_valid, time.time())
                            if not reason or attempt == 2:
                                break
                            time.sleep(0.1)
                        if reason:
                            stop_owned_child(child)
                            break
                        time.sleep(config.monitor_interval)
                    code = 124 if reason else child.returncode
                    reason = reason or 'normal_exit'
            finally:
                if child is not None:
                    stop_owned_child(child)
                job.close()
            write_state(config, 'SUCCESS' if code == 0 else 'EXITED', restart, run_id,
                        child, started, reason, code)
            diagnostic(config, f'INFO END {NAME} code={code} reason={reason}')
            if code == 0:
                return 0
            if after_cutoff(config) and open_position_state(config.open_positions) != 'open':
                write_state(config, 'STOPPED_AFTER_CUTOFF', restart, run_id, child, started,
                            'after_cutoff_nonzero_exit', 0)
                return 0
            restart += 1
            if restart > config.max_restarts:
                write_state(config, 'FAILED', restart, run_id, child, started, 'max_restarts_exceeded', code)
                return max(1, min(code, 255))
            events.append(time.monotonic())
            events = [stamp for stamp in events if time.monotonic() - stamp < config.cooldown_window]
            if len(events) >= config.cooldown_count:
                write_state(config, 'COOLDOWN', restart, run_id, reason='restart_storm')
                time.sleep(config.cooldown_delay)
            write_state(config, 'RESTARTING', restart, run_id, reason='backoff')
            time.sleep(config.restart_delay)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Fixed G3 LIVE supervisor; no order authorization')
    parser.add_argument('--check', action='store_true', help='Read-only ownership and fixed-path check')
    args = parser.parse_args(argv)
    config = Config(python=sys.executable)
    try:
        if args.check:
            preflight(config, require_ack=False)
            print('G3 LIVE supervisor check passed; no worker launched')
            return 0
        return run(config)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f'G3 LIVE supervisor blocked: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
