"""PowerShell-free standing G3 gate. Default is a read-only plan.

Only an operator's --apply --standing-policy-confirmed may mint today's
approval and enable the existing 09:15 task. This program never runs that task,
starts a LIVE worker, changes standing policy, or exposes an order API.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import ctypes
from ctypes import wintypes as W
from datetime import date, datetime, time as daytime, timedelta, timezone
from email.utils import parsedate_to_datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools import g3_live_operator as op

IST = op.config.IST
TASK = op.TASK
AUTH_TASK = 'EQIDV2_authentication_v2_0900'
RUNTIME = Path('C:/TradingData/eqidv2')
VERSION = 'V13-V10-G-3-PRODUCTION-V15'
FINGERPRINT = '89aae1fe359f6c6cd7b6cbaac4879d5818ce40c92808c4bf539544a4f80e3aed'
EXECUTION_FINGERPRINT = 'cc9210d610252534af34ee812407ba8bb46546ec466b35e09388bb4815eadc7e'
POLICY_FINGERPRINT = 'ad8a9180c2be2f9ede2b9cd27f7207ed535d06f50dd84000c6452fa4f7ff11d8'
SETUP_HASH = '750b52503841d7055a9facabb3204959f69e1673008f3e60f0dc99a93638970c'
PRIOR_IDENTITY = ('V13-V10-G-3-PRODUCTION-V12',
                  '7d4012b1886bce289f7d5036fd3a4d09ee9f8fd1f3da5d8265a83f8096d26b1c')
EXPECTED_WARNING = 'acceptance pending: waiting for the first canonical 09:20 stock-futures marker'
REQUIRED_HEALTH = (
    'task_EQIDV2_authentication_v2_0900',
    *(f'task_EQIDV2_fno_v13_v10_g_3_{name}_0915' for name in (
        'scanner_5min', 'equity_1min_feed', 'confirmation_1min', 'live_long',
        'live_short', 'trade_logger', 'net_result', 'live_kite_qty1')),
    'authentication_v2', 'fno_fast_production_runtime', 'preopen_healthcheck',
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def strict_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'Duplicate JSON key')
        result[key] = value
    return result


def read_json(path):
    for attempt in range(3):
        try:
            value = json.loads(Path(path).read_text(encoding='utf-8-sig'),
                               object_pairs_hook=strict_object,
                               parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Nonfinite JSON')))
            require(isinstance(value, dict), 'Expected a JSON object')
            return value
        except (OSError, ValueError):
            if attempt == 2:
                raise
            time.sleep(.2)


def parse_stamp(value):
    stamp = datetime.fromisoformat(value)
    require(stamp.utcoffset() == timedelta(hours=5, minutes=30), 'Expected an explicit IST timestamp')
    return stamp


def assert_window(now, day):
    require(now.utcoffset() == timedelta(hours=5, minutes=30)
            and now.date() == day and day >= date(2026, 10, 29) and day.weekday() < 5
            and daytime(9, 9) <= now.time().replace(tzinfo=None) < daytime(9, 12, 30),
            'Standing approval requires this weekday session, 09:09:00 through 09:12:29 IST')


def validate_policy(policy, day):
    op.validate_standing_policy(policy, session_date=day)
    require(policy['strategy_version'] == VERSION and policy['strategy_fingerprint'] == FINGERPRINT
            and policy['standing_policy_fingerprint'] == POLICY_FINGERPRINT
            and policy['setup_ids_sha256'] == SETUP_HASH
            and policy['policy_id'] == 'G3_LIVE_STANDING_20261029_V3'
            and policy['live_execution_policy']['live_execution_policy_fingerprint'] == EXECUTION_FINGERPRINT,
            'Standing authorization differs from the explicitly reviewed V15 policy')


def validate_preapproval(state, arm, day):
    op.validate_identity(state)
    require((state.get('strategy_version'), state.get('strategy_fingerprint')) == (VERSION, FINGERPRINT),
            'Active profile is not the reviewed V15 identity')
    if state.get('live_activation_authorized') is True:
        require(date.fromisoformat(state['live_approval']['session_date']) < day,
                'An unexpected same-day or future profile authorization already exists')
    if arm is not None:
        require(date.fromisoformat(arm['session_date']) < day,
                'An unexpected same-day or future LIVE arm already exists')
        require((arm.get('strategy_version'), arm.get('strategy_fingerprint')) in
                ((VERSION, FINGERPRINT), PRIOR_IDENTITY)
                and arm.get('live_execution_policy_fingerprint') == EXECUTION_FINGERPRINT,
                'Prior dated arm identity does not match the reviewed strategy and sizing')


def validate_task(snapshot, root, *, enabled=None, day=None):
    xml = ET.fromstring(snapshot['xml'])
    get = lambda p, default='': xml.findtext(p, default=default)
    triggers = list(xml.find('{*}Triggers'))
    actions = list(xml.find('{*}Actions'))
    require(len(triggers) == 1 and triggers[0].tag.endswith('}CalendarTrigger')
            and len(actions) == 1 and actions[0].tag.endswith('}Exec'),
            'LIVE task must have exactly one weekly trigger and one Exec action')
    trigger = triggers[0]
    start = datetime.fromisoformat(trigger.findtext('{*}StartBoundary', ''))
    weekdays = trigger.find('{*}ScheduleByWeek/{*}DaysOfWeek')
    require(start.time().replace(tzinfo=None) == daytime(9, 15)
            and trigger.findtext('{*}ScheduleByWeek/{*}WeeksInterval') == '1'
            and weekdays is not None
            and [node.tag.split('}')[-1] for node in weekdays] ==
                ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
            and trigger.find('{*}Repetition') is None and trigger.find('{*}EndBoundary') is None
            and trigger.findtext('{*}Enabled', 'true') == 'true',
            'LIVE task must retain its non-repeating Monday-Friday 09:15 trigger')
    require(get('{*}Actions/{*}Exec/{*}Command').casefold() ==
                str(Path(os.environ.get('SystemRoot', 'C:/Windows')) / 'System32/cmd.exe').casefold()
            and get('{*}Actions/{*}Exec/{*}Arguments') ==
                f'/d /c ""{root / "bat/run_g3_approved_live.bat"}""'
            and not get('{*}Actions/{*}Exec/{*}WorkingDirectory').strip(),
            'LIVE task action differs from the exact approved launcher')
    require(get('{*}Settings/{*}StartWhenAvailable', 'false') == 'false'
            and get('{*}Settings/{*}RestartOnFailure/{*}Count', '0') == '0'
            and get('{*}Settings/{*}MultipleInstancesPolicy') == 'IgnoreNew'
            and len(xml.findall('{*}Principals/{*}Principal')) == 1
            and get('{*}Principals/{*}Principal/{*}LogonType') == 'InteractiveToken'
            and get('{*}Principals/{*}Principal/{*}RunLevel', 'LeastPrivilege') == 'LeastPrivilege',
            'LIVE task restart, catch-up, overlap or principal controls differ')
    if enabled is not None:
        require(snapshot['enabled'] is enabled
                and (get('{*}Settings/{*}Enabled', 'true') == 'true') is enabled
                and snapshot['state'] == (3 if enabled else 1), 'LIVE task enabled/state did not reconcile')
        if enabled:
            # Task Scheduler DATE is local wall time even when pywin32 tags it UTC.
            next_run = datetime.fromisoformat(snapshot['next_run']).replace(tzinfo=None)
            require(next_run == datetime.combine(day, daytime(9, 15)),
                    'LIVE task next run is not exactly this session at 09:15')


def literal_status(text):
    values = {}
    for line in text.splitlines():
        key, sep, value = line.partition('=')
        require(sep and re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*', key)
                and key not in values and value.strip(), 'Malformed/duplicate authentication status')
        values[key] = value
    require(set(values) == {'status', 'script', 'ts', 'exit_code', 'log_file'},
            'Authentication status has unexpected or missing fields')
    return values


def validate_auth(task, text, now, root):
    last_run = datetime.fromisoformat(task['last_run']).replace(tzinfo=None)
    require(task['state'] != 4 and last_run.date() == now.date() and task['last_result'] == 0,
            'Authentication task has not completed successfully today')
    status = literal_status(text)
    stamp = datetime.strptime(status['ts'], '%Y-%m-%d_%H:%M:%S').replace(tzinfo=IST)
    require(status['status'] == 'SUCCESS' and status['script'] == 'authentication_v2.py'
            and status['exit_code'] == '0'
            and status['log_file'] == str(root / 'logs/authentication_v2_runner.log')
            and stamp.date() == now.date() and -5 <= (now - stamp).total_seconds() <= 5400,
            'Authentication status is not a fresh, literal, same-session success')


def validate_health(health, now):
    stamp = parse_stamp(health['ts_ist'])
    require(stamp.date() == now.date() and stamp.time().replace(tzinfo=None) >= daytime(9, 6)
            and -5 <= (now - stamp).total_seconds() <= 120, 'Pre-open health evidence is stale or early')
    checks = health.get('checks', [])
    require(isinstance(checks, list) and checks and all(isinstance(c, dict) for c in checks),
            'Missing pre-open health checks')
    warned = [c for c in checks if c.get('status') == 'WARN']
    require(health.get('overall') == 'WAIT' and all(c.get('status') in ('PASS', 'WARN') for c in checks)
            and len(warned) == 1 and warned[0].get('name') == 'fno_fast_production_first_slot'
            and warned[0].get('detail') == EXPECTED_WARNING,
            'Pre-open health is not the reviewed single-warning WAIT')
    for name in REQUIRED_HEALTH:
        matches = [c for c in checks if c.get('name') == name]
        require(len(matches) == 1 and matches[0].get('status') == 'PASS', f'Required health gate failed: {name}')


def validate_broker(report, now, *, invocation_started=None):
    op.validate_broker_report(report, now=now)
    checked, completed = parse_stamp(report['checked_at_ist']), parse_stamp(report['completed_at_ist'])
    market = report['market']
    require(checked.date() == now.date() and completed.date() == now.date()
            and 0 <= (completed - checked).total_seconds() <= 150
            and (invocation_started is None or invocation_started <= checked <= completed <= now)
            and report.get('strategy_controls_changed') is False and report.get('proposed_order', object()) is None
            and market.get('state') == 'BEFORE_REGULAR_SESSION' and market.get('schedule_only') is True
            and market.get('source') == 'https://www.nseindia.com/resources/exchange-communication-holidays?apiversion=v2'
            and market.get('regular_open_ist') == '09:15' and market.get('regular_close_ist') == '15:30'
            and sorted(report.get('readable_apps', [])) == list(op.EXPECTED_BROKER_APPS),
            'Broker evidence is not fresh exact-eight flat-account, verified pre-open evidence')


def assert_no_workers(rows):
    for row in rows:
        name, command = row['name'].lower(), row.get('command', '').lower()
        if row.get('inspect') and not command:
            raise ValueError(f'Cannot inspect a live shell/Python process: PID {row["pid"]}')
        live = ((name.startswith('python') and ('g3_live_supervisor.py' in command
                or 'fno_v13_v10_g_3_live_kite_session.py' in command
                or ('fno_v13_v10_g_3_live.py' in command and re.search(r'--execution-mode[ ="]+live', command))))
            or (name in ('powershell.exe', 'pwsh.exe') and 'supervise_command.ps1' in command
                and 'fno_v13_v10_g_3_live_kite_qty1' in command)
            or (name.startswith('cmd') and 'activate_fno_v13_v10_g_3_live_kite_qty1.bat' in command))
        require(not live, 'A G3 LIVE coordinator, worker, launcher or supervisor is already running')


@contextmanager
def activation_mutex():
    k = ctypes.WinDLL('kernel32', use_last_error=True)
    k.CreateMutexW.argtypes = [ctypes.c_void_p, W.BOOL, W.LPCWSTR]
    k.CreateMutexW.restype = W.HANDLE
    k.WaitForSingleObject.argtypes = [W.HANDLE, W.DWORD]
    k.ReleaseMutex.argtypes = k.CloseHandle.argtypes = [W.HANDLE]
    handle = k.CreateMutexW(None, False, r'Global\EQIDV2_G3_LIVE_STANDING_ACTIVATION')
    if not handle:
        raise ctypes.WinError(ctypes.get_last_error())
    acquired = False
    try:
        result = k.WaitForSingleObject(handle, 0)
        acquired = result in (0, 0x80)
        require(acquired, 'Another standing activation gate owns the mutex')
        yield
    finally:
        if acquired:
            k.ReleaseMutex(handle)
        k.CloseHandle(handle)


def bounded(command, timeout, *, env=None):
    result = subprocess.run([str(arg) for arg in command], cwd=ROOT, env=env,
                            capture_output=True, timeout=timeout,
                            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    require(result.returncode == 0, f'Bounded helper failed (exit {result.returncode})')
    return result.stdout.decode('utf-8-sig')


class NativeBackend:
    """All Windows/network side effects are explicit and replaceable in tests."""
    def now(self):
        return datetime.now(IST)

    def mutex(self):
        return activation_mutex()

    def task(self, name):
        return json.loads(bounded([sys.executable, Path(__file__), '--task-snapshot', name], 8))

    def set_enabled(self, enabled):
        bounded([Path(os.environ['SystemRoot']) / 'System32/schtasks.exe', '/Change', '/TN', TASK,
                 '/Enable' if enabled else '/Disable'], 5)

    def timezone(self):
        import winreg
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                            r'SYSTEM\CurrentControlSet\Control\TimeZoneInformation') as key:
            require(winreg.QueryValueEx(key, 'TimeZoneKeyName')[0].rstrip('\x00') == 'India Standard Time',
                    'Activation host timezone is not India Standard Time')

    def clock(self):
        result = json.loads(bounded([sys.executable, Path(__file__), '--clock-probe'], 15))
        require(0 <= result['round_trip'] <= 15 and abs(result['skew_seconds']) <= 30,
                'HTTPS clock evidence is slow or exceeds 30 seconds skew')

    def no_workers(self):
        from tools.fno_data_supervisor import native_processes
        assert_no_workers(native_processes())

    def refresh_health(self, root):
        bounded([sys.executable, root / 'preopen_session_healthcheck.py', '--max-age-min', '35',
                 '--report-path', root / 'logs/g3_live_standing_healthcheck.log'], 90)

    def refresh_broker(self, root):
        bounded([sys.executable, root / 'tools/test_kite_order_readiness.py', '--app', 'all',
                 '--minimum-live-balance', '20000'], 150)


def atomic_bytes(path, content):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix='.g3_standing_')
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class Gate:
    def __init__(self, backend=None, *, root=ROOT, runtime=RUNTIME):
        self.backend = backend or NativeBackend()
        self.root, self.runtime = Path(root), Path(runtime)
        self.profile = self.root / 'configs/strategies/active_profile.json'
        self.arm = self.runtime / 'fno_oi/v13_v10_g_3_live/live_arm.json'
        self.policy = self.root / 'configs/authorizations/g3_live_standing_policy_v3.json'
        self.health = self.root / 'logs/preopen_session_healthcheck_latest.json'
        self.broker = self.root / 'logs/kite_order_readiness/latest.json'

    def kills(self):
        for name in ('v13_v10_g_3_live', 'v6_live', 'v13_v10_g_live', 'control'):
            path = self.runtime / 'fno_oi' / name / 'kill_switch.json'
            if path.exists():
                require(read_json(path).get('enabled') is False, f'Kill switch is not explicitly clear: {name}')

    def target(self, enabled=None, day=None):
        snapshot = self.backend.task(TASK)
        validate_task(snapshot, self.root, enabled=enabled, day=day)
        return snapshot

    def disable(self):
        self.backend.set_enabled(False)
        task = self.backend.task(TASK)
        require(task['enabled'] is False and task['state'] == 1, 'CRITICAL: cannot verify target disabled')

    def auth(self):
        validate_auth(self.backend.task(AUTH_TASK),
                      (self.root / 'logs/authentication_v2_runner.status').read_text(encoding='utf-8-sig'),
                      self.backend.now(), self.root)

    def checkpoint(self, day):
        assert_window(self.backend.now(), day)
        self.target(False, day)
        self.backend.no_workers()
        self.kills()

    def record(self, message):
        """Local outcome only; never log broker payloads or process command lines."""
        log = self.root / 'logs/g3_live_standing_activation.log'
        log.parent.mkdir(parents=True, exist_ok=True)
        with log.open('a', encoding='utf-8') as stream:
            stream.write(f'{self.backend.now().isoformat()} | {message}\n')

    def disable_arm(self):
        try:
            value = read_json(self.arm)
        except (OSError, ValueError):
            value = {}
        op.atomic_json(self.arm, {**value, 'enabled': False})
        require(read_json(self.arm).get('enabled') is False, 'Cannot verify arm disabled')

    def plan(self, day):
        policy = read_json(self.policy)
        validate_policy(policy, day)
        state = read_json(self.profile)
        validate_preapproval(state, read_json(self.arm) if self.arm.exists() else None, day)
        self.kills()
        planned = op.plan(day, state, now=self.backend.now(), runtime_root=self.runtime)
        self.target()
        return dict(mode='PLAN_ONLY', session_date=str(day), strategy_version=VERSION,
                    active_setup_count=planned['active_setup_count'], standing_policy_id=policy['policy_id'],
                    approval_window_ist='09:09:00 <= now < 09:12:30', target=TASK,
                    checks_pending='same-day clock, authentication, health and fresh flat broker accounts',
                    mutates_controls=False, starts_workers=False, submits_orders=False)

    def apply(self):
        # Retain the mutex through rollback; no competing invocation can mint
        # an approval while failed control writes are being restored.
        mutex = self.backend.mutex()
        try:
            mutex.__enter__()
        except BaseException:
            self.disable()
            self.record('BLOCKED: cannot acquire standing activation mutex; target disabled')
            raise
        try:
            return self._apply_locked()
        finally:
            mutex.__exit__(None, None, None)

    def _apply_locked(self):
        original = None
        try:
            self.disable()
            day = self.backend.now().date()
            self.target(False, day)
            self.backend.timezone()
            assert_window(self.backend.now(), day)
            self.backend.clock()
            policy = read_json(self.policy)
            validate_policy(policy, day)
            self.checkpoint(day)
            state = read_json(self.profile)
            prior_arm = read_json(self.arm) if self.arm.exists() else None
            validate_preapproval(state, prior_arm, day)
            self.auth()
            self.backend.refresh_health(self.root)
            validate_health(read_json(self.health), self.backend.now())
            probe_started = self.backend.now()
            self.backend.refresh_broker(self.root)
            report = read_json(self.broker)
            validate_broker(report, self.backend.now(), invocation_started=probe_started)
            self.checkpoint(day)
            # Detect changes during slow external checks before backing up or writing.
            require(read_json(self.profile) == state and
                    (read_json(self.arm) if self.arm.exists() else None) == prior_arm
                    and read_json(self.policy) == policy, 'Authorization changed during gate checks')
            saved = (self.profile.read_bytes(), self.arm.read_bytes() if self.arm.exists() else None)
            backup = self.root / '.codex_tmp' / ('g3_live_standing_gate_' + self.backend.now().strftime('%Y%m%dT%H%M%S%f'))
            backup.mkdir(parents=True, exist_ok=False)
            (backup / 'active_profile.json').write_bytes(saved[0])
            if saved[1] is not None:
                (backup / 'live_arm.json').write_bytes(saved[1])
            (backup / 'target_task_disabled.xml').write_text(self.target(False, day)['xml'], encoding='utf-8')
            op.atomic_json(backup / 'standing_policy_verified.json', policy)
            op.atomic_json(backup / 'broker_report_verified.json', report)
            started = self.backend.now()
            updated, arm = op.standing_approval_payloads(day, state, report, now=started,
                runtime_root=self.runtime, standing_policy_path=self.policy)
            original = saved  # Transaction starts before the first control write.
            op.atomic_json(self.arm, {**arm, 'enabled': False})
            op.atomic_json(self.profile, updated)
            self.checkpoint(day)
            self.auth()
            validate_health(read_json(self.health), self.backend.now())
            require(read_json(self.broker) == report and read_json(self.policy) == policy,
                    'Broker evidence or standing policy changed during approval')
            validate_policy(read_json(self.policy), day)
            validate_broker(report, self.backend.now(), invocation_started=probe_started)
            require(read_json(self.profile) == updated and read_json(self.arm) == {**arm, 'enabled': False},
                    'Staged approval/arm changed before final commit')
            op.validate_launch(updated, arm, now=self.backend.now(), runtime_root=self.runtime,
                               standing_policy_path=self.policy)
            require(parse_stamp(arm['entry_not_before_ist']) == started,
                    'Standing approval epoch does not match this invocation')
            self.backend.clock()
            self.checkpoint(day)
            # Clock/task calls may take time: freshness is checked again at commit.
            self.auth()
            validate_health(read_json(self.health), self.backend.now())
            validate_broker(report, self.backend.now(), invocation_started=probe_started)
            require(read_json(self.profile) == updated and read_json(self.policy) == policy
                    and read_json(self.arm) == {**arm, 'enabled': False}
                    and read_json(self.broker) == report, 'Approval artifacts changed at final commit')
            assert_window(self.backend.now(), day)
            op.atomic_json(self.arm, arm)
            require(read_json(self.arm) == arm, 'Final arm write did not reconcile')
            assert_window(self.backend.now(), day)
            self.backend.set_enabled(True)
            self.target(True, day)
            assert_window(self.backend.now(), day)
            self.record(f'SUCCESS: standing policy approved {day}; exact 09:15 target enabled, not started')
            return dict(status='SUCCESS', session_date=str(day), target_enabled=True,
                        starts_workers=False, submits_orders=False, backup=str(backup))
        except BaseException as error:
            cleanup = []
            try:
                self.disable()
            except Exception as exc:
                cleanup.append(f'target disable: {type(exc).__name__}')
            if original is not None:
                try:
                    if self.arm.exists():
                        self.disable_arm()
                    atomic_bytes(self.profile, original[0])
                    require(self.profile.read_bytes() == original[0], 'Profile byte restoration failed')
                    if original[1] is None:
                        self.arm.unlink(missing_ok=True)
                        require(not self.arm.exists(), 'New arm removal failed')
                    else:
                        atomic_bytes(self.arm, original[1])
                        require(self.arm.read_bytes() == original[1], 'Arm byte restoration failed')
                except Exception as exc:
                    cleanup.append(f'artifact restoration: {type(exc).__name__}')
                    try:
                        op.atomic_json(self.arm, {'enabled': False})
                    except Exception:
                        cleanup.append('cannot verify disabled arm')
            suffix = '; FAIL-CLOSED CLEANUP ERROR: ' + ', '.join(cleanup) if cleanup else ''
            try:
                self.record(f'BLOCKED: {error}{suffix}')
            except Exception:
                suffix += '; activation outcome log unavailable'
            raise RuntimeError(f'BLOCKED: {error}{suffix}') from error


def task_snapshot(name):
    import win32com.client
    scheduler = win32com.client.Dispatch('Schedule.Service')
    scheduler.Connect()
    task = scheduler.GetFolder('\\').GetTask(name)
    # Do not astimezone() scheduler DATE; its wall clock fields are already local.
    return dict(xml=task.Xml, enabled=bool(task.Enabled), state=int(task.State),
                next_run=task.NextRunTime.replace(tzinfo=None).isoformat(),
                last_run=task.LastRunTime.replace(tzinfo=None).isoformat(), last_result=int(task.LastTaskResult))


def clock_probe():
    import requests
    before = datetime.now(timezone.utc)
    begin = time.monotonic()
    response = requests.head('https://api.kite.trade/', timeout=(5, 10), allow_redirects=False)
    after = datetime.now(timezone.utc)
    server = parsedate_to_datetime(response.headers['Date'])
    require(server.utcoffset() is not None, 'HTTPS server Date lacks timezone')
    return dict(round_trip=time.monotonic() - begin,
                skew_seconds=(server - (before + (after - before) / 2)).total_seconds())


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--plan', action='store_true')
    modes.add_argument('--apply', action='store_true')
    modes.add_argument('--task-snapshot', choices=(TASK, AUTH_TASK), help=argparse.SUPPRESS)
    modes.add_argument('--clock-probe', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--session-date', type=date.fromisoformat)
    parser.add_argument('--standing-policy-confirmed', action='store_true')
    args = parser.parse_args(argv)
    if args.apply and (not args.standing_policy_confirmed or args.session_date is not None):
        parser.error('--apply requires --standing-policy-confirmed and always uses the current IST date')
    if not args.apply and args.standing_policy_confirmed:
        parser.error('--standing-policy-confirmed requires --apply')
    try:
        if args.task_snapshot:
            result = task_snapshot(args.task_snapshot)
        elif args.clock_probe:
            result = clock_probe()
        else:
            gate = Gate()
            result = gate.apply() if args.apply else gate.plan(args.session_date or gate.backend.now().date())
        print(json.dumps(result, ensure_ascii=True))
        return 0
    except Exception as exc:
        # No broker responses, account IDs, tokens or shell command lines are logged.
        print(f'[G3-STANDING] {type(exc).__name__}: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
