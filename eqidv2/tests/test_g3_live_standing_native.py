"""Offline native gate tests: no scheduler changes, broker access or live orders."""
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import xml.etree.ElementTree as ET

import pytest
from tools import g3_live_standing as gate

NOW = datetime(2026, 11, 2, 9, 10, tzinfo=gate.IST)


def target_xml(root, enabled=False):
    ns = 'http://schemas.microsoft.com/windows/2004/02/mit/task'
    task = ET.Element('Task', xmlns=ns)
    triggers = ET.SubElement(task, 'Triggers')
    trigger = ET.SubElement(triggers, 'CalendarTrigger')
    ET.SubElement(trigger, 'StartBoundary').text = '2026-09-15T09:15:00'
    schedule = ET.SubElement(trigger, 'ScheduleByWeek')
    ET.SubElement(schedule, 'WeeksInterval').text = '1'
    days = ET.SubElement(schedule, 'DaysOfWeek')
    for name in ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'):
        ET.SubElement(days, name)
    principals = ET.SubElement(task, 'Principals')
    principal = ET.SubElement(principals, 'Principal')
    ET.SubElement(principal, 'LogonType').text = 'InteractiveToken'
    ET.SubElement(principal, 'RunLevel').text = 'LeastPrivilege'
    settings = ET.SubElement(task, 'Settings')
    ET.SubElement(settings, 'Enabled').text = str(enabled).lower()
    ET.SubElement(settings, 'MultipleInstancesPolicy').text = 'IgnoreNew'
    actions = ET.SubElement(task, 'Actions')
    action = ET.SubElement(actions, 'Exec')
    ET.SubElement(action, 'Command').text = str(Path(os.environ.get('SystemRoot', 'C:/Windows')) / 'System32/cmd.exe')
    ET.SubElement(action, 'Arguments').text = f'/d /c ""{root / "bat/run_g3_approved_live.bat"}""'
    return ET.tostring(task, encoding='unicode')


def broker(now=NOW):
    return dict(schema_version='kite_order_readiness_validation_only_v1', mode='VALIDATE_ONLY',
        state='READ_ACCESS_CONFIRMED', orders_submitted=0, order_mutations_available=False,
        strategy_controls_changed=False, checked_at_ist=now.isoformat(), completed_at_ist=now.isoformat(),
        proposed_order=None, minimum_live_balance_rupees=20000,
        market=dict(date=str(now.date()), scheduled_trading_day=True, state='BEFORE_REGULAR_SESSION',
            schedule_only=True, source='https://www.nseindia.com/resources/exchange-communication-holidays?apiversion=v2',
            regular_open_ist='09:15', regular_close_ist='15:30'), readable_apps=list(gate.op.EXPECTED_BROKER_APPS),
        apps=[dict(app=f'app{i}', credential_files_present=True, state='READ_ACCESS_CONFIRMED',
            open_net_positions=0, nonterminal_orders=0, orders_submitted=0, funds_positive=True,
            minimum_live_balance_rupees=20000, live_balance_covers_minimum=True, equity_segment_enabled=True,
            checks=dict(profile='PASS', margins='PASS', orders='PASS', positions='PASS'),
            capabilities=dict(nse=True, mis=True, limit=True, market=True, sl=True, sl_m=True)) for i in range(1, 9)])


def health(now=NOW):
    return dict(ts_ist=now.isoformat(), overall='WAIT',
        checks=[dict(name=name, status='PASS') for name in gate.REQUIRED_HEALTH]
        + [dict(name='fno_fast_production_first_slot', status='WARN', detail=gate.EXPECTED_WARNING)])


def write(path, value):
    gate.op.atomic_json(path, value)


class FakeBackend:
    def __init__(self, root):
        self.root, self.clock_now = root, NOW
        self.enabled = True
        self.events = []
        self.fail = None
        self.clock_calls = 0
        self.mutex_held = False

    @contextmanager
    def mutex(self):
        self.mutex_held = True
        try:
            yield
        finally:
            self.events.append('mutex_release')
            self.mutex_held = False

    def now(self):
        return self.clock_now

    def task(self, name):
        if name == gate.AUTH_TASK:
            return dict(state=3, last_run='2026-11-02T08:30:00', last_result=0)
        return dict(xml=target_xml(self.root, self.enabled), enabled=self.enabled,
                    state=3 if self.enabled else 1, next_run='2026-11-02T09:15:00+00:00')

    def set_enabled(self, value):
        self.events.append(('enabled', value, self.mutex_held))
        self.enabled = value
        if value and self.fail == 'enable':
            raise RuntimeError('Simulated enable acknowledgement failure')

    def timezone(self):
        if self.fail == 'timezone':
            raise RuntimeError('Wrong timezone')

    def clock(self):
        self.clock_calls += 1
        if self.fail == 'second_clock' and self.clock_calls == 2:
            raise RuntimeError('Clock evidence failed')
        if self.fail == 'stale_commit' and self.clock_calls == 2:
            self.clock_now += timedelta(seconds=61)
        if self.fail == 'deadline' and self.clock_calls == 2:
            self.clock_now += timedelta(minutes=3)

    def no_workers(self):
        if self.fail == 'worker':
            raise RuntimeError('Existing live worker')

    def refresh_health(self, root):
        self.events.append('health')
        write(root / 'logs/preopen_session_healthcheck_latest.json', health(self.clock_now))

    def refresh_broker(self, root):
        self.events.append('broker')
        write(root / 'logs/kite_order_readiness/latest.json', broker(self.clock_now))


@pytest.fixture
def fixture(tmp_path):
    backend = FakeBackend(tmp_path)
    instance = gate.Gate(backend, root=tmp_path, runtime=tmp_path / 'runtime')
    profile = gate.op.read(gate.op.PROFILE)
    profile['live_activation_authorized'] = False
    profile.pop('live_approval', None)
    write(instance.profile, profile)
    write(instance.policy, gate.op.read(gate.op.STANDING_POLICY))
    status = tmp_path / 'logs/authentication_v2_runner.status'
    status.parent.mkdir(parents=True)
    status.write_text('status=SUCCESS\nscript=authentication_v2.py\nts=2026-11-02_08:30:00\nexit_code=0\n'
                      f'log_file={tmp_path / "logs/authentication_v2_runner.log"}\n', encoding='utf-8')
    return instance, backend


def old_arm():
    return dict(enabled=True, session_date='2026-10-29', strategy_version=gate.VERSION,
                strategy_fingerprint=gate.FINGERPRINT, live_execution_policy_fingerprint=gate.EXECUTION_FINGERPRINT)


def test_plan_is_read_only_even_with_enabled_old_dated_target(fixture):
    instance, backend = fixture
    before = instance.profile.read_bytes()
    result = instance.plan(NOW.date())
    assert result['mode'] == 'PLAN_ONLY' and result['active_setup_count'] == 34
    assert result['mutates_controls'] is False and not backend.events
    assert instance.profile.read_bytes() == before and not instance.arm.exists()
    assert not (instance.root / '.codex_tmp').exists()
    assert not (instance.root / 'logs/g3_live_standing_activation.log').exists()


def test_success_only_enables_after_checks_and_stages_disabled_arm(fixture, monkeypatch):
    instance, backend = fixture
    writes = []
    real = gate.op.atomic_json
    def record(path, value):
        if Path(path) in (instance.arm, instance.profile):
            writes.append((Path(path).name, deepcopy(value), backend.enabled))
        real(path, value)
    monkeypatch.setattr(gate.op, 'atomic_json', record)
    result = instance.apply()
    assert result['status'] == 'SUCCESS' and not result['starts_workers'] and not result['submits_orders']
    assert [(x[0], x[1].get('enabled'), x[2]) for x in writes] == [
        ('live_arm.json', False, False), ('active_profile.json', None, False), ('live_arm.json', True, False)]
    assert backend.events == [('enabled', False, True), 'health', 'broker', ('enabled', True, True), 'mutex_release']
    gate.op.validate_launch(gate.read_json(instance.profile), gate.read_json(instance.arm), now=NOW,
                           runtime_root=instance.runtime, standing_policy_path=instance.policy)
    assert 'SUCCESS:' in (instance.root / 'logs/g3_live_standing_activation.log').read_text()


@pytest.mark.parametrize('partial', [False, True])
def test_recovery_identity_and_waiting_status_saved_before_next_launch(fixture, monkeypatch, partial):
    from tools import g3_launch_recovery as recovery
    instance, backend = fixture
    monkeypatch.setattr(recovery, 'enabled', lambda *a: True)
    original = backend.refresh_broker
    def refresh(root):
        original(root)
        value = gate.read_json(instance.broker)
        for row in value['apps']:
            row['account_fingerprint'] = 'a' * 64
            row['credential_fingerprint'] = row['app'][-1] * 64
        if partial:
            value['apps'][1]['checks']['positions'] = 'NETWORK_UNAVAILABLE'
            value['apps'][1]['state'] = 'PARTIAL_READ_ACCESS'
            value['state'] = 'PARTIAL_READ_ACCESS'
            value['readable_apps'].remove('app2')
        write(instance.broker, value)
    monkeypatch.setattr(backend, 'refresh_broker', refresh)
    assert instance.apply()['status'] == 'SUCCESS'
    identity = gate.read_json(instance.runtime / 'runtime_status' / recovery.IDENTITY_NAME)
    assert len(identity['credential_fingerprints']) == 8
    assert identity['session_date'] == str(NOW.date())
    status = gate.read_json(instance.runtime / 'runtime_status' / recovery.STATUS_NAME)
    assert status['state'] == 'WAITING_FOR_0915'
    if partial:
        # Approval did not relabel an incomplete app as healthy or start workers.
        assert gate.read_json(instance.broker)['state'] == 'PARTIAL_READ_ACCESS'


def test_recovery_unknown_account_blocks_preopen_and_records_reason(fixture, monkeypatch):
    from tools import g3_launch_recovery as recovery
    instance, backend = fixture
    monkeypatch.setattr(recovery, 'enabled', lambda *a: True)
    with pytest.raises(RuntimeError, match='identities'):
        instance.apply()
    assert not backend.enabled
    status = gate.read_json(instance.runtime / 'runtime_status' / recovery.STATUS_NAME)
    assert status['state'] == 'BLOCKED'


@pytest.mark.parametrize('had_arm', [False, True])
@pytest.mark.parametrize('failure', ['enable', 'second_clock', 'stale_commit', 'deadline'])
def test_failed_commit_restores_exact_bytes_and_disables_target(fixture, had_arm, failure):
    instance, backend = fixture
    if had_arm:
        write(instance.arm, old_arm())
    before = (instance.profile.read_bytes(), instance.arm.read_bytes() if had_arm else None)
    backend.fail = failure
    with pytest.raises(RuntimeError, match='BLOCKED'):
        instance.apply()
    assert backend.enabled is False
    assert instance.profile.read_bytes() == before[0]
    assert instance.arm.read_bytes() == before[1] if had_arm else not instance.arm.exists()
    assert backend.events[-2:] == [('enabled', False, True), 'mutex_release']
    assert 'BLOCKED:' in (instance.root / 'logs/g3_live_standing_activation.log').read_text()


@pytest.mark.parametrize('failure', ['timezone', 'worker'])
def test_early_failure_disables_without_touching_authorization(fixture, failure):
    instance, backend = fixture
    before = instance.profile.read_bytes()
    backend.fail = failure
    with pytest.raises(RuntimeError, match='BLOCKED'):
        instance.apply()
    assert not backend.enabled and instance.profile.read_bytes() == before
    assert not instance.arm.exists() and 'broker' not in backend.events


@pytest.mark.parametrize('hh,mm,ss,valid', [(9,8,59,False),(9,9,0,True),(9,12,29,True),(9,12,30,False)])
def test_exact_window(hh, mm, ss, valid):
    now = NOW.replace(hour=hh, minute=mm, second=ss)
    if valid:
        gate.assert_window(now, now.date())
    else:
        with pytest.raises(ValueError):
            gate.assert_window(now, now.date())


@pytest.mark.parametrize('mutation', ['version','enabled','extra','fingerprint','setups'])
def test_policy_is_pinned_and_fail_closed(fixture, mutation):
    instance, backend = fixture
    policy = gate.read_json(instance.policy)
    if mutation == 'version': policy['strategy_version'] = 'future'
    if mutation == 'enabled': policy['enabled'] = False
    if mutation == 'extra': policy['extra'] = 1
    if mutation == 'fingerprint': policy['standing_policy_fingerprint'] = 'x'
    if mutation == 'setups': policy['setup_ids'].reverse()
    write(instance.policy, policy)
    with pytest.raises(RuntimeError, match='BLOCKED'):
        instance.apply()
    assert not backend.enabled and not instance.arm.exists()


@pytest.mark.parametrize('field,value', [('enabled',True),('enabled',None),('other',False)])
def test_kills_require_explicit_false(fixture, field, value):
    instance, backend = fixture
    write(instance.runtime / 'fno_oi/control/kill_switch.json', {field:value})
    with pytest.raises(RuntimeError, match='Kill switch'):
        instance.apply()
    assert not backend.enabled


@pytest.mark.parametrize('replace', [
    ('09:15:00','09:16:00'), ('<WeeksInterval>1','<WeeksInterval>2'),
    ('<Friday />','<Saturday />'), ('<RunLevel>LeastPrivilege','<RunLevel>HighestAvailable'),
    ('<MultipleInstancesPolicy>IgnoreNew','<MultipleInstancesPolicy>Parallel'),
    ('<Enabled>false</Enabled>','<Enabled>false</Enabled><StartWhenAvailable>true</StartWhenAvailable>'),
    ('</CalendarTrigger>','<Repetition><Interval>PT1M</Interval></Repetition></CalendarTrigger>'),
    ('run_g3_approved_live.bat','unsafe.bat'),
])
def test_task_definition_cannot_weaken_gates(fixture, replace):
    instance, backend = fixture
    backend.enabled = False
    task = backend.task(gate.TASK)
    assert replace[0] in task['xml']
    task['xml'] = task['xml'].replace(*replace)
    with pytest.raises(ValueError):
        gate.validate_task(task, instance.root, enabled=False, day=NOW.date())


def test_scheduler_com_date_is_wall_time_not_utc(fixture):
    instance, backend = fixture
    gate.validate_task(backend.task(gate.TASK), instance.root, enabled=True, day=NOW.date())
    bad = backend.task(gate.TASK)
    bad['next_run'] = '2026-11-02T03:45:00+00:00'
    with pytest.raises(ValueError, match='next run'):
        gate.validate_task(bad, instance.root, enabled=True, day=NOW.date())


@pytest.mark.parametrize('bad', ['status=SUCCESS\nstatus=SUCCESS\n', 'x=1\n\n', 'x=\n', '=v\n'])
def test_status_rejects_duplicates_blank_lines_or_missing_values(bad):
    with pytest.raises(ValueError): gate.literal_status(bad)


def test_auth_path_backslashes_are_literal(fixture):
    instance, backend = fixture
    text = (instance.root / 'logs/authentication_v2_runner.status').read_text()
    gate.validate_auth(backend.task(gate.AUTH_TASK), text, NOW, instance.root)
    assert gate.literal_status(text)['log_file'] == str(instance.root / 'logs/authentication_v2_runner.log')
    with pytest.raises(ValueError): gate.validate_auth(backend.task(gate.AUTH_TASK), text, NOW + timedelta(hours=2), instance.root)


@pytest.mark.parametrize('mutation', ['old','extra_warn','missing','duplicate','fail','naive'])
def test_health_requires_exact_reviewed_wait(mutation):
    report = health()
    if mutation == 'old': report['ts_ist'] = (NOW-timedelta(seconds=121)).isoformat()
    if mutation == 'extra_warn': report['checks'].append(dict(name='other',status='WARN'))
    if mutation == 'missing': report['checks'].pop(0)
    if mutation == 'duplicate': report['checks'].append(report['checks'][0])
    if mutation == 'fail': report['checks'][0]['status'] = 'FAIL'
    if mutation == 'naive': report['ts_ist'] = NOW.replace(tzinfo=None).isoformat()
    with pytest.raises(ValueError): gate.validate_health(report, NOW)


@pytest.mark.parametrize('mutation', ['old','position','apps','calendar','mutating','future','prior_invocation'])
def test_broker_requires_fresh_same_invocation_flat_read_only_accounts(mutation):
    report = broker()
    if mutation == 'old': report['completed_at_ist'] = (NOW-timedelta(seconds=61)).isoformat()
    if mutation == 'position': report['apps'][7]['open_net_positions'] = 1
    if mutation == 'apps': report['readable_apps'][-1] = 'app1'
    if mutation == 'calendar': report['market']['source'] = 'unverified'
    if mutation == 'mutating': report['strategy_controls_changed'] = True
    if mutation == 'future': report['completed_at_ist'] = (NOW+timedelta(seconds=1)).isoformat()
    if mutation == 'prior_invocation': report['checked_at_ist'] = (NOW-timedelta(seconds=1)).isoformat()
    with pytest.raises(ValueError): gate.validate_broker(report, NOW, invocation_started=NOW)


@pytest.mark.parametrize('row', [
    dict(name='python.exe',command='python tools/g3_live_supervisor.py',pid=1),
    dict(name='python.exe',command='python fno_v13_v10_g_3_live_kite_session.py',pid=1),
    dict(name='python.exe',command='python fno_v13_v10_g_3_live.py --execution-mode LIVE',pid=1),
    dict(name='powershell.exe',command='supervise_command.ps1 fno_v13_v10_g_3_live_kite_qty1',pid=1),
    dict(name='cmd.exe',command='activate_fno_v13_v10_g_3_live_kite_qty1.bat',pid=1),
    dict(name='python.exe',command='',pid=1,inspect=True),
])
def test_existing_or_unreadable_workers_block(row):
    with pytest.raises(ValueError): gate.assert_no_workers([row])


def test_paper_worker_does_not_count_as_live():
    gate.assert_no_workers([dict(name='python.exe', command='fno_v13_v10_g_3_live.py --execution-mode PAPER', pid=1)])


def test_cli_apply_requires_explicit_confirmation_before_any_effect(monkeypatch):
    monkeypatch.setattr(gate, 'Gate', lambda: pytest.fail('Must not construct/apply gate'))
    with pytest.raises(SystemExit): gate.main(['--apply'])
    with pytest.raises(SystemExit): gate.main(['--apply','--standing-policy-confirmed','--session-date','2026-11-02'])


def test_duplicate_json_is_rejected(tmp_path, monkeypatch):
    path = tmp_path / 'bad.json'
    path.write_text('{"enabled":false,"enabled":true}')
    monkeypatch.setattr(gate.time, 'sleep', lambda _: None)
    with pytest.raises(ValueError, match='Duplicate'):
        gate.read_json(path)


def test_corrupt_partial_arm_write_still_restores_original_bytes(fixture, monkeypatch):
    instance, backend = fixture
    write(instance.arm, old_arm())
    before = (instance.profile.read_bytes(), instance.arm.read_bytes())
    real = gate.op.atomic_json
    triggered = False
    def partial_write(path, value):
        nonlocal triggered
        if Path(path) == instance.arm and not triggered:
            triggered = True
            instance.arm.write_bytes(b'{bad_partial_write')
            raise OSError('simulated interrupted arm write')
        real(path, value)
    monkeypatch.setattr(gate.op, 'atomic_json', partial_write)
    with pytest.raises(RuntimeError, match='BLOCKED'):
        instance.apply()
    assert instance.profile.read_bytes() == before[0] and instance.arm.read_bytes() == before[1]
    assert not backend.enabled


def test_enable_crossing_deadline_rolls_back(fixture, monkeypatch):
    instance, backend = fixture
    real = backend.set_enabled
    def delayed_enable(value):
        real(value)
        if value:
            backend.clock_now = NOW.replace(minute=12, second=30)
    monkeypatch.setattr(backend, 'set_enabled', delayed_enable)
    before = instance.profile.read_bytes()
    with pytest.raises(RuntimeError, match='BLOCKED'):
        instance.apply()
    assert not backend.enabled and instance.profile.read_bytes() == before
    assert not instance.arm.exists()
