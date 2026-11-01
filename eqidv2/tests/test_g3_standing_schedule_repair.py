from pathlib import Path
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
import xml.etree.ElementTree as ET

import pytest

from tools import repair_g3_standing_schedule as repair
import preopen_session_healthcheck as health


def fixture_xml(clock, *, gate):
    ns = repair.NS['t']
    root = ET.Element(f'{{{ns}}}Task')
    def child(parent, name, text=None):
        node = ET.SubElement(parent, f'{{{ns}}}{name}')
        node.text = text
        return node
    principals = child(root, 'Principals')
    principal = child(principals, 'Principal')
    child(principal, 'LogonType', 'InteractiveToken')
    # Windows omits default LeastPrivilege/Enabled in actual exported XML.
    settings = child(root, 'Settings')
    child(settings, 'MultipleInstancesPolicy', 'IgnoreNew')
    child(settings, 'StartWhenAvailable', 'false')
    child(settings, 'ExecutionTimeLimit', 'PT3M' if gate else 'PT0S')
    trigger = child(child(root, 'Triggers'), 'CalendarTrigger')
    child(trigger, 'StartBoundary', f'2026-10-23T{clock}+05:30')
    weekly = child(trigger, 'ScheduleByWeek')
    child(weekly, 'WeeksInterval', '1')
    days = child(weekly, 'DaysOfWeek')
    for day in ('Monday','Tuesday','Wednesday','Thursday','Friday'):
        child(days, day)
    action = child(child(root, 'Actions'), 'Exec')
    windows = Path(repair.os.environ.get('SystemRoot', 'C:/Windows'))
    if gate:
        child(action, 'Command', str(windows/'System32/WindowsPowerShell/v1.0/powershell.exe'))
        child(action, 'Arguments', '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + str(repair.ROOT/'bat/activate_g3_live_standing.ps1') + '"')
        child(action, 'WorkingDirectory', str(repair.ROOT))
    else:
        child(action, 'Command', str(windows/'System32/cmd.exe'))
        child(action, 'Arguments', '/d /c ""' + str(repair.ROOT/'bat/run_g3_approved_live.bat') + '""')
    return ET.tostring(root, encoding='unicode')


def test_replacement_changes_only_gate_action_and_is_idempotent():
    old, target = fixture_xml('09:10:00', gate=True), fixture_xml('09:15:00', gate=False)
    new = repair.prepared_xml(old, target)
    assert repair.prepared_xml(new, target) == new
    a, b = ET.fromstring(old), ET.fromstring(new)
    for tree in (a,b):
        actions = tree.find('t:Actions', repair.NS)
        tree.remove(actions)
    assert ET.tostring(a) == ET.tostring(b)
    assert repair.text_at(ET.fromstring(new), 't:Actions/t:Exec/t:Command') == repair.new_action()[0]


@pytest.mark.parametrize('path,value', [
    ('t:Settings/t:StartWhenAvailable','true'),
    ('t:Settings/t:MultipleInstancesPolicy','Parallel'),
    ('t:Settings/t:ExecutionTimeLimit','PT0S'),
    ('t:Triggers/t:CalendarTrigger/t:StartBoundary','2026-10-23T09:15:00'),
    ('t:Actions/t:Exec/t:Arguments','--unreviewed'),
    ('t:Principals/t:Principal/t:LogonType','ServiceAccount'),
])
def test_install_rejects_schedule_or_execution_drift(path, value):
    old = ET.fromstring(fixture_xml('09:10:00', gate=True))
    old.find(path, repair.NS).text = value
    with pytest.raises(AssertionError):
        repair.prepared_xml(ET.tostring(old), fixture_xml('09:15:00', gate=False))


def test_target_schedule_drift_is_rejected():
    with pytest.raises(AssertionError):
        repair.prepared_xml(fixture_xml('09:10:00', gate=True), fixture_xml('09:20:00', gate=False))


def test_apply_requires_explicit_operator_confirmation_before_com():
    with pytest.raises(SystemExit) as error:
        repair.main(['--apply'])
    assert error.value.code == 2


def test_health_task_query_has_a_bounded_failure(monkeypatch):
    calls=[]
    def stalled(command, **kwargs):
        calls.append(kwargs['timeout'])
        raise health.subprocess.TimeoutExpired(command, kwargs['timeout'])
    monkeypatch.setattr(health.subprocess,'run',stalled)
    assert health._run_schtasks_query('EQIDV2_test') is None
    assert calls and max(calls) == 5


INSTALL_NOW = datetime(2026, 11, 1, 22, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))


class FakeTask:
    def __init__(self, name, xml, events):
        self.name, self.Xml, self.events = name, xml, events
        self._enabled = True
        self.State = 3
        self.instances = []
        self.NextRunTime = datetime(2026, 11, 2, 9, 10, tzinfo=timezone.utc)
        self.Definition = SimpleNamespace(Principal=SimpleNamespace(UserId='test-operator'))

    @property
    def Enabled(self):
        return self._enabled

    @Enabled.setter
    def Enabled(self, value):
        if value is False and getattr(self, 'deny_disable', False):
            raise OSError('simulated disable access denied')
        self.events.append(('enabled', self.name, value))
        self._enabled, self.State = value, 3 if value else 1

    def GetInstances(self, _):
        return self.instances


class FakeFolder:
    def __init__(self, old, target, failure=None):
        self.events, self.registrations = [], []
        self.tasks = {repair.TASK:FakeTask(repair.TASK, old, self.events),
                      repair.TARGET:FakeTask(repair.TARGET, target, self.events)}
        self.failure = failure

    def GetTask(self, name):
        return self.tasks[name]

    def RegisterTask(self, name, xml, flags, user, password, logon):
        assert name == repair.TASK and flags == 6 and user == 'test-operator' and password is None and logon == 3
        self.registrations.append(name)
        count = len(self.registrations)
        task = self.tasks[name]
        if count == 1 and self.failure == 'old_action':
            return task  # API acknowledgement without actually replacing the old action.
        if count == 2 and self.failure in ('rollback_wrong', 'rollback_raise'):
            if self.failure == 'rollback_raise':
                raise OSError('rollback registration failed')
            return task  # Leaves the new action, so semantic rollback must fail.
        task.Xml = xml
        if count == 1 and self.failure in ('rollback_wrong', 'rollback_raise', 'register_raise'):
            raise OSError('registration failed after write')
        if count == 1 and self.failure == 'next_run':
            task.NextRunTime = datetime(2026, 11, 3, 9, 10, tzinfo=timezone.utc)
        if count == 2:
            task.NextRunTime = datetime(2026, 11, 2, 9, 10, tzinfo=timezone.utc)
        return task


def install_fixture(tmp_path, monkeypatch, failure=None):
    old, target = fixture_xml('09:10:00', gate=True), fixture_xml('09:15:00', gate=False)
    folder = FakeFolder(old, target, failure)
    before = {'authorization':'original'}
    monkeypatch.setattr(repair, 'control_hashes', lambda root=repair.ROOT: before.copy())
    kwargs = dict(old_xml=old, target_xml=target, replacement=repair.prepared_xml(old, target),
                  before=before, backup=tmp_path, now=INSTALL_NOW)
    return folder, kwargs


def test_mock_install_requires_exact_new_action_and_preserves_other_tasks(tmp_path, monkeypatch):
    folder, kwargs = install_fixture(tmp_path, monkeypatch)
    installed = repair.install_replacement(folder, **kwargs)
    assert repair.action_identity(installed.Xml) == repair.new_action()
    assert folder.registrations == [repair.TASK] and folder.events == []
    assert folder.GetTask(repair.TARGET).Xml == kwargs['target_xml']
    assert (tmp_path/'after.xml').is_file()


@pytest.mark.parametrize('failure', ['old_action','register_raise','next_run'])
def test_mock_install_failure_restores_and_verifies_old_task(tmp_path, monkeypatch, failure):
    folder, kwargs = install_fixture(tmp_path, monkeypatch, failure)
    with pytest.raises(RuntimeError, match='prior standing task restored and verified'):
        repair.install_replacement(folder, **kwargs)
    assert repair.semantic_task(folder.GetTask(repair.TASK).Xml) == repair.semantic_task(kwargs['old_xml'])
    assert folder.registrations == [repair.TASK,repair.TASK] and folder.events == []
    assert folder.GetTask(repair.TARGET).Xml == kwargs['target_xml']


@pytest.mark.parametrize('failure', ['rollback_wrong','rollback_raise'])
def test_unverified_rollback_disables_only_standing_gate(tmp_path, monkeypatch, failure):
    folder, kwargs = install_fixture(tmp_path, monkeypatch, failure)
    with pytest.raises(RuntimeError, match='ROLLBACK FAILED:.*standing gate disabled and verified'):
        repair.install_replacement(folder, **kwargs)
    assert folder.events == [('enabled',repair.TASK,False)]
    assert folder.GetTask(repair.TASK).Enabled is False
    assert folder.GetTask(repair.TARGET).Enabled is True
    assert folder.GetTask(repair.TARGET).Xml == kwargs['target_xml']


def test_changed_controls_disable_gate_even_if_task_rollback_succeeds(tmp_path, monkeypatch):
    folder, kwargs = install_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(repair, 'control_hashes', lambda root=repair.ROOT:
                        {'authorization':'changed' if folder.registrations else 'original'})
    with pytest.raises(RuntimeError, match='Control files changed.*ROLLBACK FAILED:.*standing gate disabled'):
        repair.install_replacement(folder, **kwargs)
    assert folder.events == [('enabled',repair.TASK,False)]


def test_next_run_uses_local_wall_weekday_and_skips_weekend():
    assert repair.next_weekday_run(INSTALL_NOW) == datetime(2026,11,2,9,10)
    assert repair.next_weekday_run(INSTALL_NOW.replace(day=2,hour=8)) == datetime(2026,11,2,9,10)
    assert repair.next_weekday_run(INSTALL_NOW.replace(day=6,hour=17)) == datetime(2026,11,9,9,10)


def test_semantic_rollback_handles_defaults_and_indentation_but_detects_action_drift():
    old = fixture_xml('09:10:00', gate=True)
    tree = ET.fromstring(old)
    ET.SubElement(tree.find('t:Settings',repair.NS), '{'+repair.NS['t']+'}Enabled').text = 'true'
    ET.SubElement(tree.find('t:Principals/t:Principal',repair.NS), '{'+repair.NS['t']+'}RunLevel').text = 'LeastPrivilege'
    ET.indent(tree)
    rewritten = ET.tostring(tree,encoding='unicode')
    assert repair.semantic_task(old) == repair.semantic_task(rewritten)
    tree.find('t:Actions/t:Exec/t:Arguments',repair.NS).text += ' --other'
    assert repair.semantic_task(old) != repair.semantic_task(ET.tostring(tree,encoding='unicode'))


def test_installer_uses_pinned_policy_validation(tmp_path, monkeypatch):
    from tools import g3_live_standing as standing
    paths = []
    monkeypatch.setattr(standing,'read_json',lambda path: paths.append(path) or {'source':str(path)})
    monkeypatch.setattr(standing.op,'validate_identity',lambda state: None)
    def reject(policy, day):
        assert day is None
        raise ValueError('Pinned V15 mismatch')
    monkeypatch.setattr(standing,'validate_policy',reject)
    with pytest.raises(ValueError,match='Pinned V15 mismatch'):
        repair.validate_authorization(tmp_path)
    assert len(paths) == 2


def test_failed_fallback_disable_reports_critical_without_touching_target(tmp_path, monkeypatch):
    folder, kwargs = install_fixture(tmp_path, monkeypatch, 'rollback_raise')
    folder.GetTask(repair.TASK).deny_disable = True
    with pytest.raises(RuntimeError, match='ROLLBACK FAILED:.*CRITICAL: could not verify standing gate disabled'):
        repair.install_replacement(folder, **kwargs)
    assert folder.GetTask(repair.TARGET).Xml == kwargs['target_xml']
    assert not any(event[1] == repair.TARGET for event in folder.events)


def test_default_main_plan_never_registers_or_disables_tasks(tmp_path, monkeypatch, capsys):
    import win32com.client
    folder, kwargs = install_fixture(tmp_path, monkeypatch)
    service = SimpleNamespace(Connect=lambda: None, GetFolder=lambda _:folder)
    monkeypatch.setattr(win32com.client, 'Dispatch', lambda _:service)
    monkeypatch.setattr(repair, 'validate_authorization', lambda:None)
    assert repair.main([]) == 0
    assert '"mode": "PLAN_ONLY"' in capsys.readouterr().out
    assert not folder.registrations and not folder.events
    assert folder.GetTask(repair.TASK).Xml == kwargs['old_xml']
    assert folder.GetTask(repair.TARGET).Xml == kwargs['target_xml']
    assert not list(tmp_path.iterdir())
