from pathlib import Path
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
