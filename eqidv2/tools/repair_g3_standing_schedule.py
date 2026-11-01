"""Operator-only installer for the Python G3 standing gate; default is read-only."""
from __future__ import annotations

import argparse
from datetime import datetime, time, timedelta
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TASK = 'EQIDV2_g3_live_standing_approval_0910'
TARGET = 'EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915'
NS = {'t': 'http://schemas.microsoft.com/windows/2004/02/mit/task'}
ET.register_namespace('', NS['t'])
if not __debug__:
    raise RuntimeError('Schedule repair requires Python validation checks; do not use optimized mode')


def text_at(tree, path):
    return tree.findtext(path, namespaces=NS)


def validate_schedule(tree, clock):
    triggers = tree.findall('t:Triggers/*', NS)
    assert len(triggers) == 1 and triggers[0].tag.endswith('}CalendarTrigger'), 'Expected one weekly trigger'
    trigger = triggers[0]
    assert text_at(trigger, 't:Enabled') in (None, 'true'), 'Trigger is disabled'
    stamp = text_at(trigger, 't:StartBoundary')
    assert stamp and datetime.fromisoformat(stamp).strftime('%H:%M:%S') == clock
    assert text_at(trigger, 't:ScheduleByWeek/t:WeeksInterval') == '1'
    assert [n.tag.split('}')[-1] for n in trigger.findall('t:ScheduleByWeek/t:DaysOfWeek/*', NS)] == ['Monday','Tuesday','Wednesday','Thursday','Friday']
    assert trigger.find('t:Repetition', NS) is None and trigger.find('t:EndBoundary', NS) is None
    assert text_at(tree, 't:Settings/t:MultipleInstancesPolicy') == 'IgnoreNew'
    assert text_at(tree, 't:Settings/t:StartWhenAvailable') in (None, 'false')
    assert tree.find('t:Settings/t:RestartOnFailure', NS) is None
    assert text_at(tree, 't:Principals/t:Principal/t:LogonType') == 'InteractiveToken'
    assert text_at(tree, 't:Principals/t:Principal/t:RunLevel') in (None, 'LeastPrivilege')
    assert len(tree.findall('t:Principals/t:Principal', NS)) == 1
    assert len(tree.findall('t:Actions/*', NS)) == 1


def new_action(root=ROOT):
    return str(Path(sys.executable).resolve()), subprocess.list2cmdline([
        '-u', str(root/'tools/g3_live_standing.py'), '--apply', '--standing-policy-confirmed'])


def prepared_xml(old_xml, target_xml, root=ROOT):
    tree, target = ET.fromstring(old_xml), ET.fromstring(target_xml)
    validate_schedule(tree, '09:10:00')
    validate_schedule(target, '09:15:00')
    assert text_at(tree, 't:Settings/t:ExecutionTimeLimit') == 'PT3M'
    assert text_at(tree, 't:Settings/t:Enabled') in (None, 'true'), 'Standing gate is disabled; review separately'
    target_actions = target.findall('t:Actions/t:Exec', NS)
    assert len(target_actions) == 1
    cmd = str(Path(os.environ.get('SystemRoot', 'C:/Windows'))/'System32/cmd.exe')
    assert (text_at(target_actions[0], 't:Command') or '').lower() == cmd.lower()
    assert text_at(target_actions[0], 't:Arguments') == '/d /c ""' + str(root/'bat/run_g3_approved_live.bat') + '""'
    assert text_at(target_actions[0], 't:WorkingDirectory') in (None, '')
    actions = tree.findall('t:Actions/t:Exec', NS)
    assert len(actions) == 1
    action = actions[0]
    command, arguments = new_action(root)
    old_command = str(Path(os.environ.get('SystemRoot', 'C:/Windows'))/'System32/WindowsPowerShell/v1.0/powershell.exe')
    old_arguments = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + str(root/'bat/activate_g3_live_standing.ps1') + '"'
    current = (text_at(action, 't:Command'), text_at(action, 't:Arguments'))
    assert current in ((old_command, old_arguments), (command, arguments)), 'Unexpected gate action; refusing replacement'
    assert text_at(action, 't:WorkingDirectory') == str(root)
    action.find('t:Command', NS).text = command
    action.find('t:Arguments', NS).text = arguments
    return ET.tostring(tree, encoding='unicode')


def control_hashes(root=ROOT):
    # Match the standing gate's fixed runtime, even if the invoking shell has
    # an unrelated research/paper EQIDV2_RUNTIME_ROOT override.
    runtime = Path('C:/TradingData/eqidv2')
    paths = [root/'configs/strategies/active_profile.json',
             root/'configs/authorizations/g3_live_standing_policy_v3.json',
             runtime/'fno_oi/v13_v10_g_3_live/live_arm.json']
    return {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}


def validate_authorization(root=ROOT):
    """Installer must use the gate's pinned identity, not only dynamic config."""
    from tools import g3_live_standing as standing
    standing.op.validate_identity(standing.read_json(root/'configs/strategies/active_profile.json'))
    standing.validate_policy(standing.read_json(root/'configs/authorizations/g3_live_standing_policy_v3.json'), None)


def action_identity(xml):
    tree = ET.fromstring(xml)
    actions = tree.findall('t:Actions/*', NS)
    assert len(actions) == 1 and actions[0].tag.endswith('}Exec'), 'Expected exactly one Exec action'
    return text_at(actions[0], 't:Command'), text_at(actions[0], 't:Arguments')


def semantic_task(xml):
    """Compare task content despite XML indentation, prefixes and default fields.

    Keep trigger/action ordering and exact leaf values, including executable
    arguments and principal identity. Settings/metadata child order is immaterial.
    """
    tree = ET.fromstring(xml)
    defaults = (
        ('t:Settings', 'Enabled', 'true'),
        ('t:Settings', 'StartWhenAvailable', 'false'),
        ('t:Principals/t:Principal', 'RunLevel', 'LeastPrivilege'),
        ('t:Triggers/t:CalendarTrigger', 'Enabled', 'true'),
    )
    for parent_path, name, value in defaults:
        parent = tree.find(parent_path, NS)
        if parent is not None and parent.find('t:'+name, NS) is None:
            ET.SubElement(parent, '{'+NS['t']+'}'+name).text = value
    ordered = {'Actions', 'Triggers', 'DaysOfWeek'}
    def freeze(node):
        children = [freeze(child) for child in node]
        if node.tag.split('}')[-1] not in ordered:
            children.sort(key=repr)
        text = '' if children else (node.text or '')
        return node.tag, tuple(sorted(node.attrib.items())), text, tuple(children)
    return freeze(tree)


def next_weekday_run(now):
    wall = now.replace(tzinfo=None)
    for offset in range(8):
        day = wall.date() + timedelta(days=offset)
        candidate = datetime.combine(day, time(9, 10))
        if day.weekday() < 5 and candidate > wall:
            return candidate
    raise ValueError('Cannot determine the next weekly gate trigger')


def verify_registered(task, expected_xml, *, expected_action, expected_next_run):
    assert task.Enabled and task.State == 3 and not list(task.GetInstances(0)), 'Standing gate is not enabled/Ready/idle'
    assert action_identity(task.Xml) == expected_action, 'Registered gate did not retain the exact requested action'
    assert semantic_task(task.Xml) == semantic_task(expected_xml), 'Registered gate definition differs from the reviewed definition'
    # COM DATE is scheduler local wall time, regardless of a pywin32 UTC tag.
    assert task.NextRunTime.replace(tzinfo=None) == expected_next_run, 'Registered gate next run differs from the next weekday 09:10'


def install_replacement(folder, *, old_xml, target_xml, replacement, before, backup, root=ROOT, now=None):
    """Only explicit installer Apply calls this; all tests use fake COM tasks."""
    task = folder.GetTask(TASK)
    if now is None:
        from tools.g3_live_standing import IST
        now = datetime.now(IST)
    expected_run = next_weekday_run(now)
    principal = task.Definition.Principal.UserId
    assert not list(task.GetInstances(0)), 'Standing gate started before installation'
    assert not list(folder.GetTask(TARGET).GetInstances(0)), 'LIVE target started before installation'
    assert semantic_task(task.Xml) == semantic_task(old_xml), 'Standing gate changed before installation'
    assert semantic_task(folder.GetTask(TARGET).Xml) == semantic_task(target_xml), 'LIVE target changed before installation'
    assert control_hashes(root) == before, 'Control files changed before installation'
    try:
        folder.RegisterTask(TASK, replacement, 6, principal, None, 3)
        installed = folder.GetTask(TASK)
        # prepared_xml accepts both legacy and new actions for idempotency;
        # post-registration verification must require only the new action.
        verify_registered(installed, replacement, expected_action=new_action(root), expected_next_run=expected_run)
        prepared_xml(installed.Xml, folder.GetTask(TARGET).Xml, root)
        assert control_hashes(root) == before, 'Control files changed during schedule installation'
        assert semantic_task(folder.GetTask(TARGET).Xml) == semantic_task(target_xml), 'LIVE target task changed during installation'
        (backup/'after.xml').write_text(installed.Xml, encoding='utf-8')
        return installed
    except Exception as original:
        try:
            folder.RegisterTask(TASK, old_xml, 6, principal, None, 3)
            restored = folder.GetTask(TASK)
            verify_registered(restored, old_xml, expected_action=action_identity(old_xml), expected_next_run=expected_run)
            assert control_hashes(root) == before, 'Control files changed; rollback cannot attest prior authorization'
            assert semantic_task(folder.GetTask(TARGET).Xml) == semantic_task(target_xml), 'LIVE target changed; rollback cannot attest prior target'
        except Exception as rollback:
            # Never change the LIVE target or approval files during installation.
            # If restoration is uncertain, only the standing approval gate is disabled.
            try:
                folder.GetTask(TASK).Enabled = False
                disabled = folder.GetTask(TASK)
                assert disabled.Enabled is False and disabled.State == 1, 'Standing gate disable did not reconcile'
                assert not list(disabled.GetInstances(0)), 'Standing gate is disabled but an instance is still running'
                cleanup = 'standing gate disabled and verified; LIVE target and authorization files untouched'
            except Exception as disable_error:
                cleanup = f'CRITICAL: could not verify standing gate disabled: {type(disable_error).__name__}: {disable_error}'
            raise RuntimeError(f'Install failed: {original}; ROLLBACK FAILED: {rollback}; {cleanup}') from original
        raise RuntimeError(f'Install failed: {original}; prior standing task restored and verified') from original


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--operator-confirmed', action='store_true')
    args = parser.parse_args(argv)
    if args.apply and not args.operator_confirmed:
        parser.error('--apply requires --operator-confirmed: this restores scheduled real-order authorization')
    try:
        import win32com.client
        service = win32com.client.Dispatch('Schedule.Service')
        service.Connect()
        folder = service.GetFolder('\\')
        task, target = folder.GetTask(TASK), folder.GetTask(TARGET)
        assert not list(task.GetInstances(0)), 'Standing gate is currently running'
        assert not list(target.GetInstances(0)), 'LIVE target is currently running'
        assert (ROOT/'tools/g3_live_standing.py').is_file(), 'Python gate is missing'
        from tools import g3_live_operator as operator
        validate_authorization()
        old_xml, target_xml = task.Xml, target.Xml
        replacement = prepared_xml(old_xml, target_xml)
        before = control_hashes()
        report = dict(mode='INSTALL' if args.apply else 'PLAN_ONLY', task=TASK,
                      next_run_local_wall=str(task.NextRunTime),
                      action=dict(zip(('command','arguments'), new_action())),
                      restores_automatic_live_authorization=args.apply,
                      writes_approval_now=False, starts_worker_now=False,
                      execution_limit='PT3M', duplicate_policy='IgnoreNew', catch_up=False)
        if args.apply:
            now = datetime.now(operator.config.IST)
            assert not (now.weekday()<5 and '09:09' <= now.strftime('%H:%M') <= '15:45'), 'Install outside the market/authorization window'
            backup = ROOT/'outputs/readiness_repair_20261102'/('standing_install_'+now.strftime('%Y%m%dT%H%M%S%f'))
            backup.mkdir(parents=True, exist_ok=False)
            (backup/'before.xml').write_text(old_xml, encoding='utf-8')
            (backup/'reviewed_target.xml').write_text(target_xml, encoding='utf-8')
            installed = install_replacement(folder, old_xml=old_xml, target_xml=target_xml,
                                            replacement=replacement, before=before, backup=backup, now=now)
            report.update(installed=True, backup=str(backup), next_run_local_wall=str(installed.NextRunTime))
            (backup/'result.json').write_text(json.dumps(report, indent=2))
        print(json.dumps(report, indent=2))
        return 0
    except Exception as exc:
        print(f'G3 schedule repair blocked: {type(exc).__name__}: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
