"""Operator-only installer for the Python G3 standing gate; default is read-only."""
from __future__ import annotations

import argparse
from datetime import datetime
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
    runtime = Path(os.environ.get('EQIDV2_RUNTIME_ROOT', 'C:/TradingData/eqidv2'))
    paths = [root/'configs/strategies/active_profile.json',
             root/'configs/authorizations/g3_live_standing_policy_v3.json',
             runtime/'fno_oi/v13_v10_g_3_live/live_arm.json']
    return {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}


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
        operator.validate_identity(operator.read(operator.PROFILE))
        operator.load_standing_policy()
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
            try:
                installed = folder.RegisterTask(TASK, replacement, 6, task.Definition.Principal.UserId, None, 3)
                assert installed.Enabled and installed.State == 3 and not list(installed.GetInstances(0))
                prepared_xml(installed.Xml, target.Xml)
                assert control_hashes() == before, 'Control files changed during schedule installation'
                assert folder.GetTask(TARGET).Xml == target_xml, 'LIVE target task changed during installation'
                (backup/'after.xml').write_text(installed.Xml, encoding='utf-8')
                report.update(installed=True, backup=str(backup), next_run_local_wall=str(installed.NextRunTime))
            except Exception:
                folder.RegisterTask(TASK, old_xml, 6, task.Definition.Principal.UserId, None, 3)
                raise
            (backup/'result.json').write_text(json.dumps(report, indent=2))
        print(json.dumps(report, indent=2))
        return 0
    except Exception as exc:
        print(f'G3 schedule repair blocked: {type(exc).__name__}: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
