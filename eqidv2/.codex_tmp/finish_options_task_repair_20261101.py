"""Finish the verified Oct30 options-data-only stale process cleanup."""
from pathlib import Path
import ctypes
from ctypes import wintypes as W
from datetime import datetime, timezone
import json
import sys
import win32com.client

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from tools.fno_data_supervisor import native_processes
service=win32com.client.Dispatch('Schedule.Service'); service.Connect()
folder=service.GetFolder('\\')
name='EQIDV2_fno_options_atm_fetch_5min_0907'
task=folder.GetTask(name)
assert not list(task.GetInstances(0)), 'Scheduled task is still running.'
owners=[p for p in native_processes() if 'fno_options_atm_fetch_5min' in p.get('command','').lower()]
assert len(owners)==1, owners
owner=owners[0]
assert (owner['pid'],owner['ppid'],owner['name'].lower())==(38748,2692,'powershell.exe')
assert 'supervise_command.ps1' in owner['command'].lower()
assert str(ROOT).lower() in owner['command'].lower()
k=ctypes.WinDLL('kernel32',use_last_error=True)
k.OpenProcess.argtypes=[W.DWORD,W.BOOL,W.DWORD]; k.OpenProcess.restype=W.HANDLE
k.GetProcessTimes.argtypes=[W.HANDLE,*([ctypes.POINTER(W.FILETIME)]*4)]
k.TerminateProcess.argtypes=[W.HANDLE,W.UINT]
k.WaitForSingleObject.argtypes=[W.HANDLE,W.DWORD]
k.CloseHandle.argtypes=[W.HANDLE]
handle=k.OpenProcess(0x100001|0x1000,False,owner['pid'])
assert handle, ctypes.get_last_error()
try:
    times=[W.FILETIME() for _ in range(4)]
    assert k.GetProcessTimes(handle,*(ctypes.byref(x) for x in times))
    created=(times[0].dwHighDateTime<<32|times[0].dwLowDateTime)/1e7-11644473600
    created_utc=datetime.fromtimestamp(created,timezone.utc)
    assert created_utc.date().isoformat()=='2026-10-30', created_utc
    assert k.TerminateProcess(handle,75)
    assert k.WaitForSingleObject(handle,5000)==0
finally:
    k.CloseHandle(handle)
assert not [p for p in native_processes() if 'fno_options_atm_fetch_5min' in p.get('command','').lower()]
definition=task.Definition
actions=list(definition.Actions)
assert len(actions)==1 and Path(actions[0].Path).resolve()==(ROOT/'bat/run_fno_options_atm_fetch_5min.bat').resolve()
assert not actions[0].Arguments
assert definition.Principal.LogonType==3 and definition.Settings.MultipleInstances==2
output=ROOT/'outputs/readiness_repair_20261102'
assert (output/'options_task_before.xml').is_file()
definition.Settings.ExecutionTimeLimit='PT7H'
installed=folder.RegisterTaskDefinition(name,definition,6,definition.Principal.UserId,None,3)
assert installed.Enabled and installed.State==3 and installed.Definition.Settings.ExecutionTimeLimit=='PT7H'
assert not list(installed.GetInstances(0))
(output/'options_task_after.xml').write_text(installed.Xml,encoding='utf-8')
report=dict(task=name,enabled=installed.Enabled,state='READY',runtime_limit='PT7H',
            next_run_local_wall=str(installed.NextRunTime),
            stopped_stale_data_process=dict(pid=owner['pid'],created_utc=created_utc.isoformat()),
            no_remaining_options_data_owner=True,started_worker=False)
(output/'options_task_repair.json').write_text(json.dumps(report,indent=2))
print(json.dumps(report,indent=2))
