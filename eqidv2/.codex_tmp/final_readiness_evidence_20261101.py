"""Persist final read-only operational evidence after the repairs."""
from pathlib import Path
from datetime import datetime, timedelta, timezone
import json
import shutil
import sys
import win32com.client

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from tools.repair_g3_standing_schedule import semantic_task
from tools.fno_data_supervisor import native_processes

OUT=ROOT/'outputs/readiness_repair_20261102'
service=win32com.client.Dispatch('Schedule.Service'); service.Connect()
folder=service.GetFolder('\\')
names=['EQIDV2_authentication_v2_0900','EQIDV2_fno_options_atm_fetch_5min_0907',
       'EQIDV2_g3_live_standing_approval_0910','EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915',
       'EQIDV2_fno_v13_v10_g_3_options_live_long_0915','EQIDV2_fno_v13_v10_g_3_options_live_short_0915']
tasks=[]
for name in names:
    task=folder.GetTask(name)
    tasks.append(dict(name=name,enabled=bool(task.Enabled),state=int(task.State),
        next_run_ist_local=task.NextRunTime.replace(tzinfo=None).isoformat(),last_result=int(task.LastTaskResult),
        runtime_limit=task.Definition.Settings.ExecutionTimeLimit,
        actions=[dict(command=a.Path,arguments=a.Arguments) for a in task.Definition.Actions]))
gate=folder.GetTask(names[2]); live=folder.GetTask(names[3])
assert semantic_task(gate.Xml)==semantic_task((OUT/'standing_before.xml').read_text())
assert semantic_task(live.Xml)==semantic_task((OUT/'live_target_before.xml').read_text())
profile=json.loads((ROOT/'configs/strategies/active_profile.json').read_text(encoding='utf-8-sig'))
arm=json.loads(Path('C:/TradingData/eqidv2/fno_oi/v13_v10_g_3_live/live_arm.json').read_text(encoding='utf-8-sig'))
workers=[]
for process in native_processes():
    command=process.get('command','').lower()
    if any(token in command for token in ('fno_options_atm_fetch_5min.py','fno_v13_v10_g_3_live_kite_session.py')):
        workers.append(dict(pid=process['pid'],name=process['name']))
report=dict(checked_at_ist=datetime.now(timezone(timedelta(hours=5,minutes=30))).isoformat(),
    session='2026-11-02',tasks=tasks,workers=workers,disk_free_bytes=shutil.disk_usage('C:/').free,
    options_paper=dict(state='SCHEDULER_REPAIRED',old_stuck_processes_removed=True,
        powershell_free_launcher=True,after_hours_scheduler_smoke='PASS; SKIPPED_CUTOFF; no worker launched',
        dated_contract_acquisition='Must obtain genuine Nov2 master after fresh morning authentication; future download not yet verified'),
    g3_live=dict(state='SOFTWARE_REPAIRED_OPERATOR_SCHEDULE_INSTALL_PENDING',
        strategy_version=profile['strategy_version'],approval_date=profile['live_approval']['session_date'],
        arm_date=arm['session_date'],standing_schedule_unchanged=True,live_target_unchanged=True,
        user_command='bat\\repair_g3_live_schedule.bat --apply --operator-confirmed',
        instruction='Run in Command Prompt from project root tonight or before 09:09 IST. Restores automatic real-broker authorization; assistant did not apply.',
        next_checks='08:30 authentication; 09:10 fresh clock, health and flat broker evidence; 09:15 approved worker launch'),
    verification=dict(combined='431 passed, 4 deselected',
        excluded='Four legacy PowerShell wrapper simulation cases; native replacement tests included',
        additional_known_legacy_failures=['test_options_cards_are_separate_from_existing_g_cards_and_fully_registered',
          'test_requested_options_session_titles_and_direction_mapping_are_explicit','test_dashboard_contract_matches_options_engine_publishers'],
        legacy_failure_context='Expanded legacy dashboard suite expects G labels/paths while active profile is G3; these three expectations were not changed by this repair',
        read_only_live_plan='PASS V15 / 34 setups',clock_probe_skew_seconds=-0.546,
        real_live_execution_tested=False,broker_orders_submitted=False))
(OUT/'final_readiness.json').write_text(json.dumps(report,indent=2))
print(json.dumps({k:report[k] for k in ('session','options_paper','g3_live','disk_free_bytes')},indent=2))
