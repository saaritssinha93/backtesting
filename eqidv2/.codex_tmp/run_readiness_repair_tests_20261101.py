"""Run the combined offline checks; no real LIVE workers or authorization."""
from pathlib import Path
import json
import subprocess
import sys
from datetime import datetime

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'outputs/readiness_repair_20261102'
tests=[
    'tests/test_g3_live_standing_native.py',
    'tests/test_g3_standing_schedule_repair.py',
    'tests/test_g3_live_supervisor.py',
    'tests/test_g3_live_operator.py',
    'tests/test_fno_data_supervisor.py',
    'tests/test_fno_options_atm_fetch_5min.py',
    'tests/test_authentication_v2_driver_bootstrap.py',
    'tests/test_preopen_fast_production_readiness.py',
    'tests/test_fno_v13_v10_g_3_runtime.py',
    'tests/test_fno_v6_live_kite_session.py',
    'tests/test_dashboard_fno_v13_v10_g_options.py::test_options_scheduler_defines_one_fetch_and_four_paper_tasks_without_starting_them',
]
command=[sys.executable,'-m','pytest',*tests,'-q','-k','not operator_task_transition_and_failure_restore_are_simulated_only']
result=subprocess.run(command,cwd=ROOT,capture_output=True,text=True,timeout=150)
log=(result.stdout or '')+(result.stderr or '')
(OUT/'combined_tests.log').write_text(log,encoding='utf-8')
(OUT/'test_command.json').write_text(json.dumps(dict(timestamp=datetime.now().isoformat(),
    command=command,exit_code=result.returncode,
    exclusions='Four legacy PowerShell wrapper simulation cases excluded because this host hangs in PowerShell. Replacement Python transaction tests included.'),indent=2))
print(log)
raise SystemExit(result.returncode)
