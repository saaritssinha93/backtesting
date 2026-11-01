"""Write a sanitized, read-only FnO recovery evidence snapshot."""
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
IST = timezone(timedelta(hours=5, minutes=30))

def kv(path):
    return dict(line.split('=', 1) for line in path.read_text(encoding='utf-8-sig').splitlines() if '=' in line)

cards = json.loads(subprocess.check_output([sys.executable, str(Path(__file__).with_name('dashboard_readiness.py'))], cwd=ROOT, text=True))
services = {}
for name in ('fno_oi_fetch_5min_fast_production', 'fno_oi_feature_ranker', 'eqidv2_eod_scheduler_for_5mins_data_live_minimal'):
    raw = kv(ROOT / 'logs' / f'{name}.supervisor.status')
    services[name] = {key: raw.get(key) for key in ('status', 'reason', 'ts', 'run_id', 'worker_pid', 'restart_count', 'exit_code')}
arm = json.loads(Path('C:/TradingData/eqidv2/fno_oi/v13_v10_g_3_live/live_arm.json').read_text(encoding='utf-8-sig'))
report = {
    'observed_at_ist': datetime.now(IST).isoformat(timespec='seconds'),
    'dashboard': 'http://127.0.0.1:8787/ - FnO section',
    'verification_method': 'Exact local dashboard backend status adapters, scheduler and worker evidence; browser UI unavailable',
    'root_cause': 'PowerShell processes waited on Defender-owned ALPC; this stalled Chrome-version bootstrap, futures fetch/ranker and cash-price data launcher before workers started.',
    'host_evidence': str(ROOT / '.codex_tmp/g3_recovery_20261030/powershell_host_diagnosis.txt'),
    'repairs': [
        'Bounded native Chrome/driver discovery replaces the hanging PowerShell dependency; standard auth task restarted and all eight apps authenticated for Oct30 at10:48:38IST.',
        'Fixed-service Python supervisor replaces PowerShell only for futures fetch, ranker and cash5m data jobs, retaining worker arguments, duplicate-writer checks, singleton lock, freshness, restart and cutoff controls.',
        'Stopped only revalidated stale data/auth task trees, ran existing scheduled tasks, verified fresh same-session data.',
        'Equity PAPER long/short restarted after verifying zero current orders to load protective management fixes.',
        'Validated saved open positions can continue protective management despite blocked entry signals; rejected exit submissions retain an explicit rejection without duplicate retry.'
    ],
    'tests': {
        'supervisor': {'passed': 34, 'scope': 'Native lock/job, fixed commands, conflicts, stable heartbeat rereads, freshness, cutoff, restart and log rotation; simulated children only'},
        'authentication_driver_bootstrap': {'passed': 8},
        'protective_management_and_slot_boundary': {'passed': 67, 'broker': 'in-memory fixtures; no real orders'},
        'legacy_powershell_date_gate': {'failed': 2, 'reason': 'Existing host PowerShell hang caused15second subprocess timeouts'},
        'git_diff_check': 'PASS for files changed in this repair'
    },
    'services': services,
    'extended_verification_correction': 'One transient heartbeat read caused an unnecessary fetch restart at10:58:53. Added three stable, complete rereads bounded by two100ms waits; persistent stale or wrong-run evidence still blocks. All three data supervisor tasks restarted between completed slots at11:02 to load this final change.',
    'fno_dashboard_cards': cards,
    'live': {
        'arm_session_date': arm.get('session_date'),
        'arm_file_enabled_flag': arm.get('enabled'),
        'current_strategy': arm.get('strategy_version'),
        'setup_count': len(arm.get('setup_ids', [])),
        'new_dated_authorization_created': False,
        'live_workers_started': False,
        'orders_submitted_by_repair': False,
        'remaining': 'Oct30 dated operator activation absent. Existing LIVE launcher/standing gate still depend on the impaired PowerShell host; final activation permitting real orders is the operator action.'
    },
    'historical_techm': {'reconciled': False, 'details': str(ROOT / 'outputs/g3_runtime_repair_20261030/techm_execution_audit.json')},
    'host_capacity': {
        'c_free_bytes': shutil.disk_usage('C:/').free,
        'archive_recovery': {
            'file': 'logs/eqidv2_eod_scheduler_for_5mins_data_live_minimal.log.20260831_101950.bak',
            'action': 'Reversible NTFS compression of this one archived log; no deletion',
            'logical_bytes': 1016221201,
            'compressed_stored_bytes': 257839104,
            'sha256_before_and_after': 'eefcc2522a37aa4191a88c5a1f84a4e55cc2d819549cf7f7ddb82d4d7e5f1944'
        },
        'additional_archive_compression': {
            'manifest': str(ROOT / 'outputs/g3_runtime_repair_20261030/archive_compression.json'),
            'files_with_matching_before_after_sha256': 107,
            'stored_bytes_reclaimed': 638942431,
            'scope': 'Only inactive text logs last modified Oct28 or earlier in workspace logs and runtime observability/logs; all contents retained'
        },
        'remaining': 'C drive still low; Windows pagefile expanded during data-worker startup. User file contents retained; no pagefile/security settings changed.'
    },
    'verified_recovered_cycles': {
        '10:50': {'cash_complete': '221/221', 'scanner_candidates': {'LONG': 1, 'SHORT': 4}, 'one_minute_feed': '5/5', 'selected_signals': 0},
        '10:55': {'cash_complete': '221/221', 'fno_cash_coverage': '218/218', 'futures_complete': '223/224; one verified no-candle exclusion under unchanged coverage rule', 'scanner_candidates': {'LONG': 1, 'SHORT': 2}, 'one_minute_feed': '3/3', 'confirmation_completed_at_ist': '2026-10-30T10:56:04+05:30', 'selected_signals': 0},
        '11:05_after_final_supervisor_reload': {'cash_complete': '221/221', 'futures_complete': '223/224; one verified no-candle exclusion', 'ranker_completed_at_ist': '2026-10-30T11:05:37+05:30', 'supervisor_restarts': 0, 'note': 'Data-only slot; next configured G3 signal slot is11:10'},
        'earlier_blocked_slots_preserved': 14
    },
    'data_integrity': 'Earlier blocked/missed slots are preserved. No fabricated historical fills, forced signals or retrospective real entries.'
}
destination = ROOT / 'outputs/g3_runtime_repair_20261030/fno_recovery_status.json'
destination.parent.mkdir(parents=True, exist_ok=True)
destination.write_text(json.dumps(report, indent=2), encoding='utf-8')
print(json.dumps({'report': str(destination), 'observed_at_ist': report['observed_at_ist'], 'c_free_bytes': report['host_capacity']['c_free_bytes']}))
