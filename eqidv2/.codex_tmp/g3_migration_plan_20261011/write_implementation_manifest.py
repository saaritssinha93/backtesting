"""Record implementation scope; no runtime or credential contents are read."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AUDIT = Path(__file__).parent
baseline = {item['path']: item.get('sha256') for item in json.loads((AUDIT / 'source_inventory.json').read_text())['files']}
groups = {
    'Contract and routing': ['fno_strategy_registry.py','fno_live_entry_guard.py','fno_live_profile.py','configs/strategies/active_profile.json','configs/strategies/v13_v10_g_3_production_v1.json'],
    'G3 adapters and independent replay': [p.name for p in ROOT.glob('fno_v13_v10_g_3*.py')] + ['fno_v13_replay_source.py','backtesting_result_v13_v10_g_3_daily.py'],
    'Shared runtime': ['fno_v5_live.py','fno_v6_live_kite_session.py','fno_equity_fetch_1min.py','fno_v13_v10_g_paper.py','fno_v13_v10_g_options_paper.py','preopen_session_healthcheck.py','preopen_session_autofix.py','ai_platform/observability/feature_ledger.py'],
    'Dashboards': ['log_dashboard_server.py','dashboard_flow.py','dashboard_flow.js','fno_eq_id_monitor_detail.py','dashboard_cockpit.py'],
    'AI and analysis': ['ai_platform/adapters/strategy.py','ai_platform/adapters/daily_replay.py','ai_platform/api/repository.py','ai_platform/api/registry.py','ai_platform/observability/runtime_collector.py','ai_platform/observability/g3_analysis.py','ai_platform/observability/g3_execution_analysis.py','ai_platform/observability/g3_shadow_gate.py','docs/ai_platform/profile_registry.json','docs/ai_platform/source_registry.json','tools/v13_strategy_research.py','tools/v13_execution_research.py','tools/v13_prospective_shadow.py','tools/v13_shadow_automation.py'],
    'Launchers and scheduler migration': [p.relative_to(ROOT).as_posix() for p in (ROOT/'bat').glob('*g_3*.bat')] + ['bat/migrate_fno_v13_v10_g_to_g3.ps1','bat/rollback_fno_v13_v10_g3_tasks.ps1','bat/preflight_fno_v13_v10_g_3.ps1','bat/g3_task_xml_helpers.ps1'],
    'Focused tests': ['tests/test_fno_v13_v10_g_3_contract.py','tests/test_fno_v13_v10_g_3_runtime.py','tests/test_fno_v13_v10_g_3_daily_replay.py','tests/test_dashboard_g3_migration.py','tests/test_ai_platform_g3.py','tests/test_g3_execution_analysis.py','tests/test_g3_task_migration.ps1','tests/test_dashboard_fno_eq_id_monitor.py'],
    'Operational documentation': ['FNO_PAPERTRADE_DASHBOARD_OPERATIONS.md','V13_V10_G_DASHBOARD_INTEGRATION.md','V13_V10_G_DAILY_BACKTEST.md','OPTIONS_V13_V10_G_PAPER_TRADING.md','docs/DASHBOARD_FLOW.md','docs/OBSERVABILITY_OPERATIONS.md'],
}
records = []
groups['AI and analysis'] += ['ai_platform/api/app.py','ai_platform/observability/live_trust.py','bat/observability/Test-V13V10GLiveTrust.ps1']
groups['Focused tests'] += ['tests/test_ai_platform_api_stage2.py','tests/test_observability_live_trust.py']
groups['Launchers and scheduler migration'] += ['bat/schedule_fno_oi_weekday.ps1','bat/schedule_backtesting_result_v13_v10_g_weekday.bat','bat/schedule_fno_v13_v10_g_options_weekday.ps1','bat/schedule_fno_v13_v10_g_live_kite_qty1_weekday.ps1']
groups['Focused tests'] += ['tests/test_g3_legacy_installer_guards.py']
groups['Launchers and scheduler migration'] += ['bat/run_v13_strategy_research_refresh.bat']
for group, paths in groups.items():
    for name in sorted(set(paths)):
        path = ROOT / name
        if not path.is_file():
            raise ValueError(f'Missing implementation file: {name}')
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        prior = baseline.get(name)
        action = 'Retained dependency' if prior == digest else 'Updated existing integration' if prior else 'Added / newly inventoried integration'
        records.append(dict(path=name, group=group, action=action, sha256=digest, baseline_sha256=prior))
payload = dict(schema_version='g3_implementation_file_inventory_v1', date='2026-10-12',
               scope='Reviewed implementation files, not every dirty file in the shared workspace',
               execution_authority=False, file_count=len(records), files=records)
(AUDIT/'implementation_files.json').write_text(json.dumps(payload,indent=2),encoding='utf-8')
document = ROOT/'docs/V13_V10_G3_MIGRATION_FILE_MANIFEST_2026-10-11.md'
body = document.read_text(encoding='utf-8')
marker = '\n## Implemented file coverage — 12 October 2026\n'
body = body.split(marker)[0]
body = body.replace('**Plan only — 11 October 2026.**','**Original inventory: 11 October 2026; implementation coverage appended 12 October 2026.**')
lines = [marker, f'The implementation inventory below covers **{len(records)} concrete files**. The original 548-file dependency review remains above; unchanged transitive dependencies and frozen evidence are retained, not renamed. Exact file hashes are recorded in `.codex_tmp/g3_migration_plan_20261011/implementation_files.json`. Concurrent visual and V14 changes in the shared workspace are not claimed as migration work.',
         '', 'Planned name substitutions: `fno_v13_v10_g_3_selection.py` is implemented inside the shared pure `fno_v13_v10_g_3_contract.py`; the neutral snapshot module is `fno_v13_replay_source.py`; readiness is `fno_v13_v10_g_3_preflight.py`; task conversion/scheduling are handled by `bat/migrate_fno_v13_v10_g_to_g3.ps1` and `bat/g3_task_xml_helpers.ps1`; options routing uses the G3 adapter with the unchanged shared options configuration.',
         '', 'Runtime additions remain protected under `C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/{production,history,reports,latest}`, `fno_oi/v13_v10_g_3_live`, `fno_oi/v13_v10_g_3_options_paper`, `v13_v10_g_3_strategy_research` and `v13_v10_g_3_execution_research`. Task rollback XMLs are in `.codex_tmp/g3_task_rollback_20261012_retry`; sanitized deployment verification is in `.codex_tmp/g3_migration_plan_20261011/deployed_tasks_audit.json`. No archive operation was performed.', '']
for group in groups:
    lines += ['### '+group,'','| File | Implementation disposition |','| --- | --- |']
    lines += [f"| `{row['path']}` | {row['action']} |" for row in records if row['group']==group]
    lines.append('')
document.write_text(body+'\n'.join(lines),encoding='utf-8')
print(json.dumps(dict(files=len(records),missing=0,manifest=str(document))))
