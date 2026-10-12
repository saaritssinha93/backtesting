"""Merge read-only audits into the requested reviewable migration file plan."""
import json
from collections import Counter, defaultdict
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
BASE=Path(__file__).resolve().parent
read=lambda name:json.loads((BASE/name).read_text(encoding='utf-8-sig'))
scan=read('source_inventory.json'); strategy=read('strategy_audit.json'); runtime=read('runtime_audit.json'); dashboard=read('dashboard_data_audit.json')
rows={}
proposed={}

def add(path,evidence,action='',reason=''):
    p=path.replace('\\','/').removeprefix('./')
    if not (ROOT/p).is_file():
        if action.lower().startswith(('new','add','create','proposed')):
            proposed[p]=reason or action
        return
    r=rows.setdefault(p,{'path':p,'evidence':set(),'recommendations':[]})
    r['evidence'].add(evidence)
    if action:r['recommendations'].append((action,reason))

for r in scan['files']:
    add(r['path'],('direct ref:'+str(r['direct_reference_line'])) if r.get('direct_reference_line') else r['discovery'])
for r in strategy['file_action_inventory']:add(r['path'],'strategy audit',r['action'],r['reason'])
for name,files in runtime['required_migration_review'].items():
    for p in files:add(p,'runtime: '+name,'review')
for key in ['protected_shared_infrastructure','legacy_reactivation_paths_preserve_history']:
    for p in runtime[key]:add(p,'runtime: '+key)
for r in runtime['additions_missing_from_source_inventory']:add(r['path'],'runtime dependency audit')
for group in dashboard['file_groups']:
    for p in group['files']:add(p,'dashboard: '+group['surface'])
for p in ['.gitignore','../.gitignore','requirements.txt','requirements-dev.txt','pyproject.toml','pytest.ini','bat/schedule_v13_shadow_weekday.ps1','bat/run_v13_shadow_prepare.bat','bat/run_v13_shadow_seal.bat']:
    add(p,'build/operational boundary')

active_core={
'fno_live_profile.py','fno_v13_v10_g_live_config.py','fno_v13_v10_g_policy.py','fno_v13_v10_g_selection.py','fno_v13_v10_g_identity.py',
'fno_v5_live.py','fno_v6_live.py','fno_v6_live_config.py','fno_v6_live_kite_session.py','fno_v13_v10_g_paper.py','fno_equity_fetch_1min.py',
'fno_live_evidence.py','fno_v13_v10_g_daily_replay.py','fno_v13_v10_g_staged_replay.py','backtesting_result_v13_v10_g_daily.py',
'preopen_session_healthcheck.py','preopen_session_autofix.py','log_dashboard_server.py','fno_eq_id_monitor_detail.py','dashboard_fno_monitor.js',
'dashboard_flow.py','dashboard_flow.js','dashboard_flow.html','dashboard_flow_core.js','dashboard_flow_compare.js'}
historical_docs={'docs/ARCHIVE_CANDIDATES_REVIEW_2026-10-10.md','docs/OBSERVABILITY_ANALYSIS_2026-09-25.md','V13_V10_G_FULL_BACKTEST_RESULT_20261004.md'}
protected=set(runtime['protected_shared_infrastructure'])
legacy=set(runtime['legacy_reactivation_paths_preserve_history'])
def classify(p):
    low=p.lower(); name=Path(p).name.lower()
    if p.startswith('v14/') or name.startswith('test_v14') or p=='bat/run_v14_backtest.bat':return 'U','Preserve unrelated V14 work; verify shared dependency compatibility.'
    if any(s in name for s in ['token','secret','api_key','auth_v2_state']) or name.startswith('authentication'):
        return 'S','Protect auth/credential infrastructure; values excluded from plan.'
    if p.startswith('tests/'):
        return 'T','Retain historical assertions; extend G3 integration/compatibility coverage where relevant.'
    if p in historical_docs or p.startswith(('docs/v13_v10_g_web/','docs/v13_v10_g_options/')):
        return 'H','Retain original historical G/archival evidence; create separately labelled G3 outputs.'
    if p.startswith('research/') or p=='fno_v13_v10_g_3_backtest.py':
        return 'H','Preserve research/archive semantics; use as reference, not production entry authority.'
    if p in active_core:return 'M','Review/refactor active profile contract and integration; preserve legacy G behavior.'
    if p in {'fno_v13_v10_g_live.py','fno_v13_v10_g_live_kite_session.py'}:
        return 'C','Keep explicit G rollback entrypoint; add G3 sibling and switch approved callers.'
    if p.startswith('fno_v13_v10_g_options_'):
        if any(x in p for x in ['projection','chart','html','fixed_profile','backtest']):
            return 'H','Preserve G option research; add separate G3-source reporting if required.'
        return 'M','Adapt equity-source profile/identity; preserve option mechanics and PAPER-only guard.'
    if p.startswith(('fno_v13_v10_g_2','fno_v13_v10_g_backtest','fno_v13_v10_g_research','fno_v13_v10_g_expansion','fno_v13_v10_g_projection','fno_v13_v10_g_3l','build_v13_v10_g_document')):
        return 'H','Retain original G/G2 research/reference behavior and provenance.'
    if p.startswith('tools/'):
        if any(x in p for x in ['extend_v13','correct_v13','build_v13_v10_h','isolated_challenger','run_g3_dated_once']):
            return 'H','Retain historical/reference helper; do not schedule as active G3 producer.'
        if any(x in p for x in ['v13_','daily_strategy_dashboard','today_six_strategy']):
            return 'M','Make active strategy/source/cohort explicit; preserve historical readers.'
        return 'V','Verify adapter/launcher compatibility; retain generic behavior.'
    if p.startswith('bat/'):
        if p in legacy or 'run_fno_v13_v10_g_' in p or 'run_backtesting_result_v13_v10_g_' in p:
            return 'C','Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover.'
        if any(x in p for x in ['schedule_fno','schedule_backtesting','migrate_','v13_shadow','v13_strategy','Test-V13']):
            return 'M','Review task/profile mapping and recovery; apply only during approved migration.'
        if any(x in p for x in ['dashboard','gmail','authentication','zerodha','rotate_log']):
            return 'S','Protect shared service/auth/export infrastructure; verify continuity.'
        return 'V','Verify shared task/launcher/supervisor dependency and preserve other strategies.'
    if p.startswith('ai_platform/'):
        return 'M','Review version-aware source/schema/analysis consumer; edit only G-specific assumptions.'
    if p.startswith('configs/'):
        return 'M','Review profile, labels, metrics and source mappings; preserve shared settings.'
    if p.endswith('.md') or p.startswith('docs/ai_platform/'):
        return 'D','Update active operational instructions/registry after implementation; preserve dated evidence.'
    if p.endswith(('.css','.js','.cjs','.mjs','.html')):
        return 'V','Regression-check shared UI/assets; preserve design unless contract/status wiring requires change.'
    if p in protected:return 'S','Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior.'
    return 'V','Transitive/reference dependency: verify compatibility; no default rewrite or removal.'

new={
'configs/strategies/v13_v10_g_3_production_v1.json':'Normalized effective14-setup spec, baseline hash, profile and execution-model metadata.',
'fno_v13_v10_g_3_live_config.py':'Validated import-safe G3 profile and centralized runtime paths.',
'fno_v13_v10_g_3_policy.py':'Canonical side thresholds and staged-stop policy; pure functions.',
'fno_v13_v10_g_3_selection.py':'Explicit G3 confirmation/ranking/quota adapter; no volume-rescaling shim.',
'fno_v13_v10_g_3_live.py':'Explicit G3 scanner/confirmation/PAPER/logging entrypoint through shared engine.',
'fno_v13_v10_g_3_paper.py':'G3 profile adapter for PAPER lifecycle; reuse shared implementation.',
'fno_v13_v10_g_3_live_kite_session.py':'G3 broker-coordinator wrapper preserving quantity-one constraints.',
'tools/g3_runtime_preflight.py':'Proposed readiness-only route: cannot auto-arm, submit orders or spawn trading workers; existing qty1 BAT is not a safe preflight.',
'fno_strategy_source_snapshot.py':'Proposed strategy-neutral sealed source/feature preparation extracted from G daily flow.',
'fno_v13_v10_g_3_daily_replay.py':'Actual date-aware production G3 replay; distinct from frozen export.',
'backtesting_result_v13_v10_g_3_daily.py':'Status/manifest/report publisher and atomic current pointers.',
'fno_strategy_registry.py':'Central active profile/runtime/task/report contract; use existing registry if suitable instead.',
'fno_v13_v10_g_3_options_config.py':'G3 equity-source adapter; retain independent option rules and PAPER-only mode.',
'bat/migrate_fno_g_to_g3.ps1':'Dry-run/export/apply/rollback of exact task/profile routing, with explicit execution gates.',
'bat/schedule_fno_v13_v10_g_3_weekday.ps1':'G3 input/PAPER service scheduling initially disabled until gates pass.',
'bat/schedule_fno_v13_v10_g_3_live_kite_qty1_weekday.ps1':'Separately gated G3 quantity-one LIVE scheduling.',
'bat/schedule_fno_v13_v10_g_3_options_weekday.ps1':'G3-source options PAPER scheduling.',
'bat/schedule_backtesting_result_v13_v10_g_3_weekday.bat':'Date-aware weekday G3 replay scheduling after source independence.',
'docs/V13_V10_G3_OPERATIONS.md':'Approved effective rules, configuration, launch/recovery and rollback runbook.'}
for role in ['scanner_5min','equity_1min_feed','confirmation_1min','live_long','live_short','trade_logger','net_result','live_kite_qty1','options_live_long','options_live_short','options_trade_logger','options_net_result']:
    new[f'bat/run_fno_v13_v10_g_3_{role}.bat']='G3 launcher with explicit profile/mode, isolated state/log/status and matching supervision.'
new['bat/run_backtesting_result_v13_v10_g_3_1620.bat']='G3 daily backtest plus explicitly versioned analysis/shadow follow-ups.'
for name,reason in {
'contract':'All14 effective setups, config precedence/hash, thresholds, strict09:25 and profile identity.',
'selection_parity':'Full candidate/selected/nofill decision equivalence and stable quota/ranking.',
'runtime':'Scanner/confirmation/PAPER/recovery/staged-stop behavior across profile branches.',
'live_execution':'Fake-broker qty1, arming, ownership, duplicate suppression and reconciliation.',
'source_independence':'G3 replay builds valid sources without G result publication.',
'daily_publication':'Wrong-day/stale/partial/zero-trade/idempotency and atomic current/history pointers.',
'dashboard_integration':'Profile-aware cards, status/timeline/restart, routes and historical selection.',
'analysis_options':'G3 cohorts/options source mapping, attribution and no trading from observers.',
'migration_rollback':'Dry-run/task mapping, legacy recovery paths and controlled rollback.'}.items():
    new[f'tests/test_fno_v13_v10_g_3_{name}.py']=reason
# New module names in individual audits are design alternatives; one canonical
# list above avoids accidentally promising duplicate parallel implementations.
proposed=new

lines=['# G-3 migration: file, task and data manifest','',
'**Plan only — 11 October 2026.** Read alongside the [migration plan](V13_V10_G3_MIGRATION_PLAN_2026-10-11.md).','',
f'The merged audit covers **{len(rows)} individually named existing files**, **{len(proposed)} proposed new files**, **{len(read("runtime_tasks_sanitized.json"))} observed scheduled tasks**, and the runtime/source paths below. The initial automated scan read {scan["source_count_scanned"]} non-secret source/support texts and identified {len(scan["files"])} source/reference/dependency files with zero Python parse errors; manual operational and dashboard audits expanded that set.','',
'Paths in the source tables are relative to the project root unless stated otherwise. Credentials are listed by filename only; their contents were not part of this audit. Generated data is covered by exact roots and artifact patterns, rather than listing hundreds of thousands of candle files individually.','',
'## Action legend','',
'| Code | Meaning |','| --- | --- |',
'| M | Review/refactor active integration; an edit is needed only where the G-specific contract changes. |',
'| C | Preserve explicit historical/rollback launcher; switch approved callers and guard legacy reactivation. |',
'| T | Extend tests/fixtures while retaining historical regression assertions. |',
'| D | Update active documentation/registry after implementation. |',
'| V | Verify shared or transitive compatibility; normally retain unchanged. |',
'| H | Preserve historical/research evidence and semantics; no relabelling as production G3. |',
'| S | Protect shared infrastructure, credentials or state; names only for secrets. |',
'| U | Preserve unrelated V14 work; verify only shared dependency compatibility. |',
'| NEW | Proposed implementation file, not created as executable/config by this plan. |','',
'All codes are planning dispositions. No file is approved for deletion, moving or archiving. A shared file can need a small profile-aware change while all its legacy behavior remains protected.','']
groups=defaultdict(list)
for p,r in sorted(rows.items()):
    category='Root source, documentation and protected configuration' if '/' not in p or p.startswith('../') else p.split('/')[0]
    code,action=classify(p);r['code']=code;r['planned_action']=action
    groups[category].append(r)
lines+=['## Existing file coverage','', '| Action | Existing files |','| --- | ---: |']
for code,count in sorted(Counter(r['code'] for r in rows.values()).items()):lines.append(f'| {code} | {count} |')
lines.append('')
for group,items in sorted(groups.items()):
    lines += [f'### {group} ({len(items)} files)','', '| File | Action | Review purpose |','| --- | --- | --- |']
    for r in items:lines.append(f'| `{r["path"]}` | {r["code"]} | {r["planned_action"]} |')
    lines.append('')
lines+=['## Proposed new files','',
'Names are concrete planning proposals. During implementation, a suitable existing profile-neutral module may replace a proposed shared helper, with the manifest updated accordingly. G3 wrappers should reuse the reviewed core rather than duplicate strategy logic.','',
'| Proposed path | Purpose |','| --- | --- |']
for p,reason in sorted(proposed.items()):lines.append(f'| `{p}` | {reason} |')
lines += ['', '## Effective accepted G-3 setup contract','',
'Read from the frozen configuration: `setup_rules.core/expanded` supplies selection fields, while `exit.setups` supplies effective stop/target fields. Price-change numbers below are magnitudes, applied in the direction of the side. Global base gates, upper OI bound, EMA gates, stable tie-breaks and the 09:25 SHORT NIFTY gate still apply.','',
'| Setup | Signal → confirmation | Quota | Picker | Core / expanded price % | Min OI % | Min 5m volume ratio |',
'| --- | --- | ---: | --- | --- | ---: | ---: |']
for s in strategy['effective_14_setup_contract']:
    lines.append(f'| {s["setup_id"]} | {s["signal_end"]} → {s["confirmation_end"]} | {s["quota"]} | {s["picker"]} | {s["core_price_change_pct"]} / {s["expanded_price_change_pct"]} | {s["minimum_oi_change_pct"]} | {s["minimum_5m_volume_ratio"]} |')
lines += ['', '| Setup | Min 1m body ratio | Max 1m wick ratio | Min 1m volume ratio | Initial → tightened stop % | Effective target % |', '| --- | ---: | ---: | ---: | --- | ---: |']
for s in strategy['effective_14_setup_contract']:
    lines.append(f'| {s["setup_id"]} | {s["minimum_1m_body_ratio"]} | {s["maximum_1m_wick_ratio"]} | {s["minimum_1m_volume_ratio"]} | {s["initial_stop_pct"]} → {s["tightened_stop_pct"]} | {s["target_pct"]} |')
lines += ['', 'The stop tightens once after120minutes using the execution-mode clock described in the plan. No partial exits or breakeven are introduced.','',
'## Dashboard routes and consumer mapping','', '| Route | Owner/current contract | Planned mapping |','| --- | --- | --- |']
for r in dashboard['route_map']:
    lines.append(f'| `{r["route"]}` | {r["owner"]}; {r["current"]} | {r["plan"]} |')
lines += ['', '## Scheduled tasks: observed snapshot, not a deployment','',
'These states are a read-only 11 October snapshot. Nonzero last results require interpretation against each launcher contract; some scanner/feed/confirmation runs occurred after their pipeline deadline. A result0 is not by itself proof of strategy correctness. Re-export and compare actions/triggers immediately before any approved cutover.','',
'| Existing task | State / enabled | Last result | Next run as reported | Proposed disposition |', '| --- | --- | ---: | --- | --- |']
for t in read('runtime_tasks_sanitized.json'):
    name=t['name']
    if 'v13_v10_g_' in name:disposition='Replace approved active route with corresponding G3 task after applicable gates; preserve exported G definition.'
    elif 'g3_dated_once' in name:disposition='Retain one-off research evidence; not the new recurring G3 daily publisher.'
    elif 'v13_shadow' in name:disposition='Version source/cohort mapping for G3 and preserve separate G history.'
    elif 'preopen' in name:disposition='Review checks/autofix targets so legacy G cannot be reactivated.'
    else:disposition='Protect shared/other-strategy schedule and current enablement; verify dependency readiness.'
    lines.append(f'| `{name}` | {t["state"]} / {t["enabled"]} | {t["last_result"]} | {t["next_run"] or "None"} | {disposition} |')
lines += ['', '## Existing runtime directories and control paths','',
'Existing paths remain in place. Preserve prior order ownership, source hashes and logs; create isolated G3 production state rather than copying old G arm/order records. Recheck externally configured paths before deployment.','',
'| Existing path | Exists at audit | Protection/use |','| --- | --- | --- |']
for r in runtime['runtime_directories']:
    p=r['path'].replace('\\','/')
    purpose='Preserve shared data/control/history dependency; validate readers during cutover.'
    if 'v13_v10_g_live' in p:purpose='Preserve G signal/order/evidence history and outstanding ownership; add isolated G3 counterpart.'
    elif 'strategy_research' in p:purpose='Immutable historical/provenance input; no in-place promotion or relocation.'
    elif 'v6_live' in p:purpose='Legacy arm/kill and compatibility state; keep emergency behavior, never copy arming authorization.'
    lines.append(f'| `{p}` | {r["exists"]} | {purpose} |')
lines += ['', '### Exact protected research/provenance sources','', 'All paths below are relative to `C:/TradingData/eqidv2`. Directory entries cover every child, source snapshot and manifest-pinned dependency.','']
for p in dashboard['protected_exact_sources']:
    if 'v13_corrected_v5/ and v6_derivative_backtests/' in p:
        lines.append('- `fno_oi/strategy_research/v13_corrected_v5/` (current options input dependency)')
        lines.append('- `fno_oi/strategy_research/v6_derivative_backtests/market_cache/` (current options input dependency)')
    else:
        lines.append(f'- `{p}`')
lines += ['', '### Proposed G3 publication structure','',
'Under `C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3`, separate new production and cumulative-history trees from the existing historical `runs` tree. Rows explicitly marked EXTERNAL use the shared runtime path helper/configured log root rather than this publication root.','',
'| Proposed location | Contract |','| --- | --- |',
f'| `{dashboard["proposed_g3_output_contract"]["production_runs"]}` | Immutable new daily production attempts; isolated from archived research exports. |',
f'| `history/runs/<through-date>/<run-id>/` | New validated cumulative G3 history; existing runs tree stays unchanged as historical/research input. |']
for p,reason in dashboard['proposed_g3_output_contract']['files'].items():
    if p.startswith('reports/'):
        p='reports/<YYYY-MM-DD>/<run-id>/backtesting_result_v13_v10_g_3.md'
        reason='Immutable per-attempt G3 report; run-id prevents same-date rerun collisions.'
    if p.startswith(('runtime_status/','run_logs','LOG_DIR')):
        p='EXTERNAL: '+p
    lines.append(f'| `{p}` | {reason} |')
lines += ['', 'Flow must read the manifest-designated production portfolio ledger. Its current preference for `trades_with_pullbacks.csv` is suitable for historical enriched reports, but an observer overlay must not become the authoritative source of production P&L or executed trades. Keep overlay IDs/fingerprints validated against the canonical ledger.','',
'### Runtime-generated files covered recursively','',
'| Root/pattern | Coverage and migration rule |','| --- | --- |',
'| `fno_oi/v13_v10_g_live/**` and proposed `fno_oi/v13_v10_g_3_live/**` | Scanner/confirmation snapshots, signals, signal CSV/JSON, PAPER/LIVE orders, open/pending/closed state, locks, seen-ID journals, broker events, trade/net-result CSVs, evidence, reports and config/session manifests. Keep version/mode namespaces separate. |',
'| `fno_oi/v6_live/**` and proposed G3 control namespace | Existing arm/kill state and compatibility controls; preserve global emergency behavior and require new explicit G3 authorization. |',
'| Existing G options PAPER root and proposed G3-source counterpart | Equity-source identity, option-master references, one-lot/cash state, orders, events and reports; no cross-version state reuse or mode escalation. |',
'| `backtesting_result_v13_v10_g*/**` | Existing dated snapshots, input/feature manifests, candidates/rejects/selected/fills, portfolio/daily CSV, HTML/MD/XLSX exports, validation and hashes. Preserve historical trees; new G3 production uses declared roles/pointers. |',
'| `fno_oi/strategy_research/**` selected/pinned dependencies | Frozen configs/source code, minute/feature snapshots, instrument masters, ledgers, models, protocols and report artifacts remain protected even when named G2/V5/V6. |',
'| `runtime_status/**`, project `logs/**`, supervisor records | Task/process status, heartbeat, spawn records, dated logs, worker locks and restart attribution; introduce consistent G3 IDs while preserving old diagnostics. |',
'| Shared market/cache/calendar/Kite export roots | Dated raw candles, rankings/OI, instrument caches, trading calendar, broker exports and reconcilable actual fills; reuse verified inputs without rewriting historical data. |',
'| G and proposed G3 analysis/shadow roots | Features, cohorts, models, data-quality outputs, predictions, metrics, reports and timestamps remain explicitly versioned; no silent reuse of G calibration as G3. |','',
'## Environment and deployment contract (names only)','',
'| Group | Names to audit |','| --- | --- |']
for k,values in runtime['environment_contract_names_only'].items():lines.append(f'| {k} | '+', '.join('`'+v+'`' for v in values)+' |')
lines += ['', 'Also preserve `LOG_DASH_*`, Gmail/SMTP configuration and broker/API credential locations. Use their current values in place; never place values in the plan or change authentication as a side effect of a strategy migration. New profile/environment aliases must have explicit precedence and reject conflicting mode/profile combinations.','',
'## Coverage and verification limits','',
'The file inventory combines current text references, static import closure, reverse strategy consumers, manual profile/runtime/dashboard audits and task-launcher dependencies. Static import edges alone do not prove a component runs, and filenames alone do not prove it is obsolete. Generated files are covered by roots and patterns; secrets by metadata only. Future dynamic paths or task changes must be added to the implementation manifest before deployment.','',
'Detailed local audit evidence is retained under `.codex_tmp/g3_migration_plan_20261011/`: `source_inventory.json`, `strategy_audit.json`, `runtime_audit.json`, `runtime_tasks_sanitized.json` and `dashboard_data_audit.json`. No executable migration was performed.','']
out=ROOT/'docs/V13_V10_G3_MIGRATION_FILE_MANIFEST_2026-10-11.md'
out.write_text('\n'.join(lines),encoding='utf-8')
serialized=[{**r,'evidence':sorted(r['evidence'])} for r in rows.values()]
(BASE/'merged_manifest.json').write_text(json.dumps({'existing':serialized,'proposed':proposed,'counts':dict(Counter(r['code'] for r in rows.values()))},indent=2),encoding='utf-8')
print(json.dumps({'existing_files':len(rows),'proposed_files':len(proposed),'tasks':len(read('runtime_tasks_sanitized.json')),'manifest':str(out),'bytes':out.stat().st_size}))
