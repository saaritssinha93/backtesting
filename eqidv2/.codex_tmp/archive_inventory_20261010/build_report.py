"""Build an approval-only archive review from metadata and read-only dependency audits."""
from __future__ import annotations
import json
import re
from collections import defaultdict, Counter
from pathlib import Path
from datetime import datetime, timezone, timedelta

WORK = Path(__file__).resolve().parent
PROJECT = WORK.parents[1]
ROOTS = {"P": PROJECT, "D": Path('C:/TradingData/eqidv2')}
REPORT = PROJECT / 'docs/ARCHIVE_CANDIDATES_REVIEW_2026-10-10.md'
meta = json.loads((WORK / 'metadata.json').read_text(encoding='utf-8'))
strategy = json.loads((WORK / 'strategy_dependencies.json').read_text(encoding='utf-8'))
runtime = json.loads((WORK / 'dashboard_runtime.json').read_text(encoding='utf-8'))
data_audit = json.loads((WORK / 'data_audit.json').read_text(encoding='utf-8')) if (WORK / 'data_audit.json').exists() else {}
files = {(row['root'], row['path']): row for row in meta['files']}
dirs = {(row['root'], row['path']) for row in meta['directories']}
protected_files, protected_dirs = {}, {}

def normalized(path):
    return str(path).replace('\\', '/').rstrip('/')

def local_key(path):
    value = normalized(path)
    for label, root in ROOTS.items():
        prefix = normalized(root)
        if value.lower().startswith(prefix.lower() + '/'):
            return label, value[len(prefix)+1:]
    return None

def protect(key, why, directory=False):
    if not key or not key[1]: return
    target = protected_dirs if directory or key in dirs else protected_files
    target.setdefault(key, why)

for rel in strategy['protected_relative_files']:
    protect(('P', normalized(rel)), 'G-family import/launcher dependency audit')
for rel in strategy['protected_relative_dirs']:
    protect(('P', normalized(rel)), 'G-family related documentation or logs', True)
for row in runtime['protected_paths']:
    if row.get('exists'):
        protect(local_key(row['path']), '; '.join(row.get('evidence', [])[:2]), row.get('kind')=='directory')
for row in strategy['shared_data_path_literals']:
    value = normalized(row['path_literal'])
    key = local_key(value)
    if key is None and not re.match(r'^[A-Za-z]:', value): key = ('P', value)
    if key in files or key in dirs:
        protect(key, f"G dependency path in {row['source']}:{row['line']}")
for row in data_audit.get('protected_roots', []):
    key = ('D', normalized(row['root']))
    if key in dirs or key in files: protect(key, row['reason'], key in dirs)
for rel in data_audit.get('frozen_manifest_dependencies', {}).get('paths', []):
    key = local_key(rel) or ('D', normalized(rel))
    if key in files: protect(key, 'Source path hash-verified by the frozen V9/G dataset manifest; required for reproducible replay')
for row in data_audit.get('protected_globs', []):
    for rel in row.get('actual_files', []): protect(('D', normalized(rel)), row['reason'])
data_candidates={normalized(row['root']):row for row in data_audit.get('candidate_roots', [])}
data_reviews={normalized(row['root']):row for row in data_audit.get('review_roots', [])}

# These are retention rules, not claims that every descendant executes today.
for label, paths, reason in [
    ('P', ['logs','docs','configs','kite_exports','ai_platform','tests','reports','report_v10_assets','report_v11_assets','report_v12_assets'], 'Retain related logs, documentation, configuration, tests, reports or shared support code'),
    ('D', ['backtesting_result_v13_v10_g','backtesting_result_v13_v10_g_3','backtesting_result_v11','dashboard_server','run_logs','reports','observability','runtime_manifests','runtime_state','runtime_status','repairs'], 'Retain strategy/dashboard results, operational evidence, shared repairs or runtime state'),
]:
    for path in paths:
        if (label,path) in dirs: protect((label,path),reason,True)

family = re.compile(r'v13|(?:^|[/_.-])g[23](?:[/_.-]|$)|v10[_-]?g|dashboard|kite|zerodha', re.I)
secrets = re.compile(r'access[_-]?token|api[_-]?(?:key|secret)|credential|(?:^|[/_.-])(?:token|tokens|secret|secrets|password|auth)(?:[/_.-]|$)', re.I)
documentation_tokens=set()
for key,row in files.items():
    if key[0]!='P' or Path(key[1]).suffix.lower() not in {'.md','.html','.htm'} or not family.search(key[1]) or row['bytes']>5_000_000: continue
    try: document=(ROOTS['P']/key[1]).read_text(encoding='utf-8-sig',errors='replace').lower()
    except OSError: continue
    documentation_tokens.update(re.findall(r'[a-z0-9_][a-z0-9_.-]*',document))
for key in list(files) + list(dirs):
    label, rel = key
    parts = rel.split('/')
    if family.search(rel): protect(key, 'Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence', key in dirs)
    elif secrets.search(Path(rel).name): protect(key, 'Credential/authentication/token-related name; contents were not inspected', key in dirs)
    elif parts[0].startswith(('stocks_', 'outputs_advanced_indicators_')):
        protect((label,parts[0]), 'Shared market data/indicator input; freshness or an older name is not evidence of disuse', True)
    if label=='D' and rel.startswith('fno_oi/strategy_research/') and re.match(r'nfo_master_.*\.parquet$',Path(rel).name,re.I):
        protect(key,'G options paper recursively searches all strategy_research for instrument masters (fno_v13_v10_g_options_paper.py:259-267)')

# All shared FnO acquisition/live/control trees are held; research is reviewed below.
for label,rel in dirs:
    if label=='D' and rel.startswith('fno_oi/') and rel.count('/')==1 and rel!='fno_oi/strategy_research':
        protect((label,rel),'Shared FnO inputs, live/paper controls, operational outputs or related historical evidence',True)

# Form a conservative code-reference closure without importing or executing the files.
code_ext = {'.py','.bat','.cmd','.ps1','.js','.json','.yaml','.yml','.toml','.ini','.cfg'}
code_cache = {}
def source_text(key):
    if key in code_cache: return code_cache[key]
    rel = key[1]
    if Path(rel).suffix.lower() not in code_ext or secrets.search(Path(rel).name): return ''
    if any(part in rel.lower() for part in ['node_modules/','.venv','__pycache__/','/site-packages/']): return ''
    row=files.get(key)
    if not row or row['bytes']>3_000_000: return ''
    try: value=(ROOTS[key[0]]/rel).read_text(encoding='utf-8-sig',errors='replace').lower()
    except OSError: value=''
    code_cache[key]=value
    return value

project_code = [key for key in files if key[0]=='P' and Path(key[1]).suffix.lower() in code_ext and not any(x in key[1] for x in ['.codex_tmp/','.venv','__pycache__/','/node_modules/'])]
active_code = {key for key in project_code if key in protected_files}
for key in project_code:
    if any(key[0]==label and key[1].startswith(path+'/') for label,path in protected_dirs if path in {'ai_platform','configs'}): active_code.add(key)

mention_tokens=set()
def mentioned(name, corpus):
    return name.lower() in mention_tokens

for round_ in range(8):
    corpus='\n'.join(source_text(key) for key in active_code)
    mention_tokens=set(re.findall(r'[a-z0-9_][a-z0-9_.-]*',corpus))
    mention_tokens.update(part for value in list(mention_tokens) if '.' in value for part in value.split('.'))
    extra=set()
    for key in project_code:
        if key in active_code: continue
        name=Path(key[1]).name
        stem=Path(key[1]).stem
        if mentioned(name,corpus) or (Path(name).suffix=='.py' and len(stem)>6 and mentioned(stem,corpus)):
            extra.add(key);protect(key,'Referenced by a protected source/launcher; conservative static code closure')
    if not extra: break
    active_code.update(extra)

# Exact directory names referenced by protected code hold their bundles as well.
for key in dirs:
    if key in protected_dirs or key[1].count('/')>2: continue
    name=Path(key[1]).name
    if len(name)>10 and mentioned(name,corpus):
        protect(key,'Directory name referenced by protected source or launcher; retained conservatively',True)

# Protect every FnO source: even optional G research hashes fno*.py for provenance.
for key in project_code:
    if Path(key[1]).name.lower().startswith('fno'):
        protect(key,'FnO source namespace retained for shared engines, optional research and provenance discovery')

children=defaultdict(set)
for label,rel in list(dirs)+list(files):
    parent,_,name=rel.rpartition('/')
    children[(label,parent)].add((label,rel))
stats=defaultdict(lambda:{'files':0,'bytes':0,'latest':None,'protected_descendants':0})
for (label,rel), row in files.items():
    parts=rel.split('/')
    for count in range(len(parts)):
        key=(label,'/'.join(parts[:count]));s=stats[key];s['files']+=1;s['bytes']+=row['bytes']
        if not s['latest'] or s['latest']<row['modified_utc']: s['latest']=row['modified_utc']
    stats[(label,rel)].update(files=1,bytes=row['bytes'],latest=row['modified_utc'])
for label,rel in set(protected_files)|set(protected_dirs):
    parts=rel.split('/')
    for count in range(len(parts)):
        stats[(label,'/'.join(parts[:count]))]['protected_descendants']+=1

container = {('P',x) for x in ['bat','tools','research','.codex_tmp']} | {('D','fno_oi'),('D','fno_oi/strategy_research')}
legacy_name = re.compile(r'(?:^|[_/-])(?:avwap|v17[a-z]*|v18[a-z0-9]*|v19[a-z0-9]*|v20|v8|v9|v10|v11|id_backtesting|b_setups|c_setups|d_setups|e_setups|g_setups)(?:[_/-]|\b)|^outputs_|^sweep_results$|^replay_out|isolated_long_3m', re.I)
units=[]
def classify(key):
    label,rel=key;directory=key in dirs;name=Path(rel).name;low=rel.lower();suffix=Path(rel).suffix.lower()
    if key in protected_dirs or key in protected_files:
        return 'KEEP', (protected_dirs.get(key) or protected_files[key])
    if directory and stats[key]['protected_descendants']:
        return 'HOLD','Mixed directory contains protected files/references. Do not archive the parent as a unit.'
    if label=='D' and rel in data_candidates:
        return 'CANDIDATE','Data review found an isolated dated benchmark and no source references or protected descendants; runtime/dependency exclusion checks also passed.'
    if label=='D' and rel in data_reviews:
        return 'HOLD',data_reviews[rel]['reason']
    if name.lower() in documentation_tokens or (not directory and suffix=='.py' and Path(name).stem.lower() in documentation_tokens):
        return 'HOLD','Referenced by G/dashboard-related documentation; retain until that relationship is reviewed.'
    if low.startswith(('.claude','.venv','__pycache__','.pytest_cache')):
        return 'HOLD','Environment, tool configuration or generated cache; retain pending a separate retention decision.'
    if low.startswith(('.tmp_resume_review','resume_output','house-of-yeashi-growth-kit')):
        return 'HOLD','Separate personal/project material; unrelated to trading does not establish that it is unused.'
    if directory and low.startswith('fno_oi/strategy_research/'):
        return 'HOLD','Shared research ancestry and generic file discovery need a package-level dependency review.'
    if label=='D' and (name.startswith(('slot_ready','nifty_')) or name in ['replay','dynamic_pre_momentum','entry_engine_1min_v5_ID','live_v15_shared_scan']):
        return 'KEEP','Shared live/paper data, replay, signal or readiness state retained conservatively.'
    if not directory and (suffix in {'.md','.html','.htm','.log','.txt','.pdf','.docx','.xlsx','.png','.jpg','.jpeg','.svg'} or '.log.' in name):
        return 'HOLD','Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed.'
    if directory and name in {'docs_early_emit','v14','Train_and_Test','experimental_prefilter','research_outputs','outputs','replay','tmp_task_backups','avwap_v11_refactored','v7_research_layer','v7_v11_parity'}:
        return 'HOLD','Mixed research, training, shared library or backup bundle; usage cannot be established from names alone.'
    if legacy_name.search(rel) and (directory or suffix in {'.py','.bat','.ps1','.cmd','.csv','.json','.parquet','.pkl','.tmp'}):
        return 'CANDIDATE','Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit.'
    if directory and any(word in low for word in ['history_clone','trial_validation','storage_writer_benchmark','invalid_seed','_backup_']):
        return 'HOLD','Backup/clone/validation evidence needs recovery and dependency review before any archive proposal.'
    return 'HOLD','Purpose or dynamic usage remains uncertain; not eligible for an archive operation.'

def visit(key):
    if key in container:
        for child in sorted(children[key]): visit(child)
        return
    status,reason=classify(key)
    s=stats[key]
    units.append({'root':key[0],'path':key[1],'kind':'directory' if key in dirs else 'file','status':status,'reason':reason,'files':s['files'],'bytes':s['bytes'],'latest_utc':s['latest']})
for label in ROOTS:
    for key in sorted(children[(label,'')]): visit(key)

# Assertions: units are disjoint, cover every scanned regular file, and no candidate
# can contain a credential/strategy/runtime/task/import-protected path.
unit_keys={(u['root'],u['path']):u for u in units}
for key in files:
    label,rel=key
    ancestors=[(label,'/'.join(rel.split('/')[:n])) for n in range(1,len(rel.split('/'))+1)]
    matches=[unit_keys[x] for x in ancestors if x in unit_keys]
    assert len(matches)==1,(key,len(matches))
for unit in units:
    if unit['status']!='CANDIDATE': continue
    key=(unit['root'],unit['path'])
    assert key not in protected_files and key not in protected_dirs
    assert not stats[key]['protected_descendants']
assert sum(u['files'] for u in units)==len(files)
assert sum(u['bytes'] for u in units)==sum(r['bytes'] for r in files.values())

def size(value):
    if value>=1024**3:return f'{value/1024**3:,.3f} GiB'
    if value>=1024**2:return f'{value/1024**2:,.2f} MiB'
    if value>=1024:return f'{value/1024:,.1f} KiB'
    return f'{value:,} B'
def esc(value):return str(value).replace('|','\\|').replace('\n',' ')
def row(unit,ident):
    day=(unit['latest_utc'] or '-')[:10]
    return f"| {ident} | {unit['root']} | `{esc(unit['path'])}{'/' if unit['kind']=='directory' else ''}` | {unit['files']:,} | {size(unit['bytes'])} | {day} | {esc(unit['reason'])} |"
def table(items,prefix):
    result=['| ID | Root | Exact relative path | Files | Logical size | Latest modified (UTC) | Reason / hold |','| --- | --- | --- | ---: | ---: | --- | --- |']
    for index,unit in enumerate(items,1):
        ident=f'{prefix}-{index:04d}';unit['id']=ident;result.append(row(unit,ident))
    return result

summary={status:{'units':sum(u['status']==status for u in units),'files':sum(u['files'] for u in units if u['status']==status),'bytes':sum(u['bytes'] for u in units if u['status']==status)} for status in ['CANDIDATE','HOLD','KEEP']}
lines=[
    '# Future archive inventory — approval required',
    '',
    '**Status: inventory only. No files or folders have been moved, archived or deleted. No archive operation is approved.**',
    '',
    'Prepared on 10 October 2026 for the requested G, G-2, G-3 and dashboard cleanup review. Only the rows marked CANDIDATE are proposals for a future archive review. HOLD and KEEP are excluded from that proposal.',
    '',
    '## Scope and path keys',
    '',
    f'- **P — project:** `{normalized(PROJECT)}`',
    f'- **D — trading data:** `{normalized(ROOTS["D"])}`',
    '- An exact directory row covers all scanned descendants recursively. Individual file rows cover only that file. Rows do not overlap.',
    '- Neither the parent Git checkout nor the rest of Downloads/Desktop was inventoried. Links/reparse directories were not traversed. `.git` metadata and this inventory work directory are retained and excluded from counts.',
    '',
    '## Summary',
    '',
    '| Classification | Listed units | Scanned files covered | Logical size | Future action |',
    '| --- | ---: | ---: | ---: | --- |',
]
for status in ['CANDIDATE','HOLD','KEEP']:
    v=summary[status];action={'CANDIDATE':'Only after explicit approval of selected IDs and a fresh dependency check','HOLD':'Keep in place; unresolved or mixed usage','KEEP':'Protected; excluded from archiving'}[status]
    lines.append(f'| {status} | {v["units"]:,} | {v["files"]:,} | {size(v["bytes"])} | {action} |')
lines += ['',f"Metadata scan: `{meta['started_utc']}` to `{meta['finished_utc']}`. Scanned {len(files):,} files and {len(dirs):,} directories, with {len(meta['errors'])} enumeration errors. Logical bytes are file lengths, not guaranteed space reclaimable on disk (OneDrive, hard links and compression may differ). Latest modification is context, not proof of disuse.",
    '', '## Protected exclusions', '',
    '- All discovered G/G-2/G-3 results, histories, frozen configurations, source code, launchers, live/paper execution state and supporting research remain protected. V13 predecessor evidence is retained conservatively.',
    '- Both dashboards, their assets, APIs, launchers and monitored results remain protected. An older version or disabled scheduled task is not automatically obsolete.',
    '- Kite/Zerodha/authentication files, API-key text files, access tokens, token caches, credential-related filenames and controls remain protected. Credential contents and raw process/task arguments are not included in this document.',
    '- Shared market data, instrument masters, calendars, repairs, indicators, ready markers, runtime state, configuration, related tests, documentation, HTML assets, reports and logs remain in place.',
    '- Loose documentation/log/text/media files are held conservatively. A CANDIDATE directory may contain its own unrelated historical report or log; the proposal covers that named experiment only, never related G/dashboard evidence.',
    '', '### Non-obvious dependencies that must stay', '',
    '| Dependency | Why it is protected |', '| --- | --- |',
    '| `fno_v5_live.py`, `fno_v5_live_config.py`, `fno_v6_live_kite_session.py`, `fno_v6_live_config.py` | G wrappers and the dynamic live profile reuse old-named engines/configuration. |',
    '| `fno_oi/v6_live/` | Shared arm/kill and live control state; do not remove because of the older version label. |',
    '| V13 V10 F/E/D/C/B/A, V9 and corrected V2–V6 ancestors | G backtest imports/default inputs still inherit these implementations and evidence. |',
    '| `fno_oi/strategy_research/v13_corrected_v10_g/run_20260914_opportunity_expansion/` | Canonical G frozen configuration and portfolio evidence. |',
    '| `fno_oi/strategy_research/fno_historical_repair_20260827/` | G3 confirmation research reads repaired minute data. |',
    '| Every `nfo_master_*.parquet` beneath `fno_oi/strategy_research/` | G options paper searches the entire research tree recursively for these masters. No containing candidate directory is allowed. |',
    '| `fno_oi/strategy_research/v13_corrected_v5/`, `v6_derivative_backtests/` | Old-looking derivative market data remains an input for G options paper. |',
    '| `backtesting_result_v11/` | Current G readiness and operational consumers still reference this older output root. |',
    '| V7/V15/V16/V17 sources and result paths referenced by dashboard/tasks | Some old strategies remain monitored, scheduled or imported; the dependency closure takes precedence over name-based grouping. |',
    '| `tools/run_g3_dated_once.py` | Referenced by the registered G3 dated task. |',
    '', '## Archive candidates — pending approval', '',
    'These are conservative proposals with explicit legacy/experiment naming and no protected dependency found by the recorded checks. Absence of a static reference is not a guarantee of disuse. Do not execute this table as a move list.',
    '',
]
candidates=sorted((u for u in units if u['status']=='CANDIDATE'),key=lambda u:(u['root'],u['kind']!='directory',-u['bytes'],u['path'].lower()))
lines += table(candidates,'A')
lines += ['', '## Hold for review — excluded from archive candidates', '',
    'Mixed folders, separate personal material, ambiguous scripts, environment files, backups and unresolved historical evidence stay where they are. A mixed parent must never be archived wholesale.', '']
holds=sorted((u for u in units if u['status']=='HOLD'),key=lambda u:(u['root'],u['kind']!='directory',u['path'].lower()))
lines += table(holds,'H')
lines += ['', '## Protected path register — do not archive', '',
    'This register includes conservatively retained related material as well as direct runtime/import dependencies. It does not claim every protected file ran today.', '']
keeps=sorted((u for u in units if u['status']=='KEEP'),key=lambda u:(u['root'],u['kind']!='directory',u['path'].lower()))
lines += table(keeps,'K')
lines += ['', '## Evidence and limitations', '',
    f"- Runtime audit inspected {runtime['summary']['scanned_code_files']} reachable source/launcher files and {runtime['summary']['registered_task_count']} registered EQIDV2 tasks ({runtime['summary']['ready_tasks']} Ready; {runtime['summary']['disabled_tasks']} Disabled). Registration is a reason to retain the target, not proof of recent execution.",
    '- Source tracing used static imports, launcher targets and path references; strategy/trading modules were not imported or executed. A further conservative source-name closure protects references from the combined audits.',
    '- No token/API-key text contents were read. Metadata enumeration uses filenames, byte lengths and modification timestamps. No trade, operational service, scheduled task or dashboard state was changed.',
    '- Running-process/task snapshots are time-bound. Open editor tabs, external tools, dynamic imports, environment overrides and arbitrary glob consumers cannot be exhaustively proven from static code. HOLD paths are intentionally not promoted to candidates.',
    '- The inventories describe a live workspace, not a filesystem snapshot. Files created after the metadata scan are outside this count and are not approved by this document.',
    '- Scan exclusions:',
]
for excluded in meta['excluded']:lines.append(f"  - {excluded['root']}: `{esc(excluded['path'])}` — {excluded['reason']}")
lines += ['', 'Local audit evidence (metadata and filenames; no credential contents):', '',
    '- `.codex_tmp/archive_inventory_20261010/metadata.json` — full scanned metadata.',
    '- `.codex_tmp/archive_inventory_20261010/strategy_dependencies.json` — G-family import and data-path evidence.',
    '- `.codex_tmp/archive_inventory_20261010/dashboard_runtime.json` — dashboards, process targets, task targets and source references.',
    '- `.codex_tmp/archive_inventory_20261010/data_audit.json` — data-tree dependency review, when present.',
    '- `.codex_tmp/archive_inventory_20261010/classification.json` — disjoint units, IDs, counts and classifications.',
    '', '## Future approval record', '',
    '**Approval: NOT GRANTED. Destination: NOT SELECTED. Archive date: NOT SCHEDULED.**', '',
    'Before any archive action, obtain your explicit approval of the exact A-IDs/paths and destination. Recheck running processes, task actions, imports, dynamic paths and any changes since this inventory. HOLD and KEEP rows require a separate, newly reviewed decision; approval of candidates does not include them.', '',
    '| Approval field | Value |', '| --- | --- |', '| Approved candidate IDs | None |', '| Approved destination | Not selected |', '| Approved operation/date | None |', '| Approved by | Awaiting user |', '| Files moved, deleted or archived in this task | 0 |', '',
]
REPORT.parent.mkdir(exist_ok=True)
REPORT.write_text('\n'.join(lines),encoding='utf-8')
(WORK/'classification.json').write_text(json.dumps({'report':str(REPORT),'summary':summary,'units':units,'protected_file_count':len(protected_files),'protected_dir_count':len(protected_dirs),'active_code_count':len(active_code),'scan_started':meta['started_utc'],'scan_finished':meta['finished_utc']},ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps({'report':str(REPORT),'summary':summary,'lines':len(lines),'active_code_count':len(active_code),'protection_files':len(protected_files),'protection_dirs':len(protected_dirs)}))
