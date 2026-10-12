"""Read source text only; inventory migration references without importing engines."""
from __future__ import annotations
import ast
import hashlib
import json
import re
from collections import defaultdict, deque
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
EXTS = {'.py', '.bat', '.ps1', '.js', '.cjs', '.mjs', '.html', '.css', '.md', '.json', '.yaml', '.yml', '.toml'}
SUBDIRS = ['ai_platform', 'bat', 'configs', 'docs', 'research', 'tests', 'tools', 'v14']
SKIP = {'__pycache__', 'node_modules', '.git', '.venv', 'source_snapshot', 'archive', 'snapshots'}
TOPIC = re.compile(r'v13[_-](?:corrected[_-])?v10[_-]g|v13_corrected_v10_g|\bg3[_-]|V13_V10_G', re.I)
texts = {}
for path in list(ROOT.iterdir()) + [f for name in SUBDIRS for f in (ROOT/name).rglob('*')]:
    if not path.is_file() or path.suffix.lower() not in EXTS:
        continue
    rel = path.relative_to(ROOT)
    if any(p in SKIP for p in rel.parts) or any(s in path.name.lower() for s in ['token', 'secret', 'api_key', 'credentials']):
        continue
    if path.stat().st_size > 5_000_000:
        continue
    try:
        texts[rel.as_posix()] = path.read_text(encoding='utf-8-sig')
    except (OSError, UnicodeError):
        pass

modules = {p[:-3].replace('/', '.'): p for p in texts if p.endswith('.py')}
for p in texts:
    if p.endswith('/__init__.py'):
        modules[p[:-12].replace('/', '.')] = p
edges = defaultdict(set)
parse_errors = []
for p, source in texts.items():
    if not p.endswith('.py'):
        continue
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        parse_errors.append({'path': p, 'line': exc.lineno})
        continue
    for node in ast.walk(tree):
        candidates = []
        if isinstance(node, ast.Import):
            candidates = [v.name for v in node.names]
        elif isinstance(node, ast.ImportFrom):
            prefix = node.module or ''
            if node.level:
                base = p.split('/')[:-1]
                if node.level > 1:
                    base = base[:-(node.level-1)]
                prefix = '.'.join(base + ([prefix] if prefix else []))
            candidates = [prefix] + [prefix+'.'+v.name for v in node.names if v.name != '*']
        for candidate in candidates:
            for n in range(1, len(candidate.split('.'))+1):
                target = modules.get('.'.join(candidate.split('.')[:n]))
                if target and target != p:
                    edges[p].add(target)

direct = {}
for p, source in texts.items():
    line = next((i for i,s in enumerate(source.splitlines(),1) if TOPIC.search(s)),None)
    if TOPIC.search(p) or line:
        direct[p] = line
seeds = set(direct)
seeds.update(p for p in texts if p.startswith('dashboard_') or p in {
    'log_dashboard_server.py', 'tools/run_dashboard_flow.py', 'bat/run_dashboard_flow.bat',
    'bat/run_log_dashboard_server.bat', 'bat/run_log_dashboard_restart_keep_url.bat',
    'bat/run_log_dashboard_stop.bat', 'bat/run_log_dashboard_public_link.bat',
    'bat/run_log_dashboard_public_link_scheduled.bat', 'bat/run_log_dashboard_public_link_capture.ps1',
    'bat/dashboard_public_link_helpers.ps1', 'bat/send_gmail_api.py',
    'bat/schedule_log_dashboard_weekday.bat', 'preopen_session_healthcheck.py', 'preopen_session_autofix.py'})

seen = set(seeds)
pending = deque(seeds)
while pending:
    p = pending.popleft()
    for dependency in edges.get(p, []):
        if dependency not in seen:
            seen.add(dependency); pending.append(dependency)
# Include reverse consumers/tests of strategy-specific modules, not every consumer
# of a generic pandas/data utility or every unrelated dashboard strategy.
family = {p for p in seen if TOPIC.search(p) and not p.startswith(('tests/','docs/','v14/'))}
reverse_consumers = {p for p, deps in edges.items() if deps & family}
seen.update(reverse_consumers)
reverse = defaultdict(set)
for p, deps in edges.items():
    for dep in deps:reverse[dep].add(p)
rows = []
for p in sorted(seen):
    if p not in texts:continue
    rows.append({'path':p,'direct_reference_line':direct.get(p),
                 'discovery': 'direct reference/name' if p in direct else 'entrypoint/asset' if p in seeds else 'reverse consumer' if p in reverse_consumers else 'transitive import',
                 'imported_by':sorted(reverse[p] & seen),
                 'imports': sorted(edges[p]),
                 'sha256':hashlib.sha256((ROOT/p).read_bytes()).hexdigest()})
OUT.mkdir(parents=True, exist_ok=True)
(OUT/'source_inventory.json').write_text(json.dumps({'source_count_scanned':len(texts),'entrypoints_and_direct_refs':len(seeds),'files':rows,'parse_errors':parse_errors,'method':'Source-text references, explicit dashboard entrypoints/assets, transitive local AST imports and reverse strategy consumers. No runtime imports, strategy execution or secret reads. Dynamic tasks/path wiring audited separately.'},indent=2),encoding='utf-8')
print(json.dumps({'scanned':len(texts),'seeds':len(seeds),'inventory_files':len(rows),'parse_errors':parse_errors}))
