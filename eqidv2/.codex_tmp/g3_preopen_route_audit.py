"""Read-only source/registry inspection; preopen functions are never executed."""
import ast
import hashlib
import json
import sys
from pathlib import Path

workspace = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(workspace))
from fno_strategy_registry import active_profile, deployment_state

profile = active_profile()
audit_path = workspace / '.codex_tmp/g3_migration_plan_20261011/deployed_tasks_audit.json'
audit = json.loads(audit_path.read_text(encoding='utf-8-sig'))
details = []
for filename in ('preopen_session_healthcheck.py', 'preopen_session_autofix.py'):
    source = (workspace / filename).read_text(encoding='utf-8')
    tree = ast.parse(source)
    tasks = sorted({node.value for node in ast.walk(tree) if isinstance(node, ast.Constant)
                    and isinstance(node.value, str) and node.value.startswith('EQIDV2_') and 'v13_v10_g_' in node.value})
    launchers = sorted({node.value for node in ast.walk(tree) if isinstance(node, ast.Constant)
                    and isinstance(node.value, str) and node.value.startswith('run_fno_v13_v10_g_') and node.value.endswith('.bat')})
    mapped = [profile.reference(value) for value in tasks]
    mapped_launchers = [profile.reference(value) for value in launchers]
    routes_registry = ('_strategy_profile = active_profile()' in source
                       and '_strategy_profile.reference' in source)
    details.append({'source': filename, 'source_sha256': hashlib.sha256(source.encode()).hexdigest(),
        'registry_mapping_present': routes_registry, 'effective_g3_tasks': mapped,
        'effective_g3_launchers': mapped_launchers,
        'mapped_launchers_exist': all((workspace / 'bat' / name).is_file() for name in mapped_launchers)})
    if not routes_registry or any('v13_v10_g_3_' not in name for name in mapped + mapped_launchers):
        audit['issues'].append('Preopen strategy routing mismatch: ' + filename)
state = deployment_state()
audit['preopen_routing'] = {'method': 'AST/source inspection plus actual read-only registry projection; no preopen execution',
    'active_profile': profile.name, 'deployment_stage': state.get('deployment_stage'),
    'live_activation_authorized': state.get('live_activation_authorized'), 'sources': details}
audit_path.write_text(json.dumps(audit, indent=2) + '\n', encoding='utf-8')
print(json.dumps({'preopen_profile': profile.name, 'sources': len(details), 'issues': audit['issues']}, indent=2))
