import ast
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fno_v13_v10_g_3_live_config as config

for filename in ('fno_v5_live.py', 'fno_v6_live_kite_session.py', 'fno_equity_fetch_1min.py'):
    tree = ast.parse(Path(filename).read_text(encoding='utf-8'))
    attrs = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)
             and isinstance(n.value, ast.Name) and n.value.id in ('config', 'g_config')}
    print(filename, 'missing config API:', sorted(a for a in attrs if not hasattr(config, a)))

import fno_v13_v10_g_3_preflight as preflight
print('preflight', preflight.inspect_deployment())
