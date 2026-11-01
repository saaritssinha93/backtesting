"""Read-only current strategy/launcher validation; no broker calls."""
import sys
import ast
import json
from pathlib import Path
from datetime import date, datetime
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools import g3_live_operator as operator

state = operator.read(operator.PROFILE)
arm = operator.read(operator.ARM)
policy = operator.load_standing_policy(session_date=date(2026,11,2))
plan = operator.plan(date(2026,11,2), state)
print('CURRENT_PLAN', json.dumps({key:plan[key] for key in ('mode','session_date','strategy_version','active_setup_count','mutates_controls','starts_workers','submits_orders')}))
print('STANDING_POLICY_VALID', policy['policy_id'])
try:
    operator.validate_launch(state,arm,now=datetime(2026,11,2,9,15,tzinfo=operator.config.IST))
    print('NEXT_SESSION_CURRENT_ARM', 'VALID')
except ValueError as exc:
    print('NEXT_SESSION_CURRENT_ARM', str(exc))
source=Path('log_dashboard_server.py').read_text(encoding='utf-8-sig')
tree=ast.parse(source)
for node in tree.body:
    if isinstance(node,ast.AnnAssign) and isinstance(node.target,ast.Name) and node.target.id in ('RESTARTABLE_CARDS','CARD_TASK_NAMES'):
        value=ast.literal_eval(node.value)
        print(node.target.id,json.dumps({key:val for key,val in value.items() if 'v13_v10_g' in key and ('live_' in key or 'kite_trade' in key)}))
lines=source.splitlines()
print('PROFILE_ROUTING', '\n'.join(f'{i+1}:{lines[i]}' for i in range(917,947)))
