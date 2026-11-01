"""Read-only sanitized identity check before stopping two stalled data launchers."""
import json
from pathlib import Path
import shutil
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tools.fno_data_supervisor import native_processes

tokens = ('eqidv2_eod_scheduler_for_5mins_data_live_minimal',) if '--cash' in sys.argv else ('fno_oi_fetch_5min', 'fno_oi_feature_ranker', 'fno_data_supervisor')
if '--all' in sys.argv:
    tokens += ('eqidv2_eod_scheduler_for_5mins_data_live_minimal',)
rows = native_processes()
owners = [row for row in rows if any(token in row.get('command', '').lower() for token in tokens)]
if '--paper' in sys.argv:
    owners = [row for row in rows if row['pid'] in (2984, 34488, 25884, 31100)]
    for side in ('long', 'short'):
        status = Path(f'C:/TradingData/eqidv2/runtime_status/fno_v13_v10_g_3_live_{side}.status')
        values = dict(line.split('=', 1) for line in status.read_text(encoding='utf-8-sig').splitlines() if '=' in line)
        print(json.dumps({side: {key: values.get(key) for key in ('execution_mode', 'signals', 'pending_entry', 'open', 'closed', 'errors', 'ts')}}))
ids = {row['pid'] for row in owners}
children = [dict(pid=row['pid'], ppid=row['ppid'], name=row['name']) for row in rows if row['ppid'] in ids]
print(json.dumps(dict(owners=owners, children=children, disk_free_bytes=shutil.disk_usage('C:/').free), indent=2))
