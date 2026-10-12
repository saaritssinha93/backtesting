import hashlib
import json
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dashboard_flow_observer_history import history_path, valid_candles
from dashboard_flow_observer_store import read_json, write_json

root = Path(r'C:\TradingData\eqidv2')
report = read_json(root, 'coverage/history_2026-07-28_2026-10-12.json')
assert report and report['completed'] == report['planned']
before = json.loads(Path('.codex_tmp/opening_range_protected_before.json').read_text())
metadata_changed = []
for name, original in before['metadata'].items():
    path = Path(name)
    if not path.exists():
        metadata_changed.append(name)
        continue
    info = path.stat()
    if [info.st_size, info.st_mtime_ns] != original:
        metadata_changed.append(name)
hash_changed = [name for name, digest in before['archive_hashes'].items()
                if not Path(name).exists() or hashlib.sha256(Path(name).read_bytes()).hexdigest() != digest]

monthly = defaultdict(Counter)
first = valid = full = 0
invalid = []
seen = set()
for row in report['outcomes']:
    identity = row['session_date'], row['symbol'], row['contract']
    assert identity not in seen
    seen.add(identity)
    day = date.fromisoformat(row['session_date'])
    saved = read_json(root, history_path(day, row['contract'])) or {}
    candles = valid_candles(saved.get('candles', []), day)
    if saved.get('symbol') != row['symbol'] or saved.get('contract') != row['contract'] or len(candles) != row['valid_minutes']:
        invalid.append(identity)
    count = len(candles)
    has_first = any(item['candle_start'].startswith(row['session_date']+'T09:15:00') for item in candles)
    first += has_first
    valid += count
    full += count == 6
    bucket = monthly[row['session_date'][:7]]
    bucket.update(stock_sessions=1, first_minute=has_first, complete_six=count == 6, valid_minutes=count)

assert not invalid, invalid[:5]
assert valid == report['valid_minutes'] and full == report['complete_contract_sessions']
assert not metadata_changed, metadata_changed
assert not hash_changed, hash_changed
summary = {
    'from_date': report['from_date'], 'to_date': report['to_date'], 'state': report['state'],
    'trading_dates_checked': sum(row['state'] != 'CLOSED' for row in report['sessions']),
    'dates_with_known_contracts': sum(row['universe_count'] > 0 for row in report['sessions']),
    'stock_sessions_checked': len(seen), 'first_minute_stock_sessions': first,
    'complete_six_minute_stock_sessions': full, 'valid_minutes': valid,
    'new_minutes_added': report['added_minutes'], 'broker_requests': report['requests'],
    'reason_counts': dict(Counter(row['error_type'] for row in report['outcomes'] if row['error_type'])),
    'monthly': {key: dict(value) for key, value in sorted(monthly.items())},
    'missing_identity_dates': [row for row in report['sessions'] if row['state'] == 'UNAVAILABLE'],
    'source_metadata_checked': len(before['metadata']), 'source_hashes_checked': len(before['archive_hashes']),
    'source_metadata_changes': metadata_changed, 'source_hash_changes': hash_changed,
    'invalid_saved_outputs': invalid,
}
write_json(root, 'coverage/verified_range_2026-07-28_2026-10-12.json', summary)
print(json.dumps(summary, indent=2))
