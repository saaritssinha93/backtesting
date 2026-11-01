"""Read-only size inventory of inactive text log files; no deletion."""
import ctypes
from datetime import datetime
from pathlib import Path
import json

root = Path(__file__).resolve().parents[2]
k = ctypes.WinDLL('kernel32', use_last_error=True)
k.GetFileAttributesW.argtypes = [ctypes.c_wchar_p]
k.GetFileAttributesW.restype = ctypes.c_uint32
rows = []
for path in Path('C:/TradingData/eqidv2/observability/logs').glob('*'):
    if not path.is_file() or path.suffix.lower() not in ('.log', '.bak', '.txt', '.jsonl'):
        continue
    info = path.stat()
    if info.st_size < 10_000_000 or datetime.fromtimestamp(info.st_mtime).date() >= datetime.now().date():
        continue
    attrs = k.GetFileAttributesW(str(path))
    if attrs == 0xffffffff or attrs & 0x800:
        continue
    rows.append(dict(path=str(path), bytes=info.st_size, modified=datetime.fromtimestamp(info.st_mtime).isoformat(timespec='seconds')))
print(json.dumps(sorted(rows, key=lambda row: row['bytes'], reverse=True)[:20], indent=2))
