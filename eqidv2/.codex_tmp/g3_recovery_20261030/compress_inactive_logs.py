"""Recover capacity by reversible NTFS compression, retaining every log byte."""
import ctypes
from datetime import datetime, date
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

root = Path(__file__).resolve().parents[2]
directories = (root / 'logs', Path('C:/TradingData/eqidv2/observability/logs'))
kernel = ctypes.WinDLL('kernel32', use_last_error=True)
kernel.GetFileAttributesW.argtypes = [ctypes.c_wchar_p]
kernel.GetFileAttributesW.restype = ctypes.c_uint32
kernel.GetCompressedFileSizeW.argtypes = [ctypes.c_wchar_p, ctypes.POINTER(ctypes.c_uint32)]
kernel.GetCompressedFileSizeW.restype = ctypes.c_uint32

def stored_bytes(path):
    high = ctypes.c_uint32()
    low = kernel.GetCompressedFileSizeW(str(path), ctypes.byref(high))
    return (high.value << 32) + low

def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

targets = []
for directory in directories:
    for path in directory.iterdir():
        if not path.is_file() or path.suffix.lower() not in ('.log', '.bak', '.txt', '.jsonl'):
            continue
        stat = path.stat()
        attrs = kernel.GetFileAttributesW(str(path))
        if attrs == 0xffffffff or attrs & 0x800 or stat.st_size < 1_000_000:
            continue
        if datetime.fromtimestamp(stat.st_mtime).date() > date(2026, 10, 28):
            continue
        targets.append((stat.st_size, path, stat.st_mtime_ns))
targets.sort(reverse=True)
report = {'scope': 'Only inactive text logs in two fixed directories, last modified Oct28 or earlier; no recursive operation, deletion, source changes or security changes', 'before_c_free': shutil.disk_usage('C:/').free, 'files': []}
destination = root / 'outputs/g3_runtime_repair_20261030/archive_compression.json'
for size, path, modified in targets:
    if shutil.disk_usage('C:/').free >= 2_000_000_000:
        break
    if path.stat().st_mtime_ns != modified:
        continue
    before_hash = digest(path)
    before_storage = stored_bytes(path)
    result = subprocess.run(['compact.exe', '/C', '/A', str(path)], capture_output=True, timeout=30, creationflags=subprocess.CREATE_NO_WINDOW)
    after_hash = digest(path)
    entry = {'path': str(path), 'logical_bytes': size, 'stored_before': before_storage, 'stored_after': stored_bytes(path), 'sha256_before': before_hash, 'sha256_after': after_hash, 'command_exit': result.returncode}
    report['files'].append(entry)
    destination.write_text(json.dumps(report, indent=2), encoding='utf-8')
    if result.returncode or before_hash != after_hash:
        raise RuntimeError('Compression verification failed: ' + str(path))
report['after_c_free'] = shutil.disk_usage('C:/').free
report['stored_bytes_reclaimed'] = sum(row['stored_before'] - row['stored_after'] for row in report['files'])
destination.write_text(json.dumps(report, indent=2), encoding='utf-8')
print(json.dumps({'files_verified': len(report['files']), 'stored_bytes_reclaimed': report['stored_bytes_reclaimed'], 'c_free_bytes': report['after_c_free'], 'manifest': str(destination)}))
