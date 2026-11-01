"""Read-only storage inventory. No file contents are read, changed or hydrated."""
from __future__ import annotations
import ctypes
from ctypes import wintypes
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
kernel = ctypes.WinDLL('kernel32', use_last_error=True)
kernel.GetCompressedFileSizeW.argtypes = [wintypes.LPCWSTR, ctypes.POINTER(wintypes.DWORD)]
kernel.GetCompressedFileSizeW.restype = wintypes.DWORD
seen_links = set()

def measure(path):
    totals = dict(logical_bytes=0, estimated_allocated_bytes=0, files=0, hardlinked_files=0, skipped_reparse=0, errors=0)
    stack = [str(path)]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        stat = entry.stat(follow_symlinks=False)
                        attrs = getattr(stat, 'st_file_attributes', 0)
                        # Includes junctions and OneDrive placeholders. Never follow them.
                        if attrs & 0x400:
                            totals['skipped_reparse'] += 1
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(entry.path)
                            continue
                        totals['files'] += 1
                        totals['logical_bytes'] += stat.st_size
                        if stat.st_nlink > 1:
                            totals['hardlinked_files'] += 1
                            identity = (stat.st_dev, stat.st_ino)
                            if identity in seen_links:
                                continue
                            seen_links.add(identity)
                        allocated = ((stat.st_size + 4095) // 4096) * 4096
                        if attrs & (0x800 | 0x200):
                            high = wintypes.DWORD()
                            ctypes.set_last_error(0)
                            low = kernel.GetCompressedFileSizeW(entry.path, ctypes.byref(high))
                            if low != 0xffffffff or ctypes.get_last_error() == 0:
                                allocated = (high.value << 32) | low
                        totals['estimated_allocated_bytes'] += allocated
                    except OSError:
                        totals['errors'] += 1
        except OSError:
            totals['errors'] += 1
    return totals

def main():
    mode = sys.argv[1]
    base = Path('C:/TradingData/eqidv2') if mode == 'trading' else ROOT
    results = []
    started = time.monotonic()
    for child in sorted(base.iterdir()):
        if not child.is_dir() or child.is_junction() or child.is_symlink():
            continue
        # Other agents inspect these detailed result trees; totals still measured here.
        result = dict(path=str(child), **measure(child))
        results.append(result)
        if result['logical_bytes'] > 1_000_000_000:
            print(json.dumps(result), flush=True)
    destination = ROOT / 'outputs' / 'storage_audit_20261101'
    destination.mkdir(parents=True, exist_ok=True)
    output = dict(scope=str(base), elapsed_seconds=round(time.monotonic()-started, 2),
                  note='Metadata only; reparse points skipped, unique hardlinks counted once across this scope. Allocation is cluster-rounded except compressed/sparse files queried natively; not a deletion estimate.',
                  directories=sorted(results, key=lambda row: row['estimated_allocated_bytes'], reverse=True))
    (destination / f'{mode}_sizes.json').write_text(json.dumps(output, indent=2), encoding='utf-8')
    print(json.dumps(dict(report=str(destination / f'{mode}_sizes.json'), top=output['directories'][:15])), flush=True)

if __name__ == '__main__':
    main()
