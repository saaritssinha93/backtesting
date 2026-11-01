"""One-time removal of only the obsolete ChromeDriver cache versions audited today."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import re
import shutil
import stat
import sys

WORKSPACE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKSPACE))
from authentication_v2 import _installed_windows_chrome, _compatible_cached_driver
from tools.fno_data_supervisor import native_processes

ROOT = Path(r'C:\Users\Saarit\.wdm\drivers\chromedriver\win64')
REPORT = WORKSPACE / 'outputs/storage_audit_20261101/cleanup_result.json'
audit = (WORKSPACE / '.codex_tmp/disposable_cache_audit_latest.txt').read_text()
summary = json.loads(next(line.removeprefix('DRIVER_SUMMARY ') for line in audit.splitlines() if line.startswith('DRIVER_SUMMARY ')))
assert len(summary['old_versions_exact']) == 84
assert all(re.fullmatch(r'\d+\.\d+\.\d+\.\d+', name) and 129 <= int(name.split('.')[0]) <= 155 for name in summary['old_versions_exact'])
assert ROOT.resolve(strict=True) == ROOT
for path in (ROOT, *ROOT.parents):
    assert not path.lstat().st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT, path
browser_path, browser_version = _installed_windows_chrome()
assert browser_version.startswith('156.0.8078.'), browser_version
assert not any(p['name'].lower() == 'chromedriver.exe' for p in native_processes()), 'Driver process is active; stop cleanup.'

protected = [Path(r'C:\Users\Saarit\.wdm\drivers.json')]
for version in ('156.0.8078.47', '156.0.8078.73'):
    directory = ROOT / version
    assert directory.is_dir()
    protected.extend(p for p in directory.rglob('*') if p.is_file())
hashes = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
targets = []
for name in summary['old_versions_exact']:
    target = ROOT / name
    assert target.resolve(strict=True).parent == ROOT, target
    assert target.is_dir()
    # Preflight every descendant before performing any removals.
    files = []
    for path in (target, *target.rglob('*')):
        info = path.lstat()
        assert not info.st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT, path
        assert path.resolve(strict=True).is_relative_to(target), path
        if stat.S_ISREG(info.st_mode):
            files.append(info.st_size)
        else:
            assert stat.S_ISDIR(info.st_mode), path
    targets.append({'path': str(target), 'logical_bytes': sum(files), 'files': len(files)})

result = {
    'started_utc': datetime.now(timezone.utc).isoformat(),
    'pip_cleanup': {'command': 'python -m pip cache purge', 'files_removed': 1620, 'reported_removed_mb': 3872.4},
    'driver_cache_root': str(ROOT),
    'browser_version': browser_version,
    'protected_sha256': hashes,
    'driver_planned_allocated_bytes_from_audit': summary['old_allocated_bytes'],
    'free_bytes_before_driver_cleanup': shutil.disk_usage('C:/').free,
    'targets': targets,
    'removed': [],
}
REPORT.write_text(json.dumps(result, indent=2))
for row in targets:
    target = Path(row['path'])
    assert target.resolve(strict=True).parent == ROOT
    assert not target.lstat().st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT
    shutil.rmtree(target)
    assert not target.exists()
    result['removed'].append(row)
    REPORT.write_text(json.dumps(result, indent=2))
for filename, expected in hashes.items():
    assert hashlib.sha256(Path(filename).read_bytes()).hexdigest() == expected, filename
cached = _compatible_cached_driver(browser_version, Path(r'C:\Users\Saarit\.wdm'))
assert cached and cached.is_file()
result.update({
    'completed_utc': datetime.now(timezone.utc).isoformat(),
    'protected_files_unchanged': True,
    'compatible_driver_after_cleanup': str(cached),
    'remaining_versions': sorted(p.name for p in ROOT.iterdir()),
    'free_bytes_after_driver_cleanup': shutil.disk_usage('C:/').free,
})
REPORT.write_text(json.dumps(result, indent=2))
print(json.dumps({k: result[k] for k in ('completed_utc', 'protected_files_unchanged', 'compatible_driver_after_cleanup', 'remaining_versions', 'free_bytes_after_driver_cleanup')}, indent=2))
print('Removed obsolete driver version directories:', len(result['removed']))
print('Report:', REPORT)
