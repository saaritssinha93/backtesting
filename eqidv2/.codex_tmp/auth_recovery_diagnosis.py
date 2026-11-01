"""Read-only, secret-free auth host diagnostics for 2026-10-30."""
import json
import os
from pathlib import Path
import ctypes
from ctypes import wintypes
import winreg

kernel = ctypes.WinDLL('kernel32', use_last_error=True)
ntdll = ctypes.WinDLL('ntdll')
class PROCESSENTRY32W(ctypes.Structure):
    _fields_ = [('dwSize', wintypes.DWORD), ('cntUsage', wintypes.DWORD), ('th32ProcessID', wintypes.DWORD), ('th32DefaultHeapID', ctypes.c_size_t), ('th32ModuleID', wintypes.DWORD), ('cntThreads', wintypes.DWORD), ('th32ParentProcessID', wintypes.DWORD), ('pcPriClassBase', wintypes.LONG), ('dwFlags', wintypes.DWORD), ('szExeFile', wintypes.WCHAR*260)]
class UNICODE_STRING(ctypes.Structure):
    _fields_ = [('Length', wintypes.USHORT), ('MaximumLength', wintypes.USHORT), ('Buffer', ctypes.c_void_p)]
kernel.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
kernel.OpenProcess.restype = wintypes.HANDLE
kernel.CloseHandle.argtypes = [wintypes.HANDLE]
ntdll.NtQueryInformationProcess.argtypes = [wintypes.HANDLE, wintypes.ULONG, ctypes.c_void_p, wintypes.ULONG, ctypes.POINTER(wintypes.ULONG)]
def process_command(pid):
    handle = kernel.OpenProcess(0x1000, False, pid)
    if not handle:
        return ''
    try:
        needed = wintypes.ULONG()
        ntdll.NtQueryInformationProcess(handle,60,None,0,ctypes.byref(needed))
        if not 0 < needed.value < 1000000:
            return ''
        buf = ctypes.create_string_buffer(needed.value)
        if ntdll.NtQueryInformationProcess(handle,60,buf,len(buf),ctypes.byref(needed)) < 0:
            return ''
        string = UNICODE_STRING.from_buffer(buf)
        return ctypes.wstring_at(string.Buffer, string.Length//2)
    finally:
        kernel.CloseHandle(handle)
procs = []
snapshot = kernel.CreateToolhelp32Snapshot(2,0)
entry = PROCESSENTRY32W()
entry.dwSize = ctypes.sizeof(entry)
kernel.Process32FirstW.argtypes = [wintypes.HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
kernel.Process32NextW.argtypes = [wintypes.HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
ok = kernel.Process32FirstW(snapshot,ctypes.byref(entry))
while ok:
    if entry.szExeFile.lower() in ('python.exe','powershell.exe','cmd.exe','chromedriver.exe'):
        command = process_command(entry.th32ProcessID)
        if any(x in command.lower() for x in ('authentication_v2', 'chromedriver', 'powershell')):
            safe_command = command if ('get-item' in command.lower() or 'authentication_v2' in command.lower()) else '<not emitted>'
            procs.append({'pid':entry.th32ProcessID,'ppid':entry.th32ParentProcessID,'name':entry.szExeFile,'command':safe_command})
    ok = kernel.Process32NextW(snapshot,ctypes.byref(entry))
kernel.CloseHandle(snapshot)
print(json.dumps({'processes':procs}, indent=2))

for hive, key in [(winreg.HKEY_CURRENT_USER, r'SOFTWARE\Google\Chrome\BLBeacon'), (winreg.HKEY_LOCAL_MACHINE, r'SOFTWARE\WOW6432Node\Google\Update\Clients\{8A69D345-D564-463C-AFF1-A69D9E530F96}')]:
    try:
        with winreg.OpenKey(hive, key) as opened:
            for name in ('version','pv'):
                try:
                    print('chrome_registry', key, name, winreg.QueryValueEx(opened, name)[0])
                except OSError:
                    pass
    except OSError:
        pass
cache = Path(os.environ['USERPROFILE']) / '.wdm' / 'drivers.json'
if cache.is_file():
    raw = json.loads(cache.read_text())
    print('chrome_driver_cache', json.dumps({k:v for k,v in raw.items() if 'chromedriver' in k}, indent=2))

log = Path('logs/authentication_v2_runner.log')
if log.is_file():
    text = log.read_text(errors='replace')
    # Auth diagnostics are already sanitized; only print today's final segment.
    start = text.rfind('[Fri 10/30/2026')
    if start < 0:
        lines = text.splitlines()[-30:]
    else:
        lines = text[start:].splitlines()
    print('auth_recent_markers', '\n'.join(line for line in lines if any(x in line for x in ('START ', 'END ', 'Fresh browser', 'AUTH-SUMMARY', 'TOTP source', '[ERROR]'))))
