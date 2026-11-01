"""Read-only metadata/storage audit of regenerable caches, no contents read."""
from pathlib import Path
import os
import sys
import ctypes
from ctypes import wintypes as W
import json
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from authentication_v2 import _installed_windows_chrome
from tools.fno_data_supervisor import native_processes

k = ctypes.WinDLL('kernel32', use_last_error=True)
k.GetCompressedFileSizeW.argtypes = [W.LPCWSTR, ctypes.POINTER(W.DWORD)]
k.GetCompressedFileSizeW.restype = W.DWORD
k.CreateFileW.argtypes = [W.LPCWSTR,W.DWORD,W.DWORD,ctypes.c_void_p,W.DWORD,W.DWORD,W.HANDLE]
k.CreateFileW.restype = W.HANDLE
k.GetFileInformationByHandleEx.argtypes = [W.HANDLE,ctypes.c_int,ctypes.c_void_p,W.DWORD]
class FileStandardInfo(ctypes.Structure):
    _fields_=[('AllocationSize',ctypes.c_longlong),('EndOfFile',ctypes.c_longlong),('NumberOfLinks',W.DWORD),('DeletePending',ctypes.c_ubyte),('Directory',ctypes.c_ubyte)]
k.OpenProcess.argtypes = [W.DWORD, W.BOOL, W.DWORD]
k.OpenProcess.restype = W.HANDLE
k.QueryFullProcessImageNameW.argtypes = [W.HANDLE, W.DWORD, W.LPWSTR, ctypes.POINTER(W.DWORD)]
k.CloseHandle.argtypes = [W.HANDLE]

def storage(root):
    out = dict(path=str(root), files=0, logical_bytes=0, allocated_bytes=0, errors=0, skipped_reparse=0, newest_mtime=None)
    stack=[root]
    while stack:
        path=stack.pop()
        try:
            stat=path.stat(follow_symlinks=False)
            if getattr(stat,'st_file_attributes',0)&0x400:
                out['skipped_reparse']+=1
                continue
            if path.is_dir():
                stack.extend(Path(entry.path) for entry in os.scandir(path))
                continue
            out['files']+=1
            out['logical_bytes']+=stat.st_size
            out['newest_mtime']=max(out['newest_mtime'] or 0,stat.st_mtime)
            handle=k.CreateFileW(str(path),0x80,7,None,3,0,None)
            if handle==ctypes.c_void_p(-1).value:
                out['errors']+=1
            else:
                try:
                    info=FileStandardInfo()
                    if k.GetFileInformationByHandleEx(handle,1,ctypes.byref(info),ctypes.sizeof(info)):
                        out['allocated_bytes']+=info.AllocationSize
                    else:
                        out['errors']+=1
                finally:
                    k.CloseHandle(handle)
        except OSError:
            out['errors']+=1
    out['logical_gib']=round(out['logical_bytes']/2**30,4)
    out['allocated_gib']=round(out['allocated_bytes']/2**30,4)
    if out['newest_mtime'] is not None:
        out['newest_utc']=datetime.fromtimestamp(out['newest_mtime'],timezone.utc).isoformat()
    return out

current_path,current_version=_installed_windows_chrome()
print('INSTALLED_CHROME',json.dumps({'path':str(current_path),'version':current_version}),flush=True)
active=[]
installers=[]
for proc in native_processes():
    name=proc['name'].lower()
    if any(token in name for token in ('setup','installer','msiexec','update')):
        installers.append({'pid':proc['pid'],'name':proc['name']})
    if proc['name'].lower() not in ('chrome.exe','chromedriver.exe','pytest.exe'):
        continue
    handle=k.OpenProcess(0x1000,False,proc['pid'])
    if not handle:
        continue
    try:
        buf=ctypes.create_unicode_buffer(32768)
        size=W.DWORD(len(buf))
        if k.QueryFullProcessImageNameW(handle,0,buf,ctypes.byref(size)):
            active.append({'pid':proc['pid'],'name':proc['name'],'exe':buf.value})
    finally:
        k.CloseHandle(handle)
print('ACTIVE_BROWSER_PATHS',json.dumps({'chrome_count':len([x for x in active if x['name']=='chrome.exe']),'drivers':[x for x in active if x['name']=='chromedriver.exe']}),flush=True)
print('ACTIVE_INSTALLER_PROCESS_NAMES',json.dumps(installers),flush=True)

base=Path(r'C:\Users\Saarit')
cache_roots=[base/'AppData/Local/pip/Cache',base/'.cache/pip',base/'.wdm',base/'AppData/Local/Temp']
for root in cache_roots:
    if not root.exists():
        continue
    totals=[]
    for entry in root.iterdir():
        totals.append(storage(entry))
    totals.sort(key=lambda row:row['allocated_bytes'],reverse=True)
    print('CACHE',json.dumps({'path':str(root),'allocated_gib':round(sum(x['allocated_bytes'] for x in totals)/2**30,4),'logical_gib':round(sum(x['logical_bytes'] for x in totals)/2**30,4),'files':sum(x['files'] for x in totals),'errors':sum(x['errors'] for x in totals),'top_children':totals[:6]}),flush=True)

driver_root=base/'.wdm/drivers/chromedriver'
drivers=[]
for architecture in driver_root.iterdir():
    if not architecture.is_dir():continue
    for version in architecture.iterdir():
        row=storage(version)
        row['version']=version.name
        row['same_browser_build']=version.name.split('.')[:3]==current_version.split('.')[:3]
        row['active']=any(str(version).lower() in item['exe'].lower() for item in active)
        drivers.append(row)
old=[x for x in drivers if not x['same_browser_build'] and not x['active']]
protected=[x for x in drivers if x['same_browser_build'] or x['active']]
print('DRIVER_SUMMARY',json.dumps({'total_versions':len(drivers),'old_versions':len(old),'old_allocated_bytes':sum(x['allocated_bytes'] for x in old),'old_allocated_gib':round(sum(x['allocated_bytes'] for x in old)/2**30,4),'old_versions_exact':[x['version'] for x in old],'protected':protected}),flush=True)
for path in [base/'AppData/Local/Temp/su_data',base/'AppData/Local/Temp/vscode-stable-user-x64']:
    print('TEMP_INSTALLER_NAMES',str(path),json.dumps([entry.name for entry in path.iterdir()]) if path.exists() else 'absent')
    if path.name=='su_data':
        print('SU_PAYLOAD_NAMES',json.dumps([str(p.relative_to(path)) for p in path.rglob('*') if p.is_file()]))
