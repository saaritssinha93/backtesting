"""Capture/verify prior G3 research files, excluding runtime/cache directories."""
import hashlib
import json
import os
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parent
PRIOR=ROOT.parent
WORKSPACE=PRIOR.parent
SKIP={ROOT.name,'node_modules','__pycache__','.pytest_cache','.git','.preview_profile'}


def sha(path):
    value=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(8*1024*1024),b''):
            value.update(chunk)
    return value.hexdigest()


def main():
    target=ROOT/'preservation_before.json'
    if '--capture' in sys.argv:
        if target.exists():
            raise FileExistsError(target)
        records={}
        for parent,dirs,files in os.walk(PRIOR,followlinks=False):
            dirs[:]=[name for name in dirs if name not in SKIP and not name.startswith('.preview') and not Path(parent,name).is_symlink()]
            for name in files:
                path=Path(parent,name)
                records[path.relative_to(WORKSPACE).as_posix()]={'sha256':sha(path),'mtime_ns':path.stat().st_mtime_ns}
        target.write_text(json.dumps(records,indent=2)+'\n',encoding='utf-8')
        print(json.dumps({'captured_prior_files':len(records)}))
        return
    records=json.loads(target.read_text(encoding='utf-8'))
    changed=[]
    for name,expected in records.items():
        path=WORKSPACE/name
        if not path.is_file() or path.stat().st_mtime_ns!=expected['mtime_ns'] or sha(path)!=expected['sha256']:
            changed.append(name)
    proof={'passed':not changed,'prior_files_checked':len(records),'hash_and_mtime_preserved':not changed,'changed':changed}
    (ROOT/'preservation_verified.json').write_text(json.dumps(proof,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(proof))
    if changed:
        raise RuntimeError('Prior files changed')


if __name__=='__main__':
    main()
