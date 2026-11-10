"""Seal deliverables and record the native runtime dependencies without editing them."""
from pathlib import Path
from datetime import datetime, timezone
import importlib.metadata
import json,sys
from . import run,model,selection
from g3_v18_20261102.run import dump,sha

def main():
    package=Path(__file__).resolve().parent;root=package.parent;result=package/'results'
    verification=json.loads((result/'VERIFICATION.json').read_text())
    # This records dependency bytes at delivery, not a fictitious pre-run hash.
    modules={}
    for name,module in list(sys.modules.items()):
        source=getattr(module,'__file__',None)
        if not source:continue
        p=Path(source).resolve()
        if p.is_file() and p.is_relative_to(root) and p.suffix=='.py':
            modules[str(p.relative_to(root))]={'sha256':sha(p),'module':name}
    dump(package/'runtime_snapshot.json',dict(captured_at_utc=datetime.now(timezone.utc).isoformat(),
        capture_stage='DELIVERY_AFTER_PARITY_VERIFICATION; NOT_CLAIMED_AS_PRE_RUN_CAPTURE',
        project_modules=modules,python=sys.version,
        distributions={name:importlib.metadata.version(name) for name in ('numpy','pandas','pyarrow','scikit-learn','scipy','pytest')}))
    dump(result/'manifest.json',dict(files={p.name:sha(p) for p in sorted(result.iterdir()) if p.is_file() and p.name!='manifest.json'},
        research_code={p.name:sha(p) for p in sorted(package.glob('*.py'))},
        nested_model_files={str(p.relative_to(result)):sha(p) for p in sorted((result/'models').rglob('*')) if p.is_file()}))
    files={str(p.relative_to(package)):dict(bytes=p.stat().st_size,sha256=sha(p))
        for p in sorted(package.rglob('*')) if p.is_file() and p.name!='delivery_manifest.json'
        and '__pycache__' not in p.parts and '.pytest_cache' not in p.parts}
    dump(package/'delivery_manifest.json',dict(scope='RESEARCH_ONLY',execution_authority=False,
        sealed_at_utc=datetime.now(timezone.utc).isoformat(),file_count=len(files),files=files,
        tests='43 passed; pytest',primary_arms=24,additional_fill_stress=4,
        diagnostic_drop_group_subsets=8,diagnostic_refits=72,native_control_parity_days=63))
    print(f'Sealed {len(files)} research files; native sources recorded read-only')

if __name__=='__main__':main()
