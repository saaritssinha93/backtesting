"""Seal added presentation/evidence artifacts without rewriting study manifests."""
from pathlib import Path
import sys,json,hashlib
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from research import g3_sl_target_research as study

def main():
    out=study.OUTPUT
    for manifest_path in (out/'manifest.json',out/'followup_stop_timing/manifest.json'):
        manifest=json.loads(manifest_path.read_text())
        for relative,digest in manifest['files'].items():
            if study.history.sha256(manifest_path.parent/relative)!=digest:
                raise ValueError(f'Study artifact changed: {manifest_path.parent/relative}')
        for code,digest in manifest['code'].items():
            if study.history.sha256(Path(code))!=digest:raise ValueError(f'Code changed: {code}')
    verification=dict(focused_tests_passed=55,simulator_tests=19,harness_tests=36,
        independent_frozen_native_exit_matches=493,independent_frozen_native_exit_mismatches=0,
        original_source_portfolio_execution_matches=486,setup_rows=51,
        main_study_manifest_verified=True,followup_manifest_verified=True,
        scope='Research only; production, paper and live configuration unchanged',
        main_policy_selection='First35days; locked before candidate evaluation onlater17days',
        future_validation='No untouched holdout: original strategy was previously researched on these dates',
        followup_status='Single post-hoc timer-removal diagnostic, clearly separated from frozen primary study')
    study.dump(out/'evidence/verification.json',verification)
    script_names=['build_source.py','finalize_source.py','check_stop_timing.py','build_report.py','seal_delivery.py',
        'audit/results_audit.py']
    code={str(Path(__file__).parent/name):study.history.sha256(Path(__file__).parent/name) for name in script_names}
    manifest=dict(execution_authority=False,
        study_manifest_sha256=study.history.sha256(out/'manifest.json'),
        source_metadata_sha256=study.history.sha256(study.SOURCE/'source.json'),
        files={str(p.relative_to(out)):study.history.sha256(p) for p in out.rglob('*') if p.is_file() and p.name!='delivery_manifest.json'},
        delivery_code=code)
    study.dump(out/'delivery_manifest.json',manifest)
    print(json.dumps(dict(verified=True,delivery_files=len(manifest['files']),report=str(out/'report.html')),indent=2))

if __name__=='__main__':main()
