"""Join disjoint independently closed daily replay partitions."""
from pathlib import Path
import argparse,json, shutil
import pandas as pd
from g3_v18_20261102.run import dump,sha
from .finalize_partition import concat_for_parquet
PACKAGE=Path(__file__).resolve().parent
COMPUTATIONAL={'run.py','selection.py','features.py','prepare.py','model.py','__init__.py'}

def run(parts,output):
    output=output.resolve()
    if not output.is_relative_to(PACKAGE) or output.exists():raise ValueError('New lab child required')
    configs=[json.loads((p/'config.json').read_text()) for p in parts]
    for c in configs[1:]:
        assert c['protocol']==configs[0]['protocol']
        assert c['input_manifest_sha256']==configs[0]['input_manifest_sha256']
        for key in COMPUTATIONAL:
            assert c['code_hashes'].get(key)==configs[0]['code_hashes'].get(key),key
    days=[d for c in configs for d in c['days']]
    assert len(days)==len(set(days))
    expected=json.loads((PACKAGE/'inputs/manifest.json').read_text())['days']
    assert set(days)==set(expected),f'Incomplete calendar: {sorted(set(expected)-set(days))}'
    for p in parts:assert (p/'rules_complete.json').exists(),p
    output.mkdir(parents=True)
    config={**configs[0],'days':sorted(days),'partitions':[{"path":str(p.resolve()),"config_sha256":sha(p/'config.json')} for p in parts]}
    dump(output/'config.json',config)
    for name in ('daily_results.csv','gate_failures.csv'):
        joined=pd.concat([pd.read_csv(p/name) for p in parts],ignore_index=True)
        joined=joined.rename(columns={'capital_rejections':'portfolio_rejections',
            'max_simultaneous_exposure_rs':'minute_close_max_exposure_rs'})
        joined.to_csv(output/name,index=False)
    normalizations={}
    for name in ('trades.parquet','model_pool.parquet'):
        joined,normalizations[name]=concat_for_parquet([pd.read_parquet(p/name) for p in parts])
        joined.to_parquet(output/name,index=False)
    dump(output/'export_normalization.json',normalizations)
    parities=[];daymap={}
    for p,c in zip(parts,configs):
        parities.extend(json.loads((p/'control_parity.json').read_text()))
        for d in c['days']:daymap[d]=str((p/d).resolve())
    dump(output/'day_artifacts.json',daymap)
    dump(output/'control_parity.json',parities)
    dump(output/'rules_complete.json',dict(days=sorted(days),variants=config['protocol']['rules'],native_parity_days=len(parities)))
    print(f'Merged {len(days)} disjoint days')

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--parts',nargs='+',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();run(a.parts,a.output)
