"""Chronological learned ranking followed by the same native allocation."""
from pathlib import Path
import argparse,json
import numpy as np
import pandas as pd
from g3_v18_20261102.run import dump,metrics,guard_writes,sha
from .model import fit_predict_walkforward
from .run import choose,allocated,mtm_metrics,PACKAGE
from . import selection,features
from .finalize_partition import concat_for_parquet

def run(result):
    result=result.resolve()
    if not result.is_relative_to(PACKAGE):raise ValueError('Lab output only')
    config=json.loads((result/'config.json').read_text())
    manifest=json.loads((PACKAGE/'inputs/manifest.json').read_text())
    if sha(PACKAGE/'inputs/manifest.json')!=config['input_manifest_sha256']:
        raise ValueError('Frozen input manifest changed')
    if sha(PACKAGE/'protocol.json')!=manifest['protocol_sha256'] or sha(PACKAGE/'model.py')!=config['code_hashes']['model.py']:
        raise ValueError('Frozen model or protocol changed')
    pool=pd.read_parquet(result/'model_pool.parquet')
    pool['day']=pool.day.astype(str).str[:10]
    guard_writes(PACKAGE)
    output=result/'models'
    if output.exists():raise FileExistsError(output)
    output.mkdir()
    predicted=fit_predict_walkforward(pool,config['days'],output)
    predicted.to_parquet(output/'predictions.parquet',index=False)
    folds=pd.read_csv(output/'model_folds.csv')
    ready_folds=folds.loc[folds.status.eq('READY')]
    daymap=json.loads((result/'day_artifacts.json').read_text())
    totals=[];books=[]
    for day in config['days']:
        folder=Path(daymap[day])
        frame=pd.read_parquet(folder/'gates.parquet')
        master=pd.read_parquet(folder/'candidate_outcomes.parquet')
        minute_path=PACKAGE/'inputs'/day/'minutes.parquet'
        source=next(s for s in manifest['sessions'] if s['day']==day)
        if sha(minute_path)!=source['minutes_sha256']:raise ValueError('Frozen execution minutes changed')
        minutes=pd.read_parquet(minute_path)
        pred=predicted.loc[predicted.day.astype(str).str[:10].eq(day)].set_index('sid')
        ready=bool(((ready_folds.test_first_day<=day)&(ready_folds.test_last_day>=day)).any())
        for variant in config['protocol']['models']:
            if ready:
                rank=frame.sid.map(pred[variant])
                mask=(frame.gate_oi_lower|features.market_path(frame))&rank.notna()
                orders,_=selection.select(frame,variant,removed_gates=('oi_lower',),extra_mask=mask,ranking_score=rank)
            else:
                orders,_=choose(frame,'control')
            for cost in config['protocol']['costs_bps']:
                book=allocated(master,orders,variant,cost)
                book['model_ready']=ready
                metric=metrics(book,[day]);metric.update(day=day,variant=variant,cost_bps=cost,model_ready=ready)
                if cost==5:
                    metric.update(mtm_metrics(book,minutes));books.append(book)
                totals.append(metric)
        print(f'Model allocation {day}: ready={ready}',flush=True)
    pd.DataFrame(totals).to_csv(output/'daily_results.csv',index=False)
    combined,normalization=concat_for_parquet(books)
    combined.to_parquet(output/'trades.parquet',index=False)
    dump(output/'export_normalization.json',normalization)
    dump(output/'complete.json',dict(days=config['days'],models=config['protocol']['models'],execution_authority=False))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--results',type=Path,required=True);a=p.parse_args();run(a.results)
