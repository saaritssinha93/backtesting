from pathlib import Path
import json,sys,hashlib
from datetime import date
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
import fno_v13_replay_source as source
import fno_v13_v10_g_3_contract as production
from research import g3_target_cutoff_contract as contract
from research.g4_late_slot_source import _source_days_and_snapshots,sha256
from research.g4_extended_target_sweep import read_frozen_csv
from research.g3_0920_backtest import allocate,_stats
from g3_one_minute_extension import last_features,extension_atr
from fno_v13_v10_g_3_daily_replay import DEFAULT_SEALED_SOURCE

OUT=Path(r'C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/research/guard_all_setups_20261014')
RUNTIME=OUT.parents[1]

def read(p): return json.loads(p.read_text(encoding='utf-8-sig'))
def main():
    pointer=read(RUNTIME/'latest/current_backtest.json')
    run=RUNTIME/pointer['run_dir']
    assert sha256(run/'manifest.json')==pointer['manifest_sha256']
    manifest=read(run/'manifest.json')
    for name,digest in manifest['files'].items():
        assert sha256(run/name)==digest,(name,'artifact drift')
    summary=read(run/'summary.json')
    days=summary['days']
    ledger=read_frozen_csv(run/'portfolio_trades.csv')
    orders=read_frozen_csv(run/'selected_orders.csv')
    assert set(ledger.signal_id)==set(orders.signal_id)
    old=read_frozen_csv(run/'baseline_trades.csv')
    supplemental=old.loc[~old.setup_id.isin(contract._ACTIVE_BY_ID)].copy()
    ledger['study_scope']='CURRENT_HISTORICAL_49'
    supplemental['study_scope']='ARCHIVED_59_PRODUCTION_ONLY_10'
    allrows=pd.concat([ledger,supplemental],ignore_index=True)
    _,snapshots,snapshot_proof=_source_days_and_snapshots()
    for item in summary['daily_path_evidence']:
        path=Path(item['snapshot_manifest'])
        assert sha256(path)==item['snapshot_manifest_sha256']
        payload=read(path)
        day=payload.get('session_date') or payload.get('day')
        if not day:
            day=next(x for x in days if x in str(path))
        snapshots[date.fromisoformat(day)]=path.parent
    for record in summary['daily_sources']:
        daily=RUNTIME/record['run_dir']
        assert sha256(daily/record['manifest_name'])==record['manifest_sha256']
        daily_manifest=read(daily/record['manifest_name'])
        info=read(daily/daily_manifest['artifacts']['source_manifest']['path'])
        path=Path(info['snapshot_manifest'])
        assert sha256(path)==info['snapshot_manifest_sha256']
        snapshots[date.fromisoformat(daily_manifest['session_date'])]=path.parent
    sealed=read(DEFAULT_SEALED_SOURCE/'dataset_manifest.json')
    sealed_hashes={str(Path(x['path']).resolve()).lower():x['sha256'] for x in sealed['sources'] if x.get('exists')}
    proofs={};cache={};snapshot_expected={}
    for day,snap in snapshots.items():
        info=read(snap/'snapshot_manifest.json')
        snapshot_expected[str(snap)]={str((snap/x['snapshot_relative_path']).resolve()).lower():x['sha256'] for x in info['sources'] if x.get('captured')}
    for i,row in allrows.iterrows():
        day=pd.Timestamp(row.day).date(); symbol=str(row.tradingsymbol)
        snap=snapshots.get(day)
        path=(snap/'equity_1m' if snap else source.DataRoots().equity_1m)/f'{symbol}_stocks_indicators_1min.parquet'
        kind='IMMUTABLE_DAY_SNAPSHOT' if snap else 'CURRENT_HISTORICAL_PARQUET'
        allrows.loc[i,'extension_source_kind']=kind
        allrows.loc[i,'extension_source_path']=str(path)
        try:
            if str(path) not in cache:
                digest=sha256(path)
                if snap:
                    assert digest==snapshot_expected[str(snap)][str(path.resolve()).lower()],'immutable file drift'
                raw=pd.read_parquet(path,columns=['date','open','high','low','close'])
                raw['ts']=source.hybrid._to_ist(raw.date)
                raw=raw.sort_values('ts',kind='stable').drop_duplicates('ts',keep='last').reset_index(drop=True)
                cache[str(path)]=raw
                original=sealed_hashes.get(str(path.resolve()).lower())
                proofs[str(path)]={'sha256':digest,'source_kind':kind,'sealed_original_sha256':original,'sealed_hash_matches':digest==original if original else None}
            raw=cache[str(path)]
            stamp=pd.Timestamp(row.confirmation_ts)
            stamp=stamp.tz_localize(production.IST) if stamp.tzinfo is None else stamp.tz_convert(production.IST)
            assert stamp.date()==day,'confirmation day mismatch'
            current=raw.loc[raw.ts.eq(stamp)]
            assert len(current)==1,'confirmation missing/duplicated'
            bar=current.iloc[0]
            diffs=[]
            for name in ['open','high','low','close']:
                value=float(row[f'confirmation_{name}'])
                diffs.append(abs(float(bar[name])-value))
                assert np.isclose(float(bar[name]),value,rtol=0,atol=.001),f'confirmation {name} mismatch'
            ema,atr=last_features(raw.loc[raw.ts.lt(stamp)],bar.to_dict())
            assert np.isfinite(ema) and np.isfinite(atr) and atr>0,'insufficient/nonfinite features'
            allrows.loc[i,'screen_1m_ema20']=ema
            allrows.loc[i,'screen_1m_atr14']=atr
            allrows.loc[i,'screen_extension_atr']=extension_atr(row.confirmation_close,ema,atr,row.side)
            allrows.loc[i,'screen_status']='VERIFIED'
            allrows.loc[i,'confirmation_ohlc_max_diff']=max(diffs)
            allrows.loc[i,'available_bar_count']=int(raw.ts.le(stamp).sum())
        except Exception as exc:
            allrows.loc[i,'screen_status']=f'{type(exc).__name__}: {exc}'
        if (i+1)%100==0: print(f'features {i+1}/{len(allrows)}',flush=True)
    allrows['already_guarded']=allrows.setup_id.isin(['1036_LONG','1036_SHORT'])
    allrows['time_group']=np.select([allrows.hhmm_int.le(940),allrows.hhmm_int.le(1030),allrows.hhmm_int.le(1130)],['09:20-09:40','09:45-10:30','10:35-11:30'],default='11:35-13:30')
    split=days[25]
    allrows['chronology']=np.where(allrows.day.astype(str)<split,'EARLY_25_SESSIONS','LATER_26_SESSIONS')
    allrows.to_csv(OUT/'selected_order_extension_audit.csv',index=False)
    current=allrows.loc[allrows.study_scope.eq('CURRENT_HISTORICAL_49')].copy()
    base_realloc=allocate(current.reset_index(drop=True))
    expected=current.set_index('signal_id');actual=base_realloc.set_index('signal_id').reindex(expected.index)
    assert expected.portfolio_executed.equals(actual.portfolio_executed),'allocator reproduction execution mismatch'
    assert np.allclose(expected.portfolio_net_profit_rupees,actual.portfolio_net_profit_rupees,rtol=0,atol=1e-6),'allocator reproduction pnl mismatch'
    base=_stats(current,[date.fromisoformat(x) for x in days])
    assert np.isclose(base['net_pnl_rs'],summary['current_g3']['net_pnl_rs'],atol=1e-6)
    overview=[];setup_rows=[];group_rows=[];daily=[];reallocation_rows=[]
    def stats(part,threshold):
        verified=part.screen_status.eq('VERIFIED')
        blocked=verified & part.screen_extension_atr.gt(threshold) & ~part.already_guarded
        executed=part.portfolio_executed.astype(bool)
        cut=part.loc[blocked & executed]
        keep=part.loc[~blocked & executed]
        pnl=cut.portfolio_net_profit_rupees
        return dict(selected=len(part),verified=int(verified.sum()),blocked_selected=int(blocked.sum()),executed=int(executed.sum()),blocked_executed=len(cut),winners_removed=int(pnl.gt(0).sum()),losers_removed=int(pnl.lt(0).sum()),winning_profit_removed_rs=float(pnl.loc[pnl.gt(0)].sum()),losing_loss_avoided_rs=float(-pnl.loc[pnl.lt(0)].sum()),net_pnl_removed_rs=float(pnl.sum()),fixed_allocation_delta_rs=float(-pnl.sum()),remaining_executed=len(keep),remaining_win_rate_pct=float(keep.portfolio_net_profit_rupees.gt(0).mean()*100) if len(keep) else None,remaining_net_pnl_rs=float(keep.portfolio_net_profit_rupees.sum()))
    for scope,cohort in allrows.groupby('study_scope'):
        for threshold in [2.5,3.0,3.5,4.0]:
            overview.append(dict(scope=scope,threshold=threshold,**stats(cohort,threshold)))
            for sid in (sorted(contract._ACTIVE_BY_ID) if scope=='CURRENT_HISTORICAL_49' else sorted(set(production._ACTIVE_BY_ID)-set(contract._ACTIVE_BY_ID))):
                part=cohort.loc[cohort.setup_id.eq(sid)]
                for period in ['ALL','EARLY_25_SESSIONS','LATER_26_SESSIONS']:
                    section=part if period=='ALL' else part.loc[part.chronology.eq(period)]
                    setup_rows.append(dict(scope=scope,setup_id=sid,threshold=threshold,chronology=period,**stats(section,threshold)))
            for group in ['side','time_group','chronology','extension_source_kind']:
                for label,part in cohort.groupby(group):
                    group_rows.append(dict(scope=scope,group=group,label=label,threshold=threshold,**stats(part,threshold)))
    for threshold in [2.5,3.0,3.5,4.0]:
        blocked=current.screen_status.eq('VERIFIED') & current.screen_extension_atr.gt(threshold) & ~current.already_guarded
        kept=current.loc[~blocked].reset_index(drop=True)
        reallocated=allocate(kept)
        result=_stats(reallocated,[date.fromisoformat(x) for x in days])
        reallocation_rows.append(dict(threshold=threshold,**result,net_delta_rs=result['net_pnl_rs']-base['net_pnl_rs'],method='REMOVE_SELECTED_ORDERS_AND_REALLOCATE_REMAINING_SELECTED_UNIVERSE_NO_RESELECTION'))
        reallocated.to_csv(OUT/f'reallocated_remaining_selected_{threshold:.1f}atr.csv',index=False)
        for day in days:
            before=current.loc[current.day.astype(str).eq(day)]
            after=reallocated.loc[reallocated.day.astype(str).eq(day)]
            daily.append(dict(day=day,threshold=threshold,before_pnl=float(before.portfolio_net_profit_rupees.sum()),after_pnl=float(after.portfolio_net_profit_rupees.sum()),delta=float(after.portfolio_net_profit_rupees.sum()-before.portfolio_net_profit_rupees.sum()),**stats(before,threshold)))
    for name,rows in [('overview',overview),('per_setup',setup_rows),('groups',group_rows),('daily',daily),('remaining_selected_reallocation',reallocation_rows)]:
        pd.DataFrame(rows).to_csv(OUT/f'{name}.csv',index=False)
    report=dict(scope='OFFLINE_SELECTED_ORDER_SCREEN_NOT_FULL_PRE_QUOTA_RESELECTION_BACKTEST',active_configuration_changed=False,baseline_run=str(run),baseline_manifest_sha256=pointer['manifest_sha256'],days=days,early_end=days[24],later_start=split,baseline=base,source_snapshot_provenance=snapshot_proof,source_files=proofs,coverage_current=current.screen_status.value_counts().to_dict(),coverage_supplemental=allrows.loc[allrows.study_scope.ne('CURRENT_HISTORICAL_49'),'screen_status'].value_counts().to_dict(),source_kind_orders=current.extension_source_kind.value_counts().to_dict(),current_setup_count=49,observed_current_setup_count=current.setup_id.nunique(),production_setup_count=len(production.ACTIVE_SETUPS),limits=['Current historical49 differs from production59; production-only10 are supplemental archived V7 evidence.','Original selections already include 10:35 3ATR guard; sensitivity only applies to other setups, without loosening existing guard.','No alternative candidates selected after guard; reallocation only among the existing selected-order universe.','Prior native price outcomes reused as filter changes no entry/exit logic; allocator reproduces original execution exactly.','Reconstructed indicators are causal but older mutable whole-file hashes may differ; confirmation OHLC checked against frozen orders.','Chronological split is diagnostic, not independent holdout because rules were developed after observing history.','No threshold optimized; 2.5/3/3.5/4 are diagnostic sensitivity points.'],overview=overview,reallocation=reallocation_rows)
    (OUT/'study_summary.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
    print(json.dumps({k:report[k] for k in ['coverage_current','coverage_supplemental','source_kind_orders','baseline','overview','reallocation']},indent=2,default=str))
if __name__=='__main__': main()
