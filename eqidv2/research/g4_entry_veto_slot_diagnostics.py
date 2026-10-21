"""Post-hoc, slot-specific diagnostics motivated by 15 October examples.

No parameter search, default changes, or live orders. These are hypotheses
selected after observing October 15, not independent validation.
"""
from pathlib import Path
from datetime import datetime
import json

import pandas as pd

from research import g4_entry_veto_research as study
from research import g4_bounded_exit_backtest as bounded
from research import g4_g3_mirror_backtest as base


def run():
    source = base.G4_ROOT/'research/20261016T131914_entry_veto_v1'
    manifest = base.read(source/'manifest.json')
    for name in ('summary.json','guard_audit.csv'):
        if base.sha256(source/name)!=manifest['files'][name]:
            raise ValueError('Guard research input changed')
    evidence=base.read(source/'summary.json')
    baseline=Path(evidence['source_run'])
    if base.sha256(baseline/'manifest.json')!=evidence['source_manifest_sha256']:
        raise ValueError('Bounded baseline changed')
    original=base.read_frozen_csv(baseline/'g4_bounded_exit/portfolio_trades.csv')
    audit=base.read_frozen_csv(source/'guard_audit.csv').set_index('sid')
    days=base.read_frozen_csv(baseline/'g4_bounded_exit/daily_results.csv').day.tolist()
    specs={
        '0930_short_stretch3':('0931_SHORT','veto_stretch_3atr'),
        '1140_short_stretch3':('1141_SHORT','veto_stretch_3atr'),
        '1230_long_stretch3':('1231_LONG','veto_stretch_3atr'),
        '1320_short_pending':('1321_SHORT','veto_pending_invalidation'),
    }
    rejects={name:audit.index[audit.setup_id.eq(sid)&audit[flag].eq(True)].tolist()
             for name,(sid,flag) in specs.items()}
    rejects['four_observed_slot_rules']=sorted(set(sid for ids in rejects.values() for sid in ids))
    periods={'all':days,'before_oct15':days[:-1],'oct15':[days[-1]]}
    output=base.G4_ROOT/'research'/datetime.now().strftime('%Y%m%dT%H%M%S_entry_veto_slot_diagnostics')
    rows=[];ledgers={}
    for name,sids in {'current_g4':[],**rejects}.items():
        book=study.allocate(original.loc[~original.sid.isin(sids)].copy())
        ledgers[name]=book
        for period,cohort in periods.items():
            stats=bounded.stats(book,cohort)
            removed=original.loc[original.day.isin(cohort)&original.sid.isin(sids)&original.portfolio_executed.eq(True)]
            rows.append(dict(scenario=name,period=period,**stats,
                net_change_rs=stats['net_pnl_rs']-bounded.stats(original,cohort)['net_pnl_rs'],
                removed_winners=int(removed.portfolio_net_profit_rupees.gt(0).sum()),
                removed_losers=int(removed.portfolio_net_profit_rupees.lt(0).sum())))
    output.mkdir(parents=True)
    comparison=pd.DataFrame(rows)
    comparison.to_csv(output/'comparison.csv',index=False)
    for name,book in ledgers.items():
        book.to_csv(output/(name+'_portfolio.csv'),index=False)
    metadata=dict(scope='BACKTEST_ONLY',execution_authority=False,
        selection_bias='Post-hoc slot selection based on October 15 losses; no untouched holdout or promotion.',
        source_guard_run=str(source),source_guard_manifest_sha256=base.sha256(source/'manifest.json'),
        baseline=str(baseline),hypotheses=specs,default_configuration_changed=False)
    (output/'summary.json').write_text(json.dumps(metadata,indent=2)+'\n',encoding='utf-8')
    proof=dict(code_sha256={str(Path(__file__).resolve()):base.sha256(Path(__file__))},
        files={p.name:base.sha256(p) for p in output.iterdir() if p.is_file()})
    (output/'manifest.json').write_text(json.dumps(proof,indent=2)+'\n',encoding='utf-8')
    print(comparison[['scenario','period','trades','win_rate_pct','stop_exits','losing_days',
                      'net_pnl_rs','net_change_rs','removed_winners','removed_losers']].to_string(index=False))
    print(output)


if __name__=='__main__':
    run()
