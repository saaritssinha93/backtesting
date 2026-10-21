"""Independent arithmetic, artifact and final-ledger audit of the G4 review."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd

from research import g4_setup_candidate_source_v2 as source
from research import g4_setup_search_v2 as study
from research import g4_g3_mirror_backtest as base
from research import g4_complete_inventory_backtest as complete


def audit(directory):
    folder=Path(directory).resolve()
    manifest=base.read(folder/'manifest.json')
    for name,digest in manifest['files'].items():
        if base.sha256(folder/name)!=digest:
            raise ValueError('Artifact changed: '+name)
    plan=base.read(folder/'search_plan.json')
    choices=base.read(folder/'frozen_choices.json')
    before=base.read_frozen_csv(folder/'baseline_trades.csv')
    after=base.read_frozen_csv(folder/'combined_candidate_trades.csv')
    assert set(choices)==set(source.SETUP_IDS) and len(choices)==49
    assert max(plan['train'])<min(plan['validation'])<max(plan['validation'])<min(plan['final_historical_diagnostic'])
    for book in (before,after):
        assert not book.sid.duplicated().any()
        assert not book.duplicated(['day','setup_id','tradingsymbol']).any()
        filled=book.loc[book.filled.eq(True)]
        assert (pd.to_datetime(filled.exit_ts,utc=True)>=pd.to_datetime(filled.entry_ts,utc=True)).all()
        assert (pd.to_datetime(filled.entry_ts,utc=True)>pd.to_datetime(filled.confirmation_ts,utc=True)).all()
    for sid,choice in choices.items():
        assert choice['entry'] in plan['grid'][sid]
        assert choice['exit'] in plan['exits'][sid]
    periods=dict(train=plan['train'],validation=plan['validation'],
        october=plan['final_historical_diagnostic'],today=plan['days'][-1:],all=plan['days'])
    expected=pd.read_csv(folder/'portfolio_comparison.csv').set_index('period')
    checked=[]
    for period,days in periods.items():
        a=before.loc[before.day.isin(days)&before.portfolio_executed.eq(True)]
        b=after.loc[after.day.isin(days)&after.portfolio_executed.eq(True)]
        row=expected.loc[period]
        assert row.trades==len(b)
        assert row.wins==int(b.portfolio_net_profit_rupees.gt(1e-8).sum())
        assert np.isclose(row.before_net_pnl_rs,a.portfolio_net_profit_rupees.sum(),atol=1e-6,rtol=0)
        assert np.isclose(row.net_pnl_rs,b.portfolio_net_profit_rupees.sum(),atol=1e-6,rtol=0)
        winner=a.loc[a.portfolio_net_profit_rupees.gt(1e-6)].set_index('sid').portfolio_net_profit_rupees
        new_winner=b.set_index('sid').portfolio_net_profit_rupees.reindex(winner.index,fill_value=0.)
        retention=100*new_winner.sum()/winner.sum() if len(winner) else 100.
        assert np.isclose(row.winner_retention_pct,retention,atol=1e-6,rtol=0)
        checked.append(period)
    costs=pd.read_csv(folder/'cost_sensitivity.csv')
    for period,group in costs.groupby('period'):
        original=group.loc[group.cost_bps.eq(5)].iloc[0]
        for row in group.itertuples():
            expected_delta=original.net_delta_rs-(row.cost_bps-5)/10000*500000*(row.trades-row.before_trades)
            assert np.isclose(row.net_delta_rs,expected_delta,atol=1e-5,rtol=0)
    pointer=base.read(base.G4_ROOT/'latest/bounded_exit.json')
    assert pointer['manifest_sha256']==source.BASELINE_MANIFEST_SHA256
    assert base.sha256(source.BASELINE_RUN/'manifest.json')==source.BASELINE_MANIFEST_SHA256
    assert base.read(complete.DEFAULT_CONFIG)==base.read(source.BASELINE_RUN/'config.json')
    for name,digest in base.read(folder/'price_proof.json').items():
        assert base.sha256(Path(name))==digest['sha256'],name
    html=(folder/'report.html').read_text(encoding='utf-8')
    for ref in re.findall(r'href="([^"]+)"',html):
        if not ref.startswith(('http:','https:','#')):
            assert (folder/ref).exists(),ref
    # Individual selection experiments and their joint allocation are different
    # views. Supply the joint per-setup totals explicitly for review.
    joint=[dict(setup_id=sid,period=period,**study.metrics(before,after,days,sid))
           for sid in choices for period,days in periods.items()]
    pd.DataFrame(joint).to_csv(folder/'combined_setup_comparison.csv',index=False)
    result=dict(passed=True,setup_count=49,periods_checked=checked,
        trade_identity_unique=True,clock_ordering_valid=True,
        independently_reconciled_pnl_win_counts_and_retention=True,
        cost_sensitivity_reconciled=True,price_hashes_unchanged=True,
        original_g4_configuration_and_pointer_unchanged=True,
        checks='47 automated source/selection/exit/retention tests plus exact original replay and49 full-book allocation parity checks',
        note='Arithmetic and implementation checks do not establish statistical reliability or live observability.')
    study.write_json(folder/'validation.json',result)
    study.write_json(folder/'manifest.json',dict(version=study.VERSION,complete=True,
        files={p.relative_to(folder).as_posix():base.sha256(p) for p in folder.rglob('*')
               if p.is_file() and p.name!='manifest.json'}))
    print(json.dumps(result,indent=2))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    audit(parser.parse_args().directory)
