import hashlib
import json

import pytest

import dashboard_flow as flow
from research import g4_g3_mirror_backtest as mirror
from tests.test_dashboard_flow import write_csv


@pytest.fixture
def g4(tmp_path,monkeypatch):
    monkeypatch.setattr(flow,'DATA_ROOT',tmp_path)
    root=tmp_path/'backtesting_result_v13_v10_g_4'
    run=root/'runs/2026-10-14/example'
    folder=run/'guard_all_ten'
    folder.mkdir(parents=True)
    summary=dict(variant_id=mirror.VERSION,execution_authority=False,primary_scenario='guard_all_ten',
        first_date='2026-10-14',last_date='2026-10-14',sessions=1,active_setups=49)
    config=dict(variant_id=mirror.VERSION,execution_authority=False,primary_scenario='guard_all_ten',
        scenarios={'guard_all_ten':dict(mirror.SCENARIOS['guard_all_ten'])})
    payload=dict(strategy_version=mirror.VERSION+'-GUARD_ALL_TEN',accepted_variant='guard_all_ten',execution_authority=False)
    for path,value in [(run/'summary.json',summary),(run/'config.json',config),(folder/'config.json',payload)]:
        path.write_text(json.dumps(value))
    write_csv(folder/'daily_results.csv',[dict(day='2026-10-14',trades=1,wins=1,losses=0,net_pnl=100,gross_pnl=105,cost=5)])
    write_csv(folder/'portfolio_trades.csv',[dict(day='2026-10-14',tradingsymbol='AAA',side='LONG',
        portfolio_executed=True,portfolio_net_profit_rupees=100,portfolio_gross_profit_rupees=105,portfolio_cost_rupees=5)])
    manifest={'files':{p.relative_to(run).as_posix():mirror.sha256(p) for p in run.rglob('*') if p.is_file()}}
    (run/'manifest.json').write_text(json.dumps(manifest))
    pointer=dict(variant_id=mirror.VERSION,execution_authority=False,run_dir='runs/2026-10-14/example',
                 manifest_sha256=mirror.sha256(run/'manifest.json'),session_date='2026-10-14')
    (root/'latest').mkdir()
    path=root/'latest/g3_mirror.json';path.write_text(json.dumps(pointer))
    monkeypatch.setattr(mirror,'verify',lambda path:summary)
    return root,run,path,pointer


def test_g4_loads_primary_ledger_once_without_g3_scope_or_local_paths(g4):
    data=flow.load_flow_data(strategy='V13-V10-G-4')
    assert data['summary']['trades']==1
    assert data['summary']['net_pnl']==100
    assert len([r for r in data['runs'] if r['strategy']=='V13-V10-G-4'])==1
    assert data['selected_run']['additional_atr_guards']==10
    assert data['data_mode']=='Verified G-4 backtest'
    assert not any('Current G-3 historical' in w for w in data['warnings'])
    assert str(g4[0]) not in json.dumps(data)


def test_g4_rejects_tampered_daily_file(g4):
    assert flow._verified_g4_current_backtest()
    (g4[1]/'guard_all_ten/daily_results.csv').write_text('day,net_pnl\n2026-10-14,999999\n')
    assert flow._verified_g4_current_backtest()==[]


def test_g4_reuses_accounting_for_unchanged_verified_manifest(g4,monkeypatch):
    calls=[]
    summary=json.loads((g4[1]/'summary.json').read_text())
    def verify(path):
        calls.append(path)
        return summary
    monkeypatch.setattr(mirror,'verify',verify)
    assert flow._verified_g4_current_backtest()
    assert flow._verified_g4_current_backtest()
    assert len(calls)==1


@pytest.mark.parametrize('field,value',[('run_dir','../outside'),('execution_authority',True),('session_date','2026-10-15')])
def test_g4_rejects_invalid_pointer(g4,field,value):
    pointer=g4[3];pointer[field]=value;g4[2].write_text(json.dumps(pointer))
    assert flow._verified_g4_current_backtest()==[]


def test_g3_remains_default_family_when_g4_is_available(g4,monkeypatch):
    run=g4[1]/'guard_all_ten'
    meta=dict(id='g3-current',strategy='V13-V10-G-3',label='G3',run_name='g3',kind='backtest',
        verified=True,period_start='2026-10-14',period_end='2026-10-14',sessions=1)
    monkeypatch.setattr(flow,'_verified_g3_current_backtest',lambda:[(meta,run/'daily_results.csv')])
    assert flow.load_flow_data()['selected_run']['id']=='g3-current'


def install_complete_profile(g4,monkeypatch):
    from research import g4_complete_inventory_backtest as complete
    root,run,_,_=g4
    folder=run/'g4_bounded_exit'
    folder.mkdir()
    for name in ['daily_results.csv','portfolio_trades.csv']:
        (folder/name).write_bytes((run/'guard_all_ten'/name).read_bytes())
    summary=dict(variant_id=complete.VERSION,execution_authority=False,primary_policy='g4_bounded_exit',
        first_date='2026-10-14',last_date='2026-10-14',sessions=1,active_setups=51)
    (run/'summary.json').write_text(json.dumps(summary))
    (run/'config.json').write_text(json.dumps(complete.configuration()))
    manifest={'files':{p.relative_to(run).as_posix():mirror.sha256(p)
        for p in run.rglob('*') if p.is_file() and p.name!='manifest.json'}}
    (run/'manifest.json').write_text(json.dumps(manifest))
    pointer=dict(variant_id=complete.VERSION,execution_authority=False,run_dir='runs/2026-10-14/example',
                 manifest_sha256=mirror.sha256(run/'manifest.json'),session_date='2026-10-14')
    (root/'latest/bounded_exit.json').write_text(json.dumps(pointer))
    monkeypatch.setattr(complete,'verify',lambda path:summary)
    return folder


def test_51_setup_g4_supersedes_old_control_without_double_counting(g4,monkeypatch):
    from research import g4_complete_inventory_backtest as complete
    install_complete_profile(g4,monkeypatch)
    data=flow.load_flow_data(strategy='V13-V10-G-4')
    assert data['summary']['trades']==1
    assert data['summary']['net_pnl']==100
    assert data['selected_run']['active_setup_count']==51
    assert data['selected_run']['strategy_version']==complete.VERSION
    assert len([r for r in data['runs'] if r['strategy']=='V13-V10-G-4'])==1


def test_tampered_51_setup_result_does_not_fall_back_to_stale_g4(g4,monkeypatch):
    folder=install_complete_profile(g4,monkeypatch)
    assert flow._verified_g4_current_backtest()
    (folder/'portfolio_trades.csv').write_text('tampered')
    assert flow._verified_g4_current_backtest()==[]
