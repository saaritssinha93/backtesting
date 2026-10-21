from datetime import date
import copy
import json
import sys

import pandas as pd
import pytest

from research import g4_complete_inventory_backtest as complete
from research import g4_g3_mirror_backtest as mirror


def candidate(slot,side,sid=1,**changes):
    stamp=pd.Timestamp('2026-10-16 '+slot,tz=mirror.production.IST)
    short=side=='SHORT'
    row=dict(sid=sid,tradingsymbol=f'STOCK{sid}',side=side,signal_ts=stamp,
        confirmation_ts=stamp+pd.Timedelta(minutes=1),v9_1m_feature_ts=stamp+pd.Timedelta(minutes=1),
        signal_close=100.,confirmation_open=100.,confirmation_high=100.4,
        confirmation_low=99.6,confirmation_close=99.7 if short else 100.3,
        price_change_pct=-.5 if short else .5,oi_change_pct=.1,oi=100100.,prev_oi=100000.,
        volume_ratio=10.-sid/10,body_ratio=.8,wick_ratio=.01,traded_value=1e7-sid*1e5,
        ema9=98. if short else 102.,ema20=99. if short else 101.,ema50=100.,
        v9_1m_volume_ratio=1.2 if short else .6,nifty_first_bar_return_pct=-.05,
        opening_0920_status='VALID',price_change_basis='09:15_CASH_OPEN_TO_09:20_CASH_CLOSE',
        oi_change_source='FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M',research_extension_atr=1.)
    row.update(changes)
    return row


def select(rows):
    pool=complete.donor.selection_audit(pd.DataFrame(rows))
    return complete.reselect(pool,complete.configuration())


def test_inventory_copies_entry_rules_and_keeps_g4_exits_and_guards():
    before=complete.donor.strategy_payload()
    config=complete.configuration()
    identity=complete.Identity(config)
    assert len(identity.ACTIVE_SETUPS)==51
    assert set(identity._ACTIVE_BY_ID)-set(mirror.frozen_donor._ACTIVE_BY_ID)==complete.ADDED_IDS
    for sid,spec in identity._ACTIVE_BY_ID.items():
        for key,value in vars(complete.donor._ACTIVE_BY_ID[sid]).items():
            if key not in {'stop_pct','target_pct','source_version'}:
                assert getattr(spec,key)==value
        assert spec.stop_pct==1.2
        assert .6<=spec.target_pct<=2.
    old=mirror.configuration()
    prior=mirror.Identity(old,old['primary_scenario'])
    assert identity.payload['maximum_confirmation_extension_atr_by_setup']==prior.payload['maximum_confirmation_extension_atr_by_setup']
    assert complete.donor.strategy_payload()==before
    for original,copied in zip(complete.donor.CORE_SETUPS,identity.payload['core_setups']):
        for key,value in vars(original).items():
            if key not in {'stop_pct','target_pct','source_version'}:
                assert copied[key]==value
    approval=config['approved_0920_decisions']
    assert approval['setup_ids']==['0921_LONG','0921_SHORT']
    assert approval['configuration_changed'] is False
    assert approval['winner_profit_give_up_limit_pct']==10.0


def test_approved_0920_comparison_records_zero_delta_for_retained_baseline():
    rows=[]
    for setup_id in complete.APPROVED_0920_IDS:
        row=candidate('09:20','SHORT' if setup_id.endswith('SHORT') else 'LONG')
        row.update(setup_id=setup_id,day='2026-10-15',filled=True,portfolio_executed=True,
            portfolio_net_profit_rupees=1250.,exit_reason='TARGET')
        rows.append(row)
    comparison=complete._approved_comparison(pd.DataFrame(rows),['2026-10-15'])
    assert set(comparison.setup_id)==set(complete.APPROVED_0920_IDS)
    assert not comparison.configuration_changed.any()
    delta=[name for name in comparison if name.startswith('delta_')]
    assert (comparison[delta]==0).all().all()


def test_new_slots_apply_distinct_quotas_and_do_not_enter_archived_g4():
    rows=[candidate('09:20','SHORT',sid=i) for i in (1,2,3)]
    rows += [candidate('10:10','LONG',sid=i) for i in (4,5)]
    selected=select(rows)
    assert selected.groupby('setup_id').size().to_dict()=={'0921_SHORT':2,'1011_LONG':1}
    assert selected.native_target_pct.eq(1.5).all()
    assert selected.native_stop_pct.eq(1.2).all()
    assert selected.strategy_version.eq(complete.VERSION).all()
    pool=complete.donor.selection_audit(pd.DataFrame(rows))
    assert mirror.reselect(pool,{}).empty


@pytest.mark.parametrize('changes',[
    {'v9_1m_volume_ratio':1.1999}, {'opening_0920_status':'MISSING'},
    {'nifty_first_bar_return_pct':-.04999}, {'oi_change_source':'PREVIOUS_SESSION'}])
def test_new_opening_short_still_requires_verified_opening_and_confirmation(changes):
    assert select([candidate('09:20','SHORT',**changes)]).empty


def test_new_long_keeps_confirmation_and_body_filters():
    assert select([candidate('10:10','LONG')]).setup_id.tolist()==['1011_LONG']
    assert select([candidate('10:10','LONG',v9_1m_volume_ratio=.5999)]).empty
    assert select([candidate('10:10','LONG',body_ratio=.49)]).empty


def test_retained_guard_replaces_top_candidate_before_quota():
    rows=[candidate('12:35','SHORT',sid=1,research_extension_atr=3.01),
          candidate('12:35','SHORT',sid=2,research_extension_atr=3.,v9_1m_volume_ratio=10.),
          candidate('10:10','LONG',sid=3)]
    chosen=select(rows)
    assert chosen.loc[chosen.setup_id.eq('1236_SHORT'),'sid'].tolist()==[2]
    assert chosen.loc[chosen.setup_id.eq('1011_LONG'),'sid'].tolist()==[3]


def test_config_drift_is_rejected(tmp_path):
    config=copy.deepcopy(complete.configuration())
    config['active_setups'][0]['stop_pct']=1.25
    path=tmp_path/'bad.json'
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError,match='drift'):
        complete.configuration(path)


def test_normal_cli_defaults_to_51_setups(tmp_path,monkeypatch):
    import fno_v13_v10_g_4_backtest as cli
    calls=[]
    monkeypatch.setattr(cli,'BACKTEST_ROOT',tmp_path)
    def fake(config,output,through):
        calls.append((config,through))
        return dict(sessions=52,first_date='2026-07-29',last_date='2026-10-15',active_setups=51,comparison=[])
    monkeypatch.setattr(cli,'run_complete',fake)
    monkeypatch.setattr(sys,'argv',['g4','--through','2026-10-15','--output-dir',str(tmp_path/'result')])
    cli.main()
    assert calls==[(complete.DEFAULT_CONFIG,date(2026,10,15))]
