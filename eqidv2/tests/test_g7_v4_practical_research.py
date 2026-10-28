"""Policy integration checks use synthetic decisions, never historical P&L."""
import numpy as np
import pandas as pd
import pytest

from research import g7_v4_practical_research as study


@pytest.mark.parametrize('which',['wrong_index','duplicate_sid','missing_native'])
def test_feature_inventory_rejected_before_source_reads(which):
    original=pd.DataFrame({'sid':[10,20]}).set_index('sid',drop=False)
    native=pd.DataFrame({'sid':[10,20]})
    if which=='wrong_index':original=original.reset_index(drop=True)
    if which=='duplicate_sid':native.sid=[10,10]
    if which=='missing_native':native=native.iloc[:1]
    with pytest.raises(ValueError,match='unique native SID'):
        study.build_features(original,native)


def test_plan_baselines_and_each_filter_remain_isolated(monkeypatch):
    native=pd.DataFrame({'sid':[10,20,30], 'sentinel':[3,4,5]})
    original=native.copy(deep=True)
    index=pd.Index([30,10,20],name='sid')
    f=pd.DataFrame({'allowed_g7_v2':[True,True,False]},index=index)
    veto=pd.Series([False,True,False],index=index)
    masks={name:pd.Series(values,index=index) for name,values in {
        'cmf20_confirm':[False,True,True],
        'vwap_slope_failure':[True,False,True],
        'close75_interaction':[True,True,True]}.items()}
    canceled=native.assign(sentinel=-1)
    monkeypatch.setattr(study.current.model,'decisions',lambda _:dict(low_efficiency=veto))
    monkeypatch.setattr(study.current.previous,'replay',lambda raw,*args:raw.copy(deep=True))
    monkeypatch.setattr(study,'entry_masks',lambda _:masks)
    monkeypatch.setattr(study,'gap_cancel',lambda *args:canceled.copy(deep=True))
    settings={'exit_policy':{'protect_after_close_pct':.6,'protected_profit_pct':.1}}
    plans=study.make_plans(native,{},f,{},settings)
    baseline=pd.Series([True,False,False],index=index)
    assert set(plans)=={'published_g7_v4','execution_control',*study.POLICIES}
    pd.testing.assert_series_equal(plans['execution_control']['allowed'],baseline,check_names=False)
    assert not plans['published_g7_v4']['integer'] and not plans['published_g7_v4']['clock']
    for name in study.POLICIES:
        assert plans[name]['integer'] and plans[name]['clock']
        if name in masks:
            pd.testing.assert_series_equal(plans[name]['allowed'],baseline & masks[name],check_names=False)
        else:
            pd.testing.assert_series_equal(plans[name]['allowed'],baseline,check_names=False)
    assert plans['loss_lock2']['limit']==2
    assert all(p['limit'] is None for n,p in plans.items() if n!='loss_lock2')
    assert plans['gap_cancel10']['raw'].sentinel.eq(-1).all()
    pd.testing.assert_frame_equal(native,original)


def test_compare_separates_strategy_gain_from_published_correction(monkeypatch):
    monkeypatch.setattr(study.current.model.changes,'comparison',lambda *a:dict(net_pnl_rs=120.,net_change_rs=20.))
    monkeypatch.setattr(study,'metrics',lambda *a:dict(net_pnl_rs=110.))
    result=study.compare(None,None,None,[])
    assert result['net_change_rs']==20.
    assert result['net_change_vs_published_rs']==10.
