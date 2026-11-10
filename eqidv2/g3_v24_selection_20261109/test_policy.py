import pandas as pd
from . import policy as p


def pool():
    return pd.DataFrame([dict(sid=i,day='2026-11-09',setup_id=next(iter(sorted(p.strict.NEW_IDS))),
        v24_core_pass=True,v9_filter_pass=True,v9_rank_in_setup_day=i+1,volume_ratio=100.,
        v24_extension_atr=e,v24_room_atr=room,v24_close_breakout=b,v24_feature_ready=True,
        v9_1m_volume_ratio=v) for i,e,room,b,v in [(0,5.,.1,False,4.),(1,2.,1.,True,2.),(2,1.,2.,False,1.)]])


def test_baseline_and_independent_ablations():
    f=pool()
    assert p.select(f,'baseline_v23')[0].sid.tolist()==[0]
    assert p.select(f,'stretch_rank')[0].sid.tolist()==[2]
    assert p.select(f,'structure_rank')[0].sid.tolist()==[1]
    assert p.select(f,'combined_rank')[0].sid.tolist()==[1]


def test_core_priority_and_no_postrank_replacement():
    f=pool();f.loc[0,'v24_core_pass']=False
    assert p.select(f,'baseline_v23')[0].sid.tolist()==[1]
    f.loc[1,'volume_ratio']=0.
    assert p.select(f,'baseline_v23')[0].empty


def test_missing_feature_falls_back_for_entire_setup():
    f=pool();f.loc[1,'v24_feature_ready']=False
    selected,audit=p.select(f,'combined_rank')
    assert selected.sid.tolist()==[0]
    assert audit.v24_missing_fallback.all()


def test_outcomes_and_input_order_never_change_selection():
    f=pool();expected=p.select(f,'combined_rank')[0].sid.tolist()
    f['future_pnl']=[-10000,0,100000];f['exit_price']=[1,999,4]
    assert p.select(f.sample(frac=1,random_state=42),'combined_rank')[0].sid.tolist()==expected


def test_original_pair_cap_three():
    f=pool();f['setup_id']=next(iter(sorted(p.strict.OLD_IDS)))
    assert len(p.select(f,'combined_rank')[0])==3
