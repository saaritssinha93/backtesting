import pandas as pd
import pytest

import fno_v13_v10_d_backtest as d


def test_confirmation_volume_filter_runs_before_native_ranking(monkeypatch):
    seen={}
    def fake_select(frame,base):
        seen['ids']=frame.sid.tolist();return frame.head(1)
    monkeypatch.setattr(d.v9,'select_orders',fake_select)
    signals=pd.DataFrame({'sid':[1,2,3],'v9_1m_volume_ratio':[1.19,1.2,float('nan')]})
    result=d.select_orders(signals,d.v9.V9Config(),1.2)
    assert seen['ids']==[2]
    assert result.sid.tolist()==[2]


def test_config_preserves_c_exit_contract_and_freezes_entry_expiry():
    source={'default':{'stop_pct':.6,'target_pct':1.2},
            'setups':{'0926_LONG':{'stop_pct':.82,'target_pct':1.64}}}
    result=d.config(source,1.2)
    assert result['minimum_confirmation_1m_volume_ratio']==1.2
    assert result['entry_expiry_minutes']==10
    assert result['exit'] is source


@pytest.mark.parametrize('threshold',[0,-1,float('nan')])
def test_invalid_confirmation_volume_threshold_rejected(threshold):
    source={'default':{'stop_pct':.6,'target_pct':1.2},'setups':{}}
    with pytest.raises(ValueError):d.config(source,threshold)


def test_non_two_to_one_exit_is_rejected():
    source={'default':{'stop_pct':.6,'target_pct':1.19},'setups':{}}
    with pytest.raises(ValueError,match='2xSL'):d.config(source,1.2)
