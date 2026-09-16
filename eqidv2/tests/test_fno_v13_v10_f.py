import copy

import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_f_backtest as f


def source_exit():
    return {
        'version': 'V13-v10-B',
        'default': {'stop_pct': .82, 'target_pct': 1.23},
        'setups': {'0941_LONG': {'stop_pct': .6, 'target_pct': 3.0}},
        'partial_exits': False, 'breakeven_stop': False,
    }


def signal(sid=1, side='SHORT', oi=.13, volume=1.2, day='2026-09-01'):
    return dict(
        sid=sid, day=day, hhmm_int=930, side=side,
        tradingsymbol=f'STOCK{sid}', price_change_pct=-.3 if side=='SHORT' else .7,
        oi_change_pct=oi, volume_ratio=2., body_ratio=.6, wick_ratio=.2,
        traded_value=1_000_000., v9_1m_volume_ratio=volume,
        signal_ts=day+' 09:30:00+05:30',
        confirmation_ts=day+' 09:31:00+05:30',
        v9_1m_feature_ts=day+' 09:31:00+05:30',
    )


def test_config_keeps_b_exits_exactly_and_strict_e_volume():
    original=source_exit()
    snapshot=copy.deepcopy(original)
    result=f.config(original)
    assert original==snapshot
    assert result['exit']==original
    result['exit']['default']['target_pct']=2.
    assert original==snapshot
    assert result['minimum_confirmation_1m_volume_ratio']==1.2
    assert result['selection_change']['oi_multiplier']==.5
    assert result['selection_change']['expansion_side']=='SHORT'
    assert 'fallback_confirmation_1m_volume_ratio' not in result


def test_short_oi_relaxation_and_unchanged_long_oi():
    rows=[
        signal(sid=1,oi=.125),
        signal(sid=2,oi=.1249,day='2026-09-02'),
        signal(sid=3,side='LONG',oi=.08),
        signal(sid=4,side='LONG',oi=.1,day='2026-09-02'),
    ]
    selected=f.select_orders(pd.DataFrame(rows),f.v9.V9Config())
    assert set(selected.sid)=={1,4}
    assert selected.set_index('sid').loc[1,'v10_f_required_oi_pct']==.125
    assert selected.set_index('sid').loc[4,'v10_f_required_oi_pct']==.1


@pytest.mark.parametrize('volume',[1.1999,.9,np.nan,np.inf])
def test_low_or_invalid_volume_never_enters_after_oi_relaxation(volume):
    selected=f.select_orders(pd.DataFrame([signal(volume=volume)]),f.v9.V9Config())
    assert selected.empty


def test_native_ranking_still_applies_after_volume_filter():
    rows=[signal(sid=1,oi=.3,volume=1.199),signal(sid=2,oi=.13,volume=1.2)]
    rows[0]['price_change_pct']=-.8  # Better-ranked but disallowed volume.
    selected=f.select_orders(pd.DataFrame(rows),f.v9.V9Config())
    assert selected.sid.tolist()==[2]


def test_future_volume_timestamp_fails_closed():
    row=signal()
    row['v9_1m_feature_ts']='2026-09-01 09:32:00+05:30'
    with pytest.raises(ValueError,match='Noncausal'):
        f.select_orders(pd.DataFrame([row]),f.v9.V9Config())


@pytest.mark.parametrize('volume',[.9,1.05,np.nan,np.inf])
def test_config_cannot_relax_e_volume_filter(volume):
    settings=f.config(source_exit())
    settings['minimum_confirmation_1m_volume_ratio']=volume
    with pytest.raises(ValueError,match='1.20'):
        f.checked_settings(settings)


def test_obsolete_fallback_config_rejected():
    settings=f.config(source_exit())
    settings['fallback_confirmation_1m_volume_ratio']=.9
    with pytest.raises(ValueError,match='Obsolete'):
        f.checked_settings(settings)


def test_evaluate_rejects_injected_low_volume_order():
    dataset={'orders':pd.DataFrame([signal(volume=1.19)])}
    with pytest.raises(ValueError,match='strict 1.20'):
        f.evaluate(dataset,f.config(source_exit()))


def test_future_outcomes_cannot_change_selections():
    rows=pd.DataFrame([signal(sid=1),signal(sid=2)])
    before=f.select_orders(rows,f.v9.V9Config())
    rows['net_profit_rupees']=[-1e9,1e9]
    rows['mfe_pct']=[0,100]
    after=f.select_orders(rows,f.v9.V9Config())
    assert before.sid.tolist()==after.sid.tolist()
