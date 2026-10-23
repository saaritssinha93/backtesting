import numpy as np
import pandas as pd

from research.g6_market_regime_research import add_history, breadth_features, decisions, index_features, IST


def test_index_feature_cannot_see_future_reversal():
    ts = pd.date_range('2026-10-21 09:20', periods=12, freq='5min', tz=IST)
    c = np.arange(100., 112.)
    f = pd.DataFrame(dict(ts=ts, open=c-.5, high=c+.5, low=c-1, close=c,
                          volume=100., oi=1000.))
    cutoff = ts[7]
    before = index_features(f, cutoff)
    f.loc[f.ts > cutoff, ['open','high','low','close']] *= 10
    assert before == index_features(f, cutoff)
    assert before['index_complete']
    assert before['efficiency30'] == 1


def test_missing_index_bar_does_not_forward_fill():
    ts = pd.date_range('2026-10-21 09:20', periods=8, freq='5min', tz=IST)
    f = pd.DataFrame(dict(ts=ts, open=100., high=101., low=99., close=100., volume=100., oi=1000.))
    assert index_features(f.drop(index=3), ts[-1]) == {'index_complete': False}


def test_same_clock_reference_excludes_current_and_future_days():
    fields = ('rv_pct','volume','range_pct','efficiency30','breadth_open_pct','dispersion_pct')
    m = pd.DataFrame({'day':pd.date_range('2026-09-01',periods=25).strftime('%Y-%m-%d'),
                      'clock':'10:00', **{x:np.arange(1.,26.) for x in fields}})
    first = add_history(m)
    assert first.loc[20,'rv_pct_median20'] == 10.5
    m.loc[20:,list(fields)] = 10000.
    second = add_history(m)
    assert second.loc[20,'rv_pct_median20'] == first.loc[20,'rv_pct_median20']
    assert first.loc[9,'rv_pct_median20'] != first.loc[9,'rv_pct_median20']


def test_breadth_excludes_quotes_arriving_after_decision():
    f = pd.DataFrame(dict(underlying=[f'S{i}' for i in range(200)], is_index_future=False,
        quality_state='VALID', fetch_timestamp='2026-10-21 10:40:15+05:30',
        price_change_pct_from_open=1.,price_change_pct_day=-1.))
    f.loc[:9,'fetch_timestamp']='2026-10-21 10:41:01+05:30'
    r=breadth_features(f,pd.Timestamp('2026-10-21 10:41',tz=IST))
    assert r['breadth_complete'] and r['breadth_n']==190 and r['breadth_open_pct']==100
    f.loc[:50,'fetch_timestamp']='2026-10-21 10:41:01+05:30'
    r=breadth_features(f,pd.Timestamp('2026-10-21 10:41',tz=IST))
    assert not r['breadth_complete'] and np.isnan(r['breadth_open_pct'])


def test_missing_features_cannot_create_a_rejection():
    columns=['return15_pct','breadth_open_pct','breadth_change15_pp','efficiency30',
        'rv_relative20','session_return_pct','vwap_distance_pct','vwap_crosses30',
        'extension_atr','volume_relative20']
    columns += [p+'_'+k for p in ('BANKNIFTY','FINNIFTY','MIDCPNIFTY','NIFTYNXT50')
                for k in ('session_return_pct','return15_pct')]
    frame=pd.DataFrame([{**dict.fromkeys(columns,np.nan),'side':'LONG'}])
    assert not decisions(frame).to_numpy().any()


def test_breadth_reversal_is_side_symmetric_and_threshold_is_inclusive():
    data=dict(return15_pct=-.1,breadth_open_pct=35.,breadth_change15_pp=-10.,
        efficiency30=.8,rv_relative20=1.,session_return_pct=-.2,vwap_distance_pct=-.1,
        vwap_crosses30=0.,extension_atr=-1.,volume_relative20=1.)
    for p in ('BANKNIFTY','FINNIFTY','MIDCPNIFTY','NIFTYNXT50'):
        data[p+'_session_return_pct']=-.2
        data[p+'_return15_pct']=-.1
    long=pd.DataFrame([{**data,'side':'LONG'}])
    short=long.copy();short['side']='SHORT'
    for c in short:
        if c.endswith('return15_pct') or c.endswith('session_return_pct') or c in ('vwap_distance_pct','extension_atr','breadth_change15_pp'):
            short[c]=-short[c]
    short['breadth_open_pct']=100-short.breadth_open_pct
    assert decisions(long).breadth_reversal.iloc[0]
    pd.testing.assert_frame_equal(decisions(long),decisions(short))
    long['breadth_change15_pp']=-9.99
    assert not decisions(long).breadth_reversal.iloc[0]
