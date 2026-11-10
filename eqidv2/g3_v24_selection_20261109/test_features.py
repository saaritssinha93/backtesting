from unittest.mock import patch
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from . import features as f


def source():
    old = pd.date_range('2026-11-06 14:40', periods=30, freq='min', tz=f.IST)
    current = pd.date_range('2026-11-09 09:16', periods=20, freq='min', tz=f.IST)
    ts = old.append(current)
    close = np.r_[np.full(len(old),100.),np.arange(len(current))*.1+100.]
    return pd.DataFrame(dict(ts=ts,open=close-.05,high=close+.2,low=close-.2,close=close,volume=100.))


def candidate(frame, side='LONG', stamp='2026-11-09 09:21'):
    stamp = pd.Timestamp(stamp,tz=f.IST)
    bar = frame.loc[frame.ts.eq(stamp)].iloc[0]
    return dict(sid=1,day=str(stamp.date()),setup_id='0921_'+side,tradingsymbol='ABC',side=side,
        confirmation_ts=stamp.isoformat(),trigger=float(bar.high if side=='LONG' else bar.low),
        target_pct=1.,v9_1m_volume_ratio=2.,**{'confirmation_'+k:float(bar[k]) for k in f.OHLCV})


def test_future_mutation_cannot_change_features():
    frame=source();row=candidate(frame)
    original=f.selection_features_at(frame,row)
    changed=frame.copy();mask=changed.ts.gt(pd.Timestamp(row['confirmation_ts']))
    changed.loc[mask,['open','high','low','close','volume']]=np.nan
    assert original==f.selection_features_at(changed,row)
    assert original['v24_feature_ready']


def test_prior_level_excludes_confirmation_and_prior_day():
    frame=source();stamp=pd.Timestamp('2026-11-09 09:21',tz=f.IST)
    frame.loc[frame.ts.dt.date.lt(stamp.date()),'high']=500.
    frame.loc[frame.ts.eq(stamp),['open','high','low','close']]=[103.,105.,102.9,104.]
    row=candidate(frame);actual=f.selection_features_at(frame,row)
    earlier=frame.loc[frame.ts.dt.date.eq(stamp.date()) & frame.ts.lt(stamp)]
    assert actual['v24_prior_session_high']==earlier.high.max()
    assert actual['v24_close_breakout']
    assert actual['v24_trigger_breakout']
    assert actual['v24_room_atr']==0.


def test_long_short_mirror_and_target_room():
    frame=source()
    frame.loc[frame.ts.eq(pd.Timestamp('2026-11-09 09:18',tz=f.IST)),'high']=104.
    row=candidate(frame);long=f.selection_features_at(frame,row)
    mirrored=frame.copy()
    for left,right in [('open','open'),('high','low'),('low','high'),('close','close')]:
        mirrored[left]=200.-frame[right]
    short_row=candidate(mirrored,'SHORT');short=f.selection_features_at(mirrored,short_row)
    assert short['v24_extension_atr']==pytest.approx(long['v24_extension_atr'])
    assert short['v24_room_atr']==pytest.approx(long['v24_room_atr'])
    assert short['v24_close_breakout']==long['v24_close_breakout']
    expected=(long['v24_prior_level']-row['trigger'])/(row['trigger']*.01)
    assert long['v24_room_target_fraction']==pytest.approx(expected)


def test_missing_confirmation_is_explicitly_neutral():
    frame=source();row=candidate(frame)
    missing=frame.loc[frame.ts.ne(pd.Timestamp(row['confirmation_ts']))]
    actual=f.selection_features_at(missing,row)
    assert actual['sid']==row['sid']
    assert not actual['v24_feature_ready']
    assert actual['v24_feature_status']=='MISSING_CONFIRMATION'
    assert np.isnan(actual['v24_extension_atr'])


def test_internal_current_session_gap_is_neutral():
    frame=source();row=candidate(frame)
    missing=frame.loc[frame.ts.ne(pd.Timestamp('2026-11-09 09:18',tz=f.IST))]
    actual=f.selection_features_at(missing,row)
    assert not actual['v24_feature_ready']
    assert actual['v24_feature_status']=='INCOMPLETE_CURRENT_SESSION'


def test_insufficient_warmup_and_flagged_source_are_neutral():
    frame=source();row=candidate(frame)
    actual=f.selection_features_at(frame.loc[frame.ts.dt.date.eq(pd.Timestamp(row['day']).date())],row)
    assert actual['v24_feature_status']=='INSUFFICIENT_OR_FLAT_WARMUP'
    frame['is_synthetic']=False
    frame.loc[frame.ts.eq(pd.Timestamp('2026-11-09 09:18',tz=f.IST)),'is_synthetic']=True
    assert f.selection_features_at(frame,row)['v24_feature_status']=='INVALID_OR_FLAGGED_CAUSAL_SOURCE'


def test_confirmation_parity_mismatch_is_not_silently_missing():
    frame=source();row=candidate(frame);row['confirmation_close']+=1.
    with pytest.raises(ValueError,match='confirmation close mismatch'):
        f.selection_features_at(frame,row)


def test_file_adapter_preserves_every_candidate_and_provenance():
    frame=source();row=candidate(frame);missing=dict(row,sid=2,confirmation_ts='2026-11-09T09:45:00+05:30')
    orders=pd.DataFrame([row,missing]);proof={'stock_sha256':'verified','snapshot_manifest_sha256':'manifest'}
    with patch.object(f.stock_features,'_inventory',return_value=({'source':[row,missing]},{'source':proof})), \
         patch.object(f.stock_features,'_prepared_frame',return_value=(frame,proof)):
        actual,proofs=f.build_selection_features(orders)
    assert actual.sid.tolist()==[1,2]
    assert actual.v24_feature_ready.tolist()==[True,False]
    assert proofs[0]['snapshot_manifest_sha256']=='manifest'
    assert proofs[0]['candidate_rows']==2


def test_target_fraction_missing_does_not_hide_other_ready_features():
    frame=source();row=candidate(frame);row.pop('target_pct')
    actual=f.selection_features_at(frame,row)
    assert actual['v24_feature_ready']
    assert np.isnan(actual['v24_room_target_fraction'])


def test_target_fraction_uses_valid_fallback_when_native_target_is_nan():
    frame=source();row=candidate(frame)
    row['native_target_pct']=np.nan
    actual=f.selection_features_at(frame,row)
    assert actual['v24_feature_ready']
    assert np.isfinite(actual['v24_room_target_fraction'])


def test_absent_sealed_member_retains_candidate_and_manifest_proof():
    frame=source();row=candidate(frame);missing=dict(row,sid=2,tradingsymbol='ABSENT')
    proof={'stock_sha256':'verified','snapshot_manifest_sha256':'manifest'}
    def inventory(orders):
        if orders.tradingsymbol.eq('ABSENT').any():
            raise ValueError('Stock minute source missing: 2026-11-09 ABSENT')
        return {'source':orders.to_dict('records')},{'source':proof}
    with patch.object(f.stock_features,'_inventory',side_effect=inventory), \
         patch.object(f.stock_features,'_manifest_for_day',return_value=(Path('manifest.json'),'pinned',{},{})), \
         patch.object(f.stock_features,'_prepared_frame',return_value=(frame,proof)):
        actual,proofs=f.build_selection_features(pd.DataFrame([row,missing]))
    assert actual.sid.tolist()==[1,2]
    assert actual.v24_feature_ready.tolist()==[True,False]
    assert actual.loc[1,'v24_feature_status']=='MISSING_SEALED_SOURCE'
    assert np.isnan(actual.loc[1,'v24_extension_atr'])
    unavailable=next(p for p in proofs if p.get('kind')=='MISSING_SEALED_SOURCE')
    assert unavailable['snapshot_manifest_sha256']=='pinned'
    assert unavailable['candidate_sids']==[2]


def test_existing_member_without_hash_and_corrupt_source_remain_fatal():
    frame=source();row=candidate(frame);orders=pd.DataFrame([row])
    with patch.object(f.stock_features,'_inventory',side_effect=ValueError('Stock minute source missing: 2026-11-09 ABC')), \
         patch.object(f.stock_features,'_manifest_for_day',return_value=(Path('manifest.json'),'pinned',{'ABC':{}},{})):
        with pytest.raises(ValueError,match='Stock minute source missing'):
            f.build_selection_features(orders)
    proof={'stock_sha256':'verified'}
    with patch.object(f.stock_features,'_inventory',return_value=({'source':[row]},{'source':proof})), \
         patch.object(f.stock_features,'_prepared_frame',side_effect=ValueError('Stock snapshot changed: source')):
        with pytest.raises(ValueError,match='Stock snapshot changed'):
            f.build_selection_features(orders)
