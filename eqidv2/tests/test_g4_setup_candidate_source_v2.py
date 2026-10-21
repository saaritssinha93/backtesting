from copy import deepcopy

import pandas as pd

from research.g4_setup_candidate_source_v2 import source_gate


def candidate():
    signal = pd.Timestamp('2026-10-16 09:25:00',tz='Asia/Kolkata')
    confirm = signal + pd.Timedelta(minutes=1)
    return dict(day='2026-10-16',setup_id='0926_LONG',side='LONG',signal_end='09:25',
        signal_timestamp=signal,confirmation_timestamp=confirm,v9_1m_feature_ts=confirm,
        v9_5m_feature_ts=signal,v9_feature_available_ts=confirm,
        ema9=103.,ema20=102.,ema50=101.,price_change_pct=.11,
        oi=100100.,prev_oi=100000.,oi_change_pct=.1,volume_ratio=.81,
        source_1m_count=5,v9_exact_confirmation_present=True,
        confirm_open=100.,confirm_high=100.3,confirm_low=99.9,confirm_close=100.1,
        signal_close=100.,body_ratio=.25,wick_ratio=.5,traded_value=1000000.,
        v9_1m_volume_ratio=.2)


def test_preserves_rows_failing_current_setup_thresholds():
    # Low setup volume, low confirmation volume/body and weak price still form
    # valid source inputs for a relaxed entry experiment.
    assert source_gate(candidate())


def test_shared_base_gate_is_never_relaxed():
    row = candidate()
    for field,value in [('volume_ratio',.79),('price_change_pct',.09),
                        ('oi_change_pct',.04),('ema9',101.),('oi_change_pct',1.01)]:
        changed = deepcopy(row)
        changed[field] = value
        assert not source_gate(changed)


def test_chronology_and_real_confirmation_remain_mandatory():
    row = candidate()
    for field,value in [('v9_feature_available_ts',row['confirmation_timestamp']+pd.Timedelta(minutes=1)),
                        ('v9_exact_confirmation_present',False),('source_1m_count',4),
                        ('confirm_close',99.99),('confirmation_source_flagged',True)]:
        changed = deepcopy(row)
        changed[field] = value
        assert not source_gate(changed)


def test_nonfinite_candidate_cannot_pass():
    row = candidate()
    row['v9_1m_volume_ratio'] = float('nan')
    assert not source_gate(row)
