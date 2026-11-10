import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from .reporting import BASELINE, comparisons, publish, validate


LABELS = ['COMPRESSION_BREAKOUT','EARLIER_BREAKOUT_CONTINUATION',
          'BREAKOUT_WITHOUT_COMPRESSION','OTHER_READY','EARLY_SESSION_NOT_APPLICABLE','UNAVAILABLE']
READY = LABELS[:4]
CONFIG = dict(label_column='compression_label', labels=LABELS,
              ready_labels=READY, primary_label=LABELS[0])


def fixture():
    days = ['2026-10-27','2026-10-28','2026-11-09']
    specs = [(0,0,'A','LONG',LABELS[0],1000.), (1,0,'A','LONG','OTHER_READY',-500.),
             (2,1,'A','LONG',LABELS[0],500.), (3,1,'A','LONG','OTHER_READY',100.),
             (4,2,'B','SHORT','BREAKOUT_WITHOUT_COMPRESSION',-250.),
             (5,2,'B','SHORT','EARLY_SESSION_NOT_APPLICABLE',2000.),
             (6,2,'B','SHORT','UNAVAILABLE',-100.), (7,2,'B','SHORT',LABELS[0],None)]
    orders, trades, features = [], [], []
    for sid, d, setup, side, label, pnl in specs:
        identity = dict(sid=sid, day=days[d], setup_id=setup, tradingsymbol='S'+str(sid), side=side)
        orders.append(identity)
        features.append(dict(identity, compression_label=label, compression_ready=label in READY,
                             is_primary=label==LABELS[0]))
        if pnl is not None:
            trades.append(dict(identity, portfolio_net_profit_rupees=pnl, entry_price=100.,
                exit_price=100.+(pnl+250.)/5000.*(1 if side=='LONG' else -1),
                entry_ts=days[d]+' 10:01:00+05:30', exit_ts=days[d]+' 10:02:00+05:30', exit_reason='TARGET'))
    return pd.DataFrame(orders),pd.DataFrame(trades),pd.DataFrame(features),days


def test_full_inventory_and_daily_partition_are_unchanged():
    orders, trades, features, days = fixture()
    original = trades.copy(deep=True)
    with tempfile.TemporaryDirectory() as directory:
        summary = publish(Path(directory), orders, trades, features, days, CONFIG)
        daily = pd.read_csv(Path(directory)/'daily.csv')
        for day in days:
            rows = daily.loc[daily.day.eq(day)].set_index('cohort')
            assert rows.loc[BASELINE,'net_pnl_rs'] == rows.loc[LABELS,'net_pnl_rs'].sum()
            assert rows.loc[BASELINE,'trades'] == rows.loc[LABELS,'trades'].sum()
        saved = pd.read_csv(Path(directory)/'labeled_trades.csv').set_index('sid')
        assert set(saved.index)==set(trades.sid)
        assert np.allclose(saved.loc[trades.sid,'portfolio_net_profit_rupees'],trades.portfolio_net_profit_rupees)
        assert np.allclose(saved.loc[trades.sid,'entry_price'],trades.entry_price)
        assert np.allclose(saved.loc[trades.sid,'exit_price'],trades.exit_price)
        assert summary['baseline_entries_changed'] is False
        assert summary['admitted_orders']==8 and summary['filled_trades']==7
        assert summary['baseline_metrics']['net_pnl_rs']==2750.
        assert summary['primary_metrics']['selected_orders']==3
        assert summary['primary_metrics']['trades']==2
    pd.testing.assert_frame_equal(original,trades)


def test_early_and_unavailable_excluded_only_from_matched_comparison():
    orders, trades, features, days = fixture()
    _, book, _, labels, primary, unavailable, ready = validate(orders,trades,features,days,CONFIG)
    paired, strata, result = comparisons(book,days,primary,unavailable,ready)
    assert result['primary_fills']==2 and result['comparison_fills']==3
    assert result['raw_primary_mean_rs']==750.
    assert np.isclose(result['raw_comparison_mean_rs'],-650./3)
    assert result['excluded_early_session_fills']==1
    assert result['excluded_unavailable_fills']==1
    assert result['excluded_nonready_fills']==2
    assert result['paired_days']==2
    assert result['paired_equal_day_mean_difference_rs']==950.
    assert result['standardized_mean_difference_rs']==950.
    assert result['small_sample'] is True
    assert strata.setup_id.tolist()==['A']


def test_standardization_uses_same_pooled_weights_not_group_composition():
    rows=[]
    for setup, fresh_count, fresh_pnl, other_count, other_pnl in [('A',1,100.,9,90.),('B',9,10.,1,0.)]:
        for primary,count,pnl in [(True,fresh_count,fresh_pnl),(False,other_count,other_pnl)]:
            for i in range(count):
                rows.append(dict(day='2026-11-09',setup_id=setup,side='LONG',
                    diagnostic_label=LABELS[0] if primary else 'OTHER_READY',portfolio_net_profit_rupees=pnl))
    frame=pd.DataFrame(rows)
    paired,strata,result=comparisons(frame,['2026-11-09'],LABELS[0],'UNAVAILABLE',READY)
    assert result['paired_equal_day_mean_difference_rs']==-62.
    assert result['standardized_mean_difference_rs']==10.
    assert strata.weight.tolist()==[.5,.5]
    assert result['common_support_setup_side_strata']==2


def test_primary_subset_illustration_accounts_for_foregone_winners():
    orders,trades,features,days=fixture()
    with tempfile.TemporaryDirectory() as directory:
        summary=publish(Path(directory),orders,trades,features,days,CONFIG)
    subset=summary['primary_only_illustration']
    assert subset['applied'] is False
    assert subset['retained_fills']==2 and subset['discarded_fills']==5
    assert subset['retained_net_pnl_rs']==1500.
    assert subset['discarded_net_pnl_rs']==1250.
    assert subset['foregone_winners']==2 and subset['avoided_losers']==3
    assert subset['foregone_winning_pnl_rs']==2100.
    assert subset['avoided_losing_pnl_rs']==850.


def test_no_primary_support_is_reported_without_fabricated_effect():
    orders,trades,features,days=fixture()
    features.loc[features.is_primary,'compression_label']='OTHER_READY'
    features['is_primary']=False
    with tempfile.TemporaryDirectory() as directory:
        summary=publish(Path(directory),orders,trades,features,days,CONFIG)
        text=(Path(directory)/'REPORT.md').read_text()
    full=summary['comparisons'][0]
    assert full['primary_fills']==0 and full['paired_days']==0
    assert full['standardized_mean_difference_rs'] is None
    assert full['paired_equal_day_mean_difference_rs'] is None
    assert 'not estimable (no common support)' in text


def test_inventory_identity_and_label_readiness_are_checked():
    orders,trades,features,days=fixture()
    with pytest.raises(ValueError,match='complete frozen admitted inventory'):
        validate(orders,trades,features.iloc[:-1],days,CONFIG)
    bad=features.copy();bad.loc[0,'tradingsymbol']='WRONG'
    with pytest.raises(ValueError,match='identity mismatch'):
        validate(orders,trades,bad,days,CONFIG)
    bad=features.copy();bad.loc[0,'compression_ready']=False
    with pytest.raises(ValueError,match='Readiness'):
        validate(orders,trades,bad,days,CONFIG)


def test_existing_results_cannot_be_overwritten():
    orders,trades,features,days=fixture()
    with tempfile.TemporaryDirectory() as directory:
        publish(Path(directory),orders,trades,features,days,CONFIG)
        with pytest.raises(ValueError,match='already exist'):
            publish(Path(directory),orders,trades,features,days,CONFIG)
