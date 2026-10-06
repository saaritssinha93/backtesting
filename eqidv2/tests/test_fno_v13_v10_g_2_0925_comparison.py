"""The integrated exception must never change another setup or replace core."""
import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_g_2_backtest as g2
from tests.test_fno_v13_v10_g_2_relaxed_0925 import row


def original(**changes):
    value = dict(day='2026-10-05', sid=3, setup_id='0931_LONG', side='LONG',
                 tradingsymbol='ORIGINAL', hhmm_int=930)
    value.update(changes)
    return pd.DataFrame([value])


def test_raw_rejected_candidate_is_added_and_other_order_preserved():
    before = original()
    combined, audit = g2.apply_relaxed_0925_long(before, pd.DataFrame([row()]))
    assert len(combined) == 2
    assert audit.relaxed_0925_added.tolist() == [True]
    old = combined.loc[combined.sid.eq(3), before.columns].reset_index(drop=True)
    before.day = pd.to_datetime(before.day).dt.date
    pd.testing.assert_frame_equal(old, before)
    added = combined.loc[combined.relaxed_0925_added].iloc[0]
    assert added.sid == 4 and added.trigger == row()['confirmation_high']


def test_original_slot_reserved_despite_more_liquid_relaxed_candidate():
    before = original(setup_id='0926_LONG', hhmm_int=925)
    combined, audit = g2.apply_relaxed_0925_long(before, pd.DataFrame([row()]))
    assert combined.tradingsymbol.tolist() == ['ORIGINAL']
    assert audit.original_slot_occupied.all()
    assert not audit.relaxed_0925_added.any()


def test_quota_per_day_and_deterministic_liquidity_ranking():
    observations = [row(tradingsymbol='ZZZ'), row(tradingsymbol='AAA'),
        row(signal_ts='2026-10-06T09:25:00+05:30', confirmation_ts='2026-10-06T09:26:00+05:30')]
    combined, _ = g2.apply_relaxed_0925_long(original(), pd.DataFrame(observations))
    assert combined.loc[combined.relaxed_0925_added, 'tradingsymbol'].tolist() == ['AAA','KALYANKJIL']


@pytest.mark.parametrize('changes', [
    dict(oi_change_pct=1.2001), dict(volume_ratio=1.7499), dict(body_ratio=.5399),
    dict(price_change_pct=.2999), dict(v9_1m_volume_ratio=1.1999),
    dict(v9_1m_upper_wick_ratio=.6001), dict(confirmation_source_flagged=True),
    dict(v9_exact_confirmation_present=False), dict(confirmation_ts='2026-10-05T09:27:00+05:30'),
    dict(v9_1m_feature_ts='2026-10-05T09:27:00+05:30'), dict(traded_value=np.nan),
])
def test_rejection_gates(changes):
    combined, audit = g2.apply_relaxed_0925_long(original(), pd.DataFrame([row(**changes)]))
    assert len(combined) == 1
    assert not audit.relaxed_0925_pass.any()


def test_inclusive_limits_and_explicit_ema_bypass():
    combined, audit = g2.apply_relaxed_0925_long(original(), pd.DataFrame([row(
        oi_change_pct=1.2, volume_ratio=1.75, body_ratio=.54, ema9=np.nan, ema20=np.nan, ema50=np.nan)]))
    assert len(combined) == 2
    assert audit.relaxed_0925_pass.all()


def test_other_clocks_are_not_relaxed():
    combined, audit = g2.apply_relaxed_0925_long(original(side='SHORT'), pd.DataFrame([row(
        signal_ts='2026-10-05T09:30:00+05:30', confirmation_ts='2026-10-05T09:31:00+05:30')]))
    assert len(combined) == 1 and combined.side.tolist() == ['SHORT']
    assert audit.empty


def test_duplicate_observations_fail_closed():
    with pytest.raises(ValueError, match='Duplicate'):
        g2.apply_relaxed_0925_long(original(), pd.DataFrame([row(), row()]))


def test_old_csv_and_new_feature_timezones_are_normalized():
    before = original(signal_ts='2026-10-05 09:30:00+05:30',
                      confirmation_ts='2026-10-05 09:31:00+05:30')
    combined, _ = g2.apply_relaxed_0925_long(before, pd.DataFrame([row()]))
    assert str(combined.confirmation_ts.dt.tz) == 'Asia/Kolkata'
    assert pd.to_datetime(combined.confirmation_ts).notna().all()


def test_empty_original_book_can_add_one_order():
    combined, _ = g2.apply_relaxed_0925_long(original().iloc[:0], pd.DataFrame([row()]))
    assert combined.sid.tolist() == [0]
    assert combined.relaxed_0925_added.tolist() == [True]
