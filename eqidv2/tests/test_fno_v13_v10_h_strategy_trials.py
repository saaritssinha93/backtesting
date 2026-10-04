"""Causal checks for the fixed H entry, stop and exit research trials."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ai_platform.research.v13h_execution import ExecutionModel, simulate_order
from ai_platform.research.v13h_strategy_trials import _attach_vix, _prepare_trial


def order(**changes):
    value = dict(sid=1, day='2026-09-28', setup_id='0926_SHORT', tradingsymbol='TEST',
                 side='SHORT', confirmation_ts=pd.Timestamp('2026-09-28 09:26', tz='Asia/Kolkata'),
                 trigger=100., native_stop_pct=1., native_target_pct=2.,
                 confirmation_high=100.4, confirmation_low=99.8)
    value.update(changes)
    return value


def path(bars):
    stamp = pd.date_range('2026-09-28 09:27', periods=len(bars), freq='min', tz='Asia/Kolkata')
    return dict(timestamp_ns=stamp.asi8,
                **{key: np.array([bar[i] for bar in bars], dtype=float)
                   for i, key in enumerate(('open', 'high', 'low', 'close'))})


def test_retest_uses_next_open_after_completed_rejection():
    minute = path([(100.2, 100.3, 99.9, 99.95),
                   (99.95, 100.1, 99.8, 99.9),
                   (99.85, 99.9, 99.5, 99.6),
                   (99.6, 99.7, 99.5, 99.6)])
    result = simulate_order(order(research_entry_rule='retest'), minute, ExecutionModel(), None)
    assert result['entry_index'] == 2
    assert result['entry_price'] == pytest.approx(99.85)
    assert result['entry_ts'] == pd.Timestamp('2026-09-28 09:29', tz='Asia/Kolkata')


def test_retest_without_rejection_never_fills():
    minute = path([(100.2, 100.3, 99.9, 99.95),
                   (99.9, 99.95, 99.6, 99.7),
                   (99.7, 99.75, 99.5, 99.6)])
    result = simulate_order(order(research_entry_rule='retest'), minute, ExecutionModel(), None)
    assert result['status'] == 'UNFILLED_RETEST'


def test_failed_breakdown_exits_after_closing_evidence():
    minute = path([(100.1, 100.2, 99.9, 99.95),
                   (99.95, 100.5, 99.8, 100.45),
                   (100.5, 100.55, 100.4, 100.5)])
    result = simulate_order(order(research_invalidation_reference=100.4), minute,
                            ExecutionModel(), None)
    assert result['exit_index'] == 2
    assert result['exit_reason'] == 'FAILED_BREAKDOWN_EXIT'
    assert result['exit_price'] == pytest.approx(100.5)


def test_structural_and_atr_stops_use_fill_and_declared_reference():
    minute = path([(100.1, 100.2, 99.9, 99.95),
                   (99.95, 100., 99.8, 99.9)])
    atr = simulate_order(order(research_stop_distance=1.5), minute, ExecutionModel(), 3000.)
    structure = simulate_order(order(research_stop_reference=101.25), minute, ExecutionModel(), 3000.)
    assert atr['stop_price'] == pytest.approx(101.5)
    assert structure['stop_price'] == pytest.approx(101.25)
    assert atr['planned_risk_rupees'] <= 3000


def test_extension_filter_keeps_missing_atr_explicit_and_does_not_refill():
    rows = pd.DataFrame([dict(**order(), v9_5m_ema9=101., v9_5m_vwap=101., atr_14_5m=0.5,
                              high=101., low=99., v9_rank_in_setup_day=1),
                         dict(**order(sid=2, tradingsymbol='OTHER'), v9_5m_ema9=101.,
                              v9_5m_vwap=101., atr_14_5m=np.nan, high=101., low=99.,
                              v9_rank_in_setup_day=2)])
    selected, decisions = _prepare_trial(rows, 'extension_ema9', ExecutionModel())
    assert selected.sid.tolist() == [2]
    assert decisions.decision.tolist() == ['FILTERED_EXTENSION', 'NO_ATR_G_RULE_RETAINED']


def test_breadth_and_relative_strength_use_completed_decision_fields():
    common = dict(v9_5m_ema9=101., v9_5m_vwap=101., atr_14_5m=1.,
                  high=101., low=99., v9_rank_in_setup_day=1)
    rows = pd.DataFrame([
        dict(**order(), **common, observed_universe_advance_fraction=.25,
             price_change_pct=-.5, nifty_return_pct=-.2),
        dict(**order(sid=2, tradingsymbol='OTHER'), **common,
             observed_universe_advance_fraction=.75,
             price_change_pct=-.1, nifty_return_pct=-.2),
    ])
    breadth, reasons = _prepare_trial(rows, 'breadth_regime', ExecutionModel())
    assert breadth.sid.tolist() == [1]
    assert reasons.decision.tolist() == ['RETAINED', 'FILTERED_BREADTH_REGIME']
    relative, reasons = _prepare_trial(rows, 'relative_strength', ExecutionModel())
    assert relative.sid.tolist() == [1]
    assert reasons.decision.tolist() == ['RETAINED', 'FILTERED_RELATIVE_WEAKNESS']


def test_sector_cap_requires_dated_complete_mapping_and_preserves_first_rank():
    common = dict(v9_5m_ema9=101., v9_5m_vwap=101., atr_14_5m=1.,
                  high=101., low=99., observed_universe_advance_fraction=.25)
    rows = pd.DataFrame([
        dict(**order(), **common, v9_rank_in_setup_day=1),
        dict(**order(sid=2, tradingsymbol='OTHER'), **common, v9_rank_in_setup_day=2),
    ])
    with pytest.raises(ValueError, match='complete dated sector'):
        _prepare_trial(rows, 'sector_cap', ExecutionModel())
    selected, decisions = _prepare_trial(rows, 'sector_cap', ExecutionModel(),
        {('2026-09-28', 'TEST'): 'METAL', ('2026-09-28', 'OTHER'): 'METAL'})
    assert selected.sid.tolist() == [1]
    assert decisions.decision.tolist() == ['RETAINED', 'FILTERED_SECTOR_DUPLICATE']


def test_vix_requires_same_session_value_available_before_signal(tmp_path):
    orders = pd.DataFrame([dict(**order(), signal_ts=pd.Timestamp('2026-09-28 09:25',
                                                                  tz='Asia/Kolkata'))])
    history = tmp_path / 'vix.csv'
    history.write_text('observed_at,available_at,india_vix\n'
                       '2026-09-28T09:24:00+05:30,2026-09-28T09:24:05+05:30,19.5\n',
                       encoding='utf-8')
    matched, _ = _attach_vix(orders, history)
    assert matched.india_vix.iloc[0] == pytest.approx(19.5)
    history.write_text('observed_at,available_at,india_vix\n'
                       '2026-09-28T09:24:00+05:30,2026-09-28T09:26:01+05:30,19.5\n',
                       encoding='utf-8')
    with pytest.raises(ValueError, match='available at decision'):
        _attach_vix(orders, history)
