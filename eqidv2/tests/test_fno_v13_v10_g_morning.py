"""The fixed three-slot morning extension must retain its donor contracts."""
import copy
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_g_backtest as g


G_CHANGE = g.SelectionChange(price_multiplier=.65, expansion_side='SHORT')
NEW_IDS = {'0951_LONG', '0956_SHORT', '1001_SHORT'}


def signal(sid=1, *, side='LONG', slot=950, **changes):
    hour, minute = divmod(slot, 100)
    stamp = pd.Timestamp(f'2026-09-01 {hour:02d}:{minute:02d}:00', tz='Asia/Kolkata')
    confirmation = stamp + pd.Timedelta(minutes=1)
    sign = 1 if side == 'LONG' else -1
    result = dict(sid=sid, day='2026-09-01', hhmm_int=slot, side=side,
        tradingsymbol=f'STOCK{sid}', price_change_pct=sign*.25, oi_change_pct=.1,
        volume_ratio=1., body_ratio=.5, wick_ratio=.2, traded_value=1_000_000.,
        v9_1m_volume_ratio=1.2, signal_ts=stamp, confirmation_ts=confirmation,
        v9_1m_feature_ts=confirmation, trigger=100., signal_close=99.9 if sign>0 else 100.1,
        confirmation_open=99.8 if sign>0 else 100.2,
        confirmation_high=100. if sign>0 else 100.3,
        confirmation_low=99.7 if sign>0 else 100.,
        confirmation_close=99.95 if sign>0 else 100.05)
    result.update(changes)
    return result


def select(rows, *, enabled=True, change=G_CHANGE):
    return g.select_orders(pd.DataFrame(rows), g.v9.V9Config(), change,
                           morning_slots=enabled)


@pytest.fixture
def frozen_exits(monkeypatch):
    source = dict(version='V13-v10-B', default=dict(stop_pct=.82, target_pct=1.23),
        setups={'0956_LONG': dict(stop_pct=.6, target_pct=.9),
                '0951_SHORT': dict(stop_pct=.6, target_pct=.93),
                '0931_SHORT': dict(stop_pct=.89, target_pct=2.)},
        partial_exits=False, breakeven_stop=False, evidence='TEST_FIXED_DONORS')
    monkeypatch.setattr(g, 'frozen_f_settings', lambda: {'exit': copy.deepcopy(source)})
    return source


def test_only_registered_missing_side_and_time_pairs_are_added():
    rows = [signal(1), signal(2, side='SHORT', slot=955),
            signal(3, side='SHORT', slot=1000),
            signal(4, side='SHORT', slot=930, price_change_pct=-.15, oi_change_pct=.13),
            signal(5, slot=955), signal(6, slot=1005), signal(7, side='SHORT', slot=1005)]
    before, after = select(rows, enabled=False), select(rows)
    assert set(before.sid) == {4, 5}
    assert set(after.sid) == {1, 2, 3, 4, 5}
    assert set(after.loc[after.sid.isin([1, 2, 3]), 'setup_id']) == NEW_IDS
    columns = ['sid', 'setup_id', 'confirmation_ts', 'trigger', 'picker', 'max_entries']
    pd.testing.assert_frame_equal(before[columns],
        after.loc[after.sid.isin(before.sid), columns].reset_index(drop=True))


@pytest.mark.parametrize('slot', [955, 1000])
def test_new_short_price_gate_stays_point_two_not_g_relaxed_point_thirteen(slot):
    rows = [signal(1, side='SHORT', slot=slot, price_change_pct=-.199999),
            signal(2, side='SHORT', slot=slot, price_change_pct=-.2)]
    assert select(rows).sid.tolist() == [2]


@pytest.mark.parametrize('side,slot,minimum', [('LONG',950,.1), ('SHORT',955,.05), ('SHORT',1000,.05)])
def test_new_oi_gates_are_fixed_donors_not_additionally_relaxed(side, slot, minimum):
    rows = [signal(1, side=side, slot=slot, oi_change_pct=minimum-.000001),
            signal(2, side=side, slot=slot, oi_change_pct=minimum)]
    assert select(rows, change=replace(G_CHANGE, oi_multiplier=.5)).sid.tolist() == [2]


@pytest.mark.parametrize('field,value', [
    ('v9_1m_volume_ratio', 1.199999), ('v9_1m_volume_ratio', np.nan),
    ('v9_1m_volume_ratio', np.inf), ('volume_ratio', .999999),
    ('body_ratio', .399999), ('wick_ratio', .600001),
])
def test_new_slots_keep_both_volume_and_candle_gates(field, value):
    aggressive = g.SelectionChange(price_multiplier=.65, oi_multiplier=.5,
        body_reduction=.1, wick_increase=.1, extra_setup_entries=1)
    assert select([signal(**{field: value})], change=aggressive).empty


def test_new_slot_quota_stays_one_and_uses_native_liquidity_ranking():
    rows = [signal(1, traded_value=1_000_000., price_change_pct=.9),
            signal(2, traded_value=2_000_000., price_change_pct=.25)]
    chosen = select(rows, change=replace(G_CHANGE, extra_setup_entries=1))
    assert chosen.sid.tolist() == [2]
    assert chosen.picker.tolist() == ['max_liquidity']
    assert chosen.max_entries.tolist() == [1]


def test_new_slot_ranking_is_after_volume_filter_and_ignores_future_outcomes():
    rows = [signal(1, traded_value=3_000_000., v9_1m_volume_ratio=1.199),
            signal(2, traded_value=2_000_000., future_pnl=-1e9),
            signal(3, traded_value=1_000_000., future_pnl=1e9)]
    assert select(rows).sid.tolist() == [2]
    rows[1]['future_pnl'], rows[2]['future_pnl'] = 1e9, -1e9
    assert select(rows[::-1]).sid.tolist() == [2]


@pytest.mark.parametrize('offset', [0, 2])
def test_morning_confirmation_must_be_exactly_one_minute_after_signal(offset):
    row = signal()
    row['confirmation_ts'] = row['signal_ts'] + pd.Timedelta(minutes=offset)
    row['v9_1m_feature_ts'] = row['confirmation_ts']
    with pytest.raises(ValueError, match='[Cc]onfirm|[Cc]lock|[Tt]imest|[Cc]ausal'):
        select([row])


def test_config_aliases_new_exits_to_exact_donors_without_mutating_source(frozen_exits):
    before = copy.deepcopy(frozen_exits)
    settings = g.config(frozen_exits, G_CHANGE, morning_slots=True)
    assert frozen_exits == before
    assert settings['exit']['setups']['0951_LONG'] == before['setups']['0956_LONG']
    assert settings['exit']['setups']['0956_SHORT'] == before['setups']['0951_SHORT']
    assert settings['exit']['setups']['1001_SHORT'] == before['setups']['0951_SHORT']
    for key, pair in before['setups'].items():
        assert settings['exit']['setups'][key] == pair
    g.checked_settings(settings)
    settings['exit']['setups']['1001_SHORT']['target_pct'] = 1.2
    with pytest.raises(ValueError):
        g.checked_settings(settings)


@pytest.mark.parametrize('value', ['true', 1, ['0951_LONG'], {'0951_LONG': True}])
def test_config_requires_fixed_whole_package_boolean(frozen_exits, value):
    with pytest.raises(ValueError):
        g.config(frozen_exits, G_CHANGE, morning_slots=value)


def test_legacy_config_without_morning_field_still_replays_old_g(frozen_exits):
    settings = g.config(frozen_exits, G_CHANGE, morning_slots=False)
    settings.pop('morning_slots', None)
    checked = g.checked_settings(settings)
    assert not checked.get('morning_slots', False)
    assert not NEW_IDS.intersection(checked['exit']['setups'])


def complete_path(row, *, first_bar_is_confirmation=False):
    first = row['confirmation_ts'] + pd.Timedelta(minutes=0 if first_bar_is_confirmation else 1)
    cutoff = first.normalize() + pd.Timedelta(hours=15, minutes=15)
    stamps = pd.date_range(first, cutoff, freq='min')
    n = len(stamps)
    return dict(timestamp_ns=stamps.asi8.copy(), open=np.full(n,100.),
                high=np.full(n,100.1), low=np.full(n,99.9), close=np.full(n,100.))


def test_real_replay_enters_after_confirmation_and_uses_donor_exit(frozen_exits):
    row = signal()
    settings = g.config(frozen_exits, G_CHANGE, morning_slots=True)
    rows = pd.DataFrame([row])
    base = replace(g.v9.V9Config(), portfolio_capital_rupees=1_000_000., max_positions=None)
    orders = g.select_orders(rows, base, G_CHANGE, morning_slots=True)
    path = complete_path(row)
    path['high'][1] = 101.
    path['close'][1] = 100.95
    dataset = dict(signals=rows, orders=orders, v9_config=base, paths={1:path}, days=['2026-09-01'])
    trades, ledger, _ = g.evaluate(dataset, settings)
    executed = ledger.loc[ledger.portfolio_executed].iloc[0]
    assert pd.Timestamp(executed.entry_ts) == row['confirmation_ts'] + pd.Timedelta(minutes=1)
    assert executed.native_stop_pct == .6
    assert executed.native_target_pct == .9
    assert executed.exit_reason == 'TARGET'
    bad = complete_path(row, first_bar_is_confirmation=True)
    with pytest.raises(RuntimeError, match='mistimed path'):
        g.evaluate({**dataset, 'paths': {1:bad}}, settings)


def test_combined_rules_preserve_old_g_and_morning_native_choices_before_two_bar_additions():
    # The 09:35 short passes G's .325 price threshold, but not F's .5 gate.
    # Both alternate candidates have higher liquidity, so retaining only the
    # F core would wrongly displace the existing G selection and morning pick.
    rows = [signal(1, side='SHORT', slot=935, price_change_pct=-.34, oi_change_pct=.5),
            signal(6, side='SHORT', slot=935, price_change_pct=-.33, oi_change_pct=.5,
                   traded_value=500_000.),
            signal(2, side='SHORT', slot=935, price_change_pct=-.09, oi_change_pct=.5,
                   traded_value=4_000_000.),
            signal(3), signal(4, price_change_pct=.09, traded_value=4_000_000.),
            signal(5, side='SHORT', slot=955, price_change_pct=-.09)]
    for row in rows:
        sign = 1 if row['side'] == 'LONG' else -1
        row.update(v10_g_two_bar_change_pct=sign*.6, v10_g_two_bar_valid=True,
            v10_g_latest_body_directional=True,
            v10_g_two_bar_previous_ts=row['signal_ts']-pd.Timedelta(minutes=5),
            v10_g_two_bar_base_ts=row['signal_ts']-pd.Timedelta(minutes=10))
    before = select(rows)
    assert set(before.sid) == {1, 3, 6}
    after = g.select_orders(pd.DataFrame(rows), g.v9.V9Config(), G_CHANGE,
                            morning_slots=True, two_bar_continuation=True)
    assert set(after.sid) == {1, 3, 5, 6}
    flags = after.set_index('sid')
    assert not flags.loc[1, 'v10_g_f_core']
    assert flags.loc[[1, 3, 6], 'v10_g_previous_selection'].all()
    assert not flags.loc[[1, 3, 6], 'v10_g_two_bar_selection'].any()
    assert flags.loc[5, 'v10_g_two_bar_selection']
    assert flags.loc[5, 'v10_g_required_price_change_pct'] == .2
