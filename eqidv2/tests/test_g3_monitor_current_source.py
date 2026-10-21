"""Current G3 source paths include new slots and sealed daily extensions."""
from datetime import date, timedelta

import pandas as pd
import pytest

from research import g3_trade_monitor_backtest as monitor
from research import g3_trade_monitor_parameter_search as search
from research import g3_1035_extension_backtest as guard
from research import g3_two_slot_backtest as slots


def test_later_sessions_cannot_change_original_selection_or_evaluation_periods():
    original = [date(2026, 7, 29) + timedelta(days=i) for i in range(50)]
    extended = original + [original[-1] + timedelta(days=i) for i in (1, 2)]
    old, new = search._periods(original), search._periods(extended)
    for name in ('calibration25', 'validation10', 'development35', 'evaluation15'):
        assert new[name] == old[name]
    assert new['daily_extensions'] == extended[50:]
    assert new['all'] == extended
    with pytest.raises(ValueError, match='chronological'):
        search._periods(extended[::-1])


@pytest.fixture
def source_paths(tmp_path, monkeypatch):
    days = [date(2026, 7, 29) + timedelta(days=i) for i in range(51)]
    def row(sid, setup, day=days[0], **extra):
        return dict(sid=sid, day=str(day), setup_id=setup, signal_id=f'signal-{sid}', **extra)
    original = pd.DataFrame([row(1, '0931_LONG')])
    revised = pd.DataFrame([row(2, '0941_LONG')])
    guarded = pd.DataFrame([row(3, '1036_LONG')])
    added = pd.DataFrame([row(4, '0921_SHORT'), row(5, '1011_LONG')])
    daily = pd.DataFrame([row(1, '1011_LONG', days[-1])])
    # Daily SID 1 collides with the historical book and was remapped on publish.
    combined_daily = daily.assign(sid=90000006, source_sid=1)
    orders = pd.concat([original, revised, guarded, added, combined_daily], ignore_index=True)
    paths = {sid: {'observed_path': f'actual-minute-{sid}'} for sid in range(1, 6)}
    daily_path = {'observed_path': 'actual-extension-minute-1'}
    monkeypatch.setattr(monitor.history, '_baseline', lambda root: (original, None, days[:50], [], {}))
    monkeypatch.setattr(monitor.history, '_historical_paths', lambda days: ({1: paths[1]}, {}))
    monkeypatch.setattr(monitor.history, '_selected_path_subset',
                        lambda data, selected: {int(sid): data[int(sid)] for sid in selected.sid})
    monkeypatch.setattr(monitor.history, '_revised_0941_orders', lambda *a: (revised, {2: paths[2]}, {}))
    monkeypatch.setattr(guard, 'reselect', lambda *a: (guarded, {3: paths[3]}, {}, None))
    monkeypatch.setattr(slots, 'reselect', lambda *a: (added, {4: paths[4], 5: paths[5]}, {}, None))
    payload = dict(session_date=str(days[-1]), complete=True,
        strategy_fingerprint=monitor.history.contract.strategy_fingerprint(),
        artifacts=dict(selected_orders=dict(path='selected.csv')))
    monkeypatch.setattr(monitor.history, '_checked_reference', lambda root, record: (tmp_path, payload))
    monkeypatch.setattr(monitor.history, '_check_files', lambda *a, **kw: None)
    monkeypatch.setattr(monitor, 'read_frozen_csv', lambda path: daily.copy())
    monkeypatch.setattr(monitor.history, '_daily_paths', lambda *a: ({1: daily_path}, {'sealed': True}))
    summary = dict(daily_sources=[{'run_dir': 'extension'}])
    return tmp_path, orders, days, summary, paths, daily_path


def test_sealed_source_paths_include_both_new_slots_guard_reselection_and_remapped_daily(source_paths):
    root, orders, days, summary, paths, daily_path = source_paths
    actual, proof = monitor._paths_for_source(root, orders, days, summary)
    assert actual == {**paths, 90000006: daily_path}
    assert 'enabled_two_slots' in proof and 'revised_1035_extension' in proof
    assert proof['daily_paths'] == [{'sealed': True}]


def test_later_daily_can_reuse_sid_of_original_order_removed_from_current_profile(source_paths, monkeypatch):
    root, orders, days, summary, paths, daily_path = source_paths
    original, saved, original_days, members, proof = monitor.history._baseline(root)
    removed = pd.DataFrame([dict(sid=7, day=str(days[0]), setup_id='1236_LONG',
                                 signal_id='removed-original-order')])
    original = pd.concat([original, removed], ignore_index=True)
    monkeypatch.setattr(monitor.history, '_baseline',
                        lambda root: (original, saved, original_days, members, proof))
    monkeypatch.setattr(monitor.history, '_historical_paths',
                        lambda days: ({1: paths[1], 7: {'observed_path': 'removed-original-minute'}}, {}))

    # The current builder reserves only current orders. Its later daily SID 7
    # therefore stays 7: the old disabled setup no longer owns that identifier.
    orders = orders.copy()
    extension = orders.sid.eq(90000006)
    orders.loc[extension, ['sid', 'source_sid', 'signal_id']] = [7, 7, 'later-daily-order']
    raw_daily = orders.loc[extension].drop(columns=['source_sid']).copy()
    monkeypatch.setattr(monitor, 'read_frozen_csv', lambda path: raw_daily.copy())
    monkeypatch.setattr(monitor.history, '_daily_paths',
                        lambda *a: ({7: daily_path}, {'sealed': True}))

    actual, _ = monitor._paths_for_source(root, orders, days, summary)
    assert actual == {**paths, 7: daily_path}
    assert actual[7]['observed_path'] == 'actual-extension-minute-1'


@pytest.mark.parametrize('corruption', ['missing_manifest', 'wrong_source_sid', 'unknown_historical_sid'])
def test_unverified_extension_or_unknown_candidate_fails_closed(source_paths, corruption):
    root, orders, days, summary, _, _ = source_paths
    if corruption == 'missing_manifest':
        summary = {'daily_sources': []}
    elif corruption == 'wrong_source_sid':
        orders.loc[orders.sid.eq(90000006), 'source_sid'] = 999
    else:
        orders.loc[orders.sid.eq(5), 'sid'] = 999
    with pytest.raises(ValueError, match='sealed daily extension|path identity|outside verified'):
        monitor._paths_for_source(root, orders, days, summary)


def test_current_source_accepts_extended_calendar_and_rejects_stale_strategy(tmp_path, monkeypatch):
    run = tmp_path / 'run'
    run.mkdir()
    value = dict(strategy_fingerprint=monitor.history.contract.strategy_fingerprint(),
                 active_setup_count=len(monitor.history.contract.ACTIVE_SETUPS), sessions=52)
    monkeypatch.setattr(monitor, 'DEFAULT_SOURCE', tmp_path)
    monkeypatch.setattr(monitor.history, 'verified_package', lambda *a: (value, {}))
    assert monitor._resolve_source(run)[2]['sessions'] == 52
    value['strategy_fingerprint'] = 'previous'
    with pytest.raises(ValueError, match='current G3 history'):
        monitor._resolve_source(run)
