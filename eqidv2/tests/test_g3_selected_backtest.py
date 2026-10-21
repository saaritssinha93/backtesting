"""Backtest exclusions preserve all retained rules and the PAPER/LIVE profile."""
from dataclasses import asdict
from datetime import date
import json

import pandas as pd
import pytest

import fno_v13_v10_g_3_contract as production
from research.archived_g3_v6 import contract as v4
from research.archived_g3_v4 import selected_profile as old_offline
import fno_v13_replay_source as source
from research import g3_backtest_profile as profile
from research import g3_current_history as history
import fno_v13_v10_g_3_daily_replay as replay
import backtesting_result_v13_v10_g_3_daily as publisher
import dashboard_flow as flow


EXPECTED = {'1236_LONG': .37, '1236_SHORT': 1.24, '1246_LONG': .23, '1246_SHORT': .49,
    '1251_LONG': .26, '1251_SHORT': .14, '1256_SHORT': .14, '1301_SHORT': .88,
    '1306_LONG': .18, '1316_LONG': .17, '1316_SHORT': .33, '1321_SHORT': 1.01,
    '1326_LONG': .41, '1326_SHORT': .76, '1331_LONG': 1.17}


def test_daily_dashboard_share_cutoff_and_preserve_archived_rules():
    contract = profile.contract
    assert replay.contract is publisher.contract is flow.backtest_contract is contract
    assert history.contract is production
    assert contract is not production
    assert len(contract.ACTIVE_SETUPS) == 51 and len(production.ACTIVE_SETUPS) == 61
    assert len(v4.ACTIVE_SETUPS) == 44
    assert v4.strategy_fingerprint() == '7d01e80f60cc874e7c7b94a894785d134b70df71b27be98fdf3c302a96f44b4a'
    assert not production.supported_identity(contract.STRATEGY_VERSION, contract.strategy_fingerprint())
    assert contract.supported_identity(contract.STRATEGY_VERSION, contract.strategy_fingerprint())
    assert not production.supported_identity(v4.STRATEGY_VERSION, v4.strategy_fingerprint())
    assert production.supported_identity(v4.STRATEGY_VERSION, v4.strategy_fingerprint(), allow_legacy=True)
    assert old_offline.contract.strategy_fingerprint() == "494ca2cc653ca5c58f297040b43e33355a3c38351d9a9ac00b592b1ac6e44eb0"
    for name in ('ACTIVE_SETUPS', 'CORE_SETUPS'):
        old = {s.setup_id: asdict(s) for s in getattr(v4, name)}
        for row in getattr(contract, name):
            if row.setup_id in old:
                actual = asdict(row)
                actual.pop('source_version')
                expected = old[row.setup_id]
                expected.pop('source_version')
                assert actual == expected
    assert {s.setup_id: s.target_pct for s in contract.ACTIVE_SETUPS if s.setup_id not in v4._ACTIVE_BY_ID} == {
        **{key: value for key, value in EXPECTED.items() if value > .75},
        "0921_SHORT": 1.5, "1011_LONG": 1.5}
    assert {s.setup_id: s.target_pct for s in production.ACTIVE_SETUPS if s.setup_id not in v4._ACTIVE_BY_ID} == {
        **EXPECTED, "0921_SHORT": 1.5, "1011_LONG": 1.5}


def test_additions_use_effective_thresholds_and_exact_clock_only():
    c = profile.contract
    for row in profile.production_configuration()['setups']:
        if row['target_pct'] <= .75:
            assert row['setup_id'] not in c._ACTIVE_BY_ID
            continue
        spec = c._ACTIVE_BY_ID[row['setup_id']]
        assert spec == c._CORE_BY_ID[row['setup_id']]
        assert spec.price_change_pct == row['thresholds']['price_change_pct']
        assert c.confirmation_volume_minimum(row['side'], setup_id=spec.setup_id) == row['thresholds']['one_minute_volume_ratio_min']
        assert spec.max_entries == 1 and spec.picker == 'max_liquidity'
    for clock, side in [('10:40', 'SHORT'), ('10:50', 'SHORT'), ('10:55', 'LONG'),
                        ('11:00', 'LONG'), ('11:05', 'SHORT'), ('11:15', 'LONG'),
                        ('11:25', 'LONG'), ('11:30', 'SHORT'), ('11:45', 'LONG'),
                        ('11:50', 'LONG'), ('11:55', 'SHORT'), ('12:00', 'LONG'),
                        ('12:05', 'SHORT'), ('12:10', 'LONG'), ('12:20', 'SHORT'), ('12:25', 'LONG')]:
        assert c.setup_for(clock, side) is None
    ts = pd.Timestamp('2026-10-13 10:55', tz='Asia/Kolkata')
    row = dict(tradingsymbol='AAA', side='SHORT', signal_timestamp=ts,
        confirmation_timestamp=ts+pd.Timedelta(minutes=1), v9_1m_feature_ts=ts+pd.Timedelta(minutes=1),
        signal_close=100., price_change_pct=-.2, oi_change_pct=.1, oi=100100., prev_oi=100000.,
        volume_ratio=2., ema9=98., ema20=99., ema50=100., body_ratio=.1, wick_ratio=.8,
        traded_value=1000., v9_1m_volume_ratio=.8, confirmed=True)
    spec = c.setup_for('10:55', 'SHORT')
    assert c.passes_selected_filters(row, spec)
    row['confirmation_timestamp'] += pd.Timedelta(minutes=1)
    row['v9_1m_feature_ts'] = row['confirmation_timestamp']
    assert not c.passes_selected_filters(row, spec)


def test_source_clock_expansion_is_per_call_and_never_mutates_shared_config():
    original = source.config.SIGNAL_TO_CONFIRMATION
    default = source._slot_times(date(2026, 10, 13))[0]
    offline = source._slot_times(date(2026, 10, 13), profile.contract.SIGNAL_TO_CONFIRMATION)[0]
    assert '10:35' in set(default.strftime('%H:%M'))
    assert len(default) == 40 and len(offline) == 35
    assert set(offline).issubset(set(default))
    assert '12:45' in set(default.strftime('%H:%M'))
    assert '12:45' not in set(offline.strftime('%H:%M'))
    assert '10:35' in set(offline.strftime('%H:%M'))
    assert source.config.SIGNAL_TO_CONFIRMATION is original
    assert source._slot_times(date(2026, 10, 13))[0].equals(default)


def test_config_drift_is_rejected_without_touching_production(tmp_path, monkeypatch):
    config = production.strategy_payload()
    next(s for s in config['active_setups'] if s['signal_end'] == '10:35' and s['side'] == 'SHORT')['price_change_pct'] = .1
    path = tmp_path / 'changed.json'
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError, match='accepted normalized contract'):
        production.load_contract(path)
    assert production.setup_for('10:35', 'SHORT').price_change_pct == .13


def test_exported_config_cannot_mutate_the_loaded_profile():
    before = profile.contract.strategy_fingerprint()
    exported = profile.contract.strategy_payload()
    exported['active_setups'][0]['target_pct'] = 99.
    assert profile.contract.strategy_fingerprint() == before
    profile.contract.validate_contract()


def test_dashboard_prefers_current_offline_history_even_with_separate_live_profile(tmp_path, monkeypatch):
    monkeypatch.setattr(flow, 'DATA_ROOT', tmp_path)
    daily = tmp_path / 'daily_results.csv'
    daily.write_text('day,trades,wins,losses,net_pnl\n2026-10-13,0,0,0,0\n')
    base = dict(strategy='V13-V10-G-3', kind='backtest', verified=True, run_name='r',
        period_start='2026-10-13', period_end='2026-10-13', sessions=1, active_setup_count=51)
    offline = dict(base, id='offline', current_full_strategy=True, strategy_matches_active=False,
        strategy_matches_backtest=True, role='backtest_history')
    live = dict(base, id='old-daily', current_full_strategy=False, strategy_matches_active=True,
        role='production_daily', kind='production_daily')
    monkeypatch.setattr(flow, '_verified_g3_current_backtest', lambda: [(offline, daily)])
    monkeypatch.setattr(flow, '_verified_g3_runs', lambda: [(live, daily)])
    for name in ('_verified_g3_0920_long_backtest', '_verified_g3_0920_daily', '_verified_g3_research_runs'):
        monkeypatch.setattr(flow, name, lambda: [])
    result = flow.load_flow_data(strategy='V13-V10-G-3')
    assert result['selected_run']['id'] == 'offline'
    assert not any('saved history has not been relabeled' in w for w in result['warnings'])
