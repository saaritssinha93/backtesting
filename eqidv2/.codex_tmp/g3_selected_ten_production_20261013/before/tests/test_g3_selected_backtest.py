"""The selected-ten offline profile must not change G3 PAPER/LIVE rules."""
from dataclasses import asdict
from datetime import date
import json

import pandas as pd
import pytest

import fno_v13_v10_g_3_contract as production
import fno_v13_replay_source as source
from research import g3_backtest_profile as profile
from research import g3_current_history as history
import fno_v13_v10_g_3_daily_replay as replay
import backtesting_result_v13_v10_g_3_daily as publisher
import dashboard_flow as flow


EXPECTED = {'1036_LONG': 2.12, '1036_SHORT': 2.86, '1041_LONG': 1.71,
    '1046_LONG': .79, '1046_SHORT': 1.01, '1051_LONG': .77, '1056_SHORT': 1.53,
    '1111_LONG': 1.76, '1111_SHORT': 1.84, '1126_SHORT': 4.20}


def test_daily_history_and_dashboard_share_offline_profile_and_preserve_production():
    contract = profile.contract
    assert replay.contract is publisher.contract is history.contract is flow.backtest_contract is contract
    assert contract is not production
    assert len(contract.ACTIVE_SETUPS) == 32 and len(production.ACTIVE_SETUPS) == 22
    assert production.strategy_fingerprint() == '181c37b6a43291311525b0b6970ea0019d5237bce172735913418cba623fb6b9'
    assert not production.supported_identity(contract.STRATEGY_VERSION, contract.strategy_fingerprint())
    for name in ('ACTIVE_SETUPS', 'CORE_SETUPS'):
        old = {s.setup_id: asdict(s) for s in getattr(production, name)}
        for row in getattr(contract, name):
            if row.setup_id in old:
                actual = asdict(row)
                actual.pop('source_version')
                expected = old[row.setup_id]
                expected.pop('source_version')
                assert actual == expected
    assert {s.setup_id: s.target_pct for s in contract.ACTIVE_SETUPS if s.setup_id not in production._ACTIVE_BY_ID} == EXPECTED


def test_additions_use_effective_thresholds_and_exact_clock_only():
    c = profile.contract
    for row in profile.configuration()['setups']:
        spec = c._ACTIVE_BY_ID[row['setup_id']]
        assert spec == c._CORE_BY_ID[row['setup_id']]
        assert spec.price_change_pct == row['thresholds']['price_change_pct']
        assert c.confirmation_volume_minimum(row['side'], setup_id=spec.setup_id) == row['thresholds']['one_minute_volume_ratio_min']
        assert spec.max_entries == 1 and spec.picker == 'max_liquidity'
    for clock, side in [('10:40', 'SHORT'), ('10:50', 'SHORT'), ('10:55', 'LONG'),
                        ('11:00', 'LONG'), ('11:05', 'SHORT'), ('11:15', 'LONG'),
                        ('11:25', 'LONG'), ('11:30', 'SHORT')]:
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
    assert '10:35' not in set(default.strftime('%H:%M'))
    assert '10:35' in set(offline.strftime('%H:%M'))
    assert source.config.SIGNAL_TO_CONFIRMATION is original
    assert source._slot_times(date(2026, 10, 13))[0].equals(default)


def test_config_drift_is_rejected_without_touching_production(tmp_path, monkeypatch):
    config = profile.configuration()
    config['setups'][1]['thresholds']['price_change_pct'] = .1
    path = tmp_path / 'changed.json'
    path.write_text(json.dumps(config))
    monkeypatch.setattr(profile, 'CONFIG_PATH', path)
    with pytest.raises(ValueError, match='selected G4 configuration'):
        profile.configuration()
    assert production.setup_for('10:35', 'SHORT') is None


def test_exported_config_cannot_mutate_the_loaded_profile():
    before = profile.contract.strategy_fingerprint()
    exported = profile.contract.strategy_payload()
    exported['source_selection_config']['setups'][0]['target_pct'] = 99.
    assert profile.contract.strategy_fingerprint() == before
    profile.contract.validate_contract()


def test_dashboard_prefers_current_offline_history_even_with_separate_live_profile(tmp_path, monkeypatch):
    monkeypatch.setattr(flow, 'DATA_ROOT', tmp_path)
    daily = tmp_path / 'daily_results.csv'
    daily.write_text('day,trades,wins,losses,net_pnl\n2026-10-13,0,0,0,0\n')
    base = dict(strategy='V13-V10-G-3', kind='backtest', verified=True, run_name='r',
        period_start='2026-10-13', period_end='2026-10-13', sessions=1, active_setup_count=32)
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
