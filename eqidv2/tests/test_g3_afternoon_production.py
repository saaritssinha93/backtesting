"""Exact afternoon promotion, old-book preservation, and causal runtime ranking."""
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

import fno_v13_v10_g_3_contract as contract
import fno_v13_v10_g_3_live_config as runtime
from research.archived_g3_v6 import contract as previous

ROOT = Path(__file__).resolve().parents[1]
SOURCE = json.loads((ROOT / 'configs/strategies/v13_v10_g_3_backtest_selected_afternoon_v7.json').read_text())
EXPECTED_TARGETS = {'1236_LONG': .37, '1236_SHORT': 1.24, '1246_LONG': .23,
    '1246_SHORT': .49, '1251_LONG': .26, '1251_SHORT': .14, '1256_SHORT': .14,
    '1301_SHORT': .88, '1306_LONG': .18, '1316_LONG': .17, '1316_SHORT': .33,
    '1321_SHORT': 1.01, '1326_LONG': .41, '1326_SHORT': .76, '1331_LONG': 1.17}


def test_exact_fifteen_and_preserved_v6_contract():
    assert contract.afternoon_setups.TARGETS == EXPECTED_TARGETS
    assert contract.afternoon_setups.SETUP_IDS == set(EXPECTED_TARGETS)
    archived = json.loads((ROOT / 'configs/strategies/v13_v10_g_3_production_v6.json').read_text())
    assert previous.strategy_payload() == archived
    assert previous.strategy_fingerprint() == "7d01e80f60cc874e7c7b94a894785d134b70df71b27be98fdf3c302a96f44b4a"
    for family in ('core_setups', 'active_setups'):
        now = contract.strategy_payload()[family]
        old = [{**s, 'source_version': archived['strategy_version']} for s in now
               if s['confirmation_end'].replace(':', '') + '_' + s['side'] not in (set(EXPECTED_TARGETS) | contract.ADDED_SETUP_IDS)]
        assert old == archived[family]
        assert len(now) == 61
    for key, value in previous.MIN_CONFIRMATION_VOLUME_BY_SETUP.items():
        assert contract.MIN_CONFIRMATION_VOLUME_BY_SETUP[key] == value
    assert contract.supported_identity(previous.STRATEGY_VERSION, previous.strategy_fingerprint(), allow_legacy=True)
    assert not contract.supported_identity(previous.STRATEGY_VERSION, previous.strategy_fingerprint())
    runtime.validate_strategy()
    assert runtime.PIPELINE_DEADLINE == '13:33'
    assert len(contract.SIGNAL_TO_CONFIRMATION) == 40
    assert (contract.ARMED, contract.EXECUTION_AUTHORITY) == (False, False)


@pytest.mark.parametrize('source', SOURCE['setups'], ids=lambda row: row['setup_id'])
def test_every_approved_threshold_target_and_causal_selection(source):
    setup = contract.setup_for(source['signal_end'], source['side'])
    assert setup == contract._CORE_BY_ID[source['setup_id']]
    assert setup.setup_id == source['setup_id']
    assert setup.confirmation_end == source['confirmation_end']
    fields = {'price_change_pct': 'price_change_pct', 'oi_change_pct': 'oi_change_pct',
              'volume_ratio': 'five_minute_volume_ratio_min', 'body_ratio': 'one_minute_body_ratio_min',
              'max_wick_ratio': 'one_minute_wick_ratio_max'}
    for actual, expected in fields.items():
        assert getattr(setup, actual) == source['thresholds'][expected]
    volume = source['thresholds']['one_minute_volume_ratio_min']
    assert contract.confirmation_volume_minimum(setup.side, setup_id=setup.setup_id) == volume
    assert (setup.target_pct, setup.stop_pct, setup.max_entries, setup.picker) == (EXPECTED_TARGETS[setup.setup_id], 1.25, 1, 'max_liquidity')
    stamp = pd.Timestamp('2026-10-14 ' + setup.signal_end, tz=contract.IST)
    row = dict(tradingsymbol='VALID', side=setup.side, signal_timestamp=stamp,
        confirmation_timestamp=stamp + pd.Timedelta(minutes=1), v9_1m_feature_ts=stamp + pd.Timedelta(minutes=1),
        signal_close=100., price_change_pct=setup.price_change_pct * (1 if setup.side == 'LONG' else -1),
        oi_change_pct=setup.oi_change_pct, oi=100100., prev_oi=100000., volume_ratio=setup.volume_ratio,
        ema9=102. if setup.side == 'LONG' else 98., ema20=101. if setup.side == 'LONG' else 99., ema50=100.,
        body_ratio=setup.body_ratio, wick_ratio=setup.max_wick_ratio, traded_value=1000.,
        v9_1m_volume_ratio=volume, confirmed=True)
    assert contract.passes_selected_filters(row, setup)
    best = dict(row, tradingsymbol='BEST', traded_value=2000.)
    delayed = dict(row, tradingsymbol='DELAYED', traded_value=3000.,
        confirmation_timestamp=stamp + pd.Timedelta(minutes=2), v9_1m_feature_ts=stamp + pd.Timedelta(minutes=2))
    assert [s['tradingsymbol'] for s in contract.rank_candidates([row, delayed, best], setup)] == ['BEST']
    for missing in (None, float('nan'), float('inf')):
        assert not contract.passes_selected_filters(dict(row, v9_1m_volume_ratio=missing), setup)
    for feature in ('price_change_pct', 'oi_change_pct', 'volume_ratio', 'body_ratio', 'v9_1m_volume_ratio'):
        if row[feature] != 0:
            weaker = row[feature] * (.999 if row[feature] > 0 else .999)
            assert not contract.passes_selected_filters(dict(row, **{feature: weaker}), setup)


@pytest.mark.parametrize('clock,side', [('12:40', 'LONG'), ('12:40', 'SHORT'),
    ('12:55', 'LONG'), ('13:00', 'LONG'), ('13:05', 'SHORT'), ('13:10', 'LONG'),
    ('13:10', 'SHORT'), ('13:20', 'LONG'), ('13:30', 'SHORT')])
def test_rejected_afternoon_pairs_remain_absent(clock, side):
    assert contract.setup_for(clock, side) is None


def test_portable_profile_registry_and_runtime_are_consistent():
    from research import g3_backtest_profile as profile
    import fno_v13_v10_g_3_daily_replay as replay
    assert profile.contract is replay.contract
    assert runtime.contract is contract is profile.production
    assert profile.CONFIG_PATH != runtime.CONFIG_PATH
    assert len(profile.contract.ACTIVE_SETUPS) == 51
    assert json.loads(runtime.CONFIG_PATH.read_text()) == contract.strategy_payload()
    registry = json.loads((ROOT / 'docs/ai_platform/profile_registry.json').read_text())
    section = registry['profiles']['V13_V10_G_3']
    assert section['strategy_fingerprint'] == contract.strategy_fingerprint()
    assert section['active_setup_count'] == 61
    assert section['signal_to_confirmation'] == contract.SIGNAL_TO_CONFIRMATION
    assert section['confirmation_policy']['minimum_volume_by_setup'] == contract.MIN_CONFIRMATION_VOLUME_BY_SETUP
