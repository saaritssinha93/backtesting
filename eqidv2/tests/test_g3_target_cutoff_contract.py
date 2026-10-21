"""Cutoff boundary, unchanged retained selection, and execution-mode isolation."""
from dataclasses import asdict
import json

import pandas as pd
import pytest

import fno_v13_v10_g_3_contract as production
import fno_v13_v10_g_3_live_config as runtime
from research import g3_target_cutoff_contract as cutoff
from research import g3_backtest_profile as profile

RESTORED = {'0951_LONG': .75, '1006_LONG': .56, '1016_SHORT': .68}
EXPECTED = {'1236_LONG': .37, '1246_LONG': .23, '1246_SHORT': .49, '1251_LONG': .26,
    '1251_SHORT': .14, '1256_SHORT': .14, '1306_LONG': .18,
    '1316_LONG': .17, '1316_SHORT': .33, '1326_LONG': .41}


def candidate(spec, symbol='VALID', move=None):
    stamp = pd.Timestamp('2026-10-13 ' + spec.signal_end, tz=production.IST)
    long = spec.side == 'LONG'
    row = dict(tradingsymbol=symbol, side=spec.side, signal_timestamp=stamp,
        confirmation_timestamp=stamp + pd.Timedelta(minutes=1), v9_1m_feature_ts=stamp + pd.Timedelta(minutes=1),
        signal_close=100., price_change_pct=move if move is not None else (.9 if long else -.9),
        oi_change_pct=.8, oi=100800., prev_oi=100000., volume_ratio=5.,
        ema9=102. if long else 98., ema20=101. if long else 99., ema50=100.,
        body_ratio=.8, wick_ratio=.05, traded_value=100000., v9_1m_volume_ratio=3.,
        confirmed=True, trigger=101. if long else 99., nifty_first_bar_return_pct=-.2,
        opening_0920_status='VALID', price_change_basis='09:15_CASH_OPEN_TO_09:20_CASH_CLOSE',
        oi_change_source='FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M')
    if spec.setup_id in production.MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP:
        row.update(confirm_open=100., confirm_high=102. if long else 100.1,
                   confirm_low=99.9 if long else 98., confirm_close=101.8 if long else 98.2,
                   v9_1m_ema20=100., v9_1m_atr14=1.)
    return row


def test_exact_restored_exceptions_and_production_isolation():
    assert {s.setup_id: s.target_pct for s in cutoff.DISABLED_SETUPS} == EXPECTED
    assert len(cutoff.ACTIVE_SETUPS) == 51
    assert len(cutoff.SIGNAL_TO_CONFIRMATION) == 35
    assert {s.setup_id: s.target_pct for s in cutoff.ACTIVE_SETUPS if s.target_pct <= .75} == RESTORED
    assert cutoff.setup_for('13:25', 'SHORT').target_pct == .76
    assert runtime.contract is production and len(runtime.ACTIVE_SETUPS) == 61
    assert production.strategy_fingerprint() == cutoff.SOURCE_FINGERPRINT
    assert profile.configuration() == cutoff.strategy_payload()
    assert not production.supported_identity(cutoff.STRATEGY_VERSION, cutoff.strategy_fingerprint())
    assert not cutoff.supported_identity(production.STRATEGY_VERSION, production.strategy_fingerprint())
    assert cutoff.supported_identity(production.STRATEGY_VERSION, production.strategy_fingerprint(), allow_legacy=True)
    assert cutoff.strategy_payload()['execution_authority'] is False
    runtime.validate_strategy()


def test_archived_v8_stays_readable_and_distinct_from_current():
    from research.archived_g3_v8 import contract as archived
    assert len(archived.ACTIVE_SETUPS) == 46 and len(archived.DISABLED_SETUPS) == 13
    assert archived.strategy_fingerprint() == '522d4405294ae9ada7089649f9c097ffc2e85d14d2876ae9ac320f362b1fbca0'
    assert cutoff.contract_for_identity(archived.STRATEGY_VERSION, archived.strategy_fingerprint()) is archived
    assert not cutoff.supported_identity(archived.STRATEGY_VERSION, archived.strategy_fingerprint())
    assert cutoff.supported_identity(archived.STRATEGY_VERSION, archived.strategy_fingerprint(), allow_legacy=True)
    assert cutoff.contract_for_identity(cutoff.STRATEGY_VERSION, archived.strategy_fingerprint()) is None


@pytest.mark.parametrize('spec', production.ACTIVE_SETUPS, ids=lambda s: s.setup_id)
def test_disable_before_execution_and_preserve_every_retained_choice(spec):
    signals = pd.DataFrame([candidate(spec)])
    original = production.select_orders(signals)
    assert len(original) == 1
    result = cutoff.select_orders(signals)
    if spec.setup_id in EXPECTED:
        assert result.empty
        assert not cutoff.passes_selected_filters(candidate(spec), spec)
        return
    assert len(result) == 1 and result.iloc[0].source_signal_id == original.iloc[0].signal_id
    assert result.iloc[0].signal_id != original.iloc[0].signal_id
    assert result.iloc[0].strategy_version == cutoff.STRATEGY_VERSION
    actual, expected = asdict(cutoff._ACTIVE_BY_ID[spec.setup_id]), asdict(spec)
    actual.pop('source_version'); expected.pop('source_version')
    assert actual == expected
    for name in ('target_pct', 'stop_pct', 'trigger', 'max_entries', 'v10_g_f_core', 'native_target_pct'):
        assert result.iloc[0][name] == original.iloc[0][name]


def test_core_priority_still_beats_more_liquid_expanded_candidate():
    spec = cutoff.setup_for('09:50', 'SHORT')
    core = candidate(spec, 'CORE', -.25)
    expanded = {**candidate(spec, 'EXPANDED', -.15), 'traded_value': 999999999.}
    result = cutoff.rank_candidates([expanded, core], spec)
    assert [row['tradingsymbol'] for row in result] == ['CORE']


def test_cutoff_config_drift_fails_without_mutating_runtime(tmp_path):
    value = cutoff.strategy_payload()
    value['active_setups'].append(asdict(production.setup_for('09:50', 'LONG')))
    path = tmp_path / 'invalid.json'
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match='normalized contract'):
        cutoff.load_contract(path)
    assert production.setup_for('09:50', 'LONG').target_pct == .75


def test_preflight_reports_intentional_backtest_difference_truthfully():
    from fno_v13_v10_g_3_preflight import inspect_deployment
    result = inspect_deployment(verify_frozen=False)
    assert result['ready'] and result['backtest_variant_valid']
    assert result['historical_daily_paper_live_contract_matches'] is False
    assert result['backtest_active_setup_count'] == 51
    assert result['active_setup_count'] == 61
