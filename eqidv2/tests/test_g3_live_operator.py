"""Operator handoff tests: disposable state only; no broker/task mutations."""
from datetime import date, datetime, timedelta
import copy
import base64
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest
from tools import g3_live_operator as op

NOW = datetime(2026, 10, 13, 8, 30, tzinfo=op.config.IST)
DAY = NOW.date()
STANDING_NOW = datetime(2026, 10, 23, 9, 10, tzinfo=op.config.IST)


@pytest.fixture
def state():
    value = op.read(op.PROFILE)
    value.update(deployment_stage='PAPER_VALIDATION', live_activation_authorized=False)
    value.pop('live_approval', None)
    return value


@pytest.fixture
def report():
    return dict(schema_version='kite_order_readiness_validation_only_v1',
        mode='VALIDATE_ONLY', state='READ_ACCESS_CONFIRMED', orders_submitted=0,
        order_mutations_available=False, checked_at_ist=NOW.isoformat(),
        completed_at_ist=NOW.isoformat(), proposed_order=None,
        minimum_live_balance_rupees=20000.0,
        market=dict(date=DAY.isoformat(), scheduled_trading_day=True),
        apps=[dict(app=f'app{i}', credential_files_present=True,
            state='READ_ACCESS_CONFIRMED', open_net_positions=0, nonterminal_orders=0,
            orders_submitted=0,
            funds_positive=True, minimum_live_balance_rupees=20000.0,
            live_balance_covers_minimum=True, equity_segment_enabled=True,
            checks=dict(profile='PASS', margins='PASS', orders='PASS', positions='PASS'),
            capabilities=dict(nse=True, mis=True, limit=True, market=True, sl=True, sl_m=True))
            for i in range(1, 9)])


def report_at(report, now):
    value = copy.deepcopy(report)
    value['checked_at_ist'] = now.isoformat()
    value['completed_at_ist'] = now.isoformat()
    value['market']['date'] = now.date().isoformat()
    return value


def test_preview_is_read_only_and_covers_every_setup(state, tmp_path):
    before = copy.deepcopy(state)
    root = tmp_path / 'absent'
    plan = op.plan(DAY, state, now=NOW, runtime_root=root)
    assert state == before and not root.exists()
    assert plan['active_setup_count'] == 61
    assert len(set(plan['setup_ids'])) == 61
    assert plan['targets_pct']['1006_LONG'] == .56
    assert plan['targets_pct']['1031_SHORT'] == 1.66
    assert plan['targets_pct']['1136_LONG'] == 7.15
    assert plan['targets_pct']['1231_SHORT'] == 1.85
    assert plan['targets_pct']['1236_LONG'] == .37
    assert plan['targets_pct']['1331_LONG'] == 1.17
    assert plan['last_confirmation'] == '13:31'
    assert plan['pipeline_deadline'] == '13:33'
    assert plan['live_execution_policy']['max_reserved_open_positions'] is None
    assert plan['live_execution_policy']['max_daily_entries'] is None
    assert plan['live_execution_policy']['max_reserved_gross_exposure_rs'] is None
    assert not plan['mutates_controls'] and not plan['starts_workers']


def test_tomorrow_plan_keeps_approved_midday_filters_and_all_previous_setups(state, tmp_path):
    plan = op.plan(date(2026, 10, 14), state, now=NOW, runtime_root=tmp_path)
    selected = json.loads((op.ROOT / 'configs/strategies/v13_v10_g_4_backtest_selected_midday_v5.json').read_text())
    previous = json.loads((op.ROOT / 'configs/strategies/v13_v10_g_3_production_v5.json').read_text())
    rows = {row['setup_id']: row for row in plan['decision_setups']}
    assert len(selected['setups']) == 12
    fields = {'price_change_pct': 'price_change_pct', 'oi_change_pct': 'oi_change_pct',
              'five_minute_volume_ratio_min': 'volume_ratio', 'one_minute_body_ratio_min': 'body_ratio',
              'one_minute_wick_ratio_max': 'max_wick_ratio',
              'one_minute_volume_ratio_min': 'minimum_confirmation_volume_ratio'}
    for setup in selected['setups']:
        actual = rows[setup['setup_id']]
        assert actual['target_pct'] == setup['target_pct']
        assert actual['stop_pct'] == setup['initial_stop_pct']
        assert actual['confirmation_end'] == setup['confirmation_end']
        assert actual['max_entries'] == setup['max_entries'] == 1
        assert actual['picker'] == setup['picker'] == 'max_liquidity'
        for source, target in fields.items():
            assert actual[target] == setup['thresholds'][source]
    for before in previous['active_setups']:
        setup_id = before['confirmation_end'].replace(':', '') + '_' + before['side']
        expected = {key: value for key, value in before.items() if key != 'source_version'}
        if setup_id == '0926_LONG':
            expected['volume_ratio'] = 1.2
        if setup_id == '1011_SHORT':
            expected['price_change_pct'] = .11
        assert {key: rows[setup_id][key] for key in before if key != 'source_version'} == {
            key: value for key, value in expected.items()}
        expected_minimum = previous['minimum_confirmation_1m_volume_ratio_by_setup'].get(
            setup_id, previous['minimum_confirmation_1m_volume_ratio'][before['side']])
        assert rows[setup_id]['minimum_confirmation_volume_ratio'] == (
            .8 if setup_id == '0941_LONG' else expected_minimum)
    assert len(previous['active_setups']) == 32
    assert len(rows) == 61 and len(plan['signal_to_confirmation']) == 40
    assert plan['exit_policy'] == dict(initial_stop_pct=1.25, tightened_stop_pct=1.,
        tighten_after_minutes=120, entry_expiry_minutes=10, square_off='15:15')
    assert not plan['mutates_controls'] and not plan['starts_workers'] and not plan['submits_orders']


def test_approval_requires_current_fingerprint_and_preserves_sizing(state, report, tmp_path):
    approved, arm = op.approval_payloads(DAY, state, report, now=NOW, runtime_root=tmp_path)
    assert approved['live_execution_policy'] == state['live_execution_policy']
    assert state['live_activation_authorized'] is False
    assert approved['live_approval']['paper_validation'] == 'OPERATOR_OVERRIDE_NOT_ATTESTED'
    assert approved['live_approval']['setup_ids'] == arm['setup_ids']
    assert approved['live_approval']['entry_not_before_ist'] == NOW.isoformat()
    assert arm['entry_not_before_ist'] == NOW.isoformat()
    op.validate_launch(approved, arm, now=NOW, runtime_root=tmp_path)
    with pytest.raises(ValueError, match='today'):
        op.validate_launch(approved, arm, now=NOW + timedelta(days=1), runtime_root=tmp_path)


def test_standing_policy_artifact_is_exactly_bound_to_v12_setups_and_live_caps():
    policy = op.load_standing_policy()
    ids = [setup.setup_id for setup in op.config.ACTIVE_SETUPS]
    assert policy['source'] == 'STANDING_OPERATOR_POLICY'
    assert policy['effective_from_session_date'] == '2026-10-23'
    assert policy['enabled'] is True and policy['revoked'] is False
    assert policy['strategy_version'] == 'V13-V10-G-3-PRODUCTION-V12'
    assert policy['strategy_fingerprint'] == op.config.strategy_fingerprint()
    assert policy['setup_ids'] == ids
    assert policy['setup_ids_sha256'] == op.canonical_fingerprint(ids)
    assert policy['standing_policy_fingerprint'] == op.standing_policy_fingerprint(policy)
    assert policy['live_execution_policy'] == op.config.live_execution_policy.execution_metadata()
    assert policy['live_execution_policy'] == {
        'live_execution_policy_schema_version': 'g3_live_execution_policy_v1',
        'live_execution_policy_fingerprint': op.config.live_execution_policy.policy_fingerprint(),
        'live_execution_profile': 'G3_LIVE_10K_5X',
        'quantity_policy': 'FLOOR_TARGET_EXPOSURE_EQUITY_SHARES',
        'capital_per_trade_rs': 10000.0, 'leverage': 5.0,
        'target_exposure_rs': 50000.0, 'max_reserved_open_positions': None,
        'max_daily_entries': None, 'max_reserved_gross_exposure_rs': None,
    }


@pytest.mark.parametrize('mutation', [
    'disabled', 'revoked', 'strategy', 'setup_order', 'setup_hash',
    'risk_cap', 'policy_fingerprint', 'extra_field',
])
def test_standing_policy_tampering_is_fail_closed(mutation):
    policy = op.read(op.STANDING_POLICY)
    if mutation == 'disabled':
        policy['enabled'] = False
    elif mutation == 'revoked':
        policy['revoked'] = True
    elif mutation == 'strategy':
        policy['strategy_fingerprint'] = 'stale'
    elif mutation == 'setup_order':
        policy['setup_ids'][0], policy['setup_ids'][1] = (
            policy['setup_ids'][1], policy['setup_ids'][0])
    elif mutation == 'setup_hash':
        policy['setup_ids_sha256'] = 'stale'
    elif mutation == 'risk_cap':
        policy['live_execution_policy']['max_daily_entries'] = 5
    elif mutation == 'policy_fingerprint':
        policy['standing_policy_fingerprint'] = 'stale'
    else:
        policy['unreviewed'] = True
    with pytest.raises(ValueError, match='Standing operator policy'):
        op.validate_standing_policy(policy, session_date=STANDING_NOW.date())


def test_standing_policy_is_not_valid_before_effective_date_or_on_weekend():
    policy = op.read(op.STANDING_POLICY)
    with pytest.raises(ValueError, match='not effective'):
        op.validate_standing_policy(policy, session_date=date(2026, 10, 22))
    with pytest.raises(ValueError, match='regular NSE sessions'):
        op.validate_standing_policy(policy, session_date=date(2026, 10, 24))


def test_standing_session_approval_is_distinct_dated_and_launchable(
    state, report, tmp_path
):
    report = report_at(report, STANDING_NOW)
    approved, arm = op.standing_approval_payloads(
        STANDING_NOW.date(), state, report, now=STANDING_NOW,
        runtime_root=tmp_path)
    policy = op.load_standing_policy(session_date=STANDING_NOW.date())
    assert approved['live_approval']['source'] == 'STANDING_OPERATOR_POLICY'
    assert arm['source'] == 'STANDING_OPERATOR_POLICY'
    assert approved['live_approval']['source'] != 'EXPLICIT_OPERATOR_APPLY'
    for row in (approved['live_approval'], arm):
        assert row['standing_policy_id'] == policy['policy_id']
        assert row['standing_policy_fingerprint'] == policy['standing_policy_fingerprint']
        assert row['setup_ids_sha256'] == policy['setup_ids_sha256']
        assert row['entry_not_before_ist'] == STANDING_NOW.isoformat()
        assert row['approved_at_ist'] == STANDING_NOW.isoformat()
        assert row['broker_checked_at_ist'] == report['completed_at_ist']
    op.validate_launch(approved, arm, now=STANDING_NOW, runtime_root=tmp_path)


def test_standing_session_requires_current_day_fresh_broker_and_clear_kills(
    state, report, tmp_path
):
    fresh = report_at(report, STANDING_NOW)
    with pytest.raises(ValueError, match='current IST session'):
        op.standing_approval_payloads(
            STANDING_NOW.date() + timedelta(days=3), state, fresh,
            now=STANDING_NOW, runtime_root=tmp_path)
    with pytest.raises(ValueError, match='60 seconds'):
        op.standing_approval_payloads(
            STANDING_NOW.date(), state, fresh,
            now=STANDING_NOW + timedelta(seconds=61), runtime_root=tmp_path)
    kill = tmp_path / 'fno_oi/v13_v10_g_3_live/kill_switch.json'
    kill.parent.mkdir(parents=True)
    kill.write_text('{"enabled":true}')
    with pytest.raises(ValueError, match='kill switch'):
        op.standing_approval_payloads(
            STANDING_NOW.date(), state, fresh,
            now=STANDING_NOW, runtime_root=tmp_path)


@pytest.mark.parametrize('clock', [(9, 8, 59), (9, 12, 30), (15, 14, 59)])
def test_standing_session_approval_is_rejected_outside_exact_preopen_window(
    state, report, tmp_path, clock
):
    outside = STANDING_NOW.replace(hour=clock[0], minute=clock[1], second=clock[2])
    fresh = report_at(report, outside)
    with pytest.raises(ValueError, match='09:09:00 through 09:12:29'):
        op.standing_approval_payloads(
            outside.date(), state, fresh, now=outside, runtime_root=tmp_path)


@pytest.mark.parametrize('fault', ['source_mismatch', 'policy_binding', 'revoked_policy'])
def test_standing_launch_revalidates_source_and_policy(
    state, report, tmp_path, fault
):
    report = report_at(report, STANDING_NOW)
    approved, arm = op.standing_approval_payloads(
        STANDING_NOW.date(), state, report, now=STANDING_NOW,
        runtime_root=tmp_path)
    policy_path = tmp_path / 'standing.json'
    policy_path.write_text(op.STANDING_POLICY.read_text(encoding='utf-8'), encoding='utf-8')
    if fault == 'source_mismatch':
        arm['source'] = 'EXPLICIT_OPERATOR_APPLY'
    elif fault == 'policy_binding':
        arm['standing_policy_fingerprint'] = 'stale'
    else:
        policy = json.loads(policy_path.read_text())
        policy['revoked'] = True
        policy_path.write_text(json.dumps(policy))
    with pytest.raises(ValueError):
        op.validate_launch(approved, arm, now=STANDING_NOW, runtime_root=tmp_path,
                           standing_policy_path=policy_path)


@pytest.mark.parametrize('fault', [
    'early_epoch', 'late_epoch', 'missing_approved_at', 'changed_approved_at',
    'missing_broker_binding', 'stale_broker_binding', 'future_broker_binding',
])
def test_standing_launch_revalidates_minting_window_and_broker_evidence(
    state, report, tmp_path, fault
):
    report = report_at(report, STANDING_NOW)
    approved, arm = op.standing_approval_payloads(
        STANDING_NOW.date(), state, report, now=STANDING_NOW,
        runtime_root=tmp_path)
    if fault in {'early_epoch', 'late_epoch'}:
        altered = STANDING_NOW.replace(
            hour=8 if fault == 'early_epoch' else 9,
            minute=0 if fault == 'early_epoch' else 12,
            second=0 if fault == 'early_epoch' else 30,
        ).isoformat()
        approved['live_approval']['entry_not_before_ist'] = altered
        approved['live_approval']['approved_at_ist'] = altered
        arm['entry_not_before_ist'] = altered
        arm['approved_at_ist'] = altered
    elif fault == 'missing_approved_at':
        approved['live_approval'].pop('approved_at_ist')
        arm.pop('approved_at_ist')
    elif fault == 'changed_approved_at':
        arm['approved_at_ist'] = STANDING_NOW.replace(second=1).isoformat()
    elif fault == 'missing_broker_binding':
        approved['live_approval'].pop('broker_checked_at_ist')
        arm.pop('broker_checked_at_ist')
    else:
        broker = STANDING_NOW + timedelta(seconds=61 if fault == 'future_broker_binding' else -61)
        approved['live_approval']['broker_checked_at_ist'] = broker.isoformat()
        arm['broker_checked_at_ist'] = broker.isoformat()
    with pytest.raises(ValueError):
        op.validate_launch(approved, arm, now=STANDING_NOW, runtime_root=tmp_path)


def test_explicit_approval_source_remains_independent_of_standing_policy(
    state, report, tmp_path
):
    approved, arm = op.approval_payloads(
        DAY, state, report, now=NOW, runtime_root=tmp_path)
    assert approved['live_approval']['source'] == 'EXPLICIT_OPERATOR_APPLY'
    assert arm['source'] == 'EXPLICIT_OPERATOR_APPLY'
    op.validate_launch(approved, arm, now=NOW, runtime_root=tmp_path,
                       standing_policy_path=tmp_path / 'does-not-exist.json')


def test_standing_artifact_cannot_be_downgraded_to_explicit_source(
    state, report, tmp_path
):
    standing_report = report_at(report, STANDING_NOW)
    approved, arm = op.standing_approval_payloads(
        STANDING_NOW.date(), state, standing_report, now=STANDING_NOW,
        runtime_root=tmp_path)
    approved['live_approval']['source'] = op.EXPLICIT_APPROVAL_SOURCE
    arm['source'] = op.EXPLICIT_APPROVAL_SOURCE
    with pytest.raises(ValueError, match='explicit approval/arm schema'):
        op.validate_launch(approved, arm, now=STANDING_NOW, runtime_root=tmp_path)


@pytest.mark.parametrize('fault', [
    'approval_standing_field', 'arm_standing_field', 'paper_validation',
    'approved_at', 'updated_at', 'broker_naive', 'broker_stale', 'broker_future',
])
def test_explicit_source_schema_and_timestamp_semantics_are_fail_closed(
    state, report, tmp_path, fault
):
    approved, arm = op.approval_payloads(
        DAY, state, report, now=NOW, runtime_root=tmp_path)
    if fault == 'approval_standing_field':
        approved['live_approval']['standing_policy_id'] = 'injected'
    elif fault == 'arm_standing_field':
        arm['standing_policy_fingerprint'] = 'injected'
    elif fault == 'paper_validation':
        approved['live_approval']['paper_validation'] = 'STANDING_OPERATOR_POLICY_OVERRIDE_NOT_ATTESTED'
    elif fault == 'approved_at':
        approved['live_approval']['approved_at_ist'] = (NOW - timedelta(seconds=1)).isoformat()
    elif fault == 'updated_at':
        arm['updated_at_ist'] = (NOW - timedelta(seconds=1)).isoformat()
    elif fault == 'broker_naive':
        approved['live_approval']['broker_checked_at_ist'] = NOW.replace(tzinfo=None).isoformat()
    else:
        seconds = -61 if fault == 'broker_stale' else 1
        approved['live_approval']['broker_checked_at_ist'] = (NOW + timedelta(seconds=seconds)).isoformat()
    with pytest.raises(ValueError):
        op.validate_launch(approved, arm, now=NOW, runtime_root=tmp_path)


def test_tomorrow_plan_preserves_prior_44_and_contains_exact_afternoon_targets(state, tmp_path):
    plan = op.plan(date(2026, 10, 14), state, now=NOW, runtime_root=tmp_path)
    previous = json.loads((op.ROOT / 'configs/strategies/v13_v10_g_3_production_v6.json').read_text())
    rows = {row['setup_id']: row for row in plan['decision_setups']}
    previous_ids = set()
    for before in previous['active_setups']:
        setup_id = before['confirmation_end'].replace(':', '') + '_' + before['side']
        previous_ids.add(setup_id)
        expected = {key: value for key, value in before.items() if key != 'source_version'}
        if setup_id == '0926_LONG':
            expected['volume_ratio'] = 1.2
        if setup_id == '1011_SHORT':
            expected['price_change_pct'] = .11
        assert {key: rows[setup_id][key] for key in before if key != 'source_version'} == {
            key: value for key, value in expected.items()}
        expected_minimum = previous['minimum_confirmation_1m_volume_ratio_by_setup'].get(
            setup_id, previous['minimum_confirmation_1m_volume_ratio'][before['side']])
        assert rows[setup_id]['minimum_confirmation_volume_ratio'] == (
            .8 if setup_id == '0941_LONG' else expected_minimum)
    expected = {'1236_LONG': .37, '1236_SHORT': 1.24, '1246_LONG': .23, '1246_SHORT': .49,
                '1251_LONG': .26, '1251_SHORT': .14, '1256_SHORT': .14, '1301_SHORT': .88,
                '1306_LONG': .18, '1316_LONG': .17, '1316_SHORT': .33, '1321_SHORT': 1.01,
                '1326_LONG': .41, '1326_SHORT': .76, '1331_LONG': 1.17}
    assert len(previous_ids) == 44
    assert set(rows) - previous_ids == set(expected) | op.config.contract.ADDED_SETUP_IDS
    assert {setup_id: rows[setup_id]['target_pct'] for setup_id in expected} == expected
    assert '12:40' not in plan['signal_to_confirmation']
    assert '13:10' not in plan['signal_to_confirmation']
    assert plan['mutates_controls'] is plan['starts_workers'] is plan['submits_orders'] is False


@pytest.mark.parametrize('key,value', [('strategy_fingerprint','stale'),
    ('live_execution_policy_fingerprint','stale'), ('session_date','2026-10-12'),
    ('setup_ids',['1006_LONG']), ('enabled',False)])
def test_stale_or_partial_arm_rejected(state, report, tmp_path, key, value):
    approved, arm = op.approval_payloads(DAY, state, report, now=NOW, runtime_root=tmp_path)
    arm[key] = value
    with pytest.raises(ValueError):
        op.validate_launch(approved, arm, now=NOW, runtime_root=tmp_path)


@pytest.mark.parametrize('value', ['', 'not-a-time', '2026-10-14T09:14:01+05:30'])
def test_missing_invalid_or_future_entry_epoch_is_rejected(
    state, report, tmp_path, value
):
    approved, arm = op.approval_payloads(
        DAY, state, report, now=NOW, runtime_root=tmp_path
    )
    arm['entry_not_before_ist'] = value
    approved['live_approval']['entry_not_before_ist'] = value
    with pytest.raises(ValueError, match='epoch'):
        op.validate_launch(approved, arm, now=NOW, runtime_root=tmp_path)


@pytest.mark.parametrize('name', ['v13_v10_g_3_live','v6_live','v13_v10_g_live','control'])
def test_never_clears_existing_emergency_switch(state, tmp_path, name):
    path = tmp_path / 'fno_oi' / name / 'kill_switch.json'
    path.parent.mkdir(parents=True)
    path.write_text('{"enabled":true}')
    before = path.read_bytes()
    with pytest.raises(ValueError, match='kill switch'):
        op.plan(DAY, state, now=NOW, runtime_root=tmp_path)
    assert path.read_bytes() == before


@pytest.mark.parametrize('field,value', [('open_net_positions',1),('nonterminal_orders',1),
    ('funds_positive',False),('state','SESSION_INVALID_OR_EXPIRED')])
def test_broker_failure_or_unreconciled_account_blocks(report, field, value):
    report['apps'][0][field] = value
    with pytest.raises(ValueError):
        op.validate_broker_report(report, now=NOW)


@pytest.mark.parametrize('field,value', [
    ('open_net_positions', False), ('nonterminal_orders', 0.0),
    ('orders_submitted', '0'),
])
def test_broker_zero_counts_require_integers(report, field, value):
    report['apps'][0][field] = value
    with pytest.raises(ValueError):
        op.validate_broker_report(report, now=NOW)


def test_broker_capabilities_require_literal_true(report):
    report['apps'][0]['capabilities']['sl_m'] = 'false'
    with pytest.raises(ValueError):
        op.validate_broker_report(report, now=NOW)


def test_broker_must_cover_two_reserved_ten_thousand_rupee_trades(report):
    report['apps'][0]['live_balance_covers_minimum'] = False
    with pytest.raises(ValueError):
        op.validate_broker_report(report, now=NOW)
    report['apps'][0]['live_balance_covers_minimum'] = True
    report['minimum_live_balance_rupees'] = 19999.0
    with pytest.raises(ValueError):
        op.validate_broker_report(report, now=NOW)


@pytest.mark.parametrize('value', [False, 0.0, '0'])
def test_report_order_count_requires_integer_zero(report, value):
    report['orders_submitted'] = value
    with pytest.raises(ValueError):
        op.validate_broker_report(report, now=NOW)


def test_stale_broker_proof_and_changed_profile_are_rejected(state, report, tmp_path):
    with pytest.raises(ValueError, match='60 seconds'):
        op.validate_broker_report(report, now=NOW + timedelta(seconds=61))
    state['strategy_fingerprint'] = 'stale'
    with pytest.raises(ValueError, match='does not match'):
        op.approval_payloads(DAY, state, report, now=NOW, runtime_root=tmp_path)


@pytest.mark.parametrize('mutation', [
    'missing_app', 'duplicate_app', 'wrong_market_day', 'non_trading_day',
    'failed_endpoint', 'missing_sl_m', 'missing_credentials',
])
def test_broker_proof_requires_exactly_eight_flat_capable_same_day_apps(report, mutation):
    if mutation == 'missing_app':
        report['apps'].pop()
    elif mutation == 'duplicate_app':
        report['apps'][-1]['app'] = 'app1'
    elif mutation == 'wrong_market_day':
        report['market']['date'] = '2026-10-12'
    elif mutation == 'non_trading_day':
        report['market']['scheduled_trading_day'] = False
    elif mutation == 'failed_endpoint':
        report['apps'][4]['checks']['orders'] = 'NETWORK_UNAVAILABLE'
    elif mutation == 'missing_sl_m':
        report['apps'][2]['capabilities']['sl_m'] = False
    else:
        report['apps'][6]['credential_files_present'] = False
    with pytest.raises(ValueError):
        op.validate_broker_report(report, now=NOW)


@pytest.mark.parametrize('day', [date(2026,10,12),date(2026,10,17),date(2026,10,23)])
def test_old_weekend_or_distant_dates_rejected(state, tmp_path, day):
    with pytest.raises(ValueError):
        op.plan(day, state, now=NOW, runtime_root=tmp_path)


def test_approve_without_explicit_operator_flag_cannot_write(state, monkeypatch):
    monkeypatch.setattr(op, 'read', lambda path: state)
    monkeypatch.setattr(op, 'atomic_json', lambda *a: pytest.fail('Unexpected control mutation'))
    assert op.main(['approve']) == 2


def test_standing_cli_requires_explicit_policy_flag_and_cannot_write(state, monkeypatch):
    monkeypatch.setattr(op, 'read', lambda path: state)
    monkeypatch.setattr(op, 'atomic_json', lambda *a: pytest.fail('Unexpected control mutation'))
    assert op.main(['approve-standing-session']) == 2


def test_standing_cli_validates_before_disabled_profile_enabled_atomic_sequence(
    state, monkeypatch
):
    approved = {**state, 'live_approval': {'source': 'STANDING_OPERATOR_POLICY'}}
    arm = {'source': 'STANDING_OPERATOR_POLICY', 'enabled': True}
    calls = []
    monkeypatch.setattr(op, 'read', lambda path: state)
    monkeypatch.setattr(op, 'standing_approval_payloads',
                        lambda *args, **kwargs: (approved, arm))
    monkeypatch.setattr(op, 'atomic_json',
                        lambda path, value: calls.append((path, copy.deepcopy(value))))
    assert op.main(['approve-standing-session', '--standing-policy-confirmed']) == 0
    assert calls == [
        (op.ARM, {'source': 'STANDING_OPERATOR_POLICY', 'enabled': False}),
        (op.PROFILE, approved),
        (op.ARM, arm),
    ]


def test_standing_cli_validation_failure_precedes_every_control_write(state, monkeypatch):
    monkeypatch.setattr(op, 'read', lambda path: state)
    monkeypatch.setattr(op, 'standing_approval_payloads',
                        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError('blocked')))
    monkeypatch.setattr(op, 'atomic_json', lambda *a: pytest.fail('Unexpected control mutation'))
    assert op.main(['approve-standing-session', '--standing-policy-confirmed']) == 2


def test_standing_cli_can_leave_arm_disabled_for_external_final_commit(
    state, monkeypatch
):
    approved = {**state, 'live_approval': {'source': 'STANDING_OPERATOR_POLICY'}}
    arm = {'source': 'STANDING_OPERATOR_POLICY', 'enabled': True}
    calls = []
    monkeypatch.setattr(op, 'read', lambda path: state)
    monkeypatch.setattr(op, 'standing_approval_payloads',
                        lambda *args, **kwargs: (approved, arm))
    monkeypatch.setattr(op, 'atomic_json',
                        lambda path, value: calls.append((path, copy.deepcopy(value))))
    assert op.main([
        'approve-standing-session', '--standing-policy-confirmed',
        '--leave-arm-disabled',
    ]) == 0
    assert calls[-1] == (op.ARM, {'source': 'STANDING_OPERATOR_POLICY', 'enabled': False})


def test_leave_arm_disabled_is_rejected_for_other_actions(state, monkeypatch):
    monkeypatch.setattr(op, 'read', lambda path: state)
    monkeypatch.setattr(op, 'atomic_json', lambda *args: pytest.fail('Unexpected mutation'))
    assert op.main(['plan', '--leave-arm-disabled']) == 2


def test_acknowledgement_is_scoped_to_validated_child_only(state, report, tmp_path, monkeypatch):
    approved, arm = op.approval_payloads(DAY, state, report, now=NOW, runtime_root=tmp_path)
    monkeypatch.setattr(op, 'read', lambda path: approved if path == op.PROFILE else arm)
    validate = op.validate_launch
    monkeypatch.setattr(op, 'validate_launch', lambda s,a: validate(s,a,now=NOW,runtime_root=tmp_path))
    monkeypatch.setattr(op, 'refresh_broker_report_for_launch', lambda: report)
    calls=[]
    monkeypatch.setattr(op.subprocess, 'call', lambda *a,**kw: calls.append((a,kw)) or 0)
    monkeypatch.delenv(op.config.LIVE_ACK_ENV, raising=False)
    assert op.main(['run-approved']) == 0
    assert calls[0][1]['env'][op.config.LIVE_ACK_ENV] == op.config.LIVE_ACK
    assert op.config.LIVE_ACK_ENV not in op.os.environ
    assert 'activate_fno_v13_v10_g_3_live_kite_qty1.bat' in calls[0][0][0]


def test_run_approved_orders_broker_probe_before_coordinator(state, monkeypatch):
    arm = {'enabled': True}
    events = []
    monkeypatch.setattr(op, 'read', lambda path: state if path == op.PROFILE else arm)
    monkeypatch.setattr(op, 'validate_launch',
                        lambda *args, **kwargs: events.append('validate'))
    monkeypatch.setattr(op, 'refresh_broker_report_for_launch',
                        lambda: events.append('probe'))
    monkeypatch.setattr(op.subprocess, 'call',
                        lambda *args, **kwargs: events.append('coordinator') or 0)
    assert op.main(['run-approved']) == 0
    assert events == ['validate', 'probe', 'validate', 'coordinator']


def test_run_approved_probe_failure_never_starts_coordinator(state, monkeypatch):
    arm = {'enabled': True}
    monkeypatch.setattr(op, 'read', lambda path: state if path == op.PROFILE else arm)
    monkeypatch.setattr(op, 'validate_launch', lambda *args, **kwargs: None)
    monkeypatch.setattr(op, 'refresh_broker_report_for_launch',
                        lambda: (_ for _ in ()).throw(ValueError('broker not flat')))
    monkeypatch.setattr(op.subprocess, 'call',
                        lambda *args, **kwargs: pytest.fail('Coordinator must not start'))
    assert op.main(['run-approved']) == 2


def test_run_approved_control_race_never_starts_coordinator(state, monkeypatch):
    arm = {'enabled': True}
    changed = copy.deepcopy(state)
    changed['live_activation_authorized'] = not state.get('live_activation_authorized', False)
    reads = iter((state, arm, changed, arm))
    monkeypatch.setattr(op, 'read', lambda path: next(reads))
    monkeypatch.setattr(op, 'validate_launch', lambda *args, **kwargs: None)
    monkeypatch.setattr(op, 'refresh_broker_report_for_launch', lambda: None)
    monkeypatch.setattr(op.subprocess, 'call',
                        lambda *args, **kwargs: pytest.fail('Coordinator must not start'))
    assert op.main(['run-approved']) == 2


def test_launch_probe_report_is_bound_to_current_read_only_invocation(report, monkeypatch):
    observed = []

    def fresh_probe(args):
        assert args == [
            '--app', 'all', '--minimum-live-balance',
            str(op.MINIMUM_LIVE_BALANCE_RS),
        ]
        stamp = datetime.now(op.config.IST)
        report['checked_at_ist'] = stamp.isoformat()
        report['completed_at_ist'] = stamp.isoformat()
        report['market']['date'] = stamp.date().isoformat()
        return 0

    monkeypatch.setattr(op.probe, 'main', fresh_probe)
    monkeypatch.setattr(op, 'read', lambda path: report)
    monkeypatch.setattr(op, 'validate_broker_report',
                        lambda value, now=None: observed.append((value, now)))
    assert op.refresh_broker_report_for_launch() is report
    assert len(observed) == 1 and observed[0][0] is report


def test_launch_probe_rejects_nonzero_or_preexisting_report(report, monkeypatch):
    monkeypatch.setattr(op.probe, 'main', lambda args: 2)
    monkeypatch.setattr(op, 'read', lambda path: pytest.fail('Failed probe report must not be read'))
    with pytest.raises(ValueError, match='reconciliation failed'):
        op.refresh_broker_report_for_launch()

    monkeypatch.setattr(op.probe, 'main', lambda args: 0)
    monkeypatch.setattr(op, 'read', lambda path: report)
    with pytest.raises(ValueError, match='current validation-only launch probe'):
        op.refresh_broker_report_for_launch()


@pytest.mark.skipif(os.name != 'nt' or not shutil.which('powershell'), reason='Windows operator wrapper')
@pytest.mark.parametrize('enable_fails', [False, True])
@pytest.mark.parametrize('had_arm', [False, True])
def test_operator_task_transition_and_failure_restore_are_simulated_only(tmp_path, enable_fails, had_arm):
    bat = tmp_path / 'bat'
    bat.mkdir()
    for name in ('approve_g3_live.ps1', 'g3_task_xml_helpers.ps1'):
        shutil.copy2(op.ROOT / 'bat' / name, bat / name)
    profile, arm = tmp_path / 'profile.json', tmp_path / 'arm.json'
    profile.write_text('{"live_activation_authorized":false}')
    if had_arm:
        arm.write_text('{"enabled":false,"previous":true}')
    original_profile = profile.read_bytes()
    original_arm = arm.read_bytes() if had_arm else None
    prepared = dict(task='SIMULATED_G3_TASK', deployment_file=str(profile), arm_file=str(arm),
        scheduled_launcher=str(bat / 'run_g3_approved_live.bat'), session_date='2026-10-14',
        strategy_fingerprint='strategy-fingerprint', active_setup_count=59,
        live_execution_policy=dict(live_execution_policy_fingerprint='policy-fingerprint',
            capital_per_trade_rs=10000.0, leverage=5.0, target_exposure_rs=50000.0,
            max_reserved_open_positions=None, max_daily_entries=None,
            max_reserved_gross_exposure_rs=None))
    (tmp_path / 'plan.json').write_text(json.dumps(prepared))
    # Every scheduler, process and Python/broker operation is replaced before
    # loading the copied wrapper. All paths are inside this disposable root.
    escaped = str(tmp_path).replace("'", "''")
    script = r'''
$ErrorActionPreference='Stop'
$TestRoot='__ROOT__'
$global:SimulatedEnabled=$false
$global:SimulatedRegistered=@()
function FakePython {
    if ($args -contains 'plan') { Get-Content -LiteralPath (Join-Path $TestRoot 'plan.json') -Raw }
    elseif ($args -contains 'approve') {
            $policy = [ordered]@{live_execution_policy_fingerprint='policy-fingerprint';capital_per_trade_rs=10000.0;leverage=5.0;target_exposure_rs=50000.0;max_reserved_open_positions=$null;max_daily_entries=$null;max_reserved_gross_exposure_rs=$null}
            $profile = [ordered]@{deployment_stage='LIVE_APPROVED';live_activation_authorized=$true;strategy_fingerprint='strategy-fingerprint';live_execution_policy=$policy;live_approval=[ordered]@{session_date='2026-10-14';live_execution_policy_fingerprint='policy-fingerprint'}}
            $armState = [ordered]@{enabled=$true;session_date='2026-10-14';strategy_fingerprint='strategy-fingerprint';live_execution_policy_fingerprint='policy-fingerprint'}
            [IO.File]::WriteAllText((Join-Path $TestRoot 'profile.json'), ($profile | ConvertTo-Json -Depth 5 -Compress))
            [IO.File]::WriteAllText((Join-Path $TestRoot 'arm.json'), ($armState | ConvertTo-Json -Depth 5 -Compress))
    }
    $global:LASTEXITCODE=0
}
    function Get-ScheduledTask { param($TaskName); [pscustomobject]@{
        State=$(if($global:SimulatedEnabled){'Ready'}else{'Disabled'});
        Settings=[pscustomobject]@{Enabled=$global:SimulatedEnabled;StartWhenAvailable=$false;RestartCount=0;MultipleInstances='IgnoreNew'};
        Actions=@([pscustomobject]@{Execute='cmd.exe';Arguments='run_g3_approved_live.bat'})
    } }
    function Get-ScheduledTaskInfo { param($TaskName); [pscustomobject]@{NextRunTime=[datetime]'2026-10-14T09:15:00'} }
function Get-CimInstance { }
function Export-ScheduledTask { param($TaskName); '<Task xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task"><Triggers><CalendarTrigger><StartBoundary>2026-09-15T09:15:00</StartBoundary></CalendarTrigger></Triggers><Settings><Enabled>false</Enabled></Settings><Actions><Exec><Command>cmd.exe</Command><Arguments>old</Arguments></Exec></Actions></Task>' }
function Register-ScheduledTask { param($TaskName,$Xml,[switch]$Force); $global:SimulatedRegistered += $Xml }
function Disable-ScheduledTask { param($TaskName); $global:SimulatedEnabled=$false }
function Enable-ScheduledTask { param($TaskName); __ENABLE__ }
try {
    & (Join-Path $TestRoot 'bat/approve_g3_live.ps1') -SessionDate '2026-10-14' -Apply -Python FakePython | Out-Null
    $outcome='SUCCESS'
} catch { $outcome='RESTORED'; $simulationError=$_.Exception.Message }
[pscustomobject]@{Error=$simulationError;Outcome=$outcome;Enabled=$global:SimulatedEnabled;Registrations=$global:SimulatedRegistered.Count;FirstXml=$global:SimulatedRegistered[0];LastXml=$global:SimulatedRegistered[-1]} | ConvertTo-Json
'''.replace('__ROOT__', escaped).replace('__ENABLE__', "throw 'SIMULATED_ENABLE_FAILURE'" if enable_fails else '$global:SimulatedEnabled=$true')
    encoded = base64.b64encode(script.encode('utf-16-le')).decode()
    result = subprocess.run(['powershell','-NoProfile','-ExecutionPolicy','Bypass','-EncodedCommand',encoded],capture_output=True,text=True,timeout=25)
    assert result.returncode == 0, result.stderr
    observed = json.loads(result.stdout)
    assert observed['FirstXml'] is not None, observed
    assert 'run_g3_approved_live.bat' in observed['FirstXml']
    assert '<Enabled>false</Enabled>' in observed['FirstXml']
    assert '2026-09-15T09:15:00' in observed['FirstXml']
    if enable_fails:
        assert observed['Outcome'] == 'RESTORED' and observed['Registrations'] == 2 and not observed['Enabled']
        assert profile.read_bytes() == original_profile
        assert arm.read_bytes() == original_arm if had_arm else not arm.exists()
        assert '<Arguments>old</Arguments>' in observed['LastXml']
    else:
        assert observed['Outcome'] == 'SUCCESS' and observed['Enabled'] and observed['Registrations'] == 1
        assert json.loads(profile.read_text())['live_activation_authorized']
        assert json.loads(arm.read_text())['enabled']
