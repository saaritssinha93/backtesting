"""Operator-only G3 activation handoff. Default plan is read-only.

An operator runs bat/approve_g3_live.ps1 -Apply to approve one dated session.
This module never exposes a broker order API. Scheduled execution delegates
to the existing supervised LIVE coordinator only after the dated approval.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date, datetime, time, timedelta
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import fno_v13_v10_g_3_live_config as config
from tools import test_kite_order_readiness as probe

RUNTIME_ROOT = Path(os.environ.get('EQIDV2_RUNTIME_ROOT', 'C:/TradingData/eqidv2'))
PROFILE = ROOT / 'configs/strategies/active_profile.json'
ARM = RUNTIME_ROOT / 'fno_oi/v13_v10_g_3_live/live_arm.json'
TASK = 'EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915'
EXPECTED_BROKER_APPS = tuple(f'app{i}' for i in range(1, 9))
EXPLICIT_APPROVAL_SOURCE = 'EXPLICIT_OPERATOR_APPLY'
STANDING_APPROVAL_SOURCE = 'STANDING_OPERATOR_POLICY'
STANDING_POLICY_SCHEMA = 'g3_live_standing_operator_policy_v1'
STANDING_POLICY_ID = 'G3_LIVE_STANDING_20261024_V2'
STANDING_POLICY_EFFECTIVE_DATE = date(2026, 10, 24)
STANDING_POLICY = ROOT / 'configs/authorizations/g3_live_standing_policy_v2.json'
# A strategy change does not inherit V12's standing LIVE authorization.  The
# V13 artifact is an identity template only until separately approved.
STANDING_POLICY_ENABLED = False
STANDING_APPROVAL_WINDOW_START = time(9, 9)
STANDING_APPROVAL_WINDOW_END = time(9, 12, 30)
BROKER_REPORT = ROOT / 'logs/kite_order_readiness/latest.json'
# Broker-readiness floor only.  It is deliberately independent of portfolio
# admissions now that simultaneous positions, daily admissions and aggregate
# reserved exposure are unlimited by the local G3 LIVE policy.
MINIMUM_LIVE_BALANCE_RS = 20000.0

EXPLICIT_APPROVAL_KEYS = frozenset({
    'source', 'approved_at_ist', 'entry_not_before_ist', 'session_date',
    'strategy_fingerprint', 'live_execution_policy_fingerprint', 'setup_ids',
    'paper_validation', 'broker_checked_at_ist',
})
EXPLICIT_ARM_KEYS = frozenset({
    'enabled', 'session_date', 'strategy_version', 'strategy_fingerprint',
    'live_execution_policy_fingerprint', 'source', 'setup_ids',
    'entry_not_before_ist', 'updated_at_ist',
})
STANDING_APPROVAL_KEYS = EXPLICIT_APPROVAL_KEYS | frozenset({
    'standing_policy_id', 'standing_policy_fingerprint', 'setup_ids_sha256',
})
STANDING_ARM_KEYS = EXPLICIT_ARM_KEYS | frozenset({
    'standing_policy_id', 'standing_policy_fingerprint', 'setup_ids_sha256',
    'approved_at_ist', 'broker_checked_at_ist',
})


def read(path):
    value = json.loads(Path(path).read_text(encoding='utf-8-sig'))
    if not isinstance(value, dict):
        raise ValueError('Expected a JSON object')
    return value


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix='.g3_operator_')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as stream:
            json.dump(value, stream, indent=2, allow_nan=False)
            stream.write('\n')
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def canonical_fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     allow_nan=False).encode('utf-8')).hexdigest()


def current_setup_ids():
    return [setup.setup_id for setup in config.ACTIVE_SETUPS]


def standing_policy_payload():
    setup_ids = current_setup_ids()
    return dict(schema_version=STANDING_POLICY_SCHEMA, source=STANDING_APPROVAL_SOURCE,
        policy_id=STANDING_POLICY_ID,
        effective_from_session_date=STANDING_POLICY_EFFECTIVE_DATE.isoformat(),
        regular_verified_nse_sessions_only=True, enabled=STANDING_POLICY_ENABLED, revoked=False,
        strategy_profile=config.STRATEGY_PROFILE, strategy_version=config.STRATEGY_VERSION,
        strategy_fingerprint=config.strategy_fingerprint(), setup_ids=setup_ids,
        setup_ids_sha256=canonical_fingerprint(setup_ids),
        live_execution_policy=config.live_execution_policy.execution_metadata())


def standing_policy_fingerprint(policy):
    material = dict(policy)
    material.pop('standing_policy_fingerprint', None)
    return canonical_fingerprint(material)


def validate_standing_policy(policy, *, session_date=None):
    """Validate the immutable standing authorization and optional dated use."""
    if not isinstance(policy, dict):
        raise ValueError('Standing operator policy must be a JSON object')
    if policy.get('enabled') is not True or policy.get('revoked') is not False:
        raise ValueError('Standing operator policy is disabled or revoked')
    expected = standing_policy_payload()
    material = dict(policy)
    fingerprint = material.pop('standing_policy_fingerprint', None)
    if material != expected:
        raise ValueError('Standing operator policy does not match the exact current G3 identity and execution policy')
    calculated = standing_policy_fingerprint(policy)
    if fingerprint != calculated:
        raise ValueError('Standing operator policy fingerprint mismatch')
    if policy.get('setup_ids_sha256') != canonical_fingerprint(current_setup_ids()):
        raise ValueError('Standing operator policy ordered setup hash mismatch')
    if session_date is not None:
        if session_date < STANDING_POLICY_EFFECTIVE_DATE:
            raise ValueError('Standing operator policy is not effective for this session')
        schedule = probe.market_schedule(
            datetime.combine(session_date, datetime.min.time(), config.IST))
        if (policy.get('regular_verified_nse_sessions_only') is not True
                or schedule.get('scheduled_trading_day') is not True):
            raise ValueError('Standing operator policy permits verified regular NSE sessions only')
    return policy


def load_standing_policy(path=STANDING_POLICY, *, session_date=None):
    return validate_standing_policy(read(path), session_date=session_date)


def validate_identity(state):
    config.validate_strategy()
    if (state.get('profile') != config.STRATEGY_PROFILE
            or state.get('strategy_version') != config.STRATEGY_VERSION
            or state.get('strategy_fingerprint') != config.strategy_fingerprint()
            or state.get('live_execution_policy') != config.live_execution_policy.execution_metadata()):
        raise ValueError('Deployment does not match the current strategy and LIVE sizing policy')


def check_kills(runtime_root=RUNTIME_ROOT):
    for name in ('v13_v10_g_3_live', 'v6_live', 'v13_v10_g_live', 'control'):
        path = Path(runtime_root) / 'fno_oi' / name / 'kill_switch.json'
        if path.exists() and read(path).get('enabled'):
            raise ValueError(f'Existing kill switch is enabled: {name}')


def plan(day, state, *, now=None, runtime_root=RUNTIME_ROOT):
    now = now or datetime.now(config.IST)
    validate_identity(state)
    if not now.date() <= day <= now.date() + timedelta(days=7):
        raise ValueError('Choose today or a session within the next seven days')
    schedule = probe.market_schedule(datetime.combine(day, datetime.min.time(), config.IST))
    if schedule.get('scheduled_trading_day') is not True:
        raise ValueError('The requested date is not a verified regular trading session')
    if day == now.date() and now.strftime('%H:%M') >= config.SQUARE_OFF:
        raise ValueError('The requested session has reached its square-off cutoff')
    check_kills(runtime_root)
    return dict(mode='PLAN_ONLY', session_date=day.isoformat(), strategy_version=config.STRATEGY_VERSION,
        strategy_fingerprint=config.strategy_fingerprint(), active_setup_count=len(config.ACTIVE_SETUPS),
        setup_ids=current_setup_ids(),
        targets_pct={s.setup_id: s.target_pct for s in config.ACTIVE_SETUPS},
        decision_setups=[dict(setup_id=s.setup_id, **asdict(s),
            minimum_confirmation_volume_ratio=config.confirmation_volume_minimum(s.side, setup_id=s.setup_id))
            for s in config.ACTIVE_SETUPS],
        signal_to_confirmation=dict(config.SIGNAL_TO_CONFIRMATION),
        pipeline_deadline=config.PIPELINE_DEADLINE,
        last_confirmation=max(config.SIGNAL_TO_CONFIRMATION.values()),
        exit_policy=dict(initial_stop_pct=config.INITIAL_STOP_PCT,
            tightened_stop_pct=config.TIGHTENED_STOP_PCT, tighten_after_minutes=config.TIGHTEN_AFTER_MINUTES,
            entry_expiry_minutes=config.ENTRY_EXPIRY_MINUTES, square_off=config.SQUARE_OFF),
        live_execution_policy=config.live_execution_policy.execution_metadata(),
        deployment_file=str(PROFILE), arm_file=str(ARM), task=TASK,
        scheduled_launcher=str(ROOT / 'bat/run_g3_approved_live.bat'),
        broker_report=str(ROOT / 'logs/kite_order_readiness/latest.json'),
        paper_validation='NOT_ATTESTED; operator Apply accepts proceeding without three validated current-profile PAPER sessions',
        mutates_controls=False, starts_workers=False, submits_orders=False)


def validate_broker_report(report, *, now=None):
    now = now or datetime.now(config.IST)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError('Broker reconciliation validation requires a timezone-aware clock')
    now_ist = now.astimezone(config.IST)
    if not isinstance(report, dict):
        raise ValueError('Broker reconciliation report must be a JSON object')
    try:
        stamp = datetime.fromisoformat(report['completed_at_ist'])
    except (KeyError, TypeError, ValueError):
        raise ValueError('Broker reconciliation timestamp is invalid') from None
    if (stamp.tzinfo is None or stamp.utcoffset() is None
            or stamp.astimezone(config.IST).date() != now_ist.date()
            or not 0 <= (now - stamp).total_seconds() <= 60):
        raise ValueError('Broker read-only reconciliation must be less than 60 seconds old')
    market = report.get('market', {})
    if (report.get('schema_version') != 'kite_order_readiness_validation_only_v1'
            or report.get('mode') != 'VALIDATE_ONLY'
            or report.get('state') != 'READ_ACCESS_CONFIRMED'
            or type(report.get('orders_submitted')) is not int
            or report.get('orders_submitted') != 0
            or report.get('order_mutations_available') is not False
            or type(report.get('minimum_live_balance_rupees')) not in (int, float)
            or isinstance(report.get('minimum_live_balance_rupees'), bool)
            or report.get('minimum_live_balance_rupees') != MINIMUM_LIVE_BALANCE_RS
            or market.get('date') != now_ist.date().isoformat()
            or market.get('scheduled_trading_day') is not True):
        raise ValueError('Invalid broker reconciliation report')
    apps = report.get('apps', [])
    app_names = [app.get('app') if isinstance(app, dict) else None
                 for app in apps] if isinstance(apps, list) else []
    if (not isinstance(apps, list) or len(apps) != len(EXPECTED_BROKER_APPS)
            or any(not isinstance(app, dict) for app in apps)
            or len(set(app_names)) != len(EXPECTED_BROKER_APPS)
            or set(app_names) != set(EXPECTED_BROKER_APPS)):
        raise ValueError('Broker reconciliation must cover exactly app1 through app8 once each')
    for app in apps:
        counts_are_zero_integers = all(
            type(app.get(key)) is int and app.get(key) == 0
            for key in ('orders_submitted', 'open_net_positions', 'nonterminal_orders'))
        if (app.get('state') != 'READ_ACCESS_CONFIRMED' or not counts_are_zero_integers
                or app.get('funds_positive') is not True
                or app.get('minimum_live_balance_rupees') != MINIMUM_LIVE_BALANCE_RS
                or app.get('live_balance_covers_minimum') is not True
                or app.get('equity_segment_enabled') is not True
                or app.get('credential_files_present') is not True
                or not all(app.get('checks', {}).get(k) == 'PASS'
                           for k in ('profile', 'margins', 'orders', 'positions'))
                or not all(app.get('capabilities', {}).get(k) is True
                           for k in ('nse', 'mis', 'limit', 'market', 'sl', 'sl_m'))):
            raise ValueError('Broker must have verified access, available funds, no open positions/orders and equity capabilities')


def refresh_broker_report_for_launch():
    """Obtain and validate fresh, read-only broker truth immediately pre-launch."""
    invocation_started = datetime.now(config.IST)
    result = probe.main([
        '--app', 'all', '--minimum-live-balance',
        str(MINIMUM_LIVE_BALANCE_RS),
    ])
    invocation_completed = datetime.now(config.IST)
    if result != 0:
        raise ValueError('Fresh launch broker reconciliation failed')
    report = read(BROKER_REPORT)
    try:
        checked = datetime.fromisoformat(report['checked_at_ist'])
        completed = datetime.fromisoformat(report['completed_at_ist'])
    except (KeyError, TypeError, ValueError):
        raise ValueError('Fresh launch broker reconciliation timestamps are invalid') from None
    if (checked.tzinfo is None or checked.utcoffset() is None
            or completed.tzinfo is None or completed.utcoffset() is None
            or not invocation_started <= checked <= completed <= invocation_completed
            or report.get('proposed_order', object()) is not None):
        raise ValueError('Broker report was not produced by the current validation-only launch probe')
    validate_broker_report(report, now=invocation_completed)
    return report


def approval_payloads(day, state, report, *, now=None, runtime_root=RUNTIME_ROOT):
    now = now or datetime.now(config.IST)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError('Explicit approval requires a timezone-aware clock')
    now_ist = now.astimezone(config.IST)
    prepared = plan(day, state, now=now_ist, runtime_root=runtime_root)
    validate_broker_report(report, now=now_ist)
    entry_not_before_ist = now_ist.isoformat()
    approval = dict(source=EXPLICIT_APPROVAL_SOURCE, approved_at_ist=entry_not_before_ist,
        entry_not_before_ist=entry_not_before_ist,
        session_date=str(day), strategy_fingerprint=config.strategy_fingerprint(),
        live_execution_policy_fingerprint=config.live_execution_policy.policy_fingerprint(),
        setup_ids=prepared['setup_ids'], paper_validation='OPERATOR_OVERRIDE_NOT_ATTESTED',
        broker_checked_at_ist=report['completed_at_ist'])
    updated = {**state, 'deployment_stage': 'LIVE_APPROVED', 'live_activation_authorized': True,
               'live_approval': approval}
    arm = dict(enabled=True, session_date=str(day), strategy_version=config.STRATEGY_VERSION,
        strategy_fingerprint=config.strategy_fingerprint(),
        live_execution_policy_fingerprint=config.live_execution_policy.policy_fingerprint(),
        source=EXPLICIT_APPROVAL_SOURCE, setup_ids=prepared['setup_ids'],
        entry_not_before_ist=entry_not_before_ist, updated_at_ist=entry_not_before_ist)
    return updated, arm


def standing_approval_payloads(day, state, report, *, now=None,
                               runtime_root=RUNTIME_ROOT,
                               standing_policy_path=STANDING_POLICY):
    """Build a same-session approval under the reviewed standing policy."""
    now = now or datetime.now(config.IST)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError('Standing policy approval requires a timezone-aware IST clock')
    now_ist = now.astimezone(config.IST)
    current_day = now.astimezone(config.IST).date()
    if day != current_day:
        raise ValueError('Standing policy can approve only the current IST session')
    clock = now_ist.time().replace(tzinfo=None)
    if not STANDING_APPROVAL_WINDOW_START <= clock < STANDING_APPROVAL_WINDOW_END:
        raise ValueError('Standing policy approval is allowed only from 09:09:00 through 09:12:29 IST')
    prepared = plan(day, state, now=now_ist, runtime_root=runtime_root)
    policy = load_standing_policy(standing_policy_path, session_date=day)
    validate_broker_report(report, now=now_ist)
    entry_not_before_ist = now_ist.isoformat()
    policy_binding = dict(standing_policy_id=policy['policy_id'],
        standing_policy_fingerprint=policy['standing_policy_fingerprint'],
        setup_ids_sha256=policy['setup_ids_sha256'],
        approved_at_ist=entry_not_before_ist,
        broker_checked_at_ist=report['completed_at_ist'])
    approval = dict(source=STANDING_APPROVAL_SOURCE,
        entry_not_before_ist=entry_not_before_ist, session_date=day.isoformat(),
        strategy_fingerprint=config.strategy_fingerprint(),
        live_execution_policy_fingerprint=config.live_execution_policy.policy_fingerprint(),
        setup_ids=prepared['setup_ids'],
        paper_validation='STANDING_OPERATOR_POLICY_OVERRIDE_NOT_ATTESTED',
        **policy_binding)
    updated = {**state, 'deployment_stage': 'LIVE_APPROVED',
               'live_activation_authorized': True, 'live_approval': approval}
    arm = dict(enabled=True, session_date=day.isoformat(),
        strategy_version=config.STRATEGY_VERSION,
        strategy_fingerprint=config.strategy_fingerprint(),
        live_execution_policy_fingerprint=config.live_execution_policy.policy_fingerprint(),
        source=STANDING_APPROVAL_SOURCE, setup_ids=prepared['setup_ids'],
        entry_not_before_ist=entry_not_before_ist,
        updated_at_ist=entry_not_before_ist, **policy_binding)
    return updated, arm


def validate_launch(state, arm, *, now=None, runtime_root=RUNTIME_ROOT,
                    standing_policy_path=STANDING_POLICY):
    now = now or datetime.now(config.IST)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError('LIVE launch validation requires a timezone-aware clock')
    now_ist = now.astimezone(config.IST)
    validate_identity(state)
    check_kills(runtime_root)
    approval = state.get('live_approval', {})
    if not isinstance(approval, dict) or not isinstance(arm, dict):
        raise ValueError('LIVE approval and arm must be JSON objects')
    expected = dict(session_date=str(now_ist.date()), strategy_fingerprint=config.strategy_fingerprint(),
        live_execution_policy_fingerprint=config.live_execution_policy.policy_fingerprint(),
        setup_ids=[s.setup_id for s in config.ACTIVE_SETUPS])
    if state.get('deployment_stage') != 'LIVE_APPROVED' or state.get('live_activation_authorized') is not True:
        raise ValueError('LIVE deployment is not approved')
    if arm.get('enabled') is not True or arm.get('strategy_version') != config.STRATEGY_VERSION:
        raise ValueError('LIVE arm is disabled or has a different strategy version')
    if any(arm.get(key) != value or approval.get(key) != value for key, value in expected.items()):
        raise ValueError('Dated approval/arm does not match today and all current setups')
    source = approval.get('source')
    if arm.get('source') != source:
        raise ValueError('LIVE approval/arm authorization source mismatch')
    if source == STANDING_APPROVAL_SOURCE:
        if (set(approval) != STANDING_APPROVAL_KEYS
                or set(arm) != STANDING_ARM_KEYS):
            raise ValueError('LIVE standing approval/arm schema mismatch')
        policy = load_standing_policy(standing_policy_path, session_date=now_ist.date())
        policy_binding = dict(standing_policy_id=policy['policy_id'],
            standing_policy_fingerprint=policy['standing_policy_fingerprint'],
            setup_ids_sha256=policy['setup_ids_sha256'])
        if any(approval.get(key) != value or arm.get(key) != value
               for key, value in policy_binding.items()):
            raise ValueError('LIVE standing approval/arm does not match the active standing policy')
    elif source == EXPLICIT_APPROVAL_SOURCE:
        if (set(approval) != EXPLICIT_APPROVAL_KEYS
                or set(arm) != EXPLICIT_ARM_KEYS):
            raise ValueError('LIVE explicit approval/arm schema mismatch')
    else:
        raise ValueError('LIVE approval/arm authorization source is not recognized')
    epoch_text = str(arm.get('entry_not_before_ist', '')).strip()
    if approval.get('entry_not_before_ist') != epoch_text:
        raise ValueError('LIVE approval/arm entry epoch mismatch')
    try:
        entry_epoch = datetime.fromisoformat(epoch_text)
    except (TypeError, ValueError):
        raise ValueError('LIVE approval/arm entry epoch is invalid') from None
    if entry_epoch.tzinfo is None or entry_epoch.utcoffset() is None:
        raise ValueError('LIVE approval/arm entry epoch is invalid')
    entry_epoch_ist = entry_epoch.astimezone(config.IST)
    if (entry_epoch_ist.date() != now_ist.date()
            or entry_epoch_ist > now_ist):
        raise ValueError('LIVE approval/arm entry epoch is not a valid past timestamp for today')
    if arm.get('updated_at_ist') != epoch_text:
        raise ValueError('LIVE approval/arm update timestamp binding mismatch')
    expected_paper_validation = (
        'STANDING_OPERATOR_POLICY_OVERRIDE_NOT_ATTESTED'
        if source == STANDING_APPROVAL_SOURCE else 'OPERATOR_OVERRIDE_NOT_ATTESTED')
    if approval.get('paper_validation') != expected_paper_validation:
        raise ValueError('LIVE approval source semantics mismatch')
    if approval.get('approved_at_ist') != epoch_text:
        raise ValueError('LIVE approval timestamp binding mismatch')
    broker_text = str(approval.get('broker_checked_at_ist', '')).strip()
    try:
        broker_stamp = datetime.fromisoformat(broker_text)
    except (TypeError, ValueError):
        raise ValueError('LIVE broker evidence timestamp is invalid') from None
    if broker_stamp.tzinfo is None or broker_stamp.utcoffset() is None:
        raise ValueError('LIVE broker evidence timestamp is invalid')
    broker_stamp_ist = broker_stamp.astimezone(config.IST)
    broker_age = (entry_epoch_ist - broker_stamp_ist).total_seconds()
    if (broker_stamp_ist.date() != now_ist.date()
            or not 0 <= broker_age <= 60):
        raise ValueError('LIVE broker evidence is not same-session and fresh at approval')
    if source == STANDING_APPROVAL_SOURCE:
        clock = entry_epoch_ist.time().replace(tzinfo=None)
        if not STANDING_APPROVAL_WINDOW_START <= clock < STANDING_APPROVAL_WINDOW_END:
            raise ValueError('LIVE standing approval epoch is outside the authorized preopen window')
        if arm.get('approved_at_ist') != epoch_text:
            raise ValueError('LIVE standing approval timestamp binding mismatch')
        if arm.get('broker_checked_at_ist') != broker_text:
            raise ValueError('LIVE standing broker evidence binding mismatch')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('plan', 'approve', 'approve-standing-session',
                                           'run-approved'), nargs='?', default='plan')
    parser.add_argument('--session-date', type=date.fromisoformat)
    parser.add_argument('--operator-confirmed', action='store_true')
    parser.add_argument('--standing-policy-confirmed', action='store_true')
    parser.add_argument('--leave-arm-disabled', action='store_true')
    args = parser.parse_args(argv)
    try:
        if args.leave_arm_disabled and args.action != 'approve-standing-session':
            raise ValueError('--leave-arm-disabled is valid only for standing-session approval')
        state = read(PROFILE)
        day = args.session_date or datetime.now(config.IST).date()
        if args.action == 'plan':
            print(json.dumps(plan(day, state), indent=2))
        elif args.action == 'approve':
            if not args.operator_confirmed:
                raise ValueError('Use the explicit operator PowerShell -Apply command')
            updated, arm = approval_payloads(day, state, read(ROOT / 'logs/kite_order_readiness/latest.json'))
            atomic_json(ARM, {**arm, 'enabled': False})
            atomic_json(PROFILE, updated)
            atomic_json(ARM, arm)
            print('Dated operator approval saved for all current G3 setups')
        elif args.action == 'approve-standing-session':
            if not args.standing_policy_confirmed:
                raise ValueError('Use --standing-policy-confirmed for the reviewed standing authorization')
            updated, arm = standing_approval_payloads(
                day, state, read(ROOT / 'logs/kite_order_readiness/latest.json'))
            # A partial write can never leave the new approval armed: all
            # validations complete first, then the arm is disabled until the
            # profile write has completed atomically.
            atomic_json(ARM, {**arm, 'enabled': False})
            atomic_json(PROFILE, updated)
            atomic_json(ARM, {**arm, 'enabled': not args.leave_arm_disabled})
            print('Standing-policy approval saved for the current G3 session; arm ' +
                  ('left disabled for final gate commit' if args.leave_arm_disabled else 'enabled'))
        else:
            arm = read(ARM)
            validate_launch(state, arm)
            refresh_broker_report_for_launch()
            refreshed_state = read(PROFILE)
            refreshed_arm = read(ARM)
            if refreshed_state != state or refreshed_arm != arm:
                raise ValueError('LIVE approval/arm changed during launch broker reconciliation')
            validate_launch(refreshed_state, refreshed_arm)
            env = {**os.environ, config.LIVE_ACK_ENV: config.LIVE_ACK}
            # Only the human-enabled task reaches this branch. The existing
            # supervisor owns duplicate prevention, worker lifecycle and exits.
            return subprocess.call(f'call "{ROOT / "bat/activate_fno_v13_v10_g_3_live_kite_qty1.bat"}"',
                                   shell=True, cwd=ROOT, env=env)
        return 0
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f'G3 activation blocked: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
