"""Operator-only G3 activation handoff. Default plan is read-only.

An operator runs bat/approve_g3_live.ps1 -Apply to approve one dated session.
This module never exposes a broker order API. Scheduled execution delegates
to the existing supervised LIVE coordinator only after the dated approval.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
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
        setup_ids=[s.setup_id for s in config.ACTIVE_SETUPS],
        targets_pct={s.setup_id: s.target_pct for s in config.ACTIVE_SETUPS},
        live_execution_policy=config.live_execution_policy.execution_metadata(),
        deployment_file=str(PROFILE), arm_file=str(ARM), task=TASK,
        scheduled_launcher=str(ROOT / 'bat/run_g3_approved_live.bat'),
        broker_report=str(ROOT / 'logs/kite_order_readiness/latest.json'),
        paper_validation='NOT_ATTESTED; operator Apply accepts proceeding without three validated V4 PAPER sessions',
        mutates_controls=False, starts_workers=False, submits_orders=False)


def validate_broker_report(report, *, now=None):
    now = now or datetime.now(config.IST)
    stamp = datetime.fromisoformat(report['completed_at_ist'])
    if stamp.tzinfo is None or not 0 <= (now - stamp).total_seconds() <= 60:
        raise ValueError('Broker read-only reconciliation must be less than 60 seconds old')
    if (report.get('mode') != 'VALIDATE_ONLY' or report.get('orders_submitted') != 0
            or report.get('order_mutations_available') is not False):
        raise ValueError('Invalid broker reconciliation report')
    present = [app for app in report.get('apps', []) if app.get('credential_files_present')]
    if not present:
        raise ValueError('No configured broker applications were checked')
    for app in present:
        if (app.get('state') != 'READ_ACCESS_CONFIRMED' or app.get('open_net_positions') != 0
                or app.get('nonterminal_orders') != 0 or app.get('funds_positive') is not True
                or app.get('equity_segment_enabled') is not True
                or not all(app.get('capabilities', {}).get(k) for k in ('nse', 'mis', 'limit', 'market', 'sl'))):
            raise ValueError('Broker must have verified access, available funds, no open positions/orders and equity capabilities')


def approval_payloads(day, state, report, *, now=None, runtime_root=RUNTIME_ROOT):
    now = now or datetime.now(config.IST)
    prepared = plan(day, state, now=now, runtime_root=runtime_root)
    validate_broker_report(report, now=now)
    approval = dict(source='EXPLICIT_OPERATOR_APPLY', approved_at_ist=now.isoformat(),
        session_date=str(day), strategy_fingerprint=config.strategy_fingerprint(),
        live_execution_policy_fingerprint=config.live_execution_policy.policy_fingerprint(),
        setup_ids=prepared['setup_ids'], paper_validation='OPERATOR_OVERRIDE_NOT_ATTESTED',
        broker_checked_at_ist=report['completed_at_ist'])
    updated = {**state, 'deployment_stage': 'LIVE_APPROVED', 'live_activation_authorized': True,
               'live_approval': approval}
    arm = dict(enabled=True, session_date=str(day), strategy_version=config.STRATEGY_VERSION,
        strategy_fingerprint=config.strategy_fingerprint(),
        live_execution_policy_fingerprint=config.live_execution_policy.policy_fingerprint(),
        source='EXPLICIT_OPERATOR_APPLY', setup_ids=prepared['setup_ids'], updated_at_ist=now.isoformat())
    return updated, arm


def validate_launch(state, arm, *, now=None, runtime_root=RUNTIME_ROOT):
    now = now or datetime.now(config.IST)
    validate_identity(state)
    check_kills(runtime_root)
    approval = state.get('live_approval', {})
    expected = dict(session_date=str(now.date()), strategy_fingerprint=config.strategy_fingerprint(),
        live_execution_policy_fingerprint=config.live_execution_policy.policy_fingerprint(),
        setup_ids=[s.setup_id for s in config.ACTIVE_SETUPS])
    if state.get('deployment_stage') != 'LIVE_APPROVED' or state.get('live_activation_authorized') is not True:
        raise ValueError('LIVE deployment is not approved')
    if arm.get('enabled') is not True or arm.get('strategy_version') != config.STRATEGY_VERSION:
        raise ValueError('LIVE arm is disabled or has a different strategy version')
    if any(arm.get(key) != value or approval.get(key) != value for key, value in expected.items()):
        raise ValueError('Dated approval/arm does not match today and all current setups')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('plan', 'approve', 'run-approved'), nargs='?', default='plan')
    parser.add_argument('--session-date', type=date.fromisoformat)
    parser.add_argument('--operator-confirmed', action='store_true')
    args = parser.parse_args(argv)
    try:
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
        else:
            validate_launch(state, read(ARM))
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
