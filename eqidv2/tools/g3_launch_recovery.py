"""Bounded, read-only prelaunch recovery; healthy-app routing after 09:18 IST."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time, timedelta
import json
import os
from pathlib import Path
import subprocess
import time as clock

from tools import g3_live_operator as op
from g3_broker_routing import ROUTE_ENV

POLICY = op.ROOT / 'configs/authorizations/g3_launch_recovery_v1.json'
STATUS_NAME = 'g3_launch_recovery.json'
IDENTITY_NAME = 'g3_broker_account_identity.json'
TRANSIENT = {'NETWORK_UNAVAILABLE', 'NETWORKEXCEPTION', 'RATE_LIMITED',
             'HTTP_ERROR_502', 'HTTP_ERROR_503', 'HTTP_ERROR_504'}


def enabled(now=None):
    now = now or datetime.now(op.config.IST)
    policy = json.loads(POLICY.read_text(encoding='utf-8-sig'))
    expected = dict(schema_version='g3_launch_recovery_v1', healthy_apps_after_ist='09:18',
                    retry_interval_seconds=15, maximum_recovery_seconds=180, minimum_healthy_apps=1,
                    require_single_verified_account=True, require_dated_approval=True,
                    preopen_requires_all_eight_apps=False,
                    preopen_requires_all_eight_account_identities=True)
    if any(policy.get(k) != v for k, v in expected.items()):
        raise ValueError('Launch recovery policy differs from the reviewed implementation')
    return policy.get('enabled') is True and now.date().isoformat() >= policy['effective_from_session_date']


def identity_from_report(report):
    rows = report.get('apps', [])
    if (len(rows) != 8 or {r.get('app') for r in rows} != set(op.EXPECTED_BROKER_APPS)
            or any(len(str(r.get(k, ''))) != 64 for r in rows
                   for k in ('account_fingerprint', 'credential_fingerprint'))
            or len({r['account_fingerprint'] for r in rows}) != 1):
        raise ValueError('All eight app identities must be verified as one broker account')
    return dict(session_date=report['market']['date'],
                account_fingerprint=rows[0]['account_fingerprint'],
                credential_fingerprints={r['app']: r['credential_fingerprint'] for r in rows})


def record_identity(report, runtime=op.RUNTIME_ROOT):
    identity = identity_from_report(report)
    op.atomic_json(Path(runtime) / 'runtime_status' / IDENTITY_NAME, identity)
    return identity


def validate_preopen_broker(report, now):
    """Authorize the session using complete account evidence, not every connection.

    This never starts a worker. The launch gate independently prevents a
    reduced app set from trading before 09:18.
    """
    if (report.get('proposed_order', object()) is not None
            or report.get('strategy_controls_changed') is not False):
        raise ValueError('Preopen broker evidence must be read-only')
    identity = identity_from_report(report)
    route = select_route(report, identity, now, allow_partial=True)
    if route is None:
        raise ValueError('No healthy app provides complete preopen account evidence')
    return route


def inspect_all():
    """Independent bounded read calls; never retry any broker mutation."""
    started = datetime.now(op.config.IST)
    def inspect(app):
        return op.probe.inspect_app(app, minimum_live_balance=op.MINIMUM_LIVE_BALANCE_RS,
                                    timeout=3, delay=.35)
    with ThreadPoolExecutor(max_workers=8) as pool:
        rows = list(pool.map(inspect, op.EXPECTED_BROKER_APPS))
    return dict(schema_version='kite_order_readiness_validation_only_v1',
                checked_at_ist=started.isoformat(), completed_at_ist=datetime.now(op.config.IST).isoformat(),
                mode='VALIDATE_ONLY', orders_submitted=0, order_mutations_available=False,
                strategy_controls_changed=False,
                proposed_order=None, minimum_live_balance_rupees=op.MINIMUM_LIVE_BALANCE_RS,
                state='READ_ACCESS_CONFIRMED' if all(r['state'] == 'READ_ACCESS_CONFIRMED' for r in rows)
                      else 'PARTIAL_READ_ACCESS',
                apps=rows, readable_apps=[r['app'] for r in rows if r['state'] == 'READ_ACCESS_CONFIRMED'],
                market=op.probe.market_schedule(started))


def select_route(report, identity, now, *, allow_partial):
    rows = report.get('apps', [])
    if (len(rows) != 8 or {r.get('app') for r in rows} != set(op.EXPECTED_BROKER_APPS)
            or identity.get('session_date') != now.date().isoformat()
            or set(identity.get('credential_fingerprints', {})) != set(op.EXPECTED_BROKER_APPS)):
        raise ValueError('Incomplete or stale broker account coverage')
    healthy = []
    for row in rows:
        app = row['app']
        if (row.get('credential_fingerprint') != identity['credential_fingerprints'][app]
                or row.get('account_fingerprint', identity['account_fingerprint']) != identity['account_fingerprint']):
            raise ValueError('Broker account/credential changed since identity verification')
        # Never discard positive evidence of exposure or insufficient funds.
        if (row.get('open_net_positions', 0) != 0 or row.get('nonterminal_orders', 0) != 0
                or row.get('live_balance_covers_minimum') is False
                or row.get('funds_positive') is False):
            raise ValueError('Broker exposure or insufficient funds blocks launch')
        try:
            op.validate_broker_report({**report, 'state': 'READ_ACCESS_CONFIRMED', 'apps': [row]},
                                      now=now, expected_apps=(app,))
            healthy.append(row)
        except ValueError:
            pass
    if not healthy or (len(healthy) != 8 and not allow_partial):
        return None
    # Keep the raw probe intact. This separately labelled selection is validated
    # against the selected app set, not presented as a successful eight-app probe.
    selected = {**report, 'state': 'READ_ACCESS_CONFIRMED', 'apps': healthy}
    op.validate_broker_report(selected, now=now, expected_apps=tuple(r['app'] for r in healthy))
    return dict(session_date=now.date().isoformat(), apps=[r['app'] for r in healthy],
                account_fingerprint=identity['account_fingerprint'],
                credential_fingerprints={r['app']: r['credential_fingerprint'] for r in healthy},
                mode='ALL_APPS' if len(healthy) == 8 else 'HEALTHY_APPS_ONLY',
                excluded_apps=[r['app'] for r in rows if r not in healthy])


def recover(*, validate_controls, identity=None, probe=inspect_all,
            now=lambda: datetime.now(op.config.IST), sleep=clock.sleep,
            monotonic=clock.monotonic, publish=lambda **kw: None):
    started = now()
    deadline = datetime.combine(started.date(), time(9, 18), op.config.IST)
    budget_end = monotonic() + max(0, min(180, (deadline - started).total_seconds()))
    attempt = 0
    while True:
        validate_controls()
        attempt += 1
        publish(state='CHECKING', attempt=attempt, next_retry_at_ist='')
        report = probe()
        current = now()
        validate_controls()
        if report.get('proposed_order', object()) is not None:
            raise ValueError('Launch probe must be validation only')
        # Same-call complete profile evidence also supports manual late starts.
        try:
            current_identity = identity_from_report(report)
        except ValueError:
            current_identity = None
        if identity is None:
            identity = current_identity
        allow_partial = current >= deadline
        route = select_route(report, identity, current, allow_partial=allow_partial) if identity else None
        failures = {r['app']: {k: v for k, v in r.get('checks', {}).items() if v != 'PASS'}
                    for r in report['apps'] if r.get('state') != 'READ_ACCESS_CONFIRMED'}
        if route:
            publish(state='READY', attempt=attempt, mode=route['mode'], healthy_apps=route['apps'],
                    excluded_apps=route['excluded_apps'], failures=failures, next_retry_at_ist='')
            return route, report
        remaining = min((deadline - current).total_seconds(), budget_end - monotonic())
        if allow_partial or remaining <= 0:
            raise ValueError('No safe healthy-app route: broker access or account identity unavailable')
        # Retry network/rate-limit failures only. Other app failures can be
        # quarantined at 09:18, but are not hammered in a retry loop.
        transient = any(v in TRANSIENT for values in failures.values() for v in values.values())
        delay = min(15 if transient else 30, remaining)
        publish(state='RETRYING' if transient else 'WAITING_FOR_0918', attempt=attempt,
                failures=failures, next_retry_at_ist=(current + timedelta(seconds=delay)).isoformat())
        sleep(delay)


def run_approved(state, arm):
    from tools.fno_data_supervisor import SingletonLock
    directory = op.RUNTIME_ROOT / 'runtime_status'
    status = dict(session_date=datetime.now(op.config.IST).date().isoformat(),
                  policy='g3_launch_recovery_v1', orders_submitted_by_probe=0)
    def publish(**values):
        status.update(values, updated_at_ist=datetime.now(op.config.IST).isoformat())
        op.atomic_json(directory / STATUS_NAME, status)
    def controls():
        if op.read(op.PROFILE) != state or op.read(op.ARM) != arm:
            raise ValueError('LIVE approval/arm changed during recovery')
        op.validate_launch(state, arm)
    # A second launch invocation cannot replace the first invocation's status
    # or probe concurrently. The existing supervisor remains the worker owner.
    with SingletonLock(directory / 'g3_launch_recovery.lock'):
        try:
            identity_path = directory / IDENTITY_NAME
            identity = op.read(identity_path) if identity_path.exists() else None
            if identity and identity.get('session_date') != status['session_date']:
                identity = None
            route, report = recover(validate_controls=controls, identity=identity, publish=publish)
            controls()
            op.atomic_json(directory / 'g3_launch_probe.json', report)
            env = {**os.environ, op.config.LIVE_ACK_ENV: op.config.LIVE_ACK,
                   ROUTE_ENV: json.dumps(route, separators=(',', ':'))}
            publish(state='STARTING', mode=route['mode'], healthy_apps=route['apps'],
                    excluded_apps=route['excluded_apps'])
            result = subprocess.call(f'call "{op.ROOT / "bat/activate_fno_v13_v10_g_3_live_kite_qty1.bat"}"',
                                     shell=True, cwd=op.ROOT, env=env)
            publish(state='STOPPED' if result == 0 else 'BLOCKED', exit_code=result)
            return result
        except (OSError, ValueError, KeyError, TypeError) as exc:
            publish(state='BLOCKED', reason=str(exc), next_retry_at_ist='')
            raise
