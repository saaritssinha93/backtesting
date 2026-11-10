"""Offline launch, account routing and dashboard tests. No real orders or network."""
from copy import deepcopy
from datetime import datetime, timedelta
import json
import os
from types import SimpleNamespace

import pytest

import g3_broker_routing as routing
from tools import g3_launch_recovery as recovery

NOW = datetime(2026, 11, 6, 9, 15, tzinfo=routing.IST)


@pytest.fixture(autouse=True)
def no_real_broker(monkeypatch):
    monkeypatch.setattr(recovery.op.probe, 'inspect_app',
                        lambda *a, **kw: pytest.fail('Real broker access is forbidden in tests'))


def report(now=NOW, bad=()):
    rows = []
    for i in range(1, 9):
        app = f'app{i}'
        rows.append(dict(app=app, credential_files_present=True,
                         account_fingerprint='a' * 64, credential_fingerprint=str(i) * 64,
                         state='PARTIAL_READ_ACCESS' if app in bad else 'READ_ACCESS_CONFIRMED',
                         open_net_positions=0, nonterminal_orders=0, orders_submitted=0,
                         funds_positive=True, live_balance_covers_minimum=True,
                         minimum_live_balance_rupees=20000, equity_segment_enabled=True,
                         checks=dict(profile='PASS', margins='PASS', orders='PASS',
                                     positions='NETWORK_UNAVAILABLE' if app in bad else 'PASS'),
                         capabilities=dict(nse=True, mis=True, limit=True, market=True, sl=True, sl_m=True)))
    return dict(schema_version='kite_order_readiness_validation_only_v1', mode='VALIDATE_ONLY',
                state='PARTIAL_READ_ACCESS' if bad else 'READ_ACCESS_CONFIRMED',
                orders_submitted=0, order_mutations_available=False, proposed_order=None,
                strategy_controls_changed=False,
                checked_at_ist=now.isoformat(), completed_at_ist=now.isoformat(),
                minimum_live_balance_rupees=20000, apps=rows,
                market=dict(date=now.date().isoformat(), scheduled_trading_day=True))


class Clock:
    def __init__(self, start=NOW):
        self.current = start
        self.elapsed = 0
    def now(self):
        return self.current
    def sleep(self, seconds):
        self.current += timedelta(seconds=seconds)
        self.elapsed += seconds


def run_recovery(clock, probe, **kwargs):
    return recovery.recover(validate_controls=lambda: None, probe=probe, now=clock.now,
                            sleep=clock.sleep, monotonic=lambda: clock.elapsed, **kwargs)


def test_full_health_starts_before_0918():
    clock = Clock()
    route, _ = run_recovery(clock, lambda: report(clock.now()))
    assert route['mode'] == 'ALL_APPS' and len(route['apps']) == 8
    assert clock.elapsed == 0


def test_transient_failure_recovers_without_waiting_for_0918():
    clock = Clock()
    attempts = []
    def probe():
        attempts.append(1)
        return report(clock.now(), ('app2',) if len(attempts) == 1 else ())
    route, _ = run_recovery(clock, probe)
    assert clock.elapsed == 15 and route['mode'] == 'ALL_APPS'


def test_today_scenario_retries_then_uses_five_apps_at_0918():
    clock = Clock()
    states = []
    route, raw = run_recovery(clock, lambda: report(clock.now(), ('app2', 'app3', 'app8')),
                              publish=lambda **kw: states.append(kw))
    assert clock.current.strftime('%H:%M:%S') == '09:18:00'
    assert clock.elapsed == 180
    assert route['apps'] == ['app1', 'app4', 'app5', 'app6', 'app7']
    assert route['mode'] == 'HEALTHY_APPS_ONLY'
    assert raw['state'] == 'PARTIAL_READ_ACCESS' and len(raw['apps']) == 8
    assert any(s['state'] == 'RETRYING' for s in states)


def test_one_verified_app_suffices_after_boundary():
    clock = Clock(NOW + timedelta(minutes=3))
    route, _ = run_recovery(clock, lambda: report(clock.now(), tuple(f'app{i}' for i in range(2, 9))))
    assert route['apps'] == ['app1'] and clock.elapsed == 0


def test_unknown_identity_never_silently_drops_an_account():
    clock = Clock(NOW + timedelta(minutes=3))
    raw = report(clock.now(), ('app2',))
    raw['apps'][1].pop('account_fingerprint')
    with pytest.raises(ValueError, match='identity unavailable'):
        run_recovery(clock, lambda: raw)
    route, _ = run_recovery(clock, lambda: raw, identity=recovery.identity_from_report(report(clock.now())))
    assert 'app2' not in route['apps']


@pytest.mark.parametrize('field,value', [('account_fingerprint', 'b' * 64),
                                       ('credential_fingerprint', 'b' * 64)])
def test_changed_account_or_credentials_blocks(field, value):
    raw = report(NOW + timedelta(minutes=3), ('app2',))
    identity = recovery.identity_from_report(raw)
    raw['apps'][1][field] = value
    with pytest.raises(ValueError, match='changed'):
        recovery.select_route(raw, identity, NOW + timedelta(minutes=3), allow_partial=True)


@pytest.mark.parametrize('field,value', [('open_net_positions', 1), ('nonterminal_orders', 1),
                                       ('live_balance_covers_minimum', False), ('funds_positive', False)])
def test_exposure_or_funds_blocks_even_on_excluded_app(field, value):
    clock = Clock(NOW + timedelta(minutes=3))
    raw = report(clock.now(), ('app2',))
    raw['apps'][1][field] = value
    with pytest.raises(ValueError, match='exposure or insufficient'):
        run_recovery(clock, lambda: raw)


def test_all_unavailable_stops_at_deadline():
    clock = Clock()
    with pytest.raises(ValueError, match='No safe'):
        run_recovery(clock, lambda: report(clock.now(), recovery.op.EXPECTED_BROKER_APPS))
    assert clock.elapsed == 180


def test_revoked_controls_stop_retries():
    clock = Clock()
    checks = []
    def validate():
        checks.append(1)
        if len(checks) == 3:
            raise ValueError('revoked')
    with pytest.raises(ValueError, match='revoked'):
        recovery.recover(validate_controls=validate, probe=lambda: report(clock.now(), ('app2',)),
                         now=clock.now, sleep=clock.sleep, monotonic=lambda: clock.elapsed)
    assert clock.elapsed == 15


def test_stale_evidence_does_not_launch():
    clock = Clock(NOW + timedelta(minutes=3))
    with pytest.raises(ValueError, match='No safe'):
        run_recovery(clock, lambda: report(NOW))


def test_duplicate_app_coverage_rejected():
    raw = report()
    raw['apps'][1]['app'] = 'app1'
    with pytest.raises(ValueError):
        recovery.identity_from_report(raw)


def test_pool_routes_execution_and_read_calls_to_same_verified_apps(monkeypatch):
    import fno_v5_live as runtime
    credentials = [SimpleNamespace(app_name=f'app{i}', api_key=f'key{i}', access_token=f'token{i}')
                   for i in range(1, 9)]
    route = dict(session_date=datetime.now(routing.IST).date().isoformat(),
                 apps=['app4', 'app7'], account_fingerprint=routing.account_digest({'user_id': 'SAME'}),
                 credential_fingerprints={c.app_name: routing.credential_digest(c.api_key, c.access_token)
                                          for c in credentials if c.app_name in {'app4', 'app7'}})
    monkeypatch.setenv(routing.ROUTE_ENV, json.dumps(route))
    monkeypatch.setenv('FNO_V6_STRATEGY_PROFILE', 'V13_V10_G_3')
    monkeypatch.setenv('FNO_V6_EXECUTION_MODE', 'LIVE')
    created = []
    def factory(c, **kw):
        created.append(c.app_name)
        return SimpleNamespace(profile=lambda: {'user_id': 'SAME'})
    pool = runtime.KitePool(8, 3, credential_loader=lambda **kw: credentials, client_factory=factory)
    assert pool.app_names == ['app4', 'app7'] and created == ['app4', 'app7']
    # Both paths use _lanes; an excluded app cannot silently return after reload.
    assert [name for name, client in pool._lanes()] == route['apps']
    credentials[3].access_token = 'rotated'
    assert pool.refresh_if_changed()
    assert pool.app_names == route['apps']
    monkeypatch.setattr(pool, '_client_factory', lambda *a, **kw: SimpleNamespace(profile=lambda: {'user_id': 'DIFFERENT'}))
    credentials[3].access_token = 'different-account'
    with pytest.raises(ValueError, match='changed broker account'):
        pool.refresh_if_changed()


def test_stale_route_is_rejected():
    with pytest.raises(ValueError, match='stale'):
        routing.read_route({routing.ROUTE_ENV: json.dumps(dict(session_date='2020-01-01', apps=['app1']))})


def test_dashboard_shows_today_failure_instead_of_yesterdays_running():
    previous = dict(status='RUNNING', ts='2026-11-05_15:00:00')
    launch = dict(session_date='2026-11-06', state='BLOCKED', reason='No healthy route', updated_at_ist=NOW.isoformat())
    result = routing.launch_dashboard_status(previous, launch, now=NOW)
    assert result['status'] == 'LAUNCH_BLOCKED' and result['reason'] == 'No healthy route'


def test_dashboard_running_degraded_and_stale_launch():
    launch = dict(session_date='2026-11-06', state='STARTING', mode='HEALTHY_APPS_ONLY',
                  healthy_apps=['app1'], excluded_apps=['app2'], updated_at_ist=NOW.isoformat())
    running = routing.launch_dashboard_status(dict(status='RUNNING', ts=NOW.isoformat()), launch, now=NOW)
    assert running['launch_state'] == 'RUNNING' and running['healthy_broker_apps'] == 'app1'
    stale = routing.launch_dashboard_status({}, launch, now=NOW + timedelta(minutes=2))
    assert stale['status'] == 'LAUNCH_STATUS_STALE'


def test_recovery_only_enabled_from_tomorrow():
    assert not recovery.enabled(NOW - timedelta(days=1))
    assert recovery.enabled(NOW)


@pytest.mark.skipif(os.name != 'nt', reason='Windows exclusive launch lock')
def test_duplicate_launch_lock_cannot_be_acquired(tmp_path):
    from tools.fno_data_supervisor import SingletonLock
    path = tmp_path / 'launch.lock'
    with SingletonLock(path):
        with pytest.raises(OSError):
            with SingletonLock(path):
                pytest.fail('Duplicate launch lock acquired')


def test_runner_binds_selected_route_to_child_environment_and_keeps_ack_local(tmp_path, monkeypatch):
    from contextlib import nullcontext
    from tools import fno_data_supervisor
    state, arm = {'approval': 'test'}, {'enabled': True}
    route = dict(apps=['app4'], excluded_apps=['app1'], mode='HEALTHY_APPS_ONLY')
    monkeypatch.setattr(recovery.op, 'RUNTIME_ROOT', tmp_path)
    monkeypatch.setattr(recovery.op, 'read', lambda p: state if p == recovery.op.PROFILE else arm)
    monkeypatch.setattr(recovery.op, 'validate_launch', lambda *a: None)
    monkeypatch.setattr(fno_data_supervisor, 'SingletonLock', lambda p: nullcontext())
    monkeypatch.setattr(recovery, 'recover', lambda **kw: (route, report()))
    monkeypatch.delenv(routing.ROUTE_ENV, raising=False)
    monkeypatch.delenv(recovery.op.config.LIVE_ACK_ENV, raising=False)
    calls = []
    monkeypatch.setattr(recovery.subprocess, 'call', lambda *a, **kw: calls.append(kw) or 0)
    assert recovery.run_approved(state, arm) == 0
    assert json.loads(calls[0]['env'][routing.ROUTE_ENV]) == route
    assert recovery.op.config.LIVE_ACK_ENV in calls[0]['env']
    assert routing.ROUTE_ENV not in os.environ and recovery.op.config.LIVE_ACK_ENV not in os.environ
    status = json.loads((tmp_path / 'runtime_status' / recovery.STATUS_NAME).read_text())
    assert status['state'] == 'STOPPED'


def test_operator_dispatches_to_recovery_when_enabled(monkeypatch):
    op = recovery.op
    state, arm = {'test': 1}, {'enabled': True}
    monkeypatch.setattr(op, 'read', lambda p: state if p == op.PROFILE else arm)
    monkeypatch.setattr(recovery, 'enabled', lambda *a: True)
    calls = []
    monkeypatch.setattr(recovery, 'run_approved', lambda s, a: calls.append((s, a)) or 0)
    assert op.main(['run-approved']) == 0 and calls == [(state, arm)]


def test_partial_preopen_evidence_can_approve_but_cannot_launch_early():
    preopen = NOW.replace(hour=9, minute=10)
    raw = report(preopen, ('app2',))
    route = recovery.validate_preopen_broker(raw, preopen)
    assert len(route['apps']) == 7
    identity = recovery.identity_from_report(raw)
    at_launch = report(NOW, ('app2',))
    assert recovery.select_route(at_launch, identity, NOW, allow_partial=False) is None
    at_0918 = NOW + timedelta(minutes=3)
    assert len(recovery.select_route(report(at_0918, ('app2',)), identity, at_0918,
                                     allow_partial=True)['apps']) == 7
