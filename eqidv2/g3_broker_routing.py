"""Session-scoped G3 routing. No broker mutations or credentials on disk here."""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import os
from zoneinfo import ZoneInfo

ROUTE_ENV = 'G3_VERIFIED_BROKER_ROUTE'
IST = ZoneInfo('Asia/Kolkata')


def account_digest(profile):
    user = profile.get('user_id')
    if not isinstance(user, str) or not user.strip():
        raise ValueError('Broker account identity is missing')
    return hashlib.sha256(('ZERODHA:' + user.strip().upper()).encode()).hexdigest()


def credential_digest(key, token):
    return hashlib.sha256((str(key) + '\0' + str(token)).encode()).hexdigest()


def read_route(environ=None, now=None):
    environ = os.environ if environ is None else environ
    raw = environ.get(ROUTE_ENV, '')
    if not raw:
        return None
    route = json.loads(raw)
    now = now or datetime.now(IST)
    apps = route.get('apps', [])
    if (route.get('session_date') != now.astimezone(IST).date().isoformat()
            or not isinstance(apps, list) or not apps or len(set(apps)) != len(apps)
            or not set(apps) <= {f'app{i}' for i in range(1, 9)}
            or len(str(route.get('account_fingerprint', ''))) != 64
            or set(route.get('credential_fingerprints', {})) != set(apps)
            or any(len(str(v)) != 64 for v in route['credential_fingerprints'].values())):
        raise ValueError('Invalid or stale verified G3 broker route')
    return route


def routed_credentials(credentials, route):
    if route is None:
        return credentials
    by_name = {str(c.app_name): c for c in credentials}
    if not set(route['apps']) <= set(by_name):
        raise ValueError('A verified G3 app credential is unavailable')
    return [by_name[name] for name in route['apps']]


def verify_clients(credentials, clients, route):
    """Token rotation is allowed only after rechecking the account on that app."""
    if route is None:
        return
    for credential, client in zip(credentials, clients):
        actual = credential_digest(credential.api_key, credential.access_token)
        if actual != route['credential_fingerprints'][credential.app_name]:
            if account_digest(client.profile()) != route['account_fingerprint']:
                raise ValueError('G3 credential rotation changed broker account')


def launch_dashboard_status(status, launch, *, now=None):
    """Current-session launch evidence cannot be replaced by yesterday's log."""
    now = now or datetime.now(IST)
    result = dict(status)
    if not launch or launch.get('session_date') != now.date().isoformat():
        return result
    result.update(launch_state=launch.get('state', ''),
                  launch_updated_at_ist=launch.get('updated_at_ist', ''),
                  broker_route_mode=launch.get('mode', ''),
                  healthy_broker_apps=','.join(launch.get('healthy_apps', [])),
                  excluded_broker_apps=','.join(launch.get('excluded_apps', [])),
                  launch_next_retry_at_ist=launch.get('next_retry_at_ist', ''),
                  launch_failures=json.dumps(launch.get('failures', {}), sort_keys=True))
    stamps = []
    for key in ('ts_utc', 'updated_utc', 'heartbeat_ts', 'ts'):
        try:
            stamp = datetime.fromisoformat(str(status.get(key, '')).replace('_', 'T'))
            stamp = stamp.replace(tzinfo=IST) if stamp.tzinfo is None else stamp
            stamps.append(stamp)
        except ValueError:
            pass
    current_worker = (stamps and 0 <= (now - max(stamps)).total_seconds() <= 90
                      and str(status.get('status', '')).upper() in {'RUNNING', 'RESTARTING', 'COOLDOWN'})
    if current_worker:
        result['launch_state'] = 'RUNNING'
    else:
        state = launch.get('state', 'UNKNOWN')
        try:
            age = (now - datetime.fromisoformat(launch['updated_at_ist'])).total_seconds()
        except (KeyError, ValueError, TypeError):
            age = float('inf')
        result['status'] = ('LAUNCH_STATUS_STALE' if state in
                            {'CHECKING', 'RETRYING', 'WAITING_FOR_0918', 'STARTING', 'READY'}
                            and age > 90 else 'LAUNCH_' + state)
        result['reason'] = launch.get('reason') or result['status']
        result['ts'] = launch.get('updated_at_ist', '')
    result['note'] = (str(result.get('note', '')) + ' | launch=' + result['launch_state']
                      + ' | route=' + result['broker_route_mode']
                      + ' | healthy=' + result['healthy_broker_apps']
                      + ' | excluded=' + result['excluded_broker_apps']
                      + ' | next_retry=' + result['launch_next_retry_at_ist']
                      + ' | failures=' + result['launch_failures'])
    return result
