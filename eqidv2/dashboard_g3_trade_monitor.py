"""Read-only dashboard adapters for G3 monitoring and its separate research."""
from __future__ import annotations

from datetime import date, datetime
import json
import math
from pathlib import Path
import re
from zoneinfo import ZoneInfo

from eqidv2_runtime_paths import runtime_dir

IST = ZoneInfo('Asia/Kolkata')
MONITOR_ID = 'g3_trade_monitor'
BACKTEST_ID = 'g3_trade_monitor_backtest'
ARTIFACTS = frozenset({'report.html', 'summary.json', 'config.json', 'policy_results.csv',
    'daily_comparison.csv', 'trade_comparison.csv', 'signals.csv', 'monthly_results.csv',
    'selected_orders.csv', 'manifest.json', 'ledgers/baseline.csv', 'ledgers/trend_failure.csv',
    'ledgers/profit_giveback.csv', 'ledgers/combined.csv'})
SNAPSHOT_KEYS = frozenset({'schema_version', 'session_date', 'generated_at_ist', 'updated_at_ist',
    'state', 'historical', 'read_only', 'advisory_only', 'execution_authority', 'refresh_seconds',
    'price_granularity', 'strategy_version', 'strategy_fingerprint', 'summary', 'modes', 'trades',
    'pending', 'warnings', 'snapshot_age_seconds', 'snapshot_stale', 'reason',
    'rules_version', 'monitor_config', 'market_data_feed', 'identity'})
TRADE_KEYS = frozenset({'id', 'signal_id', 'symbol', 'mode', 'side', 'status', 'setup_id',
    'entry_price', 'entry_at_ist', 'quantity', 'stop_price', 'target_price', 'exit_price',
    'exit_at_ist', 'recorded_net_pnl_rs', 'evidence_state', 'partial_fill', 'conflict',
    'mark_price', 'mark_timestamp', 'mark_source', 'mark_granularity', 'mark_state',
    'mark_age_seconds', 'direction', 'metrics', 'reasons', 'advisory_flags', 'freshness'})


def session_date(value=None, *, now=None):
    now = now or datetime.now(IST)
    today = now.astimezone(IST).date()
    if value is None:
        return today
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise ValueError('Use one date in YYYY-MM-DD format, no later than today')
    parsed = date.fromisoformat(value)
    if parsed > today:
        raise ValueError('Future monitoring sessions are unavailable')
    return parsed


def _clean(value):
    """Do not send local paths, non-finite numbers or internal source records."""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: _clean(item) for key, item in value.items()
                if not any(part in str(key).lower() for part in ('path', 'token', 'secret', 'password'))
                and not re.search(r'(?:[A-Za-z]:[\\/]|\\\\[^\\\s]+\\)', str(key))}
    if isinstance(value, (list, tuple)):
        return [_clean(item) for item in value]
    if isinstance(value, str):
        return '[local artifact]' if re.search(r'(?:[A-Za-z]:[\\/]|\\\\[^\\\s]+\\)', value) else value
    return value


def load_snapshot(day=None, *, now=None):
    import g3_trade_monitor as monitor
    now = now or datetime.now(IST)
    selected = session_date(day, now=now)
    if selected == now.astimezone(IST).date():
        value = monitor.load_latest(day=selected, now=now)
    else:
        value = monitor.build_snapshot(selected, now=now)
    if (not isinstance(value, dict) or value.get('execution_authority') is not False
            or value.get('read_only') is not True or value.get('advisory_only') is not True
            or value.get('session_date') != selected.isoformat()):
        raise ValueError('Monitoring snapshot authority or session identity is invalid')
    result = {key: value[key] for key in SNAPSHOT_KEYS if key in value}
    result['trades'] = [{key: row[key] for key in TRADE_KEYS if key in row}
                        for row in value.get('trades', []) if isinstance(row, dict)]
    result['pending'] = [{key: row[key] for key in ('id', 'signal_id', 'symbol', 'mode', 'status', 'reason') if key in row}
                         for row in value.get('pending', []) if isinstance(row, dict)]
    return _clean(result)


def _backtest():
    from research.g3_trade_monitor_backtest import load_latest
    return load_latest()


def load_backtest_payload():
    try:
        value = _backtest()
    except FileNotFoundError:
        return dict(state='NO_VERIFIED_REPORT', read_only=True, execution_authority=False,
                    price_granularity='COMPLETED_1M', reason='No verified monitor backtest has been published.')
    if not isinstance(value, dict) or not isinstance(value.get('summary'), dict):
        return dict(state='NO_VERIFIED_REPORT', read_only=True, execution_authority=False,
                    price_granularity='COMPLETED_1M', reason='No verified monitor backtest has been published.')
    return _clean(dict(state='VERIFIED', read_only=True, execution_authority=False,
        price_granularity='COMPLETED_1M', summary=value['summary'],
        policy_results=value.get('policy_results', []), daily_comparison=value.get('daily_comparison', []),
        report_url='/g3-trade-monitor-backtest/report.html'))


def backtest_artifact(name):
    if name not in ARTIFACTS:
        raise ValueError('Unknown monitor research artifact')
    value = _backtest()
    directory = Path(value['run_dir']).resolve()
    root = runtime_dir('v13_v10_g_3_trade_monitor_backtest').resolve()
    path = (directory / name).resolve()
    if not directory.is_relative_to(root) or not path.is_relative_to(directory):
        raise ValueError('Monitor report escaped its publication root')
    manifest = value.get('manifest', {})
    if name != 'manifest.json' and name not in manifest.get('files', {}):
        raise ValueError('Monitor report artifact is not in its verified manifest')
    body = path.read_bytes()
    if name.endswith('.json'):
        body = (json.dumps(_clean(json.loads(body)), ensure_ascii=False, allow_nan=False, indent=2) + '\n').encode('utf-8')
    return body, ('text/html; charset=utf-8' if name.endswith('.html') else
        'application/json; charset=utf-8' if name.endswith('.json') else 'text/csv; charset=utf-8')


def session_items():
    """Always expose both cards; absent evidence is a visible waiting state."""
    try:
        snapshot = load_snapshot()
        count = snapshot.get('summary', {})
        state = 'WAITING' if snapshot.get('state') == 'NO_SNAPSHOT' else 'STALE_OUTPUT' if snapshot.get('snapshot_stale') else (
            'SUCCESS' if snapshot.get('state') in {'READY', 'NO_EXECUTED_TRADES'} else 'WAITING')
        note = (f"Session: {snapshot['session_date']} | Open: {count.get('open', 'unavailable')} | "
                f"Closed: {count.get('closed', 'unavailable')} | Pending signals: {count.get('pending', 'unavailable')}")
        stamp = snapshot.get('generated_at_ist')
    except Exception:
        state, note, stamp = 'WAITING', 'Waiting for a valid read-only monitoring snapshot.', None
    research_published = (runtime_dir('v13_v10_g_3_trade_monitor_backtest') / 'latest/current_run.json').is_file()
    return [dict(id=MONITOR_ID, title='G3 Trade Monitor ·15s', file_name='v13_v10_g_3_trade_monitor/latest/latest.json',
        exists=True, mtime=stamp, size_bytes=0, card_kind='view',
        status=dict(status=state, phase='ADVISORY_ONLY', view_scope='ARTIFACT', reporting_only='true',
            derived_status='Available prices and recorded PAPER/LIVE fills; cannot place orders'),
        tail='# G3 Trade Monitor ·15s\n\n' + note + '\n\nRead-only observations; polling does not create 15-second market data.'),
        dict(id=BACKTEST_ID, title='G3 Monitor Backtest ·1m',
            file_name='v13_v10_g_3_trade_monitor_backtest/latest/current_run.json', exists=True,
            mtime=None, size_bytes=0, card_kind='view', status=dict(status='READY' if research_published else 'WAITING', phase='RESEARCH_ONLY',
                view_scope='ARTIFACT', reporting_only='true', derived_status='Separate verified monitor-exit research; actual one-minute resolution'),
            tail='# G3 Monitor Backtest ·1m\n\nOpen the monitor panel for verified fixed-policy comparisons.\n\nHistorical minute replay; not a 15-second backtest or an execution signal.')]
