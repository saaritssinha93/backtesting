"""One-date-only PAPER session; this module never enables real order placement."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import date, datetime, time as clock_time
import json
import logging
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import uuid
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / 'outputs/muhurat_20261108'
SESSION_DATE = date(2026, 11, 8)
IST = ZoneInfo('Asia/Kolkata')
SCHEDULE = dict(session_open='14:20', first_signal='14:25', last_signal='15:05',
    first_confirmation='14:26', last_confirmation='15:06', paper_exit='15:10',
    cash_collection_end='15:15', futures_collection_end='15:40', finalization_end='15:45',
    expires='2026-11-09T00:00:00+05:30', timezone='Asia/Kolkata')


def local_now():
    return datetime.now(IST)


def phase(now):
    now = now.astimezone(IST)
    if now.date() != SESSION_DATE:
        return 'EXPIRED'
    value = now.time().replace(tzinfo=None)
    if value < clock_time(14, 20):
        return 'WAITING_FOR_SESSION'
    if value < clock_time(15, 10):
        return 'PAPER_RUNNING'
    if value < clock_time(15, 45):
        return 'FINALIZING'
    return 'COMPLETE'


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + '\n', encoding='utf-8')
    os.replace(temporary, path)


def read_json(path, fallback):
    return json.loads(path.read_text(encoding='utf-8')) if path.exists() else fallback


def write_frame(path, frame):
    if not isinstance(frame, pd.DataFrame):
        frame = pd.DataFrame(frame)
    temporary = path.with_suffix(path.suffix + '.tmp')
    frame.to_csv(temporary, index=False)
    os.replace(temporary, path)


@contextmanager
def singleton(path):
    """Hold a local process lock; duplicate scheduled launches do no work."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open('a+b')
    handle.seek(0, 2)
    if not handle.tell():
        handle.write(b'0'); handle.flush()
    handle.seek(0)
    acquired = False
    try:
        if os.name == 'nt':
            import msvcrt
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                acquired = True
            except OSError:
                pass
        else:
            import fcntl
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except OSError:
                pass
        yield acquired
    finally:
        if acquired and os.name == 'nt':
            import msvcrt
            handle.seek(0); msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        handle.close()


def ensure_dashboard():
    try:
        with socket.create_connection(('127.0.0.1', 8790), timeout=1):
            return
    except OSError:
        pass
    stream = (OUTPUT / 'dashboard.log').open('a', encoding='utf-8')
    process = subprocess.Popen([sys.executable, '-m', 'muhurat_20261108.dashboard', '--port', '8790', '--no-browser'],
        cwd=ROOT, stdout=stream, stderr=stream,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
    stream.close()
    write_json(OUTPUT / 'dashboard_process.json', dict(pid=process.pid, started_at=local_now().isoformat(), port=8790))


def prepare():
    from .engine import build_plan
    OUTPUT.mkdir(parents=True, exist_ok=True)
    plan = build_plan()
    plan.update(session_date=SESSION_DATE.isoformat(), scope='PAPER_AND_BACKTEST_ONLY', schedule=SCHEDULE,
        live_order_placement_enabled=False, normal_weekday_configuration_changed=False)
    write_json(OUTPUT / 'plan.json', plan)
    if not (OUTPUT / 'slots.csv').exists():
        slots = plan.get('slots')
        if not isinstance(slots, list):
            slots = [dict(signal_end=value,
                confirmation_end=(pd.Timestamp(f'{SESSION_DATE} {value}') + pd.Timedelta(minutes=1)).strftime('%H:%M'),
                status='PENDING', healthy_symbols=0, selected_count=0) for value in plan.get('signal_clocks', [])]
        write_frame(OUTPUT / 'slots.csv', slots)
    return plan


def publish_result(result, mode):
    if mode == 'paper':
        # Commit first-seen/decision state with the ledger, before the next scan.
        write_json(OUTPUT / 'paper_state.json', result['state'])
        write_frame(OUTPUT / 'paper_trades.csv', result['trades'])
        write_frame(OUTPUT / 'scan_audit.csv', result['audit'])
        write_frame(OUTPUT / 'candidates.csv', result['candidates'])
        if 'slots' in result:
            write_frame(OUTPUT / 'slots.csv', result['slots'])
    else:
        write_frame(OUTPUT / 'daily_backtest_trades.csv', result['trades'])
        write_frame(OUTPUT / 'daily_backtest_audit.csv', result['audit'])
        write_json(OUTPUT / 'daily_backtest_summary.json', result['summary'])
        summary = result['summary']
        lines = ['# G3 Muhurat daily backtest - 8 November 2026', '',
            'Isolated shortened-session replay using Friday rules and the recorded donor additions. '
            'This is separate from prospective paper fills; delayed data may change paper eligibility.', '',
            f'Status: {"COMPLETE" if summary.get("complete") else "PARTIAL / MISSING DATA"}. '
            f'As of: {summary.get("asof", "unknown")}.', '',
            f'Selected orders: {summary.get("selected_orders", 0)}; filled trades: {summary.get("filled_trades", 0)}; '
            f'closed trades: {summary.get("closed_trades", 0)}; net P&L: Rs{summary.get("net_pnl_rs", 0):,.2f}.', '',
            '| Stock | Side | Slot | Status | Entry time | Exit time | Net P&L (Rs) |',
            '|:--|:--|:--|:--|:--|:--|--:|']
        trades = result['trades']
        records = trades.to_dict('records') if isinstance(trades, pd.DataFrame) else trades
        for row in records:
            clean = lambda key: str(row.get(key, '')).replace('|', '/')
            lines.append(f'| {clean("tradingsymbol")} | {clean("side")} | {clean("actual_setup_id")} | '
                f'{clean("status")} | {clean("entry_ts")} | {clean("exit_ts")} | {float(row.get("net_pnl_rs",0)):,.2f} |')
        lines += ['', 'No LIVE orders were placed by this setup. Missing data is not treated as a zero-loss completed session.', '']
        path = OUTPUT / 'DAILY_BACKTEST.md'
        temporary = path.with_suffix('.md.tmp')
        temporary.write_text('\n'.join(lines), encoding='utf-8')
        os.replace(temporary, path)
    return result['summary']


def run(watch=False):
    if local_now().date() != SESSION_DATE:
        print('Expired: this runner is restricted to 8 November 2026 IST. No session started.')
        return 0
    OUTPUT.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(filename=OUTPUT / 'runner.log', level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s', encoding='utf-8')
    from .data import Collector
    from .engine import evaluate
    with singleton(OUTPUT / 'runner.lock') as acquired:
        if not acquired:
            print('A Muhurat paper runner already owns the session lock.')
            return 0
        prepare()
        if watch:
            ensure_dashboard()
        collector = Collector(OUTPUT / 'data')
        auth, health_at, data_status, paper, backtest = {}, 0., {}, {}, {}
        while True:
            now = local_now()
            current_phase = phase(now)
            if current_phase == 'EXPIRED':
                return 0
            errors = []
            try:
                if time.monotonic() - health_at >= 300 or not auth:
                    auth = collector.healthcheck()
                    health_at = time.monotonic()
                if current_phase != 'WAITING_FOR_SESSION':
                    data_status = collector.collect(now)
                    if collector.last_health is not None:
                        auth = collector.last_health.copy()
                    frames = collector.load_frames()
                    frames['source_completed_through'] = (pd.Timestamp(now) - pd.Timedelta(seconds=3)).floor('min')
                    # A candle becomes actionable only after the fetch and read
                    # have finished, not at the earlier request-start timestamp.
                    decision_time = local_now()
                    result = evaluate(frames, decision_time, mode='paper', state=read_json(OUTPUT / 'paper_state.json', None))
                    paper = publish_result(result, 'paper')
                    if current_phase in {'FINALIZING', 'COMPLETE'}:
                        backtest = publish_result(evaluate(frames, decision_time, mode='replay'), 'replay')
            except Exception as exc:
                # Exception classes are enough for operational status; never expose API responses or credentials.
                errors.append(type(exc).__name__)
                import traceback
                location = traceback.extract_tb(exc.__traceback__)[-1]
                logging.error('Cycle failed with %s at %s:%s', type(exc).__name__, Path(location.filename).name, location.lineno)
            healthy = int(data_status.get('healthy_apps', auth.get('healthy_apps', auth.get('healthy_count', 0))))
            state = current_phase
            if healthy == 0 and current_phase != 'COMPLETE':
                state = 'BLOCKED_AUTH'
            if errors:
                state = 'PARTIAL'
            if current_phase == 'COMPLETE' and not backtest.get('complete', False):
                state = 'PARTIAL'
            elif current_phase == 'COMPLETE' and backtest.get('source_partial', False):
                state = 'COMPLETE_WITH_EXCLUSIONS'
            summary = dict(session_date=SESSION_DATE.isoformat(), paper=paper, backtest=backtest,
                same_configuration=True, paper_and_replay_timing_may_differ=True)
            write_json(OUTPUT / 'summary.json', summary)
            status = dict(session_date=SESSION_DATE.isoformat(), scope='PAPER_AND_BACKTEST_ONLY', state=state,
                phase=current_phase, updated_at=local_now().isoformat(), pid=os.getpid(), healthy_apps=healthy,
                total_apps=auth.get('total_apps', 8), auth=auth, data=data_status, paper=paper, backtest=backtest,
                live=dict(enabled=False, reason='Live order placement was not enabled by this setup.'),
                schedule=SCHEDULE, errors=errors, normal_weekday_configuration_changed=False)
            write_json(OUTPUT / 'status.json', status)
            if not watch or current_phase == 'COMPLETE':
                print(json.dumps(dict(state=state, phase=current_phase, healthy_apps=healthy, errors=errors)))
                return 1 if errors else 0
            time.sleep(30 if current_phase == 'WAITING_FOR_SESSION' else 15)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--prepare', action='store_true', help='Freeze the dated plan without fetching or starting services')
    group.add_argument('--watch', action='store_true', help='Watch only today through15:45; PAPER/data/backtest only')
    args = parser.parse_args()
    if args.prepare:
        if local_now().date() != SESSION_DATE:
            parser.error('Preparation is restricted to8November2026 IST')
        prepare()
        print('Prepared isolated Muhurat paper/backtest plan. No live configuration changed.')
        return 0
    return run(watch=args.watch)


if __name__ == '__main__':
    raise SystemExit(main())
