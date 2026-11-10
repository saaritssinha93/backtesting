"""Read-only, date-bound Muhurat paper/replay dashboard. Import never starts workers."""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime
from http import HTTPStatus
from http.server import ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import secrets
import threading
from urllib.parse import parse_qs, urlencode, urlparse
import webbrowser
from zoneinfo import ZoneInfo

from tools.run_dashboard_flow import FlowPreviewHandler

BASE = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = BASE / 'outputs/muhurat_20261108'
HTML = Path(__file__).with_name('dashboard.html')
SESSION_DATE = date(2026, 11, 8)
IST = ZoneInfo('Asia/Kolkata')
FILES = ('status.json', 'plan.json', 'paper_trades.csv', 'daily_backtest_trades.csv', 'slots.csv', 'summary.json')
MAX_BYTES = 8 * 1024 * 1024
MAX_ROWS = 5000
SECRET_KEYS = {'api_key', 'api_secret', 'access_token', 'refresh_token', 'password', 'token', 'authorization'}


def local_now():
    return datetime.now(IST)


def _expiry_watch(server, stop):
    """Release the port at the end of the one permitted IST date."""
    while not stop.wait(15):
        if local_now().date() != SESSION_DATE:
            server.shutdown()
            return


def _scrub(value):
    if isinstance(value, dict):
        return {str(k): _scrub(v) for k, v in value.items() if str(k).lower() not in SECRET_KEYS}
    if isinstance(value, list):
        return [_scrub(v) for v in value]
    return value


def _verify_session(record):
    # A copied Friday report must never appear as today's Muhurat evidence.
    for key in ('session_date', 'day'):
        value = record.get(key)
        if value not in (None, '') and str(value) != SESSION_DATE.isoformat():
            raise ValueError('Report belongs to another session')


def read_artifact(root: Path, name: str):
    """Read only the six explicit reports; reject traversal, symlinks and oversized files."""
    if name not in FILES:
        raise ValueError('Unknown Muhurat artifact')
    root = Path(root).resolve()
    path = root / name
    if path.is_symlink() or not path.resolve().is_relative_to(root):
        raise ValueError('Artifact must stay within the Muhurat report directory')
    if not path.is_file():
        return None
    if path.stat().st_size > MAX_BYTES:
        raise ValueError('Artifact exceeds display limit')
    raw = path.read_bytes()
    if len(raw) > MAX_BYTES:
        raise ValueError('Artifact exceeds display limit')
    text = raw.decode('utf-8-sig')
    if name.endswith('.json'):
        result = json.loads(text)
        if not isinstance(result, dict):
            raise ValueError('Expected a report object')
        _verify_session(result)
        return _scrub(result)
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for row in reader:
        if len(rows) >= MAX_ROWS:
            raise ValueError('Artifact exceeds row display limit')
        _verify_session(row)
        rows.append(_scrub(row))
    return rows


def build_payload(root=DEFAULT_ROOT, *, now=None):
    now = local_now() if now is None else now.astimezone(IST)
    artifacts, errors, present = {}, [], []
    for name in FILES:
        try:
            value = read_artifact(Path(root), name)
            if value is not None:
                present.append(name)
            artifacts[name.removesuffix('.json').removesuffix('.csv')] = value
        except (OSError, ValueError, UnicodeError, csv.Error):
            errors.append(name + ': unavailable or invalid; refresh to retry')
            artifacts[name.removesuffix('.json').removesuffix('.csv')] = None
    return dict(schema_version='muhurat_readonly_dashboard_v1', session_date=SESSION_DATE.isoformat(),
        generated_at=now.isoformat(timespec='seconds'), expires_at='2026-11-09T00:00:00+05:30',
        date_status='EXPIRED' if now.date() > SESSION_DATE else 'TODAY_ONLY' if now.date() == SESSION_DATE else 'NOT_TODAY',
        execution_authority=False, live_enabled_by_this_setup=False,
        live_message='Not enabled by this setup', files_present=present, read_errors=errors, **artifacts)


class MuhuratHandler(FlowPreviewHandler):
    """Add isolated reporting routes; inherited standard routes stay read-only."""
    def do_POST(self):
        self.send_error(HTTPStatus.FORBIDDEN, 'Read-only dashboard; no execution or process controls.')

    def do_PUT(self):
        self.do_POST()

    def do_PATCH(self):
        self.do_POST()

    def do_DELETE(self):
        self.do_POST()

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        special = parsed.path in ('/', '/standard', '/muhurat', '/muhurat/', '/api/muhurat') or parsed.path.startswith('/muhurat-artifact/')
        if not special:
            return super().do_GET()
        if not self._authorized(params):
            return self._unauthorized()
        if parsed.path == '/':
            token = (params.get('token') or [''])[0]
            target = '/muhurat' + ('?' + urlencode({'token': token}) if token else '')
            self.send_response(HTTPStatus.FOUND)
            self.send_header('Location', target)
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            return
        if parsed.path == '/standard':
            return self._send_html()
        if parsed.path == '/api/muhurat':
            return self._send_json(build_payload(self.server.muhurat_root))
        if parsed.path.startswith('/muhurat-artifact/'):
            name = parsed.path.removeprefix('/muhurat-artifact/')
            try:
                value = read_artifact(self.server.muhurat_root, name)
            except (OSError, ValueError, UnicodeError, csv.Error):
                return self.send_error(HTTPStatus.NOT_FOUND, 'Unknown or unavailable report')
            if value is None:
                return self.send_error(HTTPStatus.NOT_FOUND, 'Report not yet published')
            # Only sanitized parsed values are exposed, never arbitrary raw files.
            return self._send_json({'artifact': name, 'data': value})
        body = HTML.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.send_header('Content-Security-Policy', "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'")
        self.end_headers()
        self.wfile.write(body)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8790)
    parser.add_argument('--no-browser', action='store_true')
    parser.add_argument('--output-root', type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error('Port must be between1 and65535')
    if local_now().date() != SESSION_DATE:
        print('Expired: the automatic Muhurat dashboard runs only on8November2026 IST.')
        return 0
    server = ThreadingHTTPServer(('127.0.0.1', args.port), MuhuratHandler)
    server.username = os.environ.get('LOG_DASH_USER', '')
    server.password = os.environ.get('LOG_DASH_PASS', '')
    server.api_token = os.environ.get('LOG_DASH_TOKEN', '') or secrets.token_urlsafe(32)
    server.muhurat_root = args.output_root.resolve()
    url = f'http://127.0.0.1:{args.port}/muhurat?' + urlencode({'token': server.api_token})
    # This local access record is deliberately outside FILES and never served by the API.
    server.muhurat_root.mkdir(parents=True, exist_ok=True)
    access = server.muhurat_root / 'dashboard_access.json'
    access.write_text(json.dumps({'url': url, 'pid': os.getpid(), 'port': args.port,
        'read_only': True, 'session_date': SESSION_DATE.isoformat()}, indent=2) + '\n', encoding='utf-8')
    print(f'Read-only Muhurat dashboard on127.0.0.1:{args.port}; private access URL saved locally.', flush=True)
    stop = threading.Event()
    expiry = threading.Thread(target=_expiry_watch, args=(server, stop), daemon=True, name='muhurat-dashboard-expiry')
    expiry.start()
    try:
        if not args.no_browser:
            webbrowser.open(url, new=2)
        server.serve_forever(poll_interval=.5)
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.server_close()
        expiry.join(timeout=2)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
