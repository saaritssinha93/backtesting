"""Open a separate loopback-only Dashboard Flow session without restarting 8787."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import secrets
import sys
from http import HTTPStatus
from http.server import ThreadingHTTPServer
from urllib.parse import urlencode
import webbrowser


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from log_dashboard_server import LogDashboardHandler, _normalize_cli_auth_value


class FlowPreviewHandler(LogDashboardHandler):
    """Reuse dashboard authentication and rendering, with no process controls."""

    def do_POST(self) -> None:
        self.send_error(HTTPStatus.METHOD_NOT_ALLOWED, "This Dashboard Flow session is read-only.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Open Dashboard Flow on a separate local port")
    parser.add_argument("--port", type=int, default=8790, help="Loopback port (default: 8790)")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("Use a port from 1 to 65535.")
    token = _normalize_cli_auth_value(os.environ.get("LOG_DASH_TOKEN", "")) or secrets.token_urlsafe(32)
    try:
        server = ThreadingHTTPServer(("127.0.0.1", args.port), FlowPreviewHandler)
    except OSError:
        print(f"Cannot open local port {args.port}; it may already be in use.")
        print("Choose another port with --port. No existing service was restarted.")
        return 1
    server.username = _normalize_cli_auth_value(os.environ.get("LOG_DASH_USER", ""))
    server.password = _normalize_cli_auth_value(os.environ.get("LOG_DASH_PASS", ""))
    server.api_token = token
    url = f"http://127.0.0.1:{args.port}/dashboard-flow?" + urlencode({"token": token})
    print(f"Dashboard Flow is running on local port {args.port}.")
    print("Opening your browser. Keep this window open; press Ctrl+C to stop this session.")
    try:
        if not webbrowser.open(url, new=2):
            print("The browser did not open automatically. Set a default browser and run this launcher again.")
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\nDashboard Flow stopped.")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
