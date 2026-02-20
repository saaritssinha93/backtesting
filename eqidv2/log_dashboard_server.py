#!/usr/bin/env python3
"""Simple secured log dashboard for EQIDV2 scheduled jobs."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import parse_qs, urlparse

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"

LOG_FILES: Dict[str, str] = {
    "authentication": "authentication_runner.log",
    "eod_15min_data": "eqidv2_eod_scheduler_for_15mins_data.log",
    "eod_1540_update": "eqidv2_eod_scheduler_for_1540_update.log",
    "live_combined_csv": "eqidv2_live_combined_analyser_csv.log",
    "paper_trade": "avwap_trade_execution_PAPER_TRADE_TRUE.log",
}

STATUS_FILES: Dict[str, str] = {
    "authentication": "authentication_runner.status",
    "live_combined_csv": "eqidv2_live_combined_analyser_csv.status",
}


def parse_status_file(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.exists():
        return out
    try:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    except OSError:
        return {}
    return out


def tail_text(path: Path, lines: int = 80, max_bytes: int = 120_000) -> str:
    if not path.exists():
        return ""
    try:
        size = path.stat().st_size
        with path.open("rb") as f:
            if size > max_bytes:
                f.seek(size - max_bytes)
            chunk = f.read()
        text = chunk.decode("utf-8", errors="replace")
        return "\n".join(text.splitlines()[-lines:])
    except OSError as exc:
        return f"[ERROR reading log: {exc}]"


def iso_mtime(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    try:
        return dt.datetime.fromtimestamp(path.stat().st_mtime).isoformat(sep=" ", timespec="seconds")
    except OSError:
        return None


class LogDashboardHandler(BaseHTTPRequestHandler):
    server_version = "EQIDV2LogDashboard/1.0"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if not self._authorized(params):
            self._unauthorized()
            return

        if parsed.path == "/":
            self._send_html()
            return
        if parsed.path == "/api/snapshot":
            lines = self._int_param(params, "lines", 80, 20, 400)
            payload = self._snapshot(lines=lines)
            self._send_json(payload)
            return
        if parsed.path == "/api/log":
            name = (params.get("name") or [""])[0]
            if name not in LOG_FILES:
                self._send_json({"error": "unknown log name"}, status=HTTPStatus.BAD_REQUEST)
                return
            lines = self._int_param(params, "lines", 150, 20, 1000)
            file_path = LOG_DIR / LOG_FILES[name]
            body = tail_text(file_path, lines=lines)
            self._send_text(body)
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not Found")

    def log_message(self, fmt: str, *args) -> None:
        # Keep stdout useful but concise.
        super().log_message(fmt, *args)

    def _authorized(self, params: Dict[str, list[str]]) -> bool:
        api_token = getattr(self.server, "api_token", "") or ""
        provided_token = (params.get("token") or [""])[0]
        if api_token and provided_token and provided_token == api_token:
            return True

        username = self.server.username
        password = self.server.password
        if not username or not password:
            return True

        raw = self.headers.get("Authorization", "")
        if not raw.startswith("Basic "):
            return False
        token = raw[6:].strip()
        try:
            decoded = base64.b64decode(token).decode("utf-8", errors="strict")
        except Exception:
            return False
        if ":" not in decoded:
            return False
        user, pwd = decoded.split(":", 1)
        return user == username and pwd == password

    def _unauthorized(self) -> None:
        self.send_response(HTTPStatus.UNAUTHORIZED)
        self.send_header("WWW-Authenticate", 'Basic realm="EQIDV2 Logs"')
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"Authentication required.")

    def _send_html(self) -> None:
        api_token_json = json.dumps(getattr(self.server, "api_token", "") or "")
        html = """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>EQIDV2 Live Logs</title>
  <style>
    :root { --bg:#0f172a; --card:#111827; --line:#1f2937; --text:#e5e7eb; --muted:#93a3b8; --ok:#10b981; --bad:#ef4444; --warn:#f59e0b; }
    * { box-sizing: border-box; }
    body { margin: 0; background: linear-gradient(135deg,#0b1220,#111827); color: var(--text); font-family: "Segoe UI", "Helvetica Neue", sans-serif; }
    header { padding: 14px 16px; border-bottom: 1px solid var(--line); position: sticky; top: 0; background: rgba(15,23,42,.9); backdrop-filter: blur(6px); }
    h1 { margin: 0; font-size: 18px; }
    .sub { margin-top: 4px; font-size: 12px; color: var(--muted); }
    .toolbar { display: flex; gap: 8px; margin-top: 10px; }
    button { background: #1d4ed8; border: none; color: white; padding: 8px 10px; border-radius: 8px; font-size: 13px; }
    .wrap { padding: 12px; display: grid; gap: 12px; }
    .card { border: 1px solid var(--line); border-radius: 10px; background: rgba(17,24,39,.9); overflow: hidden; }
    .card-head { display: flex; justify-content: space-between; align-items: center; gap: 8px; padding: 9px 10px; border-bottom: 1px solid var(--line); }
    .name { font-weight: 600; font-size: 13px; }
    .meta { font-size: 11px; color: var(--muted); }
    .status-ok { color: var(--ok); font-weight: 700; }
    .status-fail { color: var(--bad); font-weight: 700; }
    pre { margin: 0; padding: 10px; white-space: pre-wrap; word-break: break-word; font-size: 11px; line-height: 1.35; max-height: 220px; overflow: auto; }
  </style>
</head>
<body>
  <header>
    <h1>EQIDV2 Scheduled Jobs Dashboard</h1>
    <div class="sub" id="info">loading...</div>
    <div class="toolbar"><button onclick="loadNow()">Refresh Now</button></div>
  </header>
  <div class="wrap" id="cards"></div>

  <script>
    const LOG_ORDER = ["authentication","eod_15min_data","eod_1540_update","live_combined_csv","paper_trade"];
    const API_TOKEN = __API_TOKEN_JSON__;

    function esc(s) {
      return (s || "")
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/\"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }

    function apiUrl(path) {
      if (!API_TOKEN) return path;
      const sep = path.includes('?') ? '&' : '?';
      return `${path}${sep}token=${encodeURIComponent(API_TOKEN)}`;
    }

    function badge(status) {
      if (!status) return "";
      const cls = status === "SUCCESS" ? "status-ok" : "status-fail";
      return `<span class="${cls}">${status}</span>`;
    }

    async function loadNow() {
      try {
        const prevY = window.scrollY;
        const res = await fetch(apiUrl('/api/snapshot?lines=80'), { cache: 'no-store' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        document.getElementById('info').textContent = `server ${data.server_time} | auto refresh every 15s`;

        const byId = {};
        for (const item of data.items) byId[item.id] = item;

        const html = LOG_ORDER.map(id => {
          const it = byId[id] || {id,exists:false,tail:""};
          const status = it.status && it.status.status ? it.status.status : "";
          const mtime = it.mtime || "-";
          const size = it.size_bytes || 0;
          return `
            <div class="card">
              <div class="card-head">
                <div>
                  <div class="name">${esc(it.id)} ${badge(status)}</div>
                  <div class="meta">file: ${esc(it.file_name || "-")} | mtime: ${esc(mtime)} | size: ${size} bytes</div>
                </div>
              </div>
              <pre>${esc(it.tail || (it.exists ? "(empty)" : "(log file not found yet)"))}</pre>
            </div>
          `;
        }).join('');

        const cards = document.getElementById('cards');
        cards.innerHTML = html;
        cards.querySelectorAll('pre').forEach((preEl) => {
          preEl.scrollTop = preEl.scrollHeight;
        });
        window.scrollTo(0, prevY);
      } catch (err) {
        const msg = (err && err.message) ? err.message : String(err);
        document.getElementById('info').textContent = `load failed: ${msg}`;
        document.getElementById('cards').innerHTML = `
          <div class="card">
            <pre>Unable to load logs now. Tap Refresh Now.
If opened inside WhatsApp/Telegram in-app browser, open the same link in Safari/Chrome.</pre>
          </div>
        `;
      }
    }

    loadNow();
    setInterval(loadNow, 15000);
  </script>
</body>
</html>"""
        html = html.replace("__API_TOKEN_JSON__", api_token_json)
        body = html.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _snapshot(self, lines: int) -> Dict[str, object]:
        items = []
        for key, file_name in LOG_FILES.items():
            path = LOG_DIR / file_name
            status = parse_status_file(LOG_DIR / STATUS_FILES[key]) if key in STATUS_FILES else {}
            try:
                size = path.stat().st_size if path.exists() else 0
            except OSError:
                size = 0
            items.append(
                {
                    "id": key,
                    "file_name": file_name,
                    "exists": path.exists(),
                    "mtime": iso_mtime(path),
                    "size_bytes": size,
                    "status": status,
                    "tail": tail_text(path, lines=lines),
                }
            )

        return {
            "server_time": dt.datetime.now().isoformat(sep=" ", timespec="seconds"),
            "log_dir": str(LOG_DIR),
            "items": items,
        }

    def _send_json(self, payload: Dict[str, object], status: HTTPStatus = HTTPStatus.OK) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_text(self, text: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        data = text.encode("utf-8", errors="replace")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    @staticmethod
    def _int_param(params, name: str, default: int, lo: int, hi: int) -> int:
        try:
            raw = (params.get(name) or [str(default)])[0]
            num = int(raw)
        except (TypeError, ValueError):
            return default
        return max(lo, min(hi, num))


def main() -> int:
    parser = argparse.ArgumentParser(description="EQIDV2 log dashboard server")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8787, help="Bind port (default: 8787)")
    parser.add_argument("--username", default=os.environ.get("LOG_DASH_USER", ""), help="Basic auth username")
    parser.add_argument("--password", default=os.environ.get("LOG_DASH_PASS", ""), help="Basic auth password")
    parser.add_argument("--api-token", default=os.environ.get("LOG_DASH_TOKEN", ""), help="Optional API token fallback")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    httpd = ThreadingHTTPServer((args.host, args.port), LogDashboardHandler)
    httpd.username = args.username
    httpd.password = args.password
    httpd.api_token = args.api_token

    mode = "NO AUTH"
    if args.username and args.password:
        mode = "BASIC AUTH ENABLED"
    if args.api_token:
        mode = mode + " + API TOKEN"
    print(f"[INFO] Serving EQIDV2 dashboard on http://{args.host}:{args.port} ({mode})")
    print(f"[INFO] Reading logs from: {LOG_DIR}")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
