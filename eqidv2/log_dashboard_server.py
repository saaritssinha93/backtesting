#!/usr/bin/env python3
"""Simple secured log dashboard for EQIDV2 scheduled jobs."""

from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import json
import math
import os
import re
from collections import deque
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable, Dict, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LIVE_SIGNAL_DIR = BASE_DIR / "live_signals"
KITE_EXPORT_DIR = BASE_DIR / "kite_exports"
IST = ZoneInfo("Asia/Kolkata")

LOG_FILES: Dict[str, str] = {
    "authentication_v2": "authentication_v2_runner.log",
    "eod_15min_data": "eqidv2_eod_scheduler_for_15mins_data.log",
    "eod_1540_update": "eqidv2_eod_scheduler_for_1540_update.log",
    "live_combined_csv_v4_short": "eqidv2_live_combined_analyser_csv_v4_short.log",
    "live_combined_csv_v4_long": "eqidv2_live_combined_analyser_csv_v4_long.log",
    "live_combined_csv_v5_short": "eqidv2_live_combined_analyser_csv_v5_short.log",
    "live_combined_csv_v5_long": "eqidv2_live_combined_analyser_csv_v5_long.log",
}
LOG_IDS = tuple(LOG_FILES.keys()) + (
    "paper_trade_v4",
    "paper_trade_v5",
    "kite_trade",
    "preopen_healthcheck",
)

STATUS_FILES: Dict[str, str] = {
    "authentication_v2": "authentication_v2_runner.status",
    "live_combined_csv_v4_short": "eqidv2_live_combined_analyser_csv_v4_short.status",
    "live_combined_csv_v4_long": "eqidv2_live_combined_analyser_csv_v4_long.status",
    "live_combined_csv_v5_short": "eqidv2_live_combined_analyser_csv_v5_short.status",
    "live_combined_csv_v5_long": "eqidv2_live_combined_analyser_csv_v5_long.status",
}


def _latest_matching_file(base_dir: Path, glob_pattern: str) -> Optional[Path]:
    try:
        candidates = list(base_dir.glob(glob_pattern))
    except OSError:
        return None
    if not candidates:
        return None
    try:
        return max(candidates, key=lambda p: p.stat().st_mtime)
    except OSError:
        return None


def resolve_log_target(name: str) -> Tuple[Path, str]:
    today_ist = dt.datetime.now(IST).date().isoformat()
    if name in LOG_FILES:
        file_name = LOG_FILES[name]
        return LOG_DIR / file_name, file_name

    if name == "paper_trade_v4":
        today_name = f"avwap_trade_execution_PAPER_TRADE_TRUE_v4_{today_ist}.log"
        today_path = LOG_DIR / today_name
        if today_path.exists():
            return today_path, today_name
        latest = _latest_matching_file(LOG_DIR, "avwap_trade_execution_PAPER_TRADE_TRUE_v4_*.log")
        if latest is not None:
            return latest, latest.name
        legacy_name = "avwap_trade_execution_PAPER_TRADE_TRUE_v4.log"
        return LOG_DIR / legacy_name, legacy_name

    if name == "paper_trade_v5":
        today_name = f"avwap_trade_execution_PAPER_TRADE_TRUE_v5_{today_ist}.log"
        today_path = LOG_DIR / today_name
        if today_path.exists():
            return today_path, today_name
        latest = _latest_matching_file(LOG_DIR, "avwap_trade_execution_PAPER_TRADE_TRUE_v5_*.log")
        if latest is not None:
            return latest, latest.name
        legacy_name = "avwap_trade_execution_PAPER_TRADE_TRUE_v5.log"
        return LOG_DIR / legacy_name, legacy_name

    if name == "kite_trade":
        today_name = f"avwap_trade_execution_PAPER_TRADE_FALSE_{today_ist}.log"
        today_path = LOG_DIR / today_name
        if today_path.exists():
            return today_path, today_name
        latest = _latest_matching_file(LOG_DIR, "avwap_trade_execution_PAPER_TRADE_FALSE_*.log")
        if latest is not None:
            return latest, latest.name
        legacy_name = "avwap_trade_execution_PAPER_TRADE_FALSE.log"
        legacy_path = LOG_DIR / legacy_name
        if legacy_path.exists():
            return legacy_path, legacy_name
        signal_log_name = "live_trade_execution.log"
        signal_log_path = LIVE_SIGNAL_DIR / signal_log_name
        if signal_log_path.exists():
            return signal_log_path, str(Path("live_signals") / signal_log_name)
        return legacy_path, legacy_name

    if name == "preopen_healthcheck":
        today_name = f"preopen_session_healthcheck_{today_ist}.log"
        today_path = LOG_DIR / today_name
        if today_path.exists():
            return today_path, today_name
        latest_name = "preopen_session_healthcheck_latest.log"
        latest_path = LOG_DIR / latest_name
        if latest_path.exists():
            return latest_path, latest_name
        fallback = _latest_matching_file(LOG_DIR, "preopen_session_healthcheck_*.log")
        if fallback is not None:
            return fallback, fallback.name
        return latest_path, latest_name

    if name == "paper_trade_exec":
        today_name = f"paper_trade_execution_{today_ist}.log"
        today_path = LIVE_SIGNAL_DIR / today_name
        if today_path.exists():
            return today_path, str(Path("live_signals") / today_name)
        latest = _latest_matching_file(LIVE_SIGNAL_DIR, "paper_trade_execution_*.log")
        if latest is not None:
            return latest, str(Path("live_signals") / latest.name)
        legacy_name = "paper_trade_execution.log"
        return LIVE_SIGNAL_DIR / legacy_name, str(Path("live_signals") / legacy_name)

    raise KeyError(name)


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


def _read_csv_tail_rows(path: Path, limit: int = 30) -> list[dict[str, str]]:
    if not path.exists():
        return []
    rows: deque[dict[str, str]] = deque(maxlen=max(1, int(limit)))
    try:
        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if not row:
                    continue
                rows.append({str(k): ("" if v is None else str(v)) for k, v in row.items()})
    except (OSError, csv.Error):
        return []
    return list(rows)


def _pick_csv_value(row: dict[str, str], keys: Sequence[str]) -> str:
    for key in keys:
        val = str(row.get(key, "")).strip()
        if val:
            return val
    return ""


def _clip_text(value: str, width: int) -> str:
    s = str(value)
    if len(s) <= width:
        return s
    if width <= 1:
        return s[:width]
    return s[: width - 1] + "~"


def _extract_time_only(value: str) -> str:
    """
    Strip YYYY-MM-DD and trailing timezone offsets from common datetime strings,
    and keep only the time part.
    Examples:
      2026-02-24 11:05:18+0530 -> 11:05:18
      2026-02-24T11:05:18+05:30 -> 11:05:18
    """
    s = str(value or "").strip()
    if not s:
        return s
    m = re.match(r"^\d{4}-\d{2}-\d{2}[ T](.+)$", s)
    if m:
        s = m.group(1).strip()
    s = re.sub(r"\s*(?:Z|[+-]\d{2}:?\d{2})$", "", s).strip()
    return s


def _to_float_or_nan(value: str) -> float:
    s = str(value or "").strip()
    if not s:
        return float("nan")
    # tolerate display formats like Rs.+1,234.56
    s = s.replace(",", "")
    s = s.replace("Rs.", "").replace("RS.", "").replace("rs.", "")
    s = s.replace("%", "")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def _fmt_indian_number(value: float, decimals: int = 2, signed: bool = False) -> str:
    if math.isnan(value):
        return "n/a"

    sign = ""
    if value < 0:
        sign = "-"
    elif signed and value > 0:
        sign = "+"

    abs_val = abs(value)
    if decimals <= 0:
        numeric = f"{abs_val:.0f}"
        int_part = numeric
        frac_part = ""
    else:
        numeric = f"{abs_val:.{decimals}f}"
        int_part, frac_part = numeric.split(".", 1)

    if len(int_part) > 3:
        head = int_part[:-3]
        tail = int_part[-3:]
        groups = []
        while len(head) > 2:
            groups.append(head[-2:])
            head = head[:-2]
        if head:
            groups.append(head)
        groups.reverse()
        int_part = ",".join(groups + [tail])

    if decimals <= 0:
        return f"{sign}{int_part}"
    return f"{sign}{int_part}.{frac_part}"


def _fmt_pct(value: float, signed: bool = True) -> str:
    return f"{_fmt_indian_number(value, decimals=2, signed=signed)}%"


def _fmt_rs(value: float) -> str:
    return f"Rs.{_fmt_indian_number(value, decimals=2, signed=True)}"


def _compute_holding_total_pnl_pct(row: dict[str, str]) -> str:
    qty = _to_float_or_nan(_pick_csv_value(row, ("quantity", "qty")))
    t1_qty = _to_float_or_nan(_pick_csv_value(row, ("t1_quantity",)))
    avg = _to_float_or_nan(_pick_csv_value(row, ("average_price", "avg_price", "price")))
    pnl = _to_float_or_nan(_pick_csv_value(row, ("pnl", "unrealised", "unrealized")))

    if math.isnan(avg) or math.isnan(pnl):
        return ""

    q = 0.0
    if not math.isnan(qty):
        q += float(qty)
    if not math.isnan(t1_qty):
        q += float(t1_qty)
    if q <= 0.0:
        return ""

    invested = q * float(avg)
    if invested <= 0.0:
        return ""

    return str((float(pnl) * 100.0) / invested)


def _format_csv_projection(
    path: Path,
    columns: Sequence[Tuple[str, Sequence[str]]],
    limit_rows: int = 25,
    time_only_cols: Optional[Set[str]] = None,
    sort_numeric_desc_by_keys: Optional[Sequence[str]] = None,
    total_numeric_by_keys: Optional[Sequence[str]] = None,
    total_numeric_label: str = "",
    total_numeric_first: bool = False,
    indian_numeric_cols: Optional[Set[str]] = None,
    indian_int_cols: Optional[Set[str]] = None,
    percent_cols: Optional[Set[str]] = None,
    signed_numeric_cols: Optional[Set[str]] = None,
    computed_cols: Optional[Dict[str, Callable[[dict[str, str]], str]]] = None,
) -> str:
    """
    Render a compact fixed-width table from selected CSV columns.
    Shows latest rows (oldest-to-newest order within the selected tail window).
    """
    if not path.exists():
        return ""

    rows_raw = _read_csv_tail_rows(path, limit=limit_rows)
    if not rows_raw:
        return "(no rows yet)"

    computed_cols = dict(computed_cols or {})

    if sort_numeric_desc_by_keys:
        def _pick_numeric_raw_for_sort(row: dict[str, str], keys: Sequence[str]) -> str:
            for key in keys:
                if key in computed_cols:
                    try:
                        val = str(computed_cols[key](row) or "").strip()
                    except Exception:
                        val = ""
                else:
                    val = _pick_csv_value(row, (key,))
                if val:
                    return val
            return ""

        def _sort_key(row: dict[str, str]) -> tuple[int, float]:
            raw = _pick_numeric_raw_for_sort(row, sort_numeric_desc_by_keys)
            num = _to_float_or_nan(raw)
            if not math.isnan(num):
                return (0, -float(num))
            return (1, 0.0)

        rows_raw = sorted(rows_raw, key=_sort_key)

    total_val = 0.0
    total_count = 0
    if total_numeric_by_keys:
        for row in rows_raw:
            raw = _pick_csv_value(row, total_numeric_by_keys)
            num = _to_float_or_nan(raw)
            if math.isnan(num):
                continue
            total_val += float(num)
            total_count += 1

    rows: list[dict[str, str]] = []
    time_only_cols = set(time_only_cols or set())
    indian_numeric_cols = set(indian_numeric_cols or set())
    indian_int_cols = set(indian_int_cols or set())
    percent_cols = set(percent_cols or set())
    signed_numeric_cols = set(signed_numeric_cols or set())
    for row in rows_raw:
        projected: dict[str, str] = {}
        for col_name, key_candidates in columns:
            if col_name in computed_cols:
                try:
                    val = str(computed_cols[col_name](row) or "")
                except Exception:
                    val = ""
            else:
                val = _pick_csv_value(row, key_candidates)
            if col_name in time_only_cols:
                val = _extract_time_only(val)
            elif col_name in percent_cols:
                num = _to_float_or_nan(val)
                if not math.isnan(num):
                    val = _fmt_pct(num, signed=(col_name in signed_numeric_cols))
            elif col_name in indian_int_cols:
                num = _to_float_or_nan(val)
                if not math.isnan(num):
                    val = _fmt_indian_number(num, decimals=0, signed=(col_name in signed_numeric_cols))
            elif col_name in indian_numeric_cols:
                num = _to_float_or_nan(val)
                if not math.isnan(num):
                    val = _fmt_indian_number(num, decimals=2, signed=(col_name in signed_numeric_cols))
            projected[col_name] = val
        rows.append(projected)

    widths: dict[str, int] = {}
    for col_name, _ in columns:
        max_len = max([len(col_name)] + [len(r[col_name]) for r in rows])
        widths[col_name] = min(max_len, 30)

    header = " | ".join(col_name.ljust(widths[col_name]) for col_name, _ in columns)
    sep = "-+-".join("-" * widths[col_name] for col_name, _ in columns)
    body = [
        " | ".join(
            _clip_text(r[col_name], widths[col_name]).ljust(widths[col_name])
            for col_name, _ in columns
        )
        for r in rows
    ]
    out_lines = [f"rows_shown={len(rows)} (latest)", header, sep] + body
    if total_numeric_by_keys and total_numeric_label:
        total_text = _fmt_rs(total_val) if total_count > 0 else "n/a"
        total_line = f"{total_numeric_label}={total_text}"
        if total_numeric_first:
            out_lines = [total_line] + out_lines
        else:
            out_lines.append(total_line)
    return "\n".join(out_lines)


def _compute_holdings_summary(path: Path) -> tuple[float, float, float, float, float, float]:
    invested = 0.0
    current = 0.0
    day_pnl = 0.0
    prev_close_value = 0.0

    if not path.exists():
        return (
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
        )

    try:
        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if not row:
                    continue
                qty = _to_float_or_nan(_pick_csv_value(row, ("quantity",)))
                t1_qty = _to_float_or_nan(_pick_csv_value(row, ("t1_quantity",)))
                avg = _to_float_or_nan(_pick_csv_value(row, ("average_price", "avg_price", "price")))
                ltp = _to_float_or_nan(_pick_csv_value(row, ("last_price", "ltp", "close_price")))
                close_price = _to_float_or_nan(_pick_csv_value(row, ("close_price",)))
                day_change = _to_float_or_nan(_pick_csv_value(row, ("day_change",)))

                q = 0.0
                if not math.isnan(qty):
                    q += float(qty)
                if not math.isnan(t1_qty):
                    q += float(t1_qty)

                if q == 0.0:
                    continue

                if not math.isnan(avg):
                    invested += q * float(avg)
                if not math.isnan(ltp):
                    current += q * float(ltp)
                if not math.isnan(day_change):
                    day_pnl += q * float(day_change)
                elif not math.isnan(ltp) and not math.isnan(close_price):
                    day_pnl += q * (float(ltp) - float(close_price))

                if not math.isnan(close_price):
                    prev_close_value += q * float(close_price)
                elif not math.isnan(ltp) and not math.isnan(day_change):
                    prev_close_value += q * (float(ltp) - float(day_change))
    except (OSError, csv.Error):
        return (
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
        )

    pnl = current - invested
    pnl_pct = (pnl * 100.0 / invested) if invested > 0 else float("nan")
    day_pnl_pct = (day_pnl * 100.0 / prev_close_value) if prev_close_value > 0 else float("nan")
    return invested, current, pnl, pnl_pct, day_pnl, day_pnl_pct


def _read_kite_snapshot_meta(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _extract_funds_available(meta: dict[str, object]) -> float:
    raw = meta.get("funds_available")
    try:
        if raw is None:
            return float("nan")
        return float(raw)
    except (TypeError, ValueError):
        return float("nan")


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
            if name not in LOG_IDS:
                self._send_json({"error": "unknown log name"}, status=HTTPStatus.BAD_REQUEST)
                return
            lines = self._int_param(params, "lines", 150, 20, 1000)
            file_path, _ = resolve_log_target(name)
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
    :root {
      --bg-a: #0a1220;
      --bg-b: #0f1f2f;
      --bg-c: #0b1827;
      --card: rgba(12, 24, 37, 0.86);
      --line: rgba(112, 145, 179, 0.22);
      --line-strong: rgba(112, 145, 179, 0.44);
      --text: #edf4ff;
      --muted: #a7bacf;
      --ok: #0fcf9a;
      --bad: #ff6b6b;
      --warn: #ffbe5c;
      --accent: #45c4ff;
      --accent-2: #4de4c9;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      color: var(--text);
      font-family: "Trebuchet MS", "Verdana", sans-serif;
      background:
        radial-gradient(1200px 900px at 110% -10%, rgba(69, 196, 255, 0.16), transparent 58%),
        radial-gradient(900px 700px at -10% 115%, rgba(77, 228, 201, 0.12), transparent 54%),
        linear-gradient(145deg, var(--bg-a), var(--bg-b) 52%, var(--bg-c));
    }

    body::after {
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      opacity: 0.12;
      background-image:
        linear-gradient(rgba(130, 170, 210, 0.12) 1px, transparent 1px),
        linear-gradient(90deg, rgba(130, 170, 210, 0.12) 1px, transparent 1px);
      background-size: 26px 26px, 26px 26px;
      mask-image: radial-gradient(circle at 50% 45%, black 35%, transparent 90%);
    }

    header {
      position: sticky;
      top: 0;
      z-index: 20;
      padding: 14px 16px 12px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(135deg, rgba(9, 19, 30, 0.94), rgba(12, 25, 38, 0.88));
      backdrop-filter: blur(8px);
    }

    h1 {
      margin: 0;
      font-size: 20px;
      font-weight: 700;
      letter-spacing: 0.2px;
      font-family: "Palatino Linotype", "Book Antiqua", serif;
    }

    .sub {
      margin-top: 4px;
      font-size: 12px;
      color: var(--muted);
    }

    .toolbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-top: 11px;
      flex-wrap: wrap;
    }

    .toolbar-note {
      font-size: 11px;
      color: var(--muted);
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 5px 10px;
      background: rgba(8, 18, 28, 0.7);
    }

    button {
      border: 1px solid rgba(84, 204, 255, 0.5);
      color: #081726;
      font-weight: 700;
      background: linear-gradient(135deg, var(--accent), var(--accent-2));
      padding: 8px 12px;
      border-radius: 10px;
      font-size: 13px;
      cursor: pointer;
      transition: transform 0.14s ease, box-shadow 0.14s ease;
      box-shadow: 0 10px 22px rgba(20, 128, 167, 0.35);
    }

    button:hover {
      transform: translateY(-1px);
      box-shadow: 0 14px 24px rgba(20, 128, 167, 0.42);
    }

    .wrap {
      max-width: 1600px;
      margin: 0 auto;
      padding: 14px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 14px;
    }

    .card {
      position: relative;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: linear-gradient(165deg, rgba(16, 31, 47, 0.9), var(--card));
      overflow: hidden;
      box-shadow: 0 15px 28px rgba(1, 8, 15, 0.45);
      animation: cardIn 0.33s ease both;
      transition: transform 0.16s ease, border-color 0.16s ease, box-shadow 0.16s ease;
    }

    .card:hover {
      transform: translateY(-2px);
      border-color: rgba(90, 182, 230, 0.45);
      box-shadow: 0 20px 34px rgba(1, 8, 15, 0.52);
    }

    .card::before {
      content: "";
      position: absolute;
      inset: 0 auto 0 0;
      width: 3px;
      background: var(--line-strong);
    }

    .card.is-ok::before { background: var(--ok); }
    .card.is-bad::before { background: var(--bad); }
    .card.is-fullscreen::before { background: var(--accent); }

    .card-head {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 8px;
      padding: 10px 11px;
      border-bottom: 1px solid var(--line);
    }

    .card-head-left {
      display: flex;
      flex-direction: column;
      align-items: flex-start;
      gap: 4px;
      min-width: 0;
      flex: 1 1 auto;
    }

    .name {
      font-size: 13px;
      font-weight: 700;
      letter-spacing: 0.2px;
      max-width: 100%;
      overflow-wrap: anywhere;
    }

    .meta {
      margin-top: 4px;
      font-size: 11px;
      color: var(--muted);
      line-height: 1.35;
      max-width: 100%;
      overflow-wrap: anywhere;
    }

    .pill {
      font-size: 10px;
      font-weight: 700;
      border-radius: 999px;
      padding: 3px 8px;
      border: 1px solid var(--line);
      color: var(--muted);
      background: rgba(8, 18, 28, 0.76);
      white-space: nowrap;
    }

    .pill.ok {
      color: #06251a;
      border-color: rgba(15, 207, 154, 0.5);
      background: rgba(15, 207, 154, 0.92);
    }

    .pill.fail {
      color: #2f0707;
      border-color: rgba(255, 107, 107, 0.6);
      background: rgba(255, 107, 107, 0.95);
    }

    .card-head-right {
      display: flex;
      align-items: flex-start;
      gap: 6px;
      flex-shrink: 0;
    }

    .card-toggle {
      border: 1px solid var(--line-strong);
      color: var(--text);
      font-weight: 700;
      background: rgba(8, 18, 28, 0.78);
      padding: 4px 8px;
      border-radius: 8px;
      font-size: 11px;
      cursor: pointer;
      box-shadow: none;
      transition: border-color 0.14s ease, background 0.14s ease;
    }

    .card-toggle:hover {
      transform: none;
      box-shadow: none;
      border-color: var(--accent);
      background: rgba(12, 26, 40, 0.9);
    }

    .card-toggle.is-active {
      border-color: var(--accent);
      background: rgba(23, 61, 89, 0.9);
    }

    pre {
      margin: 0;
      padding: 11px;
      white-space: pre;
      word-break: normal;
      font-size: 11px;
      line-height: 1.4;
      max-height: 230px;
      overflow-x: auto;
      overflow-y: auto;
      font-family: "Consolas", "Lucida Console", monospace;
      background: rgba(7, 16, 25, 0.78);
      tab-size: 4;
      scrollbar-gutter: stable both-edges;
    }

    .table-shell {
      margin: 0;
      padding: 8px;
      max-height: 230px;
      overflow-x: auto;
      overflow-y: auto;
      font-family: "Consolas", "Lucida Console", monospace;
      font-size: 10px;
      line-height: 1.25;
      background: rgba(7, 16, 25, 0.78);
      tab-size: 4;
      scrollbar-gutter: stable both-edges;
    }

    .table-summary,
    .table-meta {
      white-space: pre;
      overflow-wrap: anywhere;
    }

    .table-summary {
      color: var(--text);
      margin-bottom: 6px;
    }

    .table-meta {
      color: var(--muted);
      margin-bottom: 6px;
    }

    .log-table {
      border-collapse: collapse;
      width: max-content;
      min-width: max-content;
      table-layout: auto;
    }

    .log-table th,
    .log-table td {
      border: 1px solid var(--line);
      padding: 2px 5px;
      white-space: nowrap;
      text-align: left;
      background: rgba(9, 18, 28, 0.65);
    }

    .log-table thead th {
      position: sticky;
      top: 0;
      z-index: 1;
      background: rgba(12, 24, 37, 0.96);
    }

    .th-sort-btn {
      display: inline-flex;
      align-items: center;
      gap: 2px;
      border: 0 !important;
      color: var(--text) !important;
      font-weight: 700;
      background: transparent !important;
      padding: 0 !important;
      border-radius: 0 !important;
      font-size: 11px !important;
      cursor: pointer;
      box-shadow: none !important;
    }

    .th-sort-btn:hover {
      transform: none !important;
      box-shadow: none !important;
      color: var(--accent) !important;
      text-decoration: underline;
    }

    .sort-mark {
      color: var(--muted);
      font-size: 9px;
      min-width: 1.7em;
      text-align: right;
    }

    body.has-fullscreen {
      overflow: hidden;
    }

    body.has-fullscreen header {
      display: none;
    }

    body.has-fullscreen .wrap {
      max-width: none;
      padding: 0;
      display: block;
    }

    body.has-fullscreen .card {
      display: none;
    }

    body.has-fullscreen .card.is-fullscreen {
      display: block;
      position: fixed;
      inset: 8px;
      margin: 0;
      z-index: 999;
      border-radius: 12px;
      border-color: var(--line-strong);
      box-shadow: 0 18px 42px rgba(0, 0, 0, 0.55);
    }

    body.has-fullscreen .card.is-fullscreen .card-head {
      position: sticky;
      top: 0;
      z-index: 2;
      background: linear-gradient(165deg, rgba(16, 31, 47, 0.98), rgba(10, 20, 31, 0.98));
    }

    body.has-fullscreen .card.is-fullscreen pre {
      max-height: calc(100vh - 92px);
      height: calc(100vh - 92px);
      font-size: 12px;
      padding: 12px;
    }

    body.has-fullscreen .card.is-fullscreen .table-shell {
      max-height: calc(100vh - 92px);
      height: calc(100vh - 92px);
      font-size: 12px;
      padding: 12px;
    }

    @keyframes cardIn {
      from { opacity: 0; transform: translateY(6px); }
      to { opacity: 1; transform: translateY(0); }
    }

    @media (max-width: 720px) {
      h1 { font-size: 18px; }
      .wrap { grid-template-columns: 1fr; padding: 10px; gap: 10px; }
      .card-head { padding: 9px 10px; }
      pre { max-height: 250px; }
      .table-shell { max-height: 250px; }
    }
  </style>
</head>
<body>
  <header>
    <h1>EQIDV2 Scheduled Jobs Dashboard</h1>
    <div class="sub" id="info">loading...</div>
    <div class="toolbar">
      <button id="refreshBtn" onclick="loadNow()">Refresh Now</button>
      <div class="toolbar-note">Auto refresh every 15 seconds | Maximize and scroll horizontally for full line data</div>
    </div>
  </header>
  <div class="wrap" id="cards"></div>

  <script>
    const LOG_ORDER = [
      "eod_15min_data",
      "live_combined_csv_v4_short",
      "live_combined_csv_v4_long",
      "live_combined_csv_v5_short",
      "live_combined_csv_v5_long",
      "live_signals_csv_v4_short",
      "live_signals_csv_v4_long",
      "live_signals_csv_v5_short",
      "live_signals_csv_v5_long",
      "live_papertrade_result_csv_v4",
      "live_papertrade_result_csv_v5",
      "paper_trade_v4",
      "paper_trade_v5",
      "preopen_healthcheck",
      "kite_trade",
      "live_kite_trades_csv",
      "kite_holdings_today_csv",
      "kite_positions_day_today_csv",
      "authentication_v2",
      "eod_1540_update"
    ];
    const LOG_TITLES = {
      "eod_15min_data": "Live Data Fetch (15mins)",
      "live_combined_csv_v4_short": "Live Analysis And Signal Generation V4 Short",
      "live_combined_csv_v4_long": "Live Analysis And Signal Generation V4 Long",
      "live_combined_csv_v5_short": "Live Analysis And Signal Generation V5 Short",
      "live_combined_csv_v5_long": "Live Analysis And Signal Generation V5 Long",
      "live_signals_csv_v4_short": "Live Entries CSV V4 Short",
      "live_signals_csv_v4_long": "Live Entries CSV V4 Long",
      "live_signals_csv_v5_short": "Live Entries CSV V5 Short",
      "live_signals_csv_v5_long": "Live Entries CSV V5 Long",
      "live_papertrade_result_csv_v4": "Live Papertrade Result CSV V4",
      "live_papertrade_result_csv_v5": "Live Papertrade Result CSV V5",
      "live_kite_trades_csv": "Live Kite Trades CSV",
      "kite_holdings_today_csv": "Kite Holdings (Today)",
      "kite_positions_day_today_csv": "Kite Positions (Daily, Today)",
      "authentication_v2": "Auth_V2",
      "paper_trade_v4": "Papertrade Runner View V4",
      "paper_trade_v5": "Papertrade Runner View V5",
      "preopen_healthcheck": "Preopen Healthcheck 09:05",
      "kite_trade": "Live Kite Trades Log",
      "eod_1540_update": "Live EOD Data Fetch"
    };
    const API_TOKEN = __API_TOKEN_JSON__;
    let FULLSCREEN_ID = "";
    const TABLE_SORT_STATE = {};

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

    function displayName(id) {
      return LOG_TITLES[id] || id;
    }

    function statusBadge(status) {
      if (!status) return '<span class="pill">UNKNOWN</span>';
      if (status === "SUCCESS") return '<span class="pill ok">SUCCESS</span>';
      return `<span class="pill fail">${esc(status)}</span>`;
    }

    function parseNumberish(value) {
      const s = String(value || "").trim();
      if (!s) return NaN;
      const cleaned = s
        .replace(/,/g, "")
        .replace(/^Rs\\./i, "")
        .replace(/%/g, "")
        .replace(/\\s+/g, "");
      const n = Number.parseFloat(cleaned);
      return Number.isFinite(n) ? n : NaN;
    }

    function isTickerColumn(header) {
      return String(header || "").trim().toLowerCase() === "ticker";
    }

    function parseTabularTail(tailText) {
      const lines = String(tailText || "").split(/\\r?\\n/);
      const rowsMetaIdx = lines.findIndex((ln) => ln.startsWith("rows_shown="));
      if (rowsMetaIdx < 0 || rowsMetaIdx + 2 >= lines.length) return null;

      const headerLine = lines[rowsMetaIdx + 1] || "";
      const sepLine = lines[rowsMetaIdx + 2] || "";
      if (!headerLine.includes(" | ") || !sepLine.includes("-+-")) return null;

      const headers = headerLine.split(" | ").map((h) => h.trim());
      if (!headers.length) return null;

      const dataLines = lines.slice(rowsMetaIdx + 3).filter((ln) => String(ln || "").trim().length > 0);
      const rows = dataLines.map((ln) => {
        const parts = ln.split(" | ").map((p) => String(p || "").trim());
        while (parts.length < headers.length) parts.push("");
        if (parts.length > headers.length) return parts.slice(0, headers.length);
        return parts;
      });

      return {
        rowsMeta: lines[rowsMetaIdx] || "",
        summaryLines: lines.slice(0, rowsMetaIdx).filter((ln) => String(ln || "").trim().length > 0),
        headers,
        rows
      };
    }

    function sortMark(header, cardId, colIdx) {
      const st = TABLE_SORT_STATE[cardId];
      if (!st || st.colIdx !== colIdx) return "↕";
      if (isTickerColumn(header)) return st.dir === "asc" ? "A→Z" : "Z→A";
      return st.dir === "asc" ? "↑" : "↓";
    }

    function sortedRows(parsed, cardId) {
      const st = TABLE_SORT_STATE[cardId];
      const indexed = parsed.rows.map((cells, idx) => ({ cells, idx }));
      if (!st || st.colIdx < 0) return indexed.map((r) => r.cells);

      const colIdx = st.colIdx;
      const dirMul = st.dir === "asc" ? 1 : -1;
      const header = parsed.headers[colIdx] || "";

      indexed.sort((a, b) => {
        const av = String((a.cells[colIdx] ?? "")).trim();
        const bv = String((b.cells[colIdx] ?? "")).trim();
        const aEmpty = !av;
        const bEmpty = !bv;
        if (aEmpty && bEmpty) return a.idx - b.idx;
        if (aEmpty) return 1;
        if (bEmpty) return -1;

        let cmp = 0;
        if (!isTickerColumn(header)) {
          const an = parseNumberish(av);
          const bn = parseNumberish(bv);
          if (Number.isFinite(an) && Number.isFinite(bn)) {
            cmp = an - bn;
          }
        }
        if (cmp === 0) {
          cmp = av.localeCompare(bv, undefined, { numeric: true, sensitivity: "base" });
        }
        if (cmp === 0) return a.idx - b.idx;
        return cmp * dirMul;
      });

      return indexed.map((r) => r.cells);
    }

    function renderSortableTable(cardId, hostEl, parsed) {
      const rows = sortedRows(parsed, cardId);

      const summaryHtml = parsed.summaryLines.length
        ? `<div class="table-summary">${parsed.summaryLines.map((ln) => esc(ln)).join("<br>")}</div>`
        : "";
      const metaHtml = parsed.rowsMeta ? `<div class="table-meta">${esc(parsed.rowsMeta)}</div>` : "";
      const headHtml = parsed.headers.map((h, i) => `
        <th>
          <button type="button" class="th-sort-btn" data-col="${i}">
            <span>${esc(h)}</span>
            <span class="sort-mark">${esc(sortMark(h, cardId, i))}</span>
          </button>
        </th>
      `).join("");
      const bodyHtml = rows.map((cells) => `
        <tr>${parsed.headers.map((_, i) => `<td>${esc(cells[i] ?? "")}</td>`).join("")}</tr>
      `).join("");

      hostEl.innerHTML = `
        ${summaryHtml}
        ${metaHtml}
        <table class="log-table">
          <thead><tr>${headHtml}</tr></thead>
          <tbody>${bodyHtml}</tbody>
        </table>
      `;

      hostEl.querySelectorAll(".th-sort-btn").forEach((btn) => {
        btn.addEventListener("click", (ev) => {
          const colIdx = Number((ev.currentTarget && ev.currentTarget.getAttribute("data-col")) || "-1");
          if (!Number.isInteger(colIdx) || colIdx < 0) return;
          const prev = TABLE_SORT_STATE[cardId] || { colIdx: -1, dir: "asc" };
          const nextDir = prev.colIdx === colIdx && prev.dir === "asc" ? "desc" : "asc";
          TABLE_SORT_STATE[cardId] = { colIdx, dir: nextDir };
          renderSortableTable(cardId, hostEl, parsed);
        });
      });
    }

    function enhanceSortableTables() {
      const cards = document.querySelectorAll("#cards .card");
      cards.forEach((card) => {
        const cardId = card.getAttribute("data-id") || "";
        const preEl = card.querySelector("pre");
        if (!preEl) return;
        const parsed = parseTabularTail(preEl.textContent || "");
        if (!parsed) return;
        const host = document.createElement("div");
        host.className = "table-shell";
        preEl.replaceWith(host);
        renderSortableTable(cardId, host, parsed);
      });
    }

    function applyFullscreenState() {
      const cards = document.getElementById('cards');
      const all = cards.querySelectorAll('.card');
      all.forEach((card) => {
        const cardId = card.getAttribute('data-id') || "";
        card.classList.toggle('is-fullscreen', FULLSCREEN_ID && cardId === FULLSCREEN_ID);
      });
      document.body.classList.toggle('has-fullscreen', !!FULLSCREEN_ID);
    }

    function toggleFullscreen(id) {
      if (!id) return;
      FULLSCREEN_ID = (FULLSCREEN_ID === id) ? "" : id;
      applyFullscreenState();
    }

    function wireCardControls() {
      const buttons = document.querySelectorAll('#cards .card-toggle');
      buttons.forEach((btn) => {
        btn.addEventListener('click', (ev) => {
          const id = ev.currentTarget.getAttribute('data-toggle-id') || "";
          toggleFullscreen(id);
        });
      });
    }

    document.addEventListener('keydown', (ev) => {
      if (ev.key === "Escape" && FULLSCREEN_ID) {
        FULLSCREEN_ID = "";
        applyFullscreenState();
      }
    });

    async function loadNow() {
      try {
        const prevY = window.scrollY;
        const res = await fetch(apiUrl('/api/snapshot?lines=80'), { cache: 'no-store' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        document.getElementById('info').textContent = `server ${data.server_time} | auto refresh every 15s`;

        const byId = {};
        for (const item of data.items) byId[item.id] = item;
        const ordered = LOG_ORDER.concat(Object.keys(byId).filter((id) => !LOG_ORDER.includes(id)));

        const html = ordered.map((id, idx) => {
          const it = byId[id] || {id,exists:false,tail:""};
          const status = it.status && it.status.status ? it.status.status : "";
          const mtime = it.mtime || "-";
          const size = it.size_bytes || 0;
          const cardCls = status === "SUCCESS" ? "card is-ok" : (status ? "card is-bad" : "card");
          const isFs = FULLSCREEN_ID === id ? " is-fullscreen" : "";
          const toggleLabel = FULLSCREEN_ID === id ? "Minimize" : "Maximize";
          const toggleCls = FULLSCREEN_ID === id ? "card-toggle is-active" : "card-toggle";
          return `
            <div class="${cardCls}${isFs}" data-id="${esc(id)}" style="animation-delay:${Math.min(idx * 0.05, 0.55)}s">
              <div class="card-head">
                <div class="card-head-left">
                  <button type="button" class="${toggleCls}" data-toggle-id="${esc(id)}">${toggleLabel}</button>
                  <div class="name">${esc(displayName(it.id))}</div>
                  <div class="meta">file: ${esc(it.file_name || "-")} | mtime: ${esc(mtime)} | size: ${size} bytes</div>
                </div>
                <div class="card-head-right">
                  <div>${statusBadge(status)}</div>
                </div>
              </div>
              <pre>${esc(it.tail || (it.exists ? "(empty)" : "(log file not found yet)"))}</pre>
            </div>
          `;
        }).join('');

        const cards = document.getElementById('cards');
        cards.innerHTML = html;
        wireCardControls();
        enhanceSortableTables();
        applyFullscreenState();
        cards.querySelectorAll('pre').forEach((preEl) => {
          preEl.scrollTop = preEl.scrollHeight;
        });
        if (!FULLSCREEN_ID) window.scrollTo(0, prevY);
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
        for key in LOG_IDS:
            path, file_name = resolve_log_target(key)
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

        # Dynamic cards: today's live signal CSV(s) used by trade execution.
        today_ist = dt.datetime.now(IST).date().isoformat()
        live_entries_cols: list[Tuple[str, Sequence[str]]] = [
            ("signal_datetime", ("signal_datetime", "signal_entry_datetime_ist", "signal_bar_time_ist", "created_ts_ist")),
            ("detected_time_ist", ("detected_time_ist",)),
            ("ticker", ("ticker",)),
            ("side", ("side",)),
            ("entry_price", ("entry_price",)),
            ("target_price", ("target_price",)),
            ("stop_price", ("stop_price", "_stop_price")),
            ("quantity", ("quantity",)),
        ]

        # Dynamic card: today's live signal CSV V4 short.
        live_csv_name_v4_short = f"signals_{today_ist}_v4_short.csv"
        live_csv_path_v4_short = LIVE_SIGNAL_DIR / live_csv_name_v4_short
        try:
            live_size_v4_short = live_csv_path_v4_short.stat().st_size if live_csv_path_v4_short.exists() else 0
        except OSError:
            live_size_v4_short = 0
        live_entries_tail_v4_short = _format_csv_projection(
            live_csv_path_v4_short,
            live_entries_cols,
            limit_rows=max(5, min(35, lines // 2)),
            time_only_cols={"signal_datetime", "detected_time_ist"},
        )
        items.append(
            {
                "id": "live_signals_csv_v4_short",
                "file_name": str(Path("live_signals") / live_csv_name_v4_short),
                "exists": live_csv_path_v4_short.exists(),
                "mtime": iso_mtime(live_csv_path_v4_short),
                "size_bytes": live_size_v4_short,
                "status": {},
                "tail": live_entries_tail_v4_short,
            }
        )

        # Dynamic card: today's live signal CSV V4 long.
        live_csv_name_v4_long = f"signals_{today_ist}_v4_long.csv"
        live_csv_path_v4_long = LIVE_SIGNAL_DIR / live_csv_name_v4_long
        try:
            live_size_v4_long = live_csv_path_v4_long.stat().st_size if live_csv_path_v4_long.exists() else 0
        except OSError:
            live_size_v4_long = 0
        live_entries_tail_v4_long = _format_csv_projection(
            live_csv_path_v4_long,
            live_entries_cols,
            limit_rows=max(5, min(35, lines // 2)),
            time_only_cols={"signal_datetime", "detected_time_ist"},
        )
        items.append(
            {
                "id": "live_signals_csv_v4_long",
                "file_name": str(Path("live_signals") / live_csv_name_v4_long),
                "exists": live_csv_path_v4_long.exists(),
                "mtime": iso_mtime(live_csv_path_v4_long),
                "size_bytes": live_size_v4_long,
                "status": {},
                "tail": live_entries_tail_v4_long,
            }
        )

        # Dynamic card: today's live signal CSV V5 short.
        live_csv_name_v5_short = f"signals_{today_ist}_v5_short.csv"
        live_csv_path_v5_short = LIVE_SIGNAL_DIR / live_csv_name_v5_short
        try:
            live_size_v5_short = live_csv_path_v5_short.stat().st_size if live_csv_path_v5_short.exists() else 0
        except OSError:
            live_size_v5_short = 0
        live_entries_tail_v5_short = _format_csv_projection(
            live_csv_path_v5_short,
            live_entries_cols,
            limit_rows=max(5, min(35, lines // 2)),
            time_only_cols={"signal_datetime", "detected_time_ist"},
        )
        items.append(
            {
                "id": "live_signals_csv_v5_short",
                "file_name": str(Path("live_signals") / live_csv_name_v5_short),
                "exists": live_csv_path_v5_short.exists(),
                "mtime": iso_mtime(live_csv_path_v5_short),
                "size_bytes": live_size_v5_short,
                "status": {},
                "tail": live_entries_tail_v5_short,
            }
        )

        # Dynamic card: today's live signal CSV V5 long.
        live_csv_name_v5_long = f"signals_{today_ist}_v5_long.csv"
        live_csv_path_v5_long = LIVE_SIGNAL_DIR / live_csv_name_v5_long
        try:
            live_size_v5_long = live_csv_path_v5_long.stat().st_size if live_csv_path_v5_long.exists() else 0
        except OSError:
            live_size_v5_long = 0
        live_entries_tail_v5_long = _format_csv_projection(
            live_csv_path_v5_long,
            live_entries_cols,
            limit_rows=max(5, min(35, lines // 2)),
            time_only_cols={"signal_datetime", "detected_time_ist"},
        )
        items.append(
            {
                "id": "live_signals_csv_v5_long",
                "file_name": str(Path("live_signals") / live_csv_name_v5_long),
                "exists": live_csv_path_v5_long.exists(),
                "mtime": iso_mtime(live_csv_path_v5_long),
                "size_bytes": live_size_v5_long,
                "status": {},
                "tail": live_entries_tail_v5_long,
            }
        )

        # Dynamic cards: today's paper trade results CSV(s).
        paper_trade_cols: list[Tuple[str, Sequence[str]]] = [
            ("ticker", ("ticker",)),
            ("exit_time", ("exit_time",)),
            ("side", ("side",)),
            ("outcome", ("outcome",)),
            ("pnl_rs", ("pnl_rs",)),
            ("pnl_pct", ("pnl_pct",)),
        ]

        # Dynamic card: today's paper trade results CSV V4 unified.
        paper_trade_csv_name_v4 = f"paper_trades_{today_ist}_v4.csv"
        paper_trade_csv_path_v4 = LIVE_SIGNAL_DIR / paper_trade_csv_name_v4
        try:
            paper_trade_size_v4 = paper_trade_csv_path_v4.stat().st_size if paper_trade_csv_path_v4.exists() else 0
        except OSError:
            paper_trade_size_v4 = 0
        paper_trade_tail_v4 = _format_csv_projection(
            paper_trade_csv_path_v4,
            paper_trade_cols,
            limit_rows=max(5, min(40, lines // 2)),
            time_only_cols={"exit_time"},
        )
        items.append(
            {
                "id": "live_papertrade_result_csv_v4",
                "file_name": str(Path("live_signals") / paper_trade_csv_name_v4),
                "exists": paper_trade_csv_path_v4.exists(),
                "mtime": iso_mtime(paper_trade_csv_path_v4),
                "size_bytes": paper_trade_size_v4,
                "status": {},
                "tail": paper_trade_tail_v4,
            }
        )

        # Dynamic card: today's paper trade results CSV V5 unified.
        paper_trade_csv_name_v5 = f"paper_trades_{today_ist}_v5.csv"
        paper_trade_csv_path_v5 = LIVE_SIGNAL_DIR / paper_trade_csv_name_v5
        try:
            paper_trade_size_v5 = paper_trade_csv_path_v5.stat().st_size if paper_trade_csv_path_v5.exists() else 0
        except OSError:
            paper_trade_size_v5 = 0
        paper_trade_tail_v5 = _format_csv_projection(
            paper_trade_csv_path_v5,
            paper_trade_cols,
            limit_rows=max(5, min(40, lines // 2)),
            time_only_cols={"exit_time"},
        )
        items.append(
            {
                "id": "live_papertrade_result_csv_v5",
                "file_name": str(Path("live_signals") / paper_trade_csv_name_v5),
                "exists": paper_trade_csv_path_v5.exists(),
                "mtime": iso_mtime(paper_trade_csv_path_v5),
                "size_bytes": paper_trade_size_v5,
                "status": {},
                "tail": paper_trade_tail_v5,
            }
        )

        # Dynamic card: today's live Kite trades CSV.
        live_kite_trade_csv_name = f"live_trades_{today_ist}.csv"
        live_kite_trade_csv_path = LIVE_SIGNAL_DIR / live_kite_trade_csv_name
        try:
            live_kite_trade_size = (
                live_kite_trade_csv_path.stat().st_size if live_kite_trade_csv_path.exists() else 0
            )
        except OSError:
            live_kite_trade_size = 0
        live_kite_trade_cols: list[Tuple[str, Sequence[str]]] = [
            ("ticker", ("ticker",)),
            ("entry_time", ("entry_time",)),
            ("exit_time", ("exit_time",)),
            ("side", ("side",)),
            ("outcome", ("outcome",)),
            ("entry", ("filled_price", "entry_price")),
            ("exit", ("exit_price",)),
            ("pnl_rs", ("pnl_rs",)),
        ]
        live_kite_trade_tail = _format_csv_projection(
            live_kite_trade_csv_path,
            live_kite_trade_cols,
            limit_rows=max(5, min(40, lines // 2)),
            time_only_cols={"entry_time", "exit_time"},
        )
        items.append(
            {
                "id": "live_kite_trades_csv",
                "file_name": str(Path("live_signals") / live_kite_trade_csv_name),
                "exists": live_kite_trade_csv_path.exists(),
                "mtime": iso_mtime(live_kite_trade_csv_path),
                "size_bytes": live_kite_trade_size,
                "status": {},
                "tail": live_kite_trade_tail,
            }
        )

        # Dynamic cards: today's Kite holdings / day positions exported by zerodha_kite_export.py
        today_ymd = dt.datetime.now(IST).strftime("%Y%m%d")
        kite_meta = _read_kite_snapshot_meta(KITE_EXPORT_DIR / "kite_snapshot_meta.json")
        funds_available = _extract_funds_available(kite_meta)
        funds_available_text = _fmt_rs(funds_available) if not math.isnan(funds_available) else "n/a"

        holdings_candidates = [
            KITE_EXPORT_DIR / f"holdings_{today_ymd}.csv",
            KITE_EXPORT_DIR / "kite_holdings_today.csv",
        ]
        holdings_path = next((p for p in holdings_candidates if p.exists()), holdings_candidates[-1])
        try:
            holdings_size = holdings_path.stat().st_size if holdings_path.exists() else 0
        except OSError:
            holdings_size = 0
        holdings_cols: list[Tuple[str, Sequence[str]]] = [
            ("ticker", ("tradingsymbol", "symbol", "ticker")),
            ("exchange", ("exchange",)),
            ("qty", ("quantity", "qty")),
            ("avg_price", ("average_price", "avg_price")),
            ("last_price", ("last_price", "ltp")),
            ("pnl", ("pnl", "unrealised", "unrealized")),
            ("%total_pnl", ()),
            ("day_chg_pct", ("day_change_percentage", "day_change_pct")),
        ]
        holdings_tail = _format_csv_projection(
            holdings_path,
            holdings_cols,
            limit_rows=max(200, lines),
            sort_numeric_desc_by_keys=("%total_pnl", "pnl", "unrealised", "unrealized"),
            indian_numeric_cols={"avg_price", "last_price", "pnl"},
            indian_int_cols={"qty"},
            percent_cols={"%total_pnl", "day_chg_pct"},
            signed_numeric_cols={"pnl", "%total_pnl", "day_chg_pct"},
            computed_cols={"%total_pnl": _compute_holding_total_pnl_pct},
        )
        invested_amt, current_amt, total_pnl, total_pnl_pct, day_pnl, day_pnl_pct = _compute_holdings_summary(
            holdings_path
        )
        total_current_with_funds = (
            current_amt + funds_available
            if (not math.isnan(current_amt) and not math.isnan(funds_available))
            else float("nan")
        )
        total_invested_with_funds = (
            invested_amt + funds_available
            if (not math.isnan(invested_amt) and not math.isnan(funds_available))
            else float("nan")
        )
        holdings_summary_lines = [
            f"invested_amount={_fmt_rs(invested_amt) if not math.isnan(invested_amt) else 'n/a'}",
            f"current_amount={_fmt_rs(current_amt) if not math.isnan(current_amt) else 'n/a'}",
            f"total_pnl={_fmt_rs(total_pnl) if not math.isnan(total_pnl) else 'n/a'}",
            f"total_pnl_pct={_fmt_pct(total_pnl_pct) if not math.isnan(total_pnl_pct) else 'n/a'}",
            f"day_pnl={_fmt_rs(day_pnl) if not math.isnan(day_pnl) else 'n/a'}",
            f"day_pnl_pct={_fmt_pct(day_pnl_pct) if not math.isnan(day_pnl_pct) else 'n/a'}",
            f"funds_available={funds_available_text}",
            f"TOTAL(invested)={_fmt_rs(total_invested_with_funds) if not math.isnan(total_invested_with_funds) else 'n/a'}",
            f"TOTAL(current)={_fmt_rs(total_current_with_funds) if not math.isnan(total_current_with_funds) else 'n/a'}",
        ]
        holdings_tail = "\n".join(holdings_summary_lines + [holdings_tail])
        items.append(
            {
                "id": "kite_holdings_today_csv",
                "file_name": str(Path("kite_exports") / holdings_path.name),
                "exists": holdings_path.exists(),
                "mtime": iso_mtime(holdings_path),
                "size_bytes": holdings_size,
                "status": {},
                "tail": holdings_tail,
            }
        )

        positions_day_candidates = [
            KITE_EXPORT_DIR / f"positions_day_{today_ymd}.csv",
            KITE_EXPORT_DIR / "kite_positions_day_today.csv",
        ]
        positions_day_path = next((p for p in positions_day_candidates if p.exists()), positions_day_candidates[-1])
        try:
            positions_day_size = positions_day_path.stat().st_size if positions_day_path.exists() else 0
        except OSError:
            positions_day_size = 0
        positions_day_cols: list[Tuple[str, Sequence[str]]] = [
            ("ticker", ("tradingsymbol", "symbol", "ticker")),
            ("exchange", ("exchange",)),
            ("product", ("product",)),
            ("qty", ("quantity", "qty")),
            ("buy_qty", ("buy_quantity",)),
            ("sell_qty", ("sell_quantity",)),
            ("avg_price", ("average_price", "avg_price")),
            ("last_price", ("last_price", "ltp")),
            ("pnl", ("pnl", "unrealised", "unrealized")),
        ]
        positions_day_tail = _format_csv_projection(
            positions_day_path,
            positions_day_cols,
            limit_rows=max(200, lines),
            total_numeric_by_keys=("pnl", "unrealised", "unrealized"),
            total_numeric_label="total_pnl_ongoing",
            total_numeric_first=True,
            indian_numeric_cols={"avg_price", "last_price", "pnl"},
            indian_int_cols={"qty", "buy_qty", "sell_qty"},
            signed_numeric_cols={"pnl"},
        )
        positions_day_tail = "\n".join([f"funds_available={funds_available_text}", positions_day_tail])
        items.append(
            {
                "id": "kite_positions_day_today_csv",
                "file_name": str(Path("kite_exports") / positions_day_path.name),
                "exists": positions_day_path.exists(),
                "mtime": iso_mtime(positions_day_path),
                "size_bytes": positions_day_size,
                "status": {},
                "tail": positions_day_tail,
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
