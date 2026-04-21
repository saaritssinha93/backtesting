#!/usr/bin/env python
"""
Zero-entry watchdog for EQIDV2 V16 5min pipeline (Section 23 fix C3).

Fires once per trading day when the pipeline has produced no confirmed signals
or dispatched no trades by the scheduled check time.  Intended to be invoked
from Windows Task Scheduler via run_alert_zero_entry_watchdog.bat at 10:30 IST
(primary) and optionally again at 12:30 / 14:30 (secondary tiers).

Exit codes:
  0 = OK (entries present, or non-trading day, or alert suppressed)
  2 = Zero-entry condition detected AND alert email sent
  3 = Zero-entry condition detected but email dispatch failed
  4 = Unexpected error
"""

from __future__ import annotations

import argparse
import csv
import datetime as _dt
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Tuple

IST = _dt.timezone(_dt.timedelta(hours=5, minutes=30))

BASE_DIR = Path(__file__).resolve().parent
RUNTIME_ROOT = Path(os.getenv("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2"))
LIVE_SIGNALS_DIR = Path(os.getenv("EQIDV2_LIVE_SIGNALS_DIR", str(RUNTIME_ROOT / "live_signals")))
RUNTIME_STATUS_DIR = Path(os.getenv("EQIDV2_RUNTIME_STATUS_DIR", str(RUNTIME_ROOT / "runtime_status")))

GMAIL_SCRIPT = BASE_DIR / "bat" / "send_gmail_api.py"
GMAIL_CREDS = Path(os.getenv("GMAIL_CREDENTIALS_FILE", str(BASE_DIR / "bat" / "gmail_client_secret.json")))
GMAIL_TOKEN = Path(os.getenv("GMAIL_TOKEN_FILE", str(BASE_DIR / "bat" / "gmail_token.json")))

ALERT_TO = os.getenv(
    "EQIDV2_ALERT_EMAIL_TO",
    "saaritssinha93@gmail.com,dragontastic007@gmail.com",
)
ALERT_FROM = os.getenv("EQIDV2_ALERT_EMAIL_FROM", "saaritssinha93@gmail.com")

KILL_SWITCH_FILE = LIVE_SIGNALS_DIR / "kill_switch_false_v16_5min.json"


def _count_csv_rows_for_today(path: Path, today_str: str) -> int:
    """Return number of data rows whose timestamp column starts with today_str.

    Checks common timestamp columns (signal_time, entry_time, timestamp).
    Returns -1 if the file is missing, 0 if empty but present.
    """
    if not path.exists():
        return -1
    try:
        with path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return 0
            candidates = ("signal_time", "entry_time", "timestamp", "signal_ts", "confirmed_at")
            ts_idx = -1
            for col in candidates:
                if col in header:
                    ts_idx = header.index(col)
                    break
            count = 0
            for row in reader:
                if not row:
                    continue
                if ts_idx >= 0 and ts_idx < len(row):
                    if str(row[ts_idx]).startswith(today_str):
                        count += 1
                else:
                    count += 1
            return count
    except Exception:
        return -1


def _kill_switch_active() -> bool:
    if not KILL_SWITCH_FILE.exists():
        return False
    try:
        data = json.loads(KILL_SWITCH_FILE.read_text(encoding="utf-8"))
    except Exception:
        return True
    val = data.get("active", data.get("enabled", False))
    return bool(val)


def _send_alert(subject: str, body: str) -> bool:
    if not GMAIL_SCRIPT.exists():
        print(f"ERROR gmail_script_missing path={GMAIL_SCRIPT}", file=sys.stderr)
        return False
    if not GMAIL_CREDS.exists() or not GMAIL_TOKEN.exists():
        print("ERROR gmail_auth_missing", file=sys.stderr)
        return False
    cmd = [
        sys.executable,
        str(GMAIL_SCRIPT),
        "--credentials", str(GMAIL_CREDS),
        "--token", str(GMAIL_TOKEN),
        "--to", ALERT_TO,
        "--from", ALERT_FROM,
        "--subject", subject,
        "--body", body,
    ]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except Exception as exc:
        print(f"ERROR gmail_invoke_failed {exc}", file=sys.stderr)
        return False
    if res.returncode != 0:
        print(f"ERROR gmail_send_failed rc={res.returncode} stderr={res.stderr.strip()}", file=sys.stderr)
        return False
    print(f"OK gmail_sent {res.stdout.strip()}")
    return True


def _already_alerted_today(today_str: str, tier: str) -> bool:
    marker = RUNTIME_STATUS_DIR / f"alert_zero_entry_{tier}_{today_str}.marker"
    return marker.exists()


def _mark_alerted(today_str: str, tier: str) -> None:
    RUNTIME_STATUS_DIR.mkdir(parents=True, exist_ok=True)
    marker = RUNTIME_STATUS_DIR / f"alert_zero_entry_{tier}_{today_str}.marker"
    marker.write_text(_dt.datetime.now(IST).isoformat(), encoding="utf-8")


def _is_trading_day(d: _dt.date) -> bool:
    return d.weekday() < 5


def check(confirmed_min: int, tier: str) -> Tuple[int, str, str]:
    now = _dt.datetime.now(IST)
    today = now.date()
    today_str = today.isoformat()

    if not _is_trading_day(today):
        return 0, "non_trading_day", f"Weekend/holiday ({today_str}); no check."

    if _kill_switch_active():
        return 0, "kill_switch_active", "Kill switch engaged; zero entries expected."

    if _already_alerted_today(today_str, tier):
        return 0, "already_alerted", f"Tier '{tier}' alert already sent today."

    long_csv = LIVE_SIGNALS_DIR / f"signals_{today_str}_v16_5min_long.csv"
    short_csv = LIVE_SIGNALS_DIR / f"signals_{today_str}_v16_5min_short.csv"
    trade_csv = LIVE_SIGNALS_DIR / f"live_trades_{today_str}_v16_5min.csv"

    confirmed_long = _count_csv_rows_for_today(long_csv, today_str)
    confirmed_short = _count_csv_rows_for_today(short_csv, today_str)
    confirmed_total = max(0, confirmed_long) + max(0, confirmed_short)
    dispatched = _count_csv_rows_for_today(trade_csv, today_str)

    ok = confirmed_total >= confirmed_min

    body = (
        f"EQIDV2 V16 5min zero-entry watchdog\n"
        f"Timestamp       : {now.isoformat()}\n"
        f"Tier            : {tier} (threshold confirmed >= {confirmed_min})\n"
        f"Date (IST)      : {today_str}\n"
        f"\n"
        f"Confirmed LONG  : {confirmed_long}  ({long_csv.name})\n"
        f"Confirmed SHORT : {confirmed_short}  ({short_csv.name})\n"
        f"Confirmed TOTAL : {confirmed_total}\n"
        f"Dispatched trades: {dispatched}  ({trade_csv.name})\n"
        f"\n"
        f"Likely causes (investigate in order):\n"
        f"  1. Stage 1 Signal Engine crashed or hung (check supervisor status + log tail).\n"
        f"  2. NIFTYBEES stale -> RS compute returned no-signal regime (check niftybees parquet mtime).\n"
        f"  3. Scheduler overall_state=FAIL blocking Stage 1 (fresh_ratio < 0.95).\n"
        f"  4. Stage 2 Detection Engine rejecting every pending as filtered_parity (signal_id hash drift).\n"
        f"  5. Stage 3 Pending Fetcher not writing ready markers (missing_token starvation).\n"
        f"  6. Kill switch was cleared mid-session without a fresh restart.\n"
        f"\n"
        f"Next step: open dashboard, tail eqidv2_signal_engine_v16_5min_{today_str}.log, grep for ERROR.\n"
    )

    if ok:
        return 0, "entries_present", f"confirmed={confirmed_total} dispatched={max(dispatched,0)}"

    subject = (
        f"[EQIDV2 ALERT] Zero-entry {tier} tier at {now.strftime('%H:%M')} IST "
        f"(confirmed={confirmed_total} dispatched={max(dispatched,0)}) {today_str}"
    )
    sent = _send_alert(subject, body)
    if sent:
        _mark_alerted(today_str, tier)
        return 2, "alert_sent", subject
    return 3, "alert_failed", subject


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="EQIDV2 V16 5min zero-entry watchdog")
    p.add_argument(
        "--tier",
        default="1030",
        help="Tier label (also used in the once-per-day marker filename). Examples: 1030, 1230, 1430.",
    )
    p.add_argument(
        "--min-confirmed",
        type=int,
        default=1,
        help="Minimum confirmed signals expected by this tier. Default=1 (fires only on true zero).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the computation but do not send email or write the marker.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.dry_run:
            now = _dt.datetime.now(IST)
            today_str = now.date().isoformat()
            long_csv = LIVE_SIGNALS_DIR / f"signals_{today_str}_v16_5min_long.csv"
            short_csv = LIVE_SIGNALS_DIR / f"signals_{today_str}_v16_5min_short.csv"
            trade_csv = LIVE_SIGNALS_DIR / f"live_trades_{today_str}_v16_5min.csv"
            print(f"DRY-RUN tier={args.tier} min_confirmed={args.min_confirmed}")
            print(f"  long   = {_count_csv_rows_for_today(long_csv, today_str)} ({long_csv})")
            print(f"  short  = {_count_csv_rows_for_today(short_csv, today_str)} ({short_csv})")
            print(f"  trades = {_count_csv_rows_for_today(trade_csv, today_str)} ({trade_csv})")
            print(f"  kill_switch_active = {_kill_switch_active()}")
            print(f"  is_trading_day     = {_is_trading_day(now.date())}")
            return 0
        rc, status, detail = check(args.min_confirmed, args.tier)
        print(f"STATUS {status} | rc={rc} | {detail}")
        return rc
    except Exception as exc:
        print(f"ERROR unexpected {exc}", file=sys.stderr)
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
