#!/usr/bin/env python
"""
PF/DE waiting_ready alert tail (Tier-1 fix, 2026-04-23).

Tails today's eqidv2_detection_engine_v16_5min log and parses
[DETECTION_CYCLE] lines emitted by the DE every 2-5s. Fires a Gmail alert
(via bat/send_gmail_api.py) when waiting_ready > 0 has been observed
continuously for more than ALERT_THRESHOLD_SEC seconds — a strong indicator
that PF is lagging behind DE for the current slot.

Cooldown: ALERT_COOLDOWN_SEC seconds between consecutive alerts so a sustained
incident does not spam.

Env knobs (all optional):
  EQIDV2_PF_WAITING_ALERT_THRESHOLD_SEC   default 60
  EQIDV2_PF_WAITING_ALERT_COOLDOWN_SEC    default 600
  EQIDV2_PF_WAITING_ALERT_POLL_SEC        default 5
  SUPERVISOR_ALERT_EMAIL_TO               required (comma-separated)
  SUPERVISOR_ALERT_EMAIL_FROM             optional

Usage (called by run_eqidv2_pf_waiting_ready_alert_v16_5min.bat):
  python eqidv2_pf_waiting_ready_alert_v16_5min.py
"""
from __future__ import annotations

import os
import re
import socket
import subprocess
import sys
import time as _time
from datetime import datetime, time as dt_time
from pathlib import Path
from typing import Optional

import pytz

IST = pytz.timezone("Asia/Kolkata")
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
GMAIL_SCRIPT = BASE_DIR / "bat" / "send_gmail_api.py"
GMAIL_CRED = BASE_DIR / "bat" / "gmail_client_secret.json"
GMAIL_TOKEN = BASE_DIR / "bat" / "gmail_token.json"

ALERT_THRESHOLD_SEC = int(os.getenv("EQIDV2_PF_WAITING_ALERT_THRESHOLD_SEC", "60"))
ALERT_COOLDOWN_SEC = int(os.getenv("EQIDV2_PF_WAITING_ALERT_COOLDOWN_SEC", "600"))
POLL_SEC = max(1, int(os.getenv("EQIDV2_PF_WAITING_ALERT_POLL_SEC", "5")))
EMAIL_TO = os.getenv("SUPERVISOR_ALERT_EMAIL_TO", "").strip()
EMAIL_FROM = os.getenv("SUPERVISOR_ALERT_EMAIL_FROM", "").strip()
PYTHON_EXE = sys.executable

# Supervisor handshake — touch this file on every tick so supervise_command.ps1
# does not kill the worker for being "stale". Path matches what the launcher
# bat writes (eqidv2_pf_waiting_ready_alert_v16_5min.heartbeat under the
# runtime_status dir).
RUNTIME_STATUS_DIR = Path(os.getenv("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2")) / "runtime_status"
HEARTBEAT_FILE = RUNTIME_STATUS_DIR / "eqidv2_pf_waiting_ready_alert_v16_5min.heartbeat"
STATUS_FILE = RUNTIME_STATUS_DIR / "eqidv2_pf_waiting_ready_alert_v16_5min.status"


def _touch_heartbeat(state: str) -> None:
    try:
        RUNTIME_STATUS_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S%z")
        HEARTBEAT_FILE.write_text(f"{state}|{ts}|pid={os.getpid()}\n", encoding="utf-8")
        STATUS_FILE.write_text(f"{state}\n", encoding="utf-8")
    except Exception:
        pass

MARKET_OPEN = dt_time(9, 14)
MARKET_CLOSE = dt_time(15, 31)

# Match: [DETECTION_CYCLE] 09:20:13 | pending=3 | slots=1 | promoted=2 | waiting=1 (ready=1,data=0) | ...
_CYCLE_RE = re.compile(
    r"\[DETECTION_CYCLE\]\s+(\d{2}:\d{2}:\d{2})\s+\|.*?waiting=(\d+)\s*\(ready=(\d+),data=(\d+)\)"
)


def _today_log_path() -> Path:
    return LOG_DIR / f"eqidv2_detection_engine_v16_5min_{datetime.now(IST).strftime('%Y-%m-%d')}.log"


def _market_hours_now() -> bool:
    now_t = datetime.now(IST).time()
    return MARKET_OPEN <= now_t <= MARKET_CLOSE


def _send_alert(subject: str, body: str) -> bool:
    if not EMAIL_TO:
        print(f"[ALERT] SUPERVISOR_ALERT_EMAIL_TO unset; would have sent: {subject}", flush=True)
        return False
    if not GMAIL_SCRIPT.exists():
        print(f"[ALERT] gmail script missing: {GMAIL_SCRIPT}", flush=True)
        return False
    cmd = [
        PYTHON_EXE,
        str(GMAIL_SCRIPT),
        "--credentials", str(GMAIL_CRED),
        "--token", str(GMAIL_TOKEN),
        "--to", EMAIL_TO,
        "--subject", subject,
        "--body", body,
    ]
    if EMAIL_FROM:
        cmd.extend(["--from", EMAIL_FROM])
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            print(f"[ALERT] gmail send failed rc={result.returncode}: {result.stderr.strip()[:200]}", flush=True)
            return False
        print(f"[ALERT] sent: {subject}", flush=True)
        return True
    except Exception as exc:
        print(f"[ALERT] gmail send exception: {exc}", flush=True)
        return False


def _open_log(path: Path):
    try:
        fh = path.open("r", encoding="utf-8", errors="replace")
        fh.seek(0, os.SEEK_END)
        return fh
    except OSError:
        return None


def main() -> int:
    print(
        f"[PF_WAIT_ALERT] start | threshold={ALERT_THRESHOLD_SEC}s | "
        f"cooldown={ALERT_COOLDOWN_SEC}s | poll={POLL_SEC}s | host={socket.gethostname()}",
        flush=True,
    )
    _touch_heartbeat("STARTING")
    current_log_path: Optional[Path] = None
    fh = None
    waiting_since_ts: Optional[float] = None  # monotonic seconds when ready>0 first seen
    last_alert_ts: float = 0.0
    last_observed_ready = 0
    last_observed_data = 0
    last_cycle_hhmmss = ""

    while True:
        try:
            today_path = _today_log_path()
            if today_path != current_log_path:
                if fh:
                    try:
                        fh.close()
                    except Exception:
                        pass
                fh = _open_log(today_path) if today_path.exists() else None
                current_log_path = today_path
                # day rollover resets state
                waiting_since_ts = None
                last_alert_ts = 0.0
                last_observed_ready = 0
                last_observed_data = 0
                last_cycle_hhmmss = ""

            if fh is None:
                # log not yet present; retry next cycle
                _time.sleep(POLL_SEC)
                continue

            # drain any new lines
            for line in fh:
                m = _CYCLE_RE.search(line)
                if not m:
                    continue
                hhmmss, _waiting_total, ready_str, data_str = m.groups()
                ready = int(ready_str)
                data = int(data_str)
                last_observed_ready = ready
                last_observed_data = data
                last_cycle_hhmmss = hhmmss
                now_mono = _time.monotonic()
                if ready > 0:
                    if waiting_since_ts is None:
                        waiting_since_ts = now_mono
                else:
                    # ready cleared — reset timer (incident closed)
                    waiting_since_ts = None

            # evaluate alert condition (only during market hours)
            if _market_hours_now() and waiting_since_ts is not None:
                duration = _time.monotonic() - waiting_since_ts
                if duration >= ALERT_THRESHOLD_SEC:
                    cooldown_left = ALERT_COOLDOWN_SEC - (_time.monotonic() - last_alert_ts)
                    if last_alert_ts == 0.0 or cooldown_left <= 0:
                        host = socket.gethostname()
                        ist_now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
                        subject = (
                            f"[EQIDV2 ALERT] PF/DE waiting_ready={last_observed_ready} "
                            f"persisted {int(duration)}s on {host}"
                        )
                        body = (
                            f"DE has reported waiting_ready > 0 continuously for "
                            f"{int(duration)} seconds (threshold {ALERT_THRESHOLD_SEC}s).\n\n"
                            f"Most recent [DETECTION_CYCLE]:\n"
                            f"  time         : {last_cycle_hhmmss} IST (cycle log timestamp)\n"
                            f"  alert raised : {ist_now} IST\n"
                            f"  waiting_ready: {last_observed_ready} (PF marker / parquet missing)\n"
                            f"  waiting_data : {last_observed_data}\n"
                            f"  log file     : {current_log_path}\n"
                            f"  host         : {host}\n\n"
                            f"Likely cause: PF (pending data fetcher) is lagging behind DE "
                            f"and signals are being deferred. Inspect PF heartbeat / Kite "
                            f"timeouts.\n\n"
                            f"Cooldown until next alert: {ALERT_COOLDOWN_SEC}s.\n"
                        )
                        if _send_alert(subject, body):
                            last_alert_ts = _time.monotonic()

        except Exception as exc:
            print(f"[PF_WAIT_ALERT] tick error: {exc}", flush=True)

        _touch_heartbeat("RUNNING")
        _time.sleep(POLL_SEC)


if __name__ == "__main__":
    raise SystemExit(main())
