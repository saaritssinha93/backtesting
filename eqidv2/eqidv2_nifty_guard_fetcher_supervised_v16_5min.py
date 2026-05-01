"""Supervised NIFTY guard fetcher (v16 5min).

Long-running Python wrapper around `trading_data_continous_run_historical_alltf
_v3_parquet_niftyonly_5minonly.py` that replaces the legacy bat-loop's
`for /f ... powershell.exe Get-Date` pattern with a deterministic in-process
clock + subprocess spawn.

Why this exists:
  The legacy `bat/run_nifty_guard_fetcher_v16_5min.bat` polls the clock by
  spawning `powershell.exe` on every iteration. PowerShell cold-start can
  spike to several seconds under load, starving the slot-detection loop.
  On 2026-04-28 this caused a 40-minute gap between marker writes
  (10:55:04 -> 11:35:04) which DE saw as 7 NF_STALE aborts in a row.

  This wrapper:
    - Uses time.sleep(1) for polling — no subprocess per tick
    - Spawns the existing fetcher only once per real slot boundary
    - Writes heartbeat/status per iteration so supervise_command.ps1 sees liveness
    - Survives one-shot fetch failures via bounded per-slot retries
    - Cleanly stops at HARD_STOP_HHMM (default 15:31)

Invoked by `bat/run_eqidv2_nifty_guard_fetcher_supervised_v16_5min.bat` under
supervise_command.ps1, same pattern as PF/DE/Executor.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Any

import pytz

IST = pytz.timezone("Asia/Kolkata")

SCRIPT_NAME = "eqidv2_nifty_guard_fetcher_supervised_v16_5min"
BASE_DIR = Path(__file__).resolve().parent
RUNTIME_STATUS_DIR = Path(os.getenv("EQIDV2_RUNTIME_ROOT", "C:/TradingData/eqidv2")) / "runtime_status"
RUNTIME_STATUS_DIR.mkdir(parents=True, exist_ok=True)

# ---- Heartbeat / status (matches PF/DE convention) ----------------------------

def _touch_runtime_status(status: str, **extra: Any) -> None:
    """Write status file consumed by supervise_command.ps1."""
    path = RUNTIME_STATUS_DIR / f"{SCRIPT_NAME}.status"
    now = datetime.now(IST)
    lines = [
        f"status={status}",
        f"script={SCRIPT_NAME}.py",
        f"pid={os.getpid()}",
        f"ts={now.strftime('%Y-%m-%d_%H:%M:%S')}",
    ]
    for k, v in extra.items():
        lines.append(f"{k}={v}")
    try:
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    except OSError:
        pass


def _touch_runtime_heartbeat(state: str, **extra: Any) -> None:
    path = RUNTIME_STATUS_DIR / f"{SCRIPT_NAME}.heartbeat"
    now = datetime.now(IST)
    lines = [
        f"state={state}",
        f"script={SCRIPT_NAME}.py",
        f"pid={os.getpid()}",
        f"ts={now.strftime('%Y-%m-%d_%H:%M:%S')}",
    ]
    for k, v in extra.items():
        lines.append(f"{k}={v}")
    try:
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    except OSError:
        pass


# ---- Config ------------------------------------------------------------------

FETCHER_SCRIPT = BASE_DIR / "trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_5minonly.py"

# Primary index (existing NIFTY 50 / NIFTYBEES path)
NIFTY_SYMBOL = os.getenv("NIFTY_SYMBOL", "NIFTYBEES")
NIFTY_ALIASES = os.getenv("NIFTY_ALIASES", "NIFTYBEES,NIFTY50,NIFTY_50,NIFTY")

# Secondary index (NIFTY 500). Set EQIDV2_NF_FETCH_NIFTY500=0 to disable.
# Default ON so v17B/v17C/regime work can use NIFTY 500 as a small/mid-cap-aligned
# regime reference. Aliases include the index symbol with space, the no-space
# variant, and common NIFTY 500 ETFs as fallbacks.
NIFTY500_ENABLED = os.getenv("EQIDV2_NF_FETCH_NIFTY500", "1").strip().lower() in ("1", "true", "yes", "on")
NIFTY500_SYMBOL = os.getenv("NIFTY500_SYMBOL", "NIFTY 500")
NIFTY500_ALIASES = os.getenv("NIFTY500_ALIASES", "NIFTY 500,NIFTY500,N500,MOM500")

PYTHON_EXE = sys.executable

FIRST_SLOT_HHMM = os.getenv("EQIDV2_NF_FIRST_SLOT_HHMM", "0915")
HARD_STOP_HHMM = os.getenv("EQIDV2_NF_HARD_STOP_HHMM", "1531")
SLOT_OFFSET_SEC = int(os.getenv("EQIDV2_NF_SLOT_OFFSET_SEC", "2"))
POLL_SEC = max(0.25, float(os.getenv("EQIDV2_NF_POLL_SEC", "1")))
SLOT_MAX_RETRIES = max(1, int(os.getenv("EQIDV2_NF_SLOT_MAX_RETRIES", "3")))
SLOT_RETRY_DELAY_SEC = max(1, int(os.getenv("EQIDV2_NF_SLOT_RETRY_DELAY_SEC", "5")))
FETCH_TIMEOUT_SEC = max(15, int(os.getenv("EQIDV2_NF_FETCH_TIMEOUT_SEC", "60")))

NF_SLOT_FAIL_DIR = Path(os.getenv("EQIDV2_RUNTIME_ROOT", "C:/TradingData/eqidv2")) / "nifty_slot_fail_5m"
NF_SLOT_FAIL_DIR.mkdir(parents=True, exist_ok=True)


# ---- Slot detection ----------------------------------------------------------

SLOT_MINUTES = {0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55}


def _hhmm_to_time(s: str) -> dtime:
    s = s.strip().zfill(4)
    return dtime(int(s[:2]), int(s[2:4]))


def _is_slot_boundary(now: datetime) -> bool:
    """True if `now` is past slot_offset within a 5-min boundary minute."""
    if now.minute not in SLOT_MINUTES:
        return False
    return now.second >= SLOT_OFFSET_SEC


def _slot_key(now: datetime) -> str:
    return now.strftime("%H%M")


def _emit_fail_marker(slot_hhmm: str, exit_code: int, retries: int) -> None:
    today = datetime.now(IST).strftime("%Y%m%d")
    marker = NF_SLOT_FAIL_DIR / f"nifty_slot_fail_{today}_{slot_hhmm}.json"
    ts = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S%z")
    payload = (
        f'{{"slot":"{today}_{slot_hhmm}","symbol":"{NIFTY_SYMBOL}",'
        f'"exit_code":{exit_code},"retries":{retries},"emitted_at":"{ts}"}}'
    )
    try:
        marker.write_text(payload, encoding="utf-8")
    except OSError:
        pass


def _run_one_fetch(symbol: str, aliases: str, label: str,
                    skip_marker: bool = False) -> int:
    """Spawn one alltf_v3 fetcher subprocess for a single index symbol.
    Returns the subprocess exit code (0 = success, non-zero = failure).

    skip_marker=True passes --skip-marker, which suppresses writing the
    nifty_ready_*.json + nifty_open_slot_*.json DE markers. Use this for
    SECONDARY index fetches (e.g., NIFTY 500) so they do not overwrite the
    primary NIFTYBEES regime marker that the live detection engine consumes.
    """
    cmd = [
        PYTHON_EXE,
        "-u",
        str(FETCHER_SCRIPT),
        "--symbol", symbol,
        "--aliases", aliases,
    ]
    if skip_marker:
        cmd.append("--skip-marker")
    try:
        result = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            timeout=FETCH_TIMEOUT_SEC,
            check=False,
        )
        return int(result.returncode or 0)
    except subprocess.TimeoutExpired:
        print(f"[WARN] {label} fetcher timed out after {FETCH_TIMEOUT_SEC}s", flush=True)
        return 124
    except Exception as exc:
        print(f"[WARN] {label} fetcher subprocess failed: {exc!r}", flush=True)
        return 1


def _run_fetcher_once() -> int:
    """Run the alltf_v3 fetcher for NIFTY 50 (NIFTYBEES) and -- if enabled --
    NIFTY 500 sequentially. Slot-level success requires the PRIMARY (NIFTYBEES)
    fetch to succeed; NIFTY 500 failure is logged as WARN but does not abort
    the slot (regime engine has the F7 fallback for missing index data).

    PRIMARY fetch writes the nifty_ready_*.json DE marker (drives live
    regime gate). SECONDARY (NIFTY 500) fetch is invoked with --skip-marker
    so it does not overwrite the primary marker."""
    rc_primary = _run_one_fetch(NIFTY_SYMBOL, NIFTY_ALIASES, "NIFTY50",
                                  skip_marker=False)
    if rc_primary != 0:
        # Primary failed -- propagate so the supervisor retry path triggers.
        return rc_primary

    if NIFTY500_ENABLED:
        rc_secondary = _run_one_fetch(NIFTY500_SYMBOL, NIFTY500_ALIASES,
                                       "NIFTY500", skip_marker=True)
        if rc_secondary != 0:
            print(f"[WARN] NIFTY500 fetch failed (rc={rc_secondary}) but NIFTY50 "
                  f"succeeded; treating slot as OK to avoid retry-storm.",
                  flush=True)
            # Soft-fail: do not block the slot.
    return 0


# ---- Main loop ---------------------------------------------------------------

def main() -> int:
    print("=" * 60, flush=True)
    print(f"NIFTY guard fetcher (supervised) — v16 5min", flush=True)
    print(f"  python: {PYTHON_EXE}", flush=True)
    print(f"  fetcher: {FETCHER_SCRIPT}", flush=True)
    print(f"  primary symbol  : {NIFTY_SYMBOL}", flush=True)
    print(f"  primary aliases : {NIFTY_ALIASES}", flush=True)
    if NIFTY500_ENABLED:
        print(f"  secondary symbol  : {NIFTY500_SYMBOL}  (NIFTY 500)", flush=True)
        print(f"  secondary aliases : {NIFTY500_ALIASES}", flush=True)
    else:
        print(f"  secondary index   : DISABLED (EQIDV2_NF_FETCH_NIFTY500=0)", flush=True)
    print(f"  first_slot: {FIRST_SLOT_HHMM}, hard_stop: {HARD_STOP_HHMM}", flush=True)
    print(f"  slot_offset: {SLOT_OFFSET_SEC}s, poll: {POLL_SEC:.2f}s", flush=True)
    print(f"  slot_max_retries: {SLOT_MAX_RETRIES}, retry_delay: {SLOT_RETRY_DELAY_SEC}s", flush=True)
    print(f"  fetch_timeout: {FETCH_TIMEOUT_SEC}s", flush=True)
    print("=" * 60, flush=True)

    if not FETCHER_SCRIPT.exists():
        print(f"[FATAL] fetcher not found: {FETCHER_SCRIPT}", flush=True)
        _touch_runtime_status("FATAL", reason="fetcher_not_found")
        return 2

    first_slot_t = _hhmm_to_time(FIRST_SLOT_HHMM)
    hard_stop_t = _hhmm_to_time(HARD_STOP_HHMM)

    last_slot_processed = ""
    slot_retry_key = ""
    slot_retry_count = 0

    _touch_runtime_status("RUNNING", phase="STARTUP")
    _touch_runtime_heartbeat("RUNNING", phase="STARTUP")

    while True:
        now = datetime.now(IST)
        now_t = now.time()

        # Hard stop
        if now_t >= hard_stop_t:
            _touch_runtime_status("STOPPED", reason="hard_stop_reached")
            _touch_runtime_heartbeat("STOPPED", reason="hard_stop_reached")
            print(f"[STOP] Hard-stop {HARD_STOP_HHMM} reached. Exiting.", flush=True)
            return 0

        # Pre-market: idle
        if now_t < first_slot_t:
            _touch_runtime_status("RUNNING", phase="PREMARKET")
            _touch_runtime_heartbeat("RUNNING", phase="PREMARKET")
            time.sleep(POLL_SEC)
            continue

        # Cheap heartbeat every iteration so supervisor sees liveness
        _touch_runtime_heartbeat("RUNNING", phase="LOOP")

        # Slot detection
        if not _is_slot_boundary(now):
            time.sleep(POLL_SEC)
            continue

        slot_hhmm = _slot_key(now)
        if slot_hhmm == last_slot_processed:
            time.sleep(POLL_SEC)
            continue

        # Reset retry budget when slot changes
        if slot_retry_key != slot_hhmm:
            slot_retry_key = slot_hhmm
            slot_retry_count = 0

        _touch_runtime_status("RUNNING", phase="FETCH", slot=slot_hhmm)
        _touch_runtime_heartbeat("RUNNING", phase="FETCH", slot=slot_hhmm)
        print(f"[FETCH] slot={slot_hhmm} attempt={slot_retry_count + 1}", flush=True)

        exit_code = _run_fetcher_once()

        if exit_code == 0:
            last_slot_processed = slot_hhmm
            _touch_runtime_status("RUNNING", phase="FETCH_DONE", slot=slot_hhmm)
            _touch_runtime_heartbeat("RUNNING", phase="FETCH_DONE", slot=slot_hhmm)
            time.sleep(POLL_SEC)
            continue

        # Failure path
        slot_retry_count += 1
        print(f"[WARN] fetch failed slot={slot_hhmm} exit={exit_code} retry={slot_retry_count}/{SLOT_MAX_RETRIES}", flush=True)

        if slot_retry_count >= SLOT_MAX_RETRIES:
            _emit_fail_marker(slot_hhmm, exit_code, slot_retry_count)
            print(f"[ABORT] NF_SLOT_RETRY_EXHAUSTED slot={slot_hhmm} retries={slot_retry_count}", flush=True)
            last_slot_processed = slot_hhmm
            _touch_runtime_status("RUNNING", phase="SLOT_FAILED", slot=slot_hhmm)
            time.sleep(POLL_SEC)
            continue

        time.sleep(SLOT_RETRY_DELAY_SEC)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        _touch_runtime_status("STOPPED", reason="interrupt")
        _touch_runtime_heartbeat("STOPPED", reason="interrupt")
        raise SystemExit(130)
