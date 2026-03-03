# -*- coding: utf-8 -*-
"""
eqidv2_parallel_runner_v5.py
============================
Parallel orchestrator that runs all three eqidv2 live-day processes
simultaneously as isolated OS subprocesses:

  [15M  ] eqidv2_eod_scheduler_for_15mins_data.py
  [SHORT] eqidv2_live_combined_analyser_csv_v5_short.py
  [LONG ] eqidv2_live_combined_analyser_csv_v5_long.py

WHY SUBPROCESSES (not threads or direct imports):
  v5_short and v5_long BOTH import `eqidv2_live_combined_analyser_csv_v2`
  (the same module object) and monkey-patch its module-level globals:
      SIGNAL_CSV_PATTERN, scan_long_one_day, scan_short_one_day,
      _write_signals_csv, OUT_CHECKS_DIR, OUT_SIGNALS_DIR, STATE_FILE,
      REPORTS_DIR, END_TIME, SESSION_END, IMMEDIATE_SIGNAL_CSV_FLUSH, ...
  Running both in the same Python interpreter -- even in separate threads --
  causes their patches to overwrite each other: SHORT's patches land on top
  of LONG's (or vice versa), producing corrupt mixed signals and wrong output
  directories. Separate OS processes give each script its own Python
  interpreter with a completely independent copy of every module, at zero
  cost to signal accuracy, effectiveness, or robustness.

WHAT THIS RUNNER DOES (equivalent to running all three bat files together):
  - Applies the same env vars as the individual bat files (defaults match).
  - Streams each subprocess stdout+stderr to its own dedicated log file
    (append mode, same path as the bat files) AND to the console with a
    [LABEL] prefix so you can tell processes apart at a glance.
  - Auto-restarts non-zero exits up to MAX_RESTARTS times with a short
    delay, same as the bat-file restart loops.
  - Skips restart if the crash occurs after the process's own crash_cutoff
    (matches each bat file's END_CUTOFF_HHMM setting).
  - Force-kills any process still running at HARD_CUTOFF (15:55 IST).
  - Handles Ctrl+C / SIGTERM gracefully (terminates all subprocesses).

Usage:
    python eqidv2_parallel_runner_v5.py

All env vars from the individual bat files are respected if already set in
the environment; the defaults below match the bat files' hardcoded values:

    EQIDV2_15M_MAX_WORKERS               (default: 24)
    EQIDV2_15M_BUFFER_SEC                (default: 6)
    EQIDV2_15M_REFRESH_TOKENS            (default: 0)
    EQIDV2_INITIAL_DELAY_SECONDS         (default: 10)
    EQIDV2_NUM_SCANS_PER_SLOT            (default: 3)
    EQIDV2_SCAN_INTERVAL_SECONDS         (default: 5)
    EQIDV5_STALE_ONLY_RETRY              (default: 1)
    EQIDV5_LONG_PENDING_POLL_ENABLED     (default: 1)
    EQIDV5_LONG_PENDING_POLL_INTERVAL_SEC (default: 5)

Dashboard compatibility:
    Each subprocess writes to the same log file path as the corresponding
    bat file, so the log_dashboard_server.py sessions (eod_15min_data,
    live_combined_csv_v5_short, live_combined_csv_v5_long) continue working
    without any changes.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from datetime import time as dtime
from pathlib import Path
from typing import Dict, List, Optional

import pytz

IST = pytz.timezone("Asia/Kolkata")

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"

PYTHON_EXE = sys.executable

# After this IST wall-clock time, force-kill any subprocess still running.
HARD_CUTOFF = dtime(15, 55)

# Matches the bat files.
MAX_RESTARTS = 20
RESTART_DELAY_SEC = 15

# Prevent interleaved console output from concurrent streaming threads.
_CONSOLE_LOCK = threading.Lock()


def _now_ist() -> datetime:
    return datetime.now(IST)


def _past(cutoff: dtime) -> bool:
    return _now_ist().time() >= cutoff


def _ts() -> str:
    return _now_ist().strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# ManagedProcess
# ---------------------------------------------------------------------------

class ManagedProcess:
    """
    Wraps a long-running subprocess with:
      - stdout+stderr streamed to a log file AND to the console (with label).
      - auto-restart on non-zero exit up to MAX_RESTARTS times.
      - crash_cutoff: don't restart if crash happens after this time (IST).
    """

    def __init__(
        self,
        name: str,
        cmd: List[str],
        env: Dict[str, str],
        log_path: Path,
        crash_cutoff: dtime,
        label: str,
    ) -> None:
        self.name = name
        self.cmd = cmd
        self.env = env
        self.log_path = log_path
        self.crash_cutoff = crash_cutoff
        self.label = label          # e.g. "[SHORT] "

        self._proc: Optional[subprocess.Popen] = None
        self._restart_count = 0
        self._done = False
        self._lock = threading.Lock()
        self._log_fh = None

    # ---- public API ----

    def start(self) -> None:
        with self._lock:
            if _past(self.crash_cutoff):
                self._console(
                    f"SKIP: already past crash_cutoff "
                    f"{self.crash_cutoff.strftime('%H:%M')} IST"
                )
                self._done = True
                return
            LOG_DIR.mkdir(parents=True, exist_ok=True)
            self._open_log()
            self._launch()

    def terminate(self) -> None:
        """Gracefully stop the subprocess and close the log file."""
        with self._lock:
            self._done = True
            if self._proc and self._proc.poll() is None:
                try:
                    self._proc.terminate()
                    self._proc.wait(timeout=10)
                except Exception:
                    try:
                        self._proc.kill()
                    except Exception:
                        pass
            if self._log_fh:
                try:
                    self._log_fh.close()
                except Exception:
                    pass

    def poll_and_maybe_restart(self) -> bool:
        """
        Check exit status and restart if appropriate.
        Returns True  -> process is still active (running or just restarted).
        Returns False -> permanently done; do not call again.
        """
        if self._done:
            return False

        with self._lock:
            if self._proc is None:
                return False

            rc = self._proc.poll()
            if rc is None:
                return True  # still running normally

            self._log(f"[{_ts()}] END {self.name} (exit={rc})\n")
            self._console(f"exited (exit={rc})")

            if rc == 0:
                # Clean exit (script hit its own internal end-time).
                self._done = True
                return False

            # Non-zero exit: crashed. Decide whether to restart.
            if _past(self.crash_cutoff):
                self._console(
                    f"crash after crash_cutoff "
                    f"{self.crash_cutoff.strftime('%H:%M')}; not restarting."
                )
                self._log(f"[WARN] {self.name}: crash after cutoff. Not restarting.\n")
                self._done = True
                return False

            if self._restart_count >= MAX_RESTARTS:
                self._console(f"max restarts ({MAX_RESTARTS}) exceeded; giving up.")
                self._log(f"[ERROR] {self.name}: max restarts exceeded.\n")
                self._done = True
                return False

            self._restart_count += 1
            self._console(
                f"crash (exit={rc}); restart "
                f"{self._restart_count}/{MAX_RESTARTS} in {RESTART_DELAY_SEC}s..."
            )
            self._log(
                f"[WARN] {self.name}: crash (exit={rc}). "
                f"Restart {self._restart_count}/{MAX_RESTARTS} in {RESTART_DELAY_SEC}s...\n"
            )

        # Sleep outside the lock so terminate() can acquire it if Ctrl+C fires.
        time.sleep(RESTART_DELAY_SEC)

        with self._lock:
            if self._done:
                return False  # terminate() was called during sleep
            if _past(self.crash_cutoff):
                self._console("cutoff reached during restart delay; aborting restart.")
                self._done = True
                return False
            self._launch()
            return True

    @property
    def is_done(self) -> bool:
        return self._done

    # ---- internals ----

    def _open_log(self) -> None:
        if self._log_fh is not None:
            try:
                self._log_fh.close()
            except Exception:
                pass
        try:
            self._log_fh = open(self.log_path, "a", encoding="utf-8", buffering=1)
        except Exception as exc:
            self._console(f"WARNING: cannot open log {self.log_path}: {exc}")

    def _launch(self) -> None:
        env = {**os.environ, **self.env}
        self._proc = subprocess.Popen(
            self.cmd,
            cwd=str(BASE_DIR),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            universal_newlines=True,
            encoding="utf-8",
            errors="replace",
        )
        self._log(f"[{_ts()}] START {self.name} (pid={self._proc.pid})\n")
        self._console(f"started (pid={self._proc.pid})")

        t = threading.Thread(
            target=self._stream_output,
            daemon=True,
            name=f"stream-{self.name}",
        )
        t.start()

    def _stream_output(self) -> None:
        """Read subprocess stdout line-by-line, write to log + console."""
        try:
            for line in self._proc.stdout:
                self._log(line)
                with _CONSOLE_LOCK:
                    try:
                        sys.stdout.write(self.label + line)
                        sys.stdout.flush()
                    except Exception:
                        pass
        except Exception:
            pass

    def _log(self, text: str) -> None:
        try:
            if self._log_fh and not self._log_fh.closed:
                self._log_fh.write(text)
                self._log_fh.flush()
        except Exception:
            pass

    def _console(self, msg: str) -> None:
        with _CONSOLE_LOCK:
            try:
                print(f"[RUNNER|{self.name}] {msg}", flush=True)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Env builders — match the bat file values exactly
# ---------------------------------------------------------------------------

def _common_env() -> Dict[str, str]:
    """Env vars shared by all three subprocesses."""
    return {
        "PYTHONUNBUFFERED":                    "1",
        "EQIDV2_INITIAL_DELAY_SECONDS":        os.getenv("EQIDV2_INITIAL_DELAY_SECONDS", "10"),
        "EQIDV2_NUM_SCANS_PER_SLOT":           os.getenv("EQIDV2_NUM_SCANS_PER_SLOT", "3"),
        "EQIDV2_SCAN_INTERVAL_SECONDS":        os.getenv("EQIDV2_SCAN_INTERVAL_SECONDS", "5"),
        "EQIDV5_STALE_ONLY_RETRY":             os.getenv("EQIDV5_STALE_ONLY_RETRY", "1"),
        "EQIDV5_LONG_PENDING_POLL_ENABLED":    os.getenv("EQIDV5_LONG_PENDING_POLL_ENABLED", "1"),
        "EQIDV5_LONG_PENDING_POLL_INTERVAL_SEC": os.getenv("EQIDV5_LONG_PENDING_POLL_INTERVAL_SEC", "5"),
    }


def _15m_env() -> Dict[str, str]:
    env = _common_env()
    env["EQIDV2_15M_MAX_WORKERS"]    = os.getenv("EQIDV2_15M_MAX_WORKERS", "24")
    env["EQIDV2_15M_BUFFER_SEC"]     = os.getenv("EQIDV2_15M_BUFFER_SEC", "6")
    env["EQIDV2_15M_REFRESH_TOKENS"] = os.getenv("EQIDV2_15M_REFRESH_TOKENS", "0")
    return env


def _15m_cmd() -> List[str]:
    max_workers = os.getenv("EQIDV2_15M_MAX_WORKERS", "24")
    buffer_sec  = os.getenv("EQIDV2_15M_BUFFER_SEC", "6")
    refresh     = os.getenv("EQIDV2_15M_REFRESH_TOKENS", "0").strip().lower()
    cmd = [
        PYTHON_EXE, "-u",
        str(BASE_DIR / "eqidv2_eod_scheduler_for_15mins_data.py"),
        "--max-workers", max_workers,
        "--buffer-sec",  buffer_sec,
    ]
    if refresh in ("1", "true", "yes", "on"):
        cmd.append("--refresh-tokens")
    return cmd


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 70, flush=True)
    print("EQIDV2 PARALLEL RUNNER V5", flush=True)
    print("Runs all three live-day processes simultaneously.", flush=True)
    print(flush=True)
    print(f"  Base dir    : {BASE_DIR}", flush=True)
    print(f"  Log dir     : {LOG_DIR}", flush=True)
    print(f"  Python      : {PYTHON_EXE}", flush=True)
    print(f"  Hard cutoff : {HARD_CUTOFF.strftime('%H:%M')} IST (force-kill)", flush=True)
    print(f"  Max restarts: {MAX_RESTARTS} per process", flush=True)
    print(flush=True)
    print("Processes:", flush=True)
    print("  [15M  ] eqidv2_eod_scheduler_for_15mins_data.py", flush=True)
    print("          crash_cutoff=15:50  log=eqidv2_eod_scheduler_for_15mins_data.log", flush=True)
    print("  [SHORT] eqidv2_live_combined_analyser_csv_v5_short.py", flush=True)
    print("          crash_cutoff=15:40  log=eqidv2_live_combined_analyser_csv_v5_short.log", flush=True)
    print("  [LONG ] eqidv2_live_combined_analyser_csv_v5_long.py", flush=True)
    print("          crash_cutoff=15:00  log=eqidv2_live_combined_analyser_csv_v5_long.log", flush=True)
    print("=" * 70, flush=True)
    print(flush=True)

    common = _common_env()

    processes: List[ManagedProcess] = [
        ManagedProcess(
            name="eod_15min_data",
            cmd=_15m_cmd(),
            env=_15m_env(),
            log_path=LOG_DIR / "eqidv2_eod_scheduler_for_15mins_data.log",
            # Scheduler's own HARD_STOP is 15:50 — don't restart crashes after that.
            crash_cutoff=dtime(15, 50),
            label="[15M  ] ",
        ),
        ManagedProcess(
            name="live_combined_v5_short",
            cmd=[PYTHON_EXE, "-u",
                 str(BASE_DIR / "eqidv2_live_combined_analyser_csv_v5_short.py")],
            env=common,
            log_path=LOG_DIR / "eqidv2_live_combined_analyser_csv_v5_short.log",
            # Matches END_CUTOFF_HHMM=1540 in run_eqidv2_live_combined_analyser_csv_v5_short.bat
            crash_cutoff=dtime(15, 40),
            label="[SHORT] ",
        ),
        ManagedProcess(
            name="live_combined_v5_long",
            cmd=[PYTHON_EXE, "-u",
                 str(BASE_DIR / "eqidv2_live_combined_analyser_csv_v5_long.py")],
            env=common,
            log_path=LOG_DIR / "eqidv2_live_combined_analyser_csv_v5_long.log",
            # Matches END_CUTOFF_HHMM=1500 in run_eqidv2_live_combined_analyser_csv_v5_long.bat
            crash_cutoff=dtime(15, 0),
            label="[LONG ] ",
        ),
    ]

    # ---- Start all three processes (slight stagger to avoid import/log races) ----
    for p in processes:
        p.start()
        time.sleep(0.5)

    # ---- Graceful Ctrl+C / SIGTERM ----
    def _shutdown(sig, frame):
        print("\n[RUNNER] Signal received -- stopping all subprocesses...", flush=True)
        for p in processes:
            p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _shutdown)

    print("[RUNNER] All processes started. Monitoring (poll every 10s)...", flush=True)
    print("[RUNNER] Press Ctrl+C to stop all processes.", flush=True)
    print(flush=True)

    # ---- Monitor loop ----
    while True:
        time.sleep(10)

        # Force-kill everything at hard cutoff (safety net).
        if _past(HARD_CUTOFF):
            print(
                f"[RUNNER] Hard cutoff {HARD_CUTOFF.strftime('%H:%M')} IST reached. "
                "Force-stopping remaining subprocesses.",
                flush=True,
            )
            for p in processes:
                if not p.is_done:
                    print(f"[RUNNER] Force-stopping: {p.name}", flush=True)
                    p.terminate()
            break

        all_done = True
        for p in processes:
            if not p.is_done:
                still_active = p.poll_and_maybe_restart()
                if still_active:
                    all_done = False

        if all_done:
            print("[RUNNER] All processes have exited naturally.", flush=True)
            break

    print(flush=True)
    print("[RUNNER] Done.", flush=True)


if __name__ == "__main__":
    main()
