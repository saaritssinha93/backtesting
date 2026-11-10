"""Supervise only the fixed FnO/cash data services without PowerShell.

No broker order or authorization entrypoints are accepted. Existing workers,
arguments, data validation and supervisor paths are retained. Windows file
sharing makes the lock interoperable with supervise_command.ps1.
"""
from __future__ import annotations

import argparse
import ctypes
from ctypes import wintypes as W
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import queue
import socket
import struct
import subprocess
import sys
import threading
import time
import uuid
import xml.etree.ElementTree as ET

IST = timezone(timedelta(hours=5, minutes=30))
ROOT = Path(__file__).resolve().parents[1]
SPECS = {
    "fetch": ("fno_oi_fetch_5min_fast_production", "1534", 0, (
        "--boundary-buffer-sec", "3", "--request-interval-sec", "0.36",
        "--workers-per-app", "2", "--writer-workers", "8", "--min-coverage", "0.99",
        "--bootstrap-days", "60", "--slot-retry-attempts", "2", "--slot-retry-delay-sec", "2")),
    "ranker": ("fno_oi_feature_ranker", "1536", 600, (
        "--workers", "8", "--top-n", "20", "--min-rank-coverage", "0.70",
        "--min-price-move-pct", "0.10", "--min-oi-move-pct", "0.25")),
    "cash5m": ("eqidv2_eod_scheduler_for_5mins_data_live_minimal", "1531", 720, (
        "--max-workers", "128", "--max-workers-per-app", "16", "--buffer-sec", "2",
        "--quarter-hour-buffer-sec", "2", "--enable-opening-slot-fetch")),
    "options": ("fno_options_atm_fetch_5min", "1534", 0, (
        "--mode", "live", "--intervals", "both", "--max-apps", "8",
        "--boundary-buffer-sec", "3", "--request-interval-sec", "0.34",
        "--workers-per-app", "2", "--writer-workers", "8", "--min-coverage", "0.99",
        "--slot-retry-attempts", "2")),
}
LEGACY_TASKS = ("EQIDV2_fno_oi_fetch_5min_0905", "EQIDV2_fno_oi_fetch_5min_fast_shadow_0906")


@dataclass(frozen=True)
class Config:
    service: str
    root: Path = ROOT
    runtime: Path = Path("C:/TradingData/eqidv2")
    max_restarts: int = 20
    restart_delay: float = 15
    interval: float = 5
    grace: float | None = None
    freshness: float | None = None
    cooldown_window: float = 300
    cooldown_count: int = 6
    cooldown_delay: float = 120

    @property
    def spec(self):
        return SPECS[self.service]

    @property
    def name(self):
        return self.spec[0]

    @property
    def log(self):
        return self.root / "logs" / f"{self.name}.log"

    @property
    def status(self):
        return self.root / "logs" / f"{self.name}.supervisor.status"

    @property
    def heartbeat(self):
        if self.service == "cash5m":
            return self.root / "logs" / f"{self.name}.status.json"
        return self.runtime / "runtime_status" / f"{self.name}.heartbeat"

    @property
    def grace_seconds(self):
        return self.grace if self.grace is not None else (1500 if self.service == "cash5m" else 180)

    @property
    def freshness_seconds(self):
        return self.freshness if self.freshness is not None else (780 if self.service == "cash5m" else 180)

    def command(self):
        args = [sys.executable, "-u", str(self.root / f"{self.name}.py"), *self.spec[3]]
        if self.service == "cash5m" and os.getenv("EQIDV2_5M_REFRESH_TOKENS", "").lower() in {"1", "true"}:
            args.append("--refresh-tokens")
        return args


def after_cutoff(config: Config, now: datetime | None = None) -> bool:
    return (now or datetime.now(IST)).astimezone(IST).strftime("%H%M") >= config.spec[1]


def native_processes() -> list[dict]:
    """Inspect shell/Python processes without WMI; never emit command lines."""
    if os.name != "nt":
        raise RuntimeError("This supervisor requires Windows native process checks")
    k = ctypes.WinDLL("kernel32", use_last_error=True)
    n = ctypes.WinDLL("ntdll")

    class Entry(ctypes.Structure):
        _fields_ = [("size", W.DWORD), ("usage", W.DWORD), ("pid", W.DWORD),
                    ("heap", ctypes.c_size_t), ("module", W.DWORD), ("threads", W.DWORD),
                    ("ppid", W.DWORD), ("priority", W.LONG), ("flags", W.DWORD), ("name", W.WCHAR * 260)]

    class Unicode(ctypes.Structure):
        _fields_ = [("length", W.USHORT), ("maximum", W.USHORT), ("buffer", ctypes.c_void_p)]

    k.CreateToolhelp32Snapshot.restype = W.HANDLE
    k.Process32FirstW.argtypes = k.Process32NextW.argtypes = [W.HANDLE, ctypes.POINTER(Entry)]
    k.OpenProcess.argtypes = [W.DWORD, W.BOOL, W.DWORD]
    k.OpenProcess.restype = W.HANDLE
    k.CloseHandle.argtypes = [W.HANDLE]
    n.NtQueryInformationProcess.argtypes = [W.HANDLE, W.ULONG, ctypes.c_void_p, W.ULONG, ctypes.POINTER(W.ULONG)]
    snapshot = k.CreateToolhelp32Snapshot(2, 0)
    if snapshot == ctypes.c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    rows = []
    try:
        entry = Entry(size=ctypes.sizeof(Entry))
        ok = k.Process32FirstW(snapshot, ctypes.byref(entry))
        while ok:
            row = {"pid": entry.pid, "ppid": entry.ppid, "name": entry.name, "command": ""}
            if entry.name.lower() in {"python.exe", "pythonw.exe", "py.exe", "powershell.exe", "pwsh.exe", "cmd.exe"}:
                handle = k.OpenProcess(0x1000, False, entry.pid)
                if handle:
                    try:
                        needed = W.ULONG()
                        n.NtQueryInformationProcess(handle, 60, None, 0, ctypes.byref(needed))
                        if 0 < needed.value < 1_000_000:
                            buf = ctypes.create_string_buffer(needed.value)
                            if n.NtQueryInformationProcess(handle, 60, buf, len(buf), ctypes.byref(needed)) >= 0:
                                value = Unicode.from_buffer(buf)
                                row["command"] = ctypes.wstring_at(value.buffer, value.length // 2)
                    finally:
                        k.CloseHandle(handle)
                else:
                    row["query_error"] = ctypes.get_last_error()
                # A vanished process is harmless. An unreadable live shell is not.
                if row.get("query_error") != 87:
                    row["inspect"] = True
            rows.append(row)
            ok = k.Process32NextW(snapshot, ctypes.byref(entry))
    finally:
        k.CloseHandle(snapshot)
    return rows


def conflicting_processes(config: Config, rows: list[dict], own_pid: int) -> list[dict]:
    by_pid = {row["pid"]: row for row in rows}
    ancestors = {own_pid}
    while own_pid in by_pid:
        own_pid = by_pid[own_pid]["ppid"]
        if own_pid in ancestors or not own_pid:
            break
        ancestors.add(own_pid)
    tokens = [f"{config.name}.py", f"run_{config.name}.bat"]
    if config.service == "fetch":
        tokens += ["fno_oi_fetch_5min.py", "fno_oi_fetch_5min_fast_shadow.py",
                   "run_fno_oi_fetch_5min.bat", "run_fno_oi_fetch_5min_fast_shadow.bat",
                   "assert_fno_oi_fast_production_trial_exclusive.ps1"]
    conflicts = []
    for row in rows:
        if row["pid"] in ancestors or not row.get("inspect"):
            continue
        command = row.get("command", "").lower()
        cash_rotation = (config.service == "cash5m" and "rotate_log_if_large.ps1" in command
                         and f"{config.name}.log" in command)
        if not command or cash_rotation or any(token in command for token in tokens):
            conflicts.append({"pid": row["pid"], "name": row["name"],
                              "reason": "unreadable_process" if not command else "existing_data_owner"})
    return conflicts


def assert_legacy_tasks_disabled() -> None:
    for task in LEGACY_TASKS:
        result = subprocess.run(["schtasks.exe", "/Query", "/TN", task, "/XML"],
                                capture_output=True, timeout=10, creationflags=subprocess.CREATE_NO_WINDOW)
        if result.returncode:
            raise RuntimeError(f"Cannot verify disabled legacy task {task}")
        xml = ET.fromstring(result.stdout.decode("utf-16" if result.stdout.startswith(b'\xff\xfe') else "utf-8-sig"))
        enabled = xml.find("{*}Settings/{*}Enabled")
        if enabled is None or enabled.text != "false":
            raise RuntimeError(f"Legacy writer task is not disabled: {task}")


def preflight(config: Config) -> None:
    if not (config.root / f"{config.name}.py").is_file():
        raise RuntimeError("Whitelisted worker file is missing")
    if config.service == "fetch":
        assert_legacy_tasks_disabled()
    conflicts = conflicting_processes(config, native_processes(), os.getpid())
    if conflicts:
        raise RuntimeError("Data owner conflict: " + json.dumps(conflicts))


class SingletonLock:
    def __init__(self, path: Path):
        self.path, self.stream = path, None

    def __enter__(self):
        import msvcrt
        k = ctypes.WinDLL("kernel32", use_last_error=True)
        k.CreateFileW.argtypes = [W.LPCWSTR, W.DWORD, W.DWORD, ctypes.c_void_p, W.DWORD, W.DWORD, W.HANDLE]
        k.CreateFileW.restype = W.HANDLE
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = k.CreateFileW(str(self.path), 0xC0000000, 0, None, 4, 0x80, None)
        if handle == ctypes.c_void_p(-1).value:
            raise ctypes.WinError(ctypes.get_last_error())
        self.stream = os.fdopen(msvcrt.open_osfhandle(handle, os.O_RDWR), "w", encoding="utf-8")
        self.stream.write(f"supervisor_pid={os.getpid()}\nacquired_utc={datetime.now(timezone.utc).isoformat()}\n")
        self.stream.flush()
        return self

    def __exit__(self, *_):
        if self.stream:
            self.stream.close()


class ChildJob:
    """Closing the supervisor's job handle also closes its owned worker."""
    def __init__(self):
        self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        self.kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, W.LPCWSTR]
        self.kernel.CreateJobObjectW.restype = W.HANDLE
        self.kernel.SetInformationJobObject.argtypes = [W.HANDLE, ctypes.c_int, ctypes.c_void_p, W.DWORD]
        self.kernel.AssignProcessToJobObject.argtypes = [W.HANDLE, W.HANDLE]
        self.kernel.CloseHandle.argtypes = [W.HANDLE]
        self.handle = self.kernel.CreateJobObjectW(None, None)
        if not self.handle:
            raise ctypes.WinError(ctypes.get_last_error())
        # JOBOBJECT_EXTENDED_LIMIT_INFORMATION: BasicLimitInformation.LimitFlags
        # has offset 16 on both Windows 32/64-bit; structure sizes differ.
        buf = ctypes.create_string_buffer(144 if ctypes.sizeof(ctypes.c_void_p) == 8 else 112)
        struct.pack_into("I", buf, 16, 0x2000)  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not self.kernel.SetInformationJobObject(self.handle, 9, buf, len(buf)):
            self.close()
            raise ctypes.WinError(ctypes.get_last_error())

    def assign(self, child):
        if not self.kernel.AssignProcessToJobObject(self.handle, W.HANDLE(int(child._handle))):
            raise ctypes.WinError(ctypes.get_last_error())

    def close(self):
        if self.handle:
            self.kernel.CloseHandle(self.handle)
            self.handle = None


def ntp_offset(timeout: float = 3) -> float | None:
    """Bound DNS + UDP together; preserve old unreachable=warn behavior."""
    results = queue.Queue()

    def query():
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.settimeout(timeout)
                sock.connect(("pool.ntp.org", 123))
                sent = time.time()
                sock.send(b"\x1b" + bytes(47))
                response = sock.recv(512)
                received = time.time()
                if len(response) < 48 or response[0] & 7 != 4 or not 1 <= response[1] <= 15:
                    raise ValueError("Invalid NTP response")
                seconds, fraction = struct.unpack("!II", response[40:48])
                server = seconds - 2208988800 + fraction / 2**32
                results.put(received - server - (received - sent) / 2)
        except (OSError, ValueError):
            results.put(None)
    threading.Thread(target=query, daemon=True).start()
    try:
        return results.get(timeout=timeout)
    except queue.Empty:
        return None


def atomic_kv(path: Path, values: dict) -> None:
    temp = path.with_name(path.name + f".{os.getpid()}.tmp")
    data = "\n".join(f"{key}={value}" for key, value in sorted(values.items())) + "\n"
    for attempt in range(4):
        try:
            temp.write_text(data, encoding="utf-8")
            os.replace(temp, path)
            return
        except OSError:
            if attempt == 3:
                raise
            time.sleep(0.05 * (attempt + 1))


def _read_worker_failure(
    config: Config, run_id: str, run_started: float, now: float,
    *, require_ranker_wait: bool = False,
) -> str:
    """Accept one stable, complete liveness read; a writer may be in flight."""
    try:
        before = config.heartbeat.stat()
        raw = config.heartbeat.read_text(encoding="utf-8-sig")
        after = config.heartbeat.stat()
        if (before.st_mtime_ns, before.st_size) != (after.st_mtime_ns, after.st_size):
            return "freshness_missing"
        if config.service == "cash5m":
            payload = json.loads(raw)
        else:
            # common.atomic_write_kv always terminates complete writes with a
            # newline. Its Windows direct-write fallback can expose a prefix.
            lines = raw.splitlines()
            if not raw.endswith("\n") or not lines or any("=" not in line for line in lines):
                return "freshness_missing"
            payload = dict(line.split("=", 1) for line in lines)
        if not isinstance(payload, dict) or not payload:
            return "freshness_missing"
    except (OSError, ValueError):
        return "freshness_missing"
    if after.st_mtime < run_started or now - after.st_mtime >= config.freshness_seconds:
        return "freshness_stale"
    # The cash worker predates run correlation and publishes JSON liveness.
    if config.service != "cash5m" and payload.get("run_id") != run_id:
        return "worker_run_id_mismatch"
    if require_ranker_wait and not (
        config.service == "ranker"
        and payload.get("session") == config.name
        and payload.get("state") == "WAITING"
        and payload.get("phase") == "WAIT_FETCH_MARKER"
    ):
        return "hung_idle"
    return ""


def worker_failure(config: Config, run_id: str, run_started: float, now: float) -> str:
    # Log inactivity and worker freshness have independent grace periods.
    log_stamp = config.log.stat().st_mtime if config.log.exists() else run_started
    log_idle = bool(config.spec[2] and now - max(run_started, log_stamp) >= config.spec[2])
    if log_idle and config.service != "ranker":
        return "hung_idle"
    if now - run_started < config.grace_seconds and not log_idle:
        return ""
    # Do not kill a healthy worker because one read overlaps its liveness
    # write. Retries are bounded; persistent missing/stale/wrong-run evidence
    # still rejects the worker after at most two 100ms waits.
    reason = "freshness_missing"
    for attempt in range(3):
        # The ranker emits no log while waiting for a complete fetch marker.
        # Exempt only that exact state after the same stable-read, freshness and
        # run-identity checks; all other idle or stale workers still fail closed.
        reason = _read_worker_failure(
            config, run_id, run_started, now, require_ranker_wait=log_idle,
        )
        if not reason:
            return ""
        if attempt < 2:
            time.sleep(0.1)
    return reason


def rotate_cash_log(config: Config, max_bytes: int = 104857600) -> Path | None:
    """Preserve the existing size threshold and timestamped, reversible rename."""
    if config.service != "cash5m" or not config.log.is_file() or config.log.stat().st_size < max_bytes:
        return None
    source = config.log.resolve()
    stamp = datetime.now(IST).strftime("%Y%m%d_%H%M%S")
    archive = Path(f"{source}.{stamp}.bak")
    if archive.exists():
        archive = Path(f"{source}.{stamp}.{os.getpid()}.bak")
    # Verify both absolute targets before the Windows move.
    for path in (source, archive.resolve()):
        path.relative_to((config.root / "logs").resolve())
    source.rename(archive)
    return archive


def write_state(config: Config, state: str, restart: int, run_id: str = "", child=None,
                started: str = "", reason: str = "", exit_code: int = 0):
    values = dict(name=f"{config.name}.py", status=state, state=state,
                  ts=datetime.now(IST).strftime("%Y-%m-%d_%H:%M:%S"),
                  updated_utc=datetime.now(timezone.utc).isoformat(),
                  supervisor_pid=os.getpid(), worker_pid=child.pid if child else "",
                  launcher_pid=child.pid if child else "", worker_start_utc=started,
                  launcher_start_utc=started, run_id=run_id, restart_count=restart,
                  reason=reason, exit_code=exit_code, log_file=str(config.log),
                  status_file=str(config.status), cutoff_hhmm=config.spec[1],
                  lock_file=str(config.status) + ".lock", heartbeat_file=str(config.status).replace(".status", ".heartbeat"),
                  worker_heartbeat_file=str(config.heartbeat), supervisor_log_file=str(config.log) + ".supervisor.log")
    atomic_kv(config.status, values)
    atomic_kv(Path(str(config.status).replace(".status", ".heartbeat")), values)
    atomic_kv(Path(str(config.status) + ".spawn"), values)


def diagnostic(config: Config, message: str):
    line = f"{datetime.now(IST):%Y-%m-%d %H:%M:%S} | {message}"
    print(line, flush=True)
    with Path(str(config.log) + ".supervisor.log").open("a", encoding="utf-8") as stream:
        stream.write(line + "\n")


def stop_owned_child(child):
    if child.poll() is None:
        child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)


def run(config: Config) -> int:
    config.log.parent.mkdir(parents=True, exist_ok=True)
    with SingletonLock(Path(str(config.status) + ".lock")):
        preflight(config)
        archive = rotate_cash_log(config)
        if archive:
            diagnostic(config, f"INFO Rotated oversized cash log to {archive.name}")
        offset = ntp_offset()
        if offset is not None and abs(offset) > 30:
            write_state(config, "FAILED", 0, reason="ntp_drift_exceeded", exit_code=2)
            diagnostic(config, f"ERROR NTP drift {offset:.3f}s exceeds 30s; refusing data start")
            return 2
        diagnostic(config, "WARN NTP unreachable; proceeding as before" if offset is None else f"INFO NTP drift {offset:.3f}s")
        restart, events = 0, []
        while True:
            if after_cutoff(config):
                write_state(config, "SKIPPED_CUTOFF", restart, reason="after_cutoff")
                return 0
            # Recheck after backoff: a legacy shell must never create another writer.
            preflight(config)
            run_id = uuid.uuid4().hex
            env = dict(os.environ, EQIDV2_RUNTIME_ROOT=str(config.runtime), PYTHONUNBUFFERED="1",
                       PYTHONIOENCODING="utf-8", EQIDV2_OBS_RUN_ID=run_id, EQIDV2_OBS_SERVICE=f"{config.name}.py")
            job, child = ChildJob(), None
            reason = ""
            started_at = time.time()
            started = datetime.fromtimestamp(started_at, timezone.utc).isoformat()
            try:
                with config.log.open("ab", buffering=0) as log:
                    child = subprocess.Popen(config.command(), cwd=config.root, env=env,
                                             stdout=log, stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW)
                    job.assign(child)
                    diagnostic(config, f"INFO START {config.name} worker_pid={child.pid} run_id={run_id} restart={restart}")
                    while child.poll() is None:
                        write_state(config, "RUNNING", restart, run_id, child, started, "monitoring")
                        reason = worker_failure(config, run_id, started_at, time.time())
                        if reason:
                            stop_owned_child(child)
                            break
                        time.sleep(config.interval)
                    code = 124 if reason else child.returncode
                    reason = reason or "normal_exit"
            finally:
                if child is not None:
                    stop_owned_child(child)
                job.close()
            write_state(config, "SUCCESS" if code == 0 else "EXITED", restart, run_id, child, started, reason, code)
            diagnostic(config, f"INFO END {config.name} code={code} reason={reason}")
            if code == 0:
                return 0
            if after_cutoff(config):
                write_state(config, "STOPPED_AFTER_CUTOFF", restart, reason="after_cutoff_nonzero_exit")
                return 0
            restart += 1
            if restart > config.max_restarts:
                write_state(config, "FAILED", restart, reason="max_restarts_exceeded", exit_code=code)
                return max(1, min(code, 255))
            events.append(time.monotonic())
            events = [stamp for stamp in events if time.monotonic() - stamp < config.cooldown_window]
            if len(events) >= config.cooldown_count:
                write_state(config, "COOLDOWN", restart, reason="restart_storm")
                time.sleep(config.cooldown_delay)
            write_state(config, "RESTARTING", restart, reason="backoff")
            time.sleep(config.restart_delay)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--service", required=True, choices=tuple(SPECS))
    parser.add_argument("--check", action="store_true", help="Read-only exclusivity and launcher check")
    args = parser.parse_args(argv)
    config = Config(args.service)
    try:
        if args.check:
            preflight(config)
            print(json.dumps({"service": args.service, "preflight": "PASS", "after_cutoff": after_cutoff(config)}))
            return 0
        return run(config)
    except Exception as exc:
        # Avoid writing over another active supervisor's shared status on conflicts.
        print(f"[DATA-SUPERVISOR][FAIL] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
