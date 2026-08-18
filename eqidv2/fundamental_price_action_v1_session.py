from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from eqidv2_runtime_paths import RUNTIME_STATUS_DIR, runtime_dir


STRATEGY = "fundamental_price_action_v1"
SCRIPT_NAME = Path(__file__).name
IST = ZoneInfo("Asia/Kolkata")
SLOT_MINUTES = 5
# Windows returns WinError 5 from MoveFileEx when a reader (the dashboard, or
# OneDrive/Defender) holds the destination open without FILE_SHARE_DELETE. That
# is a transient sharing race, not a real failure: retry, then fall back to a
# direct write, and never let a status file take the trading loop down.
STATUS_WRITE_ATTEMPTS = 5
STATUS_WRITE_BACKOFF_SECONDS = 0.2
# Statuses that mean "the session is still looping"; the heartbeat reports
# RUNNING for these so the dashboard liveness check matches v7 semantics.
SESSION_LIVE_STATUSES = frozenset(
    {"RUNNING", "SUCCESS", "FAILED", "WAITING_FETCH", "SCHEDULED"}
)
MAX_CONSECUTIVE_LOOP_ERRORS = 20
DEFAULT_LIVE_FOLDER = runtime_dir("stocks_indicators_5min_eq_live")
DEFAULT_SLOT_DIR = runtime_dir("slot_ready_5m")
DEFAULT_OUTPUT_ROOT = runtime_dir(STRATEGY)
DEFAULT_WORKSPACE = Path(
    r"C:\Users\Saarit\OneDrive\Desktop\Trading\Short_term_trading"
)
DEFAULT_STRATEGY_SCRIPT = DEFAULT_WORKSPACE / "run_intraday_forensic_entries.py"
DEFAULT_FORENSIC_ROOT = DEFAULT_WORKSPACE / "filtered_stocks_MIS_v2_data_nse"
STATUS_PATH = RUNTIME_STATUS_DIR / f"{STRATEGY}.status"
HEARTBEAT_PATH = RUNTIME_STATUS_DIR / f"{STRATEGY}.heartbeat"
# Side-split entry sheets, mirroring the v7 live_signals contract so the
# dashboard cards and the paper-trade runner consume one familiar shape.
LIVE_SIGNAL_DIR = runtime_dir("live_signals")
LIVE_ENTRY_SLUG = "fpa_v1"
# Intraday MIS: Rs 10,000 of own capital per trade at 5x leverage, so each
# position carries Rs 50,000 of exposure. Quantity follows the exposure;
# percentage returns are reported against both (price move vs return on
# capital) so the leverage is never hidden.
CAPITAL_PER_TRADE_RS = float(
    os.getenv("EQIDV2_FPA_V1_CAPITAL_RS", "10000") or "10000"
)
LEVERAGE = float(os.getenv("EQIDV2_FPA_V1_LEVERAGE", "5") or "5")
EXPOSURE_PER_TRADE_RS = CAPITAL_PER_TRADE_RS * LEVERAGE
LIVE_ENTRY_FIELDS = [
    "signal_id",
    "signal_datetime",
    "detected_time_ist",
    "ticker",
    "side",
    "entry_price",
    "target_price",
    "stop_price",
    "quantity",
    "capital_rs",
    "leverage",
    "exposure_rs",
    "company_name",
    "signal_score",
    "forensic_verdict",
    "final_score",
    "market_regime",
    "valid_until",
    "square_off_time",
    "fetch_slot_ist",
    "strategy",
]
OUTPUT_FIELDS = [
    "fetch_slot_ist",
    "strategy",
    "side",
    "status",
    "symbol",
    "company_name",
    "entry_trigger",
    "stop_loss",
    "target_2r",
    "risk_pct",
    "reward_risk",
    "signal_score",
    "forensic_verdict",
    "final_score",
    "daily_technical_classification",
    "weighted_expected_return",
    "market_regime",
    "data_as_of",
    "valid_until",
    "square_off_time",
    "reasons",
]


def warn(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def discard(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


def atomic_replace(temporary: Path, path: Path) -> None:
    """os.replace, retried through transient Windows sharing violations."""
    last_error: OSError | None = None
    for attempt in range(STATUS_WRITE_ATTEMPTS):
        try:
            os.replace(temporary, path)
            return
        except OSError as exc:
            last_error = exc
            if attempt + 1 < STATUS_WRITE_ATTEMPTS:
                time.sleep(STATUS_WRITE_BACKOFF_SECONDS * (attempt + 1))
    discard(temporary)
    assert last_error is not None
    raise last_error


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(text, encoding="utf-8")
    except OSError:
        discard(temporary)
        raise
    atomic_replace(temporary, path)


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2, ensure_ascii=True))


def kv_value(value: Any) -> str:
    return " ".join(str(value).split())


def write_runtime_kv(path: Path, fields: dict[str, Any]) -> bool:
    """Best-effort `key=value` status write. Returns False, never raises.

    The format matches the dashboard's parse_status_file and the contract
    Signal discovery v7 5mins ID already uses. A status file is telemetry:
    losing one must never abort a trading scan.
    """
    payload = "".join(
        f"{key}={kv_value(value)}\n"
        for key, value in fields.items()
        if value is not None and str(value) != ""
    )
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary.write_text(payload, encoding="utf-8")
        atomic_replace(temporary, path)
        return True
    except OSError as replace_error:
        discard(temporary)
        # The destination can be writable but not replaceable when a reader
        # holds it open; a direct in-place write still lands.
        try:
            path.write_text(payload, encoding="utf-8")
            return True
        except OSError as direct_error:
            warn(
                f"[STATUS][WARN] could not update {path.name}: "
                f"replace={replace_error} direct={direct_error}"
            )
            return False


def now_ist() -> dt.datetime:
    return dt.datetime.now(IST)


PROCESS_START_IST = now_ist()


def slot_floor(moment: dt.datetime) -> dt.datetime:
    floored = moment.astimezone(IST).replace(second=0, microsecond=0)
    return floored - dt.timedelta(minutes=floored.minute % SLOT_MINUTES)


def parse_clock(value: str) -> dt.time:
    return dt.datetime.strptime(value, "%H:%M").time()


def parse_marker_timestamp(value: str) -> dt.datetime:
    timestamp = dt.datetime.fromisoformat(str(value).strip())
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=IST)
    return timestamp.astimezone(IST)


def marker_is_fully_successful(payload: dict[str, Any]) -> tuple[bool, str]:
    expected = int(payload.get("tickers_expected", 0) or 0)
    written = int(payload.get("tickers_written", 0) or 0)
    completed = int(payload.get("tickers_complete", 0) or 0)
    failed = int(payload.get("tickers_failed", 0) or 0)
    verification_failed = int(payload.get("verification_failed_count", 0) or 0)
    unresolved = int(payload.get("unresolved_symbol_count", 0) or 0)
    complete = payload.get("complete") is True
    valid = bool(
        complete
        and expected > 0
        and written == expected
        and completed == expected
        and failed == 0
        and verification_failed == 0
        and unresolved == 0
    )
    reason = (
        f"complete={complete} written={written}/{expected} completed={completed}/{expected} "
        f"failed={failed} verification_failed={verification_failed} unresolved={unresolved}"
    )
    return valid, reason


def read_marker(path: Path) -> tuple[dict[str, Any], dt.datetime]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("slot marker is not a JSON object")
    slot = parse_marker_timestamp(str(payload.get("slot_ist", "")))
    return payload, slot


def status_payload(status: str, **extra: Any) -> dict[str, Any]:
    now = now_ist()
    payload: dict[str, Any] = {
        "status": status,
        "script": SCRIPT_NAME,
        "session": STRATEGY,
        "strategy": STRATEGY,
        "pid": os.getpid(),
        "ts": now.strftime("%Y-%m-%d_%H:%M:%S"),
        "ts_iso": now.isoformat(),
        "start_ts": PROCESS_START_IST.strftime("%Y-%m-%d_%H:%M:%S"),
    }
    payload.update(extra)
    return payload


def heartbeat_state_for(status: str, override: str | None) -> str:
    if override:
        return override
    return "RUNNING" if status in SESSION_LIVE_STATUSES else status


def publish_status(status: str, *, heartbeat_state: str | None = None, **extra: Any) -> None:
    payload = status_payload(status, **extra)
    write_runtime_kv(STATUS_PATH, payload)
    write_runtime_kv(
        HEARTBEAT_PATH,
        {"state": heartbeat_state_for(status, heartbeat_state), **payload},
    )


def publish_heartbeat(status: str, *, heartbeat_state: str | None = None, **extra: Any) -> None:
    payload = status_payload(status, **extra)
    write_runtime_kv(
        HEARTBEAT_PATH,
        {"state": heartbeat_state_for(status, heartbeat_state), **payload},
    )


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def validate_strategy_rows(rows: list[dict[str, str]]) -> None:
    if len(rows) != 2:
        raise ValueError(f"strategy output must contain two rows, found {len(rows)}")
    if [row.get("side", "").upper() for row in rows] != ["LONG", "SHORT"]:
        raise ValueError("strategy output must contain LONG then SHORT")
    if any(row.get("status", "").upper() not in {"READY", "NO_SETUP"} for row in rows):
        raise ValueError("strategy output contains an unsupported status")


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({field: row.get(field, "") for field in OUTPUT_FIELDS})
    except OSError:
        discard(temporary)
        raise
    atomic_replace(temporary, path)


def daily_rows(slots_dir: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in sorted(slots_dir.glob(f"{STRATEGY}_*.csv")):
        slot_key = path.stem.rsplit("_", 1)[-1]
        try:
            slot_rows = load_csv_rows(path)
            validate_strategy_rows(slot_rows)
        except (OSError, csv.Error, ValueError):
            continue
        for row in slot_rows:
            row["fetch_slot_ist"] = row.get("fetch_slot_ist") or slot_key
            row["strategy"] = STRATEGY
            rows.append(row)
    return rows


def render_daily_report(session_date: dt.date, rows: list[dict[str, str]]) -> str:
    latest_slot = rows[-1].get("fetch_slot_ist", "") if rows else "Waiting"
    lines = [
        f"# {STRATEGY}",
        "",
        f"Session date: {session_date.isoformat()}",
        f"Latest fully successful fetch slot: {latest_slot}",
        "Dependency: Live Data Fetch (5mins) authoritative completion marker.",
        "Policy: at most one LONG and one SHORT candidate per five-minute slot.",
        "",
        "| Fetch slot | Side | Status | Symbol | Entry | Stop | Target | Score | Forensic |",
        "|---|---|---|---|---:|---:|---:|---:|---|",
    ]
    for row in rows[-120:]:
        lines.append(
            "| {fetch_slot_ist} | {side} | {status} | {symbol} | {entry_trigger} | "
            "{stop_loss} | {target_2r} | {signal_score} | {forensic_verdict} |".format(
                **{field: str(row.get(field, "")).replace("|", "/") for field in OUTPUT_FIELDS}
            )
        )
    if not rows:
        lines.append("| - | LONG/SHORT | WAITING_FETCH | - | - | - | - | - | - |")
    return "\n".join(lines) + "\n"


def refresh_daily_outputs(
    output_root: Path, session_date: dt.date, session_dir: Path, slots_dir: Path
) -> tuple[Path, Path, list[dict[str, str]]]:
    rows = daily_rows(slots_dir)
    daily_csv = session_dir / f"{STRATEGY}_entries.csv"
    daily_md = session_dir / f"{STRATEGY}.md"
    write_csv(daily_csv, rows)
    atomic_write_text(daily_md, render_daily_report(session_date, rows))

    latest_dir = output_root / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    latest_csv = latest_dir / f"latest_{STRATEGY}.csv"
    latest_md = latest_dir / f"latest_{STRATEGY}.md"
    # The dashboard serves these two files, so it can hold them open exactly
    # when we publish. Stage beside the target and use the retrying replace.
    publish_copy(daily_csv, latest_csv)
    publish_copy(daily_md, latest_md)
    return latest_csv, latest_md, rows


def publish_copy(source: Path, destination: Path) -> None:
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    try:
        shutil.copyfile(source, temporary)
    except OSError:
        discard(temporary)
        raise
    atomic_replace(temporary, destination)


def safe_refresh_daily_outputs(
    output_root: Path, session_date: dt.date, session_dir: Path, slots_dir: Path
) -> tuple[Path, Path, list[dict[str, str]]] | None:
    """Refresh the rollup, but never abort the scan loop if publishing fails."""
    try:
        return refresh_daily_outputs(output_root, session_date, session_dir, slots_dir)
    except Exception as exc:
        warn(f"[REPORT][WARN] daily rollup refresh failed: {type(exc).__name__}: {exc}")
        return None


def run_strategy_for_marker(
    marker_path: Path,
    slot: dt.datetime,
    args: argparse.Namespace,
    session_dir: Path,
    slots_dir: Path,
) -> tuple[list[dict[str, str]], Path]:
    slot_key = slot.strftime("%H%M")
    output_csv = slots_dir / f"{STRATEGY}_{slot_key}.csv"
    output_md = output_csv.with_suffix(".md")
    if output_csv.exists():
        rows = load_csv_rows(output_csv)
        validate_strategy_rows(rows)
        return rows, output_csv

    strategy_as_of = slot + dt.timedelta(seconds=args.close_grace_seconds + 1)
    command = [
        str(args.python_exe),
        "-u",
        str(args.strategy_script),
        "--live-folder",
        str(args.live_folder),
        "--forensic-root",
        str(args.forensic_root),
        "--as-of",
        strategy_as_of.isoformat(),
        "--close-grace-seconds",
        str(args.close_grace_seconds),
        "--entry-cutoff",
        args.entry_cutoff,
        "--output",
        str(output_csv),
    ]
    print(
        f"[RUN] {STRATEGY} fetch_slot={slot.isoformat()} marker={marker_path.name}",
        flush=True,
    )
    completed = subprocess.run(
        command,
        cwd=str(args.strategy_script.parent),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=args.strategy_timeout_seconds,
        check=False,
    )
    if completed.stdout:
        print(completed.stdout.rstrip(), flush=True)
    if completed.stderr:
        print(completed.stderr.rstrip(), file=sys.stderr, flush=True)
    if completed.returncode != 0:
        raise RuntimeError(f"strategy exited with code {completed.returncode}")
    if not output_csv.exists() or not output_md.exists():
        raise RuntimeError("strategy did not create both CSV and Markdown outputs")
    rows = load_csv_rows(output_csv)
    validate_strategy_rows(rows)
    for row in rows:
        row["fetch_slot_ist"] = slot.isoformat()
        row["strategy"] = STRATEGY
    write_csv(output_csv, rows)
    return rows, output_csv


def live_entry_path(session_date: dt.date, side: str) -> Path:
    return LIVE_SIGNAL_DIR / (
        f"signals_{session_date.isoformat()}_{LIVE_ENTRY_SLUG}_{side.lower()}.csv"
    )


def _to_float(value: Any) -> float | None:
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if number == number else None  # drop NaN


def live_entry_row(row: dict[str, str], slot: dt.datetime) -> dict[str, str] | None:
    """Project one READY strategy row onto the live-entry contract.

    NO_SETUP rows carry no tradable price, so they never reach the entry sheet.
    """
    if str(row.get("status", "")).strip().upper() != "READY":
        return None
    symbol = str(row.get("symbol", "")).strip()
    entry = _to_float(row.get("entry_trigger"))
    if not symbol or entry is None or entry <= 0:
        return None
    quantity = int(EXPOSURE_PER_TRADE_RS // entry)
    if quantity < 1:
        return None
    side = str(row.get("side", "")).strip().upper()
    detected = row.get("data_as_of") or slot.isoformat()
    return {
        "signal_id": f"{slot.strftime('%Y%m%d_%H%M')}|{side}|{symbol}",
        "signal_datetime": slot.isoformat(),
        "detected_time_ist": str(detected),
        "ticker": symbol,
        "side": side,
        "entry_price": f"{entry:.2f}",
        "target_price": str(row.get("target_2r", "")),
        "stop_price": str(row.get("stop_loss", "")),
        "quantity": str(quantity),
        "capital_rs": f"{CAPITAL_PER_TRADE_RS:.2f}",
        "leverage": f"{LEVERAGE:g}",
        "exposure_rs": f"{quantity * entry:.2f}",
        "company_name": str(row.get("company_name", "")),
        "signal_score": str(row.get("signal_score", "")),
        "forensic_verdict": str(row.get("forensic_verdict", "")),
        "final_score": str(row.get("final_score", "")),
        "market_regime": str(row.get("market_regime", "")),
        "valid_until": str(row.get("valid_until", "")),
        "square_off_time": str(row.get("square_off_time", "")),
        "fetch_slot_ist": slot.isoformat(),
        "strategy": STRATEGY,
    }


def append_live_entries(
    rows: list[dict[str, str]], slot: dt.datetime, session_date: dt.date
) -> dict[str, int]:
    """Append this slot's READY entries to the per-side sheets.

    Appends are idempotent on signal_id so a replayed slot cannot double-book
    a paper trade.
    """
    written: dict[str, int] = {}
    for side in ("LONG", "SHORT"):
        candidate = next(
            (
                entry
                for entry in (
                    live_entry_row(row, slot)
                    for row in rows
                    if str(row.get("side", "")).strip().upper() == side
                )
                if entry is not None
            ),
            None,
        )
        if candidate is None:
            continue
        path = live_entry_path(session_date, side)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            existing_ids: set[str] = set()
            if path.exists():
                existing_ids = {
                    str(existing.get("signal_id", ""))
                    for existing in load_csv_rows(path)
                }
            if candidate["signal_id"] in existing_ids:
                continue
            write_header = not path.exists() or path.stat().st_size == 0
            with path.open("a", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(
                    handle, fieldnames=LIVE_ENTRY_FIELDS, extrasaction="ignore"
                )
                if write_header:
                    writer.writeheader()
                writer.writerow(candidate)
            written[side] = written.get(side, 0) + 1
        except (OSError, csv.Error) as exc:
            warn(f"[ENTRIES][WARN] {side} sheet update failed: {type(exc).__name__}: {exc}")
    return written


def rebuild_live_entries(session_date: dt.date, slots_dir: Path) -> dict[str, int]:
    """Regenerate both entry sheets from the per-slot strategy outputs."""
    totals: dict[str, int] = {}
    for side in ("LONG", "SHORT"):
        discard(live_entry_path(session_date, side))
    for path in sorted(slots_dir.glob(f"{STRATEGY}_*.csv")):
        slot_key = path.stem.rsplit("_", 1)[-1]
        try:
            rows = load_csv_rows(path)
            validate_strategy_rows(rows)
        except (OSError, csv.Error, ValueError):
            continue
        raw_slot = str(rows[0].get("fetch_slot_ist", "")).strip()
        try:
            slot = parse_marker_timestamp(raw_slot)
        except ValueError:
            try:
                slot = dt.datetime.combine(
                    session_date, dt.datetime.strptime(slot_key, "%H%M").time(), tzinfo=IST
                )
            except ValueError:
                continue
        for side, count in append_live_entries(rows, slot, session_date).items():
            totals[side] = totals.get(side, 0) + count
    return totals


def marker_paths_for_date(slot_dir: Path, session_date: dt.date) -> list[Path]:
    return sorted(slot_dir.glob(f"slot_{session_date.strftime('%Y%m%d')}_*.json"))


def run_session(args: argparse.Namespace) -> int:
    current = now_ist()
    session_date = (
        dt.date.fromisoformat(args.session_date) if args.session_date else current.date()
    )
    if session_date.weekday() >= 5 and not args.allow_weekend:
        publish_status("SKIPPED_WEEKEND", session_date_ist=session_date.isoformat())
        return 0
    if not args.strategy_script.is_file():
        raise FileNotFoundError(f"Strategy script not found: {args.strategy_script}")
    if not args.live_folder.is_dir():
        raise FileNotFoundError(f"Live data folder not found: {args.live_folder}")

    session_dir = args.output_root / "sessions" / session_date.isoformat()
    slots_dir = session_dir / "slots"
    slots_dir.mkdir(parents=True, exist_ok=True)
    if args.rebuild_entries:
        totals = rebuild_live_entries(session_date, slots_dir)
        print(
            "[ENTRIES][REBUILD] "
            + (
                " ".join(f"{side}={count}" for side, count in sorted(totals.items()))
                or "no READY entries found"
            ),
            flush=True,
        )
        return 0
    latest_dir = args.output_root / "latest"
    latest_csv = latest_dir / f"latest_{STRATEGY}.csv"
    latest_md = latest_dir / f"latest_{STRATEGY}.md"
    refreshed = safe_refresh_daily_outputs(
        args.output_root, session_date, session_dir, slots_dir
    )
    if refreshed is not None:
        latest_csv, latest_md, _ = refreshed
    publish_status(
        "RUNNING",
        session_date_ist=session_date.isoformat(),
        phase="STARTUP",
        message="Waiting for fully successful Live Data Fetch (5mins) markers.",
        output=str(latest_csv),
    )

    start_clock = parse_clock(args.start_time)
    first_slot_clock = parse_clock(args.first_entry_slot)
    end_clock = parse_clock(args.end_time)
    session_deadline = dt.datetime.combine(session_date, end_clock, tzinfo=IST)
    processed: set[str] = {
        path.stem.rsplit("_", 1)[-1]
        for path in slots_dir.glob(f"{STRATEGY}_*.csv")
    }
    failures: dict[str, int] = {}
    warned_incomplete_markers: dict[str, int] = {}

    # A mid-session restart should resume from the current market state, not
    # spend several minutes replaying every unprocessed morning marker.
    if (
        not args.catch_up
        and not args.once
        and current.date() == session_date
        and current.time() >= first_slot_clock
    ):
        successful_existing: list[tuple[Path, dt.datetime]] = []
        for marker_path in marker_paths_for_date(args.slot_dir, session_date):
            try:
                payload, slot = read_marker(marker_path)
            except (OSError, ValueError, json.JSONDecodeError):
                continue
            marker_ok, _ = marker_is_fully_successful(payload)
            if marker_ok and first_slot_clock <= slot.time() <= end_clock:
                successful_existing.append((marker_path, slot))
        for _, slot in successful_existing[:-1]:
            processed.add(slot.strftime("%H%M"))
        if len(successful_existing) > 1:
            print(
                f"[RESUME] Skipped {len(successful_existing) - 1} historical successful markers; "
                f"resuming from {successful_existing[-1][1].isoformat()}.",
                flush=True,
            )

    live_mode = not args.once and not args.catch_up
    warned_late_slots: set[str] = set()
    loop_errors = 0

    def finish() -> int:
        refreshed = safe_refresh_daily_outputs(
            args.output_root, session_date, session_dir, slots_dir
        )
        done_csv, done_md, all_rows = refreshed or (latest_csv, latest_md, [])
        publish_status(
            "DONE",
            heartbeat_state="DONE",
            session_date_ist=session_date.isoformat(),
            phase="END_TIME",
            processed_slots=len(processed),
            daily_rows=len(all_rows),
            output=str(done_csv),
            report=str(done_md),
            message=f"Session ended at {args.end_time}.",
        )
        return 0

    while True:
        try:
            current = now_ist()
            if not args.once and current >= session_deadline:
                return finish()

            if not args.once and current.date() == session_date and current.time() < start_clock:
                publish_heartbeat(
                    "SCHEDULED",
                    session_date_ist=session_date.isoformat(),
                    phase="SCHEDULED",
                    message=f"Waiting for {args.start_time}.",
                    processed_slots=len(processed),
                )
                time.sleep(args.poll_seconds)
                continue

            eligible: list[tuple[Path, dict[str, Any], dt.datetime]] = []
            for marker_path in marker_paths_for_date(args.slot_dir, session_date):
                try:
                    payload, slot = read_marker(marker_path)
                except (OSError, ValueError, json.JSONDecodeError):
                    continue
                slot_key = slot.strftime("%H%M")
                if slot_key in processed or not (first_slot_clock <= slot.time() <= end_clock):
                    continue
                marker_ok, marker_reason = marker_is_fully_successful(payload)
                if not marker_ok:
                    try:
                        marker_mtime = marker_path.stat().st_mtime_ns
                    except OSError:
                        continue
                    if warned_incomplete_markers.get(marker_path.name) != marker_mtime:
                        print(
                            f"[WAIT] {marker_path.name} is not fully successful: {marker_reason}",
                            flush=True,
                        )
                        warned_incomplete_markers[marker_path.name] = marker_mtime
                    continue
                warned_incomplete_markers.pop(marker_path.name, None)
                eligible.append((marker_path, payload, slot))

            if args.once and eligible:
                eligible = [eligible[-1]]

            # Timeliness watch: the feed owes us a marker shortly after every
            # slot boundary. Say so once when it is overdue, the way v7's feed
            # gate does, instead of waiting silently.
            if live_mode:
                due_slot = slot_floor(current)
                due_key = due_slot.strftime("%H%M")
                due_lag = (current - due_slot).total_seconds()
                if (
                    first_slot_clock <= due_slot.time() <= end_clock
                    and due_key not in processed
                    and due_key not in warned_late_slots
                    and due_lag > args.marker_late_warn_seconds
                    and not any(slot == due_slot for _, _, slot in eligible)
                ):
                    warned_late_slots.add(due_key)
                    print(
                        f"[FEED][WARN] slot {due_slot.strftime('%H:%M')} has no fully "
                        f"successful marker after {due_lag:.0f}s",
                        flush=True,
                    )

            # Freshness first: a slow or retried slot must not push the live
            # scan further behind. Drop backlog beyond the lag budget, keeping
            # the newest marker so the session always reports current state.
            if live_mode and args.max_slot_lag_seconds > 0 and len(eligible) > 1:
                keep: list[tuple[Path, dict[str, Any], dt.datetime]] = []
                for index, item in enumerate(eligible):
                    slot = item[2]
                    lag = (current - slot).total_seconds()
                    if index < len(eligible) - 1 and lag > args.max_slot_lag_seconds:
                        processed.add(slot.strftime("%H%M"))
                        print(
                            f"[SKIP][STALE] slot={slot.strftime('%H:%M')} is {lag:.0f}s old "
                            f"(budget {args.max_slot_lag_seconds:.0f}s); scanning the newest "
                            f"slot instead",
                            flush=True,
                        )
                        continue
                    keep.append(item)
                eligible = keep

            for marker_path, payload, slot in eligible:
                slot_key = slot.strftime("%H%M")
                marker_lag = (now_ist() - slot).total_seconds()
                publish_status(
                    "RUNNING",
                    session_date_ist=session_date.isoformat(),
                    phase="SCAN",
                    slot=slot.isoformat(),
                    marker=marker_path.name,
                    marker_lag_sec=f"{marker_lag:.0f}",
                    processed_slots=len(processed),
                    message="Running after a fully successful Live Data Fetch (5mins) marker.",
                )
                scan_started_at = now_ist()
                started = time.monotonic()
                print(
                    f"[SCAN][START] slot={slot.strftime('%H:%M')} "
                    f"start={scan_started_at.strftime('%H:%M:%S')} "
                    f"marker_lag={marker_lag:.0f}s marker={marker_path.name}",
                    flush=True,
                )
                try:
                    rows, slot_output = run_strategy_for_marker(
                        marker_path, slot, args, session_dir, slots_dir
                    )
                    scan_seconds = time.monotonic() - started
                    processed.add(slot_key)
                    failures.pop(slot_key, None)
                    entries_written = append_live_entries(rows, slot, session_date)
                    if entries_written:
                        print(
                            "[ENTRIES] slot={} {}".format(
                                slot.strftime("%H:%M"),
                                " ".join(
                                    f"{side}={count}"
                                    for side, count in sorted(entries_written.items())
                                ),
                            ),
                            flush=True,
                        )
                    refreshed = safe_refresh_daily_outputs(
                        args.output_root, session_date, session_dir, slots_dir
                    )
                    if refreshed is not None:
                        latest_csv, latest_md, all_rows = refreshed
                    else:
                        all_rows = []
                    by_side = {row["side"]: row for row in rows}
                    long_row = by_side["LONG"]
                    short_row = by_side["SHORT"]
                    publish_status(
                        "SUCCESS",
                        session_date_ist=session_date.isoformat(),
                        phase="SCAN_DONE",
                        slot=slot.isoformat(),
                        marker=marker_path.name,
                        marker_lag_sec=f"{marker_lag:.0f}",
                        scan_seconds=f"{scan_seconds:.1f}",
                        fetch_complete=payload.get("complete"),
                        tickers_written=payload.get("tickers_written"),
                        processed_slots=len(processed),
                        daily_rows=len(all_rows),
                        long_status=long_row.get("status", ""),
                        long_symbol=long_row.get("symbol", ""),
                        short_status=short_row.get("status", ""),
                        short_symbol=short_row.get("symbol", ""),
                        output=str(latest_csv),
                        report=str(latest_md),
                        slot_output=str(slot_output),
                    )
                    print(
                        f"[SUCCESS] slot={slot.isoformat()} LONG={long_row.get('status')}:{long_row.get('symbol')} "
                        f"SHORT={short_row.get('status')}:{short_row.get('symbol')}",
                        flush=True,
                    )
                    scan_ended_at = now_ist()
                    print(
                        f"[SCAN][END] slot={slot.strftime('%H:%M')} "
                        f"start={scan_started_at.strftime('%H:%M:%S')} "
                        f"end={scan_ended_at.strftime('%H:%M:%S')} "
                        f"elapsed={scan_seconds:.1f}s",
                        flush=True,
                    )
                    print(
                        f"[TIMING] slot={slot.strftime('%H:%M')} marker_lag={marker_lag:.0f}s "
                        f"scan={scan_seconds:.1f}s total={marker_lag + scan_seconds:.0f}s",
                        flush=True,
                    )
                except Exception as exc:
                    scan_seconds = time.monotonic() - started
                    print(
                        f"[SCAN][END] slot={slot.strftime('%H:%M')} "
                        f"start={scan_started_at.strftime('%H:%M:%S')} "
                        f"end={now_ist().strftime('%H:%M:%S')} "
                        f"elapsed={scan_seconds:.1f}s status=FAILED",
                        flush=True,
                    )
                    failures[slot_key] = failures.get(slot_key, 0) + 1
                    publish_status(
                        "FAILED",
                        session_date_ist=session_date.isoformat(),
                        phase="SCAN_FAILED",
                        scan_seconds=f"{scan_seconds:.1f}",
                        slot=slot.isoformat(),
                        marker=marker_path.name,
                        retry_count=failures[slot_key],
                        error=f"{type(exc).__name__}: {exc}",
                    )
                    print(f"[ERROR] slot={slot.isoformat()} {type(exc).__name__}: {exc}", flush=True)
                    if failures[slot_key] >= args.max_slot_retries:
                        processed.add(slot_key)

            if args.once:
                if not eligible:
                    publish_status(
                        "WAITING_FETCH",
                        session_date_ist=session_date.isoformat(),
                        phase="WAITING_FETCH",
                        message="No unprocessed fully successful marker is available.",
                        processed_slots=len(processed),
                    )
                return 0

            current = now_ist()
            if current >= session_deadline:
                return finish()

            publish_heartbeat(
                "RUNNING",
                session_date_ist=session_date.isoformat(),
                phase="LOOP",
                processed_slots=len(processed),
                message="Waiting for the next fully successful five-minute fetch marker.",
            )
            loop_errors = 0
            seconds_until_end = max(0.0, (session_deadline - current).total_seconds())
            time.sleep(min(args.poll_seconds, seconds_until_end))
        except Exception as exc:
            # A scan loop must outlive its own bookkeeping. Anything unexpected
            # here is logged and retried; only a sustained failure hands the
            # process back to the launcher, which restarts it.
            loop_errors += 1
            warn(
                f"[LOOP][WARN] iteration failed ({loop_errors}/{MAX_CONSECUTIVE_LOOP_ERRORS}): "
                f"{type(exc).__name__}: {exc}"
            )
            publish_heartbeat(
                "RUNNING",
                session_date_ist=session_date.isoformat(),
                phase="LOOP_ERROR",
                loop_errors=loop_errors,
                error=f"{type(exc).__name__}: {exc}",
            )
            if loop_errors >= MAX_CONSECUTIVE_LOOP_ERRORS:
                publish_status(
                    "FAILED",
                    heartbeat_state="CRASHED",
                    session_date_ist=session_date.isoformat(),
                    phase="LOOP_ERROR",
                    error=f"{type(exc).__name__}: {exc}",
                    message=f"{loop_errors} consecutive loop failures; exiting for restart.",
                )
                return 1
            time.sleep(args.poll_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=f"Run the marker-gated weekday session for {STRATEGY}."
    )
    parser.add_argument("--python-exe", type=Path, default=Path(sys.executable))
    parser.add_argument("--strategy-script", type=Path, default=DEFAULT_STRATEGY_SCRIPT)
    parser.add_argument("--forensic-root", type=Path, default=DEFAULT_FORENSIC_ROOT)
    parser.add_argument("--live-folder", type=Path, default=DEFAULT_LIVE_FOLDER)
    parser.add_argument("--slot-dir", type=Path, default=DEFAULT_SLOT_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--session-date", default="")
    parser.add_argument("--start-time", default="09:15")
    parser.add_argument("--first-entry-slot", default="09:55")
    parser.add_argument("--entry-cutoff", default="14:30")
    parser.add_argument("--end-time", default="15:30")
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--close-grace-seconds", type=int, default=15)
    parser.add_argument("--strategy-timeout-seconds", type=int, default=180)
    parser.add_argument("--max-slot-retries", type=int, default=3)
    # Under one slot: a backlog older than this is stale for a live scan, so
    # the newest marker wins instead of replaying the queue in order.
    parser.add_argument("--max-slot-lag-seconds", type=float, default=240.0)
    parser.add_argument("--marker-late-warn-seconds", type=float, default=90.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--rebuild-entries", action="store_true")
    parser.add_argument("--catch-up", action="store_true")
    parser.add_argument("--allow-weekend", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        return run_session(args)
    except KeyboardInterrupt:
        publish_status("STOPPED", heartbeat_state="STOPPED", message="Interrupted.")
        return 0
    except Exception as exc:
        publish_status(
            "FAILED", heartbeat_state="CRASHED", error=f"{type(exc).__name__}: {exc}"
        )
        print(f"[FATAL] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
