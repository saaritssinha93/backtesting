"""Read-only dashboard metadata for the isolated G3 opening-data workers.

Two data sessions serve three analysis contexts. LIVE/PAPER share one observer;
BACKTEST reuses recovered evidence. Neither card is an execution session.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
SESSION_SPECS = {
    "dashboard_flow_opening_observer": {
        "title": "G3 LIVE / PAPER opening-data observer",
        "task": r"\EQIDV2_dashboard_flow_observer_0855",
        "status_file": "opening_observer.json",
        "schedule": "Monday-Friday 08:55 IST; collect until 09:21; no late catch-up",
        "contexts": "G3 LIVE and PAPER 09:20 inputs and flow analysis (one shared data worker)",
        "note": "Waits for the existing authentication job and dated universe before collecting. Does not refresh tokens or submit orders.",
    },
    "dashboard_flow_opening_history": {
        "title": "G3 BACKTEST opening-data recovery",
        "task": r"\EQIDV2_dashboard_flow_observer_history_1700",
        "status_file": "opening_history.json",
        "schedule": "G3 daily backtest recovers first at 16:20 IST; standalone retry Monday-Friday 17:00 IST",
        "contexts": "G3 BACKTEST 09:20 input evidence and historical opening context",
        "note": "Recovers evidence into observer storage. The 16:20 daily launcher then snapshots it for G3 V4; this standalone retry does not rerun backtests or overwrite market bars.",
    },
}
CARD_TASK_NAMES = {key: (value["task"],) for key, value in SESSION_SPECS.items()}


def status_path(data_root: Path | str, card_id: str) -> Path:
    return Path(data_root) / "dashboard_flow_observer" / "status" / SESSION_SPECS[card_id]["status_file"]


def session_items(data_root: Path | str, *, now_ist: datetime | None = None) -> list[dict]:
    """Read bounded, dedicated worker statuses; never import a collector."""
    now = now_ist or datetime.now(IST)
    now = now.replace(tzinfo=IST) if now.tzinfo is None else now.astimezone(IST)
    schedule_path = Path(data_root) / "dashboard_flow_observer" / "state" / "task_schedule.json"
    installed_tasks = set()
    try:
        if schedule_path.stat().st_size <= 64_000:
            manifest = json.loads(schedule_path.read_text(encoding="utf-8-sig"))
            if (isinstance(manifest, dict) and manifest.get("schema") == "dashboard_flow_observer_schedule_v1"
                    and manifest.get("installed") is True and isinstance(manifest.get("tasks"), list)):
                installed_tasks = {task for task in manifest["tasks"] if isinstance(task, str)}
    except (OSError, ValueError):
        pass
    items = []
    for card_id, spec in SESSION_SPECS.items():
        path = status_path(data_root, card_id)
        raw, error, info = {}, "", None
        try:
            info = path.stat()
            if info.st_size > 256_000:
                raise ValueError("Worker status exceeds the reporting size limit")
            raw = json.loads(path.read_text(encoding="utf-8-sig"))
            if not isinstance(raw, dict):
                raise ValueError("Worker status must be a JSON object")
        except FileNotFoundError:
            pass
        except (OSError, ValueError) as exc:
            raw = {}
            error = f"Worker status unavailable: {type(exc).__name__}"
        runtime = str(raw.get("status") or raw.get("state") or "NOT_SCHEDULED").upper()
        display_state = {"COMPLETE": "DONE", "SKIPPED": "STOPPED", "WAITING_INPUTS": "WAITING",
                         "WAITING_AUTH": "WAITING", "UNAVAILABLE": "PARTIAL"}.get(runtime, runtime)
        status = {
            "status": "PARTIAL" if error else display_state,
            "phase": str(raw.get("phase") or runtime),
            "view_scope": "SESSION",
            "execution_mode": "REPORTING_ONLY",
            "reporting_only": "true",
            "consumers": spec["contexts"],
            "configured_schedule": spec["schedule"],
            "derived_status": error or "Opening evidence for G3 09:20 and analysis; collector cannot place orders",
        }
        if info is None and spec["task"].lstrip("\\") in installed_tasks:
            status.update(status="SCHEDULED", phase="WAITING_FIRST_RUN",
                          derived_status="Schedule installed; waiting for the first worker observation")
        for key in ("session_date", "updated_at_ist", "updated_at", "pid", "reason"):
            value = raw.get(key)
            if isinstance(value, (str, int, float, bool)):
                status[key] = str(value)
        # The file is a historical observation, not proof that a process is
        # still running today. Keep the original phase and time for diagnosis.
        if runtime in {"RUNNING", "WAITING_INPUTS", "WAITING_AUTH", "COLLECTING"} and info and now.timestamp() - info.st_mtime > 180:
            status["status"] = "STALE_OUTPUT"
            status["derived_status"] = "Observer heartbeat is older than 3 minutes; worker activity is unverified"
        lines = [f"# {spec['title']}", "", "Mode: REPORTING_ONLY", f"Contexts: {spec['contexts']}",
                 f"Task: {spec['task']}", f"Schedule: {spec['schedule']}",
                 f"Worker status: {runtime}", f"Session date: {raw.get('session_date', 'not yet recorded')}",
                 f"Updated: {raw.get('updated_at_ist', raw.get('updated_at', 'not yet recorded'))}",
                 f"Status source: {path}", "", spec["note"],
                 "Production candle, indicator and entry calculations remain unchanged.",
                 "Exact opening evidence, first-minute history and missing values remain distinct."]
        if status["phase"] == "WAITING_FIRST_RUN":
            lines.extend(["Installation: schedule verified; waiting for the first worker observation",
                          f"Installation record: {schedule_path}"])
        for key in ("reason", "records", "symbols", "requests", "observations", "coverage", "errors", "events", "polls", "contracts", "completed", "planned", "complete_contract_sessions", "error_type"):
            value = raw.get(key)
            if isinstance(value, (str, int, float, bool)):
                lines.append(f"{key}: {str(value)[:2000]}")
        if error:
            lines.append(error)
        items.append({"id": card_id, "title": spec["title"], "file_name": str(path),
                      "exists": info is not None, "mtime": datetime.fromtimestamp(info.st_mtime, IST).isoformat() if info else None,
                      "size_bytes": info.st_size if info else 0, "status": status, "tail": "\n".join(lines),
                      "dashboard_flow_path": "/dashboard-flow#live", "card_kind": "process"})
    return items
