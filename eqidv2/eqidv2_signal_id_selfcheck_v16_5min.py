from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

from eqidv2_runtime_paths import RUNTIME_STATUS_DIR


SELF_CHECK_SCHEMA = "eqidv2_v16_5min_signal_id_selfcheck"
SELF_CHECK_VERSION = 1
PROOF_FILE_PATTERN = "eqidv2_v16_5min_signal_id_selfcheck_{}.json"

FIXTURE_TICKER = "SBIN"
FIXTURE_SIDE = "LONG"
FIXTURE_SETUP = "B_OR_PULLBACK"
FIXTURE_SIGNAL_TIME_RAW = "2026-01-15 09:20:00+0530"
FIXTURE_SIGNAL_TIME_CANONICAL = "2026-01-15 09:20:00+05:30"
FIXTURE_ENTRY_TIME_RAW = "2026-01-15 09:25:00+0530"
FIXTURE_ENTRY_TIME_CANONICAL = "2026-01-15 09:25:00+05:30"
EXPECTED_SIGNAL_ID = "997274db413d89f2"


def proof_path_for_date(session_date: str) -> Path:
    return RUNTIME_STATUS_DIR / PROOF_FILE_PATTERN.format(str(session_date).strip())


def build_stage1_fixture() -> Dict[str, Any]:
    return {
        "ticker": FIXTURE_TICKER,
        "setup": FIXTURE_SETUP,
        "signal_time_ist": FIXTURE_SIGNAL_TIME_RAW,
        "entry_time_ist": FIXTURE_ENTRY_TIME_RAW,
    }


def build_stage2_fixture() -> Dict[str, Any]:
    return {
        "ticker": FIXTURE_TICKER,
        "side": FIXTURE_SIDE,
        "setup": FIXTURE_SETUP,
        "signal_datetime": FIXTURE_SIGNAL_TIME_CANONICAL,
        "signal_bar_time": FIXTURE_SIGNAL_TIME_CANONICAL,
        "signal_entry_datetime_ist": FIXTURE_ENTRY_TIME_CANONICAL,
    }


def build_stage1_proof_payload(
    *,
    session_date: str,
    written_at: str,
    stage1_signal_id: str,
    stage1_canonical_signal_time: str,
    stage1_canonical_entry_time: str,
) -> Dict[str, Any]:
    return {
        "schema": SELF_CHECK_SCHEMA,
        "version": SELF_CHECK_VERSION,
        "session_date": str(session_date).strip(),
        "written_at": str(written_at).strip(),
        "expected_signal_id": EXPECTED_SIGNAL_ID,
        "fixture": {
            "ticker": FIXTURE_TICKER,
            "side": FIXTURE_SIDE,
            "setup": FIXTURE_SETUP,
            "signal_time_raw": FIXTURE_SIGNAL_TIME_RAW,
            "signal_time_canonical": FIXTURE_SIGNAL_TIME_CANONICAL,
            "entry_time_raw": FIXTURE_ENTRY_TIME_RAW,
            "entry_time_canonical": FIXTURE_ENTRY_TIME_CANONICAL,
        },
        "stage1_signal_id": str(stage1_signal_id).strip(),
        "stage1_canonical_signal_time": str(stage1_canonical_signal_time).strip(),
        "stage1_canonical_entry_time": str(stage1_canonical_entry_time).strip(),
    }


def write_stage1_proof(payload: Dict[str, Any]) -> Path:
    session_date = str(payload.get("session_date", "")).strip()
    if not session_date:
        raise ValueError("signal-id self-check proof missing session_date")
    path = proof_path_for_date(session_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(tmp_path, path)
    return path


def load_stage1_proof(session_date: str) -> Dict[str, Any]:
    path = proof_path_for_date(session_date)
    if not path.exists():
        raise FileNotFoundError(
            f"signal-id self-check proof missing for session_date={session_date} at {path}"
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if str(payload.get("schema", "")).strip() != SELF_CHECK_SCHEMA:
        raise RuntimeError(
            f"signal-id self-check proof schema mismatch: {payload.get('schema')!r}"
        )
    if int(payload.get("version", 0) or 0) != SELF_CHECK_VERSION:
        raise RuntimeError(
            f"signal-id self-check proof version mismatch: {payload.get('version')!r}"
        )
    payload_session_date = str(payload.get("session_date", "")).strip()
    if payload_session_date != str(session_date).strip():
        raise RuntimeError(
            f"signal-id self-check proof session mismatch: expected {session_date}, got {payload_session_date or '<blank>'}"
        )
    return payload
