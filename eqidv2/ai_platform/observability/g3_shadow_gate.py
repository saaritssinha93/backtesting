"""Explicit unprepared G3 prospective cohort, with no inherited model authority.

The G model, outcome labels and frozen pullback observers do not attest a G3
prospective model. Until a separately reviewed G3 enrollment pipeline exists,
all preparation/sealing/finalization commands publish only this blocked status.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

from fno_v13_v10_g_3_contract import STRATEGY_PROFILE, STRATEGY_VERSION, config_fingerprint

STATE = "AWAITING_G3_MODEL_AND_PROSPECTIVE_EVIDENCE"


def pending_g3_shadow(*, session_date: date, command: str, output_root: Path) -> dict:
    if type(session_date) is not date:
        raise TypeError("An explicit session date is required")
    if command not in {"prepare", "seal", "seal-decisions", "finalize", "verify"}:
        raise ValueError("Unsupported G3 shadow operation")
    fingerprint = config_fingerprint()
    cohort = "G3_PROSPECTIVE_V1_" + fingerprint[:16]
    payload = dict(schema_version="eqidv2.v13_v10_g_3.shadow_readiness.v1",
        strategy_profile=STRATEGY_PROFILE, strategy_version=STRATEGY_VERSION,
        strategy_fingerprint=fingerprint, cohort_id=cohort, session_date=session_date.isoformat(),
        requested_operation=command, state=STATE, complete=False, mode="PROSPECTIVE_SHADOW",
        execution_authority=False, broker_access=False, model_loaded=False,
        decision_evidence_sealed=False, outcome_evidence_joined=False,
        reason="A G3 model and cohort enrollment must be independently reviewed before prospective evidence can be collected.",
        required_before_enrollment=["G3-specific frozen model and causal feature specification",
            "Past-only training cutoff and independently frozen prospective cohort",
            "G3 version/fingerprint-bound inputs, decisions and outcome adapters"],
        created_at_utc=datetime.now(timezone.utc).isoformat())
    # Separate namespace even if an operator supplies an older research root.
    folder = Path(output_root).resolve() / "g3_prospective" / cohort / session_date.isoformat()
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / "readiness.json"
    descriptor, temporary = tempfile.mkstemp(prefix=".readiness_", suffix=".tmp", dir=folder)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.write("\n")
        os.replace(temporary, target)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return {**payload, "status_path": str(target)}
