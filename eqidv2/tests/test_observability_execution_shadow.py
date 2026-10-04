from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ai_platform.observability import execution_research as execution
from ai_platform.observability import prospective_shadow as shadow
from ai_platform.observability.strategy_research import _shadow_summary
from fno_v13_v10_g_identity import canonical_signal_id


IST = timezone.utc  # explicit offsets below are converted by the implementation


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_signal_id_matches_legacy_live_contract_and_normalizes_slots() -> None:
    session = date(2026, 9, 25)
    version = "FNO_V13_V10_G_RETAINED_20260914"
    raw = f"{version}|{session}|09:25|09:26|SHORT|ABC26SEPFUT"
    expected = (
        "20260925_0926_SHORT_ABC26SEPFUT_"
        + hashlib.sha1(raw.encode("ascii")).hexdigest()[:12]
    )
    assert canonical_signal_id(
        version, session, "09:25", "09:26", "SHORT", "ABC26SEPFUT"
    ) == expected
    assert canonical_signal_id(
        version, session, "0925", "0926", "short", "ABC26SEPFUT"
    ) == expected


def test_paper_calibration_uses_only_terminal_paper_states(tmp_path: Path) -> None:
    root = tmp_path / "paper"
    common = {
        "mode": "PAPER",
        "session_date": "2026-09-25",
        "confirmation_end": "09:26",
        "side": "LONG",
        "trigger_price": 100.0,
    }
    _write_json(
        root / "closed.json",
        {
            **common,
            "status": "CLOSED",
            "entry_order_activated_at_ist": "2026-09-25T09:26:08+05:30",
            "entry_at_ist": "2026-09-25T09:26:28+05:30",
            "entry_price": 100.02,
        },
    )
    _write_json(root / "cancelled.json", {**common, "status": "CANCELLED"})
    _write_json(root / "pending.json", {**common, "status": "PENDING_ENTRY"})
    result = execution.collect_paper_calibration(root)
    assert result["terminal_orders"] == 2
    assert result["fills"] == 1
    assert result["fill_ratio_pct"] == 50.0
    assert result["trigger_to_fill_distance_bps"]["median"] == pytest.approx(2.0)


def test_missed_entry_mfe_is_separate_finalized_counterfactual() -> None:
    baseline = pd.DataFrame(
        [
            {
                "sid": 7,
                "day": "2026-09-25",
                "tradingsymbol": "TEST",
                "side": "LONG",
                "setup_id": "0926_LONG",
                "trigger": 100.0,
                "filled": False,
            }
        ]
    )
    stamps = pd.date_range("2026-09-25 09:27", periods=4, freq="min", tz="Asia/Kolkata")
    paths = {
        7: {
            "timestamp_ns": stamps.astype("int64").to_numpy(),
            "open": np.array([98.0, 98.5, 99.0, 104.0]),
            "high": np.array([99.0, 99.5, 101.0, 110.0]),
            "low": np.array([97.0, 97.5, 95.0, 103.0]),
            "close": np.array([98.5, 99.0, 100.5, 109.0]),
        }
    }
    rows = execution.missed_entry_counterfactuals(
        baseline, paths, entry_expiry_minutes=2
    )
    assert len(rows) == 1
    assert rows[0]["post_expiry_trigger_touched"] is True
    assert rows[0]["post_expiry_mfe_pct"] == pytest.approx(10.0)
    assert rows[0]["post_expiry_mae_pct"] == pytest.approx(-5.0)
    assert rows[0]["safe_for_selection"] is False
    assert rows[0]["evidence_view"] == "FINALIZED_1M_COUNTERFACTUAL"


def test_shadow_lifecycle_is_hash_verified_and_no_authority(tmp_path: Path) -> None:
    session = date(2026, 9, 25)
    output = tmp_path / "research"
    dataset = tmp_path / "dataset.json"
    model = tmp_path / "model.json"
    strategy = tmp_path / "strategy.json"
    dataset.write_text('{"source":"sealed"}\n', encoding="utf-8")
    model.write_text('{"version":"candidate-1"}\n', encoding="utf-8")
    strategy.write_text('{"execution_authority":false}\n', encoding="utf-8")

    prepared = shadow.prepare_shadow_session(
        session_date=session,
        dataset=dataset,
        model=model,
        strategy=strategy,
        output_root=output,
        now=datetime(2026, 9, 25, 3, 0, tzinfo=timezone.utc),  # 08:30 IST
    )
    prepared_status = _shadow_summary(
        output / "shadow_observations.jsonl",
        allowed_manifest_root=output / "shadow_sessions",
    )
    assert prepared_status["latest_session_state"] == "PREPARED"
    assert prepared_status["prepared_sessions"] == 1
    assert prepared_status["sessions"] == 0
    prepared_value = json.loads(prepared.path.read_text(encoding="utf-8"))
    signal_id = "20260925_0926_LONG_TEST_deadbeef0000"
    decisions = tmp_path / "decisions.json"
    _write_json(
        decisions,
        {
            "schema_version": shadow.DECISIONS_SCHEMA,
            "session_date": session.isoformat(),
            "execution_authority": False,
            "complete": True,
            "dataset_sha256": prepared_value["dataset_sha256"],
            "model_sha256": prepared_value["model_sha256"],
            "strategy_fingerprint": prepared_value["strategy_fingerprint"],
            "rows": [
                {
                    "signal_id": signal_id,
                    "decision_at_ist": "2026-09-25T09:26:05+05:30",
                    "selected": True,
                }
            ],
        },
    )
    shadow.seal_shadow_decisions(
        session_date=session,
        decisions=decisions,
        output_root=output,
        now=datetime(2026, 9, 25, 7, 0, tzinfo=timezone.utc),  # 12:30 IST
    )
    sealed_status = _shadow_summary(
        output / "shadow_observations.jsonl",
        allowed_manifest_root=output / "shadow_sessions",
    )
    assert sealed_status["latest_session_state"] == "DECISIONS_SEALED"
    assert sealed_status["decisions_sealed_sessions"] == 1
    assert sealed_status["sessions"] == 0
    seal_path = output / "shadow_sessions" / session.isoformat() / "decision_seal.json"
    original_seal = seal_path.read_bytes()
    late_seal = json.loads(original_seal)
    late_seal["sealed_at_ist"] = "2026-09-25T15:30:00+05:30"
    seal_path.write_text(json.dumps(late_seal), encoding="utf-8")
    assert _shadow_summary(
        output / "shadow_observations.jsonl",
        allowed_manifest_root=output / "shadow_sessions",
    )["latest_session_state"] == "INVALID"
    seal_path.write_bytes(original_seal)
    outcomes = tmp_path / "outcomes.json"
    _write_json(
        outcomes,
        {
            "schema_version": shadow.OUTCOMES_SCHEMA,
            "session_date": session.isoformat(),
            "execution_authority": False,
            "complete": True,
            "rows": [
                {
                    "signal_id": signal_id,
                    "outcome_at_ist": "2026-09-25T15:15:00+05:30",
                    "net_return_pct": 0.4,
                }
            ],
        },
    )
    completed = shadow.finalize_shadow_session(
        session_date=session,
        outcomes=outcomes,
        output_root=output,
        now=datetime(2026, 9, 25, 10, 30, tzinfo=timezone.utc),  # 16:00 IST
    )
    verified = shadow.verify_shadow_session(session_date=session, output_root=output)
    assert completed.state == "COMPLETE"
    assert verified["verified"] is True
    completed_status = _shadow_summary(
        output / "shadow_observations.jsonl",
        allowed_manifest_root=output / "shadow_sessions",
    )
    assert completed_status["sessions"] == 1
    assert completed_status["completed_sessions"] == 1
    assert completed_status["completed_cohorts"] == 1
    assert completed_status["latest_session_state"] == "COMPLETE"
    assert completed_status["execution_authority"] is False

    manifest_path = output / "shadow_sessions" / session.isoformat() / "manifest.json"
    journal_path = output / "shadow_observations.jsonl"
    original_manifest = manifest_path.read_bytes()
    original_journal = journal_path.read_bytes()
    early_manifest = json.loads(original_manifest)
    early_manifest["completed_at_ist"] = "2026-09-25T15:29:59+05:30"
    early_bytes = json.dumps(early_manifest).encode("utf-8")
    manifest_path.write_bytes(early_bytes)
    journal_row = json.loads(original_journal)
    journal_row["manifest_sha256"] = hashlib.sha256(early_bytes).hexdigest()
    journal_path.write_text(json.dumps(journal_row) + "\n", encoding="utf-8")
    assert _shadow_summary(
        journal_path,
        allowed_manifest_root=output / "shadow_sessions",
    )["latest_session_state"] == "INVALID"
    manifest_path.write_bytes(original_manifest)
    journal_path.write_bytes(original_journal)

    (output / "shadow_sessions" / session.isoformat() / "sealed_outcomes.json").write_text(
        "{}\n", encoding="utf-8"
    )
    assert shadow.verify_shadow_session(
        session_date=session, output_root=output
    )["verified"] is False
    invalid_status = _shadow_summary(
        output / "shadow_observations.jsonl",
        allowed_manifest_root=output / "shadow_sessions",
    )
    assert invalid_status["sessions"] == 0
    assert invalid_status["latest_session_state"] == "INVALID"


def test_shadow_refuses_late_preparation(tmp_path: Path) -> None:
    artifact = tmp_path / "input.json"
    artifact.write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="before 09:15"):
        shadow.prepare_shadow_session(
            session_date=date(2026, 9, 25),
            dataset=artifact,
            model=artifact,
            strategy=artifact,
            output_root=tmp_path / "output",
            now=datetime(2026, 9, 25, 4, 0, tzinfo=timezone.utc),  # 09:30 IST
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("prepared_at_ist", "2026-09-25T09:15:00+05:30"),
        ("completed", True),
    ],
)
def test_shadow_summary_rejects_a_late_or_completed_prepared_manifest(
    tmp_path: Path, field: str, value: object,
) -> None:
    session = date(2026, 9, 25)
    output = tmp_path / "research"
    artifact = tmp_path / "input.json"
    artifact.write_text("{}\n", encoding="utf-8")
    prepared = shadow.prepare_shadow_session(
        session_date=session,
        dataset=artifact,
        model=artifact,
        strategy=artifact,
        output_root=output,
        now=datetime(2026, 9, 25, 3, 0, tzinfo=timezone.utc),
    )
    prepared_value = json.loads(prepared.path.read_text(encoding="utf-8"))
    prepared_value[field] = value
    prepared.path.write_text(json.dumps(prepared_value), encoding="utf-8")

    status = _shadow_summary(
        output / "shadow_observations.jsonl",
        allowed_manifest_root=output / "shadow_sessions",
    )

    assert status["latest_session_state"] == "INVALID"
    assert status["invalid_sessions"] == 1
    assert status["sessions"] == 0
