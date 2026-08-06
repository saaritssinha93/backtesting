from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

import research_v12_build_strict_entry_prefilter as strict


def snapshot(session: str, clock: str, rows: int = strict.BUDGET) -> pd.DataFrame:
    slot = pd.Timestamp(f"{session} {clock}", tz=strict.IST)
    return pd.DataFrame(
        {
            "slot_ist": [slot.isoformat()] * rows,
            "ticker": [f"T{rank:03d}" for rank in range(1, rows + 1)],
            "selection_rank": list(range(1, rows + 1)),
            "primary_side": ["LONG"] * rows,
            "date": [slot.isoformat()] * rows,
            "staleness_seconds": [0.0] * rows,
        }
    )


def normal_day(session: str, include_1520: bool = True) -> pd.DataFrame:
    clocks = list(strict.ACTIONABLE_TIMES)
    if include_1520:
        clocks.append(strict.AUDIT_ONLY_TIME)
    return pd.concat([snapshot(session, clock) for clock in clocks], ignore_index=True)


def test_absent_or_degraded_1520_is_permitted_and_never_emitted() -> None:
    absent = normal_day("2026-06-01", include_1520=False)
    degraded = pd.concat(
        [normal_day("2026-06-02", include_1520=False), snapshot("2026-06-02", "15:20", 7)],
        ignore_index=True,
    )

    result = strict.derive_strict_entry_snapshots(pd.concat([absent, degraded]))

    assert result.session_audit["included"].tolist() == [True, True]
    assert result.session_audit["audit_15_20_status"].tolist() == [
        "absent_permitted",
        "degraded_permitted",
    ]
    assert len(result.derived) == 2 * 6 * 300
    derived_times = pd.to_datetime(result.derived["slot_ist"], utc=True).dt.tz_convert(strict.IST)
    assert not derived_times.dt.strftime("%H:%M").eq("15:20").any()


def test_one_missing_actionable_slot_excludes_the_entire_session() -> None:
    good = normal_day("2026-06-01")
    bad = normal_day("2026-06-02")
    bad_slot = pd.Timestamp("2026-06-02 12:20", tz=strict.IST).isoformat()
    bad = bad.loc[bad["slot_ist"].ne(bad_slot)]

    result = strict.derive_strict_entry_snapshots(pd.concat([good, bad]))

    assert result.session_audit.set_index("session_date").loc["2026-06-01", "included"]
    assert not result.session_audit.set_index("session_date").loc["2026-06-02", "included"]
    reason = result.excluded_sessions.iloc[0]["actionable_failure_reasons"]
    assert "12:20:missing_slot" in reason
    assert set(pd.to_datetime(result.derived["slot_ist"]).dt.strftime("%Y-%m-%d")) == {
        "2026-06-01"
    }


def test_exact_snapshot_checks_tickers_ranks_bar_time_and_staleness() -> None:
    day = normal_day("2026-06-01")
    slot = pd.Timestamp("2026-06-01 10:20", tz=strict.IST).isoformat()
    indices = day.index[day["slot_ist"].eq(slot)].tolist()
    day.loc[indices[1], "ticker"] = day.loc[indices[0], "ticker"]
    day.loc[indices[1], "selection_rank"] = 1
    day.loc[indices[2], "date"] = pd.Timestamp(
        "2026-06-01 10:15", tz=strict.IST
    ).isoformat()
    day.loc[indices[3], "staleness_seconds"] = 5.0

    result = strict.derive_strict_entry_snapshots(day)

    assert result.derived.empty
    reasons = result.excluded_sessions.iloc[0]["actionable_failure_reasons"]
    assert "unique_tickers_299_expected_300" in reasons
    assert "rank_set_not_exact_1_through_300" in reasons
    assert "selected_bar_not_completed_exactly_at_slot" in reasons
    assert "staleness_not_exactly_zero" in reasons


def test_explicit_calendar_catches_an_entirely_absent_normal_session() -> None:
    frame = normal_day("2026-06-01")

    result = strict.derive_strict_entry_snapshots(
        frame, ["2026-06-01", "2026-06-02"]
    )

    missing = result.excluded_sessions.set_index("session_date").loc["2026-06-02"]
    assert missing["actionable_failure_reasons"].count("missing_slot") == 6
    assert result.global_audit["session_source"] == "explicit_session_calendar"


def test_cli_run_writes_reconciled_csv_contract_hashes_and_audits(
    tmp_path: Path,
) -> None:
    source = tmp_path / "combined.csv"
    output = tmp_path / "strict"
    normal_day("2026-06-01", include_1520=False).to_csv(source, index=False)
    args = strict.parse_args(
        ["--input", str(source), "--output-dir", str(output)]
    )

    contract = strict.run(args)

    derived = output / strict.DEFAULT_DERIVED_NAME
    payload = json.loads((output / "strict_entry_contract.json").read_text())
    digest = hashlib.sha256(derived.read_bytes()).hexdigest()
    assert contract["audit"]["sessions_included"] == 1
    assert payload["derived"]["rows"] == 1_800
    assert payload["derived"]["sha256"] == digest
    assert payload["derived"]["contains_15_20_rows"] is False
    assert payload["audit"]["audit_15_20_status_counts"] == {
        "absent_permitted": 1
    }
    assert (output / "session_audit.csv").is_file()
    assert (output / "slot_audit.csv").is_file()
    assert (output / "excluded_sessions.csv").is_file()
