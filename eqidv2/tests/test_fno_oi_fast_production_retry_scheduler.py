from datetime import datetime, timedelta
from unittest import mock

import pandas as pd
import pytest

import fno_oi_common as common
import fno_oi_fetch_5min_fast_production as producer


def stamp(hhmmss: str) -> datetime:
    return datetime.fromisoformat(f"2026-11-02T{hhmmss}+05:30")


def completed_except(*missing: str) -> set[str]:
    return producer._session_slot_keys(stamp("10:10:00").date()) - set(missing)


def test_partial_opening_cools_down_without_blocking_other_gap():
    latest = stamp("10:10:00")
    now = stamp("10:10:05")
    result = producer._next_due_slot(
        latest.date(), latest, now, completed_except("0920", "0925"), 3,
        {"0920": now + timedelta(seconds=30)},
    )
    assert result == stamp("09:25:00")


def test_expired_oldest_partial_does_not_starve_unattempted_gap():
    latest = stamp("10:10:00")
    now = stamp("10:10:05")
    result = producer._next_due_slot(
        latest.date(), latest, now, completed_except("0920", "0925"), 3,
        {"0920": stamp("10:09:00")},
    )
    assert result == stamp("09:25:00")


def test_two_partial_slots_use_oldest_retry_deadline_not_oldest_slot():
    latest = stamp("10:10:00")
    now = stamp("10:10:05")
    retries = {"0920": stamp("10:10:00"), "0925": stamp("10:09:30")}
    processed = completed_except("0920", "0925")
    assert producer._next_due_slot(latest.date(), latest, now, processed, 3, retries) == stamp("09:25:00")
    retries["0925"] = now + timedelta(seconds=30)
    assert producer._next_due_slot(latest.date(), latest, now, processed, 3, retries) == stamp("09:20:00")


def test_new_current_slot_takes_priority_over_eligible_repairs():
    latest = stamp("10:10:00")
    now = stamp("10:10:03")
    assert producer._next_due_slot(
        latest.date(), latest, now, completed_except("0920", "0925", "1010"),
        3, {"0920": stamp("10:09:30")},
    ) == latest


@pytest.mark.parametrize(
    "now,latest,retries,expected",
    [
        ("10:14:59", "10:10:00", {"0920": "10:15:29"}, 4.0),
        ("10:15:02.900000", "10:15:00", {}, 0.1),
        ("10:14:59", "10:10:00", {"0920": "10:15:00"}, 1.0),
    ],
)
def test_wait_clips_to_live_boundary_or_earlier_retry(now, latest, retries, expected):
    assert producer._scheduler_wait_seconds(
        stamp(now), stamp(latest), completed_except("0920", "1015"),
        {key: stamp(value) for key, value in retries.items()}, 3, 30,
    ) == pytest.approx(expected)


def test_current_partial_is_not_retried_early_or_marked_processed():
    latest = stamp("09:20:00")
    processed = set()
    retries = {"0920": stamp("09:20:40")}
    assert producer._next_due_slot(
        latest.date(), latest, stamp("09:20:39"), processed, 3, retries,
    ) is None
    assert producer._next_due_slot(
        latest.date(), latest, stamp("09:20:40"), processed, 3, retries,
    ) == latest
    assert processed == set()


def test_progress_keeps_latest_ready_separate_from_partial_repair():
    with mock.patch.object(common, "atomic_write_json") as write:
        producer._publish_scheduler_progress(
            stamp("09:30:00"), stamp("09:20:00"), {"0925", "0930"},
            {"0920": stamp("09:30:40")}, phase="SLOT_DONE", attempt_state="PARTIAL",
        )
    payload = write.call_args.args[1]
    assert write.call_args.args[0].name == "latest_fno_oi_fast_production_progress.json"
    assert payload["latest_ready_slot"] == stamp("09:30:00").isoformat()
    assert payload["latest_due_slot"] == stamp("09:30:00").isoformat()
    assert payload["active_slot"] == stamp("09:20:00").isoformat()
    assert payload["attempt_kind"] == "REPAIR"
    assert payload["attempt_state"] == "PARTIAL"
    assert payload["pending_slot_keys"] == ["0920"]


def test_session_partial_retry_sleep_does_not_delay_new_slot():
    args = producer.build_parser().parse_args([
        "--session-date", "2026-11-02", "--no-bootstrap", "--poll-sec", "5",
        "--partial-retry-sec", "30",
    ])
    clock = [stamp("09:24:59")]
    attempts = []
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += timedelta(seconds=seconds)

    def fetch(slot, *_args, **_kwargs):
        attempts.append((slot, clock[0]))
        if len(attempts) == 1:
            clock[0] += timedelta(seconds=0.1)
            return {"complete": False, "state": "PARTIAL"}
        clock[0] = stamp("15:34:00")
        return {"complete": True, "state": "SUCCESS"}

    with (
        mock.patch.object(common, "load_holidays", return_value=set()),
        mock.patch.object(common, "now_ist", side_effect=lambda: clock[0]),
        mock.patch.object(common, "publish_status"),
        mock.patch.object(common, "publish_heartbeat"),
        mock.patch.object(producer, "_publish_scheduler_progress"),
        mock.patch.object(producer.legacy, "ensure_universe", return_value=pd.DataFrame()),
        mock.patch.object(producer.legacy, "_today_processed_slots", return_value=set()),
        mock.patch.object(producer.fast_core.AppLaneSession, "acquire", return_value=([], [], None)),
        mock.patch.object(producer.CanonicalArchiveCache, "preload"),
        mock.patch.object(producer, "run_fast_slot", side_effect=fetch),
        mock.patch.object(producer.time, "sleep", side_effect=sleep),
    ):
        assert producer.run_session(args) == 2  # Unrepaired opening remains blocked.
    assert attempts == [
        (stamp("09:20:00"), stamp("09:24:59")),
        (stamp("09:25:00"), stamp("09:25:03")),
    ]
    assert max(sleeps) < 5
