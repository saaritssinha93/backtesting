"""Signal timing contract tests for T+1 with a 15-second deadline."""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import eqidv2_v7_signal_contract as contract


def test_parse_valid_and_invalid_values():
    valid = contract.parse_ist_timestamp("2026-06-07 10:00:00+05:30")
    assert valid is not None and valid.tzinfo is not None
    assert contract.parse_ist_timestamp(None) is None
    assert contract.parse_ist_timestamp("") is None
    assert contract.parse_ist_timestamp("not_a_date") is None


def test_build_intended_entry_is_t1():
    timing = contract.build_signal_timing(
        "2026-06-07 10:00:00+05:30",
        detected_time="2026-06-07 10:01:10+05:30",
    )
    assert timing is not None
    assert timing.intended_entry_ist == pd.Timestamp("2026-06-07 10:01:00+05:30")
    assert timing.detection_lag_sec == 10.0
    assert timing.deadline_ist == pd.Timestamp("2026-06-07 10:01:15+05:30")


def test_explicit_intended_entry_and_row_override_are_honoured():
    timing = contract.build_signal_timing(
        "2026-06-07 10:00:00+05:30",
        detected_time="2026-06-07 10:01:10+05:30",
        intended_entry_time="2026-06-07 10:01:00+05:30",
    )
    assert timing is not None and timing.detection_lag_sec == 10.0

    row = {
        "bar_time_ist": "2026-06-07 10:00:00+05:30",
        "intended_entry_ist": "2026-06-07 10:01:00+05:30",
    }
    ok, reason = contract.validate_signal_timing_row(
        row,
        detected_time="2026-06-07 10:01:10+05:30",
    )
    assert ok, reason


def test_missing_timestamps_fail():
    assert contract.build_signal_timing(
        None, detected_time="2026-06-07 10:01:00+05:30"
    ) is None
    assert contract.build_signal_timing(
        "2026-06-07 10:00:00+05:30", detected_time=None
    ) is None
    ok, reason = contract.validate_signal_timing(None)
    assert not ok and "MALFORMED" in reason


def test_negative_lag_fails():
    timing = contract.build_signal_timing(
        "2026-06-07 10:00:00+05:30",
        detected_time="2026-06-07 10:00:59+05:30",
    )
    ok, reason = contract.validate_signal_timing(timing)
    assert not ok and "NEGATIVE_LAG" in reason


def test_stale_lag_fails_after_15_seconds():
    timing = contract.build_signal_timing(
        "2026-06-07 10:00:00+05:30",
        detected_time="2026-06-07 10:01:16+05:30",
    )
    ok, reason = contract.validate_signal_timing(timing)
    assert not ok and "STALE_DETECTION" in reason


def test_exactly_at_15_seconds_passes():
    timing = contract.build_signal_timing(
        "2026-06-07 10:00:00+05:30",
        detected_time="2026-06-07 10:01:15+05:30",
    )
    ok, reason = contract.validate_signal_timing(timing)
    assert ok, reason


def test_cross_day_fails():
    timing = contract.build_signal_timing(
        "2026-06-06 10:00:00+05:30",
        detected_time="2026-06-07 10:01:10+05:30",
    )
    ok, reason = contract.validate_signal_timing(timing)
    assert not ok and "CROSS_DAY" in reason


def test_canonical_signal_id_is_deterministic_and_specific():
    base = contract.canonical_signal_id(
        "RELIANCE", "LONG", "C_OR_BREAKOUT", "2026-06-07 10:00:00+05:30"
    )
    assert base == contract.canonical_signal_id(
        "RELIANCE", "LONG", "C_OR_BREAKOUT", "2026-06-07 10:00:00+05:30"
    )
    assert base != contract.canonical_signal_id(
        "RELIANCE", "SHORT", "C_OR_BREAKOUT", "2026-06-07 10:00:00+05:30"
    )
    assert base != contract.canonical_signal_id(
        "RELIANCE", "LONG", "C_OR_BREAKOUT", "2026-06-07 10:05:00+05:30"
    )
