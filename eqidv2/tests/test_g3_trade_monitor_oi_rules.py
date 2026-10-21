"""OI research rules reject unavailable evidence and preserve exit chronology."""
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from research import g3_trade_monitor_oi_rules as rules


def candidate(family="oi_expansion", threshold=.5):
    return next(item for item in rules.candidate_registry()
                if item["family"] == family and item["oi_threshold_pct"] == threshold)


def feature(**changes):
    return dict(sid=1, source_signal_id="signal-1", day="2026-08-03", setup_id="setup",
        tradingsymbol="CASH", side="LONG", decision_at_ist="2026-08-03T10:01:00+05:30",
        completed_bar_count=6, consecutive_adverse_closes=3, momentum_5m_pct=-.15,
        signed_move_pct=-.30, mfe_pct=0., giveback_pct=.3,
        next_bar_open_at_ist="2026-08-03T10:01:00+05:30",
        modeled_execution_at_ist="2026-08-03T10:02:00+05:30", execution_path_index=21,
        next_bar_open_price=99.7, original_exit_at_ist="2026-08-03T10:15:00+05:30",
        native_target_pct=1., oi_valid=True, oi_change_10m_pct=.5, futures_return_10m_pct=-.2,
        oi_current_bar_end="2026-08-03T10:00:00+05:30",
        oi_reference_bar_end="2026-08-03T09:50:00+05:30", oi_contract="CASH26AUGFUT",
        oi_available_at="2026-08-03T10:00:30+05:30", **changes)


def changed(**changes):
    row = feature()
    row.update(changes)
    return row


def events(row=None, arm=None):
    return rules.candidate_events(pd.DataFrame([feature() if row is None else row]),
                                  candidate() if arm is None else arm)


def test_registry_fixed_hashes_and_independent_copies():
    registry = rules.candidate_registry()
    assert len(registry) == 8
    assert len({row["sha256"] for row in registry}) == 8
    assert all(row["sha256"] == row["candidate_sha256"] for row in registry)
    assert {row["oi_threshold_pct"] for row in registry} == {None, .5, 1.}
    changed_arm = deepcopy(registry[1])
    changed_arm["guard_constants"]["minimum_bars"] = 5
    with pytest.raises(ValueError, match="frozen"):
        events(arm=changed_arm)
    assert rules.candidate_registry() == registry


@pytest.mark.parametrize("family,change,passes", [
    ("oi_expansion", .5, True), ("oi_expansion", -.5, False),
    ("oi_contraction", -.5, True), ("oi_contraction", .5, False),
    ("oi_activity", -.5, True), ("oi_activity", .5, True),
    ("oi_activity", .4999, False), ("oi_activity", -.4999, False)])
def test_oi_family_boundaries_are_inclusive(family, change, passes):
    assert bool(events(changed(oi_change_10m_pct=change), candidate(family))) is passes


@pytest.mark.parametrize("side,return_pct,passes", [
    ("LONG", -.2, True), ("LONG", .2, False), ("SHORT", .2, True),
    ("SHORT", -.2, False), ("LONG", 0., False), ("SHORT", 0., False)])
def test_futures_confirmation_respects_trade_side(side, return_pct, passes):
    assert bool(events(changed(side=side, futures_return_10m_pct=return_pct))) is passes


@pytest.mark.parametrize("changes", [
    dict(oi_valid=False), dict(oi_valid="False"), dict(oi_valid=np.nan),
    dict(oi_change_10m_pct=np.nan), dict(oi_change_10m_pct=np.inf),
    dict(futures_return_10m_pct=-np.inf), dict(oi_contract=""),
    dict(oi_available_at="2026-08-03T10:01:01+05:30"),
    dict(oi_available_at="2026-08-03T09:59:59+05:30"),
    dict(oi_current_bar_end="2026-08-03T10:05:00+05:30"),
    dict(oi_reference_bar_end="2026-08-03T09:45:00+05:30"),
    dict(oi_reference_bar_end="2026-08-03T09:50:00"),
    dict(oi_current_bar_end="2026-08-02T10:00:00+05:30",
         oi_reference_bar_end="2026-08-02T09:50:00+05:30"),
])
def test_invalid_or_unavailable_oi_fails_closed_but_control_remains(changes):
    row = changed(**changes)
    assert not events(row)
    assert events(row, candidate("price_guard_only", None))


@pytest.mark.parametrize("changes", [
    dict(completed_bar_count=5), dict(consecutive_adverse_closes=2),
    dict(momentum_5m_pct=-.149), dict(signed_move_pct=-.299),
    dict(signed_move_pct=-np.inf), dict(completed_bar_count=np.nan),
    dict(next_bar_open_price=0), dict(side="UNKNOWN"),
    dict(next_bar_open_at_ist="2026-08-03T10:00:00+05:30"),
    dict(modeled_execution_at_ist="2026-08-03T10:03:00+05:30"),
    dict(original_exit_at_ist="2026-08-03T10:01:00+05:30"),
    dict(decision_at_ist="2026-08-03T10:01:00"),
])
def test_all_arms_keep_fixed_cash_guard_and_fill_chronology(changes):
    row = changed(**changes)
    assert not events(row)
    assert not events(row, candidate("price_guard_only", None))


def test_later_intrabar_native_exit_does_not_cancel_pending_open_exit():
    assert events(changed(original_exit_at_ist="2026-08-03T10:02:00+05:30"))


def test_unordered_interleaved_rows_choose_earliest_per_sid_without_mutating_input():
    first = feature()
    later = changed(decision_at_ist="2026-08-03T10:02:00+05:30",
        next_bar_open_at_ist="2026-08-03T10:02:00+05:30",
        modeled_execution_at_ist="2026-08-03T10:03:00+05:30", execution_path_index=22)
    another = changed(sid=2)
    frame = pd.DataFrame([later, another, first, first])
    before = frame.copy(deep=True)
    result = rules.candidate_events(frame, candidate())
    assert [row["sid"] for row in result] == [1, 2]
    assert result[0]["decision_at_ist"] == first["decision_at_ist"]
    pd.testing.assert_frame_equal(frame, before)
    assert not rules.candidate_events(frame, candidate(), days=["2026-08-04"])


def test_later_eligible_oi_event_can_follow_unavailable_earlier_observation():
    early = changed(oi_valid=False)
    later = changed(decision_at_ist="2026-08-03T10:02:00+05:30",
        next_bar_open_at_ist="2026-08-03T10:02:00+05:30",
        modeled_execution_at_ist="2026-08-03T10:03:00+05:30", execution_path_index=22)
    result = rules.candidate_events(pd.DataFrame([later, early]), candidate())
    assert result[0]["execution_path_index"] == 22


def test_oi_freshness_includes_predeclared_ingestion_lag_and_sampling_interval():
    row = changed(decision_at_ist="2026-08-03T10:05:00+05:30",
        next_bar_open_at_ist="2026-08-03T10:05:00+05:30",
        modeled_execution_at_ist="2026-08-03T10:06:00+05:30")
    assert events(row)
    row.update(decision_at_ist="2026-08-03T10:07:00+05:30",
        next_bar_open_at_ist="2026-08-03T10:07:00+05:30",
        modeled_execution_at_ist="2026-08-03T10:08:00+05:30")
    assert not events(row)
    row["oi_max_age_minutes"] = 10
    assert events(row)


def test_futures_price_control_tests_confirmation_without_oi_magnitude_selection():
    row = changed(oi_change_10m_pct=0.)
    assert events(row, candidate("futures_price_guard_only", None))
    assert not events(row)
    row["futures_return_10m_pct"] = .2
    assert not events(row, candidate("futures_price_guard_only", None))


def test_conflicting_same_time_events_rejected_instead_of_input_order_selection():
    with pytest.raises(ValueError, match="Conflicting"):
        rules.candidate_events(pd.DataFrame([feature(), changed(next_bar_open_price=99.)]), candidate())


def test_price_control_matches_existing_threshold_cache_on_valid_ordered_features():
    from research.g3_trade_monitor_parameter_search import DecisionCache, candidate_registry
    frame = pd.DataFrame([feature(), changed(sid=2, momentum_5m_pct=-.1)])
    original = next(item for item in candidate_registry()
        if item["family"] == "trend_failure" and item["trend_adverse_closes"] == 3
        and item["trend_momentum_5m_pct"] == -.15 and item["trend_entry_move_pct"] == -.30)
    expected = DecisionCache(frame).events(original)
    actual = rules.candidate_events(frame, candidate("price_guard_only", None))
    assert [{key: row[key] for key in expected[0]} for row in actual] == expected
