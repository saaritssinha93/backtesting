"""Chronological checks for historical pullback-to-exit annotations."""
import numpy as np
import pandas as pd
import pytest

from research.g3_pullback_followthrough import (
    analyze_window,
    build_path,
    sideways_metrics,
    stop_at_clock,
    stop_for_bar,
    summarize_path,
)


DAY = "2026-07-29"
ENTRY = pd.Timestamp(DAY + " 09:30", tz="Asia/Kolkata")


def example(rows, *, side="LONG", reason="TARGET", price=102., mode="INTRABAR"):
    """Rows are open/high/low/close; timestamp labels are candle ends."""
    minute = pd.DataFrame(rows, columns=["open", "high", "low", "close"])
    minute["ts"] = pd.date_range(ENTRY, periods=len(rows), freq="min")
    if side == "SHORT":
        original = minute.copy()
        for destination, source in [("open", "open"), ("high", "low"),
                                    ("low", "high"), ("close", "close")]:
            minute[destination] = 200. - original[source]
        price = 200. - price
    last = minute.ts.iloc[-1]
    tr = dict(day=DAY, tradingsymbol="TEST", setup_id="setup", sid=1,
              side=side, entry_ts=ENTRY, exit_ts=last if mode != "OPEN" else last-pd.Timedelta(minutes=1),
              exit_bar_end_ts=last, exit_event=mode, entry_price=100., exit_price=price,
              first_target_pct=2., initial_stop_pct=1.25, active_stop_pct_at_exit=1.25,
              exit_reason=reason, same_bar_ambiguous=False, portfolio_net_profit_rupees=100.)
    return tr, minute, build_path(tr, minute)


def window(*, event_minute=2, source="COMPLETED_BAR", **changes):
    row = dict(trade_id=f"{DAY}|setup|TEST|1", day=DAY, tradingsymbol="TEST",
               decision_ts=ENTRY+pd.Timedelta(minutes=1), decision_close=100.,
               event_ts=ENTRY+pd.Timedelta(minutes=event_minute), event_source=source,
               horizon_minutes=5, threshold_pct=.30, day_split="LATER16", research_watch=False)
    row.update(changes)
    return row


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_hit_candle_close_can_confirm_recovery_then_target(side):
    tr, _, path = example([
        (100., 100.1, 99.9, 100.), (100., 100.1, 99.9, 100.),
        (100., 100.1, 99.5, 100.), (100., 102.1, 90., 101.),
    ], side=side)
    result = analyze_window(tr, window(), path)
    assert result["outcome_code"] == "TARGET_AFTER_PULLBACK"
    assert result["recovery_time_ist"] == DAY + " 09:32:00"
    assert result["minutes_hit_to_recovery"] == 0
    assert result["worst_adverse_pct_anchor"] == pytest.approx(.5)
    assert result["min_stop_headroom_pct_entry"] == pytest.approx(.75)
    assert result["threshold_hit_from_ist"] == DAY + " 09:31:00"
    assert result["threshold_hit_by_ist"] == DAY + " 09:32:00"


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_unrecovered_pullback_reaches_stop_using_fill_not_terminal_wick(side):
    tr, _, path = example([
        (100., 100.1, 99.9, 100.), (100., 100.1, 99.9, 100.),
        (100., 100.1, 99.5, 99.6), (99.6, 100., 90., 91.),
    ], side=side, reason="STOP", price=98.75)
    result = analyze_window(tr, window(), path)
    assert result["outcome_code"] == "SL_BEFORE_CLOSE_RECOVERY"
    assert result["recovery_time_ist"] == ""
    assert result["worst_price"] == pytest.approx(98.75 if side == "LONG" else 101.25)
    assert result["worst_adverse_pct_anchor"] == pytest.approx(1.25)
    assert result["min_stop_headroom_pct_entry"] == pytest.approx(0.)


def test_earlier_recovery_is_not_attributed_to_later_stop():
    tr, _, path = example([
        (100., 100.1, 99.9, 100.), (100., 100.1, 99.9, 100.),
        (100., 100.1, 99.5, 100.), (100., 100.1, 99.8, 99.9),
        (99.9, 100., 98., 98.2),
    ], reason="STOP", price=98.75)
    result = analyze_window(tr, window(), path)
    assert result["outcome_code"] == "RECOVERED_THEN_LATER_SL"
    assert result["worst_adverse_pct_anchor"] == pytest.approx(.5)
    assert result["min_stop_headroom_pct_entry"] == pytest.approx(.75)


def test_next_bar_open_after_recovery_is_not_part_of_recovered_pullback():
    tr, _, path = example([
        (100., 100.1, 99.9, 100.), (100., 100.1, 99.9, 100.),
        (100., 100.1, 99.5, 100.), (99., 102.1, 98.9, 102.),
    ])
    result = analyze_window(tr, window(), path)
    # The 09:32 completed close recovers the dip; the next open shares its
    # timestamp but occurs later. The second dip is not this window's depth.
    assert result["worst_adverse_pct_anchor"] == pytest.approx(.5)
    assert result["min_stop_headroom_pct_entry"] == pytest.approx(.75)


def test_recovery_close_precedes_next_open_stop_even_with_equal_clock_labels():
    tr, _, path = example([
        (100., 100.1, 99.9, 100.), (100., 100.1, 99.9, 100.),
        (100., 100.1, 99.5, 100.), (98.5, 99., 98., 98.8),
    ], reason="STOP", price=98.5, mode="OPEN")
    result = analyze_window(tr, window(), path)
    assert result["outcome_code"] == "RECOVERED_THEN_LATER_SL"
    assert result["worst_adverse_pct_anchor"] == pytest.approx(.5)


def test_safe_entry_close_and_terminal_fill_exclude_unknown_boundary_extremes():
    tr, minute, path = example([
        (99., 105., 90., 99.8), (99.8, 100.2, 99.7, 100.),
        (100., 120., 80., 110.),
    ])
    summary = summarize_path(tr, path, [])
    entry_close = path.loc[path.point_kind.eq("COMPLETED_CLOSE") & path.ts.eq(ENTRY)].iloc[0]
    assert entry_close.price == pytest.approx(99.8)
    assert pd.isna(entry_close.observed_low) and pd.isna(entry_close.observed_high)
    assert summary["observed_max_adverse_from_entry_pct"] == pytest.approx(.3)
    assert summary["observed_max_favorable_from_entry_pct"] == pytest.approx(2.)
    assert summary["actual_pullback_windows"] == 0
    assert minute.low.iloc[-1] == 80., "input frame must not be rewritten"


def test_same_entry_bar_exit_does_not_invent_post_entry_close():
    tr, _, path = example([(100., 110., 90., 105.)])
    summary = summarize_path(tr, path, [])
    assert path.point_kind.tolist() == ["ENTRY_FILL", "EXIT_FILL"]
    assert summary["post_entry_path_points"] == 0
    assert summary["observed_max_adverse_from_entry_pct"] == 0.
    assert summary["actual_pullback_windows"] == 0
    assert "does not prove" in summary["diagnostic_note"]


def test_close_exit_includes_entire_terminal_candle():
    tr, _, path = example([
        (100., 100.1, 99.9, 100.), (100., 100.1, 99.9, 100.),
        (100., 100.1, 99.5, 100.),
    ], reason="TIME_EXIT_1515", price=100., mode="CLOSE")
    result = analyze_window(tr, window(), path)
    assert result["worst_adverse_pct_anchor"] == pytest.approx(.5)
    assert result["recovery_time_ist"] == DAY + " 09:32:00"
    assert result["outcome_code"] == "TIME_EXIT_TOO_SHORT"


def test_exit_open_event_cannot_recover_at_preceding_close_with_same_timestamp():
    tr, _, path = example([
        (100., 100.1, 99.9, 100.), (100., 100.1, 99.9, 100.),
        (100., 100.1, 99.9, 100.), (99.5, 102.1, 99.4, 102.),
    ])
    result = analyze_window(tr, window(source="EXIT_CANDLE_OPEN"), path)
    assert result["recovery_basis"] == "EXIT_FILL"
    assert result["recovery_time_ist"] == DAY + " 09:33:00"
    assert result["worst_adverse_pct_anchor"] == pytest.approx(.5)


def test_actual_exit_fill_can_be_first_confirmed_pullback_with_interval_time():
    tr, _, path = example([
        (100., 100.1, 99.9, 100.), (100., 100.1, 99.9, 100.),
        (100., 100.1, 99.9, 100.), (100., 100.1, 90., 91.),
    ], reason="STOP", price=98.75)
    result = analyze_window(tr, window(event_minute=3, source="ACTUAL_EXIT_FILL"), path)
    assert result["threshold_hit_from_ist"] == DAY + " 09:32:00"
    assert result["threshold_hit_by_ist"] == DAY + " 09:33:00"
    assert result["sl_at_hit"] == pytest.approx(98.75)
    assert result["outcome_code"] == "SL_BEFORE_CLOSE_RECOVERY"
    assert result["minutes_hit_to_exit"] == 0.
    assert result["worst_adverse_pct_anchor"] == pytest.approx(1.25)


def test_time_exit_sideways_classification_depends_on_post_hit_closes():
    rows = [(100., 100.1, 99.9, 100.), (100., 100.1, 99.9, 100.)]
    rows.extend([(99.5, 99.6, 99.4, 99.5)]*16)
    tr, _, path = example(rows, reason="TIME_EXIT_1515", price=99.5, mode="CLOSE")
    result = analyze_window(tr, window(), path)
    assert result["outcome_code"] == "SIDEWAYS_TIME_EXIT"
    assert result["sideways_observation_minutes"] == 15.
    assert result["recovery_time_ist"] == ""
    assert result["sideways_close_range_pct"] == 0.
    assert result["worst_adverse_pct_anchor"] == pytest.approx(.6)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_stop_tightening_uses_candle_open_not_end(side):
    tr, _, _ = example([(100., 100.1, 99.9, 100.)], side=side)
    old = 98.75 if side == "LONG" else 101.25
    new = 99. if side == "LONG" else 101.
    assert stop_for_bar(tr, ENTRY+pd.Timedelta(minutes=120)) == old
    assert stop_at_clock(tr, ENTRY+pd.Timedelta(minutes=120)) == new
    assert stop_for_bar(tr, ENTRY+pd.Timedelta(minutes=121)) == new


def points(prices, *, interval="min"):
    return pd.DataFrame(dict(ts=pd.date_range(ENTRY, periods=len(prices), freq=interval),
                             price=prices, point_kind="COMPLETED_CLOSE"))


def test_sideways_requires_duration_range_and_low_net_change():
    stable = sideways_metrics(points([100., 100.1]*8), 100., 1)
    assert stable["sideways_test_passed"]
    assert stable["sideways_observation_minutes"] == 15
    short = sideways_metrics(points([100.]*15), 100., 1)
    assert not short["sideways_test_passed"] and not short["sideways_sufficient"]
    wide = sideways_metrics(points([100., 100.31]*8), 100., 1)
    assert not wide["sideways_test_passed"]
    trend = sideways_metrics(points(np.linspace(100., 100.2, 16)), 100., 1)
    assert not trend["sideways_test_passed"]
    sparse = sideways_metrics(points([100.]*4, interval="5min"), 100., 1)
    assert sparse["sideways_observation_minutes"] == 15
    assert not sparse["sideways_test_passed"] and not sparse["sideways_sufficient"]


def test_sideways_uses_closes_and_fills_not_entry_or_exit_open_points():
    dense = points([100.]*16)
    excluded = pd.DataFrame([dict(ts=ENTRY-pd.Timedelta(minutes=60), price=80., point_kind="ENTRY_FILL"),
                             dict(ts=ENTRY+pd.Timedelta(minutes=15), price=120., point_kind="EXIT_BAR_OPEN")])
    metrics = sideways_metrics(pd.concat([dense, excluded], ignore_index=True), 100., 1)
    assert metrics["sideways_test_passed"]
    assert metrics["sideways_close_range_pct"] == 0.


def test_same_bar_stop_target_ambiguity_overrides_other_classification():
    tr, _, path = example([
        (100., 100.1, 99.9, 100.), (100., 100.1, 99.9, 100.),
        (100., 100.1, 99.5, 99.6), (99.6, 103., 98., 101.),
    ], reason="STOP", price=98.75)
    tr["same_bar_ambiguous"] = True
    result = analyze_window(tr, window(), path)
    assert result["outcome_code"] == "AMBIGUOUS_STOP_TARGET_ORDER"
    assert result["same_bar_ambiguous"]


def test_path_crossing_active_stop_before_non_open_exit_is_rejected():
    tr, _, path = example([
        (100., 100.1, 99.9, 100.), (100., 100.1, 99.9, 100.),
        (100., 100.1, 98., 99.6), (99.6, 102.1, 99.5, 102.),
    ])
    with pytest.raises(ValueError, match="crossed active stop"):
        analyze_window(tr, window(), path)
