"""Causal and entry-preserving integration checks for the setup exit engine."""
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from research.g3_sl_target_simulator import simulate_exit
from research.g4_setup_exit_engine_v2 import ExitVariant, replay_raw, variants_for_setup


def minute_path(rows, side="LONG", start="2026-10-16 15:10"):
    prices = np.asarray(rows, dtype=float)
    if side == "SHORT":
        prices = 200-prices
        prices[:, [1, 2]] = prices[:, [2, 1]]
    return {"timestamp_ns": pd.date_range(start, periods=len(prices), freq="min",
                                         tz="Asia/Kolkata").asi8,
            **{key: prices[:, i] for i, key in enumerate(("open", "high", "low", "close"))}}


def native_book(path, side="LONG", target=1.5):
    entry = pd.Timestamp(int(path["timestamp_ns"][0]), tz="UTC").tz_convert("Asia/Kolkata")
    result = simulate_exit(path, 0, 100., side == "LONG", 1.2, target,
                           tighten_after_minutes=120, tightened_stop_pct=1.)
    row = dict(sid=11, setup_id="0921_"+side, side=side, filled=True,
               entry_price=100., entry_ts=entry, entry_path_index=0,
               confirmation_ts=entry-pd.Timedelta(minutes=1),
               signal_ts=entry-pd.Timedelta(minutes=2),
               v9_1m_feature_ts=entry-pd.Timedelta(minutes=1),
               native_target_pct=target, native_stop_pct=1.2, cost_pct=.05,
               net_return_pct=result["gross_return_pct"]-.05,
               portfolio_net_profit_rupees=123., **result)
    return pd.DataFrame([row])


def test_catalog_is_fixed_bounded_and_removes_clipping_duplicates():
    grid = variants_for_setup(1.5)
    assert len(grid) == 11
    assert grid[0] == ExitVariant("baseline", 1.5)
    assert len(variants_for_setup(2.)) == 10
    assert len(variants_for_setup(.6)) == 10
    assert all(.6 <= v.target_pct <= 2. and v.stop_pct <= 1.2 for v in grid)
    assert {v.name for v in grid if v.protect_after_close_pct is not None} == {
        "protect_040_floor_010", "protect_040_floor_020",
        "protect_060_floor_010", "protect_060_floor_020"}
    with pytest.raises(ValueError, match="bounds"):
        variants_for_setup(2.5)
    with pytest.raises(ValueError, match="loosen"):
        variants_for_setup(1.5, .8)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_baseline_replays_but_preserves_every_original_field(side):
    path = minute_path([[100, 100.1, 99.9, 100], [100, 102, 99.9, 101]], side)
    book = native_book(path, side)
    book["unrelated_research_metadata"] = ["preserve"]
    actual = replay_raw(book, {11: path}, "0921_"+side, ExitVariant("baseline", 1.5))
    pd.testing.assert_frame_equal(actual, book)
    assert actual is not book
    changed = book.copy()
    changed.loc[0, "exit_price"] += .2
    with pytest.raises(ValueError, match="Baseline exit_price"):
        replay_raw(changed, {11: path}, "0921_"+side, ExitVariant("baseline", 1.5))


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_higher_target_continues_beyond_old_exit(side):
    path = minute_path([[100, 100.1, 99.9, 100], [100, 101.1, 99.9, 100.8],
                        [100.8, 101.3, 100.7, 101.2]], side)
    book = native_book(path, side, target=1.)
    variant = next(v for v in variants_for_setup(1.) if v.name == "target_x120")
    actual = replay_raw(book, {11: path}, "0921_"+side, variant)
    assert book.iloc[0].exit_path_index == 1
    assert actual.iloc[0].exit_path_index == 2
    assert actual.iloc[0].gross_return_pct == pytest.approx(1.2)
    assert actual.iloc[0].net_return_pct == pytest.approx(1.15)
    assert actual.iloc[0].entry_ts == book.iloc[0].entry_ts
    assert actual.attrs["requires_portfolio_reallocation"]


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_close_protection_cannot_retroactively_apply_to_arming_bar(side):
    path = minute_path([[100, 100.1, 99.9, 100],
                       [100, 100.8, 99.5, 100.7],
                       [100.6, 100.7, 100.05, 100.2],
                       [100.2, 100.3, 100., 100.1],
                       [100.1, 100.2, 100., 100.1],
                       [100.1, 100.2, 100., 100.1]], side)
    book = native_book(path, side)
    variant = ExitVariant("protection", 1.5, protect_after_close_pct=.6,
                          protected_profit_pct=.1)
    result = replay_raw(book, {11: path}, "0921_"+side, variant).iloc[0]
    assert result.protection_armed_path_index == 1
    assert result.protection_effective_path_index == 2
    assert result.exit_path_index == 2
    assert result.exit_reason == "PROTECTED_STOP"
    assert result.gross_return_pct == pytest.approx(.1)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_entry_close_and_intrabar_high_never_arm_protection(side):
    path = minute_path([[100, 100.8, 99.9, 100.7],
                       [100.2, 100.8, 99.9, 100.2],
                       [100.2, 100.3, 99.9, 100.1],
                       [100.1, 100.2, 99.9, 100.1],
                       [100.1, 100.2, 99.9, 100.1],
                       [100.1, 100.2, 99.9, 100.1]], side)
    book = native_book(path, side)
    variant = ExitVariant("protection", 1.5, protect_after_close_pct=.6,
                          protected_profit_pct=.1)
    result = replay_raw(book, {11: path}, "0921_"+side, variant).iloc[0]
    assert result.exit_reason == "TIME_EXIT_1515"
    assert pd.isna(result.protection_armed_path_index)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_ambiguous_intrabar_stop_target_is_stop_first(side):
    path = minute_path([[100, 100.1, 99.9, 100], [100, 102.5, 98., 100]], side)
    book = native_book(path, side)
    variant = replace(ExitVariant("baseline", 1.5), name="stop_080", stop_pct=.8,
                      tightened_stop_pct=.8)
    result = replay_raw(book, {11: path}, "0921_"+side, variant).iloc[0]
    assert result.exit_reason == "STOP"
    assert result.same_bar_ambiguous
    assert result.gross_return_pct == pytest.approx(-.8)


def test_unfilled_and_other_setup_rows_need_no_paths_and_keep_outcomes():
    path = minute_path([[100, 100.1, 99.9, 100], [100, 102., 99.9, 101.]])
    book = native_book(path)
    unfilled = book.iloc[0].copy()
    unfilled["sid"], unfilled["filled"], unfilled["exit_reason"] = 12, False, "UNFILLED_EXPIRED"
    other = book.iloc[0].copy()
    other["sid"], other["setup_id"] = 13, "0931_LONG"
    book = pd.concat([book, pd.DataFrame([unfilled, other])], ignore_index=True)
    actual = replay_raw(book, {11: path}, "0921_LONG", ExitVariant("target", 1.2))
    pd.testing.assert_frame_equal(actual.iloc[1:][book.columns], book.iloc[1:])
    assert actual.iloc[0].gross_return_pct == pytest.approx(1.2)


@pytest.mark.parametrize("field", ["entry_ts", "confirmation_ts", "v9_1m_feature_ts"])
def test_mismatched_or_future_source_clocks_are_rejected(field):
    path = minute_path([[100, 100.1, 99.9, 100], [100, 102., 99.9, 101.]])
    book = native_book(path)
    book.loc[0, field] = book.loc[0, "entry_ts"] + pd.Timedelta(minutes=1)
    with pytest.raises(ValueError, match="clock"):
        replay_raw(book, {11: path}, "0921_LONG", ExitVariant("baseline", 1.5))


def test_missing_new_candidate_path_and_truncated_old_exit_path_raise():
    path = minute_path([[100, 100.1, 99.9, 100], [100, 101.1, 99.9, 100.8]])
    book = native_book(path, target=1.)
    with pytest.raises(ValueError, match="Missing full minute path"):
        replay_raw(book, {}, "0921_LONG", ExitVariant("target", 1.2))
    with pytest.raises(ValueError, match="missing.*15:15"):
        replay_raw(book, {11: path}, "0921_LONG", ExitVariant("target", 1.2))


def test_duplicate_candidate_ids_are_rejected():
    path = minute_path([[100, 100.1, 99.9, 100], [100, 102., 99.9, 101.]])
    book = native_book(path)
    duplicated = pd.concat([book, book], ignore_index=True)
    with pytest.raises(ValueError, match="unique"):
        replay_raw(duplicated, {11: path}, "0921_LONG", ExitVariant("baseline", 1.5))
