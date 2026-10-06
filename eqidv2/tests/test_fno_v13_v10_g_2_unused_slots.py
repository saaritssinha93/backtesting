"""Causal capital reservations for isolated unused-window research."""
from types import SimpleNamespace

import pandas as pd
import pytest

import fno_v13_v10_g_2_unused_slots_research as research


def stamp(clock, day="2026-10-05"):
    return pd.Timestamp(f"{day} {clock}", tz="Asia/Kolkata")


def order(*, confirmation="10:06", entry=None, exit_time=None, filled=False,
          side="LONG", key="order", day="2026-10-05"):
    return {
        "day": pd.Timestamp(day).date(),
        "confirmation_ts": stamp(confirmation, day),
        "entry_ts": stamp(entry, day) if entry else pd.NaT,
        "exit_ts": stamp(exit_time, day) if exit_time else pd.NaT,
        "filled": filled,
        "side": side,
        "tradingsymbol": key,
        "research_key": key,
    }


def book(*rows):
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=order().keys())


@pytest.mark.parametrize(
    "clock, expected", [("10:05", 0), ("10:06", 1), ("10:15", 1),
                        ("10:16", 1), ("10:17", 0)]
)
def test_eventually_unfilled_order_reserves_through_expiry(clock, expected):
    assert research.occupied_orders(book(order()), stamp(clock)) == expected


def test_future_fill_outcome_does_not_change_current_pending_reservation():
    eventual_unfilled = book(order())
    eventual_winner = book(order(entry="10:14", exit_time="11:10", filled=True))
    eventual_loser = book(order(entry="10:08", exit_time="10:09", filled=True))
    now = stamp("10:07")
    assert [research.occupied_orders(rows, now)
            for rows in (eventual_unfilled, eventual_winner, eventual_loser)] == [1, 1, 1]


@pytest.mark.parametrize(
    "clock, expected", [("10:08", 1), ("11:59", 1), ("12:00", 1), ("12:01", 0)]
)
def test_entered_position_remains_reserved_through_same_bar_exit(clock, expected):
    rows = book(order(entry="10:08", exit_time="12:00", filled=True))
    assert research.occupied_orders(rows, stamp(clock)) == expected


def test_future_exit_mutation_cannot_release_an_open_position():
    earlier = book(order(entry="10:08", exit_time="12:00", filled=True))
    later = book(order(entry="10:08", exit_time="15:15", filled=True))
    assert research.occupied_orders(earlier, stamp("11:00")) == 1
    assert research.occupied_orders(later, stamp("11:00")) == 1


def test_future_unconfirmed_order_is_not_counted_as_already_pending():
    rows = book(order(confirmation="11:21", entry="11:22", exit_time="15:15", filled=True))
    assert research.occupied_orders(rows, stamp("10:06")) == 0


def test_future_setup_quota_reserved_even_without_eventual_selected_order():
    specs = [SimpleNamespace(confirmation_end="11:21", max_entries=1)]
    assert research.reserved_baseline_slots(book(), stamp("10:06"), specs) == 1
    # At confirmation, the observed absence of a selection releases its quota.
    assert research.reserved_baseline_slots(book(), stamp("11:21"), specs) == 0


def test_future_setup_quota_transfers_to_pending_then_open_without_double_count():
    specs = [SimpleNamespace(confirmation_end="11:21", max_entries=1)]
    rows = book(order(confirmation="11:21", entry="11:26", exit_time="13:00", filled=True))
    for clock in ("10:06", "11:21", "11:24", "11:26", "12:59", "13:00"):
        assert research.reserved_baseline_slots(rows, stamp(clock), specs) == 1
    assert research.reserved_baseline_slots(rows, stamp("13:01"), specs) == 0


def test_baseline_pending_morning_and_future_midday_quota_both_reserved():
    specs = [SimpleNamespace(confirmation_end="10:01", max_entries=1),
             SimpleNamespace(confirmation_end="11:21", max_entries=1)]
    rows = book(order(confirmation="10:01"))
    assert research.reserved_baseline_slots(rows, stamp("10:06"), specs) == 2
    assert research.reserved_baseline_slots(rows, stamp("10:11"), specs) == 2
    assert research.reserved_baseline_slots(rows, stamp("10:12"), specs) == 1


def test_baseline_reservations_only_use_current_day():
    specs = [SimpleNamespace(confirmation_end="11:21", max_entries=1)]
    rows = book(order(day="2026-10-04", entry="10:08", exit_time="15:15", filled=True))
    assert research.reserved_baseline_slots(rows, stamp("10:06"), specs) == 1


def test_eventually_unfilled_addon_can_block_second_until_expiry():
    specs = [SimpleNamespace(confirmation_end="11:21", max_entries=1)]
    addons = book(order(key="first"), order(confirmation="10:11", key="second"))
    admitted, rejected = research.admit_additions(addons, book(), specs, capacity=2)
    assert admitted.research_key.tolist() == ["first"]
    assert rejected == [{"case_order_key": "second", "reason": "BASELINE_CAPACITY_RESERVED", "reserved_slots": 2}]


def test_resolved_unfilled_addon_releases_capital_for_later_confirmation():
    specs = [SimpleNamespace(confirmation_end="11:21", max_entries=1)]
    addons = book(order(key="first"), order(confirmation="10:21", key="second"))
    admitted, rejected = research.admit_additions(addons, book(), specs, capacity=2)
    assert admitted.research_key.tolist() == ["first", "second"]
    assert rejected == []


def test_simultaneous_addons_reserve_capital_sequentially():
    specs = [SimpleNamespace(confirmation_end="11:21", max_entries=1)]
    addons = book(order(side="SHORT", key="short"), order(side="LONG", key="long"))
    admitted, rejected = research.admit_additions(addons, book(), specs, capacity=2)
    assert admitted.research_key.tolist() == ["long"]
    assert [row["case_order_key"] for row in rejected] == ["short"]


def test_same_bar_baseline_exit_does_not_fund_addon():
    baseline = book(order(confirmation="10:01", entry="10:02", exit_time="10:06", filled=True))
    admitted, rejected = research.admit_additions(book(order()), baseline, [], capacity=1)
    assert admitted.empty
    assert len(rejected) == 1


def test_new_setups_exclude_occupied_cell_and_inherit_explicit_donor_targets():
    source = research.g2.read_json(research.g2.DEFAULT_G_CONFIG)
    new = research.donor_setups(source)
    keys = {(setup.signal_end, setup.side) for setup in new}
    assert len(new) == len(keys) == 95
    assert ("11:20", "SHORT") not in keys
    assert ("11:20", "LONG") in keys
    assert ("10:05", "LONG") in keys and ("14:00", "SHORT") in keys
    for setup in new:
        assert setup.stop_pct == 1.25
        assert setup.target_pct == (3.0 if setup.side == "LONG" else 2.0)
        assert setup.max_entries == 1
        expected_confirmation = (pd.Timestamp("2000-01-01 " + setup.signal_end)
                                 + pd.Timedelta(minutes=1)).strftime("%H:%M")
        assert setup.confirmation_end == expected_confirmation
