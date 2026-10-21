"""Allocator parity and deterministic whole-setup coordinate search."""
import json

import numpy as np
import pandas as pd
import pytest

from research import g4_afternoon_portfolio_target_refinement as study


@pytest.mark.parametrize("count,wins,pnl,expected", [(10, 7, 1., True), (10, 6, 100., False),
    (4, 4, 100., False), (5, 4, 100., True), (10, 7, 0., False),
    (10, 7, -1., False), (10, 7, float("nan"), False), (10, 7, float("inf"), False)])
def test_exact_70_positive_and_minimum_boundary(count, wins, pnl, expected):
    assert study.passes(count, wins, pnl) is expected


def test_heap_releases_equal_timestamp_and_rejected_orders_do_not_reserve_capital():
    entries = np.array([0, 0, 1, 2, 3])
    exits = np.array([2, 9, 8, 3, 7])
    # Capacity one: order 0 wins the tie, 1 and 2 never reserve capital.
    assert study.heap_accept(entries, exits, np.arange(5), capacity=1).tolist() == [0, 3, 4]
    assert study.heap_accept(entries, exits, np.array([1, 0, 2, 3, 4]), capacity=1).tolist() == [1]


def row(sid, setup, *, entry="12:37", exit="13:00", liquidity=100., symbol="ABC", net=.5):
    return dict(sid=sid, signal_id=f"s{sid}", day="2026-10-12", setup_id=setup, tradingsymbol=symbol,
        filled=True, entry_ts=f"2026-10-12 {entry}:00+05:30", exit_ts=f"2026-10-12 {exit}:00+05:30",
        confirmation_ts="2026-10-12 12:36:00+05:30", entry_price=100., picker="max_liquidity",
        traded_value=liquidity, volume_ratio=1., price_change_pct=.3, net_return_pct=net,
        gross_return_pct=net+.05, cost_pct=.05, initial_stop_pct=1.25, target_pct=.5)


def test_fast_book_exact_native_tie_priority_and_capital_release():
    # Nine older positions leave one place for four simultaneous new candidates.
    baseline = study.allocate(pd.DataFrame([row(i, "BASE", entry="12:30", exit="13:00") for i in range(9)]))
    part = pd.DataFrame([
        row(100, "B", liquidity=10000.),  # setup name sorts before liquidity
        row(101, "A", liquidity=100., symbol="ZZZ"),
        row(102, "A", liquidity=100., symbol="AAA"),
        row(103, "A", liquidity=50., symbol="AAA"),
        row(104, "C", entry="13:00", exit="13:10"),  # release all ten at exactly 13:00
    ])
    target2 = part.copy()
    target2["target_pct"] = 1.
    target2["net_return_pct"] = .9
    target2["gross_return_pct"] = .95
    cache = pd.concat([part, target2], ignore_index=True)
    book = study.FastBook(baseline, cache, ["2026-10-12"])
    targets = {"A": .5, "B": .5, "C": .5}
    native = study.allocate(pd.concat([baseline, part], ignore_index=True))
    fast_ids = set(book.frame.iloc[book.state(targets)[1]].sid)
    assert fast_ids == set(native.loc[native.portfolio_executed, "sid"]) == set(range(9)) | {102, 104}
    assert book.evaluate(targets)["combined_net_pnl_rs"] == pytest.approx(native.portfolio_net_profit_rupees.sum())
    assert not book.evaluate(targets)["eligible"]  # insufficient actual fills in each new setup


def test_fast_book_rejects_changed_entries_between_target_levels():
    baseline = study.allocate(pd.DataFrame([row(1, "BASE")]))
    one, two = row(2, "NEW"), row(2, "NEW", entry="12:38")
    two["target_pct"] = 1.
    with pytest.raises(AssertionError):
        study.FastBook(baseline, pd.DataFrame([one, two]), ["2026-10-12"])


def metrics(profit, eligible=True):
    return dict(eligible=eligible, combined_net_pnl_rs=float(profit),
                combined_win_rate_pct=80., added_win_rate_pct=80., baseline_trades_changed=0)


def test_search_add_change_delete_and_recompute_best_gain():
    # A deterministic objective needing change, then addition, then deletion.
    profits = {
        (("A", 1.), ("B", 1.)): 100.,
        (("A", 2.), ("B", 1.)): 150.,
        (("A", 2.), ("B", 1.), ("C", 1.)): 180.,
        (("A", 2.), ("C", 1.)): 200.,
    }
    evaluate = lambda candidate: metrics(profits.get(tuple(sorted(candidate.items())), 90.))
    initial = {"B": 1., "A": 1.}
    result, trials, accepted, converged = study.coordinate_refine(initial, ["C", "B", "A"], [2., 1.], evaluate)
    assert result == {"A": 2., "C": 1.}
    assert initial == {"A": 1., "B": 1.}
    assert [(row["operation"], row["setup_id"]) for row in accepted[1:]] == [("change", "A"), ("add", "C"), ("delete", "B")]
    assert converged
    assert all(json.loads(row["target_map"]) for row in trials)
    assert len([row for row in trials if row["accepted"]]) == 4


def test_search_ignores_ineligible_best_uses_deterministic_tie_and_enforces_pass_limit():
    def evaluate(candidate):
        if candidate == {"A": 1., "B": 1., "C": 1.}:
            return metrics(1000., False)
        return metrics(100. if len(candidate) == 2 else 120.)
    final, _, maps, converged = study.coordinate_refine({"B": 1., "A": 1.}, ["C", "B", "A"], [1.], evaluate, max_passes=1)
    assert final == {"B": 1.}  # tied removals choose A by setup ID
    assert maps[-1]["operation"] == "delete"
    assert not converged  # an accepted final allowed pass cannot claim convergence


def test_search_requires_nonempty_eligible_start_and_positive_gain():
    with pytest.raises(ValueError):
        study.coordinate_refine({}, ["A"], [1.], lambda _: metrics(100.))
    with pytest.raises(ValueError):
        study.coordinate_refine({"A": 1.}, ["A"], [1.], lambda _: metrics(100., False))
    final, _, maps, converged = study.coordinate_refine({"A": 1.}, ["A", "B"], [1., 2.], lambda _: metrics(100.))
    assert final == {"A": 1.} and len(maps) == 1 and converged


def test_fast_book_cannot_hide_zero_execution_setup_from_gate():
    # A second setup has five potential wins but all collide with ten older positions.
    baseline_rows, add_rows = [], []
    for day in range(1, 6):
        for j in range(10):
            base = row(day * 100 + j, "BASE", entry="12:30", exit="13:00")
            for key in ("day", "entry_ts", "exit_ts", "confirmation_ts"):
                base[key] = base[key].replace("2026-10-12", f"2026-10-{day:02d}")
            baseline_rows.append(base)
        added = row(1000 + day, "BLOCKED")
        for key in ("day", "entry_ts", "exit_ts", "confirmation_ts"):
            added[key] = added[key].replace("2026-10-12", f"2026-10-{day:02d}")
        add_rows.append(added)
    baseline = study.allocate(pd.DataFrame(baseline_rows))
    book = study.FastBook(baseline, pd.DataFrame(add_rows), [f"2026-10-{i:02d}" for i in range(1, 6)])
    result = book.evaluate({"BLOCKED": .5})
    assert result["added_executed_trades"] == 0
    assert not result["every_setup_70_positive"] and not result["eligible"]


def test_native_guard_rejects_setup_passing_only_after_capital_blocks_losses():
    baseline_rows, add_rows = [], []
    days = [f"2026-10-{i:02d}" for i in range(1, 11)]
    for i, day in enumerate(days):
        # Last four losing native fills collide with ten older winners.
        for j in range(10 if i >= 6 else 1):
            base = row(i * 100 + j, "BASE", entry="12:30", exit="13:00")
            for key in ("day", "entry_ts", "exit_ts", "confirmation_ts"):
                base[key] = base[key].replace("2026-10-12", day)
            baseline_rows.append(base)
        added = row(10000 + i, "NEW", net=.5 if i < 6 else -.1)
        for key in ("day", "entry_ts", "exit_ts", "confirmation_ts"):
            added[key] = added[key].replace("2026-10-12", day)
        add_rows.append(added)
    baseline, cache = study.allocate(pd.DataFrame(baseline_rows)), pd.DataFrame(add_rows)
    ordinary = study.FastBook(baseline, cache, days)
    guarded = study.FastBook(baseline, cache, days, require_native70=True)
    actual, dual = ordinary.evaluate({"NEW": .5}), guarded.evaluate({"NEW": .5})
    assert actual["eligible"] and actual["every_setup_70_positive"]
    assert actual["added_executed_trades"] == 6 and actual["added_win_rate_pct"] == 100.
    assert not actual["native_every_setup_70_positive"]
    assert not dual["eligible"] and dual["every_setup_70_positive"]
    assert not dual["native_every_setup_70_positive"]
    assert ordinary.state({"NEW": .5})[1].tolist() == guarded.state({"NEW": .5})[1].tolist()
    stats = study.standalone_stats(cache)
    assert stats["standalone_native_fills"] == 10
    assert stats["standalone_native_wins"] == 6
    assert stats["standalone_native_win_rate_pct"] == 60.
    assert stats["standalone_native_net_pnl_rs"] == pytest.approx(13000.)


@pytest.mark.parametrize("profits,passes", [
    ([.5] * 7 + [-.1] * 3, True),  # Inclusive exactly 70%.
    ([.1] * 7 + [-1.] * 3, False),  # Wins alone cannot conceal negative native P&L.
    ([.5] * 4, False),  # At least five native fills required.
])
def test_precomputed_native_eligibility_checks_each_target_and_all_native_fills(profits, passes):
    baseline = study.allocate(pd.DataFrame([row(1, "BASE", entry="12:30", exit="12:31")]))
    cache = pd.DataFrame([row(100+i, "NEW", net=profit) for i, profit in enumerate(profits)])
    other = cache.copy()
    other["target_pct"] = 1.
    other["net_return_pct"] = -.1
    other["gross_return_pct"] = -.05
    book = study.FastBook(baseline, pd.concat([cache, other], ignore_index=True), ["2026-10-12"], require_native70=True)
    assert bool(book.native_qualification[book.target_index[.5], 0]) is passes
    assert not book.native_qualification[book.target_index[1.], 0]
