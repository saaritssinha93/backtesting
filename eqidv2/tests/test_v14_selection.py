"""Uncapped V14 selection retains eligibility and downstream capital controls."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from v14 import baseline, selection


def candidate(rule, sid, day="2026-10-08", **changes):
    stamp = pd.Timestamp(f"{day} {rule['signal_end']}", tz=baseline.IST)
    sign = 1 if rule["side"] == "LONG" else -1
    return dict(sid=sid, day=day, tradingsymbol=f"CASH{sid:03}", signal_ts=stamp,
                confirmation_ts=stamp + pd.Timedelta(minutes=1), side=rule["side"],
                hhmm_int=int(stamp.strftime("%H%M")),
                price_change_pct=sign * rule["price_change_pct"],
                volume_ratio=max(rule["volume_ratio"], 3.), body_ratio=.8,
                wick_ratio=.1, traded_value=1e7, v9_1m_volume_ratio=1.2,
                trigger=100.) | changes


@pytest.mark.parametrize("pair", baseline.setup_rules(),
                         ids=lambda pair: pair["expanded"]["signal_end"] + "_" + pair["expanded"]["side"])
def test_every_slot_selects_all_eligible_on_each_day(pair):
    # Seven stocks exceed every old one/two-stock quota, on each of two days.
    rows = pd.DataFrame([candidate(pair["expanded"], sid + day_no * 10, day)
                         for day_no, day in enumerate(("2026-10-08", "2026-10-09"))
                         for sid in range(7)])
    before = rows.copy(deep=True)
    chosen = selection.select_orders(rows)
    assert chosen.groupby("day").size().tolist() == [7, 7]
    assert set(chosen.sid) == set(rows.sid)
    assert "max_entries" not in chosen
    assert chosen.slot_selection_policy.eq(selection.UNLIMITED_SELECTION).all()
    assert chosen.groupby("day").v9_rank_in_setup_day.max().tolist() == [7, 7]
    pd.testing.assert_frame_equal(rows, before)
    legacy = selection.select_orders(rows, selection_policy=selection.FROZEN_G3_SELECTION)
    pd.testing.assert_frame_equal(legacy, baseline.select_orders(rows))
    assert legacy.groupby("day").size().eq(pair["expanded"]["max_entries"]).all()


def test_core_and_expanded_both_survive_without_loosening_any_gate():
    pair = next(p for p in baseline.setup_rules() if p["expanded"]["signal_end"] == "09:50")
    rule = pair["expanded"]
    rows = pd.DataFrame([
        candidate(rule, 0, price_change_pct=-.15, traded_value=9e7),
        candidate(rule, 1, price_change_pct=-.25),
        candidate(rule, 2, price_change_pct=-.12),
        candidate(rule, 3, v9_1m_volume_ratio=1.199),
        candidate(rule, 4, body_ratio=.39),
        candidate(rule, 5, wick_ratio=.61),
        candidate(rule, 6, volume_ratio=.99),
        candidate(rule, 7),  # Rejected only by replacement eligibility.
    ])
    chosen = selection.select_orders(rows, lambda group, setup: group.sid.ne(7))
    assert chosen.sid.tolist() == [0, 1]
    assert chosen.v10_g_f_core.tolist() == [False, True]
    assert chosen.v9_rank_in_setup_day.tolist() == [1, 2]
    assert baseline.select_orders(rows).sid.tolist() == [1]


def test_active_config_and_empty_outputs_have_no_maximum_selection_column():
    original = baseline.frozen_config()
    active = selection.setup_rules()
    assert len(active) == 14
    for pair in active:
        assert all("max_entries" not in pair[name] for name in ("original", "core", "expanded"))
    rows = pd.DataFrame([candidate(active[0]["expanded"], 0, max_entries=1)])
    for frame in (rows, rows.iloc[:0], rows.assign(body_ratio=0.)):
        assert "max_entries" not in selection.select_orders(frame)
    assert baseline.frozen_config() == original
    assert all(pair["expanded"]["max_entries"] in (1, 2) for pair in baseline.setup_rules())
    baseline.verify_snapshot()


def test_uncapped_selection_still_enforces_original_portfolio_capital():
    rule = next(p["expanded"] for p in selection.setup_rules()
                if p["expanded"]["signal_end"] == "09:30" and p["expanded"]["side"] == "LONG")
    orders = selection.select_orders(pd.DataFrame([candidate(rule, sid) for sid in range(13)]))
    assert len(orders) == 13
    times = pd.date_range("2026-10-08 09:32", "2026-10-08 15:15", freq="min", tz=baseline.IST)
    path = dict(timestamp_ns=times.asi8, open=np.full(len(times), 100.),
                high=np.full(len(times), 100.1), low=np.full(len(times), 99.9), close=np.full(len(times), 100.))
    ledger = baseline.simulate(orders, {sid: path for sid in orders.sid})
    assert ledger.portfolio_executed.sum() == 10
    assert ledger.portfolio_reject_reason.eq("INSUFFICIENT_PORTFOLIO_CAPITAL").sum() == 3
    assert "max_entries" not in ledger


def test_invalid_confirmation_and_unknown_policy_fail_closed():
    rows = pd.DataFrame([candidate(selection.setup_rules()[0]["expanded"], 0)])
    rows["confirmation_ts"] += pd.Timedelta(minutes=1)
    with pytest.raises(ValueError, match="exact next-minute"):
        selection.select_orders(rows)
    with pytest.raises(ValueError, match="Unsupported slot selection policy"):
        selection.select_orders(rows.iloc[:0], selection_policy="typo")
