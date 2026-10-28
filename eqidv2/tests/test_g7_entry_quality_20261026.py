import numpy as np
import pandas as pd
import pytest

from research.g7_entry_quality_20261026 import decisions, choose, same_book


def features():
    stamp = "2026-10-26 10:01:00+05:30"
    return pd.DataFrame(dict(
        confirmation_ts=[stamp]*4, feature_available_at=[stamp]*4,
        stock_context_available_at=[stamp]*4, vix_available_at=[stamp]*4,
        nifty_base_allowed=[False]*4, side_sign=[1, -1, 1, -1],
        nifty_slot_pct=[-.01, .01, -.11, .11], nifty_session_pct=[-.2, .2, -.2, .2],
        nifty_both_aligned=[False]*4, vix_30_pct=[0.]*4, vix_session_pct=[0.]*4,
        confirmation_close_position=[.8]*4, confirmation_body_direction=[.2]*4,
        stock_relative5_status=["OK"]*4, stock_relative_session_status=["OK"]*4,
        stock_relative5_pct=[.4]*4, stock_relative_session_pct=[.5]*4,
        pending_cancelled=[False]*4,
    ), index=[1, 2, 3, 4])


def test_neutral_band_is_symmetric_and_strong_counter_trend_remains_blocked():
    f = features()
    assert decisions(f)["nifty_deadband010"].tolist() == [True, True, False, False]
    f.loc[3, "nifty_slot_pct"] = -.10
    assert decisions(f)["nifty_deadband010"].loc[3]


def test_bad_or_missing_evidence_cannot_override_index():
    f = features()
    assert decisions(f)["stock_override"].all()
    f.loc[1, "stock_relative5_pct"] = np.nan
    f.loc[2, "stock_relative_session_status"] = "MISSING_OR_DUPLICATE_MINUTE"
    f.loc[3, "confirmation_body_direction"] = -.1
    f.loc[4, "confirmation_close_position"] = .5
    assert not decisions(f)["stock_override"].any()
    f.loc[1, "nifty_slot_pct"] = np.nan
    assert not decisions(f)["nifty_deadband010"].loc[1]


@pytest.mark.parametrize("column", ["feature_available_at", "stock_context_available_at", "vix_available_at"])
def test_features_from_future_are_rejected(column):
    f = features()
    f.loc[1, column] = "2026-10-26 10:02:00+05:30"
    with pytest.raises(ValueError, match="not available"):
        decisions(f)


def test_outcomes_cannot_change_entry_permission():
    f = features()
    before = decisions(f)
    after = decisions(f.assign(portfolio_net_profit_rupees=[1e9, -1e9, 1e9, -1e9], exit_reason="STOP"))
    for name in before:
        pd.testing.assert_series_equal(before[name], after[name])


def test_early_rule_uses_ist_confirmation_not_utc_or_entry_timestamp():
    f = features().assign(nifty_base_allowed=True)
    early = "2026-10-26 03:59:00+00:00"  #09:29 IST
    for col in ("confirmation_ts", "feature_available_at", "stock_context_available_at", "vix_available_at"):
        f.loc[1, col] = early
    assert decisions(f)["after0930"].tolist() == [False, True, True, True]


def comparison():
    rows = []
    for period in ("development", "validation", "assessment", "today"):
        for policy in ("g7_v2", "candidate", "bad"):
            improvement = 100 if policy == "candidate" else -10 if policy == "bad" else 0
            rows.append(dict(policy=policy, period=period, net_pnl_rs=1000.+improvement,
                net_change_rs=improvement, trades=100, daily_close_drawdown_rs=50,
                changed_executions=10, changed_days=5))
    return pd.DataFrame(rows)


def test_selection_cannot_use_known_today_or_later_assessment():
    rows = comparison()
    chosen, audit = choose(rows, ["candidate", "bad"])
    assert chosen == "candidate"
    rows.loc[rows.period.isin(["assessment", "today"]), "net_pnl_rs"] = -1e20
    assert choose(rows, ["candidate", "bad"]) == (chosen, audit)
    rows.loc[rows.policy.eq("candidate") & rows.period.eq("validation"), "net_pnl_rs"] = 999
    assert choose(rows, ["candidate", "bad"])[0] is None


def test_baseline_parity_catches_rejected_and_changed_fills():
    frame = pd.DataFrame(dict(day=["2026-10-26"], setup_id=["1000_LONG"], tradingsymbol=["TEST"],
        portfolio_executed=[True], entry_price=[100.], exit_price=[101.], portfolio_net_profit_rupees=[4750.],
        entry_ts=["2026-10-26 10:02:00+05:30"], exit_ts=["2026-10-26 10:10:00+05:30"]))
    same_book(frame, frame.copy())
    with pytest.raises(ValueError, match="allocations"):
        same_book(frame, frame.assign(portfolio_executed=False))
    with pytest.raises(ValueError, match="replay differs"):
        same_book(frame, frame.assign(entry_price=100.1))
