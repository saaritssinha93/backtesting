"""Causal minute admission and realized-loss locks on the complete inventory."""
import numpy as np
import pandas as pd
import pytest

from research import g7_v4_conservative_allocation as engine


CONFIG = dict(capital_per_entry_rs=100_000., leverage=5., portfolio_capital_rs=1_000_000.,
              max_positions_per_symbol=1, model_cost_bps_round_trip=5.)


def stamp(clock, day="2026-10-01"):
    return pd.Timestamp(f"{day} {clock}", tz="Asia/Kolkata")


def order(sid, entry="10:01", exit="10:05", *, gross=.6, symbol=None, filled=True,
          day="2026-10-01", event="INTRABAR"):
    return dict(sid=sid, day=day, setup_id="1000_LONG", tradingsymbol=symbol or f"STOCK{sid}",
                side="LONG", filled=filled, g8_baseline_filled=filled,
                entry_price=100., entry_path_index=0, entry_ts=stamp(entry, day),
                confirmation_ts=stamp(entry, day)-pd.Timedelta(minutes=1),
                exit_ts=stamp(exit, day), exit_event=event, exit_price=100.+gross,
                exit_reason="PROTECTED_STOP" if gross == .1 else "TARGET" if gross > 0 else "STOP",
                gross_return_pct=gross, net_return_pct=gross-.05, cost_pct=.05,
                initial_stop_pct=1.2, picker="max_liquidity", traded_value=1_000_000.)


def run(rows, *, cost=5., clock=True, limit=None, allowed=None, config=None):
    source = pd.DataFrame(rows)
    allowed = pd.Series(True, index=source.sid) if allowed is None else allowed
    return engine.allocate(source, allowed, config or CONFIG, "synthetic", cost,
                           conservative_entry_clock=clock, daily_loss_limit=limit)


def test_same_symbol_intrabar_exit_cannot_free_its_unknown_trigger_minute():
    rows = [order(1, symbol="BHEL"), order(2, entry="10:05", exit="10:10", symbol="BHEL")]
    actual = run(rows)
    old = run(rows, clock=False)
    assert actual.portfolio_executed.tolist() == [True, False]
    assert old.portfolio_executed.tolist() == [True, True]
    assert actual.loc[1, "entry_ts"] == stamp("10:05")
    assert actual.loc[1, "allocation_entry_ts"] == stamp("10:04")
    assert actual.loc[1, "portfolio_reject_reason"] == "MAX_SYMBOL_POSITIONS_REACHED"
    assert actual.loc[1, "exit_ts"] == stamp("10:10")


@pytest.mark.parametrize("exit", ["10:04", "10:05"])
def test_known_open_exit_releases_capacity_at_or_before_next_trigger_minute(exit):
    rows = [order(1, exit=exit, event="OPEN", symbol="SAME"),
            order(2, entry="10:06", exit="10:12", symbol="SAME")]
    assert run(rows).portfolio_executed.tolist() == [True, True]


def test_lock_counts_realized_exits_not_known_eventual_losers():
    rows = [order(1, exit="10:10", gross=-1.2), order(2, entry="10:02", exit="10:11", gross=-1.2),
            order(3, entry="10:06", exit="10:20", gross=.6),
            order(4, entry="10:13", exit="10:15", gross=.6)]
    result = run(rows, limit=2)
    assert result.portfolio_executed.tolist() == [True, True, True, False]
    assert result.final_entry_allowed.tolist() == [True, True, True, False]
    assert result.loss_lock_ts.eq(stamp("10:11")).all()
    assert result.loc[2, "exit_ts"] == stamp("10:20")  # Open trade is unmanaged.
    assert result.loc[3, "portfolio_status"] == "DAILY_LOSS_LIMIT_REACHED"


def test_loss_order_is_exit_order_not_source_or_entry_order():
    rows = [order(1, exit="10:20", gross=-1.2), order(2, entry="10:02", exit="10:04", gross=-1.2),
            order(3, entry="10:06", exit="10:07", gross=-1.2),
            order(4, entry="10:09", exit="10:12")]
    result = run(rows[::-1], limit=2).set_index("sid")
    assert result.loss_lock_ts.eq(stamp("10:07")).all()
    assert not result.loc[4, "portfolio_executed"]
    assert result.loc[1, "portfolio_executed"]


def test_admissions_at_lock_time_are_kept_and_only_strictly_later_are_blocked():
    rows = [order(1, exit="10:04", gross=-1.2), order(2, entry="10:02", exit="10:05", gross=-1.2),
            order(3, entry="10:06", exit="10:08"), order(4, entry="10:07", exit="10:09")]
    result = run(rows, limit=2)
    assert result.allocation_entry_ts.iloc[2] == result.loss_lock_ts.iloc[2]
    assert result.portfolio_executed.tolist() == [True, True, True, False]


def test_cost_stress_recalculates_which_protected_stops_are_net_losers():
    rows = [order(1, exit="10:03", gross=.1), order(2, entry="10:02", exit="10:04", gross=.1),
            order(3, entry="10:06", exit="10:07")]
    low, high = run(rows, cost=5., limit=2), run(rows, cost=15., limit=2)
    assert low.portfolio_executed.tolist() == [True, True, True]
    assert low.loss_lock_ts.isna().all()
    assert high.portfolio_executed.tolist() == [True, True, False]
    assert high.loss_lock_ts.eq(stamp("10:04")).all()
    assert high.portfolio_net_profit_rupees.iloc[:2].lt(0).all()
    assert low.portfolio_net_profit_rupees.iloc[:2].gt(0).all()


def test_unexecuted_disallowed_unfilled_and_capital_rejected_rows_do_not_count():
    rows = [order(1, exit="10:10", gross=-1.2),
            order(2, entry="10:02", exit="10:03", gross=-1.2),
            order(3, entry="10:03", exit="10:04", gross=-1.2, filled="False"),
            order(4, entry="10:04", exit="10:05", gross=-1.2),
            order(5, entry="10:12", exit="10:14")]
    allowed = pd.Series({5:"True", 4:"False", 3:"True", 2:"True", 1:"True"})
    result = run(rows, limit=2, allowed=allowed,
                 config={**CONFIG, "portfolio_capital_rs":100_000.})
    assert result.portfolio_executed.tolist() == [True, False, False, False, True]
    assert result.loss_lock_ts.isna().all()
    assert result.base_entry_allowed.tolist() == [True, True, True, False, True]
    assert result.g8_baseline_filled.tolist() == [True, True, False, True, True]


def test_future_outcomes_cannot_change_earlier_admissions():
    rows = [order(1, exit="10:10", gross=-1.2), order(2, entry="10:02", exit="10:11", gross=-1.2),
            order(3, entry="10:06", exit="10:20"), order(4, entry="10:13", exit="10:15")]
    changed = [dict(row) for row in rows]
    changed[1]["gross_return_pct"] = 1.2
    changed[2]["gross_return_pct"] = -1.2
    a, b = run(rows, limit=2), run(changed, limit=2)
    assert a.portfolio_executed.iloc[:3].tolist() == b.portfolio_executed.iloc[:3].tolist()
    assert a.loss_lock_ts.iloc[0] == stamp("10:11")
    assert b.loss_lock_ts.iloc[0] == stamp("10:20")


def test_each_day_resets_and_breakeven_after_cost_is_not_a_loss():
    rows = [order(1, exit="10:03", gross=-1.2), order(2, entry="10:02", exit="10:04", gross=-1.2),
            order(3, entry="10:06", exit="10:08"),
            order(4, entry="10:01", exit="10:03", gross=.05, day="2026-10-02"),
            order(5, entry="10:06", exit="10:08", day="2026-10-02")]
    result = run(rows, limit=2)
    assert result.portfolio_executed.tolist() == [True, True, False, True, True]
    assert result.loss_lock_ts.iloc[3:].isna().all()
    assert result.portfolio_net_profit_rupees.iloc[3] == pytest.approx(0.)


def test_full_inventory_rerun_preserves_capacity_rejections_before_lock():
    rows = [order(1, exit="10:05", gross=-1.2), order(2, entry="10:02", exit="10:03"),
            order(3, entry="10:07", exit="10:09", gross=-1.2), order(4, entry="10:11", exit="10:13")]
    result = run(rows, limit=2, config={**CONFIG,"portfolio_capital_rs":100_000.})
    assert result.portfolio_executed.tolist() == [True, False, True, False]
    assert result.loc[1, "portfolio_reject_reason"] == "INSUFFICIENT_PORTFOLIO_CAPITAL"
    assert result.loss_lock_ts.eq(stamp("10:09")).all()


def test_preserves_native_entry_reporting_input_and_original_return_sizing():
    source = pd.DataFrame([order(10), order(20, entry="10:03", exit="10:06")])
    source.index = [7, 2]
    before = source.copy(deep=True)
    permissions = pd.Series({20:False, 10:True})
    result = engine.allocate(source, permissions, CONFIG, "synthetic")
    pd.testing.assert_frame_equal(source, before)
    pd.testing.assert_series_equal(result.entry_ts, source.entry_ts)
    assert result.portfolio_executed.tolist() == [True, False]
    assert result.portfolio_cost_rupees.tolist() == [250.,0.]
    assert result.portfolio_gross_profit_rupees.tolist() == [3000.,0.]
    assert result.g8_baseline_filled.all()
    assert result.final_entry_allowed.tolist() == [True,False]
    assert result.final_permission.equals(result.final_entry_allowed)


@pytest.mark.parametrize("limit", [0,-1,True,2.,"2",np.nan])
def test_invalid_loss_limit_is_rejected(limit):
    with pytest.raises(ValueError, match="positive integer"):
        run([order(1)], limit=limit)


def test_ambiguous_boolean_permission_is_rejected():
    with pytest.raises(ValueError, match="boolean"):
        run([order(1)], allowed=pd.Series({1:"no"}))
    with pytest.raises(ValueError, match="boolean"):
        run([order(1)], clock="False")
