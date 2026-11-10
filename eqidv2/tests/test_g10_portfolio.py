"""Independent causal accounting checks for the isolated G10 allocator."""
import numpy as np
import pandas as pd
import pytest

from research import g10_portfolio as g10
from research import g7_v4_practical_research as g7


CONFIG = dict(capital_per_entry_rs=100_000., leverage=5., portfolio_capital_rs=1_000_000.)


def stamp(clock, day="2026-10-01"):
    return pd.Timestamp(f"{day} {clock}", tz="Asia/Kolkata")


def order(sid, entry="10:01", exit="10:10", *, gross=.6, stop=1.2,
          price=100., symbol=None, side="LONG", day="2026-10-01", filled=True):
    return dict(sid=sid, day=day, setup_id="1000_"+side, tradingsymbol=symbol or f"S{sid}",
                side=side, filled=filled, g8_baseline_filled=filled,
                entry_price=price, entry_path_index=0, entry_ts=stamp(entry, day),
                confirmation_ts=stamp(entry, day)-pd.Timedelta(minutes=1),
                exit_ts=stamp(exit, day), exit_event="INTRABAR",
                exit_price=price*(1+(1 if side == "LONG" else -1)*gross/100),
                exit_reason="TARGET" if gross > 0 else "STOP", gross_return_pct=gross,
                net_return_pct=gross-.05, cost_pct=.05, initial_stop_pct=stop,
                picker="max_liquidity", traded_value=1_000_000.)


def plan(rows, allowed=None):
    raw = pd.DataFrame(rows)
    return dict(raw=raw, allowed=pd.Series(True, index=raw.sid) if allowed is None else allowed,
                integer=True, clock=True, limit=2)


def test_no_overrides_exactly_reproduce_g7_frame_and_preserve_inputs():
    p = plan([order(1, gross=-1.2, exit="10:03"), order(2, entry="10:02", gross=-1.2, exit="10:04"),
              order(3, entry="10:06")])
    before = p["raw"].copy(deep=True)
    pd.testing.assert_frame_equal(g10.execute(p, CONFIG, "baseline"), g7.execute(p, CONFIG, "baseline"))
    pd.testing.assert_frame_equal(p["raw"], before)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_risk_sizing_is_integer_cost_inclusive_and_preserves_fills(side):
    p = plan([order(1, side=side)])
    r = g10.execute(p, CONFIG, "risk", risk_per_trade_rs=6000.)
    assert r.integer_cash_quantity.tolist() == [4800.]
    assert r.portfolio_trade_exposure_rupees.iloc[0] == pytest.approx(480_000.)
    assert r.portfolio_trade_initial_risk_rupees.iloc[0] == pytest.approx(6000.)
    assert r.portfolio_cost_rupees.iloc[0] == pytest.approx(240.)
    assert r.portfolio_net_profit_rupees.iloc[0] == pytest.approx(2640.)
    for c in ("entry_price", "exit_price", "gross_return_pct"):
        pd.testing.assert_series_equal(r[c], p["raw"][c])
    for c in ("entry_ts", "exit_ts"):
        pd.testing.assert_series_equal(pd.to_datetime(r[c], utc=True),
                                       pd.to_datetime(p["raw"][c], utc=True))


def test_narrow_stop_cannot_leverage_above_baseline_exposure():
    r = g10.execute(plan([order(1, stop=.1)]), CONFIG, "risk", risk_per_trade_rs=6000.)
    assert r.integer_cash_quantity.iloc[0] == 5000
    assert r.portfolio_trade_exposure_rupees.iloc[0] == 500_000


def test_gap_loss_can_exceed_planned_risk_and_is_not_clipped():
    r = g10.execute(plan([order(1, gross=-2.)]), CONFIG, "risk", risk_per_trade_rs=6000.)
    assert r.portfolio_trade_initial_risk_rupees.iloc[0] == pytest.approx(6000.)
    assert r.portfolio_net_profit_rupees.iloc[0] == pytest.approx(-9840.)


def test_total_risk_cap_rejects_overlap_then_releases_at_exit():
    rows = [order(1, exit="10:05"), order(2, entry="10:02", exit="10:08"),
            order(3, entry="10:03", exit="10:09"), order(4, entry="10:07", exit="10:12")]
    r = g10.execute(plan(rows), CONFIG, "both", risk_per_trade_rs=6000., max_open_risk_rs=12000.)
    assert r.portfolio_executed.tolist() == [True, True, False, True]
    assert r.portfolio_reject_reason.iloc[2] == "MAX_OPEN_RISK_REACHED"
    assert r.portfolio_open_risk_before_rupees.iloc[3] == pytest.approx(6000.)


def test_cap_only_uses_baseline_integer_size_and_roundtrip_fees():
    p = plan([order(1), order(2, entry="10:02")])
    r = g10.execute(p, CONFIG, "cap", max_open_risk_rs=12000.)
    assert r.integer_cash_quantity.tolist() == [5000., 5000.]
    assert r.portfolio_executed.tolist() == [True, False]  # 2 * 6250 > 12000.


def test_position_cap_and_same_symbol_use_inherited_conservative_clock():
    p = plan([order(1, symbol="SAME", exit="10:05"),
              order(2, entry="10:05", symbol="SAME"), order(3, entry="10:06", symbol="OTHER")])
    r = g10.execute(p, CONFIG, "positions", max_positions=1)
    assert r.portfolio_executed.tolist() == [True, False, True]


def test_realized_losses_lock_strictly_after_exit_and_reset_next_day():
    rows = [order(1, gross=-1.2, exit="10:03"), order(2, entry="10:02", gross=-1.2, exit="10:04"),
            order(3, entry="10:05", exit="10:08"), order(4, entry="10:06"),
            order(5, day="2026-10-02")]
    r = g10.execute(plan(rows), CONFIG, "risk", risk_per_trade_rs=6000.)
    assert r.portfolio_executed.tolist() == [True, True, True, False, True]
    assert r.loss_lock_ts.iloc[:4].eq(stamp("10:04")).all()
    assert pd.isna(r.loss_lock_ts.iloc[4])


def test_cost_stress_recomputes_size_and_net_loss_lock():
    rows = [order(1, gross=.1, exit="10:03"), order(2, entry="10:02", gross=.1, exit="10:04"),
            order(3, entry="10:06")]
    a = g10.execute(plan(rows), CONFIG, "risk", cost=5., risk_per_trade_rs=6000.)
    b = g10.execute(plan(rows), CONFIG, "risk", cost=15., risk_per_trade_rs=6000.)
    assert a.portfolio_executed.tolist() == [True, True, True]
    assert b.portfolio_executed.tolist() == [True, True, False]
    assert b.integer_cash_quantity.iloc[0] == np.floor(6000/1.35)
    assert b.g10_planned_initial_risk_rs.max() <= 6000+1e-8


def test_rejected_eventual_losers_do_not_count_and_future_returns_do_not_size():
    rows = [order(1, gross=-1.2, exit="10:05"), order(2, entry="10:02", gross=-9., exit="10:03"),
            order(3, entry="10:07", exit="10:09")]
    a = g10.execute(plan(rows), CONFIG, "risk", risk_per_trade_rs=6000., max_positions=1)
    changed = [dict(x) for x in rows]
    changed[1]["gross_return_pct"] = 50.
    changed[2]["gross_return_pct"] = -50.
    b = g10.execute(plan(changed), CONFIG, "risk", risk_per_trade_rs=6000., max_positions=1)
    assert a.portfolio_executed.tolist() == b.portfolio_executed.tolist() == [True, False, True]
    assert a.loss_lock_ts.isna().all()
    pd.testing.assert_series_equal(a.integer_cash_quantity, b.integer_cash_quantity)


def test_shuffled_inventory_and_permissions_keep_deterministic_admission():
    rows = [order(1), order(2), order(3, entry="10:02")]
    a = g10.execute(plan(rows), CONFIG, "cap", max_positions=1).set_index("sid").sort_index()
    b = g10.execute(plan(rows[::-1], pd.Series({3:True, 2:True, 1:True})), CONFIG,
                    "cap", max_positions=1).set_index("sid").sort_index()
    assert a.portfolio_executed.tolist() == b.portfolio_executed.tolist() == [True, False, False]


def test_budget_below_one_share_does_not_create_zero_size_execution():
    r = g10.execute(plan([order(1, price=1000.)]), CONFIG, "tiny", risk_per_trade_rs=1.)
    assert not r.portfolio_executed.iloc[0]
    assert r.g10_native_filled.iloc[0]
    assert r.g10_sizing_blocked.iloc[0]
    assert r.portfolio_status.iloc[0] == "G10_SIZE_BELOW_ONE_SHARE"
    assert r.portfolio_cost_rupees.iloc[0] == 0


@pytest.mark.parametrize("kw", [{"risk_per_trade_rs":0}, {"risk_per_trade_rs":np.nan},
                              {"risk_per_trade_rs":True}, {"max_open_risk_rs":-1},
                              {"max_positions":True}, {"max_positions":1.5}])
def test_invalid_overrides_fail_closed(kw):
    with pytest.raises(ValueError):
        g10.execute(plan([order(1)]), CONFIG, "bad", **kw)


def test_missing_stop_is_not_silently_sized():
    p = plan([order(1, stop=np.nan)])
    with pytest.raises(ValueError, match="initial_stop_pct"):
        g10.execute(p, CONFIG, "bad", risk_per_trade_rs=6000.)
