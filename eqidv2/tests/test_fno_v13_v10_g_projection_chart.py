import numpy as np
import pandas as pd

import fno_v13_v10_g_projection_chart as chart


def test_unaffordable_trade_has_no_pnl_or_fees():
    templates = [dict(count=2, events=[
        (10, 1, 0, "entry", 0, 10000.), (11, 1, 2, "entry", 1, 90000.),
        (20, 0, 1, "exit", 0, 10000.), (21, 0, 3, "exit", 1, 90000.),
    ])]
    result = chart.simulate(templates, np.array([[0]]), 300750., 1., 10.)
    assert result["accepted"][0, 0] == 1
    assert result["rejected"][0, 0] == 1
    assert result["pnl"][0, 0] == 8500.
    assert result["equity"][0, -1] == 309250.


def test_zero_duration_trade_reserves_then_settles_once():
    rows = pd.DataFrame([
        dict(day="2026-08-11", entry_ts=pd.Timestamp("2026-08-11 09:27"),
             exit_ts=pd.Timestamp("2026-08-11 09:27"), confirmation_ts=pd.Timestamp("2026-08-11 09:26"),
             setup_id="A", portfolio_priority_value=1, tradingsymbol="OIL", sid=1, portfolio_gross_profit_rupees=-4000.),
        dict(day="2026-08-11", entry_ts=pd.Timestamp("2026-08-11 09:27"),
             exit_ts=pd.Timestamp("2026-08-11 10:00"), confirmation_ts=pd.Timestamp("2026-08-11 09:26"),
             setup_id="B", portfolio_priority_value=1, tradingsymbol="NEXT", sid=2, portfolio_gross_profit_rupees=5000.),
    ])
    templates = chart.day_templates(rows, ["2026-08-11"])
    assert [event[3] for event in templates[0]["events"]] == ["entry", "exit", "entry", "exit"]
    result = chart.simulate(templates, np.array([[0]]), 600000., 1., 5.)
    assert result["accepted"].sum() == 2
    assert result["pnl"].sum() == -500.


def test_five_position_limit_even_with_excess_cash():
    entries = [(10+i, 1, 2*i, "entry", i, 1000.) for i in range(6)]
    exits = [(20+i, 0, 2*i+1, "exit", i, 1000.) for i in range(6)]
    result = chart.simulate([dict(count=6, events=entries+exits)], np.array([[0]]), 10_000_000., 1., 10.)
    assert result["accepted"].sum() == 5
    assert result["rejected"].sum() == 1
    assert result["pnl"].sum() == 5*(1000-1500)


def test_haircut_applies_to_gross_winners_and_costs_once():
    events=[(10,1,0,"entry",0,10000.),(11,1,2,"entry",1,-4000.),
            (20,0,1,"exit",0,10000.),(21,0,3,"exit",1,-4000.)]
    result=chart.simulate([dict(count=2, events=events)], np.array([[0]]), 1_000_000., .5, 10.)
    assert result["pnl"].sum() == 5000-4000-2*1500


def test_zero_trade_day_preserves_equity_and_no_return():
    result=chart.simulate([dict(count=0, events=[])], np.zeros((2,3), dtype=int), 1234567., .5, 10.)
    np.testing.assert_array_equal(result["equity"], np.full((2,4), 1234567.))
    assert result["accepted"].sum() == result["rejected"].sum() == result["pnl"].sum() == 0


def test_circular_blocks_and_seed_are_reproducible():
    draws=chart.block_draws(31, paths=100, horizon=252)
    np.testing.assert_array_equal(draws, chart.block_draws(31, paths=100, horizon=252))
    assert draws.shape == (100,252) and draws.min() >= 0 and draws.max() < 31
    for i in range(0,250,5):
        np.testing.assert_array_equal(draws[:,i+1:i+5], (draws[:,i:i+4]+1)%31)


def one_trade(gross):
    return dict(count=1, events=[(10, 1, 0, "entry", 0, gross), (20, 0, 1, "exit", 0, gross)])


def test_monthly_budget_rounding_growth_cap_and_uncapped_decreases():
    actual = chart.monthly_margin(np.array([2036625.55695, 4_000_000., 1_000_000., 0.]),
                                  np.array([300000., 320000., 320000., 300000.]))
    np.testing.assert_array_equal(actual, [320000., 350000., 160000., 0.])


def test_sizing_uses_each_paths_opening_equity_only_at_month_boundary():
    draws = np.array([[0]*21+[1]*21, [1]*21+[0]*21])
    result = chart.simulate([one_trade(20000.), one_trade(-20000.)], draws,
                            2036625.55695, 1., 0., sizing="monthly")
    np.testing.assert_array_equal(result["margin"][:, :21], np.full((2, 21), 320000.))
    np.testing.assert_array_equal(result["margin"][:, 21:], np.array([[350000.]*21, [250000.]*21]))
    # Changing later observations cannot change earlier balances or allocations.
    changed = draws.copy()
    changed[:, 21:] = 0
    repeat = chart.simulate([one_trade(20000.), one_trade(-20000.)], changed,
                            2036625.55695, 1., 0., sizing="monthly")
    np.testing.assert_array_equal(result["equity"][:, :22], repeat["equity"][:, :22])
    np.testing.assert_array_equal(result["margin"], repeat["margin"])


def test_resizing_scales_winners_losers_and_fees_consistently():
    templates = [one_trade(20000.), one_trade(-20000.)]
    draws = np.array([[0, 1]])
    fixed = chart.simulate(templates, draws, 2036625.55695, .5, 10.)
    monthly = chart.simulate(templates, draws, 2036625.55695, .5, 10., sizing="monthly")
    np.testing.assert_allclose(monthly["pnl"], fixed["pnl"] * 320000 / 300000)
    np.testing.assert_allclose(monthly["pnl"], [[8500 * 320/300, -21500 * 320/300]])


def test_zero_monthly_size_rejects_without_phantom_trades_or_costs():
    result = chart.simulate([one_trade(20000.)], np.zeros((2, 22), dtype=int), 100., 1., 10., sizing="monthly")
    assert result["margin"].sum() == result["accepted"].sum() == result["pnl"].sum() == 0
    assert result["rejected"].sum() == 44
    np.testing.assert_array_equal(result["equity"], np.full((2, 23), 100.))


def test_monthly_rows_reconcile_and_average_path_returns():
    draws = np.array([[0]*42, [1]*42])
    model = chart.simulate([one_trade(20000.), one_trade(-20000.)], draws,
                           2036625.55695, .5, 10., sizing="monthly")
    rows = chart.monthly_rows(model, "test", "monthly")
    assert len(rows) == 2 and rows[1]["first_additional_session"] == 22
    np.testing.assert_allclose(sum(r["mean_monthly_net_profit"] for r in rows),
                               model["equity"][:, -1].mean()-2036625.55695)
    expected_returns = (model["equity"][:, 42]/model["equity"][:, 21]-1)*100
    np.testing.assert_allclose(rows[1]["mean_monthly_account_return_pct"], expected_returns.mean())
