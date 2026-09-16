import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_g_options_projection as projection
from fno_v13_v10_g_options_execution import option_order_costs


def trade(entry=100., exit=110., lot_size=100, trade_id="A", entry_minute=0, observed_exit_minute=10):
    stamp = pd.Timestamp("2026-09-01 09:30", tz="Asia/Kolkata")
    return dict(day="2026-09-01", trade_id=trade_id, setup_id=trade_id,
                entry_ts=stamp+pd.Timedelta(minutes=entry_minute),
                exit_ts=stamp+pd.Timedelta(minutes=entry_minute),
                exit_observed_ts=stamp+pd.Timedelta(minutes=observed_exit_minute),
                entry_price=entry, exit_price=exit, quantity=lot_size*3, lot_size=lot_size)


def template(entry=100., exit=110., lot_size=100):
    return projection.day_templates(pd.DataFrame([trade(entry, exit, lot_size)]), ["2026-09-01"])[0]


def test_three_lot_reference_matches_premium_cash_and_costs_without_double_slippage():
    row = trade(100.15, 124.95)
    flow = projection.scenario_cashflows(row, 1., 0.)
    expected_buy = 100.15*300 + option_order_costs(100.15, 300, "BUY")["total"]
    expected_sell = 124.95*300 - option_order_costs(124.95, 300, "SELL")["total"]
    assert flow["lots"] == 3 and flow["quantity"] == 300
    assert flow["outlay"] == expected_buy
    assert flow["receipt"] == expected_sell
    assert flow["extra_impact_cost"] == 0.


def test_resized_costs_recompute_flat_brokerage_and_turnover_for_each_side():
    row = trade(100., 120., 100)
    three = projection.scenario_cashflows(row, .5, 10., lots=3)
    four = projection.scenario_cashflows(row, .5, 10., lots=4)
    assert four["quantity"] == 400
    assert four["scenario_exit_price"] == 110.
    expected_fee = option_order_costs(100., 400, "BUY")["total"] + option_order_costs(110., 400, "SELL")["total"]
    assert four["fees"] == expected_fee
    assert four["fees"] != pytest.approx(three["fees"]*4/3)
    assert four["extra_impact_cost"] == 84.
    assert four["receipt"]-four["outlay"] == pytest.approx(4000.-expected_fee-84.)


def test_retention_never_reduces_gross_losses_and_entry_admission_ignores_future_exit():
    losing = trade(100., 87.5)
    full = projection.scenario_cashflows(losing, 1., 10.)
    haircut = projection.scenario_cashflows(losing, .2, 10.)
    assert full == haircut
    winner = projection.scenario_cashflows(trade(100., 125.), .2, 10.)
    assert winner["outlay"] == haircut["outlay"]
    assert winner["scenario_exit_price"] == 105.


def test_exit_observed_timestamp_prevents_reusing_unobserved_proceeds():
    rows = pd.DataFrame([trade(100., 110., trade_id="A", observed_exit_minute=10),
                         trade(100., 120., trade_id="B", entry_minute=5, observed_exit_minute=20)])
    templates = projection.day_templates(rows, ["2026-09-01"])
    result = projection.simulate(templates, np.array([[0]]), 31_000., 1., 0.)
    assert result["accepted"].sum() == 1 and result["rejected"].sum() == 1
    assert result["pnl"].sum() == pytest.approx(projection.scenario_cashflows(rows.iloc[0], 1., 0.)["receipt"]-projection.scenario_cashflows(rows.iloc[0], 1., 0.)["outlay"])


def test_observed_exit_at_new_entry_releases_cash_and_same_time_trade_reserves_first():
    rows = pd.DataFrame([trade(100., 110., trade_id="A", observed_exit_minute=10),
                         trade(100., 110., trade_id="B", entry_minute=10, observed_exit_minute=10)])
    templates = projection.day_templates(rows, ["2026-09-01"])
    assert [event[3] for event in templates[0]["events"]] == ["entry", "exit", "entry", "exit"]
    result = projection.simulate(templates, np.array([[0]]), 31_000., 1., 0.)
    assert result["accepted"].sum() == 2 and result["rejected"].sum() == 0


def test_realized_losses_deplete_cash_and_unaffordable_following_day_has_no_fees():
    result = projection.simulate([template(exit=50.)], np.array([[0, 0]]), 40_000., 1., 0.)
    assert result["accepted"].tolist() == [[1, 0]]
    assert result["rejected"].tolist() == [[0, 1]]
    assert result["pnl"][0, 1] == 0.
    assert result["equity"][0, 2] == result["equity"][0, 1]
    np.testing.assert_array_equal(result["lots"], [[3, 3]])


def test_zero_trade_calendar_days_are_sampled_and_preserve_equity():
    rows = pd.DataFrame([trade()])
    templates = projection.day_templates(rows, ["2026-09-01", "2026-09-02"])
    result = projection.simulate(templates, np.ones((2, 4), dtype=int), 123_456., .5, 10.)
    np.testing.assert_array_equal(result["equity"], np.full((2, 5), 123_456.))
    assert result["accepted"].sum() == result["rejected"].sum() == result["paused"].sum() == 0


def test_seed_and_circular_five_session_blocks_reproduce_shared_draws():
    a = projection.block_draws(13, paths=25, horizon=252)
    np.testing.assert_array_equal(a, projection.block_draws(13, paths=25, horizon=252))
    assert a.shape == (25, 252)
    for start in range(0, 250, 5):
        np.testing.assert_array_equal(a[:, start+1:start+5], (a[:, start:start+4]+1) % 13)


def test_monthly_fractional_budget_carries_until_four_whole_lots_and_reduces_immediately():
    budget = np.array([3.])
    counts = []
    for _ in range(4):
        budget = projection.monthly_lot_budget([2_000_000.], budget, 1_000_000.)
        counts.append(int(np.floor(budget[0]+1e-10)))
    assert counts == [3, 3, 3, 4]
    np.testing.assert_allclose(budget, [3.*1.1**4])
    np.testing.assert_allclose(projection.monthly_lot_budget([500_000., 0.], [5., 5.], 1_000_000.), [1.5, 0.])


@pytest.mark.parametrize("sizing", ["monthly", "stepup"])
def test_monthly_sizing_changes_only_on_boundaries_from_same_path_past_equity(sizing):
    templates = [template(exit=170.), template(exit=70.)]
    draws = np.array([[0]*42+[1]*21, [1]*42+[0]*21])
    original = projection.simulate(templates, draws, 1_000_000., 1., 0., sizing)
    changed = draws.copy()
    changed[:, 42:] = 1-changed[:, 42:]
    alternate = projection.simulate(templates, changed, 1_000_000., 1., 0., sizing)
    np.testing.assert_array_equal(original["equity"][:, :43], alternate["equity"][:, :43])
    np.testing.assert_array_equal(original["lots"], alternate["lots"])
    for start in (0, 21, 42):
        np.testing.assert_array_equal(original["lots"][:, start:start+21], np.repeat(original["lots"][:, start:start+1], 21, axis=1))
    assert original["lots"][0, 21] > original["lots"][1, 21]


def test_zero_monthly_lots_pause_without_orders_fees_or_cash_rejections():
    result = projection.simulate([template(exit=.01)], np.zeros((1, 42), dtype=int), 40_000., 1., 0., "monthly")
    assert (result["lots"][0, 21:] == 0).all()
    assert result["paused"][0, 21:].sum() == 21
    assert result["accepted"][0, 21:].sum() == result["rejected"][0, 21:].sum() == 0
    assert result["pnl"][0, 21:].sum() == 0.


def test_profit_stepup_thresholds_exclude_history_hold_losses_and_recompute_increment():
    reference = 1_614_943.083211532
    opening = reference + np.array([100_000., 500_000., 1_000_000., 900_000., 499_999.])
    prior = opening - np.array([50_000., 50_000., 50_000., -100_000., 10_000.])
    lots, increments = projection.profit_stepup_lots([3, 4, 5, 8, 8], opening, prior, reference)
    np.testing.assert_array_equal(increments, [1, 2, 3, 0, 1])
    np.testing.assert_array_equal(lots, [4, 6, 8, 8, 9])
    # A positive month below the original projection equity still increases one.
    lots, increments = projection.profit_stepup_lots([8], [reference-20_000.], [reference-30_000.], reference)
    np.testing.assert_array_equal(lots, [9])
    np.testing.assert_array_equal(increments, [1])


def test_stepup_accelerates_from_realized_profits_and_holds_after_losing_month():
    templates = [template(exit=200.), template(exit=50.)]
    draws = np.array([[0]*42+[1]*21+[0]*21])
    result = projection.simulate(templates, draws, 1_000_000., 1., 0., "stepup")
    assert result["lots"][0, 0] == 3
    assert result["lot_increment"][0, 0] == 0
    assert result["lots"][0, 21] == 5  # >Rs5 lakh future profit -> add two.
    assert result["lots"][0, 42] >= 8
    assert result["lots"][0, 63] == result["lots"][0, 42]
    assert result["lot_increment"][0, 63] == 0
    assert (np.diff(result["lots"][0]) >= 0).all()


def test_stepup_keeps_planned_size_when_unaffordable_and_does_not_shrink_order():
    templates = [template(entry=100., exit=200.), template(entry=10_000., exit=11_000.)]
    draws = np.array([[0]*21+[1]*21])
    result = projection.simulate(templates, draws, 1_000_000., 1., 0., "stepup")
    assert (result["lots"][0, 21:] == 5).all()
    assert result["accepted"][0, 21:].sum() == 0
    assert result["rejected"][0, 21:].sum() == 21
    assert result["pnl"][0, 21:].sum() == 0.


@pytest.mark.parametrize("sizing", ["fixed", "monthly", "stepup"])
def test_monthly_annual_summaries_reconcile_path_returns_and_lot_statistics(sizing):
    draws = np.array([[0]*42, [1]*42])
    model = projection.simulate([template(exit=170.), template(exit=70.)], draws, 1_000_000., .75, 10., sizing)
    annual, curves, monthly = projection.summarize(model, "test", 1_000_000., sizing)
    assert len(monthly) == 2 and len(curves) == 43
    assert all(row["sizing"] == sizing for row in monthly+curves+[annual])
    assert sum(row["mean_pnl"] for row in monthly) == pytest.approx(annual["mean_future_pnl"])
    assert monthly[-1]["mean_equity"] == pytest.approx(annual["mean_ending_equity"])
    assert monthly[-1]["mean_return_pct"] == pytest.approx(np.mean((model["equity"][:, 42]/model["equity"][:, 21]-1)*100))
    assert monthly[-1]["mean_lots"] == model["lots"][:, 21].mean()
    assert monthly[-1]["p10_lots"] <= monthly[-1]["p50_lots"] <= monthly[-1]["p90_lots"]


def test_source_calendar_and_fixed_cash_replay_match_pinned_twelve_point_five_profile():
    if not (projection.SOURCE / "manifest.json").exists():
        pytest.skip("Frozen historical research artifacts unavailable")
    _, trades, history, profile, summary, verified = projection.load_history()
    assert len(history) == 13 and history.trades.eq(0).sum() == 3
    assert len(trades) == 20 and profile["stop_pct"] == 12.5 and profile["target_pct"] == 25.
    assert len(verified) >= 7
    model = projection.simulate(projection.day_templates(trades, history.day), np.arange(13)[None, :], projection.INITIAL, 1., 0.)
    np.testing.assert_allclose(model["pnl"][0], history.net_pnl, rtol=1e-12, atol=1e-7)
    np.testing.assert_array_equal(model["accepted"][0], history.trades)
    np.testing.assert_allclose(model["peak_premium"].max(), summary["peak_reserved_premium_and_fees"], atol=1e-7)
    assert model["equity"][0, -1] == pytest.approx(1_614_943.083211532)
