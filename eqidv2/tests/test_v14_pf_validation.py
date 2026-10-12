from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from v14.pf_validation import (EXPOSURE, PNL, assess_pf_target, bootstrap_day_pf,
    day_metrics, expanding_folds, net_profit_factor, select_train_variant, walk_forward_select)


def days(count=25):
    return pd.bdate_range("2026-01-01", periods=count).strftime("%Y-%m-%d").tolist()


def ledger(pnl, labels=None, exposure=500000.):
    labels = days(len(pnl)) if labels is None else labels
    return pd.DataFrame({"day": labels, "portfolio_executed": True, PNL: pnl, EXPOSURE: exposure,
                         "sid": np.arange(len(pnl))})


def test_net_pf_uses_net_trade_gains_and_losses_and_is_undefined_without_losses():
    assert net_profit_factor([200, 100, -100, 0]) == 3.
    assert net_profit_factor([-100, -200]) == 0.
    assert net_profit_factor([200, 0]) is None
    assert net_profit_factor([]) is None
    with pytest.raises(ValueError, match="finite"):
        net_profit_factor([100, np.nan, -10])


def test_stress_is_additional_roundtrip_cost_on_same_executed_notional():
    frame = ledger([1000., -500.], [days(1)[0]] * 2)
    extra = ledger([1e9], [days(1)[0]])
    extra["portfolio_executed"] = False
    frame = pd.concat([frame, extra], ignore_index=True)
    assert day_metrics(frame, days(1))["net_pf"] == 2.
    plus5 = day_metrics(frame, days(1), 5.)
    assert plus5["net_pf"] == 1.
    assert plus5["net_pnl"] == 0.
    assert plus5["additional_cost"] == 500.
    plus10 = day_metrics(frame, days(1), 10.)
    assert plus10["net_pf"] == .5
    assert plus10["net_pnl"] == -500.


def test_daily_drawdown_includes_initial_zero_and_valid_no_trade_sessions():
    calendar = days(5)
    frame = ledger([-100., 300., -250.], [calendar[0], calendar[2], calendar[4]])
    result = day_metrics(frame, calendar)
    assert result["sessions"] == 5 and result["active_sessions"] == 3
    assert result["daily_close_drawdown"] == 250.
    assert result["net_without_best_trade"] == -350.
    assert result["best_trade_fraction_of_gains"] == 1.


def test_day_metrics_do_not_count_trades_outside_requested_dates():
    frame = ledger([100., -50., -1e9])
    result = day_metrics(frame, days(2))
    assert result["trades"] == 2 and result["net_pf"] == 2.
    with pytest.raises(ValueError, match="chronological"):
        day_metrics(frame, days(2)[::-1])
    with pytest.raises(ValueError, match="unique"):
        day_metrics(frame, [days(1)[0]] * 2)


def test_bootstrap_preserves_gains_losses_together_within_each_day():
    calendar = days(25)
    frame = ledger([200., -100.] * 25, [day for day in calendar for _ in range(2)])
    result = bootstrap_day_pf(frame, calendar, n_boot=300)
    assert result["lower"] == result["upper"] == 2.
    assert result["block_sessions"] == 5
    assert result["undefined_fraction"] == 0.
    assert result == bootstrap_day_pf(frame, calendar, n_boot=300)


def test_bootstrap_retains_zero_trade_days_and_does_not_drop_no_loss_draws():
    calendar = days(20)
    frame = ledger([100.], [calendar[0]])
    result = bootstrap_day_pf(frame, calendar, n_boot=500)
    assert result["undefined_fraction"] > 0  # Some blocks have only no-trade days.
    assert result["no_loss_fraction"] == 1.
    assert result["upper"] is None and result["upper_unbounded"]
    assert not result["interval_complete"]
    empty = ledger([], [])
    empty_result = bootstrap_day_pf(empty, calendar, n_boot=100)
    assert empty_result["undefined_fraction"] == 1.
    assert empty_result["lower"] is None and empty_result["upper"] is None


def train_metrics():
    return pd.DataFrame([
        dict(variant="first", net_pf=2., net_pnl=1000., trades=25, losing_trades=5, split="TRAIN", additional_bps=0.),
        dict(variant="second", net_pf=3., net_pnl=500., trades=25, losing_trades=5, split="TRAIN", additional_bps=0.),
        dict(variant="undefined", net_pf=np.nan, net_pnl=1e9, trades=100, losing_trades=0, split="TRAIN", additional_bps=0.)])


def test_training_selector_ignores_future_partitions_and_cost_stress():
    frame = train_metrics()
    assert select_train_variant(frame)["selected_variant"] == "second"
    ignored = frame.copy()
    ignored["split"] = "TEST"
    ignored["net_pf"], ignored["net_pnl"] = 1e9, 1e15
    stressed = ignored.assign(split="TRAIN", additional_bps=10.)
    result = select_train_variant(pd.concat([frame, ignored, stressed], ignore_index=True))
    assert result["selected_variant"] == "second" and not result["used_test"]


@pytest.mark.parametrize("field,bad", [("trades", 19), ("losing_trades", 2), ("net_pnl", 0),
                                       ("net_pf", 1.2999), ("net_pf", np.inf), ("net_pf", np.nan)])
def test_training_selector_abstains_if_the_fixed_sample_or_net_pf_screen_fails(field, bad):
    frame = train_metrics().iloc[:1].copy()
    frame.loc[frame.index[0], field] = bad
    result = select_train_variant(frame)
    assert result["selected_variant"] is None
    assert result["status"] == "NO_QUALIFIED_TRAIN_VARIANT"


def test_training_pf_boundary_and_declared_grid_tiebreak_are_inclusive_and_stable():
    frame = train_metrics().iloc[:2].copy()
    frame[["net_pf", "net_pnl", "trades", "losing_trades"]] = [1.3, 100., 20, 3]
    assert select_train_variant(frame)["selected_variant"] == "first"
    frame["grid_order"] = [10, 2]
    assert select_train_variant(frame)["selected_variant"] == "second"


def test_51_session_expanding_folds_have_a_final_single_day_without_overlap():
    calendar = days(51)
    folds = expanding_folds(calendar, initial_train_sessions=20, test_sessions=5)
    assert len(folds) == 7
    assert [len(fold["test_days"]) for fold in folds] == [5, 5, 5, 5, 5, 5, 1]
    observed = [day for fold in folds for day in fold["test_days"]]
    assert observed == calendar[20:]
    assert all(max(fold["train_days"]) < min(fold["test_days"]) for fold in folds)


def test_walk_forward_missing_dates_do_not_compress_or_shift_calendar_boundaries():
    calendar = days(31)
    profits = [-100. if i % 5 == 0 else 100. for i in range(31)]
    frame = ledger(profits, calendar)
    # Exclude one training and one test date. Training still qualifies with
    # minimum19 expressly lowered only for this small deterministic fixture.
    available = [day for i, day in enumerate(calendar) if i not in (1, 21)]
    oof, decisions = walk_forward_select({"one": frame}, calendar,
        available_days=available, initial_train_sessions=20, test_sessions=5, min_train_trades=19)
    assert decisions[0]["test_days"] == calendar[20:25]
    assert decisions[0]["evaluated_test_days"] == [calendar[i] for i in (20, 22, 23, 24)]
    assert decisions[1]["test_days"] == calendar[25:30]
    assert not oof.day.eq(calendar[21]).any()
    assert oof.day.tolist() == [day for day in calendar[20:] if day in available]


def test_current_fold_selection_cannot_read_that_folds_outcomes_and_unqualified_stays_cash():
    calendar = days(25)
    a = ledger([-100. if i % 5 == 0 else 100. for i in range(25)], calendar)
    b = a.copy()
    b.loc[b[PNL].gt(0), PNL] = 90.
    original, choices = walk_forward_select({"a": a, "b": b}, calendar, initial_train_sessions=20, test_sessions=5)
    assert choices[0]["selected_variant"] == "a"
    b.loc[b.day.isin(calendar[20:]), PNL] = 1e12
    changed, changed_choices = walk_forward_select({"a": a, "b": b}, calendar, initial_train_sessions=20, test_sessions=5)
    assert choices == changed_choices
    pd.testing.assert_frame_equal(original, changed)
    no_trade, abstain = walk_forward_select({"a": a.assign(**{PNL: -100.})}, calendar,
                                           initial_train_sessions=20, test_sessions=5)
    assert no_trade.empty and abstain[0]["selected_variant"] is None
    assert abstain[0]["evaluated_test_days"] == calendar[20:]


def test_pf2_claim_requires_trade_session_and_loss_counts_and_reports_stress_separately():
    calendar = days(25)
    frame = ledger([2000., -1000.] * 25, [day for day in calendar for _ in range(2)])
    report = assess_pf_target(frame, calendar, n_boot=200)
    assert report["sample_sufficient"] and report["base_net_pf_reaches_target"]
    assert report["retrospective_target_and_sample_pass"]
    assert report["cost_stress"][1]["net_pf"] == pytest.approx(1.4)
    assert not report["robustness"]["plus5bps_pf_at_least_1p5"]
    assert not report["retrospective_target_sample_and_robustness_pass"]
    assert not report["fresh_holdout"] and not report["default_profile_promoted"]
    thin = assess_pf_target(frame.iloc[:48], calendar, n_boot=100)
    assert not thin["sample_sufficient"]


def test_evaluated_zero_trade_sessions_count_but_active_sessions_are_reported_separately():
    calendar = days(20)
    labels = [calendar[i % 5] for i in range(50)]
    frame = ledger([4000., -1000.] * 25, labels)
    result = assess_pf_target(frame, calendar, n_boot=100)
    assert result["sample_sufficient"]
    assert result["cost_stress"][0]["sessions"] == 20
    assert result["cost_stress"][0]["active_sessions"] == 5
