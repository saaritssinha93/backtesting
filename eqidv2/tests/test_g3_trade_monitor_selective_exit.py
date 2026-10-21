"""Causal selection and economic accounting for the frozen exit experiment."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research import g3_trade_monitor_selective_exit as engine


DAY = "2026-01-02"


def _inputs():
    # D has no decision snapshot and must remain in the accounting cohort.
    baseline = pd.DataFrame([
        dict(sid=sid, day=DAY, tradingsymbol=sid, side=side, setup_id="fixture",
             entry_price=100.0, entry_ts=f"{DAY}T09:24:00+05:30",
             exit_ts=f"{DAY}T15:15:00+05:30", exit_reason=reason,
             initial_stop_pct=1.0, native_target_pct=2.0,
             exposure_per_entry_rupees=exposure, portfolio_cost_rupees=40.0,
             portfolio_net_profit_rupees=net)
        for sid, side, reason, exposure, net in [
            ("A", "LONG", "TARGET", 100_000.0, 1_000.0),
            ("B", "SHORT", "STOP", 200_000.0, -1_000.0),
            ("C", "LONG", "TIME_EXIT_1515", 100_000.0, 200.0),
            ("D", "SHORT", "TIGHTENED_STOP", 100_000.0, -400.0),
        ]
    ])
    x, y = [], []
    indexed = baseline.set_index("sid")
    # Deliberately out of order: taking the first row is an invalid replay.
    for sid, bars, score, exit_net in [
        ("B", 30, .95, -900.0), ("A", 15, .8, 300.0),
        ("B", 15, .9, -700.0), ("A", 6, .5, 500.0),
        ("C", 6, .7, 50.0), ("B", 6, .6, -200.0),
    ]:
        decision = pd.Timestamp(f"{DAY}T09:24:00+05:30") + pd.Timedelta(minutes=bars)
        keys = dict(sid=sid, day=DAY, decision_at_ist=decision.isoformat(), completed_bar_count=bars)
        trade = indexed.loc[sid]
        extra = trade.exposure_per_entry_rupees * .001
        x.append(dict(**keys, feature=score, score_initial_r=score))
        y.append(dict(**keys, label_native_net_pnl_rs=trade.portfolio_net_profit_rupees,
            label_native_exit_reason=trade.exit_reason,
            label_exit_now_net_pnl_rs=exit_net, label_extra_exit_cost_rs=extra,
            label_exit_value_after_cost_rs=exit_net-trade.portfolio_net_profit_rupees-extra,
            label_native_target=trade.exit_reason == "TARGET",
            label_native_stop=trade.exit_reason in {"STOP", "TIGHTENED_STOP"},
            label_native_winner=trade.portfolio_net_profit_rupees > 0,
            label_executable_at_ist=(decision+pd.Timedelta(minutes=1)).isoformat(),
            label_available_at_ist=f"{DAY}T15:16:00+05:30"))
    return pd.DataFrame(x), pd.DataFrame(y), baseline


def test_weighted_ridge_gives_each_trade_equal_total_weight():
    x = pd.DataFrame({"sid": [1, 1, 2], "feature": [-1.0, -1.0, 1.0], "constant": 7.0})
    model = engine.fit_ridge(x, [0.0, 0.0, 2.0], ["feature", "constant"], penalty=2.0)
    assert model["mean"] == pytest.approx([0.0, 7.0])
    assert model["scale"] == pytest.approx([1.0, 1.0])
    assert model["intercept"] == pytest.approx(1.0)
    assert model["coefficients"] == pytest.approx([.5, 0.0])
    assert engine.predict(model, x) == pytest.approx([.5, .5, 1.5])
    # A single identical checkpoint for trade 1 must yield the same fit.
    sparse = engine.fit_ridge(x.iloc[[0, 2]], [0.0, 2.0], ["feature", "constant"], penalty=2.0)
    assert sparse["coefficients"] == pytest.approx(model["coefficients"])


def test_future_columns_cannot_change_fit_or_prediction_when_features_and_training_target_are_fixed():
    x, _, _ = _inputs()
    target = np.arange(len(x), dtype=float)
    original = engine.fit_ridge(x, target, ["feature"])
    contaminated = x.assign(label_native_target=True, next_open_price=1e9,
                            label_exit_value_after_cost_rs=-1e12, exit_reason="ORACLE")
    changed = engine.fit_ridge(contaminated, target, ["feature"])
    assert original == changed
    assert engine.predict(original, contaminated) == pytest.approx(engine.predict(original, x))


@pytest.mark.parametrize("penalty", [0, -1, np.nan, np.inf])
def test_ridge_rejects_invalid_penalty(penalty):
    x, _, _ = _inputs()
    with pytest.raises(ValueError, match="Invalid training"):
        engine.fit_ridge(x, np.zeros(len(x)), ["feature"], penalty=penalty)


def test_nonfinite_model_features_rejected_at_fit_and_predict():
    x, _, _ = _inputs()
    model = engine.fit_ridge(x, np.zeros(len(x)), ["feature"])
    x.loc[0, "feature"] = np.nan
    with pytest.raises(ValueError):
        engine.fit_ridge(x, np.zeros(len(x)), ["feature"])
    with pytest.raises(ValueError, match="Nonfinite prediction"):
        engine.predict(model, x)


def test_target_threshold_ignores_profitable_time_exit_and_other_outcomes():
    labels = pd.DataFrame({"label_native_target": [True, False, True, False]})
    scores = [0.2, 100.0, -0.3, 200.0]
    assert engine.calibrate_threshold(scores, labels) == pytest.approx(.25)
    assert engine.calibrate_threshold([-2.0, 100, -3.0, 200], labels) == pytest.approx(.05)
    labels["label_native_target"] = False
    assert engine.calibrate_threshold(scores, labels) is None


def test_replay_uses_first_strictly_qualifying_checkpoint_and_retains_missing_trades():
    x, y, baseline = _inputs()
    ledger = engine.replay(baseline, x, y, threshold=.5).set_index("sid")
    assert set(ledger.index) == {"A", "B", "C", "D"}
    assert ledger.loc["A", "decision_completed_bars"] == 15  # six-bar score is equal, not greater
    assert ledger.loc["B", "decision_completed_bars"] == 6
    assert ledger.loc["B", "policy_net_pnl_rs"] == pytest.approx(-400)
    assert not ledger.loc["D", "intervened"]
    assert ledger.loc["D", "policy_net_pnl_rs"] == -400
    assert ledger.loc["D", "policy_exit_reason"] == "TIGHTENED_STOP"
    assert ledger.loc["D", "policy_exit_at_ist"] == baseline.set_index("sid").loc["D", "exit_ts"]


def test_replay_selection_is_independent_of_future_outcomes():
    x, y, baseline = _inputs()
    before = engine.replay(baseline, x, y, .5)
    y["label_exit_now_net_pnl_rs"] *= -100
    y["label_native_target"] = ~y.label_native_target
    after = engine.replay(baseline, x, y, .5)
    pd.testing.assert_frame_equal(before[["sid", "intervened", "decision_at_ist"]],
                                  after[["sid", "intervened", "decision_at_ist"]])
    assert not np.allclose(before.policy_net_pnl_rs, after.policy_net_pnl_rs)


def test_abstention_reproduces_entire_baseline():
    x, y, baseline = _inputs()
    ledger = engine.replay(baseline, x, y, None)
    assert len(ledger) == len(baseline)
    assert not ledger.intervened.any()
    assert ledger.incremental_net_rs.eq(0).all()
    assert ledger.extra_exit_cost_rs.eq(0).all()
    assert ledger.policy_net_pnl_rs.equals(ledger.portfolio_net_profit_rupees)


def test_economic_metrics_distinguish_stop_label_reduction_from_losing_trades_and_winner_harm():
    x, y, baseline = _inputs()
    ledger = engine.replay(baseline, x, y, .5)
    result = engine.metrics(ledger, [DAY, "2026-01-05"])
    assert result["trades"] == 4
    assert result["sessions"] == 2
    assert result["baseline_net_rs"] == -200
    assert result["policy_net_rs"] == -650
    assert result["incremental_net_rs"] == -450
    assert result["additional_exit_cost_rs"] == 400
    assert result["baseline_stop_trades"] == 2
    assert result["remaining_native_stops"] == 1
    assert result["stopped_trades_improved"] == 1
    assert result["stop_losses_saved_rs"] == 600
    assert result["baseline_losing_trades"] == 2
    assert result["policy_losing_trades"] == 3
    assert result["baseline_winners_harmed"] == 2
    assert result["winner_profit_sacrificed_rs"] == 1050
    assert result["winners_turned_nonpositive"] == 1
    assert result["target_trades_touched"] == 1
    assert result["target_profit_sacrificed_rs"] == 800
    assert result["realized_loss_reduction_rs"] == 550
    assert result["policy_daily_close_drawdown_rs"] == 650
    daily = engine.daily_results(ledger, [DAY, "2026-01-05"])
    assert daily.iloc[1].policy_net_rs == 0


def test_execution_cost_sensitivity_changes_only_selected_exit_economics():
    x, y, baseline = _inputs()
    free = engine.replay(baseline, x, y, .5, extra_bps=0).set_index("sid")
    stressed = engine.replay(baseline, x, y, .5, extra_bps=20).set_index("sid")
    assert stressed.intervened.equals(free.intervened)
    assert (stressed.policy_net_pnl_rs-free.policy_net_pnl_rs).to_dict() == {
        "A": -200.0, "B": -400.0, "C": -200.0, "D": 0.0}


def test_validation_aligns_shuffled_labels_to_predictor_identity():
    x, y, baseline = _inputs()
    validated = engine.validate_inputs(x, y.iloc[::-1], baseline)
    pd.testing.assert_frame_equal(validated[engine.KEYS], x[engine.KEYS])
    assert validated.label_exit_now_net_pnl_rs.tolist() == y.label_exit_now_net_pnl_rs.tolist()


@pytest.mark.parametrize("part", ["predictors", "labels", "baseline"])
def test_duplicate_identity_is_rejected(part):
    x, y, baseline = _inputs()
    if part == "predictors":
        x = pd.concat([x, x.iloc[:1]], ignore_index=True)
    elif part == "labels":
        y = pd.concat([y, y.iloc[:1]], ignore_index=True)
    else:
        baseline = pd.concat([baseline, baseline.iloc[:1]], ignore_index=True)
    with pytest.raises(ValueError, match="Duplicate input identity"):
        engine.validate_inputs(x, y, baseline)


@pytest.mark.parametrize("defect,match", [
    ("missing_label", "identities differ"), ("unknown_trade", "Unknown executed"),
    ("day", "Decision day mismatch"), ("native_pnl", "Native outcome"),
    ("cost", "ten additional"), ("economics", "Economic labels"),
    ("target_flag", "label flags"), ("execution_before_decision", "Noncausal"),
    ("label_before_execution", "Noncausal"),
])
def test_invalid_identity_economics_and_chronology_rejected(defect, match):
    x, y, baseline = _inputs()
    if defect == "missing_label":
        y = y.iloc[1:]
    elif defect == "unknown_trade":
        x.loc[0, "sid"] = y.loc[0, "sid"] = "UNKNOWN"
    elif defect == "day":
        x.loc[0, "day"] = y.loc[0, "day"] = "2026-01-01"
    elif defect == "native_pnl":
        y.loc[0, "label_native_net_pnl_rs"] += 1
    elif defect == "cost":
        y.loc[0, "label_extra_exit_cost_rs"] += 100
    elif defect == "economics":
        y.loc[0, "label_exit_value_after_cost_rs"] += 100
    elif defect == "target_flag":
        y.loc[0, "label_native_target"] = not bool(y.loc[0, "label_native_target"])
    elif defect == "execution_before_decision":
        y.loc[0, "label_executable_at_ist"] = f"{DAY}T09:20:00+05:30"
    else:
        y.loc[0, "label_available_at_ist"] = y.loc[0, "label_executable_at_ist"]
    with pytest.raises(ValueError, match=match):
        engine.validate_inputs(x, y, baseline)


def test_label_maturity_uses_ist_scoring_day_boundary_and_is_strict():
    _, y, _ = _inputs()
    engine.assert_mature(y, "2026-01-05")
    # UTC boundary is the previous calendar day, 18:30 UTC.
    y.loc[0, "label_available_at_ist"] = "2026-01-04T18:30:00Z"
    with pytest.raises(ValueError, match="not mature"):
        engine.assert_mature(y, "2026-01-05")
    y.loc[0, "label_available_at_ist"] = "2026-01-04T18:29:59Z"
    engine.assert_mature(y, "2026-01-05")


def test_replay_rejects_unknown_prediction_and_missing_chosen_label():
    x, y, baseline = _inputs()
    missing = y.loc[~(y.sid.eq("B") & y.completed_bar_count.eq(6))]
    with pytest.raises(ValueError, match="Missing chosen economic label"):
        engine.replay(baseline, x, missing, .5)
    x.loc[0, "sid"] = "UNKNOWN"
    with pytest.raises(ValueError, match="outside baseline cohort"):
        engine.replay(baseline, x, y, .5)


def test_gate_cannot_pass_positive_profit_that_interferes_with_targets():
    gates = dict(minimum_calibration_exits=3, minimum_evaluation_exits=5,
        minimum_evaluation_intervention_days=3,
        maximum_target_interventions_calibration_and_evaluation=0)
    qualified = dict(early_exits=5, intervention_days=3, target_trades_touched=0,
        incremental_net_rs=100.0, realized_loss_reduction_rs=50.0,
        policy_daily_close_drawdown_rs=100.0, baseline_daily_close_drawdown_rs=200.0)
    assert engine.gate_failures(qualified, qualified, gates) == []
    interferes = {**qualified, "target_trades_touched": 1}
    assert engine.gate_failures(qualified, interferes, gates) == ["evaluation_target_winner_interference"]
    abstains = {**qualified, "early_exits": 0, "intervention_days": 0, "incremental_net_rs": 0.0,
                "realized_loss_reduction_rs": 0.0}
    assert "evaluation_no_positive_incremental_net" in engine.gate_failures(qualified, abstains, gates)
