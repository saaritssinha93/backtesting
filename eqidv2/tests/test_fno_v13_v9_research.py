from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

import fno_v13_v9_research as research


GATE = {
    "minimum_train_executed": 15,
    "minimum_validation_executed": 10,
    "minimum_fraction_control_executed_each_split": 0.5,
    "minimum_changed_execution_keys_development": 5,
}


def _metrics():
    control = {
        "TRAIN": {"trades": 30, "net_profit_rupees": 100.0, "daily_close_drawdown_rupees": 80.0},
        "VALIDATION": {"trades": 20, "net_profit_rupees": 200.0, "daily_close_drawdown_rupees": 50.0},
    }
    candidate = {
        "TRAIN": {"trades": 15, "net_profit_rupees": 150.0, "daily_close_drawdown_rupees": 100.0},
        "VALIDATION": {"trades": 10, "net_profit_rupees": 300.0, "daily_close_drawdown_rupees": 50.0},
    }
    return candidate, control


def test_development_gate_accepts_exact_sample_retention_and_drawdown_boundaries():
    candidate, control = _metrics()
    result = research.assess_candidate(candidate, control, 5, GATE)
    assert result["accepted_development"]
    assert result["rejection_reasons"] == ""
    assert result["train_delta_rupees"] == 50
    assert result["validation_delta_rupees"] == 100


@pytest.mark.parametrize(("period", "field", "value", "reason"), [
    ("TRAIN", "trades", 14, "TRAIN_INSUFFICIENT_TRADES"),
    ("VALIDATION", "trades", 9, "VALIDATION_INSUFFICIENT_TRADES"),
    ("TRAIN", "net_profit_rupees", 0, "TRAIN_NONPOSITIVE_NET"),
    ("VALIDATION", "net_profit_rupees", -10, "VALIDATION_NONPOSITIVE_NET"),
    ("TRAIN", "net_profit_rupees", 100, "TRAIN_NO_PNL_IMPROVEMENT"),
    ("VALIDATION", "net_profit_rupees", 200, "VALIDATION_NO_PNL_IMPROVEMENT"),
    ("VALIDATION", "daily_close_drawdown_rupees", 50.01, "VALIDATION_DRAWDOWN_WORSE"),
])
def test_development_gate_rejects_each_required_failure(period, field, value, reason):
    candidate, control = _metrics()
    candidate[period][field] = value
    result = research.assess_candidate(candidate, control, 5, GATE)
    assert not result["accepted_development"]
    assert reason in result["rejection_reasons"]


def test_retention_and_changed_execution_gates_are_independent_of_trade_minimum():
    candidate, control = _metrics()
    control["TRAIN"]["trades"] = 40
    result = research.assess_candidate(candidate, control, 4, GATE)
    assert not result["accepted_development"]
    assert "TRAIN_INSUFFICIENT_RETENTION" in result["rejection_reasons"]
    assert "TRAIN_INSUFFICIENT_TRADES" not in result["rejection_reasons"]
    assert "INSUFFICIENT_CHANGED_EXECUTIONS" in result["rejection_reasons"]


@pytest.mark.parametrize(("field", "value"), [
    ("trades", np.nan), ("net_profit_rupees", np.nan),
    ("net_profit_rupees", np.inf), ("daily_close_drawdown_rupees", np.nan),
])
def test_invalid_metrics_cannot_pass_development_gate(field, value):
    candidate, control = _metrics()
    candidate["VALIDATION"][field] = value
    assert not research.assess_candidate(candidate, control, 5, GATE)["accepted_development"]


def test_previously_seen_future_metrics_never_change_development_assessment():
    candidate, control = _metrics()
    expected = research.assess_candidate(candidate, control, 5, GATE)
    candidate["PSEUDO_TEST"] = {"trades": 1000000, "net_profit_rupees": -1e30,
                                "daily_close_drawdown_rupees": 1e30}
    control["PSEUDO_TEST"] = {"trades": 0, "net_profit_rupees": 1e30,
                              "daily_close_drawdown_rupees": 0}
    assert research.assess_candidate(candidate, control, 5, GATE) == expected


def test_winner_uses_validation_then_train_then_name_and_ignores_later_results():
    configs = {name: replace(research.engine.V9Config(), name=name) for name in ("A", "B", "C", "D", "E")}
    assessments = pd.DataFrame([
        {"name": "E", "accepted_development": False, "validation_delta_rupees": 1e9,
         "train_delta_rupees": 1e9, "pseudo_test_delta_rupees": 1e12},
        {"name": "D", "accepted_development": True, "validation_delta_rupees": 5,
         "train_delta_rupees": 1e9, "pseudo_test_delta_rupees": 1e12},
        {"name": "C", "accepted_development": True, "validation_delta_rupees": 10,
         "train_delta_rupees": 5, "pseudo_test_delta_rupees": 1e12},
        {"name": "B", "accepted_development": True, "validation_delta_rupees": 10,
         "train_delta_rupees": 10, "pseudo_test_delta_rupees": 1e12},
        {"name": "A", "accepted_development": True, "validation_delta_rupees": 10,
         "train_delta_rupees": 10, "pseudo_test_delta_rupees": -1e30},
    ])
    assert research.select_frozen_config(assessments.sample(frac=1, random_state=19), configs).name == "A"
    assert research.select_frozen_config(assessments.loc[assessments.name.ne("A")], configs).name == "B"
    assert research.select_frozen_config(assessments.loc[~assessments.name.isin(["A", "B"])], configs).name == "C"


def test_no_accepted_hypothesis_falls_back_to_exact_control():
    frame = pd.DataFrame([{"name": "NEGATIVE", "accepted_development": False,
                           "validation_delta_rupees": -1, "train_delta_rupees": -1}])
    assert research.select_frozen_config(frame, {}) == research.engine.V9Config()


def test_daily_metrics_include_zero_days_and_initial_capital_peak():
    ledger = pd.DataFrame([
        {"day": "2026-08-03", "portfolio_executed": True, "net_profit_rupees": -100},
        {"day": "2026-08-05", "portfolio_executed": True, "net_profit_rupees": 60},
        {"day": "2026-08-05", "portfolio_executed": False, "net_profit_rupees": 1e9},
    ])
    days = ["2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06"]
    metric, daily = research.daily_metrics(ledger, days, "CONTROL", "TRAIN")
    assert metric["sessions"] == 4
    assert metric["trades"] == 2
    assert metric["net_profit_rupees"] == -40
    assert metric["daily_close_drawdown_rupees"] == 100
    assert daily.net_profit_rupees.tolist() == [-100, 0, 60, 0]
    assert daily.daily_close_drawdown_rupees.tolist() == [100, 100, 40, 40]


def test_all_zero_day_period_has_zero_drawdown_and_zero_profit():
    ledger = pd.DataFrame(columns=["day", "portfolio_executed", "net_profit_rupees"])
    metric, daily = research.daily_metrics(ledger, ["2026-08-03", "2026-08-04"], "EMPTY", "TRAIN")
    assert metric["trades"] == 0
    assert metric["net_profit_rupees"] == 0
    assert metric["daily_close_drawdown_rupees"] == 0
    assert daily.trades.tolist() == [0, 0]
    assert daily.cumulative_profit_rupees.tolist() == [0, 0]


def test_execution_keys_compare_only_actual_portfolio_executions():
    ledger = pd.DataFrame([
        {"day": pd.Timestamp("2026-08-03"), "tradingsymbol": "A", "side": "LONG",
         "setup_id": "0926_LONG", "portfolio_executed": True},
        {"day": pd.Timestamp("2026-08-03"), "tradingsymbol": "B", "side": "LONG",
         "setup_id": "0926_LONG", "portfolio_executed": False},
    ])
    assert research.execution_keys(ledger) == {("2026-08-03", "A", "LONG", "0926_LONG")}
