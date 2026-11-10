"""Frozen policy constraints, deterministic ties and training-only purity."""
from __future__ import annotations

import json
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from . import policy as p


def metric(variant, cost=5):
    return dict(variant=variant, cost_bps=cost, trades=100,
        net_pnl_rs=100000.-(cost-5)*5000., losses=30,
        win_rate_pct=70., loss_trade_pnl_abs_rs=30000., losing_days=8,
        loss_day_pnl_abs_rs=18000., daily_close_drawdown_rs=15000.+(cost-5)*500.)


def bundle():
    return pd.DataFrame([metric(name, cost) for name in (p.REFERENCE_VARIANT, *p.RULES) for cost in p.COSTS])


def improve(frame, variant="relaxed_and", amount=1000.):
    out = frame.copy(deep=True)
    out.loc[out.variant.eq(variant), "net_pnl_rs"] += amount
    return out


def choose(frame, names=("relaxed_and",)):
    return p.choose_policy(frame, {name: name in names for name in p.RULES})


def result_candidate(result, name="relaxed_and"):
    return next(x for x in result["candidates"] if x["variant"] == name)


class TestTrainingPolicy(unittest.TestCase):
    def test_qualifying_rule_selected_and_inputs_preserved(self):
        frame = improve(bundle())
        before = frame.copy(deep=True)
        out = choose(frame)
        self.assertEqual(out["selected_variant"], "relaxed_and")
        self.assertEqual(out["reference_variant"], "available_control")
        self.assertFalse(out["fallback_used"])
        self.assertTrue(all(result_candidate(out)["criteria"].values()))
        assert_frame_equal(frame, before)
        json.dumps(out, allow_nan=False)

    def test_every_frozen_metric_constraint_blocks_independently(self):
        cases = (
            (5, "trades", 79, "minimum_trade_sample_5bps"),
            (5, "net_pnl_rs", 99999., "net_pnl_noninferior_5bps"),
            (5, "losses", 31, "losing_trade_count_noninferior_5bps"),
            (5, "win_rate_pct", 69.99, "win_rate_noninferior_5bps"),
            (5, "loss_trade_pnl_abs_rs", 30001., "losing_trade_rupees_noninferior_5bps"),
            (5, "losing_days", 9, "losing_day_count_noninferior_5bps"),
            (5, "loss_day_pnl_abs_rs", 18001., "losing_day_rupees_noninferior_5bps"),
            (5, "daily_close_drawdown_rs", 15001., "drawdown_noninferior_5bps"),
            (15, "net_pnl_rs", 49999., "net_pnl_noninferior_15bps"),
            (15, "daily_close_drawdown_rs", 20001., "drawdown_noninferior_15bps"),
        )
        for cost, name, value, flag in cases:
            with self.subTest(cost=cost, metric=name):
                frame = improve(bundle())
                frame.loc[frame.variant.eq("relaxed_and") & frame.cost_bps.eq(cost), name] = value
                out = choose(frame)
                self.assertEqual(out["selected_variant"], "available_control")
                self.assertFalse(result_candidate(out)["criteria"][flag])

    def test_absolute_training_sample_floor_and_inclusive_retention(self):
        frame = improve(bundle())
        frame.loc[frame.variant.eq("relaxed_and"), "trades"] = 80
        self.assertEqual(choose(frame)["selected_variant"], "relaxed_and")
        frame["trades"] = 40
        frame["losses"] = 10
        frame.loc[frame.variant.eq("available_control"), "trades"] = 30
        self.assertEqual(choose(frame)["selected_variant"], "relaxed_and")
        frame.loc[frame.variant.eq("relaxed_and"), "trades"] = 39
        self.assertEqual(choose(frame)["selected_variant"], "available_control")

    def test_no_actual_selection_change_or_no_improvement_cannot_qualify(self):
        out = choose(improve(bundle()), names=())
        self.assertEqual(out["selected_variant"], "available_control")
        self.assertFalse(result_candidate(out)["criteria"]["actual_selection_change"])
        out = choose(bundle())
        self.assertEqual(out["selected_variant"], "available_control")
        self.assertFalse(result_candidate(out)["criteria"]["strict_improvement"])
        frame = bundle()
        frame.loc[frame.variant.eq("relaxed_and"), "trades"] = 101
        self.assertEqual(choose(frame)["selected_variant"], "available_control")

    def test_daily_loss_and_high_cost_improvements_are_quality_improvements(self):
        for cost, field, delta in ((5, "losing_days", -1), (5, "loss_day_pnl_abs_rs", -1.),
                                  (15, "net_pnl_rs", 1.), (15, "daily_close_drawdown_rs", -1.)):
            with self.subTest(cost=cost, field=field):
                frame = bundle()
                frame.loc[frame.variant.eq("relaxed_and") & frame.cost_bps.eq(cost), field] += delta
                out = choose(frame)
                self.assertEqual(out["selected_variant"], "relaxed_and")
                self.assertIn(f"{field}@{cost}bps", result_candidate(out)["strict_improvement_metrics"])

    def test_arithmetic_tolerance_does_not_invent_economic_improvement(self):
        frame = improve(bundle(), amount=p.TOLERANCE/2)
        self.assertEqual(choose(frame)["selected_variant"], "available_control")
        frame = improve(bundle())
        mask = frame.variant.eq("relaxed_and") & frame.cost_bps.eq(5)
        frame.loc[mask, "daily_close_drawdown_rs"] += p.TOLERANCE/2
        self.assertEqual(choose(frame)["selected_variant"], "relaxed_and")
        frame.loc[mask, "daily_close_drawdown_rs"] += p.TOLERANCE
        self.assertEqual(choose(frame)["selected_variant"], "available_control")

    def test_all_lexicographic_tie_breaks_and_fixed_rule_order(self):
        cases = (
            ("net_pnl_rs", 100.), ("losses", -1), ("win_rate_pct", 1.),
            ("loss_trade_pnl_abs_rs", -100.), ("daily_close_drawdown_rs", -100.),
            ("trades", 1),
        )
        for field, delta in cases:
            with self.subTest(field=field):
                frame = improve(improve(bundle(), "relaxed_and"), "either_nonnegative")
                mask = frame.variant.eq("either_nonnegative") & frame.cost_bps.eq(5)
                frame.loc[mask, field] += delta
                out = choose(frame, names=("relaxed_and", "either_nonnegative"))
                self.assertEqual(out["selected_variant"], "either_nonnegative")
        frame = bundle()
        frame.loc[frame.variant.ne("available_control"), "net_pnl_rs"] += 1000.
        out = choose(frame.sample(frac=1, random_state=5), names=p.RULES)
        self.assertEqual(out["ranked_qualified_variants"], list(p.RULES))

    def test_lower_priority_gain_cannot_override_higher_priority_score(self):
        frame = improve(improve(bundle(), "relaxed_and", 1001.), "either_nonnegative", 1000.)
        frame.loc[frame.variant.eq("either_nonnegative"), "losses"] = 0
        frame.loc[frame.variant.eq("either_nonnegative"), "win_rate_pct"] = 100.
        out = choose(frame, names=("relaxed_and", "either_nonnegative"))
        self.assertEqual(out["selected_variant"], "relaxed_and")

    def test_nine_bps_required_but_not_an_optimization_target(self):
        frame = improve(bundle())
        mask = frame.variant.eq("relaxed_and") & frame.cost_bps.eq(9)
        frame.loc[mask, "net_pnl_rs"] = -1000000.
        self.assertEqual(choose(frame)["selected_variant"], "relaxed_and")
        frame = frame.loc[~mask]
        out = choose(frame)
        self.assertEqual(out["selected_variant"], "available_control")
        self.assertFalse(result_candidate(out)["criteria"]["complete_cost_rows"])

    def test_empty_training_and_zero_reference_have_explicit_fallback(self):
        out = p.choose_policy([], {})
        self.assertEqual(out["selected_variant"], "available_control")
        self.assertEqual(out["selection_reason"], "NO_COMPLETED_TRAINING_METRICS")
        json.dumps(out, allow_nan=False)
        frame = bundle()
        for field in p.METRICS:
            frame.loc[frame.variant.eq("available_control"), field] = None if field == "win_rate_pct" else 0
        out = choose(frame)
        self.assertEqual(out["selection_reason"], "NO_EXECUTED_REFERENCE_TRAINING_TRADES")
        json.dumps(out, allow_nan=False)

    def test_zero_candidate_win_rate_is_unavailable_and_never_qualifies(self):
        frame = bundle()
        for field in p.METRICS:
            frame.loc[frame.variant.eq("relaxed_and"), field] = None if field == "win_rate_pct" else 0
        out = choose(frame)
        self.assertEqual(out["selected_variant"], "available_control")
        self.assertFalse(result_candidate(out)["criteria"]["minimum_trade_sample_5bps"])
        json.dumps(out, allow_nan=False)

    def test_missing_invalid_candidate_and_malformed_reference(self):
        frame = improve(bundle())
        frame.loc[frame.variant.eq("relaxed_and") & frame.cost_bps.eq(5), "net_pnl_rs"] = np.inf
        out = choose(frame)
        self.assertEqual(result_candidate(out)["reason"], "NONFINITE_METRIC")
        self.assertEqual(out["selected_variant"], "available_control")
        frame = bundle().loc[lambda f: ~f.variant.eq("relaxed_and")]
        self.assertEqual(choose(frame)["selected_variant"], "available_control")
        with self.assertRaisesRegex(ValueError, "reference"):
            choose(bundle().loc[lambda f: ~f.variant.eq("available_control")])
        with self.assertRaisesRegex(ValueError, "reference"):
            choose(bundle().drop(columns="loss_day_pnl_abs_rs"))

    def test_duplicate_period_rows_and_unregistered_costs_raise(self):
        frame = improve(bundle())
        with self.assertRaisesRegex(ValueError, "Duplicate training"):
            choose(pd.concat([frame, frame.iloc[[0]]], ignore_index=True))
        frame.loc[0, "cost_bps"] = 25
        with self.assertRaisesRegex(ValueError, "fixed 5/9/15"):
            choose(frame)

    def test_file_access_is_not_used_and_future_file_changes_are_irrelevant(self):
        frame = improve(bundle())
        expected = choose(frame)
        with patch("builtins.open", side_effect=AssertionError("No file or future data access is allowed")), \
             patch("pathlib.Path.read_bytes", side_effect=AssertionError("No file or future data access is allowed")), \
             patch("pathlib.Path.read_text", side_effect=AssertionError("No file or future data access is allowed")):
            self.assertEqual(choose(frame), expected)
        # An unknown reference/report arm has no effect on the predeclared family.
        future_like = pd.DataFrame([dict(metric("base_control", cost), net_pnl_rs=1e12) for cost in p.COSTS])
        self.assertEqual(choose(pd.concat([frame, future_like], ignore_index=True)), expected)

    def test_changed_flags_must_be_boolean_and_unknown_rules_cannot_win(self):
        frame = improve(bundle())
        with self.assertRaisesRegex(ValueError, "boolean"):
            p.choose_policy(frame, {"relaxed_and": "True"})
        with self.assertRaisesRegex(ValueError, "map"):
            p.choose_policy(frame, [])
        out = p.choose_policy(frame.to_dict("records"), {"relaxed_and": np.bool_(True), "future_winner": True})
        self.assertEqual(out["selected_variant"], "relaxed_and")


if __name__ == "__main__":
    unittest.main()
