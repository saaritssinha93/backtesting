import numpy as np
import pandas as pd
import runpy

import research_prefilter_long_ml_entry_strategy as research


def test_apply_rule_is_inclusive_and_rejects_missing_values():
    frame = pd.DataFrame(
        {
            "atr_pct": [1.05, 1.049, np.nan],
            "range_pct": [1.25, 1.50, 2.0],
            "vwap_dist_atr": [0.05, 0.10, 0.20],
            "signal_minute": [855.0, 850.0, 800.0],
        }
    )
    assert research.apply_rule(frame, research.FROZEN_RULE).tolist() == [True, False, False]


def test_causal_entries_uses_earliest_match_per_ticker_day():
    frame = pd.DataFrame(
        {
            "trade_date": ["2026-01-01"] * 3,
            "ticker": ["AAA", "AAA", "BBB"],
            "entry_execution_time_ist": pd.to_datetime(
                ["2026-01-01 10:05", "2026-01-01 10:10", "2026-01-01 10:05"]
            ),
            "membership_slot_ist": pd.to_datetime(
                ["2026-01-01 10:00", "2026-01-01 10:00", "2026-01-01 10:00"]
            ),
            "selection_rank": [250, 200, 220],
            "hit_5pct": [False, True, True],
        }
    )
    selected = research.causal_entries(frame, np.ones(3, dtype=bool), daily_cap=15)
    assert len(selected) == 2
    assert selected.loc[selected["ticker"].eq("AAA"), "selection_rank"].item() == 250


def test_daily_cap_is_chronological_and_deterministic():
    rows = []
    for index in range(20):
        rows.append(
            {
                "trade_date": "2026-01-01",
                "ticker": f"T{index:02d}",
                "entry_execution_time_ist": pd.Timestamp("2026-01-01 10:05"),
                "membership_slot_ist": pd.Timestamp("2026-01-01 10:00"),
                "selection_rank": 300 - index,
                "hit_5pct": False,
            }
        )
    frame = pd.DataFrame(rows)
    selected = research.causal_entries(frame, np.ones(20, dtype=bool), daily_cap=15)
    assert len(selected) == 15
    assert selected["selection_rank"].tolist() == sorted(frame["selection_rank"].tolist())[:15]


def test_conservative_rounding_tightens_tree_thresholds():
    rule = research.conservative_round_rule(
        [
            {"feature": "atr_pct", "op": ">=", "value": 1.0472},
            {"feature": "range_pct", "op": ">=", "value": 1.2497},
            {"feature": "vwap_dist_atr", "op": ">=", "value": 0.0459},
            {"feature": "signal_minute", "op": "<=", "value": 857.5},
        ]
    )
    assert rule == [
        {"feature": "atr_pct", "op": ">=", "value": 1.05},
        {"feature": "range_pct", "op": ">=", "value": 1.25},
        {"feature": "vwap_dist_atr", "op": ">=", "value": 0.05},
        {"feature": "signal_minute", "op": "<=", "value": 855.0},
    ]


def test_model_feature_allowlist_has_no_future_fields():
    assert not set(research.MODEL_FEATURES) & research.FUTURE_EXACT
    for feature in research.MODEL_FEATURES:
        assert not any(feature.startswith(prefix) for prefix in research.FUTURE_PREFIXES)


def test_frozen_rule_meets_indicator_budget():
    indicators = {
        item["feature"]
        for item in research.FROZEN_RULE
        if item["feature"] != "signal_minute"
    }
    assert len(indicators) <= 4
    assert len(research.FROZEN_RULE) == 4


def test_exported_config_rejects_nonfinite_values(tmp_path):
    path = tmp_path / "candidate_conf.py"
    research.write_config(path, research.FROZEN_RULE, {})
    namespace = runpy.run_path(str(path))
    valid = {
        "atr_pct": 1.05,
        "range_pct": 1.25,
        "vwap_dist_atr": 0.05,
        "signal_minute": 855.0,
    }
    assert namespace["matches"](valid)
    for feature in valid:
        values = dict(valid)
        values[feature] = float("nan")
        assert not namespace["matches"](values)
    values = dict(valid)
    values["atr_pct"] = float("inf")
    assert not namespace["matches"](values)
