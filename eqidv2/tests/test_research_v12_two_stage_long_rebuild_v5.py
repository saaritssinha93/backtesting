from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import nse_intraday_costs as nse
import research_v12_two_stage_long_rebuild_v5 as subject


def test_configuration_space_is_exactly_eight_predeclared_paths() -> None:
    configs = subject.configurations()
    assert len(configs) == 8
    assert len({config.config_id for config in configs}) == 8
    assert {config.feature_family for config in configs} == {
        "LEVEL12", "LEVEL12_SEQ8"
    }
    assert {(config.sl_pct, config.tgt_pct) for config in configs} == {
        (1.0, 2.0), (1.5, 3.0)
    }
    assert {config.rolling_fraction for config in configs} == {0.25, 0.40}


def _active_rows() -> pd.DataFrame:
    times = pd.date_range(
        "2026-02-05 09:30", periods=4, freq="5min", tz="Asia/Kolkata"
    )
    return pd.DataFrame({
        "ticker": ["ABC"] * 4,
        "trade_date": ["2026-02-05"] * 4,
        "signal_time_ist": times,
        "signal_open": [100.0, 101.0, 100.5, 102.0],
        "signal_close": [101.0, 100.5, 102.0, 101.5],
        "distance_from_session_high_pct": [-0.5, -0.6, -0.2, -0.3],
        "upper_wick_pct": [0.1, 0.2, 0.1, 0.3],
        "traded_value_rs": [1_000.0, 2_000.0, 3_000.0, 4_000.0],
    })


def test_sequence_features_are_causal_to_each_row() -> None:
    original = subject.add_sequence_features(_active_rows())
    changed = _active_rows()
    changed.loc[3, [
        "signal_open", "signal_close", "distance_from_session_high_pct",
        "upper_wick_pct", "traded_value_rs",
    ]] = [1.0, 999.0, 20.0, 30.0, 9_999_999.0]
    recomputed = subject.add_sequence_features(changed)
    pd.testing.assert_frame_equal(
        original.loc[:2, list(subject.SEQ8)],
        recomputed.loc[:2, list(subject.SEQ8)],
    )
    assert original["bars_in_active_spell"].tolist() == [1.0, 2.0, 3.0, 4.0]
    assert original["positive_body_count_last3"].tolist() == [1.0, 1.0, 2.0, 1.0]


def test_stage_b_training_label_is_current_state_only() -> None:
    assert subject.stage_b_training_label(0.000001) == "ENTER"
    assert subject.stage_b_training_label(0.0) == "DEFER"
    assert subject.stage_b_training_label(-0.20) == "DEFER"


def test_stage_b_policy_uses_frozen_probability_threshold_not_argmax() -> None:
    assert subject.stage_b_policy_action(0.399999) == "DEFER"
    assert subject.stage_b_policy_action(0.40) == "ENTER"
    assert subject.stage_b_policy_action(0.45) == "ENTER"


def test_exact_outcome_fill_and_quantity_become_canonical_after_merge() -> None:
    feature_values = {feature: 0.0 for feature in subject.LEVEL12 + subject.SEQ8}
    feature_values["traded_value_rs"] = 1_000.0 if "traded_value_rs" in feature_values else 0.0
    ticket_time = pd.Timestamp("2026-02-05 10:00", tz="Asia/Kolkata")
    tickets = pd.DataFrame([{
        "ticket_id": 1,
        "ticket_time_ist": ticket_time,
        "ticker": "ABC",
        "trade_date": "2026-02-05",
        **feature_values,
    }])
    states = pd.DataFrame([{
        "ticket_id": 1,
        "ticket_step": 0,
        "remaining_wait_steps": 2,
        "ticker": "ABC",
        "trade_date": "2026-02-05",
        "signal_time_ist": ticket_time,
        "state_executable": True,
        "_optimizer_row_id": 7,
        "entry_price": 99.0,
        **feature_values,
    }])
    raw = pd.DataFrame([{
        "_optimizer_row_id": 7,
        "v7_signal_entry_time_ist": ticket_time + pd.Timedelta(minutes=1),
        "v7_signal_entry_price": 100.0,
        "selection_rank": 250,
        "quantity": 500,
    }])
    exact_entry = 100.05
    exact_exit = 102.05
    quantity = 500
    costs = nse.intraday_equity_costs(exact_entry, exact_exit, quantity, "LONG")
    outcomes = pd.DataFrame([{
        "_optimizer_row_id": 7,
        "ticker": "ABC",
        "side": "LONG",
        "setup": subject.SETUP,
        "trade_date": "2026-02-05",
        "signal_time_ist": str(ticket_time),
        "entry_time_ist": ticket_time + pd.Timedelta(minutes=1),
        "entry_price": exact_entry,
        "quantity": quantity,
        "sl_pct": 1.0,
        "tgt_pct": 2.0,
        "outcome": "TARGET",
        "exit_time_ist": ticket_time + pd.Timedelta(minutes=10),
        "exit_price": exact_exit,
        "bars_held": 9,
        "gross_pnl_rs": (exact_exit - exact_entry) * quantity,
        "cost_rs": costs.total_cost,
        "net_pnl_rs": costs.net_pnl,
        "nifty_short_size_mult": 1.0,
        "cost_rates_as_of": "test",
    }])
    _, labelled_states = subject.make_exit_dataset(
        tickets, states, raw, outcomes, sl_pct=1.0, tgt_pct=2.0
    )
    row = labelled_states.iloc[0]
    assert row["research_source_entry_price"] == 99.0
    assert row["entry_price"] == exact_entry
    assert row["quantity"] == quantity
    assert np.isclose(row["gross_risk_rs"], exact_entry * 0.01 * quantity)


def test_strict_risk_uses_exact_unrounded_resolver_stop_and_is_maximal() -> None:
    quantity, stop, loss = subject.strict_risk_quantity(20.49, 1.0)
    assert stop == 20.49 * 0.99
    assert loss <= 500.0 + 1e-9
    next_loss = -nse.intraday_equity_costs(
        20.49, stop, quantity + 1, "LONG"
    ).net_pnl
    assert next_loss > 500.0
    # This is the known case where tick-rounding would incorrectly allow 2,255.
    assert quantity < 2255


def test_strict_ledger_recomputes_risk_and_net_r() -> None:
    trades = pd.DataFrame([{
        "ticker": "ABC",
        "trade_date": "2026-02-05",
        "entry_price": 100.0,
        "exit_price": 102.0,
        "sl_pct": 1.0,
        "quantity": 500,
        "gross_pnl_rs": 1_000.0,
        "cost_rs": 100.0,
        "net_pnl_rs": 900.0,
        "gross_risk_rs": 500.0,
        "net_r": 1.8,
    }])
    strict = subject.strict_risk_ledger(trades).iloc[0]
    assert not bool(strict["strict_sizing_rejected"])
    assert strict["strict_all_in_stop_loss_rs"] <= 500.0 + 1e-9
    assert strict["strict_next_quantity_stop_loss_rs"] > 500.0
    assert np.isclose(
        strict["gross_risk_rs"],
        strict["entry_price"] * strict["sl_pct"] / 100.0 * strict["quantity"],
    )
    assert np.isclose(strict["net_r"], strict["net_pnl_rs"] / strict["gross_risk_rs"])


def test_empty_strict_ledger_preserves_trade_schema() -> None:
    empty = pd.DataFrame(columns=["trade_date", "ticker", "entry_price"])
    result = subject.strict_risk_ledger(empty)
    assert result.empty
    assert "trade_date" in result
    assert "strict_sizing_rejected" in result


def test_v12_runtime_risk_contract_is_fingerprinted() -> None:
    actual = subject.v12_risk_contract()
    assert actual["RISK_SIZING_ENABLED"] is True
    assert actual["derived_risk_budget_rs"] == 500.0


def test_rolling_reference_includes_completed_prior_day_never_current_day() -> None:
    calendar = pd.bdate_range("2026-02-02", periods=23).strftime("%Y-%m-%d").tolist()
    next_day = calendar[21]
    history = pd.DataFrame({
        "trade_date": calendar[:21],
        "ticker": [f"T{i}" for i in range(21)],
        "stage_a_score": np.linspace(0.1, 0.9, 21),
    })
    reference, prior = subject.rolling_reference_rows(history, next_day, calendar)
    assert prior == calendar[1:21]
    assert calendar[20] in set(reference["trade_date"])
    assert next_day not in set(reference["trade_date"])

    completed_current = pd.concat([
        history,
        pd.DataFrame({
            "trade_date": [next_day], "ticker": ["NEW"], "stage_a_score": [0.5]
        }),
    ], ignore_index=True)
    following_reference, _ = subject.rolling_reference_rows(
        completed_current, calendar[22], calendar
    )
    assert next_day in set(following_reference["trade_date"])
    with pytest.raises(RuntimeError, match="noncausal"):
        subject.rolling_reference_rows(completed_current, next_day, calendar)
