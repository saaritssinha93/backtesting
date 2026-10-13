"""Causal selection, isolation and relaxation boundaries for the G4 additions."""
import copy
import json
from datetime import date, timedelta

import pandas as pd
import pytest

import fno_v13_replay_source as source
import fno_v13_v10_g_3_contract as production
from research import g4_extended_slot_backtest as study
from research.g4_late_slot_source import expanded_source_clocks


def test_all_requested_clocks_and_exit_rules_are_isolated_even_on_failure():
    config = study.configuration()
    before = study.contract.strategy_payload()
    current = production.strategy_payload()
    with pytest.raises(RuntimeError):
        with study.setup_context(config):
            assert len(study.contract.ACTIVE_SETUPS) == 22
            assert {s.signal_end for s in study.contract.ACTIVE_SETUPS} == set(study.CLOCKS)
            assert "11:20" not in study.contract.SIGNAL_TO_CONFIRMATION
            for spec in study.contract.ACTIVE_SETUPS:
                assert spec.target_pct == 1.
                assert spec.stop_pct == 1.25
                assert spec.confirmation_end == study.confirmation(spec.signal_end)
            raise RuntimeError("rollback")
    assert study.contract.strategy_payload() == before
    assert production.strategy_payload() == current


def test_source_clock_expansion_restores_original_map():
    before = source.config.SIGNAL_TO_CONFIRMATION
    with expanded_source_clocks(study.CLOCKS):
        assert source.config.SIGNAL_TO_CONFIRMATION["10:55"] == "10:56"
        assert source.config.SIGNAL_TO_CONFIRMATION["11:00"] == "11:01"
        assert source.config.SIGNAL_TO_CONFIRMATION["11:30"] == "11:31"
    assert source.config.SIGNAL_TO_CONFIRMATION is before


def test_relaxation_changes_1m_first_and_remains_within_source_envelope():
    config = study.configuration()
    for side, trials in config["candidate_variants"].items():
        base = trials[0]["thresholds"]
        assert len({json.dumps(t["thresholds"], sort_keys=True) for t in trials}) == len(trials)
        for t in trials:
            level = t["thresholds"]
            assert level["price_change_pct"] >= .1
            assert level["oi_change_pct"] >= .05
            assert level["five_minute_volume_ratio_min"] >= .8
            if t["stage"] == "relax_1m":
                assert all(level[k] == base[k] for k in ("price_change_pct", "oi_change_pct", "five_minute_volume_ratio_min"))
            if t["stage"] != "strict":
                assert level["one_minute_body_ratio_min"] <= base["one_minute_body_ratio_min"]
                assert level["one_minute_wick_ratio_max"] >= base["one_minute_wick_ratio_max"]
                assert level["one_minute_volume_ratio_min"] <= base["one_minute_volume_ratio_min"]


def trial(index, stage, pnl, *, wins=80., fills=10, later_wins=0.):
    return dict(index=index, stage=stage, train_net_pnl_rs=pnl, train_fills=fills,
                train_win_rate_pct=wins, train_max_drawdown_rs=100.,
                holdout_win_rate_pct=later_wins, holdout_fills=5)


def test_training_choice_is_independent_of_later_outcomes_and_prefers_strict_stage():
    config = study.configuration()
    rows = [trial(0, "strict", 100.), trial(1, "strict", 200.),
            trial(2, "relax_1m", 1000., later_wins=100.)]
    picked, passed = study.choose(rows, config)
    assert passed and picked["index"] == 1
    altered = copy.deepcopy(rows)
    for row in altered:
        row["holdout_win_rate_pct"] = 100 - row["holdout_win_rate_pct"]
        row["all_net_pnl_rs"] = 1e9 if row["index"] == 0 else -1e9
    assert study.choose(altered, config)[0]["index"] == 1


def test_stage_advances_only_after_training_failure_and_rejects_tiny_samples():
    config = study.configuration()
    rows = [trial(0, "strict", 100., wins=60.), trial(1, "relax_1m", 1000., fills=4),
            trial(2, "relax_5m", 200.)]
    picked, passed = study.choose(rows, config)
    assert passed and picked["index"] == 2
    rows[2]["train_net_pnl_rs"] = -1.
    assert not study.choose(rows, config)[1]


def test_liquidity_pick_does_not_replace_unfilled_order_or_use_future_profit():
    config = study.configuration()
    level = config["candidate_variants"]["LONG"][0]["thresholds"]
    base = dict(day="2026-10-09", price_change_pct=.2, oi_change_pct=.1, volume_ratio=2.,
                body_ratio=.8, wick_ratio=.1, v9_1m_volume_ratio=2.)
    frame = pd.DataFrame([dict(base, sid=1, tradingsymbol="A", traded_value=200., filled=False, net_return_pct=None),
                          dict(base, sid=2, tradingsymbol="B", traded_value=100., filled=True, net_return_pct=1.)])
    selected = study.select(frame, "LONG", level)
    assert selected.sid.tolist() == [1]
    assert study.score(selected, ["2026-10-09"], config)["fills"] == 0


def test_strict_and_relaxed_contract_reject_delayed_confirmation():
    config = study.configuration()
    ts = pd.Timestamp("2026-10-09 10:35", tz="Asia/Kolkata")
    row = dict(tradingsymbol="A", side="LONG", signal_timestamp=ts,
        confirmation_timestamp=ts + pd.Timedelta(minutes=2), v9_1m_feature_ts=ts + pd.Timedelta(minutes=2),
        signal_close=100., price_change_pct=.2, oi_change_pct=.1, oi=100100., prev_oi=100000.,
        volume_ratio=2., ema9=102., ema20=101., ema50=100., body_ratio=.8, wick_ratio=.1,
        traded_value=1000., v9_1m_volume_ratio=2., confirmed=True)
    with study.setup_context(config):
        spec = study.contract.setup_for("10:35", "LONG")
        assert not study.contract.passes_selected_filters(row, spec)
        row["confirmation_timestamp"] = row["v9_1m_feature_ts"] = ts + pd.Timedelta(minutes=1)
        assert study.contract.passes_selected_filters(row, spec)


def test_sweep_has_every_requested_setup_even_if_empty():
    config = study.configuration()
    config["candidate_variants"] = {s: [v[0]] for s, v in config["candidate_variants"].items()}
    # A single-stage fixture exercises empty setups without a large trial grid.
    days = [date(2026, 1, 1) + timedelta(days=i) for i in range(49)]
    empty = pd.DataFrame(columns=["day", "side", "hhmm_int", "price_change_pct", "oi_change_pct",
        "volume_ratio", "body_ratio", "wick_ratio", "v9_1m_volume_ratio", "traded_value",
        "tradingsymbol", "filled", "net_return_pct"])
    for side in config["candidate_variants"]:
        config["candidate_variants"][side] = [dict(config["candidate_variants"][side][0], stage=s, index=i)
                                               for i, s in enumerate(study.STAGES)]
    _, decisions, _, _ = study.sweep(empty, days, config)
    assert len(decisions) == 22
    assert not decisions.training_pass.any()
    assert not decisions.chronological_check_pass.any()
