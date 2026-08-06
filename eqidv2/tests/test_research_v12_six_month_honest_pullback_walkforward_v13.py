from __future__ import annotations

from dataclasses import replace

import numpy as np
import pandas as pd

import research_v12_six_month_honest_pullback_walkforward_v13 as research


def _row(
    day: str,
    ticker: str,
    row_id: int,
    *,
    minute: int = 600,
    rank: int = 225,
    family: str = "MOMENTUM",
    gross: float = 100.0,
) -> dict:
    hour, minute_of_hour = divmod(minute, 60)
    signal = pd.Timestamp(
        f"{day} {hour:02d}:{minute_of_hour:02d}", tz="Asia/Kolkata"
    )
    return {
        "_optimizer_row_id": row_id,
        "candidate_key": f"CANDIDATE_{row_id:06d}",
        "ticker": ticker,
        "trade_date": day,
        "primary_family": family,
        "selection_rank": rank,
        "signal_minute": minute,
        "signal_time_ist": signal,
        "atr_pct": 1.10,
        "session_return_so_far_pct": 1.50,
        "vwap_dist_atr": 1.20,
        "close_position_in_bar": 0.80,
        "range_pct": 1.30,
        "ret_5m_pct": 0.20,
        "previous_ret_5m_pct": -0.20,
        "contiguous_previous": True,
        "ema20_dist_atr": 1.20,
        "score_margin": 0.10,
        "ADX": 35.0,
        "RSI": 60.0,
        "volume_ratio20": 1.0,
        "return_acceleration_5_vs_15": 0.10,
        "niftybees_context_available": True,
        "niftybees_vwap_available": True,
        "niftybees_context_time_ist": signal,
        "niftybees_above_session_vwap": True,
        "true_nifty_context_available": True,
        "true_nifty_context_time_ist": signal,
        "true_nifty_daily_change_pct": 0.25,
        "gross_pnl_rs": gross,
        "cost_rs": 0.0,
        "net_pnl_rs": gross,
        "entry_time_ist": signal + pd.Timedelta(minutes=1),
        "exit_time_ist": signal + pd.Timedelta(minutes=30),
    }


def _sessions(count: int = 120) -> list[str]:
    return pd.bdate_range("2026-01-01", periods=count).strftime("%Y-%m-%d").tolist()


def test_registry_is_fixed_bounded_and_hashes_daily_cap():
    registry = research.candidate_registry()
    assert len(registry) == 64
    assert {config.daily_cap for config in registry} == {5, 8, 10, 15}
    assert all(config.daily_cap <= 15 for config in registry)
    diagnostic = [config for config in registry if config.diagnostic_only]
    assert len(diagnostic) == 8
    assert all("DIAG_L009216" in config.config_id for config in diagnostic)
    frozen = next(
        config for config in registry
        if config.config_id == "PB_FROZEN_L009216_CAP15"
    )
    assert frozen.session_return_min == 1.0
    assert frozen.vwap_dist_atr_min == 0.90
    assert frozen.ret_5m_min == 0.15
    assert frozen.ret_5m_max == 0.35
    assert frozen.ema20_dist_atr_min == 1.0
    assert research.config_hash(frozen) != research.config_hash(
        replace(frozen, daily_cap=10)
    )


def test_protocol_uses_only_prior_sessions_and_covers_four_outer_blocks():
    sessions = _sessions()
    folds = research.outer_folds(sessions)
    assert [len(fold.training_sessions) for fold in folds] == [40, 60, 80, 100]
    assert [len(fold.evaluation_sessions) for fold in folds] == [20, 20, 20, 20]
    assert all(
        fold.training_sessions[-1] < fold.evaluation_sessions[0]
        for fold in folds
    )
    assert [day for fold in folds for day in fold.evaluation_sessions] == sessions[40:]


def test_posthoc_frontier_is_outside_registry_and_never_selectable():
    registry_ids = {config.config_id for config in research.candidate_registry()}
    precision, balanced = research.posthoc_frontier_configs()
    assert precision.config_id not in registry_ids
    assert balanced.config_id not in registry_ids
    assert precision.diagnostic_only and balanced.diagnostic_only
    assert precision.rank_min == 230 and balanced.rank_min == 220
    assert precision.return_acceleration_min == 0.0
    assert balanced.true_nifty_daily_change_min == 0.0


def test_filter_contract_has_no_post_entry_outcome_fields():
    research.assert_causal_contract()
    assert not (
        research.FILTER_REFERENCED_FEATURES & research.FORBIDDEN_FILTER_FIELDS
    )


def test_market_join_is_exact_timestamp_and_never_uses_future_bar():
    day = "2026-02-05"
    first = pd.Timestamp(f"{day} 10:00", tz="Asia/Kolkata")
    second = pd.Timestamp(f"{day} 10:05", tz="Asia/Kolkata")
    signals = pd.DataFrame({
        "trade_date": [day, day],
        "signal_time_ist": [first, second],
    })
    # Only 10:00 and a future 10:10 context exist.  The 10:05 signal must
    # remain missing; an as-of/future fallback would be leakage-prone here.
    niftybees = pd.DataFrame({
        "date": [first, second + pd.Timedelta(minutes=5)],
        "open": [100.0, 100.0],
        "close": [101.0, 102.0],
        "VWAP": [100.5, 101.5],
        "ADX": [20.0, 30.0],
    })
    true_nifty = pd.DataFrame({
        "date": [first, second + pd.Timedelta(minutes=5)],
        "Daily_Change": [0.2, 0.5],
    })
    joined = research.attach_exact_completed_market_features(
        signals, [day], niftybees=niftybees, true_nifty=true_nifty
    )
    assert joined["niftybees_context_available"].tolist() == [True, False]
    assert joined["true_nifty_context_available"].tolist() == [True, False]
    assert joined.loc[0, "true_nifty_context_time_ist"] == first
    assert pd.isna(joined.loc[1, "true_nifty_context_time_ist"])


def test_posthoc_true_nifty_diagnostic_is_fail_closed_and_nonpromotable():
    config = next(
        item for item in research.candidate_registry()
        if item.config_id
        == "PB_DIAG_L009216_MOM_R220_259_ADX25_55_TRUE_NIFTY_NONNEG_CAP15"
    )
    passing = _row("2026-02-05", "PASS", 1)
    missing = _row("2026-02-05", "MISS", 2)
    missing["true_nifty_context_available"] = False
    missing["true_nifty_context_time_ist"] = pd.NaT
    missing["true_nifty_daily_change_pct"] = np.nan
    negative = _row("2026-02-05", "NEG", 3)
    negative["true_nifty_daily_change_pct"] = -0.01
    mask = research.filter_mask(pd.DataFrame([passing, missing, negative]), config)
    assert mask.tolist() == [True, False, False]
    assert config.diagnostic_only is True


def test_chronological_cap_and_one_ticker_day_are_shuffle_invariant():
    day = "2026-02-05"
    rows = [
        _row(day, f"T{index:02d}", index, minute=600 + index * 5, rank=220 + index)
        for index in range(8)
    ]
    # A later duplicate ticker must not replace its first chronological signal.
    rows.append(_row(day, "T00", 99, minute=650, rank=201))
    frame = pd.DataFrame(rows)
    config = next(
        item for item in research.candidate_registry()
        if item.config_id == "PB_BASE_CAP05"
    )
    ordered = research.SearchArrays(frame, [day])
    shuffled = research.SearchArrays(frame.sample(frac=1.0, random_state=7), [day])
    ordered_ids = ordered.trades(ordered.selected_indices(config))["_optimizer_row_id"].tolist()
    shuffled_ids = shuffled.trades(shuffled.selected_indices(config))["_optimizer_row_id"].tolist()
    assert ordered_ids == shuffled_ids == [0, 1, 2, 3, 4]


def test_prior_only_selection_is_unchanged_when_future_pnl_is_mutated():
    sessions = _sessions()
    rows = []
    row_id = 0
    for day_index, day in enumerate(sessions):
        momentum_gross = 100.0 if day_index < 40 else -100.0
        expansion_gross = -100.0 if day_index < 40 else 100.0
        rows.append(_row(day, "MOM", row_id, family="MOMENTUM", gross=momentum_gross))
        row_id += 1
        rows.append(_row(day, "EXP", row_id, family="EXPANSION", gross=expansion_gross))
        row_id += 1
    frame = pd.DataFrame(rows)
    momentum = replace(
        next(item for item in research.candidate_registry() if item.config_id == "PB_MOMENTUM_CAP05"),
        config_id="MOM",
    )
    expansion = replace(
        next(item for item in research.candidate_registry() if item.config_id == "PB_EXPANSION_CAP05"),
        config_id="EXP",
    )
    arrays = research.SearchArrays(frame, sessions)
    first, _, _ = research.select_from_prior(arrays, [momentum, expansion], sessions[:40])
    mutated = frame.copy()
    future = mutated["trade_date"].isin(sessions[40:])
    mutated.loc[future, "gross_pnl_rs"] *= -1_000.0
    mutated.loc[future, "net_pnl_rs"] = mutated.loc[future, "gross_pnl_rs"]
    second, _, _ = research.select_from_prior(
        research.SearchArrays(mutated, sessions), [momentum, expansion], sessions[:40]
    )
    assert first.config_id == second.config_id == "MOM"


def test_pf_gate_is_strictly_above_not_equal_to_1p5():
    aggregate = {
        "profit_factor": 1.5,
        "net_pnl_rs": 1.0,
        "trades": 100,
        "trades_per_session": 1.25,
        "median_trades_per_session": 1.0,
        "active_days": 60,
        "best_day_positive_pnl_share": 0.1,
        "best_ticker_positive_pnl_share": 0.1,
    }
    stress = {"net_pnl_rs": 1.0, "profit_factor": 1.2}
    folds = pd.DataFrame({
        "training_gate_passed": [True] * 4,
        "evaluation_net_pnl_rs": [1.0] * 4,
        "evaluation_profit_factor": [1.1] * 4,
        "evaluation_trades": [25] * 4,
    })
    assert research.historical_gate(aggregate, stress, folds)["passed"] is False
    aggregate["profit_factor"] = 1.500001
    assert research.historical_gate(aggregate, stress, folds)["passed"] is True


def test_written_configuration_is_research_only(tmp_path):
    config = next(
        item for item in research.candidate_registry()
        if item.config_id == "PB_BASE_CAP08"
    )
    output = tmp_path / "conf.py"
    research.write_config(output, config, {"passed": True})
    text = output.read_text(encoding="utf-8")
    assert "PRODUCTION_APPROVED = False" in text
    assert "FRESH_FORWARD_HOLDOUT_REQUIRED = True" in text
    assert "PRODUCTION_PROMOTION_ALLOWED = False" in text
    assert "RESEARCH_FREEZE_DATE = '2026-08-06'" in text
    assert "FIRST_UNTOUCHED_EXCHANGE_SESSION_AFTER_RESEARCH_FREEZE_DATE" in text
    assert "DAILY_CAP = 8" in text
