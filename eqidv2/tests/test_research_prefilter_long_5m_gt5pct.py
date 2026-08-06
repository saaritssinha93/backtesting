from __future__ import annotations

from pathlib import Path

import pandas as pd

import research_prefilter_long_5m_gt5pct as study


def ist(value: str) -> pd.Timestamp:
    return pd.Timestamp(value, tz=study.IST)


def test_hourly_pool_activation_and_final_slot_schedule() -> None:
    morning = study.signal_schedule(ist("2026-02-05 09:20"))
    assert morning[0] == ist("2026-02-05 09:25")
    assert morning[-1] == ist("2026-02-05 10:20")
    assert len(morning) == 12

    final = study.signal_schedule(ist("2026-02-05 15:20"))
    assert final == [ist("2026-02-05 15:25")]


def test_after_market_spike_cannot_enter_five_minute_target() -> None:
    bars = pd.DataFrame(
        {
            "date": [
                ist("2026-02-05 15:25"),
                ist("2026-02-05 15:30"),
                ist("2026-02-05 15:35"),
            ],
            "open": [100.0, 100.0, 100.0],
            "high": [101.0, 102.0, 150.0],
            "low": [99.0, 99.0, 99.0],
            "close": [100.0, 101.0, 140.0],
            "gap_filled": [0, 0, 0],
            "trade_date": ["2026-02-05"] * 3,
        }
    )
    session = study.filter_end_stamped_session(
        bars, first_label=study.SESSION_FIRST_5M_END
    )
    outcome = study.add_forward_five_minute_outcomes(session)
    assert outcome["date"].max() == ist("2026-02-05 15:30")
    assert outcome.loc[0, "forward_max_high_5m"] == 102.0
    assert outcome.loc[0, "eod_close_5m"] == 101.0


def test_complete_one_minute_path_is_primary_and_starts_strictly_after_entry(
    monkeypatch,
) -> None:
    one = pd.DataFrame(
        {
            "date": pd.date_range(
                ist("2026-02-05 15:26"), ist("2026-02-05 15:30"), freq="1min"
            ),
            "open": [100.0] * 5,
            "high": [101.0, 102.0, 106.0, 103.0, 104.0],
            "low": [99.0] * 5,
            "close": [100.0] * 5,
            "trade_date": ["2026-02-05"] * 5,
        }
    )
    monkeypatch.setattr(study, "read_one_minute", lambda _: one.copy())
    opportunity = pd.DataFrame(
        {
            "trade_date": ["2026-02-05"],
            "entry_execution_time_ist": [ist("2026-02-05 15:25")],
            "entry_price": [100.0],
            "forward_real_count_5m": [1],
            "forward_exact_grid_5m": [True],
            "five_minute_day_complete": [True],
            "forward_max_high_5m": [102.0],
            "forward_max_time_5m": [ist("2026-02-05 15:30")],
            "eod_close_5m": [101.0],
        }
    )
    result, audit = study.add_primary_one_minute_outcomes(
        opportunity, "TEST", Path("unused")
    )
    assert audit["one_minute_complete_opportunities"] == 1
    assert result.loc[0, "daily_max_price"] == 106.0
    assert result.loc[0, "daily_max_bar_end_ist"] == ist("2026-02-05 15:28")
    assert result.loc[0, "daily_max_time_ist"] == ist("2026-02-05 15:27")
    assert bool(result.loc[0, "hit_5pct"])


def test_partial_one_minute_path_falls_back_to_complete_five_minute(
    monkeypatch,
) -> None:
    one = pd.DataFrame(
        {
            "date": [ist("2026-02-05 15:26"), ist("2026-02-05 15:27")],
            "open": [100.0, 100.0],
            "high": [101.0, 110.0],
            "low": [99.0, 99.0],
            "close": [100.0, 100.0],
            "trade_date": ["2026-02-05"] * 2,
        }
    )
    monkeypatch.setattr(study, "read_one_minute", lambda _: one.copy())
    opportunity = pd.DataFrame(
        {
            "trade_date": ["2026-02-05"],
            "entry_execution_time_ist": [ist("2026-02-05 15:25")],
            "entry_price": [100.0],
            "forward_real_count_5m": [1],
            "forward_exact_grid_5m": [True],
            "five_minute_day_complete": [True],
            "forward_max_high_5m": [104.0],
            "forward_max_time_5m": [ist("2026-02-05 15:30")],
            "eod_close_5m": [103.0],
        }
    )
    result, audit = study.add_primary_one_minute_outcomes(
        opportunity, "TEST", Path("unused")
    )
    assert audit["five_minute_fallback_opportunities"] == 1
    assert result.loc[0, "daily_max_time_source"] == "5min_fallback"
    assert result.loc[0, "daily_max_price"] == 104.0
    assert not bool(result.loc[0, "hit_5pct"])


def test_off_grid_one_minute_row_cannot_compensate_for_missing_minute(
    monkeypatch,
) -> None:
    one = pd.DataFrame(
        {
            "date": [
                ist("2026-02-05 15:26"),
                ist("2026-02-05 15:27"),
                ist("2026-02-05 15:27:30"),
                ist("2026-02-05 15:29"),
                ist("2026-02-05 15:30"),
            ],
            "open": [100.0] * 5,
            "high": [101.0, 102.0, 199.0, 103.0, 104.0],
            "low": [99.0] * 5,
            "close": [100.0] * 5,
            "trade_date": ["2026-02-05"] * 5,
        }
    )
    monkeypatch.setattr(study, "read_one_minute", lambda _: one.copy())
    opportunity = pd.DataFrame(
        {
            "trade_date": ["2026-02-05"],
            "entry_execution_time_ist": [ist("2026-02-05 15:25")],
            "entry_price": [100.0],
            "forward_real_count_5m": [1],
            "forward_exact_grid_5m": [True],
            "five_minute_day_complete": [True],
            "forward_max_high_5m": [104.0],
            "forward_max_time_5m": [ist("2026-02-05 15:30")],
            "eod_close_5m": [103.0],
        }
    )
    result, audit = study.add_primary_one_minute_outcomes(
        opportunity, "TEST", Path("unused")
    )
    assert audit["one_minute_complete_opportunities"] == 0
    assert result.loc[0, "daily_max_time_source"] == "5min_fallback"
    assert result.loc[0, "daily_max_price"] == 104.0


def test_off_grid_five_minute_tail_is_not_complete() -> None:
    bars = pd.DataFrame(
        {
            "date": [
                ist("2026-02-05 15:20"),
                ist("2026-02-05 15:22"),
                ist("2026-02-05 15:30"),
            ],
            "open": [100.0] * 3,
            "high": [101.0, 199.0, 102.0],
            "low": [99.0] * 3,
            "close": [100.0] * 3,
            "gap_filled": [0, 0, 0],
            "trade_date": ["2026-02-05"] * 3,
        }
    )
    result = study.add_forward_five_minute_outcomes(bars)
    assert not bool(result.loc[0, "forward_exact_grid_5m"])
    assert not bool(result.loc[1, "forward_exact_grid_5m"])
    assert bool(result.loc[2, "forward_exact_grid_5m"])


def test_naive_timestamps_are_localized_as_ist() -> None:
    normalized = study.normalise_ist(pd.Series(["2026-02-05 09:20:00"]))
    assert normalized.iloc[0] == ist("2026-02-05 09:20")


def test_float32_high_does_not_round_a_float64_target_downward() -> None:
    bars = pd.DataFrame({"high": pd.Series([217.13999938964844], dtype="float32")})
    target = 206.8000030517578 * 1.05
    assert study.bars_reaching_target(bars, target).empty


def test_authoritative_calendar_controls_split_when_a_day_has_no_rows() -> None:
    dates = pd.bdate_range("2026-01-01", periods=120).strftime("%Y-%m-%d").tolist()
    frame = pd.DataFrame({"trade_date": dates[1:], "ticker": ["X"] * 119})
    assigned, contract = study.assign_splits(frame, dates)
    assert contract["train"][2] == 72
    assert contract["validation"][2] == 24
    assert contract["holdout"][2] == 24
    assert assigned.loc[assigned["trade_date"].eq(dates[72]), "split"].iloc[0] == "VALIDATION"


def test_empty_or_unaccepted_config_fails_closed() -> None:
    text = study.render_config(
        conditions=[],
        split_contract={"train": ["a", "b", 1]},
        metrics_by_split={},
        sensitivity_by_split={},
        source_path=Path("source.csv"),
        source_sha256="abc",
        setup_status="NO_VALIDATED_FILTER_BASELINE_ONLY",
        acceptance={"passed": False, "reasons": ["none"]},
    )
    namespace: dict[str, object] = {}
    exec(compile(text, "generated_conf.py", "exec"), namespace)
    assert namespace["PRODUCTION_APPROVED"] is False
    assert namespace["matches"]({"RSI": 60.0}) is False
