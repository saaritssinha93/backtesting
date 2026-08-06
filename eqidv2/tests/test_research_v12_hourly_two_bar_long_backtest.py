from __future__ import annotations

import pandas as pd
import pytest

import research_v12_hourly_two_bar_long_backtest as replay


IST = "Asia/Kolkata"


def _ts(value: str) -> pd.Timestamp:
    return pd.Timestamp(value, tz=IST)


def test_hourly_membership_handoff_is_strict_and_non_overlapping() -> None:
    memberships = pd.DataFrame(
        {
            "ticker": ["ABC", "ABC"],
            "slot_ist": [_ts("2026-08-03 09:20"), _ts("2026-08-03 10:20")],
            "rank": [10, 15],
        }
    )

    schedule = replay.expand_membership_schedule(memberships)
    owned = schedule.set_index("signal_time_ist")

    assert owned.loc[_ts("2026-08-03 10:20"), "slot_ist"] == _ts("2026-08-03 09:20")
    assert owned.loc[_ts("2026-08-03 10:25"), "slot_ist"] == _ts("2026-08-03 10:20")
    assert not schedule[["ticker", "signal_time_ist"]].duplicated().any()


def test_trigger_latches_across_continuous_hourly_boundary() -> None:
    frame = pd.DataFrame(
        {
            "ticker": ["ABC"] * 4,
            "trade_date": ["2026-08-03"] * 4,
            "signal_time_ist": [
                _ts("2026-08-03 10:15"),
                _ts("2026-08-03 10:20"),
                _ts("2026-08-03 10:25"),
                _ts("2026-08-03 10:30"),
            ],
            "return_5m_close_pct": [0.7, 0.8, 0.9, 1.0],
            "previous_return_5m_close_pct": [0.6, 0.7, 0.8, 0.9],
            "slot_ist": [
                _ts("2026-08-03 09:20"),
                _ts("2026-08-03 09:20"),
                _ts("2026-08-03 10:20"),
                _ts("2026-08-03 10:20"),
            ],
        }
    )

    trigger = replay.mark_first_two_bar_trigger(frame)

    assert trigger.tolist() == [False, True, False, False]


def test_trigger_rearms_after_failure_and_requires_two_bars_after_gap() -> None:
    frame = pd.DataFrame(
        {
            "ticker": ["ABC"] * 6,
            "trade_date": ["2026-08-03"] * 6,
            "signal_time_ist": [
                _ts("2026-08-03 09:25"),
                _ts("2026-08-03 09:30"),
                _ts("2026-08-03 09:35"),
                _ts("2026-08-03 09:40"),
                _ts("2026-08-03 09:45"),
                _ts("2026-08-03 10:00"),
            ],
            "return_5m_close_pct": [0.7, 0.8, 0.4, 0.7, 0.8, 0.9],
            "previous_return_5m_close_pct": [0.6, 0.7, 0.8, 0.4, 0.7, 0.8],
        }
    )

    trigger = replay.mark_first_two_bar_trigger(frame)

    assert trigger.tolist() == [False, True, False, False, True, False]


def _prefilter_rows(*, omit_slot: str | None = None) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for hour in range(9, 16):
        slot_text = f"{hour:02d}:20"
        if slot_text == omit_slot:
            continue
        count = 1 if slot_text == "15:20" else 300
        slot = _ts(f"2026-08-03 {slot_text}")
        for rank in range(1, count + 1):
            rows.append(
                {
                    "slot_ist": slot.isoformat(),
                    "ticker": f"T{rank:03d}",
                    "selection_rank": rank,
                    "selection_bucket": "LONG",
                    "primary_side": "LONG",
                    "primary_family": "test",
                    "selection_reason": "test",
                    "overall_score": 1.0,
                    "long_score": 1.0,
                    "activity_score": 1.0,
                    "date": slot.isoformat(),
                    "staleness_seconds": 0.0,
                }
            )
    return pd.DataFrame(rows)


def test_membership_accepts_degraded_terminal_snapshot_after_entry_cutoff(
    tmp_path,
) -> None:
    path = tmp_path / "prefilter.csv"
    _prefilter_rows().to_csv(path, index=False)

    memberships, audit = replay.load_long_memberships(
        path, "2026-08-03", "2026-08-03"
    )

    assert len(memberships) == 6 * 300
    assert audit["completed_entry_relevant_rows"] == 6 * 300
    assert audit["session_rows"][0]["terminal_1520_rows"] == 1


def test_membership_rejects_missing_actionable_hourly_slot(tmp_path) -> None:
    path = tmp_path / "prefilter.csv"
    _prefilter_rows(omit_slot="10:20").to_csv(path, index=False)

    with pytest.raises(RuntimeError, match="actionable hourly schedule mismatch"):
        replay.load_long_memberships(path, "2026-08-03", "2026-08-03")


def test_session_metrics_supports_a_completely_empty_research_window() -> None:
    metrics, daily = replay.session_metrics(pd.DataFrame(), [])

    assert metrics["sessions"] == 0
    assert metrics["trades"] == 0
    assert metrics["profit_factor"] == 0.0
    assert daily.empty
    assert str(daily["trade_date"].dtype) == "string"


def test_mixed_timestamp_alignment_moves_late_stamped_day_back_five_minutes() -> None:
    minute_times = pd.date_range(
        _ts("2026-08-03 09:16"), periods=15, freq="1min"
    )
    one = pd.DataFrame(
        {
            "date": minute_times,
            "open": [100.0 + index * 0.1 for index in range(15)],
            "high": [100.1 + index * 0.1 for index in range(15)],
            "low": [99.9 + index * 0.1 for index in range(15)],
            "close": [100.05 + index * 0.1 for index in range(15)],
            "volume": [100 + index for index in range(15)],
        }
    )
    aggregate = replay._aggregate_one_minute_bars(one).reset_index()

    def row(completion: str, aggregate_time: str, opening: bool = False) -> dict:
        source = aggregate.loc[
            aggregate["completion_time"].eq(_ts(aggregate_time))
        ].iloc[0]
        return {
            "date": _ts(completion),
            "open": source.one_open,
            "high": source.one_high,
            "low": source.one_low,
            "close": source.one_close,
            "volume": source.one_volume,
            "gap_filled": 0,
            "opening_snapshot": opening,
        }

    five = pd.DataFrame(
        [
            row("2026-08-03 09:15", "2026-08-03 09:20", True),
            row("2026-08-03 09:20", "2026-08-03 09:20"),
            row("2026-08-03 09:25", "2026-08-03 09:20"),
            row("2026-08-03 09:30", "2026-08-03 09:25"),
            row("2026-08-03 09:35", "2026-08-03 09:30"),
        ]
    )

    aligned, audit = replay.align_mixed_five_minute_completion_times(
        five, one, ["2026-08-03"]
    )

    corrected_0925 = aligned.loc[aligned["date"].eq(_ts("2026-08-03 09:25"))].iloc[0]
    expected_0925 = aggregate.loc[
        aggregate["completion_time"].eq(_ts("2026-08-03 09:25"))
    ].iloc[0]
    assert corrected_0925["close"] == expected_0925.one_close
    assert audit["shifted_days"] == 1
    assert audit["native_days"] == 0


def _raw_execution_row(**overrides: object) -> pd.DataFrame:
    row: dict[str, object] = {
        "v7_signal_entry_price": 100.0,
        "v7_signal_stop_price": 99.0,
        "v7_signal_target_price": 101.5,
        "v7_signal_sl_pct": 1.0,
        "v7_signal_target_pct": 1.5,
        "v7_signal_notional_rs": 50_000.0,
        "quantity": 500,
        "signal_atr": 2.0,
        "signal_close": 99.8,
        "signal_low": 99.0,
        "signal_volume": 10_000.0,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def test_quantity_is_lesser_of_risk_size_and_causal_one_minute_capacity(
    monkeypatch,
) -> None:
    monkeypatch.setattr(replay.v12, "_risk_based_qty", lambda entry, stop: 125)

    sized = replay.add_execution_guards(_raw_execution_row()).iloc[0]

    assert sized["risk_based_quantity"] == 125
    assert sized["expected_1m_volume"] == pytest.approx(2_000.0)
    assert sized["causal_capacity_quantity"] == 40
    assert sized["quantity"] == 40
    assert sized["order_participation"] == pytest.approx(0.02)
    assert bool(sized["execution_guard_pass"])

    too_thin = replay.add_execution_guards(
        _raw_execution_row(signal_volume=249.0)
    ).iloc[0]
    assert too_thin["expected_1m_volume"] == pytest.approx(49.8)
    assert too_thin["causal_capacity_quantity"] == 0
    assert too_thin["quantity"] == 0
    assert not bool(too_thin["execution_guard_pass"])


def test_selected_artifact_execution_fields_match_structural_trade(
    monkeypatch,
) -> None:
    monkeypatch.setattr(replay.v12, "_risk_based_qty", lambda entry, stop: 125)

    row = replay.add_execution_guards(_raw_execution_row()).iloc[0]

    assert row["entry_engine_raw_entry_price"] == pytest.approx(100.0)
    assert row["entry_engine_placeholder_stop_price"] == pytest.approx(99.0)
    assert row["entry_engine_placeholder_target_price"] == pytest.approx(101.5)
    assert row["entry_engine_placeholder_quantity"] == 500
    assert row["entry_engine_placeholder_notional_rs"] == pytest.approx(50_000.0)

    assert row["v7_signal_entry_price"] == row["entry_price_with_slippage"]
    assert row["v7_signal_stop_price"] == row["structure_stop_price"]
    assert row["v7_signal_target_price"] == row["structure_target_price"]
    assert row["v7_signal_sl_pct"] == pytest.approx(
        row["structure_risk_per_share"] / row["entry_price_with_slippage"] * 100.0
    )
    assert row["v7_signal_target_pct"] == pytest.approx(
        (row["structure_target_price"] - row["entry_price_with_slippage"])
        / row["entry_price_with_slippage"]
        * 100.0
    )
    assert row["quantity"] == min(
        row["risk_based_quantity"], row["causal_capacity_quantity"]
    )
    assert row["v7_signal_notional_rs"] == pytest.approx(
        row["entry_price_with_slippage"] * row["quantity"]
    )


def _structural_trade_row(**overrides: object) -> pd.Series:
    row: dict[str, object] = {
        "_optimizer_row_id": 7,
        "ticker": "ABC",
        "v7_signal_entry_time_ist": _ts("2026-08-03 10:00"),
        "entry_price_with_slippage": 101.0,
        "structure_stop_price": 98.0,
        "quantity": 10,
        "risk_based_quantity": 100,
        "causal_capacity_quantity": 10,
        "expected_1m_volume": 500.0,
        "signal_high": 100.5,
        "signal_time_ist": _ts("2026-08-03 09:55"),
        "slot_ist": _ts("2026-08-03 09:20"),
        "selection_rank": 1,
        "context_score": 2,
    }
    row.update(overrides)
    return pd.Series(row)


def test_conditional_time_stop_fills_next_open_before_that_bars_intrabar_path(
    monkeypatch,
) -> None:
    times = pd.date_range(_ts("2026-08-03 10:00"), periods=17, freq="1min")
    bars = pd.DataFrame(
        {
            "open": [100.2] * 16 + [99.75],
            "high": [100.4] * 16 + [106.0],
            "low": [100.0] * 16 + [97.0],
            "close": [100.1] * 16 + [101.0],
            "volume": [1_000.0] * 17,
        },
        index=times,
    )
    monkeypatch.setattr(
        replay.v12, "_optimizer_load_1m_day", lambda ticker, day: bars,
        raising=False,
    )

    result = replay.resolve_structural_trade(
        _structural_trade_row(), replay.PRIMARY_POLICY
    )

    assert result is not None
    assert result["outcome"] == "TIME_NO_FOLLOW_THROUGH"
    assert result["exit_time_ist"] == _ts("2026-08-03 10:16")
    assert result["exit_price"] == pytest.approx(99.75)
    # The slipped entry (101) exceeded signal high (100.5), but no observed
    # post-entry print did.  The 10:16 bar then hit both target and stop; the
    # already-pending time exit must still fill first at its open.
    assert result["bars_held"] == 16


def test_simultaneous_intrabar_stop_and_target_remains_stop_first(monkeypatch) -> None:
    bars = pd.DataFrame(
        {
            "open": [100.0],
            "high": [104.0],
            "low": [97.0],
            "close": [101.0],
            "volume": [1_000.0],
        },
        index=pd.DatetimeIndex([_ts("2026-08-03 10:00")]),
    )
    monkeypatch.setattr(
        replay.v12, "_optimizer_load_1m_day", lambda ticker, day: bars,
        raising=False,
    )

    result = replay.resolve_structural_trade(
        _structural_trade_row(
            entry_price_with_slippage=100.0,
            structure_stop_price=98.0,
        ),
        replay.FIXED_POLICY,
    )

    assert result is not None
    assert result["outcome"] == "SL"
    assert result["exit_price"] == pytest.approx(98.0)
