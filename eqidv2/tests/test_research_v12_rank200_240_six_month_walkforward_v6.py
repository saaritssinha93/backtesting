from __future__ import annotations

import pandas as pd
import pytest

import research_v12_rank200_240_six_month_walkforward_v6 as subject


def _calendar() -> list[str]:
    return pd.bdate_range("2026-02-05", periods=120).strftime("%Y-%m-%d").tolist()


def test_walkforward_schedule_has_exact_nonoverlapping_future_coverage() -> None:
    calendar = _calendar()
    blocks = subject.walkforward_blocks(calendar)
    assert len(blocks) == 8
    assert [len(block.evaluation_days) for block in blocks] == [10] * 7 + [2]
    assert [len(block.train_days) for block in blocks] == [
        48, 58, 68, 78, 88, 98, 108, 118
    ]
    evaluated = [day for block in blocks for day in block.evaluation_days]
    assert evaluated == calendar[48:]
    assert len(evaluated) == len(set(evaluated)) == 72
    for block in blocks:
        assert set(block.train_days).isdisjoint(block.evaluation_days)
        assert max(block.train_days) < min(block.evaluation_days)


def test_walkforward_schedule_rejects_invalid_windows() -> None:
    calendar = _calendar()
    with pytest.raises(ValueError, match="shorter"):
        subject.walkforward_blocks(calendar, initial_train_sessions=19)
    with pytest.raises(ValueError, match="full calendar"):
        subject.walkforward_blocks(calendar, initial_train_sessions=120)
    with pytest.raises(ValueError, match="positive"):
        subject.walkforward_blocks(calendar, block_sessions=0)


def test_frozen_setup_is_exactly_the_rank_tightened_v5_choice() -> None:
    config = subject.FROZEN_CONFIG
    assert config.config_id == "LEVEL12_SEQ8_SL1p0_T2p0_F0p25"
    assert config.feature_family == "LEVEL12_SEQ8"
    assert config.sl_pct == 1.0
    assert config.tgt_pct == 2.0
    assert config.rolling_fraction == 0.25
    assert (subject.RANK_MIN, subject.RANK_MAX) == (200, 240)
    assert subject.PRODUCTION_APPROVED is False


def test_candidate_daily_keeps_zero_candidate_sessions() -> None:
    days = ["2026-02-05", "2026-02-06"]
    tickets = pd.DataFrame([{
        "ticket_id": 1,
        "ticker": "ABC",
        "trade_date": days[0],
        "ticket_time_ist": pd.Timestamp("2026-02-05 10:00", tz="Asia/Kolkata"),
    }])
    result = subject.candidate_daily(tickets, days)
    assert result["trade_date"].tolist() == days
    assert result["base_ticket_rows"].tolist() == [1, 0]
    assert result["unique_tickers"].tolist() == [1, 0]


def test_pooled_auc_normalizes_object_labels_from_empty_fold_concatenation() -> None:
    diagnostics = pd.DataFrame({
        "label": pd.Series(["0", "1", "0", "1"], dtype=object),
        "score": pd.Series([0.1, 0.9, 0.2, 0.8], dtype=object),
        "weight": pd.Series([1.0, 1.0, 1.0, 1.0], dtype=object),
    })
    assert subject.pooled_auc(diagnostics) == 1.0


def test_candidate_hourly_counts_unique_ticker_days_not_ticket_rows() -> None:
    tickets = pd.DataFrame({
        "ticket_id": [1, 2, 3],
        "ticker": ["ABC", "ABC", "ABC"],
        "trade_date": ["2026-02-05", "2026-02-05", "2026-02-06"],
        "ticket_time_ist": pd.to_datetime([
            "2026-02-05 10:00+05:30",
            "2026-02-05 10:10+05:30",
            "2026-02-06 10:00+05:30",
        ]),
    })
    result = subject.candidate_hourly(tickets).iloc[0]
    assert result["base_ticket_rows"] == 3
    assert result["unique_ticker_days"] == 2
    assert result["active_sessions"] == 2
