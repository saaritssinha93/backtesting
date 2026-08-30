from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

import fno_v10_followup_challenger_research as research
import fno_v8_windowed_1m_entry_backtest as engine


def _candidate_row(candidate_id: str, symbol: str, move: float, rank: int) -> dict:
    return {
        "candidate_id": candidate_id,
        "session_date": pd.Timestamp("2026-08-27").date(),
        "signal_time": pd.Timestamp("2026-08-27 09:35", tz="Asia/Kolkata"),
        "setup_id": "09:35_LONG",
        "side": "LONG",
        "symbol": symbol,
        "price_change_pct": move,
        "picker_value": 100.0 - rank,
        "traded_value": 10_000_000.0 - rank,
        "frozen_rank": rank,
    }


def test_registry_is_isolated_and_predeclared() -> None:
    research.validate_registry()
    assert len(research.SPECS) == 9
    assert research.SPEC_BY_NAME["STAGE7_CONTROL"].uses_previous10 is False


def test_0935_upper_ceiling_filters_before_contiguous_rerank() -> None:
    frame = pd.DataFrame(
        [
            _candidate_row("A", "AAA", 0.60, 1),
            _candidate_row("B", "BBB", 0.35, 2),
        ]
    )
    spec = research.SPEC_BY_NAME["0935_LONG_MOVE_MAX_040"]
    selected, decisions = research.selection_overlay(frame, spec)
    assert selected["candidate_id"].tolist() == ["B"]
    assert selected["frozen_rank"].tolist() == [1]
    rejected = decisions.set_index("candidate_id").loc["A"]
    assert rejected["selection_passed"] == False  # noqa: E712
    assert rejected["selection_reason"] == "0935_LONG_MOVE_ABOVE_CHALLENGER_MAX"


def test_previous10_feature_excludes_current_confirmation_bar(tmp_path: Path) -> None:
    symbol = "TEST"
    timestamps = pd.date_range(
        "2026-08-27 09:16", periods=11, freq="1min", tz="Asia/Kolkata"
    )
    source = pd.DataFrame(
        {
            "date": timestamps,
            "open": [100.0] * 11,
            "high": [101.0] * 11,
            "low": [99.0] * 11,
            "close": [100.5] * 11,
            "volume": list(range(1, 11)) + [1000],
        }
    )
    source_path = tmp_path / "TEST_stocks_indicators_1min.parquet"
    source.to_parquet(source_path, index=False)
    snapshot_path = tmp_path / "manifest.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "snapshot_fingerprint": "synthetic",
                "captures": [
                    {
                        "role": "NSE_EQUITY_1M",
                        "logical_symbol": symbol,
                        "snapshot_path": str(source_path),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    minute_paths = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "setup_id": "09:25_LONG",
                "minute_index": 1,
                "bar_ts": timestamps[-1],
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            }
        ]
    )
    frame, _, _ = research.build_previous10_features(
        candidates=pd.DataFrame(),
        minute_paths=minute_paths,
        snapshot_manifest=snapshot_path,
        output_dir=tmp_path / "features",
        from_day="2026-08-27",
        through_day="2026-08-27",
    )
    row = frame.iloc[0]
    assert row["feature_available"] == True  # noqa: E712
    assert row["prior_count"] == 10
    assert row["prior_volume_median"] == 5.5
    assert row["current_volume"] == 1000
    assert row["previous10_volume_ratio"] == 1000 / 5.5
    assert row["prior_end_ts"] == timestamps[-2]


def test_0925_long_body_challenger_is_leg_specific() -> None:
    original = research._ACTIVE_SPEC
    try:
        research._ACTIVE_SPEC = research.SPEC_BY_NAME["0925_LONG_BODY_MIN_050"]
        long_setup = next(
            setup for setup in engine.ACTIVE_SETUPS if setup.setup_id == "09:25_LONG"
        )
        short_setup = next(
            setup for setup in engine.ACTIVE_SETUPS if setup.setup_id == "09:25_SHORT"
        )
        candidate = engine.CandidateInput(
            symbol="TEST",
            signal_time=pd.Timestamp("2026-08-27 09:25", tz="Asia/Kolkata"),
            five_min_open=100,
            five_min_high=101,
            five_min_low=99,
            five_min_close=100,
            price_change_pct=0.5,
            oi_change_pct=1.0,
            volume_ratio=4.0,
            traded_value=100_000_000,
            five_min_volume=10_000,
        )
        long_bar = engine.MinuteBar(
            pd.Timestamp("2026-08-27 09:26", tz="Asia/Kolkata"),
            100.0,
            101.0,
            99.0,
            100.8,
            1000,
        )
        long_result = research.challenger_confirmation_check(
            long_setup, candidate, long_bar, engine.EntryPolicy()
        )
        assert long_result["body_ratio"] == pytest.approx(0.4)
        assert "CHALLENGER_BODY_RATIO_BELOW_MINIMUM" in long_result["rejection_codes"]

        short_candidate = engine.CandidateInput(
            **{**candidate.__dict__, "price_change_pct": -0.5, "five_min_close": 100.5}
        )
        short_bar = engine.MinuteBar(
            pd.Timestamp("2026-08-27 09:26", tz="Asia/Kolkata"),
            100.5,
            101.0,
            99.0,
            99.7,
            1000,
        )
        short_result = research.challenger_confirmation_check(
            short_setup, short_candidate, short_bar, engine.EntryPolicy()
        )
        assert "CHALLENGER_BODY_RATIO_BELOW_MINIMUM" not in short_result["rejection_codes"]
    finally:
        research._ACTIVE_SPEC = original
