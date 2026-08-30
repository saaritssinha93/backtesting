from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from zoneinfo import ZoneInfo

import fno_multi_paper_profiles as profiles
from fno_multi_paper_engine import (
    CompletedMinuteBar,
    MultiStrategyPaperEngine,
    PaperCandidate,
)
from fno_multi_paper_parity import validate_canonical_profiles


IST = ZoneInfo("Asia/Kolkata")


def ts(hour: int, minute: int) -> datetime:
    return datetime(2026, 9, 1, hour, minute, tzinfo=IST)


def candidate(
    symbol: str,
    signal: datetime,
    *,
    side: str = "LONG",
    signal_close: float = 100.0,
    move: float | None = None,
    oi_change: float = 1.2,
    volume_ratio: float = 3.5,
    traded_value: float = 30_000_000.0,
    tick_size: float = 0.05,
) -> PaperCandidate:
    long_side = side == "LONG"
    return PaperCandidate(
        symbol=symbol,
        signal_time=signal,
        five_min_open=signal_close - (1.0 if long_side else -1.0),
        five_min_high=signal_close + 2.0,
        five_min_low=signal_close - 2.0,
        five_min_close=signal_close,
        price_change_pct=(0.8 if long_side else -0.8) if move is None else move,
        oi_change_pct=oi_change,
        volume_ratio=volume_ratio,
        traded_value=traded_value,
        ema9=102.0 if long_side else 98.0,
        ema20=101.0 if long_side else 99.0,
        ema50=100.0,
        oi=101.2,
        prev_oi=100.0,
        tick_size=tick_size,
        equity_instrument_token=100,
        futures_instrument_token=200,
        futures_symbol=f"{symbol}FUT",
    )


def bar(
    timestamp: datetime,
    open_: float,
    high: float,
    low: float,
    close: float,
) -> CompletedMinuteBar:
    return CompletedMinuteBar(timestamp, open_, high, low, close, 1_000.0)


def record(engine: MultiStrategyPaperEngine, profile: str, symbol: str, setup: str) -> dict:
    return next(
        row
        for row in engine.records_by_profile()[profile]
        if row["symbol"] == symbol and row["setup_id"] == setup
    )


def test_profiles_match_canonical_sources_and_are_distinct() -> None:
    evidence = validate_canonical_profiles()

    assert evidence["source_contract_assertions_passed"] is True
    assert evidence["full_history_event_parity_certified"] is False
    assert profiles.V10_PROFILE.profile_id == "V10_STAGE7_0935_LONG_MAX_050_GAP2"
    assert profiles.V11_PROFILE.profile_id == "V11_S10_POST_HOC_TOP2_1436C7D363"
    assert profiles.V12_PROFILE.profile_id == "V12_S06_LATE_SHORT_VOLUME_MIN_150"
    assert len({profile.fingerprint for profile in profiles.PROFILES}) == 3


def test_profile_specific_five_minute_filters_and_filter_then_rerank() -> None:
    engine = MultiStrategyPaperEngine()
    signal = ts(9, 35)
    engine.register_candidates(
        "09:35_LONG",
        signal,
        [
            candidate("EDGE", signal, move=0.50),
            candidate("OVER", signal, move=0.5000001),
        ],
    )
    for key in ("v10", "v11", "v12"):
        assert [row["symbol"] for row in engine.records_by_profile()[key]] == ["EDGE"]
        audit = {
            row["symbol"]: row
            for row in engine.selection_records_by_profile()[key]
        }
        assert audit["EDGE"]["selection_status"] == "SELECTED"
        assert audit["EDGE"]["selection_rank"] == 1
        assert audit["OVER"]["selection_rejection_codes"] == ["MOVE_ABOVE_MAXIMUM"]

    late = MultiStrategyPaperEngine()
    signal = ts(9, 40)
    short = candidate("RVOL", signal, side="SHORT", move=-0.4, volume_ratio=1.2)
    late.register_candidates("09:40_SHORT", signal, [short])
    assert [row["symbol"] for row in late.records_by_profile()["v10"]] == ["RVOL"]
    assert [row["symbol"] for row in late.records_by_profile()["v11"]] == ["RVOL"]
    assert late.records_by_profile()["v12"] == []
    v12_audit = late.selection_records_by_profile()["v12"][0]
    assert v12_audit["selection_rejection_codes"] == ["VOLUME_RATIO_BELOW_MINIMUM"]


def test_gap2_is_inclusive_and_larger_gap_is_terminally_rejected() -> None:
    signal = ts(9, 30)

    accepted = MultiStrategyPaperEngine()
    exact = candidate("EXACT", signal, tick_size=0.00001)
    accepted.register_candidates("09:30_LONG", signal, [exact])
    accepted.process_completed_minute(
        ts(9, 31), {"EXACT": bar(ts(9, 31), 100.0, 100.10, 99.99, 100.08)}
    )
    trigger = record(accepted, "v10", "EXACT", "09:30_LONG")["trigger"]
    opening = trigger * 1.0002
    accepted.process_completed_minute(
        ts(9, 32),
        {"EXACT": bar(ts(9, 32), opening, opening + 0.01, opening - 0.01, opening)},
    )
    for key in ("v10", "v11", "v12"):
        result = record(accepted, key, "EXACT", "09:30_LONG")
        assert result["gap_guard_observed"] is True
        assert result["gap_guard_rejected"] is False
        assert result["entry_price"] is not None

    rejected = MultiStrategyPaperEngine()
    over = candidate("OVER", signal, tick_size=0.00001)
    rejected.register_candidates("09:30_LONG", signal, [over])
    rejected.process_completed_minute(
        ts(9, 31), {"OVER": bar(ts(9, 31), 100.0, 100.10, 99.99, 100.08)}
    )
    trigger = record(rejected, "v10", "OVER", "09:30_LONG")["trigger"]
    opening = trigger * 1.000201
    rejected.process_completed_minute(
        ts(9, 32),
        {"OVER": bar(ts(9, 32), opening, opening + 0.01, opening - 0.01, opening)},
    )
    for key in ("v10", "v11", "v12"):
        result = record(rejected, key, "OVER", "09:30_LONG")
        assert result["status"] == "POSTCONF_CANCELLED"
        assert result["reason"] == "ADVERSE_GAP_GUARD_REJECTED"
        assert result["gap_guard_rejected"] is True
        assert result["entry_price"] is None


def test_v11_and_v12_delay_0930_short_fill_until_s3() -> None:
    engine = MultiStrategyPaperEngine()
    signal = ts(9, 30)
    short = replace(
        candidate("DELAY", signal, side="SHORT", move=-0.4),
        five_min_high=102.0,
        five_min_low=96.0,
    )
    engine.register_candidates("09:30_SHORT", signal, [short])
    engine.process_completed_minute(
        ts(9, 31), {"DELAY": bar(ts(9, 31), 99.1, 99.2, 98.5, 98.6)}
    )
    engine.process_completed_minute(
        ts(9, 32), {"DELAY": bar(ts(9, 32), 98.5, 98.6, 98.4, 98.45)}
    )

    assert record(engine, "v10", "DELAY", "09:30_SHORT")["entry_minute"] == 2
    for key in ("v11", "v12"):
        pending = record(engine, key, "DELAY", "09:30_SHORT")
        assert pending["status"] == "PENDING_STOP"
        assert pending["entry_price"] is None
        assert pending["early_fill_checks_skipped"] == 1
        assert pending["early_touch_observed"] is True

    engine.process_completed_minute(
        ts(9, 33), {"DELAY": bar(ts(9, 33), 98.5, 98.6, 98.4, 98.45)}
    )
    for key in ("v11", "v12"):
        assert record(engine, key, "DELAY", "09:30_SHORT")["entry_minute"] == 3


def test_v11_same_side_two_is_independent_from_v10_and_survives_checkpoint() -> None:
    engine = MultiStrategyPaperEngine()
    first_signal = ts(9, 25)
    engine.register_candidates(
        "09:25_LONG", first_signal, [candidate("SAME", first_signal)]
    )
    engine.process_completed_minute(
        ts(9, 26), {"SAME": bar(ts(9, 26), 100.0, 101.0, 99.9, 100.8)}
    )
    engine.process_completed_minute(
        ts(9, 27), {"SAME": bar(ts(9, 27), 101.0, 101.1, 100.9, 101.05)}
    )
    for minute in (28, 29, 30):
        engine.process_completed_minute(
            ts(9, minute),
            {"SAME": bar(ts(9, minute), 101.05, 101.15, 100.95, 101.05)},
        )

    second_signal = ts(9, 30)
    engine.register_candidates(
        "09:30_LONG",
        second_signal,
        [candidate("SAME", second_signal, signal_close=101.0)],
    )
    engine.process_completed_minute(
        ts(9, 31), {"SAME": bar(ts(9, 31), 101.0, 101.8, 100.9, 101.7)}
    )

    assert record(engine, "v10", "SAME", "09:30_LONG")["portfolio_decision"] == "REJECTED"
    for key in ("v11", "v12"):
        assert record(engine, key, "SAME", "09:30_LONG")["portfolio_decision"] == "ACCEPTED"
        assert engine.engines[key].state_summary()["active_portfolio_count"] == 2

    restored = MultiStrategyPaperEngine.from_checkpoint(engine.checkpoint())
    assert restored.records() == engine.records()
    assert restored.selection_records_by_profile() == engine.selection_records_by_profile()
    assert restored.required_symbols() == engine.required_symbols()
