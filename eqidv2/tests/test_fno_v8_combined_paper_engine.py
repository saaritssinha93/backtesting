from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

import fno_v8_combined_paper_config as config
from fno_v8_combined_paper_engine import (
    CompletedMinuteBar,
    PaperCandidate,
    PaperEngine,
    PaperEngineError,
    PaperEngineConfig,
    ReplayConflictError,
)


IST = ZoneInfo("Asia/Kolkata")


def ts(hour: int, minute: int) -> datetime:
    return datetime(2026, 8, 21, hour, minute, tzinfo=IST)


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
        tick_size=0.05,
    )


def bar(
    timestamp: datetime,
    open_: float,
    high: float,
    low: float,
    close: float,
    *,
    volume: float = 1_000.0,
    **flags: bool,
) -> CompletedMinuteBar:
    return CompletedMinuteBar(
        timestamp=timestamp,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        **flags,
    )


def record(engine: PaperEngine, symbol: str) -> dict:
    return next(item for item in engine.records() if item["symbol"] == symbol)


def test_default_configuration_is_the_frozen_combined_book() -> None:
    engine = PaperEngine()

    assert len(engine.config.setups) == 10
    assert engine.config.setup_book_sha256 == config.COMBINED_SETUP_BOOK_SHA256
    assert engine.config.strategy_fingerprint == config.strategy_fingerprint()
    assert engine.config.entry_policies["09:25_LONG"].max_confirmation_minute == 3
    assert engine.config.entry_policies["09:25_SHORT"].buffer_bps == 2.0
    assert engine.config.entry_policies["09:30_SHORT"].midpoint_invalidation is True
    assert engine.config.entry_policies["09:30_SHORT"].close_location_min == 0.50
    assert engine.config.entry_policies["09:35_LONG"].max_confirmation_minute == 1


def test_registration_revalidates_full_five_minute_authority_and_rank() -> None:
    signal = ts(9, 30)
    invalid_trend = replace(candidate("BAD", signal), ema9=99.0, ema20=100.0, ema50=101.0)
    with pytest.raises(ValueError, match="five-minute authority"):
        PaperEngine().register_candidates("09:30_LONG", signal, [invalid_trend])

    invalid_oi = replace(candidate("OI", signal), oi=-1.0, prev_oi=-2.0)
    with pytest.raises(ValueError, match="five-minute authority"):
        PaperEngine().register_candidates("09:30_LONG", signal, [invalid_oi])

    engine = PaperEngine()
    # Same picker value falls through to traded value and then symbol.
    low_tv = candidate("AAA", signal, move=0.8, traded_value=20_000_000.0)
    high_tv = candidate("ZZZ", signal, move=0.8, traded_value=40_000_000.0)
    engine.register_candidates("09:30_LONG", signal, [low_tv, high_tv])
    ranks = {item["symbol"]: item["frozen_rank"] for item in engine.records()}
    assert ranks == {"ZZZ": 1, "AAA": 2}


def test_setup_threshold_is_exact_with_no_epsilon_admission() -> None:
    signal = ts(9, 30)
    exact = replace(candidate("EDGE", signal), price_change_pct=0.65)
    accepted = PaperEngine()
    accepted.register_candidates("09:30_LONG", signal, [exact])
    assert record(accepted, "EDGE")["frozen_rank"] == 1

    microscopically_below = replace(
        candidate("BELOW", signal), price_change_pct=0.65 - 5e-13
    )
    with pytest.raises(ValueError, match="five-minute authority"):
        PaperEngine().register_candidates(
            "09:30_LONG", signal, [microscopically_below]
        )


def test_no_confirmation_bar_fill_gap_fill_actual_brackets_and_stop_first() -> None:
    signal = ts(9, 30)
    engine = PaperEngine()
    engine.register_candidates("09:30_LONG", signal, [candidate("AAA", signal)])

    confirmation = bar(ts(9, 31), 100.0, 101.0, 99.9, 100.8)
    engine.process_completed_minute(ts(9, 31), {"AAA": confirmation})
    pending = record(engine, "AAA")
    assert pending["status"] == "PENDING_STOP"
    assert pending["confirmation_minute"] == 1
    assert pending["trigger"] == 101.0
    assert pending["entry_price"] is None  # confirmation candle cannot fill

    # Opens beyond the trigger, then both brackets occur in the fill candle.
    # V8 uses the adverse open, brackets from that fill, and STOP_FIRST.
    engine.process_completed_minute(
        ts(9, 32), {"AAA": bar(ts(9, 32), 101.2, 104.0, 100.0, 102.0)}
    )
    closed = record(engine, "AAA")
    assert closed["status"] == "STOPPED"
    assert closed["entry_price"] == 101.2
    assert closed["gap_fill"] is True
    assert closed["stop_price"] == 100.15
    assert closed["target_price"] == 103.70
    assert closed["exit_price"] == 100.15
    assert closed["exit_reason"] == "STOP"
    assert closed["ambiguous_entry_bar"] is True
    assert engine.required_symbols() == []


def test_midpoint_invalidation_precedes_a_simultaneous_short_confirmation() -> None:
    signal = ts(9, 30)
    engine = PaperEngine()
    short = candidate(
        "MID",
        signal,
        side="SHORT",
        signal_close=100.0,
        move=-0.4,
        oi_change=1.2,
    )
    short = replace(short, five_min_high=102.0, five_min_low=96.0)
    engine.register_candidates("09:30_SHORT", signal, [short])

    # Bearish and below the signal close, but above the 5m midpoint (99).
    # The midpoint kill has precedence over the otherwise valid morphology.
    engine.process_completed_minute(
        ts(9, 31), {"MID": bar(ts(9, 31), 100.2, 100.3, 99.4, 99.5)}
    )
    result = record(engine, "MID")
    assert result["status"] == "PRECONF_INVALIDATED"
    assert result["confirmation_minute"] is None
    assert result["confirmation_checks"][0]["rejection_codes"] == [
        "PRECONF_MIDPOINT_INVALIDATED"
    ]


def test_postconfirm_cancel_frees_local_cap_for_waiting_candidate() -> None:
    signal = ts(9, 30)
    engine = PaperEngine()
    first = candidate("AAA", signal, move=1.0)
    second = candidate("BBB", signal, move=0.8)
    engine.register_candidates("09:30_LONG", signal, [second, first])

    engine.process_completed_minute(
        ts(9, 31),
        {
            "AAA": bar(ts(9, 31), 100.0, 101.0, 99.9, 100.8),
            "BBB": bar(ts(9, 31), 100.0, 101.0, 99.9, 100.8),
        },
    )
    assert record(engine, "AAA")["status"] == "PENDING_STOP"
    assert record(engine, "BBB")["status"] == "CONFIRMED_WAITING_CAP"

    # AAA does not touch 101 and closes through the signal close; BBB holds.
    engine.process_completed_minute(
        ts(9, 32),
        {
            "AAA": bar(ts(9, 32), 100.5, 100.9, 99.0, 99.5),
            "BBB": bar(ts(9, 32), 100.5, 100.9, 100.1, 100.6),
        },
    )
    assert record(engine, "AAA")["status"] == "POSTCONF_CANCELLED"
    replacement = record(engine, "BBB")
    assert replacement["status"] == "PENDING_STOP"
    assert replacement["portfolio_decision"] == "ACCEPTED"


def test_checkpoint_restart_and_duplicate_minute_are_idempotent() -> None:
    signal = ts(9, 30)
    engine = PaperEngine()
    engine.register_candidates("09:30_LONG", signal, [candidate("RST", signal)])
    completed = {"RST": bar(ts(9, 31), 100.0, 101.0, 99.9, 100.8)}
    engine.process_completed_minute(ts(9, 31), completed)

    restored = PaperEngine.from_checkpoint_json(engine.checkpoint_json())
    assert restored.records() == engine.records()
    assert restored.last_processed_minute == ts(9, 31)
    assert restored.required_symbols() == ["RST"]
    assert restored.process_completed_minute(ts(9, 31), completed) == []
    assert restored.events() == engine.events()

    changed = {"RST": bar(ts(9, 31), 100.0, 101.1, 99.9, 100.8)}
    with pytest.raises(ReplayConflictError):
        restored.process_completed_minute(ts(9, 31), changed)


def test_skipped_or_invalid_required_bar_fails_closed() -> None:
    signal = ts(9, 30)
    skipped = PaperEngine()
    skipped.register_candidates("09:30_LONG", signal, [candidate("MISS", signal)])
    skipped.process_completed_minute(
        ts(9, 32), {"MISS": bar(ts(9, 32), 100.0, 101.0, 99.9, 100.8)}
    )
    assert record(skipped, "MISS")["status"] == "DATA_INCOMPLETE"
    assert record(skipped, "MISS")["reason"] == "MISSING_REQUIRED_MINUTE_BAR"

    invalid = PaperEngine()
    invalid.register_candidates("09:30_LONG", signal, [candidate("BAD", signal)])
    invalid.process_completed_minute(
        ts(9, 31),
        {"BAD": bar(ts(9, 31), 100.0, 101.0, 99.9, 100.8, gap_filled=True)},
    )
    assert record(invalid, "BAD")["status"] == "DATA_INCOMPLETE"
    assert record(invalid, "BAD")["reason"] == "INVALID_REQUIRED_MINUTE_BAR"

    nonfinite = PaperEngine()
    nonfinite.register_candidates("09:30_LONG", signal, [candidate("NAN", signal)])
    nonfinite.process_completed_minute(
        ts(9, 31),
        {"NAN": bar(ts(9, 31), float("nan"), 101.0, 99.9, 100.8)},
    )
    assert record(nonfinite, "NAN")["status"] == "DATA_INCOMPLETE"
    assert record(nonfinite, "NAN")["reason"] == "INVALID_REQUIRED_MINUTE_BAR"


def test_exact_1530_squareoff_uses_completed_bar_close() -> None:
    signal = ts(9, 30)
    engine = PaperEngine()
    engine.register_candidates("09:30_LONG", signal, [candidate("EOD", signal)])

    current = ts(9, 31)
    engine.process_completed_minute(
        current, {"EOD": bar(current, 100.0, 100.5, 99.9, 100.4)}
    )
    current += timedelta(minutes=1)
    engine.process_completed_minute(
        current, {"EOD": bar(current, 100.5, 100.7, 100.1, 100.5)}
    )
    # Trigger was 100.5; the S+2 open is the modeled fill.  Keep every later
    # completed bar strictly inside its 99.45/103.00 brackets.
    current += timedelta(minutes=1)
    while current <= ts(15, 30):
        close = 100.7 if current == ts(15, 30) else 100.6
        engine.process_completed_minute(
            current, {"EOD": bar(current, 100.6, 100.8, 100.2, close)}
        )
        current += timedelta(minutes=1)

    result = record(engine, "EOD")
    assert result["status"] == "SQUARE_OFF"
    assert result["exit_time"] == ts(15, 30).isoformat()
    assert result["exit_price"] == 100.7
    assert result["exit_reason"] == "SQUARE_OFF"
    assert engine.state_summary()["active_portfolio_symbols"] == []


def test_intervention_closes_only_from_supplied_completed_bar_and_is_replay_safe() -> None:
    signal = ts(9, 30)
    engine = PaperEngine()
    engine.register_candidates("09:30_LONG", signal, [candidate("KILL", signal)])
    engine.process_completed_minute(
        ts(9, 31), {"KILL": bar(ts(9, 31), 100.0, 100.5, 99.9, 100.4)}
    )
    engine.process_completed_minute(
        ts(9, 32), {"KILL": bar(ts(9, 32), 100.5, 100.7, 100.1, 100.5)}
    )

    with pytest.raises(PaperEngineError):
        engine.terminate_for_intervention(ts(9, 33), {}, "KILL_SWITCH")
    assert record(engine, "KILL")["status"] == "FILLED_OPEN"

    completed = {"KILL": bar(ts(9, 33), 100.6, 100.8, 100.2, 100.7)}
    events = engine.terminate_for_intervention(ts(9, 33), completed, "KILL_SWITCH")
    result = record(engine, "KILL")
    assert result["status"] == "INTERVENTION_CLOSED"
    assert result["exit_price"] == 100.7
    assert result["exit_reason"] == "INTERVENTION:KILL_SWITCH"
    assert any(event.scope == "PORTFOLIO" for event in events)
    assert engine.terminate_for_intervention(ts(9, 33), completed, "KILL_SWITCH") == []
    assert PaperEngine.from_checkpoint_json(engine.checkpoint_json()).records() == engine.records()


def test_global_same_timestamp_order_capacity_and_conservative_no_backfill() -> None:
    base = PaperEngine().config
    setups = tuple(
        replace(item, max_entries=1) if item.setup_id == "09:30_SHORT" else item
        for item in base.setups
    )
    constrained_config = PaperEngineConfig(
        setups=setups,
        entry_policies=base.entry_policies,
        portfolio_policy=replace(
            base.portfolio_policy,
            capital_rs=10_000.0,
            max_concurrent_positions=1,
        ),
        setup_book_sha256=base.setup_book_sha256,
        strategy_fingerprint=base.strategy_fingerprint,
    )
    engine = PaperEngine(constrained_config)
    signal = ts(9, 30)
    long = candidate("LONGA", signal, move=1.0)
    short_a = candidate("SHORTA", signal, side="SHORT", move=-0.5, volume_ratio=3.0)
    short_b = candidate("SHORTB", signal, side="SHORT", move=-0.5, volume_ratio=2.0)
    engine.register_candidates("09:30_LONG", signal, [long])
    engine.register_candidates("09:30_SHORT", signal, [short_b, short_a])

    engine.process_completed_minute(
        ts(9, 31),
        {
            "LONGA": bar(ts(9, 31), 100.0, 101.0, 99.9, 100.8),
            "SHORTA": bar(ts(9, 31), 100.0, 100.1, 99.0, 99.2),
            "SHORTB": bar(ts(9, 31), 100.0, 100.1, 99.0, 99.2),
        },
    )
    # Same event/signal time is ordered by setup_id, so LONG reserves the one
    # portfolio slot before SHORT.  Only SHORT rank 1 was locally proposed.
    assert record(engine, "LONGA")["portfolio_decision"] == "ACCEPTED"
    assert record(engine, "SHORTA")["status"] == "PORTFOLIO_REJECTED"
    assert record(engine, "SHORTA")["portfolio_reject_reason"] == (
        "CAPITAL_MARGIN_OR_CONCURRENCY_LIMIT"
    )
    assert record(engine, "SHORTB")["status"] == "CONFIRMED_WAITING_CAP"

    for minute_index in range(2, 6):
        stamp = signal + timedelta(minutes=minute_index)
        payload = {
            "LONGA": bar(stamp, 100.5, 100.9, 100.1, 100.5),
            "SHORTB": bar(stamp, 99.2, 99.4, 98.8, 99.0),
        }
        if minute_index == 2:
            # The rejected rank-1 shadow fills and stops. Its unconstrained fill
            # consumes the local cap permanently, so rank 2 is never backfilled.
            payload["SHORTA"] = bar(stamp, 98.8, 100.2, 98.0, 99.0)
        engine.process_completed_minute(stamp, payload)

    assert record(engine, "SHORTA")["unconstrained_status"] == "STOPPED"
    assert record(engine, "SHORTB")["status"] == "WINDOW_EXPIRED"
    assert record(engine, "SHORTB")["portfolio_decision"] == "NOT_APPLICABLE"


def test_one_symbol_pending_or_open_is_rejected_across_setup_legs() -> None:
    engine = PaperEngine()
    early = ts(9, 25)
    long = candidate("DUP", early, signal_close=98.0, move=0.8)
    engine.register_candidates("09:25_LONG", early, [long])
    engine.process_completed_minute(
        ts(9, 26), {"DUP": bar(ts(9, 26), 98.5, 100.0, 98.0, 99.8)}
    )
    engine.process_completed_minute(
        ts(9, 27), {"DUP": bar(ts(9, 27), 100.0, 100.5, 99.8, 100.2)}
    )
    for minute in (28, 29, 30):
        engine.process_completed_minute(
            ts(9, minute), {"DUP": bar(ts(9, minute), 100.4, 100.8, 100.1, 100.4)}
        )
    assert record(engine, "DUP")["status"] == "FILLED_OPEN"

    later = ts(9, 30)
    short = candidate("DUP", later, side="SHORT", signal_close=102.0, move=-0.5)
    short = replace(short, five_min_low=98.0, five_min_high=104.0)
    engine.register_candidates("09:30_SHORT", later, [short])
    engine.process_completed_minute(
        ts(9, 31), {"DUP": bar(ts(9, 31), 100.7, 100.8, 100.2, 100.3)}
    )

    rows = [item for item in engine.records() if item["symbol"] == "DUP"]
    early_row = next(item for item in rows if item["setup_id"] == "09:25_LONG")
    later_row = next(item for item in rows if item["setup_id"] == "09:30_SHORT")
    assert early_row["status"] == "FILLED_OPEN"
    assert later_row["status"] == "DUPLICATE_REJECTED"
    assert later_row["portfolio_reject_reason"] == "DUPLICATE_SYMBOL_PENDING_OR_OPEN"


def test_intervention_does_not_require_or_fabricate_close_for_rejected_shadow() -> None:
    base = PaperEngine().config
    engine = PaperEngine(
        PaperEngineConfig(
            setups=base.setups,
            entry_policies=base.entry_policies,
            portfolio_policy=replace(
                base.portfolio_policy,
                capital_rs=10_000.0,
                max_concurrent_positions=1,
            ),
            setup_book_sha256=base.setup_book_sha256,
            strategy_fingerprint=base.strategy_fingerprint,
        )
    )
    signal = ts(9, 30)
    engine.register_candidates(
        "09:30_LONG", signal, [candidate("HOLDER", signal, move=1.0)]
    )
    engine.register_candidates(
        "09:30_SHORT",
        signal,
        [candidate("SHADOW", signal, side="SHORT", move=-0.5)],
    )
    engine.process_completed_minute(
        ts(9, 31),
        {
            "HOLDER": bar(ts(9, 31), 100.0, 101.0, 99.9, 100.8),
            "SHADOW": bar(ts(9, 31), 100.0, 100.1, 99.0, 99.2),
        },
    )
    engine.process_completed_minute(
        ts(9, 32),
        {
            "HOLDER": bar(ts(9, 32), 100.5, 100.9, 100.1, 100.5),
            "SHADOW": bar(ts(9, 32), 98.8, 99.4, 98.0, 99.0),
        },
    )
    assert record(engine, "SHADOW")["unconstrained_status"] == "FILLED_OPEN"
    assert record(engine, "SHADOW")["status"] == "PORTFOLIO_REJECTED"

    # HOLDER is only pending, and SHADOW is not a portfolio position. No close
    # bar is needed and no shadow exit price may be invented.
    engine.terminate_for_intervention(ts(9, 33), {}, "REVOKED")
    shadow = record(engine, "SHADOW")
    assert shadow["status"] == "PORTFOLIO_REJECTED"
    assert shadow["unconstrained_status"] == "INTERVENTION_CANCELLED"
    assert shadow["exit_price"] is None
