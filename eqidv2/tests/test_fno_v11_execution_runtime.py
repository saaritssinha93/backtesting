from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

import fno_v11_execution_runtime as runtime_hooks
import fno_v8_windowed_1m_entry_backtest as engine


IST = "Asia/Kolkata"
SIGNAL_TIME = pd.Timestamp("2026-08-18 09:30", tz=IST)


def _setup(*, side: str = "LONG") -> engine.V8Setup:
    return engine.V8Setup(
        signal_end="09:30",
        side=side,
        max_entries=4,
        picker="max_liquidity",
        price_change_pct=0.20,
        oi_change_pct=0.10,
        volume_ratio=1.0,
        body_ratio=0.0,
        max_wick_ratio=1.0,
        min_traded_value=0.0,
        stop_pct=1.0,
        target_pct=10.0,
    )


def _candidate(symbol: str = "TEST") -> engine.CandidateInput:
    return engine.CandidateInput(
        symbol=symbol,
        signal_time=SIGNAL_TIME,
        five_min_open=99.5,
        five_min_high=101.0,
        five_min_low=99.0,
        five_min_close=100.0,
        price_change_pct=1.0,
        oi_change_pct=1.0,
        volume_ratio=2.0,
        traded_value=10_000_000.0,
        tick_size=0.05,
    )


def _bar(
    minute: int,
    *,
    open_: float = 100.0,
    high: float = 100.5,
    low: float = 99.5,
    close: float = 100.0,
) -> engine.MinuteBar:
    return engine.MinuteBar(
        timestamp=SIGNAL_TIME + pd.Timedelta(minutes=minute),
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=1_000.0,
    )


def _open_runtime(
    *,
    side: str = "LONG",
    symbol: str = "TEST",
) -> engine._CandidateRuntime:
    entry = 100.0
    result = engine._CandidateRuntime(
        candidate=_candidate(symbol),
        state=engine.SignalState.FILLED_OPEN,
        entry_minute=2,
        entry_time=SIGNAL_TIME + pd.Timedelta(minutes=2),
        entry_price=entry,
        stop_price=99.0 if side == "LONG" else 101.0,
        target_price=110.0 if side == "LONG" else 90.0,
        trigger=entry,
    )
    return result


def test_runtime_spec_neutral_and_single_mechanism_contract() -> None:
    neutral = runtime_hooks.RuntimeSpec()
    neutral.validate()
    assert neutral.is_neutral
    assert neutral.active_mechanisms == ()

    entry = runtime_hooks.RuntimeSpec(
        entry_setup_id="09:30_SHORT",
        entry_not_before_minute=3,
    )
    entry.validate()
    assert entry.active_mechanisms == ("ENTRY_NOT_BEFORE",)

    exit_spec = runtime_hooks.RuntimeSpec(
        exit_rule="BREAK_EVEN_NEXT_BAR",
        exit_activation_r=1.0,
    )
    exit_spec.validate()
    assert exit_spec.active_mechanisms == ("EXIT_RULE",)

    portfolio = runtime_hooks.RuntimeSpec(same_side_symbol_limit=2)
    portfolio.validate()
    assert portfolio.active_mechanisms == ("PORTFOLIO_SYMBOL_LIMIT",)


@pytest.mark.parametrize(
    "spec, message",
    [
        (runtime_hooks.RuntimeSpec(entry_setup_id="09:30_SHORT"), "together"),
        (runtime_hooks.RuntimeSpec(entry_not_before_minute=3), "together"),
        (
            runtime_hooks.RuntimeSpec(
                entry_setup_id="09:30_SHORT", entry_not_before_minute=True
            ),
            "integer",
        ),
        (
            runtime_hooks.RuntimeSpec(
                entry_setup_id="09:30_SHORT", entry_not_before_minute=1
            ),
            r"\[2, 5\]",
        ),
        (runtime_hooks.RuntimeSpec(exit_rule="UNKNOWN"), "unsupported"),
        (runtime_hooks.RuntimeSpec(exit_activation_r=1.0), "neutral"),
        (
            runtime_hooks.RuntimeSpec(exit_rule="BREAK_EVEN_NEXT_BAR"),
            "finite R threshold",
        ),
        (
            runtime_hooks.RuntimeSpec(
                exit_rule="BREAK_EVEN_NEXT_BAR", exit_activation_r=float("nan")
            ),
            "finite R threshold",
        ),
        (
            runtime_hooks.RuntimeSpec(
                exit_rule="BREAK_EVEN_NEXT_BAR", exit_activation_r=0.0
            ),
            "positive",
        ),
        (runtime_hooks.RuntimeSpec(same_side_symbol_limit=True), "integer"),
        (runtime_hooks.RuntimeSpec(same_side_symbol_limit=3), "limits 1 or 2"),
        (
            runtime_hooks.RuntimeSpec(
                entry_setup_id="09:30_SHORT",
                entry_not_before_minute=3,
                same_side_symbol_limit=2,
            ),
            "one mechanism",
        ),
    ],
)
def test_runtime_spec_rejects_invalid_or_combined_mechanisms(
    spec: runtime_hooks.RuntimeSpec,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        spec.validate()


def test_earliest_s3_suppresses_s2_touch_but_allows_same_touch_at_s3() -> None:
    setup = _setup(side="SHORT")
    trade = engine._CandidateRuntime(candidate=_candidate(), trigger=99.50)
    policy = engine.EntryPolicy(buffer_bps=0.0, slippage_bps=0.0)
    spec = runtime_hooks.RuntimeSpec(
        entry_setup_id=setup.setup_id,
        entry_not_before_minute=3,
    )

    original_entry_fill = engine._entry_fill
    with runtime_hooks.installed_runtime_hooks(spec):
        early = engine._entry_fill(
            setup,
            trade,
            _bar(2, open_=100.0, high=100.1, low=99.0, close=99.4),
            policy,
        )
        allowed = engine._entry_fill(
            setup,
            trade,
            _bar(3, open_=100.0, high=100.1, low=99.0, close=99.4),
            policy,
        )

        assert early is None
        assert allowed is not None
        assert allowed[0] == pytest.approx(99.50)
        assert allowed[1] is False
        assert trade._v11_entry_not_before_minute == 3
        assert trade._v11_early_fill_checks_skipped == 1
        assert trade._v11_early_touch_observed is True

    assert engine._entry_fill is original_entry_fill


@pytest.mark.parametrize(
    "side, entry_bar, arming_bar, exit_bar",
    [
        (
            "LONG",
            _bar(2, high=101.50, low=99.50),
            _bar(3, high=101.20, low=99.50),
            _bar(4, open_=100.10, high=100.20, low=99.90, close=100.0),
        ),
        (
            "SHORT",
            _bar(2, high=100.50, low=98.50),
            _bar(3, high=100.50, low=98.80),
            _bar(4, open_=99.90, high=100.10, low=99.80, close=100.0),
        ),
    ],
)
def test_break_even_ignores_entry_bar_and_activates_on_next_bar(
    side: str,
    entry_bar: engine.MinuteBar,
    arming_bar: engine.MinuteBar,
    exit_bar: engine.MinuteBar,
) -> None:
    setup = _setup(side=side)
    trade = _open_runtime(side=side)
    initial_stop = float(trade.stop_price)
    spec = runtime_hooks.RuntimeSpec(
        exit_rule="BREAK_EVEN_NEXT_BAR",
        exit_activation_r=1.0,
    )

    with runtime_hooks.installed_runtime_hooks(spec):
        # Even though the entry candle itself reaches more than 1R, it cannot
        # arm a close-derived stop because the position was not open at its start.
        assert (
            engine._exit_on_bar(
                setup,
                trade,
                entry_bar,
                position_open_at_bar_start=False,
            )
            is None
        )
        assert not hasattr(trade, "_v11_pending_stop")
        assert float(trade.stop_price) == pytest.approx(initial_stop)

        # The first fully observable post-entry candle arms BE but the stop
        # remains unchanged for that candle.
        assert engine._exit_on_bar(setup, trade, arming_bar) is None
        assert float(trade.stop_price) == pytest.approx(initial_stop)
        assert float(trade._v11_pending_stop) == pytest.approx(100.0)
        assert trade._v11_dynamic_stop_armed_at == arming_bar.ts

        # Only the following candle can execute the newly active stop.
        result = engine._exit_on_bar(setup, trade, exit_bar)
        assert result == ("STOP_BREAK_EVEN_AFTER_1R", 100.0)
        assert float(trade.stop_price) == pytest.approx(100.0)
        assert trade._v11_dynamic_stop_activated_at == exit_bar.ts
        assert trade._v11_dynamic_stop_activation_count == 1


def test_dynamic_stop_audit_preserves_initial_stop_and_exposes_final_stop() -> None:
    setup = _setup(side="LONG")
    trade = _open_runtime(side="LONG")
    initial_stop = float(trade.stop_price)
    spec = runtime_hooks.RuntimeSpec(
        exit_rule="BREAK_EVEN_NEXT_BAR",
        exit_activation_r=1.0,
    )

    with runtime_hooks.installed_runtime_hooks(spec):
        assert engine._exit_on_bar(
            setup, trade, _bar(3, high=101.20, low=99.50)
        ) is None
        assert engine._exit_on_bar(
            setup,
            trade,
            _bar(4, open_=100.20, high=100.40, low=100.10, close=100.20),
        ) is None
        assert float(trade.stop_price) == pytest.approx(100.0)

        record = engine._audit_record(setup, trade)

        assert record["stop_price"] == pytest.approx(initial_stop)
        assert record["v11_final_active_stop_price"] == pytest.approx(100.0)
        assert record["v11_dynamic_stop_activation_count"] == 1
        assert record["v11_dynamic_stop_active_at_terminal"] is True
        assert float(trade.stop_price) == pytest.approx(100.0)


def _event(
    ts: str,
    *,
    before: engine.SignalState,
    after: engine.SignalState,
    symbol: str,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "event_ts": pd.Timestamp(ts, tz=IST),
        "state_before": before.value,
        "state_after": after.value,
        "reason": "TEST_EVENT",
    }


def _portfolio_row(
    candidate_id: str,
    *,
    symbol: str,
    side: str,
    reserve_at: str,
    release_at: str | None = None,
    frozen_rank: int,
) -> dict[str, Any]:
    events = [
        _event(
            reserve_at,
            before=engine.SignalState.CONFIRMED_WAITING_CAP,
            after=engine.SignalState.PENDING_STOP,
            symbol=symbol,
        )
    ]
    if release_at is not None:
        events.append(
            _event(
                release_at,
                before=engine.SignalState.FILLED_OPEN,
                after=engine.SignalState.TARGETED,
                symbol=symbol,
            )
        )
    return {
        "candidate_id": candidate_id,
        "signal_time": SIGNAL_TIME,
        "setup_id": f"09:30_{side}",
        "frozen_rank": frozen_rank,
        "symbol": symbol,
        "side": side,
        "status": engine.SignalState.TARGETED.value,
        "reason": "TARGET",
        "filled": True,
        "events": events,
        "event_count": len(events),
        "net_return_pct": 1.0,
    }


def test_same_side_symbol_limit_two_enforces_duplicates_capacity_and_release_order() -> None:
    audit = pd.DataFrame(
        [
            _portfolio_row(
                "A",
                symbol="XYZ",
                side="LONG",
                reserve_at="2026-08-18 09:31",
                release_at="2026-08-18 09:35",
                frozen_rank=1,
            ),
            _portfolio_row(
                "B",
                symbol="XYZ",
                side="LONG",
                reserve_at="2026-08-18 09:32",
                release_at="2026-08-18 09:35",
                frozen_rank=2,
            ),
            _portfolio_row(
                "THIRD",
                symbol="XYZ",
                side="LONG",
                reserve_at="2026-08-18 09:33",
                frozen_rank=3,
            ),
            _portfolio_row(
                "OPPOSITE",
                symbol="XYZ",
                side="SHORT",
                reserve_at="2026-08-18 09:34",
                frozen_rank=4,
            ),
            _portfolio_row(
                "CAPACITY",
                symbol="ABC",
                side="LONG",
                reserve_at="2026-08-18 09:34",
                frozen_rank=5,
            ),
            # RELEASE actions sort before RESERVE at the same timestamp, so
            # this opposite-side trade is eligible once both longs are gone.
            _portfolio_row(
                "AFTER_RELEASE",
                symbol="XYZ",
                side="SHORT",
                reserve_at="2026-08-18 09:35",
                frozen_rank=6,
            ),
        ]
    )
    policy = engine.PortfolioPolicy(
        capital_rs=20_000.0,
        margin_per_entry_rs=10_000.0,
        target_exposure_per_entry_rs=50_000.0,
        max_concurrent_positions=2,
    )

    result = runtime_hooks.apply_same_side_symbol_limit(
        audit,
        policy,
        same_side_limit=2,
    ).set_index("candidate_id")

    assert result.loc["A", "portfolio_decision"] == "ACCEPTED"
    assert result.loc["B", "portfolio_decision"] == "ACCEPTED"
    assert result.loc["THIRD", "portfolio_reject_reason"] == (
        "DUPLICATE_SYMBOL_SAME_SIDE_LIMIT_2"
    )
    assert result.loc["OPPOSITE", "portfolio_reject_reason"] == (
        "DUPLICATE_SYMBOL_OPPOSITE_SIDE_PENDING_OR_OPEN"
    )
    assert result.loc["CAPACITY", "portfolio_reject_reason"] == (
        "CAPITAL_MARGIN_OR_CONCURRENCY_LIMIT"
    )
    assert result.loc["AFTER_RELEASE", "portfolio_decision"] == "ACCEPTED"
    assert int(result.loc["AFTER_RELEASE", "portfolio_active_at_reservation"]) == 1
    assert bool(result["v11_opposite_side_same_symbol_prohibited"].all())
    assert (result["v11_same_side_symbol_limit"] == 2).all()
    assert (result["v11_max_symbol_target_exposure_rs"] == 100_000.0).all()

    for candidate_id in ("THIRD", "OPPOSITE", "CAPACITY"):
        row = result.loc[candidate_id]
        assert row["portfolio_decision"] == "REJECTED"
        assert not bool(row["filled"])
        assert row["status"] in {
            engine.SignalState.DUPLICATE_REJECTED.value,
            engine.SignalState.PORTFOLIO_REJECTED.value,
        }
        assert row["events"][-1]["state_after"] == row["status"]


def test_same_side_symbol_limit_rejects_unsupported_parent_policies() -> None:
    empty = pd.DataFrame()
    with pytest.raises(ValueError, match="pending_reserves_margin"):
        runtime_hooks.apply_same_side_symbol_limit(
            empty,
            engine.PortfolioPolicy(pending_reserves_margin=False),
        )
    with pytest.raises(ValueError, match="one-position flag"):
        runtime_hooks.apply_same_side_symbol_limit(
            empty,
            engine.PortfolioPolicy(one_position_per_symbol=False),
        )
    with pytest.raises(ValueError, match="same_side_limit=2"):
        runtime_hooks.apply_same_side_symbol_limit(
            empty,
            engine.PortfolioPolicy(),
            same_side_limit=1,
        )
