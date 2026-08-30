from __future__ import annotations

from dataclasses import asdict, replace
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import fno_v8_combined_best_per_leg_backtest as combined
import fno_v8_combined_paper_config as paper_config
import fno_v8_windowed_1m_entry_backtest as historical
from fno_v8_combined_paper_engine import PaperEngine, PaperEngineConfig


IST = ZoneInfo("Asia/Kolkata")


def setup(setup_id: str) -> historical.V8Setup:
    return next(item for item in combined.COMBINED_SETUPS if item.setup_id == setup_id)


def policy(setup_id: str) -> historical.EntryPolicy:
    resolved = paper_config.entry_policy_for_setup(
        next(item for item in paper_config.ACTIVE_SETUPS if item.setup_id == setup_id)
    )
    return historical.EntryPolicy(**asdict(resolved))


def candidate(
    symbol: str,
    signal: datetime,
    *,
    side: str,
    move: float,
    signal_close: float = 100.0,
    oi_change: float = 1.2,
    volume_ratio: float = 3.5,
    traded_value: float = 30_000_000.0,
) -> historical.CandidateInput:
    long_side = side == "LONG"
    return historical.CandidateInput(
        symbol=symbol,
        signal_time=pd.Timestamp(signal),
        five_min_open=signal_close - (1.0 if long_side else -1.0),
        five_min_high=signal_close + 2.0,
        five_min_low=signal_close - 2.0,
        five_min_close=signal_close,
        price_change_pct=move,
        oi_change_pct=oi_change,
        volume_ratio=volume_ratio,
        traded_value=traded_value,
        tick_size=0.05,
        five_min_volume=100_000.0,
        ema9=102.0 if long_side else 98.0,
        ema20=101.0 if long_side else 99.0,
        ema50=100.0,
        oi=101.2,
        prev_oi=100.0,
    )


def minute(
    timestamp: datetime,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float = 1_000.0,
) -> historical.MinuteBar:
    return historical.MinuteBar(
        timestamp=pd.Timestamp(timestamp),
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
    )


def run_incremental(
    setup_id: str,
    signal: datetime,
    candidates: list[historical.CandidateInput],
    bars_by_symbol: dict[str, list[historical.MinuteBar]],
) -> list[dict]:
    engine = PaperEngine()
    engine.register_candidates(setup_id, signal, candidates)
    timestamps = sorted({bar.ts.to_pydatetime() for bars in bars_by_symbol.values() for bar in bars})
    for timestamp in timestamps:
        batch = {
            symbol: next(bar for bar in bars if bar.ts.to_pydatetime() == timestamp)
            for symbol, bars in bars_by_symbol.items()
            if any(bar.ts.to_pydatetime() == timestamp for bar in bars)
        }
        engine.process_completed_minute(timestamp, batch)
    return engine.records()


def normalize_timestamp(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).isoformat()


def assert_row_parity(paper: dict, batch: pd.Series) -> None:
    assert paper["status"] == batch["status"]
    assert paper["confirmation_minute"] == (
        None if pd.isna(batch["confirmation_minute"]) else int(batch["confirmation_minute"])
    )
    assert paper["entry_minute"] == (
        None if pd.isna(batch["entry_minute"]) else int(batch["entry_minute"])
    )
    assert normalize_timestamp(paper["confirmation_time"]) == normalize_timestamp(
        batch["confirmation_time"]
    )
    assert normalize_timestamp(paper["entry_time"]) == normalize_timestamp(batch["entry_time"])
    assert normalize_timestamp(paper["exit_time"]) == normalize_timestamp(batch["exit_time"])
    for column in (
        "trigger",
        "entry_price",
        "stop_price",
        "target_price",
        "exit_price",
        "gross_return_pct",
        "net_return_pct",
    ):
        paper_value = paper[column]
        batch_value = batch[column]
        if paper_value is None or pd.isna(batch_value):
            assert paper_value is None and pd.isna(batch_value)
        else:
            assert float(paper_value) == pytest.approx(float(batch_value), abs=1e-12)
    assert paper["gap_fill"] == bool(batch["gap_fill"])
    assert paper["intrabar_trigger_fill"] == bool(batch["intrabar_trigger_fill"])
    assert paper["ambiguous_entry_bar"] == bool(batch["ambiguous_entry_bar"])
    assert paper["exit_at_bar_open"] == bool(batch["exit_at_bar_open"])
    assert paper["exit_reason"] == str(batch["exit_reason"] or "")


def test_delayed_short_buffer_gap_and_entry_bar_stop_first_match_batch_engine() -> None:
    signal = datetime(2026, 8, 21, 9, 25, tzinfo=IST)
    selected = setup("09:25_SHORT")
    selected_policy = policy("09:25_SHORT")
    item = candidate("SHORTY", signal, side="SHORT", move=-0.4)
    bars = {
        "SHORTY": [
            minute(signal + timedelta(minutes=1), 100.0, 100.2, 99.8, 100.0),
            minute(signal + timedelta(minutes=2), 100.0, 100.1, 99.0, 99.2),
            minute(signal + timedelta(minutes=3), 98.8, 99.4, 95.0, 97.0),
        ]
    }

    batch = historical.simulate_setup_window(selected, [item], bars, selected_policy)
    paper = run_incremental(selected.setup_id, signal, [item], bars)

    assert len(batch) == len(paper) == 1
    assert_row_parity(paper[0], batch.iloc[0])
    assert paper[0]["confirmation_minute"] == 2
    assert paper[0]["trigger"] == 98.95  # 2 bps then floor to the cash tick
    assert paper[0]["entry_price"] == 98.8  # adverse-open gap model
    assert paper[0]["status"] == "STOPPED"


def test_frozen_rank_cap_cancel_and_reassignment_match_batch_engine() -> None:
    signal = datetime(2026, 8, 21, 9, 30, tzinfo=IST)
    selected = setup("09:30_LONG")
    selected_policy = policy("09:30_LONG")
    aaa = candidate("AAA", signal, side="LONG", move=1.0)
    bbb = candidate("BBB", signal, side="LONG", move=0.8)
    bars = {
        "AAA": [
            minute(signal + timedelta(minutes=1), 100.0, 101.0, 99.9, 100.8),
            minute(signal + timedelta(minutes=2), 100.5, 100.9, 99.0, 99.5),
        ],
        "BBB": [
            minute(signal + timedelta(minutes=1), 100.0, 101.0, 99.9, 100.8),
            minute(signal + timedelta(minutes=2), 100.5, 100.9, 100.1, 100.6),
            minute(signal + timedelta(minutes=3), 101.2, 104.0, 100.0, 102.0),
        ],
    }

    batch = historical.simulate_setup_window(selected, [bbb, aaa], bars, selected_policy)
    paper = run_incremental(selected.setup_id, signal, [bbb, aaa], bars)
    paper_by_symbol = {item["symbol"]: item for item in paper}
    batch_by_symbol = {str(row["symbol"]): row for _, row in batch.iterrows()}

    assert list(sorted(paper_by_symbol)) == list(sorted(batch_by_symbol)) == ["AAA", "BBB"]
    for symbol in ("AAA", "BBB"):
        assert_row_parity(paper_by_symbol[symbol], batch_by_symbol[symbol])
    assert paper_by_symbol["AAA"]["frozen_rank"] == 1
    assert paper_by_symbol["AAA"]["status"] == "POSTCONF_CANCELLED"
    assert paper_by_symbol["BBB"]["status"] == "STOPPED"
    assert paper_by_symbol["BBB"]["entry_minute"] == 3


def test_global_capacity_order_and_no_backfill_match_batch_overlay() -> None:
    signal = datetime(2026, 8, 21, 9, 30, tzinfo=IST)
    long_setup = setup("09:30_LONG")
    short_setup = replace(setup("09:30_SHORT"), max_entries=1)
    long_item = candidate("LONGA", signal, side="LONG", move=1.0)
    short_a = candidate("SHORTA", signal, side="SHORT", move=-0.5, volume_ratio=3.0)
    short_b = candidate("SHORTB", signal, side="SHORT", move=-0.5, volume_ratio=2.0)
    long_bars = {
        "LONGA": [
            minute(signal + timedelta(minutes=1), 100.0, 101.0, 99.9, 100.8),
            *[
                minute(signal + timedelta(minutes=index), 100.5, 100.9, 100.1, 100.5)
                for index in range(2, 6)
            ],
        ]
    }
    short_bars = {
        "SHORTA": [
            minute(signal + timedelta(minutes=1), 100.0, 100.1, 99.0, 99.2),
            minute(signal + timedelta(minutes=2), 98.8, 100.2, 98.0, 99.0),
        ],
        "SHORTB": [
            minute(signal + timedelta(minutes=1), 100.0, 100.1, 99.0, 99.2),
            *[
                minute(signal + timedelta(minutes=index), 99.2, 99.4, 98.8, 99.0)
                for index in range(2, 6)
            ],
        ],
    }

    long_audit = historical.simulate_setup_window(
        long_setup, [long_item], long_bars, policy("09:30_LONG")
    )
    short_audit = historical.simulate_setup_window(
        short_setup,
        [short_b, short_a],
        short_bars,
        policy("09:30_SHORT"),
    )
    batch = pd.DataFrame(
        long_audit.to_dict("records") + short_audit.to_dict("records"),
        columns=long_audit.columns,
    )
    batch["frozen_rank"] = batch["symbol"].map(
        {"LONGA": 1, "SHORTA": 1, "SHORTB": 2}
    )
    batch["picker"] = batch["setup_id"].map(
        {"09:30_LONG": "max_move", "09:30_SHORT": "max_volume"}
    )
    batch["picker_value"] = batch.apply(
        lambda row: (
            abs(float(row["price_change_pct"]))
            if row["setup_id"] == "09:30_LONG"
            else float(row["volume_ratio"])
        ),
        axis=1,
    )
    constrained = historical.apply_global_portfolio_constraints(
        batch,
        historical.PortfolioPolicy(
            capital_rs=10_000.0,
            margin_per_entry_rs=10_000.0,
            target_exposure_per_entry_rs=50_000.0,
            max_concurrent_positions=1,
            pending_reserves_margin=True,
            one_position_per_symbol=True,
        ),
    ).set_index("symbol")

    base = PaperEngine().config
    custom_setups = tuple(
        replace(item, max_entries=1) if item.setup_id == "09:30_SHORT" else item
        for item in base.setups
    )
    incremental = PaperEngine(
        PaperEngineConfig(
            setups=custom_setups,
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
    incremental.register_candidates("09:30_LONG", signal, [long_item])
    incremental.register_candidates("09:30_SHORT", signal, [short_b, short_a])
    for index in range(1, 6):
        stamp = signal + timedelta(minutes=index)
        payload = {
            symbol: next(item for item in bars if item.ts.to_pydatetime() == stamp)
            for source in (long_bars, short_bars)
            for symbol, bars in source.items()
            if any(item.ts.to_pydatetime() == stamp for item in bars)
        }
        incremental.process_completed_minute(stamp, payload)
    paper = {item["symbol"]: item for item in incremental.records()}

    for symbol in ("LONGA", "SHORTA", "SHORTB"):
        assert paper[symbol]["status"] == constrained.loc[symbol, "status"]
        assert paper[symbol]["portfolio_decision"] == constrained.loc[
            symbol, "portfolio_decision"
        ]
        assert paper[symbol]["portfolio_reject_reason"] == constrained.loc[
            symbol, "portfolio_reject_reason"
        ]
        assert paper[symbol]["unconstrained_status"] == constrained.loc[
            symbol, "unconstrained_status"
        ]
        paper_transitions = [
            (item["state_before"], item["state_after"], item["reason"])
            for item in paper[symbol]["events"]
        ]
        batch_transitions = [
            (item["state_before"], item["state_after"], item["reason"])
            for item in constrained.loc[symbol, "events"]
        ]
        assert paper_transitions == batch_transitions
