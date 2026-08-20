from __future__ import annotations

import ast
import dataclasses
import hashlib
import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd
import pytest

import fno_oi_common as common
import fno_v8_windowed_1m_entry_backtest as v8


IST = "Asia/Kolkata"
SIGNAL_TIME = pd.Timestamp("2026-08-18 09:30", tz=IST)

SETUP_FIELDS = (
    "signal_end",
    "side",
    "max_entries",
    "picker",
    "price_change_pct",
    "oi_change_pct",
    "volume_ratio",
    "body_ratio",
    "max_wick_ratio",
    "min_traded_value",
    "stop_pct",
    "target_pct",
    "entry_conf_minute",
    "entry_buffer_bps",
    "entry_midpoint",
    "entry_clv",
)

_INHERIT = "INHERIT"

# This is a literal, runtime-independent copy of the ten five-minute setup
# legs.  The test deliberately does not import a V6 or V7 strategy module to
# obtain the expected values.
#
# Six legs still carry their original V6-lineage values and inherit the run's
# global entry policy.  Four legs (09:25 LONG/SHORT, 09:30 SHORT, 09:40 SHORT)
# were retuned on 2026-08-19 from the setup-parameter sweep over
# 2026-05-27..2026-08-17 and pin their own entry seam.
EXPECTED_SETUP_BOOK = [
    ("09:25", "LONG", 4, "max_move", 0.30, 0.10, 3.0, 0.0, 0.5, 0.0, 0.40, 1.0, 3, 0.0, False, None),
    ("09:25", "SHORT", 4, "max_move", 0.20, 0.10, 1.5, 0.6, 0.6, 25_000_000.0, 0.50, 3.0, 3, 2.0, False, None),
    ("09:30", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.5, 0.5, 0.0, 1.00, 2.5, None, None, None, _INHERIT),
    ("09:30", "SHORT", 4, "max_volume", 0.20, 1.00, 1.0, 0.45, 0.3, 25_000_000.0, 1.00, 4.0, 3, 0.0, True, 0.50),
    ("09:35", "LONG", 1, "max_liquidity", 0.20, 0.10, 1.0, 0.6, 0.5, 0.0, 1.00, 2.5, None, None, None, _INHERIT),
    ("09:35", "SHORT", 2, "max_liquidity", 0.50, 1.00, 1.0, 0.4, 0.5, 0.0, 1.00, 3.0, None, None, None, _INHERIT),
    ("09:40", "LONG", 1, "max_liquidity", 0.20, 0.10, 2.0, 0.5, 0.5, 0.0, 0.50, 2.5, None, None, None, _INHERIT),
    ("09:40", "SHORT", 4, "max_volume", 0.20, 0.75, 1.0, 0.0, 0.2, 0.0, 1.00, 4.0, 4, 0.0, False, 0.50),
    ("09:45", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.4, 0.5, 0.0, 1.00, 3.0, None, None, None, _INHERIT),
    ("09:45", "SHORT", 1, "max_volume", 0.20, 0.75, 1.0, 0.4, 0.3, 0.0, 1.00, 2.0, None, None, None, _INHERIT),
]
EXPECTED_SETUP_BOOK_SHA256 = (
    "ed32937129246ca3500bd421a77bebca71c83014a4e2a4eb5cbc318e74016fb6"
)

# Legs that pin their own one-minute entry seam, and what they pin it to.
EXPECTED_ENTRY_OVERRIDES = {
    "09:25_LONG": (3, 0.0, False, None),
    "09:25_SHORT": (3, 2.0, False, None),
    "09:30_SHORT": (3, 0.0, True, 0.50),
    "09:40_SHORT": (4, 0.0, False, 0.50),
}


def _setup(*, side: str = "LONG", max_entries: int = 1) -> v8.V8Setup:
    return v8.V8Setup(
        signal_end="09:30",
        side=side,
        max_entries=max_entries,
        picker="max_liquidity",
        price_change_pct=0.20,
        oi_change_pct=0.10,
        volume_ratio=1.0,
        body_ratio=0.40,
        max_wick_ratio=0.50,
        min_traded_value=0.0,
        stop_pct=1.0,
        target_pct=1.0,
    )


def _policy(
    *,
    buffer_bps: float = 0.0,
    max_confirmation_minute: int = 4,
    **overrides: Any,
) -> v8.EntryPolicy:
    values: dict[str, Any] = dict(
        buffer_bps=buffer_bps,
        max_confirmation_minute=max_confirmation_minute,
        entry_expiry_minute=5,
        close_location_min=None,
        cost_bps=5.0,
        slippage_bps=0.0,
    )
    values.update(overrides)
    return v8.EntryPolicy(**values)


def _candidate(
    symbol: str,
    *,
    traded_value: float = 10_000_000.0,
    side: str = "LONG",
) -> v8.CandidateInput:
    price_change = 1.0 if side == "LONG" else -1.0
    return v8.CandidateInput(
        symbol=symbol,
        signal_time=SIGNAL_TIME,
        five_min_open=99.5,
        five_min_high=101.0,
        five_min_low=99.0,
        five_min_close=100.0,
        price_change_pct=price_change,
        oi_change_pct=1.0,
        volume_ratio=3.0,
        traded_value=traded_value,
        tick_size=0.05,
    )


def _bar(
    minute: int,
    *,
    open_: float = 100.0,
    high: float = 100.4,
    low: float = 99.8,
    close: float = 100.1,
    day_offset: int = 0,
    gap_filled: bool = False,
    opening_snapshot: bool = False,
    provisional_stale: bool = False,
) -> v8.MinuteBar:
    return v8.MinuteBar(
        timestamp=SIGNAL_TIME
        + pd.Timedelta(days=day_offset, minutes=minute),
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=1_000.0,
        gap_filled=gap_filled,
        opening_snapshot=opening_snapshot,
        provisional_stale=provisional_stale,
    )


def _strict_long(minute: int, *, high: float = 100.80) -> v8.MinuteBar:
    return _bar(
        minute,
        open_=99.80,
        high=high,
        low=99.70,
        close=100.60,
    )


def _strict_short(minute: int, *, low: float = 99.20) -> v8.MinuteBar:
    return _bar(
        minute,
        open_=100.20,
        high=100.30,
        low=low,
        close=99.40,
    )


def _quiet(minute: int, *, high: float = 100.70, low: float = 99.30) -> v8.MinuteBar:
    return _bar(
        minute,
        open_=100.10,
        high=high,
        low=low,
        close=100.05,
    )


def _records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, pd.DataFrame):
        return value.to_dict("records")
    if is_dataclass(value):
        value = [value]
    if isinstance(value, Mapping):
        for key in ("audit", "records", "results", "candidates"):
            nested = value.get(key)
            if nested is not None:
                return _records(nested)
        return [dict(value)]
    if isinstance(value, tuple) and value and not isinstance(value[0], (str, bytes)):
        # A simulation may return (audit, portfolio_summary).  The first item
        # remains the required candidate-level audit contract.
        if isinstance(value[0], (pd.DataFrame, list, tuple)):
            return _records(value[0])
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        records: list[dict[str, Any]] = []
        for item in value:
            if is_dataclass(item):
                records.append(asdict(item))
            elif isinstance(item, Mapping):
                records.append(dict(item))
            else:
                records.append(vars(item))
        return records
    raise AssertionError(f"Unsupported simulate_setup_window result: {type(value)!r}")


def _simulate(
    setup: v8.V8Setup,
    candidates: list[v8.CandidateInput],
    bars_by_symbol: Mapping[str, list[v8.MinuteBar]],
    policy: v8.EntryPolicy | None = None,
) -> list[dict[str, Any]]:
    return _records(
        v8.simulate_setup_window(
            setup,
            candidates,
            bars_by_symbol,
            policy or _policy(),
        )
    )


def _record(records: list[dict[str, Any]], symbol: str) -> dict[str, Any]:
    matches = [row for row in records if str(row.get("symbol")) == symbol]
    assert len(matches) == 1, (symbol, records)
    return matches[0]


def _value(record: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in record:
            return record[name]
    raise AssertionError(f"Expected one of {names} in audit fields {sorted(record)}")


def _terminal_text(record: Mapping[str, Any]) -> str:
    return " ".join(
        str(record.get(name, ""))
        for name in ("status", "state", "reason", "terminal_reason", "cancel_reason")
    ).upper()


def _was_filled(record: Mapping[str, Any]) -> bool:
    for name in ("entry_minute", "entry_time", "entry_ts", "entry_price", "fill_price"):
        value = record.get(name)
        if value is not None and not pd.isna(value):
            return True
    return False


def _minute_index(record: Mapping[str, Any], kind: str) -> int:
    direct = record.get(f"{kind}_minute")
    if direct is not None and not pd.isna(direct):
        return int(direct)
    timestamp = _value(record, f"{kind}_time", f"{kind}_ts")
    return int((pd.Timestamp(timestamp) - SIGNAL_TIME).total_seconds() // 60)


def _last_event(record: Mapping[str, Any]) -> Mapping[str, Any]:
    events = _value(record, "events")
    assert isinstance(events, list) and events, events
    assert isinstance(events[-1], Mapping), events[-1]
    return events[-1]


def test_v8_source_has_no_forbidden_strategy_or_legacy_engine_imports() -> None:
    source = Path(v8.__file__).resolve()
    tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)

    forbidden = (
        "fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6",
        "fno_oi_ema_confirm_0925_0930_0935_0940_0945_v7",
        "fno_oi_ema_confirm_sweep",
        "fno_oi_ema_confirm_optimize",
        "fno_oi_ema_confirm_v7_signal_cache",
        "fno_v5_hybrid_backtest",
    )
    assert not any(name.startswith(forbidden) for name in imported), imported


def test_v8_configuration_is_literal_frozen_and_versioned() -> None:
    v8.validate_configuration()
    assert len(v8.ACTIVE_SETUPS) == 10
    observed = [
        {field: getattr(setup, field) for field in SETUP_FIELDS}
        for setup in v8.ACTIVE_SETUPS
    ]
    expected = [dict(zip(SETUP_FIELDS, row, strict=True)) for row in EXPECTED_SETUP_BOOK]
    assert observed == expected
    digest = hashlib.sha256(
        json.dumps(
            observed,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()
    assert digest == EXPECTED_SETUP_BOOK_SHA256
    assert "V8" in v8.STRATEGY_VERSION.upper()


def test_per_setup_entry_overrides_resolve_against_the_global_policy() -> None:
    base = v8.EntryPolicy(
        buffer_bps=2.0,
        max_confirmation_minute=4,
        midpoint_invalidation=True,
        close_location_min=0.75,
    )
    overriding = {
        setup.setup_id for setup in v8.ACTIVE_SETUPS if setup.overrides_entry_policy
    }
    assert overriding == set(EXPECTED_ENTRY_OVERRIDES)

    for setup in v8.ACTIVE_SETUPS:
        resolved = v8.policy_for_setup(setup, base)
        if setup.setup_id not in EXPECTED_ENTRY_OVERRIDES:
            # A leg that overrides nothing must reuse the global policy
            # untouched, so the frozen legs behave identically under every
            # variant to how they did before overrides existed.
            assert resolved is base
            continue
        conf, buffer_bps, midpoint, clv = EXPECTED_ENTRY_OVERRIDES[setup.setup_id]
        assert resolved.max_confirmation_minute == conf
        assert resolved.buffer_bps == buffer_bps
        assert resolved.midpoint_invalidation is midpoint
        assert resolved.close_location_min == clv
        # Run economics are never overridable by a leg.
        assert resolved.cost_bps == base.cost_bps
        assert resolved.slippage_bps == base.slippage_bps
        assert resolved.square_off == base.square_off
        assert resolved.eod_policy == base.eod_policy
        assert resolved.entry_expiry_minute == base.entry_expiry_minute


def test_entry_clv_override_can_switch_the_floor_off() -> None:
    base = v8.EntryPolicy(close_location_min=0.75)
    # ENTRY_INHERIT keeps the global floor; an explicit None removes it.
    inheriting = v8.V8Setup(
        "09:35", "LONG", 1, "max_liquidity", 0.20, 0.10, 1.0, 0.6, 0.5, 0.0, 1.0, 2.5
    )
    assert v8.policy_for_setup(inheriting, base).close_location_min == 0.75
    disabling = dataclasses.replace(inheriting, entry_clv=None)
    assert v8.policy_for_setup(disabling, base).close_location_min is None
    assert "V8" in v8.CACHE_SCHEMA_VERSION.upper()
    assert "SAME" in v8.PATH_POLICY_VERSION.upper()
    assert "SESSION" in v8.PATH_POLICY_VERSION.upper()


def test_v8_paths_are_isolated_from_v6_and_v7_artifacts() -> None:
    protected = {
        common.LATEST_DIR / "latest_fno_oi_ema_confirm_v6_best_net.md",
        common.LATEST_DIR / "latest_fno_oi_ema_confirm_v7_extreme_break.md",
        common.FNO_ROOT
        / "strategy_research"
        / "_signal_cache_equity_1m_aggregated_5m_futures_oi_v4",
        common.FNO_ROOT
        / "strategy_research"
        / "_signal_cache_equity_1m_aggregated_5m_futures_oi_v7_high_low_breakout_v1",
    }
    observed = {Path(v8.V8_ROOT), Path(v8.CACHE_DIR), Path(v8.REPORT_PATH)}
    norm = lambda path: str(path.resolve()).casefold()
    assert {norm(path) for path in observed}.isdisjoint(
        {norm(path) for path in protected}
    )
    assert Path(v8.CACHE_DIR).resolve().is_relative_to(Path(v8.V8_ROOT).resolve())
    assert all("v8" in str(path).lower() for path in observed)


@pytest.mark.parametrize(
    ("price", "tick", "up", "down"),
    [
        (100.00, 0.05, 100.00, 100.00),
        (100.001, 0.05, 100.05, 100.00),
        (99.999, 0.05, 100.00, 99.95),
        (10.013, 0.01, 10.02, 10.01),
    ],
)
def test_tick_rounding_is_directional(
    price: float, tick: float, up: float, down: float
) -> None:
    assert v8.round_up_to_tick(price, tick) == pytest.approx(up)
    assert v8.round_down_to_tick(price, tick) == pytest.approx(down)


def test_build_trigger_applies_buffer_then_directional_tick_rounding() -> None:
    long_bar = _bar(1, high=100.01)
    short_bar = _bar(1, low=99.99)
    assert v8.build_trigger(
        _setup(side="LONG"), long_bar, _policy(buffer_bps=2.0), tick_size=0.05
    ) == pytest.approx(100.05)
    assert v8.build_trigger(
        _setup(side="SHORT"), short_bar, _policy(buffer_bps=2.0), tick_size=0.05
    ) == pytest.approx(99.95)
    assert v8.build_trigger(
        _setup(side="LONG"), _bar(1, high=100.0), _policy(), tick_size=0.05
    ) == pytest.approx(100.00)
    assert v8.build_trigger(
        _setup(side="SHORT"), _bar(1, low=100.0), _policy(), tick_size=0.05
    ) == pytest.approx(100.00)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_strict_confirmation_gate_is_directional_and_morphology_aware(
    side: str,
) -> None:
    setup = _setup(side=side)
    candidate = _candidate("PASS", side=side)
    good = _strict_long(1) if side == "LONG" else _strict_short(1)
    assert v8.strict_confirmation_passes(setup, candidate, good, _policy())

    wrong_colour = (
        _bar(1, open_=100.7, high=100.8, low=99.7, close=100.1)
        if side == "LONG"
        else _bar(1, open_=99.3, high=100.3, low=99.2, close=99.9)
    )
    assert not v8.strict_confirmation_passes(
        setup, candidate, wrong_colour, _policy()
    )


@pytest.mark.parametrize(
    "bad",
    [
        _bar(1, open_=99.5, high=100.2, low=99.3, close=100.0),
        _bar(1, open_=100.45, high=100.8, low=99.7, close=100.6),
        _bar(1, open_=99.2, high=101.5, low=99.0, close=100.2),
        _bar(1, open_=100.0, high=100.0, low=100.0, close=100.0),
        _bar(
            1,
            open_=99.8,
            high=100.8,
            low=99.7,
            close=100.6,
            gap_filled=True,
        ),
    ],
    ids=("not_beyond_c5", "body_too_small", "wick_too_large", "zero_range", "synthetic"),
)
def test_strict_confirmation_fails_closed_on_each_quality_leg(
    bad: v8.MinuteBar,
) -> None:
    assert not v8.strict_confirmation_passes(
        _setup(), _candidate("BAD"), bad, _policy()
    )


@pytest.mark.parametrize("confirmation_minute", [1, 2, 3, 4])
def test_confirmation_window_and_s5_entry_boundary(
    confirmation_minute: int,
) -> None:
    candidate = _candidate("BOUNDARY")
    bars = [_quiet(i, high=100.70) for i in range(1, confirmation_minute)]
    bars.append(_strict_long(confirmation_minute, high=100.80))
    bars.extend(
        _quiet(i, high=100.70)
        for i in range(confirmation_minute + 1, 5)
    )
    bars.append(_bar(5, open_=100.70, high=100.80, low=100.50, close=100.70))
    bars.append(_bar(6, open_=100.70, high=100.75, low=100.60, close=100.70))

    row = _record(_simulate(_setup(), [candidate], {"BOUNDARY": bars}), "BOUNDARY")
    assert _was_filled(row)
    assert _minute_index(row, "confirmation") == confirmation_minute
    assert _minute_index(row, "entry") == 5


def test_confirmation_at_s5_is_unusable_and_confirmation_cannot_fill_itself() -> None:
    candidate = _candidate("NO_SAME_BAR")
    bars = [_quiet(i, high=100.70) for i in range(1, 5)]
    bars.append(_strict_long(5, high=101.50))
    row = _record(
        _simulate(_setup(), [candidate], {"NO_SAME_BAR": bars}),
        "NO_SAME_BAR",
    )
    assert not _was_filled(row)
    assert any(word in _terminal_text(row) for word in ("NO_CONFIRM", "EXPIRED"))

    s1 = _strict_long(1, high=101.50)
    later = [_quiet(i, high=101.40) for i in range(2, 6)]
    row = _record(
        _simulate(_setup(), [candidate], {"NO_SAME_BAR": [s1, *later]}),
        "NO_SAME_BAR",
    )
    assert not _was_filled(row)
    assert any(word in _terminal_text(row) for word in ("EXPIRED", "UNFILLED"))


def test_monitoring_stops_exactly_at_configured_confirmation_boundary() -> None:
    candidate = _candidate("CONFIRM_BOUNDARY")
    bars = [
        _quiet(1, high=100.70),
        _quiet(2, high=100.70),
        # This would be a valid strict confirmation if monitoring leaked one
        # completed minute beyond the configured S+2 boundary.
        _strict_long(3, high=100.80),
    ]

    row = _record(
        _simulate(
            _setup(),
            [candidate],
            {"CONFIRM_BOUNDARY": bars},
            _policy(max_confirmation_minute=2),
        ),
        "CONFIRM_BOUNDARY",
    )

    assert _value(row, "status") == "NO_CONFIRMATION"
    assert _value(row, "reason") == "CONFIRMATION_WINDOW_EXPIRED"
    assert pd.isna(_value(row, "confirmation_minute"))
    assert pd.Timestamp(_last_event(row)["event_ts"]) == SIGNAL_TIME + pd.Timedelta(
        minutes=2
    )


def test_pre_confirmation_invalidation_precedes_later_confirmation() -> None:
    candidate = _candidate("PRE_INVALID")
    bars = [
        _bar(1, open_=100.0, high=100.2, low=99.2, close=99.50),
        _strict_long(2),
        *[_quiet(i, high=101.0) for i in range(3, 6)],
    ]
    row = _record(_simulate(_setup(), [candidate], {"PRE_INVALID": bars}), "PRE_INVALID")
    assert "PRE" in _terminal_text(row)
    assert "INVALID" in _terminal_text(row)
    assert not _was_filled(row)
    assert len(row["confirmation_checks"]) == 1
    check = row["confirmation_checks"][0]
    assert check["gate_evaluated"] is False
    assert check["passed"] is False
    assert check["rejection_codes"][0] == "PRECONF_MIDPOINT_INVALIDATED"


def test_post_confirmation_invalidation_cancels_before_a_later_trigger() -> None:
    candidate = _candidate("POST_INVALID")
    bars = [
        _strict_long(1, high=100.80),
        _bar(2, open_=100.1, high=100.70, low=99.3, close=99.80),
        _bar(3, open_=100.1, high=101.00, low=99.9, close=100.8),
        _quiet(4),
        _quiet(5),
    ]
    row = _record(
        _simulate(_setup(), [candidate], {"POST_INVALID": bars}),
        "POST_INVALID",
    )
    assert "POST" in _terminal_text(row)
    assert any(word in _terminal_text(row) for word in ("INVALID", "CANCEL"))
    assert not _was_filled(row)


def test_short_pre_and_post_confirmation_invalidation_are_symmetric() -> None:
    setup = _setup(side="SHORT")
    candidate = _candidate("SHORT_PRE", side="SHORT")
    pre_bars = [
        _bar(1, open_=100.0, high=100.8, low=99.8, close=100.50),
        _strict_short(2),
        *[_quiet(i, low=99.0) for i in range(3, 6)],
    ]
    pre = _record(
        _simulate(setup, [candidate], {"SHORT_PRE": pre_bars}), "SHORT_PRE"
    )
    assert "PRE" in _terminal_text(pre)
    assert "INVALID" in _terminal_text(pre)
    assert not _was_filled(pre)

    candidate = _candidate("SHORT_POST", side="SHORT")
    post_bars = [
        _strict_short(1, low=99.20),
        _bar(2, open_=99.9, high=100.3, low=99.3, close=100.20),
        _bar(3, open_=99.4, high=99.6, low=99.0, close=99.1),
        _quiet(4),
        _quiet(5),
    ]
    post = _record(
        _simulate(setup, [candidate], {"SHORT_POST": post_bars}), "SHORT_POST"
    )
    assert "POST" in _terminal_text(post)
    assert any(word in _terminal_text(post) for word in ("INVALID", "CANCEL"))
    assert not _was_filled(post)


def test_gap_through_entry_fills_at_adverse_open() -> None:
    candidate = _candidate("GAP")
    bars = [
        _strict_long(1, high=100.80),
        _bar(2, open_=101.20, high=101.30, low=101.00, close=101.10),
        *[
            _bar(i, open_=101.10, high=101.20, low=100.90, close=101.05)
            for i in range(3, 7)
        ],
    ]
    row = _record(_simulate(_setup(), [candidate], {"GAP": bars}), "GAP")
    assert _was_filled(row)
    assert float(_value(row, "entry_price", "fill_price")) == pytest.approx(101.20)
    assert bool(_value(row, "gap_fill", "entry_gap_fill", "gap_filled"))


def test_intrabar_trigger_fill_cannot_treat_pre_entry_open_as_stop_gap() -> None:
    candidate = _candidate("INTRABAR_STOP")
    bars = [
        _strict_long(1, high=100.80),
        # The open is below the eventual stop, but the position does not exist
        # until price later touches the 100.80 trigger within this candle.
        _bar(2, open_=99.00, high=100.80, low=98.90, close=100.00),
        *[
            _bar(i, open_=100.0, high=100.5, low=99.8, close=100.1)
            for i in range(3, 6)
        ],
    ]

    row = _record(
        _simulate(_setup(), [candidate], {"INTRABAR_STOP": bars}),
        "INTRABAR_STOP",
    )

    assert _was_filled(row)
    assert bool(_value(row, "intrabar_trigger_fill"))
    assert not bool(_value(row, "gap_fill"))
    assert _value(row, "exit_reason") == "STOP"
    assert float(_value(row, "exit_price")) == pytest.approx(
        float(_value(row, "stop_price"))
    )
    assert float(_value(row, "exit_price")) != pytest.approx(99.00)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_target_at_open_precedes_a_later_intrabar_stop_for_open_position(
    side: str,
) -> None:
    symbol = f"TARGET_OPEN_{side}"
    candidate = _candidate(symbol, side=side)
    if side == "LONG":
        bars = [
            _strict_long(1, high=100.80),
            _bar(2, open_=100.70, high=100.80, low=100.50, close=100.70),
            # Already open at the start of S+3: the opening target is known to
            # precede the later trip through the stop within this same candle.
            _bar(3, open_=102.00, high=102.20, low=99.00, close=100.00),
        ]
    else:
        bars = [
            _strict_short(1, low=99.20),
            _bar(2, open_=99.30, high=99.50, low=99.20, close=99.30),
            _bar(3, open_=98.00, high=101.00, low=97.80, close=100.00),
        ]

    row = _record(
        _simulate(_setup(side=side), [candidate], {symbol: bars}),
        symbol,
    )

    assert _value(row, "status") == "TARGETED"
    assert _value(row, "exit_reason") == "TARGET"
    assert bool(_value(row, "exit_at_bar_open"))
    assert pd.Timestamp(_value(row, "exit_time")) == SIGNAL_TIME + pd.Timedelta(
        minutes=3
    )
    assert float(_value(row, "exit_price")) == pytest.approx(
        float(_value(row, "target_price"))
    )


def test_same_bar_stop_and_target_collision_is_stop_first() -> None:
    candidate = _candidate("TIE")
    bars = [
        _strict_long(1, high=100.80),
        _bar(2, open_=100.80, high=102.00, low=99.00, close=100.80),
        *[_quiet(i) for i in range(3, 6)],
    ]
    row = _record(_simulate(_setup(), [candidate], {"TIE": bars}), "TIE")
    assert _was_filled(row)
    assert "STOP" in str(_value(row, "exit_reason", "outcome")).upper()


def test_configured_square_off_prevents_a_later_bar_from_changing_the_exit() -> None:
    candidate = _candidate("SQUARE_OFF_CUTOFF")
    bars = [
        _strict_long(1, high=100.80),
        _bar(2, open_=100.70, high=100.80, low=100.50, close=100.70),
        *[
            _bar(i, open_=100.70, high=101.20, low=100.40, close=100.80)
            for i in range(3, 6)
        ],
        _bar(6, open_=100.80, high=101.20, low=100.50, close=100.90),
        # This post-cutoff bar reaches the target and must never be inspected.
        _bar(7, open_=100.90, high=102.20, low=100.80, close=102.00),
    ]

    row = _record(
        _simulate(
            _setup(),
            [candidate],
            {"SQUARE_OFF_CUTOFF": bars},
            _policy(square_off="09:36", eod_policy="EXACT_SQUARE_OFF"),
        ),
        "SQUARE_OFF_CUTOFF",
    )

    assert _value(row, "status") == "SQUARE_OFF"
    assert _value(row, "exit_reason") == "SQUARE_OFF"
    assert pd.Timestamp(_value(row, "exit_time")) == SIGNAL_TIME + pd.Timedelta(
        minutes=6
    )
    assert float(_value(row, "exit_price")) == pytest.approx(100.90)


@pytest.mark.parametrize(
    ("square_off", "message"),
    [
        ("09:34", "later than.*S\\+5"),
        ("09:35", "later than.*S\\+5"),
        ("15:31", "15:30 session close"),
    ],
    ids=("before_s5", "at_s5", "after_market_close"),
)
def test_invalid_square_off_boundaries_are_rejected(
    square_off: str,
    message: str,
) -> None:
    candidate = _candidate("BAD_CUTOFF")
    with pytest.raises(ValueError, match=message):
        _simulate(
            _setup(),
            [candidate],
            {"BAD_CUTOFF": []},
            _policy(square_off=square_off, eod_policy="EXACT_SQUARE_OFF"),
        )


def test_missing_s5_cannot_erase_a_trade_already_terminal_at_s2() -> None:
    candidate = _candidate("EARLY_TERMINAL")
    bars = [
        _strict_long(1, high=100.80),
        _bar(2, open_=100.70, high=102.00, low=100.50, close=101.90),
        _bar(3, open_=101.90, high=102.00, low=101.70, close=101.80),
        _bar(4, open_=101.80, high=101.90, low=101.60, close=101.70),
        # S+5 intentionally absent.
    ]

    row = _record(
        _simulate(_setup(), [candidate], {"EARLY_TERMINAL": bars}),
        "EARLY_TERMINAL",
    )

    assert _value(row, "status") == "TARGETED"
    assert _value(row, "exit_reason") == "TARGET"
    assert _minute_index(row, "entry") == 2
    assert pd.Timestamp(_value(row, "exit_time")) == SIGNAL_TIME + pd.Timedelta(
        minutes=2
    )
    assert pd.Timestamp(_last_event(row)["event_ts"]) == SIGNAL_TIME + pd.Timedelta(
        minutes=2
    )


@pytest.mark.parametrize(
    ("s3_bar", "expected_reason"),
    [
        (None, "MISSING_ENTRY_WINDOW_BAR"),
        (_bar(3, gap_filled=True), "INVALID_ENTRY_WINDOW_BAR"),
    ],
    ids=("missing", "invalid"),
)
def test_bad_post_entry_minute_fails_closed_at_its_exact_timestamp(
    s3_bar: v8.MinuteBar | None,
    expected_reason: str,
) -> None:
    candidate = _candidate("POST_ENTRY_DATA")
    bars = [
        _strict_long(1, high=100.80),
        _bar(2, open_=100.70, high=100.80, low=100.50, close=100.70),
        _bar(4, open_=100.70, high=101.20, low=100.40, close=100.80),
        _bar(5, open_=100.80, high=101.20, low=100.50, close=100.90),
    ]
    if s3_bar is not None:
        bars.append(s3_bar)

    row = _record(
        _simulate(_setup(), [candidate], {"POST_ENTRY_DATA": bars}),
        "POST_ENTRY_DATA",
    )

    assert _was_filled(row)
    assert _value(row, "status") == "DATA_INCOMPLETE"
    assert _value(row, "reason") == expected_reason
    terminal_event = _last_event(row)
    assert terminal_event["reason"] == expected_reason
    assert pd.Timestamp(terminal_event["event_ts"]) == SIGNAL_TIME + pd.Timedelta(
        minutes=3
    )


def test_pending_cap_reservation_is_not_displaced_by_later_higher_rank() -> None:
    low_rank = _candidate("LOW", traded_value=10.0)
    high_rank = _candidate("HIGH", traded_value=20.0)
    bars = {
        "LOW": [
            _strict_long(1, high=100.80),
            _quiet(2, high=100.70),
            _bar(3, open_=100.7, high=100.80, low=100.5, close=100.7),
            _quiet(4),
            _quiet(5),
        ],
        "HIGH": [
            _quiet(1, high=100.70),
            _strict_long(2, high=100.80),
            _bar(3, open_=100.7, high=101.00, low=100.5, close=100.8),
            _quiet(4),
            _quiet(5),
        ],
    }
    records = _simulate(_setup(max_entries=1), [low_rank, high_rank], bars)
    assert _was_filled(_record(records, "LOW"))
    assert not _was_filled(_record(records, "HIGH"))


def test_cancelled_reservation_is_reassigned_and_allocation_is_permutation_invariant() -> None:
    low_rank = _candidate("LOW", traded_value=10.0)
    high_rank = _candidate("HIGH", traded_value=20.0)
    bars = {
        "LOW": [
            _strict_long(1, high=100.80),
            _bar(2, open_=100.1, high=100.70, low=99.3, close=99.80),
            _quiet(3),
            _quiet(4),
            _quiet(5),
        ],
        "HIGH": [
            _quiet(1, high=100.70),
            _strict_long(2, high=100.80),
            _bar(3, open_=100.7, high=100.80, low=100.5, close=100.7),
            _quiet(4),
            _quiet(5),
        ],
    }

    def outcome(candidates: list[v8.CandidateInput]) -> dict[str, tuple[str, bool, Any]]:
        records = _simulate(_setup(max_entries=1), candidates, bars)
        result: dict[str, tuple[str, bool, Any]] = {}
        for symbol in ("LOW", "HIGH"):
            row = _record(records, symbol)
            minute = row.get("entry_minute")
            result[symbol] = (
                _terminal_text(row),
                _was_filled(row),
                None if minute is None or pd.isna(minute) else int(minute),
            )
        return result

    ordered = outcome([low_rank, high_rank])
    reversed_order = outcome([high_rank, low_rank])
    assert ordered == reversed_order
    assert not ordered["LOW"][1]
    assert ordered["HIGH"][1]


def test_disabled_cap_reassignment_burns_a_cancelled_reservation() -> None:
    low_rank = _candidate("LOW", traded_value=10.0)
    high_rank = _candidate("HIGH", traded_value=20.0)
    bars = {
        "LOW": [
            _strict_long(1, high=100.80),
            _bar(2, open_=100.1, high=100.70, low=99.3, close=99.80),
            _quiet(3),
            _quiet(4),
            _quiet(5),
        ],
        "HIGH": [
            _quiet(1, high=100.70),
            _strict_long(2, high=100.80),
            _bar(3, open_=100.7, high=100.80, low=100.5, close=100.7),
            _quiet(4),
            _quiet(5),
        ],
    }

    records = _simulate(
        _setup(max_entries=1),
        [low_rank, high_rank],
        bars,
        _policy(allow_cap_reassignment=False),
    )
    low = _record(records, "LOW")
    high = _record(records, "HIGH")

    assert _value(low, "status") == "POSTCONF_CANCELLED"
    assert _value(high, "status") == "WINDOW_EXPIRED"
    assert _value(high, "reason") == "ENTRY_WINDOW_EXPIRED"
    assert not _was_filled(high)
    assert all(
        event["state_after"] != "PENDING_STOP"
        for event in _value(high, "events")
    )


def test_next_session_bar_can_never_fill_and_missing_expected_bar_fails_closed() -> None:
    candidate = _candidate("DATE")
    same_day = [
        _strict_long(1, high=100.80),
        *[_quiet(i, high=100.70) for i in range(2, 6)],
    ]
    next_day_trigger = _bar(
        1,
        day_offset=1,
        open_=101.0,
        high=102.0,
        low=100.9,
        close=101.5,
    )
    row = _record(
        _simulate(_setup(), [candidate], {"DATE": [*same_day, next_day_trigger]}),
        "DATE",
    )
    assert not _was_filled(row)

    missing_s3 = [bar for bar in same_day if bar.timestamp != SIGNAL_TIME + pd.Timedelta(minutes=3)]
    row = _record(
        _simulate(_setup(), [candidate], {"DATE": missing_s3}),
        "DATE",
    )
    assert "DATA" in _terminal_text(row)
    assert any(word in _terminal_text(row) for word in ("MISSING", "INCOMPLETE"))
    assert not _was_filled(row)


def test_portfolio_rejection_clears_constrained_trade_but_preserves_diagnostics() -> None:
    def completed_target(symbol: str) -> dict[str, Any]:
        candidate = _candidate(symbol)
        bars = [
            _strict_long(1, high=100.80),
            _bar(2, open_=100.70, high=102.00, low=100.50, close=101.90),
        ]
        return _record(_simulate(_setup(), [candidate], {symbol: bars}), symbol)

    accepted_source = completed_target("ACCEPTED")
    rejected_source = completed_target("REJECTED")
    audit = pd.DataFrame([accepted_source, rejected_source])
    audit["frozen_rank"] = [1, 2]
    audit["filled"] = True
    audit["quantity"] = [100, 100]
    audit["gross_pnl_rs"] = [100.0, 100.0]
    audit["estimated_cost_rs"] = [5.0, 5.0]
    audit["net_pnl_rs"] = [95.0, 95.0]
    audit["entry_delay_minutes"] = [1, 1]
    audit["position_notional_rs"] = [10_000.0, 10_000.0]
    audit["mfe_pct_ohlc_lower_bound"] = [1.0, 1.0]
    audit["mfe_pct_ohlc_upper_bound"] = [1.2, 1.2]
    audit["mae_pct_ohlc_lower_bound"] = [0.1, 0.1]
    audit["mae_pct_ohlc_upper_bound"] = [0.2, 0.2]
    audit["excursion_entry_bar_ambiguous"] = [True, True]
    audit["excursion_exit_bar_ambiguous"] = [True, True]
    audit["excursion_boundary_ambiguous"] = [True, True]
    audit["excursion_observed_bar_count"] = [1, 1]
    audit["excursion_complete_bar_count"] = [0, 0]
    original_rejected = audit.loc[audit["symbol"].eq("REJECTED")].iloc[0].copy()

    constrained = v8.apply_global_portfolio_constraints(
        audit,
        v8.PortfolioPolicy(
            capital_rs=10_000.0,
            margin_per_entry_rs=10_000.0,
            target_exposure_per_entry_rs=50_000.0,
            max_concurrent_positions=1,
        ),
    )
    row = constrained.loc[constrained["symbol"].eq("REJECTED")].iloc[0]

    assert row["portfolio_decision"] == "REJECTED"
    assert row["status"] == "PORTFOLIO_REJECTED"
    assert not bool(row["filled"])
    for column in (
        "entry_minute",
        "entry_delay_minutes",
        "entry_time",
        "entry_price",
        "stop_price",
        "target_price",
        "exit_time",
        "exit_price",
        "gross_return_pct",
        "net_return_pct",
        "gross_pnl_rs",
        "estimated_cost_rs",
        "net_pnl_rs",
        "position_notional_rs",
        "mfe_pct_ohlc_lower_bound",
        "mfe_pct_ohlc_upper_bound",
        "mae_pct_ohlc_lower_bound",
        "mae_pct_ohlc_upper_bound",
        "excursion_observed_bar_count",
        "excursion_complete_bar_count",
    ):
        assert pd.isna(row[column]), (column, row[column])
    assert row["exit_reason"] == ""
    assert int(row["quantity"]) == 0
    assert not bool(row["gap_fill"])
    assert not bool(row["intrabar_trigger_fill"])
    assert not bool(row["ambiguous_entry_bar"])
    assert int(row["confirmation_minute"]) == int(
        original_rejected["confirmation_minute"]
    )
    assert float(row["trigger"]) == pytest.approx(float(original_rejected["trigger"]))

    assert row["unconstrained_status"] == rejected_source["status"] == "TARGETED"
    for column in (
        "confirmation_minute",
        "entry_minute",
        "entry_delay_minutes",
        "trigger",
        "entry_price",
        "stop_price",
        "target_price",
        "exit_price",
        "gross_return_pct",
        "quantity",
        "gross_pnl_rs",
        "estimated_cost_rs",
        "net_return_pct",
        "net_pnl_rs",
        "position_notional_rs",
        "mfe_pct_ohlc_lower_bound",
        "mfe_pct_ohlc_upper_bound",
        "mae_pct_ohlc_lower_bound",
        "mae_pct_ohlc_upper_bound",
        "excursion_observed_bar_count",
        "excursion_complete_bar_count",
    ):
        assert float(row[f"unconstrained_{column}"]) == pytest.approx(
            float(original_rejected[column])
        ), column
    for column in ("confirmation_time", "entry_time", "exit_time"):
        assert pd.Timestamp(row[f"unconstrained_{column}"]) == pd.Timestamp(
            original_rejected[column]
        ), column
    for column in (
        "gap_fill",
        "intrabar_trigger_fill",
        "ambiguous_entry_bar",
        "exit_reason",
    ):
        assert row[f"unconstrained_{column}"] == original_rejected[column], column
    assert float(row["unconstrained_net_pnl_rs"]) == pytest.approx(95.0)
    assert row["unconstrained_events"] == rejected_source["events"]
    assert row["unconstrained_events"][-1]["state_after"] == "TARGETED"
    assert row["confirmation_checks"] == original_rejected["confirmation_checks"]
    for column in (
        "excursion_entry_bar_ambiguous",
        "excursion_exit_bar_ambiguous",
        "excursion_boundary_ambiguous",
    ):
        assert pd.isna(row[column])
        assert bool(row[f"unconstrained_{column}"])

    constrained_events = row["events"]
    assert int(row["event_count"]) == len(constrained_events)
    assert constrained_events[-1]["state_after"] == "PORTFOLIO_REJECTED"
    assert constrained_events[-1]["reason"] == row["reason"]


def test_summary_uses_explicit_calendar_and_zero_baseline_for_first_day_loss() -> None:
    first_session = pd.Timestamp("2026-08-17").date()
    flat_session = pd.Timestamp("2026-08-18").date()
    audit = pd.DataFrame(
        [
            {
                "candidate_id": "2026-08-17|09:30_LONG|LOSS",
                "session_date": first_session,
                "status": "STOPPED",
                "filled": True,
                "net_return_pct": -2.0,
                "net_pnl_rs": -1_000.0,
            }
        ]
    )

    summary, daily = v8.summarize_v8_results(
        audit,
        session_dates=[first_session, flat_session],
        eod_policy="EXACT_SQUARE_OFF",
    )

    assert daily["session_date"].tolist() == [first_session, flat_session]
    assert daily["candidates"].tolist() == [1, 0]
    assert daily["fills"].tolist() == [1, 0]
    assert daily["net_return_pct"].tolist() == pytest.approx([-2.0, 0.0])
    assert daily["net_pnl_rs"].tolist() == pytest.approx([-1_000.0, 0.0])
    assert summary["sessions"] == 2
    assert summary["negative_days"] == 1
    assert summary["flat_days"] == 1
    assert summary["max_daily_drawdown_percentage_points"] == pytest.approx(2.0)
    assert summary["diagnostic_closed_trade_metrics"][
        "max_daily_drawdown_percentage_points"
    ] == pytest.approx(2.0)


@pytest.mark.parametrize("field", ["buffer_bps", "cost_bps", "slippage_bps"])
@pytest.mark.parametrize("value", [float("nan"), float("inf")])
def test_entry_policy_rejects_nonfinite_economics(field: str, value: float) -> None:
    values = {field: value}
    with pytest.raises(ValueError, match="finite"):
        v8.EntryPolicy(**values).validate()


@pytest.mark.parametrize(
    "field", ["capital_rs", "margin_per_entry_rs", "target_exposure_per_entry_rs"]
)
def test_portfolio_policy_rejects_nonfinite_economics(field: str) -> None:
    with pytest.raises(ValueError, match="finite"):
        v8.PortfolioPolicy(**{field: float("nan")}).validate()


def test_filled_trade_without_finite_terminal_economics_blocks_headline() -> None:
    session_day = pd.Timestamp("2026-08-18").date()
    audit = pd.DataFrame(
        [
            {
                "candidate_id": "2026-08-18|09:30_LONG|BROKEN",
                "session_date": session_day,
                "status": "TARGETED",
                "filled": True,
                "net_return_pct": float("nan"),
                "net_pnl_rs": float("nan"),
            }
        ]
    )
    summary, _ = v8.summarize_v8_results(
        audit,
        session_dates=[session_day],
        eod_policy="EXACT_SQUARE_OFF",
        source_complete=True,
    )
    assert summary["fills"] == 1
    assert summary["closed_fills"] == 0
    assert summary["unresolved_filled_trades"] == 1
    assert summary["headline_valid"] is False
    assert summary["net_return_percentage_points"] is None
    assert "FILLED_TRADES_WITHOUT_FINITE_TERMINAL_ECONOMICS" in summary[
        "promotion_blockers"
    ]


@pytest.mark.parametrize("field", ["buffer_bps", "slippage_bps"])
def test_entry_policy_rejects_price_destroying_basis_points(field: str) -> None:
    with pytest.raises(ValueError, match="below 10,000"):
        v8.EntryPolicy(**{field: 10_000.0}).validate()


def test_global_squareoff_guard_runs_even_with_zero_candidates() -> None:
    too_early = v8.EntryPolicy(
        square_off="09:50",
        eod_policy="EXACT_SQUARE_OFF",
    )
    with pytest.raises(ValueError, match=r"latest V8 S\+5"):
        v8.run_v8_backtest(
            pd.DataFrame(),
            pd.DataFrame(),
            variant="B4",
            policy=too_early,
        )
    valid = v8.EntryPolicy(
        square_off="09:51",
        eod_policy="EXACT_SQUARE_OFF",
    )
    result = v8.run_v8_backtest(
        pd.DataFrame(),
        pd.DataFrame(),
        variant="B4",
        policy=valid,
    )
    assert result.empty


def test_confirmation_check_emits_metrics_and_ordered_rejection_codes() -> None:
    policy = _policy(close_location_min=0.75)
    bar = _bar(
        1,
        open_=100.4,
        high=102.0,
        low=99.0,
        close=99.8,
    )
    check = v8._confirmation_check(_setup(), _candidate("AUDIT"), bar, policy)

    assert check["timestamp"] == bar.ts
    assert check["open"] == pytest.approx(100.4)
    assert check["high"] == pytest.approx(102.0)
    assert check["low"] == pytest.approx(99.0)
    assert check["close"] == pytest.approx(99.8)
    assert check["volume"] == pytest.approx(1_000.0)
    assert check["candle_range"] == pytest.approx(3.0)
    assert check["body_ratio"] == pytest.approx(0.2)
    assert check["adverse_wick_ratio"] == pytest.approx(1.6 / 3.0)
    assert check["close_location"] == pytest.approx(0.8 / 3.0)
    assert check["passed"] is False
    assert check["rejection_codes"] == [
        "WRONG_CANDLE_DIRECTION",
        "CLOSE_NOT_BEYOND_FIVE_MINUTE_CLOSE",
        "BODY_RATIO_BELOW_MINIMUM",
        "ADVERSE_WICK_RATIO_ABOVE_MAXIMUM",
        "CLOSE_LOCATION_BELOW_MINIMUM",
    ]
    assert v8.strict_confirmation_passes(
        _setup(), _candidate("AUDIT"), bar, policy
    ) is check["passed"]


def test_audit_keeps_failed_attempts_before_selected_confirmation() -> None:
    candidate = _candidate("CHECKS")
    failing = _quiet(1)
    passing = _strict_long(2, high=100.80)
    fill_and_target = _bar(
        3,
        open_=100.70,
        high=102.00,
        low=100.50,
        close=101.90,
    )
    row = _record(
        _simulate(
            _setup(),
            [candidate],
            {"CHECKS": [failing, passing, fill_and_target]},
        ),
        "CHECKS",
    )

    assert [check["minute_index"] for check in row["confirmation_checks"]] == [1, 2]
    assert row["confirmation_checks"][0]["rejection_codes"]
    assert row["confirmation_checks"][1]["passed"] is True
    assert row["confirmation_checks"][1]["rejection_codes"] == []
    assert row["confirmation_open"] == pytest.approx(passing.open)
    assert row["confirmation_high"] == pytest.approx(passing.high)
    assert row["confirmation_low"] == pytest.approx(passing.low)
    assert row["confirmation_close"] == pytest.approx(passing.close)
    assert row["confirmation_volume"] == pytest.approx(passing.volume)
    assert row["confirmation_body_ratio"] == pytest.approx(0.8 / 1.1)
    assert row["confirmation_rejection_codes"] == []
    assert row["entry_delay_minutes"] == 1


def test_run_audit_has_full_context_notional_and_excursion_bounds() -> None:
    candidate_id = "2026-08-18|09:30_LONG|FULL"
    candidates = pd.DataFrame(
        [
            {
                "candidate_id": candidate_id,
                "session_date": SIGNAL_TIME.date(),
                "signal_time": SIGNAL_TIME,
                "signal_end": "09:30",
                "setup_id": "09:30_LONG",
                "side": "LONG",
                "symbol": "FULL",
                "futures_symbol": "FULL26AUGFUT",
                "equity_instrument_token": 11,
                "futures_instrument_token": 22,
                "tick_size": 0.05,
                "lot_size": 100,
                "five_min_open": 99.5,
                "five_min_high": 101.0,
                "five_min_low": 99.0,
                "five_min_close": 100.0,
                "five_min_volume": 12_345.0,
                "ema9": 100.0,
                "ema20": 99.0,
                "ema50": 98.0,
                "price_change_pct": 1.0,
                "oi": 201_000.0,
                "prev_oi": 200_000.0,
                "oi_change_pct": 1.0,
                "volume_ratio": 3.0,
                "traded_value": 12_000_000.0,
                "picker": "max_move",
                "picker_value": 1.0,
                "frozen_rank": 1,
            }
        ]
    )
    minute_paths = pd.DataFrame(
        [
            {
                "candidate_id": candidate_id,
                "bar_ts": SIGNAL_TIME + pd.Timedelta(minutes=1),
                "open": 99.8,
                "high": 100.8,
                "low": 99.7,
                "close": 100.6,
                "volume": 1_000.0,
                "gap_filled": False,
                "opening_snapshot": False,
                "provisional_stale": False,
            },
            {
                "candidate_id": candidate_id,
                "bar_ts": SIGNAL_TIME + pd.Timedelta(minutes=2),
                "open": 100.7,
                "high": 103.5,
                "low": 100.5,
                "close": 103.0,
                "volume": 2_000.0,
                "gap_filled": False,
                "opening_snapshot": False,
                "provisional_stale": False,
            },
        ]
    )

    audit = v8.run_v8_backtest(
        candidates,
        minute_paths,
        variant="B2",
        policy=v8.entry_policy_for_variant(
            "B2",
            cost_bps=5.0,
            slippage_bps=0.0,
            square_off="15:30",
            eod_policy="EXACT_SQUARE_OFF",
        ),
    )
    row = audit.iloc[0]

    for column, expected in {
        "five_min_open": 99.5,
        "five_min_high": 101.0,
        "five_min_low": 99.0,
        "five_min_close": 100.0,
        "five_min_volume": 12_345.0,
        "ema9": 100.0,
        "ema20": 99.0,
        "ema50": 98.0,
        "oi": 201_000.0,
        "prev_oi": 200_000.0,
    }.items():
        assert float(row[column]) == pytest.approx(expected), column
    assert row["ema_structure"] == "BULLISH"
    assert int(row["setup_cap"]) == 1
    assert int(row["frozen_rank"]) == 1
    assert float(row["trigger_distance_c5_bps"]) == pytest.approx(85.0)
    assert int(row["entry_delay_minutes"]) == 1
    assert float(row["position_notional_rs"]) == pytest.approx(
        float(row["entry_price"]) * int(row["quantity"])
    )
    assert row["schema_version"] == v8.TRADE_SCHEMA_VERSION
    assert bool(row["excursion_boundary_ambiguous"])
    assert float(row["mfe_pct_ohlc_lower_bound"]) <= float(
        row["mfe_pct_ohlc_upper_bound"]
    )
    assert float(row["mae_pct_ohlc_lower_bound"]) <= float(
        row["mae_pct_ohlc_upper_bound"]
    )
    assert float(row["mae_pct_ohlc_lower_bound"]) == pytest.approx(0.0)
    assert float(row["mae_pct_ohlc_upper_bound"]) > 0.0


@pytest.mark.parametrize(
    ("exit_reason", "exit_price", "expected_mfe", "expected_mae"),
    [
        ("STOP_GAP", 90.0, 2.0, 10.0),
        ("TARGET", 103.0, 3.0, 2.0),
    ],
)
def test_exit_at_open_excursions_exclude_later_exit_bar_extremes(
    exit_reason: str,
    exit_price: float,
    expected_mfe: float,
    expected_mae: float,
) -> None:
    candidate_id = "2026-08-18|09:30_LONG|OPEN_EXIT"
    entry_ts = SIGNAL_TIME + pd.Timedelta(minutes=2)
    exit_ts = SIGNAL_TIME + pd.Timedelta(minutes=4)
    audit = pd.DataFrame(
        [
            {
                "candidate_id": candidate_id,
                "side": "LONG",
                "entry_time": entry_ts,
                "entry_price": 100.0,
                "exit_time": exit_ts,
                "exit_price": exit_price,
                "exit_reason": exit_reason,
                "exit_at_bar_open": True,
                "gap_fill": True,
            }
        ]
    )
    paths = pd.DataFrame(
        [
            {
                "candidate_id": candidate_id,
                "bar_ts": entry_ts,
                "high": 101.0,
                "low": 99.0,
            },
            {
                "candidate_id": candidate_id,
                "bar_ts": entry_ts + pd.Timedelta(minutes=1),
                "high": 102.0,
                "low": 98.0,
            },
            {
                "candidate_id": candidate_id,
                "bar_ts": exit_ts,
                # These extremes occur after the deterministic open exit and
                # therefore must not enter either excursion bound.
                "high": 200.0,
                "low": 50.0,
            },
        ]
    )

    row = v8.attach_excursion_diagnostics(audit, paths).iloc[0]

    assert float(row["mfe_pct_ohlc_lower_bound"]) == pytest.approx(expected_mfe)
    assert float(row["mfe_pct_ohlc_upper_bound"]) == pytest.approx(expected_mfe)
    assert float(row["mae_pct_ohlc_lower_bound"]) == pytest.approx(expected_mae)
    assert float(row["mae_pct_ohlc_upper_bound"]) == pytest.approx(expected_mae)
    assert not bool(row["excursion_entry_bar_ambiguous"])
    assert not bool(row["excursion_exit_bar_ambiguous"])
    assert not bool(row["excursion_boundary_ambiguous"])


def test_diagnostic_breakdowns_use_calendar_blocks_and_constrained_trades() -> None:
    sessions = [
        pd.Timestamp(value).date()
        for value in (
            "2026-08-10",
            "2026-08-11",
            "2026-08-12",
            "2026-08-13",
            "2026-08-14",
            "2026-08-17",
        )
    ]
    audit = pd.DataFrame(
        [
            {
                "candidate_id": "WIN",
                "session_date": sessions[0],
                "side": "LONG",
                "setup_id": "09:30_LONG",
                "signal_end": "09:30",
                "symbol": "WIN",
                "confirmation_minute": 1,
                "entry_minute": 2,
                "buffer_bps": 2.0,
                "filled": True,
                "gap_fill": True,
                "net_return_pct": 1.0,
                "gross_pnl_rs": 105.0,
                "estimated_cost_rs": 5.0,
                "net_pnl_rs": 100.0,
            },
            {
                "candidate_id": "REJECTED",
                "session_date": sessions[-1],
                "side": "SHORT",
                "setup_id": "09:40_SHORT",
                "signal_end": "09:40",
                "symbol": "REJECTED",
                "confirmation_minute": 2,
                "entry_minute": None,
                "buffer_bps": 2.0,
                "filled": False,
                "gap_fill": False,
                "net_return_pct": None,
                "net_pnl_rs": None,
                "unconstrained_net_return_pct": 9.0,
                "unconstrained_net_pnl_rs": 900.0,
            },
        ]
    )

    breakdowns = v8.build_v8_diagnostic_breakdowns(
        audit, session_dates=sessions
    )
    assert set(breakdowns["dimension"]) == set(
        v8.DIAGNOSTIC_BREAKDOWN_DIMENSIONS
    )
    blocks = breakdowns.loc[
        breakdowns["dimension"].eq("five_session_block")
    ].reset_index(drop=True)
    assert len(blocks) == 2
    assert blocks.loc[0, "bucket_start_date"] == sessions[0]
    assert blocks.loc[0, "bucket_end_date"] == sessions[4]
    assert int(blocks.loc[0, "candidates"]) == 1
    assert int(blocks.loc[1, "candidates"]) == 1
    short = breakdowns.loc[
        breakdowns["dimension"].eq("side")
        & breakdowns["bucket"].eq("SHORT")
    ].iloc[0]
    assert int(short["fills"]) == 0
    assert float(short["net_pnl_rs"]) == pytest.approx(0.0)
    assert set(
        breakdowns.loc[breakdowns["dimension"].eq("gap_fill"), "bucket"]
    ) == {"GAP_FILL", "NO_ENTRY"}

    report = v8.render_v8_report(
        summary={},
        variant="B2",
        policy=v8.EntryPolicy(),
        cache_manifest={},
        coverage=pd.DataFrame(),
        from_day=sessions[0],
        through_day=sessions[-1],
        run_id="diagnostic-test",
        diagnostic_breakdowns=breakdowns,
    )
    assert "## B0-B5 diagnostic breakdowns" in report
    assert "diagnostic_breakdowns.csv" in report
    assert "five_session_block" in report


def test_diagnostic_artifact_is_json_safe_hashed_and_provenance_required(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate = _candidate("EXPORT")
    audit = pd.DataFrame(
        _simulate(
            _setup(),
            [candidate],
            {
                "EXPORT": [
                    _strict_long(1, high=100.8),
                    _bar(
                        2,
                        open_=100.7,
                        high=102.0,
                        low=100.5,
                        close=101.9,
                    ),
                ]
            },
        )
    )
    audit["filled"] = audit["entry_price"].notna()
    audit["buffer_bps"] = 0.0
    audit["quantity"] = 1
    audit["gross_pnl_rs"] = 1.0
    audit["estimated_cost_rs"] = 0.1
    audit["net_pnl_rs"] = 0.9
    for column in v8._AUDIT_EXPORT_REQUIRED_COLUMNS:
        if column not in audit.columns:
            audit[column] = float("nan")
    audit["variant"] = "B0"
    audit["cost_bps"] = 5.0
    audit["slippage_bps"] = 0.0
    audit["eod_policy"] = "LAST_REAL_BAR_SENSITIVITY"
    audit["position_notional_rs"] = audit["entry_price"] * audit["quantity"]
    audit["excursion_policy_version"] = v8.EXCURSION_POLICY_VERSION
    audit["portfolio_mode"] = v8.PORTFOLIO_MODE
    audit["portfolio_decision"] = "ACCEPTED"
    breakdowns = v8.build_v8_diagnostic_breakdowns(
        audit, session_dates=[SIGNAL_TIME.date()]
    )
    manifest = {
        "input_fingerprint": "test-cache",
        "input_contract": {"strategy_code_sha256": v8._module_source_sha256()},
        "session_dates": [SIGNAL_TIME.date().isoformat()],
    }
    manifest_path = tmp_path / "cache_manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setattr(v8, "RUN_ROOT", tmp_path / "runs")
    monkeypatch.setattr(v8, "PROVENANCE_ROOT", tmp_path / "provenance")
    monkeypatch.setattr(v8, "REPORT_PATH", tmp_path / "latest.md")

    run_dir, _, payload = v8.write_v8_run_artifacts(
        audit=audit,
        daily=pd.DataFrame(),
        coverage=pd.DataFrame(),
        summary={},
        variant="B0",
        policy=v8.EntryPolicy(),
        cache_manifest=manifest,
        cache_manifest_path=manifest_path,
        from_day=SIGNAL_TIME.date(),
        through_day=SIGNAL_TIME.date(),
        split_day=None,
        diagnostic_breakdowns=breakdowns,
    )

    exported_audit = pd.read_csv(run_dir / "candidate_order_audit.csv")
    parsed_checks = json.loads(exported_audit.loc[0, "confirmation_checks"])
    assert parsed_checks[0]["minute_index"] == 1
    diagnostic_path = run_dir / "diagnostic_breakdowns.csv"
    assert "diagnostic_breakdowns" in payload["outputs"]
    diagnostic_record = payload["outputs"]["diagnostic_breakdowns"]
    assert v8.provenance.artifact_matches(diagnostic_path, diagnostic_record)
    diagnostic_path.write_text(
        diagnostic_path.read_text(encoding="utf-8") + "\n", encoding="utf-8"
    )
    assert not v8.provenance.artifact_matches(diagnostic_path, diagnostic_record)

    old_outputs = {
        name: {}
        for name in (
            "candidate_order_audit",
            "state_events",
            "daily",
            "coverage",
            "setups",
            "report",
            "strategy_source_archive",
            "cache_manifest_archive",
        )
    }
    incomplete_provenance = tmp_path / "missing-diagnostics.json"
    incomplete_provenance.write_text(
        json.dumps(
            {
                "v8_run_schema_version": v8.RUN_SCHEMA_VERSION,
                "strategy_version": v8.STRATEGY_VERSION,
                "outputs": old_outputs,
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="diagnostic_breakdowns"):
        v8.validate_v8_run_provenance(incomplete_provenance)


def test_diagnostic_conventions_are_fingerprinted() -> None:
    diagnostics = v8.strategy_payload()["diagnostics"]
    assert diagnostics["trade_schema_version"] == v8.TRADE_SCHEMA_VERSION
    assert diagnostics["breakdown_schema_version"] == (
        v8.DIAGNOSTIC_BREAKDOWN_SCHEMA_VERSION
    )
    assert diagnostics["excursion_policy_version"] == v8.EXCURSION_POLICY_VERSION
    assert diagnostics["breakdown_dimensions"] == list(
        v8.DIAGNOSTIC_BREAKDOWN_DIMENSIONS
    )
    assert "FIVE_OFFICIAL_SESSION" in diagnostics["chronological_block_policy"]
