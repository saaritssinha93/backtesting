"""Honest 09:50/09:55 FnO V9 optimizer on the V8 same-session engine.

This module is deliberately disjoint from the legacy V9/V6 cache and from the
V8 setup book.  It reuses only V8's neutral frozen-source readers, exact cash
aggregation, pure one-minute state machine, and global portfolio ledger.

The selection protocol is sequential and fail closed:

* ``search`` independently selects zero or one config for each slot-side leg;
* ``evaluate-validation`` gates each selected leg once and advances only its
  passing subset;
* ``evaluate-test`` gates exactly that validation-passing subset once.

Pooled portfolio replays occur only after independent decisions and are
diagnostic: they cannot select, qualify, disqualify, replace, or resurrect a
leg.

Source coverage is a prerequisite, not an imputation rule.  The primary mode
requires complete coverage for the explicitly selected universe across all
three splits.  A TRAIN-derived rectangular panel is available only as a
watermarked diagnostic and is never eligible to cross into validation.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_v8_windowed_1m_entry_backtest as v8


OPTIMIZER_VERSION = "FNO_V9_0950_0955_HONEST_OPTIMIZER_20260820_V2"
OPTIMIZER_SCHEMA_VERSION = "fno_v9_0950_0955_honest_optimizer_v2"
CACHE_SCHEMA_VERSION = "fno_v9_0950_0955_same_session_cache_v2"
GRID_SCHEMA_VERSION = "fno_v9_0950_0955_preregistered_grid_48_per_leg_v1"
OBJECTIVE = (
    "INDEPENDENTLY_MAXIMIZE_EACH_LEG_TRAIN_FILLS_SUBJECT_TO_"
    "PF_STABILITY_AND_LEG_GUARDS"
)

V9_ROOT = common.FNO_ROOT / "strategy_research" / "v9_0950_0955_honest_v1"
CACHE_ROOT = V9_ROOT / "cache"
RUN_ROOT = V9_ROOT / "optimizer_runs"
CLAIM_REGISTRY_ROOT = V9_ROOT / "immutable_stage_claim_registry"

SLOTS: tuple[str, ...] = ("09:50", "09:55")
SIDES: tuple[str, ...] = ("LONG", "SHORT")
LEG_KEYS: tuple[str, ...] = tuple(
    f"{slot}_{side}" for slot in SLOTS for side in SIDES
)
REQUIRED_FUTURES_TIMES: tuple[str, ...] = (
    "09:45",
    "09:50",
    "09:55",
)

DEFAULT_TRAIN_FROM = "2026-05-27"
DEFAULT_TRAIN_THROUGH = "2026-07-09"
DEFAULT_VALIDATION_FROM = "2026-07-10"
DEFAULT_VALIDATION_THROUGH = "2026-07-23"
DEFAULT_TEST_FROM = "2026-07-24"
DEFAULT_TEST_THROUGH = "2026-07-31"

BASE_COST_BPS = 15.0
BASE_SLIPPAGE_BPS = 1.0
STRESS_COST_BPS = 20.0
STRESS_SLIPPAGE_BPS = 2.0
TARGET_EXPOSURE_RS = 50_000.0

CACHE_FLOOR_PRICE_CHANGE_PCT = 0.10
CACHE_FLOOR_OI_CHANGE_PCT = 0.05
CACHE_FLOOR_VOLUME_RATIO = 0.80

DIAGNOSTIC_WATERMARK = (
    "TRAIN_DERIVED_RECTANGULAR_PANEL; SOURCE_AVAILABILITY_SELECTED_POPULATION; "
    "NOT_FULL_UNIVERSE; TRAIN_ONLY_DIAGNOSTIC; VALIDATION_AND_TEST_LOCKED"
)
DIAGNOSTIC_RESEARCH_WATERMARK = (
    "EXPLICIT_DIAGNOSTIC_RESEARCH; NOT_QUALIFYING; VALIDATION_AND_TEST_LOCKED; "
    "NO_PROMOTION_OR_DEPLOYMENT_CLAIM"
)
LINEAGE_UNKNOWN_WATERMARK = (
    "LEGACY_LINEAGE_FLAGS_ABSENT; EXACT_GRID_MAY_BE_REPAIRED_BUT_ROW_LINEAGE_"
    "IS_UNCERTIFIED; DIAGNOSTIC_ONLY; PROSPECTIVE_CLEAN_SOURCE_REQUIRED"
)
MULTIPLE_TESTING_WARNING = (
    "TRAIN winner is selected from 48 preregistered configurations for each "
    "of four slot-side legs (192 visible leg hypotheses). Each leg is selected "
    "independently; the frozen subset's portfolio replay is diagnostic and "
    "cannot change a selected leg. Reported TRAIN PF is a "
    "selection statistic and is not an out-of-sample PF claim."
)


class DataEligibilityError(RuntimeError):
    """Frozen inputs cannot support the declared inference."""


class StageAccessError(RuntimeError):
    """A validation/test stage was accessed without an eligible frozen input."""


@dataclass(frozen=True)
class SplitContract:
    train_from: date
    train_through: date
    validation_from: date
    validation_through: date
    test_from: date
    test_through: date

    def validate(self) -> None:
        if self.train_through < self.train_from:
            raise ValueError("TRAIN window is reversed")
        if self.validation_through < self.validation_from:
            raise ValueError("VALIDATION window is reversed")
        if self.test_through < self.test_from:
            raise ValueError("TEST window is reversed")
        if not (
            self.train_through < self.validation_from
            and self.validation_through < self.test_from
        ):
            raise ValueError("TRAIN, VALIDATION and TEST must be disjoint/chronological")

    def payload(self) -> dict[str, str]:
        self.validate()
        return {key: value.isoformat() for key, value in asdict(self).items()}

    @property
    def split_hash(self) -> str:
        return common.canonical_json_sha256(self.payload())

    def bounds(self, split: str) -> tuple[date, date]:
        key = str(split).upper()
        if key == "TRAIN":
            return self.train_from, self.train_through
        if key == "VALIDATION":
            return self.validation_from, self.validation_through
        if key == "TEST":
            return self.test_from, self.test_through
        raise ValueError(f"unknown split {split!r}")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SplitContract":
        return cls(
            train_from=_parse_day(value["train_from"]),
            train_through=_parse_day(value["train_through"]),
            validation_from=_parse_day(value["validation_from"]),
            validation_through=_parse_day(value["validation_through"]),
            test_from=_parse_day(value["test_from"]),
            test_through=_parse_day(value["test_through"]),
        )


@dataclass(frozen=True)
class GateProfile:
    name: str
    price_change_pct: float
    oi_change_pct: float
    volume_ratio: float
    min_traded_value: float


@dataclass(frozen=True)
class EntryProfile:
    name: str
    max_confirmation_minute: int
    buffer_bps: float
    midpoint_invalidation: bool
    close_location_min: float | None
    body_ratio: float
    max_wick_ratio: float


@dataclass(frozen=True)
class BracketProfile:
    name: str
    stop_pct: float
    target_pct: float


GATE_PROFILES: tuple[GateProfile, ...] = (
    GateProfile("BROAD", 0.20, 0.10, 1.00, 0.0),
    GateProfile("MOVE", 0.40, 0.10, 1.00, 0.0),
    GateProfile("PARTICIPATION", 0.20, 0.50, 1.50, 0.0),
    GateProfile("LIQUID_QUALITY", 0.40, 0.50, 1.50, 25_000_000.0),
)

ENTRY_PROFILES: tuple[EntryProfile, ...] = (
    EntryProfile("S1_STRICT", 1, 0.0, False, None, 0.40, 0.50),
    EntryProfile("WINDOW_STRICT", 4, 0.0, False, None, 0.40, 0.50),
    EntryProfile("WINDOW_BUFFER_MID", 4, 2.0, True, None, 0.40, 0.50),
    # Direction and close beyond C5 remain mandatory in V8.  Only body/wick
    # morphology is removed to expose the high-trade-count boundary.
    EntryProfile("WINDOW_DIRECTIONAL", 4, 0.0, False, None, 0.00, 1.00),
)

BRACKETS: dict[str, tuple[BracketProfile, ...]] = {
    "LONG": (
        BracketProfile("TIGHT", 0.50, 1.50),
        BracketProfile("BASE", 1.00, 2.50),
        BracketProfile("WIDE_TARGET", 1.00, 3.00),
    ),
    "SHORT": (
        BracketProfile("TIGHT", 0.50, 1.50),
        BracketProfile("BASE", 1.00, 3.00),
        BracketProfile("WIDE_TARGET", 1.00, 4.00),
    ),
}


@dataclass(frozen=True)
class LegConfig:
    slot: str
    side: str
    gate: str
    entry: str
    bracket: str
    max_entries: int = 2

    def validate(self) -> None:
        if self.slot not in SLOTS:
            raise ValueError(f"slot must be one of {SLOTS}")
        if self.side not in SIDES:
            raise ValueError("side must be LONG or SHORT")
        if self.gate not in {value.name for value in GATE_PROFILES}:
            raise ValueError(f"unknown gate {self.gate!r}")
        if self.entry not in {value.name for value in ENTRY_PROFILES}:
            raise ValueError(f"unknown entry profile {self.entry!r}")
        if self.bracket not in {value.name for value in BRACKETS[self.side]}:
            raise ValueError(f"unknown bracket {self.bracket!r}")
        if self.max_entries != 2:
            raise ValueError("preregistered V9 grid fixes max_entries=2")

    @property
    def gate_profile(self) -> GateProfile:
        self.validate()
        return next(value for value in GATE_PROFILES if value.name == self.gate)

    @property
    def entry_profile(self) -> EntryProfile:
        self.validate()
        return next(value for value in ENTRY_PROFILES if value.name == self.entry)

    @property
    def bracket_profile(self) -> BracketProfile:
        self.validate()
        return next(value for value in BRACKETS[self.side] if value.name == self.bracket)

    @property
    def picker(self) -> str:
        return "max_liquidity" if self.side == "LONG" else "max_volume"

    @property
    def complexity(self) -> int:
        return (
            [value.name for value in GATE_PROFILES].index(self.gate)
            + [value.name for value in ENTRY_PROFILES].index(self.entry)
            + [value.name for value in BRACKETS[self.side]].index(self.bracket)
        )

    def payload(self) -> dict[str, Any]:
        gate = self.gate_profile
        entry = self.entry_profile
        bracket = self.bracket_profile
        return {
            **asdict(self),
            "picker": self.picker,
            "price_change_pct": gate.price_change_pct,
            "oi_change_pct": gate.oi_change_pct,
            "volume_ratio": gate.volume_ratio,
            "min_traded_value": gate.min_traded_value,
            "max_confirmation_minute": entry.max_confirmation_minute,
            "buffer_bps": entry.buffer_bps,
            "midpoint_invalidation": entry.midpoint_invalidation,
            "close_location_min": entry.close_location_min,
            "body_ratio": entry.body_ratio,
            "max_wick_ratio": entry.max_wick_ratio,
            "stop_pct": bracket.stop_pct,
            "target_pct": bracket.target_pct,
            "setup_id": self.setup_id,
        }

    @property
    def setup_id(self) -> str:
        return f"{self.slot}_{self.side}"

    @property
    def config_hash(self) -> str:
        return common.canonical_json_sha256(
            {"grid_schema_version": GRID_SCHEMA_VERSION, **self.payload()}
        )

    @classmethod
    def from_payload(cls, value: Mapping[str, Any]) -> "LegConfig":
        return cls(
            slot=str(value["slot"]),
            side=str(value["side"]),
            gate=str(value["gate"]),
            entry=str(value["entry"]),
            bracket=str(value["bracket"]),
            max_entries=int(value.get("max_entries", 2)),
        )


@dataclass(frozen=True)
class BookConfig:
    legs: tuple[LegConfig, ...]

    def validate(self) -> None:
        if not self.legs:
            raise ValueError("selected leg subset cannot be empty")
        for config in self.legs:
            config.validate()
        keys = tuple(config.setup_id for config in self.legs)
        expected = tuple(key for key in LEG_KEYS if key in set(keys))
        if len(keys) != len(set(keys)) or keys != expected:
            raise ValueError(
                "selected legs must be unique and ordered as a canonical subset "
                f"of {LEG_KEYS}"
            )

    @property
    def config_hash(self) -> str:
        self.validate()
        return common.canonical_json_sha256(
            {
                "grid_schema_version": GRID_SCHEMA_VERSION,
                "legs": {
                    config.setup_id: config.payload() for config in self.legs
                },
            }
        )

    def payload(self) -> dict[str, Any]:
        return {
            "legs": {config.setup_id: config.payload() for config in self.legs},
            "config_hash": self.config_hash,
        }

    @classmethod
    def from_payload(cls, value: Mapping[str, Any]) -> "BookConfig":
        encoded = dict(value["legs"])
        unknown = sorted(set(encoded) - set(LEG_KEYS))
        if unknown:
            raise ValueError(f"frozen selection contains unknown legs: {unknown}")
        book = cls(
            legs=tuple(
                LegConfig.from_payload(dict(encoded[key]))
                for key in LEG_KEYS
                if key in encoded
            )
        )
        if book.config_hash != str(value.get("config_hash", "")):
            raise AssertionError("frozen book hash is invalid")
        return book


@dataclass(frozen=True)
class Guards:
    train_min_fills_per_leg: int = 40
    validation_min_fills_per_leg: int = 15
    test_min_fills_per_leg: int = 10
    train_min_combined_pf: float = 1.50
    train_min_robust_pf: float = 1.10
    validation_min_combined_pf: float = 1.50
    test_min_combined_pf: float = 1.50
    train_min_leg_pf: float = 1.50
    train_min_leg_robust_pf: float = 1.20
    train_max_leg_top_day_share: float = 0.25
    validation_min_leg_pf: float = 1.50
    validation_max_leg_top_day_share: float = 0.35
    test_min_leg_pf: float = 1.50
    min_side_pf: float = 1.00
    min_stress_pf: float = 1.00
    train_min_active_days: int = 15
    validation_min_active_days: int = 8
    test_min_active_days: int = 6
    max_top_day_share: float = 0.45
    min_positive_blocks: int = 2


def _parse_day(value: Any) -> date:
    parsed = pd.Timestamp(value)
    if pd.isna(parsed):
        raise ValueError(f"invalid date {value!r}")
    return parsed.date()


def default_split_contract() -> SplitContract:
    return SplitContract(
        _parse_day(DEFAULT_TRAIN_FROM),
        _parse_day(DEFAULT_TRAIN_THROUGH),
        _parse_day(DEFAULT_VALIDATION_FROM),
        _parse_day(DEFAULT_VALIDATION_THROUGH),
        _parse_day(DEFAULT_TEST_FROM),
        _parse_day(DEFAULT_TEST_THROUGH),
    )


def generate_leg_grid(slot: str, side: str) -> tuple[LegConfig, ...]:
    side_key = str(side).upper()
    configs = tuple(
        LegConfig(slot, side_key, gate.name, entry.name, bracket.name)
        for gate in GATE_PROFILES
        for entry in ENTRY_PROFILES
        for bracket in BRACKETS[side_key]
    )
    if len(configs) != 48 or len({value.config_hash for value in configs}) != 48:
        raise AssertionError("V9 grid must contain 48 unique configs per leg")
    return configs


LEG_GRIDS: dict[str, tuple[LegConfig, ...]] = {
    key: generate_leg_grid(*key.split("_", 1)) for key in LEG_KEYS
}
GRID_FAMILY_SHA256 = common.canonical_json_sha256(
    {
        "schema_version": GRID_SCHEMA_VERSION,
        "legs": {
            key: [value.config_hash for value in LEG_GRIDS[key]] for key in LEG_KEYS
        },
    }
)


def split_sessions(contract: SplitContract) -> dict[str, tuple[date, ...]]:
    contract.validate()
    values = {
        split: tuple(v8.expected_regular_session_dates(*contract.bounds(split)))
        for split in ("TRAIN", "VALIDATION", "TEST")
    }
    if any(not split for split in values.values()):
        raise DataEligibilityError("each chronological split must contain a session")
    return values


def diagnostic_contract_reasons(
    *,
    contract: SplitContract,
    coverage_mode: str,
    requested_symbols: Sequence[str] | None,
) -> list[str]:
    """Return immutable reasons a search cannot be a qualifying primary run."""

    reasons: list[str] = []
    if contract != default_split_contract():
        reasons.append("CUSTOM_SPLIT_CONTRACT")
    if requested_symbols:
        reasons.append("SYMBOL_SUBSET")
    if str(coverage_mode).upper().replace("-", "_") != "FULL_UNIVERSE":
        reasons.append("TRAIN_DERIVED_RECTANGULAR_PANEL")
    return reasons


def setup_for_config(config: LegConfig) -> v8.V8Setup:
    gate = config.gate_profile
    entry = config.entry_profile
    bracket = config.bracket_profile
    return v8.V8Setup(
        signal_end=config.slot,
        side=config.side,
        max_entries=config.max_entries,
        picker=config.picker,
        price_change_pct=gate.price_change_pct,
        oi_change_pct=gate.oi_change_pct,
        volume_ratio=gate.volume_ratio,
        body_ratio=entry.body_ratio,
        max_wick_ratio=entry.max_wick_ratio,
        min_traded_value=gate.min_traded_value,
        stop_pct=bracket.stop_pct,
        target_pct=bracket.target_pct,
    )


def entry_policy(config: LegConfig, *, stress: bool) -> v8.EntryPolicy:
    profile = config.entry_profile
    return v8.EntryPolicy(
        buffer_bps=profile.buffer_bps,
        max_confirmation_minute=profile.max_confirmation_minute,
        entry_expiry_minute=5,
        close_location_min=profile.close_location_min,
        cost_bps=STRESS_COST_BPS if stress else BASE_COST_BPS,
        slippage_bps=STRESS_SLIPPAGE_BPS if stress else BASE_SLIPPAGE_BPS,
        midpoint_invalidation=profile.midpoint_invalidation,
        post_confirmation_cancel=True,
        allow_cap_reassignment=True,
        same_bar_policy="STOP_FIRST",
        square_off="15:30",
        eod_policy="EXACT_SQUARE_OFF",
    )


def _source_sha256() -> str:
    return provenance.sha256_file(Path(__file__))


def _safe_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _safe_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe_json(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, (date, datetime, pd.Timestamp)):
        return value.isoformat()
    if not isinstance(value, str) and pd.isna(value):
        return None
    return value


def _empty_candidates() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "candidate_id",
            "session_date",
            "signal_time",
            "signal_end",
            "setup_id",
            "side",
            "symbol",
            "futures_symbol",
            "equity_instrument_token",
            "futures_instrument_token",
            "tick_size",
            "lot_size",
            "five_min_open",
            "five_min_high",
            "five_min_low",
            "five_min_close",
            "five_min_volume",
            "ema9",
            "ema20",
            "ema50",
            "price_change_pct",
            "oi",
            "prev_oi",
            "oi_change_pct",
            "volume_ratio",
            "traded_value",
            "cache_schema_version",
        ]
    )


def _empty_paths() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "candidate_id",
            "session_date",
            "signal_time",
            "setup_id",
            "side",
            "symbol",
            "bar_ts",
            "minute_index",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "gap_filled",
            "opening_snapshot",
            "provisional_stale",
            "legacy_lineage_flags_absent",
            "path_policy_version",
        ]
    )


def _baseline_setup(slot: str, side: str) -> v8.V8Setup:
    return v8.V8Setup(
        signal_end=slot,
        side=side,
        max_entries=2,
        picker="max_liquidity" if side == "LONG" else "max_volume",
        price_change_pct=CACHE_FLOOR_PRICE_CHANGE_PCT,
        oi_change_pct=CACHE_FLOOR_OI_CHANGE_PCT,
        volume_ratio=CACHE_FLOOR_VOLUME_RATIO,
        body_ratio=0.0,
        max_wick_ratio=1.0,
        min_traded_value=0.0,
        stop_pct=1.0,
        target_pct=2.0,
    )


def _candidate_from_values(
    row: Mapping[str, Any], contract: Mapping[str, Any], *, slot: str, side: str
) -> v8.CandidateInput:
    return v8.CandidateInput(
        symbol=str(contract["equity_symbol"]).upper().strip(),
        signal_time=pd.Timestamp(row["ts"]),
        five_min_open=float(row["open"]),
        five_min_high=float(row["high"]),
        five_min_low=float(row["low"]),
        five_min_close=float(row["close"]),
        price_change_pct=float(row["price_change_pct"]),
        oi_change_pct=float(row["oi_change_pct"]),
        volume_ratio=float(row["volume_ratio"]),
        traded_value=float(row["traded_value"]),
        tick_size=float(contract["equity_tick_size"]),
        futures_symbol=str(contract["futures_tradingsymbol"]).upper().strip(),
        futures_instrument_token=int(contract["futures_instrument_token"]),
        equity_instrument_token=int(contract["equity_instrument_token"]),
        lot_size=1,
        five_min_volume=float(row["volume"]),
        ema9=float(row["ema9"]),
        ema20=float(row["ema20"]),
        ema50=float(row["ema50"]),
        oi=float(row["oi"]),
        prev_oi=float(row["prev_oi"]),
    )


def _broad_rows(joined: pd.DataFrame, *, slot: str, side: str) -> pd.DataFrame:
    rows = joined.loc[joined["ts"].dt.strftime("%H:%M").eq(slot)].copy()
    if rows.empty:
        return rows
    if side == "LONG":
        ema = rows["ema9"].gt(rows["ema20"]) & rows["ema20"].gt(rows["ema50"])
        move = rows["price_change_pct"].ge(CACHE_FLOOR_PRICE_CHANGE_PCT)
    else:
        ema = rows["ema9"].lt(rows["ema20"]) & rows["ema20"].lt(rows["ema50"])
        move = rows["price_change_pct"].le(-CACHE_FLOOR_PRICE_CHANGE_PCT)
    eligible = (
        ema
        & move
        & rows["oi"].gt(rows["prev_oi"])
        & rows["oi_change_pct"].ge(CACHE_FLOOR_OI_CHANGE_PCT)
        & rows["volume_ratio"].ge(CACHE_FLOOR_VOLUME_RATIO)
    )
    return rows.loc[eligible].copy()


def _coverage_row(
    minute: pd.DataFrame,
    futures: pd.DataFrame,
    *,
    symbol: str,
    futures_symbol: str,
    expected_sessions: Sequence[date],
) -> dict[str, Any]:
    expected_set = set(expected_sessions)
    complete: list[str] = []
    for session in expected_sessions:
        expected_minutes = pd.date_range(
            pd.Timestamp(f"{session.isoformat()} 09:16", tz=common.IST),
            pd.Timestamp(f"{session.isoformat()} 15:30", tz=common.IST),
            freq="1min",
        )
        day_rows = minute.loc[
            minute["ts"].dt.date.eq(session)
            & minute["ts"].between(expected_minutes[0], expected_minutes[-1])
        ]
        cash_complete = bool(
            len(day_rows) == len(expected_minutes)
            and set(day_rows["ts"]) == set(expected_minutes)
            and v8._exact_minute_end_labels(day_rows["ts"]).all()
            and v8._valid_minute_rows(day_rows).all()
        )
        expected_futures = [
            pd.Timestamp(f"{session.isoformat()} {clock}", tz=common.IST)
            for clock in REQUIRED_FUTURES_TIMES
        ]
        future_rows = futures.loc[futures["ts"].isin(expected_futures)]
        futures_complete = bool(
            len(future_rows) == len(expected_futures)
            and set(future_rows["ts"]) == set(expected_futures)
            and future_rows["oi_valid"].all()
        )
        if futures_complete:
            by_ts = {row["ts"]: row for row in future_rows.to_dict("records")}
            for timestamp in expected_futures[1:]:
                if not math.isfinite(float(by_ts[timestamp]["oi_change_pct"])):
                    futures_complete = False
                    break
        if cash_complete and futures_complete:
            complete.append(session.isoformat())
    observed_days = set(minute["ts"].dt.date) | set(futures["ts"].dt.date)
    unexpected = sorted(
        value.isoformat()
        for value in observed_days
        if min(expected_sessions) <= value <= max(expected_sessions)
        and value not in expected_set
        and value.isoformat() not in set(v8.NSE_FO_NONSTANDARD_SESSIONS_EXCLUDED)
    )
    expected_json = [value.isoformat() for value in expected_sessions]
    incomplete = sorted(set(expected_json) - set(complete))
    return {
        "symbol": symbol,
        "futures_symbol": futures_symbol,
        "session_dates_json": json.dumps(expected_json, separators=(",", ":")),
        "source_complete_session_dates_json": json.dumps(
            complete, separators=(",", ":")
        ),
        "source_incomplete_session_dates_json": json.dumps(
            incomplete, separators=(",", ":")
        ),
        "source_complete_session_count": len(complete),
        "source_incomplete_session_count": len(incomplete),
        "unexpected_session_dates_json": json.dumps(
            unexpected, separators=(",", ":")
        ),
        "unexpected_session_count": len(unexpected),
        "exact_cash_grid": "09:16..15:30_EVERY_1M",
        "required_futures_times_json": json.dumps(
            REQUIRED_FUTURES_TIMES, separators=(",", ":")
        ),
        "legacy_lineage_flags_absent": bool(
            minute["legacy_lineage_flags_absent"].all()
        ),
    }


def build_candidate_cache(
    mapped_universe: pd.DataFrame,
    source_lookup: Mapping[tuple[str, str], Path],
    *,
    from_day: date,
    through_day: date,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build broad 09:50/09:55 candidates and same-session paths."""

    expected_sessions = v8.expected_regular_session_dates(from_day, through_day)
    full_calendar = set(v8.expected_regular_session_dates("2026-01-01", "2026-12-31"))
    candidate_records: list[dict[str, Any]] = []
    path_parts: list[pd.DataFrame] = []
    coverage_records: list[dict[str, Any]] = []
    path_columns = list(_empty_paths().columns)

    for contract in mapped_universe.to_dict("records"):
        symbol = str(contract["equity_symbol"]).upper().strip()
        futures_symbol = str(contract["futures_tradingsymbol"]).upper().strip()
        equity_path = source_lookup.get(("NSE_EQUITY_1M", symbol))
        futures_path = source_lookup.get(("NFO_FUTURES_5M", futures_symbol))
        if equity_path is None or futures_path is None:
            raise FileNotFoundError(
                f"frozen source lookup lacks {symbol}/{futures_symbol}"
            )
        minute = v8.load_equity_minute_history(equity_path, symbol=symbol)
        futures = v8.load_futures_five_minute_history(
            futures_path,
            symbol=futures_symbol,
            expected_instrument_token=int(contract["futures_instrument_token"]),
            expected_expiry=contract["expiry"],
            expected_contract_month=str(contract["contract_month"]),
        )
        coverage_records.append(
            _coverage_row(
                minute,
                futures,
                symbol=symbol,
                futures_symbol=futures_symbol,
                expected_sessions=expected_sessions,
            )
        )
        equity_five = v8.aggregate_equity_one_minute_to_five_minute(minute)
        equity_feature_input = equity_five.loc[
            equity_five["ts"].dt.date.isin(full_calendar)
        ].copy()
        futures_feature_input = futures.loc[
            futures["ts"].dt.date.isin(full_calendar)
        ].copy()
        joined = v8.join_cash_features_with_futures_oi(
            equity_feature_input, futures_feature_input
        )
        if joined.empty:
            continue
        joined = joined.loc[
            joined["ts"].dt.date.between(from_day, through_day)
            & joined["ts"].dt.date.isin(set(expected_sessions))
        ].copy()
        symbol_records: list[dict[str, Any]] = []
        for slot in SLOTS:
            for side in SIDES:
                setup = _baseline_setup(slot, side)
                for row in _broad_rows(joined, slot=slot, side=side).to_dict("records"):
                    candidate = _candidate_from_values(row, contract, slot=slot, side=side)
                    if not v8.five_minute_candidate_passes(setup, candidate):
                        raise AssertionError("broad cache admitted an invalid candidate")
                    candidate_id = (
                        f"{candidate.session_date.isoformat()}|{setup.setup_id}|{symbol}"
                    )
                    record = {
                        "candidate_id": candidate_id,
                        "session_date": candidate.session_date,
                        "signal_time": candidate.signal_ts,
                        "signal_end": slot,
                        "setup_id": setup.setup_id,
                        "side": side,
                        "symbol": symbol,
                        "futures_symbol": futures_symbol,
                        "equity_instrument_token": candidate.equity_instrument_token,
                        "futures_instrument_token": candidate.futures_instrument_token,
                        "tick_size": candidate.tick_size,
                        "lot_size": candidate.lot_size,
                        "five_min_open": candidate.five_min_open,
                        "five_min_high": candidate.five_min_high,
                        "five_min_low": candidate.five_min_low,
                        "five_min_close": candidate.five_min_close,
                        "five_min_volume": candidate.five_min_volume,
                        "ema9": candidate.ema9,
                        "ema20": candidate.ema20,
                        "ema50": candidate.ema50,
                        "price_change_pct": candidate.price_change_pct,
                        "oi": candidate.oi,
                        "prev_oi": candidate.prev_oi,
                        "oi_change_pct": candidate.oi_change_pct,
                        "volume_ratio": candidate.volume_ratio,
                        "traded_value": candidate.traded_value,
                        "cache_schema_version": CACHE_SCHEMA_VERSION,
                    }
                    candidate_records.append(record)
                    symbol_records.append(record)
        for record in symbol_records:
            signal_ts = pd.Timestamp(record["signal_time"])
            path = minute.loc[
                minute["ts"].dt.date.eq(signal_ts.date())
                & minute["ts"].gt(signal_ts)
                & minute["ts"].le(
                    pd.Timestamp(
                        f"{signal_ts.date().isoformat()} 15:30", tz=common.IST
                    )
                )
                & v8._exact_minute_end_labels(minute["ts"])
            ].copy()
            if path.empty:
                continue
            path.insert(0, "candidate_id", record["candidate_id"])
            path.insert(1, "session_date", signal_ts.date())
            path.insert(2, "signal_time", signal_ts)
            path.insert(3, "setup_id", record["setup_id"])
            path.insert(4, "side", record["side"])
            path["minute_index"] = (
                (path["ts"] - signal_ts).dt.total_seconds().div(60).astype(int)
            )
            path = path.rename(columns={"ts": "bar_ts"})
            path["path_policy_version"] = v8.PATH_POLICY_VERSION
            path_parts.append(path[path_columns])

    candidates = (
        pd.DataFrame(candidate_records) if candidate_records else _empty_candidates()
    )
    if not candidates.empty:
        candidates = candidates.sort_values(
            ["session_date", "setup_id", "symbol"], kind="stable"
        ).reset_index(drop=True)
        if candidates["candidate_id"].duplicated().any():
            raise AssertionError("V9 broad candidate IDs are not unique")
        candidates = candidates[list(_empty_candidates().columns)]
    paths = pd.concat(path_parts, ignore_index=True) if path_parts else _empty_paths()
    if not paths.empty:
        paths = paths.sort_values(["candidate_id", "bar_ts"], kind="stable")
        if paths.duplicated(["candidate_id", "bar_ts"]).any():
            raise AssertionError("V9 path cache contains duplicate timestamps")
        if not paths["bar_ts"].dt.date.eq(paths["session_date"]).all():
            raise AssertionError("V9 path cache crossed a session boundary")
    coverage = pd.DataFrame(coverage_records).sort_values("symbol", kind="stable")
    return candidates.reset_index(drop=True), paths.reset_index(drop=True), coverage.reset_index(drop=True)


def derive_coverage(
    coverage: pd.DataFrame,
    *,
    symbols: Sequence[str],
    sessions: Sequence[date],
) -> dict[str, Any]:
    selected = sorted({str(value).upper().strip() for value in symbols})
    wanted = {value.isoformat() for value in sessions}
    by_symbol = {
        str(row["symbol"]).upper().strip(): set(
            json.loads(str(row["source_complete_session_dates_json"]))
        )
        for row in coverage.to_dict("records")
    }
    lineage_unknown_symbols = sorted(
        str(row["symbol"]).upper().strip()
        for row in coverage.to_dict("records")
        if str(row["symbol"]).upper().strip() in selected
        and bool(row.get("legacy_lineage_flags_absent", True))
    )
    unexpected_pairs: list[str] = []
    lower = min(sessions)
    upper = max(sessions)
    for row in coverage.to_dict("records"):
        symbol = str(row["symbol"]).upper().strip()
        if symbol not in selected:
            continue
        for raw_day in json.loads(str(row["unexpected_session_dates_json"])):
            observed_day = _parse_day(raw_day)
            if lower <= observed_day <= upper:
                unexpected_pairs.append(f"{symbol}|{observed_day.isoformat()}")
    missing_pairs: list[str] = []
    for symbol in selected:
        complete = by_symbol.get(symbol, set())
        missing_pairs.extend(
            f"{symbol}|{session}" for session in sorted(wanted - complete)
        )
    expected = len(selected) * len(wanted)
    exact_grid_pass = not missing_pairs and not unexpected_pairs
    lineage_certified = not lineage_unknown_symbols
    return {
        "symbol_count": len(selected),
        "session_count": len(wanted),
        "expected_symbol_sessions": expected,
        "complete_symbol_sessions": expected - len(missing_pairs),
        "incomplete_symbol_sessions": len(missing_pairs),
        "incomplete_symbols": sorted(
            {value.split("|", 1)[0] for value in missing_pairs}
        ),
        "incomplete_examples": missing_pairs[:100],
        "unexpected_source_symbol_sessions": len(unexpected_pairs),
        "unexpected_examples": unexpected_pairs[:100],
        "legacy_lineage_flags_absent_symbol_count": len(
            lineage_unknown_symbols
        ),
        "legacy_lineage_flags_absent_symbols": lineage_unknown_symbols,
        "lineage_certified": lineage_certified,
        "exact_grid_pass": exact_grid_pass,
        # ``pass`` remains the exact-grid/source-completeness gate so an
        # explicitly watermarked diagnostic can study repaired historical
        # grids. ``qualifying_pass`` additionally requires certified lineage.
        "pass": exact_grid_pass,
        "qualifying_pass": exact_grid_pass and lineage_certified,
    }


def derive_train_panel(
    candidates: pd.DataFrame,
    paths: pd.DataFrame,
    coverage: pd.DataFrame,
    *,
    train_sessions: Sequence[date],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    train_required = {value.isoformat() for value in train_sessions}
    panel_symbols = sorted(
        str(row["symbol"]).upper().strip()
        for row in coverage.to_dict("records")
        if train_required.issubset(
            set(json.loads(str(row["source_complete_session_dates_json"])))
        )
    )
    if not panel_symbols:
        raise DataEligibilityError("no symbol is complete across every TRAIN session")
    panel_set = set(panel_symbols)
    selected_candidates = candidates.loc[
        candidates["symbol"].astype(str).str.upper().isin(panel_set)
    ].copy()
    ids = set(selected_candidates["candidate_id"].astype(str))
    selected_paths = paths.loc[paths["candidate_id"].astype(str).isin(ids)].copy()
    selected_coverage = coverage.loc[
        coverage["symbol"].astype(str).str.upper().isin(panel_set)
    ].copy()
    metadata = {
        "policy": "TRAIN_ONLY_SOURCE_COMPLETE_PANEL_V1",
        "panel_symbols": panel_symbols,
        "panel_symbol_count": len(panel_symbols),
        "panel_symbol_set_sha256": common.symbol_set_sha256(panel_symbols),
        "panel_hash": common.canonical_json_sha256(
            {
                "policy": "TRAIN_ONLY_SOURCE_COMPLETE_PANEL_V1",
                "train_sessions": sorted(train_required),
                "symbols": panel_symbols,
            }
        ),
        "watermark": DIAGNOSTIC_WATERMARK,
    }
    return (
        selected_candidates.reset_index(drop=True),
        selected_paths.reset_index(drop=True),
        selected_coverage.reset_index(drop=True),
        metadata,
    )


def _cache_paths(fingerprint: str) -> dict[str, Path]:
    root = CACHE_ROOT / fingerprint[:20]
    return {
        "root": root,
        "manifest": root / "manifest.json",
        "candidates": root / "broad_candidates.parquet",
        "paths": root / "same_session_paths.parquet",
        "coverage": root / "coverage.parquet",
    }


def _cache_contract(
    *,
    source_snapshot: Mapping[str, Any],
    inventory: Mapping[str, Any],
    universe: Mapping[str, Any],
    symbols: Sequence[str],
    contract: SplitContract,
) -> dict[str, Any]:
    sessions = split_sessions(contract)
    all_sessions = tuple(
        value
        for split_name in ("TRAIN", "VALIDATION", "TEST")
        for value in sessions[split_name]
    )
    return {
        "schema_version": CACHE_SCHEMA_VERSION,
        "optimizer_version": OPTIMIZER_VERSION,
        "optimizer_source_sha256": _source_sha256(),
        "v8_source_sha256": provenance.sha256_file(Path(v8.__file__)),
        "v8_execution_seams": {
            "source_loader": "load_validated_source_contract",
            "cash_aggregation": "aggregate_equity_one_minute_to_five_minute",
            "feature_join": "join_cash_features_with_futures_oi",
            "state_machine": "simulate_setup_window",
            "portfolio_ledger": "apply_global_portfolio_constraints",
        },
        "slots": list(SLOTS),
        "sides": list(SIDES),
        "required_futures_signal_clocks": list(REQUIRED_FUTURES_TIMES),
        "coverage_rule": (
            "EXACT_VALID_CASH_09:16..15:30_AND_EXACT_VALID_OI_"
            "PREDECESSOR_PLUS_SIGNAL_CLOCKS"
        ),
        "broad_gate": {
            "price_change_pct": CACHE_FLOOR_PRICE_CHANGE_PCT,
            "oi_change_pct": CACHE_FLOOR_OI_CHANGE_PCT,
            "volume_ratio": CACHE_FLOOR_VOLUME_RATIO,
            "ema_structure": "STRICT_SIDE_ORDER",
        },
        "split_contract": contract.payload(),
        "session_dates": [value.isoformat() for value in all_sessions],
        "session_calendar": v8.nse_fo_calendar_payload(),
        "symbols": list(symbols),
        "universe": dict(universe),
        "source_snapshot_fingerprint": source_snapshot.get(
            "snapshot_fingerprint", ""
        ),
        "source_inventory_sha256": inventory.get("inventory_sha256", ""),
        "source_fingerprint": inventory.get("source_fingerprint", ""),
        "source_limitations": [
            "STATIC_2026_08_11_UNIVERSE_SURVIVORSHIP_RESEARCH",
            "STATIC_26AUG_FUTURES_OI_NOT_POINT_IN_TIME_ROLLING",
            "SOURCE_SNAPSHOT_IS_PER_FILE_STABLE_NOT_GLOBAL_TRANSACTION",
        ],
    }


def load_or_build_cache(
    *,
    source_snapshot_path: Path | str,
    contract: SplitContract,
    symbols: Sequence[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any], Path]:
    """Load or immutably construct the V9-specific broad candidate cache."""

    mapped, universe, snapshot, inventory, lookup = v8.load_validated_source_contract(
        source_snapshot_path, symbols=symbols
    )
    selected_symbols = sorted(
        mapped["equity_symbol"].astype(str).str.upper().str.strip().tolist()
    )
    cache_contract = _cache_contract(
        source_snapshot=snapshot,
        inventory=inventory,
        universe=universe,
        symbols=selected_symbols,
        contract=contract,
    )
    fingerprint = common.canonical_json_sha256(cache_contract)
    paths = _cache_paths(fingerprint)
    manifest_path = paths["manifest"]
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        artifacts = dict(manifest.get("artifacts", {}))
        valid = bool(
            manifest.get("schema_version") == CACHE_SCHEMA_VERSION
            and manifest.get("input_fingerprint") == fingerprint
            and common.canonical_json_sha256(manifest.get("input_contract", {}))
            == common.canonical_json_sha256(cache_contract)
            and all(
                Path(str(dict(artifacts.get(name, {})).get("path", ""))).resolve()
                == paths[name].resolve()
                and provenance.artifact_matches(paths[name], artifacts.get(name, {}))
                for name in ("candidates", "paths", "coverage")
            )
        )
        if not valid:
            raise AssertionError("V9 frozen cache failed integrity validation")
        candidates = pd.read_parquet(paths["candidates"])
        minute_paths = pd.read_parquet(paths["paths"])
        coverage = pd.read_parquet(paths["coverage"])
        all_sessions = tuple(
            value
            for split_name in ("TRAIN", "VALIDATION", "TEST")
            for value in split_sessions(contract)[split_name]
        )
        derived = derive_coverage(
            coverage, symbols=selected_symbols, sessions=all_sessions
        )
        if (
            int(manifest.get("candidate_count", -1)) != len(candidates)
            or int(manifest.get("path_row_count", -1)) != len(minute_paths)
            or int(manifest.get("coverage_row_count", -1)) != len(coverage)
            or common.canonical_json_sha256(manifest.get("coverage_summary", {}))
            != common.canonical_json_sha256(derived)
        ):
            raise AssertionError("V9 frozen cache manifest counts changed")
        return candidates, minute_paths, coverage, manifest, manifest_path

    if paths["root"].exists() and any(paths["root"].iterdir()):
        raise AssertionError("V9 cache directory has unauthenticated partial artifacts")
    paths["root"].mkdir(parents=True, exist_ok=True)
    candidates, minute_paths, coverage = build_candidate_cache(
        mapped,
        lookup,
        from_day=contract.train_from,
        through_day=contract.test_through,
    )
    if _source_sha256() != str(cache_contract["optimizer_source_sha256"]):
        raise RuntimeError("optimizer source changed during V9 cache construction")
    if provenance.sha256_file(Path(v8.__file__)) != str(
        cache_contract["v8_source_sha256"]
    ):
        raise RuntimeError("V8 engine changed during V9 cache construction")
    common.atomic_write_parquet(candidates, paths["candidates"])
    common.atomic_write_parquet(minute_paths, paths["paths"])
    common.atomic_write_parquet(coverage, paths["coverage"])
    artifacts = {
        name: provenance.artifact_record(paths[name])
        for name in ("candidates", "paths", "coverage")
    }
    all_sessions = tuple(
        value
        for split_name in ("TRAIN", "VALIDATION", "TEST")
        for value in split_sessions(contract)[split_name]
    )
    coverage_summary = derive_coverage(
        coverage, symbols=selected_symbols, sessions=all_sessions
    )
    manifest = {
        "schema_version": CACHE_SCHEMA_VERSION,
        "created_at_ist": common.now_ist().isoformat(timespec="microseconds"),
        "input_fingerprint": fingerprint,
        "input_contract": cache_contract,
        "universe": universe,
        "source_snapshot": {
            "manifest_path": snapshot.get("manifest_path", ""),
            "snapshot_fingerprint": snapshot.get("snapshot_fingerprint", ""),
            "physical_copy": bool(snapshot.get("physical_copy")),
        },
        "source_inventory": inventory,
        "candidate_count": len(candidates),
        "path_row_count": len(minute_paths),
        "coverage_row_count": len(coverage),
        "coverage_summary": coverage_summary,
        "artifacts": artifacts,
    }
    if _source_sha256() != str(cache_contract["optimizer_source_sha256"]):
        raise RuntimeError("optimizer source changed before cache manifest freeze")
    if provenance.sha256_file(Path(v8.__file__)) != str(
        cache_contract["v8_source_sha256"]
    ):
        raise RuntimeError("V8 engine changed before cache manifest freeze")
    provenance.write_immutable_json(manifest_path, _safe_json(manifest))
    return candidates, minute_paths, coverage, manifest, manifest_path


@dataclass
class PreparedDataset:
    candidates: pd.DataFrame
    paths_by_candidate: dict[str, tuple[v8.MinuteBar, ...]]
    session_dates: tuple[date, ...]


def prepare_dataset(
    candidates: pd.DataFrame,
    minute_paths: pd.DataFrame,
    session_dates: Sequence[date],
) -> PreparedDataset:
    wanted = set(session_dates)
    selected = candidates.loc[
        candidates["session_date"].map(_parse_day).isin(wanted)
    ].copy()
    ids = set(selected["candidate_id"].astype(str))
    selected_paths = minute_paths.loc[
        minute_paths["candidate_id"].astype(str).isin(ids)
    ]
    path_map = {
        str(candidate_id): tuple(v8._minute_bars_from_cache(group))
        for candidate_id, group in selected_paths.groupby("candidate_id", sort=False)
    }
    return PreparedDataset(
        selected.reset_index(drop=True), path_map, tuple(session_dates)
    )


def _picker_value(config: LegConfig, row: Mapping[str, Any]) -> float:
    if config.picker == "max_liquidity":
        return float(row["traded_value"])
    if config.picker == "max_volume":
        return float(row["volume_ratio"])
    raise ValueError(f"unsupported picker {config.picker!r}")


def _filter_and_rank_occurrence(
    group: pd.DataFrame, config: LegConfig
) -> pd.DataFrame:
    gate = config.gate_profile
    price = pd.to_numeric(group["price_change_pct"], errors="coerce")
    if config.side == "LONG":
        price_ok = price.ge(gate.price_change_pct)
    else:
        price_ok = price.le(-gate.price_change_pct)
    selected = group.loc[
        price_ok
        & pd.to_numeric(group["oi_change_pct"], errors="coerce").ge(
            gate.oi_change_pct
        )
        & pd.to_numeric(group["volume_ratio"], errors="coerce").ge(
            gate.volume_ratio
        )
        & pd.to_numeric(group["traded_value"], errors="coerce").ge(
            gate.min_traded_value
        )
    ].copy()
    if selected.empty:
        return selected
    selected["picker"] = config.picker
    selected["picker_value"] = [
        _picker_value(config, row) for row in selected.to_dict("records")
    ]
    selected = selected.sort_values(
        ["picker_value", "traded_value", "symbol"],
        ascending=[False, False, True],
        kind="stable",
    ).reset_index(drop=True)
    selected["frozen_rank"] = np.arange(1, len(selected) + 1)
    return selected


def _attach_economics(
    audit: pd.DataFrame, policy: v8.EntryPolicy
) -> pd.DataFrame:
    if audit.empty:
        return audit.copy()
    out = audit.copy()
    filled = out["entry_price"].notna()
    out["filled"] = filled
    out["quantity"] = 0
    valid_entry = filled & pd.to_numeric(out["entry_price"], errors="coerce").gt(0)
    out.loc[valid_entry, "quantity"] = np.floor(
        TARGET_EXPOSURE_RS
        / pd.to_numeric(out.loc[valid_entry, "entry_price"], errors="coerce")
    ).astype(int)
    for column in (
        "position_notional_rs",
        "gross_pnl_rs",
        "estimated_cost_rs",
        "net_pnl_rs",
    ):
        out[column] = np.nan
    out.loc[valid_entry, "position_notional_rs"] = (
        out.loc[valid_entry, "entry_price"].astype(float)
        * out.loc[valid_entry, "quantity"].astype(float)
    )
    closed = (
        valid_entry
        & out["exit_price"].notna()
        & pd.to_numeric(out["net_return_pct"], errors="coerce").notna()
    )
    direction = np.where(out.loc[closed, "side"].eq("LONG"), 1.0, -1.0)
    quantity = out.loc[closed, "quantity"].astype(float)
    entry = out.loc[closed, "entry_price"].astype(float)
    exit_price = out.loc[closed, "exit_price"].astype(float)
    gross = direction * (exit_price - entry) * quantity
    costs = entry * quantity * policy.cost_bps / 10_000.0
    out.loc[closed, "gross_pnl_rs"] = gross
    out.loc[closed, "estimated_cost_rs"] = costs
    out.loc[closed, "net_pnl_rs"] = gross - costs
    out["cost_bps"] = policy.cost_bps
    out["slippage_bps"] = policy.slippage_bps
    return out


def run_leg_preportfolio(
    prepared: PreparedDataset,
    config: LegConfig,
    *,
    stress: bool,
) -> pd.DataFrame:
    config.validate()
    setup = setup_for_config(config)
    policy = entry_policy(config, stress=stress)
    selected = prepared.candidates.loc[
        prepared.candidates["setup_id"].astype(str).eq(config.setup_id)
    ]
    parts: list[pd.DataFrame] = []
    for _, group in selected.groupby("session_date", sort=True):
        ranked = _filter_and_rank_occurrence(group, config)
        if ranked.empty:
            continue
        candidates = [
            v8._candidate_from_cache_row(row) for row in ranked.to_dict("records")
        ]
        invalid = [
            candidate.symbol
            for candidate in candidates
            if not v8.five_minute_candidate_passes(setup, candidate)
        ]
        if invalid:
            raise AssertionError(f"config filter admitted invalid candidates: {invalid}")
        bars = {
            str(row["symbol"]): prepared.paths_by_candidate.get(
                str(row["candidate_id"]), tuple()
            )
            for row in ranked.to_dict("records")
        }
        audit = v8.simulate_setup_window(setup, candidates, bars, policy)
        if audit.empty:
            continue
        audit = audit.merge(
            ranked[["candidate_id", "frozen_rank", "picker", "picker_value"]],
            on="candidate_id",
            how="left",
            validate="one_to_one",
        )
        audit["config_hash"] = config.config_hash
        audit["max_confirmation_minute"] = policy.max_confirmation_minute
        audit["midpoint_invalidation"] = policy.midpoint_invalidation
        audit["close_location_min"] = policy.close_location_min
        audit["eod_policy"] = policy.eod_policy
        parts.append(audit)
    if not parts:
        return pd.DataFrame()
    return _attach_economics(pd.concat(parts, ignore_index=True), policy)


def constrain_book(parts: Iterable[pd.DataFrame]) -> pd.DataFrame:
    nonempty = [value for value in parts if not value.empty]
    if not nonempty:
        return pd.DataFrame()
    combined = pd.concat(nonempty, ignore_index=True, sort=False)
    constrained = v8.apply_global_portfolio_constraints(
        combined, v8.PortfolioPolicy(target_exposure_per_entry_rs=TARGET_EXPOSURE_RS)
    )
    return constrained.sort_values(
        ["session_date", "signal_time", "side", "frozen_rank", "symbol"],
        kind="stable",
    ).reset_index(drop=True)


def _profit_factor(values: pd.Series) -> float:
    gains = float(values.loc[values.gt(0)].sum())
    losses = float(-values.loc[values.lt(0)].sum())
    return gains / losses if losses > 0 else (math.inf if gains > 0 else math.nan)


def score_audit(audit: pd.DataFrame, sessions: Sequence[date]) -> dict[str, Any]:
    official = tuple(sorted({_parse_day(value) for value in sessions}))
    if not official:
        raise ValueError("score_audit requires official sessions")
    work = audit.copy() if not audit.empty else pd.DataFrame()
    if "session_date" not in work:
        work["session_date"] = pd.Series(dtype="object")
    work["session_date"] = work["session_date"].map(_parse_day)
    filled = work.get("filled", pd.Series(False, index=work.index)).fillna(False).astype(bool)
    returns = pd.to_numeric(
        work.get("net_return_pct", pd.Series(np.nan, index=work.index)),
        errors="coerce",
    )
    pnl = pd.to_numeric(
        work.get("net_pnl_rs", pd.Series(np.nan, index=work.index)),
        errors="coerce",
    )
    closed_mask = filled & np.isfinite(returns) & np.isfinite(pnl)
    closed = work.loc[closed_mask].copy()
    closed_returns = returns.loc[closed_mask]
    daily = pd.Series(0.0, index=pd.Index(official, dtype="object"))
    counts = pd.Series(0, index=pd.Index(official, dtype="object"))
    for day, value in closed.assign(_net=closed_returns).groupby("session_date")["_net"].sum().items():
        if day in daily.index:
            daily.loc[day] = float(value)
    for day, value in closed.groupby("session_date").size().items():
        if day in counts.index:
            counts.loc[day] = int(value)
    best_day = daily.idxmax() if len(daily) else None
    robust = (
        closed_returns.loc[closed["session_date"].ne(best_day)]
        if best_day is not None
        else closed_returns
    )
    block_nets: list[float] = []
    block_pfs: list[float] = []
    for block in np.array_split(np.asarray(official, dtype=object), 3):
        values = closed_returns.loc[closed["session_date"].isin(set(block))]
        block_nets.append(float(values.sum()))
        block_pfs.append(_profit_factor(values))
    cumulative = np.concatenate(([0.0], daily.cumsum().to_numpy(float)))
    drawdown = cumulative - np.maximum.accumulate(cumulative)
    positive_gross = float(daily.clip(lower=0.0).sum())
    top_day = max(0.0, float(daily.max())) if len(daily) else 0.0
    status = work.get("status", pd.Series("", index=work.index)).astype(str)
    return {
        "sessions": len(official),
        "candidates": len(work),
        "fills": int(filled.sum()),
        "closed_fills": int(closed_mask.sum()),
        "trades_per_session": float(closed_mask.sum() / len(official)),
        "profit_factor": _profit_factor(closed_returns),
        "robust_profit_factor_ex_best_day": _profit_factor(robust),
        "net_return_percentage_points": float(closed_returns.sum()),
        "net_pnl_rs": float(pnl.loc[closed_mask].sum()),
        "wins": int(closed_returns.gt(0).sum()),
        "losses": int(closed_returns.lt(0).sum()),
        "active_days": int(counts.gt(0).sum()),
        "positive_days": int(daily.gt(0).sum()),
        "negative_days": int(daily.lt(0).sum()),
        "flat_days": int(daily.eq(0).sum()),
        "positive_daily_gross_percentage_points": positive_gross,
        "top_day_share": top_day / positive_gross if positive_gross > 0 else math.inf,
        "positive_contiguous_blocks": int(sum(value > 0 for value in block_nets)),
        "worst_contiguous_block_pf": min(block_pfs) if block_pfs else math.nan,
        "max_drawdown_percentage_points": max(
            0.0, float(-drawdown.min()) if len(drawdown) else 0.0
        ),
        "data_incomplete_candidates": int(
            status.eq(v8.SignalState.DATA_INCOMPLETE.value).sum()
        ),
        "unresolved_filled_trades": int((filled & ~closed_mask).sum()),
    }


def score_book(audit: pd.DataFrame, sessions: Sequence[date]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "combined": score_audit(audit, sessions),
        "sides": {},
        "legs": {},
    }
    for side in SIDES:
        subset = (
            audit.loc[audit["side"].astype(str).eq(side)]
            if not audit.empty and "side" in audit
            else pd.DataFrame()
        )
        result["sides"][side] = score_audit(subset, sessions)
    for key in LEG_KEYS:
        subset = (
            audit.loc[audit["setup_id"].astype(str).eq(key)]
            if not audit.empty and "setup_id" in audit
            else pd.DataFrame()
        )
        result["legs"][key] = score_audit(subset, sessions)
    return result


def behavior_signature(audit: pd.DataFrame) -> str:
    if audit.empty:
        return common.canonical_json_sha256([])
    columns = [
        column
        for column in (
            "candidate_id",
            "status",
            "confirmation_minute",
            "entry_time",
            "entry_price",
            "exit_time",
            "exit_price",
        )
        if column in audit.columns
    ]
    ordered = audit[columns].copy().sort_values("candidate_id", kind="stable")
    return common.canonical_json_sha256(_safe_json(ordered.to_dict("records")))


def _pf_at_least(metrics: Mapping[str, Any], minimum: float) -> bool:
    value = float(metrics.get("profit_factor", math.nan))
    return math.isfinite(value) and value >= minimum


def _net_positive(metrics: Mapping[str, Any]) -> bool:
    return float(metrics.get("net_return_percentage_points", 0.0)) > 0.0


def _data_clean(metrics: Mapping[str, Any]) -> bool:
    return bool(
        int(metrics.get("data_incomplete_candidates", 0)) == 0
        and int(metrics.get("unresolved_filled_trades", 0)) == 0
    )


def leg_train_guard(
    base: Mapping[str, Any], stress: Mapping[str, Any], guards: Guards
) -> bool:
    robust = float(base.get("robust_profit_factor_ex_best_day", math.nan))
    return bool(
        int(base.get("closed_fills", 0)) >= guards.train_min_fills_per_leg
        and int(base.get("active_days", 0)) >= guards.train_min_active_days
        and _pf_at_least(base, guards.train_min_leg_pf)
        and _net_positive(base)
        and not math.isnan(robust)
        and robust >= guards.train_min_leg_robust_pf
        and float(base.get("top_day_share", math.inf))
        <= guards.train_max_leg_top_day_share
        and int(base.get("positive_contiguous_blocks", 0))
        >= guards.min_positive_blocks
        and _pf_at_least(stress, guards.min_stress_pf)
        and _net_positive(stress)
        and _data_clean(base)
        and _data_clean(stress)
    )


def trial_leg_grid(
    prepared: PreparedDataset,
    *,
    leg_key: str,
    guards: Guards,
    configs: Sequence[LegConfig] | None = None,
) -> pd.DataFrame:
    if leg_key not in LEG_KEYS:
        raise ValueError(f"unknown leg {leg_key!r}")
    grid = tuple(configs or LEG_GRIDS[leg_key])
    records: list[dict[str, Any]] = []
    for config in grid:
        if config.setup_id != leg_key:
            raise ValueError("leg grid contains a config for another leg")
        base_audit = run_leg_preportfolio(prepared, config, stress=False)
        stress_audit = run_leg_preportfolio(prepared, config, stress=True)
        base = score_audit(base_audit, prepared.session_dates)
        stress = score_audit(stress_audit, prepared.session_dates)
        records.append(
            {
                "leg_key": leg_key,
                "config_hash": config.config_hash,
                "config": config.payload(),
                "complexity": config.complexity,
                "behavior_signature": behavior_signature(base_audit),
                "guard_pass": leg_train_guard(base, stress, guards),
                "base_metrics": base,
                "stress_metrics": stress,
                "closed_fills": base["closed_fills"],
                "trades_per_session": base["trades_per_session"],
                "profit_factor": base["profit_factor"],
                "robust_pf": base["robust_profit_factor_ex_best_day"],
                "stress_pf": stress["profit_factor"],
                "top_day_share": base["top_day_share"],
            }
        )
    return pd.DataFrame(records).sort_values("config_hash", kind="stable").reset_index(drop=True)


def select_leg_frontier(
    trials: pd.DataFrame, *, top_n: int = 2
) -> tuple[list[LegConfig], pd.DataFrame]:
    if trials.empty:
        return [], trials.copy()
    ranked = trials.copy()
    for source, target in (
        ("profit_factor", "_pf"),
        ("robust_pf", "_robust"),
        ("stress_pf", "_stress"),
    ):
        ranked[target] = pd.to_numeric(ranked[source], errors="coerce").fillna(-math.inf)
    ranked = ranked.sort_values(
        [
            "guard_pass",
            "trades_per_session",
            "_robust",
            "_pf",
            "_stress",
            "complexity",
            "config_hash",
        ],
        ascending=[False, False, False, False, False, True, True],
        kind="stable",
    )
    ranked["behavior_alias_of"] = ""
    observed: dict[str, str] = {}
    chosen: list[int] = []
    for index, row in ranked.iterrows():
        signature = str(row["behavior_signature"])
        if signature in observed:
            ranked.at[index, "behavior_alias_of"] = observed[signature]
            continue
        observed[signature] = str(row["config_hash"])
        if bool(row["guard_pass"]):
            chosen.append(index)
        if len(chosen) >= int(top_n):
            # Continue only to annotate aliases for reporting.
            continue
    configs = [
        LegConfig.from_payload(value) for value in ranked.loc[chosen[:top_n], "config"]
    ]
    return configs, ranked.drop(columns=["_pf", "_robust", "_stress"])


def select_independent_leg_winners(
    frontiers: Mapping[str, Sequence[LegConfig]],
) -> tuple[dict[str, LegConfig], dict[str, dict[str, Any]]]:
    """Freeze the first eligible representative for every leg independently.

    The frontier order is the preregistered per-leg objective order. No pooled
    portfolio statistic is consulted here or permitted to replace a winner.
    """

    selected: dict[str, LegConfig] = {}
    disabled: dict[str, dict[str, Any]] = {}
    for leg in LEG_KEYS:
        eligible = tuple(frontiers.get(leg, ()))
        for config in eligible:
            if config.setup_id != leg:
                raise ValueError(f"frontier {leg} contains config {config.setup_id}")
        if eligible:
            selected[leg] = eligible[0]
        else:
            disabled[leg] = {
                "stage": "TRAIN",
                "reason": "NO_CONFIG_PASSED_INDEPENDENT_TRAIN_LEG_GUARDS",
                "permanently_disabled_for_run": True,
            }
    return selected, disabled


def _forbidden_joint_training_book_guard(
    base: Mapping[str, Any], stress: Mapping[str, Any], guards: Guards
) -> bool:
    raise RuntimeError(
        "joint/pooled TRAIN qualification is forbidden; gate each leg independently"
    )
    combined = dict(base["combined"])
    stress_combined = dict(stress["combined"])
    robust = float(combined.get("robust_profit_factor_ex_best_day", math.nan))
    if not (
        _data_clean(combined)
        and _data_clean(stress_combined)
        and _pf_at_least(combined, guards.train_min_combined_pf)
        and _net_positive(combined)
        and not math.isnan(robust)
        and robust >= guards.train_min_robust_pf
        and int(combined.get("active_days", 0)) >= guards.train_min_active_days
        and float(combined.get("top_day_share", math.inf))
        <= guards.max_top_day_share
        and int(combined.get("positive_contiguous_blocks", 0))
        >= guards.min_positive_blocks
        and _pf_at_least(stress_combined, guards.min_stress_pf)
        and _net_positive(stress_combined)
    ):
        return False
    for side in SIDES:
        side_base = dict(base["sides"])[side]
        side_stress = dict(stress["sides"])[side]
        if not (
            _pf_at_least(side_base, guards.min_side_pf)
            and _net_positive(side_base)
            and _pf_at_least(side_stress, guards.min_stress_pf)
            and _net_positive(side_stress)
        ):
            return False
    for leg in LEG_KEYS:
        leg_base = dict(base["legs"])[leg]
        leg_stress = dict(stress["legs"])[leg]
        if not (
            int(leg_base.get("closed_fills", 0))
            >= guards.train_min_fills_per_leg
            and int(leg_base.get("active_days", 0))
            >= guards.train_min_active_days
            and _pf_at_least(leg_base, guards.train_min_leg_pf)
            and _net_positive(leg_base)
            and float(
                leg_base.get("robust_profit_factor_ex_best_day", math.nan)
            )
            >= guards.train_min_leg_robust_pf
            and float(leg_base.get("top_day_share", math.inf))
            <= guards.train_max_leg_top_day_share
            and int(leg_base.get("positive_contiguous_blocks", 0))
            >= guards.min_positive_blocks
            and _pf_at_least(leg_stress, guards.min_stress_pf)
            and _net_positive(leg_stress)
            and _data_clean(leg_base)
            and _data_clean(leg_stress)
        ):
            return False
    return True


def _forbidden_joint_train_book_search(
    prepared: PreparedDataset,
    frontiers: Mapping[str, Sequence[LegConfig]],
    *,
    guards: Guards,
) -> tuple[pd.DataFrame, dict[str, dict[str, pd.DataFrame]]]:
    raise RuntimeError(
        "joint TRAIN book search is forbidden; freeze independent leg winners"
    )
    if any(len(frontiers.get(key, ())) == 0 for key in LEG_KEYS):
        return pd.DataFrame(), {}
    if any(len(frontiers[key]) > 2 for key in LEG_KEYS):
        raise ValueError("book frontier accepts at most two configs per leg")
    configs = {
        key: tuple(frontiers[key]) for key in LEG_KEYS
    }
    base_cache = {
        config.config_hash: run_leg_preportfolio(prepared, config, stress=False)
        for key in LEG_KEYS
        for config in configs[key]
    }
    stress_cache = {
        config.config_hash: run_leg_preportfolio(prepared, config, stress=True)
        for key in LEG_KEYS
        for config in configs[key]
    }
    records: list[dict[str, Any]] = []
    audits: dict[str, dict[str, pd.DataFrame]] = {}
    for combination in itertools.product(*(configs[key] for key in LEG_KEYS)):
        book = BookConfig(tuple(combination))
        base_audit = constrain_book(base_cache[value.config_hash] for value in book.legs)
        stress_audit = constrain_book(
            stress_cache[value.config_hash] for value in book.legs
        )
        base = score_book(base_audit, prepared.session_dates)
        stress = score_book(stress_audit, prepared.session_dates)
        qualifies = _forbidden_joint_training_book_guard(base, stress, guards)
        records.append(
            {
                "book_hash": book.config_hash,
                "book": book.payload(),
                "qualifies": qualifies,
                "complexity": sum(value.complexity for value in book.legs),
                "base_metrics": base,
                "stress_metrics": stress,
                "closed_fills": base["combined"]["closed_fills"],
                "trades_per_session": base["combined"]["trades_per_session"],
                "profit_factor": base["combined"]["profit_factor"],
                "robust_pf": base["combined"]["robust_profit_factor_ex_best_day"],
                "stress_pf": stress["combined"]["profit_factor"],
                "top_day_share": base["combined"]["top_day_share"],
            }
        )
        audits[book.config_hash] = {"base": base_audit, "stress": stress_audit}
    frame = pd.DataFrame(records)
    expected = math.prod(len(configs[key]) for key in LEG_KEYS)
    if len(frame) != expected or expected > 16:
        raise AssertionError("invalid V9 TRAIN book frontier size")
    return frame, audits


def _forbidden_joint_book_ranking(frame: pd.DataFrame) -> pd.DataFrame:
    raise RuntimeError(
        "joint book ranking is forbidden; portfolio replay is diagnostic only"
    )
    if frame.empty:
        return frame.copy()
    ranked = frame.copy()
    for column in ("profit_factor", "robust_pf", "stress_pf"):
        ranked[column] = pd.to_numeric(ranked[column], errors="coerce").fillna(-math.inf)
    ranked = ranked.sort_values(
        [
            "qualifies",
            "trades_per_session",
            "robust_pf",
            "profit_factor",
            "stress_pf",
            "top_day_share",
            "complexity",
            "book_hash",
        ],
        ascending=[False, False, False, False, False, True, True, True],
        kind="stable",
    ).reset_index(drop=True)
    ranked.insert(0, "book_rank", np.arange(1, len(ranked) + 1))
    return ranked


def _forbidden_pooled_evaluation_guard(
    base: Mapping[str, Any],
    stress: Mapping[str, Any],
    *,
    stage: str,
    guards: Guards,
) -> bool:
    raise RuntimeError(
        "pooled stage qualification is forbidden; gate each leg independently"
    )
    key = str(stage).upper()
    if key == "VALIDATION":
        min_fills = guards.validation_min_fills_per_leg
        min_pf = guards.validation_min_combined_pf
        min_active = guards.validation_min_active_days
        min_leg_pf = guards.validation_min_leg_pf
        max_leg_top_share = guards.validation_max_leg_top_day_share
    elif key == "TEST":
        min_fills = guards.test_min_fills_per_leg
        min_pf = guards.test_min_combined_pf
        min_active = guards.test_min_active_days
        min_leg_pf = guards.test_min_leg_pf
        max_leg_top_share = guards.max_top_day_share
    else:
        raise ValueError("evaluation stage must be VALIDATION or TEST")
    combined = dict(base["combined"])
    stress_combined = dict(stress["combined"])
    robust = float(combined.get("robust_profit_factor_ex_best_day", math.nan))
    if not (
        _data_clean(combined)
        and _data_clean(stress_combined)
        and _pf_at_least(combined, min_pf)
        and _net_positive(combined)
        and not math.isnan(robust)
        and robust >= 1.0
        and int(combined.get("active_days", 0)) >= min_active
        and float(combined.get("top_day_share", math.inf))
        <= guards.max_top_day_share
        and int(combined.get("positive_contiguous_blocks", 0))
        >= guards.min_positive_blocks
        and _pf_at_least(stress_combined, guards.min_stress_pf)
        and _net_positive(stress_combined)
    ):
        return False
    for side in SIDES:
        if not (
            _pf_at_least(dict(base["sides"])[side], guards.min_side_pf)
            and _net_positive(dict(base["sides"])[side])
            and _pf_at_least(dict(stress["sides"])[side], guards.min_stress_pf)
            and _net_positive(dict(stress["sides"])[side])
        ):
            return False
    for leg in LEG_KEYS:
        base_leg = dict(base["legs"])[leg]
        stress_leg = dict(stress["legs"])[leg]
        if not (
            int(base_leg.get("closed_fills", 0)) >= min_fills
            and int(base_leg.get("active_days", 0)) >= min_active
            and _pf_at_least(base_leg, min_leg_pf)
            and _net_positive(base_leg)
            and float(base_leg.get("top_day_share", math.inf))
            <= max_leg_top_share
            and int(base_leg.get("positive_contiguous_blocks", 0))
            >= guards.min_positive_blocks
            and _pf_at_least(stress_leg, guards.min_stress_pf)
            and _net_positive(stress_leg)
            and _data_clean(base_leg)
            and _data_clean(stress_leg)
        ):
            return False
    return True


def _leg_guard_thresholds(stage: str, guards: Guards) -> dict[str, Any]:
    key = str(stage).upper()
    if key == "TRAIN":
        return {
            "min_fills": guards.train_min_fills_per_leg,
            "min_active_days": guards.train_min_active_days,
            "min_profit_factor": guards.train_min_leg_pf,
            "min_robust_profit_factor": guards.train_min_leg_robust_pf,
            "max_top_day_share": guards.train_max_leg_top_day_share,
            "min_stress_profit_factor": guards.min_stress_pf,
            "min_positive_blocks": guards.min_positive_blocks,
        }
    elif key == "VALIDATION":
        return {
            "min_fills": guards.validation_min_fills_per_leg,
            "min_active_days": guards.validation_min_active_days,
            "min_profit_factor": guards.validation_min_leg_pf,
            "min_robust_profit_factor": None,
            "max_top_day_share": guards.validation_max_leg_top_day_share,
            "min_stress_profit_factor": guards.min_stress_pf,
            "min_positive_blocks": guards.min_positive_blocks,
        }
    elif key == "TEST":
        return {
            "min_fills": guards.test_min_fills_per_leg,
            "min_active_days": guards.test_min_active_days,
            "min_profit_factor": guards.test_min_leg_pf,
            "min_robust_profit_factor": None,
            "max_top_day_share": guards.max_top_day_share,
            "min_stress_profit_factor": guards.min_stress_pf,
            "min_positive_blocks": guards.min_positive_blocks,
        }
    raise ValueError("leg guard stage must be TRAIN, VALIDATION or TEST")


def leg_stage_guard(
    observed: Mapping[str, Any],
    stressed: Mapping[str, Any],
    *,
    stage: str,
    guards: Guards,
) -> dict[str, Any]:
    """Evaluate one slot-side leg without any pooled-book dependency."""

    thresholds = _leg_guard_thresholds(stage, guards)
    min_robust = thresholds["min_robust_profit_factor"]
    robust = float(observed.get("robust_profit_factor_ex_best_day", math.nan))
    checks = {
        "fills": int(observed.get("closed_fills", 0))
        >= int(thresholds["min_fills"]),
        "active_days": int(observed.get("active_days", 0))
        >= int(thresholds["min_active_days"]),
        "profit_factor": _pf_at_least(
            observed, float(thresholds["min_profit_factor"])
        ),
        "robust_profit_factor": (
            True
            if min_robust is None
            else math.isfinite(robust) and robust >= float(min_robust)
        ),
        "top_day_share": float(observed.get("top_day_share", math.inf))
        <= float(thresholds["max_top_day_share"]),
        "positive_blocks": int(observed.get("positive_contiguous_blocks", 0))
        >= int(thresholds["min_positive_blocks"]),
        "positive_net": _net_positive(observed),
        "stress_profit_factor": _pf_at_least(
            stressed, float(thresholds["min_stress_profit_factor"])
        ),
        "stress_positive_net": _net_positive(stressed),
        "data_clean": _data_clean(observed) and _data_clean(stressed),
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "thresholds": thresholds,
        "observed": {
            "closed_fills": observed.get("closed_fills"),
            "active_days": observed.get("active_days"),
            "profit_factor": observed.get("profit_factor"),
            "robust_profit_factor_ex_best_day": observed.get(
                "robust_profit_factor_ex_best_day"
            ),
            "top_day_share": observed.get("top_day_share"),
            "positive_contiguous_blocks": observed.get(
                "positive_contiguous_blocks"
            ),
            "net_return_percentage_points": observed.get(
                "net_return_percentage_points"
            ),
            "stress_profit_factor": stressed.get("profit_factor"),
            "stress_net_return_percentage_points": stressed.get(
                "net_return_percentage_points"
            ),
        },
        "checks": checks,
        "failed_checks": failed,
        "pass": not failed,
    }


def leg_guard_breakdown(
    base: Mapping[str, Any],
    stress: Mapping[str, Any],
    *,
    stage: str,
    guards: Guards,
) -> dict[str, dict[str, Any]]:
    base_legs = dict(base["legs"])
    stress_legs = dict(stress["legs"])
    output: dict[str, dict[str, Any]] = {}
    for leg in LEG_KEYS:
        if leg not in base_legs or leg not in stress_legs:
            continue
        output[leg] = leg_stage_guard(
            dict(base_legs[leg]),
            dict(stress_legs[leg]),
            stage=stage,
            guards=guards,
        )
    return output


def run_book(
    prepared: PreparedDataset, book: BookConfig, *, stress: bool
) -> pd.DataFrame:
    book.validate()
    return constrain_book(
        run_leg_preportfolio(prepared, config, stress=stress)
        for config in book.legs
    )


@dataclass
class IndependentStageEvaluation:
    stage: str
    input_book: BookConfig
    advancing_book: BookConfig | None
    leg_results: dict[str, dict[str, Any]]
    disabled_legs: dict[str, dict[str, Any]]
    input_base_audit: pd.DataFrame
    input_stress_audit: pd.DataFrame
    portfolio_base_audit: pd.DataFrame
    portfolio_stress_audit: pd.DataFrame
    portfolio_base_metrics: dict[str, Any]
    portfolio_stress_metrics: dict[str, Any]


def _concat_audits(parts: Sequence[pd.DataFrame]) -> pd.DataFrame:
    nonempty = [value for value in parts if not value.empty]
    return pd.concat(nonempty, ignore_index=True, sort=False) if nonempty else pd.DataFrame()


def _evaluate_independent_legs(
    prepared: PreparedDataset,
    book: BookConfig,
    *,
    stage: str,
    guards: Guards,
) -> IndependentStageEvaluation:
    """Gate each frozen leg before an explicitly diagnostic portfolio replay."""

    book.validate()
    stage_key = str(stage).upper()
    if stage_key not in {"TRAIN", "VALIDATION", "TEST"}:
        raise ValueError("stage must be TRAIN, VALIDATION or TEST")
    base_by_leg: dict[str, pd.DataFrame] = {}
    stress_by_leg: dict[str, pd.DataFrame] = {}
    leg_results: dict[str, dict[str, Any]] = {}
    disabled: dict[str, dict[str, Any]] = {}
    advancing: list[LegConfig] = []
    for config in book.legs:
        leg = config.setup_id
        base_audit = run_leg_preportfolio(prepared, config, stress=False)
        stress_audit = run_leg_preportfolio(prepared, config, stress=True)
        base_by_leg[leg] = base_audit
        stress_by_leg[leg] = stress_audit
        base_metrics = score_audit(base_audit, prepared.session_dates)
        stress_metrics = score_audit(stress_audit, prepared.session_dates)
        guard = leg_stage_guard(
            base_metrics, stress_metrics, stage=stage_key, guards=guards
        )
        passed = bool(guard["pass"])
        if passed:
            advancing.append(config)
        else:
            disabled[leg] = {
                "stage": stage_key,
                "reason": "INDEPENDENT_LEG_GUARD_FAILED",
                "failed_checks": list(guard["failed_checks"]),
                "permanently_disabled_for_run": True,
            }
        leg_results[leg] = {
            "config": config.payload(),
            "config_hash": config.config_hash,
            "passed": passed,
            "base_metrics": _safe_json(base_metrics),
            "stress_metrics": _safe_json(stress_metrics),
            "guard": _safe_json(guard),
            "pf_claim_eligible": bool(passed and stage_key in {"VALIDATION", "TEST"}),
            "claimed_profit_factor": (
                float(base_metrics["profit_factor"])
                if passed and stage_key in {"VALIDATION", "TEST"}
                else None
            ),
        }
    advancing_book = BookConfig(tuple(advancing)) if advancing else None
    advancing_keys = {value.setup_id for value in advancing}
    portfolio_base = constrain_book(
        base_by_leg[key] for key in LEG_KEYS if key in advancing_keys
    )
    portfolio_stress = constrain_book(
        stress_by_leg[key] for key in LEG_KEYS if key in advancing_keys
    )
    return IndependentStageEvaluation(
        stage=stage_key,
        input_book=book,
        advancing_book=advancing_book,
        leg_results=leg_results,
        disabled_legs=disabled,
        input_base_audit=_concat_audits(list(base_by_leg.values())),
        input_stress_audit=_concat_audits(list(stress_by_leg.values())),
        portfolio_base_audit=portfolio_base,
        portfolio_stress_audit=portfolio_stress,
        portfolio_base_metrics=score_book(portfolio_base, prepared.session_dates),
        portfolio_stress_metrics=score_book(portfolio_stress, prepared.session_dates),
    )


def _artifact_path(manifest: Mapping[str, Any], name: str) -> Path:
    record = dict(dict(manifest.get("artifacts", {})).get(name, {}))
    path = Path(str(record.get("path", ""))).resolve()
    if not provenance.artifact_matches(path, record):
        raise AssertionError(f"V9 cache artifact is missing or changed: {name}")
    return path


def _new_run_dir(prefix: str, fingerprint: str) -> Path:
    stamp = common.now_ist().strftime("%Y%m%dT%H%M%S%f%z")
    path = RUN_ROOT / f"{prefix}_{stamp}_{fingerprint[:12]}"
    path.mkdir(parents=True, exist_ok=False)
    return path


def _write_new_text(path: Path, value: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="") as handle:
        handle.write(value)
    return path


def _write_new_csv(frame: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, mode="x")
    return path


def _csv_ready(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for column in out.columns:
        if out[column].map(lambda value: isinstance(value, (dict, list, tuple))).any():
            out[column] = out[column].map(
                lambda value: json.dumps(_safe_json(value), sort_keys=True)
                if isinstance(value, (dict, list, tuple))
                else value
            )
    return out


def load_authenticated_output(
    run_dir: Path | str, *, filename: str, output_name: str
) -> dict[str, Any]:
    root = Path(run_dir).resolve()
    artifact = (root / filename).resolve()
    provenance_path = (root / "provenance.json").resolve()
    if not artifact.exists() or not provenance_path.exists():
        raise FileNotFoundError(f"authenticated stage artifacts are missing in {root}")
    frozen = json.loads(provenance_path.read_text(encoding="utf-8"))
    record = dict(dict(frozen.get("outputs", {})).get(output_name, {}))
    if Path(str(record.get("path", ""))).resolve() != artifact:
        raise AssertionError(f"provenance does not identify {filename}")
    if not provenance.artifact_matches(artifact, record):
        raise AssertionError(f"{filename} changed after provenance freeze")
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    if str(payload.get("run_fingerprint", "")) != str(
        frozen.get("run_fingerprint", "")
    ):
        raise AssertionError(f"{filename} fingerprint is not authenticated")
    return payload


def load_authenticated_selection(search_run: Path | str) -> dict[str, Any]:
    selection = load_authenticated_output(
        search_run, filename="selection.json", output_name="selection"
    )
    frozen = json.loads(
        (Path(search_run).resolve() / "provenance.json").read_text(encoding="utf-8")
    )
    expected = dict(frozen.get("results", {})).get("selected_book_hash")
    chosen = selection.get("selected_book")
    observed = dict(chosen).get("config_hash") if chosen else None
    if observed != expected:
        raise AssertionError("selection winner differs from frozen provenance")
    if chosen:
        book = BookConfig.from_payload(dict(chosen))
        selected_keys = [value.setup_id for value in book.legs]
        encoded_legs = dict(selection.get("selected_legs") or {})
        if selected_keys != list(selection.get("selected_leg_keys") or []):
            raise AssertionError("selected leg-key list differs from frozen subset")
        if {
            key: LegConfig.from_payload(dict(encoded_legs[key])).config_hash
            for key in selected_keys
        } != {value.setup_id: value.config_hash for value in book.legs}:
            raise AssertionError("selected_legs differs from selected_book")
        if selected_keys != list(dict(frozen.get("results", {})).get("selected_leg_keys", [])):
            raise AssertionError("selected leg subset differs from frozen provenance")
    return selection


def _authenticate_stage_claim(
    run_dir: Path,
    result: Mapping[str, Any],
    frozen_provenance: Mapping[str, Any],
) -> dict[str, Any]:
    outputs = dict(frozen_provenance.get("outputs", {}))
    records = {
        name: dict(outputs.get(name, {})) for name in ("claim", "stage_registry")
    }
    paths: dict[str, Path] = {}
    for name, record in records.items():
        path = Path(str(record.get("path", ""))).resolve()
        if not provenance.artifact_matches(path, record):
            raise AssertionError(f"authenticated stage {name} is missing or changed")
        paths[name] = path
    claim_bytes = paths["claim"].read_bytes()
    if claim_bytes != paths["stage_registry"].read_bytes():
        raise AssertionError("local claim and immutable registry diverge")
    claim = json.loads(claim_bytes.decode("utf-8"))
    claim_identity = dict(claim)
    observed_claim_id = str(claim_identity.pop("claim_id", ""))
    if common.canonical_json_sha256(claim_identity) != observed_claim_id:
        raise AssertionError("immutable stage claim ID is invalid")
    provenance_binding = dict(frozen_provenance.get("stage_claim_binding", {}))
    expected = {
        "claim_id": result.get("claim_id"),
        "evaluation_id": result.get("evaluation_id"),
        "stage": result.get("stage"),
        "search_run_fingerprint": result.get("search_run_fingerprint"),
        "selection_sha256": result.get("selection_sha256"),
        "input_book_hash": result.get("book_hash"),
    }
    if any(str(claim.get(key, "")) != str(value or "") for key, value in expected.items()):
        raise AssertionError("stage claim identity differs from authenticated result")
    if any(
        str(provenance_binding.get(key, "")) != str(claim.get(key, ""))
        for key in expected
    ):
        raise AssertionError("stage provenance does not bind its immutable claim")
    if dict(result.get("prior_stage_binding") or {}) != dict(
        claim.get("prior_stage_binding") or {}
    ):
        raise AssertionError("stage result prior-stage binding differs from claim")
    if dict(provenance_binding.get("prior_stage_binding") or {}) != dict(
        claim.get("prior_stage_binding") or {}
    ):
        raise AssertionError("stage provenance prior-stage binding differs from claim")
    if Path(str(claim.get("search_run_dir", ""))).resolve() != Path(
        str(result.get("search_run_dir", ""))
    ).resolve():
        raise AssertionError("stage claim belongs to a different TRAIN run")
    prior = dict(claim.get("prior_stage_binding") or {})
    if prior:
        if str(result.get("stage")) != "TEST" or str(prior.get("stage")) != "VALIDATION":
            raise AssertionError("invalid prior-stage transition in immutable claim")
        prior_run = Path(str(prior.get("run_dir", ""))).resolve()
        prior_result_path = prior_run / "result.json"
        prior_provenance_path = prior_run / "provenance.json"
        if (
            not prior_result_path.exists()
            or provenance.sha256_file(prior_result_path)
            != str(prior.get("result_sha256", ""))
            or not prior_provenance_path.exists()
            or provenance.sha256_file(prior_provenance_path)
            != str(prior.get("provenance_sha256", ""))
        ):
            raise AssertionError("prior VALIDATION artifacts changed after TEST claim")
        authenticated_prior = load_authenticated_stage_result(prior_run)
        for prior_key, result_key in (
            ("evaluation_id", "evaluation_id"),
            ("claim_id", "claim_id"),
            ("run_fingerprint", "run_fingerprint"),
            ("advancing_book_hash", "advancing_book_hash"),
            ("advancing_leg_keys", "advancing_leg_keys"),
        ):
            if _safe_json(prior.get(prior_key)) != _safe_json(
                authenticated_prior.get(result_key)
            ):
                raise AssertionError(
                    f"prior VALIDATION {prior_key} binding is invalid"
                )
    return claim


def load_authenticated_stage_result(run_dir: Path | str) -> dict[str, Any]:
    root = Path(run_dir).resolve()
    result = load_authenticated_output(
        root, filename="result.json", output_name="result"
    )
    provenance_path = root / "provenance.json"
    frozen = json.loads(provenance_path.read_text(encoding="utf-8"))
    _authenticate_stage_claim(root, result, frozen)
    frozen_results = dict(frozen.get("results", {}))
    for key in (
        "status",
        "book_hash",
        "advancing_book_hash",
        "advancing_leg_keys",
        "disabled_legs",
        "claimed_leg_profit_factors",
        "eligible_for_test",
    ):
        if _safe_json(frozen_results.get(key)) != _safe_json(result.get(key)):
            raise AssertionError(
                f"stage result {key} differs from frozen provenance"
            )
    return result


def _verify_frozen_inputs(selection: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Path]]:
    if provenance.sha256_file(Path(__file__)) != str(
        selection.get("optimizer_source_sha256", "")
    ):
        raise AssertionError("optimizer source changed after TRAIN freeze")
    if provenance.sha256_file(Path(v8.__file__)) != str(
        selection.get("v8_source_sha256", "")
    ):
        raise AssertionError("V8 engine changed after TRAIN freeze")
    manifest_path = Path(str(selection["cache_manifest_path"])).resolve()
    if (
        not manifest_path.exists()
        or int(manifest_path.stat().st_size)
        != int(selection.get("cache_manifest_size", -1))
        or provenance.sha256_file(manifest_path)
        != str(selection.get("cache_manifest_sha256", ""))
    ):
        raise AssertionError("V9 cache manifest changed after TRAIN freeze")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("input_fingerprint") != selection.get("cache_input_fingerprint"):
        raise AssertionError("V9 cache input fingerprint changed")
    paths: dict[str, Path] = {}
    for name in ("candidates", "paths", "coverage"):
        path = Path(str(selection[f"{name}_cache_path"])).resolve()
        if (
            not path.exists()
            or int(path.stat().st_size) != int(selection.get(f"{name}_cache_size", -1))
            or provenance.sha256_file(path)
            != str(selection.get(f"{name}_cache_sha256", ""))
        ):
            raise AssertionError(f"frozen {name} cache changed")
        paths[name] = path
    paths["manifest"] = manifest_path
    return manifest, paths


def _metrics_line(label: str, metrics: Mapping[str, Any]) -> str:
    combined = dict(metrics.get("combined", {}))
    return (
        f"- {label}: closed_fills={combined.get('closed_fills')}, "
        f"trades/session={combined.get('trades_per_session')}, "
        f"PF(selection statistic)={combined.get('profit_factor')}, "
        f"robust_PF={combined.get('robust_profit_factor_ex_best_day')}, "
        f"net={combined.get('net_return_percentage_points')}, "
        f"top_day_share={combined.get('top_day_share')}"
    )


def render_search_report(selection: Mapping[str, Any]) -> str:
    lines = [
        "# FnO V9 09:50/09:55 Honest TRAIN Search",
        "",
        f"- Status: `{selection.get('selection_status')}`",
        f"- Eligible for one-shot VALIDATION: {selection.get('eligible_for_validation')}",
        f"- Coverage mode: `{selection.get('coverage_mode')}`",
        f"- TRAIN/VALIDATION/TEST sessions: "
        f"{selection.get('split_session_counts')}",
        f"- Grid: 48 configurations x 4 independent slot-side legs",
        f"- Joint book-selection attempts: {selection.get('book_attempt_count')} (must be 0)",
        f"- Post-selection diagnostic portfolio replays: "
        f"{selection.get('diagnostic_portfolio_replay_count')}",
        f"- Total TRAIN hypotheses visible: {selection.get('multiple_testing_attempt_count')}",
        f"- Bonferroni reference alpha (visibility only): "
        f"{selection.get('bonferroni_reference_alpha')}",
        f"- Frozen source symbols: {selection.get('source_symbol_count')} "
        f"(`{selection.get('source_symbol_set_sha256')}`)",
        f"- Base economics: {BASE_COST_BPS:g} bps cost + "
        f"{BASE_SLIPPAGE_BPS:g} bps slippage",
        f"- Stress economics: {STRESS_COST_BPS:g} bps cost + "
        f"{STRESS_SLIPPAGE_BPS:g} bps slippage",
        "- VALIDATION and TEST outcomes were not evaluated by search.",
        "- Research limitation: static 2026-08-11 universe and static 26AUG "
        "futures/OI history; no deployment or point-in-time-universe claim.",
        "",
        "## Multiple-testing disclosure",
        "",
        MULTIPLE_TESTING_WARNING,
        "",
    ]
    if selection.get("watermark"):
        lines.extend(["## Diagnostic-only watermark", "", str(selection["watermark"]), ""])
    chosen = selection.get("selected_book")
    if chosen:
        lines.extend(["## Frozen independently selected leg subset", ""])
        chosen_legs = dict(chosen["legs"])
        for key in LEG_KEYS:
            if key not in chosen_legs:
                continue
            config = dict(chosen_legs[key])
            lines.append(f"- {key}: `{json.dumps(config, sort_keys=True)}`")
        lines.append("")
        if bool(selection.get("eligible_for_validation")):
            lines.extend(
                [
                    "## TRAIN selection statistics (not out-of-sample PF claims)",
                    "",
                    _metrics_line(
                        "Post-selection portfolio diagnostic/base",
                        dict(selection.get("train_base_metrics") or {}),
                    ),
                    _metrics_line(
                        "Post-selection portfolio diagnostic/stress",
                        dict(selection.get("train_stress_metrics") or {}),
                    ),
                    "",
                ]
            )
            leg_guards = dict(selection.get("train_leg_guard_results") or {})
            for key in LEG_KEYS:
                if key not in leg_guards:
                    continue
                values = dict(leg_guards.get(key) or {})
                observed = dict(values.get("observed") or {})
                lines.append(
                    f"- {key}: pass={values.get('pass')}, "
                    f"fills={observed.get('closed_fills')}, "
                    f"active_days={observed.get('active_days')}, "
                    f"PF(selection statistic)={observed.get('profit_factor')}, "
                    f"robust_PF={observed.get('robust_profit_factor_ex_best_day')}, "
                    f"top_day_share={observed.get('top_day_share')}, "
                    f"stress_PF={observed.get('stress_profit_factor')}"
                )
            lines.append("")
        disabled = dict(selection.get("disabled_legs") or {})
        if disabled:
            lines.extend(["## Disabled legs", ""])
            for key in LEG_KEYS:
                if key in disabled:
                    lines.append(f"- {key}: `{json.dumps(disabled[key], sort_keys=True)}`")
            lines.append("")
    else:
        lines.extend(
            [
                "## No winner",
                "",
                "No individual leg configuration passed its preregistered TRAIN "
                "sample, PF, stability, concentration and stress guards. No PF "
                "claim is emitted and later stages remain locked.",
                "",
            ]
        )
    return "\n".join(lines)


def _stage_report(result: Mapping[str, Any]) -> str:
    stage = str(result["stage"])
    lines = [
        f"# FnO V9 One-Shot {stage} Evaluation",
        "",
        f"- Status: `{result.get('status')}`",
        f"- Input independently selected subset: `{result.get('book_hash')}`",
        f"- Input legs: {result.get('input_leg_keys')}",
        f"- Advancing legs: {result.get('advancing_leg_keys')}",
        f"- Policy evaluations on {stage}: 1",
        "- Pooled portfolio PF claim eligible: False (diagnostic replay only)",
        "",
    ]
    leg_results = dict(result.get("leg_results") or {})
    if leg_results:
        lines.extend(["## Per-leg evidence gates", ""])
        for leg in LEG_KEYS:
            if leg not in leg_results:
                continue
            leg_result = dict(leg_results[leg])
            values = dict(leg_result.get("guard") or {})
            observed = dict(values.get("observed") or {})
            checks = dict(values.get("checks") or {})
            line = (
                f"- {leg}: pass={leg_result.get('passed')}, "
                f"fills={observed.get('closed_fills')}, "
                f"active_days={observed.get('active_days')}, checks={checks}"
            )
            if bool(leg_result.get("pf_claim_eligible")):
                line += (
                    f", eligible_{stage}_PF={leg_result.get('claimed_profit_factor')}, "
                    f"robust_PF={observed.get('robust_profit_factor_ex_best_day')}, "
                    f"stress_PF={observed.get('stress_profit_factor')}"
                )
            else:
                line += ", PF=WITHHELD_INELIGIBLE_LEG"
            lines.append(line)
        lines.append("")
    disabled = dict(result.get("disabled_legs") or {})
    if disabled:
        lines.extend(["## Disabled legs and audited reasons", ""])
        for leg in LEG_KEYS:
            if leg in disabled:
                lines.append(f"- {leg}: `{json.dumps(disabled[leg], sort_keys=True)}`")
        lines.append("")
    lines.extend(
        [
            "## Portfolio replay (diagnostic only)",
            "",
            "Pooled counts, net and PF cannot qualify, disqualify, replace or "
            "resurrect any leg. Raw diagnostic metrics are retained only for audit.",
            "",
        ]
    )
    return "\n".join(lines)


def _source_symbols_from_manifest(manifest: Mapping[str, Any]) -> list[str]:
    return list(dict(manifest.get("input_contract", {})).get("symbols", []))


def _stage_registry_path(
    search_run: Path, *, search_run_fingerprint: str, stage: str
) -> Path:
    registry_key = common.canonical_json_sha256(
        {
            "search_run_dir": str(search_run.resolve()),
            "search_run_fingerprint": search_run_fingerprint,
        }
    )
    return CLAIM_REGISTRY_ROOT / registry_key / f"{stage.lower()}_claim.json"


def _prior_stage_artifact_exists(search_run: Path, stage: str) -> bool:
    prefix = f"{stage.lower()}_"
    for child in search_run.iterdir():
        if not child.is_dir() or not child.name.lower().startswith(prefix):
            continue
        # A prior stage directory is itself evidence that the one-shot policy
        # was consumed. Corrupt/incomplete directories fail closed as well.
        return True
    return False


def _claim_once(
    *,
    search_run: Path,
    search_run_fingerprint: str,
    selection_sha256: str,
    stage: str,
    evaluation_id: str,
    input_book_hash: str,
    prior_stage_binding: Mapping[str, Any] | None,
) -> tuple[Path, Path, dict[str, Any]]:
    """Create matching local/global immutable claim records before evaluation."""

    stage_key = str(stage).upper()
    claim_path = search_run / f"{stage_key.lower()}_evaluation_claim.json"
    registry_path = _stage_registry_path(
        search_run,
        search_run_fingerprint=search_run_fingerprint,
        stage=stage_key,
    )
    if claim_path.exists() or registry_path.exists() or _prior_stage_artifact_exists(
        search_run, stage_key
    ):
        raise StageAccessError(
            f"{stage_key} one-shot evaluation was already claimed or recorded"
        )
    claimed_at = common.now_ist().isoformat(timespec="microseconds")
    identity = {
        "schema_version": OPTIMIZER_SCHEMA_VERSION,
        "stage": stage_key,
        "evaluation_id": evaluation_id,
        "search_run_dir": str(search_run.resolve()),
        "search_run_fingerprint": search_run_fingerprint,
        "selection_sha256": selection_sha256,
        "input_book_hash": input_book_hash,
        "prior_stage_binding": _safe_json(dict(prior_stage_binding or {})),
        "claimed_at_ist": claimed_at,
        "policy_evaluation_count": 1,
    }
    identity["claim_id"] = common.canonical_json_sha256(identity)
    encoded = json.dumps(identity, indent=2, sort_keys=True) + "\n"
    try:
        local = _write_new_text(claim_path, encoded)
        # If this second exclusive create fails, the local claim remains and
        # the stage stays safely consumed rather than becoming replayable.
        registry = _write_new_text(registry_path, encoded)
    except FileExistsError as exc:
        raise StageAccessError(
            f"{stage_key} one-shot claim lost an exclusive-create race"
        ) from exc
    if provenance.sha256_file(local) != provenance.sha256_file(registry):
        raise AssertionError("stage claim registry write diverged")
    return local, registry, identity


def execute_search(args: argparse.Namespace) -> Path:
    contract = SplitContract(
        _parse_day(args.train_from),
        _parse_day(args.train_through),
        _parse_day(args.validation_from),
        _parse_day(args.validation_through),
        _parse_day(args.test_from),
        _parse_day(args.test_through),
    )
    sessions = split_sessions(contract)
    coverage_mode = str(args.coverage_mode).upper().replace("-", "_")
    requested_symbols = (
        [value.strip().upper() for value in str(args.symbols).split(",") if value.strip()]
        if args.symbols
        else None
    )
    allow_diagnostic = bool(args.allow_diagnostic_research)
    diagnostic_reasons = diagnostic_contract_reasons(
        contract=contract,
        coverage_mode=coverage_mode,
        requested_symbols=requested_symbols,
    )
    if diagnostic_reasons and not allow_diagnostic:
        raise DataEligibilityError(
            "custom symbols, custom split dates and rectangular-panel mode are "
            "diagnostic-only; pass --allow-diagnostic-research to run a "
            "permanently non-advancing audit"
        )
    diagnostic_only = allow_diagnostic
    if allow_diagnostic and not diagnostic_reasons:
        diagnostic_reasons.append("EXPLICIT_DIAGNOSTIC_RESEARCH")
    candidates, minute_paths, coverage, manifest, manifest_path = load_or_build_cache(
        source_snapshot_path=args.source_snapshot,
        contract=contract,
        symbols=requested_symbols,
    )
    frozen_cache_contract = dict(manifest.get("input_contract", {}))
    if _source_sha256() != str(
        frozen_cache_contract.get("optimizer_source_sha256", "")
    ):
        raise RuntimeError("optimizer/cache source identity mismatch")
    if provenance.sha256_file(Path(v8.__file__)) != str(
        frozen_cache_contract.get("v8_source_sha256", "")
    ):
        raise RuntimeError("V8/cache source identity mismatch")
    source_symbols = _source_symbols_from_manifest(manifest)
    split_coverage: dict[str, Any]
    panel_metadata: dict[str, Any] | None = None
    if coverage_mode == "FULL_UNIVERSE":
        split_coverage = {
            split: derive_coverage(
                coverage, symbols=source_symbols, sessions=sessions[split]
            )
            for split in ("TRAIN", "VALIDATION", "TEST")
        }
        failed = [split for split, value in split_coverage.items() if not value["pass"]]
        if failed:
            counts = {
                split: split_coverage[split]["incomplete_symbol_sessions"]
                for split in failed
            }
            raise DataEligibilityError(
                f"full-universe source coverage failed for {failed}: {counts}"
            )
        research_symbols = source_symbols
    elif coverage_mode == "RECTANGULAR_PANEL":
        candidates, minute_paths, coverage, panel_metadata = derive_train_panel(
            candidates,
            minute_paths,
            coverage,
            train_sessions=sessions["TRAIN"],
        )
        panel_symbols = list(panel_metadata["panel_symbols"])
        split_coverage = {
            split: derive_coverage(
                coverage, symbols=panel_symbols, sessions=sessions[split]
            )
            for split in ("TRAIN", "VALIDATION", "TEST")
        }
        research_symbols = panel_symbols
    else:
        raise ValueError(f"unknown coverage mode {args.coverage_mode!r}")

    lineage_unknown = sorted(
        {
            symbol
            for values in split_coverage.values()
            for symbol in values.get(
                "legacy_lineage_flags_absent_symbols", []
            )
        }
    )
    lineage_certified = not lineage_unknown
    if not lineage_certified and not allow_diagnostic:
        raise DataEligibilityError(
            "legacy_lineage_flags_absent prevents a qualifying historical run; "
            "use --allow-diagnostic-research only for watermarked research, or "
            "provide prospective clean source with certified row lineage"
        )
    if not lineage_certified:
        diagnostic_only = True
        diagnostic_reasons.append("LEGACY_LINEAGE_FLAGS_ABSENT")
    watermark_parts: list[str] = []
    if diagnostic_only:
        watermark_parts.append(DIAGNOSTIC_RESEARCH_WATERMARK)
    if coverage_mode == "RECTANGULAR_PANEL":
        watermark_parts.append(DIAGNOSTIC_WATERMARK)
    if not lineage_certified:
        watermark_parts.append(LINEAGE_UNKNOWN_WATERMARK)
    watermark = "; ".join(watermark_parts) or None

    guards = Guards()
    train = prepare_dataset(candidates, minute_paths, sessions["TRAIN"])
    trials_by_leg: dict[str, pd.DataFrame] = {}
    ranked_by_leg: dict[str, pd.DataFrame] = {}
    frontiers: dict[str, list[LegConfig]] = {}
    for leg_key in LEG_KEYS:
        trials = trial_leg_grid(train, leg_key=leg_key, guards=guards)
        frontier, ranked = select_leg_frontier(trials, top_n=2)
        trials_by_leg[leg_key] = trials
        ranked_by_leg[leg_key] = ranked
        frontiers[leg_key] = frontier
    selected_by_leg, train_disabled_legs = select_independent_leg_winners(frontiers)
    selected_book = (
        BookConfig(tuple(selected_by_leg[key] for key in LEG_KEYS if key in selected_by_leg))
        if selected_by_leg
        else None
    )
    train_evaluation = (
        _evaluate_independent_legs(
            train, selected_book, stage="TRAIN", guards=guards
        )
        if selected_book is not None
        else None
    )
    if train_evaluation is not None and (
        train_evaluation.advancing_book is None
        or train_evaluation.advancing_book.config_hash != selected_book.config_hash
    ):
        raise AssertionError(
            "independent TRAIN winner failed when replayed under its unchanged leg guard"
        )
    if selected_book is None:
        status = "NO_QUALIFYING_TRAIN_LEGS"
    elif diagnostic_only:
        status = "DIAGNOSTIC_TRAIN_LEG_SUBSET_VALIDATION_LOCKED"
    else:
        status = "INDEPENDENT_TRAIN_LEG_SUBSET_FROZEN"
    eligible_for_validation = bool(
        selected_book is not None
        and not diagnostic_only
        and lineage_certified
        and not diagnostic_reasons
        and coverage_mode == "FULL_UNIVERSE"
        and requested_symbols is None
        and contract == default_split_contract()
    )

    optimizer_hash = _source_sha256()
    v8_hash = provenance.sha256_file(Path(v8.__file__))
    if optimizer_hash != str(frozen_cache_contract["optimizer_source_sha256"]):
        raise RuntimeError("optimizer source changed during TRAIN search")
    if v8_hash != str(frozen_cache_contract["v8_source_sha256"]):
        raise RuntimeError("V8 source changed during TRAIN search")
    run_fingerprint = common.canonical_json_sha256(
        {
            "optimizer_version": OPTIMIZER_VERSION,
            "optimizer_source_sha256": optimizer_hash,
            "v8_source_sha256": v8_hash,
            "cache_input_fingerprint": manifest["input_fingerprint"],
            "split": contract.payload(),
            "grid_family_sha256": GRID_FAMILY_SHA256,
            "guards": asdict(guards),
            "coverage_mode": coverage_mode,
            "panel_hash": (panel_metadata or {}).get("panel_hash"),
            "diagnostic_only": diagnostic_only,
            "diagnostic_reasons": diagnostic_reasons,
            "lineage_certified": lineage_certified,
            "base_economics": [BASE_COST_BPS, BASE_SLIPPAGE_BPS],
            "stress_economics": [STRESS_COST_BPS, STRESS_SLIPPAGE_BPS],
        }
    )
    run_dir = _new_run_dir("search", run_fingerprint)
    candidate_path = _artifact_path(manifest, "candidates")
    path_path = _artifact_path(manifest, "paths")
    coverage_path = _artifact_path(manifest, "coverage")
    selection = {
        "schema_version": OPTIMIZER_SCHEMA_VERSION,
        "optimizer_version": OPTIMIZER_VERSION,
        "run_fingerprint": run_fingerprint,
        "generated_at_ist": common.now_ist().isoformat(timespec="microseconds"),
        "search_run_dir": str(run_dir.resolve()),
        "selection_status": status,
        "objective": OBJECTIVE,
        "selected_book": selected_book.payload() if selected_book else None,
        "selected_legs": {
            key: selected_by_leg[key].payload()
            for key in LEG_KEYS
            if key in selected_by_leg
        },
        "selected_leg_keys": [key for key in LEG_KEYS if key in selected_by_leg],
        "disabled_legs": _safe_json(train_disabled_legs),
        "selection_method": "INDEPENDENT_PER_LEG_ONLY_NO_POOLED_QUALIFICATION",
        "selected_book_rank": None,
        "train_base_metrics": (
            _safe_json(train_evaluation.portfolio_base_metrics)
            if train_evaluation is not None
            else None
        ),
        "train_stress_metrics": (
            _safe_json(train_evaluation.portfolio_stress_metrics)
            if train_evaluation is not None
            else None
        ),
        "train_portfolio_metrics_are_diagnostic_only": True,
        "train_independent_leg_results": (
            _safe_json(train_evaluation.leg_results)
            if train_evaluation is not None
            else {}
        ),
        "train_leg_guard_results": (
            {
                key: _safe_json(train_evaluation.leg_results[key]["guard"])
                for key in train_evaluation.leg_results
            }
            if train_evaluation is not None
            else {}
        ),
        "eligible_for_validation": eligible_for_validation,
        "validation_outcomes_accessed": False,
        "test_outcomes_accessed": False,
        "pf_claim_eligible": False,
        "claimed_profit_factor": None,
        "promotion_eligible": False,
        "coverage_mode": coverage_mode,
        "diagnostic_only": diagnostic_only,
        "diagnostic_reasons": diagnostic_reasons,
        "watermark": watermark,
        "lineage_certified": lineage_certified,
        "legacy_lineage_flags_absent_symbols": lineage_unknown,
        "prospective_clean_source_required_for_promotion": not lineage_certified,
        "split_contract": contract.payload(),
        "split_hash": contract.split_hash,
        "split_session_counts": {key: len(value) for key, value in sessions.items()},
        "split_coverage": _safe_json(split_coverage),
        "panel_metadata": _safe_json(panel_metadata),
        "guards": asdict(guards),
        "grid_schema_version": GRID_SCHEMA_VERSION,
        "grid_family_sha256": GRID_FAMILY_SHA256,
        "attempted_configs_per_leg": 48,
        "attempted_leg_configs_total": 48 * len(LEG_KEYS),
        "leg_frontier_counts": {key: len(frontiers[key]) for key in LEG_KEYS},
        "book_attempt_count": 0,
        "diagnostic_portfolio_replay_count": 1 if selected_book else 0,
        "multiple_testing_attempt_count": 48 * len(LEG_KEYS),
        "familywise_reference_alpha": 0.05,
        "bonferroni_reference_alpha": (
            0.05 / (48 * len(LEG_KEYS))
        ),
        "multiple_testing_warning": MULTIPLE_TESTING_WARNING,
        "source_symbol_count": len(research_symbols),
        "source_symbol_set_sha256": common.symbol_set_sha256(research_symbols),
        "cache_universe_symbol_count": len(source_symbols),
        "base_cost_bps": BASE_COST_BPS,
        "base_slippage_bps": BASE_SLIPPAGE_BPS,
        "stress_cost_bps": STRESS_COST_BPS,
        "stress_slippage_bps": STRESS_SLIPPAGE_BPS,
        "cache_manifest_path": str(Path(manifest_path).resolve()),
        "cache_manifest_size": int(Path(manifest_path).stat().st_size),
        "cache_manifest_sha256": provenance.sha256_file(manifest_path),
        "cache_input_fingerprint": manifest["input_fingerprint"],
        "optimizer_source_sha256": optimizer_hash,
        "v8_source_sha256": v8_hash,
    }
    for name, path in (
        ("candidates", candidate_path),
        ("paths", path_path),
        ("coverage", coverage_path),
    ):
        selection[f"{name}_cache_path"] = str(path)
        selection[f"{name}_cache_size"] = int(path.stat().st_size)
        selection[f"{name}_cache_sha256"] = provenance.sha256_file(path)

    outputs: dict[str, Path] = {}
    for leg_key in LEG_KEYS:
        outputs[f"trials_{leg_key}"] = _write_new_csv(
            _csv_ready(ranked_by_leg[leg_key]),
            run_dir / f"train_trials_{leg_key.replace(':', '')}.csv",
        )
    outputs["selection"] = provenance.write_immutable_json(
        run_dir / "selection.json", _safe_json(selection)
    )
    outputs["report"] = _write_new_text(
        run_dir / "report.md", render_search_report(selection)
    )
    outputs["optimizer_source_archive"] = provenance.publish_immutable_copy(
        Path(__file__), run_dir / Path(__file__).name, expected_sha256=optimizer_hash
    )
    outputs["v8_source_archive"] = provenance.publish_immutable_copy(
        Path(v8.__file__), run_dir / Path(v8.__file__).name, expected_sha256=v8_hash
    )
    if train_evaluation is not None:
        outputs["selected_train_independent_base_audit"] = _write_new_csv(
            _csv_ready(train_evaluation.input_base_audit),
            run_dir / "selected_train_independent_base_audit.csv",
        )
        outputs["selected_train_independent_stress_audit"] = _write_new_csv(
            _csv_ready(train_evaluation.input_stress_audit),
            run_dir / "selected_train_independent_stress_audit.csv",
        )
        outputs["selected_train_portfolio_base_diagnostic"] = _write_new_csv(
            _csv_ready(train_evaluation.portfolio_base_audit),
            run_dir / "selected_train_portfolio_base_diagnostic.csv",
        )
        outputs["selected_train_portfolio_stress_diagnostic"] = _write_new_csv(
            _csv_ready(train_evaluation.portfolio_stress_audit),
            run_dir / "selected_train_portfolio_stress_diagnostic.csv",
        )
    provenance_payload = provenance.build_run_provenance(
        generated_at=common.now_ist(),
        strategy_version=OPTIMIZER_VERSION,
        objective=OBJECTIVE,
        strategy_payload={
            "schema_version": OPTIMIZER_SCHEMA_VERSION,
            "grid_schema_version": GRID_SCHEMA_VERSION,
            "grid_family_sha256": GRID_FAMILY_SHA256,
            "slots": list(SLOTS),
            "sides": list(SIDES),
            "v8_strategy": v8.strategy_payload(),
        },
        parameters={
            "split_contract": contract.payload(),
            "guards": asdict(guards),
            "coverage_mode": coverage_mode,
            "diagnostic_only": diagnostic_only,
            "diagnostic_reasons": diagnostic_reasons,
            "lineage_certified": lineage_certified,
            "selection_method": "INDEPENDENT_PER_LEG_ONLY_NO_POOLED_QUALIFICATION",
            "attempted_leg_configs_total": 192,
            "book_attempt_count": 0,
            "diagnostic_portfolio_replay_count": 1 if selected_book else 0,
        },
        backtest_window={
            "from_day": contract.train_from.isoformat(),
            "through_day": contract.test_through.isoformat(),
            "outcomes_accessed": ["TRAIN"],
        },
        cache_manifest_path=manifest_path,
        cache_manifest=manifest,
        output_paths=outputs,
        results={
            "selection_status": status,
            "selected_book_hash": selected_book.config_hash if selected_book else None,
            "selected_leg_keys": list(selection["selected_leg_keys"]),
            "disabled_legs": dict(selection["disabled_legs"]),
            "eligible_for_validation": eligible_for_validation,
            "pf_claim_eligible": False,
        },
    )
    provenance_payload["run_fingerprint"] = run_fingerprint
    provenance.write_immutable_json(run_dir / "provenance.json", provenance_payload)
    return run_dir


def _evaluate_frozen_stage(
    *,
    search_run: Path,
    selection: Mapping[str, Any],
    stage: str,
    input_book: BookConfig,
    prior_stage_binding: Mapping[str, Any] | None = None,
    inherited_disabled_legs: Mapping[str, Any] | None = None,
) -> Path:
    stage_key = str(stage).upper()
    if stage_key not in {"VALIDATION", "TEST"}:
        raise ValueError("stage must be VALIDATION or TEST")
    manifest, paths = _verify_frozen_inputs(selection)
    contract = SplitContract.from_mapping(selection["split_contract"])
    sessions = split_sessions(contract)[stage_key]
    coverage = pd.read_parquet(paths["coverage"])
    symbols = _source_symbols_from_manifest(manifest)
    coverage_gate = derive_coverage(coverage, symbols=symbols, sessions=sessions)
    if not bool(coverage_gate["pass"]):
        raise DataEligibilityError(
            f"{stage_key} source coverage failed before policy evaluation: "
            f"{coverage_gate['incomplete_symbol_sessions']} incomplete symbol-sessions"
        )
    if not bool(coverage_gate.get("lineage_certified")):
        raise DataEligibilityError(
            f"{stage_key} has legacy_lineage_flags_absent; qualifying evaluation "
            "requires prospective clean source lineage"
        )
    input_book.validate()
    evaluation_id = (
        f"{stage_key.lower()}_{common.now_ist().strftime('%Y%m%dT%H%M%S%f%z')}_"
        f"{uuid.uuid4().hex[:8]}"
    )
    selection_path = search_run / "selection.json"
    selection_sha256 = provenance.sha256_file(selection_path)
    claim, registry, claim_payload = _claim_once(
        search_run=search_run,
        search_run_fingerprint=str(selection["run_fingerprint"]),
        selection_sha256=selection_sha256,
        stage=stage_key,
        evaluation_id=evaluation_id,
        input_book_hash=input_book.config_hash,
        prior_stage_binding=prior_stage_binding,
    )
    evaluation_dir = search_run / evaluation_id
    evaluation_dir.mkdir(parents=False, exist_ok=False)
    candidates = pd.read_parquet(paths["candidates"])
    minute_paths = pd.read_parquet(paths["paths"])
    prepared = prepare_dataset(candidates, minute_paths, sessions)
    guards = Guards(**dict(selection["guards"]))
    evaluation = _evaluate_independent_legs(
        prepared, input_book, stage=stage_key, guards=guards
    )
    advancing_book = evaluation.advancing_book
    advancing_keys = (
        [value.setup_id for value in advancing_book.legs]
        if advancing_book is not None
        else []
    )
    disabled_legs = {
        **dict(inherited_disabled_legs or {}),
        **evaluation.disabled_legs,
    }
    status = (
        f"{stage_key}_INDEPENDENT_LEG_SUBSET_ADVANCED"
        if advancing_book is not None
        else f"{stage_key}_NO_QUALIFYING_LEGS"
    )
    run_fingerprint = common.canonical_json_sha256(
        {
            "stage": stage_key,
            "search_run_fingerprint": selection["run_fingerprint"],
            "input_book_hash": input_book.config_hash,
            "advancing_book_hash": (
                advancing_book.config_hash if advancing_book else None
            ),
            "claim_id": claim_payload["claim_id"],
            "prior_stage_binding": dict(prior_stage_binding or {}),
            "sessions": [value.isoformat() for value in sessions],
            "policy_evaluation_count": 1,
            "base_economics": [BASE_COST_BPS, BASE_SLIPPAGE_BPS],
            "stress_economics": [STRESS_COST_BPS, STRESS_SLIPPAGE_BPS],
        }
    )
    result = {
        "schema_version": OPTIMIZER_SCHEMA_VERSION,
        "run_fingerprint": run_fingerprint,
        "evaluation_id": evaluation_id,
        "claim_id": claim_payload["claim_id"],
        "stage": stage_key,
        "status": status,
        "search_run_dir": str(search_run),
        "search_run_fingerprint": selection["run_fingerprint"],
        "book_hash": input_book.config_hash,
        "input_book": input_book.payload(),
        "input_leg_keys": [value.setup_id for value in input_book.legs],
        "advancing_book": advancing_book.payload() if advancing_book else None,
        "advancing_book_hash": advancing_book.config_hash if advancing_book else None,
        "advancing_leg_keys": advancing_keys,
        "disabled_legs": _safe_json(disabled_legs),
        "leg_results": _safe_json(evaluation.leg_results),
        "prior_stage_binding": _safe_json(dict(prior_stage_binding or {})),
        "selection_sha256": selection_sha256,
        "policy_evaluation_count": 1,
        "source_coverage": coverage_gate,
        "lineage_certified": True,
        "base_metrics": _safe_json(evaluation.portfolio_base_metrics),
        "stress_metrics": _safe_json(evaluation.portfolio_stress_metrics),
        "portfolio_diagnostic": {
            "cannot_qualify_or_disqualify_any_leg": True,
            "base_metrics": _safe_json(evaluation.portfolio_base_metrics),
            "stress_metrics": _safe_json(evaluation.portfolio_stress_metrics),
        },
        "leg_guard_results": {
            key: value["guard"] for key, value in evaluation.leg_results.items()
        },
        "guards": asdict(guards),
        "pf_claim_eligible": False,
        "claimed_profit_factor": None,
        "claimed_leg_profit_factors": {
            key: value["claimed_profit_factor"]
            for key, value in evaluation.leg_results.items()
            if value["pf_claim_eligible"]
        },
        "eligible_for_test": bool(
            advancing_book is not None and stage_key == "VALIDATION"
        ),
        "test_outcomes_accessed": stage_key == "TEST",
        "multiple_testing_context": MULTIPLE_TESTING_WARNING,
        "promotion_eligible": False,
        "base_cost_bps": BASE_COST_BPS,
        "base_slippage_bps": BASE_SLIPPAGE_BPS,
        "stress_cost_bps": STRESS_COST_BPS,
        "stress_slippage_bps": STRESS_SLIPPAGE_BPS,
    }
    outputs: dict[str, Path] = {
        "claim": claim,
        "stage_registry": registry,
        "independent_base_audit": _write_new_csv(
            _csv_ready(evaluation.input_base_audit),
            evaluation_dir / f"{stage_key.lower()}_independent_base_audit.csv",
        ),
        "independent_stress_audit": _write_new_csv(
            _csv_ready(evaluation.input_stress_audit),
            evaluation_dir / f"{stage_key.lower()}_independent_stress_audit.csv",
        ),
        "portfolio_base_diagnostic": _write_new_csv(
            _csv_ready(evaluation.portfolio_base_audit),
            evaluation_dir / f"{stage_key.lower()}_portfolio_base_diagnostic.csv",
        ),
        "portfolio_stress_diagnostic": _write_new_csv(
            _csv_ready(evaluation.portfolio_stress_audit),
            evaluation_dir / f"{stage_key.lower()}_portfolio_stress_diagnostic.csv",
        ),
    }
    outputs["result"] = provenance.write_immutable_json(
        evaluation_dir / "result.json", _safe_json(result)
    )
    outputs["report"] = _write_new_text(
        evaluation_dir / "report.md", _stage_report(result)
    )
    provenance_payload = provenance.build_run_provenance(
        generated_at=common.now_ist(),
        strategy_version=OPTIMIZER_VERSION,
        objective=f"ONE_SHOT_INDEPENDENT_PER_LEG_{stage_key}_SUBSET_GATE",
        strategy_payload={
            "schema_version": OPTIMIZER_SCHEMA_VERSION,
            "input_book": input_book.payload(),
            "advancing_book": advancing_book.payload() if advancing_book else None,
            "v8_strategy": v8.strategy_payload(),
        },
        parameters={
            "stage": stage_key,
            "policy_evaluation_count": 1,
            "qualification_scope": "EACH_LEG_INDEPENDENTLY_NO_POOLED_GATES",
            "guards": asdict(guards),
            "base_economics": [BASE_COST_BPS, BASE_SLIPPAGE_BPS],
            "stress_economics": [STRESS_COST_BPS, STRESS_SLIPPAGE_BPS],
        },
        backtest_window={
            "from_day": min(sessions).isoformat(),
            "through_day": max(sessions).isoformat(),
            "outcomes_accessed": [stage_key],
        },
        cache_manifest_path=paths["manifest"],
        cache_manifest=manifest,
        output_paths=outputs,
        results={
            "status": status,
            "book_hash": input_book.config_hash,
            "input_leg_keys": list(result["input_leg_keys"]),
            "advancing_book_hash": result["advancing_book_hash"],
            "advancing_leg_keys": list(advancing_keys),
            "disabled_legs": dict(result["disabled_legs"]),
            "pf_claim_eligible": False,
            "claimed_profit_factor": None,
            "claimed_leg_profit_factors": dict(
                result["claimed_leg_profit_factors"]
            ),
            "eligible_for_test": result["eligible_for_test"],
        },
    )
    provenance_payload["run_fingerprint"] = run_fingerprint
    provenance_payload["search_run_fingerprint"] = selection["run_fingerprint"]
    provenance_payload["stage_claim_binding"] = {
        "claim_id": claim_payload["claim_id"],
        "evaluation_id": evaluation_id,
        "stage": stage_key,
        "search_run_fingerprint": selection["run_fingerprint"],
        "selection_sha256": selection_sha256,
        "input_book_hash": input_book.config_hash,
        "prior_stage_binding": _safe_json(dict(prior_stage_binding or {})),
    }
    provenance.write_immutable_json(
        evaluation_dir / "provenance.json", provenance_payload
    )
    return evaluation_dir


def execute_validation(args: argparse.Namespace) -> Path:
    search_run = Path(args.search_run).resolve()
    selection = load_authenticated_selection(search_run)
    if not bool(selection.get("eligible_for_validation")):
        raise StageAccessError(
            "TRAIN did not freeze an eligible winner; VALIDATION remains locked"
        )
    if selection.get("selected_book") is None:
        raise StageAccessError("TRAIN selection has no independently frozen leg subset")
    if bool(selection.get("diagnostic_only")):
        raise StageAccessError("diagnostic research selections cannot access VALIDATION")
    if not bool(selection.get("lineage_certified")):
        raise StageAccessError("uncertified historical lineage cannot access VALIDATION")
    book = BookConfig.from_payload(dict(selection["selected_book"]))
    return _evaluate_frozen_stage(
        search_run=search_run,
        selection=selection,
        stage="VALIDATION",
        input_book=book,
        inherited_disabled_legs=dict(selection.get("disabled_legs") or {}),
    )


def execute_test(args: argparse.Namespace) -> Path:
    validation_run = Path(args.validation_run).resolve()
    validation = load_authenticated_stage_result(validation_run)
    if str(validation.get("stage")) != "VALIDATION":
        raise StageAccessError("provided run is not an authenticated VALIDATION")
    if not bool(validation.get("eligible_for_test")):
        raise StageAccessError(
            "no independently gated leg passed VALIDATION; TEST remains locked"
        )
    search_run = Path(str(validation["search_run_dir"])).resolve()
    if validation_run.parent.resolve() != search_run:
        raise AssertionError("VALIDATION run is outside its frozen TRAIN run")
    selection = load_authenticated_selection(search_run)
    validation_provenance = json.loads(
        (validation_run / "provenance.json").read_text(encoding="utf-8")
    )
    validation_results = dict(validation_provenance.get("results", {}))
    if (
        str(validation.get("search_run_fingerprint", ""))
        != str(selection.get("run_fingerprint", ""))
        or str(validation_provenance.get("search_run_fingerprint", ""))
        != str(selection.get("run_fingerprint", ""))
        or str(validation_results.get("book_hash", ""))
        != str(validation.get("book_hash", ""))
        or str(validation_results.get("advancing_book_hash", ""))
        != str(validation.get("advancing_book_hash", ""))
        or list(validation_results.get("advancing_leg_keys", []))
        != list(validation.get("advancing_leg_keys", []))
        or bool(validation_results.get("eligible_for_test"))
        != bool(validation.get("eligible_for_test"))
    ):
        raise AssertionError("VALIDATION result does not match frozen provenance")
    if dict(selection.get("selected_book") or {}).get("config_hash") != str(
        validation.get("book_hash", "")
    ):
        raise AssertionError("VALIDATION book differs from TRAIN selection")
    advancing_payload = validation.get("advancing_book")
    if not advancing_payload:
        raise StageAccessError("VALIDATION did not freeze a TEST-eligible leg subset")
    test_book = BookConfig.from_payload(dict(advancing_payload))
    if test_book.config_hash != str(validation.get("advancing_book_hash", "")):
        raise AssertionError("VALIDATION advancing subset hash is invalid")
    prior_stage_binding = {
        "stage": "VALIDATION",
        "run_dir": str(validation_run),
        "evaluation_id": validation["evaluation_id"],
        "claim_id": validation["claim_id"],
        "run_fingerprint": validation["run_fingerprint"],
        "result_sha256": provenance.sha256_file(validation_run / "result.json"),
        "provenance_sha256": provenance.sha256_file(
            validation_run / "provenance.json"
        ),
        "advancing_book_hash": test_book.config_hash,
        "advancing_leg_keys": list(validation["advancing_leg_keys"]),
    }
    return _evaluate_frozen_stage(
        search_run=search_run,
        selection=selection,
        stage="TEST",
        input_book=test_book,
        prior_stage_binding=prior_stage_binding,
        inherited_disabled_legs=dict(validation.get("disabled_legs") or {}),
    )


def validate_preregistered_contract() -> None:
    if SLOTS != ("09:50", "09:55") or LEG_KEYS != (
        "09:50_LONG",
        "09:50_SHORT",
        "09:55_LONG",
        "09:55_SHORT",
    ):
        raise AssertionError("V9 optimizer scope changed")
    derived_clocks = sorted(
        {
            slot
            for signal in SLOTS
            for slot in (
                (datetime.strptime(signal, "%H:%M") - pd.Timedelta(minutes=5)).strftime("%H:%M"),
                signal,
            )
        }
    )
    if tuple(derived_clocks) != REQUIRED_FUTURES_TIMES:
        raise AssertionError("V9 predecessor+signal OI coverage grid changed")
    if any(len(LEG_GRIDS[key]) != 48 for key in LEG_KEYS):
        raise AssertionError("V9 grid count changed")
    frozen_contract = default_split_contract()
    frozen_sessions = split_sessions(frozen_contract)
    if frozen_contract.payload() != {
        "train_from": "2026-05-27",
        "train_through": "2026-07-09",
        "validation_from": "2026-07-10",
        "validation_through": "2026-07-23",
        "test_from": "2026-07-24",
        "test_through": "2026-07-31",
    } or {key: len(value) for key, value in frozen_sessions.items()} != {
        "TRAIN": 30,
        "VALIDATION": 10,
        "TEST": 6,
    }:
        raise AssertionError("primary 30/10/6 split contract changed")
    for key in LEG_KEYS:
        if any(config.setup_id != key or config.max_entries != 2 for config in LEG_GRIDS[key]):
            raise AssertionError(f"V9 grid contains an invalid config for {key}")
    if (BASE_COST_BPS, BASE_SLIPPAGE_BPS) != (15.0, 1.0):
        raise AssertionError("base economics changed")
    if (STRESS_COST_BPS, STRESS_SLIPPAGE_BPS) != (20.0, 2.0):
        raise AssertionError("stress economics changed")
    if (
        CACHE_FLOOR_PRICE_CHANGE_PCT,
        CACHE_FLOOR_OI_CHANGE_PCT,
        CACHE_FLOOR_VOLUME_RATIO,
    ) != (0.10, 0.05, 0.80):
        raise AssertionError("neutral V9 cache authority floors changed")
    guards = Guards()
    if (
        guards.train_min_fills_per_leg,
        guards.train_min_active_days,
        guards.train_min_leg_pf,
        guards.train_min_leg_robust_pf,
        guards.train_max_leg_top_day_share,
    ) != (40, 15, 1.50, 1.20, 0.25):
        raise AssertionError("TRAIN per-leg evidence contract changed")
    if (
        guards.validation_min_fills_per_leg,
        guards.validation_min_active_days,
        guards.validation_min_leg_pf,
        guards.validation_max_leg_top_day_share,
    ) != (15, 8, 1.50, 0.35):
        raise AssertionError("VALIDATION per-leg evidence contract changed")
    if (
        guards.test_min_fills_per_leg,
        guards.test_min_active_days,
        guards.test_min_leg_pf,
    ) != (10, 6, 1.50):
        raise AssertionError("TEST per-leg evidence contract changed")


def _add_split_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--train-from", default=DEFAULT_TRAIN_FROM)
    parser.add_argument("--train-through", default=DEFAULT_TRAIN_THROUGH)
    parser.add_argument("--validation-from", default=DEFAULT_VALIDATION_FROM)
    parser.add_argument("--validation-through", default=DEFAULT_VALIDATION_THROUGH)
    parser.add_argument("--test-from", default=DEFAULT_TEST_FROM)
    parser.add_argument("--test-through", default=DEFAULT_TEST_THROUGH)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Honest FnO V9 09:50/09:55 optimizer on V8 execution"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    search = subparsers.add_parser("search")
    search.add_argument("--source-snapshot", required=True)
    search.add_argument("--symbols")
    search.add_argument(
        "--coverage-mode",
        choices=("full-universe", "rectangular-panel"),
        default="full-universe",
        help=(
            "full-universe is primary and fail-closed; rectangular-panel is "
            "TRAIN-only diagnostic and cannot unlock later stages"
        ),
    )
    search.add_argument(
        "--allow-diagnostic-research",
        action="store_true",
        help=(
            "explicitly watermark and permanently lock a custom-symbol, "
            "custom-split, rectangular-panel or lineage-unknown research run"
        ),
    )
    _add_split_args(search)
    validation = subparsers.add_parser("evaluate-validation")
    validation.add_argument("--search-run", required=True)
    test = subparsers.add_parser("evaluate-test")
    test.add_argument("--validation-run", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    validate_preregistered_contract()
    args = build_parser().parse_args(argv)
    if args.command == "search":
        print(execute_search(args))
        return 0
    if args.command == "evaluate-validation":
        print(execute_validation(args))
        return 0
    if args.command == "evaluate-test":
        print(execute_test(args))
        return 0
    raise AssertionError(f"unhandled command {args.command!r}")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (DataEligibilityError, StageAccessError, ValueError) as exc:
        print(f"ERROR: {exc}", file=os.sys.stderr)
        raise SystemExit(2) from exc
