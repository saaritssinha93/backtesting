"""Flexible single-setup parameter sweep for the FNO V8 windowed strategy.

``fno_v8_windowed_1m_entry_optimize`` deliberately freezes the five-minute
setup book and searches only the one-minute entry seam.  This module does the
opposite: it varies one setup's own authority fields (picker, cap, price/OI/
volume/traded-value thresholds, candle morphology and the stop/target bracket)
*together with* the entry seam, for a single ``setup_id``.

That is a strictly larger and far more overfittable space, and a single V8
setup carries very few candidates per split.  Every run is therefore
watermarked, no run can emit a deployable champion, and the retrospective TEST
window is never touched by this module at all.

Search modes
------------
``grid``        full cartesian product of the supplied axis values
``random``      seeded uniform sampling of the full axis space
``coordinate``  steepest-ascent coordinate descent from one or more seeds
``hybrid``      random sampling, then coordinate polish of the best seeds

Cache floors
------------
The V8 candidate cache is built with the frozen book's ``price_change_pct``,
``oi_change_pct``, ``volume_ratio`` and ``min_traded_value`` already applied by
``_setup_eligible_rows``.  Values below those floors are unobservable in the
cache, so the sweep fails closed on an explicit request rather than silently
reporting a truncated pool.  Loosening below a floor requires editing
``ACTIVE_SETUPS`` (and its book hash) and rebuilding the cache.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import random
import sys
import warnings
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import numpy as np
import pandas as pd

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_v8_windowed_1m_entry_backtest as v8
import fno_v8_windowed_1m_entry_optimize as opt


SWEEP_VERSION = "FNO_V8_SETUP_PARAM_SWEEP_20260819_V1"
SWEEP_SCHEMA_VERSION = "fno_v8_setup_param_sweep_v1"
RESULT_ROOT = v8.V8_ROOT / "setup_param_sweeps"

SWEEP_WATERMARK = (
    "SETUP_BOOK_PARAMETER_SWEEP; THE_FROZEN_V8_FIVE_MINUTE_AUTHORITY_WAS_VARIED; "
    "SINGLE_SETUP_THIN_SAMPLE; MULTIPLE_TESTING_UNCORRECTED; "
    "HYPOTHESIS_GENERATION_ONLY; NOT_PROMOTABLE"
)

DEFAULT_FIT_FROM = "2026-07-13"
DEFAULT_FIT_THROUGH = "2026-07-22"

# The V8 cache key includes its date span, so a narrower request rebuilds the
# whole cache instead of reusing it.  Load the span the cache was built for and
# slice the fit window out of it; sessions outside the fit window are never
# prepared, simulated or scored.
DEFAULT_CACHE_FROM = "2026-07-13"
DEFAULT_CACHE_THROUGH = "2026-07-31"

SELECTION_COST_BPS = opt.SELECTION_COST_BPS
SELECTION_SLIPPAGE_BPS = opt.SELECTION_SLIPPAGE_BPS
STRESS_COST_BPS = opt.STRESS_COST_BPS
STRESS_SLIPPAGE_BPS = opt.STRESS_SLIPPAGE_BPS

PICKERS = ("max_liquidity", "max_move", "max_volume", "max_oi")

PARAMETER_ORDER = (
    "picker",
    "max_entries",
    "price_change_pct",
    "oi_change_pct",
    "volume_ratio",
    "min_traded_value",
    "body_ratio",
    "max_wick_ratio",
    "stop_pct",
    "target_pct",
    "max_confirmation_minute",
    "entry_expiry_minute",
    "buffer_bps",
    "midpoint_invalidation",
    "close_location_min",
)

# Fields the cache builder already filtered on; a sweep may only tighten them.
CACHE_FLOOR_PARAMETERS = (
    "price_change_pct",
    "oi_change_pct",
    "volume_ratio",
    "min_traded_value",
)

DEFAULT_AXES: dict[str, tuple[Any, ...]] = {
    "picker": PICKERS,
    "max_entries": (1, 2, 3),
    "price_change_pct": (0.20, 0.30, 0.45, 0.60, 0.80, 1.00),
    "oi_change_pct": (0.10, 0.25, 0.50, 1.00, 1.50),
    "volume_ratio": (1.0, 2.0, 3.0, 4.0, 5.0, 6.0),
    "min_traded_value": (0.0, 5.0e7, 1.0e8, 2.0e8),
    "body_ratio": (0.0, 0.15, 0.30, 0.45, 0.60),
    "max_wick_ratio": (0.20, 0.35, 0.50, 0.70, 1.00),
    "stop_pct": (0.30, 0.40, 0.50, 0.75, 1.00),
    "target_pct": (1.00, 1.50, 2.00, 2.50, 3.00),
    "max_confirmation_minute": (1, 2, 3, 4),
    "entry_expiry_minute": (5,),
    "buffer_bps": (0.0, 2.0, 5.0),
    "midpoint_invalidation": (False, True),
    "close_location_min": (None, 0.50, 0.75),
}

OBJECTIVES = ("combined", "profit_factor", "trade_count")

BOOLEAN_PARAMETERS = ("midpoint_invalidation",)
INTEGER_PARAMETERS = (
    "max_entries",
    "max_confirmation_minute",
    "entry_expiry_minute",
)
OPTIONAL_FLOAT_PARAMETERS = ("close_location_min",)


class SweepEligibilityError(RuntimeError):
    """The cache cannot support the requested sweep honestly."""


@dataclass(frozen=True)
class SweepConfig:
    picker: str
    max_entries: int
    price_change_pct: float
    oi_change_pct: float
    volume_ratio: float
    min_traded_value: float
    body_ratio: float
    max_wick_ratio: float
    stop_pct: float
    target_pct: float
    max_confirmation_minute: int
    entry_expiry_minute: int
    buffer_bps: float
    midpoint_invalidation: bool
    close_location_min: float | None

    def infeasible_reason(self) -> str | None:
        if self.picker not in PICKERS:
            return f"unknown picker {self.picker!r}"
        if int(self.max_entries) < 1:
            return "max_entries must be at least 1"
        for name in (
            "price_change_pct",
            "oi_change_pct",
            "volume_ratio",
            "min_traded_value",
            "buffer_bps",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0:
                return f"{name} must be finite and non-negative"
        for name in ("body_ratio", "max_wick_ratio"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                return f"{name} must lie in [0, 1]"
        for name in ("stop_pct", "target_pct"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value <= 0:
                return f"{name} must be positive"
        if int(self.max_confirmation_minute) < 1:
            return "max_confirmation_minute must be positive"
        if int(self.entry_expiry_minute) <= int(self.max_confirmation_minute):
            return "entry_expiry_minute must be later than max_confirmation_minute"
        if self.close_location_min is not None:
            value = float(self.close_location_min)
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                return "close_location_min must be None or lie in [0, 1]"
        return None

    @property
    def feasible(self) -> bool:
        return self.infeasible_reason() is None

    def values(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in PARAMETER_ORDER}

    def payload(self) -> dict[str, Any]:
        return {
            **self.values(),
            "post_confirmation_cancel": True,
            "allow_cap_reassignment": True,
            "same_bar_policy": "STOP_FIRST",
            "square_off": "15:30",
            "eod_policy": "EXACT_SQUARE_OFF",
        }

    @property
    def config_hash(self) -> str:
        return common.canonical_json_sha256(
            {"schema_version": SWEEP_SCHEMA_VERSION, **self.payload()}
        )

    def distance_from(self, other: "SweepConfig") -> int:
        return int(
            sum(
                1
                for name in PARAMETER_ORDER
                if getattr(self, name) != getattr(other, name)
            )
        )

    def replace_value(self, name: str, value: Any) -> "SweepConfig":
        if name not in PARAMETER_ORDER:
            raise ValueError(f"unknown sweep parameter {name!r}")
        return replace(self, **{name: value})

    @classmethod
    def from_values(cls, values: Mapping[str, Any]) -> "SweepConfig":
        close_location = values.get("close_location_min")
        # Round-tripping through a DataFrame turns the optional None into NaN.
        if isinstance(close_location, float) and math.isnan(close_location):
            close_location = None
        return cls(
            picker=str(values["picker"]),
            max_entries=int(values["max_entries"]),
            price_change_pct=float(values["price_change_pct"]),
            oi_change_pct=float(values["oi_change_pct"]),
            volume_ratio=float(values["volume_ratio"]),
            min_traded_value=float(values["min_traded_value"]),
            body_ratio=float(values["body_ratio"]),
            max_wick_ratio=float(values["max_wick_ratio"]),
            stop_pct=float(values["stop_pct"]),
            target_pct=float(values["target_pct"]),
            max_confirmation_minute=int(values["max_confirmation_minute"]),
            entry_expiry_minute=int(values["entry_expiry_minute"]),
            buffer_bps=float(values["buffer_bps"]),
            midpoint_invalidation=bool(values["midpoint_invalidation"]),
            close_location_min=(
                None if close_location is None else float(close_location)
            ),
        )

    @classmethod
    def from_setup(
        cls, setup: v8.V8Setup, *, entry: Mapping[str, Any] | None = None
    ) -> "SweepConfig":
        """Baseline config: the frozen book leg plus a B0-style entry seam."""

        entry = dict(entry or {})
        return cls(
            picker=str(setup.picker),
            max_entries=int(setup.max_entries),
            price_change_pct=float(setup.price_change_pct),
            oi_change_pct=float(setup.oi_change_pct),
            volume_ratio=float(setup.volume_ratio),
            min_traded_value=float(setup.min_traded_value),
            body_ratio=float(setup.body_ratio),
            max_wick_ratio=float(setup.max_wick_ratio),
            stop_pct=float(setup.stop_pct),
            target_pct=float(setup.target_pct),
            max_confirmation_minute=int(entry.get("max_confirmation_minute", 1)),
            entry_expiry_minute=int(entry.get("entry_expiry_minute", 5)),
            buffer_bps=float(entry.get("buffer_bps", 0.0)),
            midpoint_invalidation=bool(entry.get("midpoint_invalidation", False)),
            close_location_min=entry.get("close_location_min"),
        )


@dataclass
class SetupOccurrence:
    session_date: date
    candidates: tuple[v8.CandidateInput, ...]
    bars_by_symbol: dict[str, tuple[v8.MinuteBar, ...]]


@dataclass
class PreparedSetup:
    setup_id: str
    side: str
    occurrences: tuple[SetupOccurrence, ...]
    session_dates: tuple[date, ...]

    @property
    def candidate_count(self) -> int:
        return int(sum(len(item.candidates) for item in self.occurrences))

    @property
    def active_sessions(self) -> int:
        return int(sum(1 for item in self.occurrences if item.candidates))


def active_setup_by_id(setup_id: str) -> v8.V8Setup:
    for setup in v8.ACTIVE_SETUPS:
        if setup.setup_id == str(setup_id):
            return setup
    known = ", ".join(sorted(item.setup_id for item in v8.ACTIVE_SETUPS))
    raise ValueError(f"unknown setup_id {setup_id!r}; known ids: {known}")


def prepare_setup_dataset(
    candidates: pd.DataFrame,
    minute_paths: pd.DataFrame,
    *,
    setup_id: str,
    session_dates: Sequence[date],
) -> PreparedSetup:
    setup = active_setup_by_id(setup_id)
    selected, paths = opt._slice_split(candidates, minute_paths, session_dates)
    selected = selected.loc[selected["setup_id"].astype(str).eq(setup.setup_id)]
    selected_ids = set(selected["candidate_id"].astype(str))
    paths = paths.loc[paths["candidate_id"].astype(str).isin(selected_ids)]
    path_map = {
        str(candidate_id): tuple(v8._minute_bars_from_cache(group))
        for candidate_id, group in paths.groupby("candidate_id", sort=False)
    }
    occurrences: list[SetupOccurrence] = []
    for session_value, group in selected.groupby("session_date", sort=True):
        rows = group.to_dict("records")
        inputs = tuple(v8._candidate_from_cache_row(row) for row in rows)
        bars = {
            str(row["symbol"]): path_map.get(str(row["candidate_id"]), tuple())
            for row in rows
        }
        occurrences.append(
            SetupOccurrence(opt._parse_day(session_value), inputs, bars)
        )
    return PreparedSetup(
        setup.setup_id,
        setup.side,
        tuple(occurrences),
        tuple(sorted({opt._parse_day(value) for value in session_dates})),
    )


def build_setup(base: v8.V8Setup, config: SweepConfig) -> v8.V8Setup:
    # Clear any per-leg entry-seam override carried by the book. This sweep
    # varies the entry seam itself and passes its own EntryPolicy straight to
    # simulate_setup_window, so a stale override must not shadow it if this
    # ever gets routed through v8.policy_for_setup.
    return replace(
        base,
        entry_conf_minute=None,
        entry_buffer_bps=None,
        entry_midpoint=None,
        entry_clv=v8.ENTRY_INHERIT,
        max_entries=int(config.max_entries),
        picker=str(config.picker),
        price_change_pct=float(config.price_change_pct),
        oi_change_pct=float(config.oi_change_pct),
        volume_ratio=float(config.volume_ratio),
        body_ratio=float(config.body_ratio),
        max_wick_ratio=float(config.max_wick_ratio),
        min_traded_value=float(config.min_traded_value),
        stop_pct=float(config.stop_pct),
        target_pct=float(config.target_pct),
    )


def build_policy(
    config: SweepConfig, *, cost_bps: float, slippage_bps: float
) -> v8.EntryPolicy:
    policy = v8.EntryPolicy(
        buffer_bps=float(config.buffer_bps),
        max_confirmation_minute=int(config.max_confirmation_minute),
        entry_expiry_minute=int(config.entry_expiry_minute),
        close_location_min=config.close_location_min,
        cost_bps=float(cost_bps),
        slippage_bps=float(slippage_bps),
        midpoint_invalidation=bool(config.midpoint_invalidation),
        post_confirmation_cancel=True,
        allow_cap_reassignment=True,
        same_bar_policy="STOP_FIRST",
        square_off="15:30",
        eod_policy="EXACT_SQUARE_OFF",
    )
    v8.validate_backtest_policy(policy)
    return policy


def run_config(
    prepared: PreparedSetup,
    base_setup: v8.V8Setup,
    config: SweepConfig,
    *,
    cost_bps: float,
    slippage_bps: float,
    portfolio: v8.PortfolioPolicy,
) -> pd.DataFrame:
    """Simulate one swept configuration over the prepared setup occurrences."""

    setup = build_setup(base_setup, config)
    policy = build_policy(config, cost_bps=cost_bps, slippage_bps=slippage_bps)
    parts: list[pd.DataFrame] = []
    for occurrence in prepared.occurrences:
        eligible = tuple(
            candidate
            for candidate in occurrence.candidates
            if v8.five_minute_candidate_passes(setup, candidate)
        )
        if not eligible:
            continue
        ranked = sorted(
            eligible,
            key=lambda candidate: (
                -v8._picker_value(setup, candidate),
                -float(candidate.traded_value),
                str(candidate.symbol),
            ),
        )
        rank_by_symbol = {
            candidate.symbol: index
            for index, candidate in enumerate(ranked, start=1)
        }
        picker_by_symbol = {
            candidate.symbol: float(v8._picker_value(setup, candidate))
            for candidate in eligible
        }
        bars = {
            candidate.symbol: occurrence.bars_by_symbol.get(
                candidate.symbol, tuple()
            )
            for candidate in eligible
        }
        audit = v8.simulate_setup_window(setup, eligible, bars, policy)
        if audit.empty:
            continue
        audit["frozen_rank"] = (
            audit["symbol"].map(rank_by_symbol).fillna(0).astype(int)
        )
        audit["picker"] = setup.picker
        audit["picker_value"] = audit["symbol"].map(picker_by_symbol)
        audit["sweep_config_hash"] = config.config_hash
        audit["buffer_bps"] = policy.buffer_bps
        audit["cost_bps"] = policy.cost_bps
        audit["slippage_bps"] = policy.slippage_bps
        audit["eod_policy"] = policy.eod_policy
        parts.append(audit)
    if not parts:
        return pd.DataFrame()
    with warnings.catch_warnings():
        # Terminal audit frames legitimately carry all-NA columns for states
        # that never reached a fill; pandas only warns about the future dtype
        # inference change, which does not affect these results.
        warnings.filterwarnings(
            "ignore",
            message="The behavior of DataFrame concatenation with empty",
            category=FutureWarning,
        )
        out = pd.concat(parts, ignore_index=True, sort=False)
    out = opt._attach_economics(
        out,
        cost_bps=cost_bps,
        target_exposure_per_entry_rs=float(portfolio.target_exposure_per_entry_rs),
    )
    out = v8.apply_global_portfolio_constraints(out, portfolio)
    out["portfolio_mode"] = v8.PORTFOLIO_MODE
    return out.sort_values(
        ["session_date", "signal_time", "frozen_rank", "symbol"], kind="stable"
    ).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def _finite(value: Any, default: float = -math.inf) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def score_config(
    prepared: PreparedSetup,
    base_setup: v8.V8Setup,
    config: SweepConfig,
    *,
    guards: opt.SelectionGuards,
    portfolio: v8.PortfolioPolicy,
    baseline: SweepConfig,
    with_stress: bool = True,
) -> dict[str, Any]:
    base_audit = run_config(
        prepared,
        base_setup,
        config,
        cost_bps=SELECTION_COST_BPS,
        slippage_bps=SELECTION_SLIPPAGE_BPS,
        portfolio=portfolio,
    )
    metrics = opt.score_audit(base_audit, prepared.session_dates)
    record: dict[str, Any] = {
        "setup_id": prepared.setup_id,
        "side": prepared.side,
        "config_hash": config.config_hash,
        "config": config.payload(),
        "distance_from_book": config.distance_from(baseline),
        # Two configurations that differ only in a non-binding parameter
        # produce the same trades; collapse them so the ranked tables show
        # distinct behaviours rather than repeated ones.
        "behavior_signature": opt.behavior_signature(base_audit),
        "guard_pass": opt.side_guard_pass(metrics, guards),
        **metrics,
    }
    for name in PARAMETER_ORDER:
        record[f"param_{name}"] = getattr(config, name)
    if with_stress:
        stress_audit = run_config(
            prepared,
            base_setup,
            config,
            cost_bps=STRESS_COST_BPS,
            slippage_bps=STRESS_SLIPPAGE_BPS,
            portfolio=portfolio,
        )
        stress = opt.score_audit(stress_audit, prepared.session_dates)
        record["stress_profit_factor"] = stress["profit_factor"]
        record["stress_net_return_percentage_points"] = stress[
            "net_return_percentage_points"
        ]
        record["stress_closed_fills"] = stress["closed_fills"]
        record["stress_survives"] = bool(
            _finite(stress["profit_factor"]) >= 1.0
            and float(stress["net_return_percentage_points"]) > 0.0
        )
    return record


def objective_key(
    record: Mapping[str, Any], objective: str, *, min_fills: int
) -> tuple:
    """Rank key for a trial record.  Larger is better in every position."""

    fills = int(record.get("closed_fills", 0))
    pf = _finite(record.get("profit_factor"))
    robust = _finite(record.get("robust_profit_factor_ex_best_day"))
    trades = _finite(record.get("trades_per_session"), 0.0)
    guard = 1 if bool(record.get("guard_pass")) else 0
    enough = 1 if fills >= int(min_fills) else 0
    proximity = -int(record.get("distance_from_book", 0))
    key = str(objective).lower()
    if key == "profit_factor":
        return (enough, pf, robust, trades, proximity)
    if key == "trade_count":
        profitable = 1 if pf >= 1.0 else 0
        return (enough, profitable, trades, pf, proximity)
    if key == "combined":
        return (guard, enough, trades, pf, robust, proximity)
    raise ValueError(f"unknown objective {objective!r}")


# ---------------------------------------------------------------------------
# Axis handling and cache floors
# ---------------------------------------------------------------------------


def parse_axis_values(name: str, raw: str) -> tuple[Any, ...]:
    tokens = [item.strip() for item in str(raw).split(",") if item.strip()]
    if not tokens:
        raise ValueError(f"--{name.replace('_', '-')} requires at least one value")
    values: list[Any] = []
    for token in tokens:
        lowered = token.lower()
        if name == "picker":
            if lowered not in PICKERS:
                raise ValueError(
                    f"unknown picker {token!r}; expected one of {', '.join(PICKERS)}"
                )
            values.append(lowered)
        elif name in BOOLEAN_PARAMETERS:
            if lowered in {"1", "true", "on", "yes"}:
                values.append(True)
            elif lowered in {"0", "false", "off", "no"}:
                values.append(False)
            else:
                raise ValueError(f"{name} expects true/false, got {token!r}")
        elif name in OPTIONAL_FLOAT_PARAMETERS and lowered in {"none", "null", "off"}:
            values.append(None)
        elif name in INTEGER_PARAMETERS:
            values.append(int(token))
        else:
            values.append(float(token))
    deduped: list[Any] = []
    for value in values:
        if value not in deduped:
            deduped.append(value)
    return tuple(deduped)


def cache_floors(base_setup: v8.V8Setup) -> dict[str, float]:
    """Thresholds already applied when the candidate cache was built."""

    return {
        "price_change_pct": float(base_setup.price_change_pct),
        "oi_change_pct": float(base_setup.oi_change_pct),
        "volume_ratio": float(base_setup.volume_ratio),
        "min_traded_value": float(base_setup.min_traded_value),
    }


def enforce_cache_floors(
    axes: Mapping[str, Sequence[Any]],
    floors: Mapping[str, float],
    *,
    explicit: Iterable[str],
    clamp: bool = False,
) -> tuple[dict[str, tuple[Any, ...]], list[dict[str, Any]]]:
    """Drop unobservable values; fail closed when the user asked for them.

    ``clamp`` collapses below-floor values onto the floor instead of failing,
    which is what a multi-setup batch needs: one shared axis list cannot
    satisfy eight legs whose frozen book thresholds differ.  The collapse is
    always recorded in ``floor_notes`` so the report states what was actually
    searched rather than what was requested.
    """

    explicit_names = set(explicit)
    adjusted: dict[str, tuple[Any, ...]] = {}
    notes: list[dict[str, Any]] = []
    for name, values in axes.items():
        floor = floors.get(name)
        if floor is None:
            adjusted[name] = tuple(values)
            continue
        below = [value for value in values if float(value) + 1e-12 < floor]
        kept = tuple(value for value in values if float(value) + 1e-12 >= floor)
        if below and name in explicit_names and not clamp:
            raise SweepEligibilityError(
                f"{name} values {below} are below the cache floor {floor:g}. "
                "The V8 candidate cache was built with the frozen book "
                "threshold already applied, so those rows do not exist and a "
                "sweep over them would silently report a truncated pool. "
                "Raise the values, pass --clamp-below-floor to collapse them "
                "onto the floor, or edit ACTIVE_SETUPS (plus "
                "V8_SETUP_BOOK_SHA256) and rebuild the cache."
            )
        if below and clamp:
            kept = tuple(sorted({float(floor), *(float(v) for v in kept)}))
            notes.append(
                {
                    "parameter": name,
                    "cache_floor": floor,
                    "clamped_to_floor": below,
                    "note": "below-floor values collapsed onto the cache floor",
                }
            )
        elif below:
            notes.append(
                {
                    "parameter": name,
                    "cache_floor": floor,
                    "dropped_default_values": below,
                }
            )
        if not kept:
            kept = (floor,)
            notes.append(
                {
                    "parameter": name,
                    "cache_floor": floor,
                    "note": "all values were below the floor; clamped to the floor",
                }
            )
        adjusted[name] = kept
    return adjusted, notes


# ---------------------------------------------------------------------------
# Search strategies
# ---------------------------------------------------------------------------


def grid_configs(axes: Mapping[str, Sequence[Any]]) -> Iterator[SweepConfig]:
    names = list(PARAMETER_ORDER)
    for combination in itertools.product(*(axes[name] for name in names)):
        config = SweepConfig.from_values(dict(zip(names, combination)))
        if config.feasible:
            yield config


def grid_size(axes: Mapping[str, Sequence[Any]]) -> int:
    total = 1
    for name in PARAMETER_ORDER:
        total *= max(1, len(axes[name]))
    return int(total)


def random_configs(
    axes: Mapping[str, Sequence[Any]], *, samples: int, seed: int
) -> list[SweepConfig]:
    rng = random.Random(int(seed))
    seen: set[str] = set()
    out: list[SweepConfig] = []
    attempts = 0
    budget = int(samples) * 20
    while len(out) < int(samples) and attempts < budget:
        attempts += 1
        values = {name: rng.choice(list(axes[name])) for name in PARAMETER_ORDER}
        config = SweepConfig.from_values(values)
        if not config.feasible:
            continue
        if config.config_hash in seen:
            continue
        seen.add(config.config_hash)
        out.append(config)
    return out


def neighbours(
    config: SweepConfig, axes: Mapping[str, Sequence[Any]]
) -> list[SweepConfig]:
    """Every one-axis move away from ``config``."""

    out: list[SweepConfig] = []
    seen = {config.config_hash}
    for name in PARAMETER_ORDER:
        for value in axes[name]:
            if value == getattr(config, name):
                continue
            candidate = config.replace_value(name, value)
            if not candidate.feasible:
                continue
            if candidate.config_hash in seen:
                continue
            seen.add(candidate.config_hash)
            out.append(candidate)
    return out


# ---------------------------------------------------------------------------
# Parallel evaluation
# ---------------------------------------------------------------------------


_WORKER_STATE: dict[str, Any] = {}


def _worker_initializer(
    candidate_path: str,
    minute_path: str,
    setup_id: str,
    session_dates: Sequence[str],
    guards_payload: Mapping[str, Any],
    portfolio_payload: Mapping[str, Any],
    baseline_values: Mapping[str, Any],
    with_stress: bool,
) -> None:
    candidates = pd.read_parquet(candidate_path)
    paths = pd.read_parquet(minute_path)
    dates = tuple(opt._parse_day(value) for value in session_dates)
    _WORKER_STATE["prepared"] = prepare_setup_dataset(
        candidates, paths, setup_id=setup_id, session_dates=dates
    )
    _WORKER_STATE["base_setup"] = active_setup_by_id(setup_id)
    _WORKER_STATE["guards"] = opt.SelectionGuards(**dict(guards_payload))
    _WORKER_STATE["portfolio"] = v8.PortfolioPolicy(**dict(portfolio_payload))
    _WORKER_STATE["baseline"] = SweepConfig.from_values(baseline_values)
    _WORKER_STATE["with_stress"] = bool(with_stress)


def _worker_trial(values: Mapping[str, Any]) -> dict[str, Any]:
    if "prepared" not in _WORKER_STATE:
        raise RuntimeError("sweep worker was not initialized")
    return score_config(
        _WORKER_STATE["prepared"],
        _WORKER_STATE["base_setup"],
        SweepConfig.from_values(values),
        guards=_WORKER_STATE["guards"],
        portfolio=_WORKER_STATE["portfolio"],
        baseline=_WORKER_STATE["baseline"],
        with_stress=_WORKER_STATE["with_stress"],
    )


class Evaluator:
    """Deduplicating trial evaluator over an optional process pool."""

    def __init__(
        self,
        *,
        prepared: PreparedSetup,
        base_setup: v8.V8Setup,
        guards: opt.SelectionGuards,
        portfolio: v8.PortfolioPolicy,
        baseline: SweepConfig,
        with_stress: bool,
        pool: ProcessPoolExecutor | None,
    ) -> None:
        self.prepared = prepared
        self.base_setup = base_setup
        self.guards = guards
        self.portfolio = portfolio
        self.baseline = baseline
        self.with_stress = with_stress
        self.pool = pool
        self.records: dict[str, dict[str, Any]] = {}

    def evaluate(self, configs: Sequence[SweepConfig]) -> list[dict[str, Any]]:
        pending: list[SweepConfig] = []
        seen: set[str] = set()
        for config in configs:
            digest = config.config_hash
            if digest in self.records or digest in seen:
                continue
            seen.add(digest)
            pending.append(config)
        if pending:
            payloads = [config.values() for config in pending]
            if self.pool is None:
                fresh = [
                    score_config(
                        self.prepared,
                        self.base_setup,
                        config,
                        guards=self.guards,
                        portfolio=self.portfolio,
                        baseline=self.baseline,
                        with_stress=self.with_stress,
                    )
                    for config in pending
                ]
            else:
                fresh = list(self.pool.map(_worker_trial, payloads, chunksize=8))
            for record in fresh:
                self.records[str(record["config_hash"])] = record
        return [self.records[config.config_hash] for config in configs]

    def best(self, objective: str, *, min_fills: int) -> dict[str, Any] | None:
        if not self.records:
            return None
        return max(
            self.records.values(),
            key=lambda record: (
                objective_key(record, objective, min_fills=min_fills),
                record["config_hash"],
            ),
        )


def coordinate_descent(
    evaluator: Evaluator,
    axes: Mapping[str, Sequence[Any]],
    seed_config: SweepConfig,
    *,
    objective: str,
    min_fills: int,
    rounds: int,
) -> tuple[SweepConfig, list[dict[str, Any]]]:
    """Steepest-ascent coordinate descent: evaluate all one-axis moves."""

    incumbent = seed_config
    evaluator.evaluate([incumbent])
    history: list[dict[str, Any]] = []
    for round_index in range(1, int(rounds) + 1):
        options = neighbours(incumbent, axes)
        if not options:
            break
        evaluator.evaluate(options)
        incumbent_key = objective_key(
            evaluator.records[incumbent.config_hash], objective, min_fills=min_fills
        )
        ranked = sorted(
            options,
            key=lambda config: (
                objective_key(
                    evaluator.records[config.config_hash],
                    objective,
                    min_fills=min_fills,
                ),
                config.config_hash,
            ),
            reverse=True,
        )
        challenger = ranked[0]
        challenger_key = objective_key(
            evaluator.records[challenger.config_hash], objective, min_fills=min_fills
        )
        improved = challenger_key > incumbent_key
        history.append(
            {
                "round": round_index,
                "seed_hash": seed_config.config_hash,
                "incumbent_hash": incumbent.config_hash,
                "challenger_hash": challenger.config_hash,
                "moved": bool(improved),
                "neighbours_evaluated": len(options),
            }
        )
        if not improved:
            break
        incumbent = challenger
    return incumbent, history


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


RANKING_COLUMNS = (
    "config_hash",
    "guard_pass",
    "closed_fills",
    "trades_per_session",
    "profit_factor",
    "robust_profit_factor_ex_best_day",
    "net_return_percentage_points",
    "net_pnl_rs",
    "wins",
    "losses",
    "active_days",
    "positive_days",
    "negative_days",
    "max_drawdown_percentage_points",
    "top_day_share",
    "positive_contiguous_blocks",
    "worst_contiguous_block_pf",
    "stress_profit_factor",
    "stress_net_return_percentage_points",
    "stress_survives",
    "distance_from_book",
    "data_incomplete_candidates",
    "unresolved_filled_trades",
    *(f"param_{name}" for name in PARAMETER_ORDER),
)


def ranked_frame(
    records: Sequence[Mapping[str, Any]],
    objective: str,
    *,
    min_fills: int,
    dedupe_behaviour: bool = False,
) -> pd.DataFrame:
    """Rank trials, marking configurations that produced identical trades.

    ``dedupe_behaviour`` keeps only the best-ranked representative of each
    distinct trade sequence, which is what the headline tables want; the full
    trial export keeps every row and records the alias instead.
    """

    if not records:
        return pd.DataFrame()
    ordered = sorted(
        records,
        key=lambda record: (
            objective_key(record, objective, min_fills=min_fills),
            record["config_hash"],
        ),
        reverse=True,
    )
    representatives: dict[str, str] = {}
    aliases: list[str] = []
    keep: list[bool] = []
    for record in ordered:
        signature = str(record.get("behavior_signature", ""))
        leader = representatives.get(signature)
        if leader is None:
            representatives[signature] = str(record["config_hash"])
            aliases.append("")
            keep.append(True)
        else:
            aliases.append(leader)
            keep.append(False)
    frame = pd.DataFrame(list(ordered))
    frame["behavior_alias_of"] = aliases
    frame["behavior_representative"] = keep
    if dedupe_behaviour:
        frame = frame.loc[frame["behavior_representative"]].copy()
    columns = [name for name in RANKING_COLUMNS if name in frame.columns]
    remaining = [name for name in frame.columns if name not in columns]
    frame = frame[columns + remaining].reset_index(drop=True)
    frame.insert(0, "rank", np.arange(1, len(frame) + 1))
    frame.insert(1, "objective", objective)
    return frame


def _fmt(value: Any, spec: str = ".3f") -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(number):
        return "inf" if number > 0 else ("-inf" if number < 0 else "nan")
    return format(number, spec)


def _top_table(frame: pd.DataFrame, limit: int) -> list[str]:
    if frame.empty:
        return ["_No configuration produced a scored trade._", ""]
    header = (
        "| # | Hash | Guard | Fills | Trades/day | PF | Robust PF | Net %pt | "
        "Top-day | Blocks | Stress PF | Picker | Price | OI | Vol | Body | "
        "Wick | Stop | Target | Conf | Buf | Mid | CLV | Cap |"
    )
    rule = "|" + "---|" * 24
    lines = [header, rule]
    for row in frame.head(int(limit)).to_dict("records"):
        clv = row.get("param_close_location_min")
        lines.append(
            "| {rank} | `{hash}` | {guard} | {fills} | {trades} | {pf} | {robust} "
            "| {net} | {top} | {blocks} | {spf} | {picker} | {price} | {oi} | "
            "{vol} | {body} | {wick} | {stop} | {target} | S+{conf} | {buf} | "
            "{mid} | {clv} | {cap} |".format(
                rank=row.get("rank"),
                hash=str(row.get("config_hash", ""))[:12],
                guard="Y" if bool(row.get("guard_pass")) else "n",
                fills=int(row.get("closed_fills", 0)),
                trades=_fmt(row.get("trades_per_session"), ".2f"),
                pf=_fmt(row.get("profit_factor")),
                robust=_fmt(row.get("robust_profit_factor_ex_best_day")),
                net=_fmt(row.get("net_return_percentage_points"), "+.2f"),
                top=_fmt(row.get("top_day_share"), ".0%")
                if math.isfinite(_finite(row.get("top_day_share"), math.nan))
                else "n/a",
                blocks=int(row.get("positive_contiguous_blocks", 0)),
                spf=_fmt(row.get("stress_profit_factor")),
                picker=str(row.get("param_picker", "")).replace("max_", ""),
                price=_fmt(row.get("param_price_change_pct"), ".2f"),
                oi=_fmt(row.get("param_oi_change_pct"), ".2f"),
                vol=_fmt(row.get("param_volume_ratio"), ".1f"),
                body=_fmt(row.get("param_body_ratio"), ".2f"),
                wick=_fmt(row.get("param_max_wick_ratio"), ".2f"),
                stop=_fmt(row.get("param_stop_pct"), ".2f"),
                target=_fmt(row.get("param_target_pct"), ".2f"),
                conf=int(row.get("param_max_confirmation_minute", 0)),
                buf=_fmt(row.get("param_buffer_bps"), ".0f"),
                mid="on" if bool(row.get("param_midpoint_invalidation")) else "off",
                clv="none" if clv is None or (
                    isinstance(clv, float) and math.isnan(clv)
                ) else _fmt(clv, ".2f"),
                cap=int(row.get("param_max_entries", 0)),
            )
        )
    lines.append("")
    return lines


def build_report(summary: Mapping[str, Any], rankings: Mapping[str, pd.DataFrame]) -> str:
    pool = dict(summary.get("pool") or {})
    lines = [
        f"# FNO V8 Setup Parameter Sweep - {summary.get('setup_id')}",
        "",
        "## DIAGNOSTIC-ONLY WATERMARK",
        "",
        str(summary.get("watermark") or SWEEP_WATERMARK),
        "",
        f"- Sweep version: `{SWEEP_VERSION}`",
        f"- Mode: `{summary.get('mode')}`",
        f"- Objective used for coordinate moves: `{summary.get('objective')}`",
        f"- Fit window: {summary.get('fit_from')} to {summary.get('fit_through')} "
        f"({summary.get('fit_sessions')} official sessions)",
        f"- Coverage mode: `{summary.get('coverage_mode')}`",
        f"- Configurations evaluated: {summary.get('configurations_evaluated')}",
        f"- Configurations with at least one closed trade: "
        f"{summary.get('configurations_with_trades')}",
        f"- Configurations passing every side guard: "
        f"{summary.get('configurations_guard_pass')}",
        f"- Distinct trade behaviours among them: "
        f"{summary.get('distinct_behaviours')} "
        f"(ranked tables deduped: {bool(summary.get('dedupe_behaviour'))})",
        f"- Selection economics: {SELECTION_COST_BPS:g} bps cost + "
        f"{SELECTION_SLIPPAGE_BPS:g} bps slippage; severe stress "
        f"{STRESS_COST_BPS:g} + {STRESS_SLIPPAGE_BPS:g}",
        "- The retrospective TEST window was not read by this module.",
        "",
        "## Sample size",
        "",
        f"- Cached candidates for this setup in the fit window: "
        f"**{pool.get('candidate_count')}**",
        f"- Sessions with at least one candidate: {pool.get('active_sessions')} "
        f"of {summary.get('fit_sessions')}",
        f"- Baseline (frozen book leg) closed fills: {pool.get('baseline_fills')}",
        "",
    ]
    warning = summary.get("sample_warning")
    if warning:
        lines.extend([f"> **{warning}**", ""])
    floors = dict(summary.get("cache_floors") or {})
    if floors:
        lines.extend(
            [
                "## Cache floors (cannot be swept below without a rebuild)",
                "",
                "| Parameter | Floor |",
                "|---|---:|",
            ]
        )
        for name, value in floors.items():
            lines.append(f"| `{name}` | {value:g} |")
        lines.append("")
    notes = list(summary.get("floor_notes") or [])
    if notes:
        lines.extend(["Adjustments applied to default axes:", ""])
        for note in notes:
            lines.append(f"- `{note.get('parameter')}`: {json.dumps(note, sort_keys=True)}")
        lines.append("")
    axes = dict(summary.get("axes") or {})
    lines.extend(["## Axes swept", "", "| Parameter | Values |", "|---|---|"])
    for name in PARAMETER_ORDER:
        values = axes.get(name, ())
        rendered = ", ".join("none" if item is None else str(item) for item in values)
        lines.append(f"| `{name}` | {rendered} |")
    lines.append("")
    baseline = dict(summary.get("baseline_metrics") or {})
    if baseline:
        lines.extend(
            [
                "## Frozen-book baseline on the fit window",
                "",
                f"- Closed fills: {baseline.get('closed_fills')}",
                f"- Trades/session: {_fmt(baseline.get('trades_per_session'), '.3f')}",
                f"- PF: {_fmt(baseline.get('profit_factor'))}",
                f"- Robust PF: "
                f"{_fmt(baseline.get('robust_profit_factor_ex_best_day'))}",
                f"- Net: {_fmt(baseline.get('net_return_percentage_points'), '+.3f')} "
                "percentage points",
                f"- Top-day share: {_fmt(baseline.get('top_day_share'), '.1%')}",
                "",
            ]
        )
    titles = {
        "profit_factor": "Best by profit factor",
        "trade_count": "Best by trade count",
        "combined": "Best combined (guards, then frequency, then PF)",
    }
    for objective in ("profit_factor", "trade_count", "combined"):
        frame = rankings.get(objective, pd.DataFrame())
        lines.append(f"## {titles[objective]}")
        lines.append("")
        lines.extend(_top_table(frame, int(summary.get("report_top_n", 15))))
    lines.extend(
        [
            "## How to read this",
            "",
            "A single V8 setup carries very few candidates per split, and this "
            "sweep varies up to fifteen parameters at once.  The headline rows "
            "above are the extreme order statistics of a large multiple-testing "
            "exercise: some of them will look excellent purely by chance.  "
            "Treat `top_day_share`, `positive_contiguous_blocks`, "
            "`closed_fills` and `stress_profit_factor` as the honesty columns "
            "and discard any row that leans on one day or one block.",
            "",
            "Nothing here is validated.  Confirm a surviving hypothesis on a "
            "window this sweep never read, then prospectively, before it goes "
            "anywhere near the live book.",
            "",
        ]
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Command execution
# ---------------------------------------------------------------------------


def _resolve_axes(args: argparse.Namespace) -> tuple[dict[str, tuple[Any, ...]], set[str]]:
    axes: dict[str, tuple[Any, ...]] = {}
    explicit: set[str] = set()
    for name in PARAMETER_ORDER:
        raw = getattr(args, name, None)
        if raw:
            axes[name] = parse_axis_values(name, raw)
            explicit.add(name)
        else:
            axes[name] = tuple(DEFAULT_AXES[name])
    return axes, explicit


def execute_sweep(args: argparse.Namespace) -> Path:
    fit_from = opt._parse_day(args.fit_from)
    fit_through = opt._parse_day(args.fit_through)
    if fit_through < fit_from:
        raise ValueError("fit window is reversed")
    cache_from = opt._parse_day(args.cache_from)
    cache_through = opt._parse_day(args.cache_through)
    if cache_through < cache_from:
        raise ValueError("cache window is reversed")
    if fit_from < cache_from or fit_through > cache_through:
        raise ValueError(
            f"fit window {fit_from}..{fit_through} must lie inside the cache "
            f"window {cache_from}..{cache_through}"
        )
    coverage_mode = opt._normalize_coverage_mode(args.coverage_mode)
    opt._require_diagnostic_opt_in(
        coverage_mode, bool(args.allow_conditional_diagnostic)
    )
    base_setup = active_setup_by_id(args.setup_id)
    symbols = (
        [item.strip().upper() for item in str(args.symbols).split(",") if item.strip()]
        if args.symbols
        else None
    )
    candidates, minute_paths, coverage, manifest, manifest_path = (
        v8.load_or_build_v8_cache(
            source_snapshot_path=args.source_snapshot,
            from_day=cache_from,
            through_day=cache_through,
            symbols=symbols,
            rebuild=bool(args.rebuild_cache),
        )
    )
    fit_sessions = opt._session_dates_from_manifest(manifest, fit_from, fit_through)
    broad_eligibility = opt.cache_eligibility(coverage, manifest)
    if coverage_mode == "RECTANGULAR_PANEL":
        broad_coverage = coverage
        candidates, minute_paths, coverage, eligibility = (
            opt.derive_rectangular_panel(
                candidates,
                minute_paths,
                broad_coverage,
                session_dates=fit_sessions,
            )
        )
        eligibility["split_coverage"] = {
            "FIT": opt.panel_split_coverage(
                broad_coverage, list(eligibility["panel_symbols"]), fit_sessions
            )
        }
        watermark = f"{SWEEP_WATERMARK}; {opt.PANEL_WATERMARK}"
    elif coverage_mode == "FULL_UNIVERSE":
        eligibility = broad_eligibility
        if not bool(eligibility["optimization_source_eligible"]):
            raise SweepEligibilityError(
                "V8 cache is incomplete for the selected universe/session grid: "
                f"{eligibility['source_incomplete_symbol_sessions']} incomplete "
                "symbol-sessions. Repair the source data or select a watermarked "
                "diagnostic coverage mode."
            )
        watermark = SWEEP_WATERMARK
    elif coverage_mode == "CONDITIONAL_STREAM":
        eligibility = broad_eligibility
        watermark = f"{SWEEP_WATERMARK}; {opt.CONDITIONAL_WATERMARK}"
    else:
        raise ValueError(f"unknown coverage mode {args.coverage_mode!r}")

    floors = cache_floors(base_setup)
    axes, explicit = _resolve_axes(args)
    axes, floor_notes = enforce_cache_floors(
        axes, floors, explicit=explicit, clamp=bool(args.clamp_below_floor)
    )

    prepared = prepare_setup_dataset(
        candidates,
        minute_paths,
        setup_id=base_setup.setup_id,
        session_dates=fit_sessions,
    )
    if prepared.candidate_count == 0:
        raise SweepEligibilityError(
            f"no cached candidates for {base_setup.setup_id} in "
            f"{fit_from}..{fit_through}"
        )

    guards = opt.SelectionGuards(
        min_side_train_fills=int(args.min_fills),
        min_side_active_days=int(args.min_active_days),
        min_side_train_pf=float(args.min_pf),
        min_side_robust_pf=float(args.min_robust_pf),
        max_top_day_share=float(args.max_top_day_share),
    )
    portfolio = v8.PortfolioPolicy()
    baseline = SweepConfig.from_setup(base_setup)

    if coverage_mode == "RECTANGULAR_PANEL":
        derived_fingerprint = common.canonical_json_sha256(
            {
                "broad_cache": manifest.get("input_fingerprint", ""),
                "panel": eligibility,
                "candidate_ids": sorted(candidates["candidate_id"].astype(str)),
            }
        )
        candidate_path, minute_path = opt._materialize_worker_inputs(
            candidates, minute_paths, fingerprint=derived_fingerprint
        )
    else:
        candidate_path = opt._artifact_path(manifest, "candidates")
        minute_path = opt._artifact_path(manifest, "paths")

    mode = str(args.mode).lower()
    total_grid = grid_size(axes)
    if mode == "grid" and total_grid > int(args.max_configs):
        raise SweepEligibilityError(
            f"grid mode would evaluate {total_grid:,} configurations, above "
            f"--max-configs {int(args.max_configs):,}. Narrow the axes, raise "
            "the cap, or use --mode hybrid."
        )

    workers = int(args.workers)
    pool: ProcessPoolExecutor | None = None
    if workers > 1:
        pool = ProcessPoolExecutor(
            max_workers=workers,
            initializer=_worker_initializer,
            initargs=(
                str(candidate_path),
                str(minute_path),
                base_setup.setup_id,
                [value.isoformat() for value in fit_sessions],
                asdict(guards),
                asdict(portfolio),
                baseline.values(),
                bool(args.with_stress),
            ),
        )
    evaluator = Evaluator(
        prepared=prepared,
        base_setup=base_setup,
        guards=guards,
        portfolio=portfolio,
        baseline=baseline,
        with_stress=bool(args.with_stress),
        pool=pool,
    )
    descent_history: list[dict[str, Any]] = []
    try:
        evaluator.evaluate([baseline])
        if mode == "grid":
            evaluator.evaluate(list(grid_configs(axes)))
        elif mode == "random":
            evaluator.evaluate(
                random_configs(axes, samples=int(args.samples), seed=int(args.seed))
            )
        elif mode == "coordinate":
            seeds = [baseline] + random_configs(
                axes, samples=max(0, int(args.restarts) - 1), seed=int(args.seed)
            )
            for seed_config in seeds:
                _, history = coordinate_descent(
                    evaluator,
                    axes,
                    seed_config,
                    objective=str(args.objective),
                    min_fills=int(args.min_fills),
                    rounds=int(args.rounds),
                )
                descent_history.extend(history)
        elif mode == "hybrid":
            sampled = random_configs(
                axes, samples=int(args.samples), seed=int(args.seed)
            )
            evaluator.evaluate(sampled)
            ranked = sorted(
                evaluator.records.values(),
                key=lambda record: (
                    objective_key(
                        record, str(args.objective), min_fills=int(args.min_fills)
                    ),
                    record["config_hash"],
                ),
                reverse=True,
            )
            seeds = [baseline] + [
                SweepConfig.from_values(
                    {name: record[f"param_{name}"] for name in PARAMETER_ORDER}
                )
                for record in ranked[: int(args.polish_top)]
            ]
            for seed_config in seeds:
                _, history = coordinate_descent(
                    evaluator,
                    axes,
                    seed_config,
                    objective=str(args.objective),
                    min_fills=int(args.min_fills),
                    rounds=int(args.rounds),
                )
                descent_history.extend(history)
        else:
            raise ValueError(f"unknown mode {args.mode!r}")
    finally:
        if pool is not None:
            pool.shutdown()

    records = list(evaluator.records.values())
    rankings = {
        objective: ranked_frame(
            records,
            objective,
            min_fills=int(args.min_fills),
            dedupe_behaviour=bool(args.dedupe_behaviour),
        )
        for objective in OBJECTIVES
    }
    distinct_behaviours = len(
        {str(item.get("behavior_signature", "")) for item in records}
    )
    baseline_record = evaluator.records[baseline.config_hash]
    with_trades = int(sum(1 for item in records if int(item["closed_fills"]) > 0))
    guard_pass = int(sum(1 for item in records if bool(item["guard_pass"])))

    sample_warning = None
    if prepared.candidate_count < 40:
        sample_warning = (
            f"SMALL SAMPLE: only {prepared.candidate_count} cached candidates and "
            f"{baseline_record['closed_fills']} baseline closed fills support a "
            f"{len(records):,}-configuration search. Rankings below are dominated "
            "by multiple-testing noise."
        )

    source_hash = provenance.sha256_file(Path(__file__))
    optimizer_hash = provenance.sha256_file(Path(opt.__file__))
    v8_hash = provenance.sha256_file(Path(v8.__file__))
    fingerprint = common.canonical_json_sha256(
        {
            "sweep_version": SWEEP_VERSION,
            "sweep_source_sha256": source_hash,
            "optimizer_source_sha256": optimizer_hash,
            "v8_source_sha256": v8_hash,
            "cache_input_fingerprint": manifest.get("input_fingerprint", ""),
            "setup_id": base_setup.setup_id,
            "fit_from": fit_from.isoformat(),
            "fit_through": fit_through.isoformat(),
            "axes": {
                name: [
                    "none" if item is None else item for item in axes[name]
                ]
                for name in PARAMETER_ORDER
            },
            "mode": mode,
            "objective": str(args.objective),
            "seed": int(args.seed),
            "samples": int(args.samples),
            "restarts": int(args.restarts),
            "rounds": int(args.rounds),
            "guards": asdict(guards),
            "coverage_mode": coverage_mode,
        }
    )
    generated = common.now_ist().strftime("%Y%m%dT%H%M%S%f%z")
    slug = base_setup.setup_id.replace(":", "")
    run_dir = RESULT_ROOT / f"sweep_{slug}_{generated}_{fingerprint[:12]}"
    run_dir.mkdir(parents=True, exist_ok=False)

    summary = {
        "schema_version": SWEEP_SCHEMA_VERSION,
        "sweep_version": SWEEP_VERSION,
        "generated_at_ist": common.now_ist().isoformat(timespec="microseconds"),
        "run_dir": str(run_dir.resolve()),
        "sweep_fingerprint": fingerprint,
        "watermark": watermark,
        "diagnostic_only": True,
        "promotion_eligible": False,
        "retrospective_test_accessed": False,
        "setup_id": base_setup.setup_id,
        "side": base_setup.side,
        "frozen_book_leg": asdict(base_setup),
        "mode": mode,
        "objective": str(args.objective),
        "seed": int(args.seed),
        "samples": int(args.samples),
        "restarts": int(args.restarts),
        "rounds": int(args.rounds),
        "polish_top": int(args.polish_top),
        "fit_from": fit_from.isoformat(),
        "fit_through": fit_through.isoformat(),
        "cache_from": cache_from.isoformat(),
        "cache_through": cache_through.isoformat(),
        "cache_span_note": (
            "the cache spans a wider window than the fit window; sessions "
            "outside the fit window were never prepared, simulated or scored"
        ),
        "fit_sessions": len(fit_sessions),
        "fit_session_dates": [value.isoformat() for value in fit_sessions],
        "coverage_mode": coverage_mode,
        "cache_floors": floors,
        "floor_notes": floor_notes,
        "axes": {name: list(axes[name]) for name in PARAMETER_ORDER},
        "axis_space_size": total_grid,
        "configurations_evaluated": len(records),
        "configurations_with_trades": with_trades,
        "configurations_guard_pass": guard_pass,
        "distinct_behaviours": distinct_behaviours,
        "dedupe_behaviour": bool(args.dedupe_behaviour),
        "guards": asdict(guards),
        "portfolio_policy": asdict(portfolio),
        "selection_cost_bps": SELECTION_COST_BPS,
        "selection_slippage_bps": SELECTION_SLIPPAGE_BPS,
        "stress_cost_bps": STRESS_COST_BPS,
        "stress_slippage_bps": STRESS_SLIPPAGE_BPS,
        "with_stress": bool(args.with_stress),
        "report_top_n": int(args.report_top_n),
        "sample_warning": sample_warning,
        "pool": {
            "candidate_count": prepared.candidate_count,
            "active_sessions": prepared.active_sessions,
            "baseline_fills": int(baseline_record["closed_fills"]),
        },
        "baseline_config": baseline.payload(),
        "baseline_metrics": opt._safe_for_json(baseline_record),
        "best_by_profit_factor": opt._safe_for_json(
            rankings["profit_factor"].head(1).to_dict("records")
        ),
        "best_by_trade_count": opt._safe_for_json(
            rankings["trade_count"].head(1).to_dict("records")
        ),
        "best_combined": opt._safe_for_json(
            rankings["combined"].head(1).to_dict("records")
        ),
        "cache_manifest_path": str(Path(manifest_path).resolve()),
        "cache_manifest_sha256": provenance.sha256_file(manifest_path),
        "cache_input_fingerprint": manifest.get("input_fingerprint", ""),
        "candidate_cache_path": str(candidate_path),
        "minute_path_cache_path": str(minute_path),
        "sweep_source_sha256": source_hash,
        "optimizer_source_sha256": optimizer_hash,
        "v8_source_sha256": v8_hash,
        "cache_eligibility": opt._safe_for_json(eligibility),
    }

    outputs: dict[str, Path] = {}
    outputs["all_trials"] = opt._write_new_csv(
        opt._csv_ready(
            ranked_frame(
                records,
                "combined",
                min_fills=int(args.min_fills),
                dedupe_behaviour=False,
            )
        ),
        run_dir / "all_trials.csv",
    )
    for objective, filename in (
        ("profit_factor", "best_by_profit_factor.csv"),
        ("trade_count", "best_by_trade_count.csv"),
        ("combined", "best_combined.csv"),
    ):
        outputs[objective] = opt._write_new_csv(
            opt._csv_ready(rankings[objective].head(int(args.export_top_n))),
            run_dir / filename,
        )
    if descent_history:
        outputs["coordinate_history"] = opt._write_new_csv(
            pd.DataFrame(descent_history), run_dir / "coordinate_history.csv"
        )
    outputs["summary"] = provenance.write_immutable_json(
        run_dir / "sweep_summary.json", opt._safe_for_json(summary)
    )
    outputs["report"] = opt._write_new_text(
        run_dir / "report.md", build_report(summary, rankings)
    )
    outputs["sweep_source_archive"] = provenance.publish_immutable_copy(
        Path(__file__), run_dir / Path(__file__).name, expected_sha256=source_hash
    )

    if args.export_best_audits:
        best_config = SweepConfig.from_values(
            {
                name: rankings["combined"].iloc[0][f"param_{name}"]
                for name in PARAMETER_ORDER
            }
        )
        best_audit = run_config(
            prepared,
            base_setup,
            best_config,
            cost_bps=SELECTION_COST_BPS,
            slippage_bps=SELECTION_SLIPPAGE_BPS,
            portfolio=portfolio,
        )
        outputs["best_combined_audit"] = opt._write_new_csv(
            opt._csv_ready(best_audit), run_dir / "best_combined_audit.csv"
        )

    provenance_payload = provenance.build_run_provenance(
        generated_at=common.now_ist(),
        strategy_version=SWEEP_VERSION,
        objective=f"SETUP_PARAMETER_SWEEP_{str(args.objective).upper()}",
        strategy_payload={
            "sweep_schema_version": SWEEP_SCHEMA_VERSION,
            "setup_id": base_setup.setup_id,
            "frozen_book_leg": asdict(base_setup),
            "v8_strategy": v8.strategy_payload(),
        },
        parameters={
            "mode": mode,
            "axes": {name: list(axes[name]) for name in PARAMETER_ORDER},
            "guards": asdict(guards),
            "workers": workers,
            "seed": int(args.seed),
            "diagnostic_only": True,
        },
        backtest_window={
            "from_day": fit_from.isoformat(),
            "through_day": fit_through.isoformat(),
            "split": "FIT_ONLY_NO_HOLDOUT_ACCESS",
        },
        cache_manifest_path=manifest_path,
        cache_manifest=manifest,
        output_paths=outputs,
        results={
            "configurations_evaluated": len(records),
            "configurations_guard_pass": guard_pass,
            "promotion_eligible": False,
            "diagnostic_only": True,
        },
    )
    provenance_payload["sweep_fingerprint"] = fingerprint
    provenance_payload["watermark"] = watermark
    provenance.write_immutable_json(run_dir / "provenance.json", provenance_payload)
    return run_dir


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Flexible single-setup parameter sweep for FNO V8. Diagnostic only; "
            "never promotable."
        )
    )
    parser.add_argument("--source-snapshot", required=True)
    parser.add_argument(
        "--setup-id",
        required=True,
        help="e.g. 09:25_LONG (signal_end underscore side)",
    )
    parser.add_argument("--symbols")
    parser.add_argument("--rebuild-cache", action="store_true")
    parser.add_argument("--fit-from", default=DEFAULT_FIT_FROM)
    parser.add_argument("--fit-through", default=DEFAULT_FIT_THROUGH)
    parser.add_argument(
        "--cache-from",
        default=DEFAULT_CACHE_FROM,
        help=(
            "date span the V8 cache was built for; must contain the fit window. "
            "Narrowing it forces a full cache rebuild."
        ),
    )
    parser.add_argument("--cache-through", default=DEFAULT_CACHE_THROUGH)
    parser.add_argument(
        "--coverage-mode",
        choices=("rectangular-panel", "full-universe", "conditional-stream"),
        default="full-universe",
    )
    parser.add_argument("--allow-conditional-diagnostic", action="store_true")
    parser.add_argument(
        "--clamp-below-floor",
        dest="clamp_below_floor",
        action="store_true",
        help=(
            "collapse requested threshold values that sit below this setup's "
            "cache floor onto the floor instead of failing. Needed when one "
            "shared axis list is swept across several setups whose frozen "
            "book thresholds differ. The collapse is recorded in the report."
        ),
    )
    parser.add_argument(
        "--mode",
        choices=("grid", "random", "coordinate", "hybrid"),
        default="hybrid",
    )
    parser.add_argument("--objective", choices=OBJECTIVES, default="combined")
    parser.add_argument("--samples", type=int, default=20_000)
    parser.add_argument("--restarts", type=int, default=12)
    parser.add_argument("--rounds", type=int, default=12)
    parser.add_argument("--polish-top", type=int, default=12)
    parser.add_argument("--seed", type=int, default=20260819)
    parser.add_argument("--max-configs", type=int, default=2_000_000)
    parser.add_argument(
        "--workers", type=int, default=max(1, min(6, (os.cpu_count() or 2) - 1))
    )
    parser.add_argument("--with-stress", dest="with_stress", action="store_true")
    parser.add_argument("--no-stress", dest="with_stress", action="store_false")
    parser.set_defaults(with_stress=True)
    parser.add_argument(
        "--dedupe-behaviour",
        dest="dedupe_behaviour",
        action="store_true",
        help=(
            "collapse configurations that produced identical trades to one "
            "representative in the ranked tables (default)"
        ),
    )
    parser.add_argument(
        "--keep-behaviour-duplicates",
        dest="dedupe_behaviour",
        action="store_false",
    )
    parser.set_defaults(dedupe_behaviour=True)
    parser.add_argument("--report-top-n", type=int, default=15)
    parser.add_argument("--export-top-n", type=int, default=200)
    parser.add_argument("--export-best-audits", action="store_true")

    parser.add_argument("--min-fills", type=int, default=10)
    parser.add_argument("--min-active-days", type=int, default=4)
    parser.add_argument("--min-pf", type=float, default=1.10)
    parser.add_argument("--min-robust-pf", type=float, default=1.00)
    parser.add_argument("--max-top-day-share", type=float, default=0.50)

    axis_group = parser.add_argument_group(
        "sweep axes",
        "Comma-separated value lists. Omit an axis to use its built-in default.",
    )
    for name in PARAMETER_ORDER:
        default_values = ", ".join(
            "none" if item is None else str(item) for item in DEFAULT_AXES[name]
        )
        axis_group.add_argument(
            f"--{name.replace('_', '-')}",
            dest=name,
            help=f"default: {default_values}",
        )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_dir = execute_sweep(args)
    print(run_dir)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (SweepEligibilityError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
