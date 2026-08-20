"""Independent LONG/SHORT optimiser for the FNO V8 one-minute entry seam.

The module deliberately imports only the independent V8 backtester.  It keeps
the frozen five-minute setup book, exits and portfolio rules unchanged while
searching a small, preregistered side-specific one-minute entry grid.

Honesty contract
----------------
The normal ``search`` command uses the full universe and fails closed unless
the V8 cache is complete for every selected symbol and official session.
``--allow-conditional-diagnostic`` explicitly authorises either research-only
alternative: a TRAIN-derived frozen rectangular panel or a run conditional on
the observed candidate stream.  Both are watermarked and can never emit a
deployable champion.

``search`` records later-split source-coverage pass/fail but never scores TEST
outcomes or economics.  ``evaluate-test`` evaluates exactly one frozen
LONG/SHORT policy (at both preregistered cost scenarios) and creates an
exclusive claim file so the same search run cannot be tested twice.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sys
import tempfile
import uuid
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_v8_windowed_1m_entry_backtest as v8


OPTIMIZER_VERSION = "FNO_V8_SIDE_ENTRY_OPTIMIZER_20260819_V1"
OPTIMIZER_SCHEMA_VERSION = "fno_v8_side_entry_optimizer_v1"
DERIVED_CACHE_SCHEMA_VERSION = "fno_v8_optimizer_derived_cache_v2"
OBJECTIVE = "MAX_CLOSED_FILLS_PER_OFFICIAL_SESSION_SUBJECT_TO_PF_FLOORS"
GRID_SCHEMA_VERSION = "fno_v8_side_entry_grid_192_per_side_v1"
RESULT_ROOT = v8.V8_ROOT / "optimizer_runs"

DEFAULT_TRAIN_FROM = "2026-07-13"
DEFAULT_TRAIN_THROUGH = "2026-07-22"
DEFAULT_VALIDATION_FROM = "2026-07-23"
DEFAULT_VALIDATION_THROUGH = "2026-07-27"
DEFAULT_TEST_FROM = "2026-07-28"
DEFAULT_TEST_THROUGH = "2026-07-31"

SELECTION_COST_BPS = 15.0
SELECTION_SLIPPAGE_BPS = 1.0
STRESS_COST_BPS = 20.0
STRESS_SLIPPAGE_BPS = 2.0

CONDITIONAL_WATERMARK = (
    "CONDITIONAL_ON_OBSERVED_CANDIDATE_STREAM; UPSTREAM_UNIVERSE_COVERAGE_"
    "INCOMPLETE; MISSING_SYMBOLS_CAN_BIAS_RANKS_AND_CAPS; DIAGNOSTIC_ONLY"
)
PANEL_WATERMARK = (
    "FROZEN_RECTANGULAR_SOURCE_COMPLETE_SYMBOL_PANEL; PANEL_DERIVED_ONLY_FROM_"
    "SOURCE_AVAILABILITY; STATIC_LATER_DATED_UNIVERSE_AND_STATIC_AUGUST_OI; "
    "TINY_ALREADY_INSPECTED_RETROSPECTIVE_SAMPLE; DIAGNOSTIC_ONLY"
)


class DataEligibilityError(RuntimeError):
    """The cache cannot support an honest optimisation claim."""


class HoldoutAccessError(RuntimeError):
    """The frozen holdout was accessed outside the one-shot command."""


@dataclass(frozen=True)
class MorphologyPreset:
    name: str
    body_scale: float
    wick_add: float
    ignore_morphology: bool = False


MORPHOLOGY_PRESETS: tuple[MorphologyPreset, ...] = (
    MorphologyPreset("STRICT", 1.00, 0.00),
    MorphologyPreset("MODERATE", 0.75, 0.10),
    MorphologyPreset("RELAXED", 0.50, 0.20),
    # This remains direction-and-close-beyond-C5 aware.  It removes only the
    # body and adverse-wick legs; it is not the weak V7 high/low-only rule.
    MorphologyPreset("DIRECTIONAL_ONLY", 0.00, 0.00, True),
)
_MORPHOLOGY_BY_NAME = {item.name: item for item in MORPHOLOGY_PRESETS}


@dataclass(frozen=True)
class SideEntryConfig:
    max_confirmation_minute: int
    buffer_bps: float
    midpoint_invalidation: bool
    close_location_min: float | None
    morphology: str

    def validate(self) -> None:
        if self.max_confirmation_minute not in (1, 2, 3, 4):
            raise ValueError("max_confirmation_minute must be one of 1,2,3,4")
        if self.buffer_bps not in (0.0, 2.0, 5.0):
            raise ValueError("buffer_bps must be one of 0,2,5")
        if self.close_location_min not in (None, 0.75):
            raise ValueError("close_location_min must be None or 0.75")
        if self.morphology not in _MORPHOLOGY_BY_NAME:
            raise ValueError(f"unknown morphology preset {self.morphology!r}")

    def payload(self) -> dict[str, Any]:
        self.validate()
        preset = _MORPHOLOGY_BY_NAME[self.morphology]
        return {
            **asdict(self),
            "body_scale": preset.body_scale,
            "wick_add": preset.wick_add,
            "ignore_morphology": preset.ignore_morphology,
            "entry_expiry_minute": 5,
            "post_confirmation_cancel": True,
            "allow_cap_reassignment": True,
        }

    @property
    def config_hash(self) -> str:
        return common.canonical_json_sha256(
            {"schema_version": GRID_SCHEMA_VERSION, **self.payload()}
        )

    @property
    def complexity(self) -> int:
        morphology_cost = {
            "STRICT": 0,
            "MODERATE": 1,
            "RELAXED": 2,
            "DIRECTIONAL_ONLY": 3,
        }[self.morphology]
        return int(
            (self.max_confirmation_minute - 1)
            + (self.buffer_bps > 0)
            + self.midpoint_invalidation
            + (self.close_location_min is not None)
            + morphology_cost
        )

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "SideEntryConfig":
        close_location = payload.get("close_location_min")
        return cls(
            max_confirmation_minute=int(payload["max_confirmation_minute"]),
            buffer_bps=float(payload["buffer_bps"]),
            midpoint_invalidation=bool(payload["midpoint_invalidation"]),
            close_location_min=(
                None if close_location is None else float(close_location)
            ),
            morphology=str(payload["morphology"]),
        )


@dataclass(frozen=True)
class LongShortConfig:
    long: SideEntryConfig
    short: SideEntryConfig

    @property
    def config_hash(self) -> str:
        return common.canonical_json_sha256(
            {
                "schema_version": GRID_SCHEMA_VERSION,
                "long": self.long.payload(),
                "short": self.short.payload(),
            }
        )

    def payload(self) -> dict[str, Any]:
        return {
            "long": self.long.payload(),
            "short": self.short.payload(),
            "config_hash": self.config_hash,
        }


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
            raise ValueError("train window is reversed")
        if self.validation_through < self.validation_from:
            raise ValueError("validation window is reversed")
        if self.test_through < self.test_from:
            raise ValueError("test window is reversed")
        if not (
            self.train_through < self.validation_from
            and self.validation_through < self.test_from
        ):
            raise ValueError("train, validation and test must be chronological/disjoint")

    def payload(self) -> dict[str, str]:
        self.validate()
        return {name: value.isoformat() for name, value in asdict(self).items()}

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
    def from_mapping(cls, payload: Mapping[str, Any]) -> "SplitContract":
        names = (
            "train_from",
            "train_through",
            "validation_from",
            "validation_through",
            "test_from",
            "test_through",
        )
        return cls(**{name: _parse_day(payload[name]) for name in names})


@dataclass(frozen=True)
class SelectionGuards:
    min_side_train_fills: int = 15
    min_side_active_days: int = 4
    min_side_train_pf: float = 1.10
    min_side_robust_pf: float = 1.00
    min_pair_train_fills: int = 40
    min_pair_train_pf: float = 1.50
    min_validation_fills: int = 16
    min_validation_pf: float = 1.50
    max_top_day_share: float = 0.50


@dataclass
class PreparedOccurrence:
    setup: v8.V8Setup
    candidates: tuple[v8.CandidateInput, ...]
    bars_by_symbol: dict[str, tuple[v8.MinuteBar, ...]]
    ranks: pd.DataFrame


@dataclass
class PreparedSideDataset:
    side: str
    occurrences: tuple[PreparedOccurrence, ...]
    session_dates: tuple[date, ...]


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


def _normalize_coverage_mode(value: Any) -> str:
    return str(value).upper().replace("-", "_")


def _require_diagnostic_opt_in(coverage_mode: str, allowed: bool) -> None:
    mode = _normalize_coverage_mode(coverage_mode)
    if mode in {"RECTANGULAR_PANEL", "CONDITIONAL_STREAM"} and not allowed:
        raise DataEligibilityError(
            f"{str(coverage_mode).lower()} mode is diagnostic-only and requires "
            "--allow-conditional-diagnostic"
        )


def generate_side_grid() -> tuple[SideEntryConfig, ...]:
    configs = [
        SideEntryConfig(maximum, buffer, midpoint, close_location, morphology.name)
        for maximum in (1, 2, 3, 4)
        for buffer in (0.0, 2.0, 5.0)
        for midpoint in (False, True)
        for close_location in (None, 0.75)
        for morphology in MORPHOLOGY_PRESETS
    ]
    hashes = [item.config_hash for item in configs]
    if len(configs) != 192 or len(set(hashes)) != 192:
        raise AssertionError("V8 optimizer grid must contain 192 unique configs")
    return tuple(configs)


SIDE_GRID = generate_side_grid()


def _session_dates_from_manifest(
    manifest: Mapping[str, Any], start: date, end: date
) -> tuple[date, ...]:
    values = manifest.get("session_dates") or dict(
        dict(manifest.get("input_contract", {})).get("session_calendar", {})
    ).get("expected_session_dates", [])
    sessions = tuple(
        day for day in sorted({_parse_day(value) for value in values})
        if start <= day <= end
    )
    if not sessions:
        raise DataEligibilityError(
            f"no official sessions for requested split {start}..{end}"
        )
    return sessions


def validate_split_against_manifest(
    contract: SplitContract, manifest: Mapping[str, Any]
) -> dict[str, tuple[date, ...]]:
    contract.validate()
    output: dict[str, tuple[date, ...]] = {}
    for split in ("TRAIN", "VALIDATION", "TEST"):
        output[split] = _session_dates_from_manifest(
            manifest, *contract.bounds(split)
        )
    all_values = [day for values in output.values() for day in values]
    if len(all_values) != len(set(all_values)):
        raise AssertionError("official split sessions overlap")
    return output


def cache_eligibility(
    coverage: pd.DataFrame, manifest: Mapping[str, Any]
) -> dict[str, Any]:
    contract = dict(manifest.get("input_contract", {}))
    derived = v8.derive_coverage_completeness(
        coverage,
        selected_symbols=contract.get("symbols", []),
        expected_session_dates=dict(contract.get("session_calendar", {})).get(
            "expected_session_dates", []
        ),
    )
    eligible = bool(derived["headline_source_complete"])
    return {
        **derived,
        "optimization_source_eligible": eligible,
        "watermark": None if eligible else CONDITIONAL_WATERMARK,
    }


def _coverage_complete_dates(row: Mapping[str, Any]) -> set[str]:
    raw = row.get("source_complete_session_dates_json", "[]")
    if isinstance(raw, (list, tuple, set)):
        values = raw
    else:
        try:
            values = json.loads(str(raw))
        except (TypeError, ValueError, json.JSONDecodeError):
            values = []
    return {
        _parse_day(value).isoformat()
        for value in values
        if value not in (None, "")
    }


def panel_split_coverage(
    coverage: pd.DataFrame,
    panel_symbols: Sequence[str],
    session_dates: Sequence[date],
) -> dict[str, Any]:
    """Measure a frozen panel against a split without changing membership."""

    symbols = tuple(sorted({str(value).upper().strip() for value in panel_symbols}))
    sessions = tuple(sorted({_parse_day(value) for value in session_dates}))
    required = {value.isoformat() for value in sessions}
    by_symbol = {
        str(row.get("symbol", "")).upper().strip(): _coverage_complete_dates(row)
        for row in coverage.to_dict("records")
    }
    incomplete_pairs: list[str] = []
    for symbol in symbols:
        complete = by_symbol.get(symbol, set())
        incomplete_pairs.extend(
            f"{symbol}|{session}"
            for session in sorted(required - complete)
        )
    incomplete_symbols = sorted(
        {value.split("|", 1)[0] for value in incomplete_pairs}
    )
    expected = len(symbols) * len(sessions)
    incomplete = len(incomplete_pairs)
    return {
        "panel_symbol_count": len(symbols),
        "session_count": len(sessions),
        "session_dates": [value.isoformat() for value in sessions],
        "expected_symbol_sessions": expected,
        "complete_symbol_sessions": expected - incomplete,
        "source_incomplete_symbol_sessions": incomplete,
        "incomplete_symbol_count": len(incomplete_symbols),
        "incomplete_symbols": incomplete_symbols,
        "incomplete_symbol_session_examples": incomplete_pairs[:100],
        "pass": incomplete == 0,
    }


def derive_rectangular_panel(
    candidates: pd.DataFrame,
    minute_paths: pd.DataFrame,
    coverage: pd.DataFrame,
    *,
    session_dates: Sequence[date],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Freeze symbols complete on every TRAIN session and re-rank them.

    The caller must pass TRAIN sessions only.  The panel depends only on
    source-availability metadata, never outcomes or later-split availability.
    Ranking is recomputed within the fixed panel exactly as the V8 builder
    ranks candidates, so a removed non-panel symbol cannot retain a phantom
    rank ahead of an eligible panel candidate.
    """

    required = {value.isoformat() for value in session_dates}
    if not required:
        raise DataEligibilityError("rectangular panel requires TRAIN sessions")
    panel_symbols: list[str] = []
    for row in coverage.to_dict("records"):
        complete = _coverage_complete_dates(row)
        if required.issubset(complete):
            panel_symbols.append(str(row["symbol"]).upper().strip())
    panel_symbols = sorted(set(panel_symbols))
    if not panel_symbols:
        raise DataEligibilityError(
            "no symbol is source-complete across the requested rectangular panel"
        )
    panel_set = set(panel_symbols)
    panel_candidates = candidates.loc[
        candidates["symbol"].astype(str).str.upper().isin(panel_set)
    ].copy()
    panel_candidates = panel_candidates.sort_values(
        ["session_date", "setup_id", "picker_value", "traded_value", "symbol"],
        ascending=[True, True, False, False, True],
        kind="stable",
    ).reset_index(drop=True)
    panel_candidates["frozen_rank"] = (
        panel_candidates.groupby(["session_date", "setup_id"], sort=False)
        .cumcount()
        .add(1)
    )
    candidate_ids = set(panel_candidates["candidate_id"].astype(str))
    panel_paths = minute_paths.loc[
        minute_paths["candidate_id"].astype(str).isin(candidate_ids)
    ].copy().reset_index(drop=True)
    panel_coverage = coverage.loc[
        coverage["symbol"].astype(str).str.upper().isin(panel_set)
    ].copy().reset_index(drop=True)
    panel_hash = common.canonical_json_sha256(
        {
            "policy": "TRAIN_ONLY_SOURCE_COMPLETE_RECTANGULAR_PANEL_V2",
            "train_session_dates": sorted(required),
            "symbols": panel_symbols,
        }
    )
    metadata = {
        "coverage_mode": "RECTANGULAR_PANEL",
        "panel_policy": (
            "SOURCE_COMPLETE_ON_EVERY_TRAIN_SESSION_THEN_FREEZE_AND_RERANK"
        ),
        "panel_derivation_split": "TRAIN_ONLY",
        "panel_derivation_session_dates": sorted(required),
        "panel_symbol_count": len(panel_symbols),
        "panel_symbols": panel_symbols,
        "panel_symbol_set_sha256": common.symbol_set_sha256(panel_symbols),
        "panel_hash": panel_hash,
        "expected_symbol_sessions": len(panel_symbols) * len(required),
        "complete_symbol_sessions": len(panel_symbols) * len(required),
        "source_incomplete_symbol_sessions": 0,
        "unexpected_source_symbol_sessions": int(
            pd.to_numeric(
                panel_coverage.get(
                    "unexpected_session_count",
                    pd.Series(0, index=panel_coverage.index),
                ),
                errors="coerce",
            ).fillna(0).sum()
        ),
        "headline_source_complete": True,
        "optimization_source_eligible": True,
        "watermark": PANEL_WATERMARK,
    }
    return panel_candidates, panel_paths, panel_coverage, metadata


def _materialize_worker_inputs(
    candidates: pd.DataFrame,
    minute_paths: pd.DataFrame,
    *,
    fingerprint: str,
) -> tuple[Path, Path]:
    """Create a deterministic derived cache used by spawned workers."""

    cache_key = common.canonical_json_sha256(
        {
            "schema_version": DERIVED_CACHE_SCHEMA_VERSION,
            "fingerprint": fingerprint,
        }
    )
    root = v8.V8_ROOT / "optimizer_derived_cache" / cache_key[:20]
    root.mkdir(parents=True, exist_ok=True)
    candidate_path = root / "candidates.parquet"
    minute_path = root / "minute_paths.parquet"
    manifest_path = root / "manifest.json"
    expected = {
        "schema_version": DERIVED_CACHE_SCHEMA_VERSION,
        "fingerprint": fingerprint,
        "cache_key": cache_key,
        "candidate_rows": len(candidates),
        "minute_path_rows": len(minute_paths),
    }
    if manifest_path.exists():
        observed = json.loads(manifest_path.read_text(encoding="utf-8"))
        if any(observed.get(key) != value for key, value in expected.items()):
            raise AssertionError("optimizer derived cache is inconsistent")
        records = dict(observed.get("artifacts", {}))
        for name, path in (
            ("candidates", candidate_path),
            ("minute_paths", minute_path),
        ):
            record = dict(records.get(name, {}))
            if Path(str(record.get("path", ""))).resolve() != path.resolve():
                raise AssertionError(
                    f"optimizer derived cache path changed: {name}"
                )
            if not provenance.artifact_matches(path, record):
                raise AssertionError(
                    f"optimizer derived cache artifact changed: {name}"
                )
        return candidate_path, minute_path
    if candidate_path.exists() or minute_path.exists():
        raise AssertionError(
            "optimizer derived cache has artifacts without an authenticated manifest"
        )
    common.atomic_write_parquet(candidates, candidate_path)
    common.atomic_write_parquet(minute_paths, minute_path)
    payload = {
        **expected,
        "artifacts": {
            "candidates": provenance.artifact_record(candidate_path),
            "minute_paths": provenance.artifact_record(minute_path),
        },
    }
    provenance.write_immutable_json(manifest_path, payload)
    return candidate_path, minute_path


def _slice_split(
    candidates: pd.DataFrame,
    minute_paths: pd.DataFrame,
    session_dates: Sequence[date],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    wanted = set(session_dates)
    candidate_days = candidates["session_date"].map(_parse_day)
    selected = candidates.loc[candidate_days.isin(wanted)].copy()
    candidate_ids = set(selected["candidate_id"].astype(str))
    paths = minute_paths.loc[
        minute_paths["candidate_id"].astype(str).isin(candidate_ids)
    ].copy()
    return selected.reset_index(drop=True), paths.reset_index(drop=True)


def prepare_side_dataset(
    candidates: pd.DataFrame,
    minute_paths: pd.DataFrame,
    *,
    side: str,
    session_dates: Sequence[date],
) -> PreparedSideDataset:
    side_key = str(side).upper()
    if side_key not in {"LONG", "SHORT"}:
        raise ValueError("side must be LONG or SHORT")
    selected, paths = _slice_split(candidates, minute_paths, session_dates)
    selected = selected.loc[selected["side"].astype(str).str.upper().eq(side_key)]
    selected_ids = set(selected["candidate_id"].astype(str))
    paths = paths.loc[paths["candidate_id"].astype(str).isin(selected_ids)]
    path_map = {
        str(candidate_id): tuple(v8._minute_bars_from_cache(group))
        for candidate_id, group in paths.groupby("candidate_id", sort=False)
    }
    setup_by_id = {setup.setup_id: setup for setup in v8.ACTIVE_SETUPS}
    occurrences: list[PreparedOccurrence] = []
    for (_, setup_id), group in selected.groupby(
        ["session_date", "setup_id"], sort=True
    ):
        setup = setup_by_id.get(str(setup_id))
        if setup is None:
            raise ValueError(f"unknown V8 setup {setup_id!r}")
        inputs = tuple(
            v8._candidate_from_cache_row(row) for row in group.to_dict("records")
        )
        bars = {
            str(row["symbol"]): path_map.get(str(row["candidate_id"]), tuple())
            for row in group.to_dict("records")
        }
        ranks = group[
            ["candidate_id", "frozen_rank", "picker", "picker_value"]
        ].copy()
        occurrences.append(PreparedOccurrence(setup, inputs, bars, ranks))
    return PreparedSideDataset(side_key, tuple(occurrences), tuple(session_dates))


def _configured_setup(
    setup: v8.V8Setup, config: SideEntryConfig
) -> v8.V8Setup:
    preset = _MORPHOLOGY_BY_NAME[config.morphology]
    if preset.ignore_morphology:
        body_ratio = 0.0
        max_wick_ratio = 1.0
    else:
        body_ratio = float(setup.body_ratio) * preset.body_scale
        max_wick_ratio = min(1.0, float(setup.max_wick_ratio) + preset.wick_add)
    return replace(
        setup, body_ratio=body_ratio, max_wick_ratio=max_wick_ratio
    )


def entry_policy(
    config: SideEntryConfig, *, cost_bps: float, slippage_bps: float
) -> v8.EntryPolicy:
    config.validate()
    policy = v8.EntryPolicy(
        buffer_bps=config.buffer_bps,
        max_confirmation_minute=config.max_confirmation_minute,
        entry_expiry_minute=5,
        close_location_min=config.close_location_min,
        cost_bps=float(cost_bps),
        slippage_bps=float(slippage_bps),
        midpoint_invalidation=config.midpoint_invalidation,
        post_confirmation_cancel=True,
        allow_cap_reassignment=True,
        same_bar_policy="STOP_FIRST",
        square_off="15:30",
        eod_policy="EXACT_SQUARE_OFF",
    )
    v8.validate_backtest_policy(policy)
    return policy


def _attach_economics(
    audit: pd.DataFrame,
    *,
    cost_bps: float,
    target_exposure_per_entry_rs: float = 50_000.0,
) -> pd.DataFrame:
    if audit.empty:
        return audit.copy()
    out = audit.copy()
    filled = out["entry_price"].notna()
    out["filled"] = filled
    out["quantity"] = 0
    entry_numeric = pd.to_numeric(out["entry_price"], errors="coerce")
    out.loc[filled, "quantity"] = np.floor(
        target_exposure_per_entry_rs / entry_numeric.loc[filled]
    ).astype(int)
    for column in (
        "gross_pnl_rs",
        "estimated_cost_rs",
        "net_pnl_rs",
        "position_notional_rs",
    ):
        out[column] = np.nan
    out.loc[filled, "position_notional_rs"] = (
        entry_numeric.loc[filled] * out.loc[filled, "quantity"].astype(float)
    )
    closed = (
        filled
        & out["exit_price"].notna()
        & pd.to_numeric(out["net_return_pct"], errors="coerce").notna()
    )
    direction = np.where(out.loc[closed, "side"].eq("LONG"), 1.0, -1.0)
    quantity = out.loc[closed, "quantity"].astype(float)
    entry = entry_numeric.loc[closed].astype(float)
    exit_price = pd.to_numeric(out.loc[closed, "exit_price"], errors="coerce")
    gross_rs = direction * (exit_price - entry) * quantity
    cost_rs = entry * quantity * float(cost_bps) / 10_000.0
    out.loc[closed, "gross_pnl_rs"] = gross_rs
    out.loc[closed, "estimated_cost_rs"] = cost_rs
    out.loc[closed, "net_pnl_rs"] = gross_rs - cost_rs
    return out


def run_side_preportfolio(
    prepared: PreparedSideDataset,
    config: SideEntryConfig,
    *,
    cost_bps: float = SELECTION_COST_BPS,
    slippage_bps: float = SELECTION_SLIPPAGE_BPS,
) -> pd.DataFrame:
    policy = entry_policy(config, cost_bps=cost_bps, slippage_bps=slippage_bps)
    parts: list[pd.DataFrame] = []
    for occurrence in prepared.occurrences:
        setup = _configured_setup(occurrence.setup, config)
        audit = v8.simulate_setup_window(
            setup, occurrence.candidates, occurrence.bars_by_symbol, policy
        )
        if audit.empty:
            continue
        audit = audit.merge(
            occurrence.ranks,
            on="candidate_id",
            how="left",
            validate="one_to_one",
        )
        audit["variant"] = f"OPT_{config.config_hash[:12]}"
        audit["buffer_bps"] = policy.buffer_bps
        audit["cost_bps"] = policy.cost_bps
        audit["slippage_bps"] = policy.slippage_bps
        audit["eod_policy"] = policy.eod_policy
        audit["optimizer_config_hash"] = config.config_hash
        audit["optimizer_morphology"] = config.morphology
        parts.append(audit)
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True, sort=False)
    out = _attach_economics(out, cost_bps=cost_bps)
    out["portfolio_mode"] = v8.PORTFOLIO_MODE
    return out.sort_values(
        ["session_date", "signal_time", "side", "frozen_rank", "symbol"],
        kind="stable",
    ).reset_index(drop=True)


def combine_and_constrain(
    long_audit: pd.DataFrame,
    short_audit: pd.DataFrame,
    portfolio_policy: v8.PortfolioPolicy | None = None,
) -> pd.DataFrame:
    parts = [item for item in (long_audit, short_audit) if not item.empty]
    if not parts:
        return pd.DataFrame()
    combined = pd.concat(parts, ignore_index=True, sort=False)
    constrained = v8.apply_global_portfolio_constraints(
        combined, portfolio_policy or v8.PortfolioPolicy()
    )
    return constrained.sort_values(
        ["session_date", "signal_time", "side", "frozen_rank", "symbol"],
        kind="stable",
    ).reset_index(drop=True)


def _profit_factor(values: pd.Series) -> float:
    profit = float(values.loc[values.gt(0)].sum())
    loss = float(-values.loc[values.lt(0)].sum())
    return profit / loss if loss > 0 else (math.inf if profit > 0 else math.nan)


def score_audit(
    audit: pd.DataFrame, session_dates: Sequence[date]
) -> dict[str, Any]:
    sessions = tuple(sorted({_parse_day(value) for value in session_dates}))
    if not sessions:
        raise ValueError("score_audit requires official sessions")
    if audit.empty:
        work = pd.DataFrame(columns=["session_date", "filled", "net_return_pct"])
    else:
        work = audit.copy()
        work["session_date"] = work["session_date"].map(_parse_day)
    filled = (
        work.get("filled", pd.Series(False, index=work.index))
        .fillna(False)
        .astype(bool)
    )
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
    daily_values = closed.assign(_net=closed_returns).groupby("session_date")[
        "_net"
    ].sum()
    daily_counts = closed.groupby("session_date").size()
    daily = pd.Series(0.0, index=pd.Index(sessions, dtype="object"))
    closed_counts = pd.Series(0, index=pd.Index(sessions, dtype="object"))
    for day, value in daily_values.items():
        if day in daily.index:
            daily.loc[day] = float(value)
    for day, value in daily_counts.items():
        if day in closed_counts.index:
            closed_counts.loc[day] = int(value)
    active = closed_counts.gt(0)
    cumulative = np.concatenate(([0.0], daily.cumsum().to_numpy(float)))
    drawdown = cumulative - np.maximum.accumulate(cumulative)
    best_day = daily.idxmax() if len(daily) else None
    robust_returns = (
        closed_returns.loc[closed["session_date"].ne(best_day)]
        if best_day is not None
        else closed_returns
    )
    fold_pfs: list[float] = []
    fold_nets: list[float] = []
    for block in np.array_split(np.asarray(sessions, dtype=object), 3):
        values = closed_returns.loc[closed["session_date"].isin(set(block))]
        fold_pfs.append(_profit_factor(values))
        fold_nets.append(float(values.sum()))
    net = float(closed_returns.sum())
    positive_daily_gross = float(daily.clip(lower=0.0).sum())
    top_day = max(0.0, float(daily.max())) if len(daily) else 0.0
    incomplete = int(
        work.get("status", pd.Series("", index=work.index))
        .astype(str)
        .eq(v8.SignalState.DATA_INCOMPLETE.value)
        .sum()
    )
    unresolved = int((filled & ~closed_mask).sum())
    return {
        "sessions": len(sessions),
        "candidates": int(len(work)),
        "fills": int(filled.sum()),
        "closed_fills": int(closed_mask.sum()),
        "trades_per_session": float(closed_mask.sum() / len(sessions)),
        "profit_factor": _profit_factor(closed_returns),
        "robust_profit_factor_ex_best_day": _profit_factor(robust_returns),
        "net_return_percentage_points": net,
        "net_pnl_rs": float(pnl.loc[closed_mask].sum()),
        "wins": int(closed_returns.gt(0).sum()),
        "losses": int(closed_returns.lt(0).sum()),
        "active_days": int(active.sum()),
        "positive_days": int(daily.gt(0).sum()),
        "negative_days": int(daily.lt(0).sum()),
        "flat_days": int(daily.eq(0).sum()),
        "max_drawdown_percentage_points": max(
            0.0, float(-drawdown.min()) if drawdown.size else 0.0
        ),
        "positive_daily_gross_percentage_points": positive_daily_gross,
        "top_day_share": (
            float(top_day / positive_daily_gross)
            if positive_daily_gross > 0
            else math.inf
        ),
        "positive_contiguous_blocks": int(sum(value > 0 for value in fold_nets)),
        "worst_contiguous_block_pf": min(fold_pfs) if fold_pfs else math.nan,
        "data_incomplete_candidates": incomplete,
        "unresolved_filled_trades": unresolved,
    }


def score_with_sides(
    audit: pd.DataFrame, session_dates: Sequence[date]
) -> dict[str, Any]:
    result = {"combined": score_audit(audit, session_dates), "sides": {}}
    for side in ("LONG", "SHORT"):
        subset = (
            audit.loc[audit["side"].astype(str).eq(side)].copy()
            if not audit.empty and "side" in audit.columns
            else pd.DataFrame()
        )
        result["sides"][side] = score_audit(subset, session_dates)
    return result


def behavior_signature(audit: pd.DataFrame) -> str:
    if audit.empty:
        return common.canonical_json_sha256([])
    records: list[dict[str, Any]] = []
    ordered = audit.sort_values(
        ["candidate_id", "signal_time", "frozen_rank"], kind="stable"
    )
    for row in ordered.to_dict("records"):
        events = []
        for event in row.get("events", []) or []:
            events.append(
                {
                    "event_ts": str(event.get("event_ts", "")),
                    "state_before": str(event.get("state_before", "")),
                    "state_after": str(event.get("state_after", "")),
                    "reason": str(event.get("reason", "")),
                }
            )
        def normalized(name: str) -> Any:
            value = row.get(name)
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return None
            if isinstance(value, pd.Timestamp):
                return value.isoformat()
            if isinstance(value, (np.integer, np.floating)):
                return value.item()
            return str(value) if name.endswith("time") else value
        records.append(
            {
                "candidate_id": str(row["candidate_id"]),
                "status": str(row.get("status", "")),
                "confirmation_minute": normalized("confirmation_minute"),
                "trigger": normalized("trigger"),
                "entry_time": normalized("entry_time"),
                "entry_price": normalized("entry_price"),
                "exit_time": normalized("exit_time"),
                "exit_price": normalized("exit_price"),
                "events": events,
            }
        )
    return common.canonical_json_sha256(records)


def side_guard_pass(metrics: Mapping[str, Any], guards: SelectionGuards) -> bool:
    pf = float(metrics.get("profit_factor", math.nan))
    robust_pf = float(metrics.get("robust_profit_factor_ex_best_day", math.nan))
    return bool(
        int(metrics.get("closed_fills", 0)) >= guards.min_side_train_fills
        and int(metrics.get("active_days", 0)) >= guards.min_side_active_days
        and math.isfinite(pf)
        and pf >= guards.min_side_train_pf
        and math.isfinite(robust_pf)
        and robust_pf >= guards.min_side_robust_pf
        and float(metrics.get("top_day_share", math.inf))
        <= guards.max_top_day_share
        and int(metrics.get("positive_contiguous_blocks", 0)) >= 2
        and int(metrics.get("data_incomplete_candidates", 0)) == 0
        and int(metrics.get("unresolved_filled_trades", 0)) == 0
    )


_WORKER_PREPARED: PreparedSideDataset | None = None
_WORKER_COST_BPS = SELECTION_COST_BPS
_WORKER_SLIPPAGE_BPS = SELECTION_SLIPPAGE_BPS
_WORKER_GUARDS = SelectionGuards()


def _worker_initializer(
    candidate_path: str,
    minute_path: str,
    side: str,
    session_dates: Sequence[str],
    cost_bps: float,
    slippage_bps: float,
    guards_payload: Mapping[str, Any],
) -> None:
    global _WORKER_PREPARED, _WORKER_COST_BPS, _WORKER_SLIPPAGE_BPS, _WORKER_GUARDS
    candidates = pd.read_parquet(candidate_path)
    paths = pd.read_parquet(minute_path)
    dates = tuple(_parse_day(value) for value in session_dates)
    _WORKER_PREPARED = prepare_side_dataset(
        candidates, paths, side=side, session_dates=dates
    )
    _WORKER_COST_BPS = float(cost_bps)
    _WORKER_SLIPPAGE_BPS = float(slippage_bps)
    _WORKER_GUARDS = SelectionGuards(**dict(guards_payload))


def _trial_record(
    prepared: PreparedSideDataset,
    config: SideEntryConfig,
    *,
    cost_bps: float,
    slippage_bps: float,
    guards: SelectionGuards,
) -> dict[str, Any]:
    preportfolio = run_side_preportfolio(
        prepared, config, cost_bps=cost_bps, slippage_bps=slippage_bps
    )
    constrained = (
        v8.apply_global_portfolio_constraints(preportfolio, v8.PortfolioPolicy())
        if not preportfolio.empty
        else preportfolio
    )
    metrics = score_audit(constrained, prepared.session_dates)
    return {
        "side": prepared.side,
        "config_hash": config.config_hash,
        "config": config.payload(),
        "complexity": config.complexity,
        "behavior_signature": behavior_signature(preportfolio),
        "guard_pass": side_guard_pass(metrics, guards),
        **metrics,
    }


def _worker_trial(config_payload: Mapping[str, Any]) -> dict[str, Any]:
    if _WORKER_PREPARED is None:
        raise RuntimeError("optimizer worker was not initialized")
    return _trial_record(
        _WORKER_PREPARED,
        SideEntryConfig.from_payload(config_payload),
        cost_bps=_WORKER_COST_BPS,
        slippage_bps=_WORKER_SLIPPAGE_BPS,
        guards=_WORKER_GUARDS,
    )


def search_side_grid(
    *,
    candidate_path: Path,
    minute_path: Path,
    side: str,
    session_dates: Sequence[date],
    workers: int,
    guards: SelectionGuards,
    cost_bps: float = SELECTION_COST_BPS,
    slippage_bps: float = SELECTION_SLIPPAGE_BPS,
    configs: Sequence[SideEntryConfig] = SIDE_GRID,
) -> pd.DataFrame:
    payloads = [config.payload() for config in configs]
    if workers <= 1:
        candidates = pd.read_parquet(candidate_path)
        paths = pd.read_parquet(minute_path)
        prepared = prepare_side_dataset(
            candidates, paths, side=side, session_dates=session_dates
        )
        records = [
            _trial_record(
                prepared,
                config,
                cost_bps=cost_bps,
                slippage_bps=slippage_bps,
                guards=guards,
            )
            for config in configs
        ]
    else:
        with ProcessPoolExecutor(
            max_workers=int(workers),
            initializer=_worker_initializer,
            initargs=(
                str(candidate_path),
                str(minute_path),
                str(side).upper(),
                [value.isoformat() for value in session_dates],
                float(cost_bps),
                float(slippage_bps),
                asdict(guards),
            ),
        ) as pool:
            records = list(pool.map(_worker_trial, payloads, chunksize=1))
    return pd.DataFrame(records).sort_values(
        ["config_hash"], kind="stable"
    ).reset_index(drop=True)


def select_top_side_configs(
    trials: pd.DataFrame, *, top_n: int = 8
) -> tuple[list[SideEntryConfig], pd.DataFrame]:
    if trials.empty:
        return [], trials.copy()
    ranked = trials.copy()
    ranked["_pf_rank"] = pd.to_numeric(
        ranked["profit_factor"], errors="coerce"
    ).replace([np.inf, -np.inf], np.nan).fillna(-math.inf)
    ranked["_robust_rank"] = pd.to_numeric(
        ranked["robust_profit_factor_ex_best_day"], errors="coerce"
    ).replace([np.inf, -np.inf], np.nan).fillna(-math.inf)
    ranked = ranked.sort_values(
        [
            "guard_pass",
            "trades_per_session",
            "_pf_rank",
            "_robust_rank",
            "complexity",
            "config_hash",
        ],
        ascending=[False, False, False, False, True, True],
        kind="stable",
    )
    ranked["behavior_alias_of"] = ""
    representatives: list[int] = []
    observed: dict[str, str] = {}
    for index, row in ranked.iterrows():
        signature = str(row["behavior_signature"])
        if signature in observed:
            ranked.at[index, "behavior_alias_of"] = observed[signature]
        else:
            observed[signature] = str(row["config_hash"])
            if bool(row["guard_pass"]):
                representatives.append(index)
    chosen_indexes = list(representatives[: int(top_n)])
    top = ranked.loc[chosen_indexes]
    configs = [SideEntryConfig.from_payload(value) for value in top["config"]]
    return configs, ranked.drop(columns=["_pf_rank", "_robust_rank"])


def _config_lookup(configs: Sequence[SideEntryConfig]) -> dict[str, SideEntryConfig]:
    return {config.config_hash: config for config in configs}


def _run_config_set(
    prepared: PreparedSideDataset,
    configs: Sequence[SideEntryConfig],
    *,
    cost_bps: float,
    slippage_bps: float,
) -> dict[str, pd.DataFrame]:
    return {
        config.config_hash: run_side_preportfolio(
            prepared,
            config,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
        )
        for config in configs
    }


def _pair_guard_pass(
    train: Mapping[str, Any],
    validation: Mapping[str, Any],
    stress_train: Mapping[str, Any],
    stress_validation: Mapping[str, Any],
    guards: SelectionGuards,
) -> bool:
    train_combined = dict(train["combined"])
    validation_combined = dict(validation["combined"])
    stress_train_combined = dict(stress_train["combined"])
    stress_validation_combined = dict(stress_validation["combined"])
    values = (
        train_combined,
        validation_combined,
        stress_train_combined,
        stress_validation_combined,
    )
    if any(
        int(item.get("data_incomplete_candidates", 0)) > 0
        or int(item.get("unresolved_filled_trades", 0)) > 0
        for item in values
    ):
        return False
    def pf_at_least(metrics: Mapping[str, Any], floor: float) -> bool:
        value = float(metrics.get("profit_factor", math.nan))
        return not math.isnan(value) and value >= floor

    def net_positive(metrics: Mapping[str, Any]) -> bool:
        return float(metrics.get("net_return_percentage_points", 0.0)) > 0.0

    def concentration_and_robustness(metrics: Mapping[str, Any]) -> bool:
        robust = float(
            metrics.get("robust_profit_factor_ex_best_day", math.nan)
        )
        return bool(
            not math.isnan(robust)
            and robust >= guards.min_side_robust_pf
            and float(metrics.get("top_day_share", math.inf))
            <= guards.max_top_day_share
            and int(metrics.get("positive_contiguous_blocks", 0)) >= 2
        )

    return bool(
        int(train_combined.get("closed_fills", 0)) >= guards.min_pair_train_fills
        and pf_at_least(train_combined, guards.min_pair_train_pf)
        and int(validation_combined.get("closed_fills", 0))
        >= guards.min_validation_fills
        and pf_at_least(validation_combined, guards.min_validation_pf)
        and concentration_and_robustness(train_combined)
        and concentration_and_robustness(validation_combined)
        and pf_at_least(stress_train_combined, 1.0)
        and net_positive(stress_train_combined)
        and pf_at_least(stress_validation_combined, 1.0)
        and net_positive(stress_validation_combined)
        and all(
            pf_at_least(dict(train["sides"])[side], 1.0)
            and net_positive(dict(train["sides"])[side])
            and pf_at_least(dict(validation["sides"])[side], 1.0)
            and net_positive(dict(validation["sides"])[side])
            for side in ("LONG", "SHORT")
        )
    )


def evaluate_pair_frontier(
    *,
    long_configs: Sequence[SideEntryConfig],
    short_configs: Sequence[SideEntryConfig],
    train_long: PreparedSideDataset,
    train_short: PreparedSideDataset,
    validation_long: PreparedSideDataset,
    validation_short: PreparedSideDataset,
    guards: SelectionGuards,
) -> tuple[pd.DataFrame, dict[str, dict[str, pd.DataFrame]]]:
    if len(long_configs) > 8 or len(short_configs) > 8:
        raise ValueError("pair frontier accepts at most eight configs per side")
    if not long_configs or not short_configs:
        return pd.DataFrame(), {}
    base_train = {
        "LONG": _run_config_set(
            train_long,
            long_configs,
            cost_bps=SELECTION_COST_BPS,
            slippage_bps=SELECTION_SLIPPAGE_BPS,
        ),
        "SHORT": _run_config_set(
            train_short,
            short_configs,
            cost_bps=SELECTION_COST_BPS,
            slippage_bps=SELECTION_SLIPPAGE_BPS,
        ),
    }
    base_validation = {
        "LONG": _run_config_set(
            validation_long,
            long_configs,
            cost_bps=SELECTION_COST_BPS,
            slippage_bps=SELECTION_SLIPPAGE_BPS,
        ),
        "SHORT": _run_config_set(
            validation_short,
            short_configs,
            cost_bps=SELECTION_COST_BPS,
            slippage_bps=SELECTION_SLIPPAGE_BPS,
        ),
    }
    stress_train = {
        "LONG": _run_config_set(
            train_long,
            long_configs,
            cost_bps=STRESS_COST_BPS,
            slippage_bps=STRESS_SLIPPAGE_BPS,
        ),
        "SHORT": _run_config_set(
            train_short,
            short_configs,
            cost_bps=STRESS_COST_BPS,
            slippage_bps=STRESS_SLIPPAGE_BPS,
        ),
    }
    stress_validation = {
        "LONG": _run_config_set(
            validation_long,
            long_configs,
            cost_bps=STRESS_COST_BPS,
            slippage_bps=STRESS_SLIPPAGE_BPS,
        ),
        "SHORT": _run_config_set(
            validation_short,
            short_configs,
            cost_bps=STRESS_COST_BPS,
            slippage_bps=STRESS_SLIPPAGE_BPS,
        ),
    }
    records: list[dict[str, Any]] = []
    pair_audits: dict[str, dict[str, pd.DataFrame]] = {}
    for long_config in long_configs:
        for short_config in short_configs:
            pair = LongShortConfig(long_config, short_config)
            train_audit = combine_and_constrain(
                base_train["LONG"][long_config.config_hash],
                base_train["SHORT"][short_config.config_hash],
            )
            validation_audit = combine_and_constrain(
                base_validation["LONG"][long_config.config_hash],
                base_validation["SHORT"][short_config.config_hash],
            )
            train_stress_audit = combine_and_constrain(
                stress_train["LONG"][long_config.config_hash],
                stress_train["SHORT"][short_config.config_hash],
            )
            validation_stress_audit = combine_and_constrain(
                stress_validation["LONG"][long_config.config_hash],
                stress_validation["SHORT"][short_config.config_hash],
            )
            train_metrics = score_with_sides(train_audit, train_long.session_dates)
            validation_metrics = score_with_sides(
                validation_audit, validation_long.session_dates
            )
            train_stress_metrics = score_with_sides(
                train_stress_audit, train_long.session_dates
            )
            validation_stress_metrics = score_with_sides(
                validation_stress_audit, validation_long.session_dates
            )
            qualifies = _pair_guard_pass(
                train_metrics,
                validation_metrics,
                train_stress_metrics,
                validation_stress_metrics,
                guards,
            )
            records.append(
                {
                    "pair_hash": pair.config_hash,
                    "long_config_hash": long_config.config_hash,
                    "short_config_hash": short_config.config_hash,
                    "long_config": long_config.payload(),
                    "short_config": short_config.payload(),
                    "complexity": long_config.complexity + short_config.complexity,
                    "qualifies": qualifies,
                    "train_metrics": train_metrics,
                    "validation_metrics": validation_metrics,
                    "train_stress_metrics": train_stress_metrics,
                    "validation_stress_metrics": validation_stress_metrics,
                    "train_trades_per_session": train_metrics["combined"][
                        "trades_per_session"
                    ],
                    "train_pf": train_metrics["combined"]["profit_factor"],
                    "validation_trades_per_session": validation_metrics["combined"][
                        "trades_per_session"
                    ],
                    "validation_pf": validation_metrics["combined"][
                        "profit_factor"
                    ],
                    "stress_validation_pf": validation_stress_metrics["combined"][
                        "profit_factor"
                    ],
                    "stress_validation_net": validation_stress_metrics["combined"][
                        "net_return_percentage_points"
                    ],
                }
            )
            pair_audits[pair.config_hash] = {
                "train": train_audit,
                "validation": validation_audit,
                "train_stress": train_stress_audit,
                "validation_stress": validation_stress_audit,
            }
    frame = pd.DataFrame(records)
    expected_pairs = len(long_configs) * len(short_configs)
    if len(frame) != expected_pairs or expected_pairs > 64:
        raise AssertionError("side frontier produced an invalid pair count")
    return frame, pair_audits


def rank_pairs(pairs: pd.DataFrame) -> pd.DataFrame:
    if pairs.empty:
        return pairs.copy()
    out = pairs.copy()
    out["_min_pf"] = out[["train_pf", "validation_pf"]].min(axis=1)
    out = out.sort_values(
        [
            "qualifies",
            "validation_trades_per_session",
            "_min_pf",
            "validation_pf",
            "train_trades_per_session",
            "complexity",
            "pair_hash",
        ],
        ascending=[False, False, False, False, False, True, True],
        kind="stable",
    ).reset_index(drop=True)
    out.insert(0, "pair_rank", np.arange(1, len(out) + 1))
    return out.drop(columns=["_min_pf"])


def _artifact_path(manifest: Mapping[str, Any], name: str) -> Path:
    record = dict(dict(manifest.get("artifacts", {})).get(name, {}))
    path = Path(str(record.get("path", "")))
    if not provenance.artifact_matches(path, record):
        raise AssertionError(f"V8 cache artifact is missing or changed: {name}")
    return path.resolve()


def _new_run_dir(prefix: str, fingerprint: str) -> Path:
    generated = common.now_ist().strftime("%Y%m%dT%H%M%S%f%z")
    target = RESULT_ROOT / f"{prefix}_{generated}_{fingerprint[:12]}"
    target.mkdir(parents=True, exist_ok=False)
    return target


def _write_new_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="") as handle:
        handle.write(text)
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
                lambda value: json.dumps(value, sort_keys=True, default=str)
                if isinstance(value, (dict, list, tuple))
                else value
            )
    return out


def _safe_for_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _safe_for_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe_for_json(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, (date, datetime, pd.Timestamp)):
        return value.isoformat()
    if pd.isna(value) if not isinstance(value, str) else False:
        return None
    return value


def _search_report(selection: Mapping[str, Any]) -> str:
    diagnostic = bool(selection.get("diagnostic_only"))
    frontier_long = int(selection.get("long_frontier_count", 0))
    frontier_short = int(selection.get("short_frontier_count", 0))
    pair_count = int(selection.get("pair_count", 0))
    lines = [
        "# FNO V8 Side-Specific 1-Minute Entry Optimizer",
        "",
        f"- Status: `{selection.get('selection_status', '')}`",
        f"- Objective: `{OBJECTIVE}`",
        f"- Raw grid: 192 LONG + 192 SHORT",
        f"- Guard-passing behavioral frontier: {frontier_long} LONG x "
        f"{frontier_short} SHORT = {pair_count} global-ledger portfolios",
        f"- Coverage mode: `{selection.get('coverage_mode', '')}`",
        f"- Selected pair rank: {selection.get('selected_pair_rank')}",
        f"- Selected qualifies all preregistered guards: "
        f"{bool(selection.get('selected_qualifies'))}",
        f"- Selection economics: {SELECTION_COST_BPS:g} bps cost + "
        f"{SELECTION_SLIPPAGE_BPS:g} bps entry slippage",
        f"- Severe stress: {STRESS_COST_BPS:g} bps cost + "
        f"{STRESS_SLIPPAGE_BPS:g} bps entry slippage",
        "- Retrospective TEST outcomes/economics were not accessed by search.",
        "",
    ]
    if diagnostic:
        lines.extend(
            [
                "## DIAGNOSTIC-ONLY WATERMARK",
                "",
                str(selection.get("watermark") or "MISSING_WATERMARK"),
                "",
            ]
        )
    eligibility = dict(selection.get("cache_eligibility") or {})
    if eligibility:
        lines.extend(["## Source coverage", ""])
        if eligibility.get("panel_symbol_count") is not None:
            lines.append(
                f"- Frozen TRAIN-derived panel symbols: "
                f"{eligibility.get('panel_symbol_count')}"
            )
        split_coverage = dict(eligibility.get("split_coverage") or {})
        for split_name in ("TRAIN", "VALIDATION", "TEST"):
            values = dict(split_coverage.get(split_name) or {})
            if values:
                lines.append(
                    f"- {split_name}: coverage_pass={values.get('pass')}, "
                    f"complete={values.get('complete_symbol_sessions')}/"
                    f"{values.get('expected_symbol_sessions')}, "
                    f"incomplete={values.get('source_incomplete_symbol_sessions')}"
                )
        lines.append("")
    chosen = selection.get("selected_config")
    if chosen:
        long_payload = dict(chosen.get("long", {}))
        short_payload = dict(chosen.get("short", {}))
        lines.extend(
            [
                "## Frozen selection",
                "",
                f"- Pair hash: `{chosen.get('config_hash', '')}`",
                "- LONG: " + json.dumps(long_payload, sort_keys=True),
                "- SHORT: " + json.dumps(short_payload, sort_keys=True),
                "",
            ]
        )
        metric_sets = (
            ("TRAIN base", selection.get("selected_train_metrics")),
            ("VALIDATION base", selection.get("selected_validation_metrics")),
            ("TRAIN severe", selection.get("selected_train_stress_metrics")),
            (
                "VALIDATION severe",
                selection.get("selected_validation_stress_metrics"),
            ),
        )
        lines.extend(["## Frozen selection metrics", ""])
        for label, raw_metrics in metric_sets:
            bundle = dict(raw_metrics or {})
            combined = dict(bundle.get("combined") or {})
            sides = dict(bundle.get("sides") or {})
            lines.append(
                f"- {label}: closed_fills={combined.get('closed_fills')}, "
                f"trades/session={combined.get('trades_per_session')}, "
                f"profit_factor={combined.get('profit_factor')} "
                f"(PF={combined.get('profit_factor')}), "
                f"robust_PF={combined.get('robust_profit_factor_ex_best_day')}, "
                f"net={combined.get('net_return_percentage_points')}, "
                f"max_drawdown={combined.get('max_drawdown_percentage_points')}, "
                f"top_day_share={combined.get('top_day_share')}, "
                f"positive_blocks={combined.get('positive_contiguous_blocks')}, "
                f"data_incomplete={combined.get('data_incomplete_candidates')}, "
                f"unresolved={combined.get('unresolved_filled_trades')}; "
                f"LONG_PF={dict(sides.get('LONG') or {}).get('profit_factor')}, "
                f"LONG_net={dict(sides.get('LONG') or {}).get('net_return_percentage_points')}, "
                f"SHORT_PF={dict(sides.get('SHORT') or {}).get('profit_factor')}, "
                f"SHORT_net={dict(sides.get('SHORT') or {}).get('net_return_percentage_points')}"
            )
        lines.append("")
    else:
        lines.extend(
            [
                "## Frozen selection",
                "",
                "No configuration passed every preregistered side and pair guard. "
                "No failed configuration was backfilled or frozen.",
                "",
            ]
        )
    lines.extend(
        [
            "## Interpretation",
            "",
            "The retrospective TEST/STRESS window is already inspected history, not a "
            "virgin holdout. Only the separate one-shot command may evaluate it. "
            "Prospective shadow sessions are still required before promotion.",
            "",
        ]
    )
    return "\n".join(lines)


def execute_search(args: argparse.Namespace) -> Path:
    split = SplitContract(
        _parse_day(args.train_from),
        _parse_day(args.train_through),
        _parse_day(args.validation_from),
        _parse_day(args.validation_through),
        _parse_day(args.test_from),
        _parse_day(args.test_through),
    )
    split.validate()
    coverage_mode = _normalize_coverage_mode(args.coverage_mode)
    _require_diagnostic_opt_in(
        coverage_mode, bool(args.allow_conditional_diagnostic)
    )
    symbols = (
        [item.strip().upper() for item in str(args.symbols).split(",") if item.strip()]
        if args.symbols
        else None
    )
    candidates, minute_paths, coverage, manifest, manifest_path = (
        v8.load_or_build_v8_cache(
            source_snapshot_path=args.source_snapshot,
            from_day=split.train_from,
            through_day=split.test_through,
            symbols=symbols,
            rebuild=bool(args.rebuild_cache),
        )
    )
    split_sessions = validate_split_against_manifest(split, manifest)
    broad_eligibility = cache_eligibility(coverage, manifest)
    if coverage_mode == "RECTANGULAR_PANEL":
        broad_coverage = coverage
        candidates, minute_paths, coverage, eligibility = derive_rectangular_panel(
            candidates,
            minute_paths,
            broad_coverage,
            session_dates=split_sessions["TRAIN"],
        )
        panel_symbols = list(eligibility["panel_symbols"])
        split_coverage = {
            split_name: panel_split_coverage(
                broad_coverage, panel_symbols, split_sessions[split_name]
            )
            for split_name in ("TRAIN", "VALIDATION", "TEST")
        }
        eligibility["split_coverage"] = split_coverage
        eligibility["later_split_coverage_policy"] = (
            "FROZEN_TRAIN_PANEL_MEMBERSHIP; LATER_SPLITS_PASS_OR_INVALIDATE"
        )
        if not bool(split_coverage["VALIDATION"]["pass"]):
            raise DataEligibilityError(
                "frozen TRAIN-derived rectangular panel is incomplete in "
                "VALIDATION: "
                f"{split_coverage['VALIDATION']['source_incomplete_symbol_sessions']} "
                "symbol-sessions"
            )
        diagnostic_only = True
        diagnostic_kind = "RECTANGULAR_PANEL_TINY_RETROSPECTIVE"
    elif coverage_mode == "FULL_UNIVERSE":
        eligibility = broad_eligibility
        diagnostic_only = not bool(eligibility["optimization_source_eligible"])
        diagnostic_kind = "INCOMPLETE_FULL_UNIVERSE" if diagnostic_only else None
        if diagnostic_only:
            raise DataEligibilityError(
                "V8 cache is incomplete for the selected universe/session grid: "
                f"{eligibility['source_incomplete_symbol_sessions']} incomplete "
                "symbol-sessions. Repair the source data or explicitly select a "
                "watermarked diagnostic coverage mode."
            )
    elif coverage_mode == "CONDITIONAL_STREAM":
        eligibility = broad_eligibility
        diagnostic_only = True
        diagnostic_kind = "CONDITIONAL_OBSERVED_CANDIDATE_STREAM"
    else:
        raise ValueError(f"unknown coverage mode {args.coverage_mode!r}")

    guards = SelectionGuards(
        min_side_train_fills=int(args.min_side_train_fills),
        min_side_active_days=int(args.min_side_active_days),
        min_side_train_pf=float(args.min_side_train_pf),
        min_side_robust_pf=float(args.min_side_robust_pf),
        min_pair_train_fills=int(args.min_pair_train_fills),
        min_pair_train_pf=float(args.min_pair_train_pf),
        min_validation_fills=int(args.min_validation_fills),
        min_validation_pf=float(args.min_validation_pf),
        max_top_day_share=float(args.max_top_day_share),
    )
    broad_candidate_path = _artifact_path(manifest, "candidates")
    broad_minute_path = _artifact_path(manifest, "paths")
    if coverage_mode == "RECTANGULAR_PANEL":
        derived_fingerprint = common.canonical_json_sha256(
            {
                "broad_cache": manifest.get("input_fingerprint", ""),
                "panel": eligibility,
                "candidate_ids": sorted(candidates["candidate_id"].astype(str)),
            }
        )
        candidate_path, minute_path = _materialize_worker_inputs(
            candidates, minute_paths, fingerprint=derived_fingerprint
        )
    else:
        candidate_path, minute_path = broad_candidate_path, broad_minute_path
    long_trials = search_side_grid(
        candidate_path=candidate_path,
        minute_path=minute_path,
        side="LONG",
        session_dates=split_sessions["TRAIN"],
        workers=int(args.workers),
        guards=guards,
    )
    short_trials = search_side_grid(
        candidate_path=candidate_path,
        minute_path=minute_path,
        side="SHORT",
        session_dates=split_sessions["TRAIN"],
        workers=int(args.workers),
        guards=guards,
    )
    long_top, long_ranked = select_top_side_configs(long_trials, top_n=8)
    short_top, short_ranked = select_top_side_configs(short_trials, top_n=8)

    train_candidates, train_paths = _slice_split(
        candidates, minute_paths, split_sessions["TRAIN"]
    )
    validation_candidates, validation_paths = _slice_split(
        candidates, minute_paths, split_sessions["VALIDATION"]
    )
    train_long = prepare_side_dataset(
        train_candidates,
        train_paths,
        side="LONG",
        session_dates=split_sessions["TRAIN"],
    )
    train_short = prepare_side_dataset(
        train_candidates,
        train_paths,
        side="SHORT",
        session_dates=split_sessions["TRAIN"],
    )
    validation_long = prepare_side_dataset(
        validation_candidates,
        validation_paths,
        side="LONG",
        session_dates=split_sessions["VALIDATION"],
    )
    validation_short = prepare_side_dataset(
        validation_candidates,
        validation_paths,
        side="SHORT",
        session_dates=split_sessions["VALIDATION"],
    )
    pair_trials, pair_audits = evaluate_pair_frontier(
        long_configs=long_top,
        short_configs=short_top,
        train_long=train_long,
        train_short=train_short,
        validation_long=validation_long,
        validation_short=validation_short,
        guards=guards,
    )
    pair_ranked = rank_pairs(pair_trials)
    qualifying_rows = (
        pair_ranked.loc[pair_ranked["qualifies"].fillna(False).astype(bool)]
        if not pair_ranked.empty
        else pair_ranked
    )
    best_row = qualifying_rows.iloc[0] if not qualifying_rows.empty else None
    qualifies = best_row is not None
    selected_pair: LongShortConfig | None = None
    selected_payload: dict[str, Any] | None = None
    if qualifies:
        selected_pair = LongShortConfig(
            SideEntryConfig.from_payload(best_row["long_config"]),
            SideEntryConfig.from_payload(best_row["short_config"]),
        )
        selected_payload = selected_pair.payload()
        selection_status = (
            "DIAGNOSTIC_ONLY_SELECTION"
            if diagnostic_only
            else "CHAMPION_FROZEN_FOR_RETROSPECTIVE_TEST"
        )
    else:
        selection_status = "NO_QUALIFYING_CONFIGURATION"

    watermark = (
        PANEL_WATERMARK
        if coverage_mode == "RECTANGULAR_PANEL"
        else CONDITIONAL_WATERMARK
        if diagnostic_only
        else None
    )

    source_hash = provenance.sha256_file(Path(__file__))
    v8_source_hash = provenance.sha256_file(Path(v8.__file__))
    search_fingerprint = common.canonical_json_sha256(
        {
            "optimizer_version": OPTIMIZER_VERSION,
            "optimizer_source_sha256": source_hash,
            "v8_source_sha256": v8_source_hash,
            "cache_input_fingerprint": manifest.get("input_fingerprint", ""),
            "split": split.payload(),
            "grid_hashes": [config.config_hash for config in SIDE_GRID],
            "guards": asdict(guards),
            "selection_cost_bps": SELECTION_COST_BPS,
            "selection_slippage_bps": SELECTION_SLIPPAGE_BPS,
            "stress_cost_bps": STRESS_COST_BPS,
            "stress_slippage_bps": STRESS_SLIPPAGE_BPS,
            "diagnostic_only": diagnostic_only,
            "diagnostic_kind": diagnostic_kind,
            "coverage_mode": coverage_mode,
            "panel_hash": eligibility.get("panel_hash"),
        }
    )
    run_dir = _new_run_dir("fno_v8_entry_search", search_fingerprint)
    selection = {
        "schema_version": OPTIMIZER_SCHEMA_VERSION,
        "optimizer_version": OPTIMIZER_VERSION,
        "objective": OBJECTIVE,
        "generated_at_ist": common.now_ist().isoformat(timespec="microseconds"),
        "search_run_dir": str(run_dir.resolve()),
        "search_fingerprint": search_fingerprint,
        "selection_status": selection_status,
        "diagnostic_only": diagnostic_only,
        "diagnostic_kind": diagnostic_kind,
        "coverage_mode": coverage_mode,
        "watermark": watermark,
        "promotion_eligible": False,
        "retrospective_test_accessed": False,
        "retrospective_test_outcomes_accessed": False,
        "retrospective_test_source_coverage_checked": (
            coverage_mode == "RECTANGULAR_PANEL"
        ),
        "retrospective_test_disclosure": (
            "2026-07-13..2026-07-31 is already-inspected historical stress, "
            "not a virgin holdout"
        ),
        "selected_config": selected_payload,
        "selected_pair_rank": (
            int(best_row["pair_rank"]) if best_row is not None else None
        ),
        "selected_qualifies": qualifies,
        "selected_train_metrics": (
            _safe_for_json(best_row["train_metrics"])
            if best_row is not None
            else None
        ),
        "selected_validation_metrics": (
            _safe_for_json(best_row["validation_metrics"])
            if best_row is not None
            else None
        ),
        "selected_train_stress_metrics": (
            _safe_for_json(best_row["train_stress_metrics"])
            if best_row is not None
            else None
        ),
        "selected_validation_stress_metrics": (
            _safe_for_json(best_row["validation_stress_metrics"])
            if best_row is not None
            else None
        ),
        "split_contract": split.payload(),
        "split_hash": split.split_hash,
        "split_session_counts": {
            key: len(value) for key, value in split_sessions.items()
        },
        "guards": asdict(guards),
        "grid_schema_version": GRID_SCHEMA_VERSION,
        "grid_count_per_side": len(SIDE_GRID),
        "long_frontier_count": len(long_top),
        "short_frontier_count": len(short_top),
        "pair_count": len(pair_ranked),
        "selection_cost_bps": SELECTION_COST_BPS,
        "selection_slippage_bps": SELECTION_SLIPPAGE_BPS,
        "stress_cost_bps": STRESS_COST_BPS,
        "stress_slippage_bps": STRESS_SLIPPAGE_BPS,
        "cache_manifest_path": str(Path(manifest_path).resolve()),
        "cache_manifest_sha256": provenance.sha256_file(manifest_path),
        "cache_input_fingerprint": manifest.get("input_fingerprint", ""),
        "candidate_cache_path": str(candidate_path),
        "minute_path_cache_path": str(minute_path),
        "candidate_cache_sha256": provenance.sha256_file(candidate_path),
        "minute_path_cache_sha256": provenance.sha256_file(minute_path),
        "candidate_cache_size": int(candidate_path.stat().st_size),
        "minute_path_cache_size": int(minute_path.stat().st_size),
        "coverage_mode": coverage_mode,
        "diagnostic_kind": diagnostic_kind,
        "optimizer_source_sha256": source_hash,
        "v8_source_sha256": v8_source_hash,
        "cache_eligibility": _safe_for_json(eligibility),
    }

    outputs: dict[str, Path] = {}
    outputs["long_trials"] = _write_new_csv(
        _csv_ready(long_ranked), run_dir / "train_long_trials.csv"
    )
    outputs["short_trials"] = _write_new_csv(
        _csv_ready(short_ranked), run_dir / "train_short_trials.csv"
    )
    outputs["pair_trials"] = _write_new_csv(
        _csv_ready(pair_ranked), run_dir / "train_validation_pair_frontier.csv"
    )
    outputs["selection"] = provenance.write_immutable_json(
        run_dir / "selection.json", _safe_for_json(selection)
    )
    outputs["report"] = _write_new_text(
        run_dir / "report.md", _search_report(selection)
    )
    outputs["optimizer_source_archive"] = provenance.publish_immutable_copy(
        Path(__file__), run_dir / Path(__file__).name, expected_sha256=source_hash
    )
    outputs["v8_source_archive"] = provenance.publish_immutable_copy(
        Path(v8.__file__), run_dir / Path(v8.__file__).name,
        expected_sha256=v8_source_hash,
    )
    if selected_payload is not None and selected_pair is not None:
        chosen_audits = pair_audits[selected_pair.config_hash]
        outputs["selected_train_trades"] = _write_new_csv(
            _csv_ready(chosen_audits["train"]), run_dir / "selected_train_audit.csv"
        )
        outputs["selected_validation_trades"] = _write_new_csv(
            _csv_ready(chosen_audits["validation"]),
            run_dir / "selected_validation_audit.csv",
        )

    provenance_payload = provenance.build_run_provenance(
        generated_at=common.now_ist(),
        strategy_version=OPTIMIZER_VERSION,
        objective=OBJECTIVE,
        strategy_payload={
            "optimizer_schema_version": OPTIMIZER_SCHEMA_VERSION,
            "grid_schema_version": GRID_SCHEMA_VERSION,
            "v8_strategy": v8.strategy_payload(),
        },
        parameters={
            "split_contract": split.payload(),
            "guards": asdict(guards),
            "workers": int(args.workers),
            "diagnostic_only": diagnostic_only,
            "grid_count_per_side": len(SIDE_GRID),
            "selection_cost_bps": SELECTION_COST_BPS,
            "selection_slippage_bps": SELECTION_SLIPPAGE_BPS,
            "stress_cost_bps": STRESS_COST_BPS,
            "stress_slippage_bps": STRESS_SLIPPAGE_BPS,
        },
        backtest_window={
            "from_day": split.train_from.isoformat(),
            "through_day": split.test_through.isoformat(),
            "split_contract": split.payload(),
        },
        cache_manifest_path=manifest_path,
        cache_manifest=manifest,
        output_paths=outputs,
        results={
            "selection_status": selection_status,
            "selected_config_hash": (
                selected_pair.config_hash if selected_pair is not None else None
            ),
            "retrospective_test_accessed": False,
            "promotion_eligible": False,
            "diagnostic_only": diagnostic_only,
        },
    )
    provenance_payload["optimizer_source_sha256"] = source_hash
    provenance_payload["v8_source_sha256"] = v8_source_hash
    provenance_payload["search_fingerprint"] = search_fingerprint
    provenance_payload["watermark"] = selection.get("watermark")
    provenance.write_immutable_json(run_dir / "provenance.json", provenance_payload)
    return run_dir


def load_authenticated_selection(search_run: Path | str) -> dict[str, Any]:
    """Load the frozen selection only after matching search provenance."""

    run_dir = Path(search_run).resolve()
    selection_path = (run_dir / "selection.json").resolve()
    provenance_path = (run_dir / "provenance.json").resolve()
    if not selection_path.exists():
        raise FileNotFoundError(f"search selection is missing: {selection_path}")
    if not provenance_path.exists():
        raise FileNotFoundError(f"search provenance is missing: {provenance_path}")
    frozen_provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    selection_record = dict(
        dict(frozen_provenance.get("outputs", {})).get("selection", {})
    )
    recorded_path = Path(str(selection_record.get("path", ""))).resolve()
    if recorded_path != selection_path:
        raise AssertionError(
            "search provenance does not identify this selection.json"
        )
    if not provenance.artifact_matches(selection_path, selection_record):
        raise AssertionError("selection.json changed after search provenance freeze")
    selection = json.loads(selection_path.read_text(encoding="utf-8"))
    if str(selection.get("search_run_dir", "")):
        if Path(str(selection["search_run_dir"])).resolve() != run_dir:
            raise AssertionError("selection search_run_dir is inconsistent")
    if str(selection.get("search_fingerprint", "")) != str(
        frozen_provenance.get("search_fingerprint", "")
    ):
        raise AssertionError("selection search fingerprint is not authenticated")
    frozen_results = dict(frozen_provenance.get("results", {}))
    expected_hash = frozen_results.get("selected_config_hash")
    chosen = selection.get("selected_config")
    observed_hash = dict(chosen).get("config_hash") if chosen else None
    if "selected_config_hash" in frozen_results and observed_hash != expected_hash:
        raise AssertionError(
            "selection winner does not match frozen provenance results"
        )
    return selection


def execute_test_once(args: argparse.Namespace) -> Path:
    search_run = Path(args.search_run).resolve()
    selection = load_authenticated_selection(search_run)
    if selection.get("selected_config") is None:
        raise HoldoutAccessError("search did not freeze a selected configuration")
    diagnostic_only = bool(selection.get("diagnostic_only"))
    if diagnostic_only and not bool(args.allow_conditional_diagnostic):
        raise HoldoutAccessError(
            "diagnostic selection requires --allow-conditional-diagnostic"
        )
    if provenance.sha256_file(Path(__file__)) != str(
        selection.get("optimizer_source_sha256", "")
    ):
        raise AssertionError("optimizer source changed after search freeze")
    if provenance.sha256_file(Path(v8.__file__)) != str(
        selection.get("v8_source_sha256", "")
    ):
        raise AssertionError("V8 source changed after search freeze")
    manifest_path = Path(str(selection["cache_manifest_path"])).resolve()
    if provenance.sha256_file(manifest_path) != str(
        selection["cache_manifest_sha256"]
    ):
        raise AssertionError("V8 cache manifest changed after search freeze")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("input_fingerprint") != selection.get("cache_input_fingerprint"):
        raise AssertionError("V8 cache fingerprint changed after search freeze")

    split = SplitContract.from_mapping(selection["split_contract"])
    sessions = validate_split_against_manifest(split, manifest)["TEST"]
    if str(selection.get("coverage_mode", "")) == "RECTANGULAR_PANEL":
        test_coverage = dict(
            dict(selection.get("cache_eligibility") or {})
            .get("split_coverage", {})
            .get("TEST", {})
        )
        if not bool(test_coverage.get("pass")):
            raise DataEligibilityError(
                "frozen TRAIN-derived panel failed TEST source-coverage gate"
            )
    candidate_path = Path(str(selection["candidate_cache_path"])).resolve()
    minute_path = Path(str(selection["minute_path_cache_path"])).resolve()
    cache_records = (
        (
            "candidate",
            candidate_path,
            int(selection.get("candidate_cache_size", -1)),
            str(selection.get("candidate_cache_sha256", "")),
        ),
        (
            "minute-path",
            minute_path,
            int(selection.get("minute_path_cache_size", -1)),
            str(selection.get("minute_path_cache_sha256", "")),
        ),
    )
    for label, path, expected_size, expected_sha in cache_records:
        if not path.exists() or int(path.stat().st_size) != expected_size:
            raise AssertionError(f"selected {label} cache size changed after search")
        if provenance.sha256_file(path) != expected_sha:
            raise AssertionError(f"selected {label} cache changed after search")
    chosen = dict(selection["selected_config"])
    long_config = SideEntryConfig.from_payload(chosen["long"])
    short_config = SideEntryConfig.from_payload(chosen["short"])
    pair = LongShortConfig(long_config, short_config)
    if pair.config_hash != str(chosen["config_hash"]):
        raise AssertionError("frozen pair configuration hash is invalid")

    claim_path = search_run / "retrospective_test_evaluation_claim.json"
    if claim_path.exists():
        raise HoldoutAccessError(
            f"retrospective TEST/STRESS was already claimed: {claim_path}"
        )
    evaluation_id = (
        f"test_{common.now_ist().strftime('%Y%m%dT%H%M%S%f%z')}_"
        f"{uuid.uuid4().hex[:8]}"
    )
    _write_new_text(
        claim_path,
        json.dumps(
            {
                "schema_version": OPTIMIZER_SCHEMA_VERSION,
                "evaluation_id": evaluation_id,
                "claimed_at_ist": common.now_ist().isoformat(timespec="microseconds"),
                "test_policy_evaluation_count": 1,
                "disclosure": "already-inspected retrospective TEST/STRESS",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    evaluation_dir = search_run / evaluation_id
    evaluation_dir.mkdir(parents=False, exist_ok=False)

    candidates = pd.read_parquet(candidate_path)
    paths = pd.read_parquet(minute_path)
    test_candidates, test_paths = _slice_split(candidates, paths, sessions)
    prepared_long = prepare_side_dataset(
        test_candidates, test_paths, side="LONG", session_dates=sessions
    )
    prepared_short = prepare_side_dataset(
        test_candidates, test_paths, side="SHORT", session_dates=sessions
    )
    base_audit = combine_and_constrain(
        run_side_preportfolio(
            prepared_long,
            long_config,
            cost_bps=SELECTION_COST_BPS,
            slippage_bps=SELECTION_SLIPPAGE_BPS,
        ),
        run_side_preportfolio(
            prepared_short,
            short_config,
            cost_bps=SELECTION_COST_BPS,
            slippage_bps=SELECTION_SLIPPAGE_BPS,
        ),
    )
    stress_audit = combine_and_constrain(
        run_side_preportfolio(
            prepared_long,
            long_config,
            cost_bps=STRESS_COST_BPS,
            slippage_bps=STRESS_SLIPPAGE_BPS,
        ),
        run_side_preportfolio(
            prepared_short,
            short_config,
            cost_bps=STRESS_COST_BPS,
            slippage_bps=STRESS_SLIPPAGE_BPS,
        ),
    )
    base_metrics = score_with_sides(base_audit, sessions)
    stress_metrics = score_with_sides(stress_audit, sessions)
    result = {
        "schema_version": OPTIMIZER_SCHEMA_VERSION,
        "evaluation_id": evaluation_id,
        "search_run_dir": str(search_run),
        "pair_config_hash": pair.config_hash,
        "frozen_config": pair.payload(),
        "diagnostic_only": diagnostic_only,
        "watermark": selection.get("watermark"),
        "retrospective_test_disclosure": (
            "already-inspected historical stress; not a virgin holdout"
        ),
        "test_policy_evaluation_count": 1,
        "selection_economics": {
            "cost_bps": SELECTION_COST_BPS,
            "slippage_bps": SELECTION_SLIPPAGE_BPS,
            "metrics": _safe_for_json(base_metrics),
        },
        "severe_stress_economics": {
            "cost_bps": STRESS_COST_BPS,
            "slippage_bps": STRESS_SLIPPAGE_BPS,
            "metrics": _safe_for_json(stress_metrics),
        },
        "promotion_eligible": False,
    }
    outputs: dict[str, Path] = {}
    outputs["base_audit"] = _write_new_csv(
        _csv_ready(base_audit), evaluation_dir / "retrospective_test_audit.csv"
    )
    outputs["stress_audit"] = _write_new_csv(
        _csv_ready(stress_audit),
        evaluation_dir / "retrospective_test_severe_stress_audit.csv",
    )
    outputs["result"] = provenance.write_immutable_json(
        evaluation_dir / "result.json", _safe_for_json(result)
    )
    report_lines = [
        "# FNO V8 Retrospective TEST/STRESS Evaluation",
        "",
        "**Already-inspected history; not a virgin holdout.**",
        "",
        f"- Pair: `{pair.config_hash}`",
        f"- LONG config: {json.dumps(long_config.payload(), sort_keys=True)}",
        f"- SHORT config: {json.dumps(short_config.payload(), sort_keys=True)}",
        "- Policy evaluations: 1",
        f"- Base closed fills: {base_metrics['combined']['closed_fills']}",
        f"- Base PF: {base_metrics['combined']['profit_factor']}",
        f"- Base robust PF: "
        f"{base_metrics['combined']['robust_profit_factor_ex_best_day']}",
        f"- Base net: {base_metrics['combined']['net_return_percentage_points']}",
        f"- Base trades/session: {base_metrics['combined']['trades_per_session']}",
        f"- Base LONG PF/net: {base_metrics['sides']['LONG']['profit_factor']} / "
        f"{base_metrics['sides']['LONG']['net_return_percentage_points']}",
        f"- Base SHORT PF/net: {base_metrics['sides']['SHORT']['profit_factor']} / "
        f"{base_metrics['sides']['SHORT']['net_return_percentage_points']}",
        f"- Severe PF: {stress_metrics['combined']['profit_factor']}",
        f"- Severe net: "
        f"{stress_metrics['combined']['net_return_percentage_points']}",
        f"- Diagnostic only: {diagnostic_only}",
        "",
    ]
    if selection.get("watermark"):
        report_lines.extend([str(selection["watermark"]), ""])
    report = "\n".join(report_lines)
    outputs["report"] = _write_new_text(evaluation_dir / "report.md", report)
    provenance_payload = provenance.build_run_provenance(
        generated_at=common.now_ist(),
        strategy_version=OPTIMIZER_VERSION,
        objective="ONE_SHOT_RETROSPECTIVE_TEST_STRESS_EVALUATION",
        strategy_payload={
            "optimizer_schema_version": OPTIMIZER_SCHEMA_VERSION,
            "frozen_pair": pair.payload(),
            "v8_strategy": v8.strategy_payload(),
        },
        parameters={
            "test_policy_evaluation_count": 1,
            "diagnostic_only": diagnostic_only,
            "selection_cost_bps": SELECTION_COST_BPS,
            "selection_slippage_bps": SELECTION_SLIPPAGE_BPS,
            "stress_cost_bps": STRESS_COST_BPS,
            "stress_slippage_bps": STRESS_SLIPPAGE_BPS,
        },
        backtest_window={
            "from_day": split.test_from.isoformat(),
            "through_day": split.test_through.isoformat(),
            "split": "RETROSPECTIVE_TEST_STRESS_ALREADY_INSPECTED",
        },
        cache_manifest_path=manifest_path,
        cache_manifest=manifest,
        output_paths=outputs,
        results=_safe_for_json(result),
    )
    provenance_payload["test_policy_evaluation_count"] = 1
    provenance_payload["watermark"] = result["watermark"]
    provenance.write_immutable_json(
        evaluation_dir / "provenance.json", provenance_payload
    )
    return evaluation_dir


def _add_split_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--train-from", default=DEFAULT_TRAIN_FROM)
    parser.add_argument("--train-through", default=DEFAULT_TRAIN_THROUGH)
    parser.add_argument("--validation-from", default=DEFAULT_VALIDATION_FROM)
    parser.add_argument("--validation-through", default=DEFAULT_VALIDATION_THROUGH)
    parser.add_argument("--test-from", default=DEFAULT_TEST_FROM)
    parser.add_argument("--test-through", default=DEFAULT_TEST_THROUGH)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Independent side-specific FNO V8 one-minute entry optimizer"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    search = subparsers.add_parser("search")
    search.add_argument("--source-snapshot", required=True)
    search.add_argument("--symbols")
    search.add_argument("--rebuild-cache", action="store_true")
    search.add_argument(
        "--coverage-mode",
        choices=("rectangular-panel", "full-universe", "conditional-stream"),
        default="full-universe",
        help=(
            "full-universe is the fail-closed default; rectangular-panel derives "
            "membership from TRAIN coverage and recomputes ranks; both panel and "
            "conditional-stream alternatives require explicit diagnostic opt-in"
        ),
    )
    search.add_argument(
        "--workers", type=int, default=max(1, min(4, (os.cpu_count() or 2) - 1))
    )
    search.add_argument("--allow-conditional-diagnostic", action="store_true")
    _add_split_arguments(search)
    defaults = SelectionGuards()
    search.add_argument(
        "--min-side-train-fills", type=int, default=defaults.min_side_train_fills
    )
    search.add_argument(
        "--min-side-active-days", type=int, default=defaults.min_side_active_days
    )
    search.add_argument(
        "--min-side-train-pf", type=float, default=defaults.min_side_train_pf
    )
    search.add_argument(
        "--min-side-robust-pf", type=float, default=defaults.min_side_robust_pf
    )
    search.add_argument(
        "--min-pair-train-fills", type=int, default=defaults.min_pair_train_fills
    )
    search.add_argument(
        "--min-pair-train-pf", type=float, default=defaults.min_pair_train_pf
    )
    search.add_argument(
        "--min-validation-fills", type=int, default=defaults.min_validation_fills
    )
    search.add_argument(
        "--min-validation-pf", type=float, default=defaults.min_validation_pf
    )
    search.add_argument(
        "--max-top-day-share", type=float, default=defaults.max_top_day_share
    )

    test = subparsers.add_parser("evaluate-test")
    test.add_argument("--search-run", required=True)
    test.add_argument("--allow-conditional-diagnostic", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "search":
        run_dir = execute_search(args)
        print(run_dir)
        return 0
    if args.command == "evaluate-test":
        evaluation_dir = execute_test_once(args)
        print(evaluation_dir)
        return 0
    raise AssertionError(f"unhandled command {args.command!r}")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (DataEligibilityError, HoldoutAccessError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
