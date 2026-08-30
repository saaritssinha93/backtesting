"""Isolated Stage 1 runner for predeclared V10 5m/1m experiments.

The frozen V10B launcher and neutral V8 state-machine are not edited.  This
module installs process-local adapters that:

* apply monotone five-minute selection overlays before replay and rerank;
* optionally require causal one-minute confirmation relative volume (RV1);
* optionally expire pending entries at S+4 rather than S+5; and
* bind every mechanism, source and selection decision into provenance.

Every output remains research-only and promotion-ineligible.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v10_experiment_config as experiment_config
import fno_v10_unified_5m_1m_backtest as v10
import fno_v8_windowed_1m_entry_backtest as engine


STRATEGY_FAMILY = "FNO_V10_STAGE1_ISOLATED_EXPERIMENTS_20260826"
SELECTION_DECISION_SCHEMA_VERSION = "fno_v10_selection_decision_v1"
SLOT_RVOL_LOOKBACK_SESSIONS = 20
SLOT_RVOL_MIN_HISTORY = 10

ROOT = (
    common.FNO_ROOT
    / "strategy_research"
    / experiment_config.EXPERIMENT_ROOT_VERSION
)
CACHE_DIR = ROOT / "cache"
FEATURE_CACHE_DIR = ROOT / "feature_cache"
RUN_ROOT = ROOT / "runs"
PROVENANCE_ROOT = ROOT / "provenance"
LATEST_ROOT = ROOT / "latest"
SNAPSHOT_ROOT = ROOT / "snapshots"


@dataclass(frozen=True)
class ExperimentEntryPolicy(engine.EntryPolicy):
    confirmation_volume_ratio_min: float | None = None

    def validate(self) -> None:
        super().validate()
        value = self.confirmation_volume_ratio_min
        if value is not None and (
            not math.isfinite(float(value)) or float(value) <= 0
        ):
            raise ValueError(
                "confirmation_volume_ratio_min must be finite and positive"
            )


# Neutral seams are captured once.  v10.configure_engine() resets engine
# globals before every experiment configuration, so these never stack.
_NEUTRAL_ENTRY_POLICY_FOR_VARIANT = engine.entry_policy_for_variant
_NEUTRAL_CONFIRMATION_CHECK = engine._confirmation_check
_NEUTRAL_LOAD_OR_BUILD_CACHE = engine.load_or_build_v8_cache
_NEUTRAL_RUN_BACKTEST = engine.run_v8_backtest

_ACTIVE_SPEC: experiment_config.ExperimentSpec = experiment_config.get_spec(
    "V10B"
)
_V10_STRATEGY_PAYLOAD: Any = None
_V10_PROVENANCE_BUILDER: Any = None
_V10_PROVENANCE_VALIDATOR: Any = None
_LAST_SELECTION_DECISIONS: pd.DataFrame | None = None
_ACTIVE_SIDECAR_MANIFEST_PATH: Path | None = None
_ACTIVE_SIDECAR_BINDING: dict[str, Any] | None = None


def runner_sha256() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def config_source_sha256() -> str:
    return hashlib.sha256(Path(experiment_config.__file__).read_bytes()).hexdigest()


def _slot_rvol_binding(
    manifest: Mapping[str, Any], manifest_path: Path
) -> dict[str, Any]:
    table_record = dict(dict(manifest.get("artifacts", {})).get("slot_rvol20", {}))
    table_path = Path(str(table_record.get("path", "")))
    if not engine.provenance.artifact_matches(table_path, table_record):
        raise AssertionError("Same-slot RVOL table does not match its manifest")
    return {
        "schema_version": experiment_config.SLOT_RVOL_SCHEMA_VERSION,
        "input_fingerprint": str(manifest.get("input_fingerprint", "")),
        "source_manifest_sha256": engine.provenance.sha256_file(manifest_path),
        "table_sha256": str(table_record.get("sha256", "")),
        "table_bytes": int(table_record.get("bytes", -1)),
        "row_count": int(manifest.get("row_count", -1)),
    }


def resolved_experiment_contract(
    spec: experiment_config.ExperimentSpec | None = None,
) -> dict[str, Any]:
    selected = spec or _ACTIVE_SPEC
    return {
        "schema_version": experiment_config.CONFIG_SCHEMA_VERSION,
        "strategy_family": STRATEGY_FAMILY,
        "selected_variant": selected.payload(),
        "variant_config_sha256": experiment_config.variant_config_sha256(
            selected
        ),
        "registry_sha256": experiment_config.registry_sha256(),
        "base_v10_unified_contract_sha256": v10.UNIFIED_CONTRACT_SHA256,
        "base_v10_setup_book_sha256": v10.ACTIVE_SETUP_BOOK_SHA256,
        "selection_overlay": {
            "base_cache": "FROZEN_V10B_CANDIDATE_SUPERSET",
            "application": "FILTER_BEFORE_STATE_MACHINE_THEN_RERANK",
            "rank_order": [
                "session_date_ASC",
                "setup_id_ASC",
                "picker_value_DESC",
                "traded_value_DESC",
                "symbol_ASC",
            ],
            "rejected_candidate_backfill": True,
        },
        "confirmation_volume_ratio": {
            "formula": "CONFIRMATION_1M_VOLUME/(SIGNAL_5M_VOLUME/5)",
            "evaluated": "AFTER_CONFIRMATION_BAR_CLOSE",
            "same_confirmation_bar_fill": False,
            "unavailable_policy": "FAIL_CLOSED",
        },
        "same_slot_rvol20": {
            "schema_version": experiment_config.SLOT_RVOL_SCHEMA_VERSION,
            "formula": (
                "CURRENT_EXACT_5M_VOLUME/MEDIAN_SAME_SYMBOL_HHMM_VOLUME_"
                "PREVIOUS_20_OFFICIAL_SESSIONS"
            ),
            "lookback_sessions": SLOT_RVOL_LOOKBACK_SESSIONS,
            "minimum_prior_observations": SLOT_RVOL_MIN_HISTORY,
            "shift_before_rolling": True,
            "unavailable_policy": "FAIL_CLOSED",
        },
        "entry_execution": {
            "pending_expiry_minute": selected.entry_expiry_minute,
            "same_confirmation_bar_fill": False,
            "post_confirmation_cancel": True,
            "same_bar_exit_policy": "STOP_FIRST",
        },
        "runner_source_sha256": runner_sha256(),
        "config_source_sha256": config_source_sha256(),
        "research_only": True,
        "promotion_eligible": False,
    }


def resolved_experiment_contract_sha256(
    spec: experiment_config.ExperimentSpec | None = None,
) -> str:
    return common.canonical_json_sha256(resolved_experiment_contract(spec))


def _entry_policy_for_variant(
    variant: str,
    *,
    cost_bps: float,
    slippage_bps: float,
    square_off: str,
    eod_policy: str,
) -> ExperimentEntryPolicy:
    spec = experiment_config.get_spec(variant)
    base = _NEUTRAL_ENTRY_POLICY_FOR_VARIANT(
        spec.variant,
        cost_bps=cost_bps,
        slippage_bps=slippage_bps,
        square_off=square_off,
        eod_policy=eod_policy,
    )
    payload = asdict(base)
    payload["entry_expiry_minute"] = spec.entry_expiry_minute
    policy = ExperimentEntryPolicy(
        **payload,
        confirmation_volume_ratio_min=spec.confirmation_volume_ratio_min,
    )
    engine.validate_backtest_policy(policy)
    return policy


def _confirmation_check(
    setup: engine.V8Setup,
    candidate: engine.CandidateInput,
    bar: engine.MinuteBar,
    policy: engine.EntryPolicy | None = None,
) -> dict[str, Any]:
    record = _NEUTRAL_CONFIRMATION_CHECK(setup, candidate, bar, policy)
    threshold = getattr(policy, "confirmation_volume_ratio_min", None)
    if threshold is None:
        # Exact V10B seam: do not even add diagnostic keys when RV1 is off.
        return record
    signal_volume = float(candidate.five_min_volume)
    confirmation_volume = float(bar.volume)
    denominator = signal_volume / 5.0
    ratio = (
        confirmation_volume / denominator
        if math.isfinite(confirmation_volume)
        and math.isfinite(denominator)
        and denominator > 0
        else math.nan
    )
    record["confirmation_volume_ratio_denominator"] = denominator
    record["confirmation_volume_ratio"] = ratio
    record["confirmation_volume_ratio_min"] = float(threshold)
    rejection_codes = list(record.get("rejection_codes", []))
    if not math.isfinite(ratio):
        rejection_codes.append("CONFIRMATION_VOLUME_RATIO_UNAVAILABLE")
    elif ratio + 1e-12 < float(threshold):
        rejection_codes.append("CONFIRMATION_VOLUME_RATIO_BELOW_MINIMUM")
    record["rejection_codes"] = rejection_codes
    record["passed"] = not rejection_codes
    return record


def compute_same_slot_rvol20(
    five_minute: pd.DataFrame,
    *,
    official_session_dates: Sequence[date | str | pd.Timestamp],
    signal_slots: Sequence[str],
) -> pd.DataFrame:
    """Return causal same-slot volume features for one symbol.

    Missing official sessions remain inside the 20-session rolling window.
    The current session is shifted out before the median is calculated.
    """

    required = {"ts", "volume"}
    missing = sorted(required - set(five_minute.columns))
    if missing:
        raise ValueError(f"Five-minute input is missing columns: {missing}")
    official = sorted(
        {
            engine._parse_day(value)
            for value in official_session_dates
        }
    )
    if not official:
        raise ValueError("Official session calendar cannot be empty")
    frame = five_minute[["ts", "volume"]].copy()
    frame["ts"] = frame["ts"].map(engine._to_ist_timestamp)
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    frame = frame.sort_values("ts", kind="stable").reset_index(drop=True)
    if frame["ts"].duplicated().any():
        raise ValueError("Same-slot feature input contains duplicate timestamps")
    frame["session_date"] = frame["ts"].dt.date
    frame["signal_end"] = frame["ts"].dt.strftime("%H:%M")
    records: list[pd.DataFrame] = []
    index = pd.Index(official, name="session_date")
    for slot in signal_slots:
        slot_rows = frame.loc[
            frame["signal_end"].eq(slot)
            & frame["session_date"].isin(official)
        ].copy()
        if slot_rows["session_date"].duplicated().any():
            raise ValueError(f"Multiple exact five-minute bars for slot {slot}")
        current = (
            slot_rows.set_index("session_date")["volume"]
            .reindex(index)
            .astype(float)
        )
        prior = current.shift(1)
        rolling = prior.rolling(
            SLOT_RVOL_LOOKBACK_SESSIONS,
            min_periods=1,
        )
        prior_count = rolling.count().astype(int)
        prior_median = prior.rolling(
            SLOT_RVOL_LOOKBACK_SESSIONS,
            min_periods=SLOT_RVOL_MIN_HISTORY,
        ).median()
        available = (
            current.notna()
            & np.isfinite(current)
            & prior_count.ge(SLOT_RVOL_MIN_HISTORY)
            & prior_median.notna()
            & np.isfinite(prior_median)
            & prior_median.gt(0)
        )
        ratio = current.div(prior_median.where(prior_median.gt(0))).where(
            available
        )
        part = pd.DataFrame(
            {
                "session_date": official,
                "signal_end": slot,
                "current_five_min_volume": current.to_numpy(dtype=float),
                "prior_slot_observation_count_20": prior_count.to_numpy(dtype=int),
                "prior_slot_volume_median": prior_median.to_numpy(dtype=float),
                "slot_rvol20": ratio.to_numpy(dtype=float),
                "feature_available": available.to_numpy(dtype=bool),
            }
        )
        part["signal_time"] = part.apply(
            lambda row: pd.Timestamp(
                f"{row['session_date'].isoformat()} {row['signal_end']}",
                tz=common.IST,
            ),
            axis=1,
        )
        records.append(part)
    out = pd.concat(records, ignore_index=True)
    out["feature_schema_version"] = experiment_config.SLOT_RVOL_SCHEMA_VERSION
    return out.sort_values(
        ["session_date", "signal_end"], kind="stable"
    ).reset_index(drop=True)


def _slot_rvol_contract(
    *,
    snapshot: Mapping[str, Any],
    inventory: Mapping[str, Any],
    universe_record: Mapping[str, Any],
    symbols: Sequence[str],
    from_day: date,
    through_day: date,
) -> dict[str, Any]:
    return {
        "schema_version": experiment_config.SLOT_RVOL_SCHEMA_VERSION,
        "runner_source_sha256": runner_sha256(),
        "registry_sha256": experiment_config.registry_sha256(),
        "base_v10_setup_book_sha256": v10.ACTIVE_SETUP_BOOK_SHA256,
        "snapshot_fingerprint": snapshot.get("snapshot_fingerprint", ""),
        "source_inventory_sha256": inventory.get("inventory_sha256", ""),
        "source_fingerprint": inventory.get("source_fingerprint", ""),
        "universe": dict(universe_record),
        "calendar_sha256": engine.NSE_FO_CALENDAR_SHA256,
        "from_day": from_day.isoformat(),
        "through_day": through_day.isoformat(),
        "symbols": list(symbols),
        "signal_slots": list(engine.active_signal_slots()),
        "formula": (
            "EXACT_5M_VOLUME_DIVIDED_BY_SHIFT1_MEDIAN_SAME_HHMM_"
            "PREVIOUS_20_OFFICIAL_SESSIONS"
        ),
        "lookback_sessions": SLOT_RVOL_LOOKBACK_SESSIONS,
        "minimum_prior_observations": SLOT_RVOL_MIN_HISTORY,
        "missing_session_policy": "RETAIN_IN_ROLLING_WINDOW_AS_NAN",
        "invalid_or_insufficient_policy": "FAIL_CLOSED",
    }


def load_or_build_slot_rvol_sidecar(
    *,
    source_snapshot_path: Path | str,
    from_day: date | str,
    through_day: date | str,
    symbols: Iterable[str] | None = None,
    rebuild: bool = False,
) -> tuple[pd.DataFrame, dict[str, Any], Path]:
    start_day = engine._parse_day(from_day)
    end_day = engine._parse_day(through_day)
    mapped, universe_record, snapshot, inventory, source_lookup = (
        engine.load_validated_source_contract(
            source_snapshot_path,
            symbols=symbols,
        )
    )
    selected_symbols = sorted(
        mapped["equity_symbol"].astype(str).str.upper().tolist()
    )
    contract = _slot_rvol_contract(
        snapshot=snapshot,
        inventory=inventory,
        universe_record=universe_record,
        symbols=selected_symbols,
        from_day=start_day,
        through_day=end_day,
    )
    fingerprint = common.canonical_json_sha256(contract)
    root = FEATURE_CACHE_DIR / fingerprint[:16]
    table_path = root / "slot_rvol20.parquet"
    manifest_path = root / "manifest.json"
    if not rebuild and manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError) as exc:
            raise ValueError(
                f"Same-slot RVOL manifest is unreadable: {manifest_path}"
            ) from exc
        valid = (
            manifest.get("schema_version")
            == experiment_config.SLOT_RVOL_SCHEMA_VERSION
            and manifest.get("complete") is True
            and manifest.get("input_fingerprint") == fingerprint
            and common.canonical_json_sha256(manifest.get("input_contract", {}))
            == fingerprint
            and engine.provenance.artifact_matches(
                table_path,
                dict(manifest.get("artifacts", {})).get("slot_rvol20", {}),
            )
        )
        if valid:
            frame = pd.read_parquet(table_path)
            if int(manifest.get("row_count", -1)) == len(frame):
                return frame, manifest, manifest_path

    full_official_dates = engine.expected_regular_session_dates(
        "2026-01-01", "2026-12-31"
    )
    parts: list[pd.DataFrame] = []
    for row in mapped.to_dict("records"):
        symbol = str(row["equity_symbol"]).upper().strip()
        minute = engine.load_equity_minute_history(
            source_lookup[("NSE_EQUITY_1M", symbol)],
            symbol=symbol,
        )
        five = engine.aggregate_equity_one_minute_to_five_minute(minute)
        if not five.empty:
            official_set = set(full_official_dates)
            five = five.loc[five["ts"].dt.date.isin(official_set)].copy()
        features = compute_same_slot_rvol20(
            five,
            official_session_dates=full_official_dates,
            signal_slots=engine.active_signal_slots(),
        )
        features = features.loc[
            pd.Series(features["session_date"], index=features.index).between(
                start_day, end_day
            )
        ].copy()
        features.insert(0, "symbol", symbol)
        parts.append(features)
    sidecar = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if not sidecar.empty:
        sidecar = sidecar.sort_values(
            ["symbol", "signal_time"], kind="stable"
        ).reset_index(drop=True)
        if sidecar.duplicated(["symbol", "signal_time"]).any():
            raise AssertionError("Same-slot RVOL sidecar contains duplicate keys")
    root.mkdir(parents=True, exist_ok=True)
    common.atomic_write_parquet(sidecar, table_path)
    manifest = {
        "schema_version": experiment_config.SLOT_RVOL_SCHEMA_VERSION,
        "complete": True,
        "input_fingerprint": fingerprint,
        "input_contract": contract,
        "row_count": int(len(sidecar)),
        "artifacts": {
            "slot_rvol20": engine.provenance.artifact_record(table_path)
        },
    }
    common.atomic_write_json(manifest_path, manifest)
    return sidecar, manifest, manifest_path


def _augment_candidates_with_slot_rvol(
    candidates: pd.DataFrame,
    sidecar: pd.DataFrame,
) -> pd.DataFrame:
    if candidates.empty:
        return candidates.copy()
    required = {
        "symbol",
        "signal_time",
        "current_five_min_volume",
        "prior_slot_observation_count_20",
        "prior_slot_volume_median",
        "slot_rvol20",
        "feature_available",
    }
    missing = sorted(required - set(sidecar.columns))
    if missing:
        raise ValueError(f"Same-slot RVOL sidecar is missing columns: {missing}")
    metadata = sidecar[list(required)].copy()
    metadata["signal_time"] = metadata["signal_time"].map(
        engine._to_ist_timestamp
    )
    base = candidates.copy()
    base["signal_time"] = base["signal_time"].map(engine._to_ist_timestamp)
    merged = base.merge(
        metadata,
        on=["symbol", "signal_time"],
        how="left",
        validate="many_to_one",
        indicator=True,
    )
    if not merged["_merge"].eq("both").all():
        missing_ids = merged.loc[
            ~merged["_merge"].eq("both"), "candidate_id"
        ].astype(str).tolist()
        raise AssertionError(
            "Same-slot sidecar does not cover baseline candidates: "
            f"{missing_ids[:5]}"
        )
    cached_volume = pd.to_numeric(merged["five_min_volume"], errors="coerce")
    feature_volume = pd.to_numeric(
        merged["current_five_min_volume"], errors="coerce"
    )
    if not np.isclose(
        cached_volume.to_numpy(dtype=float),
        feature_volume.to_numpy(dtype=float),
        rtol=0.0,
        atol=1e-9,
        equal_nan=False,
    ).all():
        raise AssertionError(
            "Same-slot feature current volume disagrees with V10B candidate cache"
        )
    return merged.drop(columns=["_merge"])


def _load_or_build_cache(
    *,
    source_snapshot_path: Path | str,
    from_day: date | str,
    through_day: date | str,
    symbols: Iterable[str] | None = None,
    rebuild: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any], Path]:
    global _ACTIVE_SIDECAR_MANIFEST_PATH
    global _ACTIVE_SIDECAR_BINDING
    base = _NEUTRAL_LOAD_OR_BUILD_CACHE(
        source_snapshot_path=source_snapshot_path,
        from_day=from_day,
        through_day=through_day,
        symbols=symbols,
        rebuild=rebuild,
    )
    candidates, paths, coverage, manifest, manifest_path = base
    _ACTIVE_SIDECAR_MANIFEST_PATH = None
    _ACTIVE_SIDECAR_BINDING = None
    if not _ACTIVE_SPEC.uses_slot_rvol20:
        return base
    sidecar, sidecar_manifest, sidecar_manifest_path = load_or_build_slot_rvol_sidecar(
        source_snapshot_path=source_snapshot_path,
        from_day=from_day,
        through_day=through_day,
        symbols=symbols,
        rebuild=rebuild,
    )
    _ACTIVE_SIDECAR_MANIFEST_PATH = sidecar_manifest_path
    _ACTIVE_SIDECAR_BINDING = _slot_rvol_binding(
        sidecar_manifest, sidecar_manifest_path
    )
    augmented = _augment_candidates_with_slot_rvol(candidates, sidecar)
    return augmented, paths, coverage, manifest, manifest_path


def apply_selection_overlay(
    candidates: pd.DataFrame,
    spec: experiment_config.ExperimentSpec,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filter the frozen V10B candidate superset and recompute contiguous rank."""

    if candidates.empty:
        decisions = pd.DataFrame(
            columns=[
                "candidate_id",
                "session_date",
                "signal_time",
                "setup_id",
                "side",
                "symbol",
                "price_change_pct",
                "five_min_volume",
                "picker",
                "picker_value",
                "traded_value",
                "original_frozen_rank",
                "recalculated_frozen_rank",
                "selection_passed",
                "selection_reason",
                "experiment_variant",
                "selection_overlay_id",
                "slot_rvol20_min",
                "confirmation_volume_ratio_min",
                "entry_expiry_minute",
                "variant_config_sha256",
                "schema_version",
            ]
        )
        return candidates.copy(), decisions
    required = {
        "candidate_id",
        "session_date",
        "signal_time",
        "setup_id",
        "side",
        "symbol",
        "price_change_pct",
        "picker_value",
        "traded_value",
        "frozen_rank",
    }
    missing = sorted(required - set(candidates.columns))
    if missing:
        raise ValueError(f"V10B candidate cache is missing columns: {missing}")
    base = candidates.copy()
    if base["candidate_id"].duplicated().any():
        raise AssertionError("V10B candidate IDs must be unique")
    reasons = pd.Series("PASSED", index=base.index, dtype=object)

    if spec.disabled_setup_ids:
        disabled = base["setup_id"].astype(str).isin(spec.disabled_setup_ids)
        reasons.loc[disabled] = "SETUP_DISABLED"

    for setup_id, threshold in spec.price_threshold_overrides:
        affected = (
            reasons.eq("PASSED")
            & base["setup_id"].astype(str).eq(setup_id)
            & pd.to_numeric(base["price_change_pct"], errors="coerce")
            .add(1e-12)
            .lt(float(threshold))
        )
        reasons.loc[affected] = "PRICE_CHANGE_BELOW_VARIANT_MINIMUM"

    if spec.slot_rvol20_min is not None:
        slot_required = {
            "prior_slot_observation_count_20",
            "prior_slot_volume_median",
            "slot_rvol20",
            "feature_available",
        }
        slot_missing = sorted(slot_required - set(base.columns))
        if slot_missing:
            raise ValueError(
                f"RVOL experiment candidates lack sidecar columns: {slot_missing}"
            )
        count = pd.to_numeric(
            base["prior_slot_observation_count_20"], errors="coerce"
        )
        median = pd.to_numeric(
            base["prior_slot_volume_median"], errors="coerce"
        )
        ratio = pd.to_numeric(base["slot_rvol20"], errors="coerce")
        active = reasons.eq("PASSED")
        insufficient = active & (
            count.isna() | count.lt(SLOT_RVOL_MIN_HISTORY)
        )
        reasons.loc[insufficient] = "SLOT_RVOL_HISTORY_INSUFFICIENT"
        active = reasons.eq("PASSED")
        invalid = active & (
            median.isna()
            | ~np.isfinite(median)
            | median.le(0)
            | ratio.isna()
            | ~np.isfinite(ratio)
        )
        reasons.loc[invalid] = "SLOT_RVOL_BASELINE_INVALID"
        active = reasons.eq("PASSED")
        below = active & ratio.add(1e-12).lt(float(spec.slot_rvol20_min))
        reasons.loc[below] = "SLOT_RVOL_BELOW_MINIMUM"

    passed = reasons.eq("PASSED")
    filtered = base.loc[passed].copy()
    filtered = filtered.sort_values(
        [
            "session_date",
            "setup_id",
            "picker_value",
            "traded_value",
            "symbol",
        ],
        ascending=[True, True, False, False, True],
        kind="stable",
    ).reset_index(drop=True)
    filtered["frozen_rank"] = (
        filtered.groupby(["session_date", "setup_id"], sort=False)
        .cumcount()
        .add(1)
    )
    rank_map = filtered.set_index("candidate_id")["frozen_rank"]

    decision_columns = [
        "candidate_id",
        "session_date",
        "signal_time",
        "setup_id",
        "side",
        "symbol",
        "price_change_pct",
        "five_min_volume",
        "picker",
        "picker_value",
        "traded_value",
        "frozen_rank",
    ]
    optional = [
        "current_five_min_volume",
        "prior_slot_observation_count_20",
        "prior_slot_volume_median",
        "slot_rvol20",
        "feature_available",
    ]
    decisions = base[
        [column for column in decision_columns + optional if column in base.columns]
    ].copy()
    decisions = decisions.rename(columns={"frozen_rank": "original_frozen_rank"})
    decisions["recalculated_frozen_rank"] = decisions["candidate_id"].map(
        rank_map
    )
    decisions["selection_passed"] = passed.to_numpy(dtype=bool)
    decisions["selection_reason"] = reasons.to_numpy(dtype=object)
    decisions["experiment_variant"] = spec.variant
    decisions["selection_overlay_id"] = spec.selection_overlay_id
    decisions["slot_rvol20_min"] = spec.slot_rvol20_min
    decisions["confirmation_volume_ratio_min"] = (
        spec.confirmation_volume_ratio_min
    )
    decisions["entry_expiry_minute"] = spec.entry_expiry_minute
    decisions["variant_config_sha256"] = (
        experiment_config.variant_config_sha256(spec)
    )
    decisions["schema_version"] = SELECTION_DECISION_SCHEMA_VERSION
    return filtered, decisions


def _confirmation_ratio_from_checks(value: Any) -> float:
    if not isinstance(value, list):
        return math.nan
    for check in value:
        if isinstance(check, Mapping) and bool(check.get("passed")):
            ratio = check.get("confirmation_volume_ratio")
            try:
                parsed = float(ratio)
            except (TypeError, ValueError):
                return math.nan
            return parsed if math.isfinite(parsed) else math.nan
    return math.nan


def _run_backtest(
    candidates: pd.DataFrame,
    minute_paths: pd.DataFrame,
    *,
    variant: str,
    policy: engine.EntryPolicy,
    target_exposure_per_entry_rs: float | None = None,
    portfolio_policy: engine.PortfolioPolicy | None = None,
) -> pd.DataFrame:
    global _LAST_SELECTION_DECISIONS
    spec = experiment_config.get_spec(variant)
    if spec.variant != _ACTIVE_SPEC.variant:
        raise AssertionError(
            f"Configured variant {_ACTIVE_SPEC.variant} does not match run {variant}"
        )
    filtered, decisions = apply_selection_overlay(candidates, spec)
    _LAST_SELECTION_DECISIONS = decisions
    audit = _NEUTRAL_RUN_BACKTEST(
        filtered,
        minute_paths,
        variant=variant,
        policy=policy,
        target_exposure_per_entry_rs=target_exposure_per_entry_rs,
        portfolio_policy=portfolio_policy,
    )
    if audit.empty:
        return audit
    audit = audit.copy()
    audit["experiment_variant"] = spec.variant
    audit["selection_overlay_id"] = spec.selection_overlay_id
    audit["variant_config_sha256"] = experiment_config.variant_config_sha256(
        spec
    )
    audit["entry_expiry_minute"] = int(policy.entry_expiry_minute)
    audit["confirmation_volume_ratio_min"] = (
        spec.confirmation_volume_ratio_min
    )
    audit["confirmation_volume_ratio"] = audit["confirmation_checks"].map(
        _confirmation_ratio_from_checks
    )
    metadata_columns = [
        "candidate_id",
        "prior_slot_observation_count_20",
        "prior_slot_volume_median",
        "slot_rvol20",
        "feature_available",
    ]
    metadata_columns = [
        column for column in metadata_columns if column in filtered.columns
    ]
    if len(metadata_columns) > 1:
        audit = audit.merge(
            filtered[metadata_columns],
            on="candidate_id",
            how="left",
            validate="one_to_one",
        )
    return audit


def _strategy_payload() -> dict[str, Any]:
    if _V10_STRATEGY_PAYLOAD is None:
        raise RuntimeError("Experiment engine has not been configured")
    payload = _V10_STRATEGY_PAYLOAD()
    contract = resolved_experiment_contract()
    feature_binding: dict[str, Any] | None = None
    if _ACTIVE_SPEC.uses_slot_rvol20:
        if _ACTIVE_SIDECAR_BINDING is None:
            raise AssertionError(
                "RVOL strategy payload requested before its feature input was bound"
            )
        feature_binding = dict(_ACTIVE_SIDECAR_BINDING)
    payload["v10_stage1_experiment"] = {
        "contract_sha256": common.canonical_json_sha256(contract),
        "contract": contract,
        "feature_input_binding": feature_binding,
    }
    return payload


def _build_run_provenance(**kwargs: Any) -> dict[str, Any]:
    if _V10_PROVENANCE_BUILDER is None:
        raise RuntimeError("Experiment provenance builder is not configured")
    if _LAST_SELECTION_DECISIONS is None:
        raise AssertionError("Selection decisions were not captured before output")
    output_paths = dict(kwargs.get("output_paths", {}))
    engine_archive = Path(str(output_paths["strategy_source_archive"]))
    run_dir = engine_archive.parent
    runner_archive = run_dir / Path(__file__).name
    config_archive = run_dir / Path(experiment_config.__file__).name
    resolved_path = run_dir / "resolved_experiment_config.json"
    selection_path = run_dir / "selection_decisions.csv"
    engine.provenance.publish_immutable_copy(
        Path(__file__), runner_archive, expected_sha256=runner_sha256()
    )
    engine.provenance.publish_immutable_copy(
        Path(experiment_config.__file__),
        config_archive,
        expected_sha256=config_source_sha256(),
    )
    common.atomic_write_json(resolved_path, resolved_experiment_contract())
    common.atomic_write_csv(_LAST_SELECTION_DECISIONS, selection_path)
    output_paths["experiment_runner_source_archive"] = runner_archive
    output_paths["experiment_config_source_archive"] = config_archive
    output_paths["resolved_experiment_config"] = resolved_path
    output_paths["selection_decisions"] = selection_path
    if _ACTIVE_SPEC.uses_slot_rvol20:
        if (
            _ACTIVE_SIDECAR_MANIFEST_PATH is None
            or _ACTIVE_SIDECAR_BINDING is None
        ):
            raise AssertionError("RVOL run has no active feature-sidecar manifest")
        source_manifest = json.loads(
            _ACTIVE_SIDECAR_MANIFEST_PATH.read_text(encoding="utf-8")
        )
        source_table_record = dict(
            dict(source_manifest.get("artifacts", {})).get("slot_rvol20", {})
        )
        source_table_path = Path(str(source_table_record.get("path", "")))
        sidecar_table_archive = run_dir / "slot_rvol20.parquet"
        engine.provenance.publish_immutable_copy(
            source_table_path,
            sidecar_table_archive,
            expected_sha256=str(_ACTIVE_SIDECAR_BINDING["table_sha256"]),
        )
        sidecar_archive = run_dir / "slot_rvol20_manifest.json"
        archived_manifest = json.loads(json.dumps(source_manifest))
        archived_manifest["source_cache_manifest"] = {
            "path": str(_ACTIVE_SIDECAR_MANIFEST_PATH.resolve()),
            "sha256": str(_ACTIVE_SIDECAR_BINDING["source_manifest_sha256"]),
        }
        archived_manifest["artifacts"]["slot_rvol20"] = (
            engine.provenance.artifact_record(sidecar_table_archive)
        )
        common.atomic_write_json(sidecar_archive, archived_manifest)
        output_paths["slot_rvol20_manifest_archive"] = sidecar_archive
        output_paths["slot_rvol20_table_archive"] = sidecar_table_archive
    forwarded = dict(kwargs)
    forwarded["output_paths"] = output_paths
    payload = _V10_PROVENANCE_BUILDER(**forwarded)
    contract_hash = resolved_experiment_contract_sha256()
    payload.update(
        {
            "v10_experiment_run_schema_version": (
                experiment_config.RUN_SCHEMA_VERSION
            ),
            "v10_experiment_variant": _ACTIVE_SPEC.variant,
            "v10_experiment_registry_sha256": (
                experiment_config.registry_sha256()
            ),
            "v10_experiment_variant_config_sha256": (
                experiment_config.variant_config_sha256(_ACTIVE_SPEC)
            ),
            "v10_experiment_contract_sha256": contract_hash,
            "experiment_runner_source_sha256": runner_sha256(),
            "experiment_config_source_sha256": config_source_sha256(),
            "v10_experiment_feature_input_binding": (
                dict(_ACTIVE_SIDECAR_BINDING)
                if _ACTIVE_SIDECAR_BINDING is not None
                else None
            ),
            "research_only": True,
            "promotion_eligible": False,
        }
    )
    return payload


def _artifact_hash(record: Mapping[str, Any], label: str) -> str:
    if not engine.provenance.artifact_matches(record.get("path", ""), record):
        raise AssertionError(f"Experiment output artifact changed: {label}")
    return engine.provenance.sha256_file(Path(str(record.get("path", ""))))


def validate_experiment_run_provenance(path: Path | str) -> dict[str, Any]:
    if _V10_PROVENANCE_VALIDATOR is None:
        raise RuntimeError("Experiment provenance validator is not configured")
    payload = _V10_PROVENANCE_VALIDATOR(path)
    if payload.get("v10_experiment_run_schema_version") != (
        experiment_config.RUN_SCHEMA_VERSION
    ):
        raise ValueError("Not a supported V10 Stage 1 experiment run")
    variant = str(payload.get("v10_experiment_variant", ""))
    parameters_variant = str(dict(payload.get("parameters", {})).get("variant", ""))
    current_spec = experiment_config.get_spec(variant)
    if variant != parameters_variant or variant != _ACTIVE_SPEC.variant:
        raise AssertionError("Experiment variant identity is inconsistent")
    if payload.get("research_only") is not True:
        raise AssertionError("Experiment research-only flag changed")
    if payload.get("promotion_eligible") is not False:
        raise AssertionError("Experiment promotion flag changed")

    outputs = dict(payload.get("outputs", {}))
    required = {
        "experiment_runner_source_archive",
        "experiment_config_source_archive",
        "resolved_experiment_config",
        "selection_decisions",
    }
    missing = sorted(required - set(outputs))
    if missing:
        raise ValueError(f"Experiment provenance is missing outputs: {missing}")
    runner_hash = _artifact_hash(
        dict(outputs["experiment_runner_source_archive"]), "runner source"
    )
    config_hash = _artifact_hash(
        dict(outputs["experiment_config_source_archive"]), "config source"
    )
    if runner_hash != payload.get("experiment_runner_source_sha256"):
        raise AssertionError("Archived experiment runner hash is invalid")
    if config_hash != payload.get("experiment_config_source_sha256"):
        raise AssertionError("Archived experiment config hash is invalid")
    resolved_record = dict(outputs["resolved_experiment_config"])
    _artifact_hash(resolved_record, "resolved experiment config")
    resolved = json.loads(
        Path(str(resolved_record["path"])).read_text(encoding="utf-8")
    )
    resolved_hash = common.canonical_json_sha256(resolved)
    selected_variant = dict(resolved.get("selected_variant", {}))
    if str(selected_variant.get("variant", "")) != variant:
        raise AssertionError("Archived resolved variant identity changed")
    if payload.get("v10_experiment_contract_sha256") != resolved_hash:
        raise AssertionError("Archived resolved experiment config changed")
    if payload.get("v10_experiment_registry_sha256") != resolved.get(
        "registry_sha256"
    ):
        raise AssertionError("Archived experiment registry binding changed")
    archived_variant_hash = common.canonical_json_sha256(
        {
            "schema_version": resolved.get("schema_version"),
            "variant": selected_variant,
            "research_only": True,
            "promotion_eligible": False,
        }
    )
    if payload.get("v10_experiment_variant_config_sha256") != (
        archived_variant_hash
    ):
        raise AssertionError("Archived experiment variant config changed")
    archived_contract = dict(
        dict(payload.get("strategy_payload", {})).get(
            "v10_stage1_experiment", {}
        )
    )
    if archived_contract.get("contract_sha256") != resolved_hash or (
        common.canonical_json_sha256(archived_contract.get("contract", {}))
        != resolved_hash
    ):
        raise AssertionError("Experiment strategy payload contract is invalid")
    uses_slot_rvol20 = selected_variant.get("slot_rvol20_min") is not None
    if uses_slot_rvol20:
        required_sidecar = {
            "slot_rvol20_manifest_archive",
            "slot_rvol20_table_archive",
        }
        missing_sidecar = sorted(required_sidecar - set(outputs))
        if missing_sidecar:
            raise ValueError(
                f"RVOL provenance is missing outputs: {missing_sidecar}"
            )

    decision_record = dict(outputs["selection_decisions"])
    _artifact_hash(decision_record, "selection decisions")
    decisions = pd.read_csv(Path(str(decision_record["path"])))
    required_decision_columns = {
        "candidate_id",
        "selection_passed",
        "selection_reason",
        "experiment_variant",
        "original_frozen_rank",
        "recalculated_frozen_rank",
        "session_date",
        "setup_id",
        "picker_value",
        "traded_value",
        "symbol",
        "schema_version",
    }
    missing_decisions = sorted(required_decision_columns - set(decisions.columns))
    if missing_decisions:
        raise ValueError(
            f"Selection decisions are missing columns: {missing_decisions}"
        )
    if not decisions["schema_version"].astype(str).eq(
        SELECTION_DECISION_SCHEMA_VERSION
    ).all() or not decisions["experiment_variant"].astype(str).eq(variant).all():
        raise AssertionError("Selection decision identity is invalid")
    passed = decisions["selection_passed"].astype(str).str.lower().eq("true")
    audit_record = dict(outputs["candidate_order_audit"])
    audit_rows = len(pd.read_csv(Path(str(audit_record["path"]))))
    if int(passed.sum()) != audit_rows:
        raise AssertionError(
            "Selection decisions do not reconcile to the candidate audit"
        )
    retained = decisions.loc[passed].copy()
    if not retained.empty:
        observed = pd.to_numeric(
            retained["recalculated_frozen_rank"], errors="raise"
        ).astype(int)
        expected = (
            retained.sort_values(
                [
                    "session_date",
                    "setup_id",
                    "picker_value",
                    "traded_value",
                    "symbol",
                ],
                ascending=[True, True, False, False, True],
                kind="stable",
            )
            .groupby(["session_date", "setup_id"], sort=False)
            .cumcount()
            .add(1)
        )
        expected.index = retained.sort_values(
            [
                "session_date",
                "setup_id",
                "picker_value",
                "traded_value",
                "symbol",
            ],
            ascending=[True, True, False, False, True],
            kind="stable",
        ).index
        if not observed.sort_index().equals(expected.sort_index()):
            raise AssertionError("Selection ranks are not deterministic/contiguous")

    strategy_binding = archived_contract.get("feature_input_binding")
    direct_binding = payload.get("v10_experiment_feature_input_binding")
    if uses_slot_rvol20:
        if not isinstance(strategy_binding, Mapping) or not isinstance(
            direct_binding, Mapping
        ):
            raise AssertionError("RVOL feature input is not bound into provenance")
        binding = dict(strategy_binding)
        if binding != dict(direct_binding):
            raise AssertionError("RVOL feature bindings disagree")
        sidecar_record = dict(outputs["slot_rvol20_manifest_archive"])
        _artifact_hash(sidecar_record, "same-slot RVOL manifest")
        sidecar_table_record = dict(outputs["slot_rvol20_table_archive"])
        sidecar_table_hash = _artifact_hash(
            sidecar_table_record, "same-slot RVOL table"
        )
        sidecar_manifest = json.loads(
            Path(str(sidecar_record["path"])).read_text(encoding="utf-8")
        )
        if sidecar_manifest.get("schema_version") != binding.get(
            "schema_version"
        ):
            raise AssertionError("Same-slot RVOL manifest schema changed")
        contract = dict(sidecar_manifest.get("input_contract", {}))
        input_fingerprint = common.canonical_json_sha256(contract)
        if (
            sidecar_manifest.get("input_fingerprint") != input_fingerprint
            or binding.get("input_fingerprint") != input_fingerprint
        ):
            raise AssertionError("Same-slot RVOL input fingerprint is invalid")
        manifest_table_record = dict(
            dict(sidecar_manifest.get("artifacts", {})).get("slot_rvol20", {})
        )
        if not engine.provenance.artifact_matches(
            manifest_table_record.get("path", ""), manifest_table_record
        ):
            raise AssertionError("Same-slot RVOL archived table changed")
        if Path(str(manifest_table_record.get("path", ""))).resolve() != Path(
            str(sidecar_table_record.get("path", ""))
        ).resolve():
            raise AssertionError("Same-slot RVOL manifest is not run-local")
        if (
            sidecar_table_hash != binding.get("table_sha256")
            or manifest_table_record.get("sha256") != sidecar_table_hash
            or int(manifest_table_record.get("bytes", -1))
            != int(binding.get("table_bytes", -2))
            or int(sidecar_manifest.get("row_count", -1))
            != int(binding.get("row_count", -2))
        ):
            raise AssertionError("Same-slot RVOL table binding is invalid")
        source_manifest_record = dict(
            sidecar_manifest.get("source_cache_manifest", {})
        )
        if source_manifest_record.get("sha256") != binding.get(
            "source_manifest_sha256"
        ):
            raise AssertionError("Same-slot RVOL source manifest binding changed")
    elif strategy_binding is not None or direct_binding is not None:
        raise AssertionError("Non-RVOL run unexpectedly binds an RVOL feature input")

    payload["current_experiment_runner_matches_archive"] = (
        runner_sha256() == runner_hash
    )
    payload["current_experiment_config_matches_archive"] = (
        config_source_sha256() == config_hash
    )
    payload["current_experiment_registry_matches_archive"] = (
        experiment_config.registry_sha256()
        == payload.get("v10_experiment_registry_sha256")
    )
    payload["current_experiment_variant_matches_archive"] = (
        experiment_config.variant_config_sha256(current_spec)
        == payload.get("v10_experiment_variant_config_sha256")
    )
    payload["current_experiment_contract_matches_archive"] = (
        resolved_experiment_contract_sha256(current_spec) == resolved_hash
    )
    return payload


def validate_experiment_configuration() -> None:
    experiment_config.validate_registry()
    if _ACTIVE_SPEC.variant not in experiment_config.EXPERIMENT_REGISTRY:
        raise AssertionError("Active experiment variant is not predeclared")
    if engine.ACTIVE_SETUPS != v10.ACTIVE_SETUPS:
        raise AssertionError("Experiment runner changed the frozen V10 setup book")
    if engine.V8_SETUP_BOOK_SHA256 != v10.ACTIVE_SETUP_BOOK_SHA256:
        raise AssertionError("Experiment runner changed the V10 setup-book hash")
    if set(engine.VARIANT_REGISTRY) != set(experiment_config.EXPERIMENT_REGISTRY):
        raise AssertionError("Engine variant registry does not match Stage 1")
    resolved_root = ROOT.resolve()
    if resolved_root == v10.ROOT.resolve() or v10.ROOT.resolve() in resolved_root.parents:
        raise AssertionError("Experiment outputs cannot use the frozen V10 root")
    engine.validate_configuration()
    policy = _entry_policy_for_variant(
        _ACTIVE_SPEC.variant,
        cost_bps=15.0,
        slippage_bps=0.0,
        square_off="15:30",
        eod_policy="LAST_REAL_BAR_SENSITIVITY",
    )
    if policy.entry_expiry_minute != _ACTIVE_SPEC.entry_expiry_minute:
        raise AssertionError("Experiment expiry policy was not resolved")
    if policy.confirmation_volume_ratio_min != (
        _ACTIVE_SPEC.confirmation_volume_ratio_min
    ):
        raise AssertionError("Experiment RV1 policy was not resolved")


def configure_engine(spec: experiment_config.ExperimentSpec | str) -> None:
    global _ACTIVE_SPEC
    global _V10_STRATEGY_PAYLOAD
    global _V10_PROVENANCE_BUILDER
    global _V10_PROVENANCE_VALIDATOR
    global _LAST_SELECTION_DECISIONS
    global _ACTIVE_SIDECAR_MANIFEST_PATH
    global _ACTIVE_SIDECAR_BINDING

    selected = experiment_config.get_spec(spec) if isinstance(spec, str) else spec
    experiment_config.validate_registry()
    # Reset to the frozen V10 launcher first, then layer experiment adapters.
    v10.configure_engine()
    _V10_STRATEGY_PAYLOAD = engine.strategy_payload
    _V10_PROVENANCE_BUILDER = engine.provenance.build_run_provenance
    _V10_PROVENANCE_VALIDATOR = engine.validate_v8_run_provenance
    _ACTIVE_SPEC = selected
    _LAST_SELECTION_DECISIONS = None
    _ACTIVE_SIDECAR_MANIFEST_PATH = None
    _ACTIVE_SIDECAR_BINDING = None

    source_hash = runner_sha256()
    engine.STRATEGY_VERSION = f"{STRATEGY_FAMILY}_{source_hash[:12]}"
    engine.OBJECTIVE = (
        "PREDECLARED_ISOLATED_5M_SELECTION_AND_1M_ENTRY_EXPERIMENTS;"
        "FULL_CHRONOLOGICAL_V10_STATE_MACHINE_REPLAY"
    )
    engine.CONFIG_SOURCE = (
        "FROZEN_V10B_BASE;PREDECLARED_STAGE1_REGISTRY;"
        f"REGISTRY_SHA256={experiment_config.registry_sha256()};"
        f"RUNNER_SHA256={source_hash}"
    )
    engine.CACHE_SCHEMA_VERSION = "fno_v10_stage1_base_candidate_cache_v1"
    engine.RUN_SCHEMA_VERSION = experiment_config.RUN_SCHEMA_VERSION
    engine.PATH_POLICY_VERSION = "fno_v10_stage1_same_session_path_v1"
    engine.ACTIVE_SETUPS = v10.ACTIVE_SETUPS
    engine.V8_SETUP_BOOK_SHA256 = v10.ACTIVE_SETUP_BOOK_SHA256
    engine.VARIANT_REGISTRY = {
        item.variant: {
            "description": item.description,
            "max_confirmation_minute": 1,
            "buffer_bps": 0.0,
            "midpoint_invalidation": False,
            "close_location_min": None,
        }
        for item in experiment_config.EXPERIMENT_SPECS
    }
    engine.V8_ROOT = ROOT
    engine.CACHE_DIR = CACHE_DIR
    engine.SNAPSHOT_ROOT = SNAPSHOT_ROOT
    engine.RUN_ROOT = RUN_ROOT
    engine.PROVENANCE_ROOT = PROVENANCE_ROOT
    engine.REPORT_PATH = LATEST_ROOT / f"latest_{selected.variant.lower()}.md"
    engine.CACHE_MANIFEST_PATH = CACHE_DIR / "manifest.json"
    engine.CANDIDATE_CACHE_PATH = CACHE_DIR / "five_minute_candidates.parquet"
    engine.PATH_CACHE_PATH = CACHE_DIR / "same_session_minute_paths.parquet"
    engine.DEFAULT_SOURCE_SNAPSHOT = None

    engine.entry_policy_for_variant = _entry_policy_for_variant
    engine._confirmation_check = _confirmation_check
    engine.load_or_build_v8_cache = _load_or_build_cache
    engine.run_v8_backtest = _run_backtest
    engine.strategy_payload = _strategy_payload
    engine.provenance.build_run_provenance = _build_run_provenance
    engine.validate_v8_run_provenance = validate_experiment_run_provenance
    validate_experiment_configuration()


def _extract_variant(args: Sequence[str]) -> str:
    for index, value in enumerate(args):
        if value == "--variant" and index + 1 < len(args):
            return str(args[index + 1]).upper().strip()
        if value.startswith("--variant="):
            return value.split("=", 1)[1].upper().strip()
    raise ValueError("Stage 1 run/smoke requires an explicit --variant")


def _variant_from_provenance(args: Sequence[str]) -> str:
    try:
        equals_value = next(
            (
                value.split("=", 1)[1]
                for value in args
                if value.startswith("--provenance=")
            ),
            None,
        )
        if equals_value is not None:
            path = Path(equals_value)
        else:
            index = list(args).index("--provenance")
            path = Path(args[index + 1])
    except (ValueError, IndexError) as exc:
        raise ValueError("validate requires --provenance PATH") from exc
    payload = json.loads(path.read_text(encoding="utf-8"))
    return str(
        payload.get("v10_experiment_variant")
        or dict(payload.get("parameters", {})).get("variant", "")
    ).upper().strip()


def _parse_symbols(value: str) -> list[str] | None:
    parsed = [item.strip().upper() for item in value.split(",") if item.strip()]
    return parsed or None


def _build_slot_rvol_command(args: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Build the causal same-slot RVOL20 feature sidecar"
    )
    parser.add_argument("--source-snapshot", type=Path, required=True)
    parser.add_argument("--from-day", required=True)
    parser.add_argument("--through-day", required=True)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--rebuild-cache", action="store_true")
    parsed = parser.parse_args(args)
    frame, manifest, manifest_path = load_or_build_slot_rvol_sidecar(
        source_snapshot_path=parsed.source_snapshot,
        from_day=parsed.from_day,
        through_day=parsed.through_day,
        symbols=_parse_symbols(parsed.symbols),
        rebuild=parsed.rebuild_cache,
    )
    print(
        f"[V10-EXPERIMENT][SLOT-RVOL] rows={len(frame)} "
        f"fingerprint={manifest['input_fingerprint']} manifest={manifest_path}"
    )
    return 0


def _print_variants() -> int:
    rows = [
        {
            "variant": spec.variant,
            "description": spec.description,
            "config_sha256": experiment_config.variant_config_sha256(spec),
            "selection_overlay_id": spec.selection_overlay_id,
        }
        for spec in experiment_config.EXPERIMENT_SPECS
    ]
    print(
        json.dumps(
            {
                "schema_version": experiment_config.CONFIG_SCHEMA_VERSION,
                "registry_sha256": experiment_config.registry_sha256(),
                "variants": rows,
            },
            indent=2,
        )
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        raise ValueError("A Stage 1 command is required")
    command = args[0]
    if command == "list-variants":
        experiment_config.validate_registry()
        return _print_variants()
    if command == "build-slot-rvol-cache":
        configure_engine("SLOT_RVOL_150")
        return _build_slot_rvol_command(args[1:])
    if command in {"run", "smoke"} and not ({"-h", "--help"} & set(args[1:])):
        selected = _extract_variant(args)
    elif command in {"run", "smoke"}:
        selected = "V10B"
    elif command == "validate":
        selected = _variant_from_provenance(args)
    else:
        selected = "V10B"
    configure_engine(selected)
    return engine.main(args)


if __name__ == "__main__":
    raise SystemExit(main())
