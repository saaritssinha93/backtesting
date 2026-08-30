"""Causal five-minute selection resolver for staged FNO V12 research.

Every variant is reconstructed from the complete V10/V11 all-candidate frame,
never from V11's already-filtered 1,134 rows.  The resolver first applies the
fully resolved V11/V12 selection configuration, then recomputes deterministic
within-session/setup ranks.  Relaxed thresholds can therefore restore rows
that the frozen V11 09:35 ceiling or 09:40 floor excluded.
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, replace
from types import MappingProxyType
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

import fno_v12_variant_registry as registry


RUNTIME_SCHEMA_VERSION = "fno_v12_all_candidate_selection_runtime_v1"
EQUAL_RANK_PICKER = "v12_equal_rank"
BOUND_TOLERANCE = 1e-12

EQUAL_RANK_ALGORITHM = {
    "algorithm_id": "V12_0940_SHORT_EQUAL_WEIGHT_RANK_V1",
    "group": ["session_date", "setup_id"],
    "features": ["directional_move_pct", "volume_ratio", "traded_value"],
    "feature_rank": "DESCENDING_METHOD_MIN",
    "picker_score": "NEGATIVE_MEAN_OF_THREE_FEATURE_RANKS",
    "higher_picker_score_is_better": True,
    "tie_breakers": ["traded_value_desc", "symbol_asc"],
}
EQUAL_RANK_ALGORITHM_SHA256 = registry.canonical_json_sha256(
    EQUAL_RANK_ALGORITHM
)

DECISION_COLUMNS = (
    "candidate_id",
    "variant_id",
    "stage_id",
    "setup_id",
    "kept",
    "reason",
    "evaluated_rules",
    "failed_rules",
    "resolved_picker",
    "resolved_picker_value",
    "resolved_frozen_rank",
    "original_frozen_rank",
    "resolved_config_sha256",
    "ema_gap_algorithm_sha256",
    "ema9_prior",
    "ema20_prior",
    "ema50_prior",
    "ema_gap_fast_current",
    "ema_gap_slow_current",
    "ema_gap_fast_prior",
    "ema_gap_slow_prior",
    "ema_gap_fast_persistence_ratio",
    "ema_gap_slow_persistence_ratio",
    "equal_rank_move_rank",
    "equal_rank_volume_rank",
    "equal_rank_liquidity_rank",
    "equal_rank_score",
)

_EMA_DIAGNOSTIC_COLUMNS = (
    "ema9_prior",
    "ema20_prior",
    "ema50_prior",
    "ema_gap_fast_current",
    "ema_gap_slow_current",
    "ema_gap_fast_prior",
    "ema_gap_slow_prior",
    "ema_gap_fast_persistence_ratio",
    "ema_gap_slow_persistence_ratio",
)

_EQUAL_RANK_DIAGNOSTIC_COLUMNS = (
    "equal_rank_move_rank",
    "equal_rank_volume_rank",
    "equal_rank_liquidity_rank",
    "equal_rank_score",
)


@dataclass(frozen=True)
class SetupFieldOverride:
    setup_id: str
    field_name: str
    old_value: Any
    new_value: Any


@dataclass(frozen=True)
class SetupPatchMetadata:
    variant_id: str
    resolved_config_sha256: str
    field_overrides: tuple[SetupFieldOverride, ...]
    requires_equal_rank_picker_hook: bool
    equal_rank_algorithm_sha256: str | None


@dataclass(frozen=True)
class SelectionMetadata:
    variant_id: str
    stage_id: str
    input_candidate_count: int
    selected_candidate_count: int
    rejected_candidate_count: int
    resolved_config_sha256: str
    ema_gap_algorithm_sha256: str | None
    equal_rank_algorithm_sha256: str | None
    equal_rank_score_items: tuple[tuple[str, float], ...]

    @property
    def equal_rank_picker_scores(self) -> Mapping[str, float]:
        return MappingProxyType(dict(self.equal_rank_score_items))

    def payload(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "equal_rank_score_items": [
                [candidate_id, score]
                for candidate_id, score in self.equal_rank_score_items
            ],
        }


@dataclass
class PreparedSelection:
    config: registry.ResolvedConfig
    candidates: pd.DataFrame
    decisions: pd.DataFrame
    setups: tuple[Any, ...]
    selection_metadata: SelectionMetadata
    setup_patch_metadata: SetupPatchMetadata


VariantLike = registry.ResolvedConfig | registry.VariantSpec | str


def _resolve_config(value: VariantLike) -> registry.ResolvedConfig:
    registry.validate_registry(
        require_pinned_hash=True,
        require_parent_contract=False,
    )
    if isinstance(value, registry.ResolvedConfig):
        registry.validate_resolved_config(value)
        if not value.post_hoc:
            canonical = registry.resolve_variant(value.variant_id)
            if value != canonical:
                raise ValueError(
                    f"FNO V12 resolved config {value.variant_id!r} is not canonical"
                )
        return value
    return registry.resolve_variant(value)


def _require_columns(frame: pd.DataFrame, columns: Sequence[str]) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"FNO V12 candidate frame missing columns: {missing}")


def _validated_input(candidates: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(candidates, pd.DataFrame):
        raise TypeError("FNO V12 candidates must be a pandas DataFrame")
    _require_columns(
        candidates,
        (
            "candidate_id",
            "session_date",
            "setup_id",
            "side",
            "symbol",
            "price_change_pct",
            "volume_ratio",
            "traded_value",
            "oi_change_pct",
            "frozen_rank",
        ),
    )
    work = candidates.copy(deep=True)
    if work["candidate_id"].isna().any():
        raise ValueError("FNO V12 candidate_id cannot be null")
    work["candidate_id"] = work["candidate_id"].astype(str)
    if work["candidate_id"].str.strip().eq("").any():
        raise ValueError("FNO V12 candidate_id cannot be empty")
    if work["candidate_id"].duplicated().any():
        duplicates = sorted(
            work.loc[work["candidate_id"].duplicated(), "candidate_id"].unique()
        )
        raise ValueError(f"FNO V12 candidate IDs must be unique: {duplicates}")
    if work["session_date"].isna().any():
        raise ValueError("FNO V12 session_date cannot be null")
    if work["setup_id"].isna().any():
        raise ValueError("FNO V12 setup_id cannot be null")
    unknown = sorted(set(work["setup_id"].astype(str)) - registry.VALID_SETUP_IDS)
    if unknown:
        raise ValueError(f"FNO V12 candidate frame has unknown setups: {unknown}")
    expected_sides = work["setup_id"].astype(str).str.rsplit("_", n=1).str[-1]
    wrong_side = work["side"].astype(str).str.upper().ne(expected_sides)
    if wrong_side.any():
        ids = work.loc[wrong_side, "candidate_id"].tolist()
        raise ValueError(f"FNO V12 setup/side mismatch: {ids}")
    if work["symbol"].isna().any() or work["symbol"].astype(str).str.strip().eq(
        ""
    ).any():
        raise ValueError("FNO V12 symbol cannot be null or empty")
    work["setup_id"] = work["setup_id"].astype(str)
    work["side"] = work["side"].astype(str).str.upper()
    work["symbol"] = work["symbol"].astype(str)
    work["original_frozen_rank"] = work["frozen_rank"]
    return work.reset_index(drop=True)


def _finite_numeric(
    frame: pd.DataFrame, column: str, target: pd.Series
) -> pd.Series:
    _require_columns(frame, (column,))
    values = pd.to_numeric(frame[column], errors="coerce")
    finite = values.notna() & values.map(
        lambda value: False if pd.isna(value) else math.isfinite(float(value))
    )
    invalid = target & ~finite
    if invalid.any():
        ids = frame.loc[invalid, "candidate_id"].astype(str).tolist()
        raise ValueError(
            f"FNO V12 {column} must be finite for targeted candidates: {ids}"
        )
    return values.astype(float)


def _directional_move(frame: pd.DataFrame, target: pd.Series) -> pd.Series:
    raw = _finite_numeric(frame, "price_change_pct", target)
    return raw.abs()


def _register_rule(
    *,
    name: str,
    target: pd.Series,
    passed: pd.Series,
    measured: pd.Series,
    threshold: float,
    comparator: str,
    evaluated: list[list[dict[str, Any]]],
    failed: list[list[str]],
) -> None:
    for index in np.flatnonzero(target.to_numpy(dtype=bool)):
        value = float(measured.iloc[index])
        evaluated[index].append(
            {
                "rule": name,
                "measured": value,
                "threshold": float(threshold),
                "comparator": comparator,
                "passed": bool(passed.iloc[index]),
            }
        )
        if not bool(passed.iloc[index]):
            failed[index].append(name)


def _apply_min_rule(
    frame: pd.DataFrame,
    *,
    setup_id: str,
    column: str,
    threshold: float,
    name: str,
    evaluated: list[list[dict[str, Any]]],
    failed: list[list[str]],
) -> None:
    target = frame["setup_id"].eq(setup_id)
    measured = (
        _directional_move(frame, target)
        if column == "directional_move_pct"
        else _finite_numeric(frame, column, target)
    )
    passed = measured.add(BOUND_TOLERANCE).ge(float(threshold))
    _register_rule(
        name=name,
        target=target,
        passed=passed,
        measured=measured,
        threshold=threshold,
        comparator="GE_INCLUSIVE",
        evaluated=evaluated,
        failed=failed,
    )


def _apply_max_rule(
    frame: pd.DataFrame,
    *,
    setup_id: str,
    threshold: float,
    name: str,
    evaluated: list[list[dict[str, Any]]],
    failed: list[list[str]],
) -> None:
    target = frame["setup_id"].eq(setup_id)
    measured = _directional_move(frame, target)
    passed = measured.le(float(threshold) + BOUND_TOLERANCE)
    _register_rule(
        name=name,
        target=target,
        passed=passed,
        measured=measured,
        threshold=threshold,
        comparator="LE_INCLUSIVE",
        evaluated=evaluated,
        failed=failed,
    )


def _ema_prior(current: pd.Series, close: pd.Series, span: int) -> pd.Series:
    alpha = 2.0 / (float(span) + 1.0)
    return (current - alpha * close) / (1.0 - alpha)


def _apply_ema_persistence_rule(
    frame: pd.DataFrame,
    *,
    threshold: float,
    evaluated: list[list[dict[str, Any]]],
    failed: list[list[str]],
) -> None:
    target = frame["setup_id"].eq("09:25_SHORT")
    _require_columns(frame, ("ema9", "ema20", "ema50", "five_min_close"))
    ema9 = _finite_numeric(frame, "ema9", target)
    ema20 = _finite_numeric(frame, "ema20", target)
    ema50 = _finite_numeric(frame, "ema50", target)
    close = _finite_numeric(frame, "five_min_close", target)
    invalid_close = target & close.le(0)
    if invalid_close.any():
        ids = frame.loc[invalid_close, "candidate_id"].tolist()
        raise ValueError(f"FNO V12 EMA recurrence requires close > 0: {ids}")

    prior9 = _ema_prior(ema9, close, 9)
    prior20 = _ema_prior(ema20, close, 20)
    prior50 = _ema_prior(ema50, close, 50)
    current_fast = ema20 - ema9
    current_slow = ema50 - ema20
    prior_fast = prior20 - prior9
    prior_slow = prior50 - prior20
    fast_ratio = current_fast / prior_fast.where(prior_fast > 0)
    slow_ratio = current_slow / prior_slow.where(prior_slow > 0)

    diagnostics = {
        "ema9_prior": prior9,
        "ema20_prior": prior20,
        "ema50_prior": prior50,
        "ema_gap_fast_current": current_fast,
        "ema_gap_slow_current": current_slow,
        "ema_gap_fast_prior": prior_fast,
        "ema_gap_slow_prior": prior_slow,
        "ema_gap_fast_persistence_ratio": fast_ratio,
        "ema_gap_slow_persistence_ratio": slow_ratio,
    }
    for column, values in diagnostics.items():
        frame.loc[target, column] = values.loc[target]
    frame.loc[target, "ema_gap_algorithm_sha256"] = (
        registry.EMA_GAP_PERSISTENCE_ALGORITHM_SHA256
    )

    prior_positive = prior_fast.gt(0) & prior_slow.gt(0)
    persisted = current_fast.add(BOUND_TOLERANCE).ge(
        float(threshold) * prior_fast
    ) & current_slow.add(BOUND_TOLERANCE).ge(float(threshold) * prior_slow)
    passed = prior_positive & persisted
    measured = pd.concat((fast_ratio, slow_ratio), axis=1).min(axis=1)
    _register_rule(
        name="EMA_GAP_0925_SHORT_PERSISTENCE_MIN",
        target=target,
        passed=passed,
        measured=measured.fillna(0.0),
        threshold=threshold,
        comparator="BOTH_GE_INCLUSIVE_AND_PRIOR_GAPS_POSITIVE",
        evaluated=evaluated,
        failed=failed,
    )
    for index in np.flatnonzero((target & ~prior_positive).to_numpy(dtype=bool)):
        failed[index].append("EMA_GAP_0925_SHORT_PRIOR_GAP_NONPOSITIVE")


def _picker_measure(
    frame: pd.DataFrame, picker: str, target: pd.Series
) -> pd.Series:
    if picker == "max_move":
        return _directional_move(frame, target)
    if picker == "max_volume":
        return _finite_numeric(frame, "volume_ratio", target)
    if picker == "max_liquidity":
        return _finite_numeric(frame, "traded_value", target)
    if picker == "max_oi":
        return _finite_numeric(frame, "oi_change_pct", target)
    raise ValueError(f"unsupported FNO V12 picker: {picker}")


def _rerank_selected(
    selected: pd.DataFrame, config: registry.ResolvedConfig
) -> tuple[pd.DataFrame, tuple[tuple[str, float], ...]]:
    work = selected.copy(deep=True)
    for column in _EQUAL_RANK_DIAGNOSTIC_COLUMNS:
        if column not in work:
            work[column] = np.nan
    if work.empty:
        return work, ()
    traded_value = _finite_numeric(work, "traded_value", pd.Series(True, index=work.index))
    picker_by_setup = dict(registry.BASE_SETUP_PICKERS)
    picker_by_setup["09:40_SHORT"] = config.selection.picker_0940_short
    work["picker"] = work["setup_id"].map(picker_by_setup)
    if work["picker"].isna().any():
        raise AssertionError("FNO V12 resolved picker mapping is incomplete")
    work["picker_value"] = np.nan

    score_items: list[tuple[str, float]] = []
    for setup_id, picker in picker_by_setup.items():
        target = work["setup_id"].eq(setup_id)
        if not target.any():
            continue
        if picker != EQUAL_RANK_PICKER:
            values = _picker_measure(work, picker, target)
            work.loc[target, "picker_value"] = values.loc[target]
            continue
        for _, group in work.loc[target].groupby("session_date", sort=False):
            move = pd.to_numeric(group["price_change_pct"], errors="coerce").abs()
            volume = pd.to_numeric(group["volume_ratio"], errors="coerce")
            liquidity = pd.to_numeric(group["traded_value"], errors="coerce")
            if not (
                np.isfinite(move).all()
                and np.isfinite(volume).all()
                and np.isfinite(liquidity).all()
            ):
                raise ValueError("FNO V12 equal-rank inputs must be finite")
            move_rank = move.rank(method="min", ascending=False)
            volume_rank = volume.rank(method="min", ascending=False)
            liquidity_rank = liquidity.rank(method="min", ascending=False)
            score = -(move_rank + volume_rank + liquidity_rank) / 3.0
            work.loc[group.index, "equal_rank_move_rank"] = move_rank
            work.loc[group.index, "equal_rank_volume_rank"] = volume_rank
            work.loc[group.index, "equal_rank_liquidity_rank"] = liquidity_rank
            work.loc[group.index, "equal_rank_score"] = score
            work.loc[group.index, "picker_value"] = score
            score_items.extend(
                (str(work.at[index, "candidate_id"]), float(score.at[index]))
                for index in group.index
            )

    if work["picker_value"].isna().any():
        ids = work.loc[work["picker_value"].isna(), "candidate_id"].tolist()
        raise AssertionError(f"FNO V12 picker values are incomplete: {ids}")
    work = work.sort_values(
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
    work["frozen_rank"] = (
        work.groupby(["session_date", "setup_id"], sort=False)
        .cumcount()
        .add(1)
        .astype(int)
    )
    return work, tuple(sorted(score_items))


def apply_variant_to_all_candidates(
    candidates: pd.DataFrame, value: VariantLike
) -> tuple[pd.DataFrame, pd.DataFrame, SelectionMetadata]:
    """Resolve one V12 variant from the complete all-candidate frame."""

    config = _resolve_config(value)
    input_columns = tuple(candidates.columns)
    work = _validated_input(candidates)
    for column in _EMA_DIAGNOSTIC_COLUMNS + _EQUAL_RANK_DIAGNOSTIC_COLUMNS:
        if column not in work:
            work[column] = np.nan
    work["ema_gap_algorithm_sha256"] = ""
    evaluated: list[list[dict[str, Any]]] = [[] for _ in range(len(work))]
    failed: list[list[str]] = [[] for _ in range(len(work))]
    selection = config.selection

    _apply_min_rule(
        work,
        setup_id="09:40_LONG",
        column="directional_move_pct",
        threshold=selection.move_0940_long_min_pct,
        name="MOVE_0940_LONG_MIN",
        evaluated=evaluated,
        failed=failed,
    )
    if selection.move_0935_long_max_pct is not None:
        _apply_max_rule(
            work,
            setup_id="09:35_LONG",
            threshold=selection.move_0935_long_max_pct,
            name="MOVE_0935_LONG_MAX",
            evaluated=evaluated,
            failed=failed,
        )
    if selection.move_0925_long_max_pct is not None:
        _apply_max_rule(
            work,
            setup_id="09:25_LONG",
            threshold=selection.move_0925_long_max_pct,
            name="MOVE_0925_LONG_MAX",
            evaluated=evaluated,
            failed=failed,
        )
    if selection.move_0925_short_max_pct is not None:
        _apply_max_rule(
            work,
            setup_id="09:25_SHORT",
            threshold=selection.move_0925_short_max_pct,
            name="MOVE_0925_SHORT_MAX",
            evaluated=evaluated,
            failed=failed,
        )
    for setup_id, threshold, name in (
        ("09:35_LONG", selection.volume_0935_long_min, "VOLUME_0935_LONG_MIN"),
        (
            "09:40_SHORT",
            selection.volume_0940_short_min,
            "VOLUME_0940_SHORT_MIN",
        ),
        (
            "09:45_SHORT",
            selection.volume_0945_short_min,
            "VOLUME_0945_SHORT_MIN",
        ),
    ):
        _apply_min_rule(
            work,
            setup_id=setup_id,
            column="volume_ratio",
            threshold=threshold,
            name=name,
            evaluated=evaluated,
            failed=failed,
        )
    if selection.ema_gap_0925_short_persistence_min_ratio is not None:
        _apply_ema_persistence_rule(
            work,
            threshold=selection.ema_gap_0925_short_persistence_min_ratio,
            evaluated=evaluated,
            failed=failed,
        )

    kept = pd.Series([not values for values in failed], index=work.index, dtype=bool)
    selected, score_items = _rerank_selected(work.loc[kept], config)
    rank_map = selected.set_index("candidate_id")["frozen_rank"]
    picker_map = selected.set_index("candidate_id")["picker"]
    picker_value_map = selected.set_index("candidate_id")["picker_value"]
    selected_diagnostics = selected.set_index("candidate_id")
    config_hash = registry.resolved_config_sha256(config)

    decisions = pd.DataFrame(
        {
            "candidate_id": work["candidate_id"],
            "variant_id": config.variant_id,
            "stage_id": config.stage_id,
            "setup_id": work["setup_id"],
            "kept": kept,
            "reason": [
                "PASSED_ALL_RESOLVED_SELECTION_RULES"
                if not values
                else "REJECTED:" + ";".join(values)
                for values in failed
            ],
            "evaluated_rules": [
                json.dumps(value, sort_keys=True, separators=(",", ":"))
                for value in evaluated
            ],
            "failed_rules": [";".join(value) for value in failed],
            "resolved_picker": work["candidate_id"].map(picker_map),
            "resolved_picker_value": work["candidate_id"].map(picker_value_map),
            "resolved_frozen_rank": work["candidate_id"].map(rank_map),
            "original_frozen_rank": work["original_frozen_rank"],
            "resolved_config_sha256": config_hash,
            "ema_gap_algorithm_sha256": work["ema_gap_algorithm_sha256"],
        }
    )
    for column in _EMA_DIAGNOSTIC_COLUMNS + _EQUAL_RANK_DIAGNOSTIC_COLUMNS:
        decisions[column] = work["candidate_id"].map(
            selected_diagnostics[column]
            if column in selected_diagnostics
            else pd.Series(dtype=float)
        )
        rejected_values = work.loc[~kept].set_index("candidate_id")[column]
        decisions.loc[~kept, column] = decisions.loc[~kept, "candidate_id"].map(
            rejected_values
        )
    decisions = decisions.loc[:, DECISION_COLUMNS].reset_index(drop=True)
    metadata = SelectionMetadata(
        variant_id=config.variant_id,
        stage_id=config.stage_id,
        input_candidate_count=len(work),
        selected_candidate_count=len(selected),
        rejected_candidate_count=int((~kept).sum()),
        resolved_config_sha256=config_hash,
        ema_gap_algorithm_sha256=(
            registry.EMA_GAP_PERSISTENCE_ALGORITHM_SHA256
            if selection.ema_gap_0925_short_persistence_min_ratio is not None
            else None
        ),
        equal_rank_algorithm_sha256=(
            EQUAL_RANK_ALGORITHM_SHA256
            if selection.picker_0940_short == EQUAL_RANK_PICKER
            else None
        ),
        equal_rank_score_items=score_items,
    )
    # Engine-facing candidates retain exactly the input schema and order.
    # Internal recurrence/rank diagnostics are deliberately confined to the
    # decision ledger and metadata so frozen Stage0 can preserve V11's input
    # binding byte-for-byte at the DataFrame contract level.
    public_selected = selected.loc[:, input_columns].copy()
    return public_selected, decisions, metadata


def _validated_base_setups(base_setups: Sequence[Any]) -> tuple[Any, ...]:
    setups = tuple(base_setups)
    ids = [str(getattr(setup, "setup_id", "")) for setup in setups]
    if len(ids) != len(set(ids)) or set(ids) != registry.VALID_SETUP_IDS:
        raise ValueError("FNO V12 requires the complete unique ten-leg V11 setup book")
    by_id = {setup.setup_id: setup for setup in setups}
    observed_pickers = {setup_id: str(setup.picker) for setup_id, setup in by_id.items()}
    if observed_pickers != dict(registry.BASE_SETUP_PICKERS):
        raise ValueError("FNO V12 base setup pickers differ from frozen V11")
    expected_fields = {
        # V11 enforces the 0.40 Stage-7 floor in its external selection
        # overlay; the underlying setup authority remains the earlier 0.20.
        ("09:40_LONG", "price_change_pct"): 0.20,
        ("09:35_LONG", "volume_ratio"): 1.00,
        ("09:40_SHORT", "volume_ratio"): 1.00,
        ("09:45_SHORT", "volume_ratio"): 1.00,
    }
    for (setup_id, field), expected in expected_fields.items():
        observed = float(getattr(by_id[setup_id], field))
        if not math.isclose(observed, expected, rel_tol=0.0, abs_tol=1e-12):
            raise ValueError(
                f"FNO V12 base {setup_id}.{field} differs from frozen V11"
            )
    return setups


def derive_patched_engine_setups(
    base_setups: Sequence[Any], value: VariantLike
) -> tuple[tuple[Any, ...], SetupPatchMetadata]:
    config = _resolve_config(value)
    setups = list(_validated_base_setups(base_setups))
    requested: dict[str, dict[str, Any]] = {
        "09:40_LONG": {},
        "09:35_LONG": {"volume_ratio": config.selection.volume_0935_long_min},
        "09:40_SHORT": {
            "volume_ratio": config.selection.volume_0940_short_min,
            "picker": config.selection.picker_0940_short,
        },
        "09:45_SHORT": {"volume_ratio": config.selection.volume_0945_short_min},
    }
    if not math.isclose(
        config.selection.move_0940_long_min_pct,
        registry.BASE_SELECTION_CONFIG.move_0940_long_min_pct,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        requested["09:40_LONG"]["price_change_pct"] = (
            config.selection.move_0940_long_min_pct
        )
    overrides: list[SetupFieldOverride] = []
    for index, setup in enumerate(setups):
        changes = requested.get(setup.setup_id, {})
        effective: dict[str, Any] = {}
        for field, new_value in changes.items():
            old_value = getattr(setup, field)
            equal = (
                math.isclose(
                    float(old_value), float(new_value), rel_tol=0.0, abs_tol=1e-12
                )
                if isinstance(old_value, (int, float))
                and isinstance(new_value, (int, float))
                else old_value == new_value
            )
            if equal:
                continue
            effective[field] = new_value
            overrides.append(
                SetupFieldOverride(setup.setup_id, field, old_value, new_value)
            )
        if effective:
            setups[index] = replace(setup, **effective)
    metadata = SetupPatchMetadata(
        variant_id=config.variant_id,
        resolved_config_sha256=registry.resolved_config_sha256(config),
        field_overrides=tuple(overrides),
        requires_equal_rank_picker_hook=(
            config.selection.picker_0940_short == EQUAL_RANK_PICKER
        ),
        equal_rank_algorithm_sha256=(
            EQUAL_RANK_ALGORITHM_SHA256
            if config.selection.picker_0940_short == EQUAL_RANK_PICKER
            else None
        ),
    )
    return tuple(setups), metadata


def prepare_variant_selection(
    candidates: pd.DataFrame,
    base_setups: Sequence[Any],
    value: VariantLike,
) -> PreparedSelection:
    config = _resolve_config(value)
    selected, decisions, selection_metadata = apply_variant_to_all_candidates(
        candidates, config
    )
    setups, setup_patch_metadata = derive_patched_engine_setups(base_setups, config)
    return PreparedSelection(
        config=config,
        candidates=selected,
        decisions=decisions,
        setups=setups,
        selection_metadata=selection_metadata,
        setup_patch_metadata=setup_patch_metadata,
    )


__all__ = [
    "BOUND_TOLERANCE",
    "DECISION_COLUMNS",
    "EQUAL_RANK_ALGORITHM",
    "EQUAL_RANK_ALGORITHM_SHA256",
    "EQUAL_RANK_PICKER",
    "PreparedSelection",
    "RUNTIME_SCHEMA_VERSION",
    "SelectionMetadata",
    "SetupFieldOverride",
    "SetupPatchMetadata",
    "apply_variant_to_all_candidates",
    "derive_patched_engine_setups",
    "prepare_variant_selection",
]
