"""Isolated causal adverse-gap research for locked FNO V10 Stage 7.

This module does not edit or replace any frozen/live strategy file.  It loads
the already-frozen V10B candidate/path caches, applies the locked Stage-7
``09:40_LONG >= 0.40%`` selection overlay, and installs a process-local entry
adapter around the neutral V8 state machine.

The adapter treats a pending stop entry as a *bar-level gap fill* when the
next completed one-minute bar opens through its trigger.  Adverse distance is
measured from the un-slipped trigger:

* LONG: ``(bar_open - trigger) / trigger * 10_000`` bps;
* SHORT: ``(trigger - bar_open) / trigger * 10_000`` bps.

For a maximum-gap challenger, a fill is allowed when the distance is less
than or equal to the configured threshold.  A larger distance terminally
cancels that candidate after the completed bar is known, releases its setup
capacity, and lets the unchanged Stage-7 rank/backfill state machine allocate
from the next minute.  ``REJECT_ALL_GAP_FILLS`` also rejects an exact-open-at-
trigger gap fill; this is the only intentional difference from ``MAX_0_BPS``.

All generated artifacts are research-only and promotion-ineligible.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import math
import shutil
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v10_backtest_config as locked_config
import fno_v10_experiment_backtest as experiment
import fno_v10_experiment_config as experiment_config
import fno_v8_windowed_1m_entry_backtest as engine


IST = ZoneInfo("Asia/Kolkata")
SCHEMA_VERSION = "fno_v10_stage7_gap_guard_research_v1"
STRATEGY_ID = "V10_STAGE7_0940_LONG_MOVE_040"
DEFAULT_OUTPUT_ROOT = (
    common.FNO_ROOT / "strategy_research" / "v10_stage7_gap_guard_research_v1"
)

DEFAULT_HISTORICAL_CACHE_MANIFEST = Path(
    "C:\\TradingData\\eqidv2\\fno_oi\\strategy_research\\"
    "v10_stage1_isolated_experiments_v1\\cache\\7a5fbbe68381a0fe\\manifest.json"
)
DEFAULT_HISTORICAL_REFERENCE_AUDIT = Path(
    "C:\\TradingData\\eqidv2\\fno_oi\\strategy_research\\"
    "v10_stage1_isolated_experiments_v1\\runs\\"
    "fno_v8_0940_long_move_040_20260827T095803334178+0530_1f7a35d5bb00\\"
    "candidate_order_audit.csv"
)
DEFAULT_TODAY_CACHE_MANIFEST = Path(
    "C:\\TradingData\\eqidv2\\fno_oi\\strategy_research\\"
    "today_v6_v10_stage7_20260827\\v10_stage7\\cache\\"
    "5eff5080751ac93d\\manifest.json"
)
DEFAULT_TODAY_REFERENCE_AUDIT = Path(
    "C:\\TradingData\\eqidv2\\fno_oi\\strategy_research\\"
    "today_v6_v10_stage7_20260827\\v10_stage7\\runs\\"
    "fno_v8_0940_long_move_040_20260827T210349262849+0530_c60e7dc230cb\\"
    "candidate_order_audit.csv"
)

REFERENCE_COST_BPS = 15.0
REFERENCE_SLIPPAGE_BPS = 0.0
SPLIT_DAY = date(2026, 8, 6)
HISTORICAL_FROM_DAY = date(2026, 5, 27)
HISTORICAL_THROUGH_DAY = date(2026, 8, 19)
TODAY_DAY = date(2026, 8, 27)


@dataclass(frozen=True)
class GapGuardSpec:
    variant: str
    max_adverse_gap_bps: float | None
    reject_all_gap_fills: bool = False

    @property
    def is_control(self) -> bool:
        return self.max_adverse_gap_bps is None and not self.reject_all_gap_fills

    def validate(self) -> None:
        if self.is_control:
            return
        if self.reject_all_gap_fills:
            if self.max_adverse_gap_bps is not None:
                raise ValueError("reject-all guard cannot also have a threshold")
            return
        if self.max_adverse_gap_bps is None:
            raise ValueError("threshold guard requires max_adverse_gap_bps")
        value = float(self.max_adverse_gap_bps)
        if not math.isfinite(value) or value < 0:
            raise ValueError("maximum adverse gap must be finite and non-negative")


GAP_GUARDS: tuple[GapGuardSpec, ...] = (
    GapGuardSpec("CONTROL", None),
    GapGuardSpec("MAX_0_BPS", 0.0),
    GapGuardSpec("MAX_2_BPS", 2.0),
    GapGuardSpec("MAX_5_BPS", 5.0),
    GapGuardSpec("REJECT_ALL_GAP_FILLS", None, reject_all_gap_fills=True),
)

COST_SCENARIOS: tuple[tuple[str, float, float], ...] = (
    ("REFERENCE_15_0", 15.0, 0.0),
    ("STRESS_20_2", 20.0, 2.0),
    ("STRESS_25_5", 25.0, 5.0),
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_ready(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value.resolve())
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not math.isfinite(float(value)) else float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value is pd.NA:
        return None
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_ready(payload), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def adverse_gap_bps(side: str, bar_open: float, trigger: float) -> float | None:
    """Return side-aware gap-through-trigger distance, otherwise ``None``."""

    selected_side = str(side).upper().strip()
    opening = float(bar_open)
    stop = float(trigger)
    if not math.isfinite(opening) or not math.isfinite(stop) or stop <= 0:
        raise ValueError("bar open and trigger must be finite; trigger must be positive")
    if selected_side == "LONG":
        return (opening - stop) / stop * 10_000.0 if opening >= stop else None
    if selected_side == "SHORT":
        return (stop - opening) / stop * 10_000.0 if opening <= stop else None
    raise ValueError(f"unsupported side: {side!r}")


def gap_is_rejected(spec: GapGuardSpec, gap_bps: float) -> bool:
    spec.validate()
    if spec.is_control:
        return False
    if spec.reject_all_gap_fills:
        return True
    assert spec.max_adverse_gap_bps is not None
    return float(gap_bps) > float(spec.max_adverse_gap_bps) + 1e-12


@contextlib.contextmanager
def installed_gap_guard(spec: GapGuardSpec) -> Iterator[None]:
    """Install a process-local terminal gap rejection around the state machine."""

    spec.validate()
    original_entry_fill = engine._entry_fill
    original_invalidation = engine._postconfirmation_invalidated
    original_transition = engine._CandidateRuntime.transition
    original_audit_record = engine._audit_record
    rejected_candidates: set[int] = set()

    def guarded_entry_fill(
        setup: engine.V8Setup,
        runtime: engine._CandidateRuntime,
        bar: engine.MinuteBar,
        policy: engine.EntryPolicy,
    ) -> tuple[float, bool] | None:
        fill = original_entry_fill(setup, runtime, bar, policy)
        if fill is None:
            return None
        entry_price, is_gap_fill = fill
        if not is_gap_fill:
            return fill
        assert runtime.trigger is not None
        distance = adverse_gap_bps(setup.side, float(bar.open), float(runtime.trigger))
        if distance is None:
            raise AssertionError("neutral engine labelled a non-gap as a gap fill")
        runtime._gap_guard_observed = True
        runtime._gap_guard_bar_open = float(bar.open)
        runtime._gap_guard_trigger = float(runtime.trigger)
        runtime._gap_guard_adverse_bps = float(distance)
        runtime._gap_guard_event_ts = bar.ts
        runtime._gap_guard_rejected = gap_is_rejected(spec, distance)
        if not runtime._gap_guard_rejected:
            return entry_price, is_gap_fill
        rejected_candidates.add(id(runtime.candidate))
        return None

    def guarded_invalidation(
        setup: engine.V8Setup,
        candidate: engine.CandidateInput,
        bar: engine.MinuteBar,
    ) -> bool:
        if id(candidate) in rejected_candidates:
            return True
        return original_invalidation(setup, candidate, bar)

    def guarded_transition(
        runtime: engine._CandidateRuntime,
        new_state: engine.SignalState,
        *,
        event_ts: pd.Timestamp,
        reason: str,
    ) -> None:
        effective_reason = reason
        if (
            new_state == engine.SignalState.POSTCONF_CANCELLED
            and bool(getattr(runtime, "_gap_guard_rejected", False))
        ):
            effective_reason = "ADVERSE_GAP_GUARD_REJECTED"
        original_transition(
            runtime,
            new_state,
            event_ts=event_ts,
            reason=effective_reason,
        )

    def guarded_audit_record(
        setup: engine.V8Setup, runtime: engine._CandidateRuntime
    ) -> dict[str, Any]:
        record = original_audit_record(setup, runtime)
        record.update(
            {
                "gap_guard_variant": spec.variant,
                "gap_guard_max_adverse_bps": spec.max_adverse_gap_bps,
                "gap_guard_reject_all": spec.reject_all_gap_fills,
                "gap_guard_observed": bool(
                    getattr(runtime, "_gap_guard_observed", False)
                ),
                "gap_guard_rejected": bool(
                    getattr(runtime, "_gap_guard_rejected", False)
                ),
                "gap_guard_bar_open": getattr(
                    runtime, "_gap_guard_bar_open", None
                ),
                "gap_guard_trigger": getattr(runtime, "_gap_guard_trigger", None),
                "gap_guard_adverse_bps": getattr(
                    runtime, "_gap_guard_adverse_bps", None
                ),
                "gap_guard_event_ts": getattr(
                    runtime, "_gap_guard_event_ts", pd.NaT
                ),
            }
        )
        return record

    engine._entry_fill = guarded_entry_fill
    engine._postconfirmation_invalidated = guarded_invalidation
    engine._CandidateRuntime.transition = guarded_transition
    engine._audit_record = guarded_audit_record
    try:
        yield
    finally:
        engine._entry_fill = original_entry_fill
        engine._postconfirmation_invalidated = original_invalidation
        engine._CandidateRuntime.transition = original_transition
        engine._audit_record = original_audit_record


def _artifact_from_manifest(
    manifest: Mapping[str, Any], name: str, manifest_path: Path
) -> Path:
    record = dict(dict(manifest.get("artifacts", {})).get(name, {}))
    path = Path(str(record.get("path", ""))).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"cache artifact missing: {name}: {path}")
    observed_size = path.stat().st_size
    observed_sha = _sha256_file(path)
    if observed_size != int(record.get("size", -1)):
        raise AssertionError(f"cache artifact size changed: {name}")
    if observed_sha != str(record.get("sha256", "")):
        raise AssertionError(f"cache artifact hash changed: {name}")
    if manifest_path.resolve() == path:
        raise AssertionError("manifest cannot point to itself as a cache artifact")
    return path


@dataclass(frozen=True)
class DatasetBundle:
    name: str
    manifest_path: Path
    manifest: Mapping[str, Any]
    candidates: pd.DataFrame
    minute_paths: pd.DataFrame
    sessions: tuple[date, ...]
    reference_audit_path: Path

    @property
    def source_complete(self) -> bool:
        return bool(self.manifest.get("headline_source_complete", False))

    @property
    def incomplete_symbol_sessions(self) -> int:
        return int(self.manifest.get("source_incomplete_symbol_sessions", 0))

    @property
    def unexpected_symbol_sessions(self) -> int:
        return int(self.manifest.get("unexpected_source_symbol_sessions", 0))


def load_dataset(
    name: str, manifest_path: Path, reference_audit_path: Path
) -> DatasetBundle:
    manifest_path = manifest_path.expanduser().resolve()
    reference_audit_path = reference_audit_path.expanduser().resolve()
    if not manifest_path.is_file():
        raise FileNotFoundError(f"cache manifest does not exist: {manifest_path}")
    if not reference_audit_path.is_file():
        raise FileNotFoundError(f"reference audit does not exist: {reference_audit_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    candidates_path = _artifact_from_manifest(manifest, "candidates", manifest_path)
    paths_path = _artifact_from_manifest(manifest, "paths", manifest_path)
    candidates = pd.read_parquet(candidates_path)
    minute_paths = pd.read_parquet(paths_path)
    sessions = tuple(
        sorted(engine._parse_day(value) for value in manifest.get("session_dates", []))
    )
    if not sessions:
        raise AssertionError(f"{name} cache has no official sessions")
    if len(candidates) != int(manifest.get("candidate_count", -1)):
        raise AssertionError(f"{name} candidate cache row count changed")
    if len(minute_paths) != int(manifest.get("path_row_count", -1)):
        raise AssertionError(f"{name} minute-path cache row count changed")
    return DatasetBundle(
        name=name,
        manifest_path=manifest_path,
        manifest=manifest,
        candidates=candidates,
        minute_paths=minute_paths,
        sessions=sessions,
        reference_audit_path=reference_audit_path,
    )


def _entry_policy(cost_bps: float, slippage_bps: float) -> engine.EntryPolicy:
    return experiment._entry_policy_for_variant(
        locked_config.ACTIVE_VARIANT,
        cost_bps=float(cost_bps),
        slippage_bps=float(slippage_bps),
        square_off="15:30",
        eod_policy="LAST_REAL_BAR_SENSITIVITY",
    )


def replay_dataset(
    bundle: DatasetBundle,
    spec: GapGuardSpec,
    *,
    cost_bps: float,
    slippage_bps: float,
) -> pd.DataFrame:
    policy = _entry_policy(cost_bps, slippage_bps)
    with installed_gap_guard(spec):
        audit = engine.run_v8_backtest(
            bundle.candidates,
            bundle.minute_paths,
            variant=locked_config.ACTIVE_VARIANT,
            policy=policy,
            target_exposure_per_entry_rs=50_000.0,
        )
    audit = audit.copy()
    audit["research_variant"] = spec.variant
    audit["research_cost_bps"] = float(cost_bps)
    audit["research_slippage_bps"] = float(slippage_bps)
    audit["research_only"] = True
    audit["promotion_eligible"] = False
    return audit


def _closed_mask(audit: pd.DataFrame) -> pd.Series:
    if audit.empty:
        return pd.Series(False, index=audit.index, dtype=bool)
    return (
        audit["filled"].fillna(False).astype(bool)
        & np.isfinite(pd.to_numeric(audit["net_return_pct"], errors="coerce"))
        & np.isfinite(pd.to_numeric(audit["net_pnl_rs"], errors="coerce"))
    )


def metric_row(
    audit: pd.DataFrame,
    session_dates: Iterable[date],
    *,
    dataset: str,
    period: str,
    scenario: str,
    spec: GapGuardSpec,
    cost_bps: float,
    slippage_bps: float,
) -> tuple[dict[str, Any], pd.DataFrame]:
    sessions = tuple(sorted(set(session_dates)))
    summary, daily = engine.summarize_v8_results(
        audit,
        session_dates=sessions,
        split_day=None,
        eod_policy="LAST_REAL_BAR_SENSITIVITY",
        source_complete=False,
    )
    diagnostic = dict(summary["diagnostic_closed_trade_metrics"])
    closed = audit.loc[_closed_mask(audit)].copy()
    returns = pd.to_numeric(closed.get("net_return_pct"), errors="coerce")
    gap_fills = (
        closed["gap_fill"].fillna(False).astype(bool)
        if "gap_fill" in closed
        else pd.Series(False, index=closed.index, dtype=bool)
    )
    rejections = (
        audit["gap_guard_rejected"].fillna(False).astype(bool)
        if "gap_guard_rejected" in audit
        else pd.Series(False, index=audit.index, dtype=bool)
    )
    row = {
        "dataset": dataset,
        "period": period,
        "scenario": scenario,
        "variant": spec.variant,
        "max_adverse_gap_bps": spec.max_adverse_gap_bps,
        "reject_all_gap_fills": spec.reject_all_gap_fills,
        "cost_bps": float(cost_bps),
        "slippage_bps": float(slippage_bps),
        "sessions": len(sessions),
        "candidates": len(audit),
        "fills": len(closed),
        "wins": int(returns.gt(0).sum()),
        "losses": int(returns.lt(0).sum()),
        "flat_trades": int(returns.eq(0).sum()),
        "win_rate_pct": (
            float(returns.gt(0).mean() * 100.0) if len(returns) else math.nan
        ),
        "profit_factor": diagnostic.get("profit_factor"),
        "net_return_points": diagnostic.get("net_return_percentage_points"),
        "net_pnl_rs": diagnostic.get("net_pnl_rs"),
        "max_daily_drawdown_points": diagnostic.get(
            "max_daily_drawdown_percentage_points"
        ),
        "positive_days": int(daily["net_return_pct"].gt(0).sum()),
        "negative_days": int(daily["net_return_pct"].lt(0).sum()),
        "flat_days": int(daily["net_return_pct"].eq(0).sum()),
        "remaining_gap_fills": int(gap_fills.sum()),
        "guard_rejections": int(rejections.sum()),
        "data_incomplete_candidates": int(
            audit["status"].eq(engine.SignalState.DATA_INCOMPLETE.value).sum()
        ),
        "headline_valid": False,
        "research_only": True,
        "promotion_eligible": False,
    }
    daily = daily.copy()
    daily["dataset"] = dataset
    daily["period_label"] = period
    daily["scenario"] = scenario
    daily["variant"] = spec.variant
    daily["cumulative_net_return_points"] = daily["net_return_pct"].cumsum()
    return row, daily


PARITY_COLUMNS = (
    "candidate_id",
    "status",
    "reason",
    "filled",
    "entry_time",
    "entry_price",
    "stop_price",
    "target_price",
    "exit_time",
    "exit_price",
    "exit_reason",
    "gross_return_pct",
    "net_return_pct",
    "quantity",
    "gross_pnl_rs",
    "estimated_cost_rs",
    "net_pnl_rs",
)


def validate_control_parity(
    observed: pd.DataFrame, reference_path: Path
) -> dict[str, Any]:
    reference = pd.read_csv(reference_path)
    missing = sorted(set(PARITY_COLUMNS) - set(observed.columns) - set(reference.columns))
    if missing:
        raise AssertionError(f"parity columns unavailable: {missing}")
    left = observed[list(PARITY_COLUMNS)].copy().sort_values("candidate_id")
    right = reference[list(PARITY_COLUMNS)].copy().sort_values("candidate_id")
    left = left.reset_index(drop=True)
    right = right.reset_index(drop=True)
    if left["candidate_id"].astype(str).tolist() != right["candidate_id"].astype(str).tolist():
        raise AssertionError("control candidate IDs differ from frozen reference")
    mismatches: dict[str, int] = {}
    numeric_columns = {
        "entry_price",
        "stop_price",
        "target_price",
        "exit_price",
        "gross_return_pct",
        "net_return_pct",
        "quantity",
        "gross_pnl_rs",
        "estimated_cost_rs",
        "net_pnl_rs",
    }
    for column in PARITY_COLUMNS:
        if column == "candidate_id":
            continue
        if column in numeric_columns:
            lnum = pd.to_numeric(left[column], errors="coerce").to_numpy(float)
            rnum = pd.to_numeric(right[column], errors="coerce").to_numpy(float)
            equal = np.isclose(lnum, rnum, rtol=0.0, atol=1e-9, equal_nan=True)
        else:
            ltxt = (
                left[column]
                .fillna("<NA>")
                .astype(str)
                .replace({"": "<NA>"})
                .to_numpy()
            )
            rtxt = (
                right[column]
                .fillna("<NA>")
                .astype(str)
                .replace({"": "<NA>"})
                .to_numpy()
            )
            equal = ltxt == rtxt
        count = int((~equal).sum())
        if count:
            mismatches[column] = count
    if mismatches:
        raise AssertionError(f"control parity failed: {mismatches}")
    return {
        "passed": True,
        "candidate_rows": len(left),
        "columns_compared": list(PARITY_COLUMNS),
        "reference_path": str(reference_path.resolve()),
        "reference_sha256": _sha256_file(reference_path),
    }


def _safe_name(value: str) -> str:
    return str(value).lower().replace("+", "plus").replace(" ", "_")


def _write_run_artifacts(
    run_dir: Path,
    audit: pd.DataFrame,
    daily: pd.DataFrame,
    summary: Mapping[str, Any],
) -> None:
    run_dir.mkdir(parents=True, exist_ok=False)
    audit.to_parquet(run_dir / "candidate_order_audit.parquet", index=False)
    audit.loc[_closed_mask(audit)].to_csv(run_dir / "closed_trades.csv", index=False)
    daily.to_csv(run_dir / "daily.csv", index=False)
    _write_json(run_dir / "summary.json", summary)


def paired_candidate_comparison(
    control: pd.DataFrame,
    challenger: pd.DataFrame,
    *,
    dataset: str,
    variant: str,
) -> pd.DataFrame:
    columns = [
        "candidate_id",
        "session_date",
        "setup_id",
        "side",
        "symbol",
        "status",
        "filled",
        "gap_fill",
        "entry_price",
        "net_return_pct",
        "net_pnl_rs",
        "gap_guard_rejected",
        "gap_guard_adverse_bps",
    ]
    left = control[[column for column in columns if column in control]].copy()
    right = challenger[[column for column in columns if column in challenger]].copy()
    paired = left.merge(
        right,
        on="candidate_id",
        how="outer",
        suffixes=("_control", "_challenger"),
        validate="one_to_one",
    )
    control_filled = paired["filled_control"].fillna(False).astype(bool)
    challenger_filled = paired["filled_challenger"].fillna(False).astype(bool)
    paired["fill_change"] = np.select(
        [
            control_filled & ~challenger_filled,
            ~control_filled & challenger_filled,
            control_filled & challenger_filled,
        ],
        ["REMOVED_FILL", "ADDED_FILL", "RETAINED_FILL"],
        default="NONFILL",
    )
    paired["net_return_delta_points"] = (
        pd.to_numeric(paired.get("net_return_pct_challenger"), errors="coerce").fillna(0.0)
        - pd.to_numeric(paired.get("net_return_pct_control"), errors="coerce").fillna(0.0)
    )
    paired["net_pnl_delta_rs"] = (
        pd.to_numeric(paired.get("net_pnl_rs_challenger"), errors="coerce").fillna(0.0)
        - pd.to_numeric(paired.get("net_pnl_rs_control"), errors="coerce").fillna(0.0)
    )
    paired["dataset"] = dataset
    paired["challenger_variant"] = variant
    return paired


def _metric_delta_table(summary: pd.DataFrame) -> pd.DataFrame:
    keys = ["dataset", "period", "scenario"]
    control = summary.loc[summary["variant"].eq("CONTROL")].copy()
    metrics = [
        "fills",
        "wins",
        "losses",
        "win_rate_pct",
        "profit_factor",
        "net_return_points",
        "net_pnl_rs",
        "max_daily_drawdown_points",
        "remaining_gap_fills",
        "guard_rejections",
    ]
    baseline = control[keys + metrics].rename(
        columns={metric: f"control_{metric}" for metric in metrics}
    )
    challengers = summary.loc[~summary["variant"].eq("CONTROL")].copy()
    compared = challengers.merge(baseline, on=keys, how="left", validate="many_to_one")
    for metric in metrics:
        compared[f"delta_{metric}"] = (
            pd.to_numeric(compared[metric], errors="coerce")
            - pd.to_numeric(compared[f"control_{metric}"], errors="coerce")
        )
    return compared


def _format_number(value: Any, digits: int = 3) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if not math.isfinite(parsed):
        return "n/a"
    return f"{parsed:.{digits}f}"


def _markdown_table(frame: pd.DataFrame, columns: Sequence[str]) -> list[str]:
    selected = frame[list(columns)].copy()
    rows = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for record in selected.to_dict("records"):
        values: list[str] = []
        for column in columns:
            value = record[column]
            if isinstance(value, (float, np.floating)):
                values.append(_format_number(value))
            else:
                values.append(str(value))
        rows.append("| " + " | ".join(values) + " |")
    return rows


def build_report(
    output_dir: Path,
    summary: pd.DataFrame,
    train_test: pd.DataFrame,
    parity: Mapping[str, Any],
) -> str:
    reference = summary.loc[
        summary["scenario"].eq("REFERENCE_15_0") & summary["period"].eq("FULL")
    ].copy()
    historical = reference.loc[reference["dataset"].eq("HISTORICAL")]
    today = reference.loc[reference["dataset"].eq("TODAY")]
    historical = historical.sort_values("variant", kind="stable")
    today = today.sort_values("variant", kind="stable")
    lines = [
        "# V10 Stage 7 adverse-gap guard research",
        "",
        "Research-only causal replay. Frozen/live files were not modified.",
        "",
        "## Historical reference economics (59 sessions, 15 bps / 0 slippage)",
        "",
    ]
    table_columns = [
        "variant",
        "fills",
        "wins",
        "losses",
        "win_rate_pct",
        "profit_factor",
        "net_return_points",
        "net_pnl_rs",
        "max_daily_drawdown_points",
        "remaining_gap_fills",
        "guard_rejections",
    ]
    lines.extend(_markdown_table(historical, table_columns))
    lines.extend(
        [
            "",
            "## Today (2026-08-27, last-real-bar sensitivity)",
            "",
        ]
    )
    lines.extend(_markdown_table(today, table_columns))
    lines.extend(
        [
            "",
            "## Train/test reference economics",
            "",
        ]
    )
    train_reference = train_test.loc[
        train_test["scenario"].eq("REFERENCE_15_0")
    ].sort_values(["period", "variant"], kind="stable")
    lines.extend(
        _markdown_table(
            train_reference,
            [
                "period",
                "variant",
                "sessions",
                "fills",
                "win_rate_pct",
                "profit_factor",
                "net_return_points",
                "max_daily_drawdown_points",
            ],
        )
    )
    lines.extend(
        [
            "",
            "## Guard definition",
            "",
            "- LONG adverse gap bps = `(bar open - trigger) / trigger * 10,000`.",
            "- SHORT adverse gap bps = `(trigger - bar open) / trigger * 10,000`.",
            "- Threshold variants allow distance `<=` their threshold and terminally reject larger gaps.",
            "- Reject-all rejects every bar-open-through-trigger fill, including an exact trigger open.",
            "- Rejection releases setup capacity after that completed one-minute bar; ordinary Stage-7 reranking/backfill then continues from the next minute.",
            "",
            "## Integrity",
            "",
            f"- Historical control parity: `{bool(parity['HISTORICAL']['passed'])}`.",
            f"- Today control parity: `{bool(parity['TODAY']['passed'])}`.",
            "- Stage-7 `09:40_LONG >= 0.40%` overlay remained active in every run.",
            "- Outputs are diagnostic because source coverage is incomplete and EOD uses last-real-bar sensitivity.",
            "- `research_only=true`; `promotion_eligible=false`.",
            "",
            f"Package: `{output_dir}`",
            "",
        ]
    )
    return "\n".join(lines)


def _inventory_files(root: Path, *, exclude: set[Path] | None = None) -> list[dict[str, Any]]:
    excluded = {path.resolve() for path in (exclude or set())}
    rows: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.resolve() in excluded:
            continue
        rows.append(
            {
                "relative_path": path.relative_to(root).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
        )
    return rows


def run_research(args: argparse.Namespace) -> Path:
    locked_config.validate_locked_profile()
    experiment_config.validate_registry()
    if locked_config.ACTIVE_VARIANT != "0940_LONG_MOVE_040":
        raise AssertionError("this research requires locked V10 Stage 7")
    spec = experiment_config.get_spec(locked_config.ACTIVE_VARIANT)
    if spec.price_threshold_overrides != (("09:40_LONG", 0.40),):
        raise AssertionError("Stage-7 selection overlay changed")

    historical = load_dataset(
        "HISTORICAL",
        args.historical_cache_manifest,
        args.historical_reference_audit,
    )
    today = load_dataset(
        "TODAY", args.today_cache_manifest, args.today_reference_audit
    )
    if historical.sessions != tuple(
        day.date() for day in pd.bdate_range(HISTORICAL_FROM_DAY, HISTORICAL_THROUGH_DAY)
        if day.date() in set(historical.sessions)
    ):
        # The cache calendar is authoritative (exchange holidays are not plain
        # weekdays); only exact endpoints/count are enforced below.
        pass
    if historical.sessions[0] != HISTORICAL_FROM_DAY:
        raise AssertionError("historical cache start date changed")
    if historical.sessions[-1] != HISTORICAL_THROUGH_DAY:
        raise AssertionError("historical cache end date changed")
    if len(historical.sessions) != 59:
        raise AssertionError("historical cache must contain 59 official sessions")
    if today.sessions != (TODAY_DAY,):
        raise AssertionError("today cache is not the frozen 2026-08-27 snapshot")

    experiment.configure_engine(locked_config.ACTIVE_VARIANT)
    timestamp = datetime.now(IST).strftime("%Y%m%dT%H%M%S%f%z")
    output_dir = args.output_root.expanduser().resolve() / "runs" / f"gap_guard_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=False)

    source_archive = output_dir / "source"
    source_archive.mkdir(parents=True, exist_ok=False)
    source_files = [
        Path(__file__).resolve(),
        Path(engine.__file__).resolve(),
        Path(experiment.__file__).resolve(),
        Path(experiment_config.__file__).resolve(),
        Path(locked_config.__file__).resolve(),
    ]
    for path in source_files:
        shutil.copy2(path, source_archive / path.name)

    scenarios = COST_SCENARIOS if not args.skip_cost_stress else COST_SCENARIOS[:1]
    summary_rows: list[dict[str, Any]] = []
    daily_parts: list[pd.DataFrame] = []
    train_test_rows: list[dict[str, Any]] = []
    paired_parts: list[pd.DataFrame] = []
    parity: dict[str, Any] = {}
    reference_audits: dict[tuple[str, str], pd.DataFrame] = {}

    for bundle in (historical, today):
        for scenario_name, cost_bps, slippage_bps in scenarios:
            control_audit: pd.DataFrame | None = None
            for guard in GAP_GUARDS:
                print(
                    f"[GAP-GUARD] dataset={bundle.name} scenario={scenario_name} "
                    f"variant={guard.variant}",
                    flush=True,
                )
                audit = replay_dataset(
                    bundle,
                    guard,
                    cost_bps=cost_bps,
                    slippage_bps=slippage_bps,
                )
                row, daily = metric_row(
                    audit,
                    bundle.sessions,
                    dataset=bundle.name,
                    period="FULL",
                    scenario=scenario_name,
                    spec=guard,
                    cost_bps=cost_bps,
                    slippage_bps=slippage_bps,
                )
                summary_rows.append(row)
                daily_parts.append(daily)
                run_dir = (
                    output_dir
                    / "variants"
                    / bundle.name.lower()
                    / _safe_name(scenario_name)
                    / _safe_name(guard.variant)
                )
                _write_run_artifacts(run_dir, audit, daily, row)

                if scenario_name == "REFERENCE_15_0":
                    reference_audits[(bundle.name, guard.variant)] = audit
                    if guard.is_control:
                        parity[bundle.name] = validate_control_parity(
                            audit, bundle.reference_audit_path
                        )
                    if bundle.name == "HISTORICAL":
                        train_days = tuple(day for day in bundle.sessions if day < SPLIT_DAY)
                        test_days = tuple(day for day in bundle.sessions if day >= SPLIT_DAY)
                        for period, days in (("TRAIN", train_days), ("TEST", test_days)):
                            subset = audit.loc[audit["session_date"].isin(days)].copy()
                            split_row, _ = metric_row(
                                subset,
                                days,
                                dataset=bundle.name,
                                period=period,
                                scenario=scenario_name,
                                spec=guard,
                                cost_bps=cost_bps,
                                slippage_bps=slippage_bps,
                            )
                            train_test_rows.append(split_row)

                if guard.is_control:
                    control_audit = audit
                else:
                    if control_audit is None:
                        raise AssertionError("control must run before challengers")
                    if scenario_name == "REFERENCE_15_0":
                        paired_parts.append(
                            paired_candidate_comparison(
                                control_audit,
                                audit,
                                dataset=bundle.name,
                                variant=guard.variant,
                            )
                        )

    summary = pd.DataFrame(summary_rows)
    daily_all = pd.concat(daily_parts, ignore_index=True)
    train_test = pd.DataFrame(train_test_rows)
    deltas = _metric_delta_table(summary)
    paired = pd.concat(paired_parts, ignore_index=True)

    summary.to_csv(output_dir / "all_results_summary.csv", index=False)
    train_test.to_csv(output_dir / "historical_train_test.csv", index=False)
    deltas.to_csv(output_dir / "paired_summary_vs_control.csv", index=False)
    paired.to_csv(output_dir / "paired_candidates_vs_control.csv", index=False)
    daily_all.to_csv(output_dir / "daywise.csv", index=False)

    today_trades = pd.concat(
        [
            audit.loc[_closed_mask(audit)].assign(guard_variant=variant)
            for (dataset, variant), audit in reference_audits.items()
            if dataset == "TODAY"
        ],
        ignore_index=True,
    )
    today_trades.to_csv(output_dir / "today_trades.csv", index=False)

    report = build_report(output_dir, summary, train_test, parity)
    (output_dir / "report.md").write_text(report, encoding="utf-8")

    source_inputs = {}
    for bundle in (historical, today):
        source_inputs[bundle.name] = {
            "cache_manifest": str(bundle.manifest_path),
            "cache_manifest_sha256": _sha256_file(bundle.manifest_path),
            "cache_input_fingerprint": bundle.manifest.get("input_fingerprint"),
            "session_dates": [day.isoformat() for day in bundle.sessions],
            "candidate_count": len(bundle.candidates),
            "minute_path_rows": len(bundle.minute_paths),
            "headline_source_complete": bundle.source_complete,
            "source_incomplete_symbol_sessions": bundle.incomplete_symbol_sessions,
            "reference_audit": str(bundle.reference_audit_path),
            "reference_audit_sha256": _sha256_file(bundle.reference_audit_path),
        }

    provenance_path = output_dir / "provenance.json"
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "created_at_ist": datetime.now(IST),
        "strategy_id": STRATEGY_ID,
        "locked_stage7_profile_id": locked_config.PROFILE_ID,
        "locked_stage7_profile_sha256": locked_config.profile_sha256(),
        "locked_variant": locked_config.ACTIVE_VARIANT,
        "locked_variant_config_sha256": locked_config.EXPECTED_VARIANT_CONFIG_SHA256,
        "selection_overlay": {
            "setup_id": "09:40_LONG",
            "price_change_pct_min": 0.40,
            "other_selection_changes": False,
        },
        "gap_definition": {
            "long_bps": "(BAR_OPEN-TRIGGER)/TRIGGER*10000",
            "short_bps": "(TRIGGER-BAR_OPEN)/TRIGGER*10000",
            "threshold_comparison": "ALLOW_IF_DISTANCE_LE_THRESHOLD",
            "reject_all_exact_trigger_open": True,
            "trigger_basis": "UNSLIPPED_TRIGGER",
            "evaluation": "FIRST_COMPLETED_1M_BAR_OPEN_THROUGH_PENDING_STOP",
            "rejection": "TERMINAL_POSTCONF_CANCELLED",
            "capacity_release": "AFTER_COMPLETED_REJECTION_BAR",
            "backfill": "UNCHANGED_STAGE7_RERANK;NEXT_MINUTE_EARLIEST_FILL",
        },
        "variants": [asdict(item) for item in GAP_GUARDS],
        "cost_scenarios": [
            {"scenario": name, "cost_bps": cost, "slippage_bps": slip}
            for name, cost, slip in scenarios
        ],
        "split_day": SPLIT_DAY,
        "source_inputs": source_inputs,
        "control_parity": parity,
        "source_archives": [
            {
                "path": str((source_archive / path.name).resolve()),
                "sha256": _sha256_file(source_archive / path.name),
            }
            for path in source_files
        ],
        "limitations": [
            "STATIC_LATER_DATED_UNIVERSE",
            "STATIC_AUGUST_FUTURES_OI_NOT_ROLLING_POINT_IN_TIME",
            "LEGACY_EQUITY_ROW_LINEAGE_UNPROVEN",
            "GLOBAL_PORTFOLIO_LEDGER_USES_CONSERVATIVE_NO_BACKFILL_OVERLAY",
            "UPSTREAM_SOURCE_SLOT_COVERAGE_INCOMPLETE",
            "LAST_REAL_BAR_SENSITIVITY_NOT_HEADLINE",
            "MULTIPLE_VARIANTS_REQUIRE_UNTOUCHED_PROSPECTIVE_VALIDATION",
        ],
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }
    _write_json(provenance_path, provenance)
    inventory_path = output_dir / "artifact_inventory.json"
    _write_json(
        inventory_path,
        {
            "schema_version": SCHEMA_VERSION,
            "artifacts": _inventory_files(
                output_dir, exclude={inventory_path, provenance_path}
            ),
        },
    )
    provenance["artifact_inventory"] = {
        "path": str(inventory_path.resolve()),
        "sha256": _sha256_file(inventory_path),
    }
    _write_json(provenance_path, provenance)

    latest = args.output_root.expanduser().resolve() / "latest.json"
    _write_json(
        latest,
        {
            "schema_version": SCHEMA_VERSION,
            "run_dir": str(output_dir),
            "provenance_sha256": _sha256_file(provenance_path),
            "report_sha256": _sha256_file(output_dir / "report.md"),
            "research_only": True,
            "promotion_eligible": False,
        },
    )
    print(f"[GAP-GUARD] complete: {output_dir}", flush=True)
    return output_dir


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run isolated V10 Stage-7 adverse-gap challengers"
    )
    parser.add_argument(
        "--historical-cache-manifest",
        type=Path,
        default=DEFAULT_HISTORICAL_CACHE_MANIFEST,
    )
    parser.add_argument(
        "--historical-reference-audit",
        type=Path,
        default=DEFAULT_HISTORICAL_REFERENCE_AUDIT,
    )
    parser.add_argument(
        "--today-cache-manifest",
        type=Path,
        default=DEFAULT_TODAY_CACHE_MANIFEST,
    )
    parser.add_argument(
        "--today-reference-audit",
        type=Path,
        default=DEFAULT_TODAY_REFERENCE_AUDIT,
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--skip-cost-stress", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_research(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
