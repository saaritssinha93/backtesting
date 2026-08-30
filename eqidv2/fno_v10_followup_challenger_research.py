"""Isolated follow-up challengers for the locked F&O V10 Stage-7 backtest.

This module never edits or writes into the frozen V10, Stage-7, or live roots.
It consumes their immutable candidate/path caches and source snapshots, then
runs the unchanged chronological V8/V10 state machine after applying exactly
one declared challenger to the Stage-7 control.

The previous-ten-minute features are causal.  For a confirmation candle ending
at T, the denominator uses the ten consecutive completed candles T-10m..T-1m
from the same session.  The current candle is never included.  Missing,
non-consecutive, invalid, or zero-median history fails closed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v10_experiment_backtest as experiment
import fno_v8_windowed_1m_entry_backtest as engine


SCHEMA_VERSION = "fno_v10_stage7_followup_challengers_v1"
FEATURE_SCHEMA_VERSION = "fno_v10_previous10_completed_1m_features_v1"
SELECTION_SCHEMA_VERSION = "fno_v10_stage7_followup_selection_v1"
ROOT = (
    common.FNO_ROOT
    / "strategy_research"
    / "v10_stage7_followup_challengers_v1"
)

HISTORICAL_CACHE_MANIFEST = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\v10_stage1_isolated_experiments_v1\cache\7a5fbbe68381a0fe"
    r"\manifest.json"
)
HISTORICAL_SNAPSHOT_MANIFEST = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\v8_windowed_strict_v1\snapshots"
    r"\snapshot_20260820T124734626995+0530_mnofor_c\manifest.json"
)
TODAY_CACHE_MANIFEST = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\today_v6_v10_stage7_20260827\v10_stage7\cache"
    r"\5eff5080751ac93d\manifest.json"
)
TODAY_SNAPSHOT_MANIFEST = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\today_v6_v10_stage7_20260827\snapshots"
    r"\snapshot_20260827T205346985816+0530_5mypchpj\manifest.json"
)


@dataclass(frozen=True)
class ChallengerSpec:
    variant: str
    description: str
    move_0935_long_max: float | None = None
    body_0925_long_min: float | None = None
    previous10_volume_ratio_min: float | None = None
    previous10_range_ratio_min: float | None = None

    def validate(self) -> None:
        mechanisms = sum(
            value is not None
            for value in (
                self.move_0935_long_max,
                self.body_0925_long_min,
                self.previous10_volume_ratio_min,
                self.previous10_range_ratio_min,
            )
        )
        if self.variant == "STAGE7_CONTROL":
            if mechanisms:
                raise AssertionError("Stage-7 control cannot contain a challenger")
        elif mechanisms != 1:
            raise AssertionError(
                f"Challenger {self.variant} must contain exactly one mechanism"
            )
        for value in (
            self.move_0935_long_max,
            self.body_0925_long_min,
            self.previous10_volume_ratio_min,
            self.previous10_range_ratio_min,
        ):
            if value is not None and (
                not math.isfinite(float(value)) or float(value) <= 0
            ):
                raise AssertionError(f"Invalid threshold for {self.variant}")
        if self.body_0925_long_min is not None and not (
            0.0 <= float(self.body_0925_long_min) <= 1.0
        ):
            raise AssertionError("Body ratio threshold must be in [0, 1]")

    def payload(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def uses_previous10(self) -> bool:
        return (
            self.previous10_volume_ratio_min is not None
            or self.previous10_range_ratio_min is not None
        )


SPECS: tuple[ChallengerSpec, ...] = (
    ChallengerSpec(
        "STAGE7_CONTROL",
        "Locked Stage 7: 09:40 LONG five-minute move >= 0.40%",
    ),
    ChallengerSpec(
        "0935_LONG_MOVE_MAX_040",
        "Stage 7 plus 09:35 LONG five-minute move <= 0.40%",
        move_0935_long_max=0.40,
    ),
    ChallengerSpec(
        "0935_LONG_MOVE_MAX_050",
        "Stage 7 plus 09:35 LONG five-minute move <= 0.50%",
        move_0935_long_max=0.50,
    ),
    ChallengerSpec(
        "0935_LONG_MOVE_MAX_060",
        "Stage 7 plus 09:35 LONG five-minute move <= 0.60%",
        move_0935_long_max=0.60,
    ),
    ChallengerSpec(
        "0925_LONG_BODY_MIN_050",
        "Stage 7 plus 09:25 LONG confirmation candle body/range >= 0.50",
        body_0925_long_min=0.50,
    ),
    ChallengerSpec(
        "PREV10_VOLUME_RATIO_MIN_100",
        "Stage 7 plus confirmation volume / prior-10 one-minute median >= 1.00",
        previous10_volume_ratio_min=1.00,
    ),
    ChallengerSpec(
        "PREV10_VOLUME_RATIO_MIN_125",
        "Stage 7 plus confirmation volume / prior-10 one-minute median >= 1.25",
        previous10_volume_ratio_min=1.25,
    ),
    ChallengerSpec(
        "PREV10_RANGE_RATIO_MIN_100",
        "Stage 7 plus confirmation range / prior-10 one-minute median >= 1.00",
        previous10_range_ratio_min=1.00,
    ),
    ChallengerSpec(
        "PREV10_RANGE_RATIO_MIN_125",
        "Stage 7 plus confirmation range / prior-10 one-minute median >= 1.25",
        previous10_range_ratio_min=1.25,
    ),
)
SPEC_BY_NAME = {spec.variant: spec for spec in SPECS}


@dataclass(frozen=True)
class DatasetContract:
    label: str
    cache_manifest: Path
    snapshot_manifest: Path
    split_day: str | None


_ACTIVE_SPEC = SPEC_BY_NAME["STAGE7_CONTROL"]
_FEATURE_LOOKUP: dict[tuple[str, pd.Timestamp], dict[str, Any]] = {}
_NEUTRAL_CONFIRMATION_CHECK = experiment._NEUTRAL_CONFIRMATION_CHECK


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def artifact_record(path: Path) -> dict[str, Any]:
    return {
        "path": str(path.resolve()),
        "bytes": int(path.stat().st_size),
        "sha256": sha256_file(path),
    }


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp")
    temp.write_text(
        json.dumps(value, indent=2, ensure_ascii=True, allow_nan=False),
        encoding="utf-8",
        newline="\n",
    )
    os.replace(temp, path)


def atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp")
    frame.to_csv(temp, index=False)
    os.replace(temp, path)


def _to_ist(value: Any) -> pd.Timestamp:
    return engine._to_ist_timestamp(value)


def validate_registry() -> None:
    if len(SPEC_BY_NAME) != len(SPECS):
        raise AssertionError("Challenger names must be unique")
    expected = {
        "STAGE7_CONTROL",
        "0935_LONG_MOVE_MAX_040",
        "0935_LONG_MOVE_MAX_050",
        "0935_LONG_MOVE_MAX_060",
        "0925_LONG_BODY_MIN_050",
        "PREV10_VOLUME_RATIO_MIN_100",
        "PREV10_VOLUME_RATIO_MIN_125",
        "PREV10_RANGE_RATIO_MIN_100",
        "PREV10_RANGE_RATIO_MIN_125",
    }
    if set(SPEC_BY_NAME) != expected:
        raise AssertionError("Predeclared challenger registry changed")
    for spec in SPECS:
        spec.validate()


def selection_overlay(
    candidates: pd.DataFrame, spec: ChallengerSpec
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply Stage 7 and, if declared, the 09:35 upper ceiling, then rerank."""

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
        raise ValueError(f"Candidate cache missing columns: {missing}")
    base = candidates.copy()
    if base["candidate_id"].duplicated().any():
        raise AssertionError("Candidate IDs must be unique")
    reasons = pd.Series("PASSED", index=base.index, dtype=object)
    move = pd.to_numeric(base["price_change_pct"], errors="coerce")
    stage7_rejected = (
        base["setup_id"].astype(str).eq("09:40_LONG")
        & move.add(1e-12).lt(0.40)
    )
    reasons.loc[stage7_rejected] = "STAGE7_0940_LONG_MOVE_BELOW_040"
    if spec.move_0935_long_max is not None:
        rejected = (
            reasons.eq("PASSED")
            & base["setup_id"].astype(str).eq("09:35_LONG")
            & move.sub(1e-12).gt(float(spec.move_0935_long_max))
        )
        reasons.loc[rejected] = "0935_LONG_MOVE_ABOVE_CHALLENGER_MAX"

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
    decisions = base.copy()
    decisions = decisions.rename(columns={"frozen_rank": "original_frozen_rank"})
    decisions["recalculated_frozen_rank"] = decisions["candidate_id"].map(rank_map)
    decisions["selection_passed"] = passed.to_numpy(dtype=bool)
    decisions["selection_reason"] = reasons.to_numpy(dtype=object)
    decisions["variant"] = spec.variant
    decisions["stage7_0940_long_move_min"] = 0.40
    decisions["challenger_0935_long_move_max"] = spec.move_0935_long_max
    decisions["variant_config_sha256"] = canonical_sha256(spec.payload())
    decisions["schema_version"] = SELECTION_SCHEMA_VERSION
    return filtered, decisions


def _valid_source_bar(row: Mapping[str, Any]) -> bool:
    values = np.asarray(
        [row["open"], row["high"], row["low"], row["close"], row["volume"]],
        dtype=float,
    )
    return bool(
        np.isfinite(values).all()
        and (values[:4] > 0).all()
        and values[1] >= max(values[0], values[3])
        and values[2] <= min(values[0], values[3])
        and values[1] >= values[2]
        and values[4] >= 0
    )


def _snapshot_equity_files(snapshot: Mapping[str, Any]) -> dict[str, Path]:
    records = snapshot.get("captures", [])
    result: dict[str, Path] = {}
    for record in records:
        if str(record.get("role", "")) != "NSE_EQUITY_1M":
            continue
        symbol = str(record.get("logical_symbol", "")).upper().strip()
        path = Path(str(record.get("snapshot_path", "")))
        if symbol and path.is_file():
            result[symbol] = path
    if result:
        return result
    root = Path(str(snapshot.get("equity_1m_root", "")))
    if root.is_dir():
        suffix = "_stocks_indicators_1min.parquet"
        for path in root.glob(f"*{suffix}"):
            result[path.name[: -len(suffix)].upper()] = path
    return result


def build_previous10_features(
    *,
    candidates: pd.DataFrame,
    minute_paths: pd.DataFrame,
    snapshot_manifest: Path,
    output_dir: Path,
    from_day: str,
    through_day: str,
) -> tuple[pd.DataFrame, Path, Path]:
    """Build a causal feature sidecar for every possible confirmation candle."""

    snapshot = json.loads(snapshot_manifest.read_text(encoding="utf-8"))
    equity_files = _snapshot_equity_files(snapshot)
    setup_max = {
        setup.setup_id: int(
            setup.entry_conf_minute
            if setup.entry_conf_minute is not None
            else 1
        )
        for setup in engine.ACTIVE_SETUPS
    }
    paths = minute_paths.copy()
    paths["minute_index"] = pd.to_numeric(paths["minute_index"], errors="raise")
    paths = paths.loc[
        paths["minute_index"]
        <= paths["setup_id"].astype(str).map(setup_max).astype(int)
    ].copy()
    paths["bar_ts"] = paths["bar_ts"].map(_to_ist)
    targets = (
        paths[
            [
                "symbol",
                "bar_ts",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]
        ]
        .drop_duplicates(["symbol", "bar_ts"])
        .sort_values(["symbol", "bar_ts"], kind="stable")
    )
    records: list[dict[str, Any]] = []
    used_sources: list[dict[str, Any]] = []
    start = pd.Timestamp(from_day, tz=common.IST)
    stop = pd.Timestamp(through_day, tz=common.IST) + pd.Timedelta(days=1)
    for symbol, group in targets.groupby("symbol", sort=True):
        symbol = str(symbol).upper()
        source_path = equity_files.get(symbol)
        if source_path is None:
            for target in group.to_dict("records"):
                records.append(
                    _unavailable_feature_record(
                        symbol, target["bar_ts"], "SOURCE_EQUITY_FILE_MISSING"
                    )
                )
            continue
        source = pd.read_parquet(
            source_path,
            columns=["date", "open", "high", "low", "close", "volume"],
            filters=[
                [
                    ("date", ">=", start.to_pydatetime()),
                    ("date", "<", stop.to_pydatetime()),
                ]
            ],
        ).rename(columns={"date": "bar_ts"})
        source["bar_ts"] = source["bar_ts"].map(_to_ist)
        source = source.sort_values("bar_ts", kind="stable")
        if source["bar_ts"].duplicated().any():
            duplicates = source.loc[source["bar_ts"].duplicated(), "bar_ts"].head()
            raise AssertionError(f"Duplicate source bars for {symbol}: {duplicates.tolist()}")
        indexed = source.set_index("bar_ts")
        used_sources.append(
            {
                "symbol": symbol,
                "path": str(source_path.resolve()),
                "snapshot_manifest_bound": True,
            }
        )
        for target in group.to_dict("records"):
            target_ts = _to_ist(target["bar_ts"])
            expected = pd.DatetimeIndex(
                [target_ts - pd.Timedelta(minutes=value) for value in range(10, 0, -1)]
            )
            if target_ts not in indexed.index:
                records.append(
                    _unavailable_feature_record(
                        symbol, target_ts, "CURRENT_SOURCE_BAR_MISSING"
                    )
                )
                continue
            current = indexed.loc[target_ts]
            cached_current = np.asarray(
                [target[name] for name in ("open", "high", "low", "close", "volume")],
                dtype=float,
            )
            source_current = np.asarray(
                [current[name] for name in ("open", "high", "low", "close", "volume")],
                dtype=float,
            )
            if not np.allclose(
                cached_current,
                source_current,
                rtol=0.0,
                atol=1e-6,
                equal_nan=False,
            ):
                raise AssertionError(
                    f"Snapshot/cache current-bar mismatch for {symbol} {target_ts}"
                )
            if not expected.isin(indexed.index).all():
                records.append(
                    _unavailable_feature_record(
                        symbol, target_ts, "PRIOR10_NOT_CONSECUTIVE"
                    )
                )
                continue
            prior = indexed.loc[expected]
            if not all(_valid_source_bar(row) for row in prior.to_dict("records")):
                records.append(
                    _unavailable_feature_record(
                        symbol, target_ts, "PRIOR10_INVALID_BAR"
                    )
                )
                continue
            volume_median = float(pd.to_numeric(prior["volume"]).median())
            ranges = pd.to_numeric(prior["high"]) - pd.to_numeric(prior["low"])
            range_median = float(ranges.median())
            if not math.isfinite(volume_median) or volume_median <= 0:
                records.append(
                    _unavailable_feature_record(
                        symbol, target_ts, "PRIOR10_VOLUME_MEDIAN_NONPOSITIVE"
                    )
                )
                continue
            if not math.isfinite(range_median) or range_median <= 0:
                records.append(
                    _unavailable_feature_record(
                        symbol, target_ts, "PRIOR10_RANGE_MEDIAN_NONPOSITIVE"
                    )
                )
                continue
            current_volume = float(current["volume"])
            current_range = float(current["high"] - current["low"])
            records.append(
                {
                    "symbol": symbol,
                    "bar_ts": target_ts,
                    "prior_start_ts": expected[0],
                    "prior_end_ts": expected[-1],
                    "prior_count": 10,
                    "prior_volume_median": volume_median,
                    "prior_range_median": range_median,
                    "current_volume": current_volume,
                    "current_range": current_range,
                    "previous10_volume_ratio": current_volume / volume_median,
                    "previous10_range_ratio": current_range / range_median,
                    "feature_available": True,
                    "unavailable_reason": "",
                    "schema_version": FEATURE_SCHEMA_VERSION,
                }
            )
    frame = pd.DataFrame(records).sort_values(
        ["symbol", "bar_ts"], kind="stable"
    ).reset_index(drop=True)
    if len(frame) != len(targets):
        raise AssertionError("Previous-10 sidecar cardinality changed")
    if frame.duplicated(["symbol", "bar_ts"]).any():
        raise AssertionError("Previous-10 sidecar keys must be unique")
    output_dir.mkdir(parents=True, exist_ok=True)
    table_path = output_dir / "previous10_completed_1m_features.parquet"
    frame.to_parquet(table_path, index=False)
    manifest_path = output_dir / "manifest.json"
    available = frame["feature_available"].fillna(False).astype(bool)
    manifest = {
        "schema_version": FEATURE_SCHEMA_VERSION,
        "complete": True,
        "causal_contract": {
            "current_confirmation_candle_excluded_from_denominator": True,
            "prior_window": "T_MINUS_10_MINUTES_THROUGH_T_MINUS_1_MINUTE",
            "same_session_only": True,
            "exact_consecutive_end_labels_required": True,
            "unavailable_policy": "FAIL_CLOSED",
        },
        "from_day": from_day,
        "through_day": through_day,
        "source_snapshot_manifest": artifact_record(snapshot_manifest),
        "source_snapshot_fingerprint": snapshot.get("snapshot_fingerprint"),
        "source_files": used_sources,
        "rows": int(len(frame)),
        "available_rows": int(available.sum()),
        "unavailable_rows": int((~available).sum()),
        "unavailable_reason_counts": {
            str(key): int(value)
            for key, value in frame.loc[~available, "unavailable_reason"]
            .value_counts()
            .items()
        },
        "table": artifact_record(table_path),
        "research_only": True,
        "promotion_eligible": False,
    }
    atomic_json(manifest_path, manifest)
    return frame, table_path, manifest_path


def _unavailable_feature_record(
    symbol: str, target_ts: pd.Timestamp, reason: str
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "bar_ts": _to_ist(target_ts),
        "prior_start_ts": pd.NaT,
        "prior_end_ts": pd.NaT,
        "prior_count": 0,
        "prior_volume_median": math.nan,
        "prior_range_median": math.nan,
        "current_volume": math.nan,
        "current_range": math.nan,
        "previous10_volume_ratio": math.nan,
        "previous10_range_ratio": math.nan,
        "feature_available": False,
        "unavailable_reason": reason,
        "schema_version": FEATURE_SCHEMA_VERSION,
    }


def feature_lookup(frame: pd.DataFrame) -> dict[tuple[str, pd.Timestamp], dict[str, Any]]:
    return {
        (str(row["symbol"]).upper(), _to_ist(row["bar_ts"])): row
        for row in frame.to_dict("records")
    }


def challenger_confirmation_check(
    setup: engine.V8Setup,
    candidate: engine.CandidateInput,
    bar: engine.MinuteBar,
    policy: engine.EntryPolicy | None = None,
) -> dict[str, Any]:
    """Apply one declared challenger after the frozen structural confirmation."""

    record = _NEUTRAL_CONFIRMATION_CHECK(setup, candidate, bar, policy)
    rejection_codes = list(record.get("rejection_codes", []))
    record["challenger_variant"] = _ACTIVE_SPEC.variant
    if (
        _ACTIVE_SPEC.body_0925_long_min is not None
        and setup.setup_id == "09:25_LONG"
    ):
        threshold = float(_ACTIVE_SPEC.body_0925_long_min)
        record["challenger_body_ratio_min"] = threshold
        value = record.get("body_ratio")
        if value is None or not math.isfinite(float(value)):
            rejection_codes.append("CHALLENGER_BODY_RATIO_UNAVAILABLE")
        elif float(value) + 1e-12 < threshold:
            rejection_codes.append("CHALLENGER_BODY_RATIO_BELOW_MINIMUM")

    if _ACTIVE_SPEC.uses_previous10:
        feature = _FEATURE_LOOKUP.get((candidate.symbol.upper(), bar.ts))
        available = bool(feature and feature.get("feature_available"))
        record["previous10_feature_available"] = available
        record["previous10_feature_unavailable_reason"] = (
            "" if available else str((feature or {}).get("unavailable_reason", "MISSING_SIDECAR_KEY"))
        )
        for name in (
            "prior_count",
            "prior_volume_median",
            "prior_range_median",
            "previous10_volume_ratio",
            "previous10_range_ratio",
        ):
            record[name] = (feature or {}).get(name)
        if not available:
            rejection_codes.append("PREVIOUS10_FEATURE_UNAVAILABLE")
        if _ACTIVE_SPEC.previous10_volume_ratio_min is not None and available:
            threshold = float(_ACTIVE_SPEC.previous10_volume_ratio_min)
            ratio = float(feature["previous10_volume_ratio"])
            record["previous10_volume_ratio_min"] = threshold
            if not math.isfinite(ratio) or ratio + 1e-12 < threshold:
                rejection_codes.append("PREVIOUS10_VOLUME_RATIO_BELOW_MINIMUM")
        if _ACTIVE_SPEC.previous10_range_ratio_min is not None and available:
            threshold = float(_ACTIVE_SPEC.previous10_range_ratio_min)
            ratio = float(feature["previous10_range_ratio"])
            record["previous10_range_ratio_min"] = threshold
            if not math.isfinite(ratio) or ratio + 1e-12 < threshold:
                rejection_codes.append("PREVIOUS10_RANGE_RATIO_BELOW_MINIMUM")

    record["rejection_codes"] = rejection_codes
    record["passed"] = not rejection_codes
    return record


def _resolved_check(checks: Any) -> Mapping[str, Any]:
    if not isinstance(checks, list) or not checks:
        return {}
    for check in checks:
        if isinstance(check, Mapping) and bool(check.get("passed")):
            return check
    last = checks[-1]
    return last if isinstance(last, Mapping) else {}


def _metric_row(
    audit: pd.DataFrame,
    *,
    label: str,
    variant: str,
    sessions: Sequence[date],
) -> dict[str, Any]:
    filled = audit["filled"].fillna(False).astype(bool)
    returns = pd.to_numeric(audit["net_return_pct"], errors="coerce")
    pnl = pd.to_numeric(audit["net_pnl_rs"], errors="coerce")
    closed = filled & np.isfinite(returns) & np.isfinite(pnl)
    closed_returns = returns.loc[closed]
    wins = int(closed_returns.gt(0).sum())
    losses = int(closed_returns.lt(0).sum())
    gains = float(closed_returns.loc[closed_returns.gt(0)].sum())
    loss = float(-closed_returns.loc[closed_returns.lt(0)].sum())
    daily_values = (
        pd.DataFrame({"session_date": list(sessions)})
        .merge(
            audit.loc[closed]
            .groupby("session_date", as_index=False)
            .agg(net_return_pct=("net_return_pct", "sum")),
            on="session_date",
            how="left",
        )["net_return_pct"]
        .fillna(0.0)
        .to_numpy(dtype=float)
    )
    cumulative = np.concatenate(([0.0], np.cumsum(daily_values)))
    drawdown = cumulative - np.maximum.accumulate(cumulative)
    return {
        "dataset": label,
        "variant": variant,
        "sessions": int(len(sessions)),
        "candidates": int(len(audit)),
        "confirmed": int(audit["confirmation_minute"].notna().sum()),
        "fills": int(filled.sum()),
        "closed_fills": int(closed.sum()),
        "wins": wins,
        "losses": losses,
        "win_rate_pct": 100.0 * wins / len(closed_returns) if len(closed_returns) else math.nan,
        "profit_factor": gains / loss if loss > 0 else math.inf if gains > 0 else math.nan,
        "net_return_points": float(closed_returns.sum()),
        "net_pnl_rs": float(pnl.loc[closed].sum()),
        "max_daily_drawdown_points": max(0.0, float(-drawdown.min())),
        "positive_days": int((daily_values > 0).sum()),
        "negative_days": int((daily_values < 0).sum()),
        "flat_days": int((daily_values == 0).sum()),
        "data_incomplete_candidates": int(
            audit["status"].astype(str).eq(engine.SignalState.DATA_INCOMPLETE.value).sum()
        ),
        "last_real_bar_sensitivity": True,
        "headline_valid": False,
        "research_only": True,
        "promotion_eligible": False,
    }


def _daily_frame(
    audit: pd.DataFrame, sessions: Sequence[date], split_day: str | None
) -> pd.DataFrame:
    filled = audit["filled"].fillna(False).astype(bool)
    returns = pd.to_numeric(audit["net_return_pct"], errors="coerce")
    pnl = pd.to_numeric(audit["net_pnl_rs"], errors="coerce")
    closed = filled & np.isfinite(returns) & np.isfinite(pnl)
    candidates = audit.groupby("session_date", as_index=False).agg(
        candidates=("candidate_id", "size")
    )
    trades = audit.loc[closed].groupby("session_date", as_index=False).agg(
        fills=("candidate_id", "size"),
        net_return_points=("net_return_pct", "sum"),
        net_pnl_rs=("net_pnl_rs", "sum"),
    )
    daily = pd.DataFrame({"session_date": list(sessions)})
    daily = daily.merge(candidates, on="session_date", how="left").merge(
        trades, on="session_date", how="left"
    )
    for column in ("candidates", "fills"):
        daily[column] = daily[column].fillna(0).astype(int)
    for column in ("net_return_points", "net_pnl_rs"):
        daily[column] = daily[column].fillna(0.0).astype(float)
    if split_day:
        split = engine._parse_day(split_day)
        daily["period"] = np.where(daily["session_date"].lt(split), "TRAIN", "TEST")
    else:
        daily["period"] = "TODAY"
    return daily


def _period_rows(daily: pd.DataFrame, variant: str, dataset: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for period, frame in daily.groupby("period", sort=False):
        returns = pd.to_numeric(frame["net_return_points"], errors="coerce")
        rows.append(
            {
                "dataset": dataset,
                "variant": variant,
                "period": str(period),
                "sessions": int(len(frame)),
                "fills": int(frame["fills"].sum()),
                "net_return_points": float(returns.sum()),
                "net_pnl_rs": float(pd.to_numeric(frame["net_pnl_rs"]).sum()),
                "positive_days": int(returns.gt(0).sum()),
                "negative_days": int(returns.lt(0).sum()),
            }
        )
    return rows


def _setup_summary(audit: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for setup_id, frame in audit.groupby("setup_id", sort=True):
        row = _metric_row(
            frame,
            label="SETUP",
            variant=str(setup_id),
            sessions=sorted(set(frame["session_date"])),
        )
        row["setup_id"] = setup_id
        rows.append(row)
    return pd.DataFrame(rows)


def _load_dataset(contract: DatasetContract) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, list[date]]:
    cache = json.loads(contract.cache_manifest.read_text(encoding="utf-8"))
    if not cache.get("complete"):
        raise AssertionError(f"Cache is not complete: {contract.cache_manifest}")
    snapshot = json.loads(contract.snapshot_manifest.read_text(encoding="utf-8"))
    cache_snapshot = str(cache.get("input_contract", {}).get("snapshot_fingerprint", ""))
    if cache_snapshot != str(snapshot.get("snapshot_fingerprint", "")):
        raise AssertionError(f"Cache/snapshot fingerprint mismatch for {contract.label}")
    artifacts = cache["artifacts"]
    candidate_path = Path(artifacts["candidates"]["path"])
    minute_path = Path(artifacts["paths"]["path"])
    if sha256_file(candidate_path) != artifacts["candidates"]["sha256"]:
        raise AssertionError("Candidate cache hash changed")
    if sha256_file(minute_path) != artifacts["paths"]["sha256"]:
        raise AssertionError("Minute-path cache hash changed")
    candidates = pd.read_parquet(candidate_path)
    minute_paths = pd.read_parquet(minute_path)
    sessions = [engine._parse_day(value) for value in cache["session_dates"]]
    return cache, candidates, minute_paths, sessions


def run_dataset(
    contract: DatasetContract,
    *,
    run_stamp: str,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]], dict[str, Path]]:
    global _ACTIVE_SPEC, _FEATURE_LOOKUP

    cache, candidates, minute_paths, sessions = _load_dataset(contract)
    from_day = str(cache["input_contract"]["from_day"])
    through_day = str(cache["input_contract"]["through_day"])
    feature_dir = ROOT / "feature_cache" / str(cache["input_fingerprint"])[:16]
    features, feature_table, feature_manifest = build_previous10_features(
        candidates=candidates,
        minute_paths=minute_paths,
        snapshot_manifest=contract.snapshot_manifest,
        output_dir=feature_dir,
        from_day=from_day,
        through_day=through_day,
    )
    _FEATURE_LOOKUP = feature_lookup(features)

    summary_rows: list[dict[str, Any]] = []
    period_rows: list[dict[str, Any]] = []
    audit_by_variant: dict[str, pd.DataFrame] = {}
    provenance_paths: dict[str, Path] = {}
    experiment.configure_engine("0940_LONG_MOVE_040")
    policy = experiment._entry_policy_for_variant(
        "0940_LONG_MOVE_040",
        cost_bps=15.0,
        slippage_bps=0.0,
        square_off="15:30",
        eod_policy="LAST_REAL_BAR_SENSITIVITY",
    )
    engine._confirmation_check = challenger_confirmation_check

    for spec in SPECS:
        _ACTIVE_SPEC = spec
        filtered, decisions = selection_overlay(candidates, spec)
        audit = experiment._NEUTRAL_RUN_BACKTEST(
            filtered,
            minute_paths,
            variant=spec.variant,
            policy=policy,
        )
        audit = audit.copy()
        audit["challenger_variant"] = spec.variant
        audit["challenger_config_sha256"] = canonical_sha256(spec.payload())
        resolved = audit["confirmation_checks"].map(_resolved_check)
        for column in (
            "challenger_body_ratio_min",
            "previous10_feature_available",
            "previous10_feature_unavailable_reason",
            "prior_count",
            "prior_volume_median",
            "prior_range_median",
            "previous10_volume_ratio",
            "previous10_range_ratio",
            "previous10_volume_ratio_min",
            "previous10_range_ratio_min",
        ):
            audit[column] = resolved.map(lambda value, key=column: value.get(key))
        metric = _metric_row(
            audit,
            label=contract.label,
            variant=spec.variant,
            sessions=sessions,
        )
        daily = _daily_frame(audit, sessions, contract.split_day)
        period_rows.extend(_period_rows(daily, spec.variant, contract.label))
        summary_rows.append(metric)
        audit_by_variant[spec.variant] = audit

        run_dir = ROOT / "runs" / contract.label / f"{spec.variant.lower()}_{run_stamp}"
        run_dir.mkdir(parents=True, exist_ok=False)
        audit_path = run_dir / "candidate_order_audit.csv"
        decisions_path = run_dir / "selection_decisions.csv"
        daily_path = run_dir / "daily.csv"
        setup_path = run_dir / "setup_summary.csv"
        summary_path = run_dir / "summary.json"
        source_archive = run_dir / Path(__file__).name
        atomic_csv(audit, audit_path)
        atomic_csv(decisions, decisions_path)
        atomic_csv(daily, daily_path)
        atomic_csv(_setup_summary(audit), setup_path)
        atomic_json(summary_path, metric)
        shutil.copy2(Path(__file__), source_archive)
        outputs = {
            "candidate_order_audit": artifact_record(audit_path),
            "selection_decisions": artifact_record(decisions_path),
            "daily": artifact_record(daily_path),
            "setup_summary": artifact_record(setup_path),
            "summary": artifact_record(summary_path),
            "runner_source_archive": artifact_record(source_archive),
            "previous10_feature_table": artifact_record(feature_table),
            "previous10_feature_manifest": artifact_record(feature_manifest),
        }
        provenance = {
            "schema_version": SCHEMA_VERSION,
            "complete": True,
            "dataset": contract.label,
            "variant": spec.payload(),
            "variant_config_sha256": canonical_sha256(spec.payload()),
            "stage7_base": {
                "variant": "0940_LONG_MOVE_040",
                "price_change_0940_long_min": 0.40,
                "other_five_minute_and_entry_parameters_changed": False,
            },
            "execution": {
                "full_chronological_state_machine_replay": True,
                "cost_bps": 15.0,
                "slippage_bps": 0.0,
                "square_off": "15:30",
                "eod_policy": "LAST_REAL_BAR_SENSITIVITY",
                "same_confirmation_bar_fill": False,
                "same_bar_exit_policy": "STOP_FIRST",
                "portfolio_mode": engine.PORTFOLIO_MODE,
            },
            "window": {
                "from_day": from_day,
                "through_day": through_day,
                "split_day": contract.split_day,
                "sessions": len(sessions),
            },
            "inputs": {
                "cache_manifest": artifact_record(contract.cache_manifest),
                "cache_input_fingerprint": cache["input_fingerprint"],
                "candidate_cache": cache["artifacts"]["candidates"],
                "minute_path_cache": cache["artifacts"]["paths"],
                "snapshot_manifest": artifact_record(contract.snapshot_manifest),
                "snapshot_fingerprint": cache["input_contract"]["snapshot_fingerprint"],
            },
            "feature_contract": json.loads(feature_manifest.read_text(encoding="utf-8"))[
                "causal_contract"
            ],
            "results": metric,
            "outputs": outputs,
            "known_limitations": [
                "STATIC_26AUG_FUTURES_UNIVERSE_NOT_POINT_IN_TIME_ROLLING",
                "UPSTREAM_SYMBOL_SESSION_COVERAGE_INCOMPLETE",
                "LAST_REAL_BAR_SENSITIVITY_NOT_HEADLINE",
            ],
            "research_only": True,
            "promotion_eligible": False,
            "live_or_paper_authority": False,
        }
        provenance_path = run_dir / "provenance.json"
        atomic_json(provenance_path, provenance)
        provenance_paths[spec.variant] = provenance_path

    control = audit_by_variant["STAGE7_CONTROL"]
    _assert_control_anchor(contract.label, control)
    return (
        pd.DataFrame(summary_rows),
        pd.DataFrame(period_rows),
        _pairwise_rows(audit_by_variant, contract.label),
        provenance_paths,
    )


def _assert_control_anchor(label: str, audit: pd.DataFrame) -> None:
    metric = _metric_row(
        audit,
        label=label,
        variant="STAGE7_CONTROL",
        sessions=sorted(set(audit["session_date"])),
    )
    expected = (
        {
            "fills": 224,
            "wins": 110,
            "losses": 114,
            "net_return_points": 63.03417975487417,
            "profit_factor": 1.7001760098069238,
        }
        if label == "historical_59_sessions"
        else {
            "fills": 6,
            "wins": 2,
            "losses": 4,
        }
    )
    for key in ("fills", "wins", "losses"):
        if int(metric[key]) != int(expected[key]):
            raise AssertionError(
                f"{label} Stage-7 parity failed for {key}: {metric[key]}"
            )
    for key in ("net_return_points", "profit_factor"):
        if key in expected and not math.isclose(
            float(metric[key]), float(expected[key]), rel_tol=0.0, abs_tol=1e-10
        ):
            raise AssertionError(
                f"{label} Stage-7 parity failed for {key}: {metric[key]}"
            )


def _pairwise_rows(
    audits: Mapping[str, pd.DataFrame], dataset: str
) -> list[dict[str, Any]]:
    control = audits["STAGE7_CONTROL"]
    sessions = sorted(set(control["session_date"]))
    base = _metric_row(
        control,
        label=dataset,
        variant="STAGE7_CONTROL",
        sessions=sessions,
    )
    rows: list[dict[str, Any]] = []
    for variant, audit in audits.items():
        if variant == "STAGE7_CONTROL":
            continue
        value = _metric_row(
            audit, label=dataset, variant=variant, sessions=sessions
        )
        rows.append(
            {
                "dataset": dataset,
                "variant": variant,
                "delta_candidates": value["candidates"] - base["candidates"],
                "delta_confirmed": value["confirmed"] - base["confirmed"],
                "delta_fills": value["fills"] - base["fills"],
                "delta_wins": value["wins"] - base["wins"],
                "delta_losses": value["losses"] - base["losses"],
                "delta_win_rate_pct": value["win_rate_pct"] - base["win_rate_pct"],
                "delta_profit_factor": value["profit_factor"] - base["profit_factor"],
                "delta_net_return_points": value["net_return_points"] - base["net_return_points"],
                "delta_net_pnl_rs": value["net_pnl_rs"] - base["net_pnl_rs"],
                "delta_max_daily_drawdown_points": value["max_daily_drawdown_points"]
                - base["max_daily_drawdown_points"],
            }
        )
    return rows


def market_sector_feasibility(
    historical_snapshot_manifest: Path,
    today_snapshot_manifest: Path,
) -> dict[str, Any]:
    snapshots = []
    for label, path in (
        ("historical_59_sessions", historical_snapshot_manifest),
        ("today_2026_08_27", today_snapshot_manifest),
    ):
        payload = json.loads(path.read_text(encoding="utf-8"))
        roles = {
            str(record.get("role", ""))
            for record in payload.get("captures", [])
        }
        logical = {
            str(record.get("logical_symbol", "")).upper()
            for record in payload.get("captures", [])
        }
        desired = {
            "NIFTY",
            "NIFTY50",
            "NIFTYBEES",
            "BANKNIFTY",
            "BANKBEES",
            "NIFTYIT",
            "ITBEES",
            "NIFTYAUTO",
            "AUTOBEES",
            "NIFTYPHARMA",
            "PHARMABEES",
        }
        snapshots.append(
            {
                "dataset": label,
                "captured_roles": sorted(roles),
                "captured_market_or_sector_symbols": sorted(logical & desired),
                "point_in_time_market_sector_source_available": bool(logical & desired),
            }
        )
    feasible = all(
        item["point_in_time_market_sector_source_available"]
        for item in snapshots
    )
    return {
        "test": "COMPLETED_5M_MARKET_AND_SECTOR_ALIGNMENT",
        "feasible": feasible,
        "executed": False,
        "reason": (
            "NOT_EXECUTED_BECAUSE_IMMUTABLE_SNAPSHOTS_HAVE_NO_BOUND_INDEX_OR_"
            "SECTOR_1M_5M_SERIES"
            if not feasible
            else "AVAILABLE_BUT_NOT_PREDECLARED_IN_THIS_RUN"
        ),
        "snapshot_audit": snapshots,
        "mapping_warning": (
            "configs/sector_etf_map.json contains generic proxy assignments for "
            "several sectors and is not a frozen point-in-time research input."
        ),
        "research_only": True,
        "promotion_eligible": False,
    }


def run_all(args: argparse.Namespace) -> Path:
    validate_registry()
    stamp = pd.Timestamp.now(tz=common.IST).strftime("%Y%m%dT%H%M%S%f%z")
    contracts = (
        DatasetContract(
            "historical_59_sessions",
            args.historical_cache_manifest,
            args.historical_snapshot_manifest,
            "2026-08-06",
        ),
        DatasetContract(
            "today_2026_08_27",
            args.today_cache_manifest,
            args.today_snapshot_manifest,
            None,
        ),
    )
    summaries: list[pd.DataFrame] = []
    periods: list[pd.DataFrame] = []
    deltas: list[dict[str, Any]] = []
    provenance_index: list[dict[str, Any]] = []
    for contract in contracts:
        summary, period, pairwise, paths = run_dataset(contract, run_stamp=stamp)
        summaries.append(summary)
        periods.append(period)
        deltas.extend(pairwise)
        provenance_index.extend(
            {
                "dataset": contract.label,
                "variant": variant,
                "provenance_path": str(path.resolve()),
                "provenance_sha256": sha256_file(path),
            }
            for variant, path in paths.items()
        )

    comparison_dir = ROOT / "comparisons" / f"comparison_{stamp}"
    comparison_dir.mkdir(parents=True, exist_ok=False)
    summary_path = comparison_dir / "all_results_summary.csv"
    period_path = comparison_dir / "train_test_today_summary.csv"
    delta_path = comparison_dir / "stage7_pairwise_deltas.csv"
    provenance_index_path = comparison_dir / "run_provenance_index.csv"
    feasibility_path = comparison_dir / "market_sector_feasibility.json"
    registry_path = comparison_dir / "challenger_registry.json"
    atomic_csv(pd.concat(summaries, ignore_index=True), summary_path)
    atomic_csv(pd.concat(periods, ignore_index=True), period_path)
    atomic_csv(pd.DataFrame(deltas), delta_path)
    atomic_csv(pd.DataFrame(provenance_index), provenance_index_path)
    atomic_json(
        feasibility_path,
        market_sector_feasibility(
            args.historical_snapshot_manifest, args.today_snapshot_manifest
        ),
    )
    atomic_json(
        registry_path,
        {
            "schema_version": SCHEMA_VERSION,
            "registry_sha256": canonical_sha256([spec.payload() for spec in SPECS]),
            "variants": [spec.payload() for spec in SPECS],
            "research_only": True,
            "promotion_eligible": False,
        },
    )
    manifest_path = comparison_dir / "manifest.json"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "created_at_ist": pd.Timestamp.now(tz=common.IST).isoformat(),
        "runner_source": artifact_record(Path(__file__)),
        "outputs": {
            "all_results_summary": artifact_record(summary_path),
            "train_test_today_summary": artifact_record(period_path),
            "stage7_pairwise_deltas": artifact_record(delta_path),
            "run_provenance_index": artifact_record(provenance_index_path),
            "market_sector_feasibility": artifact_record(feasibility_path),
            "challenger_registry": artifact_record(registry_path),
        },
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }
    atomic_json(manifest_path, manifest)
    latest = ROOT / "latest_comparison.json"
    atomic_json(
        latest,
        {
            "comparison_manifest": str(manifest_path.resolve()),
            "comparison_manifest_sha256": sha256_file(manifest_path),
        },
    )
    print(f"[V10-FOLLOWUP] comparison={comparison_dir}")
    return comparison_dir


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--historical-cache-manifest", type=Path, default=HISTORICAL_CACHE_MANIFEST)
    value.add_argument("--historical-snapshot-manifest", type=Path, default=HISTORICAL_SNAPSHOT_MANIFEST)
    value.add_argument("--today-cache-manifest", type=Path, default=TODAY_CACHE_MANIFEST)
    value.add_argument("--today-snapshot-manifest", type=Path, default=TODAY_SNAPSHOT_MANIFEST)
    return value


def main(argv: Sequence[str] | None = None) -> int:
    run_all(parser().parse_args(argv))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
