"""Reconstruct and replay ONLY retained V13-V10-G for one explicit session.

Historical bars warm up causal indicators; candidates, selections, execution
paths and statistics are restricted to the requested date. This module does
not load frozen research signals, optimize parameters, fetch broker data or
run any other strategy. Stocks missing a required futures OI bar are excluded
and disclosed; other incomplete required-day coverage blocks publication.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
import fno_v13_v10_g_backtest as g
from fno_v13_v10_g_identity import SIGNAL_ID_SCHEMA_VERSION, canonical_signal_id
import fno_v13_v10_g_live_config as config
import fno_v13_v10_g_policy as policy
import fno_v13_v10_g_selection as promoted_selection
import fno_v13_v10_g_staged_replay as staged_replay
import fno_v13_v9_data as features
from ai_platform.observability.data_quality import (
    canonical_frame_sha256,
    canonical_payload_sha256,
    canonical_row_sha256,
    evaluate_ohlcv,
)
from ai_platform.observability.feature_ledger import (
    build_v13_v10_g_feature_ledger,
    write_feature_ledger,
)

SCHEMA_VERSION = "fno_v13_v10_g_daily_replay_v1"
SNAPSHOT_SCHEMA_VERSION = "fno_v13_v10_g_input_snapshot_v1"
STRATEGY = "V13-V10-G"
EXCLUDABLE_COVERAGE_REASONS = {"MISSING_SIGNAL_OR_PRIOR_FUTURES_OI_BAR"}
SNAPSHOT_COPY_ATTEMPTS = 3


@dataclass(frozen=True)
class DataRoots:
    universe: Path = field(default_factory=lambda: common.UNIVERSE_DIR)
    equity_1m: Path = field(default_factory=lambda: hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR)
    futures_5m: Path = field(default_factory=lambda: common.RAW_CONTRACT_DIR)


@dataclass(frozen=True)
class InputSnapshot:
    """A sealed set of replay data inputs and its content identity."""

    root: Path
    roots: DataRoots
    manifest_path: Path
    fingerprint: str
    sources: tuple[dict[str, Any], ...]
    problems: tuple[dict[str, Any], ...]


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, (date, datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value.resolve())
    if isinstance(value, np.generic):
        value = value.item()
    if value is pd.NA or value is pd.NaT or (isinstance(value, float) and not math.isfinite(value)):
        return None
    return value


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_identity(path: Path) -> tuple[int, int, int, int]:
    """Return fields that detect replacement as well as in-place mutation."""

    stat = path.stat()
    return (int(stat.st_dev), int(stat.st_ino), int(stat.st_size), int(stat.st_mtime_ns))


def _stable_snapshot_copy(
    source: Path,
    destination: Path,
    *,
    role: str,
    logical_path: str,
    attempts: int = SNAPSHOT_COPY_ATTEMPTS,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Copy one source only when one complete source generation was observed.

    Live producers generally replace or append Parquet files.  Merely copying a
    pathname can therefore preserve a torn generation.  We require the source
    identity to remain unchanged across the copy and both the copied bytes and
    the post-copy source bytes to hash identically.  A producer may change the
    original after this function returns; the replay is isolated from that
    change by the snapshot copy.
    """

    source = source.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    base = {
        "role": role,
        "original_path": str(source),
        "snapshot_relative_path": logical_path.replace("\\", "/"),
    }
    if not source.is_file():
        return (
            {**base, "exists": False, "captured": False, "sha256": None, "size_bytes": None},
            None,
        )

    last_detail = "source changed while it was being copied"
    for attempt in range(1, max(1, int(attempts)) + 1):
        temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.part")
        try:
            before = _file_identity(source)
            shutil.copyfile(source, temporary)
            copied_sha256 = _sha(temporary)
            source_sha256 = _sha(source)
            after = _file_identity(source)
            if before == after and copied_sha256 == source_sha256:
                os.replace(temporary, destination)
                return (
                    {
                        **base,
                        "exists": True,
                        "captured": True,
                        "sha256": copied_sha256,
                        "size_bytes": destination.stat().st_size,
                        "capture_attempt": attempt,
                    },
                    None,
                )
            last_detail = (
                "source identity or content changed during copy "
                f"(attempt {attempt}/{attempts})"
            )
        except OSError as exc:
            last_detail = f"{type(exc).__name__}: {exc} (attempt {attempt}/{attempts})"
        finally:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass

    record = {
        **base,
        "exists": True,
        "captured": False,
        "sha256": None,
        "size_bytes": None,
    }
    problem = {
        "symbol": source.name,
        "reason": "SOURCE_SNAPSHOT_UNSTABLE",
        "path": str(source),
        "detail": last_detail,
    }
    return record, problem


def _snapshot_fingerprint(day: date, sources: list[dict[str, Any]]) -> str:
    """Hash content and logical roles, never staging/output locations."""

    identity = [
        {
            **{
                key: source.get(key)
                for key in (
                    "role",
                    "snapshot_relative_path",
                    "exists",
                    "captured",
                    "sha256",
                    "size_bytes",
                )
            },
            # Captured objects are addressed by content.  An unresolved member
            # has no content identity, so retain its authority path to avoid
            # conflating failures from unrelated source roots.
            "unresolved_original_path": (
                None if source.get("captured") else source.get("original_path")
            ),
        }
        for source in sources
    ]
    return common.canonical_json_sha256({
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "session_date": day.isoformat(),
        "sources": identity,
    })


def _verify_input_snapshot(root: Path, manifest: dict[str, Any]) -> None:
    """Reject a reused snapshot if any captured object was altered."""

    expected = str(manifest.get("snapshot_fingerprint", ""))
    actual = _snapshot_fingerprint(
        date.fromisoformat(str(manifest["session_date"])), list(manifest.get("sources", []))
    )
    if not expected or expected != actual:
        raise ValueError("Input snapshot manifest fingerprint mismatch")
    root = root.resolve()
    for source in manifest.get("sources", []):
        path = (root / str(source["snapshot_relative_path"])).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise ValueError("Input snapshot member escapes snapshot root") from exc
        if not source.get("captured"):
            continue
        if not path.is_file() or _sha(path) != source.get("sha256"):
            raise ValueError(f"Input snapshot member failed verification: {path.name}")


def create_input_snapshot(
    day: date,
    snapshot_base: Path,
    *,
    roots: DataRoots | None = None,
) -> InputSnapshot:
    """Capture all data inputs needed by one replay into a sealed snapshot.

    Discovery is performed from the already captured dated universe, never
    from a second read of its mutable original.  A failed or missing member is
    represented in the manifest and later blocks publication; it is never
    substituted with the live source.
    """

    if type(day) is not date:
        raise TypeError("An explicit datetime.date session is required")
    original = roots or DataRoots()
    base = Path(snapshot_base).resolve()
    base.mkdir(parents=True, exist_ok=True)
    staging = base / f".staging-{uuid4().hex}"
    staged_roots = DataRoots(
        staging / "universe",
        staging / "equity_1m",
        staging / "futures_5m",
    )
    for directory in (staged_roots.universe, staged_roots.equity_1m, staged_roots.futures_5m):
        directory.mkdir(parents=True, exist_ok=True)

    captured: list[dict[str, Any]] = []
    problems: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def capture(source: Path, destination: Path, role: str) -> None:
        logical = destination.relative_to(staging).as_posix()
        key = (str(source.resolve()), logical)
        if key in seen:
            return
        seen.add(key)
        record, problem = _stable_snapshot_copy(
            source, destination, role=role, logical_path=logical
        )
        captured.append(record)
        if problem is not None:
            problems.append(problem)

    universe_name = f"near_month_{day.isoformat()}.parquet"
    universe_source = original.universe / universe_name
    universe_snapshot = staged_roots.universe / universe_name
    capture(universe_source, universe_snapshot, "DATED_UNIVERSE")

    if universe_snapshot.is_file():
        try:
            full = pd.read_parquet(universe_snapshot)
            stocks = full.loc[~full.is_index_future.fillna(False).astype(bool)]
            expiry = pd.to_datetime(stocks.expiry, errors="coerce").dropna().unique()
            if len(expiry) != 1:
                raise ValueError("Dated stock universe must have exactly one expiry")
            month = pd.Timestamp(expiry[0]).strftime("%y%b").upper()
            mapped, _ = provenance.load_backtest_universe(
                universe_path=universe_snapshot,
                universe_date=day,
                contract_month_contains=month,
                require_persisted_mapping=True,
            )
            nifty = full.loc[full.underlying.astype(str).str.upper().eq("NIFTY")]
            if len(nifty) != 1:
                raise ValueError("Dated near-month NIFTY futures mapping is missing or ambiguous")
            nifty_symbol = str(nifty.iloc[0].tradingsymbol)
            nifty_name = f"{common.safe_contract_stem(nifty_symbol)}_5minute.parquet"
            capture(
                original.futures_5m / nifty_name,
                staged_roots.futures_5m / nifty_name,
                "NIFTY_FUTURES_CONTEXT",
            )
            for contract in mapped.to_dict("records"):
                requested_symbol = str(contract["equity_symbol"])
                equity_symbol = hybrid.resolve_backtest_equity_symbol(
                    requested_symbol, root=original.equity_1m
                )
                equity_name = f"{equity_symbol.upper()}_stocks_indicators_1min.parquet"
                future_name = (
                    f"{common.safe_contract_stem(str(contract['futures_tradingsymbol']))}"
                    "_5minute.parquet"
                )
                capture(
                    original.equity_1m / equity_name,
                    staged_roots.equity_1m / equity_name,
                    "EQUITY_ONE_MINUTE",
                )
                capture(
                    original.futures_5m / future_name,
                    staged_roots.futures_5m / future_name,
                    "STOCK_FUTURES_OI",
                )
        except (OSError, ValueError, KeyError, TypeError) as exc:
            problems.append({
                "symbol": "UNIVERSE",
                "reason": "SOURCE_SNAPSHOT_DISCOVERY_FAILED",
                "path": str(universe_source.resolve()),
                "detail": f"{type(exc).__name__}: {exc}",
            })

    fingerprint = _snapshot_fingerprint(day, captured)
    manifest = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "session_date": day.isoformat(),
        "snapshot_fingerprint": fingerprint,
        "complete": not problems and all(row.get("captured") for row in captured),
        "sources": captured,
        "problems": problems,
    }
    common.atomic_write_json(staging / "snapshot_manifest.json", _json_ready(manifest))
    final_root = base / fingerprint
    try:
        if final_root.exists():
            existing_path = final_root / "snapshot_manifest.json"
            existing = json.loads(existing_path.read_text(encoding="utf-8"))
            _verify_input_snapshot(final_root, existing)
            manifest = existing
            shutil.rmtree(staging, ignore_errors=True)
        else:
            os.replace(staging, final_root)
            _verify_input_snapshot(final_root, manifest)
        final_roots = DataRoots(
            final_root / "universe",
            final_root / "equity_1m",
            final_root / "futures_5m",
        )
        return InputSnapshot(
            root=final_root,
            roots=final_roots,
            manifest_path=final_root / "snapshot_manifest.json",
            fingerprint=str(manifest["snapshot_fingerprint"]),
            sources=tuple(manifest.get("sources", [])),
            problems=tuple(manifest.get("problems", [])),
        )
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


def _record_source(path: Path, role: str, sources: list[dict]) -> None:
    path = path.resolve()
    if any(row["path"] == str(path) for row in sources):
        return
    exists = path.is_file()
    sources.append(dict(path=str(path), role=role, exists=exists,
                        sha256=_sha(path) if exists else None,
                        size_bytes=path.stat().st_size if exists else None))


def _annotate_snapshot_sources(sources: list[dict], snapshot: InputSnapshot) -> None:
    """Attach original lineage without weakening snapshot-path verification."""

    lookup = {
        str((snapshot.root / str(row["snapshot_relative_path"])).resolve()): row
        for row in snapshot.sources
    }
    for source in sources:
        captured = lookup.get(str(Path(source["path"]).resolve()))
        if captured is None:
            continue
        source["original_path"] = captured.get("original_path")
        source["snapshot_relative_path"] = captured.get("snapshot_relative_path")
        source["snapshot_fingerprint"] = snapshot.fingerprint


def _replay_source_fingerprint(sources: list[dict], snapshot: InputSnapshot) -> str:
    """Build a location-independent identity for snapshot data plus code."""

    identities = []
    for source in sources:
        identities.append({
            "role": source.get("role"),
            "authority_path": source.get("original_path", source.get("path")),
            "exists": source.get("exists"),
            "sha256": source.get("sha256"),
            "size_bytes": source.get("size_bytes"),
        })
    return common.canonical_json_sha256({
        "input_snapshot_fingerprint": snapshot.fingerprint,
        "sources": identities,
    })


def _problem(problems: list, symbol: str, reason: str, **details) -> None:
    problems.append(dict(symbol=symbol, reason=reason, **_json_ready(details)))


def _telemetry_error(
    errors: list[dict[str, Any]] | None,
    *,
    day: date,
    component: str,
    phase: str,
    error: BaseException,
    symbol: str = "",
    source: str = "",
) -> dict[str, Any]:
    """Record an observability failure without changing replay authority."""

    row = {
        "schema_version": "v13_v10_g_telemetry_error_v1",
        "session_date": day.isoformat(),
        "status": "TELEMETRY_ERROR",
        "component": component,
        "phase": phase,
        "source": source,
        "symbol": symbol,
        "error_type": type(error).__name__,
        "error": str(error),
    }
    if errors is not None:
        errors.append(row)
    return row


def _append_quality_evidence(
    rows: list[dict[str, Any]],
    errors: list[dict[str, Any]],
    frame: pd.DataFrame,
    *,
    day: date,
    expected_timestamps: Any,
    source: str,
    symbol: str,
) -> None:
    """Evaluate a data source while keeping diagnostics outside replay truth."""

    try:
        rows.append({
            **evaluate_ohlcv(
                # The observer never receives the authoritative frame itself.
                # A defective evaluator therefore cannot mutate inputs later
                # consumed by coverage, feature construction, or execution.
                frame.copy(deep=True),
                expected_timestamps=expected_timestamps,
                source=source,
                symbol=symbol,
            ).to_dict(),
            "session_date": day.isoformat(),
            "layer": "RAW_HISTORICAL",
        })
    except Exception as exc:
        evidence = _telemetry_error(
            errors,
            day=day,
            component="data_quality",
            phase="evaluate_source",
            source=source,
            symbol=symbol,
            error=exc,
        )
        rows.append({
            "schema_version": "ai_platform_data_quality_v1",
            "session_date": day.isoformat(),
            "layer": "RAW_HISTORICAL",
            "source": source,
            "symbol": symbol,
            "status": "TELEMETRY_ERROR",
            "row_count": len(frame),
            "content_sha256": None,
            "issues": [{
                "code": "TELEMETRY_ERROR",
                "severity": "OBSERVABILITY",
                "error_type": evidence["error_type"],
            }],
            "error_type": evidence["error_type"],
            "error": evidence["error"],
        })


def _cutoff(day: date) -> pd.Timestamp:
    return pd.Timestamp(config.slot_datetime(day, config.SQUARE_OFF))


def _slot_times(day: date) -> tuple[pd.DatetimeIndex, pd.DatetimeIndex, pd.DatetimeIndex]:
    signal = pd.DatetimeIndex([config.slot_datetime(day, clock) for clock in config.SIGNAL_TO_CONFIRMATION])
    minute = set([_cutoff(day)])
    oi = set()
    for stamp in signal:
        minute.update(pd.date_range(stamp - pd.Timedelta(minutes=4), stamp + pd.Timedelta(minutes=1), freq="min"))
        oi.update([stamp - pd.Timedelta(minutes=5), stamp])
    return signal, pd.DatetimeIndex(sorted(minute)), pd.DatetimeIndex(sorted(oi))


def _load_minute(path: Path, day: date, problems: list, symbol: str) -> pd.DataFrame:
    names = set(pq.ParquetFile(path).schema.names)
    required = ["date", "open", "high", "low", "close", "volume"]
    absent = set(required) - names
    if absent:
        raise ValueError(f"Missing equity fields: {sorted(absent)}")
    optional = [x for x in ("gap_filled", "opening_snapshot", "provisional_stale") if x in names]
    frame = pd.read_parquet(path, columns=required + optional)
    frame["ts"] = hybrid._to_ist(frame["date"]).dt.as_unit("ns")
    target = frame.loc[frame.ts.dt.date.eq(day)]
    if target.ts.duplicated().any():
        _problem(problems, symbol, "DUPLICATE_REQUESTED_DAY_EQUITY_MINUTES")
    if frame.ts.isna().any():
        _problem(problems, symbol, "INVALID_EQUITY_TIMESTAMP")
    numeric = ["open", "high", "low", "close", "volume"]
    frame[numeric] = frame[numeric].apply(pd.to_numeric, errors="coerce")
    # Future dates never participate even indirectly in feature construction.
    return (frame.loc[frame.ts.le(_cutoff(day))].dropna(subset=["ts"])
            .sort_values("ts", kind="stable").drop_duplicates("ts", keep="last").reset_index(drop=True))


def _load_future(path: Path, day: date, problems: list, symbol: str) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    required = {"timestamp", "open", "high", "low", "close", "volume", "oi"}
    if required.difference(frame):
        raise ValueError(f"Missing futures fields: {sorted(required.difference(frame))}")
    frame["ts"] = hybrid._to_ist(frame["timestamp"]).dt.as_unit("ns")
    if frame.loc[frame.ts.dt.date.eq(day), "ts"].duplicated().any():
        _problem(problems, symbol, "DUPLICATE_REQUESTED_DAY_FUTURES_BARS")
    return (frame.loc[frame.ts.le(_cutoff(day))].sort_values("ts", kind="stable")
            .drop_duplicates("ts", keep="last").reset_index(drop=True))


def _coverage(minute: pd.DataFrame, future: pd.DataFrame, day: date,
              symbol: str, problems: list) -> dict:
    signal_times, _, required_oi = _slot_times(day)
    required_minutes = pd.date_range(config.slot_datetime(day, "09:16"), _cutoff(day), freq="min")
    missing_minute = required_minutes.difference(pd.DatetimeIndex(minute.ts))
    missing_oi = required_oi.difference(pd.DatetimeIndex(future.ts))
    if len(missing_minute):
        _problem(problems, symbol, "MISSING_REQUIRED_EQUITY_MINUTES", count=len(missing_minute),
                 timestamps=[x.isoformat() for x in missing_minute])
    if len(missing_oi):
        _problem(problems, symbol, "MISSING_SIGNAL_OR_PRIOR_FUTURES_OI_BAR", count=len(missing_oi),
                 timestamps=[x.isoformat() for x in missing_oi])
    # Data completeness is deliberately stricter than the retained mathematical
    # rolling(20, min_periods=5) definition: never let a skipped download change
    # the volume denominator. Keep the native full history and fail explicitly
    # if that history supplies non-session or interrupted baseline bars.
    clock = minute.ts.dt.hour * 60 + minute.ts.dt.minute
    if (~clock.between(9 * 60 + 16, 15 * 60 + 30)).any():
        _problem(problems, symbol, "NON_REGULAR_EQUITY_HISTORY_MINUTES")
    warmup = minute.loc[minute.ts.lt(signal_times[0] + pd.Timedelta(minutes=1))].tail(20)
    if len(warmup) < 20:
        _problem(problems, symbol, "INSUFFICIENT_CONFIRMATION_VOLUME_WARMUP", observed=len(warmup), required=20)
    if len(warmup):
        same_session = warmup.ts.dt.date.eq(warmup.ts.shift().dt.date)
        gaps = same_session & warmup.ts.diff().ne(pd.Timedelta(minutes=1))
        if gaps.any():
            _problem(problems, symbol, "INTERRUPTED_CONFIRMATION_VOLUME_WARMUP", count=int(gaps.sum()))
        warmup_volume = pd.to_numeric(warmup.volume, errors="coerce").to_numpy(float)
        if (~np.isfinite(warmup_volume) | (warmup_volume < 0)).any() or not np.any(warmup_volume > 0):
            _problem(problems, symbol, "INVALID_CONFIRMATION_VOLUME_WARMUP")
    target = minute.loc[minute.ts.dt.date.eq(day)]
    values = target[["open", "high", "low", "close", "volume"]].to_numpy(float)
    if len(target):
        invalid = (~np.isfinite(values).all(axis=1) | (values[:, :4] <= 0).any(axis=1)
                   | (values[:, 4] < 0) | (values[:, 1] < np.maximum(values[:, 0], values[:, 3]))
                   | (values[:, 2] > np.minimum(values[:, 0], values[:, 3])))
        if invalid.any():
            _problem(problems, symbol, "INVALID_REQUESTED_DAY_EQUITY_OHLCV", count=int(invalid.sum()))
        for name in ("gap_filled", "opening_snapshot", "provisional_stale"):
            if name in target:
                flagged = pd.to_numeric(target[name], errors="coerce").fillna(0).ne(0) | target[name].astype(str).str.lower().isin(["true", "yes", "on"])
                if flagged.any():
                    _problem(problems, symbol, "FLAGGED_REQUESTED_DAY_EQUITY_SOURCE", field=name, count=int(flagged.sum()))
    needed = future.loc[future.ts.isin(required_oi)]
    oi_values = pd.to_numeric(needed.oi, errors="coerce").to_numpy(float)
    if (~np.isfinite(oi_values) | (oi_values <= 0)).any():
        _problem(problems, symbol, "INVALID_REQUIRED_FUTURES_OI")
    return dict(symbol=symbol, required_equity_minutes=len(required_minutes),
                missing_equity_minutes=len(missing_minute), required_futures_bars=len(required_oi),
                missing_futures_bars=len(missing_oi), observed_session_minutes=len(target),
                observed_warmup_minutes=int(minute.ts.lt(signal_times[0]).sum()),
                confirmation_prior_minutes=len(warmup))


def _strict_signals(pool: pd.DataFrame, nifty_return: float) -> pd.DataFrame:
    """Native V13 strict raw signal conditions, prior to G setup ranking."""
    out = pool.copy()
    bull, bear = out.v9_5m_ema_bull.fillna(False), out.v9_5m_ema_bear.fillna(False)
    common_gate = (out.oi.gt(out.prev_oi) & out.oi_change_pct.between(.05, 1.) & out.volume_ratio.ge(.80))
    longs = bull & out.price_change_pct.ge(.10)
    shorts = bear & out.price_change_pct.le(-.10)
    out["side"] = np.where(longs, "LONG", "SHORT")
    sign = np.where(longs, 1., -1.)
    strict = (common_gate & (longs | shorts) & out.v9_exact_confirmation_present
              & out.confirmation_high.gt(out.confirmation_low)
              & ((out.confirmation_close - out.confirmation_open) * sign).gt(0)
              & ((out.confirmation_close - out.signal_close) * sign).gt(0))
    out["nifty_first_bar_return_pct"] = nifty_return
    gated = out.hhmm_int.eq(925) & out.side.eq("SHORT")
    strict &= ~gated | (np.isfinite(nifty_return) and nifty_return <= -.05)
    out["wick_ratio"] = np.where(longs, out.v9_1m_upper_wick_ratio, out.v9_1m_lower_wick_ratio)
    out["trigger"] = np.where(longs, out.confirmation_high, out.confirmation_low)
    return out.loc[strict].copy()


def _observed_pool(minute: pd.DataFrame, future: pd.DataFrame, *, day: date,
                   symbol: str, future_symbol: str, month: str,
                   observability_errors: list[dict[str, Any]] | None = None) -> pd.DataFrame:
    """Only retained G's consumed features, using native arithmetic/history.

    The general research builder also computes VWAP, RSI-like contexts and
    several 1m EMAs across the whole history. G does not consume those fields;
    excluding that unused work keeps a daily dashboard replay responsive.
    """
    equity_five = hybrid.aggregate_equity_one_minute_to_five_minute(minute)
    if equity_five.empty:
        return pd.DataFrame()
    five = hybrid.join_equity_price_with_futures_oi(equity_five, future)
    signal_times, _, _ = _slot_times(day)
    five = five.loc[five.ts.isin(signal_times)].copy()
    if five.empty:
        return five
    five["signal_ts"] = five.ts
    five["confirmation_ts"] = five.ts + pd.Timedelta(minutes=1)
    five["day"] = day
    five["hhmm"] = five.ts.dt.strftime("%H%M")
    five["hhmm_int"] = five.hhmm.astype(int)
    five["signal_close"] = five.close
    five["tradingsymbol"] = symbol
    five["futures_tradingsymbol"] = future_symbol
    five["contract_month"] = month
    five["price_source"] = hybrid.BACKTEST_EQUITY_5M_CONSTRUCTION
    five["oi_source"] = "NFO_FUTURE"
    five["data_contract"] = hybrid.DATA_CONTRACT_VERSION
    for span in (9, 20, 50):
        five[f"v9_5m_ema{span}"] = five[f"ema{span}"]
    five["v9_5m_ema_bull"] = (five.ema9.gt(five.ema20) & five.ema20.gt(five.ema50)).astype("boolean")
    five["v9_5m_ema_bear"] = (five.ema9.lt(five.ema20) & five.ema20.lt(five.ema50)).astype("boolean")
    five["v9_5m_feature_ts"] = five.signal_ts
    volume = pd.to_numeric(minute.volume, errors="coerce")
    denominator = volume.shift(1).rolling(20, min_periods=5).mean()
    confirmation = minute[["ts", "open", "high", "low", "close", "volume"]].copy()
    confirmation["v9_1m_volume_ratio"] = volume.div(denominator.where(denominator.gt(0)))
    confirmation = confirmation.loc[confirmation.ts.isin(five.confirmation_ts)].copy()
    confirmation["v9_1m_feature_ts"] = confirmation.ts
    o, h, low, c = [confirmation[name].astype(float) for name in ("open", "high", "low", "close")]
    bar_range = (h - low).where(h.gt(low))
    confirmation["body_ratio"] = (c - o).abs().div(bar_range)
    confirmation["v9_1m_upper_wick_ratio"] = (h - pd.concat([o, c], axis=1).max(axis=1)).div(bar_range)
    confirmation["v9_1m_lower_wick_ratio"] = (pd.concat([o, c], axis=1).min(axis=1) - low).div(bar_range)
    confirmation = confirmation.rename(columns={"ts": "confirmation_ts", **{name: f"confirmation_{name}"
                                                   for name in ("open", "high", "low", "close", "volume")}})
    five = five.merge(confirmation, on="confirmation_ts", how="left", validate="one_to_one")
    five["v9_exact_confirmation_present"] = five.v9_1m_feature_ts.notna()
    five["v9_feature_available_ts"] = five.v9_1m_feature_ts
    features.assert_feature_chronology(five)
    # Hash the exact effective histories consumed by each decision.  The
    # feature builder consumes the completed 5m equity history through the
    # signal, the last two futures rows for current/previous OI, and the
    # confirmation candle plus its previous twenty observed 1m volumes.
    equity_columns = [name for name in (
        "ts", "open", "high", "low", "close", "volume", "source_1m_count"
    ) if name in equity_five]
    futures_columns = [name for name in (
        "ts", "open", "high", "low", "close", "volume", "oi"
    ) if name in future]
    minute_columns = [name for name in (
        "ts", "open", "high", "low", "close", "volume", "gap_filled",
        "opening_snapshot", "provisional_stale",
    ) if name in minute]
    for column in (
        "equity_5m_history_sha256",
        "futures_oi_pair_sha256",
        "confirmation_1m_window_sha256",
        "input_slice_sha256",
    ):
        five[column] = ""
    for index, decision in five.iterrows():
        signal_stamp = pd.Timestamp(decision["signal_ts"])
        confirmation_stamp = pd.Timestamp(decision["confirmation_ts"])
        equity_window = equity_five.loc[equity_five.ts.le(signal_stamp), equity_columns]
        futures_window = future.loc[future.ts.le(signal_stamp), futures_columns].tail(2)
        confirmation_window = minute.loc[
            minute.ts.le(confirmation_stamp), minute_columns
        ].tail(config.CONFIRMATION_VOLUME_LOOKBACK + 1)
        try:
            components = {
                "schema_version": "v13_v10_g_effective_input_slice_v1",
                "equity_5m_history_sha256": canonical_frame_sha256(
                    equity_window, sort_by=["ts"]
                ),
                "equity_5m_history_rows": len(equity_window),
                "futures_oi_pair_sha256": canonical_frame_sha256(
                    futures_window, sort_by=["ts"]
                ),
                "futures_oi_pair_rows": len(futures_window),
                "confirmation_1m_window_sha256": canonical_frame_sha256(
                    confirmation_window, sort_by=["ts"]
                ),
                "confirmation_1m_window_rows": len(confirmation_window),
                "signal_ts": signal_stamp.isoformat(),
                "confirmation_ts": confirmation_stamp.isoformat(),
            }
            five.at[index, "equity_5m_history_sha256"] = components["equity_5m_history_sha256"]
            five.at[index, "futures_oi_pair_sha256"] = components["futures_oi_pair_sha256"]
            five.at[index, "confirmation_1m_window_sha256"] = components["confirmation_1m_window_sha256"]
            five.at[index, "input_slice_sha256"] = canonical_payload_sha256(components)
        except Exception as exc:
            _telemetry_error(
                observability_errors,
                day=day,
                component="input_slice_hash",
                phase="hash_effective_history",
                source="EFFECTIVE_STRATEGY_INPUTS",
                symbol=symbol,
                error=exc,
            )
    return five.drop(columns=["date", "ts"], errors="ignore")


def _empty_signals() -> pd.DataFrame:
    columns = ["sid", "signal_id", "day", "signal_ts", "confirmation_ts", "hhmm", "hhmm_int", "side",
               "tradingsymbol", "futures_tradingsymbol", "price_change_pct", "oi_change_pct",
               "volume_ratio", "body_ratio", "wick_ratio", "traded_value", "trigger",
               "v9_1m_volume_ratio", "v9_1m_feature_ts"]
    return pd.DataFrame(columns=columns)


def _assert_day(frame: pd.DataFrame, day: date) -> None:
    if "day" not in frame or frame.empty:
        return
    observed = pd.to_datetime(frame.day, errors="coerce").dt.date
    if observed.isna().any() or not observed.eq(day).all():
        raise ValueError("Replay frame contains dates outside the requested session")


def select_day_orders(signals: pd.DataFrame, observed: pd.DataFrame, base, settings: dict, day: date):
    """Original selection has priority; the dated exception only fills its vacancy."""
    audit = g.selection_audit(signals, base, g.SelectionChange(**settings["selection_change"]),
                             core_first=True, morning_slots=False, two_bar_continuation=False)
    orders = audit.loc[audit.v9_selected.eq(True)].copy()
    combined, relaxed_audit = promoted_selection.apply_relaxed_0925_long(
        orders, observed, session_date=day)
    if not policy.enabled_for_session(day):
        return signals, audit, orders, relaxed_audit
    additions = (combined.loc[combined.relaxed_0925_added.eq(True)].copy()
                 if "relaxed_0925_added" in combined else combined.iloc[:0].copy())
    if not additions.empty:
        # Reserve IDs for every original candidate, including ranked-out rows.
        start = int(signals.sid.max()) + 1 if len(signals) else 0
        additions["signal_id"] = [canonical_signal_id(
            config.STRATEGY_VERSION, day, "0925", "09:26", "LONG", str(symbol))
            for symbol in additions.tradingsymbol]
        old_ids = dict(zip(signals.signal_id, signals.sid))
        additions["sid"] = [int(old_ids.get(signal_id, start + index))
                            for index, signal_id in enumerate(additions.signal_id)]
        additions["v9_selected"] = True
        additions["v9_decision"] = "SELECTED_RELAXED_0925_LONG"
        additions["v9_filter_pass"] = True
        additions["v9_rank_in_setup_day"] = additions.relaxed_0925_rank
        additions["v10_g_f_core"] = False
        additions["v10_g_morning_slot"] = False
        additions["configured_confirmation_end"] = "09:26"
        additions["picker"] = "max_liquidity"
        additions["max_entries"] = 1
        additions["native_stop_pct"] = policy.INITIAL_STOP_PCT
        additions["native_target_pct"] = settings["exit"]["setups"]["0926_LONG"]["target_pct"]
        retained = signals.loc[~signals.signal_id.isin(additions.signal_id)]
        signals = pd.concat([retained, additions], ignore_index=True, sort=False) if len(retained) else additions.copy()
        if "signal_id" in audit:
            audit = audit.loc[~audit.signal_id.isin(additions.signal_id)]
        audit = pd.concat([audit, additions], ignore_index=True, sort=False) if len(audit) else additions.copy()
        orders = pd.concat([orders, additions], ignore_index=True, sort=False) if len(orders) else additions.copy()
        for frame in (signals, orders):
            if frame.signal_id.duplicated().any() or frame.sid.duplicated().any():
                raise ValueError("Duplicate promoted replay candidate identity")
    # Persist executable exits in the order snapshot, not merely in the later
    # portfolio simulation; the frozen table remains the target source.
    for frame in (audit, orders):
        frame["native_stop_pct"] = policy.INITIAL_STOP_PCT
        frame["initial_stop_pct"] = policy.INITIAL_STOP_PCT
        frame["tightened_stop_pct"] = policy.TIGHTENED_STOP_PCT
        frame["tighten_after_minutes"] = policy.TIGHTEN_AFTER_MINUTES
        frame["strategy_policy_revision"] = policy.REVISION
    return signals, audit, orders, relaxed_audit


def build_day_dataset(day: date, *, roots: DataRoots | None = None) -> dict:
    if type(day) is not date:
        raise TypeError("An explicit datetime.date session is required")
    roots = roots or DataRoots()
    config.validate_strategy()
    strategy_fingerprint = config.strategy_fingerprint()
    observed_pools = []
    sources, problems, coverage, pools, feature_ledgers, data_quality_rows, observability_errors, minutes = [], [], [], [], [], [], [], {}
    universe_path = roots.universe / f"near_month_{day.isoformat()}.parquet"
    _record_source(universe_path, "DATED_UNIVERSE", sources)
    for path in (Path(__file__), Path(config.__file__), Path(g.__file__), Path(features.__file__),
                 Path(hybrid.__file__), Path(g.v9.__file__), Path(g.v9.v5.__file__), Path(g.v9.v6.__file__),
                 Path(provenance.__file__), Path(common.__file__), Path(policy.__file__),
                 Path(promoted_selection.__file__), Path(staged_replay.__file__), config.CONFIG_PATH):
        _record_source(path, "CODE_OR_PINNED_CONFIGURATION", sources)
    result = dict(day=day, days=[day], sources=sources, problems=problems, coverage_rows=coverage,
                  excluded_stocks=[],
                  signals=_empty_signals(), orders=pd.DataFrame(columns=["day", "setup_id", "sid"]),
                  selection_audit=pd.DataFrame(columns=["day", "setup_id", "sid", "v9_selected"]),
                  feature_ledger=pd.DataFrame(),
                  data_quality_rows=data_quality_rows,
                  observability_errors=observability_errors,
                  paths={}, universe_count=0, mapped_universe=pd.DataFrame(),
                  strategy_policy=policy.policy_for_day(day), relaxed_0925_audit=pd.DataFrame())
    try:
        full = pd.read_parquet(universe_path)
        stocks = full.loc[~full.is_index_future.fillna(False).astype(bool)]
        expiry = pd.to_datetime(stocks.expiry, errors="coerce").dropna().unique()
        if len(expiry) != 1:
            raise ValueError("Dated stock universe must have exactly one expiry")
        month = pd.Timestamp(expiry[0]).strftime("%y%b").upper()
        mapped, proof = provenance.load_backtest_universe(universe_path=universe_path, universe_date=day,
                         contract_month_contains=month, require_persisted_mapping=True)
        if mapped.equity_symbol.duplicated().any() or mapped.futures_tradingsymbol.duplicated().any():
            raise ValueError("Dated stock/equity mapping contains duplicates")
        result.update(universe_count=len(mapped), mapped_universe=mapped, universe_proof=proof)
        nifty = full.loc[full.underlying.astype(str).str.upper().eq("NIFTY")]
        if len(nifty) != 1 or pd.Timestamp(nifty.iloc[0].expiry) != pd.Timestamp(expiry[0]):
            raise ValueError("Dated near-month NIFTY futures mapping is missing or ambiguous")
        nifty_symbol = str(nifty.iloc[0].tradingsymbol)
        nifty_path = roots.futures_5m / f"{common.safe_contract_stem(nifty_symbol)}_5minute.parquet"
        _record_source(nifty_path, "NIFTY_FUTURES_CONTEXT", sources)
        nifty_frame = _load_future(nifty_path, day, problems, nifty_symbol)
        nifty_target = nifty_frame.loc[nifty_frame.ts.dt.date.eq(day)]
        nifty_return = config.nifty_context_from_bars(nifty_frame, day)
        if not np.isfinite(nifty_return):
            _problem(problems, nifty_symbol, "MISSING_OR_INVALID_EXACT_0920_NIFTY_CONTEXT")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        _problem(problems, "UNIVERSE_OR_NIFTY", "REQUIRED_SOURCE_UNAVAILABLE", detail=f"{type(exc).__name__}: {exc}")
        return result
    _append_quality_evidence(
        data_quality_rows,
        observability_errors,
        nifty_target,
        day=day,
        expected_timestamps=[config.slot_datetime(day, config.NIFTY_FIRST_BAR_END)],
        source="NIFTY_FUTURES_CONTEXT",
        symbol=nifty_symbol,
    )
    for number, contract in enumerate(mapped.to_dict("records"), 1):
        symbol = hybrid.resolve_backtest_equity_symbol(str(contract["equity_symbol"]), root=roots.equity_1m)
        future_symbol = str(contract["futures_tradingsymbol"])
        minute_path = hybrid.equity_one_minute_path(symbol, roots.equity_1m)
        future_path = roots.futures_5m / f"{common.safe_contract_stem(future_symbol)}_5minute.parquet"
        _record_source(minute_path, "EQUITY_ONE_MINUTE", sources)
        _record_source(future_path, "STOCK_FUTURES_OI", sources)
        try:
            minute = _load_minute(minute_path, day, problems, symbol)
            future = _load_future(future_path, day, problems, symbol)
            _, _, required_oi = _slot_times(day)
            required_minutes = pd.date_range(
                config.slot_datetime(day, "09:16"), _cutoff(day), freq="min"
            )
            minute_target = minute.loc[minute.ts.dt.date.eq(day)]
            future_target = future.loc[future.ts.dt.date.eq(day)]
            _append_quality_evidence(
                data_quality_rows,
                observability_errors,
                minute_target,
                day=day,
                expected_timestamps=required_minutes,
                source="NSE_EQUITY_1M",
                symbol=symbol,
            )
            _append_quality_evidence(
                data_quality_rows,
                observability_errors,
                future_target,
                day=day,
                expected_timestamps=required_oi,
                source="NFO_FUTURE_5M",
                symbol=future_symbol,
            )
            problem_start = len(problems)
            coverage.append(_coverage(minute, future, day, symbol, problems))
            new_reasons = {row["reason"] for row in problems[problem_start:]}
            if new_reasons & EXCLUDABLE_COVERAGE_REASONS:
                result["excluded_stocks"].append(dict(
                    symbol=symbol,
                    reasons=sorted(new_reasons & EXCLUDABLE_COVERAGE_REASONS),
                ))
                continue
            # Cache only target-day execution prices, not the warmup history.
            minutes[symbol] = minute.loc[minute.ts.dt.date.eq(day)].copy()
            pool = _observed_pool(minute, future, day=day, symbol=symbol,
                                  future_symbol=future_symbol, month=month,
                                  observability_errors=observability_errors)
            if pool.empty:
                _problem(problems, symbol, "NO_REQUESTED_DAY_COMPLETE_EQUITY_FIVE_MINUTE_BARS")
                continue
            pool = pool.loc[pool.hhmm_int.isin([int(x.replace(":", "")) for x in config.SIGNAL_TO_CONFIRMATION])].copy()
            if len(pool) != len(config.SIGNAL_TO_CONFIRMATION):
                _problem(problems, symbol, "INCOMPLETE_REQUIRED_FIVE_MINUTE_BAR_CONSTRUCTION", observed=len(pool))
            pool["instrument_token"] = int(contract["equity_instrument_token"])
            pool["futures_instrument_token"] = int(contract["futures_instrument_token"])
            pool["exchange"] = "NSE"
            observed_pools.append(pool)
            pools.append(_strict_signals(pool, nifty_return))
            try:
                feature_ledgers.append(build_v13_v10_g_feature_ledger(
                    pool.assign(
                        run_id=common.PROCESS_RUN_ID,
                        replay_id=common.PROCESS_RUN_ID,
                    ),
                    nifty_return=nifty_return,
                    strategy_version=config.STRATEGY_VERSION,
                    strategy_fingerprint=strategy_fingerprint,
                ))
            except Exception as exc:
                _telemetry_error(
                    observability_errors,
                    day=day,
                    component="feature_ledger",
                    phase="build_symbol_ledger",
                    source="V13_V10_G_FEATURES",
                    symbol=symbol,
                    error=exc,
                )
        except (OSError, ValueError, KeyError, TypeError, AssertionError) as exc:
            _problem(problems, symbol, "STOCK_SOURCE_BUILD_FAILED", detail=f"{type(exc).__name__}: {exc}")
        if number % 25 == 0 or number == len(mapped):
            print(f"[V13-V10-G daily] {day}: reconstructed {number}/{len(mapped)} stocks", flush=True)
    if pools:
        signals = pd.concat(pools, ignore_index=True).sort_values(["tradingsymbol", "signal_ts", "side"], kind="stable").reset_index(drop=True)
        signals["sid"] = np.arange(len(signals), dtype=int)
        signals["signal_id"] = [
            canonical_signal_id(
                config.STRATEGY_VERSION,
                day,
                str(row.hhmm),
                config.SIGNAL_TO_CONFIRMATION[
                    f"{str(row.hhmm).zfill(4)[:2]}:{str(row.hhmm).zfill(4)[2:]}"
                ],
                str(row.side),
                str(row.tradingsymbol),
            )
            for row in signals.itertuples(index=False)
        ]
        if signals["signal_id"].duplicated().any():
            raise ValueError("Canonical signal identity collision in replay candidates")
        _assert_day(signals, day)
        result["signals"] = signals
    base = g.v9.V9Config(portfolio_capital_rupees=config.PORTFOLIO_CAPITAL_RS,
                        capital_per_entry_rupees=config.CAPITAL_PER_ENTRY_RS, leverage_factor=config.LEVERAGE,
                        max_positions=None, cost_bps=config.ROUND_TRIP_COST_BPS)
    settings = config.load_frozen_config()
    observed = pd.concat(observed_pools, ignore_index=True) if observed_pools else pd.DataFrame()
    result["signals"], audit, orders, result["relaxed_0925_audit"] = select_day_orders(
        result["signals"], observed, base, settings, day)
    if feature_ledgers:
        try:
            ledger = pd.concat(feature_ledgers, ignore_index=True)
            ledger["final_selected"] = False
            ledger["rank_within_setup"] = pd.array([pd.NA] * len(ledger), dtype="Int64")
            ledger["selection_decision"] = np.select(
                [ledger.strict_signal_pass.eq(False), ledger.setup_filter_pass.eq(False)],
                ["BASE_OR_CONFIRMATION_REJECTED", "SETUP_FILTER_REJECTED"],
                default="RANKED_OUT",
            )
            audit_lookup: dict[tuple[str, pd.Timestamp, str], dict[str, Any]] = {}
            for audit_row in audit.to_dict("records"):
                audit_lookup[(
                    str(audit_row.get("tradingsymbol", "")).upper(),
                    pd.Timestamp(audit_row.get("signal_ts")),
                    str(audit_row.get("side", "")).upper(),
                )] = audit_row
            for index, ledger_row in ledger.iterrows():
                key = (
                    str(ledger_row["tradingsymbol"]).upper(),
                    pd.Timestamp(ledger_row["signal_ts"]),
                    str(ledger_row["base_side"]).upper(),
                )
                observed = audit_lookup.get(key)
                if observed is not None:
                    selected = bool(observed.get("v9_selected", False))
                    rank = observed.get("v9_rank_in_setup_day")
                    ledger.at[index, "final_selected"] = selected
                    if pd.notna(rank):
                        ledger.at[index, "rank_within_setup"] = int(rank)
                    ledger.at[index, "selection_decision"] = (
                        "SELECTED" if selected else str(observed.get("v9_decision", "RANKED_OUT"))
                    )
                    if observed.get("setup_id"):
                        ledger.at[index, "setup_id"] = str(observed["setup_id"])
                unhashed = ledger.loc[index].drop(
                    labels=["ledger_row_sha256"], errors="ignore"
                ).to_dict()
                ledger.at[index, "ledger_row_sha256"] = canonical_row_sha256(unhashed)
            finalized_ledger = ledger.sort_values(
                ["signal_ts", "tradingsymbol"], kind="stable"
            ).reset_index(drop=True)
        except Exception as exc:
            _telemetry_error(
                observability_errors,
                day=day,
                component="feature_ledger",
                phase="finalize_selection_ledger",
                source="V13_V10_G_FEATURES",
                error=exc,
            )
        else:
            # Publish only a fully finalized diagnostic ledger.  Selection and
            # order truth above is intentionally independent of this object.
            result["feature_ledger"] = finalized_ledger
    for row in orders.drop_duplicates("sid").itertuples(index=False):
        minute = minutes.get(row.tradingsymbol, pd.DataFrame())
        path_frame = minute.loc[minute.ts.gt(row.confirmation_ts) & minute.ts.le(_cutoff(day))]
        path = {"timestamp_ns": path_frame.ts.astype("int64").to_numpy(),
                **{key: path_frame[key].to_numpy(float) for key in ("open", "high", "low", "close")}}
        result["paths"][int(row.sid)] = path
        try:
            g.v9.validate_paths(orders.loc[orders.sid.eq(row.sid)], {int(row.sid): path})
        except RuntimeError as exc:
            _problem(problems, row.tradingsymbol, "INCOMPLETE_SELECTED_EXECUTION_PATH", detail=str(exc))
    _assert_day(audit, day)
    _assert_day(orders, day)
    result.update(orders=orders, selection_audit=audit, v9_config=base, settings=settings)
    # Atomic producer replacements during reconstruction invalidate the run.
    for source in sources:
        path = Path(source["path"])
        if path.is_file() != source["exists"] or (source["exists"] and _sha(path) != source["sha256"]):
            _problem(problems, path.name, "SOURCE_CHANGED_DURING_REPLAY", path=str(path))
    return result


def simulate_day(dataset: dict) -> tuple[pd.DataFrame, dict]:
    """Session-effective G exits and fixed capital, also used for diagnostic parity."""
    day, base = dataset["day"], dataset["v9_config"]
    orders = dataset["orders"].copy()
    _assert_day(orders, day)
    exits = dataset["settings"]["exit"]["setups"]
    orders["native_stop_pct"] = orders.setup_id.map({key: value["stop_pct"] for key, value in exits.items()})
    orders["native_target_pct"] = orders.setup_id.map({key: value["target_pct"] for key, value in exits.items()})
    g.v9.validate_paths(orders, dataset["paths"])
    simulator = staged_replay.simulate_staged if policy.enabled_for_session(day) else g.v9.v5.simulate_native
    trades = simulator(orders, dataset["paths"], cost_bps=base.cost_bps, max_entry_delay_minutes=10)
    for column, default in (("filled", False), ("entry_ts", pd.NaT), ("exit_ts", pd.NaT),
                            ("net_return_pct", np.nan), ("gross_return_pct", np.nan), ("cost_pct", np.nan)):
        if column not in trades:
            trades[column] = default
    trades = g.v9.v5.apply_fixed_capital_model(trades, base.capital_per_entry_rupees, base.leverage_factor)
    ledger, _ = g.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())
    if ledger.empty:
        # The after-close shadow finalizer still validates the CSV identity
        # schema on a valid zero-order session.
        for column in ("sid", "signal_id", "day", "hhmm", "side", "tradingsymbol",
                       "setup_id", "configured_confirmation_end"):
            if column not in ledger:
                ledger[column] = pd.Series(index=ledger.index, dtype=object)
    _assert_day(ledger, day)
    for field_name in ("entry_ts", "exit_ts"):
        stamps = pd.to_datetime(ledger[field_name], utc=True, errors="coerce").dt.tz_convert(common.IST)
        if not stamps.dropna().dt.date.eq(day).all():
            raise ValueError("Execution timestamp escaped the requested session")
    metric = g.r.metric(ledger, [day])
    metric.update(sessions=1, orders=metric["selected_orders"], fills=metric["trades"])
    return ledger, metric


def replay_day(
    day: date,
    output_dir: Path,
    *,
    roots: DataRoots | None = None,
    snapshot_root: Path | None = None,
) -> dict:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    snapshot = create_input_snapshot(
        day,
        Path(snapshot_root).resolve() if snapshot_root is not None else output / "input_snapshots",
        roots=roots,
    )
    dataset = build_day_dataset(day, roots=snapshot.roots)
    # Capture failures are authoritative blockers.  Missing source files are
    # independently identified by the normal dataset coverage checks.
    dataset["problems"][:0] = [dict(row) for row in snapshot.problems]
    sources = dataset["sources"]
    _annotate_snapshot_sources(sources, snapshot)
    source_fingerprint = _replay_source_fingerprint(sources, snapshot)
    artifacts = {"input_snapshot_manifest": str(snapshot.manifest_path)}
    observability_errors = dataset.setdefault("observability_errors", [])
    for name, frame in (("candidate_signals", dataset["signals"]),
                        ("selection_audit", dataset.get("selection_audit", pd.DataFrame())),
                        ("relaxed_0925_audit", dataset.get("relaxed_0925_audit", pd.DataFrame())),
                        ("selected_orders", dataset.get("orders", pd.DataFrame())),
                        ("coverage", pd.DataFrame(dataset["coverage_rows"]) if dataset["coverage_rows"] else
                         pd.DataFrame(columns=["symbol", "missing_equity_minutes", "missing_futures_bars"]))):
        if len(frame.columns) == 0:
            continue
        _assert_day(frame, day)
        path = output / f"{name}.csv"
        common.atomic_write_csv(frame, path)
        artifacts[name] = str(path)

    # Diagnostic artifacts are strictly fail-open.  They are persisted only
    # after all authoritative replay inputs, candidates, and selections exist.
    try:
        data_quality_frame = (
            pd.DataFrame(dataset.get("data_quality_rows", []))
            if dataset.get("data_quality_rows")
            else pd.DataFrame(columns=[
                "schema_version", "session_date", "layer", "source", "symbol",
                "status", "row_count", "content_sha256", "issues",
            ])
        )
        data_quality_path = output / "data_quality.csv"
        common.atomic_write_csv(data_quality_frame, data_quality_path)
    except Exception as exc:
        _telemetry_error(
            observability_errors,
            day=day,
            component="data_quality",
            phase="persist_artifact",
            source="data_quality.csv",
            error=exc,
        )
    else:
        artifacts["data_quality"] = str(data_quality_path)

    feature_path = output / "feature_ledger.csv"
    try:
        write_feature_ledger(dataset.get("feature_ledger", pd.DataFrame()), feature_path)
    except Exception as exc:
        _telemetry_error(
            observability_errors,
            day=day,
            component="feature_ledger",
            phase="persist_artifact",
            source="feature_ledger.csv",
            error=exc,
        )
    else:
        artifacts["feature_ledger"] = str(feature_path)
        artifacts["feature_ledger_manifest"] = str(
            feature_path.with_name(f"{feature_path.name}.manifest.json")
        )
    ignored_problems = [row for row in dataset["problems"]
                        if row.get("reason") in EXCLUDABLE_COVERAGE_REASONS]
    blocking_problems = [row for row in dataset["problems"]
                         if row.get("reason") not in EXCLUDABLE_COVERAGE_REASONS]
    included_stocks = dataset["universe_count"] - len(dataset.get("excluded_stocks", []))
    complete = not blocking_problems and included_stocks > 0
    metrics = None
    if complete:
        ledger, metrics = simulate_day(dataset)
        ledger["strategy"] = STRATEGY
        path = output / "portfolio_trades.csv"
        common.atomic_write_csv(ledger, path)
        artifacts["portfolio_trades"] = str(path)
        daily = output / "daily_results.csv"
        common.atomic_write_csv(pd.DataFrame([dict(day=day.isoformat(), strategy=STRATEGY, **metrics)]), daily)
        artifacts["daily_results"] = str(daily)
    observability = dict(
        state="DEGRADED" if observability_errors else "GOOD",
        error_count=len(observability_errors),
        errors=list(observability_errors),
    )
    manifest_path = output / "source_manifest.json"
    manifest = dict(schema_version=SCHEMA_VERSION, session_date=day.isoformat(), days=[day.isoformat()],
                    run_id=common.PROCESS_RUN_ID, replay_id=common.PROCESS_RUN_ID,
                    strategy=STRATEGY, frozen_config_sha256=config.CONFIG_SHA256,
                    signal_identity_schema=SIGNAL_ID_SCHEMA_VERSION,
                    source_fingerprint=source_fingerprint, sources=sources,
                    input_snapshot={
                        "schema_version": SNAPSHOT_SCHEMA_VERSION,
                        "snapshot_fingerprint": snapshot.fingerprint,
                        "manifest_path": str(snapshot.manifest_path),
                        "root": str(snapshot.root),
                    },
                    universe=dataset.get("universe_proof", {}),
                    chronology="Warmup history ends no later than requested cutoff; all candidate/order/exit rows use only requested session.",
                    coverage_policy={"equity_session": "Every completed minute end 09:16 through 15:15",
                                     "confirmation_warmup": "Prior 20 observed regular-session minutes required; no within-session gaps",
                                     "volume_calculation": "Unchanged native rolling20/min5; completeness guard requires20",
                                     "indicators": "Native full causal history; no truncated EMA approximation"},
                    selection="Session-effective G policy; original selections retain priority; no optimization",
                    strategy_policy=policy.policy_for_day(day),
                     complete=complete, coverage_problems=blocking_problems,
                     ignored_coverage_problems=ignored_problems,
                     excluded_stocks=dataset.get("excluded_stocks", []),
                     observability=observability)
    common.atomic_write_json(manifest_path, _json_ready(manifest))
    artifacts["source_manifest"] = str(manifest_path)
    result = dict(schema_version=SCHEMA_VERSION, strategy=STRATEGY, strategy_version=config.STRATEGY_VERSION,
                  strategy_policy=policy.policy_for_day(day),
                  signal_identity_schema=SIGNAL_ID_SCHEMA_VERSION,
                  run_id=common.PROCESS_RUN_ID, replay_id=common.PROCESS_RUN_ID,
                  session_date=day.isoformat(), days=[day.isoformat()], complete=complete,
                  state="SUCCESS" if complete else "BLOCKED_INCOMPLETE_DATA", metrics=metrics,
                   artifacts=artifacts, source_fingerprint=source_fingerprint,
                   observability=observability,
                   coverage=dict(universe_stocks=dataset["universe_count"], included_stocks=included_stocks,
                                checked_stocks=len(dataset["coverage_rows"]), problems=blocking_problems,
                                ignored_problems=ignored_problems,
                                excluded_stocks=dataset.get("excluded_stocks", [])),
                  partial_diagnostics=dict(observed_candidates=len(dataset["signals"]),
                                           selected_from_available_data=len(dataset.get("orders", [])),
                                           publishable=complete))
    result = _json_ready(result)
    common.atomic_write_json(output / "replay_result.json", result)
    common.append_observation(
        "historical_v13_v10_g_replay",
        result,
        identity={
            "session_date": day.isoformat(),
            "replay_id": common.PROCESS_RUN_ID,
            "strategy": STRATEGY,
            "strategy_version": config.STRATEGY_VERSION,
            "source_fingerprint": source_fingerprint,
        },
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", required=True, type=date.fromisoformat)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--snapshot-root",
        type=Path,
        help=(
            "Optional content-addressed input-snapshot store. Defaults to "
            "<output-dir>/input_snapshots."
        ),
    )
    args = parser.parse_args()
    result = replay_day(args.session_date, args.output_dir, snapshot_root=args.snapshot_root)
    print(json.dumps(result, indent=2))
    return 0 if result["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
