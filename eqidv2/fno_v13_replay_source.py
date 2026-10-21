"""Immutable raw market-input capture and causal V13 feature reconstruction.

The low-level snapshot/coverage/arithmetic functions were extracted unchanged
from the retained G replay. No G selection, G result, G policy, or G publisher
is imported here. Full-day checks are after-close replay eligibility only.
"""
from __future__ import annotations
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
import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
import fno_oi_backtest_provenance as provenance
import fno_v13_v9_data as features
import fno_v13_v10_g_3_contract as config
import fno_v13_v10_g_3_opening_inputs as opening_inputs
SNAPSHOT_SCHEMA_VERSION = "fno_v13_input_snapshot_v1"
SNAPSHOT_COPY_ATTEMPTS = 3
EXCLUDABLE_COVERAGE_REASONS = {"MISSING_SIGNAL_OR_PRIOR_FUTURES_OI_BAR"}


def _remove_staging(staging: Path, base: Path) -> None:
    """Remove only this snapshot producer's direct staging child."""
    target, parent = staging.resolve(), base.resolve()
    if target.parent != parent or not target.name.startswith(".staging-"):
        raise ValueError("Snapshot cleanup target escaped the intended staging directory")
    shutil.rmtree(target, ignore_errors=True)

def slot_datetime(day: date, clock: str) -> datetime:
    hour, minute = map(int, clock.split(":"))
    return datetime.combine(day, datetime.min.time()).replace(hour=hour, minute=minute, tzinfo=common.IST)

@dataclass(frozen=True)
class DataRoots:
    universe: Path = field(default_factory=lambda: common.UNIVERSE_DIR)
    equity_1m: Path = field(default_factory=lambda: hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR)
    futures_5m: Path = field(default_factory=lambda: common.RAW_CONTRACT_DIR)
    opening_observer: Path = field(default_factory=lambda: opening_inputs.DEFAULT_ROOT)


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
        staging / "opening_observer",
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
                opening_contract = str(contract["futures_tradingsymbol"])
                capture(opening_inputs.minute_path(original.opening_observer, day, opening_contract),
                        opening_inputs.minute_path(staged_roots.opening_observer, day, opening_contract),
                        "OPENING_0916_FUTURES_OI")
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
            _remove_staging(staging, base)
        else:
            os.replace(staging, final_root)
            _verify_input_snapshot(final_root, manifest)
        final_roots = DataRoots(
            final_root / "universe",
            final_root / "equity_1m",
            final_root / "futures_5m",
            final_root / "opening_observer",
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
            _remove_staging(staging, base)


def _problem(problems: list, symbol: str, reason: str, **details) -> None:
    problems.append(dict(symbol=symbol, reason=reason, **_json_ready(details)))


def _cutoff(day: date) -> pd.Timestamp:
    return pd.Timestamp(slot_datetime(day, config.SQUARE_OFF))


def _slot_times(day: date, signal_clocks=None) -> tuple[pd.DatetimeIndex, pd.DatetimeIndex, pd.DatetimeIndex]:
    signal = pd.DatetimeIndex([slot_datetime(day, clock) for clock in
                              (config.SIGNAL_TO_CONFIRMATION if signal_clocks is None else signal_clocks)])
    minute = set([_cutoff(day)])
    oi = set()
    for stamp in signal:
        minute.update(pd.date_range(stamp - pd.Timedelta(minutes=4), stamp + pd.Timedelta(minutes=1), freq="min"))
        oi.add(stamp)
        # 09:20 has a separately verified 09:16 baseline, not a 09:15 5m bar.
        if stamp.strftime("%H:%M") != "09:20":
            oi.add(stamp - pd.Timedelta(minutes=5))
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
              symbol: str, problems: list, *, signal_clocks=None) -> dict:
    signal_times, _, required_oi = _slot_times(day, signal_clocks)
    required_minutes = pd.date_range(slot_datetime(day, "09:16"), _cutoff(day), freq="min")
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
                   observability_errors: list[dict[str, Any]] | None = None,
                   signal_clocks=None) -> pd.DataFrame:
    """Only retained G's consumed features, using native arithmetic/history.

    The general research builder also computes VWAP, RSI-like contexts and
    several 1m EMAs across the whole history. G does not consume those fields;
    excluding that unused work keeps a daily dashboard replay responsive.
    """
    equity_five = hybrid.aggregate_equity_one_minute_to_five_minute(minute)
    if equity_five.empty:
        return pd.DataFrame()
    five = hybrid.join_equity_price_with_futures_oi(equity_five, future)
    signal_times, _, _ = _slot_times(day, signal_clocks)
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
    # Point-in-time 10:36 entry extension for the G3 10:35 LONG/SHORT setups.
    # Compute before selecting the confirmation row so the EMA and ATR see
    # completed history through that row, never subsequent candles.
    if (five.confirmation_ts.dt.strftime("%H:%M") == "10:36").any():
        one_close = pd.to_numeric(minute.close, errors="coerce")
        previous_close = one_close.shift()
        true_range = pd.concat([minute.high - minute.low,
            (minute.high - previous_close).abs(),
            (minute.low - previous_close).abs()], axis=1).max(axis=1)
        confirmation["v9_1m_ema20"] = one_close.ewm(span=20, adjust=False).mean()
        confirmation["v9_1m_atr14"] = true_range.rolling(14, min_periods=14).mean()
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


def build_day_features(day: date, *, roots: DataRoots, signal_clocks=None) -> dict:
    """Rebuild the complete pre-quota pool from one sealed dated universe."""
    problems, coverage, excluded, observed, pools, minutes = [], [], [], [], [], {}
    result = dict(day=day, problems=problems, coverage_rows=coverage,
                  excluded_stocks=excluded, signals=_empty_signals(),
                  opening_0920_exclusions=[], feature_ledger=pd.DataFrame(), minutes=minutes, universe_count=0)
    try:
        universe_path = roots.universe / f"near_month_{day}.parquet"
        full = pd.read_parquet(universe_path)
        stocks = full.loc[~full.is_index_future.fillna(False).astype(bool)]
        expiry = pd.to_datetime(stocks.expiry, errors="coerce").dropna().unique()
        if len(expiry) != 1:
            raise ValueError("Dated stock universe must have exactly one expiry")
        month = pd.Timestamp(expiry[0]).strftime("%y%b").upper()
        mapped, proof = provenance.load_backtest_universe(
            universe_path=universe_path, universe_date=day,
            contract_month_contains=month, require_persisted_mapping=True)
        if mapped.equity_symbol.duplicated().any() or mapped.futures_tradingsymbol.duplicated().any():
            raise ValueError("Duplicate dated stock/equity mapping")
        result.update(universe_count=len(mapped), universe_proof=proof)
        nifty = full.loc[full.underlying.astype(str).str.upper().eq("NIFTY")]
        if len(nifty) != 1 or pd.Timestamp(nifty.iloc[0].expiry) != pd.Timestamp(expiry[0]):
            raise ValueError("Missing/ambiguous dated near-month NIFTY")
        symbol = str(nifty.iloc[0].tradingsymbol)
        future = _load_future(roots.futures_5m / f"{common.safe_contract_stem(symbol)}_5minute.parquet", day, problems, symbol)
        bar = future.loc[future.ts.eq(pd.Timestamp(slot_datetime(day, "09:20")))]
        if len(bar) != 1 or not np.isfinite(bar[["open", "close"]].to_numpy(float)).all() or (bar[["open", "close"]] <= 0).any().any():
            raise ValueError("Missing/invalid exact 09:20 NIFTY context")
        nifty_return = (float(bar.iloc[0].close) / float(bar.iloc[0].open) - 1.) * 100.
    except (OSError, ValueError, KeyError, TypeError) as exc:
        _problem(problems, "UNIVERSE_OR_NIFTY", "REQUIRED_SOURCE_UNAVAILABLE", detail=str(exc))
        return result
    for number, contract in enumerate(mapped.to_dict("records"), 1):
        symbol = hybrid.resolve_backtest_equity_symbol(str(contract["equity_symbol"]), root=roots.equity_1m)
        future_symbol = str(contract["futures_tradingsymbol"])
        try:
            minute = _load_minute(hybrid.equity_one_minute_path(symbol, roots.equity_1m), day, problems, symbol)
            future = _load_future(roots.futures_5m / f"{common.safe_contract_stem(future_symbol)}_5minute.parquet", day, problems, symbol)
            start = len(problems)
            coverage.append(_coverage(minute, future, day, symbol, problems, signal_clocks=signal_clocks))
            reasons = {p["reason"] for p in problems[start:]}
            if reasons & EXCLUDABLE_COVERAGE_REASONS:
                excluded.append(dict(symbol=symbol, reasons=sorted(reasons & EXCLUDABLE_COVERAGE_REASONS)))
                continue
            minutes[symbol] = minute.loc[minute.ts.dt.date.eq(day)].copy()
            pool = _observed_pool(minute, future, day=day, symbol=symbol, future_symbol=future_symbol,
                                  month=month, signal_clocks=signal_clocks)
            if len(pool) != len(config.SIGNAL_TO_CONFIRMATION if signal_clocks is None else signal_clocks):
                _problem(problems, symbol, "INCOMPLETE_REQUIRED_FIVE_MINUTE_BAR_CONSTRUCTION", observed=len(pool))
            if pool.empty:
                continue
            opening_mask = pool.signal_ts.dt.strftime("%H:%M").eq("09:20")
            if opening_mask.any():
                opening = opening_inputs.build_opening_features(pool.loc[opening_mask].iloc[0], future,
                    day=day, symbol=symbol, contract=future_symbol, expiry=expiry[0], root=roots.opening_observer)
                for key, value in opening.items():
                    if key not in pool:
                        pool[key] = None
                    pool.loc[opening_mask, key] = value
                if opening["opening_0920_status"] != "VALID":
                    opening_setups = [s for s in config.ACTIVE_SETUPS if s.signal_end == "09:20"]
                    result["opening_0920_exclusions"].append(dict(symbol=symbol,
                        reason="MISSING_OR_INVALID_0920_OPENING_OI",
                        setup_id=opening_setups[0].setup_id if len(opening_setups) == 1 else "",
                        setup_ids=[s.setup_id for s in opening_setups],
                        sides=[s.side for s in opening_setups]))
            pool["instrument_token"] = int(contract["equity_instrument_token"])
            pool["futures_instrument_token"] = int(contract["futures_instrument_token"])
            pool["exchange"] = "NSE"
            pool["nifty_first_bar_return_pct"] = nifty_return
            observed.append(pool)
            pools.append(_strict_signals(pool, nifty_return))
        except (OSError, ValueError, KeyError, TypeError, AssertionError) as exc:
            _problem(problems, symbol, "STOCK_SOURCE_BUILD_FAILED", detail=str(exc))
        if number % 25 == 0 or number == len(mapped):
            print(f"[G3 raw replay] {day}: reconstructed {number}/{len(mapped)}", flush=True)
    if pools:
        signals = pd.concat(pools, ignore_index=True).sort_values(["tradingsymbol", "signal_ts", "side"], kind="stable").reset_index(drop=True)
        signals["sid"] = np.arange(len(signals), dtype=int)
        _assert_day(signals, day)
        result["signals"] = signals
    result["feature_ledger"] = pd.concat(observed, ignore_index=True) if observed else pd.DataFrame(columns=["day", "signal_ts", "tradingsymbol"])
    ledger = result["feature_ledger"]
    if "opening_0920_status" not in ledger or not ledger.opening_0920_status.eq("VALID").any():
        _problem(problems, "UNIVERSE", "NO_VALID_0920_OPENING_INPUTS")
    return result


def selected_paths(orders: pd.DataFrame, minutes: dict, day: date) -> dict:
    paths = {}
    for row in orders.itertuples(index=False):
        frame = minutes[row.tradingsymbol]
        frame = frame.loc[frame.ts.gt(row.confirmation_ts) & frame.ts.le(_cutoff(day))]
        paths[int(row.sid)] = {"timestamp_ns": frame.ts.astype("int64").to_numpy(),
                              **{key: frame[key].to_numpy(float) for key in ("open", "high", "low", "close")}}
    return paths
