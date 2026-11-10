"""Read-only, hash-verified G3 research inputs; this module never writes files.

Historical cached bars retain their disclosed source eligibility and universe.
Current source files are never used to silently repair a historical snapshot.
"""
from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
DATA = Path("C:/TradingData/eqidv2")
CACHE = ROOT / "outputs/g3_early_entry_20261023/full_history_source/cache"
PRODUCTION = DATA / "backtesting_result_v13_v10_g_3/production/runs"
RECOVERED = ROOT / "outputs/g7_v6_20261102/source_20261030_recovered_with_opening"
LOCAL_RECOVERED = Path(__file__).resolve().parent / "sources/2026-10-28"
SPOT = DATA / "stocks_indicators_5min_eq_live/NIFTY50_INDEX_stocks_indicators_5min.parquet"
IST = "Asia/Kolkata"
FLAGS = ("gap_filled", "opening_snapshot", "provisional_stale", "is_synthetic", "synthetic")
EXCLUDED_DAYS = {"2026-10-01": "Canonical G3 history excludes incomplete immutable source; old V1 completion flag is not sufficient for current slots."}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _verified_bytes(path: Path, expected: str) -> bytes:
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError(f"Source hash mismatch: {path}")
    return raw


def _child(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError(f"Source path escaped its manifest: {relative}")
    return path


def _normalize_features(frame: pd.DataFrame, day: str) -> pd.DataFrame:
    frame = frame.copy()
    for key in ("signal_ts", "confirmation_ts", "v9_5m_feature_ts", "v9_1m_feature_ts", "v9_feature_available_ts"):
        if key in frame:
            frame[key] = pd.to_datetime(frame[key], utc=True).dt.tz_convert(IST).dt.as_unit("ns")
    if not frame.signal_ts.dt.strftime("%Y-%m-%d").eq(day).all():
        raise ValueError(f"Feature date mismatch: {day}")
    frame["day"] = day
    if frame.duplicated(["tradingsymbol", "signal_ts"]).any():
        raise ValueError(f"Duplicate feature identity: {day}")
    return frame


def _normalize_minutes(frame: pd.DataFrame, day: str) -> pd.DataFrame:
    frame = frame.copy()
    field = "ts" if "ts" in frame else "date"
    frame["ts"] = pd.to_datetime(frame[field], utc=True).dt.tz_convert(IST).dt.as_unit("ns")
    start, end = (pd.Timestamp(f"{day} {clock}", tz=IST) for clock in ("09:16", "15:15"))
    frame = frame.loc[frame.ts.between(start, end)].copy()
    for flag in FLAGS:
        if flag in frame and frame[flag].astype(str).str.lower().isin(["true", "1", "1.0", "yes"]).any():
            raise ValueError(f"Flagged execution minute: {day} {flag}")
    fields = ["open", "high", "low", "close"]
    values = frame[fields].apply(pd.to_numeric, errors="coerce").to_numpy(float)
    if not np.isfinite(values).all() or (values <= 0).any():
        raise ValueError(f"Invalid OHLC in execution minutes: {day}")
    if "volume" in frame:
        volume = pd.to_numeric(frame.volume, errors="coerce").to_numpy(float)
        if not np.isfinite(volume).all() or (volume < 0).any():
            raise ValueError(f"Invalid volume in execution minutes: {day}")
    if ((frame.high < frame[["open", "low", "close"]].max(axis=1)) |
            (frame.low > frame[["open", "high", "close"]].min(axis=1))).any():
        raise ValueError(f"Inconsistent execution OHLC: {day}")
    expected = pd.date_range(start, end, freq="min").asi8
    for symbol, part in frame.groupby("tradingsymbol", sort=False):
        actual = pd.DatetimeIndex(part.ts.sort_values()).asi8
        if not np.array_equal(actual, expected):
            raise ValueError(f"Incomplete/duplicate execution minutes: {day} {symbol} ({len(part)}/360)")
    return frame.sort_values(["tradingsymbol", "ts"]).reset_index(drop=True)


def _daily_runs() -> dict[str, Path]:
    result = {}
    if PRODUCTION.is_dir():
        for folder in sorted(PRODUCTION.iterdir()):
            if not folder.is_dir():
                continue
            if all((CACHE / folder.name / name).is_file() for name in
                   ("features.parquet", "minutes.parquet", "provenance.json")):
                # Archived research cache is the pinned source for these days;
                # multiple historical production profile versions are irrelevant.
                continue
            paths = sorted(folder.glob("*/run_manifest.json"))
            complete = [p for p in paths if _read_json(p).get("complete") is True]
            if len(complete) > 1:
                raise ValueError(f"Ambiguous sealed daily source: {folder.name}")
            if complete:
                result[folder.name] = complete[0].parent
    if (RECOVERED / "run_manifest.json").is_file():
        manifest = _read_json(RECOVERED / "run_manifest.json")
        if manifest.get("complete") is True:
            result[manifest["session_date"]] = RECOVERED
    if (LOCAL_RECOVERED / "run_manifest.json").is_file():
        manifest = _read_json(LOCAL_RECOVERED / "run_manifest.json")
        if manifest.get("complete") is True and manifest.get("session_date") == "2026-10-28":
            result["2026-10-28"] = LOCAL_RECOVERED
    return result


def available_days(through: str = "2026-11-02") -> list[str]:
    days = {p.name for p in CACHE.iterdir() if p.is_dir() and
            all((p / name).is_file() for name in ("features.parquet", "minutes.parquet", "provenance.json"))}
    days.update(_daily_runs())
    return sorted(day for day in days if day <= str(through)[:10] and day not in EXCLUDED_DAYS)


def load_day(day: str) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Return preselection features, execution minutes, and source proof.

    Cached minute files sometimes contain OHLC only; use context_bars() for
    volume-derived features. All times are completed-bar timestamps in IST/ns.
    """
    day = str(day)[:10]
    if day in EXCLUDED_DAYS:
        raise ValueError(f"Excluded session {day}: {EXCLUDED_DAYS[day]}")
    folder = CACHE / day
    if all((folder / name).is_file() for name in ("features.parquet", "minutes.parquet", "provenance.json")):
        proof_path = folder / "provenance.json"
        proof = _read_json(proof_path)
        fp, mp = folder / "features.parquet", folder / "minutes.parquet"
        features = pd.read_parquet(io.BytesIO(_verified_bytes(fp, proof["features_sha256"])))
        minutes = pd.read_parquet(io.BytesIO(_verified_bytes(mp, proof["minutes_sha256"])))
        proof = dict(proof, feature_path=str(fp), minute_path=str(mp),
                     provenance_path=str(proof_path), provenance_sha256=sha256(proof_path),
                     loader_source="HASH_VERIFIED_PRESELECTION_RESEARCH_CACHE")
    else:
        runs = _daily_runs()
        if day not in runs:
            raise ValueError(f"No complete sealed G3 source for {day}")
        folder = runs[day]
        manifest_path = folder / "run_manifest.json"
        manifest = _read_json(manifest_path)
        if manifest.get("complete") is not True or manifest.get("execution_authority") is not False:
            raise ValueError(f"Source is not a completed nonexecuting replay: {day}")
        artifacts = manifest["artifacts"]
        feature_path = _child(folder, artifacts["feature_ledger"]["path"])
        features = pd.read_csv(io.BytesIO(_verified_bytes(feature_path, artifacts["feature_ledger"]["sha256"])), low_memory=False)
        source_path = _child(folder, artifacts["source_manifest"]["path"])
        provenance = json.loads(_verified_bytes(source_path, artifacts["source_manifest"]["sha256"]))
        snapshot_path = Path(provenance["snapshot_manifest"]).resolve()
        if not snapshot_path.is_relative_to(folder.resolve()):
            raise ValueError("Snapshot manifest escaped source run")
        snapshot = json.loads(_verified_bytes(snapshot_path, provenance["snapshot_manifest_sha256"]))
        pieces, minute_sources, excluded = [], [], []
        wanted = set(features.tradingsymbol)
        for item in snapshot["sources"]:
            if item["role"] != "EQUITY_ONE_MINUTE" or not item.get("captured"):
                continue
            path = _child(snapshot_path.parent, item["snapshot_relative_path"])
            symbol = path.name.removesuffix("_stocks_indicators_1min.parquet")
            raw = _verified_bytes(path, item["sha256"])
            names = set(pq.ParquetFile(io.BytesIO(raw)).schema.names)
            columns = ["date", "open", "high", "low", "close", "volume"] + [flag for flag in FLAGS if flag in names]
            minute = pd.read_parquet(io.BytesIO(raw), columns=columns)
            minute["tradingsymbol"] = symbol
            try:
                minute = _normalize_minutes(minute, day)
            except ValueError as error:
                if symbol in wanted:
                    raise
                excluded.append(dict(symbol=symbol, reason=str(error)))
                continue
            if minute.empty:
                if symbol in wanted:
                    raise ValueError(f"Missing selected-universe minute history: {day} {symbol}")
                excluded.append(dict(symbol=symbol, reason="NO_COMPLETE_SESSION_MINUTES"))
                continue
            pieces.append(minute[["tradingsymbol", "ts", "open", "high", "low", "close", "volume"]])
            minute_sources.append(dict(symbol=symbol, path=str(path), sha256=item["sha256"]))
        if not pieces:
            raise ValueError(f"No execution input for {day}")
        minutes = pd.concat(pieces, ignore_index=True)
        proof = dict(loader_source="HASH_VERIFIED_SEALED_DAILY_FEATURE_LEDGER_AND_RAW_SNAPSHOT",
                     day=day, feature_path=str(feature_path), features_sha256=artifacts["feature_ledger"]["sha256"],
                     run_manifest=str(manifest_path), run_manifest_sha256=sha256(manifest_path),
                     source_manifest=str(source_path), source_manifest_sha256=artifacts["source_manifest"]["sha256"],
                     snapshot_manifest=str(snapshot_path), snapshot_manifest_sha256=provenance["snapshot_manifest_sha256"],
                     minute_sources=minute_sources, excluded_execution_symbols=excluded,
                     coverage=manifest.get("coverage", {}),
                     retrospective_recovery=folder.resolve() == RECOVERED.resolve())
        if proof["retrospective_recovery"]:
            proof["recovery_link"] = _read_json(RECOVERED / "recovery_link.json")
    features = _normalize_features(features, day)
    minutes = _normalize_minutes(minutes, day)
    if not set(features.tradingsymbol).issubset(set(minutes.tradingsymbol)):
        raise ValueError(f"Feature symbols missing execution inputs: {day}")
    universe_path = DATA / f"fno_oi/universe/near_month_{day}.parquet"
    if universe_path.is_file():
        u = pd.read_parquet(universe_path)
        symbol_field = "equity_symbol" if "equity_symbol" in u else "underlying"
        stocks = u.loc[~u.is_index_future.fillna(False).astype(bool), symbol_field].dropna().astype(str)
        proof["breadth_universe"] = dict(count=int(stocks.nunique()), path=str(universe_path),
                                         sha256=sha256(universe_path), policy="DATED_FNO_STOCK_UNIVERSE")
    else:
        proof["breadth_universe"] = dict(count=int(features.tradingsymbol.nunique()),
                                         policy="ARCHIVED_AFTER_DATED_COHORT_NOT_POINT_IN_TIME")
    proof["feature_stocks"] = int(features.tradingsymbol.nunique())
    proof["minute_stocks"] = int(minutes.tradingsymbol.nunique())
    return features, minutes, proof


def context_bars(features: pd.DataFrame, minutes: pd.DataFrame) -> pd.DataFrame:
    """Full completed 5m OHLCV sequence for causal features, including gap checks.

    Cached history offers every clock through 13:30 (last strategy signal).
    Recent raw snapshots offer every clock through the 15:15 execution close.
    """
    if "volume" not in minutes:
        fields = ["tradingsymbol", "signal_ts", "open", "high", "low", "close", "volume", "source_1m_count"]
        return features[fields].copy().sort_values(["tradingsymbol", "signal_ts"]).reset_index(drop=True)
    pieces = []
    for symbol, part in minutes.groupby("tradingsymbol", sort=False):
        bars = part.set_index("ts").resample("5min", label="right", closed="right").agg(
            open=("open", "first"), high=("high", "max"), low=("low", "min"),
            close=("close", "last"), volume=("volume", "sum"), source_1m_count=("close", "count"))
        bars = bars.loc[bars.source_1m_count.eq(5)].reset_index().rename(columns={"ts": "signal_ts"})
        bars["tradingsymbol"] = symbol
        pieces.append(bars)
    return pd.concat(pieces, ignore_index=True).sort_values(["tradingsymbol", "signal_ts"]).reset_index(drop=True)


def execution_paths(orders: pd.DataFrame, minutes: pd.DataFrame) -> dict:
    """Simulator paths begin strictly after the completed confirmation candle."""
    paths = {}
    by_symbol = {symbol: part for symbol, part in minutes.groupby("tradingsymbol", sort=False)}
    for row in orders.itertuples(index=False):
        confirmed = pd.Timestamp(row.confirmation_ts)
        if confirmed.tzinfo is None:
            raise ValueError("Confirmation timestamps must be timezone-aware")
        part = by_symbol[row.tradingsymbol]
        part = part.loc[part.ts.gt(confirmed)]
        if part.empty:
            raise ValueError(f"No post-confirmation execution path for sid {row.sid}")
        paths[int(row.sid)] = dict(timestamp_ns=part.ts.astype("int64").to_numpy(),
                                  **{key: part[key].to_numpy(float) for key in ("open", "high", "low", "close")})
    return paths


def coverage_status(today: str = "2026-11-02") -> dict:
    days = available_days(today)
    weekdays = pd.bdate_range(days[0], days[-1]).strftime("%Y-%m-%d").tolist()
    evidence = []
    for path, timestamp_field, kind in (
        (SPOT, "date", "TRUE_NIFTY_50_SPOT"),
        (DATA / "fno_oi/raw_contracts_5m/NIFTY26NOVFUT_5minute.parquet", "timestamp", "NIFTY_NOVEMBER_FUTURES"),
        (DATA / "stocks_indicators_5min_eq_live/RELIANCE_stocks_indicators_5min.parquet", "date", "SAMPLE_LIVE_CASH_5M"),
        (DATA / "stocks_indicators_1min_eq/RELIANCE_stocks_indicators_1min.parquet", "date", "SAMPLE_EXECUTION_ARCHIVE_1M")):
        if not path.is_file():
            evidence.append(dict(kind=kind, path=str(path), state="MISSING"))
            continue
        raw = path.read_bytes()
        frame = pd.read_parquet(io.BytesIO(raw), columns=[timestamp_field])
        stamps = pd.to_datetime(frame[timestamp_field], utc=True).dt.tz_convert(IST)
        evidence.append(dict(kind=kind, path=str(path), sha256=hashlib.sha256(raw).hexdigest(),
                             latest_timestamp_ist=stamps.max().isoformat(),
                             requested_day_rows=int(stamps.dt.strftime("%Y-%m-%d").eq(today).sum())))
    return dict(requested_through=today, complete_backtest_through=days[-1], complete_session_count=len(days),
                days=days, weekday_dates_without_complete_source=[d for d in weekdays if d not in days],
                missing_date_note="Missing weekdays may be exchange holidays or source exclusions; not assigned zero P&L.",
                explicit_source_exclusions=EXCLUDED_DAYS,
                requested_day_state="COMPLETE_SOURCE_AVAILABLE" if today in days else "NO_COMPLETE_SEALED_CASH_MINUTE_REPLAY",
                requested_day_pnl_included=today in days, latest_observed_evidence=evidence,
                recovery_note="Oct30 uses separate reconstructed cash source (218 stocks); 204 opening OI observations are retrospective; 96 cash symbols disagreed with the stale live5m store.",
                limitations=["Earlier archived cohorts include after-dated membership and inherited full-day eligibility.",
                             "Historical opening OI reconstruction does not prove contemporaneous live receipt.",
                             "Only completed sealed sessions enter performance totals; a partial current session is not fabricated as an end-of-day result."])
