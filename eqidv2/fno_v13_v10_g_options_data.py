"""Auditable ATM mapping and local five-minute option paths for V13-v10-G.

No broker calls, synthetic prices, alternate-strike substitutions, or writes occur
in this module. Dated option metadata is preferred; later metadata recovering the
same monthly series is explicitly marked retrospective. ``MAPPED_CAUSAL`` means
the snapshot date is no later than the trade day, not that its intraday publication
time or the underlying strategy has independently been established as causal.

Research-cache timestamps are broker bar starts. General raw-options timestamps
are bar ends with an explicit ``candle_start``. All returned timestamps are starts.
"""

from __future__ import annotations

import calendar
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common


FNO_ROOT = common.FNO_ROOT
BAR_COLUMNS = [
    "timestamp", "candle_start", "candle_end", "open", "high", "low", "close",
    "volume", "oi", "source_interval", "data_source",
]
REQUIRED_SIGNAL_COLUMNS = {
    "trade_id", "day", "side", "equity_symbol", "entry_ts", "atm_spot",
}


@dataclass
class MasterSnapshot:
    path: Path
    day: pd.Timestamp
    frame: pd.DataFrame


def _ist(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    return stamp.tz_localize(common.IST) if stamp.tzinfo is None else stamp.tz_convert(common.IST)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _last_tuesday(year: int, month: int) -> pd.Timestamp:
    day = pd.Timestamp(year, month, calendar.monthrange(year, month)[1])
    return day - pd.Timedelta(days=(day.weekday() - 1) % 7)


def intended_month(day: Any) -> str:
    """Monthly NSE stock series for the 2026 sample; actual expiry from master.

    The theoretical monthly boundary only identifies a missing series. It never
    manufactures a contract token or replaces an unavailable month with a later one.
    """
    stamp = pd.Timestamp(day).normalize().tz_localize(None)
    if stamp > _last_tuesday(stamp.year, stamp.month):
        stamp = stamp + pd.offsets.MonthBegin(1)
    return stamp.strftime("%Y-%m")


def discover_snapshots(extra_root: Path | str | None = None) -> list[MasterSnapshot]:
    candidates: set[Path] = set()
    candidates.update((FNO_ROOT / "instrument_master").glob("*.parquet"))
    v5 = FNO_ROOT / "strategy_research" / "v13_corrected_v5"
    candidates.update((v5 / "derivative_market_data" / "instrument_master").glob("*.parquet"))
    candidates.update((v5 / "daily_options").glob("**/instrument_master/*.parquet"))
    candidates.update((FNO_ROOT / "strategy_research" / "v6_derivative_backtests" / "market_cache").glob("*master*.parquet"))
    if extra_root is not None:
        extra = Path(extra_root)
        candidates.update(extra.glob("**/*master*.parquet"))
    snapshots = []
    for path in sorted(candidates):
        frame = pd.read_parquet(path)
        if "instrument_type" not in frame:
            continue
        frame = frame.loc[frame.instrument_type.isin(["CE", "PE"])].copy()
        if frame.empty:
            continue
        if "underlying" not in frame and "name" in frame:
            frame["underlying"] = frame["name"]
        required = {"underlying", "expiry", "strike", "lot_size", "tick_size", "instrument_token", "tradingsymbol"}
        if not required.issubset(frame.columns):
            continue
        dated = pd.to_datetime(frame.get("master_date", pd.Series(dtype=str)), errors="coerce").dropna()
        match = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
        if len(dated):
            day = pd.Timestamp(dated.max()).tz_localize(None).normalize()
        elif match:
            day = pd.Timestamp(match.group(1))
        else:
            # An undated "latest" master cannot establish historical availability.
            continue
        frame["expiry"] = pd.to_datetime(frame.expiry, errors="coerce").dt.normalize()
        frame["_expiry_month"] = frame.expiry.dt.strftime("%Y-%m")
        frame["underlying"] = frame.underlying.astype(str).str.upper()
        for column in ["strike", "lot_size", "tick_size", "instrument_token"]:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.loc[
            frame[list(required - {"tradingsymbol", "underlying"})].notna().all(axis=1)
            & frame.lot_size.gt(0) & frame.tick_size.gt(0) & frame.strike.gt(0)
        ].copy()
        snapshots.append(MasterSnapshot(path, day, frame))
    return sorted(snapshots, key=lambda x: (x.day, str(x.path)))


def map_signal(signal: dict[str, Any], snapshots: list[MasterSnapshot]) -> dict[str, Any]:
    side = str(signal["side"]).upper()
    if side not in {"LONG", "SHORT"}:
        raise ValueError(f"Unsupported signal side: {side}")
    option_type = "CE" if side == "LONG" else "PE"
    day = pd.Timestamp(signal["day"]).tz_localize(None).normalize()
    month = intended_month(day)
    result = {
        **signal, "option_type": option_type, "position_side": "BUY",
        "required_expiry_month": month, "option_symbol": "", "instrument_token": pd.NA,
        "expiry": pd.NaT, "strike": np.nan, "lot_size": pd.NA, "tick_size": np.nan,
        "atm_distance": np.nan, "metadata_source": "", "metadata_date": "",
        "metadata_retrospective": False, "mapping_status": "MISSING_MONTHLY_OPTION_METADATA",
    }
    if ("signal_status" in signal and str(signal["signal_status"]) != "READY") or pd.isna(signal["entry_ts"]):
        result["mapping_status"] = "UNDERLYING_UNFILLED"
        return result
    spot = float(signal["atm_spot"])
    if not np.isfinite(spot) or spot <= 0:
        raise ValueError(f"Invalid ATM spot for {signal['trade_id']}: {spot}")
    ordered = sorted(
        snapshots,
        key=lambda x: (x.day > day, -(x.day.value) if x.day <= day else x.day.value, str(x.path)),
    )
    for snapshot in ordered:
        series = snapshot.frame.loc[
            snapshot.frame.underlying.eq(str(signal["equity_symbol"]).upper())
            & snapshot.frame.instrument_type.eq(option_type)
        ].copy()
        requested = series.loc[series["_expiry_month"].eq(month)]
        resolved_month = month
        if not requested.empty and requested.expiry.max() < day:
            # An actual holiday-adjusted expiry can precede last Tuesday.
            resolved_month = (pd.Timestamp(month + "-01") + pd.offsets.MonthBegin(1)).strftime("%Y-%m")
        candidates = series.loc[series["_expiry_month"].eq(resolved_month) & series.expiry.ge(day)].copy()
        if candidates.empty:
            continue
        # Stocks have one monthly series; nearest unexpired actual date handles
        # exchange holiday adjustments without constructing expiry dates.
        expiry = candidates.expiry.min()
        candidates = candidates.loc[candidates.expiry.eq(expiry)].copy()
        candidates["_distance"] = (candidates.strike - spot).abs()
        chosen = candidates.sort_values(["_distance", "strike", "tradingsymbol"], kind="stable").iloc[0]
        retrospective = snapshot.day > day
        result.update({
            "option_symbol": str(chosen.tradingsymbol), "instrument_token": int(chosen.instrument_token),
            "expiry": pd.Timestamp(chosen.expiry), "strike": float(chosen.strike),
            "lot_size": int(chosen.lot_size), "tick_size": float(chosen.tick_size),
            "atm_distance": float(chosen._distance), "metadata_source": str(snapshot.path),
            "metadata_date": snapshot.day.strftime("%Y-%m-%d"),
            "metadata_retrospective": bool(retrospective),
            "mapping_status": "MAPPED_RETROSPECTIVE_METADATA" if retrospective else "MAPPED_CAUSAL",
            "required_expiry_month": resolved_month,
        })
        return result
    return result


def candle_roots(extra_root: Path | str | None = None) -> list[tuple[Path, int]]:
    """Earlier roots win exact timestamp duplicates; no available-strike search."""
    roots: list[tuple[Path, int]] = []
    if extra_root is not None:
        extra = Path(extra_root)
        roots.extend([(extra / "raw_options_1m", 1), (extra / "raw_options_5m", 5)])
        if extra.name in {"raw_options_1m", "raw_options_5m"}:
            roots.insert(0, (extra, 1 if extra.name.endswith("1m") else 5))
    v5 = FNO_ROOT / "strategy_research" / "v13_corrected_v5"
    roots.append((v5 / "derivative_market_data" / "raw_options_1m", 1))
    roots.extend((p, 1) for p in sorted((v5 / "daily_options").glob("**/raw_options_1m")))
    legacy_cache = FNO_ROOT / "strategy_research" / "v6_derivative_backtests" / "market_cache"
    roots.extend((p, 1) for p in sorted(legacy_cache.glob("20??-??-??")) if p.is_dir())
    roots.extend([(FNO_ROOT / "raw_options_1m", 1), (FNO_ROOT / "raw_options_5m", 5)])
    return list(dict.fromkeys(roots))


def normalize_local_candles(frame: pd.DataFrame, *, minutes: int, source: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=BAR_COLUMNS)
    out = frame.copy()
    if "candle_start" in out:
        starts = common._to_ist(out.candle_start)
    elif "timestamp" in out:
        versions = " ".join(out.get("data_version", pd.Series(dtype=str)).astype(str).unique())
        # General store without its explicit start field must not be misread.
        if "fno_options_raw_" in versions:
            raise ValueError(f"Bar-end options source lacks candle_start: {source}")
        starts = common._to_ist(out.timestamp)
    elif "date" in out:
        starts = common._to_ist(out.date)
    else:
        raise ValueError(f"Option source has no timestamp: {source}")
    out["timestamp"] = starts
    for column in ["open", "high", "low", "close", "volume", "oi"]:
        out[column] = pd.to_numeric(out[column], errors="coerce") if column in out else np.nan
    valid = out.timestamp.notna() & out.volume.ge(0)
    valid &= out[["open", "high", "low", "close"]].notna().all(axis=1)
    valid &= out[["open", "high", "low", "close"]].gt(0).all(axis=1)
    valid &= out.high.ge(out[["open", "close", "low"]].max(axis=1))
    valid &= out.low.le(out[["open", "close", "high"]].min(axis=1))
    valid &= out.timestamp.dt.time.ge(pd.Timestamp("09:15").time())
    valid &= out.timestamp.dt.time.lt(pd.Timestamp("15:30").time())
    out = out.loc[valid].copy()
    if minutes == 5:
        out = out.loc[out.timestamp.eq(out.timestamp.dt.floor("5min"))].copy()
    out["candle_start"] = out.timestamp
    out["candle_end"] = out.timestamp + pd.Timedelta(minutes=minutes)
    out["source_interval"] = "NATIVE_5M" if minutes == 5 else "NATIVE_1M"
    out["data_source"] = source
    return out[BAR_COLUMNS].drop_duplicates("timestamp", keep="first").sort_values("timestamp").reset_index(drop=True)


def aggregate_exact_five_minutes(minute_bars: pd.DataFrame) -> pd.DataFrame:
    if minute_bars.empty:
        return pd.DataFrame(columns=BAR_COLUMNS)
    rows = []
    for start, group in minute_bars.groupby(minute_bars.timestamp.dt.floor("5min"), sort=True):
        group = group.sort_values("timestamp")
        expected = pd.date_range(start, periods=5, freq="1min")
        if len(group) != 5 or not pd.DatetimeIndex(group.timestamp).equals(expected):
            continue
        rows.append({
            "timestamp": start, "candle_start": start, "candle_end": start + pd.Timedelta(minutes=5),
            "open": float(group.open.iloc[0]), "high": float(group.high.max()),
            "low": float(group.low.min()), "close": float(group.close.iloc[-1]),
            "volume": float(group.volume.sum()), "oi": float(group.oi.iloc[-1]),
            "source_interval": "EXACT_5X_1M", "data_source": "|".join(sorted(set(group.data_source))),
        })
    return pd.DataFrame(rows, columns=BAR_COLUMNS)


def _conflict_audit(winner: pd.DataFrame, later: pd.DataFrame, *, kind: str) -> dict[str, Any] | None:
    """Report disagreement while preserving explicit source precedence."""
    if winner.empty or later.empty:
        return None
    joined = winner.merge(later, on="timestamp", suffixes=("_winner", "_other"))
    if joined.empty:
        return None
    fields = []
    masks = []
    for column in ["open", "high", "low", "close", "volume", "oi"]:
        mask = ~np.isclose(joined[column + "_winner"], joined[column + "_other"], rtol=1e-7, atol=1e-7, equal_nan=True)
        if mask.any():
            fields.append(column)
            masks.append(mask)
    if not masks:
        return None
    conflicts = joined.loc[np.logical_or.reduce(masks)]
    return {
        "kind": kind, "conflicted_bars": len(conflicts), "fields": fields,
        "winning_sources": sorted(set(conflicts.data_source_winner)),
        "other_sources": sorted(set(conflicts.data_source_other)),
        "first_conflict": str(conflicts.timestamp.min()), "last_conflict": str(conflicts.timestamp.max()),
        "resolution": "EXPLICIT_SOURCE_PRIORITY; COMPLETE_1M_AGGREGATE_BEFORE_NATIVE_5M",
        "examples": [{
            "timestamp": str(row["timestamp"]),
            "winner": {col: None if pd.isna(row[col + "_winner"]) else float(row[col + "_winner"]) for col in fields},
            "other": {col: None if pd.isna(row[col + "_other"]) else float(row[col + "_other"]) for col in fields},
        } for row in conflicts.head(3).to_dict("records")],
    }


def _combine_with_audit(parts: list[pd.DataFrame], sources: list[dict[str, Any]]) -> pd.DataFrame:
    combined = pd.DataFrame(columns=BAR_COLUMNS)
    for part in parts:
        if combined.empty:
            combined = part.copy()
            continue
        conflict = _conflict_audit(combined, part, kind="DUPLICATE_SOURCE_CONFLICT")
        if conflict:
            sources.append(conflict)
        combined = pd.concat([combined, part], ignore_index=True).drop_duplicates("timestamp", keep="first")
    return combined.sort_values("timestamp").reset_index(drop=True)


def load_contract_five_minutes(
    option_symbol: str, roots: list[tuple[Path, int]],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    parts: dict[int, list[pd.DataFrame]] = {1: [], 5: []}
    sources = []
    seen = set()
    invalid_sources: list[tuple[str, int, pd.DatetimeIndex]] = []
    stem = common.safe_contract_stem(option_symbol)
    for root, minutes in roots:
        suffixes = ["1minute", "minute", "1m"] if minutes == 1 else ["5minute", "5m"]
        for suffix in suffixes:
            path = root / f"{stem}_{suffix}.parquet"
            if path in seen or not path.is_file():
                continue
            seen.add(path)
            raw = pd.read_parquet(path)
            normalized = normalize_local_candles(raw, minutes=minutes, source=str(path))
            time_column = "candle_start" if "candle_start" in raw else ("timestamp" if "timestamp" in raw else "date")
            raw_starts = common._to_ist(raw[time_column]) if time_column in raw else pd.Series(dtype="datetime64[ns, Asia/Kolkata]")
            regular = raw_starts.loc[raw_starts.notna() & raw_starts.dt.time.ge(pd.Timestamp("09:15").time()) & raw_starts.dt.time.lt(pd.Timestamp("15:30").time())]
            invalid = pd.DatetimeIndex(regular).difference(pd.DatetimeIndex(normalized.timestamp))
            if len(invalid):
                invalid_sources.append((str(path), minutes, invalid))
            if "tradingsymbol" in raw and not raw.tradingsymbol.dropna().astype(str).eq(option_symbol).all():
                raise ValueError(f"Option symbol mismatch inside {path}")
            if not normalized.empty:
                parts[minutes].append(normalized)
            sources.append({
                "kind": "CANDLES", "path": str(path), "sha256": _sha256(path),
                "option_symbol": option_symbol, "minutes": minutes, "raw_rows": len(raw),
                "usable_rows": len(normalized),
                "invalid_session_bars": len(invalid),
                "timestamp_convention": "EXPLICIT_CANDLE_START" if "candle_start" in raw else "BROKER_BAR_START",
            })
    minutes = _combine_with_audit(parts[1], sources)
    native = _combine_with_audit(parts[5], sources)
    aggregate = aggregate_exact_five_minutes(minutes)
    conflict = _conflict_audit(aggregate, native, kind="AGGREGATED_1M_VS_NATIVE_5M_CONFLICT")
    if conflict:
        sources.append(conflict)
    pieces = [part for part in (aggregate, native) if not part.empty]
    merged = pd.concat(pieces, ignore_index=True).drop_duplicates("timestamp", keep="first").sort_values("timestamp").reset_index(drop=True) if pieces else pd.DataFrame(columns=BAR_COLUMNS)
    for path, interval, invalid in invalid_sources:
        same_interval = minutes if interval == 1 else native
        recovered = same_interval.loc[same_interval.timestamp.isin(invalid)]
        fallback = merged.loc[merged.timestamp.isin(invalid.floor("5min")) & merged.source_interval.eq("NATIVE_5M")] if interval == 1 else pd.DataFrame()
        if not recovered.empty or not fallback.empty:
            sources.append({
                "kind": "INVALID_SOURCE_BAR_REPLACED", "invalid_source": path,
                "invalid_interval_minutes": interval, "replacement_same_interval_bars": len(recovered),
                "native_5m_fallback_intervals": len(fallback),
                "replacement_sources": sorted(set(recovered.data_source)) if not recovered.empty else [],
                "native_5m_fallback_sources": sorted(set(fallback.data_source)) if not fallback.empty else [],
                "resolution": "VALID_ALTERNATE_SOURCE_WITH_EXPLICIT_AUDIT; NO_SYNTHETIC_BARS",
            })
    return merged, sources


def map_and_load(
    signals: pd.DataFrame, *, extra_root: Path | str | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], list[dict[str, Any]]]:
    missing = REQUIRED_SIGNAL_COLUMNS - set(signals.columns)
    if missing:
        raise ValueError(f"Missing signal columns: {sorted(missing)}")
    if signals.trade_id.astype(str).duplicated().any():
        raise ValueError("trade_id must be unique")
    snapshots = discover_snapshots(extra_root)
    roots = candle_roots(extra_root)
    cache: dict[str, pd.DataFrame] = {}
    paths: dict[str, pd.DataFrame] = {}
    mapped = []
    sources: list[dict[str, Any]] = []
    used_master_paths: set[Path] = set()
    for signal in signals.to_dict("records"):
        row = map_signal(signal, snapshots)
        row.update({"data_status": "NOT_MAPPED", "data_timestamp_convention": "BAR_START", "available_day_bars": 0, "required_bars": 0, "missing_required_bars": 0})
        if row["option_symbol"]:
            used_master_paths.add(Path(row["metadata_source"]))
            symbol = row["option_symbol"]
            if symbol not in cache:
                cache[symbol], used_sources = load_contract_five_minutes(symbol, roots)
                sources.extend(used_sources)
            bars = cache[symbol]
            day = _ist(signal["day"]).date()
            day_bars = bars.loc[bars.timestamp.dt.date.eq(day)].copy() if not bars.empty else bars.copy()
            # Entry is synchronized to the next five-minute boundary; require
            # every remaining session bar so missing prices cannot become wins.
            start = _ist(signal["entry_ts"]).ceil("5min")
            end = _ist(f"{day} 15:25")
            required = pd.date_range(start, end, freq="5min")
            present = pd.DatetimeIndex(day_bars.timestamp)
            absent = required.difference(present)
            row.update({
                "available_day_bars": len(day_bars), "required_bars": len(required),
                "missing_required_bars": len(absent), "first_required_bar": start,
                "last_required_bar": end,
                "data_status": "COMPLETE_5M_PATH" if len(absent) == 0 else ("MISSING_ATM_DAY_CANDLES" if day_bars.empty else "INCOMPLETE_5M_PATH"),
            })
            paths[str(signal["trade_id"])] = day_bars.reset_index(drop=True)
        mapped.append(row)
    sources.extend({"kind": "INSTRUMENT_MASTER", "path": str(path), "sha256": _sha256(path)} for path in sorted(used_master_paths))
    return pd.DataFrame(mapped), paths, sources


def build_fetch_plan(mapped: pd.DataFrame) -> pd.DataFrame:
    """One exact contract/day request per incomplete path, including pre-entry bars."""
    columns = ["option_symbol", "instrument_token", "underlying", "expiry", "strike", "instrument_type", "lot_size", "tick_size", "day", "from_date", "to_date", "interval", "linked_trade_ids"]
    rows = []
    if mapped.empty:
        return pd.DataFrame(columns=columns)
    selected = mapped.loc[mapped.option_symbol.ne("") & mapped.data_status.ne("COMPLETE_5M_PATH")]
    for (symbol, day), group in selected.groupby(["option_symbol", "day"], sort=True):
        item = group.iloc[0]
        rows.append({
            "option_symbol": symbol, "instrument_token": int(item.instrument_token),
            "underlying": item.equity_symbol, "expiry": item.expiry, "strike": item.strike,
            "instrument_type": item.option_type, "lot_size": int(item.lot_size), "tick_size": item.tick_size,
            "day": str(day), "from_date": _ist(f"{day} 09:15"), "to_date": _ist(f"{day} 15:30"),
            "interval": "minute", "linked_trade_ids": "|".join(group.trade_id.astype(str)),
        })
    return pd.DataFrame(rows, columns=columns)
