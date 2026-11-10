"""Causal stretch and breakout-position features for isolated G3 V24 research.

Features describe completed confirmation candles, never trade outcomes. The
file-backed adapter uses the same sealed, hash-verified cash history as V21/G7.
Missing features produce an explicit neutral row, not an eligibility veto.
"""
from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104 import stock_features


IST = "Asia/Kolkata"
VERSION = "G3_V24_COMPLETED_CONFIRMATION_STRETCH_STRUCTURE_V2"
NUMERIC_FIELDS = (
    "v24_ema20_1m", "v24_atr14_1m", "v24_extension_atr",
    "v24_prior_session_high", "v24_prior_session_low", "v24_prior_level",
    "v24_breakout_clearance_atr", "v24_room_atr", "v24_room_pct",
    "v24_room_target_fraction", "v24_confirmation_volume_ratio",
)
OHLCV = ("open", "high", "low", "close", "volume")
FLAGS = ("gap_filled", "opening_snapshot", "provisional_stale", "is_synthetic")


def _stamp(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("V24 confirmation timestamp must be timezone aware")
    return stamp.tz_convert(IST)


def _number(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def _blank(row: dict) -> dict:
    stamp = _stamp(row["confirmation_ts"])
    if str(row["day"])[:10] != str(stamp.date()) or row["side"] not in ("LONG", "SHORT"):
        raise ValueError("V24 candidate identity is invalid")
    value = dict(sid=int(row["sid"]), day=str(stamp.date()), setup_id=row.get("setup_id", ""),
        tradingsymbol=str(row["tradingsymbol"]), side=row["side"],
        confirmation_ts=stamp.isoformat(), v24_available_at=stamp.isoformat(),
        v24_feature_version=VERSION, v24_feature_ready=False,
        v24_feature_status="MISSING_CONFIRMATION", v24_close_breakout=False,
        v24_trigger_breakout=False, v24_history_bars=0, v24_prior_session_bars=0)
    value.update({key: math.nan for key in NUMERIC_FIELDS})
    return value


def _prepared_values(history: pd.DataFrame) -> pd.DataFrame:
    """One forward-only calculation per source file; no backfills or centering."""
    if not isinstance(history, pd.DataFrame):
        raise ValueError("V24 history must be a DataFrame")
    if history.empty:
        return pd.DataFrame()
    column = next((key for key in ("ts", "date", "timestamp") if key in history), None)
    if column is None or not set(OHLCV).issubset(history):
        raise ValueError("V24 history requires timestamps and OHLCV")
    values = history.copy()
    values["ts"] = pd.to_datetime(values[column], utc=True, errors="raise").dt.tz_convert(IST).dt.as_unit("ns")
    values = values.sort_values("ts", kind="stable").reset_index(drop=True)
    if values.ts.isna().any() or values.ts.duplicated().any():
        raise ValueError("V24 history contains missing or duplicate timestamps")
    numeric = values[list(OHLCV)].apply(pd.to_numeric, errors="coerce")
    values[list(OHLCV)] = numeric
    bad = (~np.isfinite(numeric).all(axis=1) | numeric[["open", "high", "low", "close"]].le(0).any(axis=1)
        | numeric.volume.lt(0) | numeric.high.lt(numeric[["open", "low", "close"]].max(axis=1))
        | numeric.low.gt(numeric[["open", "high", "close"]].min(axis=1)))
    flagged = pd.Series(False, index=values.index)
    for name in FLAGS:
        if name in values:
            flag = values[name]
            if flag.dtype == object:
                flag = flag.map(lambda x: str(x).strip().lower() in ("true", "1", "yes") if pd.notna(x) else False)
            flagged |= flag.fillna(False).astype(bool)
    # A malformed numerical sample contaminates a continuous EMA. It remains
    # explicitly unavailable; flags on otherwise numerical old history are
    # checked in the recent warm-up and the current session below.
    values["_invalid_prefix"] = bad.cumsum().gt(0)
    values["_recent_flagged"] = flagged.rolling(20, min_periods=1).max().astype(bool)
    values["_history_bars"] = np.arange(1, len(values) + 1)
    close = numeric.close
    previous = close.shift()
    tr = pd.concat([numeric.high-numeric.low, (numeric.high-previous).abs(),
                    (numeric.low-previous).abs()], axis=1).max(axis=1)
    values["_ema20"] = close.ewm(span=20, adjust=False).mean()
    values["_atr14"] = tr.rolling(14, min_periods=14).mean()
    values["_day"] = values.ts.dt.strftime("%Y-%m-%d")
    session = values.groupby("_day", sort=False)
    values["_session_count"] = session.cumcount() + 1
    values["_session_first"] = session.ts.transform("first")
    minute_gap = values.ts.diff().ne(pd.Timedelta(minutes=1)) & values._day.eq(values._day.shift())
    values["_session_gaps"] = minute_gap.groupby(values._day).cumsum()
    values["_session_flagged"] = (bad | flagged).groupby(values._day).cumsum().gt(0)
    # Shift inside the session: previous day's extremes and current candle's
    # high/low cannot enter the level against which confirmation is assessed.
    values["_prior_high"] = session.high.transform(lambda x: x.cummax().shift())
    values["_prior_low"] = session.low.transform(lambda x: x.cummin().shift())
    return values.set_index("ts", drop=False)


def _features_from_prepared(prepared: pd.DataFrame, row: dict) -> dict:
    result = _blank(row)
    stamp = _stamp(row["confirmation_ts"])
    if prepared.empty or stamp not in prepared.index:
        return result
    observed = prepared.loc[stamp]
    result.update(v24_history_bars=int(observed._history_bars),
                  v24_prior_session_bars=int(observed._session_count)-1)
    for key in OHLCV:
        expected = _number(row.get("confirmation_"+key))
        if not math.isfinite(expected) or not np.isclose(expected, observed[key], atol=.001, rtol=0):
            raise ValueError(f"V24 source/confirmation {key} mismatch: {row['sid']}")
    if observed._invalid_prefix or observed._recent_flagged or observed._session_flagged:
        result["v24_feature_status"] = "INVALID_OR_FLAGGED_CAUSAL_SOURCE"
        return result
    first = stamp.normalize() + pd.Timedelta(hours=9, minutes=16)
    expected_count = int((stamp-first).total_seconds()/60)+1
    if observed._session_first != first or observed._session_gaps or observed._session_count != expected_count:
        result["v24_feature_status"] = "INCOMPLETE_CURRENT_SESSION"
        return result
    ema, atr = float(observed._ema20), float(observed._atr14)
    if observed._history_bars < 20 or not math.isfinite(ema) or not math.isfinite(atr) or atr <= 0:
        result["v24_feature_status"] = "INSUFFICIENT_OR_FLAT_WARMUP"
        return result
    high, low = float(observed._prior_high), float(observed._prior_low)
    if not all(math.isfinite(x) for x in (high, low)):
        result["v24_feature_status"] = "MISSING_PRIOR_SESSION_LEVEL"
        return result
    sign = 1 if row["side"] == "LONG" else -1
    level = high if sign == 1 else low
    trigger = _number(row.get("trigger", row.get("trigger_price")))
    if not math.isfinite(trigger) or trigger <= 0:
        result["v24_feature_status"] = "INVALID_TRIGGER"
        return result
    clearance = sign*(float(observed.close)-level)
    room = max(0., sign*(level-trigger))
    target = _number(row.get("native_target_pct"))
    if not math.isfinite(target) or target <= 0:
        target = _number(row.get("target_pct"))
    result.update(v24_feature_ready=True, v24_feature_status="READY",
        v24_ema20_1m=ema, v24_atr14_1m=atr,
        v24_extension_atr=sign*(float(observed.close)-ema)/atr,
        v24_prior_session_high=high, v24_prior_session_low=low, v24_prior_level=level,
        v24_close_breakout=bool(clearance > 1e-9),
        v24_trigger_breakout=bool(sign*(trigger-level) > 1e-9),
        v24_breakout_clearance_atr=clearance/atr, v24_room_atr=room/atr,
        v24_room_pct=room/trigger*100,
        v24_room_target_fraction=room/(trigger*target/100) if math.isfinite(target) and target > 0 else math.nan,
        v24_confirmation_volume_ratio=_number(row.get("v9_1m_volume_ratio")))
    return result


def selection_features_at(history: pd.DataFrame, row: dict) -> dict:
    """Pure single-candidate API, excluding every later row before validation."""
    stamp = _stamp(row["confirmation_ts"])
    if history.empty:
        return _blank(row)
    column = next((key for key in ("ts", "date", "timestamp") if key in history), None)
    if column is None:
        raise ValueError("V24 history has no timestamp column")
    times = pd.to_datetime(history[column], utc=True, errors="raise")
    prefix = history.loc[times.le(stamp.tz_convert("UTC"))]
    return _features_from_prepared(_prepared_values(prefix), row)


def _inventory_with_missing(orders: pd.DataFrame) -> tuple[dict, dict, list, list]:
    """Keep absent captured members neutral; never mask corrupt sealed inputs.

    The original adapter validates inventory identities before looking up a
    member. Its precise missing-member error is recoverable only after the
    pinned manifest independently confirms that the member is absent. An
    existing member without a hash, changed bytes, or an unavailable manifest
    remains fatal. This changes feature availability, never G3 eligibility.
    """
    try:
        grouped, source_proofs = stock_features._inventory(orders)
        return grouped, source_proofs, [], []
    except ValueError as error:
        if not str(error).startswith("Stock minute source missing: "):
            raise
    grouped, source_proofs, missing_rows, missing_proofs = {}, {}, [], []
    for (day, symbol), subset in orders.groupby(["day", "tradingsymbol"], sort=False):
        day, symbol = str(day)[:10], str(symbol)
        try:
            available, proofs = stock_features._inventory(subset)
        except ValueError as error:
            if str(error) != f"Stock minute source missing: {day} {symbol}":
                raise
            manifest, manifest_sha, members, provenance = stock_features._manifest_for_day(day)
            if members.get(symbol) is not None:
                # Existing captured metadata without a usable hash is an
                # integrity failure, not a missing optional feature.
                raise
            for row in subset.to_dict("records"):
                value = _blank(row)
                value["v24_feature_status"] = "MISSING_SEALED_SOURCE"
                value["v24_stock_source_sha256"] = None
                missing_rows.append(value)
            missing_proofs.append(dict(kind="MISSING_SEALED_SOURCE", day=day, symbol=symbol,
                snapshot_manifest=str(manifest), snapshot_manifest_sha256=manifest_sha,
                source_provenance=provenance, candidate_rows=len(subset),
                candidate_sids=subset.sid.astype(int).tolist(), candidate_days=[day],
                v24_feature_version=VERSION,
                policy="Retain candidate; original ranking for its entire day/setup"))
            continue
        for path, rows in available.items():
            if path in source_proofs and source_proofs[path]["stock_sha256"] != proofs[path]["stock_sha256"]:
                raise ValueError(f"Conflicting stock snapshot hashes: {path}")
            grouped.setdefault(path, []).extend(rows)
            source_proofs.setdefault(path, proofs[path])
    return grouped, source_proofs, missing_rows, missing_proofs


def build_selection_features(orders: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """One feature row per input candidate from sealed, hash-verified sources.

    Call with the full eligible pre-quota inventory, not realized winners or
    only earlier selected trades. This function preserves input inventory.
    Missing/insufficient feature rows are neutral and explicitly flagged;
    corrupt hashes and confirmation mismatches are integrity errors.
    """
    if orders.empty:
        return pd.DataFrame(columns=["sid", "v24_feature_ready", "v24_feature_status", *NUMERIC_FIELDS]), []
    grouped, source_proofs, results, proofs = _inventory_with_missing(orders)
    for path, rows in grouped.items():
        frame, proof = stock_features._prepared_frame(path, rows, source_proofs[path])
        prepared = _prepared_values(frame)
        for row in rows:
            value = _features_from_prepared(prepared, row)
            value["v24_stock_source_sha256"] = proof["stock_sha256"]
            results.append(value)
        proofs.append(dict(proof, v24_feature_version=VERSION, candidate_rows=len(rows),
            candidate_days=sorted({str(row["day"])[:10] for row in rows}), source_rows=len(frame),
            definition="Continuous EMA20/simple ATR14; prior same-session extremes exclude confirmation; no future bars"))
    audit = pd.DataFrame(results).sort_values("sid", kind="stable").reset_index(drop=True)
    if len(audit) != len(orders) or audit.sid.duplicated().any() or set(audit.sid) != set(orders.sid):
        raise ValueError("V24 feature builder changed candidate inventory")
    if not pd.to_datetime(audit.v24_available_at, utc=True).le(pd.to_datetime(audit.confirmation_ts, utc=True)).all():
        raise ValueError("V24 feature availability is later than confirmation")
    return audit, proofs
