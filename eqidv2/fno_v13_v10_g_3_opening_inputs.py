"""Read-only opening evidence for the production G3 09:20 LONG setup.

The collector's isolated archive is an input, never a destination. Historical
replay snapshots the minute evidence. Runtime requires receipts already present
at evaluation, and never treats a later recovery as a live opening observation.
"""
from __future__ import annotations

from datetime import time, timedelta
import hashlib
import json
import math
from pathlib import Path
import re

import pandas as pd

from eqidv2_runtime_paths import RUNTIME_ROOT
from fno_v13_v10_g_3_opening_oi import (
    _quality, _stamp, bootstrap_0920_oi_change, bootstrap_0920_oi_change_from_quote,
)

DEFAULT_ROOT = Path(RUNTIME_ROOT) / "dashboard_flow_observer"
PRICE_BASIS = "09:15_CASH_OPEN_TO_09:20_CASH_CLOSE"


def load_futures_frame(path):
    """Retain contract, expiry, source quality and receipt (legacy loader drops them)."""
    try:
        frame = pd.read_parquet(path)
        frame["ts"] = pd.to_datetime(frame["timestamp"], utc=True).dt.tz_convert("Asia/Kolkata")
        return frame.sort_values("ts").reset_index(drop=True)
    except (OSError, KeyError, ValueError):
        return pd.DataFrame()


def minute_path(root, day, contract):
    stem = re.sub(r"[^A-Z0-9._-]+", "_", contract).strip("._") or "CONTRACT"
    if stem != contract:
        stem += "_" + hashlib.sha1(contract.encode()).hexdigest()[:8]
    return Path(root) / "historical" / str(day) / (stem + ".json")


def _read(path):
    try:
        raw = Path(path).read_bytes()
        return json.loads(raw), hashlib.sha256(raw).hexdigest()
    except (OSError, ValueError):
        return None, None


def _same_symbol(observed, symbol, contract):
    return observed == symbol or (observed == "LTM" and symbol == "LTIM"
                                  and re.fullmatch(r"LTM\d{2}[A-Z]{3}FUT", contract) is not None)


def read_first_minute(root, day, symbol, contract):
    raw, digest = _read(minute_path(root, day, contract))
    if (not isinstance(raw, dict) or raw.get("session_date") != str(day)
            or raw.get("contract") != contract or not _same_symbol(raw.get("symbol"), symbol, contract)
            or raw.get("source") != "KITE_HISTORICAL_MINUTE"):
        return None, digest
    matching = []
    for candle in raw.get("candles", []):
        if not isinstance(candle, dict):
            continue
        start, end = _stamp(candle.get("candle_start")), _stamp(candle.get("candle_end"))
        if (start is not None and end is not None and start.date() == day
                and start.time() == time(9, 15) and end - start == timedelta(minutes=1)):
            matching.append(candle)
    if len(matching) != 1:
        return None, digest
    return {**matching[0], "instrument": "FUTURES", "price_basis": "DATED_NEAR_MONTH_FUTURES",
            "contract": contract, "source": raw["source"], "acquired_at": raw.get("acquired_at")}, digest


def read_quote(root, day, symbol, contract):
    raw, digest = _read(Path(root) / "coverage" / f"live_opening_oi_{day}.json")
    if (not isinstance(raw, dict) or raw.get("schema") != "dashboard_flow_live_opening_oi_v1"
            or raw.get("session_date") != str(day) or raw.get("universe_source") != "DATED_UNIVERSE"
            or not isinstance(raw.get("rows"), list)):
        return None, digest
    matching = [row for row in raw["rows"] if isinstance(row, dict)
                and row.get("contract") == contract and _same_symbol(row.get("symbol"), symbol, contract)]
    return (matching[0] if len(matching) == 1 else None), digest


def _clean(record):
    # Optional Parquet fields may be NaN rather than absent. Non-null quality
    # flags still go through the strict pure validator.
    return {k: v for k, v in dict(record).items() if v is not None and not (isinstance(v, float) and math.isnan(v))}


def build_opening_features(cash_row, futures_frame, *, day, symbol, contract, expiry,
                           root=DEFAULT_ROOT, as_of=None):
    """Return an explicit valid feature bundle or a fail-closed status.

    ``as_of`` is mandatory at live/paper call sites. Its omission is reserved
    for after-close replay. Only this setup uses open-to-close cash movement
    and 09:16-to-09:20 OI; regular bars/indicators remain untouched.
    """
    invalid = dict(opening_0920_status="MISSING_OR_INVALID_OPENING_EVIDENCE",
                   price_change_pct=math.nan, oi_change_pct=math.nan, prev_oi=math.nan)
    boundary = pd.Timestamp(f"{day}T09:20:00+05:30")
    if as_of is not None and (_stamp(as_of) is None or _stamp(as_of).date() != day):
        return invalid
    cash = _clean(cash_row)
    try:
        cash_time = pd.Timestamp(cash.get("ts", cash.get("signal_ts")))
        if cash_time != boundary or not _quality(cash) or bool(cash.get("opening_snapshot", False)):
            return invalid
        o, c = float(cash["open"]), float(cash["close"])
        if not all(math.isfinite(x) and x > 0 for x in (o, c)) or float(cash.get("source_1m_count", 5)) != 5:
            return invalid
        stamps = pd.to_datetime(futures_frame.get("ts", futures_frame.get("timestamp")), utc=True)
        selected = futures_frame.loc[stamps.eq(boundary)]
        if len(selected) != 1:
            return invalid
        future = _clean(selected.iloc[0].to_dict())
        if (future.get("tradingsymbol") != contract or not _quality(future)
                or pd.Timestamp(future.get("expiry")).date() != pd.Timestamp(expiry).date()
                or float(future.get("source_1m_count", 5)) != 5):
            return invalid
        if "candle_start" in future and pd.Timestamp(future["candle_start"]) != boundary - pd.Timedelta(minutes=5):
            return invalid
        completed = {**future, "oi": float(future["oi"]), "contract": contract,
                     "oi_source": "NEAR_MONTH_FUTURES_5M", "received_at": future.get("fetch_timestamp")}
        first, evidence_hash = read_first_minute(root, day, symbol, contract)
        # An archive recovery after 09:20 must not replace the timely quote.
        if as_of is not None and (not first or _stamp(first.get("acquired_at")) is None
                                  or _stamp(first["acquired_at"]) >= boundary):
            first = None
        feature = bootstrap_0920_oi_change(first, completed, boundary=boundary, as_of=as_of)
        if feature is None and as_of is not None:
            quote, evidence_hash = read_quote(root, day, symbol, contract)
            feature = bootstrap_0920_oi_change_from_quote(quote, completed, boundary=boundary, as_of=as_of)
        if feature is None:
            return invalid
        return {**feature, "opening_0920_status": "VALID", "price_change_pct": (c / o - 1) * 100,
                "price_change_basis": PRICE_BASIS, "opening_evidence_sha256": evidence_hash,
                "feature_available_at_ist": str(as_of) if as_of is not None else None,
                "opening_oi_contract": contract}
    except (KeyError, TypeError, ValueError, OverflowError):
        return invalid
