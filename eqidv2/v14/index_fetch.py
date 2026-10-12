"""Read only the five opening NIFTY futures minutes for missing V14 context.

Tokens must be recorded against the exact contract in a dated mapping/master.
Expired contracts may be queried with their archived tokens; a current token
is never guessed or substituted. No continuous-contract or OI request is made.
"""
from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import shutil
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import data, index_data

IST = ZoneInfo("Asia/Kolkata")
MISSING_CONTEXT_REASONS = {"MISSING_DATED_MAPPING", "MISSING_RAW_CONTRACT_FILE", "MISSING_EXACT_0920_BAR"}


def _identity(row: pd.Series, day: str) -> tuple[str, int, str]:
    contract = index_data._contract(row.get("tradingsymbol", ""))
    expiry = pd.to_datetime(row.get("expiry"), errors="coerce")
    if not contract or pd.isna(expiry) or expiry.date() < pd.Timestamp(day).date():
        raise ValueError("INVALID_DATED_NIFTY_CONTRACT")
    if contract != f"NIFTY{expiry:%y%b}FUT".upper():
        raise ValueError("DATED_CONTRACT_EXPIRY_MISMATCH")
    token = next((row.get(name) for name in ("instrument_token", "futures_instrument_token")
                  if pd.notna(row.get(name))), None)
    try:
        number = float(token)
        if not np.isfinite(number) or number <= 0 or number != int(number):
            raise ValueError
    except (TypeError, ValueError, OverflowError):
        raise ValueError("NO_TRUSTWORTHY_ARCHIVED_NIFTY_TOKEN") from None
    return contract, int(number), expiry.date().isoformat()


def _nifty_rows(frame: pd.DataFrame, *, master: bool) -> pd.DataFrame:
    column = "underlying" if "underlying" in frame else "name" if "name" in frame else None
    if column is None:
        raise ValueError("DATED_MASTER_HAS_NO_UNDERLYING")
    rows = frame.loc[frame[column].astype(str).str.upper().eq("NIFTY")].copy()
    if master:
        if "instrument_type" in rows:
            rows = rows.loc[rows.instrument_type.astype(str).str.upper().eq("FUT")]
        if "segment" in rows:
            rows = rows.loc[rows.segment.astype(str).str.upper().eq("NFO-FUT")]
    return rows


def _mapping(day: str, root: Path, fno_root: Path) -> dict:
    near = fno_root / "universe" / f"near_month_{day}.parquet"
    masters = [fno_root / "instrument_master" / f"instrument_master_{day}.parquet",
               root / "data/instruments" / f"nfo_{day}.parquet"]
    candidates = [(near, False)] if near.is_file() else [(p, True) for p in masters if p.is_file()]
    if not candidates:
        raise ValueError("NO_EXACT_DATED_NIFTY_MAPPING_OR_MASTER")
    identities = []
    for path, master in candidates:
        digest = index_data._sha256(path)
        frame = pd.read_parquet(path)
        if index_data._sha256(path) != digest:
            raise ValueError("DATED_NIFTY_MAPPING_CHANGED_DURING_READ")
        rows = _nifty_rows(frame, master=master)
        if master and not rows.empty:
            if "expiry" not in rows:
                raise ValueError("DATED_MASTER_HAS_NO_EXPIRY")
            expiry = pd.to_datetime(rows.get("expiry"), errors="coerce")
            rows = rows.loc[expiry.notna() & expiry.dt.date.ge(pd.Timestamp(day).date())].copy()
            if not rows.empty:
                expiries = pd.to_datetime(rows.expiry, errors="coerce")
                rows = rows.loc[expiries.eq(expiries.min())]
        if len(rows) != 1:
            raise ValueError("AMBIGUOUS_OR_MISSING_DATED_NIFTY_NEAR_CONTRACT")
        contract, token, expiry = _identity(rows.iloc[0], day)
        identities.append(dict(day=day, contract=contract, instrument_token=token, expiry=expiry,
            mapping_original_source=str(path.resolve()), mapping_sha256=digest,
            mapping_kind="EXACT_DATED_MASTER_NEAREST_VALID_EXPIRY" if master else "EXACT_DATED_NEAR_MONTH_MAPPING"))
    if len({(x["contract"], x["instrument_token"], x["expiry"]) for x in identities}) != 1:
        raise ValueError("EXACT_DATED_MASTERS_DISAGREE_ON_NIFTY")
    return identities[0]


def _opening_frame(raw: list[dict], day: str, mapping: dict) -> pd.DataFrame:
    frame = pd.DataFrame(raw)
    required = {"date", "open", "high", "low", "close", "volume"}
    if required.difference(frame) or len(frame) != 5:
        raise ValueError("BROKER_DID_NOT_RETURN_EXACTLY_FIVE_OPENING_MINUTES")
    stamp = pd.to_datetime(frame.date, errors="coerce")
    stamp = stamp.dt.tz_localize(IST) if stamp.dt.tz is None else stamp.dt.tz_convert(IST)
    expected = pd.date_range(day + " 09:15", day + " 09:19", freq="min", tz=IST)
    if not np.array_equal(stamp.dt.as_unit("ns").array.asi8, expected.asi8):
        raise ValueError("BROKER_OPENING_MINUTES_MISSING_DUPLICATED_OR_MISTIMED")
    out = frame[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric, errors="coerce")
    values = out.to_numpy(float)
    o, h, low, c, volume = values.T
    if (not np.isfinite(values).all() or (values[:, :4] <= 0).any() or (volume < 0).any()
            or (h < np.maximum(o, c)).any() or (low > np.minimum(o, c)).any() or (h < low).any()):
        raise ValueError("INVALID_BROKER_OPENING_OHLCV")
    out.insert(0, "ts", stamp.dt.as_unit("ns") + pd.Timedelta(minutes=1))
    out["contract"] = mapping["contract"]
    out["instrument_token"] = mapping["instrument_token"]
    return out


def refresh_nifty_context(days, root: Path, *, fno_root: Path = index_data.DEFAULT_FNO_ROOT,
                          client=None) -> Path:
    """Refresh genuinely missing opening context after stock staging completes.

    The shared staging lock prevents concurrent stock/index fetch jobs. Valid
    raw/frozen/cache context is reused without authenticating. ``client`` exists
    for deterministic tests; production uses the read-only market-data wrapper.
    """
    root, fno_root = Path(root), Path(fno_root)
    days = sorted({pd.Timestamp(day).date().isoformat() for day in days})
    output = root / "data/index1m"
    output.mkdir(parents=True, exist_ok=True)
    records, broker_error = [], None
    with data._fetch_lock(root):
        existing = index_data.load_nifty_context(days, root=root, fno_root=fno_root)
        for row in existing.itertuples(index=False):
            day = str(row.day)
            if np.isfinite(row.nifty_first_bar_return_pct):
                records.append(dict(day=day, status="REUSED_VERIFIED_CONTEXT", source_path=row.source_path,
                                    source_sha256=row.source_sha256, quality_status=row.quality_status))
                continue
            if row.reason not in MISSING_CONTEXT_REASONS:
                records.append(dict(day=day, status="UNAVAILABLE", reason=str(row.reason)))
                continue
            try:
                mapping = _mapping(day, root, fno_root)
            except ValueError as exc:
                # A live completed session may use the current full master;
                # an older date never substitutes today's token/master.
                if str(exc) == "NO_EXACT_DATED_NIFTY_MAPPING_OR_MASTER" and day == datetime.now(IST).date().isoformat():
                    try:
                        if client is None:
                            client = data.MarketDataClient()
                        master = pd.DataFrame(client.instruments("NFO"))
                        data._parquet(root / "data/instruments" / f"nfo_{day}.parquet", master)
                        mapping = _mapping(day, root, fno_root)
                    except (OSError, ValueError, RuntimeError) as inner:
                        records.append(dict(day=day, status="UNAVAILABLE", reason=str(inner)))
                        continue
                else:
                    records.append(dict(day=day, status="UNAVAILABLE", reason=str(exc)))
                    continue
            if broker_error:
                records.append(dict(**mapping, status="UNAVAILABLE", reason=broker_error))
                continue
            try:
                if client is None:
                    client = data.MarketDataClient()
                raw = client._call("historical_data", mapping["instrument_token"],
                    f"{day} 09:15:00", f"{day} 09:19:00", "minute", continuous=False, oi=False)
                minute = _opening_frame(raw, day, mapping)
                source_snapshot = output / "mapping_sources" / (mapping["mapping_sha256"] + ".parquet")
                source_snapshot.parent.mkdir(exist_ok=True)
                if not source_snapshot.exists():
                    shutil.copyfile(mapping["mapping_original_source"], source_snapshot)
                if index_data._sha256(source_snapshot) != mapping["mapping_sha256"]:
                    raise ValueError("ARCHIVED_NIFTY_MAPPING_SNAPSHOT_CHANGED")
                source = output / f"{day}.parquet"
                data._parquet(source, minute)
                metadata = dict(schema_version=index_data.CACHE_SCHEMA, status="VALID", **mapping,
                    mapping_source=str(source_snapshot.resolve()), sha256=index_data._sha256(source),
                    fetched_at=datetime.now(IST).isoformat(), requested_interval="minute",
                    requested_from=f"{day} 09:15:00", requested_through=f"{day} 09:19:00",
                    continuous=False, oi=False, timestamp_label="end", synthetic_bars=False)
                data._json(source.with_suffix(".json"), metadata)
                verified = index_data.load_nifty_context([day], root=root, fno_root=fno_root).iloc[0]
                records.append(dict(day=day, status="FETCHED_VERIFIED" if pd.notna(verified.nifty_first_bar_return_pct) else "UNAVAILABLE",
                    reason=verified.reason, source_path=str(source), source_sha256=metadata["sha256"],
                    contract=mapping["contract"], instrument_token=mapping["instrument_token"]))
            except (OSError, ValueError, RuntimeError) as exc:
                # MarketDataClient sanitizes broker exceptions; record only
                # exception type for other failures, never arbitrary API text.
                reason = str(exc) if isinstance(exc, (ValueError, RuntimeError)) else type(exc).__name__
                if "authenticated" in reason or "TokenException" in reason or "PermissionException" in reason:
                    broker_error = reason
                records.append(dict(day=day, status="UNAVAILABLE", reason=reason,
                                    contract=mapping["contract"], instrument_token=mapping["instrument_token"]))
        path = output / "fetch_manifest.json"
        data._json(path, dict(schema_version="V14_NIFTY_CONTEXT_FETCH_V1", generated_at=datetime.now(IST).isoformat(),
            requested_days=days, records=records, broker_error=broker_error,
            readonly_market_data=True, oi_requested=False, orders_submitted=False))
        return path
