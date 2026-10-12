"""Read existing futures minute archives for isolated opening-data recovery.

Only known producer directories are searched. Nothing in this module writes,
contacts a broker, repairs a source, or invents a missing minute. Callers must
validate the combined records so conflicting observations remain excluded.
"""
from __future__ import annotations

from datetime import timedelta
from functools import lru_cache
import hashlib
from pathlib import Path
import re

from dashboard_flow_observer import safe_name, stamp

MONTHS = {name: index for index, name in enumerate(
    ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"), 1)}
SYNTHETIC_FLAGS = ("gap_filled", "is_gap_filled", "synthetic", "is_synthetic", "is_forward_filled",
                   "provisional_stale", "forward_filled", "is_filled", "interpolated", "synthetic_repair")


def _stem(contract):
    stem = re.sub(r"[^A-Z0-9._-]+", "_", contract).strip("._") or "CONTRACT"
    return f"{stem}_{hashlib.sha1(contract.encode()).hexdigest()[:8]}" if stem != contract else stem


def _month(contract):
    match = re.search(r"(\d{2})([A-Z]{3})FUT$", contract)
    if not match or match[2] not in MONTHS:
        raise ValueError("Expected a dated monthly futures contract")
    return 2000 + int(match[1]), MONTHS[match[2]]


def _present(value):
    return value is not None and str(value).strip().lower() not in {"", "none", "nan", "nat", "<na>"}


@lru_cache(maxsize=8)
def _registry_identities(path, mtime_ns, size):
    import pandas as pd
    frame = pd.read_parquet(path)
    result = {}
    for row in frame.to_dict("records"):
        name = str(row.get("tradingsymbol", "")).strip().upper()
        expiry = stamp(row.get("expiry"))
        if name and expiry:
            result.setdefault(name, set()).add(expiry.date())
    return result


def _expiry_dates(root, contract):
    registry = root / "fno_oi/universe/contract_registry.parquet"
    if not registry.exists():
        return set()
    path = registry.resolve()
    if not path.is_relative_to(root):
        raise ValueError("Contract registry resolves outside data root")
    info = path.stat()
    return _registry_identities(str(path), info.st_mtime_ns, info.st_size).get(contract, set())


def _normalize(row, contract, expiries):
    for name in ("tradingsymbol", "futures_tradingsymbol", "contract"):
        if _present(row.get(name)) and str(row[name]).strip().upper() != contract:
            return None
    expected_month = _month(contract)
    for name in ("expiry", "expiry_date"):
        if _present(row.get(name)):
            expiry = stamp(row[name])
            if not expiry or (expiry.year, expiry.month) != expected_month:
                return None
            if expiries and expiry.date() not in expiries:
                return None
    if _present(row.get("instrument_type")) and str(row["instrument_type"]).strip().upper() != "FUT":
        return None
    starts = [stamp(row[name]) for name in ("candle_start", "bar_start", "timestamp", "date", "ts")
              if _present(row.get(name))]
    if not starts or any(value is None or value != starts[0] for value in starts):
        return None
    start = starts[0]
    if start.hour != 9 or not 15 <= start.minute <= 20 or start.second or start.microsecond:
        return None
    ends = [stamp(row[name]) for name in ("candle_end", "bar_end") if name in row]
    if any(value != start + timedelta(minutes=1) for value in ends):
        return None
    source = str(row.get("source", "")).upper()
    if any(value in source for value in ("SYNTHETIC", "GAP_FILL", "GAPFIL", "FORWARD", "INTERPOLAT", "PROXY")):
        return None
    if any(str(row.get(name, "")).strip().lower() not in {"", "0", "0.0", "false", "no", "none", "nan"}
           for name in SYNTHETIC_FLAGS):
        return None
    if str(row.get("quality_state", "VALID")).upper() != "VALID":
        return None
    return {**row, "candle_start": start.isoformat(),
            "candle_end": (start + timedelta(minutes=1)).isoformat()}


@lru_cache(maxsize=512)
def _read_records(path, mtime_ns, size, contract, expiries):
    import pandas as pd
    frame = pd.read_parquet(path)
    timestamp_column = next((name for name in ("candle_start", "bar_start", "timestamp", "date", "ts")
                             if name in frame), None)
    if timestamp_column is None:
        return ()
    # Producer timestamps are timezone-aware. _normalize below remains the
    # authority for naive timestamps and conflicting timestamp aliases.
    values = frame[timestamp_column].map(stamp)
    frame = frame.loc[values.map(lambda value: value is not None and value.hour == 9 and 15 <= value.minute <= 20)]
    return tuple(value for row in frame.to_dict("records")
                 if (value := _normalize(row, contract, set(expiries))) is not None)


def load_contract_archives(data_root, contract):
    """Return ``[{records, path}]``; preserve duplicates for cross-source validation.

    Filename identity is accepted only for exact, collision-resistant producer
    names. Explicit row identities must agree with that name and the registry.
    Each record represents a real stored minute beginning between 09:15–09:20.
    """
    contract = safe_name(contract)
    _month(contract)
    root = Path(data_root).resolve()
    fno = root / "fno_oi"
    name = f"{_stem(contract)}_1minute.parquet"
    v5 = fno / "strategy_research/v13_corrected_v5"
    paths = [fno / "raw_contracts_1m_hist" / name,
             v5 / "derivative_market_data/raw_futures_1m" / name]
    paths.extend((fno / "raw_contracts_1m").glob(f"????-??-??/{name}"))
    paths.extend((v5 / "daily_options").glob(f"*/derivative_market_data/raw_futures_1m/{name}"))
    paths.extend((fno / "strategy_research/v6_derivative_backtests/market_cache").glob(f"????-??-??/{name}"))
    expiries = tuple(sorted(_expiry_dates(root, contract)))
    result, seen = [], set()
    for candidate in sorted(paths):
        if not candidate.is_file():
            continue
        path = candidate.resolve()
        if not path.is_relative_to(root):
            raise ValueError("Minute archive resolves outside data root")
        if path in seen:
            continue
        seen.add(path)
        info = path.stat()
        records = _read_records(str(path), info.st_mtime_ns, info.st_size, contract, expiries)
        result.append({"records": [dict(row) for row in records], "path": str(path)})
    return result
