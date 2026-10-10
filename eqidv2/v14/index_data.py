"""Read the unchanged G-3 NIFTY opening-price context without stock futures OI.

Only exact completed 09:15--09:20 bars are accepted.  Dated contract mappings
take precedence; verified frozen ledgers supply historical mapping/context
when the original dated universe or bar is absent.  Every requested day has
an audit row, including failures, and no missing value is filled forward.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

IST = "Asia/Kolkata"
DEFAULT_FNO_ROOT = Path(os.environ.get("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2")) / "fno_oi"
FROZEN_RELATIVE = Path("strategy_research/v13_corrected_v10_g_3/frozen_20261008_long110_nextminute_v1")
CONTEXT_COLUMNS = ["day", "contract_month", "nifty_first_bar_return_pct"]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _verified(path: Path, expected: str) -> str:
    actual = _sha256(path)
    if not expected or actual != expected:
        raise ValueError(f"Frozen NIFTY context source hash mismatch: {path}")
    return actual


def _contract(value: object) -> str:
    value = str(value).strip().upper()
    if re.fullmatch(r"NIFTY\d{2}[A-Z]{3}FUT", value):
        return value
    if re.fullmatch(r"\d{2}[A-Z]{3}", value):
        return f"NIFTY{value}FUT"
    if re.fullmatch(r"\d{4}-\d{2}", value):
        return f"NIFTY{pd.Timestamp(value + '-01').strftime('%y%b').upper()}FUT"
    return ""


def _frozen_context(folder: Path | None) -> dict[str, dict]:
    if folder is None or not (folder / "manifest.json").is_file():
        return {}
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    artifacts = manifest["artifacts"]
    inputs: list[tuple[Path, str]] = []
    if "trades.csv" in artifacts:
        inputs.append((folder / "trades.csv", artifacts["trades.csv"]["sha256"]))
    provenance_name = "source_study_provenance.json"
    if provenance_name in artifacts:
        provenance_path = folder / provenance_name
        _verified(provenance_path, artifacts[provenance_name]["sha256"])
        provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
        bundle = Path(provenance["source_bundle"])
        bundle_manifest_path = bundle / "bundle_manifest.json"
        if bundle_manifest_path.is_file():
            _verified(bundle_manifest_path, provenance["source_bundle_manifest_sha256"])
            bundle_manifest = json.loads(bundle_manifest_path.read_text(encoding="utf-8"))
            relative = "dataset/annotated.parquet"
            if relative in bundle_manifest["artifacts"]:
                inputs.append((bundle / relative, bundle_manifest["artifacts"][relative]["sha256"]))
    result: dict[str, dict] = {}
    for path, expected in inputs:
        if not path.is_file():
            continue
        digest = _verified(path, expected)
        if path.suffix == ".csv":
            frame = pd.read_csv(path, usecols=CONTEXT_COLUMNS)
        else:
            frame = pd.read_parquet(path, columns=CONTEXT_COLUMNS)
        _verified(path, digest)
        frame["day"] = pd.to_datetime(frame.day, errors="raise").dt.strftime("%Y-%m-%d")
        for day, group in frame.groupby("day", sort=True):
            values = pd.to_numeric(group.nifty_first_bar_return_pct, errors="coerce")
            finite = values.loc[np.isfinite(values)]
            contracts = {_contract(value) for value in group.contract_month}
            contracts.discard("")
            if len(contracts) > 1 or (len(finite) and not np.allclose(finite, finite.iloc[0], rtol=0, atol=1e-10)):
                raise ValueError(f"Conflicting frozen NIFTY context on {day}: {path}")
            if not len(finite):
                continue
            context = dict(contract=next(iter(contracts), ""), value=float(finite.iloc[0]),
                           source_path=str(path), source_sha256=digest)
            previous = result.get(day)
            if previous is not None:
                if not np.isclose(previous["value"], context["value"], rtol=0, atol=1e-10):
                    raise ValueError(f"Conflicting frozen ledgers for NIFTY on {day}")
                if previous["contract"] and context["contract"] and previous["contract"] != context["contract"]:
                    raise ValueError(f"Conflicting frozen contract mapping on {day}")
                if not previous["contract"]:
                    previous["contract"] = context["contract"]
            else:
                result[day] = context
    return result


def _bar_context(frame: pd.DataFrame, day: str, contract: str) -> tuple[float, str]:
    if "timestamp" not in frame:
        return np.nan, "MISSING_TIMESTAMP_COLUMN"
    stamps = pd.to_datetime(frame.timestamp, errors="coerce")
    if stamps.dt.tz is None:
        stamps = stamps.dt.tz_localize(IST)
    else:
        stamps = stamps.dt.tz_convert(IST)
    target = pd.Timestamp(f"{day} 09:20", tz=IST)
    rows = frame.loc[stamps.eq(target)]
    if len(rows) != 1:
        return np.nan, "MISSING_EXACT_0920_BAR" if rows.empty else "DUPLICATE_EXACT_0920_BAR"
    row = rows.iloc[0]
    if "tradingsymbol" in rows and str(row.tradingsymbol).upper() != contract:
        return np.nan, "CONTRACT_IDENTITY_MISMATCH"
    if "underlying" in rows and str(row.underlying).upper() != "NIFTY":
        return np.nan, "UNDERLYING_IDENTITY_MISMATCH"
    if "candle_start" in rows:
        start = pd.Timestamp(row.candle_start)
        start = start.tz_localize(IST) if start.tzinfo is None else start.tz_convert(IST)
        if start != target - pd.Timedelta(minutes=5):
            return np.nan, "INVALID_CANDLE_INTERVAL"
    if "quality_state" in rows and str(row.quality_state).upper() != "VALID":
        return np.nan, "FLAGGED_QUALITY_STATE"
    for flag in ("gap_filled", "opening_snapshot", "provisional_stale"):
        if flag in rows and str(row[flag]).strip().lower() in {"true", "yes", "on", "1", "1.0"}:
            return np.nan, f"FLAGGED_{flag.upper()}"
    prices = pd.to_numeric(pd.Series([row.get("open"), row.get("close")]), errors="coerce").to_numpy(float)
    if not np.isfinite(prices).all() or (prices <= 0).any():
        return np.nan, "INVALID_OPEN_CLOSE"
    if "high" in rows and "low" in rows:
        bounds = pd.to_numeric(pd.Series([row.high, row.low]), errors="coerce").to_numpy(float)
        if not np.isfinite(bounds).all() or bounds[0] < max(prices) or bounds[1] > min(prices) or bounds[1] <= 0:
            return np.nan, "INVALID_OHLC_BOUNDS"
    return float((prices[1] / prices[0] - 1) * 100), "PASS"


def load_nifty_context(days: Iterable[object], *, fno_root: Path | str = DEFAULT_FNO_ROOT,
                       frozen_dir: Path | str | None = None,
                       use_frozen_fallback: bool = True) -> pd.DataFrame:
    """Return audited, causal opening returns; failed days carry NaN.

    The fallback verifies frozen artifact hashes and reads only already-built
    opening-price context, never outcomes.  A malformed dated mapping fails
    closed instead of switching contracts.  Invalid/duplicate bars likewise
    cannot be bypassed via ledger fallback; missing raw bars can.
    """
    root = Path(fno_root)
    folder = Path(frozen_dir) if frozen_dir is not None else root / FROZEN_RELATIVE
    fallback = _frozen_context(folder) if use_frozen_fallback else {}
    bar_cache: dict[str, tuple[pd.DataFrame, str]] = {}
    records = []
    for day in sorted({pd.Timestamp(value).strftime("%Y-%m-%d") for value in days}):
        record = dict(day=day, nifty_first_bar_return_pct=np.nan, contract="", mapping_source="",
                      source_path="", source_sha256="", quality_status="UNAVAILABLE", reason="MISSING_DATED_MAPPING")
        frozen = fallback.get(day)
        mapping = root / "universe" / f"near_month_{day}.parquet"
        if mapping.is_file():
            universe = pd.read_parquet(mapping)
            rows = universe.loc[universe.get("underlying", pd.Series(index=universe.index, dtype=str)).astype(str).str.upper().eq("NIFTY")]
            if len(rows) != 1:
                record["reason"] = "MISSING_OR_AMBIGUOUS_DATED_NIFTY_MAPPING"
                records.append(record)
                continue
            contract = _contract(rows.iloc[0].get("tradingsymbol", ""))
            expiry = pd.to_datetime(rows.iloc[0].get("expiry"), errors="coerce")
            if not contract or pd.isna(expiry) or expiry.date() < pd.Timestamp(day).date():
                record["reason"] = "INVALID_DATED_NIFTY_MAPPING"
                records.append(record)
                continue
            record.update(contract=contract, mapping_source=str(mapping), mapping_sha256=_sha256(mapping))
            if frozen and frozen["contract"] and frozen["contract"] != contract:
                record["reason"] = "DATED_MAPPING_CONFLICTS_WITH_FROZEN"
                records.append(record)
                continue
        elif frozen:
            record.update(contract=frozen["contract"], mapping_source="VERIFIED_FROZEN_LEDGER")
        contract = record["contract"]
        path = root / "raw_contracts_5m" / f"{contract}_5minute.parquet" if contract else None
        if path is not None and path.is_file():
            if contract not in bar_cache:
                digest = _sha256(path)
                bars = pd.read_parquet(path)
                if _sha256(path) != digest:
                    raise ValueError(f"NIFTY source changed while reading: {path}")
                bar_cache[contract] = (bars, digest)
            bars, digest = bar_cache[contract]
            value, reason = _bar_context(bars, day, contract)
            record.update(source_path=str(path), source_sha256=digest, reason=reason)
            if np.isfinite(value):
                if frozen and not np.isclose(frozen["value"], value, rtol=0, atol=1e-10):
                    record["reason"] = "RAW_CONTEXT_CONFLICTS_WITH_FROZEN"
                else:
                    record.update(nifty_first_bar_return_pct=value, quality_status="PASS_RAW")
            elif reason != "MISSING_EXACT_0920_BAR":
                records.append(record)
                continue
        elif contract:
            record["reason"] = "MISSING_RAW_CONTRACT_FILE"
        if (record["quality_status"] == "UNAVAILABLE" and frozen
                and record["reason"] in {"MISSING_DATED_MAPPING", "MISSING_RAW_CONTRACT_FILE", "MISSING_EXACT_0920_BAR"}):
            record.update(nifty_first_bar_return_pct=frozen["value"], source_path=frozen["source_path"],
                          source_sha256=frozen["source_sha256"], quality_status="PASS_FROZEN_CONTEXT",
                          reason="VERIFIED_FROZEN_OPENING_CONTEXT")
        records.append(record)
    return pd.DataFrame.from_records(records)


def nifty_return_map(days: Iterable[object], **kwargs) -> dict[str, float]:
    """Map ISO session day to return, retaining unavailable days as NaN."""
    frame = load_nifty_context(days, **kwargs)
    return dict(zip(frame.day, frame.nifty_first_bar_return_pct)) if len(frame) else {}
