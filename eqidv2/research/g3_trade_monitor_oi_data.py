"""Research-only causal joins of archived, dated-contract futures OI.

The default deliberately models availability rather than claiming historical
receipt: the raw archive is mutable and contains retrospectively fetched bars.
No source data, baseline trades, or live configuration is modified here.
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_DATA_ROOT = Path("C:/TradingData/eqidv2/fno_oi")
IST = "Asia/Kolkata"


def _stamp(values):
    return pd.to_datetime(values, errors="coerce", utc=True, format="mixed").dt.tz_convert(IST)


def _stem(symbol):
    symbol = str(symbol).strip().upper()
    stem = re.sub(r"[^A-Z0-9._-]+", "_", symbol).strip("._") or "CONTRACT"
    if stem != symbol:
        stem += "_" + hashlib.sha1(symbol.encode()).hexdigest()[:8]
    return stem


def _sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read(path, sources):
    before = _sha(path)
    frame = pd.read_parquet(path)
    after = _sha(path)
    if before != after:
        raise RuntimeError(f"OI source changed while reading: {path}")
    sources[str(path)] = dict(path=str(path), sha256_before=before,
                              sha256_after=after, rows=len(frame))
    return frame


def _prepare(raw, contract, lag_minutes, mode):
    required = {"timestamp", "oi", "close", "tradingsymbol", "underlying",
                "instrument_token", "expiry", "quality_state"}
    if not required.issubset(raw.columns):
        raise ValueError(f"OI archive missing fields: {sorted(required - set(raw.columns))}")
    raw = raw.copy()
    raw["_end"] = _stamp(raw["timestamp"])
    raw = raw.loc[raw._end.notna()].sort_values("_end", kind="stable")
    raw["_duplicate"] = raw._end.duplicated(keep=False)
    raw = raw.drop_duplicates("_end", keep="last").set_index("_end", drop=False)
    # Use exact timestamps, not a positional shift across missing bars or days.
    ref = raw.reindex(raw.index - pd.Timedelta(minutes=10)).copy()
    mid = raw.reindex(raw.index - pd.Timedelta(minutes=5)).copy()
    ref.index = mid.index = raw.index
    result = pd.DataFrame(index=raw.index)
    result["oi_current_bar_end"] = raw._end
    result["oi_reference_bar_end"] = ref._end
    result["oi_contract"] = contract
    result["_underlying"] = raw.underlying.astype(str).str.upper()
    result["_token"] = pd.to_numeric(raw.instrument_token, errors="coerce")
    result["oi_reason"] = "OK"
    valid = pd.Series(True, index=raw.index)
    for label, frame in [("CURRENT", raw), ("MIDPOINT", mid), ("REFERENCE", ref)]:
        oi = pd.to_numeric(frame.oi, errors="coerce")
        close = pd.to_numeric(frame.close, errors="coerce")
        expiry = pd.to_datetime(frame.expiry, errors="coerce", utc=True).dt.tz_localize(None).dt.normalize()
        checks = [
            (frame._end.notna(), f"MISSING_{label}_BAR"),
            (frame._end.dt.date.eq(raw._end.dt.date), "CROSS_SESSION"),
            (frame._duplicate.eq(False), "DUPLICATE_BAR"),
            (frame.tradingsymbol.astype(str).str.upper().eq(contract), "CONTRACT_MISMATCH"),
            (frame.underlying.astype(str).str.upper().eq(raw.underlying.astype(str).str.upper()), "UNDERLYING_MISMATCH"),
            (pd.to_numeric(frame.instrument_token, errors="coerce").eq(result._token), "TOKEN_MISMATCH"),
            (expiry.ge(raw._end.dt.tz_localize(None).dt.normalize()), "EXPIRED_CONTRACT"),
            (frame.quality_state.eq("VALID"), "INVALID_SOURCE_QUALITY"),
            (np.isfinite(oi) & oi.gt(0), "NONPOSITIVE_OR_INVALID_OI"),
            (np.isfinite(close) & close.gt(0), "NONPOSITIVE_OR_INVALID_FUTURES_CLOSE"),
        ]
        for check, reason in checks:
            check = check.fillna(False)
            result.loc[valid & ~check, "oi_reason"] = reason
            valid &= check
    result["oi_valid"] = valid
    result["oi_change_10m_pct"] = (pd.to_numeric(raw.oi, errors="coerce") /
        pd.to_numeric(ref.oi, errors="coerce") - 1) * 100
    result["futures_return_10m_pct"] = (pd.to_numeric(raw.close, errors="coerce") /
        pd.to_numeric(ref.close, errors="coerce") - 1) * 100
    for label, frame in [("current", raw), ("midpoint", mid), ("reference", ref)]:
        result[f"oi_{label}_value"] = pd.to_numeric(frame.oi, errors="coerce")
        result[f"oi_{label}_futures_close"] = pd.to_numeric(frame.close, errors="coerce")
    receipts = []
    receipt_valid = pd.Series(True, index=raw.index)
    for label, frame in [("current", raw), ("midpoint", mid), ("reference", ref)]:
        received = (_stamp(frame.fetch_timestamp) if "fetch_timestamp" in frame
                    else pd.Series(pd.NaT, index=frame.index, dtype=f"datetime64[ns, {IST}]"))
        receipt_valid &= received.notna() & received.ge(frame._end)
        receipts.append(received)
        result[f"oi_{label}_received_at"] = received
    # Availability for the entire three-bar evidence set, not only the endpoint.
    result["oi_received_at"] = pd.concat(receipts, axis=1).max(axis=1)
    result["_receipt_valid"] = receipt_valid
    if mode == "received_time":
        result["oi_available_at"] = result.oi_received_at.where(receipt_valid)
        result.loc[valid & ~receipt_valid, "oi_reason"] = "MISSING_OR_INVALID_RECEIPT"
        result["oi_valid"] &= receipt_valid
    else:
        result["oi_available_at"] = raw._end + pd.Timedelta(minutes=lag_minutes)
    result.loc[~result.oi_valid, ["oi_change_10m_pct", "futures_return_10m_pct"]] = np.nan
    return result.reset_index(drop=True)


def join_decision_features(features, data_root=DEFAULT_DATA_ROOT,
                           availability_mode="bar_end_plus_lag", lag_minutes=1):
    """Return (enriched decisions, per-contract/session coverage, provenance).

    Inputs require day, tradingsymbol and decision_at_ist. Contract identity is
    coalesced from futures_tradingsymbol, opening_oi_contract, contract; otherwise
    the session's dated near-month universe is required. In received_time mode
    a retained raw revision must have been fetched before the decision; this is
    proof of that retained version's receipt, never proof of first availability.
    Missing/invalid OI always retains the input decision with oi_valid=False.
    """
    if availability_mode not in {"bar_end_plus_lag", "received_time"}:
        raise ValueError("Unknown OI availability mode")
    if not np.isfinite(lag_minutes) or lag_minutes < 0:
        raise ValueError("lag_minutes must be finite and nonnegative")
    required = {"day", "tradingsymbol", "decision_at_ist"}
    if not required.issubset(features.columns):
        raise ValueError(f"Decision features missing {sorted(required-set(features.columns))}")
    root = Path(data_root)
    out = features.copy().reset_index(drop=True)
    sources = {}
    out["_decision"] = _stamp(out.decision_at_ist)
    out["oi_contract"] = ""
    out["oi_identity_source"] = "MISSING"
    identity_conflict = pd.Series(False, index=out.index)
    for column in ["futures_tradingsymbol", "opening_oi_contract", "contract"]:
        if column not in out:
            continue
        values = out[column].fillna("").astype(str).str.strip().str.upper()
        is_contract = values.str.fullmatch(r".+\d{2}[A-Z]{3}FUT", na=False)
        identity_conflict |= out.oi_contract.ne("") & is_contract & values.ne(out.oi_contract)
        use = out.oi_contract.eq("") & is_contract
        out.loc[use, "oi_contract"] = values[use]
        out.loc[use, "oi_identity_source"] = column
    for day in out.loc[out.oi_contract.eq(""), "day"].astype(str).unique():
        path = root / "universe" / f"near_month_{day}.parquet"
        if not path.exists():
            continue
        universe = _read(path, sources)
        if not {"underlying", "tradingsymbol"}.issubset(universe.columns):
            continue
        if "master_date" in universe:
            universe = universe.loc[pd.to_datetime(universe.master_date, errors="coerce").dt.strftime("%Y-%m-%d").eq(day)]
        counts = universe.groupby("underlying").size()
        universe = universe.loc[universe.underlying.map(counts).eq(1)]
        mapping = universe.set_index("underlying").tradingsymbol.to_dict()
        mask = out.day.astype(str).eq(day) & out.oi_contract.eq("")
        mapped = out.loc[mask, "tradingsymbol"].map(mapping)
        use = mapped.notna()
        out.loc[mapped.index[use], "oi_contract"] = mapped[use]
        out.loc[mapped.index[use], "oi_identity_source"] = "DATED_NEAR_MONTH_UNIVERSE"
    out["oi_valid"] = False
    out["oi_reason"] = "MISSING_CONTRACT_IDENTITY"
    out["oi_change_10m_pct"] = np.nan
    out["futures_return_10m_pct"] = np.nan
    for column in ["oi_current_bar_end", "oi_reference_bar_end", "oi_available_at", "oi_received_at"]:
        out[column] = pd.Series(pd.NaT, index=out.index, dtype=f"datetime64[ns, {IST}]")
    out["point_in_time_proven"] = False
    out["oi_max_age_minutes"] = float(lag_minutes+5)
    out["oi_source_path"] = ""
    out["oi_source_sha256"] = ""
    input_value_columns = []
    for label in ["current", "midpoint", "reference"]:
        for field in ["value", "futures_close"]:
            column = f"oi_{label}_{field}"
            out[column] = np.nan
            input_value_columns.append(column)
        column = f"oi_{label}_received_at"
        out[column] = pd.Series(pd.NaT, index=out.index, dtype=f"datetime64[ns, {IST}]")
        input_value_columns.append(column)
    for contract, indices in out.loc[out.oi_contract.ne("")].groupby("oi_contract", sort=True).groups.items():
        path = root / "raw_contracts_5m" / f"{_stem(contract)}_5minute.parquet"
        if not path.exists():
            out.loc[indices, "oi_reason"] = "MISSING_CONTRACT_ARCHIVE"
            continue
        raw = _read(path, sources)
        out.loc[indices, "oi_source_path"] = str(path)
        out.loc[indices, "oi_source_sha256"] = sources[str(path)]["sha256_before"]
        # Limit inspected market data to input sessions; no subsequent outcomes.
        raw_dates = _stamp(raw.timestamp).dt.strftime("%Y-%m-%d")
        raw = raw.loc[raw_dates.isin(set(out.loc[indices, "day"].astype(str)))].copy()
        prepared = _prepare(raw, contract, lag_minutes, availability_mode)
        prepared = prepared.loc[prepared.oi_available_at.notna()]
        for day, day_indices in out.loc[indices].groupby("day", sort=False).groups.items():
            right = prepared.loc[prepared.oi_current_bar_end.dt.strftime("%Y-%m-%d").eq(str(day))]
            left = out.loc[day_indices, ["_decision"]].copy()
            left["_row"] = left.index
            left = left.loc[left._decision.notna()].sort_values("_decision", kind="stable")
            out.loc[day_indices, "oi_reason"] = "NO_AVAILABLE_SESSION_BAR"
            if left.empty or right.empty:
                continue
            # Received bars can arrive out of order; use newest available bar
            # end, never let an older delayed record replace a newer observation.
            right = right.sort_values(["oi_available_at", "oi_current_bar_end"], kind="stable")
            newest_end = right.oi_current_bar_end.cummax()
            right = right.loc[right.oi_current_bar_end.eq(newest_end)]
            joined = pd.merge_asof(left, right, left_on="_decision", right_on="oi_available_at", direction="backward")
            joined.index = joined._row.astype(int)
            present = joined.oi_current_bar_end.notna()
            valid = joined.oi_valid.eq(True)
            joined.loc[~present, "oi_reason"] = "NO_AVAILABLE_SESSION_BAR"
            decision_day = joined._decision.dt.strftime("%Y-%m-%d").eq(str(day))
            joined.loc[valid & ~decision_day, "oi_reason"] = "DECISION_SESSION_MISMATCH"
            valid &= decision_day
            age = (joined._decision - joined.oi_current_bar_end).dt.total_seconds()/60
            fresh = age.between(0, lag_minutes+5)
            joined.loc[valid & ~fresh, "oi_reason"] = "STALE_OI_BAR"
            valid &= fresh
            cash_match = joined._underlying.eq(out.loc[joined.index, "tradingsymbol"].astype(str).str.upper())
            joined.loc[valid & ~cash_match, "oi_reason"] = "TRADE_UNDERLYING_MISMATCH"
            valid &= cash_match
            if "futures_instrument_token" in out:
                expected = pd.to_numeric(out.loc[joined.index, "futures_instrument_token"], errors="coerce")
                token_match = expected.isna() | expected.eq(joined._token)
                joined.loc[valid & ~token_match, "oi_reason"] = "TRADE_TOKEN_MISMATCH"
                valid &= token_match
            joined["oi_valid"] = valid
            joined["point_in_time_proven"] = (valid & joined._receipt_valid.eq(True)
                & joined.oi_received_at.le(joined._decision) & (availability_mode == "received_time"))
            joined.loc[~valid, ["oi_change_10m_pct", "futures_return_10m_pct"]] = np.nan
            for column in ["oi_valid", "oi_reason", "oi_change_10m_pct", "futures_return_10m_pct",
                           "oi_current_bar_end", "oi_reference_bar_end", "oi_available_at", "oi_received_at", "point_in_time_proven"] + input_value_columns:
                out.loc[joined.index, column] = joined[column]
    invalid_decision = out._decision.isna()
    out.loc[invalid_decision, "oi_reason"] = "INVALID_DECISION_TIMESTAMP"
    out.loc[identity_conflict, "oi_reason"] = "CONFLICTING_CONTRACT_IDENTITY"
    out.loc[identity_conflict, ["oi_valid", "point_in_time_proven"]] = False
    out.loc[identity_conflict, ["oi_change_10m_pct", "futures_return_10m_pct"]] = np.nan
    coverage = out.groupby(["day", "tradingsymbol", "oi_contract", "oi_reason"], dropna=False).agg(
        decision_rows=("oi_valid", "size"), valid_rows=("oi_valid", "sum"),
        receipt_proven_rows=("point_in_time_proven", "sum")).reset_index()
    for source in sources.values():
        source["sha256_at_join_end"] = _sha(Path(source["path"]))
        source["changed_after_stable_read"] = source["sha256_at_join_end"] != source["sha256_before"]
    provenance = dict(schema_version="G3_OI_ARCHIVE_JOIN_V1", availability_mode=availability_mode,
        availability_label=("ASSUMED_AVAILABLE_REUSED_HISTORY" if availability_mode == "bar_end_plus_lag"
                            else "RETAINED_REVISION_RECEIPT_CONSTRAINED"),
        lag_minutes=float(lag_minutes), maximum_bar_age_minutes=float(lag_minutes+5),
        interval_minutes=10, exact_midpoint_required=True, input_decision_rows=len(features),
        valid_decision_rows=int(out.oi_valid.sum()), point_in_time_proven_rows=int(out.point_in_time_proven.sum()),
        source_root=str(root), sources=list(sources.values()), source_hashes_verified=True,
        source_files_unchanged_through_join=not any(s["changed_after_stable_read"] for s in sources.values()),
        timestamp_semantics="raw timestamp is completed 5-minute bar end; candle_start is bar start",
        receipt_semantics="fetch_timestamp is retained revision fetch, not first receipt; archives may be overwritten",
        oi_units="Unscaled broker historical futures OI; percent changes within identical contract; not option OI or contracts inferred from lots")
    return out.drop(columns=["_decision"]), coverage, provenance
