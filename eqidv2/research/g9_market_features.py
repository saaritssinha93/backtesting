"""Causal, fail-closed market inputs for isolated G-9 sizing research.

Nifty raw-contract timestamps are candle CLOSE; VIX CSV dates are candle START.
Only completed bars enter features. No source, strategy, or live file is edited.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

IST = "Asia/Kolkata"
RAW_DIR = Path("C:/TradingData/eqidv2/fno_oi/raw_contracts_5m")
VERSION = "g9_market_features_v1"


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _times(values):
    return pd.to_datetime(values, utc=True).dt.tz_convert(IST)


def _prepare(frame, timestamp_column, start_stamped=False):
    """Deduplicate equal observations, reject conflicting observations."""
    frame = frame.copy()
    frame["available_at"] = _times(frame[timestamp_column])
    if start_stamped:
        frame["available_at"] += pd.Timedelta(minutes=5)
    if frame.available_at.isna().any():
        raise ValueError("Missing market timestamp")
    duplicate_count = int(frame.available_at.duplicated().sum())
    check_columns = [c for c in ("open", "high", "low", "close", "quality_state",
                                 "tradingsymbol", "expiry") if c in frame]
    duplicate_rows = frame.loc[frame.available_at.duplicated(keep=False)]
    for _, group in duplicate_rows.groupby("available_at"):
        if len(group[check_columns].drop_duplicates()) != 1:
            raise ValueError("Conflicting duplicate market bars")
    frame = frame.drop_duplicates("available_at").sort_values("available_at")
    frame["day"] = frame.available_at.dt.strftime("%Y-%m-%d")
    p = frame[["open", "high", "low", "close"]].apply(pd.to_numeric, errors="coerce")
    frame[p.columns] = p
    valid = np.isfinite(p).all(axis=1) & p.gt(0).all(axis=1)
    valid &= p.high.ge(p[["open", "close", "low"]].max(axis=1))
    valid &= p.low.le(p[["open", "close", "high"]].min(axis=1))
    if "quality_state" in frame:
        valid &= frame.quality_state.eq("VALID")
    if "candle_start" in frame:
        valid &= _times(frame.candle_start).eq(frame.available_at - pd.Timedelta(minutes=5))
    frame["bar_valid"] = valid
    return frame, duplicate_count


def _prefix_values(frame, grid):
    """A hole or invalid observation poisons only that and later same-day bars."""
    if frame is None or frame.empty:
        return np.full((len(grid), 4), np.nan), np.zeros(len(grid), dtype=bool)
    indexed = frame.set_index("available_at")
    part = indexed.reindex(grid)
    valid = part.bar_valid.eq(True).to_numpy(dtype=bool)
    unexpected = indexed.index.difference(grid)
    # Out-of-session rows do not count; unexpected timestamps during the
    # regular session invalidate the prefix once they have become observable.
    unexpected = unexpected[(unexpected >= grid[0] - pd.Timedelta(minutes=5)) &
                            (unexpected <= grid[-1])]
    if len(unexpected):
        valid[grid >= unexpected.min()] = False
    ready = np.logical_and.accumulate(valid)
    values = part[["open", "high", "low", "close"]].to_numpy(dtype=float)
    return values, ready


def compute_day(day, nifty_frame=None, vix_frame=None, contract=""):
    """Pure one-session calculation; inputs have ``_prepare`` columns."""
    grid = pd.date_range(day + " 09:20", day + " 15:30", freq="5min", tz=IST)
    n, nr = _prefix_values(nifty_frame, grid)
    v, vr = _prefix_values(vix_frame, grid)
    with np.errstate(divide="ignore", invalid="ignore"):
        nifty_previous = np.r_[n[0, 0], n[:-1, 3]]
        rv = np.sqrt(np.cumsum(np.square(np.log(n[:, 3] / nifty_previous)))) * 100
        vix_previous = np.r_[v[0, 0], v[:-1, 3]]
        change = (v[:, 3] / vix_previous - 1) * 100
    rv[~nr] = np.nan
    change[~vr] = np.nan
    return pd.DataFrame({"day": day, "clock": grid.strftime("%H:%M"),
        "available_at": grid, "nifty_contract": contract, "rv_pct": rv,
        "nifty_prefix_ready": nr, "vix_prefix_ready": vr,
        "vix_change_5m_pct": change})


def add_reference(market, history_sessions=20):
    """Last N calendar-session rows, not last N nonmissing observations.

    The complete session calendar must already be represented at every clock.
    All N preceding values must be valid; current/future values are excluded.
    """
    if history_sessions != 20:
        raise ValueError("G-9 baseline requires exactly 20 prior sessions")
    m = market.sort_values(["clock", "day"]).copy()
    if m.duplicated(["day", "clock"]).any():
        raise ValueError("Duplicate market grid key")
    grouped = m.groupby("clock", sort=False).rv_pct
    m["rv_prior_count"] = grouped.transform(
        lambda s: s.shift().rolling(20, min_periods=1).count()).fillna(0).astype(int)
    m["rv_reference_median20"] = grouped.transform(
        lambda s: s.shift().rolling(20, min_periods=20).median())
    m["rv_ratio"] = m.rv_pct / m.rv_reference_median20.where(m.rv_reference_median20.gt(0))
    m["features_ready"] = (m.nifty_prefix_ready & m.vix_prefix_ready &
        m.rv_prior_count.eq(20) & np.isfinite(m.rv_ratio))
    def reason(row):
        reasons = []
        if not row.nifty_prefix_ready:
            reasons.append("nifty_missing_or_invalid_prefix")
        if not row.vix_prefix_ready:
            reasons.append("vix_missing_or_invalid_prefix")
        if row.rv_prior_count != 20:
            reasons.append("fewer_than_20_valid_prior_session_values")
        elif not np.isfinite(row.rv_ratio):
            reasons.append("invalid_or_zero_reference")
        return ";".join(reasons)
    m["missing_reason"] = m.apply(reason, axis=1)
    return m.sort_values(["day", "clock"]).reset_index(drop=True)


def build_features(days, vix_path, output_dir, raw_dir=RAW_DIR,
                   history_sessions=20, contract_chain_start=None):
    """Return ``(requested_day_grid, provenance)`` and freeze input slices.

    ``vix_path`` may be one CSV or several nonoverlapping CSVs. The earliest
    archived contract cannot prove that it was the near month before its own
    expiry month: default trusted chain start is that month's first day.
    An earlier ``contract_chain_start`` requires caller-supplied audited proof.
    Missing dates on the proper near contract never cause an early roll.
    """
    days = sorted(set(str(d) for d in days))
    if not days or any(pd.Timestamp(d).strftime("%Y-%m-%d") != d for d in days):
        raise ValueError("ISO requested session dates required")
    if history_sessions != 20:
        raise ValueError("G-9 baseline requires exactly 20 prior sessions")
    output = Path(output_dir)
    frozen = output / "market_inputs"
    frozen.mkdir(parents=True, exist_ok=True)
    if any(frozen.iterdir()):
        raise ValueError("Fresh market_inputs directory required")
    paths = [Path(vix_path)] if isinstance(vix_path, (str, Path)) else list(map(Path, vix_path))
    source_audit = {}
    contracts = []
    session_days = set(days)
    last_day = days[-1]
    raw_paths = [p for p in sorted(Path(raw_dir).glob("NIFTY*FUT_5minute.parquet"))
                 if re.fullmatch(r"NIFTY\d{2}[A-Z]{3}FUT_5minute\.parquet", p.name)]
    if not raw_paths:
        raise ValueError("No dated Nifty futures raw files")
    for path in raw_paths:
        digest = _sha(path)
        raw = pd.read_parquet(path)
        if _sha(path) != digest:
            raise ValueError("Source changed during read: " + str(path))
        frame, duplicates = _prepare(raw, "timestamp")
        if "underlying" in frame and not frame.underlying.eq("NIFTY").all():
            raise ValueError("Non-Nifty instrument in source: " + str(path))
        expiries = pd.to_datetime(frame.expiry).dt.strftime("%Y-%m-%d").unique()
        symbols = frame.tradingsymbol.unique()
        if len(expiries) != 1 or len(symbols) != 1:
            raise ValueError("Inconsistent contract metadata: " + str(path))
        frame = frame.loc[frame.day.le(last_day)].copy()
        if frame.empty:
            continue
        source_audit[str(path.resolve())] = {"sha256": digest, "rows": len(raw),
            "duplicate_equal_rows": duplicates, "expiry": str(expiries[0]),
            "tradingsymbol": str(symbols[0]), "first_day": frame.day.min(),
            "last_day": frame.day.max(), "invalid_bars": int((~frame.bar_valid).sum())}
        if "fetch_timestamp" in frame:
            late = _times(frame.fetch_timestamp).gt(frame.available_at + pd.Timedelta(minutes=1))
            source_audit[str(path.resolve())]["retrieved_more_than_one_minute_after_close"] = int(late.sum())
        contracts.append({"frame": frame, "expiry": str(expiries[0]), "symbol": str(symbols[0]),
                          "first_day": frame.day.min(), "source": str(path.resolve())})
        session_days.update(frame.day)
    if not contracts:
        raise ValueError("No Nifty source observations before requested end date")
    inferred_start = min(c["expiry"] for c in contracts)[:8] + "01"
    chain_start = contract_chain_start or inferred_start
    if pd.Timestamp(chain_start).strftime("%Y-%m-%d") != chain_start:
        raise ValueError("ISO contract_chain_start required")
    vix_parts = []
    for path in paths:
        digest = _sha(path)
        raw = pd.read_csv(path)
        if _sha(path) != digest:
            raise ValueError("Source changed during read: " + str(path))
        vix_parts.append(raw)
        source_audit[str(path.resolve())] = {"sha256": digest, "rows": len(raw), "kind": "vix_start_stamped"}
    vix, duplicate_vix = _prepare(pd.concat(vix_parts, ignore_index=True), "date", True)
    vix = vix.loc[vix.day.le(last_day)].copy()
    session_days.update(vix.day)
    # Include warmup observations where the actual front-contract chain is
    # supportable. Earlier requested days remain explicit missing grid rows.
    calendar = sorted(d for d in session_days if min(days[0], chain_start) <= d <= last_day)
    vix_days = {d: f for d, f in vix.groupby("day")}
    pieces, selected, frozen_parts = [], [], []
    for day in calendar:
        eligible = [c for c in contracts if c["expiry"] >= day and c["first_day"] <= day]
        contract = min(eligible, key=lambda c: c["expiry"]) if eligible and day >= chain_start else None
        part = None if contract is None else contract["frame"].loc[contract["frame"].day.eq(day)]
        symbol = "" if contract is None else contract["symbol"]
        pieces.append(compute_day(day, part, vix_days.get(day), symbol))
        selected.append({"day": day, "contract": symbol, "expiry": None if contract is None else contract["expiry"],
            "rows": 0 if part is None else len(part), "source": None if contract is None else contract["source"],
            "chain_verified": bool(day >= chain_start), "requested": day in days})
        if part is not None and len(part):
            frozen_parts.append(part)
    full = add_reference(pd.concat(pieces, ignore_index=True), history_sessions)
    result = full.loc[full.day.isin(days)].reset_index(drop=True)
    expected_rows = 75 * len(days)
    assert len(result) == expected_rows and not result.duplicated(["day", "clock"]).any()
    freeze_columns = ["timestamp", "candle_start", "underlying", "tradingsymbol", "expiry",
                      "open", "high", "low", "close", "quality_state", "fetch_timestamp", "source"]
    if frozen_parts:
        selected_raw = pd.concat(frozen_parts, ignore_index=True)
        selected_raw[[c for c in freeze_columns if c in selected_raw]].to_parquet(frozen / "nifty_selected_front_bars.parquet", index=False)
    vix.loc[vix.day.isin(calendar), ["date", "open", "high", "low", "close", "volume"]].to_csv(frozen / "vix_5minute.csv", index=False)
    pd.DataFrame(selected).to_csv(output / "market_contract_selection.csv", index=False)
    full.to_csv(output / "market_grid_with_warmup.csv", index=False)
    result.to_csv(output / "market_features.csv", index=False)
    coverage = result.groupby("day").agg(grid_rows=("clock", "size"), ready_rows=("features_ready", "sum"),
        nifty_ready_rows=("nifty_prefix_ready", "sum"), vix_ready_rows=("vix_prefix_ready", "sum"),
        prior_count_min=("rv_prior_count", "min"), prior_count_max=("rv_prior_count", "max"))
    coverage.to_csv(output / "market_feature_coverage.csv")
    provenance = {"version": VERSION, "scope": "BACKTEST_ONLY", "execution_authority": False,
        "requested_days": days, "calendar_days": calendar, "history_sessions_required": 20,
        "contract_chain_start": chain_start,
        "contract_chain_start_basis": "caller_supplied_audited_start" if contract_chain_start else
            "conservative first day of earliest archived expiry month; no prior-month near-contract evidence",
        "nifty_timestamp_semantics": "candle close; availability assumed at close for research",
        "vix_timestamp_semantics": "candle start; available_at=date+5min",
        "rv_definition": "100*sqrt(sum(log(close/previous_close)^2)); first previous_close=session open",
        "reference_definition": "median of all20 preceding same-clock session values; all20required; no missing-value compression",
        "vix_change_definition": "100*(current close/previous adjacent close-1); first bar uses same-session open",
        "vix_equal_duplicate_rows": duplicate_vix,
        "requested_grid_rows": len(result), "requested_ready_rows": int(result.features_ready.sum()),
        "source_files": source_audit,
        "limitations": ["Historical candle completeness does not establish actual contemporaneous arrival or repair latency; source fetch timestamps are audited but not used to claim live availability.",
            "Missing front-contract bars never cause early rollover. Invalid/missing intraday prefixes remain unavailable thereafter.",
            "The session calendar is the union of observed Nifty/VIX dates and requested sessions; a session missing in all sources cannot be reconstructed.",
            "The earliest archived contract's deferred-contract observations before the trusted near-month chain start are excluded, not used as fabricated warmup."],
        "files": {str(p.relative_to(output)): _sha(p) for p in [*frozen.glob("*"),
            output / "market_contract_selection.csv", output / "market_grid_with_warmup.csv",
            output / "market_features.csv", output / "market_feature_coverage.csv"]}}
    (output / "market_feature_provenance.json").write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
    return result, provenance
