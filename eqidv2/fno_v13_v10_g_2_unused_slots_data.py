"""Sealed, causal extension inputs for isolated G-2 unused-slot research.

This module neither changes the existing strategy nor selects a research winner.
Full native history warms up EMA calculations. Only the output directory supplied
by the independent research runner receives generated artifacts.
"""
from __future__ import annotations

import hashlib
import json
import argparse
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

import fno_oi_hybrid_data as hybrid
import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext
import fno_v13_v10_g_daily_replay as replay


SCHEMA = "g2_unused_slots_extension_inputs_v1"
SLOT_CLOCKS = tuple(pd.date_range("2000-01-01 10:05", "2000-01-01 14:00", freq="5min").strftime("%H:%M"))
PARITY_FIELDS = (
    "open", "high", "low", "close", "volume", "source_1m_count",
    "ema9", "ema20", "ema50", "prev_close", "price_change_pct",
    "volume_ratio", "traded_value", "oi", "prev_oi", "oi_change_pct",
    "signal_close", "confirmation_open", "confirmation_high", "confirmation_low",
    "confirmation_close", "confirmation_volume", "body_ratio",
    "v9_1m_volume_ratio", "v9_1m_upper_wick_ratio", "v9_1m_lower_wick_ratio",
    "v9_5m_ema9", "v9_5m_ema20", "v9_5m_ema50",
)


def _fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def _flagged(frame):
    result = pd.Series(False, index=frame.index)
    for name in ("gap_filled", "opening_snapshot", "provisional_stale"):
        if name in frame:
            values = frame[name]
            result |= (pd.to_numeric(values, errors="coerce").fillna(0).ne(0)
                       | values.astype(str).str.strip().str.lower().isin(["true", "yes", "on"]))
    return result


def _pool(minute, future, *, day, symbol, future_symbol, month):
    """Daily-replay arithmetic with all completed session slots retained."""
    equity_five = hybrid.aggregate_equity_one_minute_to_five_minute(minute)
    if equity_five.empty:
        return pd.DataFrame()
    # No history truncation: EMA initialization is identical to daily replay.
    five = hybrid.join_equity_price_with_futures_oi(equity_five, future)
    five = five.loc[five.ts.dt.date.eq(day)].copy()
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
    for span in (9, 20, 50):
        five[f"v9_5m_ema{span}"] = five[f"ema{span}"]
    five["v9_5m_ema_bull"] = (five.ema9.gt(five.ema20) & five.ema20.gt(five.ema50)).astype("boolean")
    five["v9_5m_ema_bear"] = (five.ema9.lt(five.ema20) & five.ema20.lt(five.ema50)).astype("boolean")
    five["v9_5m_feature_ts"] = five.signal_ts
    volume = pd.to_numeric(minute.volume, errors="coerce")
    denominator = volume.shift(1).rolling(20, min_periods=5).mean()
    confirmation = minute[["ts", "open", "high", "low", "close", "volume"]].copy()
    confirmation["v9_1m_volume_ratio"] = volume.div(denominator.where(denominator.gt(0)))
    confirmation["confirmation_source_flagged"] = _flagged(minute)
    confirmation = confirmation.loc[confirmation.ts.isin(five.confirmation_ts)].copy()
    confirmation["v9_1m_feature_ts"] = confirmation.ts
    op, high, low, close = (confirmation[name].astype(float) for name in ("open", "high", "low", "close"))
    span = (high - low).where(high.gt(low))
    confirmation["body_ratio"] = (close - op).abs().div(span)
    confirmation["v9_1m_upper_wick_ratio"] = (high - pd.concat([op, close], axis=1).max(axis=1)).div(span)
    confirmation["v9_1m_lower_wick_ratio"] = (pd.concat([op, close], axis=1).min(axis=1) - low).div(span)
    confirmation = confirmation.rename(columns={"ts": "confirmation_ts", **{
        name: f"confirmation_{name}" for name in ("open", "high", "low", "close", "volume")}})
    five = five.merge(confirmation, on="confirmation_ts", how="left", validate="one_to_one")
    five["v9_exact_confirmation_present"] = five.v9_1m_feature_ts.notna()
    five["v9_feature_available_ts"] = five.v9_1m_feature_ts
    previous_future_stamp = future.ts.shift(1)
    exact_pairs = set(future.loc[future.ts.sub(previous_future_stamp).eq(pd.Timedelta(minutes=5)), "ts"])
    five["research_exact_oi_pair"] = five.signal_ts.isin(exact_pairs)
    replay.features.assert_feature_chronology(five)
    return five.drop(columns=["date", "ts"], errors="ignore")


def _parity(raw, official, day, symbol):
    old = official.loc[official.tradingsymbol.eq(symbol)].copy()
    if old.empty:
        return 0
    old["signal_ts"] = pd.to_datetime(old.signal_ts, utc=True).dt.tz_convert("Asia/Kolkata")
    check = old.merge(raw, on="signal_ts", suffixes=("_old", "_new"), validate="one_to_one")
    if len(check) != len(old):
        raise ValueError(f"Missing original candidate parity rows: {day} {symbol}")
    for field in PARITY_FIELDS:
        if field not in old or field not in raw:
            continue
        if not np.allclose(check[field + "_old"], check[field + "_new"], atol=1e-8, rtol=0, equal_nan=True):
            error = (check[field + "_old"] - check[field + "_new"]).abs().max()
            raise ValueError(f"Original feature parity failed: {day} {symbol} {field} max_delta={error}")
    for field in ("confirmation_ts", "v9_1m_feature_ts", "v9_5m_feature_ts"):
        if field in old and field in raw:
            if not pd.to_datetime(check[field + "_old"], utc=True).equals(pd.to_datetime(check[field + "_new"], utc=True)):
                raise ValueError(f"Original clock parity failed: {day} {symbol} {field}")
    return len(check)


def _coverage(raw, minute, day, symbol):
    expected = pd.DatetimeIndex([pd.Timestamp(f"{day} {clock}", tz="Asia/Kolkata") for clock in SLOT_CLOCKS])
    window = raw.loc[raw.signal_ts.isin(expected)].copy() if len(raw) else pd.DataFrame()
    missing = len(expected.difference(pd.DatetimeIndex(window.signal_ts))) if len(window) else len(expected)
    invalid = pd.Series(False, index=window.index)
    if len(window):
        positive = window[["oi", "prev_oi"]].apply(pd.to_numeric, errors="coerce")
        invalid |= ~np.isfinite(positive).all(axis=1) | ~positive.gt(0).all(axis=1)
        invalid |= ~window.research_exact_oi_pair | ~window.v9_exact_confirmation_present
        invalid |= window.v9_1m_feature_ts.ne(window.confirmation_ts) | window.v9_5m_feature_ts.ne(window.signal_ts)
        invalid |= window.confirmation_source_flagged.astype("boolean").fillna(True).astype(bool) | window.source_1m_count.ne(5)
    required_minutes = pd.date_range(f"{day} 10:01", f"{day} 15:15", freq="min", tz="Asia/Kolkata")
    target = minute.loc[minute.ts.isin(required_minutes)]
    missing_minutes = len(required_minutes.difference(pd.DatetimeIndex(target.ts)))
    flagged_minutes = int(_flagged(target).sum())
    numeric = target[["open", "high", "low", "close", "volume"]].to_numpy(float)
    invalid_minutes = int((~np.isfinite(numeric).all(axis=1) | (numeric[:, :4] <= 0).any(axis=1)
                          | (numeric[:, 4] < 0) | (numeric[:, 1] < np.maximum(numeric[:, 0], numeric[:, 3]))
                          | (numeric[:, 2] > np.minimum(numeric[:, 0], numeric[:, 3]))).sum())
    eligible = not (missing or int(invalid.sum()) or missing_minutes or flagged_minutes or invalid_minutes)
    return window.loc[~invalid].copy(), dict(day=str(day), symbol=symbol,
        expected_slots=len(expected), observed_slots=len(window), missing_slots=missing,
        invalid_slots=int(invalid.sum()),
        invalid_rows=(len(window) if missing_minutes or flagged_minutes or invalid_minutes else int(invalid.sum())),
        missing_path_minutes=missing_minutes,
        flagged_path_minutes=flagged_minutes, invalid_path_minutes=invalid_minutes,
        research_eligible=eligible)


def _read_cache(output, identity):
    manifest_path = output / "manifest.json"
    if manifest_path.is_file():
        manifest = g2.read_json(manifest_path)
        if manifest.get("identity") != identity or manifest.get("complete") is not True:
            raise ValueError("Existing extension cache identity differs")
        for name, checksum in manifest["artifacts"].items():
            if g2.sha256(output / name) != checksum:
                raise ValueError(f"Extension cache changed: {name}")
        signals = pd.read_parquet(output / "signals.parquet")
        paths = {}
        with np.load(output / "paths.npz", allow_pickle=False) as archive:
            for name in archive.files:
                sid, field = name.split("_", 1)
                paths.setdefault(int(sid), {})[field] = archive[name].copy()
        coverage = pd.read_csv(output / "coverage.csv")
        if len(signals):
            g2.g.v9.validate_paths(signals, paths)
        return signals, paths, coverage, manifest["evidence"]
    return None


def _write_cache(output, identity, signals, paths, coverage, evidence):
    output.mkdir(parents=True, exist_ok=True)
    signals.to_parquet(output / "signals.parquet", index=False)
    np.savez_compressed(output / "paths.npz", **{
        f"{key}_{field}": values for key, path in paths.items() for field, values in path.items()})
    coverage.to_csv(output / "coverage.csv", index=False)
    artifacts = {name: g2.sha256(output / name) for name in ("signals.parquet", "paths.npz", "coverage.csv")}
    g2.dump_json(output / "manifest.json", dict(complete=True, identity=identity, artifacts=artifacts, evidence=evidence))


def _build_day(job):
    index, info, manifest_path_source, expected_snapshot_hash, output, parent_identity = job
    day = date.fromisoformat(info["day"])
    run = Path(info["source_run"])
    identity = dict(parent=parent_identity, source=info, snapshot_sha256=expected_snapshot_hash, sid_block=index)
    output = Path(output)
    cached = _read_cache(output, identity)
    if cached is not None:
        return index, cached
    if output.exists():
        raise FileExistsError(f"Incomplete existing per-day extension cache: {output}")
    manifest_path_source = Path(manifest_path_source)
    if g2.sha256(manifest_path_source) != expected_snapshot_hash:
        raise ValueError(f"Snapshot manifest changed: {day}")
    snapshot_manifest = g2.read_json(manifest_path_source)
    snapshot = manifest_path_source.parent
    print(f"Verifying sealed extension inputs for {day}", flush=True)
    replay._verify_input_snapshot(snapshot, snapshot_manifest)
    source_manifest = g2.read_json(run / "source_manifest.json")
    record = next(item for item in source_manifest["sources"] if item["role"] == "DATED_UNIVERSE")
    universe_path = Path(record["path"])
    if g2.sha256(universe_path) != record["sha256"]:
        raise ValueError(f"Dated universe changed: {day}")
    universe = pd.read_parquet(universe_path)
    prior_coverage = pd.read_csv(run / "coverage.csv")
    clean = prior_coverage[["missing_equity_minutes", "missing_futures_bars"]].eq(0).all(axis=1)
    symbols = prior_coverage.loc[clean, "symbol"].tolist()
    official = pd.read_csv(run / "candidate_signals.csv", float_precision="round_trip")
    parity_count = 0
    sid = 10_000_000 + index * 1_000_000
    frames, rows, paths = [], [], {}
    for position, symbol in enumerate(symbols):
        problems = []
        contracts = universe.loc[universe.equity_symbol.eq(symbol)]
        if len(contracts) != 1:
            raise ValueError(f"Ambiguous dated contract: {day} {symbol}")
        contract = contracts.iloc[0]
        future_symbol = contract.futures_tradingsymbol
        minute_path = hybrid.equity_one_minute_path(symbol, snapshot / "equity_1m")
        future_path = snapshot / "futures_5m" / f"{ext.common.safe_contract_stem(future_symbol)}_5minute.parquet"
        minute = replay._load_minute(minute_path, day, problems, symbol)
        future = replay._load_future(future_path, day, problems, symbol)
        if problems:
            raise ValueError(problems)
        raw = _pool(minute, future, day=day, symbol=symbol,
                    future_symbol=future_symbol, month=contract.contract_month)
        parity_count += _parity(raw, official, day, symbol)
        window, coverage_row = _coverage(raw, minute, day, symbol)
        rows.append(coverage_row)
        if len(window) and coverage_row["research_eligible"]:
            strict = replay._strict_signals(window, float("nan"))
            if len(strict):
                strict["sid"] = np.arange(sid, sid + len(strict), dtype=np.int64)
                sid += len(strict)
                strict["instrument_token"] = contract.equity_instrument_token
                strict["futures_instrument_token"] = contract.futures_instrument_token
                strict["exchange"] = contract.equity_exchange
                for decision in strict.itertuples(index=False):
                    selected = minute.loc[minute.ts.gt(decision.confirmation_ts)
                                          & minute.ts.le(replay._cutoff(day))]
                    paths[int(decision.sid)] = {"timestamp_ns": selected.ts.astype("int64").to_numpy(),
                        **{field: selected[field].to_numpy(float) for field in ("open", "high", "low", "close")}}
                frames.append(strict)
        if position % 40 == 0 or position + 1 == len(symbols):
            print(f"Unused-slot reconstruction {day}: {position + 1}/{len(symbols)} stocks", flush=True)
    if parity_count != len(official):
        raise ValueError(f"Original candidate parity count mismatch: {day}: {parity_count}/{len(official)}")
    evidence = dict(day=str(day), source_run=str(run),
        source_manifest_sha256=g2.sha256(run / "source_manifest.json"),
        coverage_sha256=g2.sha256(run / "coverage.csv"),
        original_candidates_sha256=g2.sha256(run / "candidate_signals.csv"),
        snapshot_manifest=str(manifest_path_source), snapshot_manifest_sha256=expected_snapshot_hash,
        universe_path=str(universe_path), universe_sha256=record["sha256"],
        original_candidate_feature_parity_rows=parity_count,
        original_excluded_symbols=prior_coverage.loc[~clean, "symbol"].tolist())
    signals = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
    coverage = pd.DataFrame(rows)
    if len(signals):
        g2.g.v9.validate_paths(signals, paths)
    _write_cache(output, identity, signals, paths, coverage, evidence)
    return index, (signals, paths, coverage, evidence)


def build_extension_data(baseline_summary: dict, output: Path):
    """Return strict new-slot signals, paths, per-symbol coverage, provenance."""
    output = Path(output) / "extension_inputs"
    identity = dict(schema=SCHEMA, baseline_summary_sha256=_fingerprint(baseline_summary),
                    helper_sha256=g2.sha256(Path(__file__)))
    cached = _read_cache(output, identity)
    if cached is not None:
        return cached
    identity_path = output / "build_identity.json"
    if output.exists():
        if not identity_path.is_file() or g2.read_json(identity_path) != identity:
            raise FileExistsError(f"Unrecognized existing extension cache: {output}")
    else:
        output.mkdir(parents=True)
        g2.dump_json(identity_path, identity)
    known = baseline_summary["verified_snapshot_manifests"]
    snapshots = {}
    for name, checksum in known.items():
        path = Path(name)
        if g2.sha256(path) != checksum:
            raise ValueError(f"Snapshot manifest changed: {path}")
        manifest = g2.read_json(path)
        if manifest.get("complete") is not True:
            raise ValueError(f"Incomplete snapshot: {path}")
        snapshots[manifest["session_date"]] = (str(path), checksum)
    jobs = []
    for index, info in enumerate(baseline_summary["daily_sources"]):
        source_day = info["day"]
        snapshot_day = source_day if source_day in snapshots else "2026-09-25"
        if source_day not in snapshots and source_day != "2026-09-24":
            raise ValueError(f"No explicitly supported sealed snapshot for {source_day}")
        manifest_path_source, checksum = snapshots[snapshot_day]
        jobs.append((index, info, manifest_path_source, checksum, output / "days" / source_day, identity))
    results = {}
    with ProcessPoolExecutor(max_workers=3, mp_context=multiprocessing.get_context("spawn")) as executor:
        pending = [executor.submit(_build_day, job) for job in jobs]
        for future in as_completed(pending):
            index, result = future.result()
            results[index] = result
    ordered = [results[index] for index in sorted(results)]
    signals = pd.concat([item[0] for item in ordered if len(item[0])], ignore_index=True, sort=False)
    paths = {sid: path for item in ordered for sid, path in item[1].items()}
    coverage = pd.concat([item[2] for item in ordered], ignore_index=True)
    if len(signals):
        if signals.sid.duplicated().any():
            raise ValueError("Parallel extension signal identity collision")
        g2.g.v9.validate_paths(signals, paths)
    evidence = dict(schema=SCHEMA, full_native_history_ema=True, original_feature_parity_atol=1e-8,
        original_feature_parity_rtol=0, sources=[item[3] for item in ordered], strict_candidates=len(signals),
        path_count=len(paths), research_slots=list(SLOT_CLOCKS), selection_authority=False,
        excluded_research_days=sorted(coverage.loc[~coverage.research_eligible, "day"].unique().tolist()))
    _write_cache(output, identity, signals, paths, coverage, evidence)
    return signals, paths, coverage, evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-summary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    signals, paths, coverage, evidence = build_extension_data(g2.read_json(args.baseline_summary), args.output)
    print(f"Extension inputs complete: {len(signals)} strict candidates, {len(paths)} exact paths", flush=True)
    print(coverage.groupby("day").agg(symbols=("symbol", "size"), observed_slots=("observed_slots", "sum"),
                                      invalid_rows=("invalid_rows", "sum")).to_string(), flush=True)
    print(f"Excluded research days: {evidence['excluded_research_days']}", flush=True)


if __name__ == "__main__":
    main()
