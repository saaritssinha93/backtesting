"""Targeted, isolated recovery of real cash bars for unresolved V14 paths.

This module never changes canonical data, creates bars, aliases symbols,
places orders, or alters recorded selections. Broker requests are cached in
the supplied repair folder; existing canonical observations remain primary.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from . import data

FLAGS = ("gap_filled", "provisional_stale", "opening_snapshot")
DEFAULT_RUN = data.DEFAULT_ROOT / "runs/20261010_participation_uncapped_v1"
DEFAULT_OUTPUT = data.DEFAULT_ROOT / "repairs/20261010_uncapped_gaps_v1"


def missing_minutes(frame: pd.DataFrame, day: str) -> list[pd.Timestamp]:
    expected = pd.date_range(f"{day} 09:16", f"{day} 15:15", freq="min", tz="Asia/Kolkata")
    actual = pd.DatetimeIndex(frame.ts) if len(frame) else pd.DatetimeIndex([], tz="Asia/Kolkata")
    return list(expected.difference(actual))


def append_real_missing(canonical: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    """Never replace existing bars or create timestamp rows absent from source."""
    if incoming.ts.duplicated().any() or canonical.ts.duplicated().any():
        raise ValueError("Ambiguous duplicate timestamp")
    extra = incoming.loc[~incoming.ts.isin(canonical.ts)]
    return pd.concat([canonical, extra], ignore_index=True).sort_values("ts").reset_index(drop=True)


def exact_cash_tokens(master: pd.DataFrame, symbols: list[str]) -> dict[str, dict[str, Any]]:
    rows = master.loc[master.exchange.eq("NSE") & master.instrument_type.eq("EQ") & master.tradingsymbol.isin(symbols)]
    if rows.tradingsymbol.duplicated().any():
        raise ValueError("Ambiguous exact cash identity")
    return {str(r.tradingsymbol): dict(instrument_token=int(r.instrument_token),
        tradingsymbol=str(r.tradingsymbol), exchange="NSE", instrument_type="EQ",
        name=str(getattr(r, "name", ""))) for r in rows.itertuples()}


def _raw(path: Path) -> pd.DataFrame:
    columns = pq.ParquetFile(path).schema_arrow.names
    wanted = [c for c in ("ts", "timestamp", "date", *data.OHLCV, *FLAGS,
                          "timestamp_convention", "raw_source") if c in columns]
    return pd.read_parquet(path, columns=wanted)


def _native_status(frame: pd.DataFrame, stamp: pd.Timestamp) -> dict:
    if frame.empty:
        return dict(present=False, flags=[])
    time_col = next(c for c in ("ts", "timestamp", "date") if c in frame)
    times = data._ist(frame[time_col])
    rows = frame.loc[times.eq(stamp)]
    flags = [name for name in FLAGS if name in rows and rows[name].fillna(False).astype(str).str.lower().isin(["true", "1", "1.0"]).any()]
    return dict(present=bool(len(rows)), rows=int(len(rows)), flags=flags)


def inventory(run: Path, root: Path, output: Path) -> dict:
    unknown_path, evidence_path = run / "unknown_outcomes.csv", run / "path_evidence.json"
    unknown = pd.read_csv(unknown_path)
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    masters = root / "data/instruments/nse_latest.parquet"
    master = pd.read_parquet(masters)
    identities = exact_cash_tokens(master, sorted(unknown.symbol.unique()))
    canonical_hashes, alternative_hashes, rows, pairs = {}, {}, [], []
    for symbol, group in unknown.groupby("symbol", sort=True):
        source = root / "data/1m" / f"{symbol}.parquet"
        canonical_hashes[str(source)] = data._sha(source)
        minute = data.load_symbol_1m(symbol, root=root)
        native_path = data.SOURCE_ROOT / "stocks_indicators_1min_eq" / f"{symbol}_stocks_indicators_1min.parquet"
        native = _raw(native_path) if native_path.exists() else pd.DataFrame()
        raw_path = data.SOURCE_ROOT / "stocks_raw_1min_entry_v5_id_live" / f"{symbol}_stocks_raw_1min.parquet"
        alternative = pd.DataFrame()
        if raw_path.exists():
            raw = _raw(raw_path)
            alternative_hashes[str(raw_path)] = data._sha(raw_path)
            # Only explicit source labels authorize a timestamp conversion.
            if "timestamp_convention" in raw:
                raw = raw.loc[raw.timestamp_convention.eq("candle_start_1m")].copy()
                alternative = data.normalize_cash_minutes(raw, symbol,
                    source="native_raw_kite_historical_candle_start_1m", start_labeled=True)
        for day, day_group in group.groupby("day", sort=True):
            current = minute.loc[minute.ts.dt.strftime("%Y-%m-%d").eq(day)].copy()
            missing = missing_minutes(current, day)
            alt_day = (alternative.loc[alternative.ts.dt.strftime("%Y-%m-%d").eq(day)].copy()
                       if len(alternative) else data.normalize_cash_minutes(pd.DataFrame(), symbol, source="empty"))
            extra = alt_day.loc[~alt_day.ts.isin(current.ts)]
            if len(extra):
                data._parquet(output / "native_recovery_1m" / f"{symbol}_{day}.parquet", extra)
            pairs.append(dict(symbol=symbol, day=day, unresolved_orders=len(day_group),
                canonical_minutes=len(current), missing_execution_minutes=[str(t) for t in missing],
                native_alternative_recovered_minutes=[str(t) for t in extra.ts],
                cash_identity=identities.get(symbol), canonical_sha256=canonical_hashes[str(source)]))
            for order in day_group.itertuples():
                proof = evidence[str(order.sid)]
                interruption = proof.get("first_interruption") or {}
                stamp = pd.Timestamp(interruption["expected_ts"])
                native_status = _native_status(native, stamp)
                rows.append(dict(sid=int(order.sid), symbol=symbol, day=day, setup_id=order.setup_id,
                    reason=order.reason, interruption_reason=interruption.get("reason"),
                    first_missing_ts=str(stamp), next_observed_ts=interruption.get("observed_ts"),
                    entry=proof.get("entry"), canonical_first_gap_present=bool(current.ts.eq(stamp).any()),
                    native_first_gap=native_status,
                    alternative_first_gap_present=bool(len(alt_day) and alt_day.ts.eq(stamp).any())))
    result = dict(status="INVENTORY_COMPLETE", created_at=datetime.now(data.IST).isoformat(),
        source_run=str(run), canonical_root=str(root), output=str(output),
        unresolved_orders=len(unknown), unique_symbols=int(unknown.symbol.nunique()),
        unique_days=int(unknown.day.nunique()), symbol_days=len(pairs),
        reason_counts=unknown.reason.value_counts().to_dict(),
        first_gap_native_present=sum(r["native_first_gap"]["present"] for r in rows),
        first_gap_native_flagged=sum(bool(r["native_first_gap"]["flags"]) for r in rows),
        first_gap_alternative_present=sum(r["alternative_first_gap_present"] for r in rows),
        missing_exact_cash_identities=sorted(set(unknown.symbol)-set(identities)),
        source_sha256={str(unknown_path): data._sha(unknown_path), str(evidence_path): data._sha(evidence_path), str(masters): data._sha(masters)},
        canonical_sha256=canonical_hashes, alternative_source_sha256=alternative_hashes,
        synthetic_bars=False, canonical_mutations=False, orders=rows, pairs=pairs)
    data._json(output / "inventory.json", result)
    pd.DataFrame([{k:v for k,v in r.items() if k not in ["entry", "native_first_gap"]} |
                  {"native_first_gap_present": r["native_first_gap"]["present"],
                   "native_first_gap_flags": ";".join(r["native_first_gap"]["flags"])} for r in rows]).to_csv(output / "first_interruptions.csv", index=False)
    return result


def fetch_pair(client, pair: dict, output: Path, *, interval: str = "minute") -> dict:
    symbol, day = pair["symbol"], pair["day"]
    unit = "1m" if interval == "minute" else "5m"
    audit_path = output / f"broker_{unit}" / f"{symbol}_{day}.json"
    bars_path = audit_path.with_suffix(".parquet")
    identity = pair["cash_identity"]
    if audit_path.exists() and bars_path.exists():
        saved = json.loads(audit_path.read_text(encoding="utf-8"))
        if saved.get("cash_identity") == identity and saved.get("interval") == interval and saved.get("bars_sha256") == data._sha(bars_path):
            return saved
    record = dict(symbol=symbol, day=day, cash_identity=identity, interval=interval,
                  fetched_at=datetime.now(data.IST).isoformat(), source="kite_historical_data", synthetic_bars=False,
                  request_from=f"{day} 09:15:00", request_through=f"{day} 15:29:00",
                  raw_timestamp_convention="candle_start", continuous=False, oi=False)
    if identity is None:
        record.update(status="NO_EXACT_CASH_IDENTITY")
    else:
        try:
            if interval == "minute":
                raw = client.history(identity["instrument_token"], day, day)
            else:
                raw = client._call("historical_data", identity["instrument_token"],
                    f"{day} 09:15:00", f"{day} 15:29:00", "5minute", continuous=False, oi=False)
            returned = pd.DataFrame(raw)
            # Preserve the broker's original start-labelled payload separately.
            raw_path = output / f"broker_raw_{unit}" / bars_path.name
            data._parquet(raw_path, returned)
            record["raw_payload_sha256"] = data._sha(raw_path)
            if interval == "minute":
                bars = data.normalize_cash_minutes(returned, symbol, source="kite_targeted_gap_recovery_1m", start_labeled=True)
            else:
                bars = returned.copy()
                if len(bars):
                    bars["ts"] = data._ist(bars["date"]) + pd.Timedelta(minutes=5)
                    bars["symbol"], bars["source"] = symbol, "KITE_HISTORICAL_5MINUTE"
                    bars["independent_interval"] = True
                    bars["candle_minutes"] = 5
                    bars["source_hash"] = record["raw_payload_sha256"]
            data._parquet(bars_path, bars)
            record.update(status="FETCHED", rows_returned=len(raw), normalized_rows=len(bars),
                timestamp_convention="candle_end_1m" if interval == "minute" else "candle_end_5m",
                bars_sha256=data._sha(bars_path),
                recovered_missing_minutes=([str(t) for t in bars.ts if str(t) in pair["missing_execution_minutes"]] if interval == "minute" else []))
        except Exception as exc:
            # Exception type only: external messages can contain secret URLs.
            record.update(status="FETCH_ERROR", error_type=type(exc).__name__)
    data._json(audit_path, record)
    return record


def _validate_ohlcv(frame: pd.DataFrame, day: str, candle_minutes: int) -> None:
    if frame.empty:
        return
    stamps = data._ist(frame.ts)
    values = frame[data.OHLCV].apply(pd.to_numeric, errors="raise")
    assert not stamps.duplicated().any(), "Duplicate repaired timestamps"
    assert stamps.dt.strftime("%Y-%m-%d").eq(day).all(), "Wrong repaired date"
    assert stamps.dt.second.eq(0).all(), "Off-clock repaired timestamp"
    assert np.isfinite(values).all().all() and values.volume.ge(0).all(), "Invalid repaired OHLCV"
    assert values[["open", "high", "low", "close"]].gt(0).all().all(), "Nonpositive price"
    assert values.high.ge(values[["open", "close", "low"]].max(axis=1)).all(), "Invalid high"
    assert values.low.le(values[["open", "close", "high"]].min(axis=1)).all(), "Invalid low"
    clock = stamps.dt.hour * 60 + stamps.dt.minute
    assert clock.between(555 + candle_minutes, 930).all(), "Outside NSE cash session"
    assert ((clock - 555) % candle_minutes).eq(0).all(), "Wrong interval alignment"


def load_verified_repair(symbol: str, day: str, repairdir: Path, *,
                         root: Path | None = None, source_run: Path | None = None
                         ) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Verify request identity/payload and return only real missing minute bars.

    Existing bars are never overwritten. The optional caller paths must match
    the inventory; all canonical, master, audit and payload hashes are checked.
    Early 1m audits are augmented by a separate hash-bound verification record,
    preserving the original audit and raw payload byte for byte.
    """
    repairdir = Path(repairdir).resolve()
    inventory_path = repairdir / "inventory.json"
    inv = json.loads(inventory_path.read_text(encoding="utf-8"))
    verified_path = repairdir / "verified_payloads" / f"{symbol}_{day}.json"
    previous = json.loads(verified_path.read_text(encoding="utf-8")) if verified_path.exists() else None
    if previous is not None:
        assert previous["inventory_sha256"] == data._sha(inventory_path), "Verified inventory changed"
    canonical_root, recorded_run = Path(inv["canonical_root"]).resolve(), Path(inv["source_run"]).resolve()
    assert root is None or Path(root).resolve() == canonical_root, "Canonical root differs from inventory"
    assert source_run is None or Path(source_run).resolve() == recorded_run, "Source run differs from inventory"
    pair = next(p for p in inv["pairs"] if p["symbol"] == symbol and p["day"] == day)
    source_path = canonical_root / "data/1m" / f"{symbol}.parquet"
    assert data._sha(source_path) == inv["canonical_sha256"][str(source_path)], "Canonical source hash mismatch"
    master_path = canonical_root / "data/instruments/nse_latest.parquet"
    assert data._sha(master_path) == inv["source_sha256"][str(master_path)], "Cash instrument master changed"
    identity = exact_cash_tokens(pd.read_parquet(master_path), [symbol]).get(symbol)
    assert identity is not None and identity == pair["cash_identity"], "Cash identity mismatch"
    for filename in ("unknown_outcomes.csv", "path_evidence.json"):
        source_file = recorded_run / filename
        assert data._sha(source_file) == inv["source_sha256"][str(source_file)], "Source outcome evidence changed"
    canonical = data.load_symbol_1m(symbol, root=canonical_root, start=day, end=day)
    verified_frames, records = {}, {}
    for unit, interval, count in [("1m", "minute", 1), ("5m", "5minute", 5)]:
        audit_path = repairdir / f"broker_{unit}" / f"{symbol}_{day}.json"
        bars_path = audit_path.with_suffix(".parquet")
        raw_path = repairdir / f"broker_raw_{unit}" / bars_path.name
        audit = json.loads(audit_path.read_text(encoding="utf-8"))
        assert audit["status"] == "FETCHED", "Repair fetch did not succeed"
        assert audit["symbol"] == symbol and audit["day"] == day and audit["interval"] == interval, "Wrong repair request"
        assert audit["cash_identity"] == identity and audit["source"] == "kite_historical_data", "Untrusted repair source"
        assert audit["synthetic_bars"] is False, "Synthetic repair rejected"
        assert data._sha(bars_path) == audit["bars_sha256"], "Repair bar hash mismatch"
        raw_hash = data._sha(raw_path)
        if previous is not None:
            prior = previous["verified_sources"][unit]
            assert prior["audit_sha256"] == data._sha(audit_path), "Previously verified audit changed"
            assert prior["raw_payload_sha256"] == raw_hash, "Previously verified raw payload changed"
            assert prior["bars_sha256"] == data._sha(bars_path), "Previously verified bars changed"
        legacy_request_contract = "raw_payload_sha256" not in audit
        if legacy_request_contract:
            assert unit == "1m", "Missing independent interval provenance"
            # The original 1m fetch used this immutable, read-only helper:
            # full cash day, interval minute, continuous=False, oi=False.
            protocol = json.loads((recorded_run / "protocol.json").read_text(encoding="utf-8"))
            assert data._sha(Path(data.__file__)) == protocol["source_sha256"]["data.py"], "Legacy request helper changed"
        else:
            assert raw_hash == audit["raw_payload_sha256"], "Raw repair payload hash mismatch"
            assert audit["request_from"] == f"{day} 09:15:00" and audit["request_through"] == f"{day} 15:29:00", "Wrong request window"
            assert audit["continuous"] is False and audit["oi"] is False, "Wrong request flags"
            assert audit["raw_timestamp_convention"] == "candle_start", "Unknown raw timestamp convention"
        raw, frame = pd.read_parquet(raw_path), pd.read_parquet(bars_path)
        assert audit["rows_returned"] == len(raw) and audit["normalized_rows"] == len(frame), "Repair row count mismatch"
        assert audit["timestamp_convention"] == f"candle_end_{unit}", "Wrong repaired timestamp convention"
        if unit == "1m":
            expected = data.normalize_cash_minutes(raw, symbol, source="kite_targeted_gap_recovery_1m", start_labeled=True)
            pd.testing.assert_frame_equal(frame.reset_index(drop=True), expected.reset_index(drop=True), check_dtype=False)
        elif len(frame):
            assert not set(FLAGS + ("source_1m_count",)).intersection(frame), "Derived interval rejected"
            assert frame.source.eq("KITE_HISTORICAL_5MINUTE").all(), "Not independent Kite interval"
            assert frame.independent_interval.eq(True).all() and frame.candle_minutes.eq(5).all(), "Wrong interval provenance"
            assert frame.source_hash.eq(raw_hash).all(), "Interval source hash mismatch"
            assert frame.symbol.eq(symbol).all(), "Wrong interval symbol"
            assert data._ist(raw.date).add(pd.Timedelta(minutes=5)).equals(data._ist(frame.ts)), "Interval timestamp mismatch"
            pd.testing.assert_frame_equal(frame[data.OHLCV].reset_index(drop=True), raw[data.OHLCV].reset_index(drop=True), check_dtype=False)
        _validate_ohlcv(frame, day, count)
        verified_frames[unit] = frame
        records[unit] = dict(audit_sha256=data._sha(audit_path), bars_sha256=audit["bars_sha256"],
            raw_payload_sha256=raw_hash, cash_identity=identity, interval=interval,
            request_from=f"{day} 09:15:00", request_through=f"{day} 15:29:00", continuous=False, oi=False,
            legacy_request_contract_from_frozen_history_helper=legacy_request_contract)
    incoming = verified_frames["1m"]
    overlap = canonical[["ts", *data.OHLCV]].merge(incoming[["ts", *data.OHLCV]], on="ts", suffixes=("_canonical", "_broker"))
    disagreements, material_price_differences = [], []
    for row in overlap.itertuples(index=False):
        delta = {c: float(getattr(row, c+"_broker") - getattr(row, c+"_canonical")) for c in data.OHLCV}
        if any(v != 0 for v in delta.values()):
            disagreements.append(dict(ts=str(row.ts), broker_minus_canonical=delta))
        # Native OHLC archives store float32 values. Allow only their storage
        # rounding (half one float32 ULP), not a tick-size or percentage margin.
        material = {c: delta[c] for c in ("open", "high", "low", "close")
                    if abs(delta[c]) > abs(float(np.spacing(np.float32(abs(getattr(row, c+"_canonical")))))) / 2. + 1e-8}
        if material:
            material_price_differences.append(dict(ts=str(row.ts), broker_minus_canonical=material))
    price_scale_compatible = bool(len(overlap) and not material_price_differences)
    additions = incoming.loc[~incoming.ts.isin(canonical.ts)].copy()
    rejected_additions = len(additions) if not price_scale_compatible else 0
    envelopes = verified_frames["5m"].copy()
    envelopes["price_scale_compatible"] = price_scale_compatible
    if not price_scale_compatible:
        # A trustworthy current payload may nevertheless be on a different
        # historical price scale. Withhold both kinds of evidence; no rescale.
        additions, envelopes = additions.iloc[:0].copy(), envelopes.iloc[:0].copy()
    provenance = dict(status="VERIFIED", symbol=symbol, day=day, inventory_sha256=data._sha(inventory_path),
        canonical_sha256=inv["canonical_sha256"][str(source_path)], master_sha256=data._sha(master_path),
        fetched_at_1m=json.loads((repairdir / "broker_1m" / f"{symbol}_{day}.json").read_text())["fetched_at"],
        verified_sources=records, minute_additions=len(additions), existing_observations_retained=True,
        price_scale_compatible=price_scale_compatible, material_price_difference_minutes=len(material_price_differences),
        material_price_differences=material_price_differences,
        withheld_minute_additions=rejected_additions, withheld_5m_rows=len(verified_frames["5m"]) - len(envelopes),
        compatibility_tolerance="half_float32_storage_ULP_plus_1e-8_absolute",
        overlap_minutes=len(overlap), overlap_exact_differences=len(disagreements), overlap_differences=disagreements)
    data._json(verified_path, provenance)
    return additions, envelopes, provenance


def run(*, source_run=DEFAULT_RUN, root=data.DEFAULT_ROOT, output=DEFAULT_OUTPUT,
        fetch=False, workers=3, interval="minute") -> dict:
    source_run, root, output = Path(source_run).resolve(), Path(root).resolve(), Path(output).resolve()
    if output == root or output == source_run or root / "data" == output or (root / "data") in output.parents:
        raise ValueError("Repair output must be separate from canonical data and source run")
    output.mkdir(parents=True, exist_ok=True)
    inventory_path = output / "inventory.json"
    inv = json.loads(inventory_path.read_text(encoding="utf-8")) if inventory_path.exists() else inventory(source_run, root, output)
    assert inv["source_run"] == str(source_run) and inv["canonical_root"] == str(root)
    if not fetch:
        return {k:v for k,v in inv.items() if k not in ["orders", "pairs", "canonical_sha256", "source_sha256", "alternative_source_sha256"]}
    try:
        client = data.MarketDataClient()
    except Exception as exc:
        result = dict(status="AUTH_UNAVAILABLE", error_type=type(exc).__name__, fetched_pairs=0)
        data._json(output / "fetch_summary.json", result)
        return result
    results = []
    with ThreadPoolExecutor(max_workers=max(1, min(3, int(workers)))) as executor:
        pending = [executor.submit(fetch_pair, client, pair, output, interval=interval) for pair in inv["pairs"]]
        for task in as_completed(pending):
            results.append(task.result())
            if len(results) % 20 == 0 or len(results) == len(pending):
                progress = dict(status="FETCHING", completed=len(results), total=len(pending), interval=interval)
                data._json(output / "progress.json", progress)
                print(json.dumps(progress), flush=True)
    recovered = [(r["symbol"], r["day"], ts) for r in results for ts in r.get("recovered_missing_minutes", [])]
    intact = all(data._sha(Path(path)) == sha for path, sha in inv["canonical_sha256"].items())
    result = dict(status="COMPLETE", interval=interval, completed=len(results), app=client.app_name,
        request_rate_limit="one process, >=0.46s between starts", canonical_hashes_unchanged=intact,
        errors=sum(r["status"] != "FETCHED" for r in results),
        recovered_missing_minutes=len(recovered), recovered_symbol_days=len(set((s,d) for s,d,t in recovered)),
        first_interruptions_recovered=sum((r["symbol"],r["day"],r["first_missing_ts"]) in recovered for r in inv["orders"]),
        results=results)
    assert intact, "Canonical input changed during repair audit"
    data._json(output / ("fetch_summary.json" if interval == "minute" else "fetch_5m_summary.json"), result)
    data._json(output / "progress.json", {k:v for k,v in result.items() if k != "results"})
    return {k:v for k,v in result.items() if k != "results"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-run", type=Path, default=DEFAULT_RUN)
    parser.add_argument("--root", type=Path, default=data.DEFAULT_ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--interval", choices=["minute", "5minute"], default="minute")
    args = parser.parse_args()
    print(json.dumps(run(**vars(args)), indent=2), flush=True)


if __name__ == "__main__":
    main()
