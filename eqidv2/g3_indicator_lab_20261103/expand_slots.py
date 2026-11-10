"""Rebuild missing donor clocks from sealed inputs; never replace existing rows.

Run: python -B -m g3_indicator_lab_20261103.expand_slots --workers 4
Only the all51_slots ablation may consume these supplementary rows. The caller
assigns globally unique sid values after loading each supplementary parquet.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
import hashlib
import json
from pathlib import Path
import time

import numpy as np
import pandas as pd

import fno_v13_replay_source as native
from research import g3_target_cutoff_contract as donor

LAB = Path(__file__).resolve().parent
VERSION = "sealed_missing_donor_clocks_v1"
CLOCKS = tuple(sorted(donor.SIGNAL_TO_CONFIRMATION))
PARITY_FIELDS = ("open", "high", "low", "close", "volume", "prev_close",
    "ema9", "ema20", "ema50", "price_change_pct", "volume_ratio", "traded_value",
    "oi", "prev_oi", "oi_change_pct", "confirmation_open", "confirmation_high",
    "confirmation_low", "confirmation_close", "confirmation_volume",
    "v9_1m_volume_ratio", "v9_1m_ema20", "v9_1m_atr14", "body_ratio",
    "v9_1m_upper_wick_ratio", "v9_1m_lower_wick_ratio")


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, default=str, allow_nan=False), encoding="utf-8")
    temporary.replace(path)


def resolve_snapshot(provenance):
    path = Path(provenance["snapshot_manifest"])
    expected = provenance["snapshot_manifest_sha256"]
    chain = []
    while True:
        if sha(path) != expected:
            raise ValueError(f"Snapshot manifest hash mismatch: {path}")
        manifest = json.loads(path.read_text(encoding="utf-8"))
        chain.append({"path": str(path), "sha256": expected})
        if "DAY_ONLY" not in str(manifest.get("price_history_scope", "")):
            break
        # A compact execution snapshot cannot initialize historical EMAs.
        path = Path(manifest["original_snapshot_manifest"])
        expected = manifest["original_snapshot_manifest_sha256"]
    native._verify_input_snapshot(path.parent, manifest)
    return path, manifest, chain


def member_path(root, manifest, relative):
    members = {x["snapshot_relative_path"].replace("\\", "/"): x
               for x in manifest["sources"]}
    key = relative.replace("\\", "/")
    record = members.get(key)
    if not record or not record.get("captured"):
        raise ValueError(f"Sealed member unavailable: {relative}")
    path = (root / relative).resolve()
    path.relative_to(root.resolve())
    return path, record


def load_minute_window(path, day, problems, symbol):
    """Native minute normalization, with a parquet predicate for bounded warmup.

    The complete sealed file has already passed its content hash. Twenty
    calendar days are ample for 1,000 earlier session minutes; existing-clock
    parity rejects sparse histories that would make this window insufficient.
    """
    schema = native.pq.ParquetFile(path).schema_arrow
    required = ["date", "open", "high", "low", "close", "volume"]
    absent = set(required)-set(schema.names)
    if absent:
        raise ValueError(f"Missing equity fields: {sorted(absent)}")
    date_type = schema.field("date").type
    if not hasattr(date_type, "tz"):
        return native._load_minute(path, day, problems, symbol)
    floor = pd.Timestamp(day) - pd.Timedelta(days=20)
    if date_type.tz:
        floor = floor.tz_localize(date_type.tz)
    optional = [x for x in ("gap_filled", "opening_snapshot", "provisional_stale") if x in schema.names]
    frame = pd.read_parquet(path, columns=required+optional, filters=[("date", ">=", floor)])
    frame["ts"] = native.hybrid._to_ist(frame["date"]).dt.as_unit("ns")
    if frame.loc[frame.ts.dt.date.eq(day)].ts.duplicated().any():
        native._problem(problems, symbol, "DUPLICATE_REQUESTED_DAY_EQUITY_MINUTES")
    if frame.ts.isna().any():
        native._problem(problems, symbol, "INVALID_EQUITY_TIMESTAMP")
    numeric = ["open", "high", "low", "close", "volume"]
    frame[numeric] = frame[numeric].apply(pd.to_numeric, errors="coerce")
    return (frame.loc[frame.ts.le(native._cutoff(day))].dropna(subset=["ts"])
            .sort_values("ts", kind="stable").drop_duplicates("ts", keep="last").reset_index(drop=True))


def seed_exact_emas(pool, minute_window, archived):
    """Continue native EMA recurrences from an already sealed earlier value.

    This is algebraically the same adjust=False EMA, without recomputing every
    preceding year of minutes. Rolling-volume and ATR windows retain 1,000 real
    minutes; parity is checked at every existing non-opening clock afterwards.
    """
    anchor = archived.sort_values("signal_ts").iloc[0]
    anchor_ts = anchor.signal_ts
    five = native.hybrid.aggregate_equity_one_minute_to_five_minute(minute_window)
    following = five.loc[five.ts.gt(anchor_ts), ["ts", "close"]]
    if pool.signal_ts.min() < anchor_ts:
        raise ValueError("No earlier sealed EMA anchor for supplementary clocks")
    for span in (9, 20, 50):
        current = float(anchor[f"ema{span}"])
        if not np.isfinite(current):
            raise ValueError("Nonfinite sealed EMA anchor")
        values = {anchor_ts: current}
        alpha = 2.0 / (span + 1.0)
        for stamp, close in following.itertuples(index=False, name=None):
            current = (1.0 - alpha) * current + alpha * float(close)
            values[stamp] = current
        pool[f"ema{span}"] = pool.signal_ts.map(values)
        pool[f"v9_5m_ema{span}"] = pool[f"ema{span}"]
    pool["v9_5m_ema_bull"] = pool.ema9.gt(pool.ema20) & pool.ema20.gt(pool.ema50)
    pool["v9_5m_ema_bear"] = pool.ema9.lt(pool.ema20) & pool.ema20.lt(pool.ema50)
    one_anchor = archived.loc[archived.v9_1m_ema20.notna()].sort_values("confirmation_ts").iloc[0]
    stamp = one_anchor.confirmation_ts
    current = float(one_anchor.v9_1m_ema20)
    values = {stamp: current}
    alpha = 2.0 / 21.0
    following = minute_window.loc[minute_window.ts.gt(stamp), ["ts", "close"]]
    for stamp, close in following.itertuples(index=False, name=None):
        current = (1.0 - alpha) * current + alpha * float(close)
        values[stamp] = current
    pool["v9_1m_ema20"] = pool.confirmation_ts.map(values)
    return pool


def parity_check(original, rebuilt):
    a = original.loc[original.signal_ts.dt.strftime("%H:%M").ne("09:20")]
    b = a.merge(rebuilt, on=["tradingsymbol", "signal_ts"], how="left",
                suffixes=("_archived", "_rebuilt"), validate="one_to_one", indicator=True)
    if not b._merge.eq("both").all():
        raise ValueError("Native reconstruction lost an original non-opening row")
    differences = {}
    for field in PARITY_FIELDS:
        x = pd.to_numeric(b[field + "_archived"], errors="coerce").to_numpy(float)
        y = pd.to_numeric(b[field + "_rebuilt"], errors="coerce").to_numpy(float)
        good = np.isclose(x, y, rtol=1e-10, atol=1e-10, equal_nan=True)
        if not good.all():
            raise ValueError(f"Existing-row feature parity failure {field}: {int((~good).sum())}")
        delta = np.abs(x-y)
        differences[field] = float(np.nanmax(delta)) if np.isfinite(delta).any() else 0.0
    return len(b), differences


def expand_day(day_string, input_root, output_root):
    started = time.monotonic()
    folder = Path(input_root) / day_string
    out = Path(output_root)
    path = folder / "raw.parquet"
    raw_hash = sha(path)
    raw = pd.read_parquet(path)
    missing = sorted(set(CLOCKS) - set(raw.signal_ts.dt.strftime("%H:%M")))
    result = {"day": day_string, "version": VERSION, "missing_clocks": missing,
              "raw_path": str(path), "raw_sha256": raw_hash,
              "existing_rows": len(raw), "original_rows_replaced": 0,
              "consumer": "all51_slots_only", "scope": "BACKTEST_ONLY"}
    if not missing:
        result.update(status="ALREADY_COVERED", supplement_path=None, rows=0)
        return result
    try:
        day = date.fromisoformat(day_string)
        provenance_path = folder / "source_provenance.json"
        provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
        snapshot_path, snapshot, chain = resolve_snapshot(provenance)
        root = snapshot_path.parent
        result.update(snapshot_manifest_chain=chain,
                      snapshot_fingerprint=snapshot["snapshot_fingerprint"],
                      snapshot_all_captured_members_sha256_verified=True,
                      provenance_sha256=sha(provenance_path),
                      feature_ledger_path=provenance["feature_path"],
                      feature_ledger_sha256=provenance["features_sha256"])
        if sha(provenance["feature_path"]) != provenance["features_sha256"]:
            raise ValueError("Sealed feature ledger hash mismatch")
        symbols = sorted(raw.tradingsymbol.unique())
        extra, issues, sources = [], [], []
        parity_rows = 0
        max_differences = {field: 0. for field in PARITY_FIELDS}
        per_symbol = []
        for symbol in symbols:
            result["current_symbol"] = symbol
            archived = raw.loc[raw.tradingsymbol.eq(symbol)].copy()
            contracts = archived.futures_tradingsymbol.dropna().unique()
            months = archived.contract_month.dropna().unique()
            if len(contracts) != 1 or len(months) != 1:
                raise ValueError(f"Ambiguous original contract for {symbol}")
            contract = str(contracts[0])
            minute_path, minute_record = member_path(root, snapshot,
                f"equity_1m/{symbol}_stocks_indicators_1min.parquet")
            future_path, future_record = member_path(root, snapshot,
                "futures_5m/" + native.common.safe_contract_stem(contract) + "_5minute.parquet")
            problems = []
            minute = load_minute_window(minute_path, day, problems, symbol)
            future = native._load_future(future_path, day, problems, symbol)
            # The whole session plus a large rolling warmup, from the identical
            # sealed input. Earlier EMA state comes from the immutable ledger.
            earlier = minute.loc[minute.ts.dt.date.lt(day)].tail(1000)
            today = minute.loc[minute.ts.dt.date.eq(day)]
            window = pd.concat([earlier, today], ignore_index=True)
            rebuilt = native._observed_pool(window, future, day=day, symbol=symbol,
                future_symbol=contract, month=str(months[0]), signal_clocks=CLOCKS)
            if rebuilt.empty:
                raise ValueError(f"No rebuilt source features for original stock {symbol}")
            rebuilt = seed_exact_emas(rebuilt, window, archived)
            # A short window can retain narrower integer sum dtypes than the
            # full history. Evaluate the unchanged close*volume formula in
            # float64, matching the sealed ledger and avoiding overflow.
            rebuilt["traded_value"] = rebuilt.close.astype("float64") * rebuilt.volume.astype("float64")
            count, deltas = parity_check(archived, rebuilt)
            parity_rows += count
            for field, value in deltas.items():
                max_differences[field] = max(max_differences[field], value)
            rows = rebuilt.loc[rebuilt.signal_ts.dt.strftime("%H:%M").isin(missing)].copy()
            # Native OI joins shift the previous observed row. A new missing
            # t-5 bar must never silently become a wider-interval OI change.
            actual_future_times = set(future.ts)
            exact_prior = rows.signal_ts.map(lambda stamp: stamp - pd.Timedelta(minutes=5)
                                            in actual_future_times)
            rows.loc[~exact_prior, ["prev_oi", "oi_change_pct"]] = np.nan
            rows["supplement_exact_prior_oi_bar"] = exact_prior
            rows["supplement_source"] = VERSION
            for field in ("instrument_token", "futures_instrument_token", "exchange",
                          "nifty_first_bar_return_pct"):
                values = archived[field].dropna().unique()
                if len(values) > 1:
                    raise ValueError(f"Inconsistent immutable {field} for {symbol}")
                rows[field] = values[0] if len(values) else np.nan
            if rows.duplicated(["tradingsymbol", "signal_ts"]).any():
                raise ValueError("Duplicate supplemental candidate")
            present_clocks = set(rows.signal_ts.dt.strftime("%H:%M"))
            missing_stock_clocks = sorted(set(missing)-present_clocks)
            valid_oi = (pd.to_numeric(rows.oi, errors="coerce").gt(0)
                        & pd.to_numeric(rows.prev_oi, errors="coerce").gt(0)
                        & np.isfinite(pd.to_numeric(rows.oi_change_pct, errors="coerce")))
            if problems:
                issues.extend(problems)
            per_symbol.append({"symbol": symbol, "rows": len(rows),
                "missing_stock_clocks": missing_stock_clocks,
                "invalid_new_clock_oi_rows": int((~valid_oi).sum()),
                "missing_exact_prior_oi_rows": int((~exact_prior).sum())})
            sources.append({"symbol": symbol, "minute_path": str(minute_path),
                "minute_sha256": minute_record["sha256"], "future_path": str(future_path),
                "future_sha256": future_record["sha256"]})
            extra.append(rows)
        supplement = pd.concat(extra, ignore_index=True)
        if "sid" in supplement:
            supplement = supplement.drop(columns="sid")
        expected_rows = len(symbols)*len(missing)
        actual_rows = len(supplement)
        invalid_oi = sum(row["invalid_new_clock_oi_rows"] for row in per_symbol)
        if sha(path) != raw_hash:
            raise ValueError("Original lab raw input changed during reconstruction")
        output_path = out / f"{day_string}.parquet"
        supplement.to_parquet(output_path, index=False)
        result.update(status="RECONSTRUCTED" if actual_rows == expected_rows else "PARTIAL_COVERAGE",
            supplement_path=str(output_path), supplement_sha256=sha(output_path), rows=actual_rows,
            original_cohort_stocks=len(symbols), expected_stock_clock_rows=expected_rows,
            missing_stock_clock_rows=expected_rows-actual_rows, invalid_new_clock_oi_rows=invalid_oi,
            exact_same_existing_stock_cohort=True, existing_nonopening_parity_rows=parity_rows,
            existing_nonopening_parity_tolerance="rtol=1e-10, atol=1e-10",
            maximum_existing_feature_absolute_differences=max_differences,
            ema_method="Exact native adjust=False recurrence seeded from first sealed existing signal and confirmation; no future anchors",
            volume_atr_history="Native methods on previous 1000 real minutes plus session",
            traded_value_method="Native close*volume formula evaluated in float64 to avoid narrowed-window integer overflow",
            missing_oi_policy="Reject affected new candidates only; no original whole-stock/day removal",
            sid_policy="Caller must assign unique sid; supplementary file deliberately omits sid",
            no_downloads=True, no_original_input_writes=True, source_members=sources,
            stock_clock_coverage=per_symbol, native_source_issues=issues,
            elapsed_seconds=round(time.monotonic()-started, 3))
        result.pop("current_symbol", None)
    except Exception as exc:
        result.update(status="BLOCKED_RECONSTRUCTION", supplement_path=None, rows=0,
                      reason=f"{type(exc).__name__}: {exc}",
                      elapsed_seconds=round(time.monotonic()-started, 3))
    write_json(out / f"{day_string}.proof.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, default=LAB / "inputs")
    parser.add_argument("--output", type=Path, default=LAB / "expanded_slots")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    days = sorted(p.name for p in args.inputs.iterdir() if p.is_dir() and (p/"raw.parquet").is_file())
    records, needed = [], []
    for day in days:
        raw = pd.read_parquet(args.inputs / day / "raw.parquet", columns=["signal_ts"])
        missing = sorted(set(CLOCKS)-set(raw.signal_ts.dt.strftime("%H:%M")))
        if missing:
            proof_path = args.output / f"{day}.proof.json"
            cached = json.loads(proof_path.read_text(encoding="utf-8")) if proof_path.exists() else {}
            cached_file = Path(cached["supplement_path"]) if cached.get("supplement_path") else None
            if (cached.get("status") in {"RECONSTRUCTED", "PARTIAL_COVERAGE"}
                    and cached_file and cached_file.is_file()
                    and cached.get("supplement_sha256") == sha(cached_file)
                    and cached.get("raw_sha256") == sha(args.inputs/day/"raw.parquet")):
                # Published supplements are immutable for concurrent consumers.
                records.append(cached)
            else:
                needed.append(day)
        else:
            records.append({"day": day, "status": "ALREADY_COVERED", "rows": 0,
                            "missing_clocks": [], "supplement_path": None})
    manifest = {"version": VERSION, "scope": "BACKTEST_ONLY", "consumer": "all51_slots_only",
                "donor_setups": len(donor.ACTIVE_SETUPS), "donor_signal_clocks": list(CLOCKS),
                "pending_days": needed, "days": records}
    write_json(args.output / "manifest.json", manifest)
    with ProcessPoolExecutor(max_workers=max(1,args.workers)) as executor:
        futures = {executor.submit(expand_day, day, str(args.inputs), str(args.output)): day for day in needed}
        for completed in as_completed(futures):
            result = completed.result()
            records.append(result)
            manifest["pending_days"].remove(result["day"])
            manifest["days"] = sorted(records, key=lambda item: item["day"])
            write_json(args.output / "manifest.json", manifest)
            print(json.dumps({k:result.get(k) for k in ("day","status","rows","reason","elapsed_seconds")}), flush=True)
    manifest["complete"] = not any(x["status"] == "BLOCKED_RECONSTRUCTION" for x in records)
    write_json(args.output / "manifest.json", manifest)


if __name__ == "__main__":
    main()
