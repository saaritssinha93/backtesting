"""Hash-verified, point-in-time G3 feature expansion through the 14:00 setup.

The V18 loader remains the authority for every existing feature and execution
minute.  This adapter only appends clocks absent from that sealed source.  The
47 older sessions use the dated all-5m archive; the 17 later sessions rebuild
missing clocks from each day's immutable cash-minute/futures-OI snapshot.
"""
from __future__ import annotations

from datetime import date, timedelta
from functools import lru_cache
import json
from pathlib import Path
import warnings

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from g3_v18_20261102 import data as baseline
import fno_v13_replay_source as raw
from research.g3_early_full_history_source_20261023 import ARCHIVES, LEGACY_FIELDS


IST = "Asia/Kolkata"
CLOCKS = pd.date_range("2000-01-01 09:20", "2000-01-01 14:00", freq="5min").strftime("%H:%M").tolist()
LATE_CLOCKS = CLOCKS[-6:]
BUILDER_VERSION = "g3_v21_causal_expanded_slots_v1"
OVERLAP_FIELDS = ("open", "high", "low", "close", "volume", "source_1m_count",
                  "confirmation_open", "confirmation_high", "confirmation_low",
                  "confirmation_close", "confirmation_volume", "volume_ratio",
                  "oi", "prev_oi", "oi_change_pct", "ema9", "ema20", "ema50")


def available_days(through: str = "2026-11-03") -> list[str]:
    return baseline.available_days(through)


@lru_cache(maxsize=2)
def _archive_identity(path: Path) -> dict:
    manifest_path = path.parent / "dataset_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected = manifest["output_sha256"][path.name]
    actual = baseline.sha256(path)
    if actual != expected:
        raise ValueError(f"Historical all-5m archive hash mismatch: {path}")
    return dict(path=str(path), sha256=actual, manifest=str(manifest_path),
                manifest_sha256=baseline.sha256(manifest_path))


def _archive_frame(day: str) -> tuple[pd.DataFrame, dict]:
    part = next((folder for folder, first, last in ARCHIVES if first <= day <= last), None)
    if part is None:
        raise ValueError(f"No dated all-5m archive for {day}")
    path = part / "all_5m_features.parquet"
    proof = _archive_identity(path)
    names = set(pq.ParquetFile(path).schema.names)
    fields = [x for x in LEGACY_FIELDS if x in names]
    frame = pd.read_parquet(path, columns=fields, filters=[("day", "=", date.fromisoformat(day))])
    frame = frame.loc[frame.hhmm_int.between(920, 1400)].copy()
    return frame, proof


def _verify_member(snapshot: Path, records: dict[str, dict], relative: str) -> Path:
    record = records.get(relative)
    if not record or not record.get("captured"):
        raise ValueError(f"Snapshot member not captured: {relative}")
    path = (snapshot / relative).resolve()
    path.relative_to(snapshot.resolve())
    if baseline.sha256(path) != record["sha256"]:
        raise ValueError(f"Snapshot member hash mismatch: {path}")
    return path


@lru_cache(maxsize=1)
def _oct28_prior_snapshot() -> tuple[Path, dict[str, dict], dict]:
    """Causal warm-up source for the compact Oct28 current-session seal."""
    run = baseline._daily_runs()["2026-10-27"]
    run_manifest = json.loads((run / "run_manifest.json").read_text(encoding="utf-8"))
    source_record = run_manifest["artifacts"]["source_manifest"]
    source_path = (run / source_record["path"]).resolve()
    source_path.relative_to(run.resolve())
    if baseline.sha256(source_path) != source_record["sha256"]:
        raise ValueError("Oct27 prior source manifest hash mismatch")
    source_proof = json.loads(source_path.read_text(encoding="utf-8"))
    manifest_path = Path(source_proof["snapshot_manifest"])
    manifest_sha = baseline.sha256(manifest_path)
    if manifest_sha != source_proof["snapshot_manifest_sha256"]:
        raise ValueError("Oct27 prior snapshot manifest hash mismatch")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    records = {x["snapshot_relative_path"]: x for x in manifest["sources"]
               if x.get("snapshot_relative_path")}
    return manifest_path.parent, records, dict(snapshot_manifest=str(manifest_path),
                                                snapshot_manifest_sha256=manifest_sha,
                                                policy="Only cash minutes strictly before 2026-10-28 from the sealed 2026-10-27 snapshot")


def _snapshot_frame(day: str, original: pd.DataFrame, proof: dict) -> tuple[pd.DataFrame, dict]:
    manifest_path = Path(proof["snapshot_manifest"])
    manifest_sha = baseline.sha256(manifest_path)
    expected_manifest_sha = proof.get("snapshot_manifest_sha256", proof.get("snapshot_sha256"))
    if manifest_sha != expected_manifest_sha:
        raise ValueError(f"Snapshot manifest hash mismatch: {day}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    snapshot = manifest_path.parent
    records = {x["snapshot_relative_path"]: x for x in manifest["sources"]
               if x.get("snapshot_relative_path")}
    rebuilt, issues = [], []
    for symbol, old in original.groupby("tradingsymbol", sort=True):
        old = old.sort_values("signal_ts")
        seed = old.loc[old.signal_ts.dt.strftime("%H:%M").eq("09:20")]
        if len(seed) != 1:
            issues.append(dict(symbol=symbol, reason="MISSING_UNIQUE_0920_EMA_SEED"))
            continue
        first = seed.iloc[0]
        future_symbol = str(first.futures_tradingsymbol)
        cash_name = f"equity_1m/{symbol}_stocks_indicators_1min.parquet"
        future_name = f"futures_5m/{raw.common.safe_contract_stem(future_symbol)}_5minute.parquet"
        try:
            cash_path = _verify_member(snapshot, records, cash_name)
            future_path = _verify_member(snapshot, records, future_name)
            # Ten preceding days supply warm-up. All bars after this session's
            # 15:15 cutoff are excluded, including from rolling calculations.
            start = pd.Timestamp(date.fromisoformat(day) - timedelta(days=10), tz=IST)
            cutoff = pd.Timestamp(f"{day} 15:15", tz=IST)
            required = ["date", "open", "high", "low", "close", "volume"]
            names = set(pq.ParquetFile(cash_path).schema.names)
            fields = required + [x for x in ("gap_filled", "opening_snapshot", "provisional_stale") if x in names]
            minute = pd.read_parquet(cash_path, columns=fields,
                                     filters=[("date", ">=", start), ("date", "<=", cutoff)])
            minute["ts"] = raw.hybrid._to_ist(minute.date).dt.as_unit("ns")
            if day == "2026-10-28":
                # This recovered compact seal holds only today's 360 bars. The
                # preceding day's sealed snapshot provides the required rolling
                # price/volume warm-up without using a later market-data file.
                prior_root, prior_records, _ = _oct28_prior_snapshot()
                prior_path = _verify_member(prior_root, prior_records, cash_name)
                prior_names = set(pq.ParquetFile(prior_path).schema.names)
                prior_fields = required + [x for x in ("gap_filled", "opening_snapshot", "provisional_stale")
                                           if x in prior_names]
                previous = pd.read_parquet(prior_path, columns=prior_fields,
                                           filters=[("date", ">=", start),
                                                    ("date", "<", pd.Timestamp(day, tz=IST))])
                previous["ts"] = raw.hybrid._to_ist(previous.date).dt.as_unit("ns")
                if previous.empty or previous.ts.max() >= pd.Timestamp(day, tz=IST):
                    raise ValueError("NO_PREVIOUS_SEALED_CASH_HISTORY")
                minute = pd.concat([previous, minute], ignore_index=True)
                # The compact current-day member has no optional source flags,
                # whereas the prior seal does. Preserve recorded prior flags
                # and give today's absent flag fields explicit false values.
                for flag in ("gap_filled", "opening_snapshot", "provisional_stale"):
                    if flag in minute:
                        minute[flag] = minute[flag].astype("boolean").fillna(False).astype(bool)
            minute = minute.sort_values("ts").reset_index(drop=True)
            if minute.ts.duplicated().any():
                raise ValueError("Duplicate cash minute timestamp")
            problems = []
            future = raw._load_future(future_path, date.fromisoformat(day), problems, symbol)
            raw._coverage(minute, future, date.fromisoformat(day), symbol, problems, signal_clocks=CLOCKS)
            if any(x["reason"] != "MISSING_SIGNAL_OR_PRIOR_FUTURES_OI_BAR" for x in problems):
                raise ValueError(f"Cash/futures coverage failure: {problems}")
            frame = raw._observed_pool(minute, future, day=date.fromisoformat(day),
                                       symbol=symbol, future_symbol=future_symbol,
                                       month=str(first.contract_month), signal_clocks=CLOCKS)
            frame = frame.sort_values("signal_ts").reset_index(drop=True)
            if len(frame) != len(CLOCKS):
                raise ValueError(f"Incomplete completed-5m/confirmation pool: {len(frame)}/{len(CLOCKS)}")
            # The sealed 09:20 EMA already includes earlier completed sessions.
            # Every subsequent value consumes just the next completed 5m close.
            for span in (9, 20, 50):
                value = float(first[f"ema{span}"])
                alpha = 2 / (span + 1)
                values = [value]
                for close in frame.close.iloc[1:]:
                    value = (1 - alpha) * value + alpha * float(close)
                    values.append(value)
                frame[f"ema{span}"] = values
                frame[f"v9_5m_ema{span}"] = values
            frame["v9_5m_ema_bull"] = frame.ema9.gt(frame.ema20) & frame.ema20.gt(frame.ema50)
            frame["v9_5m_ema_bear"] = frame.ema9.lt(frame.ema20) & frame.ema20.lt(frame.ema50)
            for name in ("instrument_token", "futures_instrument_token", "exchange",
                         "nifty_first_bar_return_pct"):
                frame[name] = first.get(name)
            for key in ("oi", "prev_oi", "oi_change_pct", "price_change_pct"):
                frame[key] = pd.to_numeric(frame[key], errors="coerce").astype(float)
                frame.loc[0, key] = first.get(key)
            rebuilt.append(frame)
            issues.extend(problems)
        except (OSError, ValueError, KeyError, TypeError, AssertionError) as error:
            issues.append(dict(symbol=symbol, reason="RAW_RECONSTRUCTION_FAILED", detail=str(error)))
    if not rebuilt:
        raise ValueError(f"No stock features rebuilt from sealed snapshot: {day}")
    out = pd.concat(rebuilt, ignore_index=True)
    prior_proof = _oct28_prior_snapshot()[2] if day == "2026-10-28" else None
    return out, dict(kind="SEALED_RAW_CASH_1M_AND_FUTURES_5M_RECONSTRUCTION",
                     snapshot_manifest=str(manifest_path), snapshot_manifest_sha256=manifest_sha,
                     snapshot_members_verified=int(len(rebuilt) * 2), issues=issues,
                     prior_cash_warmup=prior_proof,
                     ema_seed_policy="Sealed09:20EMA then exact recurrence through completed5m closes")


def _assert_overlap(original: pd.DataFrame, candidate: pd.DataFrame, day: str,
                    allowed_missing_symbols: set[str]) -> dict:
    left = original[["tradingsymbol", "signal_ts"] + [x for x in OVERLAP_FIELDS if x in original]].copy()
    right = candidate[["tradingsymbol", "signal_ts"] + [x for x in OVERLAP_FIELDS if x in candidate]].copy()
    merged = left.merge(right, on=["tradingsymbol", "signal_ts"], how="left",
                        suffixes=("_sealed", "_rebuilt"), validate="one_to_one", indicator=True)
    absent = merged._merge.ne("both")
    unexpected = absent & ~merged.tradingsymbol.isin(allowed_missing_symbols)
    if unexpected.any():
        missing = merged.loc[unexpected, ["tradingsymbol", "signal_ts"]].head(5)
        raise ValueError(f"Sealed feature rows absent from expanded source {day}: {missing.to_dict('records')}")
    merged = merged.loc[~absent].copy()
    checks = {}
    for name in OVERLAP_FIELDS:
        a, b = f"{name}_sealed", f"{name}_rebuilt"
        if a not in merged or b not in merged:
            continue
        # 09:20 has a verified special opening-OI basis; later rows use a
        # standard preceding 5m futures OI bar.
        sample = merged if name not in ("oi", "prev_oi", "oi_change_pct") else merged.loc[merged.signal_ts.dt.strftime("%H:%M").ne("09:20")]
        va = pd.to_numeric(sample[a], errors="coerce").to_numpy(float)
        vb = pd.to_numeric(sample[b], errors="coerce").to_numpy(float)
        ok = np.isclose(va, vb, rtol=1e-7, atol=1e-7, equal_nan=True)
        checks[name] = dict(rows=int(len(ok)), mismatches=int((~ok).sum()))
        if not ok.all():
            raise ValueError(f"Expanded source disagrees with sealed input {day} {name}: {int((~ok).sum())}")
    return checks


def load_day(day: str) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Preserve baseline rows and minutes, appending all source-backed clocks <=14:00."""
    day = str(day)[:10]
    original, minutes, proof = baseline.load_day(day)
    if day <= "2026-10-08":
        candidate, source_proof = _archive_frame(day)
        source_proof = dict(kind="HASH_VERIFIED_DATED_ALL_5M_ARCHIVE", **source_proof)
    else:
        candidate, source_proof = _snapshot_frame(day, original, proof)
    candidate = baseline._normalize_features(candidate, day)
    candidate = candidate.loc[candidate.tradingsymbol.isin(original.tradingsymbol.unique())
                              & candidate.signal_ts.dt.strftime("%H:%M").isin(CLOCKS)].copy()
    failed_symbols = {x["symbol"] for x in source_proof.get("issues", [])
                      if x.get("reason") in ("RAW_RECONSTRUCTION_FAILED", "MISSING_UNIQUE_0920_EMA_SEED")}
    overlap = _assert_overlap(original, candidate, day, failed_symbols)
    existing = pd.MultiIndex.from_frame(original[["tradingsymbol", "signal_ts"]])
    unseen = ~pd.MultiIndex.from_frame(candidate[["tradingsymbol", "signal_ts"]]).isin(existing)
    appended = candidate.loc[unseen].copy()
    # Carry only invariant metadata missing from an older archive. Dynamic
    # fields receive their own completed clock, never a later row's value.
    stable = ("instrument_token", "futures_instrument_token", "exchange",
              "nifty_first_bar_return_pct", "data_contract", "price_source", "oi_source")
    first = original.groupby("tradingsymbol", sort=False).first()
    for name in stable:
        if name not in appended and name in first:
            appended[name] = appended.tradingsymbol.map(first[name])
    if "v9_1m_atr14" not in appended and "v9_1m_atr14" in minutes:
        atr = minutes[["tradingsymbol", "ts", "v9_1m_atr14"]].rename(columns={"ts": "confirmation_ts"})
        appended = appended.merge(atr, on=["tradingsymbol", "confirmation_ts"], how="left", validate="one_to_one")
    for name in original.columns:
        if name not in appended:
            appended[name] = pd.NA
    appended = appended[original.columns]
    appended = appended.sort_values(["tradingsymbol", "signal_ts"], kind="stable")
    with warnings.catch_warnings():
        # Pandas warns when an older cohort lacks fields that are intentionally
        # null on every newly appended nonopening row. Original dtypes and
        # values are asserted immediately below.
        warnings.filterwarnings("ignore", message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated", category=FutureWarning)
        expanded = pd.concat([original, appended], ignore_index=True)
    if expanded.duplicated(["tradingsymbol", "signal_ts"]).any():
        raise ValueError(f"Expanded feature identity duplicated: {day}")
    try:
        pd.testing.assert_frame_equal(expanded.iloc[:len(original)][original.columns].reset_index(drop=True),
                                      original.reset_index(drop=True), check_dtype=False, check_exact=True)
    except AssertionError as error:
        raise ValueError(f"Original sealed feature rows changed during expansion: {day}: {error}") from error
    expected_new = len(original.tradingsymbol.unique()) * len(CLOCKS) - len(original)
    expansion = dict(builder_version=BUILDER_VERSION, through_clock="14:00",
                     source=source_proof, overlapping_sealed_checks=overlap,
                     original_feature_rows=int(len(original)), appended_feature_rows=int(len(appended)),
                     added_clock_rows=int(len(appended)),
                     expected_append_rows=int(expected_new),
                     missing_append_rows=int(expected_new - len(appended)),
                     missing_clock_rows=int(expected_new - len(appended)),
                     appended_clock_counts={str(k): int(v) for k, v in appended.signal_ts.dt.strftime("%H:%M").value_counts().sort_index().items()},
                     minute_rows_unchanged=int(len(minutes)))
    return expanded, minutes, dict(proof, v21_expansion=expansion,
                                   added_clock_rows=int(len(appended)),
                                   missing_clock_rows=int(expected_new - len(appended)))
