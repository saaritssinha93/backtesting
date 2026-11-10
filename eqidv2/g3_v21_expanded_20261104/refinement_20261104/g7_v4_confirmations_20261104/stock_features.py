"""Verified G7 V4 stock confirmations for the isolated V21 replay.

Each calculation uses a stock's immutable, hash-checked minute snapshot.  The
G7 functions themselves restrict the current session to the completed
confirmation candle and historical quantiles to sessions strictly before it.
No realized return or future price is read to decide an entry.
"""
from __future__ import annotations

from collections import defaultdict
from functools import lru_cache
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from g3_one_minute_extension import extension_atr
from g3_v18_20261102 import data as v18_data
from g3_v21_expanded_20261104 import source as v21_source
from research import g7_v4_extremes, g7_v4_indicator_iteration


ROOT = Path(__file__).resolve().parents[3]
CACHE = ROOT / "outputs/g3_early_entry_20261023/full_history_source/cache"
EARLY_MANIFEST = Path(
    "C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/production/runs/"
    "2026-10-14/20261021T183531060381_39efe64c/input_snapshots/"
    "4e03fbb05aa63bc26d5042eb1eaf8d8eb74be44644ff04d2339a4d55253c6923/"
    "snapshot_manifest.json"
)
EARLY_MANIFEST_SHA256 = "7c7ba43ad19d6424aebfd22f41c0ffe0f729dd81e3862ea5be98d26402aa9bb0"
IST = "Asia/Kolkata"
OHLCV = ("open", "high", "low", "close", "volume")
FLAGS = ("gap_filled", "opening_snapshot", "provisional_stale", "is_synthetic")
G7_ATR_LIMITS = {
    "0926_SHORT": 4.0, "0936_LONG": 3.5, "0946_LONG": 3.0,
    "0946_SHORT": 3.0, "1001_LONG": 4.0, "1036_LONG": 3.0,
    "1036_SHORT": 3.0, "1136_LONG": 3.5, "1146_SHORT": 4.0,
    "1236_SHORT": 3.0, "1301_SHORT": 3.5, "1326_SHORT": 3.0,
}


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


@lru_cache(maxsize=80)
def _manifest_for_day(day: str) -> tuple[Path, str, dict, dict]:
    """Choose the historical snapshot tied to the sealed G3 source cohort."""
    if day <= "2026-10-08":
        manifest_path = EARLY_MANIFEST
        expected = EARLY_MANIFEST_SHA256
        provenance = dict(kind="PINNED_EARLY_G3_RESEARCH_SNAPSHOT",
                          snapshot_manifest=str(manifest_path),
                          snapshot_manifest_sha256=expected)
    elif day <= "2026-10-23":
        provenance_path = CACHE / day / "provenance.json"
        provenance = _json(provenance_path)
        manifest_path = Path(provenance["snapshot_manifest"])
        expected = provenance.get("snapshot_manifest_sha256", provenance.get("snapshot_sha256"))
        provenance = dict(kind="SEALED_G3_RESEARCH_CACHE_PROVENANCE",
                          provenance_path=str(provenance_path),
                          provenance_sha256=_sha(provenance_path),
                          snapshot_manifest=str(manifest_path),
                          snapshot_manifest_sha256=expected)
    else:
        run = v18_data._daily_runs().get(day)
        if run is None:
            raise ValueError(f"Missing sealed G3 minute source for {day}")
        run_manifest = run / "run_manifest.json"
        sealed = _json(run_manifest)
        if sealed.get("complete") is not True or sealed.get("execution_authority") is not False:
            raise ValueError(f"Unsealed G3 minute source for {day}")
        source = sealed["artifacts"]["source_manifest"]
        source_path = (run / source["path"]).resolve()
        source_path.relative_to(run.resolve())
        if _sha(source_path) != source["sha256"]:
            raise ValueError(f"Sealed source manifest changed: {day}")
        source_payload = _json(source_path)
        manifest_path = Path(source_payload["snapshot_manifest"])
        expected = source_payload["snapshot_manifest_sha256"]
        provenance = dict(kind="SEALED_G3_DAILY_RUN",
                          parent_run_complete=True,
                          run_manifest=str(run_manifest),
                          run_manifest_sha256=_sha(run_manifest),
                          source_manifest=str(source_path),
                          source_manifest_sha256=source["sha256"],
                          snapshot_manifest=str(manifest_path),
                          snapshot_manifest_sha256=expected)
    manifest_path = manifest_path.resolve()
    if not expected or _sha(manifest_path) != expected:
        raise ValueError(f"Stock snapshot manifest changed: {day} {manifest_path}")
    manifest = _json(manifest_path)
    if manifest.get("session_date") != day and day > "2026-10-08":
        raise ValueError(f"Stock snapshot date differs: {day}")
    # The 30 October recovered run is sealed complete although its source
    # capture is marked partial for unrelated universe members.  The precise
    # stock used below must still be captured and individually hash verified.
    if manifest.get("complete") is not True and not provenance.get("parent_run_complete"):
        raise ValueError(f"Incomplete stock snapshot: {day}")
    provenance["snapshot_complete"] = bool(manifest.get("complete"))
    provenance["snapshot_problem_count"] = len(manifest.get("problems", []))
    members = {}
    for item in manifest["sources"]:
        if item.get("role") != "EQUITY_ONE_MINUTE" or not item.get("captured"):
            continue
        relative = str(item["snapshot_relative_path"]).replace("\\", "/")
        if not relative.startswith("equity_1m/") or not relative.endswith("_stocks_indicators_1min.parquet"):
            continue
        symbol = Path(relative).name.removesuffix("_stocks_indicators_1min.parquet")
        if symbol in members:
            raise ValueError(f"Duplicate stock snapshot member: {day} {symbol}")
        members[symbol] = item
    return manifest_path, expected, members, provenance


def _source_for(day: str, symbol: str) -> tuple[Path, str, dict]:
    manifest_path, manifest_sha, members, provenance = _manifest_for_day(day)
    member = members.get(symbol)
    if member is None or not member.get("sha256"):
        raise ValueError(f"Stock minute source missing: {day} {symbol}")
    path = (manifest_path.parent / member["snapshot_relative_path"]).resolve()
    path.relative_to(manifest_path.parent.resolve())
    proof = dict(day=day, symbol=symbol, stock_path=str(path),
                 stock_sha256=member["sha256"], snapshot_manifest=str(manifest_path),
                 snapshot_manifest_sha256=manifest_sha, source_provenance=provenance)
    return path, member["sha256"], proof


def _minute_frame(path: Path) -> pd.DataFrame:
    names = set(pq.ParquetFile(path).schema.names)
    if not set(("date", *OHLCV)).issubset(names):
        raise ValueError(f"Incomplete stock minute schema: {path}")
    columns = ["date", *OHLCV, *(name for name in FLAGS if name in names)]
    frame = pd.read_parquet(path, columns=columns)
    # Recovered cash can store microsecond timestamps.  G7's exact completed
    # minute validator compares against a nanosecond date_range.
    frame["ts"] = pd.to_datetime(frame.date, utc=True).dt.tz_convert(IST).dt.as_unit("ns")
    frame = frame.sort_values("ts", kind="stable").reset_index(drop=True)
    return frame


def _extension_series(frame: pd.DataFrame) -> pd.DataFrame:
    """G3/G7 one-minute EMA20 and simple ATR14, computed once per source."""
    close = pd.to_numeric(frame.close, errors="raise")
    high = pd.to_numeric(frame.high, errors="raise")
    low = pd.to_numeric(frame.low, errors="raise")
    prev = close.shift()
    true_range = pd.concat([high - low, (high - prev).abs(), (low - prev).abs()],
                           axis=1).max(axis=1)
    ema20 = close.ewm(span=20, adjust=False).mean()
    atr14 = true_range.rolling(14, min_periods=14).mean()
    return pd.DataFrame(dict(ema20=ema20, atr14=atr14), index=frame.index)


def _prepared_frame(path: Path, rows: list[dict], proof: dict) -> tuple[pd.DataFrame, dict]:
    """Load the used member and the sealed prior cash warmup for compact Oct28."""
    if _sha(path) != proof["stock_sha256"]:
        raise ValueError(f"Stock snapshot changed: {path}")
    frame = _minute_frame(path)
    if any(str(row["day"])[:10] == "2026-10-28" for row in rows):
        if any(str(row["day"])[:10] != "2026-10-28" for row in rows):
            raise ValueError("Compact Oct28 source mixed with another session")
        symbol = proof["symbol"]
        root, members, prior_proof = v21_source._oct28_prior_snapshot()
        relative = f"equity_1m/{symbol}_stocks_indicators_1min.parquet"
        member = members.get(relative)
        if not member or member.get("role") != "EQUITY_ONE_MINUTE" or not member.get("captured"):
            # The compact Oct28 cohort contains one selected symbol absent
            # from the Oct27 capture.  Use the earliest later immutable seal
            # containing that symbol, but read *only* observations before
            # Oct28.  This is retrospective source recovery, disclosed below.
            member = None
            for later_day in ("2026-10-29", "2026-10-30", "2026-11-02",
                              "2026-11-03", "2026-11-04"):
                later_manifest, later_sha, later_members, later_provenance = _manifest_for_day(later_day)
                candidate = later_members.get(symbol)
                if candidate and candidate.get("captured") and candidate.get("sha256"):
                    root = later_manifest.parent
                    member = candidate
                    prior_proof = dict(snapshot_manifest=str(later_manifest),
                                       snapshot_manifest_sha256=later_sha,
                                       source_provenance=later_provenance,
                                       captured_session=later_day,
                                       retrospective_prior_recovery=True,
                                       policy="Use only cash observations strictly before 2026-10-28")
                    break
            if member is None:
                raise ValueError(f"Missing any sealed Oct28 warmup stock: {symbol}")
        prior_path = (root / relative).resolve()
        prior_path.relative_to(root.resolve())
        if _sha(prior_path) != member["sha256"]:
            raise ValueError(f"Oct27 warmup stock snapshot changed: {symbol}")
        cutoff = pd.Timestamp("2026-10-28", tz=IST)
        prior = _minute_frame(prior_path)
        prior = prior.loc[prior.ts.lt(cutoff)]
        current = frame.loc[frame.ts.ge(cutoff)]
        if prior.empty or current.empty:
            raise ValueError(f"Incomplete stitched Oct28 stock history: {symbol}")
        frame = pd.concat([prior, current], ignore_index=True, sort=False)
        proof = dict(proof, warmup=dict(prior_proof,
                          stock_path=str(prior_path), stock_sha256=member["sha256"],
                          cutoff_exclusive="2026-10-28 00:00:00 Asia/Kolkata"))
    maximum = max(pd.Timestamp(row["confirmation_ts"]).tz_convert(IST) for row in rows)
    frame = frame.loc[frame.ts.le(maximum)].copy().sort_values("ts", kind="stable").reset_index(drop=True)
    if frame.empty or frame.ts.duplicated().any():
        raise ValueError(f"Missing or duplicate stock minute chronology: {path}")
    return frame, proof


def _current_confirmation(row: dict, frame: pd.DataFrame,
                          components: pd.DataFrame) -> tuple[int, float]:
    stamp = pd.Timestamp(row["confirmation_ts"]).tz_convert(IST)
    match = frame.index[frame.ts.eq(stamp)]
    if len(match) != 1:
        raise ValueError(f"Missing exact confirmation minute: {row['sid']}")
    at = int(match[0])
    observed = frame.iloc[at]
    for key in OHLCV:
        expected = float(row["confirmation_" + key])
        if not np.isclose(float(observed[key]), expected, rtol=0, atol=.001):
            raise ValueError(f"Stock source/confirmation {key} mismatch: {row['sid']}")
    distance = extension_atr(observed.close, components.ema20.iloc[at],
                             components.atr14.iloc[at], row["side"])
    if row.get("setup_id") in G7_ATR_LIMITS and not np.isfinite(distance):
        raise ValueError(f"Missing completed G7 ATR guard feature: {row['sid']}")
    native = pd.to_numeric(row.get("v9_1m_extension_atr"), errors="coerce")
    if np.isfinite(native) and not np.isclose(native, distance, rtol=1e-5, atol=1e-5):
        raise ValueError(f"Saved V21/G7 extension parity failed: {row['sid']}")
    return at, float(distance)


def _inventory(orders: pd.DataFrame) -> tuple[dict[Path, list[dict]], dict[Path, dict]]:
    required = {"sid", "day", "tradingsymbol", "side", "confirmation_ts",
                "confirmation_open", "confirmation_high", "confirmation_low",
                "confirmation_close", "confirmation_volume", "trigger"}
    missing = sorted(required - set(orders))
    if missing or orders.sid.isna().any() or orders.sid.duplicated().any():
        raise ValueError(f"Invalid V21 stock confirmation inventory: {missing}")
    grouped: dict[Path, list[dict]] = defaultdict(list)
    source_proofs: dict[Path, dict] = {}
    for row in orders.to_dict("records"):
        day = str(row["day"])[:10]
        symbol = str(row["tradingsymbol"])
        stamp = pd.Timestamp(row["confirmation_ts"])
        if pd.isna(stamp) or stamp.tzinfo is None or str(stamp.tz_convert(IST).date()) != day:
            raise ValueError(f"Invalid confirmation date: {row['sid']}")
        path, expected_sha, proof = _source_for(day, symbol)
        if path in source_proofs and source_proofs[path]["stock_sha256"] != expected_sha:
            raise ValueError(f"Conflicting stock snapshot hashes: {path}")
        source_proofs.setdefault(path, proof)
        grouped[path].append(row)
    return grouped, source_proofs


def _preflight(grouped: dict[Path, list[dict]], source_proofs: dict[Path, dict]) -> dict:
    """Check every required stock/confirmation/guard before historical work."""
    guarded = 0
    unguarded_without_atr = 0
    partial_snapshot_rows = 0
    checked = 0
    for path, rows in grouped.items():
        frame, proof = _prepared_frame(path, rows, source_proofs[path])
        components = _extension_series(frame)
        current_days = {str(row["day"])[:10] for row in rows}
        current_frames = {day: frame.loc[frame.ts.dt.date.eq(pd.Timestamp(day).date())]
                          for day in current_days}
        for row in rows:
            _, distance = _current_confirmation(row, frame, components)
            stamp = pd.Timestamp(row["confirmation_ts"]).tz_convert(IST)
            day = str(stamp.date())
            g7_v4_extremes.checked_session(current_frames[day], stamp)
            guarded += int(row.get("setup_id") in G7_ATR_LIMITS)
            unguarded_without_atr += int(row.get("setup_id") not in G7_ATR_LIMITS
                                         and not np.isfinite(distance))
            partial_snapshot_rows += int(not proof["source_provenance"]["snapshot_complete"])
            checked += 1
    return dict(rows=checked, source_files=len(grouped), guarded_rows=guarded,
                unguarded_without_atr=unguarded_without_atr,
                partial_snapshot_rows=partial_snapshot_rows)


def preflight_stock_audit(orders: pd.DataFrame) -> dict:
    grouped, source_proofs = _inventory(orders)
    return _preflight(grouped, source_proofs)


def build_stock_audit(orders: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """Return one point-in-time G7 stock confirmation row per selected V21 SID.

    Historical references use the actual stock snapshot's last 60 *prior*
    same-clock sessions, rather than only the 64 V21 test sessions.  A snapshot
    captured after an old research day is disclosed in the proof; the G7
    feature code still excludes all later bars from that day's decision.
    """
    if orders.empty:
        return pd.DataFrame(columns=["sid", "g7_low_efficiency_veto", "g7_extension_atr"]), []
    grouped, source_proofs = _inventory(orders)
    _preflight(grouped, source_proofs)
    results = []
    proofs = []
    for path, rows in grouped.items():
        proof = source_proofs[path]
        frame, proof = _prepared_frame(path, rows, proof)
        components = _extension_series(frame)
        # Validate every selected confirmation and every applicable ATR guard
        # before the relatively expensive 60-session historical feature work.
        distances = {int(row["sid"]): _current_confirmation(row, frame, components)[1]
                     for row in rows}
        by_day = {day: part for day, part in frame.groupby(frame.ts.dt.strftime("%Y-%m-%d"), sort=True)}
        historical_cache = {}
        for row in rows:
            stamp = pd.Timestamp(row["confirmation_ts"]).tz_convert(IST)
            f = g7_v4_extremes.feature_row(by_day, row, cache=historical_cache)
            f.update(g7_v4_indicator_iteration.indicator_row(by_day[str(stamp.date())], row))
            f["g7_extension_atr"] = distances[int(row["sid"])]
            results.append(f)
        proofs.append(dict(proof, candidate_days=sorted({str(row["day"])[:10] for row in rows}),
                           selected_sids=len(rows), historical_rows=int(len(frame))))
    features = pd.DataFrame(results).set_index("sid", drop=False)
    if features.index.has_duplicates or set(features.sid) != set(orders.sid):
        raise ValueError("G7 stock confirmation audit lost selected orders")
    veto = g7_v4_indicator_iteration.decisions(features)["low_efficiency"]
    features["g7_low_efficiency_veto"] = veto.astype(bool)
    features["g7_stock_source_sha256"] = features.sid.map({int(row["sid"]):
        source_proofs[path]["stock_sha256"] for path, rows in grouped.items() for row in rows})
    return features.reset_index(drop=True), proofs
