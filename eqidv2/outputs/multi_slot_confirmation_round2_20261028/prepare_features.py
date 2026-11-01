"""Frozen, feature-only round-two confirmation package for ten requested slots.

No outcome or trial-result table is read. Inputs are verified prior candidate
packages and exact completed minute OHLCV through the original confirmation.
All new indicators restart at 09:16 and use the same definitions for every slot.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
OUT = Path(__file__).resolve().parent
PRIOR = ROOT / "outputs/multi_slot_short_iteration_20261028"
G3_RUN = Path(r"C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/backtests/runs/2026-10-27/20261027T172043456343_3d50ca01_current_full_history")
SETUPS = ("0931_LONG", "0931_SHORT", "1001_LONG", "1051_LONG", "1111_SHORT",
          "1146_SHORT", "1226_SHORT", "1231_SHORT", "1301_SHORT", "1321_SHORT")
RULES = ("r2_volume_ge_1p0", "r2_volume_ge_1p5", "r2_ema5_13_aligned",
         "r2_vwap_band_0p5", "r2_vwap_band_1p0", "r2_efficiency10_ge_0p3",
         "r2_breakout3", "r2_close_location_ge_0p8")
FIELDS = ("open", "high", "low", "close", "volume")
IST = "Asia/Kolkata"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False,
                               default=str) + "\n", encoding="utf-8")


def timestamp(value) -> pd.Timestamp:
    result = pd.Timestamp(value)
    if pd.isna(result):
        raise ValueError("Missing confirmation timestamp")
    return result.tz_localize(IST) if result.tz is None else result.tz_convert(IST)


def checked_prefix(raw: pd.DataFrame, row: dict) -> pd.DataFrame:
    from research.g4_vwap_rsi_features import _check_flags

    stamp = timestamp(row["confirmation_ts"])
    start = stamp.normalize() + pd.Timedelta(hours=9, minutes=16)
    if (stamp != stamp.floor("min") or str(stamp.date()) != str(row["day"])[:10]
            or stamp.strftime("%H%M") != str(row["setup_id"]).split("_")[0]):
        raise ValueError("Candidate clock, session, and setup disagree")
    missing = set(["ts", *FIELDS]).difference(raw.columns)
    if missing:
        raise ValueError("Missing OHLCV columns: " + repr(sorted(missing)))
    window = raw.loc[raw.ts.ge(start) & raw.ts.le(stamp)].sort_values("ts", kind="stable").copy()
    expected = pd.date_range(start, stamp, freq="min")
    observed = pd.DatetimeIndex(window.ts)
    # Compare UTC nanosecond values, preserving exact-minute checks while
    # ignoring only irrelevant Index.name and timestamp storage resolution.
    observed_ns = observed.as_unit("ns").asi8
    expected_ns = expected.as_unit("ns").asi8
    if not np.array_equal(observed_ns, expected_ns):
        raise ValueError("Missing, duplicate, or out-of-order session minute")
    if len(window) < 16:
        raise ValueError("Fewer than sixteen completed session bars")
    values = window[list(FIELDS)].apply(pd.to_numeric, errors="coerce").to_numpy(float)
    if (not np.isfinite(values).all() or (values[:, :4] <= 0).any()
            or (values[:, 4] < 0).any() or values[:, 4].sum() <= 0
            or (values[:, 1] < values[:, [0, 2, 3]].max(axis=1)).any()
            or (values[:, 2] > values[:, [0, 1, 3]].min(axis=1)).any()):
        raise ValueError("Invalid OHLCV in confirmation prefix")
    _check_flags(window)
    if "is_synthetic" in window:
        _check_flags(window.assign(gap_filled=window.is_synthetic))
    for index, field in enumerate(FIELDS[:4]):
        value = float(row["confirmation_" + field])
        if not np.isfinite(value) or not np.isclose(values[-1, index], value, atol=.001, rtol=0):
            raise ValueError("Candidate confirmation OHLC differs from minute source")
    window[list(FIELDS)] = values
    return window


def feature_row(raw: pd.DataFrame, row: dict) -> dict:
    window = checked_prefix(raw, row)
    side = row["side"]
    if side not in ("LONG", "SHORT"):
        raise ValueError("Unknown direction")
    sign = 1.0 if side == "LONG" else -1.0
    close = window.close.astype(float)
    last = window.iloc[-1]
    prior10 = window.iloc[-11:-1]
    if len(prior10) != 10 or prior10.ts.max() >= last.ts:
        raise ValueError("Volume denominator is not exactly ten earlier minutes")
    prior_mean = float(prior10.volume.mean())
    if not np.isfinite(prior_mean) or prior_mean <= 0:
        raise ValueError("Nonpositive prior-ten volume mean")
    volume_ratio = float(last.volume) / prior_mean
    ema5 = close.ewm(span=5, adjust=False, min_periods=5).mean()
    ema13 = close.ewm(span=13, adjust=False, min_periods=13).mean()
    ema_spread = sign * float(ema5.iloc[-1] - ema13.iloc[-1])
    ema_slope = sign * float(ema5.iloc[-1] - ema5.iloc[-2])
    weights = window.volume.to_numpy(float)
    typical = window[["high", "low", "close"]].mean(axis=1).to_numpy(float)
    vwap = float(np.average(typical, weights=weights))
    directional_vwap_delta = sign * (float(last.close) - vwap)
    vwap_distance = directional_vwap_delta / vwap * 100.0
    ten_changes = np.diff(close.iloc[-11:].to_numpy(float))
    directional_change = sign * float(close.iloc[-1] - close.iloc[-11])
    total_change = float(np.abs(ten_changes).sum())
    # A completely flat ten-minute path is a valid zero-efficiency observation.
    efficiency = abs(directional_change) / total_change if total_change > 0 else 0.0
    prior3 = window.iloc[-4:-1]
    breakout_edge = float(prior3.high.max() if side == "LONG" else prior3.low.min())
    breakout_margin = sign * (float(last.close) - breakout_edge)
    candle_range = float(last.high - last.low)
    if candle_range <= 0:
        raise ValueError("Confirmation has no range")
    location = float((last.close - last.low) / candle_range if side == "LONG"
                     else (last.high - last.close) / candle_range)
    result = {
        "r2_feature_available_at": timestamp(row["confirmation_ts"]).isoformat(),
        "r2_session_start": window.ts.iloc[0].isoformat(),
        "r2_prefix_last_ts": last.ts.isoformat(),
        "r2_session_bars": len(window),
        "r2_prior10_volume_mean": prior_mean,
        "r2_confirmation_volume": float(last.volume),
        "r2_confirmation_volume_ratio10": volume_ratio,
        "r2_volume_denominator_last_ts": prior10.ts.iloc[-1].isoformat(),
        "r2_ema5": float(ema5.iloc[-1]),
        "r2_ema13": float(ema13.iloc[-1]),
        "r2_directional_ema5_13_spread": ema_spread,
        "r2_directional_ema5_change1": ema_slope,
        "r2_session_vwap": vwap,
        "r2_directional_vwap_distance_pct": vwap_distance,
        "r2_efficiency10": efficiency,
        "r2_directional_close_change10": directional_change,
        "r2_total_absolute_close_change10": total_change,
        "r2_prior3_breakout_edge": breakout_edge,
        "r2_directional_breakout3_margin": breakout_margin,
        "r2_directional_close_location": location,
        "r2_volume_ge_1p0": volume_ratio >= 1.0,
        "r2_volume_ge_1p5": volume_ratio >= 1.5,
        "r2_ema5_13_aligned": ema_spread > 0.0 and ema_slope > 0.0,
        # Algebraic price-space comparisons avoid ratio-minus-one cancellation
        # excluding an exact 1% boundary (101/100-1 rounds above .01).
        "r2_vwap_band_0p5": 0.0 <= directional_vwap_delta <= vwap * .005,
        "r2_vwap_band_1p0": 0.0 <= directional_vwap_delta <= vwap * .01,
        "r2_efficiency10_ge_0p3": efficiency >= .3 and directional_change > 0.0,
        "r2_breakout3": breakout_margin > 0.0,
        "r2_close_location_ge_0p8": location >= .8,
    }
    numeric = [value for value in result.values() if isinstance(value, (int, float))]
    if not np.isfinite(numeric).all():
        raise ValueError("Nonfinite advertised-ready round-two feature")
    # Prefix checksum pins every feature input, excluding all future bars.
    serial = window[["ts", *FIELDS]].copy()
    serial["ts"] = serial.ts.map(lambda x: x.isoformat())
    result["r2_prefix_sha256"] = hashlib.sha256(serial.to_csv(index=False, float_format="%.17g").encode()).hexdigest()
    return result


def load_pool() -> tuple[pd.DataFrame, list[dict]]:
    pieces, evidence = [], []
    for batch in ("early", "mid", "late", "missing_early_long", "missing_midday"):
        directory = PRIOR / batch
        manifest_path = directory / "artifact_manifest.json"
        manifest = read_json(manifest_path)
        # Hash all members without interpreting result tables. Thresholds have
        # already been fixed; no outcomes are consulted by this script.
        for filename, digest in manifest["files"].items():
            member = (directory / filename).resolve()
            if not member.is_relative_to(directory.resolve()) or sha256(member) != digest:
                raise ValueError("Prior package member changed: " + str(member))
        filename = "candidate_pool_full.pkl" if batch == "late" else "candidate_pool.pkl"
        if filename not in manifest["files"]:
            raise ValueError("Candidate pool is not pinned by prior manifest")
        pool = pd.read_pickle(directory / filename)
        pool["r2_source_batch"] = batch
        pool["r2_source_pool_sid"] = pool.sid
        pool["r2_source_candidate_uid"] = pool.candidate_uid
        pieces.append(pool)
        evidence.append({"batch": batch, "manifest": str(manifest_path),
                         "manifest_sha256": sha256(manifest_path),
                         "verified_members": len(manifest["files"]),
                         "pool_sha256": manifest["files"][filename], "pool_rows": len(pool)})
    pool = pd.concat(pieces, ignore_index=True, sort=False)
    pool["day"] = pool.day.astype(str).str[:10]
    if set(pool.setup_id) != set(SETUPS) or pool.duplicated(["day", "setup_id", "tradingsymbol"]).any():
        raise ValueError("Candidate setup coverage or unique identity mismatch")
    pool = pool.sort_values(["day", "setup_id", "tradingsymbol"], kind="stable").reset_index(drop=True)
    pool["candidate_uid"] = np.arange(1, len(pool) + 1, dtype=np.int64)
    pool["sid"] = 550_000_000 + pool.candidate_uid
    for field in ("v9_1m_feature_ts", "v9_feature_available_ts"):
        observed = pd.to_datetime(pool[field], utc=True, errors="raise")
        stamp = pd.to_datetime(pool.confirmation_ts, utc=True, errors="raise")
        if observed.isna().any() or (observed > stamp).any():
            raise ValueError("Missing or future original feature timestamp")
    if not pd.to_datetime(pool.v9_1m_feature_ts, utc=True).equals(pd.to_datetime(pool.confirmation_ts, utc=True)):
        raise ValueError("Original feature timestamp differs from confirmation")
    return pool, evidence


def main() -> None:
    from research.g4_complete_inventory_backtest import Prices

    qa = read_json(OUT / "feature_qa.json")
    if not qa.get("passed") or qa.get("source_code_sha256") != sha256(Path(__file__)):
        raise ValueError("Run test_prepare_features.py successfully against this code before preparing")
    protocol_path = OUT / "protocol.json"
    protocol = read_json(protocol_path)
    protocol_sha256 = sha256(protocol_path)
    preparation_started_at = datetime.now(ZoneInfo(IST)).isoformat()
    pool, prior_evidence = load_pool()
    summary = read_json(G3_RUN / "summary.json")
    manifest = read_json(G3_RUN / "manifest.json")
    if sha256(G3_RUN / "summary.json") != manifest["files"]["summary.json"]:
        raise ValueError("G3 source summary changed")
    if set(pool.day).difference(summary["days"]) or pool.day.max() > "2026-10-27":
        raise ValueError("Candidate outside completed-session calendar")
    prices = Prices(summary)
    feature_rows = []
    checks = {"exact_prefix_and_ohlcv": 0, "future_append_invariance": 0,
              "long_short_symmetry": 0, "wick_close_location_complement": 0}
    for number, row in enumerate(pool.to_dict("records"), 1):
        raw, source_key = prices.frame(pd.Timestamp(row["day"]).date(), row["tradingsymbol"])
        record = feature_row(raw, row)
        # Repeating with only the cutoff prefix proves later rows cannot affect
        # this candidate. Changing direction must mirror directional features.
        truncated = raw.loc[raw.ts.le(timestamp(row["confirmation_ts"]))]
        if feature_row(truncated, row) != record:
            raise ValueError("Feature depends on post-confirmation rows")
        reverse = feature_row(truncated, dict(row, side="SHORT" if row["side"] == "LONG" else "LONG"))
        for field in ("r2_directional_ema5_13_spread", "r2_directional_ema5_change1",
                      "r2_directional_vwap_distance_pct", "r2_directional_close_change10"):
            if not np.isclose(record[field], -reverse[field], rtol=0, atol=1e-12):
                raise ValueError("Direction sign is inconsistent: " + field)
        if not np.isclose(record["r2_directional_close_location"], 1-float(row["wick_ratio"]), rtol=0, atol=1e-9):
            raise ValueError("Close location differs from directional wick complement")
        for name in checks:
            checks[name] += 1
        record["r2_source_path"] = source_key
        record["r2_source_sha256"] = prices.proof[source_key]["sha256"]
        feature_rows.append(record)
        if number % 25 == 0 or number == len(pool):
            print(f"Feature-only preparation: {number}/{len(pool)} candidates", flush=True)
    features = pd.DataFrame(feature_rows, index=pool.index)
    if features[list(RULES)].isna().any().any():
        raise ValueError("Missing frozen rule decision")
    pool = pd.concat([pool, features], axis=1)
    OUT.mkdir(parents=True, exist_ok=True)
    pool.to_pickle(OUT / "candidate_pool.pkl")
    audit_columns = ["candidate_uid", "sid", "source_sid", "day", "setup_id", "tradingsymbol", "side",
                     "confirmation_ts", "r2_source_batch", "r2_source_pool_sid", *features.columns]
    pool[audit_columns].to_csv(OUT / "candidate_features.csv", index=False)
    inventory = pool.groupby("setup_id").agg(candidates=("candidate_uid", "size"), sessions=("day", "nunique"))
    inventory.to_csv(OUT / "candidate_inventory.csv")
    if sha256(protocol_path) != protocol_sha256:
        raise ValueError("Frozen round-two protocol changed during feature preparation")
    source_proof = {"prior_packages": prior_evidence, "minute_sources": prices.proof,
                    "protocol_path": str(protocol_path), "protocol_sha256": protocol_sha256,
                    "protocol": protocol, "preparation_started_at_ist": preparation_started_at,
                    "g3_summary_path": str(G3_RUN / "summary.json"),
                    "g3_summary_sha256": sha256(G3_RUN / "summary.json"),
                    "snapshot_inventory": prices.snapshot_proof}
    write_json(OUT / "feature_source_proof.json", source_proof)
    write_json(OUT / "candidate_pool_proof.json", source_proof)
    audit = {"scope": "BACKTEST_ONLY", "execution_authority": False, "outcome_tables_read": False,
             "complete": True, "candidates": len(pool), "setups": list(SETUPS), "rules": list(RULES),
             "unique_candidate_uid": bool(pool.candidate_uid.is_unique), "unique_sid": bool(pool.sid.is_unique),
             "calendar_first": summary["days"][0], "calendar_last": summary["days"][-1],
             "calendar_sessions": len(summary["days"]), "checks": checks,
             "minimum_completed_prefix_bars": int(pool.r2_session_bars.min()),
             "verified_prior_members": sum(x["verified_members"] for x in prior_evidence),
             "minute_sources": len(prices.proof),
             "volume_definition": "Current incremental volume / mean(previous ten completed session minutes), excludes current",
             "ema_definition": "Session-reset close EWMA adjust=False, spans5/13, minimum5/13; positive directional spread and EMA5 prior-minute change",
             "efficiency_definition": "Absolute C[t]-C[t-10] / sum absolute ten close changes; >=0.3 and directional C[t]-C[t-10]>0; flat=0",
             "vwap_definition": "Session HLC3 weighted by incremental bar volume; confirmation CLOSE directional distance",
             "breakout_definition": "Confirmation close strictly beyond previous3 completed highs LONG / lows SHORT; current excluded",
             "wick_caveat": "Native directional wick is the rejection-side wick; close location equals one minus native wick for directional confirmations",
             "new_namespace": "All r2 indicators recomputed session-wise; no v9 feature values imputed or overwritten",
             "cutoff_reason": "October28 excluded to retain the frozen through-October27 comparison calendar",
             "limitations": ["Chronological bar availability does not establish original live receipt latency.",
                             "Historical OHLCV prices may be current historical parquet when no dated snapshot exists; exact source hashes recorded.",
                             "Prior candidate pools already satisfy native setup gates; this grid only adds confirmations."]}
    write_json(OUT / "feature_audit.json", audit)
    files = ["prepare_features.py", "candidate_pool.pkl", "candidate_features.csv", "candidate_inventory.csv",
             "candidate_pool_proof.json", "feature_source_proof.json", "feature_audit.json", "protocol.json",
             "test_prepare_features.py", "feature_qa.json"]
    write_json(OUT / "feature_manifest.json", {"complete": True,
        "created_at_ist": datetime.now(ZoneInfo(IST)).isoformat(),
        "files": {name: sha256(OUT / name) for name in files}})
    print(json.dumps({"complete": True, "candidates": len(pool), "checks": checks,
                      "minute_sources": len(prices.proof)}), flush=True)


if __name__ == "__main__":
    main()
