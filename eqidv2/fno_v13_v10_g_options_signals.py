"""Causal retained-G trigger events for the three-lot ATM options replay.

The equity entry timestamp labels the END of the minute that first touched
the underlying trigger. An options order starts at the first five-minute
OPEN at or after that observation. Equal boundaries assume zero latency.
ATM uses the underlying minute CLOSE exactly at that boundary, never the
option entry bar's eventual close. Stock exits and P&L are not inputs.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd


RESEARCH_ROOT = Path("C:/TradingData/eqidv2/fno_oi/strategy_research")
DEFAULT_SOURCE_DIR = RESEARCH_ROOT / "v13_corrected_v10_g/run_20260914_opportunity_expansion"
DEFAULT_DATASET_DIR = RESEARCH_ROOT / "v13_corrected_v9/run_20260913/dataset"
IST = "Asia/Kolkata"
ORDER_COLUMNS = [
    "sid", "day", "setup_id", "side", "tradingsymbol", "signal_ts",
    "confirmation_ts", "trigger", "filled", "entry_ts",
]
OUTPUT_COLUMNS = [
    "trade_id", "day", "setup_id", "side", "equity_symbol", "signal_ts",
    "confirmation_ts", "underlying_trigger_observed_ts", "entry_ts",
    "atm_spot", "atm_spot_ts", "signal_status",
]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verified_file(path: Path, expected: str, records: list[dict]) -> Path:
    observed = _sha256(path)
    if observed != expected:
        raise RuntimeError(f"Frozen options signal source drift: {path}")
    records.append({"path": str(path.resolve()), "sha256": observed, "verified": True})
    return path


def _ist(value: Any) -> pd.Timestamp:
    result = pd.Timestamp(value)
    if pd.isna(result) or result.tzinfo is None:
        raise ValueError("Signal timestamps must be present and timezone-aware")
    return result.tz_convert(IST)


def _boolean(value: Any) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if str(value).lower() in ("true", "false"):
        return str(value).lower() == "true"
    raise ValueError("Frozen fill flags must be true or false")


def causal_signal_rows(
    orders: pd.DataFrame,
    paths: Mapping[int, Mapping[str, np.ndarray]],
) -> pd.DataFrame:
    """Recompute trigger eligibility, retaining unfilled orders as explicit skips.

    Only the ten minutes after confirmation can set the trigger event. The
    first such touch is observed at its candle end; any later values in a
    frozen path cannot change the trigger or ATM spot chosen for an order.
    """
    if orders[["sid", "setup_id"]].duplicated().any():
        raise ValueError("Duplicate G selected order identity")
    records = []
    for row in orders[ORDER_COLUMNS].itertuples(index=False):
        signal, confirmation = _ist(row.signal_ts), _ist(row.confirmation_ts)
        if confirmation - signal != pd.Timedelta(minutes=1):
            raise ValueError("G confirmation must be exactly one minute after signal")
        if signal != signal.floor("5min") or signal.date().isoformat() != str(row.day):
            raise ValueError("G signal must be a same-day completed five-minute boundary")
        if row.side not in ("LONG", "SHORT"):
            raise ValueError("Invalid G side")
        trigger = float(row.trigger)
        if not np.isfinite(trigger) or trigger <= 0:
            raise ValueError("Invalid underlying trigger")
        path = paths[int(row.sid)]
        stamp = np.asarray(path["timestamp_ns"], dtype=np.int64)
        if stamp.ndim != 1 or len(stamp) < 10 or np.any(np.diff(stamp) <= 0):
            raise ValueError("Frozen underlying path must be ordered with at least ten minutes")
        expected_window = confirmation.value + np.arange(1, 11, dtype=np.int64) * 60_000_000_000
        if not np.array_equal(stamp[:10], expected_window):
            raise ValueError("Missing exact minutes inside the G trigger window")
        high, low = np.asarray(path["high"], float), np.asarray(path["low"], float)
        close = np.asarray(path["close"], float)
        if not all(len(values) == len(stamp) for values in (high, low, close)):
            raise ValueError("Frozen underlying OHLC path length mismatch")
        if not np.isfinite(high[:10]).all() or not np.isfinite(low[:10]).all():
            raise ValueError("Invalid underlying trigger-window OHLC")
        touched = high[:10] >= trigger if row.side == "LONG" else low[:10] <= trigger
        hits = np.flatnonzero(touched)
        filled = bool(len(hits))
        if filled != _boolean(row.filled):
            raise RuntimeError("Recomputed underlying trigger differs from frozen G fill flag")
        record = {
            "trade_id": f"G_{int(row.sid)}_{row.setup_id}",
            "day": str(row.day), "setup_id": str(row.setup_id), "side": row.side,
            "equity_symbol": str(row.tradingsymbol), "signal_ts": signal,
            "confirmation_ts": confirmation, "underlying_trigger_observed_ts": pd.NaT,
            "entry_ts": pd.NaT, "atm_spot": np.nan, "atm_spot_ts": pd.NaT,
            "signal_status": "UNDERLYING_UNFILLED",
        }
        if filled:
            observed = pd.Timestamp(int(stamp[int(hits[0])]), tz="UTC").tz_convert(IST)
            if observed != _ist(row.entry_ts):
                raise RuntimeError("Recomputed underlying trigger time differs from frozen G ledger")
            entry = observed.ceil("5min")
            # These timestamps are candle END; entry is an option candle OPEN.
            # Equality uses the just-completed equity minute and assumes zero latency.
            atm_index = int(np.searchsorted(stamp, entry.value, side="left"))
            if atm_index >= len(stamp) or stamp[atm_index] != entry.value:
                raise RuntimeError(f"Missing exact completed underlying ATM minute: {row.tradingsymbol} {entry}")
            spot = float(close[atm_index])
            if not np.isfinite(spot) or spot <= 0:
                raise ValueError("Invalid underlying ATM spot")
            record.update(
                underlying_trigger_observed_ts=observed, entry_ts=entry,
                atm_spot=spot, atm_spot_ts=entry, signal_status="READY",
            )
        elif pd.notna(row.entry_ts):
            raise RuntimeError("Unfilled frozen G order has an entry timestamp")
        records.append(record)
    return pd.DataFrame(records, columns=OUTPUT_COLUMNS)


def make_signals(
    source_dir: Path | str = DEFAULT_SOURCE_DIR,
    *,
    dataset_dir: Path | str = DEFAULT_DATASET_DIR,
) -> tuple[pd.DataFrame, list[str], list[dict]]:
    """Return all selected G orders, complete session calendar and source hashes.

    The retained G manifest pins its selection ledger, calendar, configuration,
    and source verification. That verification pins the V9 dataset manifest,
    which pins the exact frozen underlying paths used here. Mutable current
    stock data and stock portfolio/exits are never consulted.
    """
    source, dataset = Path(source_dir), Path(dataset_dir)
    manifest_path = source / "research_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("complete") is not True:
        raise RuntimeError("Retained G research manifest is incomplete")
    artifacts = {key.replace("\\", "/"): value for key, value in manifest["artifacts"].items()}
    records = [{"path": str(manifest_path.resolve()), "sha256": _sha256(manifest_path),
                "verified": True, "role": "source_manifest_root"}]

    def source_file(relative: str) -> Path:
        return _verified_file(source / relative, artifacts[relative], records)

    config = json.loads(source_file("frozen_config.json").read_text(encoding="utf-8"))
    if config.get("morning_slots", False) or config.get("two_bar_continuation", False):
        raise ValueError("Options replay requires retained G; rejected expansions are disabled")
    ledger_path = source_file("final/V13_V10_G/selected_trades.csv")
    orders = pd.read_csv(ledger_path, usecols=ORDER_COLUMNS, float_precision="round_trip")
    calendar = pd.read_csv(source_file("daily_detailed.csv"), usecols=["day"]).day.astype(str).tolist()
    if calendar != sorted(set(calendar)) or not set(orders.day.astype(str)).issubset(calendar):
        raise ValueError("G session calendar is unordered, duplicated or incomplete")
    proof = json.loads(source_file("source_verification.json").read_text(encoding="utf-8"))
    source_proof = proof.get("source", proof)
    data_manifest_path = _verified_file(
        dataset / "dataset_manifest.json", source_proof["dataset_manifest_sha256"], records,
    )
    data_manifest = json.loads(data_manifest_path.read_text(encoding="utf-8"))
    if data_manifest["days"] != calendar:
        raise RuntimeError("Frozen equity path and retained G calendars disagree")
    path_file = _verified_file(dataset / "paths.npz", data_manifest["output_sha256"]["paths.npz"], records)
    with np.load(path_file, allow_pickle=False) as archive:
        paths = {}
        for sid in orders.sid.unique():
            paths[int(sid)] = {
                name: archive[f"{int(sid)}_{name}"]
                for name in ("timestamp_ns", "high", "low", "close")
            }
        result = causal_signal_rows(orders, paths)
    return result, calendar, records
