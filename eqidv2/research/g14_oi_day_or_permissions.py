"""Causal G14 morning entry permissions inherited from G7 V4 Lock2.

The selector supplies a fresh, pre-allocation candidate book. This module
recomputes the G7 Nifty-futures both-counter gate and the V4 low-efficiency
extreme veto for those candidates. Nifty inputs and stock history are read
from retained, hash-checked snapshots; no later price or outcome is used in
an entry decision.
"""
from __future__ import annotations

from functools import lru_cache
from hashlib import sha256
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g4_nifty_direction_backtest as nifty
from research import g6_nifty_direction as both_counter
from research import g7_v4_extremes as extremes
from research import g7_v4_indicator_iteration as indicators


ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT / "g3_indicator_lab_20261103" / "inputs"
G7_NIFTY = Path(
    "C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/runs/"
    "2026-10-22/20261022T171000000000_g7_v3_two_stage_exit"
)
EARLY_STOCK_SNAPSHOT = Path(
    "C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/production/"
    "runs/2026-10-14/20261021T183531060381_39efe64c/input_snapshots/"
    "4e03fbb05aa63bc26d5042eb1eaf8d8eb74be44644ff04d2339a4d55253c6923"
)
IST = "Asia/Kolkata"
REQUIRED = frozenset({
    "sid", "day", "setup_id", "tradingsymbol", "side", "confirmation_ts",
    "confirmation_open", "confirmation_high", "confirmation_low",
    "confirmation_close", "trigger",
})


@lru_cache(maxsize=1024)
def _file_sha_at_stat(path: Path, size: int, mtime_ns: int) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_sha(path: Path) -> str:
    stat = path.stat()
    return _file_sha_at_stat(path, stat.st_size, stat.st_mtime_ns)


@lru_cache(maxsize=128)
def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _manifest_file(root: Path, relative: str) -> tuple[Path, str]:
    manifest = _json(root / "snapshot_manifest.json")
    matches = [x for x in manifest["sources"]
               if x.get("snapshot_relative_path", "").replace("\\", "/") == relative]
    if len(matches) != 1 or not matches[0].get("captured"):
        raise ValueError(f"Missing immutable source {relative} in {root}")
    path = root / relative
    digest = str(matches[0]["sha256"])
    if file_sha(path) != digest:
        raise ValueError(f"Immutable source hash mismatch: {path}")
    return path, digest


@lru_cache(maxsize=128)
def _lab_provenance(day: str) -> dict:
    return _json(LAB / day / "source_provenance.json")


def _snapshot_root(day: str) -> Path | None:
    item = _lab_provenance(day).get("snapshot_manifest")
    if not item:
        return None
    path = Path(item)
    expected = _lab_provenance(day).get("snapshot_manifest_sha256")
    if expected and file_sha(path) != expected:
        raise ValueError(f"Daily snapshot manifest hash mismatch: {day}")
    return path.parent


@lru_cache(maxsize=128)
def _nifty_day(day: str) -> tuple[pd.DataFrame, dict]:
    """Use the archived G7 day file, then the dated G3 immutable snapshot."""
    archived = G7_NIFTY / "nifty_inputs" / f"{day}.parquet"
    if archived.exists():
        manifest = _json(G7_NIFTY / "manifest.json")
        relative = f"nifty_inputs/{day}.parquet"
        expected = manifest["files"].get(relative)
        if not expected or file_sha(archived) != expected:
            raise ValueError(f"Archived G7 Nifty input hash mismatch: {day}")
        frame = pd.read_parquet(archived)
        proof = pd.read_csv(G7_NIFTY / "nifty_source_proof.csv")
        match = proof.loc[proof.day.eq(day)]
        if len(match) != 1:
            raise ValueError(f"Missing archived Nifty source proof: {day}")
        contract = str(match.contract.iloc[0])
        source = "G7_V3_ARCHIVED_DAY"
        source_sha = expected
    else:
        root = _snapshot_root(day)
        if root is None:
            raise ValueError(f"No frozen Nifty source: {day}")
        paths = sorted((root / "futures_5m").glob("NIFTY*FUT_5minute.parquet"))
        paths = [p for p in paths if p.name.startswith("NIFTY26")]
        if len(paths) != 1:
            raise ValueError(f"Ambiguous frozen near-month Nifty source: {day}")
        relative = f"futures_5m/{paths[0].name}"
        path, source_sha = _manifest_file(root, relative)
        frame = pd.read_parquet(path)
        frame["ts"] = pd.to_datetime(frame.timestamp, errors="raise", utc=True).dt.tz_convert(IST)
        frame = frame.loc[frame.ts.dt.strftime("%Y-%m-%d").eq(day)].copy()
        contract = path.name.removesuffix("_5minute.parquet")
        source = "G3_DATED_INPUT_SNAPSHOT"
    if frame.empty or frame.ts.duplicated().any() or not frame.tradingsymbol.eq(contract).all():
        raise ValueError(f"Invalid frozen Nifty session: {day}")
    return frame.sort_values("ts").reset_index(drop=True), dict(
        nifty_contract=contract, nifty_source_kind=source,
        nifty_snapshot_sha256=source_sha,
    )


def _stock_snapshot(day: str, symbol: str) -> tuple[Path, str, str]:
    root = _snapshot_root(day)
    if root is None or not (root / "equity_1m").exists():
        # G7 V4's early historical experiment used the frozen Oct14 G3 run.
        root = EARLY_STOCK_SNAPSHOT
    relative = f"equity_1m/{symbol}_stocks_indicators_1min.parquet"
    path, digest = _manifest_file(root, relative)
    return path, digest, "DATED_G3_SNAPSHOT" if root != EARLY_STOCK_SNAPSHOT else "OCT14_G3_SNAPSHOT"


@lru_cache(maxsize=24)
def _stock_days(path: Path, digest: str, before_day: str) -> dict[str, pd.DataFrame]:
    if file_sha(path) != digest:
        raise ValueError(f"Stock snapshot changed: {path}")
    columns = ["date", "open", "high", "low", "close", "volume",
               "gap_filled", "opening_snapshot", "provisional_stale", "is_synthetic"]
    import pyarrow.parquet as pq
    schema = set(pq.read_schema(path).names)
    frame = pd.read_parquet(path, columns=[name for name in columns if name in schema])
    frame["ts"] = pd.to_datetime(frame.date, errors="raise", utc=True).dt.tz_convert(IST)
    frame = frame.loc[frame.ts.dt.strftime("%Y-%m-%d").le(before_day)].copy()
    frame["source_day"] = frame.ts.dt.strftime("%Y-%m-%d")
    prior = sorted(d for d in frame.source_day.unique() if d < before_day)[-60:]
    frame = frame.loc[frame.source_day.isin([*prior, before_day])]
    days = {key: group.drop(columns="source_day").sort_values("ts").reset_index(drop=True)
            for key, group in frame.groupby("source_day", sort=True)}
    if file_sha(path) != digest:
        raise ValueError(f"Stock snapshot changed while reading: {path}")
    return days


def _minute_day(minutes: pd.DataFrame, day: str, symbol: str) -> pd.DataFrame:
    if "ts" not in minutes or "tradingsymbol" not in minutes:
        raise ValueError("G14 minute input requires ts and tradingsymbol")
    frame = minutes.loc[minutes.tradingsymbol.eq(symbol)].copy()
    frame["ts"] = pd.to_datetime(frame.ts, errors="raise", utc=True).dt.tz_convert(IST)
    frame = frame.loc[frame.ts.dt.strftime("%Y-%m-%d").eq(day)].copy()
    if frame.empty:
        raise ValueError(f"Missing G14 selected stock minutes: {day} {symbol}")
    return frame.sort_values("ts").reset_index(drop=True)


def _veto(row: dict, minutes: pd.DataFrame, day: str) -> tuple[bool, dict]:
    stamp = extremes.timestamp(row["confirmation_ts"])
    if stamp.strftime("%H:%M") < "09:45":
        return False, dict(low_efficiency_source="WARMUP_LT30", indicator_ready=False,
                           historical_ready=False, signed_vwap_z=np.nan,
                           efficiency10=np.nan, signed_rsi_delta3=np.nan)
    current = _minute_day(minutes, day, str(row["tradingsymbol"]))
    if stamp.strftime("%H:%M") != "09:46":
        raise ValueError(f"Unexpected G14 morning confirmation: {stamp}")
    source_path = source_digest = source_kind = None
    history = None
    # Some sealed lab day minute files retain OHLC but omit volume. The V4
    # veto needs volume-weighted VWAP, so recover that day's complete prefix
    # from the manifest-verified original stock snapshot in those cases.
    if "volume" not in current or current.volume.isna().any():
        source_path, source_digest, source_kind = _stock_snapshot(day, str(row["tradingsymbol"]))
        history = dict(_stock_days(source_path, source_digest, day))
        if day not in history:
            raise ValueError(f"Frozen stock snapshot lacks current session: {day}")
        current = history[day]
    # The original V4 feature functions validate exact completed prefixes and
    # use only the confirmation minute or earlier, including prior sessions.
    indicator = indicators.indicator_row(current, row)
    if not indicator["indicator_ready"]:
        raise ValueError(f"09:46 V4 indicator unexpectedly unavailable: {day}")
    window = extremes.checked_session(current, stamp)
    signed = 1 if row["side"] == "LONG" else -1
    rsi = extremes.wilder_rsi(window.close.to_numpy(float))
    delta = signed * float(rsi[-1] - rsi[-4])
    if not np.isfinite(delta):
        raise ValueError(f"09:46 V4 RSI unexpectedly unavailable: {day}")
    if not (float(indicator["efficiency10"]) < .30 and delta < 0):
        return False, dict(low_efficiency_source="CURRENT_PREFIX_NO_VETO",
                           indicator_ready=True, historical_ready=False,
                           signed_vwap_z=np.nan,
                           efficiency10=float(indicator["efficiency10"]),
                           signed_rsi_delta3=delta)
    if history is None:
        source_path, source_digest, source_kind = _stock_snapshot(day, str(row["tradingsymbol"]))
        history = dict(_stock_days(source_path, source_digest, day))
    history[day] = current
    feature = extremes.feature_row(history, row)
    combined = pd.DataFrame([{**feature, **indicator}])
    veto = bool(indicators.decisions(combined)["low_efficiency"].iloc[0])
    return veto, dict(low_efficiency_source=source_kind, stock_snapshot_sha256=source_digest,
                       indicator_ready=bool(indicator["indicator_ready"]),
                       historical_ready=bool(feature["historical_ready"]),
                       signed_vwap_z=float(feature["signed_vwap_z"]),
                       efficiency10=float(indicator["efficiency10"]),
                       signed_rsi_delta3=delta,
                       historical_samples=int(feature["historical_samples"]))


def allowed_day(selected: pd.DataFrame, day: str, minutes: pd.DataFrame,
                *, strict: bool = True) -> tuple[pd.Series, pd.DataFrame]:
    """Return SID-indexed inherited permission and a per-candidate audit.

    Missing/invalid frozen source files and hash mismatches always raise.
    Other missing candidate evidence raises in strict mode; a non-strict call
    records a false permission and the error for diagnostic use only.
    """
    if selected.empty:
        return pd.Series(dtype=bool, name="permission_allowed"), pd.DataFrame()
    missing = REQUIRED.difference(selected.columns)
    if missing or selected.sid.isna().any() or selected.sid.duplicated().any():
        raise ValueError(f"Invalid G14 selected inventory; missing {sorted(missing)}")
    if not selected.day.astype(str).eq(day).all():
        raise ValueError("G14 selected day differs from permission day")
    nifty_frame, proof = _nifty_day(day)
    records = []
    for row in selected.to_dict("records"):
        audit = dict(sid=int(row["sid"]), day=day, setup_id=row["setup_id"],
                     tradingsymbol=row["tradingsymbol"], side=row["side"],
                     confirmation_ts=str(row["confirmation_ts"]), **proof)
        try:
            if row["side"] not in ("LONG", "SHORT"):
                raise ValueError("Invalid side")
            if "contract_month" in row and pd.notna(row["contract_month"]):
                month = nifty.normalize_month(row["contract_month"])
                if proof["nifty_contract"] != f"NIFTY{month}FUT":
                    raise ValueError("Nifty contract month differs from candidate")
            direction = nifty.direction_at(nifty_frame, row["confirmation_ts"])
            if not np.isfinite([direction["slot_candle"],
                                direction["session_open"]]).all():
                raise ValueError(f"Incomplete exact Nifty direction: {direction['status']}")
            stamp = extremes.timestamp(row["confirmation_ts"])
            if extremes.timestamp(direction["nifty_bar_ts"]) > stamp:
                raise ValueError("Future Nifty bar used")
            nifty_allowed = both_counter.allowed(row["side"], direction["slot_candle"],
                                                 direction["session_open"])
            veto, veto_audit = _veto(row, minutes, day)
            audit.update(direction, **veto_audit,
                         nifty_allowed=bool(nifty_allowed),
                         low_efficiency_veto=bool(veto),
                         permission_allowed=bool(nifty_allowed and not veto),
                         permission_status="OK")
        except (ValueError, KeyError, TypeError, OSError) as exc:
            if strict:
                raise ValueError(f"G14 permission unavailable for {day} "
                                 f"{row['setup_id']} {row['tradingsymbol']}: {exc}") from exc
            audit.update(nifty_allowed=False, low_efficiency_veto=False,
                         permission_allowed=False, permission_status=f"ERROR: {exc}")
        records.append(audit)
    frame = pd.DataFrame(records).set_index("sid", drop=False)
    allowed = frame.permission_allowed.astype(bool).rename("permission_allowed")
    if not allowed.index.is_unique or set(allowed.index) != set(selected.sid):
        raise ValueError("G14 permission audit inventory differs")
    return allowed, frame
