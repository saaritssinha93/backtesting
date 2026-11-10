"""Causal G7 dated-Nifty direction decisions for an isolated V21 backtest.

The input is a selected V21 order book.  Every order receives a decision from
the exact completed Nifty futures five-minute bar available at confirmation.
Immutable daily snapshots are preferred when the G3 source archived one;
older sessions use the named historical futures file with a recorded hash.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v18_20261102 import data as g3_data
from research.g4_nifty_direction_backtest import direction_at, normalize_month
from research.g6_nifty_direction import allowed


IST = "Asia/Kolkata"
FUTURES = g3_data.DATA / "fno_oi" / "raw_contracts_5m"


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _month_from_source(day: str) -> str:
    """Read the selected session's dated near-month from its G3 input source."""
    cache = g3_data.CACHE / day / "features.parquet"
    if cache.is_file():
        months = pd.read_parquet(cache, columns=["contract_month"]).contract_month
    else:
        run = g3_data._daily_runs().get(day)
        if run is None:
            raise ValueError(f"No G3 source to establish Nifty contract month: {day}")
        manifest = json.loads((run / "run_manifest.json").read_text(encoding="utf-8"))
        record = manifest["artifacts"]["feature_ledger"]
        path = (run / record["path"]).resolve()
        if not path.is_relative_to(run.resolve()) or _sha(path) != record["sha256"]:
            raise ValueError(f"G3 feature ledger provenance changed: {day}")
        months = pd.read_csv(path, usecols=["contract_month"]).contract_month
    unique = months.dropna().map(normalize_month).unique()
    if len(unique) != 1:
        raise ValueError(f"Ambiguous G3 near-month contract on {day}: {unique}")
    return str(unique[0])


def _source_proof(day: str, month: str) -> dict:
    contract = f"NIFTY{month}FUT"
    relative = f"futures_5m/{contract}_5minute.parquet"
    cached = g3_data.CACHE / day / "provenance.json"
    if cached.is_file():
        source_record = json.loads(cached.read_text(encoding="utf-8"))
        source_path = cached
    else:
        run = g3_data._daily_runs().get(day)
        if run is None:
            raise ValueError(f"No sealed G3 session source: {day}")
        run_manifest = json.loads((run / "run_manifest.json").read_text(encoding="utf-8"))
        source_member = run_manifest["artifacts"]["source_manifest"]
        source_path = (run / source_member["path"]).resolve()
        if not source_path.is_relative_to(run.resolve()) or _sha(source_path) != source_member["sha256"]:
            raise ValueError(f"G3 source manifest provenance changed: {day}")
        source_record = json.loads(source_path.read_text(encoding="utf-8"))

    common = dict(day=day, contract_month=month, contract=contract,
                  g3_source_proof=str(source_path.resolve()),
                  g3_source_proof_sha256=_sha(source_path))
    snapshot_name = source_record.get("snapshot_manifest")
    if snapshot_name:
        manifest_path = Path(snapshot_name).resolve()
        expected = source_record.get("snapshot_manifest_sha256", source_record.get("snapshot_sha256"))
        if not expected or _sha(manifest_path) != expected:
            raise ValueError(f"G3 dated snapshot manifest changed: {day}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if str(manifest.get("session_date")) != day:
            raise ValueError(f"Wrong G3 snapshot session: {day}")
        members = [item for item in manifest.get("sources", [])
                   if item.get("snapshot_relative_path") == relative and item.get("captured")]
        if len(members) > 1:
            raise ValueError(f"Duplicate dated Nifty snapshot member: {day}")
        if members:
            path = (manifest_path.parent / relative).resolve()
            if not path.is_relative_to(manifest_path.parent.resolve()):
                raise ValueError("Nifty snapshot path escaped its manifest")
            digest = _sha(path)
            if digest != members[0]["sha256"]:
                raise ValueError(f"Dated Nifty snapshot changed: {day}")
            return dict(common, path=str(path), sha256=digest,
                        source_kind="IMMUTABLE_DAY_SNAPSHOT",
                        snapshot_manifest=str(manifest_path),
                        snapshot_manifest_sha256=expected)

    path = (FUTURES / f"{contract}_5minute.parquet").resolve()
    if not path.is_file():
        raise ValueError(f"Missing dated Nifty futures history: {day} {contract}")
    return dict(common, path=str(path), sha256=_sha(path),
                source_kind="CURRENT_HISTORICAL_PARQUET")


def _day_frame(day: str, proof: dict) -> pd.DataFrame:
    path = Path(proof["path"])
    if _sha(path) != proof["sha256"]:
        raise ValueError(f"Nifty source changed before read: {day}")
    raw = pd.read_parquet(path)
    if "timestamp" not in raw or "tradingsymbol" not in raw:
        raise ValueError(f"Nifty source lacks required identity fields: {day}")
    raw["ts"] = pd.to_datetime(raw.timestamp, utc=True).dt.tz_convert(IST).dt.as_unit("ns")
    frame = raw.loc[raw.ts.dt.strftime("%Y-%m-%d").eq(day)].sort_values("ts").reset_index(drop=True)
    if frame.empty or frame.ts.duplicated().any():
        raise ValueError(f"Missing or duplicate dated Nifty session: {day}")
    if not frame.tradingsymbol.eq(proof["contract"]).all():
        raise ValueError(f"Wrong Nifty futures contract in session: {day}")
    if _sha(path) != proof["sha256"]:
        raise ValueError(f"Nifty source changed during read: {day}")
    proof["session_bars"] = int(len(frame))
    proof["first_bar"] = str(frame.ts.iloc[0])
    proof["last_bar"] = str(frame.ts.iloc[-1])
    return frame


def build_nifty_audit(orders: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """Return one exact-clock G7 Nifty permission per selected V21 order.

    Missing exact/opening bars, invalid prices or a changed source abort the
    replay.  No substitute contract, later candle or forward-filled value is
    permitted.  ``orders`` may contain ``contract_month``; otherwise its
    sealed G3 source establishes the single month for the day.
    """
    required = {"sid", "day", "setup_id", "tradingsymbol", "side", "confirmation_ts"}
    missing = required - set(orders)
    if missing:
        raise ValueError(f"Selected V21 orders lack Nifty audit fields: {sorted(missing)}")
    if orders.sid.isna().any() or orders.sid.duplicated().any():
        raise ValueError("Selected V21 order SIDs must be unique and present")

    rows: list[dict] = []
    proofs: list[dict] = []
    for day_value, group in orders.groupby(orders.day.astype(str).str[:10], sort=True):
        day = str(day_value)
        if "contract_month" in group and group.contract_month.notna().any():
            candidates = group.contract_month.dropna().map(normalize_month).unique()
            if len(candidates) != 1:
                raise ValueError(f"Ambiguous order contract month: {day}")
            month = str(candidates[0])
            if month != _month_from_source(day):
                raise ValueError(f"Order contract month differs from G3 source: {day}")
        else:
            month = _month_from_source(day)
        proof = _source_proof(day, month)
        frame = _day_frame(day, proof)
        proofs.append(proof)
        for order in group.itertuples(index=False):
            confirmation = pd.Timestamp(order.confirmation_ts)
            if confirmation.tzinfo is None:
                raise ValueError("Timezone-aware confirmation required")
            confirmation = confirmation.tz_convert(IST)
            if str(confirmation.date()) != day or confirmation != confirmation.floor("min"):
                raise ValueError(f"Invalid V21 confirmation clock: {order.sid}")
            result = direction_at(frame, confirmation)
            slot = float(result["slot_candle"])
            session = float(result["session_open"])
            if not np.isfinite([slot, session]).all():
                raise ValueError(f"Missing exact completed Nifty direction: {day} {order.setup_id}")
            bar = pd.Timestamp(result["nifty_bar_ts"])
            if bar.tzinfo is None or bar.tz_convert(IST) != confirmation.floor("5min") or bar > confirmation:
                raise ValueError(f"Noncausal Nifty bar: {day} {order.setup_id}")
            permitted = allowed(str(order.side), slot, session)
            rows.append(dict(sid=int(order.sid), day=day, setup_id=str(order.setup_id),
                             tradingsymbol=str(order.tradingsymbol), side=str(order.side),
                             confirmation_ts=confirmation.isoformat(),
                             nifty_contract=proof["contract"],
                             nifty_source_path=proof["path"],
                             nifty_source_sha256=proof["sha256"],
                             nifty_source_kind=proof["source_kind"],
                             g7_nifty_status=result["status"],
                             g7_nifty_bar_ts=bar.isoformat(),
                             g7_nifty_slot_change_pct=slot,
                             g7_nifty_session_change_pct=session,
                             g7_nifty_allowed=bool(permitted),
                             g7_nifty_reason="PERMITTED" if permitted else "BOTH_DIRECTIONS_OPPOSE"))
    audit = pd.DataFrame(rows)
    if len(audit) != len(orders) or audit.sid.duplicated().any() or set(audit.sid) != set(orders.sid):
        raise ValueError("Nifty decision inventory differs from selected V21 orders")
    return audit, proofs
