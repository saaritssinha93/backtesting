"""Reselect G3 10:35 LONG/SHORT with completed one-minute extension inputs."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_replay_source as source
import fno_v13_v10_g_3_contract as production
from g3_one_minute_extension import last_features
from research.archived_g3_v7 import contract as baseline
from research.g4_extended_slot_backtest import CLOCKS, DEFAULT_CACHE
from research.g4_extended_target_sweep import read_frozen_csv
from research.g4_late_slot_source import load_late_source, sha256
from research.g3_0950_long_backtest import normalize_signals


SLOTS = frozenset({"1036_LONG", "1036_SHORT"})


def _snapshot_from_member(directory: Path, payload: dict) -> Path:
    record = payload["artifacts"]["source_manifest"]
    info = json.loads((directory / record["path"]).read_text())
    manifest = Path(info["snapshot_manifest"])
    if sha256(manifest) != info["snapshot_manifest_sha256"]:
        raise ValueError("10:35 daily snapshot manifest changed")
    return manifest.parent


def _enrich(pool: pd.DataFrame, snapshots: dict[date, Path]) -> tuple[pd.DataFrame, dict]:
    pool = pool.copy()
    proofs = {}
    for index, row in pool.iterrows():
        day = pd.Timestamp(row.day).date()
        symbol = str(row.tradingsymbol)
        if day in snapshots:
            path = snapshots[day] / "equity_1m" / f"{symbol}_stocks_indicators_1min.parquet"
            source_kind = "IMMUTABLE_DAY_SNAPSHOT"
        else:
            path = source.DataRoots().equity_1m / f"{symbol}_stocks_indicators_1min.parquet"
            source_kind = "CURRENT_HISTORICAL_PARQUET"
        if not path.is_file():
            raise FileNotFoundError(path)
        stamp = pd.Timestamp(row.confirmation_ts)
        stamp = stamp.tz_localize(production.IST) if stamp.tzinfo is None else stamp.tz_convert(production.IST)
        if stamp.strftime("%H:%M") != "10:36" or stamp.date() != day:
            raise ValueError("10:35 candidate confirmation clock changed")
        raw = pd.read_parquet(path, columns=["date", "open", "high", "low", "close"])
        raw["ts"] = source.hybrid._to_ist(raw.date)
        raw = (raw.loc[raw.ts.le(stamp)].sort_values("ts", kind="stable")
               .drop_duplicates("ts", keep="last"))
        current = raw.loc[raw.ts.eq(stamp)]
        if len(current) != 1:
            raise ValueError(f"Missing 10:36 source bar: {day} {symbol}")
        bar = current.iloc[0]
        for name in ("open", "high", "low", "close"):
            if not np.isclose(float(bar[name]), float(row[f"confirmation_{name}"]), rtol=0, atol=.001):
                raise ValueError(f"10:36 candidate/source OHLC mismatch: {day} {symbol} {name}")
        ema20, atr14 = last_features(raw.loc[raw.ts.lt(stamp)], bar.to_dict())
        if not (np.isfinite(ema20) and np.isfinite(atr14) and atr14 > 0):
            raise ValueError(f"Insufficient 10:36 EMA/ATR history: {day} {symbol}")
        pool.loc[index, "v9_1m_ema20"] = ema20
        pool.loc[index, "v9_1m_atr14"] = atr14
        proofs[str(path)] = dict(sha256=sha256(path), source_kind=source_kind)
    return pool, dict(files=proofs, immutable_snapshot_files=sum(
        item["source_kind"] == "IMMUTABLE_DAY_SNAPSHOT" for item in proofs.values()),
        current_historical_files=sum(
        item["source_kind"] == "CURRENT_HISTORICAL_PARQUET" for item in proofs.values()))


def reselect(original_orders: pd.DataFrame, days: list[date], members: list) -> tuple[pd.DataFrame, dict, dict, pd.DataFrame]:
    candidates, candidate_paths, historical_days, source_proof, snapshots = load_late_source(
        DEFAULT_CACHE, clocks=CLOCKS, sid_base=5_000_000, verify_snapshots=True)
    if historical_days != days[:49]:
        raise ValueError("10:35 source calendar differs from historical baseline")
    pool = candidates.loc[candidates.hhmm_int.eq(1035)].copy()
    member_map = {}
    for directory, payload, _ in members:
        frame = read_frozen_csv(directory / payload["artifacts"]["candidate_signals"]["path"])
        frame = normalize_signals(frame)
        pool = pd.concat([pool, frame.loc[frame.hhmm_int.eq(1035)]], ignore_index=True, sort=False)
        day = date.fromisoformat(payload["session_date"])
        snapshots[day] = _snapshot_from_member(directory, payload)
        member_map[day] = (directory, payload)
    if pool.duplicated(["day", "tradingsymbol", "side"]).any():
        raise ValueError("Duplicate 10:35 candidate in pre-quota source")
    pool, price_proof = _enrich(pool, snapshots)
    from fno_v13_v10_g_3_daily_replay import DEFAULT_SEALED_SOURCE
    sealed_manifest = json.loads((DEFAULT_SEALED_SOURCE / "dataset_manifest.json").read_text())
    original_hashes = {str(Path(item["path"]).resolve()).lower(): item["sha256"]
                       for item in sealed_manifest["sources"] if item.get("exists")}
    same_hash, changed_hash = 0, 0
    for filename, record in price_proof["files"].items():
        if record["source_kind"] != "CURRENT_HISTORICAL_PARQUET":
            continue
        original = original_hashes.get(str(Path(filename).resolve()).lower())
        if original == record["sha256"]:
            same_hash += 1
        elif original:
            changed_hash += 1
    price_proof["sealed_historical_file_hash_matches"] = same_hash
    price_proof["sealed_historical_file_hash_changed"] = changed_hash
    pool["input_sid"] = pool.sid
    pool["sid"] = np.arange(95_000_000, 95_000_000 + len(pool))
    previous = baseline.selection_audit(pool)
    revised = production.selection_audit(pool)
    old = original_orders.loc[original_orders.setup_id.isin(SLOTS)]
    old_keys = set(zip(old.day.astype(str), old.setup_id, old.tradingsymbol))
    reproduced = set(zip(previous.loc[previous.v9_selected, "day"].astype(str),
                         previous.loc[previous.v9_selected, "setup_id"],
                         previous.loc[previous.v9_selected, "tradingsymbol"]))
    if old_keys != reproduced:
        raise ValueError(f"10:35 original selection differs from pinned orders: missing={len(old_keys - reproduced)}, extra={len(reproduced - old_keys)}")
    selected = revised.loc[revised.v9_selected].copy()
    identity = {(str(row.day), row.setup_id, row.tradingsymbol): row.signal_id
                for row in old.itertuples()}
    selected["source_signal_id"] = [identity.get(
        (str(row.day), row.setup_id, row.tradingsymbol), row.signal_id)
        for row in selected.itertuples()]
    paths = {}
    for row in selected.itertuples():
        day = pd.Timestamp(row.day).date()
        if day in member_map:
            directory, payload = member_map[day]
            from research.g3_target_cutoff_backtest import _daily_paths
            native = selected.loc[[row.Index]].copy()
            native["sid"] = native.input_sid.astype(int)
            group, _ = _daily_paths(directory, payload, native)
            paths[int(row.sid)] = group[int(row.input_sid)]
        else:
            paths[int(row.sid)] = candidate_paths[int(row.input_sid)]
    if set(paths) != set(selected.sid.astype(int)):
        raise ValueError("10:35 reselected execution paths incomplete")
    before = previous.loc[previous.v9_selected, ["day", "setup_id", "tradingsymbol"]].copy()
    before["decision"] = "BEFORE"
    after = revised.loc[revised.v9_selected, ["day", "setup_id", "tradingsymbol", "v9_1m_extension_atr"]].copy()
    after["decision"] = "AFTER"
    audit = pd.concat([before, after], ignore_index=True, sort=False)
    proof = dict(candidate_source=source_proof, price_features=price_proof,
                 original_selected=len(old), revised_selected=len(selected),
                 original_keys=len(old_keys), revised_keys=len(set(zip(
                     selected.day.astype(str), selected.setup_id, selected.tradingsymbol))))
    return selected, paths, proof, audit


def main() -> None:
    from research.g3_target_cutoff_backtest import _baseline
    root = runtime_dir("backtesting_result_v13_v10_g_3")
    original, _, days, members, _ = _baseline(root)
    selected, _, proof, audit = reselect(original, days, members)
    print(json.dumps(dict(prior_selected=proof["original_selected"],
                          revised_selected=len(selected),
                          changed_rows=audit.to_dict("records")), indent=2, default=str))


if __name__ == "__main__":
    from eqidv2_runtime_paths import runtime_dir
    main()
