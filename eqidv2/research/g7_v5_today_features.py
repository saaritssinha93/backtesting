"""Rebuild the G7 V5 preselection stock ledger at a frozen intraday cutoff."""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from datetime import date
import json
from pathlib import Path

import pandas as pd
import numpy as np

import fno_v13_replay_source as source
from research import g7_g6_replica_backtest as g7


def _partition(args):
    day, cutoff, roots, clocks = args
    original = source._cutoff
    source._cutoff = lambda session: cutoff if session == day else original(session)
    try:
        result = source.build_day_features(day, roots=roots, signal_clocks=clocks)
    finally:
        source._cutoff = original
    result.pop("minutes", None)
    return result


def run(day: date, cutoff: pd.Timestamp, merged: Path, retrieval: Path, output: Path,
        *, snapshot_path: Path | None = None, workers: int = 1) -> dict:
    if output.exists():
        raise FileExistsError(output)
    if cutoff.date() != day or cutoff >= pd.Timestamp.now(tz="Asia/Kolkata").floor("min"):
        raise ValueError("Require a completed as-of minute on the session date")
    merge_proof = json.loads((merged / "merge_provenance.json").read_text(encoding="utf-8"))
    fetch_proof = json.loads((retrieval / "retrieval_provenance.json").read_text(encoding="utf-8"))
    if merge_proof["cutoff"] != fetch_proof["cutoff"] or merge_proof["stocks"] != 218:
        raise ValueError("Cash and opening receipts use different cutoff/universe")
    if snapshot_path is None:
        original_roots = source.DataRoots(equity_1m=merged / "equity_1m",
                                          opening_observer=retrieval / "opening_observer")
        snapshot = source.create_input_snapshot(day, output / "input_snapshots", roots=original_roots)
        snapshot_root = snapshot.root
        snapshot_manifest = snapshot.manifest_path
    else:
        snapshot_root = snapshot_path.resolve()
        snapshot_manifest = snapshot_root / "snapshot_manifest.json"
    manifest = json.loads(snapshot_manifest.read_text(encoding="utf-8"))
    source._verify_input_snapshot(snapshot_root, manifest)
    if manifest["session_date"] != day.isoformat():
        raise ValueError("Snapshot date differs from requested session")
    roots = source.DataRoots(snapshot_root / "universe", snapshot_root / "equity_1m",
                             snapshot_root / "futures_5m", snapshot_root / "opening_observer")
    unexpected = [p for p in manifest["problems"] if p.get("reason") != "SOURCE_SNAPSHOT_UNSTABLE"
                  and not str(p.get("path", "")).endswith(".json")]
    if unexpected:
        raise ValueError(f"Non-opening snapshot members unavailable: {len(unexpected)}")
    config = g7.configuration()
    clocks = sorted({s["signal_end"] for s in config["active_setups"]
                     if pd.Timestamp(source.slot_datetime(day, s["confirmation_end"])) <= cutoff})
    if not clocks:
        raise ValueError("No G7 setup has confirmed by the requested cutoff")
    if workers == 1:
        result = _partition((day, cutoff, roots, clocks))
    else:
        full = pd.read_parquet(roots.universe / f"near_month_{day}.parquet")
        stocks = full.loc[~full.is_index_future.fillna(False).astype(bool)]
        indexes = full.loc[full.is_index_future.fillna(False).astype(bool)]
        args = []
        for index in range(workers):
            folder = output / "feature_partitions" / str(index)
            folder.mkdir(parents=True, exist_ok=False)
            pd.concat((stocks.iloc[index::workers], indexes), ignore_index=True).to_parquet(
                folder / f"near_month_{day}.parquet", index=False)
            args.append((day, cutoff, source.DataRoots(folder, roots.equity_1m,
                                                       roots.futures_5m, roots.opening_observer), clocks))
        with ProcessPoolExecutor(max_workers=workers) as executor:
            parts = list(executor.map(_partition, args))
        result = dict(universe_count=sum(part["universe_count"] for part in parts))
        if result["universe_count"] != len(stocks):
            raise ValueError("Parallel reconstruction lost dated universe stocks")
        for field in ("problems", "coverage_rows", "excluded_stocks", "opening_0920_exclusions"):
            result[field] = [item for part in parts for item in part[field]]
        if any("opening_0920_status" in p["feature_ledger"] and
               p["feature_ledger"].opening_0920_status.eq("VALID").any() for p in parts):
            result["problems"] = [p for p in result["problems"] if p["reason"] != "NO_VALID_0920_OPENING_INPUTS"]
        result["feature_ledger"] = pd.concat((p["feature_ledger"] for p in parts), ignore_index=True)
        result["signals"] = pd.concat((p["signals"] for p in parts), ignore_index=True)
        result["signals"] = result["signals"].sort_values(
            ["tradingsymbol", "signal_ts", "side"], kind="stable").reset_index(drop=True)
        result["signals"]["sid"] = np.arange(len(result["signals"]), dtype=int)
    blockers = [p for p in result["problems"] if p["reason"] not in
                source.EXCLUDABLE_COVERAGE_REASONS | {"NO_VALID_0920_OPENING_INPUTS"}]
    output.mkdir(parents=True, exist_ok=True)
    result["feature_ledger"].to_parquet(output / "features.parquet", index=False)
    result["signals"].to_parquet(output / "strict_signals.parquet", index=False)
    pd.DataFrame(result["coverage_rows"]).to_csv(output / "coverage.csv", index=False)
    meta = dict(scope="BACKTEST_ONLY", execution_authority=False,
                day=day.isoformat(), cutoff=str(cutoff), snapshot=str(snapshot_root),
                snapshot_fingerprint=manifest["snapshot_fingerprint"],
                snapshot_problems=list(manifest["problems"]),
                signal_clocks=clocks, universe_stocks=result["universe_count"],
                feature_rows=len(result["feature_ledger"]), strict_signals=len(result["signals"]),
                excluded_stocks=result["excluded_stocks"],
                opening_exclusions=result["opening_0920_exclusions"],
                problems=result["problems"], blocking_problems=blockers,
                source_kind="RETROSPECTIVE_KITE_MINUTE_AND_DATED_FUTURES_OI",
                live_equivalence=False,
                live_5m_price_conflict_stocks=merge_proof["live_5m_price_conflict_stocks"])
    (output / "feature_provenance.json").write_text(json.dumps(source._json_ready(meta), indent=2), encoding="utf-8")
    return meta


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", type=date.fromisoformat, required=True)
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--merged", type=Path, required=True)
    parser.add_argument("--retrieval", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--workers", type=int, default=1, choices=range(1, 9))
    args = parser.parse_args()
    cutoff = pd.Timestamp(f"{args.day} {args.cutoff}", tz=source.common.IST)
    result = run(args.day, cutoff, args.merged, args.retrieval, args.output,
                 snapshot_path=args.snapshot, workers=args.workers)
    print(json.dumps({key: value for key, value in result.items()
                      if key not in ("problems", "snapshot_problems", "opening_exclusions")},
                     indent=2, default=str))


if __name__ == "__main__":
    main()
