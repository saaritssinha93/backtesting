"""Combine disjoint V18 session partitions without resimulating trades."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from datetime import date
import shutil
import pandas as pd

from g3_v18_20261102.run import PACKAGE, dump, sha, guard_writes, finalize
from g3_v18_20261102 import data, selection
from research.g3_0920_backtest import allocate


def run(args):
    output = args.output.resolve()
    if not output.is_relative_to(PACKAGE) or output.exists():
        raise ValueError("Final output must be a fresh directory in the new package")
    guard_writes(PACKAGE)
    ledgers, audits = {v: [] for v in selection.VARIANTS}, {v: [] for v in selection.VARIANTS}
    days, proofs, feature_tables, day_coverage, sources = [], [], [], [], []
    historical_spot = None
    config = None
    engine_hashes = None
    for folder in args.inputs:
        folder = folder.resolve()
        if not folder.is_relative_to(PACKAGE):
            raise ValueError("Partition must belong to this new package")
        manifest = json.loads((folder / "manifest.json").read_text())
        for name, digest in manifest["files"].items():
            if sha(folder / name) != digest:
                raise ValueError(f"Partition artifact changed: {folder.name}/{name}")
        summary = json.loads((folder / "summary.json").read_text())
        part_config = json.loads((folder / "config.json").read_text())
        if set(summary["days"]) & set(days):
            raise ValueError("Overlapping partitions")
        days.extend(summary["days"])
        settings = {k: v for k, v in part_config.items() if k != "planned_calendar"}
        if config is not None and config != settings:
            raise ValueError("Partitions differ in strategy configuration")
        config = settings
        for variant in selection.VARIANTS:
            ledgers[variant].append(pd.read_csv(folder / f"{variant}_trades.csv", low_memory=False))
            audits[variant].append(pd.read_csv(folder / f"{variant}_selection_audit.csv", low_memory=False))
            for frame in (ledgers[variant][-1], audits[variant][-1]):
                if not set(frame.day.astype(str)).issubset(summary["days"]):
                    raise ValueError("Partition row escaped its declared calendar")
        # Each partition numbered sids from zero. Re-key all inventories below.
        features = pd.read_parquet(folder / "candidate_features.parquet")
        features["_partition"] = folder.name
        feature_tables.append(features)
        for variant in selection.VARIANTS:
            ledgers[variant][-1]["_partition"] = folder.name
            audits[variant][-1]["_partition"] = folder.name
        proof = json.loads((folder / "source_provenance.json").read_text())
        engines = {name: proof["new_code"][name] for name in ("data.py", "features.py", "selection.py", "run.py")}
        engines.update(proof.get("legacy_code", {}))
        if engine_hashes is not None and engines != engine_hashes:
            raise ValueError("Partitions used different computation code")
        engine_hashes = engines
        proofs.extend(proof["sessions"])
        coverage = pd.read_csv(folder / "coverage.csv")
        day_coverage.extend(coverage.loc[coverage.day.isin(summary["days"])].to_dict("records"))
        spot = pd.read_parquet(folder / "nifty_spot_input.parquet")
        spot = spot.loc[pd.to_datetime(spot.date, utc=True).dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%d").le(max(data.available_days(str(args.through))))]
        if historical_spot is not None:
            pd.testing.assert_frame_equal(historical_spot.reset_index(drop=True), spot.reset_index(drop=True))
        historical_spot = spot
        sources.append(dict(path=str(folder), manifest_sha256=sha(folder / "manifest.json")))
    days = sorted(days)
    expected = data.available_days(str(args.through))
    if days != expected:
        raise ValueError(f"Partition calendar differs from available source calendar: {set(days)^set(expected)}")
    config["planned_calendar"] = days
    output.mkdir(parents=True)
    dump(output / "config.json", config)
    fingerprint = sha(output / "config.json")
    shutil.copyfile(args.inputs[0] / "nifty_spot_input.parquet", output / "nifty_spot_input.parquet")
    # Unique deterministic IDs preserve candidate identity within each partition.
    inventory = pd.concat(feature_tables, ignore_index=True)
    if inventory.duplicated(["_partition", "sid"]).any():
        raise ValueError("Duplicate partition candidate identity")
    inventory = inventory.sort_values(["day", "tradingsymbol", "signal_ts"], kind="stable")
    mapping = {key: n for n, key in enumerate(zip(inventory._partition, inventory.sid))}
    def rekey(frame):
        frame["source_partition_sid"] = frame.sid
        frame["sid"] = [mapping[(part, sid)] for part, sid in zip(frame._partition, frame.sid)]
        return frame.drop(columns="_partition")
    feature_tables = [rekey(inventory)]
    for variant in selection.VARIANTS:
        ledgers[variant] = [rekey(pd.concat(ledgers[variant], ignore_index=True))]
        ledgers[variant][0]["source_partition_fingerprint"] = ledgers[variant][0].strategy_fingerprint
        ledgers[variant][0]["strategy_fingerprint"] = fingerprint
        audits[variant] = [rekey(pd.concat(audits[variant], ignore_index=True))]
    dump(output / "partition_provenance.json", dict(partitions=sources, historical_nifty_frames_identical=True,
                                                   reallocation="Within each day; portfolio is flat overnight; partition boundaries do not change execution"))
    return finalize(args, output, config, days, ledgers, audits, proofs, feature_tables, day_coverage,
                    sha(output / "nifty_spot_input.parquet"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", nargs="+", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--through", type=date.fromisoformat, default=date(2026, 11, 2))
    run(parser.parse_args())
