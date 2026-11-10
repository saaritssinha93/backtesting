"""Seal identical Oct28 execution-day cash data before removing task-created excess copies.

This compacts only this task's restricted source. It never removes files.
Deletion uses a separately reviewed literal-path list after validation passes.
"""
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from g3_v18_20261102 import data
from fno_v13_replay_source import _snapshot_fingerprint


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, obj):
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def main():
    out = data.LOCAL_RECOVERED.resolve()
    allowed = Path(__file__).resolve().parent
    if not out.is_relative_to(allowed):
        raise ValueError("Source escaped the new task directory")
    features_before, minutes_before, proof_before = data.load_day("2026-10-28")
    manifest_path, source_path = out / "run_manifest.json", out / "source_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    source = json.loads(source_path.read_text())
    old_snapshot_path = Path(source["snapshot_manifest"])
    snapshot = json.loads(old_snapshot_path.read_text())
    compact = out / "input_snapshots/compact_research_215"
    compact.mkdir(exist_ok=False)
    for label, original in (("run_manifest", manifest_path), ("source_manifest", source_path),
                            ("snapshot_manifest", old_snapshot_path)):
        shutil.copyfile(original, compact / ("parent_" + label + ".json"))
    groups = dict(tuple(minutes_before.groupby("tradingsymbol", sort=False)))
    sources, transformations = [], []
    for item in snapshot["sources"]:
        rec = dict(item)
        relative = Path(item["snapshot_relative_path"])
        destination = compact / relative
        if not destination.resolve().is_relative_to(compact.resolve()):
            raise ValueError("Snapshot member escaped compact directory")
        if item.get("captured"):
            destination.parent.mkdir(parents=True, exist_ok=True)
            if item["role"] == "EQUITY_ONE_MINUTE":
                symbol = relative.name.removesuffix("_stocks_indicators_1min.parquet")
                frame = groups[symbol][["ts", "open", "high", "low", "close", "volume"]].rename(columns={"ts": "date"})
                frame.to_parquet(destination, index=False, compression="zstd")
                rec.update(sha256=sha(destination), size_bytes=destination.stat().st_size,
                           original_full_history_sha256=item["sha256"],
                           original_full_history_size_bytes=item["size_bytes"],
                           retained_scope="EXACT_VALIDATED_2026_10_28_0916_TO_1515_EXECUTION_MINUTES")
                transformations.append(dict(symbol=symbol, rows=len(frame),
                    original_sha256=item["sha256"], compact_sha256=rec["sha256"],
                    original_size_bytes=item["size_bytes"], compact_size_bytes=rec["size_bytes"]))
            else:
                os.link(old_snapshot_path.parent / relative, destination)
        sources.append(rec)
    snapshot.update(sources=sources, schema_version="g3_v18_compact_restricted_research_input_snapshot_v1",
                    original_snapshot_manifest_sha256=sha(old_snapshot_path),
                    original_snapshot_manifest=str(old_snapshot_path),
                    snapshot_fingerprint=_snapshot_fingerprint(date(2026, 10, 28), sources),
                    price_history_scope="DAY_ONLY_EXECUTION; ORIGINAL_FULL_HISTORY_PRESELECTION_FEATURE_LEDGER_RETAINED_UNCHANGED")
    new_snapshot_path = compact / "snapshot_manifest.json"
    write(new_snapshot_path, snapshot)
    compaction = dict(scope="ONLY_TASK_CREATED_OCT28_SOURCE", feature_ledger_unchanged=True,
        execution_day_values_unchanged=True, original_snapshot_manifest_sha256=sha(old_snapshot_path),
        original_source_manifest_sha256=sha(source_path), original_run_manifest_sha256=sha(manifest_path),
        transformations=transformations,
        retained_cash_minutes="360 exact completed minutes per eligible stock,09:16 through15:15IST",
        full_history_note="Derived preselection features retain original full-history calculations; compact prices suffice for unchanged downstream indicator enrichment and execution replay.",
        raw_retention_note="Task-created whole-history equity snapshots can be removed only after compaction_validation.json passes; original preexisting market archives are never removed.")
    source.update(snapshot_manifest=str(new_snapshot_path), snapshot_manifest_sha256=sha(new_snapshot_path),
                  compaction=compaction)
    write(source_path, source)
    manifest["artifacts"]["source_manifest"]["sha256"] = sha(source_path)
    manifest["compaction"] = {k: v for k, v in compaction.items() if k != "transformations"}
    write(manifest_path, manifest)
    features_after, minutes_after, proof_after = data.load_day("2026-10-28")
    pd.testing.assert_frame_equal(features_before, features_after, check_exact=True)
    pd.testing.assert_frame_equal(minutes_before, minutes_after, check_exact=True)
    pd.testing.assert_frame_equal(data.context_bars(features_before, minutes_before),
                                  data.context_bars(features_after, minutes_after), check_exact=True)
    obsolete = sorted(p.resolve() for p in allowed.glob("sources/2026-10-28*/input_snapshots/*/equity_1m")
                      if p.resolve() != (compact / "equity_1m").resolve())
    for path in obsolete:
        if not path.is_relative_to(allowed) or path.name != "equity_1m":
            raise ValueError("Unsafe obsolete cash directory")
    validation = dict(status="PASS", exact_feature_frame_equality=True, exact_minute_frame_equality=True,
        exact_context_bar_equality=True, feature_rows=len(features_after), minute_rows=len(minutes_after),
        stocks=minutes_after.tradingsymbol.nunique(), feature_ledger_sha256=sha(out / "feature_ledger.csv"),
        compact_snapshot_manifest_sha256=sha(new_snapshot_path), restricted_run_manifest_sha256=sha(manifest_path),
        compact_cash_total_bytes=sum(x["compact_size_bytes"] for x in transformations),
        original_cash_total_bytes=sum(x["original_size_bytes"] for x in transformations),
        safe_to_remove_only_these_task_created_directories=[str(p) for p in obsolete],
        blocked_manifest_sha256=sha(out.with_name("2026-10-28_blocked") / "run_manifest.json"))
    write(out / "compaction_validation.json", validation)
    print(json.dumps(validation, indent=2), flush=True)


if __name__ == "__main__":
    main()
