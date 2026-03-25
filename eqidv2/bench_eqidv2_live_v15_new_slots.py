from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

from eqidv2_runtime_paths import RUNTIME_ROOT
from live_v15_slot_snapshot_v15_new import build_slot_snapshots, normalize_slot_ist, slot_key


DEFAULT_SLOTS = [
    "2026-03-23 09:30:00+05:30",
    "2026-03-23 10:00:00+05:30",
    "2026-03-23 11:15:00+05:30",
    "2026-03-23 13:30:00+05:30",
]


def _run_slot_benchmark(
    slot_ts: Any,
    *,
    bench_root: Path,
    snapshot_workers: int,
    scan_workers: int,
    shard_count: int,
) -> Dict[str, Any]:
    slot_ist = normalize_slot_ist(slot_ts)
    slot_runtime_root = bench_root / slot_key(slot_ist)
    slot_runtime_root.mkdir(parents=True, exist_ok=True)
    logs_dir = slot_runtime_root / "bench_logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    snapshot_meta = build_slot_snapshots(
        slot_ist,
        runtime_root=slot_runtime_root,
        shard_count=int(shard_count),
        max_workers=int(snapshot_workers),
    )

    runner = Path(__file__).resolve().parent / "eqidv2_live_combined_analyser_csv_v15_new.py"
    python_exe = Path(sys.executable)
    procs: List[Tuple[subprocess.Popen, Any, str, Path]] = []

    scan_started = time.perf_counter()
    launch_times: Dict[str, float] = {}
    for shard_id in range(1, int(shard_count) + 1):
        shard_label = f"{shard_id:02d}"
        log_path = logs_dir / f"shard_{shard_label}.log"
        fh = open(log_path, "w", encoding="utf-8")
        cmd = [
            str(python_exe),
            str(runner),
            "--slot",
            slot_ist.isoformat(),
            "--shard-id",
            str(shard_id),
            "--runtime-root",
            str(slot_runtime_root),
            "--scan-workers",
            str(scan_workers),
        ]
        proc = subprocess.Popen(
            cmd,
            stdout=fh,
            stderr=subprocess.STDOUT,
            cwd=str(runner.parent.parent),
        )
        launch_times[shard_label] = round(time.perf_counter() - scan_started, 3)
        procs.append((proc, fh, shard_label, log_path))

    proc_results: List[Dict[str, Any]] = []
    for proc, fh, shard_label, log_path in procs:
        rc = proc.wait()
        fh.close()
        summary_path = (
            slot_runtime_root
            / "live_v15_new_scan"
            / slot_key(slot_ist)
            / f"shard_{shard_label}"
            / "summary.json"
        )
        if summary_path.exists():
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        else:
            payload = {
                "shard_id": shard_label,
                "error": f"missing summary, rc={rc}",
                "log_path": str(log_path),
            }
        payload["returncode"] = int(rc)
        proc_results.append(payload)

    scan_elapsed = time.perf_counter() - scan_started
    shard_elapsed = [
        float(item.get("elapsed_sec", 0.0))
        for item in proc_results
        if "elapsed_sec" in item
    ]
    short_rows = sum(int(item.get("short_signals_rows", 0)) for item in proc_results)
    long_rows = sum(int(item.get("long_signals_rows", 0)) for item in proc_results)
    failed = [item for item in proc_results if int(item.get("returncode", 0)) != 0]

    summary = {
        "slot_ist": slot_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
        "slot_key": slot_key(slot_ist),
        "runtime_root": str(slot_runtime_root),
        "snapshot_total_elapsed_sec": float(snapshot_meta.get("total_elapsed_sec", 0.0)),
        "snapshot_load_elapsed_sec": float(snapshot_meta.get("load_elapsed_sec", 0.0)),
        "snapshot_write_elapsed_sec": float(snapshot_meta.get("write_elapsed_sec", 0.0)),
        "scan_batch_elapsed_sec": round(scan_elapsed, 3),
        "scan_shard_avg_elapsed_sec": round(sum(shard_elapsed) / len(shard_elapsed), 3) if shard_elapsed else 0.0,
        "scan_shard_max_elapsed_sec": round(max(shard_elapsed), 3) if shard_elapsed else 0.0,
        "end_to_end_elapsed_sec": round(float(snapshot_meta.get("total_elapsed_sec", 0.0)) + scan_elapsed, 3),
        "launch_times_sec": launch_times,
        "short_signals_rows": int(short_rows),
        "long_signals_rows": int(long_rows),
        "failed_shards": failed,
        "shard_results": proc_results,
    }
    (slot_runtime_root / "slot_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Benchmark the v15_new combined scan path on selected slots.")
    ap.add_argument("--bench-root", default=str(Path(RUNTIME_ROOT) / "bench_v15_new_slots"))
    ap.add_argument("--snapshot-workers", type=int, default=12)
    ap.add_argument("--scan-workers", type=int, default=1)
    ap.add_argument("--shard-count", type=int, default=10)
    ap.add_argument("--slots", nargs="*", default=DEFAULT_SLOTS)
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    bench_root = Path(args.bench_root)
    bench_root.mkdir(parents=True, exist_ok=True)

    all_summaries = []
    for slot in args.slots:
        print(f"[BENCH] slot={slot}", flush=True)
        summary = _run_slot_benchmark(
            slot,
            bench_root=bench_root,
            snapshot_workers=int(args.snapshot_workers),
            scan_workers=int(args.scan_workers),
            shard_count=int(args.shard_count),
        )
        all_summaries.append(summary)
        print(json.dumps(summary, indent=2, sort_keys=True), flush=True)

    final_summary = {"bench_root": str(bench_root), "slots": all_summaries}
    (bench_root / "summary.json").write_text(
        json.dumps(final_summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(final_summary, indent=2, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
