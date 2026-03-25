from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

import eqidv2_live_combined_analyser_csv_v15_new_persistent as persistent_v15_new
from eqidv2_runtime_paths import RUNTIME_ROOT
from live_v15_slot_delta_bundle_shadow import build_slot_delta_bundle
from live_v15_slot_snapshot_v15_new import normalize_slot_ist, slot_key


DEFAULT_SLOTS = [
    "2026-03-25 10:30:00+0530",
    "2026-03-25 10:45:00+0530",
    "2026-03-25 11:00:00+0530",
    "2026-03-25 11:15:00+0530",
    "2026-03-25 11:30:00+0530",
]


def _normalize_signals(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "ticker",
                "side",
                "setup",
                "bar_time_ist",
                "entry_price",
                "sl_price",
                "target_price",
                "score",
            ]
        )
    out = df.copy()
    cols = [c for c in [
        "ticker",
        "side",
        "setup",
        "bar_time_ist",
        "entry_price",
        "sl_price",
        "target_price",
        "score",
    ] if c in out.columns]
    out = out[cols].copy()
    if "ticker" in out.columns:
        out["ticker"] = out["ticker"].astype(str).str.upper()
    if "side" in out.columns:
        out["side"] = out["side"].astype(str).str.upper()
    if "bar_time_ist" in out.columns:
        out["bar_time_ist"] = pd.to_datetime(out["bar_time_ist"], errors="coerce")
    for col in ("entry_price", "sl_price", "target_price", "score"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(6)
    return out.sort_values(cols).reset_index(drop=True)


def _compare_signals(
    slot_ts: Any,
    runtime_root_current: Path,
    runtime_root_shadow: Path,
    shard_count: int,
) -> Dict[str, Any]:
    short_current, long_current = persistent_v15_new._load_slot_signal_frames(
        slot_ts,
        runtime_root=runtime_root_current,
        shard_count=shard_count,
    )
    short_shadow, long_shadow = persistent_v15_new._load_slot_signal_frames(
        slot_ts,
        runtime_root=runtime_root_shadow,
        shard_count=shard_count,
    )
    short_equal = _normalize_signals(short_current).equals(_normalize_signals(short_shadow))
    long_equal = _normalize_signals(long_current).equals(_normalize_signals(long_shadow))
    return {
        "short_equal": bool(short_equal),
        "long_equal": bool(long_equal),
        "short_rows_current": int(0 if short_current is None else len(short_current)),
        "short_rows_shadow": int(0 if short_shadow is None else len(short_shadow)),
        "long_rows_current": int(0 if long_current is None else len(long_current)),
        "long_rows_shadow": int(0 if long_shadow is None else len(long_shadow)),
    }


def _load_slot_summary(runtime_root: Path, slot_ts: Any) -> Dict[str, Any]:
    path = runtime_root / "live_v15_new_persistent" / f"slot_summary_{slot_key(slot_ts)}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _compare_shard_coverage(summary_current: Dict[str, Any], summary_shadow: Dict[str, Any]) -> Dict[str, Any]:
    def _extract(summary: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for item in list(summary.get("shard_results", []) or []):
            out[str(item.get("shard_id", ""))] = {
                "assigned_tickers": int(item.get("assigned_tickers", 0)),
                "short_checks_rows": int(item.get("short_checks_rows", 0)),
                "long_checks_rows": int(item.get("long_checks_rows", 0)),
                "returncode": int(item.get("returncode", 0)),
            }
        return out
    current = _extract(summary_current)
    shadow = _extract(summary_shadow)
    return {
        "coverage_equal": bool(current == shadow),
        "current": current,
        "shadow": shadow,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Backend-only parity/timing comparison for current vs shadow bundle-backed v15_new scanner.")
    ap.add_argument("--runtime-root", default=str(RUNTIME_ROOT / "eqidv2_v15new_shadow_bundle_parity"))
    ap.add_argument("--slots", nargs="*", default=DEFAULT_SLOTS)
    ap.add_argument("--snapshot-workers", type=int, default=persistent_v15_new.DEFAULT_SNAPSHOT_WORKERS)
    ap.add_argument("--scan-workers", type=int, default=persistent_v15_new.DEFAULT_SCAN_WORKERS)
    ap.add_argument("--shard-count", type=int, default=persistent_v15_new.DEFAULT_SHARD_COUNT)
    args = ap.parse_args()

    runtime_root = Path(args.runtime_root)
    current_root = runtime_root / "current"
    shadow_root = runtime_root / "shadow"
    bundle_root = runtime_root / "bundles"

    for path in (current_root, shadow_root, bundle_root):
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)

    slots = [normalize_slot_ist(slot) for slot in args.slots]
    bundle_meta = []
    for slot_ist in slots:
        bundle_meta.append(
            build_slot_delta_bundle(
                slot_ist,
                runtime_root=bundle_root,
                shard_count=int(args.shard_count),
                max_workers=int(args.snapshot_workers),
            )
        )

    current_summary = persistent_v15_new._run_replay_slots(
        [slot.isoformat() for slot in slots],
        runtime_root=current_root,
        snapshot_workers=int(args.snapshot_workers),
        scan_workers=int(args.scan_workers),
        shard_count=int(args.shard_count),
    )
    shadow_summary = persistent_v15_new._run_replay_slots(
        [slot.isoformat() for slot in slots],
        runtime_root=shadow_root,
        snapshot_workers=int(args.snapshot_workers),
        scan_workers=int(args.scan_workers),
        shard_count=int(args.shard_count),
        slot_bundle_root=bundle_root,
        prefer_slot_bundle=True,
    )

    per_slot = []
    for slot_ist in slots:
        current_slot_summary = _load_slot_summary(current_root, slot_ist)
        shadow_slot_summary = _load_slot_summary(shadow_root, slot_ist)
        signal_compare = _compare_signals(slot_ist, current_root, shadow_root, int(args.shard_count))
        coverage_compare = _compare_shard_coverage(current_slot_summary, shadow_slot_summary)
        per_slot.append(
            {
                "slot_ist": slot_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
                "slot_key": slot_key(slot_ist),
                "current_end_to_end_sec": float(current_slot_summary.get("end_to_end_elapsed_sec", 0.0)),
                "shadow_end_to_end_sec": float(shadow_slot_summary.get("end_to_end_elapsed_sec", 0.0)),
                "current_snapshot_sec": float(current_slot_summary.get("snapshot_total_elapsed_sec", 0.0)),
                "shadow_snapshot_sec": float(shadow_slot_summary.get("snapshot_total_elapsed_sec", 0.0)),
                "current_scan_sec": float(current_slot_summary.get("scan_batch_elapsed_sec", 0.0)),
                "shadow_scan_sec": float(shadow_slot_summary.get("scan_batch_elapsed_sec", 0.0)),
                "signals": signal_compare,
                "coverage": coverage_compare,
            }
        )

    final_summary = {
        "bundle_meta": bundle_meta,
        "current_runtime_root": str(current_root),
        "shadow_runtime_root": str(shadow_root),
        "bundle_runtime_root": str(bundle_root),
        "per_slot": per_slot,
        "all_signal_parity": bool(all(item["signals"]["short_equal"] and item["signals"]["long_equal"] for item in per_slot)),
        "all_coverage_parity": bool(all(item["coverage"]["coverage_equal"] for item in per_slot)),
        "current_replay_summary_path": str(current_root / "live_v15_new_persistent" / "replay_summary_v15_new_persistent.json"),
        "shadow_replay_summary_path": str(shadow_root / "live_v15_new_persistent" / "replay_summary_v15_new_persistent.json"),
    }
    out_path = runtime_root / "shadow_bundle_parity_summary.json"
    out_path.write_text(json.dumps(final_summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(final_summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
