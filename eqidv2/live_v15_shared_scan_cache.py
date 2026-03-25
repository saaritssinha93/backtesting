from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from eqidv2_runtime_paths import runtime_dir


BASE_DIR = runtime_dir("live_v15_shared_scan")
BASE_DIR.mkdir(parents=True, exist_ok=True)


def _normalize_slot(slot_ts: Any) -> pd.Timestamp:
    ts = pd.Timestamp(slot_ts)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("Asia/Kolkata")
    return ts.floor("min")


def _slot_key(slot_ts: Any) -> str:
    return _normalize_slot(slot_ts).strftime("%Y%m%d_%H%M")


def _bundle_dir(shard_id: str, slot_ts: Any, run_tag: str) -> Path:
    shard = str(shard_id or "00").strip() or "00"
    tag = str(run_tag or "A").strip().upper() or "A"
    return BASE_DIR / f"shard_{shard}" / _slot_key(slot_ts) / tag


def bundle_paths(shard_id: str, slot_ts: Any, run_tag: str) -> Tuple[Path, Path, Path]:
    bundle_dir = _bundle_dir(shard_id, slot_ts, run_tag)
    return (
        bundle_dir / "checks.parquet",
        bundle_dir / "signals.parquet",
        bundle_dir / "meta.json",
    )


def write_bundle(
    shard_id: str,
    slot_ts: Any,
    run_tag: str,
    checks_df: pd.DataFrame,
    signals_df: pd.DataFrame,
    producer: str,
) -> Tuple[Path, Path, Path]:
    checks_path, signals_path, meta_path = bundle_paths(shard_id, slot_ts, run_tag)
    checks_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_suffix = f".tmp_{int(time.time() * 1000)}"
    checks_tmp = checks_path.with_name(checks_path.name + tmp_suffix)
    signals_tmp = signals_path.with_name(signals_path.name + tmp_suffix)
    meta_tmp = meta_path.with_name(meta_path.name + tmp_suffix)

    (checks_df if checks_df is not None else pd.DataFrame()).to_parquet(checks_tmp, index=False)
    (signals_df if signals_df is not None else pd.DataFrame()).to_parquet(signals_tmp, index=False)
    meta_tmp.write_text(
        json.dumps(
            {
                "shard_id": str(shard_id or ""),
                "slot": _slot_key(slot_ts),
                "run_tag": str(run_tag or "").upper(),
                "producer": str(producer or ""),
                "checks_rows": 0 if checks_df is None else int(len(checks_df)),
                "signals_rows": 0 if signals_df is None else int(len(signals_df)),
                "written_at": pd.Timestamp.utcnow().isoformat(),
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    checks_tmp.replace(checks_path)
    signals_tmp.replace(signals_path)
    meta_tmp.replace(meta_path)
    return checks_path, signals_path, meta_path


def read_bundle(
    shard_id: str,
    slot_ts: Any,
    run_tag: str,
) -> Optional[Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]]:
    checks_path, signals_path, meta_path = bundle_paths(shard_id, slot_ts, run_tag)
    if not (checks_path.exists() and signals_path.exists() and meta_path.exists()):
        return None
    try:
        checks_df = pd.read_parquet(checks_path)
        signals_df = pd.read_parquet(signals_path)
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return checks_df, signals_df, meta
    except Exception:
        return None


def wait_for_bundle(
    shard_id: str,
    slot_ts: Any,
    run_tag: str,
    timeout_sec: float,
    poll_sec: float = 0.5,
) -> Optional[Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]]:
    deadline = time.time() + max(0.0, float(timeout_sec))
    while True:
        bundle = read_bundle(shard_id, slot_ts, run_tag)
        if bundle is not None:
            return bundle
        if time.time() >= deadline:
            return None
        time.sleep(max(0.05, float(poll_sec)))
