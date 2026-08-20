"""Isolated signal cache for the FNO V7 high/low-breakout replay.

V6's cached signal table is not a raw five-minute candidate cache: its builder
removes one-minute confirmation candles that fail the V6 colour and
close-displacement rules.  V7 must therefore build and fingerprint a separate
candidate table in which any valid, positive-range confirmation candle may set
the later high/low stop-entry trigger.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_oi_ema_confirm_sweep as sweep
import fno_oi_hybrid_data as hybrid


RESULT_DIR = common.FNO_ROOT / "strategy_research"
CACHE_DIR = (
    RESULT_DIR
    / "_signal_cache_equity_1m_aggregated_5m_futures_oi_v7_high_low_breakout_v1"
)
CACHE_MANIFEST_PATH = CACHE_DIR / "manifest.json"
SOURCE_SNAPSHOT_ROOT = RESULT_DIR / "_source_snapshots_v7_high_low_breakout_v1"
CONFIRMATION_POLICY = sweep.CONFIRMATION_POLICY_V7_BREAKOUT


def _read_cache_manifest() -> dict[str, Any]:
    if not CACHE_MANIFEST_PATH.exists():
        return {}
    try:
        payload = json.loads(CACHE_MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _cache_artifacts_valid(
    manifest: dict[str, Any], sig_path: Path, npz_path: Path
) -> bool:
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        return False
    signal_record = artifacts.get("signals")
    paths_record = artifacts.get("paths")
    if not isinstance(signal_record, dict) or not isinstance(paths_record, dict):
        return False
    return provenance.artifact_matches(
        sig_path, signal_record
    ) and provenance.artifact_matches(npz_path, paths_record)


def _load_cached_signals(
    sig_path: Path, npz_path: Path
) -> tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]]]:
    signals = pd.read_parquet(sig_path)
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    paths: dict[int, dict[str, np.ndarray]] = {}
    with np.load(npz_path) as raw:
        for sid in signals["sid"]:
            key = str(int(sid))
            if f"{key}_h" in raw:
                paths[int(sid)] = {
                    "high": raw[f"{key}_h"],
                    "low": raw[f"{key}_l"],
                    "close": raw[f"{key}_c"],
                }
    return signals, paths


def _atomic_write_npz(path: Path, arrays: dict[str, np.ndarray]) -> None:
    def _write(temp_path: Path) -> None:
        with temp_path.open("wb") as handle:
            np.savez_compressed(handle, **arrays)

    common._atomic_replace_bytes(path, _write)


def load_signals(
    square_off: str,
    max_forward_bars: int,
    rebuild: bool,
    *,
    universe_path: Path | str | None = None,
    universe_date: date | str | None = None,
    require_persisted_mapping: bool = False,
    require_complete_sources: bool = False,
    expected_universe_hashes: dict[str, str] | None = None,
    return_provenance: bool = False,
    freeze_sources: bool = False,
    source_snapshot_path: Path | str | None = None,
    source_snapshot_root: Path | str | None = None,
):
    """Load or build the policy-isolated V7 breakout signal superset.

    Universe pinning, whole-source inventory verification, artifact hashing and
    mid-build source-drift rejection intentionally match V6's cache contract.
    The V7 confirmation policy is an additional cache input, so changing it
    always invalidates the cached artifacts.
    """

    if freeze_sources and source_snapshot_path is not None:
        raise ValueError(
            "Choose either freeze_sources or an existing source_snapshot_path."
        )
    if source_snapshot_root is not None and not freeze_sources:
        raise ValueError("source_snapshot_root requires freeze_sources=True.")

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    sig_path = CACHE_DIR / "signals.parquet"
    npz_path = CACHE_DIR / "paths.npz"
    expected_hashes = expected_universe_hashes or {}
    mapped_universe, universe_record = provenance.load_backtest_universe(
        universe_path=universe_path,
        universe_date=universe_date,
        require_persisted_mapping=require_persisted_mapping,
        expected_file_sha256=expected_hashes.get("file_sha256", ""),
        expected_universe_sha256=expected_hashes.get("universe_sha256", ""),
        expected_mapped_universe_sha256=expected_hashes.get(
            "mapped_universe_sha256", ""
        ),
        expected_mapped_symbol_set_sha256=expected_hashes.get(
            "mapped_symbol_set_sha256", ""
        ),
    )

    source_snapshot: dict[str, Any] | None = None
    if freeze_sources:
        source_snapshot = provenance.create_source_snapshot(
            mapped_universe,
            universe_record,
            universe_path=Path(str(universe_record["path"])),
            snapshot_root=source_snapshot_root or SOURCE_SNAPSHOT_ROOT,
            require_complete_sources=require_complete_sources,
        )
        print(
            f"[SNAPSHOT V7 BREAKOUT] {source_snapshot['manifest_path']}",
            flush=True,
        )
    elif source_snapshot_path is not None:
        source_snapshot = provenance.load_source_snapshot(source_snapshot_path)

    futures_5m_root: Path | None = None
    equity_1m_root: Path | None = None
    snapshot_contract: dict[str, Any] | None = None
    if source_snapshot is not None:
        source_snapshot, source_inventory = provenance.validate_source_snapshot(
            source_snapshot,
            mapped_universe,
            universe_record,
            require_complete_sources=require_complete_sources,
        )
        futures_5m_root = Path(source_snapshot["futures_5m_root"])
        equity_1m_root = Path(source_snapshot["equity_1m_root"])
        snapshot_contract = {
            "schema_version": source_snapshot["schema_version"],
            "manifest_path": source_snapshot["manifest_path"],
            "snapshot_fingerprint": source_snapshot["snapshot_fingerprint"],
            "physical_copy": bool(source_snapshot["physical_copy"]),
            "capture_scope": source_snapshot["capture_scope"],
            "source_inventory_sha256": source_inventory["inventory_sha256"],
        }
    else:
        source_inventory = None

    observed_manifest = _read_cache_manifest()
    previous_inventory = observed_manifest.get("source_inventory")
    if not isinstance(previous_inventory, dict):
        previous_inventory = None
    if source_inventory is None:
        source_inventory = provenance.build_source_inventory(
            mapped_universe,
            universe_record,
            previous_inventory=previous_inventory,
        )
    if require_complete_sources and int(source_inventory["missing_count"]) != 0:
        missing = [
            f"{entry['role']}:{entry['logical_symbol']}"
            for entry in source_inventory["entries"]
            if not entry["exists"]
        ]
        raise FileNotFoundError(
            "V7 requires every mapped source file; missing " f"{missing[:20]}"
        )
    if require_complete_sources:
        provenance.validate_source_inventory_readable(source_inventory)

    input_contract = {
        "schema_version": provenance.CACHE_MANIFEST_SCHEMA_VERSION,
        "hybrid_data_contract": hybrid.cache_manifest_payload(),
        "confirmation_policy": CONFIRMATION_POLICY,
        "forward_path_policy": sweep.FORWARD_PATH_POLICY,
        "source_snapshot": snapshot_contract,
        "square_off": str(square_off),
        "max_forward_bars": int(max_forward_bars),
        "universe": universe_record,
        "source_fingerprint": source_inventory["source_fingerprint"],
        "require_complete_sources": bool(require_complete_sources),
    }
    input_fingerprint = common.canonical_json_sha256(input_contract)
    cache_valid = bool(
        observed_manifest.get("schema_version")
        == provenance.CACHE_MANIFEST_SCHEMA_VERSION
        and observed_manifest.get("input_fingerprint") == input_fingerprint
        and _cache_artifacts_valid(observed_manifest, sig_path, npz_path)
    )
    if cache_valid and not rebuild:
        print("[CACHE V7 BREAKOUT] loading signal table", flush=True)
        signals, paths = _load_cached_signals(sig_path, npz_path)
        # Preserve V6's cheap metadata refresh after a same-content file touch.
        if observed_manifest.get("source_inventory") != source_inventory:
            observed_manifest["source_inventory"] = source_inventory
            observed_manifest["universe"] = universe_record
            observed_manifest["verified_at_ist"] = common.now_ist().isoformat(
                timespec="seconds"
            )
            common.atomic_write_json(CACHE_MANIFEST_PATH, observed_manifest)
        result = (
            (signals, paths, observed_manifest)
            if return_provenance
            else (signals, paths)
        )
        return result

    if not rebuild and (sig_path.exists() or npz_path.exists()) and not cache_valid:
        print(
            "[CACHE V7 BREAKOUT] source, policy, or artifact hash changed; rebuilding",
            flush=True,
        )
    print("[BUILD V7 BREAKOUT] hybrid signal superset...", flush=True)
    signals, paths = sweep.build_signal_table(
        None,
        square_off=square_off,
        max_forward_bars=max_forward_bars,
        mapped_universe=mapped_universe,
        confirmation_policy=CONFIRMATION_POLICY,
        futures_5m_root=futures_5m_root,
        equity_1m_root=equity_1m_root,
    )
    verified_inventory = provenance.build_source_inventory(
        mapped_universe,
        universe_record,
        previous_inventory=source_inventory,
        futures_5m_root=futures_5m_root,
        equity_1m_root=equity_1m_root,
    )
    if (
        verified_inventory["source_fingerprint"]
        != source_inventory["source_fingerprint"]
    ):
        raise RuntimeError(
            "FNO V7 backtest sources changed while the signal cache was being "
            "built; discarding the uncommitted build."
        )

    common.atomic_write_parquet(signals, sig_path)
    flat: dict[str, np.ndarray] = {}
    for sid, path in paths.items():
        flat[f"{sid}_h"] = path["high"]
        flat[f"{sid}_l"] = path["low"]
        flat[f"{sid}_c"] = path["close"]
    _atomic_write_npz(npz_path, flat)
    manifest = {
        "schema_version": provenance.CACHE_MANIFEST_SCHEMA_VERSION,
        "built_at_ist": common.now_ist().isoformat(timespec="seconds"),
        "input_fingerprint": input_fingerprint,
        "input_contract": input_contract,
        "universe": universe_record,
        "source_inventory": verified_inventory,
        "source_snapshot": snapshot_contract,
        "artifacts": {
            "signals": provenance.artifact_record(sig_path),
            "paths": provenance.artifact_record(npz_path),
        },
    }
    common.atomic_write_json(CACHE_MANIFEST_PATH, manifest)
    result = (signals, paths, manifest) if return_provenance else (signals, paths)
    return result
