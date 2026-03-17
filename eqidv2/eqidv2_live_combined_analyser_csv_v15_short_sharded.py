# -*- coding: utf-8 -*-
"""
Sharded wrappers for EQIDV2 LIVE Scanner V15 SHORT.

Purpose:
- keep the existing v15 short scanner untouched
- split the current 15m ticker universe into fixed, permanent shards
- isolate mutable runtime files per shard
- preserve the existing shared live signal CSV behavior
"""

from __future__ import annotations

import math
import os
from pathlib import Path
from typing import Dict, List

import eqidv2_live_combined_analyser_csv_v15_short as base_short
from eqidv2_runtime_paths import report_subdir, runtime_dir


ROOT = Path(__file__).resolve().parent
SHARD_DIR = ROOT / "shards" / "v15_short"


def _env_int(name: str, default: int, min_value: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        val = int(default)
    else:
        try:
            val = int(str(raw).strip())
        except Exception:
            val = int(default)
    return max(int(min_value), val)


def _manifest_path(shard_id: int, shard_count: int) -> Path:
    return SHARD_DIR / f"v15_short_shard_{shard_id:02d}_of_{shard_count:02d}.txt"


def load_v15_short_shard_tickers(shard_id: int, shard_count: int = 10) -> List[str]:
    path = _manifest_path(shard_id, shard_count)
    if not path.exists():
        raise FileNotFoundError(f"Missing shard manifest: {path}")
    tickers: List[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        ticker = str(raw).strip().upper()
        if not ticker or ticker.startswith("#"):
            continue
        tickers.append(ticker)
    if not tickers:
        raise ValueError(f"Shard manifest is empty: {path}")
    return tickers


def verify_v15_short_shards(shard_count: int = 10) -> Dict[str, object]:
    manifests: Dict[int, List[str]] = {}
    all_seen: Dict[str, int] = {}
    overlaps: Dict[str, List[int]] = {}

    for shard_id in range(1, int(shard_count) + 1):
        tickers = load_v15_short_shard_tickers(shard_id, shard_count)
        manifests[shard_id] = tickers
        for ticker in tickers:
            prev = all_seen.get(ticker)
            if prev is not None:
                overlaps.setdefault(ticker, [prev]).append(shard_id)
            else:
                all_seen[ticker] = shard_id

    return {
        "shard_count": int(shard_count),
        "total_unique": len(all_seen),
        "counts": {f"shard_{k:02d}": len(v) for k, v in manifests.items()},
        "overlap_count": len(overlaps),
        "overlaps": overlaps,
    }


def configure_v15_short_shard(shard_id: int, shard_count: int = 10) -> List[str]:
    assigned_tickers = load_v15_short_shard_tickers(shard_id, shard_count)

    base_short._apply_v15_short_overrides()

    suffix = f"s{int(shard_id):02d}_of_{int(shard_count):02d}"
    v2 = base_short.v2

    v2.REPORTS_DIR = report_subdir(f"eqidv2_reports_v15_short_{suffix}")
    v2.REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    v2.OUT_CHECKS_DIR = runtime_dir(f"out_eqidv2_live_checks_15m_v15_short_{suffix}")
    v2.OUT_SIGNALS_DIR = runtime_dir(f"out_eqidv2_live_signals_15m_v15_short_{suffix}")
    v2.OUT_CHECKS_DIR.mkdir(parents=True, exist_ok=True)
    v2.OUT_SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

    v2.STATE_FILE = ROOT / "logs" / f"eqidv2_avwap_live_state_v11_v15_short_{suffix}.json"

    # Keep the same shared final CSV behavior so existing executors remain compatible.
    v2.SIGNAL_CSV_PATTERN = "signals_{}_v15_short.csv"

    # Permanent shard ticker universe.
    v2.list_tickers_15m = lambda: list(assigned_tickers)

    # Scale internal block parallelism for a ~104/105-ticker shard.
    default_workers = min(4, len(assigned_tickers))
    default_block_size = max(1, math.ceil(len(assigned_tickers) / max(1, default_workers)))
    v2.SCAN_MAX_WORKERS = min(
        len(assigned_tickers),
        _env_int("EQIDV15_SHARD_SCAN_MAX_WORKERS", default_workers, min_value=1),
    )
    v2.SCAN_BLOCK_SIZE = min(
        len(assigned_tickers),
        _env_int("EQIDV15_SHARD_SCAN_BLOCK_SIZE", default_block_size, min_value=1),
    )

    return assigned_tickers


def run_v15_short_shard_main(shard_id: int, shard_count: int = 10) -> None:
    assigned_tickers = configure_v15_short_shard(shard_id, shard_count)
    base_short._refresh_v15_nifty_context()
    print(
        f"[V15_SHORT_SHARD] shard={shard_id}/{shard_count} | "
        f"tickers={len(assigned_tickers)} | "
        f"first={assigned_tickers[0]} | last={assigned_tickers[-1]} | "
        f"state={base_short.v2.STATE_FILE.name} | "
        f"block_size={base_short.v2.SCAN_BLOCK_SIZE} | "
        f"max_workers={base_short.v2.SCAN_MAX_WORKERS} | "
        "signal_csv=signals_YYYY-MM-DD_v15_short.csv",
        flush=True,
    )
    base_short.v2.main()
