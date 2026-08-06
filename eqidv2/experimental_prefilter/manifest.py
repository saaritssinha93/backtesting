from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from .config import PrefilterConfig
from .io import BarLoadStats, SlotMarker, UniverseManifest, dataclass_dict


IST = "Asia/Kolkata"
SCHEMA_VERSION = "eqidv2_experimental_prefilter_slot_v1"
FEATURE_VERSION = "deterministic_multifamily_v1"


def config_sha256(config: PrefilterConfig) -> str:
    encoded = json.dumps(config.to_dict(), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def candidate_sha256(candidates: pd.DataFrame) -> str:
    if candidates is None or candidates.empty:
        return hashlib.sha256(b"").hexdigest()
    ordered = candidates.sort_values("selection_rank")
    payload = "\n".join(
        f"{row.ticker}|{int(row.selection_rank)}|{row.primary_side}|{row.primary_family}"
        for row in ordered.itertuples()
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_shadow_manifest(
    *,
    slot_marker: SlotMarker,
    universe_manifest: UniverseManifest,
    config: PrefilterConfig,
    candidates: pd.DataFrame,
    full_ranking: pd.DataFrame,
    load_stats: BarLoadStats,
    timing: dict[str, float],
) -> dict[str, Any]:
    candidate_columns = (
        "ticker",
        "selection_rank",
        "selection_bucket",
        "primary_side",
        "primary_family",
        "selection_reason",
        "overall_score",
        "long_score",
        "short_score",
        "activity_score",
        "date",
        "staleness_seconds",
    )
    records: list[dict[str, Any]] = []
    for row in candidates.loc[:, [column for column in candidate_columns if column in candidates.columns]].to_dict("records"):
        clean: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, pd.Timestamp):
                clean[key] = value.isoformat()
            elif pd.isna(value):
                clean[key] = None
            elif hasattr(value, "item"):
                clean[key] = value.item()
            else:
                clean[key] = value
        records.append(clean)
    eligible_count = int(full_ranking["eligible"].fillna(False).sum()) if not full_ranking.empty else 0
    state = "OK"
    if load_stats.missing_files or load_stats.read_errors or eligible_count < len(universe_manifest.symbols):
        state = "DEGRADED"
    return {
        "schema_version": SCHEMA_VERSION,
        "mode": "SHADOW_RESEARCH_ONLY",
        "production_consumption_allowed": False,
        "created_at_ist": datetime.now(ZoneInfo(IST)).isoformat(),
        "slot_ist": universe_manifest.slot_ist,
        "state": state,
        "complete": True,
        "source": {
            "final_slot_marker_path": slot_marker.path,
            "final_slot_marker_sha256": slot_marker.sha256,
            "feed_published_at_ist": slot_marker.published_at_ist,
            "universe_manifest_path": universe_manifest.path,
            "universe_schema_version": universe_manifest.schema_version,
            "universe_count": len(universe_manifest.symbols),
            "universe_sha256": universe_manifest.universe_sha256,
        },
        "config": config.to_dict(),
        "config_sha256": config_sha256(config),
        "feature_version": FEATURE_VERSION,
        "model_version": None,
        "model_used": False,
        "statistics": {
            "scored_count": int(len(full_ranking)),
            "eligible_count": eligible_count,
            "selected_count": int(len(candidates)),
            "candidate_budget": int(config.budget),
            "candidate_sha256": candidate_sha256(candidates),
            "bar_load": dataclass_dict(load_stats),
            "timing_seconds": {key: float(value) for key, value in timing.items()},
        },
        "candidates": records,
    }
