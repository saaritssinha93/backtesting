from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from .io import load_slot_marker


@dataclass(frozen=True)
class SlotLatency:
    slot_ist: str
    feed_published_at_ist: str
    decision_ready_at_ist: str | None
    feed_publish_lag_seconds: float
    decision_lag_seconds: float | None
    post_feed_scan_seconds: float | None
    feed_duration_ms: float | None
    candidate_count: int | None


def _ts(value: object) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("Asia/Kolkata")
    return timestamp.tz_convert("Asia/Kolkata")


def profile_slot(
    feed_marker_path: str | Path,
    scanner_marker_path: str | Path | None = None,
) -> SlotLatency:
    feed = load_slot_marker(feed_marker_path)
    slot = _ts(feed.slot_ist)
    feed_published = _ts(feed.published_at_ist)
    decision_ready: pd.Timestamp | None = None
    candidate_count: int | None = None
    if scanner_marker_path is not None:
        with Path(scanner_marker_path).open("r", encoding="utf-8") as handle:
            scanner = json.load(handle)
        if not bool(scanner.get("complete", False)):
            raise ValueError("scanner marker is incomplete")
        scanner_slot = _ts(scanner.get("slot_ist"))
        if scanner_slot != slot:
            raise ValueError(
                f"scanner/feed slot mismatch: scanner={scanner_slot} feed={slot}"
            )
        decision_ready = _ts(
            scanner.get("decision_ready_at_ist") or scanner.get("published_at_ist")
        )
        candidate_count = int(scanner.get("candidate_count", 0))
    return SlotLatency(
        slot_ist=slot.isoformat(),
        feed_published_at_ist=feed_published.isoformat(),
        decision_ready_at_ist=(decision_ready.isoformat() if decision_ready is not None else None),
        feed_publish_lag_seconds=float((feed_published - slot).total_seconds()),
        decision_lag_seconds=(
            float((decision_ready - slot).total_seconds()) if decision_ready is not None else None
        ),
        post_feed_scan_seconds=(
            float((decision_ready - feed_published).total_seconds())
            if decision_ready is not None
            else None
        ),
        feed_duration_ms=feed.duration_ms,
        candidate_count=candidate_count,
    )


def _summary(values: Iterable[float | None]) -> dict[str, float | int | None]:
    clean = np.asarray([float(value) for value in values if value is not None and np.isfinite(value)], dtype=float)
    if clean.size == 0:
        return {"count": 0, "p50": None, "p95": None, "max": None, "mean": None}
    return {
        "count": int(clean.size),
        "p50": float(np.percentile(clean, 50)),
        "p95": float(np.percentile(clean, 95)),
        "max": float(clean.max()),
        "mean": float(clean.mean()),
    }


def summarize_latencies(rows: Iterable[SlotLatency]) -> dict[str, Any]:
    records = list(rows)
    return {
        "matched_slots": len(records),
        "feed_publish_lag_seconds": _summary(row.feed_publish_lag_seconds for row in records),
        "decision_lag_seconds": _summary(row.decision_lag_seconds for row in records),
        "post_feed_scan_seconds": _summary(row.post_feed_scan_seconds for row in records),
        "feed_duration_ms": _summary(row.feed_duration_ms for row in records),
    }


def profile_archives(
    feed_marker_dir: str | Path,
    scanner_marker_dir: str | Path,
    *,
    date_prefix: str | None = None,
) -> tuple[list[SlotLatency], dict[str, Any]]:
    """Match archived final feed and scanner markers by slot timestamp."""

    feed_dir = Path(feed_marker_dir)
    scanner_dir = Path(scanner_marker_dir)
    scanner_by_key = {
        path.stem.replace("slot_complete_", ""): path
        for path in scanner_dir.glob("slot_complete_*.json")
    }
    rows: list[SlotLatency] = []
    for feed_path in sorted(feed_dir.glob("slot_*.json")):
        key = feed_path.stem.replace("slot_", "")
        if date_prefix and not key.startswith(date_prefix.replace("-", "")):
            continue
        scanner_path = scanner_by_key.get(key)
        if scanner_path is None:
            continue
        try:
            rows.append(profile_slot(feed_path, scanner_path))
        except (ValueError, KeyError, TypeError, json.JSONDecodeError):
            continue
    return rows, summarize_latencies(rows)
