"""Candidate-level parity decision funnel writer."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd


FUNNEL_SCHEMA_VERSION = "eqidv2_candidate_decision_funnel_v1"
_REASON_COLUMNS = (
    "reject_reason",
    "research_live_filter_reason",
    "v11_live_overlay_reject_reason",
    "v11_live_overlay_reason",
    "pre_momentum_gate_reason",
    "reason",
)
_DETAIL_COLUMNS = (
    "ticker", "side", "setup", "signal_time_ist", "quality_score",
    "ranker_score", "v8_live_gate_status", "v8_live_gate_rule",
    "v11_live_overlay_status", "v11_selected_strategy_rule",
    "research_live_filter_status", "research_live_filter_reason",
    "pre_momentum_gate_rule", "pre_momentum_gate_reason",
)


def _candidate_id(row: Mapping[str, Any]) -> str:
    existing = str(row.get("candidate_id", "") or "").strip()
    if existing:
        return existing
    raw = "|".join(
        str(row.get(key, "") or "").strip()
        for key in ("ticker", "side", "setup", "signal_time_ist")
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def records_for_stage(
    frame: pd.DataFrame | None,
    *,
    stage: str,
    outcome: str,
    recorded_at_ist: str,
    default_reason: str = "",
) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    records: list[dict[str, Any]] = []
    for _, series in frame.iterrows():
        row = series.to_dict()
        reason = default_reason
        for col in _REASON_COLUMNS:
            value = str(row.get(col, "") or "").strip()
            if value and value.lower() != "nan":
                reason = value
                break
        details = {
            key: row.get(key)
            for key in _DETAIL_COLUMNS
            if key in row
        }
        records.append(
            {
                "schema_version": FUNNEL_SCHEMA_VERSION,
                "recorded_at_ist": recorded_at_ist,
                "candidate_id": _candidate_id(row),
                "stage": str(stage),
                "outcome": str(outcome).upper(),
                "reason": reason,
                **details,
                "details_json": json.dumps(details, sort_keys=True, default=str),
            }
        )
    return records


def difference_frame(
    source: pd.DataFrame | None,
    accepted: pd.DataFrame | None,
) -> pd.DataFrame:
    if source is None or source.empty:
        return pd.DataFrame()
    src = source.copy()
    if accepted is None or accepted.empty:
        return src
    accepted_ids = set(accepted.get("candidate_id", pd.Series(dtype=str)).astype(str))
    if "candidate_id" not in src.columns:
        return pd.DataFrame()
    return src.loc[~src["candidate_id"].astype(str).isin(accepted_ids)].copy()


def write_slot_funnel(
    *,
    records: Iterable[Mapping[str, Any]],
    csv_path: Path,
    jsonl_path: Path,
) -> int:
    rows = [dict(row) for row in records]
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows)
    tmp = csv_path.with_suffix(csv_path.suffix + f".{os.getpid()}.tmp")
    try:
        frame.to_csv(tmp, index=False)
        os.replace(tmp, csv_path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass
    tmp_jsonl = jsonl_path.with_suffix(jsonl_path.suffix + f".{os.getpid()}.tmp")
    try:
        with tmp_jsonl.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")
        os.replace(tmp_jsonl, jsonl_path)
    finally:
        try:
            if tmp_jsonl.exists():
                tmp_jsonl.unlink()
        except OSError:
            pass
    return len(rows)
