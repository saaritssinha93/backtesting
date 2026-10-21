"""Reconstruct newly enabled G3 opening SHORT and 10:10 LONG selections.

Selections come from pre-ranking market features, including stocks never
selected by the old contract. Saved trade outcomes are not selection inputs.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import RUNTIME_ROOT
import fno_v13_v10_g_3_contract as production
from research import g3_0920_backtest as opening_inputs
from research.g3_0920_parameter_search import DEFAULT_FEATURES
from research.g3_0920_strategy import REQUIRED_COLUMNS, _candidate
from research.g3_0950_long_backtest import normalize_signals
from research.g4_extended_target_sweep import read_frozen_csv
from research.g4_late_slot_source import load_late_source, sha256


SETUP_IDS = frozenset({"0921_SHORT", "1011_LONG"})
SID_START = 98_000_000


def opening_candidates(features: pd.DataFrame, days: list[date]) -> pd.DataFrame:
    """Adapt the complete sealed opening universe without research thresholds."""
    missing = REQUIRED_COLUMNS - set(features.columns)
    if missing:
        raise ValueError(f"Opening source lacks required fields: {sorted(missing)}")
    work = features.loc[features.session_date.astype(str).isin({str(day) for day in days})].copy()
    if set(work.session_date.astype(str)) != {str(day) for day in days}:
        raise ValueError("Opening source does not cover every historical session")
    if work.duplicated(["session_date", "symbol"]).any():
        raise ValueError("Duplicate stock in opening source universe")
    rows = []
    for index, raw in enumerate(work.sort_values(["session_date", "symbol"]).to_dict("records")):
        day = date.fromisoformat(str(raw["session_date"]))
        row = _candidate(raw, day, "SHORT", index)
        # The archived adapter also reports the NIFTY strategy gate. Keep that
        # separate from source availability; current G3 evaluates the gate.
        reasons = [reason for reason in row["source_reasons"].split(";")
                   if reason and reason != "NIFTY_FIRST_BAR_SHORT_GATE"]
        row.update(opening_0920_status="INVALID_SOURCE" if reasons else "VALID",
                   source_reasons=";".join(reasons),
                   v9_feature_available_ts=row["confirmation_ts"],
                   new_slot_source="SEALED_OPENING_UNIVERSE")
        rows.append(row)
    return pd.DataFrame(rows)


def _slot_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    frame = normalize_signals(frame)
    mask = ((frame.signal_end.eq("09:20") & frame.side.eq("SHORT"))
            | (frame.signal_end.eq("10:10") & frame.side.eq("LONG")))
    frame = frame.loc[mask].copy()
    # Fill aliases before joining with the opening export: otherwise pandas
    # introduces null canonical columns that hide valid OHLC/timestamp aliases.
    for canonical, alias in (("signal_timestamp", "signal_ts"),
            ("confirmation_timestamp", "confirmation_ts"), ("confirm_open", "confirmation_open"),
            ("confirm_high", "confirmation_high"), ("confirm_low", "confirmation_low"),
            ("confirm_close", "confirmation_close")):
        frame[canonical] = frame[canonical].fillna(frame[alias]) if canonical in frame else frame[alias]
    trigger = frame.confirm_high.where(frame.side.eq("LONG"), frame.confirm_low)
    frame["trigger"] = frame.trigger.fillna(trigger) if "trigger" in frame else trigger
    return frame


def _opening_coverage(pool: pd.DataFrame, days: list[date]) -> dict:
    opening = pool.loc[pool.signal_end.eq("09:20")]
    per_day = []
    for day in days:
        frame = opening.loc[opening.day.astype(str).eq(str(day))]
        invalid = frame.loc[~frame.opening_0920_status.eq("VALID")]
        reasons = invalid.source_reasons.fillna("") if "source_reasons" in invalid else pd.Series(dtype=str)
        per_day.append(dict(day=str(day), candidate_stock_rows=len(frame),
            source_valid_stock_rows=int(frame.opening_0920_status.eq("VALID").sum()),
            excluded_stock_rows=len(invalid),
            excluded_reason_counts={str(key): int(value) for key, value in reasons.value_counts().items()}))
    return dict(availability="RETROSPECTIVE_RESEARCH; HISTORICAL_LIVE_AVAILABILITY_UNPROVEN",
        scope="COMPLETE_DATED_OPENING_UNIVERSE_FOR_FIRST_49_SESSIONS",
        per_day=per_day,
        sessions_without_valid_opening_source=[item["day"] for item in per_day if not item["source_valid_stock_rows"]],
        excluded_stock_rows=sum(item["excluded_stock_rows"] for item in per_day))


def reselect(original_orders: pd.DataFrame, days: list[date], members: list):
    """Return fresh orders, uniquely keyed minute paths, provenance and audit."""
    if not SETUP_IDS.issubset(production._ACTIVE_BY_ID):
        raise ValueError("Both new G3 setups must be enabled before reconstructing history")
    if original_orders.setup_id.isin(SETUP_IDS).any():
        raise ValueError("Pinned historical baseline already contains a new slot")
    candidates, late_paths, historical_days, late_proof, _ = load_late_source(verify_snapshots=True)
    if historical_days != days[:49]:
        raise ValueError("New-slot candidate calendar differs from pinned baseline")
    late = _slot_candidates(candidates)
    late = late.loc[late.signal_end.eq("10:10")].copy()
    late["new_slot_source"] = "SEALED_LATE_CANDIDATES"

    features, coverage, feature_proof = opening_inputs.load_feature_evidence(DEFAULT_FEATURES)
    universe_proof = opening_inputs.validate_feature_universe(features, Path(RUNTIME_ROOT), coverage)
    opening = opening_candidates(features, historical_days)
    opening_coverage = _opening_coverage(opening, historical_days)
    parts, member_map, member_proofs = [opening, late], {}, []
    for directory, payload, _ in members:
        directory = Path(directory)
        record = payload["artifacts"]["candidate_signals"]
        path = directory / record["path"]
        if sha256(path) != record["sha256"]:
            raise ValueError("New-slot daily candidate source changed")
        frame = _slot_candidates(read_frozen_csv(path))
        day = date.fromisoformat(payload["session_date"])
        if set(pd.to_datetime(frame.day).dt.date) - {day}:
            raise ValueError("New-slot daily candidates escaped their session")
        frame["new_slot_source"] = "PINNED_DAILY_CANDIDATES"
        parts.append(frame)
        member_map[day] = (directory, payload)
        member_proofs.append(dict(day=str(day), candidate_signals=str(path), sha256=sha256(path),
            candidate_rows=len(frame), source_scope="PRE_RANKING_BASE_FILTERED_CANDIDATES"))
    pool = pd.concat(parts, ignore_index=True, sort=False)
    pool["day"] = pd.to_datetime(pool.day).dt.date
    if pool.duplicated(["day", "signal_end", "side", "tradingsymbol"]).any():
        raise ValueError("Duplicate new-slot candidate in pre-ranking universe")
    pool["input_sid"] = pool.sid
    pool["sid"] = np.arange(SID_START, SID_START + len(pool))
    if set(pool.sid) & set(original_orders.sid):
        raise ValueError("New-slot execution path identifiers overlap the pinned baseline")
    audit = production.selection_audit(pool)
    if len(audit) != len(pool) or set(audit.setup_id) - SETUP_IDS:
        raise ValueError("Current contract dropped or misidentified a new-slot candidate")
    selected = audit.loc[audit.v9_selected].copy()
    selected["source_signal_id"] = selected.signal_id

    historical_opening = selected.loc[selected.new_slot_source.eq("SEALED_OPENING_UNIVERSE")]
    paths, opening_path_proof = opening_inputs.new_execution_paths(historical_opening, Path(RUNTIME_ROOT))
    for row in selected.loc[selected.new_slot_source.eq("SEALED_LATE_CANDIDATES")].itertuples():
        if int(row.input_sid) not in late_paths:
            raise ValueError("New 10:10 LONG selection has no verified minute path")
        paths[int(row.sid)] = late_paths[int(row.input_sid)]
    daily_path_proof = {}
    from research.g3_target_cutoff_backtest import _daily_paths
    for day, (directory, payload) in member_map.items():
        daily = selected.loc[selected.day.eq(day)].copy()
        if daily.empty:
            continue
        group, proof = _daily_paths(directory, payload, daily)
        if set(group) & set(paths):
            raise ValueError("New-slot daily and historical execution paths overlap")
        paths.update(group)
        daily_path_proof[str(day)] = proof
    if set(paths) != set(selected.sid.astype(int)):
        raise ValueError("New-slot selected execution paths are incomplete")
    proof = dict(setup_ids=sorted(SETUP_IDS), selection_contract=production.STRATEGY_VERSION,
        source_kind="PRE_RANKING_MARKET_FEATURES_WITH_FRESH_MINUTE_EXECUTION",
        outcomes_used_for_selection=False, historical_days=[str(day) for day in historical_days],
        candidate_rows=len(pool), selected_orders=len(selected),
        selected_by_setup={setup_id: int(selected.setup_id.eq(setup_id).sum()) for setup_id in sorted(SETUP_IDS)},
        late_candidates=late_proof, opening_features=feature_proof,
        opening_universe=universe_proof, opening_coverage=opening_coverage,
        opening_execution_paths=opening_path_proof, daily_candidate_sources=member_proofs,
        daily_execution_paths=daily_path_proof)
    return selected, paths, proof, audit
