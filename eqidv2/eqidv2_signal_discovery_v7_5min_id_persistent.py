"""
Persistent session runner for "Signal discovery v7 5mins ID".

This is not a trade signal writer. It scans the completed 5-minute signal
candle and writes candidate tickers only, in CSV and JSON. Entry candle and
entry price are intentionally absent.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

import avwap_5min_ID_v7_candidate_scan as candidate_scan
import eqidv2_live_combined_analyser_csv_v15 as base_v15
from eqidv2_runtime_paths import RUNTIME_ROOT, RUNTIME_STATUS_DIR, runtime_dir


SESSION_NAME = "Signal discovery v7 5mins ID"
SESSION_SLUG = "signal_discovery_v7_5mins_ID"
SESSION_ROOT = runtime_dir("signal_discovery_v7_5mins_ID")
CSV_DIR = SESSION_ROOT / "csv"
JSON_DIR = SESSION_ROOT / "json"
LATEST_DIR = SESSION_ROOT / "latest"
AUDIT_DIR = SESSION_ROOT / "audit"
HEARTBEAT_DIR = SESSION_ROOT / "heartbeat"

SLOT_MINUTES = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_SLOT_MINUTES", "5"))
POST_SLOT_DELAY_SEC = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_POST_SLOT_DELAY_SEC", "15"))
DEFAULT_SCAN_WORKERS = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_SCAN_WORKERS", "8"))
V8_LIVE_GATE_ENABLE = str(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_V8_GATE", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
    "disabled",
}
V8_ACCEPTED_RULES_CSV = Path(
    os.getenv(
        "EQIDV2_SIGNAL_DISCOVERY_V7_V8_ACCEPTED_RULES",
        r"C:\TradingData\eqidv2\outputs_ID_v8_5min_research_restore\accepted_rules.csv",
    )
)
RESEARCH_LIVE_FILTER_ENABLE = str(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_RESEARCH_FILTERS", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
    "disabled",
}
RESEARCH_LIVE_FILTER_MODE = os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_RESEARCH_FILTER_MODE", "active").strip().lower()
LONG_ANTI_CHASE_CLOSE_LOC_GT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_LONG_ANTI_CHASE_CLOSE_LOC_GT", "0.88"))
LONG_ANTI_CHASE_VWAP_DIST_ATR_GT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_LONG_ANTI_CHASE_VWAP_DIST_ATR_GT", "0.52"))
B_AVWAP_RECLAIM_RANKER_MIN = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_B_AVWAP_RECLAIM_RANKER_MIN", "0.65"))
L_TREND_PULLBACK_PROBATION_BLOCK = str(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_L_TREND_PULLBACK_PROBATION_BLOCK", "1")
).strip().lower() not in {"0", "false", "no", "off", "disabled"}
EARLY_LIVE_GATE_MIN_SCORE = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_LIVE_GATE_MIN_SCORE", "95"))
EARLY_LIVE_GATE_MAX_PER_SIDE = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_LIVE_GATE_MAX_PER_SIDE", "4"))
EARLY_LIVE_GATE_MAX_PER_SLOT = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_LIVE_GATE_MAX_PER_SLOT", "8"))
RESEARCH_FILTER_VERSION = "v7_live_research_2026_05_29"
RESEARCH_TRUTH_DIR = runtime_dir("live_research_v7_research_layer", "truth_table")
START_TIME = base_v15.dtime(9, 15)
END_TIME = base_v15.dtime(15, 0)
HARD_STOP_TIME = base_v15.dtime(15, 30)

LIVE_SAFE_RULE_FIELDS = {
    "_signal_hour",
    "atr_pct",
    "body_pct",
    "close_loc",
    "day_value_so_far_rs",
    "market_ret_pct",
    "quality_score",
    "rs_pct",
    "signal_close",
    "signal_high",
    "signal_low",
    "signal_open",
    "signal_volume",
    "vol_ratio",
    "vwap_dist_atr",
}
SIMPLE_RULE_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(<=|>=|<|>|==)\s*(-?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?)\s*$",
    re.I,
)
RULE_AND_RE = re.compile(r"\s+AND\s+", re.I)


for _p in (SESSION_ROOT, CSV_DIR, JSON_DIR, LATEST_DIR, AUDIT_DIR, HEARTBEAT_DIR):
    _p.mkdir(parents=True, exist_ok=True)


def _set_status_env() -> None:
    os.environ["EQIDV2_RUNTIME_STATUS_FILE"] = str(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.status")
    os.environ["EQIDV2_RUNTIME_HEARTBEAT_FILE"] = str(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.heartbeat")


def _touch_status(status: str, **extra: Any) -> None:
    _set_status_env()
    payload = {"session": SESSION_NAME, **extra}
    base_v15._touch_runtime_status(status, **payload)
    status_path = HEARTBEAT_DIR / "candidate_tickers.status.json"
    status_payload = {
        "status": status,
        "session": SESSION_NAME,
        "updated_at_ist": base_v15.now_ist().strftime("%Y-%m-%d %H:%M:%S%z"),
        **extra,
    }
    status_path.write_text(json.dumps(status_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def _touch_heartbeat(status: str = "RUNNING", **extra: Any) -> None:
    _set_status_env()
    base_v15._touch_runtime_heartbeat(status, session=SESSION_NAME, **extra)
    hb_path = HEARTBEAT_DIR / "candidate_tickers.heartbeat.json"
    hb_path.write_text(
        json.dumps(
            {
                "status": status,
                "session": SESSION_NAME,
                "updated_at_ist": base_v15.now_ist().strftime("%Y-%m-%d %H:%M:%S%z"),
                **extra,
            },
            indent=2,
            sort_keys=True,
            default=str,
        ),
        encoding="utf-8",
    )


def _fmt_slot(slot: pd.Timestamp) -> str:
    offset = slot.strftime("%z")
    return f"{slot.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _slot_key(slot: pd.Timestamp) -> str:
    return slot.strftime("%Y%m%d_%H%M")


def _ensure_ist_ts(ts: Any) -> pd.Timestamp:
    out = pd.Timestamp(ts)
    if out.tz is None:
        out = out.tz_localize(base_v15.IST)
    else:
        out = out.tz_convert(base_v15.IST)
    return out


def _next_slot_after(now: datetime) -> datetime:
    now = now.astimezone(base_v15.IST)
    today = now.date()
    start_dt = base_v15.IST.localize(datetime.combine(today, START_TIME))
    end_dt = base_v15.IST.localize(datetime.combine(today, END_TIME))

    if now <= start_dt:
        return start_dt
    if now > end_dt:
        tomorrow = today + timedelta(days=1)
        return base_v15.IST.localize(datetime.combine(tomorrow, START_TIME))

    minute = (now.minute // SLOT_MINUTES) * SLOT_MINUTES
    slot = now.replace(minute=minute, second=0, microsecond=0)
    if slot < now:
        slot += timedelta(minutes=SLOT_MINUTES)
    if slot < start_dt:
        slot = start_dt
    if slot > end_dt:
        tomorrow = today + timedelta(days=1)
        slot = base_v15.IST.localize(datetime.combine(tomorrow, START_TIME))
    return slot


def _load_universe() -> List[str]:
    try:
        universe = candidate_scan.v2._load_universe()
    except Exception as exc:
        print(f"[{SESSION_NAME}] universe load failed: {type(exc).__name__}: {exc}", flush=True)
        universe = []
    return sorted({str(t).strip().upper() for t in universe if str(t).strip()})


def _daily_csv_path(day: str) -> Path:
    return CSV_DIR / f"candidate_tickers_{day}.csv"


def _daily_raw_csv_path(day: str) -> Path:
    return CSV_DIR / f"raw_candidate_tickers_{day}.csv"


def _daily_research_filter_rejected_csv_path(day: str) -> Path:
    return CSV_DIR / f"research_filter_rejected_candidate_tickers_{day}.csv"


def _slot_json_path(slot: pd.Timestamp) -> Path:
    return JSON_DIR / f"candidate_tickers_{_slot_key(slot)}.json"


def _raw_slot_json_path(slot: pd.Timestamp) -> Path:
    return JSON_DIR / f"raw_candidate_tickers_{_slot_key(slot)}.json"


def _research_filter_rejected_slot_json_path(slot: pd.Timestamp) -> Path:
    return JSON_DIR / f"research_filter_rejected_candidate_tickers_{_slot_key(slot)}.json"


def _daily_audit_path(day: str) -> Path:
    return AUDIT_DIR / f"candidate_tickers_audit_{day}.csv"


def _load_existing_ids(path: Path) -> set[str]:
    if not path.exists() or path.stat().st_size <= 0:
        return set()
    try:
        df = pd.read_csv(path, usecols=["candidate_id"])
    except Exception:
        return set()
    return set(df["candidate_id"].dropna().astype(str))


def _load_existing_tickers(path: Path) -> set[str]:
    if not path.exists() or path.stat().st_size <= 0:
        return set()
    try:
        df = pd.read_csv(path, usecols=["ticker"])
    except Exception:
        return set()
    return set(df["ticker"].dropna().astype(str).str.upper().str.strip())


def _append_daily_candidates(df: pd.DataFrame, day: str) -> Dict[str, int]:
    path = _daily_csv_path(day)
    return _append_candidates_to_path(df, path)


def _append_daily_raw_candidates(df: pd.DataFrame, day: str) -> Dict[str, int]:
    path = _daily_raw_csv_path(day)
    return _append_candidates_to_path(df, path)


def _append_daily_research_filter_rejections(df: pd.DataFrame, day: str) -> Dict[str, int]:
    path = _daily_research_filter_rejected_csv_path(day)
    return _append_candidates_to_path(df, path)


def _append_candidates_to_path(df: pd.DataFrame, path: Path) -> Dict[str, int]:
    if df is None or df.empty:
        if not path.exists():
            df.to_csv(path, index=False)
        return {"written": 0, "duplicates": 0}

    if path.exists() and path.stat().st_size > 0:
        try:
            existing_header = list(pd.read_csv(path, nrows=0).columns)
        except Exception:
            existing_header = []
        incoming_header = list(df.columns)
        if existing_header and existing_header != incoming_header:
            backup = path.with_name(f"{path.stem}_schema_backup_{base_v15.now_ist().strftime('%Y%m%d_%H%M%S')}{path.suffix}")
            try:
                path.replace(backup)
                print(f"[{SESSION_NAME}] archived schema-mismatched CSV: {backup}", flush=True)
            except OSError as exc:
                print(f"[{SESSION_NAME}] schema backup failed for {path}: {exc}", flush=True)

    existing = _load_existing_ids(path)
    existing_tickers = _load_existing_tickers(path)
    rows = []
    duplicates = 0
    run_tickers: set[str] = set()
    for _, row in df.iterrows():
        cid = str(row.get("candidate_id", ""))
        ticker = str(row.get("ticker", "")).upper().strip()
        if not cid or not ticker or cid in existing or ticker in existing_tickers or ticker in run_tickers:
            duplicates += 1
            continue
        rows.append(row.to_dict())
        existing.add(cid)
        run_tickers.add(ticker)

    write_df = pd.DataFrame(rows)
    file_exists = path.exists() and path.stat().st_size > 0
    if not write_df.empty:
        write_df.to_csv(path, mode="a", index=False, header=not file_exists)
    elif not file_exists:
        df.head(0).to_csv(path, index=False)
    return {"written": int(len(write_df)), "duplicates": int(duplicates)}


def _write_json_snapshots(
    df: pd.DataFrame,
    slot: pd.Timestamp,
    *,
    slot_json_path: Optional[Path] = None,
    latest_json_name: str = "latest_candidate_tickers.json",
    latest_csv_name: str = "latest_candidate_tickers.csv",
    payload_extra: Optional[Dict[str, Any]] = None,
) -> None:
    rows = [] if df is None or df.empty else df.to_dict("records")
    side_counts = {"LONG": 0, "SHORT": 0}
    setup_counts: Dict[str, int] = {}
    for row in rows:
        side = str(row.get("side", "")).upper()
        setup = str(row.get("setup", ""))
        side_counts[side] = side_counts.get(side, 0) + 1
        setup_counts[setup] = setup_counts.get(setup, 0) + 1

    payload = {
        "session": SESSION_NAME,
        "slot_ist": _fmt_slot(slot),
        "created_at_ist": _fmt_slot(pd.Timestamp.now(tz=base_v15.IST)),
        "total_candidates": int(len(rows)),
        "long_candidates": int(side_counts.get("LONG", 0)),
        "short_candidates": int(side_counts.get("SHORT", 0)),
        "setup_counts": setup_counts,
        "candidates": rows,
        **(payload_extra or {}),
    }
    text = json.dumps(payload, indent=2, sort_keys=True, default=str)
    (slot_json_path or _slot_json_path(slot)).write_text(text, encoding="utf-8")
    (LATEST_DIR / latest_json_name).write_text(text, encoding="utf-8")
    latest_csv = LATEST_DIR / latest_csv_name
    if df is None:
        pd.DataFrame().to_csv(latest_csv, index=False)
    else:
        df.to_csv(latest_csv, index=False)


def _parse_rule(rule: str) -> Optional[Dict[str, Any]]:
    text = str(rule or "").strip()
    if not text:
        return None
    conditions: List[Dict[str, Any]] = []
    for part in RULE_AND_RE.split(text):
        m = SIMPLE_RULE_RE.match(part)
        if not m:
            return None
        field, op, threshold = m.groups()
        field = field.strip()
        if field not in LIVE_SAFE_RULE_FIELDS:
            return None
        try:
            value = float(threshold)
        except ValueError:
            return None
        conditions.append({"field": field, "op": op, "threshold": value})
    if not conditions:
        return None
    first = conditions[0]
    return {
        "field": first["field"],
        "op": first["op"],
        "threshold": first["threshold"],
        "conditions": conditions,
        "rule": text,
    }


def _load_live_safe_v8_rules() -> pd.DataFrame:
    if not V8_LIVE_GATE_ENABLE or not V8_ACCEPTED_RULES_CSV.exists():
        return pd.DataFrame()
    try:
        rules = pd.read_csv(V8_ACCEPTED_RULES_CSV)
    except Exception as exc:
        print(f"[{SESSION_NAME}] v8 gate rules load failed: {type(exc).__name__}: {exc}", flush=True)
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    for _, row in rules.iterrows():
        parsed = _parse_rule(str(row.get("rule", "")))
        if not parsed:
            continue
        rows.append(
            {
                "setup": str(row.get("setup", "")).strip(),
                "stage": str(row.get("stage", "")).strip(),
                "round": row.get("round", ""),
                **parsed,
            }
        )
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    out = out.drop_duplicates(subset=["setup", "rule"]).reset_index(drop=True)
    return out


def _candidate_field_value(row: pd.Series, field: str) -> float:
    if field == "_signal_hour":
        try:
            ts = _ensure_ist_ts(row.get("signal_time_ist"))
            return float(ts.hour + ts.minute / 60.0 + ts.second / 3600.0)
        except Exception:
            return float("nan")
    raw = row.get(field, "")
    if (raw == "" or pd.isna(raw)) and "diagnostics_json" in row:
        try:
            diag = json.loads(str(row.get("diagnostics_json") or "{}"))
            raw = diag.get(field, raw)
        except Exception:
            pass
    return pd.to_numeric(pd.Series([raw]), errors="coerce").iloc[0]


def _eval_condition(row: pd.Series, condition: Dict[str, Any]) -> bool:
    lhs = _candidate_field_value(row, str(condition.get("field", "")))
    rhs = float(condition.get("threshold"))
    if pd.isna(lhs):
        return False
    op = str(condition.get("op", ""))
    if op == "<=":
        return float(lhs) <= rhs
    if op == ">=":
        return float(lhs) >= rhs
    if op == "<":
        return float(lhs) < rhs
    if op == ">":
        return float(lhs) > rhs
    if op == "==":
        return float(lhs) == rhs
    return False


def _eval_rule(row: pd.Series, rule: pd.Series) -> bool:
    conditions = rule.get("conditions")
    if isinstance(conditions, list) and conditions:
        return all(_eval_condition(row, cond) for cond in conditions if isinstance(cond, dict))
    return _eval_condition(
        row,
        {
            "field": rule.get("field", ""),
            "op": rule.get("op", ""),
            "threshold": rule.get("threshold"),
        },
    )


_RANKER_MEMORY_CACHE: Dict[str, Dict[tuple[str, str], float]] = {}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if np.isfinite(out) else default


def _clip01(value: Any) -> float:
    val = _safe_float(value, 0.0)
    return float(min(1.0, max(0.0, val)))


def _signed_rs_score(side: Any, rs_pct: Any) -> float:
    rs = _safe_float(rs_pct, 0.0)
    signed = -rs if str(side or "").upper().strip() == "SHORT" else rs
    return _clip01((signed + 1.0) / 7.0)


def _close_location_score(side: Any, close_loc: Any) -> float:
    loc = _safe_float(close_loc, float("nan"))
    if not np.isfinite(loc):
        return 0.35
    ideal = 0.25 if str(side or "").upper().strip() == "SHORT" else 0.75
    return _clip01(1.0 - abs(loc - ideal) / 0.75)


def _vwap_extension_score(vwap_dist_atr: Any) -> float:
    dist = abs(_safe_float(vwap_dist_atr, 0.0))
    return _clip01(1.0 - max(0.0, dist - 0.5) / 5.0)


def _truth_numeric(df: pd.DataFrame, column: str, default: float = np.nan) -> pd.Series:
    if column in df.columns:
        return pd.to_numeric(df[column], errors="coerce")
    return pd.Series(default, index=df.index, dtype="float64")


def _setup_memory(history: pd.DataFrame) -> Dict[tuple[str, str], float]:
    if history is None or history.empty or not {"side", "setup"}.issubset(history.columns):
        return {}
    mfe = _truth_numeric(history, "forward_mfe_pct")
    mae = _truth_numeric(history, "forward_mae_pct")
    pnl = _truth_numeric(history, "paper_pnl_rs")
    work = history.copy()
    work["ranker_clean_move_label"] = ((mfe >= 0.8) & (mae <= 0.8)) | (pnl > 0)
    work["ranker_bad_move_label"] = ((mae >= 0.8) & (mfe < 0.8)) | (pnl < 0)

    memory: Dict[tuple[str, str], float] = {}
    for (side, setup), group in work.groupby(["side", "setup"], dropna=False):
        clean = group["ranker_clean_move_label"].astype(bool)
        bad = group["ranker_bad_move_label"].astype(bool)
        score = (float(clean.sum()) + 1.0) / (float(clean.sum() + bad.sum()) + 2.0)
        memory[(str(side).upper().strip(), str(setup).upper().strip())] = score
    return memory


def _setup_memory_for_day(day: str) -> Dict[tuple[str, str], float]:
    day = str(day)[:10]
    if day in _RANKER_MEMORY_CACHE:
        return _RANKER_MEMORY_CACHE[day]
    frames: List[pd.DataFrame] = []
    try:
        paths = sorted(RESEARCH_TRUTH_DIR.glob("truth_table_*.csv"))
    except Exception:
        paths = []
    for path in paths:
        path_day = path.stem.replace("truth_table_", "")
        if path_day >= day:
            continue
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        if not df.empty:
            frames.append(df)
    history = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
    memory = _setup_memory(history)
    _RANKER_MEMORY_CACHE[day] = memory
    return memory


def _heuristic_live_rank_score(row: pd.Series, setup_memory: Dict[tuple[str, str], float]) -> float:
    side = str(row.get("side", "")).upper().strip()
    setup = str(row.get("setup", "")).upper().strip()
    quality = _clip01(_safe_float(row.get("quality_score"), 0.0) / 250.0)
    rs_score = _signed_rs_score(side, row.get("rs_pct"))
    vol_score = _clip01(_safe_float(row.get("vol_ratio"), 1.0) / 6.0)
    atr_score = _clip01(_safe_float(row.get("atr_pct"), 0.0) / 0.006)
    close_score = _close_location_score(side, row.get("close_loc"))
    vwap_score = _vwap_extension_score(row.get("vwap_dist_atr"))
    market = _safe_float(row.get("market_ret_pct"), 0.0)
    market_score = _clip01((market + 0.20) / 0.40)
    setup_score = setup_memory.get((side, setup), 0.50)
    score = (
        0.24 * quality
        + 0.16 * rs_score
        + 0.14 * vol_score
        + 0.10 * atr_score
        + 0.14 * close_score
        + 0.12 * vwap_score
        + 0.04 * market_score
        + 0.06 * setup_score
    )
    return round(float(score), 6)


def add_live_ranker_scores(df: pd.DataFrame, day: str) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    out = df.copy()
    if out.empty:
        return out
    memory = _setup_memory_for_day(day)
    out["ranker_score"] = [_heuristic_live_rank_score(row, memory) for _, row in out.iterrows()]
    out["ranker_model"] = "heuristic_v1_live_scanner"
    return out


def _research_filter_reasons(row: pd.Series) -> List[str]:
    side = str(row.get("side", "")).upper().strip()
    setup = str(row.get("setup", "")).upper().strip()
    selection_mode = str(row.get("selection_mode", "")).lower().strip()
    candidate_family = str(row.get("candidate_family", "")).upper().strip()
    if setup.startswith("E_") or selection_mode.startswith("early") or candidate_family == "EARLY":
        return []
    if side != "LONG":
        return []

    reasons: List[str] = []
    close_loc = _candidate_field_value(row, "close_loc")
    vwap_dist_atr = _candidate_field_value(row, "vwap_dist_atr")
    ranker_score = _safe_float(row.get("ranker_score"), float("nan"))

    if (
        np.isfinite(close_loc)
        and np.isfinite(vwap_dist_atr)
        and close_loc > LONG_ANTI_CHASE_CLOSE_LOC_GT
        and vwap_dist_atr > LONG_ANTI_CHASE_VWAP_DIST_ATR_GT
    ):
        reasons.append("LONG_ANTI_CHASE_CLOSE_LOC_GT_0P88_AND_VWAP_DIST_ATR_GT_0P52")

    if setup == "B_AVWAP_RECLAIM_REVERSAL" and (not np.isfinite(ranker_score) or ranker_score < B_AVWAP_RECLAIM_RANKER_MIN):
        reasons.append("LONG_B_AVWAP_RECLAIM_RANKER_LT_0P65")

    if L_TREND_PULLBACK_PROBATION_BLOCK and setup == "L_TREND_PULLBACK":
        reasons.append("LONG_L_TREND_PULLBACK_PROBATION")

    return reasons


def apply_research_live_filters(df: pd.DataFrame, day: str) -> tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    base = add_live_ranker_scores(df, day)
    stats: Dict[str, Any] = {
        "research_live_filters_enabled": bool(RESEARCH_LIVE_FILTER_ENABLE),
        "research_live_filter_mode": RESEARCH_LIVE_FILTER_MODE,
        "research_live_filter_version": RESEARCH_FILTER_VERSION,
        "research_live_filter_rejected": 0,
        "research_live_filter_shadow_rejected": 0,
        "research_live_filter_anti_chase_rejected": 0,
        "research_live_filter_b_reclaim_ranker_rejected": 0,
        "research_live_filter_l_trend_probation_rejected": 0,
        "research_live_filter_long_anti_chase_close_loc_gt": LONG_ANTI_CHASE_CLOSE_LOC_GT,
        "research_live_filter_long_anti_chase_vwap_dist_atr_gt": LONG_ANTI_CHASE_VWAP_DIST_ATR_GT,
        "research_live_filter_b_avwap_reclaim_ranker_min": B_AVWAP_RECLAIM_RANKER_MIN,
    }
    if base.empty:
        return base, base.copy(), stats

    if not RESEARCH_LIVE_FILTER_ENABLE:
        out = base.copy()
        out["research_live_filter_status"] = "DISABLED"
        out["research_live_filter_reason"] = ""
        out["research_live_filter_version"] = RESEARCH_FILTER_VERSION
        return out, out.iloc[0:0].copy(), stats

    active_mode = RESEARCH_LIVE_FILTER_MODE in {"active", "block", "reject", "live", "on", "1", "true"}
    template = base.copy()
    template["research_live_filter_status"] = ""
    template["research_live_filter_reason"] = ""
    template["research_live_filter_version"] = RESEARCH_FILTER_VERSION
    template = template.iloc[0:0].copy()
    accepted_rows: List[Dict[str, Any]] = []
    rejected_rows: List[Dict[str, Any]] = []
    for _, row in base.iterrows():
        item = row.to_dict()
        reasons = _research_filter_reasons(row)
        item["research_live_filter_reason"] = ";".join(reasons)
        item["research_live_filter_version"] = RESEARCH_FILTER_VERSION
        if reasons and active_mode:
            item["research_live_filter_status"] = "REJECTED"
            rejected_rows.append(item)
        else:
            item["research_live_filter_status"] = "SHADOW_REJECT" if reasons else "PASSED"
            accepted_rows.append(item)

    accepted = pd.DataFrame(accepted_rows) if accepted_rows else template.copy()
    rejected = pd.DataFrame(rejected_rows) if rejected_rows else template.copy()
    if not accepted.empty and {"candidate_id", "signal_time_ist", "ticker"}.issubset(accepted.columns):
        accepted = candidate_scan._dedupe_candidate_frame(accepted)
    if not rejected.empty and {"candidate_id", "signal_time_ist", "ticker"}.issubset(rejected.columns):
        rejected = candidate_scan._dedupe_candidate_frame(rejected)

    reason_text = rejected.get("research_live_filter_reason", pd.Series(dtype=str)).astype(str) if not rejected.empty else pd.Series(dtype=str)
    shadow_text = (
        accepted.loc[accepted.get("research_live_filter_status", "").astype(str).eq("SHADOW_REJECT"), "research_live_filter_reason"].astype(str)
        if not accepted.empty and "research_live_filter_status" in accepted.columns
        else pd.Series(dtype=str)
    )
    stats.update(
        {
            "research_live_filter_rejected": int(len(rejected)),
            "research_live_filter_shadow_rejected": int(len(shadow_text)),
            "research_live_filter_anti_chase_rejected": int(reason_text.str.contains("ANTI_CHASE").sum()),
            "research_live_filter_b_reclaim_ranker_rejected": int(reason_text.str.contains("B_AVWAP_RECLAIM").sum()),
            "research_live_filter_l_trend_probation_rejected": int(reason_text.str.contains("L_TREND_PULLBACK").sum()),
        }
    )
    return accepted, rejected, stats


def apply_v8_live_gate(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, Any]]:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy(), {
            "v8_live_gate_enabled": bool(V8_LIVE_GATE_ENABLE),
            "v8_live_gate_rules": 0,
            "v8_live_gate_rejected": 0,
            "early_live_gate_candidates": 0,
            "early_live_gate_accepted": 0,
        }
    work = df.copy()
    family = work["candidate_family"] if "candidate_family" in work.columns else pd.Series("", index=work.index)
    selection_mode = work["selection_mode"] if "selection_mode" in work.columns else pd.Series("", index=work.index)
    setup_col = work["setup"] if "setup" in work.columns else pd.Series("", index=work.index)
    early_mask = (
        family.astype(str).str.upper().eq("EARLY")
        | selection_mode.astype(str).str.lower().str.startswith("early")
        | setup_col.astype(str).str.startswith("E_")
    )
    early_df = work.loc[early_mask].copy()
    standard_df = work.loc[~early_mask].copy()
    early_candidate_count = int(len(early_df))
    early_rows: List[Dict[str, Any]] = []
    if not early_df.empty:
        early_df["_early_score_num"] = pd.to_numeric(early_df.get("quality_score"), errors="coerce").fillna(0.0)
        if "side" not in early_df.columns:
            early_df["side"] = ""
        early_df["side"] = early_df["side"].astype(str).str.upper().str.strip()
        early_df = early_df.loc[early_df["_early_score_num"] >= EARLY_LIVE_GATE_MIN_SCORE].copy()
        if not early_df.empty:
            early_df = (
                early_df.sort_values(["side", "_early_score_num", "ticker"], ascending=[True, False, True])
                .groupby("side", group_keys=False)
                .head(max(1, EARLY_LIVE_GATE_MAX_PER_SIDE))
                .sort_values(["_early_score_num", "ticker"], ascending=[False, True])
                .head(max(1, EARLY_LIVE_GATE_MAX_PER_SLOT))
                .drop(columns=["_early_score_num"], errors="ignore")
                .reset_index(drop=True)
            )
    for _, row in early_df.iterrows():
        item = row.to_dict()
        item["v8_live_gate_status"] = "EARLY_PASSED"
        item["v8_live_gate_rule"] = "early_mode_v1_live_gate"
        item["v8_live_gate_stage"] = "early_live_gate"
        item["v8_live_gate_field"] = "selection_mode"
        early_rows.append(item)

    if not V8_LIVE_GATE_ENABLE:
        out = work.copy()
        out["v8_live_gate_status"] = "DISABLED"
        return out, {
            "v8_live_gate_enabled": False,
            "v8_live_gate_rules": 0,
            "v8_live_gate_rejected": 0,
            "early_live_gate_candidates": early_candidate_count,
            "early_live_gate_accepted": int(len(early_df)),
            "early_live_gate_rejected": int(max(0, early_candidate_count - len(early_df))),
            "early_live_gate_min_score": EARLY_LIVE_GATE_MIN_SCORE,
            "early_live_gate_max_per_side": EARLY_LIVE_GATE_MAX_PER_SIDE,
            "early_live_gate_max_per_slot": EARLY_LIVE_GATE_MAX_PER_SLOT,
        }

    rules = _load_live_safe_v8_rules()
    if rules.empty:
        out = pd.DataFrame(early_rows)
        if not out.empty and {"candidate_id", "signal_time_ist", "ticker"}.issubset(out.columns):
            out = candidate_scan._dedupe_candidate_frame(out)
        return out, {
            "v8_live_gate_enabled": True,
            "v8_live_gate_rules": 0,
            "v8_live_gate_rejected": int(len(standard_df)),
            "v8_live_gate_rules_csv": str(V8_ACCEPTED_RULES_CSV),
            "early_live_gate_candidates": early_candidate_count,
            "early_live_gate_accepted": int(len(early_df)),
            "early_live_gate_rejected": int(max(0, early_candidate_count - len(early_df))),
            "early_live_gate_min_score": EARLY_LIVE_GATE_MIN_SCORE,
            "early_live_gate_max_per_side": EARLY_LIVE_GATE_MAX_PER_SIDE,
            "early_live_gate_max_per_slot": EARLY_LIVE_GATE_MAX_PER_SLOT,
        }

    accepted: List[Dict[str, Any]] = list(early_rows)
    rejected = 0
    for _, row in standard_df.iterrows():
        setup = str(row.get("setup", "")).strip()
        setup_rules = rules[rules["setup"].astype(str).eq(setup)]
        matched_rule: Optional[pd.Series] = None
        for _, rule in setup_rules.iterrows():
            if _eval_rule(row, rule):
                matched_rule = rule
                break
        if matched_rule is None:
            rejected += 1
            continue
        item = row.to_dict()
        item["v8_live_gate_status"] = "PASSED"
        item["v8_live_gate_rule"] = str(matched_rule.get("rule", ""))
        item["v8_live_gate_stage"] = str(matched_rule.get("stage", ""))
        item["v8_live_gate_field"] = str(matched_rule.get("field", ""))
        accepted.append(item)

    out = pd.DataFrame(accepted)
    if not out.empty:
        if {"candidate_id", "signal_time_ist", "ticker"}.issubset(out.columns):
            out = candidate_scan._dedupe_candidate_frame(out)
        else:
            out = out.drop_duplicates().reset_index(drop=True)
    return out, {
        "v8_live_gate_enabled": True,
        "v8_live_gate_rules": int(len(rules)),
        "v8_live_gate_rejected": int(rejected),
        "v8_live_gate_rules_csv": str(V8_ACCEPTED_RULES_CSV),
        "early_live_gate_candidates": early_candidate_count,
        "early_live_gate_accepted": int(len(early_rows)),
        "early_live_gate_rejected": int(max(0, early_candidate_count - len(early_rows))),
        "early_live_gate_min_score": EARLY_LIVE_GATE_MIN_SCORE,
        "early_live_gate_max_per_side": EARLY_LIVE_GATE_MAX_PER_SIDE,
        "early_live_gate_max_per_slot": EARLY_LIVE_GATE_MAX_PER_SLOT,
    }


def _append_audit(slot: pd.Timestamp, summary: Dict[str, Any]) -> None:
    path = _daily_audit_path(slot.strftime("%Y-%m-%d"))
    row = {
        "session": SESSION_NAME,
        "slot_ist": _fmt_slot(slot),
        "created_at_ist": _fmt_slot(pd.Timestamp.now(tz=base_v15.IST)),
        **summary,
    }
    file_exists = path.exists() and path.stat().st_size > 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def run_slot(slot_ts: Any, *, scan_workers: int = DEFAULT_SCAN_WORKERS) -> Dict[str, Any]:
    slot = _ensure_ist_ts(slot_ts).floor("min")
    tickers = _load_universe()
    started = time.perf_counter()
    try:
        market_ctx = candidate_scan.build_market_context_once()
    except Exception as exc:
        market_ctx = {}
        print(f"[{SESSION_NAME}] market context failed: {type(exc).__name__}: {exc}", flush=True)

    try:
        candidates = candidate_scan.scan_slot_candidates(slot, tickers, market_ctx, max_workers=scan_workers)
    except Exception as exc:
        print(f"[{SESSION_NAME}] scan failed: {type(exc).__name__}: {exc}", flush=True)
        candidates = pd.DataFrame()

    day = slot.strftime("%Y-%m-%d")
    raw_candidates = add_live_ranker_scores(candidates.copy() if candidates is not None else pd.DataFrame(), day)
    v8_candidates, gate_stats = apply_v8_live_gate(raw_candidates)
    gated_candidates, research_rejected, research_filter_stats = apply_research_live_filters(v8_candidates, day)

    raw_write_stats = _append_daily_raw_candidates(raw_candidates, day)
    research_rejected_write_stats = _append_daily_research_filter_rejections(research_rejected, day)
    write_stats = _append_daily_candidates(gated_candidates, day)
    _write_json_snapshots(
        raw_candidates,
        slot,
        slot_json_path=_raw_slot_json_path(slot),
        latest_json_name="latest_raw_candidate_tickers.json",
        latest_csv_name="latest_raw_candidate_tickers.csv",
        payload_extra={"v8_live_gate_output": "raw_pre_gate"},
    )
    _write_json_snapshots(
        gated_candidates,
        slot,
        payload_extra={"v8_live_gate_output": "gated_for_entry_engine", **gate_stats, **research_filter_stats},
    )
    _write_json_snapshots(
        research_rejected,
        slot,
        slot_json_path=_research_filter_rejected_slot_json_path(slot),
        latest_json_name="latest_research_filter_rejected_candidate_tickers.json",
        latest_csv_name="latest_research_filter_rejected_candidate_tickers.csv",
        payload_extra={"v8_live_gate_output": "research_live_filter_rejected", **gate_stats, **research_filter_stats},
    )

    total = int(0 if gated_candidates is None else len(gated_candidates))
    raw_total = int(0 if raw_candidates is None else len(raw_candidates))
    long_count = int(0 if gated_candidates is None or gated_candidates.empty else (gated_candidates["side"].astype(str).str.upper() == "LONG").sum())
    short_count = int(0 if gated_candidates is None or gated_candidates.empty else (gated_candidates["side"].astype(str).str.upper() == "SHORT").sum())
    summary = {
        "universe_size": int(len(tickers)),
        "candidate_count": total,
        "raw_candidate_count": raw_total,
        "long_candidates": long_count,
        "short_candidates": short_count,
        "written": int(write_stats["written"]),
        "duplicates": int(write_stats["duplicates"]),
        "raw_written": int(raw_write_stats["written"]),
        "raw_duplicates": int(raw_write_stats["duplicates"]),
        "research_filter_rejected_written": int(research_rejected_write_stats["written"]),
        "research_filter_rejected_duplicates": int(research_rejected_write_stats["duplicates"]),
        "elapsed_sec": round(time.perf_counter() - started, 3),
        "csv_path": str(_daily_csv_path(day)),
        "raw_csv_path": str(_daily_raw_csv_path(day)),
        "research_filter_rejected_csv_path": str(_daily_research_filter_rejected_csv_path(day)),
        "json_path": str(_slot_json_path(slot)),
        **gate_stats,
        **research_filter_stats,
    }
    _append_audit(slot, summary)
    _touch_status("RUNNING", phase="SCAN_DONE", slot=slot.strftime("%H:%M"), **summary)
    _touch_heartbeat("RUNNING", phase="SCAN_DONE", slot=slot.strftime("%H:%M"))
    return {"session": SESSION_NAME, "slot_ist": _fmt_slot(slot), **summary}


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Signal discovery v7 5mins ID candidate ticker session")
    ap.add_argument("--runtime-root", default=str(RUNTIME_ROOT))
    ap.add_argument("--scan-workers", type=int, default=DEFAULT_SCAN_WORKERS)
    ap.add_argument("--replay-slots", nargs="*", default=None)
    ap.add_argument("--post-slot-delay-sec", type=int, default=POST_SLOT_DELAY_SEC)
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    holidays = base_v15._read_holidays_safe()
    print(f"[LIVE] {SESSION_NAME}", flush=True)
    print(f"[INFO] root={SESSION_ROOT}", flush=True)
    print(f"[INFO] scan_workers={int(args.scan_workers)} post_slot_delay={int(args.post_slot_delay_sec)}s", flush=True)

    if args.replay_slots:
        summaries = []
        for raw in args.replay_slots:
            slot = _ensure_ist_ts(raw)
            print(f"[REPLAY] {slot}", flush=True)
            summary = run_slot(slot, scan_workers=int(args.scan_workers))
            summaries.append(summary)
            print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
        (LATEST_DIR / "latest_replay_summary.json").write_text(
            json.dumps({"session": SESSION_NAME, "slots": summaries}, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return

    while True:
        now = base_v15.now_ist()
        _touch_status("RUNNING", phase="LOOP")
        _touch_heartbeat("RUNNING", phase="LOOP")

        if now.time() >= HARD_STOP_TIME:
            _touch_status("STOPPED_AFTER_CUTOFF", phase="HARD_STOP")
            _touch_heartbeat("STOPPED", phase="HARD_STOP")
            print("[STOP] Hard-stop reached for today. Exiting.", flush=True)
            return

        if not base_v15.is_trading_day_safe(now.date(), holidays):
            nxt = base_v15._next_trading_day_start(now, holidays)
            print(f"[SKIP] Not a trading day. Sleeping until {base_v15._fmt_ist_dt(nxt)}.", flush=True)
            base_v15._sleep_until(nxt)
            holidays = base_v15._read_holidays_safe()
            continue

        slot = _next_slot_after(now)
        if slot.date() != now.date():
            base_v15._sleep_until(slot)
            holidays = base_v15._read_holidays_safe()
            continue
        if now < slot:
            print(f"[WAIT] Sleeping until slot {slot.strftime('%Y-%m-%d %H:%M:%S%z')}", flush=True)
            base_v15._sleep_until(slot)

        now = base_v15.now_ist()
        if now.time() > END_TIME:
            _touch_status("STOPPED_AFTER_CUTOFF", phase="END_TIME")
            _touch_heartbeat("STOPPED", phase="END_TIME")
            print("[STOP] End-time reached for today. Exiting.", flush=True)
            return

        print(f"[WAIT] slot={slot.strftime('%H:%M')} post-slot delay {int(args.post_slot_delay_sec)}s", flush=True)
        time.sleep(max(0, int(args.post_slot_delay_sec)))
        _touch_status("RUNNING", phase="SCAN", slot=slot.strftime("%H:%M"))
        _touch_heartbeat("RUNNING", phase="SCAN", slot=slot.strftime("%H:%M"))
        summary = run_slot(slot, scan_workers=int(args.scan_workers))
        print(json.dumps(summary, indent=2, sort_keys=True), flush=True)

        next_slot = slot + timedelta(minutes=SLOT_MINUTES)
        if base_v15.now_ist() < next_slot:
            time.sleep(1.0)


if __name__ == "__main__":
    main()
