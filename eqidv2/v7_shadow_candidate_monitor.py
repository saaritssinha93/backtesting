#!/usr/bin/env python3
"""
Read-only shadow candidate registry and daily monitor.

This process keeps setup/pattern ideas separate from live configuration:

* Frozen candidate terms live in C:\\TradingData\\eqidv2\\v7_shadow_candidates.
* Daily results are append-only by (date, candidate_id).
* New full-pipeline research discoveries can be auto-registered as SHADOW, but
  existing candidate terms are never re-trained or mutated by this script.
* No live scanner, filter, setup, executor, or backtesting config is modified.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import RUNTIME_STATUS_DIR, runtime_dir


IST = ZoneInfo("Asia/Kolkata")
SESSION_SLUG = "v7_shadow_candidate_monitor"
SESSION_ROOT = runtime_dir("v7_shadow_candidates")
REGISTRY_DIR = SESSION_ROOT / "registry"
DAILY_DIR = SESSION_ROOT / "daily"
LATEST_DIR = SESSION_ROOT / "latest"
REPORT_DIR = SESSION_ROOT / "reports"
HEARTBEAT_DIR = SESSION_ROOT / "heartbeat"

FULL_PIPELINE_ROOT = runtime_dir("v7_full_pipeline_entry_research")
FULL_PIPELINE_LATEST_DIR = FULL_PIPELINE_ROOT / "latest"
FULL_PIPELINE_LATEST_JSON = FULL_PIPELINE_LATEST_DIR / "latest_v7_full_pipeline_entry_research.json"

# v2 governed miner (Full-Pipeline Entry Research v2). Its survivors are tracked
# here under the "v2" tag alongside the v1 discoveries.
FULL_PIPELINE_V2_ROOT = runtime_dir("v7_full_pipeline_entry_research_v2")
FULL_PIPELINE_V2_LATEST_DIR = FULL_PIPELINE_V2_ROOT / "latest"
FULL_PIPELINE_V2_LATEST_JSON = FULL_PIPELINE_V2_LATEST_DIR / "latest_v7_full_pipeline_entry_research_v2.json"

# (research_version, latest_dir, latest_json) monitored each run, in order.
SOURCES: tuple[tuple[str, Path, Path], ...] = (
    ("v1", FULL_PIPELINE_LATEST_DIR, FULL_PIPELINE_LATEST_JSON),
    ("v2", FULL_PIPELINE_V2_LATEST_DIR, FULL_PIPELINE_V2_LATEST_JSON),
)

REGISTRY_PATH = REGISTRY_DIR / "shadow_candidates.json"
EVENT_LOG_PATH = DAILY_DIR / "shadow_candidate_events.csv"
DAILY_RESULTS_PATH = DAILY_DIR / "daily_shadow_results.csv"

TARGET_PCT = 1.10
STOP_PCT = 0.85
DEFAULT_COST_BPS = 16.0
DEFAULT_NOTIONAL_RS = 50_000.0

AUTO_MIN_VAL_TRADES = 25
AUTO_MIN_VAL_DAYS = 2
AUTO_MIN_VAL_PF = 1.20
AUTO_MIN_TRAIN_PF = 1.20


for _path in (SESSION_ROOT, REGISTRY_DIR, DAILY_DIR, LATEST_DIR, REPORT_DIR, HEARTBEAT_DIR, RUNTIME_STATUS_DIR):
    _path.mkdir(parents=True, exist_ok=True)


SEEDED_CANDIDATES: tuple[dict[str, Any], ...] = (
    {
        "id": "CONF_A_DEEP_EMA50_200_SHORT",
        "name": "Conf A Deep EMA50/200 Short",
        "status": "SHADOW",
        "side": "SHORT",
        "base_pipeline_level": "RAW_SCANNER",
        "trigger": "existing_live_truth_table_candidate",
        "terms": [
            {"feature": "m_ema50_dist_atr", "op": ">", "value": -5.973},
            {"feature": "m_ema50_dist_atr", "op": "<=", "value": -4.615},
            {"feature": "m_ema200_dist_atr", "op": ">", "value": -7.138},
            {"feature": "m_ema200_dist_atr", "op": "<=", "value": -5.323},
        ],
        "target_pct": TARGET_PCT,
        "stop_pct": STOP_PCT,
        "cost_bps": DEFAULT_COST_BPS,
        "notional_rs": DEFAULT_NOTIONAL_RS,
        "source": "manual_seed_from_2026-07-08_full_pipeline_research",
    },
    {
        "id": "CONF_B_DEEP_EMA20_200_SHORT",
        "name": "Conf B Deep EMA20/200 Short",
        "status": "SHADOW",
        "side": "SHORT",
        "base_pipeline_level": "RAW_SCANNER",
        "trigger": "existing_live_truth_table_candidate",
        "terms": [
            {"feature": "m_ema20_dist_atr", "op": ">", "value": -4.017},
            {"feature": "m_ema20_dist_atr", "op": "<=", "value": -3.066},
            {"feature": "m_ema200_dist_atr", "op": ">", "value": -5.323},
            {"feature": "m_ema200_dist_atr", "op": "<=", "value": -3.164},
        ],
        "target_pct": TARGET_PCT,
        "stop_pct": STOP_PCT,
        "cost_bps": DEFAULT_COST_BPS,
        "notional_rs": DEFAULT_NOTIONAL_RS,
        "source": "manual_seed_from_2026-07-08_full_pipeline_research",
    },
)


def _now_ist() -> dt.datetime:
    return dt.datetime.now(IST)


def _today_ist() -> dt.date:
    return _now_ist().date()


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8", errors="replace")
    tmp.replace(path)


def _json_sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_sanitize(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        f = float(value)
        return f if math.isfinite(f) else None
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (pd.Timestamp, dt.datetime, dt.date)):
        return str(value)
    if not isinstance(value, (str, bytes, bytearray, list, tuple, dict)) and pd.isna(value):
        return None
    return value


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    _write_text_atomic(path, json.dumps(_json_sanitize(payload), indent=2, sort_keys=True))


def _write_status(state: str, **extra: Any) -> None:
    now = _now_ist()
    payload = {
        "status": state,
        "state": state,
        "ts_ist": now.isoformat(),
        "session": SESSION_SLUG,
        **extra,
    }
    text = "\n".join(f"{k}={v}" for k, v in payload.items()) + "\n"
    _write_text_atomic(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.status", text)
    _write_text_atomic(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.heartbeat", text)
    _write_json_atomic(HEARTBEAT_DIR / f"{SESSION_SLUG}.status.json", payload)
    _write_json_atomic(HEARTBEAT_DIR / f"{SESSION_SLUG}.heartbeat.json", payload)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size <= 2:
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return {}


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 2:
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def _append_events(events: list[dict[str, Any]]) -> None:
    if not events:
        return
    existing = _read_csv(EVENT_LOG_PATH)
    out = pd.concat([existing, pd.DataFrame(events)], ignore_index=True, sort=False)
    out.to_csv(EVENT_LOG_PATH, index=False)


def _normalise_terms(terms: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    norm: list[dict[str, Any]] = []
    for term in terms:
        feature = str(term.get("feature", "")).strip()
        op = str(term.get("op", "")).strip()
        if op not in {">", ">=", "<", "<=", "=="} or not feature:
            continue
        value = float(term.get("value"))
        norm.append({"feature": feature, "op": op, "value": round(value, 10)})
    return sorted(norm, key=lambda x: (x["feature"], x["op"], x["value"]))


def _fingerprint(candidate: dict[str, Any]) -> str:
    payload = {
        "side": str(candidate.get("side", "")).upper().strip(),
        "base_pipeline_level": str(candidate.get("base_pipeline_level", "RAW_SCANNER")).upper().strip(),
        "target_pct": float(candidate.get("target_pct", TARGET_PCT)),
        "stop_pct": float(candidate.get("stop_pct", STOP_PCT)),
        "terms": _normalise_terms(candidate.get("terms", [])),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _candidate_id_for(direction: str, terms: list[dict[str, Any]], source_day: str, version: str = "v1") -> str:
    temp = {
        "side": direction,
        "base_pipeline_level": "RAW_SCANNER",
        "target_pct": TARGET_PCT,
        "stop_pct": STOP_PCT,
        "terms": terms,
    }
    prefix = "" if str(version).lower() == "v1" else f"{str(version).upper()}_"
    return f"{prefix}AUTO_{source_day.replace('-', '')}_{direction.upper()}_{_fingerprint(temp)[:10].upper()}"


def _terms_text(terms: Iterable[dict[str, Any]]) -> str:
    return " AND ".join(f"{t['feature']} {t['op']} {float(t['value']):.6g}" for t in terms)


def _seed_candidate(candidate: dict[str, Any], now: str) -> dict[str, Any]:
    out = dict(candidate)
    out["side"] = str(out.get("side", "")).upper().strip()
    out["base_pipeline_level"] = str(out.get("base_pipeline_level", "RAW_SCANNER")).upper().strip()
    out["terms"] = _normalise_terms(out.get("terms", []))
    out["target_pct"] = float(out.get("target_pct", TARGET_PCT))
    out["stop_pct"] = float(out.get("stop_pct", STOP_PCT))
    out["cost_bps"] = float(out.get("cost_bps", DEFAULT_COST_BPS))
    out["notional_rs"] = float(out.get("notional_rs", DEFAULT_NOTIONAL_RS))
    out.setdefault("status", "SHADOW")
    out.setdefault("research_version", "v1")
    out.setdefault("created_at_ist", now)
    out.setdefault("frozen_at_ist", now)
    out["fingerprint"] = _fingerprint(out)
    out["terms_text"] = _terms_text(out["terms"])
    out.setdefault("notes", [])
    return out


def _load_registry() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    now = _now_ist().isoformat()
    events: list[dict[str, Any]] = []
    if REGISTRY_PATH.exists():
        registry = _read_json(REGISTRY_PATH)
        if not isinstance(registry.get("candidates"), list):
            registry["candidates"] = []
    else:
        registry = {
            "schema_version": 1,
            "created_at_ist": now,
            "updated_at_ist": now,
            "description": "Research-only frozen V7 shadow candidate registry. Not used by live trading.",
            "auto_register_rules": {
                "min_val_trades": AUTO_MIN_VAL_TRADES,
                "min_val_days": AUTO_MIN_VAL_DAYS,
                "min_val_profit_factor": AUTO_MIN_VAL_PF,
                "min_train_profit_factor": AUTO_MIN_TRAIN_PF,
            },
            "candidates": [],
        }

    fingerprints = {str(c.get("fingerprint", "")) for c in registry["candidates"]}
    ids = {str(c.get("id", "")) for c in registry["candidates"]}
    for seed in SEEDED_CANDIDATES:
        candidate = _seed_candidate(seed, now)
        if candidate["fingerprint"] in fingerprints or candidate["id"] in ids:
            continue
        registry["candidates"].append(candidate)
        fingerprints.add(candidate["fingerprint"])
        ids.add(candidate["id"])
        events.append({
            "event_ts_ist": now,
            "event": "SEED_SHADOW_CANDIDATE",
            "candidate_id": candidate["id"],
            "status": candidate["status"],
            "source": candidate.get("source", ""),
            "terms_text": candidate["terms_text"],
        })

    return registry, events


_COND_RE = re.compile(r"(?P<feature>[A-Za-z_][A-Za-z0-9_]*)\s+\((?P<left>[-+0-9.eE]+),\s*(?P<right>[-+0-9.eE]+)\]")


def _parse_condition(condition: str) -> list[dict[str, Any]]:
    terms: list[dict[str, Any]] = []
    for match in _COND_RE.finditer(str(condition or "")):
        feature = match.group("feature")
        left = float(match.group("left"))
        right = float(match.group("right"))
        terms.extend([
            {"feature": feature, "op": ">", "value": left},
            {"feature": feature, "op": "<=", "value": right},
        ])
    return _normalise_terms(terms)


def _latest_full_pipeline_summary() -> dict[str, Any]:
    return _read_json(FULL_PIPELINE_LATEST_JSON)


def _auto_register_discoveries(registry: dict[str, Any], day: str, now: str,
                               version: str = "v1", latest_dir: Path = FULL_PIPELINE_LATEST_DIR) -> list[dict[str, Any]]:
    path = latest_dir / "latest_pattern_candidates.csv"
    patterns = _read_csv(path)
    if patterns.empty:
        return []

    existing_fp = {str(c.get("fingerprint", "")) for c in registry.get("candidates", [])}
    events: list[dict[str, Any]] = []

    for _, row in patterns.iterrows():
        if str(row.get("honesty_status", "")).strip() != "PASS_VALIDATION_RESEARCH_ONLY":
            continue
        val_trades = float(row.get("val_trades", 0) or 0)
        val_days = float(row.get("val_days", 0) or 0)
        val_pf = float(row.get("val_profit_factor", 0) or 0)
        val_pnl = float(row.get("val_pnl_rs", 0) or 0)
        train_pf = float(row.get("train_profit_factor", 0) or 0)
        train_pnl = float(row.get("train_pnl_rs", 0) or 0)
        if (
            val_trades < AUTO_MIN_VAL_TRADES
            or val_days < AUTO_MIN_VAL_DAYS
            or val_pf < AUTO_MIN_VAL_PF
            or val_pnl <= 0
            or train_pf < AUTO_MIN_TRAIN_PF
            or train_pnl <= 0
        ):
            continue
        direction = str(row.get("direction", "")).upper().strip()
        terms = _parse_condition(str(row.get("condition", "")))
        if direction not in {"LONG", "SHORT"} or not terms:
            continue
        candidate = _seed_candidate({
            "id": _candidate_id_for(direction, terms, day, version),
            "name": f"Auto {version.upper()} {direction} Shadow {day} {_fingerprint({'side': direction, 'terms': terms})[:8].upper()}",
            "status": "SHADOW",
            "research_version": version,
            "side": direction,
            "base_pipeline_level": "RAW_SCANNER",
            "trigger": "existing_live_truth_table_candidate",
            "terms": terms,
            "target_pct": TARGET_PCT,
            "stop_pct": STOP_PCT,
            "cost_bps": DEFAULT_COST_BPS,
            "notional_rs": DEFAULT_NOTIONAL_RS,
            "source": f"auto_from_full_pipeline_entry_research_{version}_{day}",
            "source_condition": str(row.get("condition", "")),
            "source_metrics": {
                "train_trades": val_to_number(row.get("train_trades")),
                "train_pnl_rs": val_to_number(row.get("train_pnl_rs")),
                "train_profit_factor": val_to_number(row.get("train_profit_factor")),
                "val_trades": val_to_number(row.get("val_trades")),
                "val_days": val_to_number(row.get("val_days")),
                "val_pnl_rs": val_to_number(row.get("val_pnl_rs")),
                "val_profit_factor": val_to_number(row.get("val_profit_factor")),
            },
        }, now)
        if candidate["fingerprint"] in existing_fp:
            continue
        registry["candidates"].append(candidate)
        existing_fp.add(candidate["fingerprint"])
        events.append({
            "event_ts_ist": now,
            "event": "AUTO_REGISTER_SHADOW_CANDIDATE",
            "candidate_id": candidate["id"],
            "status": candidate["status"],
            "research_version": version,
            "source": candidate.get("source", ""),
            "terms_text": candidate["terms_text"],
            "val_trades": val_trades,
            "val_days": val_days,
            "val_pnl_rs": val_pnl,
            "val_profit_factor": val_pf,
        })

    return events


def val_to_number(value: Any) -> float | int | None:
    try:
        f = float(value)
    except Exception:
        return None
    if not math.isfinite(f):
        return None
    return int(f) if f.is_integer() else f


def _apply_terms(df: pd.DataFrame, terms: Iterable[dict[str, Any]]) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    for term in terms:
        feature = str(term.get("feature", "")).strip()
        op = str(term.get("op", "")).strip()
        if feature not in df.columns:
            return pd.Series(False, index=df.index)
        values = pd.to_numeric(df[feature], errors="coerce")
        threshold = float(term.get("value"))
        if op == ">":
            mask &= values.gt(threshold)
        elif op == ">=":
            mask &= values.ge(threshold)
        elif op == "<":
            mask &= values.lt(threshold)
        elif op == "<=":
            mask &= values.le(threshold)
        elif op == "==":
            mask &= values.eq(threshold)
        else:
            return pd.Series(False, index=df.index)
    return mask.fillna(False)


def _profit_factor(target_hits: int, sl_hits: int) -> float:
    gross_win = float(target_hits) * 470.0
    gross_loss = float(sl_hits) * 520.0
    if gross_loss > 0:
        return gross_win / gross_loss
    return float("inf") if gross_win > 0 else 0.0


def _summarize_matches(matches: pd.DataFrame) -> dict[str, Any]:
    if matches.empty:
        return {
            "entries": 0,
            "target_hits": 0,
            "sl_hits": 0,
            "no_hits": 0,
            "no_data": 0,
            "net_pnl_rs": 0.0,
            "gross_pnl_rs": 0.0,
            "cost_rs": 0.0,
            "win_rate_all": 0.0,
            "resolved_win_rate": 0.0,
            "profit_factor": 0.0,
            "tickers": 0,
            "top_setup": "",
            "top_setup_pnl_rs": 0.0,
            "top_ticker": "",
            "top_ticker_pnl_rs": 0.0,
        }

    target_hits = int(matches["outcome"].eq("TARGET").sum())
    sl_hits = int(matches["outcome"].eq("SL").sum())
    no_hits = int(matches["outcome"].eq("NO_HIT").sum())
    no_data = int(matches["outcome"].eq("NO_DATA").sum())
    entries = int(len(matches))
    resolved = target_hits + sl_hits

    def _top(col: str) -> tuple[str, float]:
        if col not in matches.columns or matches.empty:
            return "", 0.0
        grouped = matches.groupby(col, dropna=False)["pnl_rs"].sum().sort_values(ascending=False)
        if grouped.empty:
            return "", 0.0
        return str(grouped.index[0]), float(grouped.iloc[0])

    top_setup, top_setup_pnl = _top("setup")
    top_ticker, top_ticker_pnl = _top("ticker")

    return {
        "entries": entries,
        "target_hits": target_hits,
        "sl_hits": sl_hits,
        "no_hits": no_hits,
        "no_data": no_data,
        "net_pnl_rs": float(matches.get("pnl_rs", pd.Series(dtype=float)).sum()),
        "gross_pnl_rs": float(matches.get("gross_pnl_rs", pd.Series(dtype=float)).sum()),
        "cost_rs": float(matches.get("cost_rs", pd.Series(dtype=float)).sum()),
        "win_rate_all": target_hits / entries if entries else 0.0,
        "resolved_win_rate": target_hits / resolved if resolved else 0.0,
        "profit_factor": _profit_factor(target_hits, sl_hits),
        "tickers": int(matches["ticker"].nunique()) if "ticker" in matches.columns else 0,
        "top_setup": top_setup,
        "top_setup_pnl_rs": top_setup_pnl,
        "top_ticker": top_ticker,
        "top_ticker_pnl_rs": top_ticker_pnl,
    }


def _candidate_active(candidate: dict[str, Any]) -> bool:
    return str(candidate.get("status", "")).upper().strip() in {
        "DISCOVERED",
        "SHADOW",
        "CONFIRMED_SHADOW",
        "WATCH",
    }


def _load_sim(latest_dir: Path, summary: dict[str, Any]) -> tuple[pd.DataFrame, Path]:
    latest_outputs = summary.get("latest_outputs") or {}
    path = Path(latest_outputs.get("simulated_candidates_csv") or latest_dir / "latest_simulated_candidates.csv")
    return _read_csv(path), path


def _monitor_day(registry: dict[str, Any], day: str, sim_by_version: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    match_parts: list[pd.DataFrame] = []
    now = _now_ist().isoformat()

    day_cache: dict[str, pd.DataFrame] = {}
    for ver, sim in sim_by_version.items():
        day_cache[ver] = (
            sim[sim.get("truth_date", "").astype(str).eq(day)].copy() if sim is not None and not sim.empty else pd.DataFrame()
        )

    for candidate in registry.get("candidates", []):
        if not _candidate_active(candidate):
            continue
        candidate_id = str(candidate.get("id", "")).strip()
        version = str(candidate.get("research_version", "v1")).lower().strip() or "v1"
        side = str(candidate.get("side", "")).upper().strip()
        pipeline = str(candidate.get("base_pipeline_level", "RAW_SCANNER")).upper().strip()
        terms = _normalise_terms(candidate.get("terms", []))
        day_df = day_cache.get(version, day_cache.get("v1", pd.DataFrame()))
        if day_df is None or day_df.empty:
            base = pd.DataFrame()
        else:
            base = day_df[
                day_df.get("direction", "").astype(str).str.upper().eq(side)
                & day_df.get("pipeline_level", "").astype(str).str.upper().eq(pipeline)
            ].copy()
        matches = base[_apply_terms(base, terms)].copy() if not base.empty else pd.DataFrame()
        metrics = _summarize_matches(matches)
        row = {
            "run_date": day,
            "run_ts_ist": now,
            "candidate_id": candidate_id,
            "research_version": version,
            "name": candidate.get("name", candidate_id),
            "status": candidate.get("status", ""),
            "side": side,
            "base_pipeline_level": pipeline,
            "terms_text": _terms_text(terms),
            "active_day": bool(metrics["entries"] > 0),
            **metrics,
        }
        rows.append(row)
        if not matches.empty:
            if "candidate_id" in matches.columns:
                matches = matches.rename(columns={"candidate_id": "source_candidate_id"})
            matches.insert(0, "candidate_id", candidate_id)
            matches.insert(1, "candidate_name", candidate.get("name", candidate_id))
            match_parts.append(matches)

    daily = pd.DataFrame(rows)
    matches_df = pd.concat(match_parts, ignore_index=True, sort=False) if match_parts else pd.DataFrame()
    return daily, matches_df


def _merge_daily_results(new_rows: pd.DataFrame) -> pd.DataFrame:
    old = _read_csv(DAILY_RESULTS_PATH)
    if old.empty:
        merged = new_rows.copy()
    else:
        merged = pd.concat([old, new_rows], ignore_index=True, sort=False)
    if not merged.empty:
        merged = merged.drop_duplicates(["run_date", "candidate_id"], keep="last")
        merged = merged.sort_values(["candidate_id", "run_date"]).reset_index(drop=True)
    merged.to_csv(DAILY_RESULTS_PATH, index=False)
    return merged


def _rolling_summary(all_results: pd.DataFrame) -> pd.DataFrame:
    if all_results.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for candidate_id, part in all_results.groupby("candidate_id", dropna=False):
        part = part.sort_values("run_date").copy()
        active = part[part["entries"].fillna(0).astype(float) > 0].copy()
        last5 = active.tail(5)
        last10 = active.tail(10)

        def agg(df: pd.DataFrame, prefix: str) -> dict[str, Any]:
            entries = int(df["entries"].sum()) if not df.empty else 0
            target = int(df["target_hits"].sum()) if not df.empty else 0
            sl = int(df["sl_hits"].sum()) if not df.empty else 0
            pnl = float(df["net_pnl_rs"].sum()) if not df.empty else 0.0
            positive_sum = float(df.loc[df["net_pnl_rs"] > 0, "net_pnl_rs"].sum()) if not df.empty else 0.0
            best_day = float(df["net_pnl_rs"].max()) if not df.empty else 0.0
            return {
                f"{prefix}_active_days": int(len(df)),
                f"{prefix}_entries": entries,
                f"{prefix}_target_hits": target,
                f"{prefix}_sl_hits": sl,
                f"{prefix}_net_pnl_rs": pnl,
                f"{prefix}_win_rate_all": target / entries if entries else 0.0,
                f"{prefix}_profit_factor": _profit_factor(target, sl),
                f"{prefix}_best_day_pnl_rs": best_day,
                f"{prefix}_best_day_positive_share": best_day / positive_sum if positive_sum > 0 and best_day > 0 else 0.0,
                f"{prefix}_without_best_day_pnl_rs": pnl - best_day,
            }

        total = agg(active, "total")
        r5 = agg(last5, "last5")
        r10 = agg(last10, "last10")
        latest = part.tail(1).iloc[0].to_dict()
        review = _review_label(total, r5, r10)
        rows.append({
            "candidate_id": candidate_id,
            "research_version": latest.get("research_version", "v1"),
            "name": latest.get("name", ""),
            "status": latest.get("status", ""),
            "side": latest.get("side", ""),
            "terms_text": latest.get("terms_text", ""),
            "calendar_days_tracked": int(len(part)),
            "active_days": int(len(active)),
            "latest_run_date": latest.get("run_date", ""),
            "latest_entries": int(latest.get("entries", 0) or 0),
            "latest_net_pnl_rs": float(latest.get("net_pnl_rs", 0) or 0),
            "recommended_action": review,
            **total,
            **r5,
            **r10,
        })
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["recommended_action", "total_net_pnl_rs"], ascending=[True, False])
    return out


def _review_label(total: dict[str, Any], r5: dict[str, Any], r10: dict[str, Any]) -> str:
    active_days = int(total.get("total_active_days", 0))
    if active_days < 5:
        return "COLLECT_MORE_DATA"
    total_pnl = float(total.get("total_net_pnl_rs", 0.0))
    total_pf = float(total.get("total_profit_factor", 0.0))
    last5_pnl = float(r5.get("last5_net_pnl_rs", 0.0))
    last5_pf = float(r5.get("last5_profit_factor", 0.0))
    best_share = float(total.get("total_best_day_positive_share", 0.0))
    without_best = float(total.get("total_without_best_day_pnl_rs", 0.0))
    if active_days >= 5 and (total_pnl < 0 or last5_pnl < 0 or last5_pf < 1.0):
        return "REJECT_REVIEW"
    if active_days >= 5 and total_pnl > 0 and total_pf >= 1.25 and last5_pnl > 0 and last5_pf >= 1.15 and best_share <= 0.45 and without_best > 0:
        return "CONFIRMED_SHADOW_REVIEW"
    return "CONTINUE_SHADOW"


def _md_table(df: pd.DataFrame, columns: list[str], max_rows: int = 30) -> list[str]:
    if df.empty:
        return ["_No rows._"]
    rows = df.head(max_rows)
    out = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for _, row in rows.iterrows():
        vals = []
        for col in columns:
            value = row.get(col, "")
            if isinstance(value, float):
                if "rate" in col or "share" in col:
                    vals.append(f"{value * 100:.2f}%")
                elif "factor" in col:
                    vals.append("inf" if math.isinf(value) else f"{value:.2f}")
                else:
                    vals.append(f"{value:,.2f}")
            else:
                vals.append(str(value).replace("|", "/"))
        out.append("| " + " | ".join(vals) + " |")
    return out


def _build_report(day: str, registry: dict[str, Any], daily: pd.DataFrame, summary: pd.DataFrame, events: list[dict[str, Any]], sim_paths: dict[str, Path]) -> str:
    n_v1 = sum(1 for c in registry.get("candidates", []) if str(c.get("research_version", "v1")).lower() == "v1")
    n_v2 = sum(1 for c in registry.get("candidates", []) if str(c.get("research_version", "")).lower() == "v2")
    lines = [
        f"# V7 Shadow Candidate Monitor - {day}",
        "",
        "Research-only monitor. It reads full-pipeline outputs (v1 + v2) and writes shadow results; it does not modify live trading configuration.",
        "",
        "## Sources",
        f"- Registry: `{REGISTRY_PATH}` (candidates: v1=`{n_v1}`, v2=`{n_v2}`)",
        f"- v1 simulated candidates: `{sim_paths.get('v1', '')}`",
        f"- v2 (governed miner) simulated candidates: `{sim_paths.get('v2', '')}`",
        f"- Daily results: `{DAILY_RESULTS_PATH}`",
        "",
        "## Registry Rules",
        "- Candidate terms are frozen by fingerprint.",
        "- Existing candidates are tracked daily and not re-trained.",
        "- New full-pipeline candidates are auto-registered only if they pass validation thresholds.",
        "- Promotion/rejection labels are recommendations only.",
        "",
        "## Today",
    ]
    lines.extend(_md_table(
        daily,
        [
            "candidate_id", "research_version", "status", "side", "entries", "target_hits", "sl_hits",
            "no_hits", "net_pnl_rs", "profit_factor", "top_setup", "top_ticker",
        ],
        40,
    ))
    lines.extend(["", "## Rolling Shadow Summary"])
    lines.extend(_md_table(
        summary,
        [
            "candidate_id", "research_version", "recommended_action", "active_days", "total_entries",
            "total_net_pnl_rs", "total_profit_factor", "last5_net_pnl_rs",
            "last5_profit_factor", "total_best_day_positive_share",
            "total_without_best_day_pnl_rs",
        ],
        50,
    ))
    lines.extend(["", "## New Registry Events"])
    if events:
        lines.extend(_md_table(pd.DataFrame(events), ["event", "candidate_id", "status", "source", "terms_text"], 20))
    else:
        lines.append("_No new candidates registered today._")
    lines.extend([
        "",
        "## Guardrail",
        "A candidate can move toward live only after separate approval, longer-history validation, shadow stability, capacity checks, and live-parity review.",
        "",
    ])
    return "\n".join(lines)


def run(day: str) -> dict[str, Any]:
    _write_status("RUNNING", phase="START", day=day)
    now = _now_ist().isoformat()
    registry, seed_events = _load_registry()
    summaries = {ver: _read_json(latest_json) for ver, _dir, latest_json in SOURCES}
    if not day:
        day = str(summaries.get("v1", {}).get("day") or summaries.get("v2", {}).get("day") or _today_ist())

    events = list(seed_events)
    sim_by_version: dict[str, pd.DataFrame] = {}
    sim_paths: dict[str, Path] = {}
    for ver, latest_dir, _latest_json in SOURCES:
        events.extend(_auto_register_discoveries(registry, day, now, ver, latest_dir))
        sim_df, sim_path = _load_sim(latest_dir, summaries.get(ver, {}))
        sim_by_version[ver] = sim_df
        sim_paths[ver] = sim_path

    registry["updated_at_ist"] = now
    registry["candidate_count"] = len(registry.get("candidates", []))
    _write_json_atomic(REGISTRY_PATH, registry)
    _append_events(events)

    daily, matches = _monitor_day(registry, day, sim_by_version)
    all_results = _merge_daily_results(daily)
    rollup = _rolling_summary(all_results)

    latest_daily = LATEST_DIR / "latest_shadow_daily_results.csv"
    latest_rollup = LATEST_DIR / "latest_shadow_candidate_summary.csv"
    latest_matches = LATEST_DIR / "latest_shadow_candidate_matches.csv"
    latest_md = LATEST_DIR / "latest_v7_shadow_candidate_monitor.md"
    latest_json = LATEST_DIR / "latest_v7_shadow_candidate_monitor.json"
    dated_md = REPORT_DIR / f"v7_shadow_candidate_monitor_{day}.md"
    dated_json = REPORT_DIR / f"v7_shadow_candidate_monitor_{day}.json"

    daily.to_csv(latest_daily, index=False)
    rollup.to_csv(latest_rollup, index=False)
    matches.to_csv(latest_matches, index=False)

    md = _build_report(day, registry, daily, rollup, events, sim_paths)
    _write_text_atomic(latest_md, md)
    _write_text_atomic(dated_md, md)

    payload = {
        "status": "DONE",
        "day": day,
        "registry": str(REGISTRY_PATH),
        "candidate_count": len(registry.get("candidates", [])),
        "new_events": events,
        "daily_results": str(DAILY_RESULTS_PATH),
        "latest_report": str(latest_md),
        "dated_report": str(dated_md),
        "latest_daily_csv": str(latest_daily),
        "latest_summary_csv": str(latest_rollup),
        "latest_matches_csv": str(latest_matches),
        "summary": rollup.replace([np.inf, -np.inf], np.nan).to_dict(orient="records"),
    }
    _write_json_atomic(latest_json, payload)
    _write_json_atomic(dated_json, payload)
    _write_status(
        "DONE",
        phase="COMPLETE",
        day=day,
        report=str(latest_md),
        candidate_count=len(registry.get("candidates", [])),
        daily_rows=len(daily),
        new_candidates=sum(1 for e in events if e.get("event") == "AUTO_REGISTER_SHADOW_CANDIDATE"),
    )
    print(f"[{SESSION_SLUG}] DONE day={day} candidates={len(registry.get('candidates', []))} report={latest_md}", flush=True)
    return payload


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default="", help="Shadow run date. Defaults to latest full-pipeline report day.")
    args = ap.parse_args(list(argv) if argv is not None else None)
    try:
        run(str(args.date or ""))
        return 0
    except Exception as exc:
        _write_status("ERROR", phase="FAILED", day=str(args.date or ""), error=f"{type(exc).__name__}: {exc}")
        raise


if __name__ == "__main__":
    raise SystemExit(main())
