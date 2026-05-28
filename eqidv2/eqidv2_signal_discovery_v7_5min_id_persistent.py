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


def _slot_json_path(slot: pd.Timestamp) -> Path:
    return JSON_DIR / f"candidate_tickers_{_slot_key(slot)}.json"


def _raw_slot_json_path(slot: pd.Timestamp) -> Path:
    return JSON_DIR / f"raw_candidate_tickers_{_slot_key(slot)}.json"


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


def apply_v8_live_gate(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, Any]]:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy(), {
            "v8_live_gate_enabled": bool(V8_LIVE_GATE_ENABLE),
            "v8_live_gate_rules": 0,
            "v8_live_gate_rejected": 0,
        }
    if not V8_LIVE_GATE_ENABLE:
        out = df.copy()
        out["v8_live_gate_status"] = "DISABLED"
        return out, {"v8_live_gate_enabled": False, "v8_live_gate_rules": 0, "v8_live_gate_rejected": 0}

    rules = _load_live_safe_v8_rules()
    if rules.empty:
        out = df.iloc[0:0].copy()
        return out, {
            "v8_live_gate_enabled": True,
            "v8_live_gate_rules": 0,
            "v8_live_gate_rejected": int(len(df)),
            "v8_live_gate_rules_csv": str(V8_ACCEPTED_RULES_CSV),
        }

    accepted: List[Dict[str, Any]] = []
    rejected = 0
    for _, row in df.iterrows():
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

    raw_candidates = candidates.copy() if candidates is not None else pd.DataFrame()
    gated_candidates, gate_stats = apply_v8_live_gate(raw_candidates)

    day = slot.strftime("%Y-%m-%d")
    raw_write_stats = _append_daily_raw_candidates(raw_candidates, day)
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
        payload_extra={"v8_live_gate_output": "gated_for_entry_engine", **gate_stats},
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
        "elapsed_sec": round(time.perf_counter() - started, 3),
        "csv_path": str(_daily_csv_path(day)),
        "raw_csv_path": str(_daily_raw_csv_path(day)),
        "json_path": str(_slot_json_path(slot)),
        **gate_stats,
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
