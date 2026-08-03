"""
Persistent session runner for "Signal discovery v7 5mins ID".

This is not a trade signal writer. It scans the completed 5-minute signal
candle and writes candidate tickers only, in CSV and JSON. Entry candle and
entry price are intentionally absent.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

import avwap_5min_ID_v7_candidate_scan as candidate_scan
import eqidv2_v11_live_overlay as v11_live_overlay
import eqidv2_live_combined_analyser_csv_v15 as base_v15
import eqidv2_final_conf_live_bootstrap as _conf_boot
import eqidv2_conf_tier_c_live_scan as _conf_tier_c
import eqidv2_decision_funnel as decision_funnel
from eqidv2_runtime_manifest import freeze_runtime_manifest
from eqidv2_runtime_paths import RUNTIME_ROOT, RUNTIME_STATUS_DIR, runtime_dir


# --- configured final_setup_conf book runtime activation --------------------
# When EQIDV2_USE_FINAL_SETUP_CONF is set, the conf becomes the single source of
# truth: the detection whitelist + legacy blocklists are replaced by the conf's
# setups. Native conf setups continue through v8 + research filters; only the
# provenance groups v11 explicitly readmits are merged back from raw candidates
# before the final conf mask. Default OFF -> live behaviour unchanged.
# _ensure_conf_scanner() is idempotent and called once per process at run_slot.
_CONF_SCANNER_ACTIVE = False
_CONF_SCANNER_BOOTSTRAPPED = False


def _ensure_conf_scanner() -> bool:
    global _CONF_SCANNER_ACTIVE, _CONF_SCANNER_BOOTSTRAPPED
    if _CONF_SCANNER_BOOTSTRAPPED:
        return _CONF_SCANNER_ACTIVE
    _CONF_SCANNER_BOOTSTRAPPED = True
    if not _conf_boot.is_enabled():
        return False
    keys = set(_conf_boot.conf_keys())
    ukeys = set(_conf_boot.conf_keys_upper())
    # Detection whitelist = conf setups; clear legacy blocklists for conf setups.
    candidate_scan.ALLOWED_SETUPS = set(keys)
    candidate_scan.FILTER_TO_V8_EXIT_SETUPS = True
    # USER_DIRECTED conf-only detector(s): emit when the conf book is active and whitelisted.
    if hasattr(candidate_scan, "v2") and any(_conf_boot._u(k) == "S9_MIDDAY_LOSE" for k in keys):
        candidate_scan.v2.ENABLE_S9_MIDDAY_LOSE = True
    if hasattr(candidate_scan, "v2") and any(_conf_boot._u(k) == "DOC5D_AVWAP_RECLAIM_LONG" for k in keys):
        candidate_scan.v2.ENABLE_DOC5D_AVWAP_RECLAIM = True
    candidate_scan.EXCLUDED_SETUPS = {s for s in candidate_scan.EXCLUDED_SETUPS if _conf_boot._u(s) not in ukeys}
    if hasattr(candidate_scan, "EARLY_BLOCKED_SETUPS"):
        candidate_scan.EARLY_BLOCKED_SETUPS = {
            s for s in candidate_scan.EARLY_BLOCKED_SETUPS if _conf_boot._u(s) not in ukeys
        }
    if hasattr(candidate_scan, "RESEARCH_PROBATION_SETUPS"):
        candidate_scan.RESEARCH_PROBATION_SETUPS = {
            s for s in candidate_scan.RESEARCH_PROBATION_SETUPS if _conf_boot._u(s) not in ukeys
        }
    _CONF_SCANNER_ACTIVE = True
    _n_readmit = len(_conf_boot.readmit_setups())
    print(
        f"[{SESSION_NAME}] final_setup_conf ACTIVE ({_conf_boot.GATE_VERSION}): "
        f"source={_conf_boot.conf_source()} {len(keys)} setups (match-v11): "
        f"{len(keys) - _n_readmit} native through v8+overlay+research, "
        f"{_n_readmit} non-native readmitted; conf mask is the final selection.",
        flush=True,
    )
    return True


SESSION_NAME = "Signal discovery v7 5mins ID"
SESSION_SLUG = "signal_discovery_v7_5mins_ID"
# P2-14: schema version stamped into every candidate row.  Bump this string
# whenever the candidate CSV column set changes so the entry engine can detect
# stale-format files without a column-name comparison.
CANDIDATE_SCHEMA_VERSION = "v7_candidate_2026_07_28_parity_v1"

# When EQIDV2_REPLAY_OUTPUT_ROOT is set, all session-level outputs (JSON
# snapshots, audit CSVs, JSONL, status, heartbeat) redirect there instead of
# the production runtime root. This prevents --replay-slots from overwriting
# live outputs. The BAT guard checks market hours; this provides Python-side
# isolation so replay is safe even if the BAT guard is bypassed.
_REPLAY_OUTPUT_ROOT_RAW = os.getenv("EQIDV2_REPLAY_OUTPUT_ROOT", "").strip()
if _REPLAY_OUTPUT_ROOT_RAW:
    import pathlib as _pathlib
    SESSION_ROOT = _pathlib.Path(_REPLAY_OUTPUT_ROOT_RAW) / SESSION_SLUG
    _is_replay_isolated = True
else:
    SESSION_ROOT = runtime_dir("signal_discovery_v7_5mins_ID")
    _is_replay_isolated = False

CSV_DIR = SESSION_ROOT / "csv"
JSON_DIR = SESSION_ROOT / "json"
LATEST_DIR = SESSION_ROOT / "latest"
AUDIT_DIR = SESSION_ROOT / "audit"
HEARTBEAT_DIR = SESSION_ROOT / "heartbeat"
_RUNTIME_MANIFEST_PATH = ""

SLOT_MINUTES = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_SLOT_MINUTES", "5"))
POST_SLOT_DELAY_SEC = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_POST_SLOT_DELAY_SEC", "15"))

# Feed-completion gate. The 5-min indicator feed
# (eqidv2_eod_scheduler_for_5mins_data_live_minimal) finishes writing each slot's
# bar at ~slot+45-60s. Scanning before that reads pre-bar files and silently drops
# A/B/C/etc setups. Instead of guessing a fixed post-slot delay, poll the feed's
# status.json and wait until it reports the current slot complete (timeout-bounded).
FEED_GATE_ENABLE = str(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_FEED_GATE", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
    "disabled",
}
FEED_STATUS_JSON = Path(
    os.getenv(
        "EQIDV2_SIGNAL_DISCOVERY_V7_FEED_STATUS_JSON",
        str(Path(__file__).resolve().parent / "logs" / "eqidv2_eod_scheduler_for_5mins_data_live_minimal.status.json"),
    )
)
FEED_GATE_MAX_WAIT_SEC = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_FEED_GATE_MAX_WAIT_SEC", "90"))
FEED_GATE_POLL_SEC = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_FEED_GATE_POLL_SEC", "2"))
FEED_GATE_MIN_DELAY_SEC = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_FEED_GATE_MIN_DELAY_SEC", "5"))
FEED_GATE_MAX_VERIFICATION_FAILURES = max(
    0,
    int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_FEED_GATE_MAX_VERIFICATION_FAILURES", "5")),
)
FEED_UNIVERSE_MANIFEST = Path(
    os.getenv(
        "EQIDV2_SIGNAL_DISCOVERY_V7_FEED_UNIVERSE_MANIFEST",
        str(RUNTIME_STATUS_DIR / "feed_universe_5m.json"),
    )
)
REQUIRE_FEED_UNIVERSE_MANIFEST = str(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_REQUIRE_FEED_UNIVERSE_MANIFEST", "0")
).strip().lower() in {"1", "true", "yes", "on"}
LATENCY_WARN_SEC = float(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_LATENCY_WARN_SEC", "45")
)
LATENCY_CRITICAL_SEC = float(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_LATENCY_CRITICAL_SEC", "55")
)
LATENCY_HARD_SEC = float(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_LATENCY_HARD_SEC", "60")
)
LATENCY_WRITE_RESERVE_SEC = float(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_LATENCY_WRITE_RESERVE_SEC", "2")
)
LATENCY_EXPECTED_SCAN_SEC = float(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EXPECTED_SCAN_SEC", "15")
)
LATENCY_FAIL_CLOSED = str(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_LATENCY_FAIL_CLOSED", "0")
).strip().lower() in {"1", "true", "yes", "on"}
LATENCY_STATUS_PATH = Path(
    os.getenv(
        "EQIDV2_SIGNAL_DISCOVERY_V7_LATENCY_STATUS_PATH",
        str(RUNTIME_STATUS_DIR / "signal_discovery_v7_5mins_ID.latency.json"),
    )
)
# reject_slot: skip scan entirely on feed timeout (default, fail-closed).
# degraded_scan: scan on whatever data is present (emergency fallback only).
FEED_TIMEOUT_ACTION = os.getenv(
    "EQIDV2_SIGNAL_DISCOVERY_V7_FEED_TIMEOUT_ACTION", "reject_slot"
).strip().lower()
DEFAULT_SCAN_WORKERS = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_SCAN_WORKERS", "8"))
TIER123_SCAN_WORKERS = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_TIER123_SCAN_WORKERS", str(DEFAULT_SCAN_WORKERS)))
PARALLEL_SCAN_BRANCHES_ENABLE = str(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_PARALLEL_SCAN_BRANCHES", "1")
).strip().lower() not in {"0", "false", "no", "off", "disabled"}
TIER123_LATEST_START_LAG_SEC = max(
    0.0,
    float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_TIER123_LATEST_START_LAG_SEC", "40")),
)
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
LONG_ANTI_CHASE_CLOSE_LOC_GT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_LONG_ANTI_CHASE_CLOSE_LOC_GT", "0.97"))
LONG_ANTI_CHASE_VWAP_DIST_ATR_GT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_LONG_ANTI_CHASE_VWAP_DIST_ATR_GT", "3.50"))
B_AVWAP_RECLAIM_RANKER_MIN = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_B_AVWAP_RECLAIM_RANKER_MIN", "0.65"))
L_TREND_PULLBACK_PROBATION_BLOCK = str(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_L_TREND_PULLBACK_PROBATION_BLOCK", "1")
).strip().lower() not in {"0", "false", "no", "off", "disabled"}
SHORT_FOCUS_ENABLE = str(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS", "1")
).strip().lower() not in {"0", "false", "no", "off", "disabled"}
SHORT_FOCUS_ALLOWED_SIDES = {
    part.strip().upper()
    for part in os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS_ALLOWED_SIDES", "SHORT").split(",")
    if part.strip()
}
# Setups exempt from the SHORT_FOCUS side filter. A_MOD_BREAK_C1_HIGH is LONG
# but has a validated gate (rs_pct/atr_pct/time) so it should pass through even
# when SHORT_FOCUS is active. Add comma-separated names to the env var to extend.
SHORT_FOCUS_EXEMPT_SETUPS = {
    part.strip().upper()
    for part in os.getenv(
        "EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS_EXEMPT_SETUPS",
        "A_MOD_BREAK_C1_HIGH",
    ).split(",")
    if part.strip()
}
EARLY_LIVE_GATE_MIN_SCORE = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_LIVE_GATE_MIN_SCORE", "95"))
EARLY_LIVE_GATE_MAX_PER_SIDE = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_LIVE_GATE_MAX_PER_SIDE", "4"))
EARLY_LIVE_GATE_MAX_PER_SLOT = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_LIVE_GATE_MAX_PER_SLOT", "8"))
UNCOVERED_FALLBACK_ENABLE = str(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK", "1")
).strip().lower() not in {"0", "false", "no", "off", "disabled"}
UNCOVERED_FALLBACK_START_TIME = os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_START_TIME", "11:05").strip()
UNCOVERED_FALLBACK_END_TIME = os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_END_TIME", "14:30").strip()
UNCOVERED_FALLBACK_MIN_RANKER = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_MIN_RANKER", "0.75"))
UNCOVERED_FALLBACK_MIN_QUALITY = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_MIN_QUALITY", "125"))
UNCOVERED_FALLBACK_MAX_PER_SLOT = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_MAX_PER_SLOT", "1"))
UNCOVERED_FALLBACK_ALLOWED_SIDES = {
    part.strip().upper()
    for part in os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_ALLOWED_SIDES", "SHORT").split(",")
    if part.strip()
}
UNCOVERED_FALLBACK_ALLOWED_SETUPS = {
    part.strip().upper()
    for part in os.getenv(
        "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_ALLOWED_SETUPS",
        "A_MOD_BREAK_C1_LOW,C_OR_BREAKDOWN,A_PULLBACK_C2_THEN_BREAK_C2_LOW,"
        "B_HUGE_RED_FAILED_BOUNCE,D_AVWAP_LOSE_REVERSAL,G_LOWER_LOW_BREAK",
    ).split(",")
    if part.strip()
}
V11_BACKTESTING_OVERLAY_ENABLE = str(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_V11_BACKTESTING_OVERLAY", "1")
).strip().lower() not in {"0", "false", "no", "off", "disabled"}
V11_BACKTESTING_OVERLAY_PROFILE = os.getenv(
    "EQIDV2_SIGNAL_DISCOVERY_V7_V11_BACKTESTING_PROFILE",
    v11_live_overlay.DEFAULT_SELECTED_STRATEGY_PROFILE,
).strip()
V11_BACKTESTING_OVERLAY_INCLUDE_TIER123 = str(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_V11_TIER123", "1")
).strip().lower() not in {"0", "false", "no", "off", "disabled"}
V11_BACKTESTING_OVERLAY_AB_GATE_PROFILE = os.getenv(
    "EQIDV2_SIGNAL_DISCOVERY_V7_V11_AB_GATE_PROFILE",
    v11_live_overlay.DEFAULT_AB_GATE_PROFILE,
).strip()
RESEARCH_FILTER_VERSION = "v7_live_research_2026_05_29"
RESEARCH_TRUTH_DIR = runtime_dir("live_research_v7_research_layer", "truth_table")
START_TIME = base_v15.dtime(9, 15)
END_TIME = base_v15.dtime(15, 0)
HARD_STOP_TIME = base_v15.dtime(15, 30)
ENTRY_WINDOW_START_TIME = os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_ENTRY_WINDOW_START", "09:30").strip()
ENTRY_WINDOW_END_TIME = os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_ENTRY_WINDOW_END", "14:30").strip()
ENTRY_SIGNAL_TO_ENTRY_LAG_MIN = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_ENTRY_LAG_MIN", "1"))

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


def _parse_hhmm(value: str, default: str) -> Any:
    raw = str(value or default).strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(raw, fmt).time()
        except ValueError:
            continue
    return datetime.strptime(default, "%H:%M").time()


def _set_status_env() -> None:
    os.environ["EQIDV2_RUNTIME_STATUS_FILE"] = str(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.status")
    os.environ["EQIDV2_RUNTIME_HEARTBEAT_FILE"] = str(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.heartbeat")
    os.environ["EQIDV2_RUNTIME_SCRIPT_NAME"] = Path(__file__).name


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


def _read_feed_status_slot() -> Tuple[Optional[pd.Timestamp], str, int, Tuple[str, ...]]:
    """Return the feed slot, state, verification count, and partition failures."""
    try:
        data = json.loads(FEED_STATUS_JSON.read_text(encoding="utf-8"))
    except Exception:
        return None, "", 0, ()
    raw_slot = data.get("slot_ist") or data.get("last_slot_ist")
    state = str(data.get("overall_state") or data.get("state") or "")
    verification_failed_count = int(data.get("verification_failed_count", 0) or 0)
    failures = tuple(str(item) for item in (data.get("failures", []) or []))
    if not raw_slot:
        return None, state, verification_failed_count, failures
    try:
        return (
            _ensure_ist_ts(raw_slot).floor(f"{SLOT_MINUTES}min"),
            state,
            verification_failed_count,
            failures,
        )
    except Exception:
        return None, state, verification_failed_count, failures


def _feed_state_is_ready(
    state: str,
    verification_failed_count: int = 0,
    failures: Tuple[str, ...] = (),
) -> bool:
    state_upper = str(state or "").strip().upper()
    if state_upper in {
        "OK",
        "WARN",
        "DONE",
        "SUCCESS",
        "COMPLETE",
        "COMPLETED",
        "READY",
    }:
        return True
    verification_only = bool(failures) and all("verify_failed=" in item for item in failures)
    return (
        state_upper == "FAIL"
        and verification_only
        and 0 < int(verification_failed_count) <= FEED_GATE_MAX_VERIFICATION_FAILURES
    )


def _wait_for_feed_slot(slot: Any) -> bool:
    """Block until the feed reports `slot` written, or until the timeout elapses.

    Returns True if the feed confirmed the slot, False if it timed out (in which
    case the caller scans anyway on whatever data is present — degraded, but the
    session never hangs if the feed dies).
    """
    slot_floor = _ensure_ist_ts(slot).floor(f"{SLOT_MINUTES}min")
    # Small floor so we don't hammer the file the instant the slot opens while the
    # feed is still mid-fetch; counts toward the overall budget.
    if FEED_GATE_MIN_DELAY_SEC > 0:
        time.sleep(min(FEED_GATE_MIN_DELAY_SEC, max(0, FEED_GATE_MAX_WAIT_SEC)))
    deadline = time.monotonic() + max(0, FEED_GATE_MAX_WAIT_SEC - FEED_GATE_MIN_DELAY_SEC)
    waited_from = time.monotonic()
    last_feed_slot: Optional[pd.Timestamp] = None
    while True:
        feed_slot, state, verification_failed_count, failures = _read_feed_status_slot()
        last_feed_slot = feed_slot
        if (
            feed_slot is not None
            and feed_slot >= slot_floor
            and _feed_state_is_ready(state, verification_failed_count, failures)
        ):
            elapsed = time.monotonic() - waited_from + FEED_GATE_MIN_DELAY_SEC
            if str(state).strip().upper() == "FAIL":
                print(
                    f"[FEED][WARN] accepting slot with {verification_failed_count} "
                    f"verification-only stale symbol(s); "
                    f"limit={FEED_GATE_MAX_VERIFICATION_FAILURES}",
                    flush=True,
                )
            print(
                f"[FEED] slot {slot.strftime('%H:%M')} ready "
                f"(feed_slot={feed_slot.strftime('%H:%M')} state={state or 'NA'}) "
                f"after ~{elapsed:.0f}s",
                flush=True,
            )
            return True
        if time.monotonic() >= deadline:
            last = last_feed_slot.strftime("%H:%M") if last_feed_slot is not None else "NONE"
            print(
                f"[FEED][WARN] timeout after {FEED_GATE_MAX_WAIT_SEC}s waiting for feed slot "
                f"{slot.strftime('%H:%M')} (last feed_slot={last} state={state or 'NA'})",
                flush=True,
            )
            return False
        time.sleep(max(0.5, FEED_GATE_POLL_SEC))


def _entry_window_ok_for_signal_ts(signal_ts: Any) -> bool:
    entry_ts = _ensure_ist_ts(signal_ts).floor("min") + pd.Timedelta(minutes=ENTRY_SIGNAL_TO_ENTRY_LAG_MIN)
    t = entry_ts.time()
    start_t = _parse_hhmm(ENTRY_WINDOW_START_TIME, "09:30")
    end_t = _parse_hhmm(ENTRY_WINDOW_END_TIME, "14:00")
    return start_t <= t <= end_t


def _row_signal_ts(row: pd.Series) -> Optional[pd.Timestamp]:
    for key in ("signal_time_ist", "bar_time_ist", "signal_bar_time_ist", "signal_datetime"):
        raw = row.get(key, "")
        if not str(raw or "").strip():
            continue
        try:
            return _ensure_ist_ts(raw).floor("min")
        except Exception:
            continue
    return None


def _filter_entry_window(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    if df is None or df.empty:
        return pd.DataFrame(), 0
    keep: List[bool] = []
    rejected = 0
    for _, row in df.iterrows():
        signal_ts = _row_signal_ts(row)
        ok = signal_ts is not None and _entry_window_ok_for_signal_ts(signal_ts)
        keep.append(bool(ok))
        if not ok:
            rejected += 1
    return df.loc[keep].reset_index(drop=True), rejected


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


def _canonical_universe(symbols: Iterable[str]) -> List[str]:
    return sorted({str(t).strip().upper() for t in symbols if str(t).strip()})


def _universe_sha256(symbols: Iterable[str]) -> str:
    return hashlib.sha256("\n".join(_canonical_universe(symbols)).encode("utf-8")).hexdigest()


def _read_feed_universe_manifest(expected_day: Optional[str] = None) -> Dict[str, Any]:
    """Read and validate the feed's atomic canonical-universe manifest."""
    payload = json.loads(FEED_UNIVERSE_MANIFEST.read_text(encoding="utf-8"))
    symbols = _canonical_universe(payload.get("symbols", []) or [])
    count = int(payload.get("universe_count", -1))
    expected_hash = str(payload.get("universe_sha256", "")).strip().lower()
    actual_hash = _universe_sha256(symbols)
    if count != len(symbols):
        raise ValueError(f"manifest count mismatch: declared={count} actual={len(symbols)}")
    if not expected_hash or expected_hash != actual_hash:
        raise ValueError("manifest sha256 mismatch")
    slot = _ensure_ist_ts(payload.get("slot_ist")).floor(f"{SLOT_MINUTES}min")
    if expected_day and slot.strftime("%Y-%m-%d") != str(expected_day):
        raise ValueError(
            f"manifest day mismatch: expected={expected_day} actual={slot.strftime('%Y-%m-%d')}"
        )
    return {
        **payload,
        "symbols": symbols,
        "universe_count": len(symbols),
        "universe_sha256": actual_hash,
        "_slot_ts": slot,
    }


def _load_universe() -> List[str]:
    try:
        manifest = _read_feed_universe_manifest()
        return list(manifest["symbols"])
    except Exception as exc:
        if REQUIRE_FEED_UNIVERSE_MANIFEST:
            raise RuntimeError(
                f"required feed universe manifest unavailable/invalid: {type(exc).__name__}: {exc}"
            ) from exc
        print(
            f"[{SESSION_NAME}] feed universe unavailable; using legacy universe: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
    try:
        universe = candidate_scan.v2._load_universe()
    except Exception as exc:
        print(f"[{SESSION_NAME}] universe load failed: {type(exc).__name__}: {exc}", flush=True)
        universe = []
    return _canonical_universe(universe)


def _check_universe_feed_parity(scanner_tickers: List[str]) -> None:
    """Verify exact symbol/hash parity with the authoritative feed universe."""
    scanner_symbols = _canonical_universe(scanner_tickers)
    scanner_hash = _universe_sha256(scanner_symbols)
    try:
        manifest = _read_feed_universe_manifest()
        feed_symbols = list(manifest["symbols"])
        feed_hash = str(manifest["universe_sha256"])
        if scanner_symbols != feed_symbols or scanner_hash != feed_hash:
            missing = sorted(set(feed_symbols) - set(scanner_symbols))
            extra = sorted(set(scanner_symbols) - set(feed_symbols))
            raise RuntimeError(
                f"scanner={len(scanner_symbols)} feed={len(feed_symbols)} "
                f"missing={missing[:8]} extra={extra[:8]}"
            )
        print(
            f"[{SESSION_NAME}] universe parity EXACT: "
            f"count={len(scanner_symbols)} sha256={scanner_hash[:12]}",
            flush=True,
        )
    except Exception as exc:
        if REQUIRE_FEED_UNIVERSE_MANIFEST:
            raise
        print(f"[{SESSION_NAME}] universe parity check skipped: {type(exc).__name__}: {exc}", flush=True)


def _daily_csv_path(day: str) -> Path:
    return CSV_DIR / f"candidate_tickers_{day}.csv"


def _daily_raw_csv_path(day: str) -> Path:
    return CSV_DIR / f"raw_candidate_tickers_{day}.csv"


def _daily_research_filter_rejected_csv_path(day: str) -> Path:
    return CSV_DIR / f"research_filter_rejected_candidate_tickers_{day}.csv"


def _daily_v11_overlay_csv_path(day: str) -> Path:
    return CSV_DIR / f"v11_overlay_candidate_tickers_{day}.csv"


def _daily_v11_overlay_rejected_csv_path(day: str) -> Path:
    return CSV_DIR / f"v11_overlay_rejected_candidate_tickers_{day}.csv"


def _slot_json_path(slot: pd.Timestamp) -> Path:
    return JSON_DIR / f"candidate_tickers_{_slot_key(slot)}.json"


def _raw_slot_json_path(slot: pd.Timestamp) -> Path:
    return JSON_DIR / f"raw_candidate_tickers_{_slot_key(slot)}.json"


def _research_filter_rejected_slot_json_path(slot: pd.Timestamp) -> Path:
    return JSON_DIR / f"research_filter_rejected_candidate_tickers_{_slot_key(slot)}.json"


def _v11_overlay_slot_json_path(slot: pd.Timestamp) -> Path:
    return JSON_DIR / f"v11_overlay_candidate_tickers_{_slot_key(slot)}.json"


def _v11_overlay_rejected_slot_json_path(slot: pd.Timestamp) -> Path:
    return JSON_DIR / f"v11_overlay_rejected_candidate_tickers_{_slot_key(slot)}.json"


def _slot_complete_marker_path(slot: pd.Timestamp) -> Path:
    return JSON_DIR / f"slot_complete_{_slot_key(slot)}.json"


def _slot_funnel_csv_path(slot: pd.Timestamp) -> Path:
    return AUDIT_DIR / f"candidate_decision_funnel_{_slot_key(slot)}.csv"


def _slot_funnel_jsonl_path(slot: pd.Timestamp) -> Path:
    return AUDIT_DIR / f"candidate_decision_funnel_{_slot_key(slot)}.jsonl"


def _daily_audit_path(day: str) -> Path:
    return AUDIT_DIR / f"candidate_tickers_audit_{day}.csv"


def _daily_audit_jsonl_path(day: str) -> Path:
    return AUDIT_DIR / f"candidate_tickers_audit_{day}.jsonl"


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


def _append_daily_v11_overlay_candidates(df: pd.DataFrame, day: str) -> Dict[str, int]:
    path = _daily_v11_overlay_csv_path(day)
    return _append_candidates_to_path(df, path)


def _append_daily_v11_overlay_rejections(df: pd.DataFrame, day: str) -> Dict[str, int]:
    path = _daily_v11_overlay_rejected_csv_path(day)
    return _append_candidates_to_path(df, path)


def _append_candidates_to_path(df: pd.DataFrame, path: Path) -> Dict[str, int]:
    if df is None or df.empty:
        if not path.exists():
            df.to_csv(path, index=False)
        return {"written": 0, "duplicates": 0}
    # P2-14: stamp schema version so consumers can detect format changes
    if "candidate_schema_version" not in df.columns:
        df = df.copy()
        df["candidate_schema_version"] = CANDIDATE_SCHEMA_VERSION

    if path.exists() and path.stat().st_size > 0:
        try:
            existing_header = list(pd.read_csv(path, nrows=0).columns)
        except Exception:
            existing_header = []
        incoming_header = list(df.columns)
        if existing_header and existing_header != incoming_header:
            try:
                existing_frame = pd.read_csv(path, low_memory=False)
                union_header = existing_header + [
                    col for col in incoming_header if col not in existing_header
                ]
                _atomic_write_csv(path, existing_frame.reindex(columns=union_header))
                df = df.reindex(columns=union_header)
                print(
                    f"[{SESSION_NAME}] expanded daily CSV schema in place: "
                    f"{path} columns={len(union_header)}",
                    flush=True,
                )
            except Exception as exc:
                print(
                    f"[{SESSION_NAME}] daily CSV schema expansion failed for "
                    f"{path}: {type(exc).__name__}: {exc}",
                    flush=True,
                )

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


def _atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _atomic_write_csv(path: Path, df: pd.DataFrame) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        df.to_csv(tmp, index=False)
        os.replace(tmp, path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


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
    _atomic_write_text(slot_json_path or _slot_json_path(slot), text)
    _atomic_write_text(LATEST_DIR / latest_json_name, text)
    _atomic_write_csv(LATEST_DIR / latest_csv_name, pd.DataFrame() if df is None else df)


def _write_slot_complete_marker(
    slot: pd.Timestamp,
    *,
    decision_ready_at_ist: str,
    candidate_count: int,
) -> Path:
    candidate_path = _slot_json_path(slot)
    candidate_sha256 = ""
    if candidate_path.exists():
        candidate_sha256 = hashlib.sha256(candidate_path.read_bytes()).hexdigest()
    payload = {
        "schema_version": "eqidv2_scanner_slot_complete_v1",
        "session": SESSION_NAME,
        "slot_ist": _fmt_slot(slot),
        "decision_ready_at_ist": decision_ready_at_ist,
        "published_at_ist": _fmt_slot(pd.Timestamp.now(tz=base_v15.IST)),
        "candidate_count": int(candidate_count),
        "candidate_json_path": str(candidate_path),
        "candidate_json_sha256": candidate_sha256,
        "runtime_manifest_path": _RUNTIME_MANIFEST_PATH,
        "complete": True,
    }
    text = json.dumps(payload, indent=2, sort_keys=True, default=str)
    marker_path = _slot_complete_marker_path(slot)
    _atomic_write_text(marker_path, text)
    _atomic_write_text(LATEST_DIR / "latest_slot_complete.json", text)
    return marker_path


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


def _v8_gate_base_stats() -> Dict[str, Any]:
    return {
        "v8_live_gate_enabled": bool(V8_LIVE_GATE_ENABLE),
        "v8_live_gate_rules": 0,
        "v8_live_gate_rejected": 0,
        "v8_live_gate_rules_csv": str(V8_ACCEPTED_RULES_CSV),
        "early_live_gate_candidates": 0,
        "early_live_gate_accepted": 0,
        "early_live_gate_rejected": 0,
        "early_live_gate_min_score": EARLY_LIVE_GATE_MIN_SCORE,
        "early_live_gate_max_per_side": EARLY_LIVE_GATE_MAX_PER_SIDE,
        "early_live_gate_max_per_slot": EARLY_LIVE_GATE_MAX_PER_SLOT,
        "uncovered_fallback_enabled": bool(UNCOVERED_FALLBACK_ENABLE),
        "uncovered_fallback_candidates": 0,
        "uncovered_fallback_eligible": 0,
        "uncovered_fallback_accepted": 0,
        "uncovered_fallback_rejected": 0,
        "uncovered_fallback_rejected_by_cap": 0,
        "uncovered_fallback_rejected_by_universe": 0,
        "uncovered_fallback_start_time": UNCOVERED_FALLBACK_START_TIME,
        "uncovered_fallback_end_time": UNCOVERED_FALLBACK_END_TIME,
        "uncovered_fallback_min_ranker": UNCOVERED_FALLBACK_MIN_RANKER,
        "uncovered_fallback_min_quality": UNCOVERED_FALLBACK_MIN_QUALITY,
        "uncovered_fallback_max_per_slot": UNCOVERED_FALLBACK_MAX_PER_SLOT,
        "uncovered_fallback_allowed_sides": ",".join(sorted(UNCOVERED_FALLBACK_ALLOWED_SIDES)),
        "uncovered_fallback_allowed_setups": ",".join(sorted(UNCOVERED_FALLBACK_ALLOWED_SETUPS)),
    }


def _candidate_signal_time(row: pd.Series) -> Optional[pd.Timestamp]:
    try:
        return _ensure_ist_ts(row.get("signal_time_ist")).floor("min")
    except Exception:
        return None


def _uncovered_fallback_time_ok(row: pd.Series) -> bool:
    signal_ts = _candidate_signal_time(row)
    if signal_ts is None:
        return False
    start_t = _parse_hhmm(UNCOVERED_FALLBACK_START_TIME, "11:05")
    end_t = _parse_hhmm(UNCOVERED_FALLBACK_END_TIME, "14:30")
    t = signal_ts.time()
    return start_t <= t <= end_t


def _uncovered_fallback_score_ok(row: pd.Series) -> bool:
    ranker_score = _safe_float(row.get("ranker_score"), float("nan"))
    quality_score = _safe_float(row.get("quality_score"), float("nan"))
    return (
        np.isfinite(ranker_score)
        and np.isfinite(quality_score)
        and ranker_score >= UNCOVERED_FALLBACK_MIN_RANKER
        and quality_score >= UNCOVERED_FALLBACK_MIN_QUALITY
    )


def _uncovered_fallback_universe_ok(row: pd.Series) -> bool:
    side = str(row.get("side", "")).upper().strip()
    setup = str(row.get("setup", "")).upper().strip()
    if UNCOVERED_FALLBACK_ALLOWED_SIDES and side not in UNCOVERED_FALLBACK_ALLOWED_SIDES:
        return False
    if UNCOVERED_FALLBACK_ALLOWED_SETUPS and setup not in UNCOVERED_FALLBACK_ALLOWED_SETUPS:
        return False
    return True


def _select_uncovered_fallback_rows(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows or not UNCOVERED_FALLBACK_ENABLE:
        return pd.DataFrame()
    work = pd.DataFrame(rows)
    if work.empty:
        return work
    work["_fallback_signal_ts"] = pd.to_datetime(work.get("signal_time_ist"), errors="coerce")
    if getattr(work["_fallback_signal_ts"].dt, "tz", None) is None:
        work["_fallback_signal_ts"] = work["_fallback_signal_ts"].dt.tz_localize(base_v15.IST)
    else:
        work["_fallback_signal_ts"] = work["_fallback_signal_ts"].dt.tz_convert(base_v15.IST)
    work["_fallback_ranker"] = pd.to_numeric(work.get("ranker_score"), errors="coerce").fillna(-np.inf)
    work["_fallback_quality"] = pd.to_numeric(work.get("quality_score"), errors="coerce").fillna(-np.inf)
    work["ticker"] = work.get("ticker", "").astype(str).str.upper().str.strip()
    max_per_slot = max(0, int(UNCOVERED_FALLBACK_MAX_PER_SLOT))
    if max_per_slot <= 0:
        return work.iloc[0:0].drop(
            columns=["_fallback_signal_ts", "_fallback_ranker", "_fallback_quality"],
            errors="ignore",
        )
    selected = (
        work.sort_values(
            ["_fallback_signal_ts", "_fallback_ranker", "_fallback_quality", "ticker"],
            ascending=[True, False, False, True],
        )
        .groupby("_fallback_signal_ts", group_keys=False)
        .head(max_per_slot)
        .copy()
    )
    return selected.drop(columns=["_fallback_signal_ts", "_fallback_ranker", "_fallback_quality"], errors="ignore")


_RANKER_MEMORY_CACHE: Dict[str, Dict[tuple[str, str], float]] = {}

# Universe: load once per trading day (filtered_stocks glob or CSV — cheap but wasteful per-slot).
_UNIVERSE_CACHE_BY_DAY: Dict[str, List[str]] = {}


def _load_universe_for_day(day: str) -> List[str]:
    if day not in _UNIVERSE_CACHE_BY_DAY:
        _UNIVERSE_CACHE_BY_DAY.clear()
        _RANKER_MEMORY_CACHE.clear()
        tickers = _load_universe()
        _UNIVERSE_CACHE_BY_DAY[day] = tickers
        _check_universe_feed_parity(tickers)
    return _UNIVERSE_CACHE_BY_DAY[day]


# v8 rules: loaded once per session (CSV never changes intraday).
_V8_RULES_CACHE: Optional[pd.DataFrame] = None


def _load_live_safe_v8_rules_cached() -> pd.DataFrame:
    global _V8_RULES_CACHE
    if _V8_RULES_CACHE is None:
        _V8_RULES_CACHE = _load_live_safe_v8_rules()
    return _V8_RULES_CACHE


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
    reasons: List[str] = []

    if (
        SHORT_FOCUS_ENABLE
        and SHORT_FOCUS_ALLOWED_SIDES
        and side not in SHORT_FOCUS_ALLOWED_SIDES
        and setup not in SHORT_FOCUS_EXEMPT_SETUPS
    ):
        reasons.append(f"SHORT_FOCUS_SIDE_NOT_ALLOWED_{side or 'BLANK'}")

    if setup.startswith("E_") or selection_mode.startswith("early") or candidate_family == "EARLY":
        return reasons
    if side != "LONG":
        return reasons

    close_loc = _candidate_field_value(row, "close_loc")
    vwap_dist_atr = _candidate_field_value(row, "vwap_dist_atr")
    ranker_score = _safe_float(row.get("ranker_score"), float("nan"))

    if (
        np.isfinite(close_loc)
        and np.isfinite(vwap_dist_atr)
        and close_loc > LONG_ANTI_CHASE_CLOSE_LOC_GT
        and vwap_dist_atr > LONG_ANTI_CHASE_VWAP_DIST_ATR_GT
    ):
        reasons.append(
            "LONG_ANTI_CHASE_"
            f"CLOSE_LOC_GT_{LONG_ANTI_CHASE_CLOSE_LOC_GT:.2f}_"
            f"VWAP_DIST_ATR_GT_{LONG_ANTI_CHASE_VWAP_DIST_ATR_GT:.2f}"
        )

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
        "research_live_filter_short_focus_enabled": bool(SHORT_FOCUS_ENABLE),
        "research_live_filter_short_focus_allowed_sides": ",".join(sorted(SHORT_FOCUS_ALLOWED_SIDES)),
        "research_live_filter_short_focus_exempt_setups": ",".join(sorted(SHORT_FOCUS_EXEMPT_SETUPS)),
        "research_live_filter_short_focus_rejected": 0,
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
            "research_live_filter_short_focus_rejected": int(reason_text.str.contains("SHORT_FOCUS").sum()),
            "research_live_filter_anti_chase_rejected": int(reason_text.str.contains("ANTI_CHASE").sum()),
            "research_live_filter_b_reclaim_ranker_rejected": int(reason_text.str.contains("B_AVWAP_RECLAIM").sum()),
            "research_live_filter_l_trend_probation_rejected": int(reason_text.str.contains("L_TREND_PULLBACK").sum()),
        }
    )
    return accepted, rejected, stats


def apply_v8_live_gate(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, Any]]:
    base_stats = _v8_gate_base_stats()
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy(), base_stats
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
        stats = dict(base_stats)
        stats.update(
            {
                "v8_live_gate_enabled": False,
                "early_live_gate_candidates": early_candidate_count,
                "early_live_gate_accepted": int(len(early_df)),
                "early_live_gate_rejected": int(max(0, early_candidate_count - len(early_df))),
            }
        )
        return out, stats

    rules = _load_live_safe_v8_rules_cached()
    if rules.empty:
        out = pd.DataFrame(early_rows)
        if not out.empty and {"candidate_id", "signal_time_ist", "ticker"}.issubset(out.columns):
            out = candidate_scan._dedupe_candidate_frame(out)
        stats = dict(base_stats)
        stats.update(
            {
                "v8_live_gate_enabled": True,
                "v8_live_gate_rejected": int(len(standard_df)),
                "early_live_gate_candidates": early_candidate_count,
                "early_live_gate_accepted": int(len(early_df)),
                "early_live_gate_rejected": int(max(0, early_candidate_count - len(early_df))),
            }
        )
        return out, stats

    accepted: List[Dict[str, Any]] = list(early_rows)
    fallback_pool: List[Dict[str, Any]] = []
    uncovered_candidates = 0
    uncovered_eligible = 0
    uncovered_rejected_by_universe = 0
    rejected = 0
    covered_setups = set(rules["setup"].astype(str))
    for _, row in standard_df.iterrows():
        setup = str(row.get("setup", "")).strip()
        setup_rules = rules[rules["setup"].astype(str).eq(setup)]
        matched_rule: Optional[pd.Series] = None
        for _, rule in setup_rules.iterrows():
            if _eval_rule(row, rule):
                matched_rule = rule
                break
        if matched_rule is None:
            if setup not in covered_setups:
                uncovered_candidates += 1
                if (
                    UNCOVERED_FALLBACK_ENABLE
                    and _uncovered_fallback_universe_ok(row)
                    and _uncovered_fallback_time_ok(row)
                    and _uncovered_fallback_score_ok(row)
                ):
                    uncovered_eligible += 1
                    item = row.to_dict()
                    item["v8_live_gate_status"] = "UNCOVERED_FALLBACK_ELIGIBLE"
                    item["v8_live_gate_rule"] = (
                        f"uncovered_setup_ranker>={UNCOVERED_FALLBACK_MIN_RANKER:g}"
                        f"_quality>={UNCOVERED_FALLBACK_MIN_QUALITY:g}"
                    )
                    item["v8_live_gate_stage"] = "uncovered_setup_fallback"
                    item["v8_live_gate_field"] = "ranker_score,quality_score"
                    fallback_pool.append(item)
                    continue
                if not _uncovered_fallback_universe_ok(row):
                    uncovered_rejected_by_universe += 1
            rejected += 1
            continue
        item = row.to_dict()
        item["v8_live_gate_status"] = "PASSED"
        item["v8_live_gate_rule"] = str(matched_rule.get("rule", ""))
        item["v8_live_gate_stage"] = str(matched_rule.get("stage", ""))
        item["v8_live_gate_field"] = str(matched_rule.get("field", ""))
        accepted.append(item)

    fallback_df = _select_uncovered_fallback_rows(fallback_pool)
    fallback_accepted = int(len(fallback_df))
    fallback_rejected_by_cap = int(max(0, len(fallback_pool) - fallback_accepted))
    if not fallback_df.empty:
        for _, row in fallback_df.iterrows():
            item = row.to_dict()
            item["v8_live_gate_status"] = "UNCOVERED_FALLBACK_PASSED"
            accepted.append(item)
    rejected += int(uncovered_eligible - fallback_accepted)
    out = pd.DataFrame(accepted)
    if not out.empty:
        if {"candidate_id", "signal_time_ist", "ticker"}.issubset(out.columns):
            out = candidate_scan._dedupe_candidate_frame(out)
        else:
            out = out.drop_duplicates().reset_index(drop=True)
    stats = dict(base_stats)
    stats.update(
        {
            "v8_live_gate_enabled": True,
            "v8_live_gate_rules": int(len(rules)),
            "v8_live_gate_rejected": int(rejected),
            "early_live_gate_candidates": early_candidate_count,
            "early_live_gate_accepted": int(len(early_rows)),
            "early_live_gate_rejected": int(max(0, early_candidate_count - len(early_rows))),
            "uncovered_fallback_candidates": int(uncovered_candidates),
            "uncovered_fallback_eligible": int(uncovered_eligible),
            "uncovered_fallback_accepted": int(fallback_accepted),
            "uncovered_fallback_rejected": int(max(0, uncovered_candidates - fallback_accepted)),
            "uncovered_fallback_rejected_by_cap": int(fallback_rejected_by_cap),
            "uncovered_fallback_rejected_by_universe": int(uncovered_rejected_by_universe),
        }
    )
    return out, stats


def _append_audit(slot: pd.Timestamp, summary: Dict[str, Any]) -> None:
    day = slot.strftime("%Y-%m-%d")
    row = {
        "session": SESSION_NAME,
        "slot_ist": _fmt_slot(slot),
        "created_at_ist": _fmt_slot(pd.Timestamp.now(tz=base_v15.IST)),
        **summary,
    }

    # Primary: append-only JSONL — one compact JSON object per line, no
    # schema migrations, no backup files. Each run appends exactly one line.
    jsonl_path = _daily_audit_jsonl_path(day)
    try:
        with open(jsonl_path, "a", encoding="utf-8") as jf:
            jf.write(json.dumps(row, default=str) + "\n")
    except OSError as exc:
        print(f"[{SESSION_NAME}] audit JSONL write failed for {jsonl_path}: {exc}", flush=True)

    # Secondary: CSV kept for backwards-compat with downstream readers.
    # Schema drift is handled by rewriting the header when the fieldnames
    # change — no backup archive, no 29-file accumulation.
    csv_path = _daily_audit_path(day)
    fieldnames = list(row.keys())
    file_exists = csv_path.exists() and csv_path.stat().st_size > 0
    rewrite_header = False
    if file_exists:
        try:
            with open(csv_path, "r", newline="", encoding="utf-8") as f:
                existing_header = next(csv.reader(f), [])
            if existing_header and existing_header != fieldnames:
                rewrite_header = True
        except Exception:
            rewrite_header = True
    if rewrite_header:
        # Schema changed: truncate and restart the CSV — no backup archive.
        print(f"[{SESSION_NAME}] audit CSV schema changed, restarting without backup: {csv_path}", flush=True)
        file_exists = False
    with open(csv_path, "a" if file_exists else "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def _active_conf_tier_c_setups() -> set[str]:
    if not _CONF_SCANNER_ACTIVE:
        return set()
    return set(_conf_boot.conf_keys()).intersection(_conf_tier_c.TIER_C_SETUPS)


def _legacy_tier123_eligible(scan_start_lag_sec: float) -> bool:
    # final_setup_conf uses its own causal detector for any active Tier-C setup.
    # Running the legacy full-universe branch as well adds latency and candidates
    # that the final conf gate immediately rejects.
    return bool(
        not _CONF_SCANNER_ACTIVE
        and V11_BACKTESTING_OVERLAY_INCLUDE_TIER123
        and scan_start_lag_sec <= TIER123_LATEST_START_LAG_SEC
    )


def run_slot(slot_ts: Any, *, scan_workers: int = DEFAULT_SCAN_WORKERS) -> Dict[str, Any]:
    _ensure_conf_scanner()
    slot = _ensure_ist_ts(slot_ts).floor("min")
    day = slot.strftime("%Y-%m-%d")
    tickers = _load_universe_for_day(day)
    conf_tier_c_setups = _active_conf_tier_c_setups()
    started = time.perf_counter()
    try:
        market_ctx = candidate_scan.build_market_context_once()
    except Exception as exc:
        market_ctx = {}
        print(f"[{SESSION_NAME}] market context failed: {type(exc).__name__}: {exc}", flush=True)
    _t_after_ctx = time.perf_counter()

    scan_start_lag_sec = max(0.0, (base_v15.now_ist() - slot.to_pydatetime()).total_seconds())
    tier123_for_slot = _legacy_tier123_eligible(scan_start_lag_sec)
    if V11_BACKTESTING_OVERLAY_INCLUDE_TIER123 and _CONF_SCANNER_ACTIVE:
        print(
            f"[{SESSION_NAME} timing] legacy tier123 skipped: "
            "final_setup_conf uses native/dedicated causal detectors",
            flush=True,
        )
    elif V11_BACKTESTING_OVERLAY_INCLUDE_TIER123 and not tier123_for_slot:
        print(
            f"[{SESSION_NAME} timing] tier123 skipped: scan_start_lag={scan_start_lag_sec:.1f}s "
            f"> latest_start_lag={TIER123_LATEST_START_LAG_SEC:.1f}s",
            flush=True,
        )

    tier123_executor: Optional[ThreadPoolExecutor] = None
    tier123_future = None
    tier123_started = time.perf_counter()
    if (
        PARALLEL_SCAN_BRANCHES_ENABLE
        and V11_BACKTESTING_OVERLAY_ENABLE
        and tier123_for_slot
        and not _CONF_SCANNER_ACTIVE
    ):
        tier123_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="v7-tier123")
        tier123_future = tier123_executor.submit(
            v11_live_overlay.scan_tier123_live_slot,
            slot,
            tickers,
            market_ctx=market_ctx,
            profile=V11_BACKTESTING_OVERLAY_PROFILE,
            max_workers=TIER123_SCAN_WORKERS,
        )

    conf_tier_c_executor: Optional[ThreadPoolExecutor] = None
    conf_tier_c_future = None
    conf_tier_c_started = time.perf_counter()
    if conf_tier_c_setups and PARALLEL_SCAN_BRANCHES_ENABLE:
        conf_tier_c_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="v7-conf-tierc-branch")
        conf_tier_c_future = conf_tier_c_executor.submit(
            _conf_tier_c.scan_conf_tier_c_live_slot,
            slot,
            tickers,
            market_ctx=market_ctx,
            max_workers=scan_workers,
        )

    try:
        scanned_candidates = candidate_scan.scan_slot_candidates(
            slot,
            tickers,
            market_ctx,
            max_workers=scan_workers,
            dedupe=not V11_BACKTESTING_OVERLAY_ENABLE,
        )
        candidates = (
            candidate_scan._dedupe_candidate_frame(scanned_candidates)
            if V11_BACKTESTING_OVERLAY_ENABLE
            else scanned_candidates
        )
    except Exception as exc:
        print(f"[{SESSION_NAME}] scan failed: {type(exc).__name__}: {exc}", flush=True)
        candidates = pd.DataFrame()
        scanned_candidates = pd.DataFrame()
    _t_after_native_scan = time.perf_counter()
    if conf_tier_c_setups:
        # conf mode: add the 4 Tier-C setups (no native detector) via their
        # dedicated live causal scanner, merged into the candidate stream before
        # ranking + the conf gate. Run this independent full-universe branch in
        # parallel with the native scan. The legacy v11 tier123 overlay is skipped.
        try:
            tier_c = (
                conf_tier_c_future.result()
                if conf_tier_c_future is not None
                else _conf_tier_c.scan_conf_tier_c_live_slot(
                    slot, tickers, market_ctx=market_ctx, max_workers=scan_workers,
                )
            )
            if tier_c is not None and not tier_c.empty:
                candidates = (
                    pd.concat([candidates, tier_c], ignore_index=True)
                    if candidates is not None and not candidates.empty
                    else tier_c
                )
                candidates = candidate_scan._dedupe_candidate_frame(candidates)
                print(f"[{SESSION_NAME}] conf tier-c scan added {len(tier_c)} candidates", flush=True)
        except Exception as exc:
            print(f"[{SESSION_NAME}] conf tier-c scan failed: {type(exc).__name__}: {exc}", flush=True)
        finally:
            if conf_tier_c_executor is not None:
                conf_tier_c_executor.shutdown(wait=True)
        print(
            f"[{SESSION_NAME} timing] conf_tier_c_branch="
            f"{time.perf_counter()-conf_tier_c_started:.3f}s "
            f"parallel={bool(conf_tier_c_future is not None)} "
            f"setups={','.join(sorted(conf_tier_c_setups))}",
            flush=True,
        )
    _t_after_scan = time.perf_counter()
    print(
        f"[{SESSION_NAME} timing] market_ctx={_t_after_ctx-started:.3f}s "
        f"native_scan={_t_after_native_scan-_t_after_ctx:.3f}s "
        f"required_scan_wall={_t_after_scan-_t_after_ctx:.3f}s",
        flush=True,
    )
    raw_candidates = add_live_ranker_scores(candidates.copy() if candidates is not None else pd.DataFrame(), day)
    raw_candidates_for_v11 = add_live_ranker_scores(
        scanned_candidates.copy() if scanned_candidates is not None else pd.DataFrame(),
        day,
    )
    v11_overlay_candidates = pd.DataFrame()
    v11_overlay_rejected = pd.DataFrame()
    v11_overlay_stats: Dict[str, Any] = {
        "v11_live_overlay_enabled": bool(V11_BACKTESTING_OVERLAY_ENABLE),
        "v11_live_overlay_profile": V11_BACKTESTING_OVERLAY_PROFILE,
        "v11_live_overlay_include_tier123": bool(V11_BACKTESTING_OVERLAY_INCLUDE_TIER123),
        "v11_live_overlay_tier123_eligible_this_slot": bool(tier123_for_slot),
        "v11_live_overlay_scan_start_lag_sec": round(scan_start_lag_sec, 3),
        "v11_live_overlay_tier123_latest_start_lag_sec": float(TIER123_LATEST_START_LAG_SEC),
        "v11_live_overlay_raw_all_setup_candidates": int(0 if raw_candidates_for_v11 is None else len(raw_candidates_for_v11)),
    }
    if V11_BACKTESTING_OVERLAY_ENABLE:
        try:
            v11_profile_candidates, v11_profile_rejected, v11_profile_stats = (
                v11_live_overlay.apply_live_candidate_overlay(
                    raw_candidates_for_v11,
                    profile=V11_BACKTESTING_OVERLAY_PROFILE,
                    ab_gate_profile=V11_BACKTESTING_OVERLAY_AB_GATE_PROFILE,
                )
            )
            tier_candidates = pd.DataFrame()
            tier_rejected = pd.DataFrame()
            tier_stats: Dict[str, Any] = {
                "v11_tier123_live_enabled": bool(tier123_for_slot),
                "v11_tier123_live_scan_mode": (
                    "pending"
                    if tier123_for_slot
                    else (
                        "final_setup_conf_dedicated_detectors"
                        if _CONF_SCANNER_ACTIVE
                        else "deadline_budget_skipped"
                    )
                ),
                "v11_tier123_live_skip_reason": (
                    ""
                    if tier123_for_slot
                    else (
                        "final_setup_conf uses native/dedicated causal detectors"
                        if _CONF_SCANNER_ACTIVE
                        else (
                            f"scan_start_lag {scan_start_lag_sec:.1f}s exceeded "
                            f"{TIER123_LATEST_START_LAG_SEC:.1f}s"
                        )
                    )
                ),
            }
            if tier123_for_slot:
                if tier123_future is not None:
                    tier_candidates, tier_rejected, tier_stats = tier123_future.result()
                else:
                    tier123_started = time.perf_counter()
                    tier_candidates, tier_rejected, tier_stats = v11_live_overlay.scan_tier123_live_slot(
                        slot,
                        tickers,
                        market_ctx=market_ctx,
                        profile=V11_BACKTESTING_OVERLAY_PROFILE,
                        max_workers=TIER123_SCAN_WORKERS,
                    )
            print(
                f"[{SESSION_NAME} timing] "
                f"tier123_branch_elapsed="
                f"{(time.perf_counter()-tier123_started) if tier123_for_slot else 0.0:.3f}s "
                f"parallel_branches={bool(tier123_future is not None)} "
                f"tier123_for_slot={tier123_for_slot}",
                flush=True,
            )
            overlay_frames = [df for df in (v11_profile_candidates, tier_candidates) if df is not None and not df.empty]
            rejected_frames = [df for df in (v11_profile_rejected, tier_rejected) if df is not None and not df.empty]
            v11_overlay_candidates = pd.concat(overlay_frames, ignore_index=True, sort=False) if overlay_frames else pd.DataFrame()
            v11_overlay_rejected = pd.concat(rejected_frames, ignore_index=True, sort=False) if rejected_frames else pd.DataFrame()
            v11_overlay_stats.update(v11_profile_stats)
            v11_overlay_stats.update(tier_stats)
            v11_overlay_stats.update(
                {
                    "v11_live_overlay_selected_total": int(len(v11_overlay_candidates)),
                    "v11_live_overlay_rejected_total": int(len(v11_overlay_rejected)),
                }
            )
        except Exception as exc:
            v11_overlay_candidates = pd.DataFrame()
            v11_overlay_rejected = pd.DataFrame()
            v11_overlay_stats.update(
                {
                    "v11_live_overlay_error": f"{type(exc).__name__}: {exc}",
                    "v11_live_overlay_selected_total": 0,
                    "v11_live_overlay_rejected_total": 0,
                }
            )
        finally:
            if tier123_executor is not None:
                tier123_executor.shutdown(wait=True)
    _t_postproc_start = time.perf_counter()
    v8_candidates, gate_stats = apply_v8_live_gate(raw_candidates)
    # Tag V8-sourced candidates so rejection reports can identify which source
    # was affected by downstream policy filters.
    if v8_candidates is not None and not v8_candidates.empty and "candidate_source" not in v8_candidates.columns:
        v8_candidates = v8_candidates.copy()
        v8_candidates["candidate_source"] = "V7_V8"
    pre_merge_v8_count = int(0 if v8_candidates is None else len(v8_candidates))
    # Merge V11 candidates BEFORE applying research/policy filters so that every
    # candidate — regardless of source — passes through short-focus, anti-chase,
    # and probation checks exactly once.
    if V11_BACKTESTING_OVERLAY_ENABLE:
        merged_candidates = v11_live_overlay.merge_v7_and_v11_candidates(
            v8_candidates,
            v11_overlay_candidates,
            profile=V11_BACKTESTING_OVERLAY_PROFILE,
        )
    else:
        merged_candidates = v8_candidates if v8_candidates is not None else pd.DataFrame()
    gated_candidates, research_rejected, research_filter_stats = apply_research_live_filters(merged_candidates, day)
    post_research_candidates = (
        gated_candidates.copy()
        if gated_candidates is not None
        else pd.DataFrame()
    )
    research_rejected_pre_conf = (
        research_rejected.copy()
        if research_rejected is not None
        else pd.DataFrame()
    )
    _conf_rejected = pd.DataFrame()
    if _CONF_SCANNER_ACTIVE:
        # (i) match-v11: native conf setups have now passed v8 + overlay + research
        # exactly as in v11. Readmit the NON-native conf setups (RAW_PRE_GATE_POOL /
        # TIER123 / NEW_SETUPS) straight from raw_candidates — bypassing v8 + research,
        # mirroring v11 _FINAL_CONF_READMIT_SETUPS — then apply the conf mask as the
        # final selection (drops non-conf setups and conf setups failing their mask).
        _readmit = _conf_boot.readmit_setups()
        if _readmit and raw_candidates is not None and not raw_candidates.empty and "setup" in raw_candidates.columns:
            _rd = raw_candidates[raw_candidates["setup"].astype(str).isin(_readmit)]
            if not _rd.empty:
                gated_candidates = pd.concat(
                    [gated_candidates if gated_candidates is not None else pd.DataFrame(), _rd],
                    ignore_index=True, sort=False,
                )
                if "candidate_id" in gated_candidates.columns:
                    gated_candidates = candidate_scan._dedupe_candidate_frame(gated_candidates)
        gated_candidates, _conf_rejected, _conf_stats = _conf_boot.apply_conf_gate(gated_candidates, day)
        research_filter_stats.update(_conf_stats)
        if _conf_rejected is not None and not _conf_rejected.empty:
            research_rejected = pd.concat(
                [research_rejected if research_rejected is not None else pd.DataFrame(), _conf_rejected],
                ignore_index=True, sort=False,
            )
    v11_overlay_stats["regular_v7_gated_candidates_before_v11_override"] = pre_merge_v8_count
    v11_overlay_stats["post_merge_pre_filter_candidates"] = int(0 if merged_candidates is None else len(merged_candidates))
    v11_overlay_stats["final_candidates_before_entry_window"] = int(0 if gated_candidates is None else len(gated_candidates))
    pre_entry_window_candidates = (
        gated_candidates.copy()
        if gated_candidates is not None
        else pd.DataFrame()
    )
    gated_candidates, entry_window_rejected_final = _filter_entry_window(gated_candidates)
    entry_window_rejected_df = decision_funnel.difference_frame(
        pre_entry_window_candidates,
        gated_candidates,
    )
    v11_overlay_stats["final_candidates_after_v11_override"] = int(len(gated_candidates))
    _t_write_start = time.perf_counter()

    # Publish the authoritative final snapshot first.  The exact-slot completion
    # marker is written only after the JSON and latest pointer are durable; the
    # entry engine watches this marker and never consumes a prior slot.
    decision_ready_at_ist = _fmt_slot(pd.Timestamp.now(tz=base_v15.IST))
    if gated_candidates is not None and not gated_candidates.empty:
        gated_candidates = gated_candidates.copy()
        gated_candidates["bar_closed_at_ist"] = _fmt_slot(slot)
        gated_candidates["decision_ready_at_ist"] = decision_ready_at_ist
        gated_candidates["candidate_schema_version"] = CANDIDATE_SCHEMA_VERSION
    _write_json_snapshots(
        gated_candidates,
        slot,
        payload_extra={
            "v8_live_gate_output": "gated_for_entry_engine_with_v11_backtesting_overlay",
            "decision_ready_at_ist": decision_ready_at_ist,
            "slot_complete_marker_required": True,
            **gate_stats,
            **research_filter_stats,
            **v11_overlay_stats,
        },
    )
    slot_complete_marker = _write_slot_complete_marker(
        slot,
        decision_ready_at_ist=decision_ready_at_ist,
        candidate_count=int(0 if gated_candidates is None else len(gated_candidates)),
    )

    raw_write_stats = _append_daily_raw_candidates(raw_candidates, day)
    research_rejected_write_stats = _append_daily_research_filter_rejections(research_rejected, day)
    v11_overlay_write_stats = _append_daily_v11_overlay_candidates(v11_overlay_candidates, day)
    v11_overlay_rejected_write_stats = _append_daily_v11_overlay_rejections(v11_overlay_rejected, day)
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
        v11_overlay_candidates,
        slot,
        slot_json_path=_v11_overlay_slot_json_path(slot),
        latest_json_name="latest_v11_overlay_candidate_tickers.json",
        latest_csv_name="latest_v11_overlay_candidate_tickers.csv",
        payload_extra={"v8_live_gate_output": "v11_backtesting_overlay_passed", **v11_overlay_stats},
    )
    _write_json_snapshots(
        research_rejected,
        slot,
        slot_json_path=_research_filter_rejected_slot_json_path(slot),
        latest_json_name="latest_research_filter_rejected_candidate_tickers.json",
        latest_csv_name="latest_research_filter_rejected_candidate_tickers.csv",
        payload_extra={"v8_live_gate_output": "research_live_filter_rejected", **gate_stats, **research_filter_stats},
    )
    _write_json_snapshots(
        v11_overlay_rejected,
        slot,
        slot_json_path=_v11_overlay_rejected_slot_json_path(slot),
        latest_json_name="latest_v11_overlay_rejected_candidate_tickers.json",
        latest_csv_name="latest_v11_overlay_rejected_candidate_tickers.csv",
        payload_extra={"v8_live_gate_output": "v11_backtesting_overlay_rejected", **v11_overlay_stats},
    )

    funnel_records: List[Dict[str, Any]] = []
    funnel_stages = (
        (raw_candidates_for_v11, "raw_detection", "PASS", ""),
        (v8_candidates, "v8_gate", "PASS", ""),
        (
            decision_funnel.difference_frame(raw_candidates, v8_candidates),
            "v8_gate",
            "REJECT",
            "v8_live_gate_rejected",
        ),
        (v11_overlay_candidates, "v11_overlay", "PASS", ""),
        (v11_overlay_rejected, "v11_overlay", "REJECT", "v11_overlay_rejected"),
        (merged_candidates, "overlay_merge_replace", "PASS", ""),
        (
            decision_funnel.difference_frame(v8_candidates, merged_candidates),
            "overlay_merge_replace",
            "REJECT",
            "replaced_by_v11_overlay_universe",
        ),
        (post_research_candidates, "research_filter", "PASS", ""),
        (research_rejected_pre_conf, "research_filter", "REJECT", "research_filter_rejected"),
        (pre_entry_window_candidates, "final_setup_conf", "PASS", ""),
        (_conf_rejected, "final_setup_conf", "REJECT", "final_setup_conf_rejected"),
        (gated_candidates, "entry_window", "PASS", ""),
        (entry_window_rejected_df, "entry_window", "REJECT", "entry_window_rejected"),
        (gated_candidates, "scanner_final", "PASS", ""),
    )
    for frame, stage, outcome, reason in funnel_stages:
        funnel_records.extend(
            decision_funnel.records_for_stage(
                frame,
                stage=stage,
                outcome=outcome,
                recorded_at_ist=decision_ready_at_ist,
                default_reason=reason,
            )
        )
    decision_funnel_rows = decision_funnel.write_slot_funnel(
        records=funnel_records,
        csv_path=_slot_funnel_csv_path(slot),
        jsonl_path=_slot_funnel_jsonl_path(slot),
    )

    total = int(0 if gated_candidates is None else len(gated_candidates))
    raw_total = int(0 if raw_candidates is None else len(raw_candidates))
    raw_all_setup_total = int(0 if raw_candidates_for_v11 is None else len(raw_candidates_for_v11))
    long_count = int(0 if gated_candidates is None or gated_candidates.empty else (gated_candidates["side"].astype(str).str.upper() == "LONG").sum())
    short_count = int(0 if gated_candidates is None or gated_candidates.empty else (gated_candidates["side"].astype(str).str.upper() == "SHORT").sum())
    summary = {
        "universe_size": int(len(tickers)),
        "candidate_count": total,
        "raw_candidate_count": raw_total,
        "raw_all_setup_candidate_count": raw_all_setup_total,
        "long_candidates": long_count,
        "short_candidates": short_count,
        "written": int(write_stats["written"]),
        "duplicates": int(write_stats["duplicates"]),
        "raw_written": int(raw_write_stats["written"]),
        "raw_duplicates": int(raw_write_stats["duplicates"]),
        "research_filter_rejected_written": int(research_rejected_write_stats["written"]),
        "research_filter_rejected_duplicates": int(research_rejected_write_stats["duplicates"]),
        "v11_overlay_candidates": int(len(v11_overlay_candidates)),
        "v11_overlay_rejected": int(len(v11_overlay_rejected)),
        "v11_overlay_written": int(v11_overlay_write_stats["written"]),
        "v11_overlay_duplicates": int(v11_overlay_write_stats["duplicates"]),
        "v11_overlay_rejected_written": int(v11_overlay_rejected_write_stats["written"]),
        "v11_overlay_rejected_duplicates": int(v11_overlay_rejected_write_stats["duplicates"]),
        "decision_ready_at_ist": decision_ready_at_ist,
        "slot_complete_marker": str(slot_complete_marker),
        "decision_funnel_rows": int(decision_funnel_rows),
        "decision_funnel_csv": str(_slot_funnel_csv_path(slot)),
        "entry_window_start": ENTRY_WINDOW_START_TIME,
        "entry_window_end": ENTRY_WINDOW_END_TIME,
        "entry_signal_to_entry_lag_min": int(ENTRY_SIGNAL_TO_ENTRY_LAG_MIN),
        "entry_window_rejected_final": int(entry_window_rejected_final),
        "elapsed_sec": round(time.perf_counter() - started, 3),
        "_timing_postproc_sec": round(_t_write_start - _t_postproc_start, 3),
        "_timing_writes_sec": round(time.perf_counter() - _t_write_start, 3),
        "csv_path": str(_daily_csv_path(day)),
        "raw_csv_path": str(_daily_raw_csv_path(day)),
        "research_filter_rejected_csv_path": str(_daily_research_filter_rejected_csv_path(day)),
        "v11_overlay_csv_path": str(_daily_v11_overlay_csv_path(day)),
        "v11_overlay_rejected_csv_path": str(_daily_v11_overlay_rejected_csv_path(day)),
        "json_path": str(_slot_json_path(slot)),
        **gate_stats,
        **research_filter_stats,
        **v11_overlay_stats,
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
    ap.add_argument(
        "--production-replay",
        action="store_true",
        help="Allow --replay-slots to write to production paths (default: REFUSED "
             "unless EQIDV2_REPLAY_OUTPUT_ROOT is set)",
    )
    ap.add_argument("--post-slot-delay-sec", type=int, default=POST_SLOT_DELAY_SEC)
    return ap.parse_args()


def main() -> None:
    global _RUNTIME_MANIFEST_PATH
    args = _parse_args()
    manifest_path, _ = freeze_runtime_manifest(
        SESSION_SLUG,
        runtime_root=RUNTIME_ROOT,
        source_files=(
            Path(__file__),
            Path(candidate_scan.__file__),
            Path(v11_live_overlay.__file__),
            Path(_conf_boot.__file__),
        ),
        resolved_config={
            "session_root": str(SESSION_ROOT),
            "scan_workers": int(args.scan_workers),
            "tier123_scan_workers": int(TIER123_SCAN_WORKERS),
            "parallel_scan_branches": bool(PARALLEL_SCAN_BRANCHES_ENABLE),
            "feed_gate_enable": bool(FEED_GATE_ENABLE),
            "feed_gate_max_wait_sec": int(FEED_GATE_MAX_WAIT_SEC),
            "entry_window_start": ENTRY_WINDOW_START_TIME,
            "entry_window_end": ENTRY_WINDOW_END_TIME,
            "entry_signal_to_entry_lag_min": int(ENTRY_SIGNAL_TO_ENTRY_LAG_MIN),
            "v11_overlay_enabled": bool(V11_BACKTESTING_OVERLAY_ENABLE),
            "v11_overlay_profile": V11_BACKTESTING_OVERLAY_PROFILE,
        },
    )
    _RUNTIME_MANIFEST_PATH = str(manifest_path)
    holidays = base_v15._read_holidays_safe()
    print(f"[LIVE] {SESSION_NAME}", flush=True)
    print(f"[INFO] root={SESSION_ROOT}", flush=True)
    print(f"[INFO] frozen_runtime_manifest={manifest_path}", flush=True)
    print(
        f"[INFO] scan_workers={int(args.scan_workers)} "
        f"tier123_scan_workers={int(TIER123_SCAN_WORKERS)} "
        f"parallel_scan_branches={bool(PARALLEL_SCAN_BRANCHES_ENABLE)} "
        f"tier123_latest_start_lag={TIER123_LATEST_START_LAG_SEC:g}s "
        f"post_slot_delay={int(args.post_slot_delay_sec)}s",
        flush=True,
    )
    print(
        f"[INFO] entry_window={ENTRY_WINDOW_START_TIME}-{ENTRY_WINDOW_END_TIME} "
        f"signal_to_entry_lag={ENTRY_SIGNAL_TO_ENTRY_LAG_MIN}m",
        flush=True,
    )
    if FEED_GATE_ENABLE:
        print(
            f"[INFO] feed_gate=ON status={FEED_STATUS_JSON} "
            f"max_wait={FEED_GATE_MAX_WAIT_SEC}s poll={FEED_GATE_POLL_SEC}s "
            f"min_delay={FEED_GATE_MIN_DELAY_SEC}s "
            f"max_verification_failures={FEED_GATE_MAX_VERIFICATION_FAILURES}",
            flush=True,
        )
    else:
        print(f"[INFO] feed_gate=OFF (fixed post_slot_delay={int(args.post_slot_delay_sec)}s)", flush=True)

    prewarm_slot = (
        _ensure_ist_ts(args.replay_slots[0])
        if args.replay_slots
        else _next_slot_after(base_v15.now_ist())
    )
    try:
        prewarm_stats = candidate_scan.prewarm_scan_pool(
            prewarm_slot,
            max_workers=int(args.scan_workers),
        )
        print(
            f"[INFO] candidate scanner pool prewarmed for "
            f"{prewarm_slot.strftime('%Y-%m-%d %H:%M%z')}: "
            f"requested={int(prewarm_stats.get('workers_requested', 0))} "
            f"pids_seen={int(prewarm_stats.get('worker_pids_seen', 0))} "
            f"elapsed={float(prewarm_stats.get('seconds', 0.0)):.3f}s",
            flush=True,
        )
    except Exception as exc:
        print(
            f"[WARN] candidate scanner pool prewarm failed: "
            f"{type(exc).__name__}: {exc}; first scan will retry pool startup",
            flush=True,
        )

    if args.replay_slots:
        if _is_replay_isolated:
            print(f"[REPLAY] isolated output root={SESSION_ROOT}", flush=True)
        elif args.production_replay:
            print(
                "[REPLAY][WARN] --production-replay set: writing to PRODUCTION paths.",
                flush=True,
            )
        else:
            print(
                "[ERROR] --replay-slots refused: would overwrite production outputs.\n"
                "  Option A (recommended): set EQIDV2_REPLAY_OUTPUT_ROOT=<replay_dir>\n"
                "  Option B (dangerous):   pass --production-replay to confirm.",
                flush=True,
            )
            sys.exit(1)
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

        if FEED_GATE_ENABLE:
            print(
                f"[WAIT] slot={slot.strftime('%H:%M')} waiting for 5-min feed completion "
                f"(max {FEED_GATE_MAX_WAIT_SEC}s)",
                flush=True,
            )
            feed_ready = _wait_for_feed_slot(slot)
            if not feed_ready and FEED_TIMEOUT_ACTION == "reject_slot":
                print(
                    f"[FEED][SKIP] slot={slot.strftime('%H:%M')} feed timed out; "
                    f"slot skipped (FEED_TIMEOUT_ACTION=reject_slot)",
                    flush=True,
                )
                _touch_status("RUNNING", phase="FEED_TIMEOUT", slot=slot.strftime("%H:%M"))
                _touch_heartbeat("RUNNING", phase="FEED_TIMEOUT", slot=slot.strftime("%H:%M"))
                next_slot = slot + timedelta(minutes=SLOT_MINUTES)
                if base_v15.now_ist() < next_slot:
                    time.sleep(1.0)
                continue
        else:
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
