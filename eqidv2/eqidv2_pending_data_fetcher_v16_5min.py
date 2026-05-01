# -*- coding: utf-8 -*-
"""
EQIDV2 — V16 5min Pending Data Fetcher (Stage 4)
=================================================
Fetches fresh 5-min OHLCV + indicators via Kite API for ONLY the tickers
currently in the pending signal pool. Runs every 60 seconds.

Why this is fast:
  Full fetcher : 1044 tickers × Kite API → ~2-3 minutes per slot
  This fetcher : ~5-30 tickers × Kite API → ~5-15 seconds per run
  → Can run every 1 minute without overloading Kite API

Outputs:
  stocks_indicators_5min_eq_live/{TICKER}_stocks_indicators_5min.parquet
  slot_ready_5m_pending/{YYYYMMDD_HHMM}.ready

Status files (to logs/):
  eqidv2_pending_data_fetcher_v16_5min.log
  eqidv2_pending_data_fetcher_v16_5min.status
  eqidv2_pending_data_fetcher_v16_5min.heartbeat

STRATEGY V2 — DESIGN INVARIANTS (see strategy_v2.txt §F)
========================================================
F1. Pool row monotonicity:
    PF never mutates (ticker, side, setup, entry_slot, entry_price, stop_price)
    of a signal row. Only `status`, `filter_reason`, and PF-owned outcome
    fields may change (status∈{pending, filtered_missing_token,
    filtered_verify_failed}).
F2. PF/DE slot equality:
    PF's ready-marker file name IS the entry-slot key. A marker for slot Y
    never claims data for Y-5 or Y+5. Older-slot markers are emitted per
    distinct pending source_slot.
F3. No writes for past entry_slots:
    SE (not PF) enforces this. PF forwards whatever SE wrote — if a past
    slot sneaks in, the DE freshness gate catches it.
F4. One-writer-many-readers:
    PF is the only writer of slot_ready_5m_pending/*.ready.
    PF updates the pool JSON only for status transitions
    (pending → filtered_missing_token / filtered_verify_failed).
    SE is the canonical writer of new pool rows; PF never inserts.
F5. Slot is the OPEN time:
    `ready_slot` / `source_slot` / marker filename all use bar-OPEN time.

PF-specific strategy-v2 hooks (see strategy_v2.txt):
  B3  Kite fetch retry (1s / 2s / 4s) wraps `scheduler.run_update_5m_once`.
  E2  pool_lifecycle_<date>_v16_5min.jsonl append-only audit:
        DROPPED    on missing_token / verify_failed transitions
        PF_VERIFIED on ready-marker write
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pytz

# ---------------------------------------------------------------------------
# Runtime paths — must import BEFORE the core module which reads DATA_5M_DIR
# ---------------------------------------------------------------------------
from eqidv2_runtime_paths import (
    DATA_5M_DIR as RUNTIME_DATA_5M_DIR,
    SLOT_READY_PENDING_DIR,
    LIVE_SIGNALS_DIR as RUNTIME_LIVE_SIGNALS_DIR,
    RUNTIME_STATUS_DIR,
)

# strategy_v2 §C3 — cross-process pool-JSON lock (see eqidv2_pool_lock.py).
# PF shares the same lockfile namespace as SE and DE; every status-transition
# write sequence (load → mutate → atomic replace) must run inside pool_lock.
from eqidv2_pool_lock import pool_lock, bump_pool_rev, lifecycle_lock

# ---------------------------------------------------------------------------
# Reuse the full scheduler's fetch infrastructure (core.run_mode, session setup)
# ---------------------------------------------------------------------------
import eqidv2_eod_scheduler_for_5mins_data_live_minimal as scheduler

# We borrow core from the scheduler so all its patches (freshness, universe, etc.) are in place
core = scheduler.core

IST = pytz.timezone("Asia/Kolkata")

# ===========================================================================
# CONSTANTS
# ===========================================================================
SCRIPT_DIR   = Path(__file__).resolve().parent
_LOG_DIR     = SCRIPT_DIR / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)

_SCRIPT_NAME = "eqidv2_pending_data_fetcher_v16_5min.py"
_LOG_FILE    = _LOG_DIR / "eqidv2_pending_data_fetcher_v16_5min.log"

PENDING_JSON_PATTERN  = "pending_signals_{}_v16_5min.json"
POOL_LIFECYCLE_PATTERN = "pool_lifecycle_{}_v16_5min.jsonl"
END_5M                = "_stocks_indicators_5min.parquet"
TOKENS_CACHE_PATH     = SCRIPT_DIR / "stocks_tokens_cache.json"

# B3 — Kite fetch retry (strategy_v2 §B3): default 3 retries 1s/2s/4s; total
# worst-case added latency ~7s (inside the LATE_ENTRY band defined in §B1).
# Env-overridable as a comma-separated list of positive floats (seconds).
def _parse_retry_delays(raw: str) -> tuple:
    out = []
    for p in str(raw or "").split(","):
        p = p.strip()
        if not p:
            continue
        try:
            v = float(p)
        except ValueError:
            continue
        if v > 0:
            out.append(v)
    return tuple(out) if out else (1.0, 2.0, 4.0)


PENDING_FETCH_RETRY_DELAYS_SEC = _parse_retry_delays(
    os.getenv("EQIDV2_PENDING_FETCH_RETRY_DELAYS_SEC", "1,2,4")
)
# When True (default), any ticker verification failure triggers a retry on
# just the still-failing subset (rerouted to a different app via
# partition_shift). When False, retry only fires on a full-slot miss or an
# exception (legacy behavior).
PENDING_FETCH_RETRY_PARTIAL = os.getenv(
    "EQIDV2_PENDING_FETCH_RETRY_PARTIAL", "1"
).strip().lower() not in ("0", "false", "off", "no", "")

FETCH_INTERVAL_SEC    = int(os.getenv("EQIDV2_PENDING_FETCH_INTERVAL_SEC", "60"))
MARKET_OPEN           = dtime(9, 15)
HARD_STOP             = dtime(15, 35)
REPORT_DIR            = str(RUNTIME_DATA_5M_DIR.parent / "reports")
ALIGN_TO_5MIN_BOUNDARY = str(
    os.getenv("EQIDV2_PENDING_FETCH_ALIGN_TO_5MIN", "0")
).strip().lower() not in {"0", "false", "no", "off"}
SLOT_OFFSET_SEC       = int(os.getenv("EQIDV2_PENDING_FETCH_SLOT_OFFSET_SEC", "2"))
SIGNAL_ENGINE_SLOT_START_OFFSET_SEC = int(
    os.getenv("EQIDV16_5MIN_SLOT_START_OFFSET_SECONDS", "45")
)
PENDING_RECHECK_AFTER_SEC = int(
    os.getenv(
        "EQIDV2_PENDING_FETCH_RECHECK_AFTER_SEC",
        str(max(SLOT_OFFSET_SEC, SIGNAL_ENGINE_SLOT_START_OFFSET_SEC + 10)),
    )
)
# strategy_v2 §SEE-A12 fix (2026-04-22): SEE typically finishes by
# slot+45s, but under load (1044-ticker universe, app-quota contention) it
# can lag past the historical slot+55s recheck. Previously PF would give
# up at `PENDING_RECHECK_AFTER_SEC` and sleep to `next_wake` (~4min), so
# any signal written after slot+55s was processed one full 5-min bar
# late. Now PF keeps polling the pool every
# PENDING_POOL_POLL_INTERVAL_SEC until PENDING_POOL_DEADLINE_SEC from
# slot start, then gives up. 240s leaves 60s headroom before the next
# slot boundary for the fetch/verify path. Env-tunable for tight slots
# (10:55 → 11:00 morning-end) if operators want a smaller deadline.
PENDING_POOL_DEADLINE_SEC = int(
    os.getenv("EQIDV2_PENDING_POOL_DEADLINE_SEC", "240")
)
PENDING_POOL_POLL_INTERVAL_SEC = float(
    os.getenv("EQIDV2_PENDING_POOL_POLL_INTERVAL_SEC", "5")
)

# A closed-row presence check is not enough in live parquet because some feeds
# can materialize the forming 5-minute row before its close. Keep PF markers
# behind the documented entry-slot clock regardless of row presence.
BLOCK_EARLY_MARKERS = str(
    os.getenv("EQIDV16_5MIN_BLOCK_EARLY_MARKERS", "1")
).strip().lower() not in {"0", "false", "no", "off", ""}

# Max concurrent workers for the pending-only refresh using the shared 8-app fetch path.
PENDING_MAX_WORKERS     = int(os.getenv("EQIDV2_PENDING_FETCH_MAX_WORKERS", "8"))
PENDING_MAX_WORKERS_PER_APP = int(os.getenv("EQIDV2_PENDING_FETCH_MAX_WORKERS_PER_APP", "8"))

# A2 fix (partial-ready marker): after this many consecutive verify failures for
# the same ticker within the same source-slot, give up on that ticker for the
# slot — mark it `filtered_verify_failed` in the pending state and let the
# marker proceed with the verified subset. Prevents a single persistently
# failing ticker from starving confirmations for every other pending ticker.
# Set to 0 to disable (restores old behaviour: marker withheld on any failure).
VERIFY_FAIL_MAX_RETRIES = int(os.getenv("EQIDV2_PENDING_VERIFY_FAIL_MAX_RETRIES", "2"))

# PF-TRIGGER-FIX (2026-04-23): SE stamps `source_slot = entry_bar_open` (F5).
# Historically PF verified that bar `source_slot` was present in the parquet
# before publishing `<source_slot>.ready`. That bar only closes at
# source_slot+5min, so the marker could never publish before the entry bar
# itself closed — injecting a hard 5-min lag per slot (observed today on
# ANUP 09:55 and DEEPINDS 10:05). The rescan only needs data through the
# TRIGGER bar (source_slot-5min, which closes AT source_slot), so verifying
# that bar lets the marker publish at ~source_slot+fetch_time. Filename and
# payload slot remain source_slot so DE's exact-slot match is unchanged.
# Flip to 0 to revert to pre-fix behaviour.
PF_VERIFY_TRIGGER_BAR = str(
    os.getenv("EQIDV16_5MIN_PF_VERIFY_TRIGGER_BAR", "1")
).strip().lower() not in {"0", "false", "no", "off"}


# Stale-marker expiry: refuse to publish a marker for a slot that's already
# older than MAX_PF_MARKER_LAG_SEC. Without this guard, a PF stall + recovery
# can publish markers for slots 30-40min in the past, which the executor's
# late-detection gate then silently rejects (one such cascade on 2026-04-27
# cost 36 ENTRY_SKIPPED_STALE_DETECTION). Guard fires before the parquet
# verify so even stale-but-verifiable signals are dropped at PF, not DE.
# Default 90s leaves headroom for healthy fetch (~2-5s) plus brief Kite blips.
MAX_PF_MARKER_LAG_SEC = max(
    1, int(os.getenv("EQIDV16_5MIN_MAX_PF_MARKER_LAG_SEC", "90"))
)


# ===========================================================================
# STDOUT TEE
# ===========================================================================
class _Tee:
    def __init__(self, *streams):
        self._streams = streams

    def write(self, data):
        for s in self._streams:
            try:
                s.write(data)
                s.flush()
            except Exception:
                pass

    def flush(self):
        for s in self._streams:
            try:
                s.flush()
            except Exception:
                pass


def _start_tee() -> None:
    try:
        for name in ("stdout", "stderr", "__stdout__", "__stderr__"):
            stream = getattr(sys, name, None)
            if stream and hasattr(stream, "reconfigure"):
                try:
                    stream.reconfigure(encoding="utf-8", errors="replace")
                except Exception:
                    pass
        fh = open(_LOG_FILE, "a", encoding="utf-8", buffering=1)
        base_stdout = getattr(sys, "__stdout__", None) or sys.stdout
        base_stderr = getattr(sys, "__stderr__", None) or sys.stderr
        sys.stdout = _Tee(base_stdout, fh)  # type: ignore[assignment]
        sys.stderr = _Tee(base_stderr, fh)  # type: ignore[assignment]
    except Exception:
        pass


# ===========================================================================
# STATUS / HEARTBEAT (reuse scheduler's base_v15 helpers via scheduler module)
# ===========================================================================
def _now_ist() -> datetime:
    return datetime.now(IST)


def _floor_to_5m(dt: datetime) -> datetime:
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)


def _parse_ist_timestamp(raw: Any) -> Optional[datetime]:
    text = str(raw or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.astimezone(IST)
        except ValueError:
            continue
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return IST.localize(datetime.strptime(text, fmt))
        except ValueError:
            continue
    return None


def _pending_signal_source_slot(sig: Dict[str, Any]) -> Optional[datetime]:
    candidates = (
        sig.get("source_slot"),
        sig.get("signal_entry_datetime_ist"),
        sig.get("signal_datetime"),
    )
    for raw in candidates:
        slot_ts = _parse_ist_timestamp(raw)
        if slot_ts is not None:
            return _floor_to_5m(slot_ts.astimezone(IST))
    return None


# strategy_v2 §F2 — PF marker filename MUST equal DE's _pending_signal_source_slot
# output, which is `source_slot + (lag-1)*5min`. Without this shift, every setup
# with lag>=2 (e.g. B_HUGE_C1_CLOSE_RECLAIM_BREAK, A_MOD_CLOSE_CONTINUATION_BREAK)
# produces a marker DE never matches — the signal stalls until
# LATE_DETECTION_MAX_LAG_SEC drops it. Mirrors detection_engine_v16_5min.py
# SETUP_LAG_BARS exactly, including the documented emit-name vs runner-name
# mismatch: setups whose SEE emit-names aren't in this dict fall through to the
# default lag=1, identical to DE's `SETUP_LAG_BARS.get(setup, 1)`. PF/DE shift
# parity is the load-bearing invariant; do not diverge from DE's table here.
try:
    import avwap_combined_runner_v16_5min as v16_runner
except Exception:
    v16_runner = None  # type: ignore[assignment]


SETUP_LAG_BARS: Dict[str, int] = {
    "A_MOD_BREAK_C1_HIGH":            int(getattr(v16_runner, "LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH", 1)),
    "A_PULLBACK_C2_BREAK_C2_HIGH":    int(getattr(v16_runner, "LONG_LAG_BARS_A_PULLBACK_C2_BREAK_C2_HIGH", 2)),
    "B_HUGE_PULLBACK_HOLD_BREAK":     int(getattr(v16_runner, "LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK", 999)),
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK":  int(getattr(v16_runner, "LONG_LAG_BARS_B_HUGE_C1_CLOSE_RECLAIM_BREAK", 2)),
    "A_MOD_CLOSE_CONTINUATION_BREAK": int(getattr(v16_runner, "LONG_LAG_BARS_A_MOD_CLOSE_CONTINUATION_BREAK", 2)),
    "A_MOD_BREAK_C1_LOW":             int(getattr(v16_runner, "SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW", 1)),
    "A_PULLBACK_C2_BREAK_C2_LOW":     int(getattr(v16_runner, "SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW", 2)),
    "B_HUGE_FAILED_BOUNCE":           int(getattr(v16_runner, "SHORT_LAG_BARS_B_HUGE_FAILED_BOUNCE", -1)),
}


# Entry-bar-verify mode (2026-04-24): when
# EQIDV16_5MIN_DISABLE_LAG_SHIFT=1, every setup's trigger_slot = source_slot
# = entry_ist (no lag shift). Paired with PF_VERIFY_TRIGGER_BAR=0, this makes
# PF wait for the ENTRY bar itself to appear in parquet before writing the
# marker, symmetric for lag=1 and lag=2. Matches strategy_map_v16_5min.md.
# Default 0 preserves pre-fix (lag-1)*5min shift.
_DISABLE_LAG_SHIFT = str(
    os.getenv("EQIDV16_5MIN_DISABLE_LAG_SHIFT", "0")
).strip().lower() not in {"0", "false", "no", "off", ""}


def _setup_lag_shift(setup: Any) -> timedelta:
    if _DISABLE_LAG_SHIFT:
        return timedelta(0)
    lag = SETUP_LAG_BARS.get(str(setup or "").strip(), 1)
    if lag <= 0 or lag > 4:
        return timedelta(0)
    return timedelta(minutes=(lag - 1) * 5)


def _pending_signal_trigger_slot(sig: Dict[str, Any]) -> Optional[datetime]:
    """DE-trigger slot for a pending signal — the slot DE seeks when matching
    .ready markers. Equals `source_slot + (lag-1)*5min` floored to 5min, where
    lag is looked up in SETUP_LAG_BARS (DE's table mirror). This is the slot
    PF MUST name `.ready` markers with so DE's exact-slot match fires.
    """
    shift = _setup_lag_shift(sig.get("setup", ""))
    candidates = (
        sig.get("source_slot"),
        sig.get("signal_entry_datetime_ist"),
        sig.get("signal_datetime"),
    )
    for raw in candidates:
        slot_ts = _parse_ist_timestamp(raw)
        if slot_ts is not None:
            return _floor_to_5m((slot_ts + shift).astimezone(IST))
    return None


def _verification_slot_end_for_ready_slot(ready_slot: datetime) -> datetime:
    ready_slot_ist = ready_slot.astimezone(IST)
    return ready_slot_ist + timedelta(minutes=5)


def _touch_status(status: str, **extra: Any) -> None:
    os.environ["EQIDV2_RUNTIME_STATUS_FILE"]    = str(RUNTIME_STATUS_DIR / "eqidv2_pending_data_fetcher_v16_5min.status")
    os.environ["EQIDV2_RUNTIME_HEARTBEAT_FILE"] = str(RUNTIME_STATUS_DIR / "eqidv2_pending_data_fetcher_v16_5min.heartbeat")
    os.environ["EQIDV2_RUNTIME_SCRIPT_NAME"]    = _SCRIPT_NAME
    import eqidv2_live_combined_analyser_csv_v15 as base_v15
    base_v15._touch_runtime_status(status, **extra)


def _touch_heartbeat(state: str = "RUNNING", **extra: Any) -> None:
    import eqidv2_live_combined_analyser_csv_v15 as base_v15
    base_v15._touch_runtime_heartbeat(state, **extra)


# ===========================================================================
# TOKEN CACHE
# ===========================================================================
_token_cache: Optional[Dict[str, int]] = None


def _load_token_cache() -> Dict[str, int]:
    global _token_cache
    if _token_cache is not None:
        return _token_cache
    if not TOKENS_CACHE_PATH.exists():
        print(f"[WARN] stocks_tokens_cache.json not found at {TOKENS_CACHE_PATH}", flush=True)
        _token_cache = {}
        return _token_cache
    try:
        raw = json.loads(TOKENS_CACHE_PATH.read_text(encoding="utf-8"))
        _token_cache = {str(k).upper().strip(): int(v) for k, v in raw.items()}
        print(f"[INFO] Loaded {len(_token_cache)} tokens from cache", flush=True)
    except Exception as exc:
        print(f"[WARN] Failed to load token cache: {exc}", flush=True)
        _token_cache = {}
    return _token_cache


# ===========================================================================
# PENDING SIGNAL STATE
# ===========================================================================
def _today_pending_json_path() -> Path:
    date_str = _now_ist().strftime("%Y-%m-%d")
    return Path(RUNTIME_LIVE_SIGNALS_DIR) / PENDING_JSON_PATTERN.format(date_str)


def _load_today_pending_state() -> Dict[str, Any]:
    path = _today_pending_json_path()
    if not path.exists():
        return {"date": _now_ist().strftime("%Y-%m-%d"), "last_updated": "", "signals": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[WARN] Could not load pending state for update: {exc}", flush=True)
        return {"date": _now_ist().strftime("%Y-%m-%d"), "last_updated": "", "signals": []}


def _write_today_pending_state_atomic(state: Dict[str, Any]) -> None:
    path = _today_pending_json_path()
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    os.replace(tmp_path, path)


# ---------------------------------------------------------------------------
# E2 — pool lifecycle JSONL (append-only audit, see strategy_v2.txt §E2)
# ---------------------------------------------------------------------------
def _pool_lifecycle_path(date_str: str) -> Path:
    return Path(RUNTIME_LIVE_SIGNALS_DIR) / POOL_LIFECYCLE_PATTERN.format(date_str)


def _append_pool_lifecycle_event(date_str: str, event: Dict[str, Any]) -> None:
    """Append a single JSONL event to the daily pool_lifecycle audit file.

    Event shape: {"signal_id": ..., "event": "WRITTEN|PF_VERIFIED|DE_PASSED|DROPPED",
                  "ts": "...", <extra keys>}.
    """
    # Mo2 (2026-04-22) — see eqidv2_pool_lock.lifecycle_lock docstring.
    try:
        path = _pool_lifecycle_path(date_str)
        payload = dict(event)
        payload.setdefault("ts", _now_ist().strftime("%Y-%m-%dT%H:%M:%S%z"))
        line = json.dumps(payload, ensure_ascii=False) + "\n"
        with lifecycle_lock(date_str):
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(line)
    except Exception as exc:
        print(f"[WARN] pool_lifecycle append failed: {exc}", flush=True)


def _emit_dropped_events(
    affected: List[Dict[str, Any]],
    reason: str,
    date_str: str,
) -> None:
    for sig in affected:
        sid = str(sig.get("signal_id", "")).strip()
        if not sid:
            continue
        _append_pool_lifecycle_event(
            date_str,
            {
                "signal_id": sid,
                "event": "DROPPED",
                "reason": reason,
                "ticker": str(sig.get("ticker", "")),
                "side": str(sig.get("side", "")),
                "setup": str(sig.get("setup", "")),
                "source_slot": str(sig.get("source_slot", "")),
            },
        )


def _mark_missing_token_pending_signals(missing_tokens: List[str]) -> int:
    missing_set = {
        str(t).upper().strip()
        for t in missing_tokens
        if str(t).strip()
    }
    if not missing_set:
        return 0

    date_str = _now_ist().strftime("%Y-%m-%d")
    # strategy_v2 §C3 — hold the pool lock across the full read→mutate→write
    # sequence so SE/DE cannot clobber a concurrent update. The lifecycle
    # audit emit happens after the write inside the lock to preserve ordering
    # relative to `pool_rev`.
    with pool_lock(date_str):
        state = _load_today_pending_state()
        signals = state.get("signals", [])
        updated = 0
        now_str = _now_ist().strftime("%Y-%m-%d %H:%M:%S%z")
        affected: List[Dict[str, Any]] = []

        for sig in signals:
            if str(sig.get("status", "")).lower() != "pending":
                continue
            ticker = str(sig.get("ticker", "")).upper().strip()
            if ticker not in missing_set:
                continue
            sig["status"] = "filtered_missing_token"
            sig["filter_reason"] = "no_token_in_stocks_tokens_cache"
            affected.append(sig)
            updated += 1

        if updated > 0:
            state["last_updated"] = now_str
            bump_pool_rev(state)
            _write_today_pending_state_atomic(state)
            _emit_dropped_events(affected, "no_token_in_stocks_tokens_cache", date_str)

    return updated


# A2 fix: per-slot verify-failure retry counts. Keyed by (slot_iso, ticker).
_verify_fail_counts: Dict[Tuple[str, str], int] = {}
_verify_fail_tracked_slot: Optional[str] = None


def _update_verify_fail_counts(
    slot_ts: Optional[datetime],
    failed_tickers: List[str],
) -> List[str]:
    """Increment per-slot retry counts for verify_failed tickers and return
    the list that have exceeded VERIFY_FAIL_MAX_RETRIES (give-up set).

    Returns [] when the feature is disabled or slot_ts is missing.
    """
    global _verify_fail_tracked_slot
    if VERIFY_FAIL_MAX_RETRIES <= 0 or slot_ts is None:
        return []

    slot_key = slot_ts.astimezone(IST).strftime("%Y-%m-%dT%H:%M")
    if _verify_fail_tracked_slot != slot_key:
        _verify_fail_counts.clear()
        _verify_fail_tracked_slot = slot_key

    give_up: List[str] = []
    for raw in failed_tickers:
        t = str(raw).upper().strip()
        if not t:
            continue
        key = (slot_key, t)
        _verify_fail_counts[key] = _verify_fail_counts.get(key, 0) + 1
        if _verify_fail_counts[key] >= VERIFY_FAIL_MAX_RETRIES:
            give_up.append(t)
    return give_up


def _mark_verify_failed_pending_signals(tickers: List[str]) -> int:
    target_set = {
        str(t).upper().strip()
        for t in tickers
        if str(t).strip()
    }
    if not target_set:
        return 0

    date_str = _now_ist().strftime("%Y-%m-%d")
    reason = f"verify_failed_after_{VERIFY_FAIL_MAX_RETRIES}_retries_in_slot"
    # strategy_v2 §C3 — hold the pool lock across the full read→mutate→write
    # sequence; SE/DE could otherwise overwrite the status flip.
    with pool_lock(date_str):
        state = _load_today_pending_state()
        signals = state.get("signals", [])
        updated = 0
        now_str = _now_ist().strftime("%Y-%m-%d %H:%M:%S%z")
        affected: List[Dict[str, Any]] = []

        for sig in signals:
            if str(sig.get("status", "")).lower() != "pending":
                continue
            ticker = str(sig.get("ticker", "")).upper().strip()
            if ticker not in target_set:
                continue
            sig["status"] = "filtered_verify_failed"
            sig["filter_reason"] = reason
            affected.append(sig)
            updated += 1

        if updated > 0:
            state["last_updated"] = now_str
            bump_pool_rev(state)
            _write_today_pending_state_atomic(state)
            _emit_dropped_events(affected, reason, date_str)

    return updated


def _get_pending_fetch_targets() -> Tuple[List[str], Optional[datetime]]:
    """Return pending tickers plus the newest source-slot that must be present."""
    path = _today_pending_json_path()
    if not path.exists():
        return [], None

    latest_source_slot: Optional[datetime] = None
    tickers: List[str] = []
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
        signals = state.get("signals", [])
        for sig in signals:
            if str(sig.get("status", "")).lower() != "pending":
                continue
            ticker = str(sig.get("ticker", "")).upper().strip()
            if not ticker:
                continue
            tickers.append(ticker)
            slot_ts = _pending_signal_source_slot(sig)
            if slot_ts is not None and (latest_source_slot is None or slot_ts > latest_source_slot):
                latest_source_slot = slot_ts
    except Exception as exc:
        print(f"[WARN] Could not read pending state: {exc}", flush=True)
        return [], None

    return tickers, latest_source_slot


def _get_pending_trigger_slot_map() -> Dict[datetime, List[str]]:
    """Return a map from each distinct DE-trigger slot to its tickers.

    Trigger slot = `source_slot + (lag-1)*5min` (see _pending_signal_trigger_slot).
    This is the slot DE matches `.ready` markers against. Used by the per-slot
    marker emission pass so PF emits one marker per trigger slot, each named
    with the slot DE seeks. Without per-trigger-slot fan-out, a single batch
    spanning multiple source_slots and lag values can only ever match one of
    DE's expected slots — every other signal stalls.
    """
    path = _today_pending_json_path()
    if not path.exists():
        return {}
    slot_map: Dict[datetime, List[str]] = {}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
        for sig in state.get("signals", []):
            if str(sig.get("status", "")).lower() != "pending":
                continue
            ticker = str(sig.get("ticker", "")).upper().strip()
            if not ticker:
                continue
            slot_ts = _pending_signal_trigger_slot(sig)
            if slot_ts is None:
                continue
            slot_map.setdefault(slot_ts, [])
            if ticker not in slot_map[slot_ts]:
                slot_map[slot_ts].append(ticker)
    except Exception as exc:
        print(f"[WARN] Could not read pending state for slot map: {exc}", flush=True)
        return {}
    return slot_map


def _slot_row_present_in_parquet(ticker: str, slot_ts: datetime) -> bool:
    """Return True if the parquet for `ticker` already contains `slot_ts`.

    Used by the older-slot marker pass: the parquet is the same one the main
    fetch writes to, so once today's data is fetched, any historical slot_ts
    within today's session is already present and can be re-verified cheaply.
    """
    ticker_u = ticker.upper().strip()
    path = Path(RUNTIME_DATA_5M_DIR) / f"{ticker_u}_stocks_indicators_5min.parquet"
    if not path.exists():
        return False
    try:
        df = pd.read_parquet(path, columns=["date"])
    except Exception:
        return False
    if df.empty:
        return False
    target = pd.Timestamp(slot_ts).tz_convert(IST).floor("5min")
    try:
        dates = pd.to_datetime(df["date"])
        if getattr(dates.dt, "tz", None) is None:
            dates = dates.dt.tz_localize(IST)
        else:
            dates = dates.dt.tz_convert(IST)
        return bool((dates == target).any())
    except Exception:
        return False


def _emit_per_trigger_slot_markers(
    slot_map: Dict[datetime, List[str]],
    eligible_tickers: Optional[set] = None,
) -> List[datetime]:
    """Write a `.ready` marker for every distinct DE-trigger slot whose ticker
    bars are already present in the live parquet. One marker per trigger slot,
    each named with the slot DE seeks.

    Replaces the legacy "single marker at latest_source_slot + older-slot pass"
    that produced one marker per fetch cycle named with the unshifted source
    slot — incompatible with DE's `_pending_signal_source_slot` which shifts
    by `(lag-1)*5min`.

    Does NOT skip slots whose marker file already exists — latecomer tickers
    (signals SEE emits after a marker was first written for the slot) must
    be merged in. Writes the UNION of already-present tickers in the existing
    marker plus the newly verified ones, so previously-emitted tickers are
    preserved across rewrites. Re-writes whose content is unchanged are a
    harmless no-op; DE dedupes by signal_id. Write is atomic via os.replace
    inside _write_ready_marker.

    Partial verification still emits a marker containing only the verified
    tickers (A2-fix behavior). Setups with lag>=2 whose trigger bar hasn't
    formed yet naturally fail the parquet check and roll over to the next
    cycle.
    """
    written: List[datetime] = []
    if not slot_map:
        return written
    now_ist = datetime.now(IST)
    for slot_ts, slot_tickers in sorted(slot_map.items()):
        slot_ist = slot_ts.astimezone(IST)
        marker_path = SLOT_READY_PENDING_DIR / (slot_ist.strftime("%Y%m%d_%H%M") + ".ready")
        if BLOCK_EARLY_MARKERS:
            earliest_marker_ist = slot_ist + timedelta(seconds=max(0, SLOT_OFFSET_SEC))
            early_wait_sec = (earliest_marker_ist - now_ist).total_seconds()
            if early_wait_sec > 0:
                print(
                    f"[PENDING_FETCH] marker_wait_pre_entry | slot={slot_ist.strftime('%H:%M')} "
                    f"| wait={early_wait_sec:.0f}s | tickers={len(slot_tickers)}",
                    flush=True,
                )
                continue
        # Stale-marker expiry guard: drop slots whose entry-time is already
        # past MAX_PF_MARKER_LAG_SEC. Prevents post-stall recovery from
        # publishing markers for 30-40min-old slots that DE/executor would
        # then reject anyway. Logged so we can see what was dropped.
        marker_lag_sec = (now_ist - slot_ist).total_seconds()
        if marker_lag_sec > MAX_PF_MARKER_LAG_SEC:
            print(
                f"[PENDING_FETCH] marker_expired_pre_verify | slot={slot_ist.strftime('%H:%M')} "
                f"| lag={marker_lag_sec:.0f}s > {MAX_PF_MARKER_LAG_SEC}s | tickers={len(slot_tickers)}",
                flush=True,
            )
            continue
        if eligible_tickers is not None:
            candidates = [t for t in slot_tickers if t in eligible_tickers]
        else:
            candidates = list(slot_tickers)
        # PF-TRIGGER-FIX: verify the TRIGGER bar (slot_ts - 5min, closes AT
        # slot_ts), not the ENTRY bar (slot_ts, closes 5min later). Marker
        # filename still uses slot_ts so DE's exact-slot match is unchanged.
        # Flip EQIDV16_5MIN_PF_VERIFY_TRIGGER_BAR=0 to revert.
        verify_slot = slot_ts - timedelta(minutes=5) if PF_VERIFY_TRIGGER_BAR else slot_ts
        verified = [t for t in candidates if _slot_row_present_in_parquet(t, verify_slot)]
        # Merge with any tickers already in a pre-existing marker so previously-
        # verified tickers aren't dropped when a new ticker joins the slot.
        existing_tickers: List[str] = []
        if marker_path.exists():
            try:
                existing_payload = json.loads(marker_path.read_text(encoding="utf-8"))
                existing_tickers = [
                    str(t).upper().strip()
                    for t in existing_payload.get("tickers", [])
                    if str(t).strip()
                ]
            except Exception as exc:
                print(f"[WARN] marker read failed for {marker_path.name}: {exc}", flush=True)
        merged = list(dict.fromkeys(existing_tickers + verified))
        if not merged:
            continue
        if merged == existing_tickers:
            # No new tickers to add — avoid the rewrite so mtime and E2 emission don't churn.
            continue
        _write_ready_marker(slot_ts, merged)
        written.append(slot_ts)
        new_count = len(merged) - len(existing_tickers)
        print(
            f"[PENDING_FETCH] per_slot_marker_written | slot={slot_ist.strftime('%H:%M')} "
            f"| tickers={len(merged)} (new={new_count}, existing={len(existing_tickers)})",
            flush=True,
        )
    return written


def _get_pending_tickers() -> List[str]:
    """Read the pending JSON and return tickers with status='pending'."""
    tickers, _latest_source_slot = _get_pending_fetch_targets()
    return tickers


# ===========================================================================
# READY MARKER
# ===========================================================================
def _write_ready_marker(
    slot_ts: datetime,
    tickers: List[str],
    verify_failed_sample: Optional[List[str]] = None,
) -> None:
    """Write a .ready marker only after the source-slot candle is confirmed present.

    E2: Emits a PF_VERIFIED lifecycle event per pending signal whose ticker is
    in the ready set for this slot (strategy_v2.txt §E2).
    """
    slot_ist = slot_ts.astimezone(IST)
    written_at_ist = _now_ist()
    fname = slot_ist.strftime("%Y%m%d_%H%M") + ".ready"
    path  = SLOT_READY_PENDING_DIR / fname
    ready_tickers = list(dict.fromkeys(
        str(t).upper().strip() for t in tickers if str(t).strip()
    ))
    payload = {
        "slot":    slot_ist.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "written_at": written_at_ist.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "tickers": ready_tickers,
        "count":   len(ready_tickers),
    }
    if verify_failed_sample:
        payload["verify_failed_sample"] = list(verify_failed_sample)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(tmp_path, path)

    # E2: PF_VERIFIED lifecycle event per pending signal that matched this slot.
    try:
        date_str = written_at_ist.strftime("%Y-%m-%d")
        slot_iso = slot_ist.strftime("%Y-%m-%dT%H:%M:%S%z")
        slot_key_ist = slot_ist.replace(second=0, microsecond=0)
        ready_set = set(ready_tickers)
        state = _load_today_pending_state()
        for sig in state.get("signals", []):
            tk = str(sig.get("ticker", "")).upper().strip()
            if tk not in ready_set:
                continue
            # Match by TRIGGER slot — the marker filename equals the trigger
            # slot (source_slot + (lag-1)*5min), so signals must compare on
            # the same key. The lifecycle field stays labeled `source_slot`
            # but reports the signal's actual SE source_slot, not the
            # trigger slot used for the marker match.
            sig_trigger = _pending_signal_trigger_slot(sig)
            if sig_trigger is None:
                continue
            if sig_trigger.astimezone(IST).replace(second=0, microsecond=0) != slot_key_ist:
                continue
            sid = str(sig.get("signal_id", "")).strip()
            if not sid:
                continue
            sig_source_slot = str(sig.get("source_slot", "")) or slot_iso
            _append_pool_lifecycle_event(
                date_str,
                {
                    "signal_id": sid,
                    "event": "PF_VERIFIED",
                    "ticker": tk,
                    "side": str(sig.get("side", "")),
                    "setup": str(sig.get("setup", "")),
                    "source_slot": sig_source_slot,
                    "trigger_slot": slot_iso,
                    "marker_written_at": payload["written_at"],
                },
            )
    except Exception as exc:
        print(f"[WARN] PF_VERIFIED lifecycle emit failed: {exc}", flush=True)


# ===========================================================================
# FETCH PENDING TICKERS (live-dir, multi-app override)
# ===========================================================================
def _fetch_pending_tickers(
    tickers: List[str],
    token_map: Dict[str, int],
    required_ready_slot: Optional[datetime],
) -> Dict[str, Any]:
    """
    Fetch fresh 5-min data for the given tickers into the shared live 5-minute
    directory while limiting the effective universe to the current pending pool.

    This override keeps the live directory as the single source of truth and
    reuses the main 5-minute scheduler's 8-app partitioning.
    """
    if not tickers:
        return {
            "fetched": 0,
            "ready_tickers": [],
            "verify_failed_sample": [],
            "required_ready_slot": required_ready_slot,
            "verification_slot_end": None,
            "marker_ready": False,
            "missing_tokens": [],
            "fetchable_count": 0,
        }

    full_token_cache = _load_token_cache()
    pending_token_map: Dict[str, int] = {}
    missing_tokens: List[str] = []
    for t in tickers:
        tok = token_map.get(t) or full_token_cache.get(t)
        if tok:
            pending_token_map[t] = int(tok)
        else:
            missing_tokens.append(t)
    if missing_tokens:
        print(
            f"[WARN] No token for: {missing_tokens} - skipping. "
            f"Update stocks_tokens_cache.json if these tickers are new.",
            flush=True,
        )

    fetchable = [t for t in tickers if t in pending_token_map]
    if not fetchable:
        print("[WARN] No fetchable pending tickers with tokens. Skipping Kite API call.", flush=True)
        return {
            "fetched": 0,
            "ready_tickers": [],
            "verify_failed_sample": [],
            "required_ready_slot": required_ready_slot,
            "verification_slot_end": None,
            "marker_ready": False,
            "missing_tokens": list(missing_tokens),
            "fetchable_count": 0,
        }

    ready_slot = required_ready_slot or _floor_to_5m(_now_ist())
    slot_proven = required_ready_slot is not None
    if PF_VERIFY_TRIGGER_BAR:
        # Verify the TRIGGER bar (ready_slot-5min), which closes at ready_slot.
        # See PF-TRIGGER-FIX note at module top. Marker filename still uses
        # ready_slot so DE's exact-slot match is unchanged.
        verification_slot_end = ready_slot.astimezone(IST)
    else:
        verification_slot_end = _verification_slot_end_for_ready_slot(ready_slot)

    orig_out_dir = str(core.DIRS["5min"]["out"])
    orig_universe = core.load_stocks_universe

    try:
        core.DIRS["5min"]["out"] = str(RUNTIME_DATA_5M_DIR)

        # B3 — retry with exponential backoff on Kite fetch transients.
        # Retry is triggered on:
        #   - exception in run_update_5m_once
        #   - any ticker verification failure (if RETRY_PARTIAL enabled, default)
        #   - only on full-slot miss / exception (if RETRY_PARTIAL disabled)
        # Each retry narrows the pending set to the still-failing tickers and
        # rotates the ticker->app mapping via `partition_shift=attempt_idx`, so
        # a ticker that failed on app N on attempt 0 is fetched via app N+1 on
        # attempt 1, etc. Worst-case latency = sum(PENDING_FETCH_RETRY_DELAYS_SEC).
        extract_failed = getattr(core, "_extract_failed_tickers", None)

        result: Dict[str, Any] = {}
        attempts = 1 + len(PENDING_FETCH_RETRY_DELAYS_SEC)
        last_exc: Optional[BaseException] = None
        still_pending: List[str] = list(fetchable)
        ready_accum: set = set()
        final_failure_sample: List[Any] = []
        last_attempt_idx = 0

        for attempt_idx in range(attempts):
            last_attempt_idx = attempt_idx
            if not still_pending:
                break

            batch = list(still_pending)
            batch_token_map = {
                t: pending_token_map[t] for t in batch if t in pending_token_map
            }

            def _pending_loader(logger, _b=batch, _tm=batch_token_map, _a=attempt_idx):
                logger.info(
                    "[PENDING_FETCH] attempt=%d universe=%d pending tickers (shift=%d)",
                    _a + 1, len(_b), _a,
                )
                return list(_b), dict(_tm)

            core.load_stocks_universe = _pending_loader

            try:
                result = dict(
                    scheduler.run_update_5m_once(
                        max_workers=max(1, min(PENDING_MAX_WORKERS, len(batch))),
                        max_workers_per_app=max(1, min(PENDING_MAX_WORKERS_PER_APP, len(batch))),
                        report_dir=REPORT_DIR,
                        buffer_sec=0,
                        refresh_tokens=False,
                        opening_slot=True,
                        slot_end=verification_slot_end,
                        ready_marker_enabled=False,
                        # P0 fix (2026-04-23): PF must NEVER write into LF's
                        # slot_ready_5m/ — it would clobber the full-universe
                        # marker with a 6-ticker pending subset, fooling SE
                        # into believing only 6 tickers are tradable. PF's
                        # own slot_ready_5m_pending/<slot>.ready marker is
                        # written separately by `_write_slot_ready_marker`.
                        publish_completion_marker=False,
                        # Cross-app redistribution: each retry rotates the
                        # ticker->app mapping by `attempt_idx`, so a failing
                        # ticker is re-fetched via a different Kite session.
                        partition_shift=attempt_idx,
                    ) or {}
                )
                last_exc = None
            except Exception as exc:  # transient Kite / network failure
                last_exc = exc
                result = {}

            verify_sample = list(result.get("verification_failure_sample", []) or [])
            failed_this: List[str] = []
            if callable(extract_failed) and verify_sample:
                try:
                    failed_this = list(extract_failed(verify_sample, batch) or [])
                except Exception:
                    failed_this = []
            failed_this_set = set(failed_this)

            # A clean attempt (no exception) confirms every non-failed ticker
            # from this batch as ready. If the call raised, treat the whole
            # batch as still-pending.
            if last_exc is None:
                for t in batch:
                    if t not in failed_this_set:
                        ready_accum.add(t)

            still_pending = [t for t in fetchable if t not in ready_accum]
            final_failure_sample = verify_sample

            # Success exit: clean attempt, nothing left pending.
            if last_exc is None and not still_pending:
                break
            # No more retries budgeted.
            if attempt_idx >= len(PENDING_FETCH_RETRY_DELAYS_SEC):
                break
            # Legacy gate: if partial-retry is off, only continue on a full miss
            # or an exception (matches pre-fix behavior).
            if not PENDING_FETCH_RETRY_PARTIAL:
                full_miss = (
                    last_exc is not None
                    or (len(batch) > 0 and len(failed_this) >= len(batch))
                )
                if not full_miss:
                    break

            delay = PENDING_FETCH_RETRY_DELAYS_SEC[attempt_idx]
            reason_txt = (
                f"exc={type(last_exc).__name__}" if last_exc is not None
                else f"still_pending={len(still_pending)}/{len(fetchable)}"
            )
            print(
                f"[PENDING_FETCH] retry attempt={attempt_idx + 1}/{len(PENDING_FETCH_RETRY_DELAYS_SEC)} "
                f"| delay={delay:.1f}s | reason={reason_txt} | app_shift={attempt_idx + 1}",
                flush=True,
            )
            time.sleep(delay)

        # Only re-raise if every attempt failed AND we recovered nothing.
        # If some tickers succeeded on earlier attempts, keep those and report.
        if last_exc is not None and not ready_accum:
            raise last_exc

        ready_tickers = [t for t in fetchable if t in ready_accum]
        failed_tickers = [t for t in fetchable if t not in ready_accum]
        verify_failed = len(failed_tickers)
        verify_failed_sample = final_failure_sample
        if verify_failed > 0:
            print(
                f"[WARN] {verify_failed}/{len(fetchable)} pending tickers "
                f"failed verification after {last_attempt_idx + 1} attempt(s)",
                flush=True,
            )
        return {
            "fetched": len(ready_tickers),
            "ready_tickers": ready_tickers,
            "verify_failed_sample": verify_failed_sample,
            "required_ready_slot": ready_slot,
            "verification_slot_end": verification_slot_end,
            "marker_ready": (
                slot_proven
                and
                verify_failed == 0
                and len(ready_tickers) == len(fetchable)
            ),
            "missing_tokens": list(missing_tokens),
            "fetchable_count": len(fetchable),
        }

    except Exception as exc:
        print(f"[ERROR] Pending fetch failed: {exc}", flush=True)
        return {
            "fetched": 0,
            "ready_tickers": [],
            "verify_failed_sample": [],
            "required_ready_slot": ready_slot,
            "verification_slot_end": verification_slot_end,
            "marker_ready": False,
            "missing_tokens": list(missing_tokens),
            "fetchable_count": len(fetchable),
        }

    finally:
        core.DIRS["5min"]["out"] = orig_out_dir
        core.load_stocks_universe = orig_universe


# ===========================================================================
# MAIN LOOP
# ===========================================================================
def main() -> None:
    _start_tee()

    print(
        "=" * 70 + "\n"
        "EQIDV2 V16 5min PENDING DATA FETCHER (Stage 3)\n"
        f"  LIVE_DATA_DIR    : {RUNTIME_DATA_5M_DIR}\n"
        f"  READY_MARKER_DIR : {SLOT_READY_PENDING_DIR}\n"
        f"  INTERVAL         : {FETCH_INTERVAL_SEC}s\n"
        f"  ENTRY_CLOCK_GATE : {BLOCK_EARLY_MARKERS} (offset={SLOT_OFFSET_SEC}s)\n"
        "  Fetches fresh 5-min data for ONLY pending pool tickers\n"
        "=" * 70,
        flush=True,
    )

    # P0 fix (2026-04-23): config attestation. Without this, PF can silently
    # read/write the wrong 5-min directory (EOD `_5min_eq` vs intraday
    # `_5min_eq_live`) when the bat forgets to export EQIDV2_DATA_5M_DIR.
    # Soft by default (logs DRIFT, continues); EQIDV2_CONFIG_CHECK_STRICT=1
    # makes it sys.exit(2). EQIDV2_CONFIG_CHECK_BYPASS=1 emergency override.
    try:
        from eqidv2_config_attestation import verify_claim as _verify_config_claim
        _verify_config_claim(
            process_name="eqidv2_pending_data_fetcher_v16_5min",
            whitelist=(
                "EQIDV2_DATA_5M_DIR",
                "EQIDV2_RUNTIME_ROOT",
                "EQIDV2_LIVE_SIGNALS_DIR",
            ),
        )
    except SystemExit:
        raise
    except Exception as exc:
        print(f"[STARTUP.CONFIG] attestation_skipped err={exc!r} (non-fatal in soft mode)", flush=True)

    _touch_status("STARTING")
    _load_token_cache()  # warm up token cache

    # P0 fix (2026-04-23): restart-reconciliation. Before entering the main
    # loop, replay the per-trigger-slot marker pass with eligible_tickers=None
    # so that any pending signal whose trigger bar is ALREADY on disk (from a
    # PF run prior to the crash/restart) gets its `slot_ready_5m_pending/
    # <slot>.ready` marker written immediately. Without this, the first
    # post-restart cycle only re-considers tickers it just fetched fresh,
    # leaving older-slot pending signals stuck in DE's waiting_ready bucket
    # until they hit `expires_at` and get DROPPED with reason
    # `expired_window_parity` (the 28-min stuck-loop / 33-signal mass-expiry
    # incident on 2026-04-23). _emit_per_trigger_slot_markers is idempotent:
    # marker rewrites whose ticker set is unchanged are a no-op, and merges
    # are atomic via os.replace inside _write_ready_marker.
    try:
        startup_slot_map = _get_pending_trigger_slot_map()
        if startup_slot_map:
            print(
                f"[PENDING_FETCH][STARTUP_RECONCILE] replaying markers for "
                f"{len(startup_slot_map)} pending trigger slot(s) from disk",
                flush=True,
            )
            replayed = _emit_per_trigger_slot_markers(startup_slot_map, eligible_tickers=None)
            print(
                f"[PENDING_FETCH][STARTUP_RECONCILE] markers_published={len(replayed)} "
                f"slots={[s.astimezone(IST).strftime('%H:%M') for s in replayed]}",
                flush=True,
            )
    except Exception as exc:
        print(f"[WARN] PF startup reconciliation failed: {exc!r} (continuing)", flush=True)

    last_run_slot: Optional[str] = None
    last_empty_log_key: Optional[str] = None

    while True:
        now = _now_ist()
        _touch_heartbeat("RUNNING", phase="LOOP")

        if now.time() >= HARD_STOP:
            _touch_status("STOPPED_AFTER_CUTOFF")
            _touch_heartbeat("STOPPED")
            print("[STOP] Hard-stop reached. Exiting.", flush=True)
            return

        if now.time() < MARKET_OPEN:
            market_open_dt = IST.localize(datetime.combine(now.date(), MARKET_OPEN))
            if ALIGN_TO_5MIN_BOUNDARY:
                wake = market_open_dt + timedelta(seconds=int(SLOT_OFFSET_SEC))
                phase = "WAIT_SLOT"
            else:
                wake = market_open_dt - timedelta(seconds=60)
                phase = "WAIT_MARKET_OPEN"
            wait_secs = max(5.0, (wake - now).total_seconds())
            _touch_status("RUNNING", phase=phase)
            time.sleep(min(wait_secs, 60.0))
            continue

        slot_key: Optional[str] = None
        slot_display: Optional[str] = None
        next_wake: Optional[datetime] = None
        pending_recheck_at: Optional[datetime] = None
        pending_pool_deadline: Optional[datetime] = None
        if ALIGN_TO_5MIN_BOUNDARY:
            slot = _floor_to_5m(now)
            slot_key = slot.strftime("%Y%m%d_%H%M")
            slot_display = slot.strftime("%H:%M")
            run_at = slot + timedelta(seconds=int(SLOT_OFFSET_SEC))
            next_wake = slot + timedelta(minutes=5, seconds=int(SLOT_OFFSET_SEC))
            pending_recheck_at = slot + timedelta(seconds=int(PENDING_RECHECK_AFTER_SEC))
            pending_pool_deadline = slot + timedelta(seconds=int(PENDING_POOL_DEADLINE_SEC))
            if now < run_at:
                _touch_status("RUNNING", phase="WAIT_SLOT", slot=slot_display)
                time.sleep(max(0.5, (run_at - now).total_seconds()))
                continue
            if last_run_slot == slot_key:
                _touch_status("RUNNING", phase="WAIT_NEXT_SLOT", slot=slot_display)
                time.sleep(max(0.5, (next_wake - now).total_seconds()))
                continue

        # Read pending tickers
        pending_tickers, latest_source_slot = _get_pending_fetch_targets()
        deduplicated = list(dict.fromkeys(pending_tickers))  # preserve order, deduplicate

        if not deduplicated:
            if ALIGN_TO_5MIN_BOUNDARY and slot_key is not None and next_wake is not None:
                now_after = _now_ist()
                # strategy_v2 §SEE-A12 fix: three-band wait.
                #   slot .. slot+RECHECK (default 55s): sleep until RECHECK
                #       (SEE almost always done by then).
                #   slot+RECHECK .. slot+DEADLINE (default 240s): poll pool
                #       every POLL_INTERVAL_SEC (default 5s) — catches slow
                #       SEE writes without losing a full bar.
                #   slot+DEADLINE and beyond: give up, wait for next slot.
                if pending_recheck_at is not None and now_after < pending_recheck_at:
                    phase = "WAIT_PENDING_POOL"
                    sleep_for = min(
                        (pending_recheck_at - now_after).total_seconds(),
                        (next_wake - now_after).total_seconds(),
                    )
                elif (
                    pending_pool_deadline is not None
                    and now_after < pending_pool_deadline
                ):
                    phase = "POLL_PENDING_POOL"
                    remaining = (pending_pool_deadline - now_after).total_seconds()
                    sleep_for = max(
                        0.5,
                        min(PENDING_POOL_POLL_INTERVAL_SEC, remaining),
                    )
                else:
                    phase = "WAIT_NEXT_SLOT"
                    sleep_for = max(0.5, (next_wake - now_after).total_seconds())
                log_key = f"{slot_key}:{phase}"
                if last_empty_log_key != log_key:
                    if phase == "WAIT_PENDING_POOL":
                        msg_tail = "waiting for raw pool"
                    elif phase == "POLL_PENDING_POOL":
                        msg_tail = (
                            f"polling raw pool every {PENDING_POOL_POLL_INTERVAL_SEC:.0f}s "
                            f"(SEE-A12 extended window, deadline slot+{PENDING_POOL_DEADLINE_SEC}s)"
                        )
                    else:
                        msg_tail = "no pending, waiting for next slot"
                    print(
                        f"[PENDING_FETCH] {now.strftime('%H:%M:%S')} | pending_tickers=0 | "
                        f"{msg_tail}",
                        flush=True,
                    )
                    last_empty_log_key = log_key
                _touch_status("RUNNING", phase=phase, slot=slot_display)
                time.sleep(max(0.5, sleep_for))
            else:
                print(
                    f"[PENDING_FETCH] {now.strftime('%H:%M:%S')} | pending_tickers=0 | "
                    f"skipped (no pending signals)",
                    flush=True,
                )
                _touch_status("RUNNING", phase="IDLE")
                time.sleep(float(FETCH_INTERVAL_SEC))
            continue

        last_empty_log_key = None
        _touch_status("RUNNING", phase="FETCH", pending_count=len(deduplicated))
        fetch_started = time.perf_counter()

        # Get token map for pending tickers
        full_token_cache = _load_token_cache()
        token_map = {
            t: full_token_cache[t]
            for t in deduplicated
            if t in full_token_cache
        }

        fetch_result = _fetch_pending_tickers(
            deduplicated,
            token_map,
            required_ready_slot=latest_source_slot,
        )
        elapsed = time.perf_counter() - fetch_started
        fetched = int(fetch_result.get("fetched", 0) or 0)
        ready_tickers = list(fetch_result.get("ready_tickers", []) or [])
        verify_failed_sample = list(fetch_result.get("verify_failed_sample", []) or [])
        ready_slot = fetch_result.get("required_ready_slot")
        verification_slot_end = fetch_result.get("verification_slot_end")
        marker_ready = bool(fetch_result.get("marker_ready", False))
        missing_tokens = list(fetch_result.get("missing_tokens", []) or [])
        missing_token_filtered = 0
        if missing_tokens:
            missing_token_filtered = _mark_missing_token_pending_signals(missing_tokens)
            if missing_token_filtered > 0:
                print(
                    f"[PENDING_FETCH] filtered_missing_token={missing_token_filtered} "
                    f"| tickers={sorted(set(missing_tokens))}",
                    flush=True,
                )

        # A2 fix: give up on tickers that keep failing verification in the same
        # slot, so the marker can proceed with the verified subset instead of
        # starving every other pending ticker.
        give_up_verify_failed: List[str] = []
        verify_failed_filtered = 0
        if (
            VERIFY_FAIL_MAX_RETRIES > 0
            and not marker_ready
            and ready_tickers
            and isinstance(ready_slot, datetime)
        ):
            fetchable_now = [t for t in deduplicated if t not in set(missing_tokens)]
            failed_now = [t for t in fetchable_now if t not in set(ready_tickers)]
            if failed_now:
                give_up_verify_failed = _update_verify_fail_counts(ready_slot, failed_now)
                if give_up_verify_failed:
                    verify_failed_filtered = _mark_verify_failed_pending_signals(
                        give_up_verify_failed
                    )
                    print(
                        f"[PENDING_FETCH] filtered_verify_failed={verify_failed_filtered} "
                        f"| tickers={sorted(set(give_up_verify_failed))} "
                        f"| max_retries={VERIFY_FAIL_MAX_RETRIES}",
                        flush=True,
                    )
                    marker_ready = True  # allow partial marker with verified subset

        # Per-trigger-slot fan-out (replaces legacy single-marker-at-latest +
        # older-slot pass). For every distinct DE-trigger slot in the pending
        # JSON, write one `.ready` marker named with that slot if all required
        # parquet bars are present. Lag-1 setups land in the same cycle as the
        # source_slot fetch; lag>=2 setups roll over to the next cycle when
        # their trigger bar (source_slot + 5min) arrives in parquet.
        markers_written: List[datetime] = []
        try:
            slot_map = _get_pending_trigger_slot_map()
            ready_set = set(ready_tickers) if ready_tickers else None
            markers_written = _emit_per_trigger_slot_markers(slot_map, ready_set)
        except Exception as exc:
            print(f"[WARN] per-trigger-slot marker pass failed: {exc}", flush=True)

        marker_written = bool(markers_written)
        if not marker_written and ready_tickers:
            slot_txt = ready_slot.strftime("%H:%M") if isinstance(ready_slot, datetime) else "unknown"
            slot_end_txt = (
                verification_slot_end.strftime("%H:%M")
                if isinstance(verification_slot_end, datetime)
                else "unknown"
            )
            fetchable_count = int(fetch_result.get("fetchable_count", len(ready_tickers)) or 0)
            reason_bits: List[str] = []
            if missing_tokens:
                reason_bits.append(f"missing_tokens={len(missing_tokens)}")
            if verify_failed_sample:
                reason_bits.append(f"verify_failed={len(verify_failed_sample)}")
            if not reason_bits and len(ready_tickers) != fetchable_count:
                reason_bits.append(f"ready={len(ready_tickers)}/{fetchable_count}")
            reason_text = ", ".join(reason_bits) if reason_bits else "no trigger-slot fully verified yet"
            print(
                f"[PENDING_FETCH] marker_withheld | target_ready_slot={slot_txt} "
                f"| scheduler_slot_end={slot_end_txt} | reason={reason_text}",
                flush=True,
            )

        print(
            f"[PENDING_FETCH] {now.strftime('%H:%M:%S')} | "
            f"pending_tickers={len(deduplicated)} | "
            f"ready={len(ready_tickers)} | fetched={fetched} | "
            f"markers={len(markers_written)} | "
            f"slots={[s.astimezone(IST).strftime('%H:%M') for s in markers_written] if markers_written else '[]'} | "
            f"elapsed={elapsed:.1f}s",
            flush=True,
        )

        _touch_status("RUNNING", phase="FETCH_DONE", fetched=fetched, elapsed=round(elapsed, 1))
        _touch_heartbeat("RUNNING", phase="FETCH_DONE", fetched=fetched)

        if ALIGN_TO_5MIN_BOUNDARY and slot_key is not None and next_wake is not None:
            if marker_written:
                last_run_slot = slot_key
                time.sleep(max(0.5, (next_wake - _now_ist()).total_seconds()))
            else:
                retry_sleep = min(float(FETCH_INTERVAL_SEC), max(0.5, (next_wake - _now_ist()).total_seconds()))
                time.sleep(max(0.5, retry_sleep))
        else:
            # Sleep until next fetch cycle
            time.sleep(float(FETCH_INTERVAL_SEC))


if __name__ == "__main__":
    main()
