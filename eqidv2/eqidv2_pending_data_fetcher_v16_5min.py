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
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
END_5M                = "_stocks_indicators_5min.parquet"
TOKENS_CACHE_PATH     = SCRIPT_DIR / "stocks_tokens_cache.json"

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

# Max concurrent workers for the pending-only refresh using the shared 8-app fetch path.
PENDING_MAX_WORKERS     = int(os.getenv("EQIDV2_PENDING_FETCH_MAX_WORKERS", "8"))
PENDING_MAX_WORKERS_PER_APP = int(os.getenv("EQIDV2_PENDING_FETCH_MAX_WORKERS_PER_APP", "8"))


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
    """Write a .ready marker only after the source-slot candle is confirmed present."""
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
            "all_ready": False,
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
            "all_ready": False,
            "missing_tokens": list(missing_tokens),
            "fetchable_count": 0,
        }

    ready_slot = required_ready_slot or _floor_to_5m(_now_ist())
    slot_proven = required_ready_slot is not None
    verification_slot_end = _verification_slot_end_for_ready_slot(ready_slot)

    orig_out_dir = str(core.DIRS["5min"]["out"])
    orig_universe = core.load_stocks_universe

    try:
        core.DIRS["5min"]["out"] = str(RUNTIME_DATA_5M_DIR)

        def _pending_loader(logger):
            logger.info(
                "[PENDING_FETCH] universe override: %d pending tickers", len(fetchable)
            )
            return list(fetchable), dict(pending_token_map)

        core.load_stocks_universe = _pending_loader

        result = dict(
            scheduler.run_update_5m_once(
                max_workers=max(1, min(PENDING_MAX_WORKERS, len(fetchable))),
                max_workers_per_app=max(1, min(PENDING_MAX_WORKERS_PER_APP, len(fetchable))),
                report_dir=REPORT_DIR,
                buffer_sec=0,
                refresh_tokens=False,
                opening_slot=True,
                slot_end=verification_slot_end,
                ready_marker_enabled=False,
            ) or {}
        )

        verify_failed = int(result.get("verification_failed_count", 0) or 0)
        verify_failed_sample = list(result.get("verification_failure_sample", []) or [])
        failed_tickers: List[str] = []
        extract_failed = getattr(core, "_extract_failed_tickers", None)
        if callable(extract_failed) and verify_failed_sample:
            try:
                failed_tickers = list(extract_failed(verify_failed_sample, fetchable) or [])
            except Exception:
                failed_tickers = []
        ready_tickers = [t for t in fetchable if t not in set(failed_tickers)]
        if verify_failed > 0:
            print(
                f"[WARN] {verify_failed} pending tickers failed verification in multi-app fetch",
                flush=True,
            )
        return {
            "fetched": len(ready_tickers),
            "ready_tickers": ready_tickers,
            "verify_failed_sample": verify_failed_sample,
            "required_ready_slot": ready_slot,
            "verification_slot_end": verification_slot_end,
            "all_ready": (
                slot_proven
                and
                verify_failed == 0
                and not missing_tokens
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
            "all_ready": False,
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
        "  Fetches fresh 5-min data for ONLY pending pool tickers\n"
        "=" * 70,
        flush=True,
    )

    _touch_status("STARTING")
    _load_token_cache()  # warm up token cache
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
        if ALIGN_TO_5MIN_BOUNDARY:
            slot = _floor_to_5m(now)
            slot_key = slot.strftime("%Y%m%d_%H%M")
            slot_display = slot.strftime("%H:%M")
            run_at = slot + timedelta(seconds=int(SLOT_OFFSET_SEC))
            next_wake = slot + timedelta(minutes=5, seconds=int(SLOT_OFFSET_SEC))
            pending_recheck_at = slot + timedelta(seconds=int(PENDING_RECHECK_AFTER_SEC))
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
                if pending_recheck_at is not None and now_after < pending_recheck_at:
                    phase = "WAIT_PENDING_POOL"
                    sleep_for = min(
                        (pending_recheck_at - now_after).total_seconds(),
                        (next_wake - now_after).total_seconds(),
                    )
                else:
                    phase = "WAIT_NEXT_SLOT"
                    sleep_for = max(0.5, (next_wake - now_after).total_seconds())
                log_key = f"{slot_key}:{phase}"
                if last_empty_log_key != log_key:
                    print(
                        f"[PENDING_FETCH] {now.strftime('%H:%M:%S')} | pending_tickers=0 | "
                        f"{'waiting for raw pool' if phase == 'WAIT_PENDING_POOL' else 'no pending, waiting for next slot'}",
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
        all_ready = bool(fetch_result.get("all_ready", False))
        marker_written = False

        if all_ready and ready_tickers and isinstance(ready_slot, datetime):
            _write_ready_marker(
                ready_slot,
                ready_tickers,
                verify_failed_sample=verify_failed_sample,
            )
            marker_written = True
        elif ready_tickers:
            slot_txt = ready_slot.strftime("%H:%M") if isinstance(ready_slot, datetime) else "unknown"
            slot_end_txt = (
                verification_slot_end.strftime("%H:%M")
                if isinstance(verification_slot_end, datetime)
                else "unknown"
            )
            fetchable_count = int(fetch_result.get("fetchable_count", len(ready_tickers)) or 0)
            missing_tokens = list(fetch_result.get("missing_tokens", []) or [])
            reason_bits: List[str] = []
            if missing_tokens:
                reason_bits.append(f"missing_tokens={len(missing_tokens)}")
            if verify_failed_sample:
                reason_bits.append(f"verify_failed={len(verify_failed_sample)}")
            if not reason_bits and len(ready_tickers) != fetchable_count:
                reason_bits.append(f"ready={len(ready_tickers)}/{fetchable_count}")
            reason_text = ", ".join(reason_bits) if reason_bits else "source-slot not fully verified"
            print(
                f"[PENDING_FETCH] marker_withheld | target_ready_slot={slot_txt} "
                f"| scheduler_slot_end={slot_end_txt} | reason={reason_text}",
                flush=True,
            )

        print(
            f"[PENDING_FETCH] {now.strftime('%H:%M:%S')} | "
            f"pending_tickers={len(deduplicated)} | "
            f"ready={len(ready_tickers)} | fetched={fetched} | "
            f"marker={'written' if marker_written else 'withheld'} | "
            f"target_slot={ready_slot.strftime('%H:%M') if isinstance(ready_slot, datetime) else 'unknown'} | "
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
