# -*- coding: utf-8 -*-
"""
eqidv2_eod_scheduler_for_5mins_data_live_minimal.py
=================================================
Live-only 5-minute scheduler using the minimal indicator fetch core.

Why this exists:
- keeps the original scheduler/core intact
- computes only the indicator set used by the live/backtest v7/v15 path
- adds a dedicated 5m live fetch session without disturbing the 15m path

Inherited fixes from the base scheduler:

1) Your core loader can treat filtered_stocks_MIS.selected_stocks (dict) as a TOKEN MAP
   if values are numeric, then int() casts weights like 0.6 -> 0, leading to:
      "No token for XYZ, skipping."
   This scheduler monkey-patches core.load_stocks_universe to IGNORE such "small-value" token maps
   and force a real token fetch via kite.instruments().

2) The old scheduler ran too close to the candle boundary. Kite can lag a bit.
   This version runs exactly once per slot and uses a configurable buffer
   (default 2s; override via --buffer-sec / EQIDV2_5M_BUFFER_SEC).

3) The old scheduler referenced core.HOLIDAYS_FILE (not present). Core exposes HOLIDAYS_FILE_DEFAULT.

Run:
    python eqidv2_eod_scheduler_for_5mins_data_live_minimal.py

Optional:
    python eqidv2_eod_scheduler_for_5mins_data_live_minimal.py --buffer-sec 20 --max-workers 16
    The scheduler treats --max-workers as a total worker budget across all 8 app partitions.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import queue
import sys
import threading
import time
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Callable, Optional

import pytz
from eqidv2_runtime_paths import DATA_5M_DIR as RUNTIME_DATA_5M_DIR
from eqidv2_runtime_paths import REPORTS_DIR as RUNTIME_REPORTS_DIR
from eqidv2_runtime_paths import runtime_dir

IST = pytz.timezone("Asia/Kolkata")

# ---------------------------------------------------------------------
# Locate eqidv2 folder (robust even if your repo is nested oddly)
# ---------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
CORE_FILENAME = "trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_live_minimal.py"

def _find_core_dir(start: Path, max_up: int = 6) -> Path:
    cur = start
    for _ in range(max_up + 1):
        if (cur / CORE_FILENAME).exists():
            return cur
        cur = cur.parent
    # fallback: use script dir
    return start

EQIDV2_DIR = _find_core_dir(SCRIPT_DIR)
if str(EQIDV2_DIR) not in sys.path:
    sys.path.insert(0, str(EQIDV2_DIR))

import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_live_minimal as core  # noqa: E402


# ---------------------------------------------------------------------
# Monkey-patch kite session to read api_key.txt / access_token.txt from EQIDV2_DIR
# (core.setup_kite_session reads relative files from CWD) fileciteturn36file10L31-L38
# ---------------------------------------------------------------------
def _read_first_token(p: Path) -> str:
    txt = p.read_text(encoding="utf-8").strip()
    if not txt:
        raise RuntimeError(f"Auth file is empty: {p}")
    return txt.split()[0].strip()

def setup_kite_session_from_eqidv2_dir():
    from kiteconnect import KiteConnect  # imported here to avoid import costs on module import
    api_key = _read_first_token(EQIDV2_DIR / "api_key.txt")
    access_token = _read_first_token(EQIDV2_DIR / "access_token.txt")
    kc = KiteConnect(api_key=api_key, timeout=DEFAULT_KITE_REQUEST_TIMEOUT_SEC)
    kc.set_access_token(access_token)
    return kc

def _setup_kite_session_n_from_eqidv2_dir(app_idx: int):
    """
    Additional app session (app2/app3/app4/app5/app6/app7/app8):
    - access_tokenN.txt is used for auth
    - request_tokenN.txt is optional during live fetch fallback; if missing/empty
      we continue with access-token validation instead of failing the whole slot
    - api_keyN.txt is preferred; fallback to api_key.txt if absent
    """
    from kiteconnect import KiteConnect  # imported here to avoid import costs on module import

    if app_idx not in (2, 3, 4, 5, 6, 7, 8):
        raise ValueError(f"Unsupported app index: {app_idx}")

    request_token_n = EQIDV2_DIR / f"request_token{app_idx}.txt"
    access_token_n = EQIDV2_DIR / f"access_token{app_idx}.txt"
    api_key_n = EQIDV2_DIR / f"api_key{app_idx}.txt"
    api_key1 = EQIDV2_DIR / "api_key.txt"

    if not access_token_n.exists():
        raise FileNotFoundError(f"Missing app{app_idx} auth file: {access_token_n}")

    if request_token_n.exists():
        try:
            _ = _read_first_token(request_token_n)
        except Exception as exc:
            print(f"[WARN] app{app_idx} request_token validation skipped: {exc}")
    else:
        print(f"[WARN] app{app_idx} request_token file missing; continuing with access_token only.")

    api_key_path = api_key_n if api_key_n.exists() else api_key1
    if api_key_path == api_key1:
        print(f"[WARN] api_key{app_idx}.txt not found; app{app_idx} will use api_key.txt.")

    api_key = _read_first_token(api_key_path)
    access_token = _read_first_token(access_token_n)

    kc = KiteConnect(api_key=api_key, timeout=DEFAULT_KITE_REQUEST_TIMEOUT_SEC)
    kc.set_access_token(access_token)
    return kc

def setup_kite_session2_from_eqidv2_dir():
    return _setup_kite_session_n_from_eqidv2_dir(2)

def setup_kite_session3_from_eqidv2_dir():
    return _setup_kite_session_n_from_eqidv2_dir(3)

def setup_kite_session4_from_eqidv2_dir():
    return _setup_kite_session_n_from_eqidv2_dir(4)

def setup_kite_session5_from_eqidv2_dir():
    return _setup_kite_session_n_from_eqidv2_dir(5)

def setup_kite_session6_from_eqidv2_dir():
    return _setup_kite_session_n_from_eqidv2_dir(6)

def setup_kite_session7_from_eqidv2_dir():
    return _setup_kite_session_n_from_eqidv2_dir(7)

def setup_kite_session8_from_eqidv2_dir():
    return _setup_kite_session_n_from_eqidv2_dir(8)

def _setup_fn_map() -> dict[str, Callable[[], object]]:
    return {
        "app1": setup_kite_session_from_eqidv2_dir,
        "app2": setup_kite_session2_from_eqidv2_dir,
        "app3": setup_kite_session3_from_eqidv2_dir,
        "app4": setup_kite_session4_from_eqidv2_dir,
        "app5": setup_kite_session5_from_eqidv2_dir,
        "app6": setup_kite_session6_from_eqidv2_dir,
        "app7": setup_kite_session7_from_eqidv2_dir,
        "app8": setup_kite_session8_from_eqidv2_dir,
    }

core.setup_kite_session = setup_kite_session_from_eqidv2_dir


# ---------------------------------------------------------------------
# Monkey-patch universe loader: if token_map looks like weights/flags (all small),
# ignore it so core will fetch real instrument tokens.
#
# Core currently can treat selected_stocks dict numeric values as tokens fileciteturn43file2L95-L103
# ---------------------------------------------------------------------
_orig_load_universe = getattr(core, "load_stocks_universe", None)

def _looks_like_real_tokens(token_map: dict) -> bool:
    try:
        vals = [int(v) for v in token_map.values()]
        if not vals:
            return False
        # Instrument tokens are typically 5+ digits. If everything is tiny, it's likely weights/flags.
        return max(vals) >= 1000
    except Exception:
        return False

def load_stocks_universe_fixed(*args, **kwargs):
    if _orig_load_universe is None:
        raise RuntimeError("core.load_stocks_universe not found")
    tickers, token_map = _orig_load_universe(*args, **kwargs)
    if token_map and not _looks_like_real_tokens(token_map):
        # Force core to fetch tokens from kite.instruments()
        try:
            logger = args[0] if args else None
            if logger is not None:
                logger.warning("Token map from filtered_stocks_MIS looks like weights/flags (small ints). Ignoring it and forcing token fetch.")
        except Exception:
            pass
        token_map = {}
    return tickers, token_map

core.load_stocks_universe = load_stocks_universe_fixed

# ---------------------------------------------------------------------
# IMPORTANT FIX: core.ticker_is_fresh() has a +/- one-step tolerance that can
# wrongly treat a file that is ONE candle behind as "fresh".
# For 5m, that can make updates happen every 10 minutes (it skips every alternate slot).
# So we patch it to be STRICT: last_ts must be >= expected_ts (minus a tiny tol).
# ---------------------------------------------------------------------
_orig_ticker_is_fresh = getattr(core, 'ticker_is_fresh', None)

def ticker_is_fresh_strict(mode: str, out_path: str, now_ist: datetime, holidays: set, intraday_ts: str) -> bool:
    existing_path = core._resolve_existing_store_path(out_path)
    if not os.path.exists(existing_path):
        return False
    last_ts = core._read_last_ts_from_store(existing_path)
    if last_ts is None:
        return False
    # normalize tz
    if last_ts.tzinfo is None:
        last_ts = last_ts.tz_localize(core.IST_TZ)
    else:
        last_ts = last_ts.tz_convert(core.IST_TZ)
    spec = core.expected_last_stamp(mode, now_ist, holidays, intraday_ts)
    exp_ts = spec['value']
    if exp_ts.tzinfo is None:
        exp_ts = core.IST_TZ.localize(exp_ts)
    tol = timedelta(seconds=1)
    if last_ts < (exp_ts - tol):
        return False
    if mode.lower().strip() == "5min" and getattr(core, "DEFAULT_ENFORCE_5MIN_SESSION_COMPLETENESS", False):
        missing_session = core._missing_5min_session_stamps_from_store(existing_path, exp_ts)
        if missing_session:
            return False
    return True

# Apply patch only if core exposes expected_last_stamp (newer core); otherwise keep original.
if hasattr(core, 'expected_last_stamp') and _orig_ticker_is_fresh is not None:
    core.ticker_is_fresh = ticker_is_fresh_strict


# ---------------------------------------------------------------------
# Scheduler logic
# ---------------------------------------------------------------------
MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 35)  # keep scheduler alive long enough to process the 15:30 close bar
HARD_STOP = dtime(15, 50)  # exit after this
FIRST_5M_CLOSE = dtime(9, 20)  # first completed 5m candle close timestamp
DEFAULT_MAX_WORKERS = int(os.getenv("EQIDV2_5M_MAX_WORKERS", "64"))
DEFAULT_MAX_WORKERS_PER_APP = int(os.getenv("EQIDV2_5M_MAX_WORKERS_PER_APP", "8"))
DEFAULT_BUFFER_SEC = int(os.getenv("EQIDV2_5M_BUFFER_SEC", "2"))
DEFAULT_QUARTER_HOUR_BUFFER_SEC = int(
    os.getenv("EQIDV2_5M_QUARTER_HOUR_BUFFER_SEC", str(DEFAULT_BUFFER_SEC))
)
DEFAULT_SLOT_SLA_WARN_SEC = float(os.getenv("EQIDV2_5M_SLOT_SLA_WARN_SEC", "20"))
DEFAULT_KITE_REQUEST_TIMEOUT_SEC = float(
    os.getenv("EQIDV2_5M_KITE_TIMEOUT_SEC", os.getenv("EQIDV2_KITE_REQUEST_TIMEOUT_SEC", "12"))
)
DEFAULT_PARTITION_TIMEOUT_SEC = float(os.getenv("EQIDV2_5M_PARTITION_TIMEOUT_SEC", "150"))
DEFAULT_PARTITION_JOIN_POLL_SEC = float(os.getenv("EQIDV2_5M_PARTITION_JOIN_POLL_SEC", "0.25"))
DEFAULT_PARTITION_TERMINATE_GRACE_SEC = float(os.getenv("EQIDV2_5M_PARTITION_TERMINATE_GRACE_SEC", "5"))
DEFAULT_ADAPTIVE_THROTTLE = str(os.getenv("EQIDV2_5M_ADAPTIVE_THROTTLE", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
DEFAULT_ADAPTIVE_MIN_WORKERS = max(8, int(os.getenv("EQIDV2_5M_ADAPTIVE_MIN_WORKERS", "40")))
DEFAULT_ADAPTIVE_MIN_WORKERS_PER_APP = max(1, int(os.getenv("EQIDV2_5M_ADAPTIVE_MIN_WORKERS_PER_APP", "5")))
DEFAULT_ADAPTIVE_TOTAL_STEP = max(1, int(os.getenv("EQIDV2_5M_ADAPTIVE_TOTAL_STEP", "32")))
DEFAULT_ADAPTIVE_PER_APP_STEP = max(1, int(os.getenv("EQIDV2_5M_ADAPTIVE_PER_APP_STEP", "4")))
DEFAULT_ADAPTIVE_RECOVERY_OK_RATIO = float(os.getenv("EQIDV2_5M_ADAPTIVE_RECOVERY_OK_RATIO", "0.80"))
DEFAULT_ADAPTIVE_RECOVERY_STREAK = max(1, int(os.getenv("EQIDV2_5M_ADAPTIVE_RECOVERY_STREAK", "2")))
DEFAULT_READY_MARKER_ENABLED = str(os.getenv("EQIDV2_5M_READY_MARKER_ENABLED", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
DEFAULT_READY_MARKER_SAMPLE_SIZE = int(os.getenv("EQIDV2_5M_READY_MARKER_SAMPLE_SIZE", "24"))
DEFAULT_READY_MARKER_POLL_SECONDS = float(os.getenv("EQIDV2_5M_READY_MARKER_POLL_SECONDS", "1"))
DEFAULT_READY_MARKER_MIN_FRESH_RATIO = float(os.getenv("EQIDV2_5M_READY_MARKER_MIN_FRESH_RATIO", "0.70"))
DEFAULT_REFRESH_TOKENS = str(os.getenv("EQIDV2_5M_REFRESH_TOKENS", "0")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
DEFAULT_ENABLE_OPENING_SLOT_FETCH = str(
    os.getenv("EQIDV2_5M_ENABLE_OPENING_SLOT_FETCH", "1")
).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
_APP_VALIDATION_CACHE: dict[str, tuple[tuple[tuple[str, int, int], ...], bool, str]] = {}
_APP_SELF_HEAL_STATE: dict[str, dict[str, object]] = {}
DEFAULT_APP_SELF_HEAL_COOLDOWN_SEC = int(os.getenv("EQIDV2_5M_APP_SELF_HEAL_COOLDOWN_SEC", "1800"))
DEFAULT_APP_SELF_HEAL_MAX_ATTEMPTS_PER_DAY = max(
    1,
    int(os.getenv("EQIDV2_5M_APP_SELF_HEAL_MAX_ATTEMPTS_PER_DAY", "2")),
)
SLOT_STATUS_PATH = EQIDV2_DIR / "logs" / "eqidv2_eod_scheduler_for_5mins_data_live_minimal.status.json"


class ParallelPartitionRunError(RuntimeError):
    def __init__(self, message: str, summary: dict[str, object]):
        super().__init__(message)
        self.summary = dict(summary)


# ---------------------------------------------------------------------
# Opening-slot expected stamp override:
# For 09:15 slot we intentionally allow a "start-ts" snapshot fetch.
# Core expected_last_stamp(start) normally resolves to 09:10 at 09:15,
# which suppresses opening updates. We force expected to 09:15 for this slot.
# ---------------------------------------------------------------------
_orig_expected_last_stamp = getattr(core, "expected_last_stamp", None)

def expected_last_stamp_opening_fix(mode: str, now_ist_dt: datetime, holidays: set, intraday_ts: str):
    if _orig_expected_last_stamp is None:
        raise RuntimeError("core.expected_last_stamp not found")

    mode_l = str(mode).lower().strip()
    ts_mode = str(intraday_ts).lower().strip()

    if now_ist_dt.tzinfo is None:
        now_ist_dt = IST.localize(now_ist_dt)

    t = now_ist_dt.time()
    in_opening_window = (MARKET_OPEN <= t < FIRST_5M_CLOSE)
    if mode_l == "5min" and ts_mode == "start" and in_opening_window:
        open_anchor = IST.localize(
            datetime(
                now_ist_dt.year,
                now_ist_dt.month,
                now_ist_dt.day,
                MARKET_OPEN.hour,
                MARKET_OPEN.minute,
                0,
            )
        )
        return {"kind": "ts", "value": open_anchor, "step_min": 5}

    return _orig_expected_last_stamp(mode, now_ist_dt, holidays, intraday_ts)

if _orig_expected_last_stamp is not None:
    core.expected_last_stamp = expected_last_stamp_opening_fix


READY_MARKER_DIR = runtime_dir("slot_ready_5m")
READY_MARKER_DIR.mkdir(parents=True, exist_ok=True)
END_5M = "_stocks_indicators_5min.parquet"


def _sample_tickers_for_ready_marker(tickers: list[str], sample_size: int) -> list[str]:
    uniq = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
    if not uniq:
        return []
    size = min(max(1, int(sample_size)), len(uniq))
    if size >= len(uniq):
        return uniq
    if size == 1:
        return [uniq[len(uniq) // 2]]
    step = max(1.0, (len(uniq) - 1) / float(size - 1))
    picks: list[str] = []
    for idx in range(size):
        pos = int(round(idx * step))
        pos = max(0, min(len(uniq) - 1, pos))
        picks.append(uniq[pos])
    return sorted(set(picks))


def _slot_ready_marker_path(slot_end: datetime) -> Path:
    slot_ts = slot_end if slot_end.tzinfo is not None else IST.localize(slot_end)
    slot_ts = slot_ts.astimezone(IST)
    return READY_MARKER_DIR / f"slot_{slot_ts.strftime('%Y%m%d_%H%M')}.json"


def _last_5m_bar_for_ticker_ist(ticker: str) -> Optional[datetime]:
    out_path = str(RUNTIME_DATA_5M_DIR / f"{str(ticker).strip().upper()}{END_5M}")
    try:
        existing_path = core._resolve_existing_store_path(out_path)
        if not os.path.exists(existing_path):
            return None
        last_ts = core._read_last_ts_from_store(existing_path)
    except Exception:
        return None
    if last_ts is None:
        return None
    if getattr(last_ts, "tzinfo", None) is None:
        return core.IST_TZ.localize(last_ts)
    return last_ts.tz_convert(core.IST_TZ)


def _ticker_has_required_5m_slot_data(
    ticker: str,
    expected_ts_ist: datetime,
) -> tuple[bool, Optional[datetime], list]:
    out_path = str(RUNTIME_DATA_5M_DIR / f"{str(ticker).strip().upper()}{END_5M}")
    try:
        existing_path = core._resolve_existing_store_path(out_path)
        if not os.path.exists(existing_path):
            return False, None, []
        last_ts = core._read_last_ts_from_store(existing_path)
    except Exception:
        return False, None, []

    if last_ts is None:
        return False, None, []

    if getattr(last_ts, "tzinfo", None) is None:
        last_ts = core.IST_TZ.localize(last_ts)
    else:
        last_ts = last_ts.tz_convert(core.IST_TZ)

    target_ts = expected_ts_ist if expected_ts_ist.tzinfo is not None else IST.localize(expected_ts_ist)
    target_ts = target_ts.astimezone(IST)
    tol = timedelta(seconds=1)
    if last_ts < (target_ts - tol):
        return False, last_ts, []

    missing_session: list = []
    if getattr(core, "DEFAULT_ENFORCE_5MIN_SESSION_COMPLETENESS", False):
        try:
            missing_session = list(core._missing_5min_session_stamps_from_store(existing_path, target_ts))
        except Exception:
            missing_session = []
        if missing_session:
            return False, last_ts, missing_session

    return True, last_ts, []


def _publish_slot_ready_marker(
    slot_end: datetime,
    *,
    fresh: int,
    checked: int,
    ratio: float,
    source: str,
) -> None:
    path = _slot_ready_marker_path(slot_end)
    payload = {
        "slot_ist": slot_end.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S%z"),
        "published_at_ist": now_ist().strftime("%Y-%m-%d %H:%M:%S%z"),
        "fresh_count": int(fresh),
        "checked_count": int(checked),
        "fresh_ratio": float(ratio),
        "source": str(source),
    }
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(tmp_path, path)


def _read_marker_meta(path) -> tuple:
    """
    Return (published_at_datetime, source_str) for an existing marker file,
    or (None, None) if the file is missing / unreadable / malformed.
    Used by the §J3 fix #6 write-if-newer guard so a backwards clock jump
    cannot clobber a fresher authoritative payload.
    """
    try:
        if not path.exists():
            return None, None
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw) if raw else {}
        pub_raw = str(data.get("published_at_ist", "")).strip()
        src = str(data.get("source", "")).strip().lower()
        pub_dt = None
        if pub_raw:
            for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%dT%H:%M:%S%z"):
                try:
                    pub_dt = datetime.strptime(pub_raw, fmt)
                    break
                except Exception:
                    pub_dt = None
            if pub_dt is None:
                try:
                    pub_dt = datetime.fromisoformat(pub_raw)
                except Exception:
                    pub_dt = None
        return pub_dt, src
    except Exception:
        return None, None


def _publish_slot_completion_marker(
    slot_end: datetime,
    *,
    tickers_expected: int,
    tickers_written: int,
    failures: list,
    verification_failed_count: int,
    verification_failure_sample: list,
    duration_ms: float,
    sample_fresh: int,
    sample_checked: int,
) -> None:
    """
    strategy_v2 §M1 — authoritative LF completion marker.

    Written after all partition workers join and `_write_slot_status` runs,
    so every field reflects the real fetch outcome (not a sampled probe).
    SE gates on this file: `complete=True` ⇒ safe to scan; `complete=False`
    ⇒ `[ABORT] LF_INCOMPLETE`. Overwrites any earlier watcher-published
    marker for the same slot so there is exactly one source of truth.

    strategy_v2 §J3 fix #6 — write-if-newer guard. A backwards clock jump
    (NTP correction, VM migration) would otherwise let a later invocation
    overwrite a fresher on-disk `final` marker with an older payload and
    silently break the SE gate. If the existing marker is a `final` payload
    with a `published_at_ist` newer than `now_ist()` by more than the
    configured tolerance, skip the write.
    """
    failures_list = [str(f) for f in (failures or [])]
    verify_sample_list = [str(t) for t in (verification_failure_sample or [])]
    complete = (
        not failures_list
        and int(verification_failed_count) == 0
        and int(tickers_written) >= int(tickers_expected)
    )
    tickers_failed = max(0, int(tickers_expected) - int(tickers_written)) + int(verification_failed_count)
    ratio = (float(sample_fresh) / float(sample_checked)) if sample_checked > 0 else 0.0

    path = _slot_ready_marker_path(slot_end)
    new_pub_dt = now_ist()

    # §J3 fix #6 — backward-clock-skew guard.
    existing_pub_dt, existing_src = _read_marker_meta(path)
    skew_tolerance = timedelta(seconds=1)
    if (
        existing_pub_dt is not None
        and existing_src == "final"
        and existing_pub_dt > (new_pub_dt + skew_tolerance)
    ):
        print(
            f"[READY] SKIP overwrite of 5m completion marker "
            f"slot={slot_end.astimezone(IST).strftime('%Y-%m-%d %H:%M:%S%z')} — "
            f"on-disk final published_at={existing_pub_dt.isoformat()} is newer "
            f"than now={new_pub_dt.isoformat()}. Possible clock skew; keeping "
            f"the fresher payload in place.",
            flush=True,
        )
        return

    payload = {
        "slot_ist": slot_end.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S%z"),
        "published_at_ist": new_pub_dt.strftime("%Y-%m-%d %H:%M:%S%z"),
        "source": "final",
        "complete": bool(complete),
        # authoritative fetch outcome (full universe, not a sample)
        "tickers_expected": int(tickers_expected),
        "tickers_written":  int(tickers_written),
        "tickers_failed":   int(tickers_failed),
        "partition_failures": failures_list,
        "verification_failed_count": int(verification_failed_count),
        "verification_failure_sample": verify_sample_list,
        "duration_ms": float(duration_ms),
        # back-compat fields (kept so pre-M1 SE builds still parse the marker)
        "fresh_count":   int(sample_fresh),
        "checked_count": int(sample_checked),
        "fresh_ratio":   float(ratio),
    }
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(tmp_path, path)
    print(
        f"[READY] Published authoritative 5m completion marker "
        f"slot={slot_end.astimezone(IST).strftime('%Y-%m-%d %H:%M:%S%z')} "
        f"complete={complete} written={tickers_written}/{tickers_expected} "
        f"failed={tickers_failed} duration_ms={duration_ms:.0f}",
        flush=True,
    )


def _watch_and_publish_slot_ready_marker(
    slot_end: datetime,
    sample_tickers: list[str],
    *,
    poll_seconds: float,
    min_fresh_ratio: float,
    stop_event: threading.Event,
) -> None:
    if not sample_tickers:
        return
    target_slot = slot_end.astimezone(IST)
    published = False
    while not stop_event.is_set():
        fresh = 0
        checked = 0
        for ticker in sample_tickers:
            ok, last_ts, _ = _ticker_has_required_5m_slot_data(ticker, target_slot)
            if last_ts is None:
                continue
            checked += 1
            if ok:
                fresh += 1
        ratio = (fresh / checked) if checked > 0 else 0.0
        if checked > 0 and ratio >= float(min_fresh_ratio):
            _publish_slot_ready_marker(
                slot_end,
                fresh=fresh,
                checked=checked,
                ratio=ratio,
                source="watcher",
            )
            print(
                f"[READY] Published 5m slot marker for {target_slot.strftime('%Y-%m-%d %H:%M:%S%z')} "
                f"via watcher (fresh={fresh}/{checked}, ratio={ratio:.2f})"
            )
            published = True
            break
        stop_event.wait(max(0.25, float(poll_seconds)))

    if published:
        return

    fresh = 0
    checked = 0
    for ticker in sample_tickers:
        ok, last_ts, _ = _ticker_has_required_5m_slot_data(ticker, target_slot)
        if last_ts is None:
            continue
        checked += 1
        if ok:
            fresh += 1
    ratio = (fresh / checked) if checked > 0 else 0.0
    if checked > 0 and ratio >= float(min_fresh_ratio):
        _publish_slot_ready_marker(
            slot_end,
            fresh=fresh,
            checked=checked,
            ratio=ratio,
            source="final",
        )
        print(
            f"[READY] Published 5m slot marker for {target_slot.strftime('%Y-%m-%d %H:%M:%S%z')} "
            f"after worker completion (fresh={fresh}/{checked}, ratio={ratio:.2f})"
        )


def _write_slot_status(
    slot_end: datetime,
    *,
    total_elapsed_sec: float,
    partition_elapsed: dict[str, float],
    partition_symbol_counts: dict[str, int],
    total_budget: int,
    per_app_cap: int,
    effective_per_app: int,
    sla_warn_sec: float,
    failures: list[str],
    verification_failed_count: int,
    verification_failure_sample: list[str],
) -> None:
    slot_ts = slot_end if slot_end.tzinfo is not None else IST.localize(slot_end)
    slot_ts = slot_ts.astimezone(IST)
    elapsed_values = [float(v) for v in partition_elapsed.values() if v is not None]
    payload = {
        "slot_ist": slot_ts.strftime("%Y-%m-%d %H:%M:%S%z"),
        "updated_at_ist": now_ist().strftime("%Y-%m-%d %H:%M:%S%z"),
        "total_elapsed_sec": float(total_elapsed_sec),
        "partition_elapsed_sec": {k: float(v) for k, v in partition_elapsed.items()},
        "partition_symbol_counts": {k: int(v) for k, v in partition_symbol_counts.items()},
        "max_partition_elapsed_sec": max(elapsed_values) if elapsed_values else 0.0,
        "min_partition_elapsed_sec": min(elapsed_values) if elapsed_values else 0.0,
        "avg_partition_elapsed_sec": (sum(elapsed_values) / len(elapsed_values)) if elapsed_values else 0.0,
        "total_worker_budget": int(total_budget),
        "per_app_cap": int(per_app_cap),
        "effective_per_app": int(effective_per_app),
        "sla_warn_sec": float(sla_warn_sec),
        "sla_state": "WARN" if float(total_elapsed_sec) > float(sla_warn_sec) else "OK",
        "sla_mode": "soft_warn_only",
        "completion_policy": "continue_until_verified",
        "failures": list(failures),
        "verification_failed_count": int(verification_failed_count),
        "verification_failure_sample": list(verification_failure_sample),
        "overall_state": (
            "FAIL" if failures or int(verification_failed_count) > 0
            else ("WARN" if float(total_elapsed_sec) > float(sla_warn_sec) else "OK")
        ),
    }
    SLOT_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = SLOT_STATUS_PATH.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(tmp_path, SLOT_STATUS_PATH)


def _adapt_worker_budget(
    *,
    configured_total: int,
    configured_per_app: int,
    current_total: int,
    current_per_app: int,
    slot_summary: dict[str, object],
    healthy_streak: int,
) -> tuple[int, int, int, str]:
    if not DEFAULT_ADAPTIVE_THROTTLE:
        return current_total, current_per_app, 0, "adaptive_throttle_disabled"

    total_elapsed_sec = float(slot_summary.get("total_elapsed_sec", 0.0) or 0.0)
    max_partition_elapsed_sec = float(slot_summary.get("max_partition_elapsed_sec", 0.0) or 0.0)
    sla_warn_sec = float(slot_summary.get("sla_warn_sec", DEFAULT_SLOT_SLA_WARN_SEC) or DEFAULT_SLOT_SLA_WARN_SEC)
    failures = list(slot_summary.get("failures", []) or [])

    timeout_pressure = (
        total_elapsed_sec > sla_warn_sec
        or max_partition_elapsed_sec > max(sla_warn_sec * 0.95, float(DEFAULT_KITE_REQUEST_TIMEOUT_SEC))
        or bool(failures)
    )
    if timeout_pressure:
        next_total = max(DEFAULT_ADAPTIVE_MIN_WORKERS, current_total - DEFAULT_ADAPTIVE_TOTAL_STEP)
        next_per_app = max(
            DEFAULT_ADAPTIVE_MIN_WORKERS_PER_APP,
            current_per_app - DEFAULT_ADAPTIVE_PER_APP_STEP,
        )
        reason_bits = []
        if total_elapsed_sec > sla_warn_sec:
            reason_bits.append(f"slot_elapsed={total_elapsed_sec:.2f}s")
        if max_partition_elapsed_sec > max(sla_warn_sec * 0.95, float(DEFAULT_KITE_REQUEST_TIMEOUT_SEC)):
            reason_bits.append(f"max_partition={max_partition_elapsed_sec:.2f}s")
        if failures:
            reason_bits.append(f"failures={len(failures)}")
        reason = "pressure:" + ",".join(reason_bits)
        return next_total, next_per_app, 0, reason

    healthy_threshold = sla_warn_sec * DEFAULT_ADAPTIVE_RECOVERY_OK_RATIO
    if (
        total_elapsed_sec <= healthy_threshold
        and max_partition_elapsed_sec <= healthy_threshold
        and not failures
    ):
        next_streak = healthy_streak + 1
        if (
            next_streak >= DEFAULT_ADAPTIVE_RECOVERY_STREAK
            and (current_total < configured_total or current_per_app < configured_per_app)
        ):
            next_total = min(configured_total, current_total + DEFAULT_ADAPTIVE_TOTAL_STEP)
            next_per_app = min(configured_per_app, current_per_app + DEFAULT_ADAPTIVE_PER_APP_STEP)
            return next_total, next_per_app, 0, f"recover_after_{next_streak}_clean_slots"
        return current_total, current_per_app, next_streak, f"healthy_streak={next_streak}"

    return current_total, current_per_app, 0, "steady"


def now_ist() -> datetime:
    return datetime.now(IST)

def _floor_to_5m(dt: datetime) -> datetime:
    # dt is tz-aware
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)

def _next_boundary(dt: datetime) -> datetime:
    flo = _floor_to_5m(dt)
    if dt == flo:
        return flo + timedelta(minutes=5)
    return flo + timedelta(minutes=5)


def _slot_buffer_seconds(slot_end: datetime, base_buffer_sec: int, quarter_hour_buffer_sec: int) -> int:
    if int(slot_end.minute) % 15 == 0:
        return max(int(base_buffer_sec), int(quarter_hour_buffer_sec))
    return int(base_buffer_sec)

def _is_trading_time(dt: datetime) -> bool:
    t = dt.time()
    return (t >= MARKET_OPEN) and (t <= MARKET_CLOSE)

def _is_trading_day(dt: datetime, holidays: set) -> bool:
    if dt.weekday() >= 5:
        return False
    return dt.date() not in holidays

def _next_trading_day_open(dt: datetime, holidays: set) -> datetime:
    probe = dt
    while True:
        if _is_trading_day(probe, holidays):
            return IST.localize(
                datetime(
                    probe.year,
                    probe.month,
                    probe.day,
                    MARKET_OPEN.hour,
                    MARKET_OPEN.minute,
                    0,
                )
            )
        probe = probe + timedelta(days=1)

def _read_holidays_set() -> set:
    # Core exposes HOLIDAYS_FILE_DEFAULT fileciteturn36file2L54-L56
    hf = getattr(core, "HOLIDAYS_FILE_DEFAULT", "holidays.csv")
    try:
        return set(core._read_holidays(str(Path(hf))))
    except Exception:
        return set()

def _split_tickers_evenly(
    tickers: list[str],
    partition_count: int,
    shift: int = 0,
) -> list[list[str]]:
    ordered = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
    partition_count = max(1, int(partition_count))
    partitions: list[list[str]] = [[] for _ in range(partition_count)]
    # shift rotates the ticker->partition mapping so callers (e.g. PF retry
    # path) can reroute the same ticker set to a different app on a subsequent
    # attempt. shift=0 preserves the original round-robin assignment.
    shift = int(shift) % partition_count if partition_count else 0
    for idx, ticker in enumerate(ordered):
        partitions[(idx + shift) % partition_count].append(ticker)
    return partitions

def _app_index(app_name: str) -> int:
    if app_name == "app1":
        return 1
    suffix = str(app_name).replace("app", "", 1).strip()
    if not suffix.isdigit():
        raise ValueError(f"Unsupported app name: {app_name}")
    return int(suffix)

def _should_attempt_app_self_heal(app_name: str, detail: str) -> tuple[bool, str]:
    text = str(detail).strip().lower()
    if not text:
        return True, "empty_error_detail"
    if "unsupported app index" in text:
        return False, "unsupported_app_index"
    if "invalid api_key" in text:
        return False, "invalid_api_key_configuration"
    if "api_key" in text and ("not found" in text or "missing" in text):
        return False, "missing_api_key_configuration"
    if "must contain" in text and "api_key" in text:
        return False, "invalid_api_key_file_format"
    return True, "recoverable_auth_validation_error"

def _get_app_self_heal_state(app_name: str) -> dict[str, object]:
    today = str(now_ist().date())
    state = _APP_SELF_HEAL_STATE.get(app_name)
    if state is None or str(state.get("day", "")) != today:
        state = {
            "day": today,
            "attempts": 0,
            "cooldown_until_monotonic": 0.0,
            "last_failure": "",
            "last_attempt_ist": "",
            "last_success_ist": "",
        }
        _APP_SELF_HEAL_STATE[app_name] = state
    return state

def _validate_app_session(app_name: str, setup_fn: Callable[[], object]) -> tuple[bool, str]:
    auth_files = []
    suffix = "" if app_name == "app1" else app_name.replace("app", "", 1)
    for fname in (f"api_key{suffix}.txt" if suffix else "api_key.txt", f"access_token{suffix}.txt" if suffix else "access_token.txt"):
        p = EQIDV2_DIR / fname
        try:
            st = p.stat()
            auth_files.append((fname, int(st.st_mtime_ns), int(st.st_size)))
        except FileNotFoundError:
            auth_files.append((fname, -1, -1))
    signature = tuple(auth_files)
    cached = _APP_VALIDATION_CACHE.get(app_name)
    if cached and cached[0] == signature:
        return cached[1], cached[2]
    try:
        kite = setup_fn()
        profile = kite.profile()
        user_name = str(
            profile.get("user_name") or profile.get("user_id") or profile.get("user_shortname") or "N/A"
        ).strip() or "N/A"
        _APP_VALIDATION_CACHE[app_name] = (signature, True, user_name)
        return True, user_name
    except Exception as exc:
        msg = str(exc).strip() or exc.__class__.__name__
        _APP_VALIDATION_CACHE[app_name] = (signature, False, msg)
        return False, msg

def _attempt_app_session_self_heal(
    app_name: str,
    setup_fn: Callable[[], object],
    failure_detail: str,
) -> tuple[bool, str]:
    should_attempt, attempt_reason = _should_attempt_app_self_heal(app_name, failure_detail)
    if not should_attempt:
        return False, f"self_heal_skipped={attempt_reason}"

    state = _get_app_self_heal_state(app_name)
    attempts_today = int(state.get("attempts", 0) or 0)
    if attempts_today >= DEFAULT_APP_SELF_HEAL_MAX_ATTEMPTS_PER_DAY:
        return False, f"self_heal_daily_limit={DEFAULT_APP_SELF_HEAL_MAX_ATTEMPTS_PER_DAY}"

    cooldown_until = float(state.get("cooldown_until_monotonic", 0.0) or 0.0)
    now_mono = time.monotonic()
    if now_mono < cooldown_until:
        remaining = max(0.0, cooldown_until - now_mono)
        return False, f"self_heal_cooldown={remaining:.0f}s"

    state["attempts"] = attempts_today + 1
    state["last_attempt_ist"] = now_ist().strftime("%Y-%m-%d %H:%M:%S%z")
    print(
        f"[INFO] {app_name}: auth validation failed ({failure_detail}). "
        f"Attempting self-heal {state['attempts']}/{DEFAULT_APP_SELF_HEAL_MAX_ATTEMPTS_PER_DAY}."
    )
    try:
        from authentication_v2 import (
            _read_key_secret,
            _seed_additional_session_for_today,
            _seed_session_for_today,
        )

        primary_parts = _read_key_secret()
        _APP_VALIDATION_CACHE.pop(app_name, None)
        if app_name == "app1":
            _seed_session_for_today(primary_parts, force_login=False)
        else:
            _seed_additional_session_for_today(
                primary_parts,
                force_login=False,
                app_idx=_app_index(app_name),
            )

        _APP_VALIDATION_CACHE.pop(app_name, None)
        ok, detail = _validate_app_session(app_name, setup_fn)
        if ok:
            state["cooldown_until_monotonic"] = 0.0
            state["last_failure"] = ""
            state["last_success_ist"] = now_ist().strftime("%Y-%m-%d %H:%M:%S%z")
            print(f"[OK] {app_name}: self-heal succeeded; session restored for {detail}.")
            return True, f"{detail} (self-healed)"

        state["last_failure"] = str(detail)
        state["cooldown_until_monotonic"] = time.monotonic() + float(DEFAULT_APP_SELF_HEAL_COOLDOWN_SEC)
        return False, f"self_heal_validation_failed={detail}"
    except Exception as exc:
        state["last_failure"] = str(exc).strip() or exc.__class__.__name__
        state["cooldown_until_monotonic"] = time.monotonic() + float(DEFAULT_APP_SELF_HEAL_COOLDOWN_SEC)
        return False, f"self_heal_failed={state['last_failure']}"

def _app_auth_label(app_name: str) -> str:
    if app_name == "app1":
        return "api_key.txt/access_token.txt"
    suffix = app_name.replace("app", "", 1)
    return f"api_key{suffix}.txt/access_token{suffix}.txt"

def _build_working_app_partitions(
    tickers: list[str],
    token_map: dict[str, int],
    partition_shift: int = 0,
) -> tuple[list[tuple[str, list[str], dict[str, int], str]], list[tuple[str, str]]]:
    working_apps: list[tuple[str, str]] = []
    failed_apps: list[tuple[str, str]] = []

    for app_name, setup_fn in _setup_fn_map().items():
        ok, detail = _validate_app_session(app_name, setup_fn)
        if not ok:
            healed, healed_detail = _attempt_app_session_self_heal(app_name, setup_fn, detail)
            if healed:
                ok = True
                detail = healed_detail
            else:
                detail = f"{detail}; {healed_detail}"
        if ok:
            working_apps.append((app_name, detail))
        else:
            failed_apps.append((app_name, detail))

    if not working_apps:
        failure_summary = " | ".join(f"{app_name}: {detail}" for app_name, detail in failed_apps) or "no auth profiles found"
        raise RuntimeError(f"No valid Kite sessions available for 5min fetch. {failure_summary}")

    ticker_partitions = _split_tickers_evenly(
        tickers, len(working_apps), shift=partition_shift
    )
    assignments: list[tuple[str, list[str], dict[str, int], str]] = []
    for idx, (app_name, user_name) in enumerate(working_apps):
        partition_tickers = ticker_partitions[idx]
        assignments.append(
            (
                app_name,
                partition_tickers,
                {ticker: token_map[ticker] for ticker in partition_tickers if ticker in token_map},
                user_name,
            )
        )
    return assignments, failed_apps

def _partition_worker_budget(total_budget: int, active_partitions: int, per_app_cap: int) -> int:
    total_budget = max(1, int(total_budget))
    active_partitions = max(1, int(active_partitions))
    per_app_cap = max(1, int(per_app_cap))
    workers_per_partition = (total_budget + active_partitions - 1) // active_partitions
    return max(1, min(per_app_cap, workers_per_partition))

def _run_partition(
    mode: str,
    partition_name: str,
    partition_tickers: list[str],
    partition_token_map: dict[str, int],
    setup_kite_fn: Callable[[], object],
    *,
    max_workers: int,
    report_dir: str,
    holidays: set,
    refresh_tokens: bool,
    intraday_ts: str,
    skip_if_fresh: bool,
) -> dict[str, object]:
    if not partition_tickers:
        print(f"[INFO] {partition_name}: no tickers assigned; skipping.")
        return {"verify_failed_count": 0, "verify_failed_sample": []}

    current_loader = core.load_stocks_universe
    current_setup = core.setup_kite_session

    def _partition_loader(logger):
        logger.info("[%s] universe override for %s: %d symbols", mode.upper(), partition_name, len(partition_tickers))
        return list(partition_tickers), dict(partition_token_map)

    try:
        core.load_stocks_universe = _partition_loader
        core.setup_kite_session = setup_kite_fn
        return dict(core.run_mode(
            mode,
            max_workers=max_workers,
            skip_if_fresh=bool(skip_if_fresh),
            intraday_ts=str(intraday_ts),
            holidays=holidays,
            report_dir=report_dir,
            refresh_tokens=refresh_tokens,
            print_missing_rows=False,
            print_missing_rows_max=5,
        ) or {})
    finally:
        core.load_stocks_universe = current_loader
        core.setup_kite_session = current_setup

def _run_partition_worker(
    mode: str,
    partition_name: str,
    partition_tickers: list[str],
    partition_token_map: dict[str, int],
    setup_kind: str,
    *,
    max_workers: int,
    report_dir: str,
    holidays: set,
    refresh_tokens: bool,
    intraday_ts: str,
    skip_if_fresh: bool,
    expected_ts_ist: datetime,
    result_queue,
) -> None:
    # Use stream-only logger in child process to avoid concurrent file truncation.
    logger = core.logging.getLogger("stocks_fetcher")
    logger.setLevel(core.logging.INFO)
    if not logger.handlers:
        fmt = core.logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        sh = core.logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)

    setup_fn = _setup_fn_map().get(setup_kind, setup_kite_session_from_eqidv2_dir)
    started_at = time.perf_counter()
    try:
        print(
            f"[PART] {partition_name}: start tickers={len(partition_tickers)} "
            f"max_workers={int(max_workers)} intraday_ts={intraday_ts} skip_if_fresh={bool(skip_if_fresh)}"
        )
        summary = _run_partition(
            mode,
            partition_name,
            partition_tickers,
            partition_token_map,
            setup_fn,
            max_workers=max_workers,
            report_dir=report_dir,
            holidays=holidays,
            refresh_tokens=refresh_tokens,
            intraday_ts=intraday_ts,
            skip_if_fresh=skip_if_fresh,
        )
        verify_failed = list((summary or {}).get("verify_failed_sample", []) or [])
        verify_failed_count = int((summary or {}).get("verify_failed_count", len(verify_failed)) or 0)
        elapsed_sec = time.perf_counter() - started_at
        if verify_failed_count > 0:
            sample = ", ".join(verify_failed[:8])
            print(
                f"[PART] {partition_name}: completed with verification failures "
                f"elapsed={elapsed_sec:.2f}s verify_failed={verify_failed_count}"
            )
            result_queue.put(
                (
                    partition_name,
                    False,
                    f"verify_failed={verify_failed_count} sample={sample}",
                    elapsed_sec,
                    len(partition_tickers),
                    verify_failed_count,
                    verify_failed[:8],
                )
            )
        else:
            print(
                f"[PART] {partition_name}: completed successfully "
                f"elapsed={elapsed_sec:.2f}s tickers={len(partition_tickers)}"
            )
            result_queue.put(
                (
                    partition_name,
                    True,
                    "",
                    elapsed_sec,
                    len(partition_tickers),
                    0,
                    [],
                )
            )
    except Exception as e:
        elapsed_sec = time.perf_counter() - started_at
        print(f"[PART] {partition_name}: failed after {elapsed_sec:.2f}s error={e}")
        result_queue.put(
            (
                partition_name,
                False,
                str(e),
                elapsed_sec,
                len(partition_tickers),
                0,
                [],
            )
        )

def _drain_partition_results(
    result_queue,
    result_map: dict[str, tuple[bool, str, float, int, int, list[str]]],
) -> int:
    drained = 0
    while True:
        try:
            pname, ok, msg, elapsed_sec, ticker_count, verify_failed_count, verify_failed_sample = result_queue.get_nowait()
        except queue.Empty:
            break
        except Exception:
            break

        result_map[str(pname)] = (
            bool(ok),
            str(msg),
            float(elapsed_sec),
            int(ticker_count),
            int(verify_failed_count),
            list(verify_failed_sample),
        )
        drained += 1
    return drained

def _terminate_partition_process(proc: object, pname: str, timeout_sec: float) -> None:
    try:
        proc.terminate()
    except Exception as exc:
        print(f"[WARN] {pname}: terminate() failed after timeout: {exc}")
    try:
        proc.join(timeout=max(0.5, float(timeout_sec)))
    except Exception:
        pass
    if getattr(proc, "is_alive", lambda: False)():
        print(f"[WARN] {pname}: terminate grace elapsed; escalating to kill().")
        try:
            proc.kill()
        except Exception as exc:
            print(f"[WARN] {pname}: kill() failed: {exc}")
        try:
            proc.join(timeout=max(0.5, float(timeout_sec)))
        except Exception:
            pass

def run_update_5m_once(
    max_workers: int,
    max_workers_per_app: int,
    report_dir: str,
    buffer_sec: int,
    refresh_tokens: bool,
    opening_slot: bool = False,
    slot_end: Optional[datetime] = None,
    ready_marker_enabled: bool = DEFAULT_READY_MARKER_ENABLED,
    ready_marker_sample_size: int = DEFAULT_READY_MARKER_SAMPLE_SIZE,
    ready_marker_poll_seconds: float = DEFAULT_READY_MARKER_POLL_SECONDS,
    ready_marker_min_fresh_ratio: float = DEFAULT_READY_MARKER_MIN_FRESH_RATIO,
    slot_sla_warn_sec: float = DEFAULT_SLOT_SLA_WARN_SEC,
    partition_timeout_sec: float = DEFAULT_PARTITION_TIMEOUT_SEC,
    publish_completion_marker: bool = True,
    partition_shift: int = 0,
) -> dict[str, object]:
    slot_started_at = time.perf_counter()
    holidays = _read_holidays_set()
    logger = core.logging.getLogger("stocks_fetcher")
    all_tickers, pre_token_map = core.load_stocks_universe(logger)
    token_map = {str(k).strip().upper(): int(v) for k, v in dict(pre_token_map).items()}

    intraday_ts_mode = "start" if opening_slot else "end"
    skip_if_fresh_mode = False if opening_slot else True
    if opening_slot:
        print(
            "[INFO] Opening-slot mode active: intraday_ts=start, skip_if_fresh=False "
            "(attempting 09:15 opening snapshot fetch)."
        )

    app_assignments, failed_apps = _build_working_app_partitions(
        all_tickers, token_map, partition_shift=partition_shift
    )
    active_apps = [app_name for app_name, ptickers, _, _ in app_assignments if ptickers]

    print(
        "[INFO] 5min working apps:",
        ", ".join(
            f"{app_name}={len(ptickers)} tickers ({_app_auth_label(app_name)}, user={user_name})"
            for app_name, ptickers, _, user_name in app_assignments
        ),
    )
    if failed_apps:
        print(
            "[WARN] 5min auth fallback active:",
            ", ".join(f"{app_name} skipped ({detail})" for app_name, detail in failed_apps),
        )
    if len(active_apps) < len(app_assignments):
        print(
            "[INFO] 5min working apps with empty partitions:",
            ", ".join(app_name for app_name, ptickers, _, _ in app_assignments if not ptickers),
        )

    ctx = mp.get_context("spawn")
    result_queue = ctx.Queue()
    partitions = [
        (app_name, partition_tickers, partition_token_map)
        for app_name, partition_tickers, partition_token_map, _ in app_assignments
    ]
    active_partition_count = sum(1 for _, ptickers, _ in partitions if ptickers)
    partition_max_workers = _partition_worker_budget(
        total_budget=max_workers,
        active_partitions=active_partition_count,
        per_app_cap=max_workers_per_app,
    )

    print(
        "[INFO] 5min worker budget:",
        f"total={max_workers},",
        f"configured_apps=8,",
        f"working_apps={len(app_assignments)},",
        f"active_apps={active_partition_count},",
        f"per_app_cap={max_workers_per_app},",
        f"effective_per_app={partition_max_workers},",
        f"partition_timeout={float(partition_timeout_sec):.1f}s",
    )

    marker_stop_event: Optional[threading.Event] = None
    marker_thread: Optional[threading.Thread] = None
    if bool(ready_marker_enabled) and slot_end is not None:
        ready_sample = _sample_tickers_for_ready_marker(all_tickers, ready_marker_sample_size)
        print(
            "[INFO] Ready marker tuning:",
            f"sample={len(ready_sample)},",
            f"poll_s={float(ready_marker_poll_seconds):.1f},",
            f"min_ratio={float(ready_marker_min_fresh_ratio):.2f}",
        )
        marker_stop_event = threading.Event()
        marker_thread = threading.Thread(
            target=_watch_and_publish_slot_ready_marker,
            args=(slot_end, ready_sample),
            kwargs={
                "poll_seconds": float(ready_marker_poll_seconds),
                "min_fresh_ratio": float(ready_marker_min_fresh_ratio),
                "stop_event": marker_stop_event,
            },
            daemon=True,
        )
        marker_thread.start()

    workers: list[tuple[str, object]] = []
    partition_ticker_counts: dict[str, int] = {}
    for pname, ptickers, ptoken_map in partitions:
        partition_ticker_counts[pname] = len(ptickers)
        proc = ctx.Process(
            target=_run_partition_worker,
            args=(
                "5min",
                pname,
                ptickers,
                ptoken_map,
                pname,
            ),
            kwargs={
                "max_workers": partition_max_workers,
                "report_dir": os.path.join(report_dir, pname),
                "holidays": holidays,
                "refresh_tokens": refresh_tokens,
                "intraday_ts": intraday_ts_mode,
                "skip_if_fresh": skip_if_fresh_mode,
                "expected_ts_ist": (slot_end if slot_end is not None else now_ist()),
                "result_queue": result_queue,
            },
        )
        workers.append((pname, proc))

    for _, proc in workers:
        proc.start()

    result_map: dict[str, tuple[bool, str, float, int, int, list[str]]] = {}
    timed_out_workers: dict[str, tuple[bool, str, float, int, int, list[str]]] = {}
    worker_start_times = {pname: time.perf_counter() for pname, _ in workers}
    pending_workers = {pname: proc for pname, proc in workers}

    while pending_workers:
        _drain_partition_results(result_queue, result_map)
        for pname, proc in list(pending_workers.items()):
            if not proc.is_alive():
                try:
                    proc.join(timeout=0.1)
                except Exception:
                    pass
                pending_workers.pop(pname, None)
                continue

            elapsed_sec = time.perf_counter() - worker_start_times[pname]
            if elapsed_sec >= float(partition_timeout_sec):
                print(
                    f"[WARN] {pname}: partition exceeded timeout "
                    f"({elapsed_sec:.2f}s >= {float(partition_timeout_sec):.2f}s). Terminating worker."
                )
                _terminate_partition_process(proc, pname, DEFAULT_PARTITION_TERMINATE_GRACE_SEC)
                timed_out_workers[pname] = (
                    False,
                    f"partition_timeout={float(partition_timeout_sec):.1f}s",
                    elapsed_sec,
                    int(partition_ticker_counts.get(pname, 0)),
                    0,
                    [],
                )
                pending_workers.pop(pname, None)

        if pending_workers:
            time.sleep(max(0.05, float(DEFAULT_PARTITION_JOIN_POLL_SEC)))

    if marker_stop_event is not None:
        marker_stop_event.set()
    if marker_thread is not None:
        marker_thread.join(timeout=2.0)

    _drain_partition_results(result_queue, result_map)
    for pname, timed_out_result in timed_out_workers.items():
        result_map.setdefault(pname, timed_out_result)
    try:
        result_queue.close()
    except Exception:
        pass
    try:
        result_queue.join_thread()
    except Exception:
        pass

    failures: list[str] = []
    partition_elapsed: dict[str, float] = {}
    partition_symbol_counts: dict[str, int] = {}
    verification_failed_count = 0
    verification_failure_sample: list[str] = []
    for pname, proc in workers:
        ok, msg, elapsed_sec, ticker_count, part_verify_failed_count, part_verify_failed_sample = result_map.get(
            pname,
            (proc.exitcode == 0, f"worker_exit={proc.exitcode}", 0.0, 0, 0, []),
        )
        partition_elapsed[pname] = float(elapsed_sec)
        partition_symbol_counts[pname] = int(ticker_count)
        verification_failed_count += int(part_verify_failed_count)
        for item in part_verify_failed_sample:
            text = str(item).strip()
            if text and text not in verification_failure_sample:
                verification_failure_sample.append(text)
                if len(verification_failure_sample) >= 20:
                    break
        if (not ok) or (proc.exitcode not in (0, None)):
            failures.append(f"{pname}: {msg}")

    total_elapsed_sec = time.perf_counter() - slot_started_at
    elapsed_values = [v for v in partition_elapsed.values() if v > 0]
    if elapsed_values:
        print(
            "[INFO] 5min slot SLA:",
            f"total={total_elapsed_sec:.2f}s,",
            f"avg_partition={sum(elapsed_values)/len(elapsed_values):.2f}s,",
            f"max_partition={max(elapsed_values):.2f}s,",
            f"min_partition={min(elapsed_values):.2f}s,",
            f"warn_threshold={float(slot_sla_warn_sec):.2f}s",
        )
    if total_elapsed_sec > float(slot_sla_warn_sec):
        print(
            "[WARN] 5min slot SLA breach:",
            f"slot={slot_end.strftime('%H:%M') if slot_end is not None else 'n/a'}",
            f"total={total_elapsed_sec:.2f}s > {float(slot_sla_warn_sec):.2f}s",
            "| continuing until verification completes",
        )

    if slot_end is not None:
        try:
            _write_slot_status(
                slot_end,
                total_elapsed_sec=total_elapsed_sec,
                partition_elapsed=partition_elapsed,
                partition_symbol_counts=partition_symbol_counts,
                total_budget=max_workers,
                per_app_cap=max_workers_per_app,
                effective_per_app=partition_max_workers,
                sla_warn_sec=slot_sla_warn_sec,
                failures=failures,
                verification_failed_count=verification_failed_count,
                verification_failure_sample=verification_failure_sample,
            )
        except Exception as exc:
            print(f"[WARN] Failed to write 5min slot status: {exc}")

        # strategy_v2 §M1 — publish the authoritative completion marker AFTER
        # workers join and status is written. This overwrites any earlier
        # watcher-published marker and carries the real fetch outcome so SE can
        # decide between "scan" and "[ABORT] LF_INCOMPLETE" without guessing.
        # P0 fix (2026-04-23): gate behind `publish_completion_marker` so the
        # PF (which calls this with a 6-ticker pending subset) cannot clobber
        # LF's full-universe slot_ready_5m/slot_<…>.json marker. PF passes
        # publish_completion_marker=False; LF leaves it at the default True.
        if bool(publish_completion_marker):
            try:
                tickers_expected_total = int(len(all_tickers))
                tickers_written_total = int(sum(partition_symbol_counts.values()))
                sample_fresh = 0
                sample_checked = 0
                if bool(ready_marker_enabled) and 'ready_sample' in locals() and ready_sample:
                    target_slot = slot_end.astimezone(IST)
                    for ticker in ready_sample:
                        ok, last_ts, _ = _ticker_has_required_5m_slot_data(ticker, target_slot)
                        if last_ts is None:
                            continue
                        sample_checked += 1
                        if ok:
                            sample_fresh += 1
                _publish_slot_completion_marker(
                    slot_end,
                    tickers_expected=tickers_expected_total,
                    tickers_written=tickers_written_total,
                    failures=failures,
                    verification_failed_count=verification_failed_count,
                    verification_failure_sample=verification_failure_sample,
                    duration_ms=float(total_elapsed_sec) * 1000.0,
                    sample_fresh=sample_fresh,
                    sample_checked=sample_checked,
                )
            except Exception as exc:
                print(f"[WARN] Failed to write 5min completion marker: {exc}")

    summary = {
        "total_elapsed_sec": float(total_elapsed_sec),
        "partition_elapsed_sec": partition_elapsed,
        "partition_symbol_counts": partition_symbol_counts,
        "max_partition_elapsed_sec": max(elapsed_values) if elapsed_values else 0.0,
        "min_partition_elapsed_sec": min(elapsed_values) if elapsed_values else 0.0,
        "avg_partition_elapsed_sec": (sum(elapsed_values) / len(elapsed_values)) if elapsed_values else 0.0,
        "total_worker_budget": int(max_workers),
        "per_app_cap": int(max_workers_per_app),
        "effective_per_app": int(partition_max_workers),
        "partition_timeout_sec": float(partition_timeout_sec),
        "sla_warn_sec": float(slot_sla_warn_sec),
        "sla_breached": bool(total_elapsed_sec > float(slot_sla_warn_sec)),
        "failures": list(failures),
        "verification_failed_count": int(verification_failed_count),
        "verification_failure_sample": list(verification_failure_sample),
    }
    if failures:
        raise ParallelPartitionRunError(
            "Parallel partition run failed: " + " | ".join(failures),
            summary,
        )

    return summary

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="Total worker budget across all 8 app partitions.",
    )
    ap.add_argument(
        "--max-workers-per-app",
        type=int,
        default=DEFAULT_MAX_WORKERS_PER_APP,
        help="Hard cap for workers inside each app partition.",
    )
    ap.add_argument("--buffer-sec", type=int, default=DEFAULT_BUFFER_SEC, help="How long after boundary to run (Kite can lag).")
    ap.add_argument(
        "--quarter-hour-buffer-sec",
        type=int,
        default=DEFAULT_QUARTER_HOUR_BUFFER_SEC,
        help="How long after :00/:15/:30/:45 boundaries to run 5-minute slots to avoid colliding with the 15-minute fetch.",
    )
    ap.add_argument(
        "--ready-marker-enabled",
        dest="ready_marker_enabled",
        action="store_true",
        help="Publish a per-slot readiness marker once sampled 5m parquet files look fresh enough.",
    )
    ap.add_argument(
        "--no-ready-marker-enabled",
        dest="ready_marker_enabled",
        action="store_false",
        help="Disable readiness marker publishing.",
    )
    ap.set_defaults(ready_marker_enabled=DEFAULT_READY_MARKER_ENABLED)
    ap.add_argument("--ready-marker-sample-size", type=int, default=DEFAULT_READY_MARKER_SAMPLE_SIZE)
    ap.add_argument("--ready-marker-poll-seconds", type=float, default=DEFAULT_READY_MARKER_POLL_SECONDS)
    ap.add_argument("--ready-marker-min-fresh-ratio", type=float, default=DEFAULT_READY_MARKER_MIN_FRESH_RATIO)
    ap.add_argument("--refresh-tokens", dest="refresh_tokens", action="store_true", help="Force refresh kite instrument token cache.")
    ap.add_argument("--no-refresh-tokens", dest="refresh_tokens", action="store_false", help="Do not refresh kite instrument token cache.")
    ap.set_defaults(refresh_tokens=DEFAULT_REFRESH_TOKENS)
    ap.add_argument(
        "--enable-opening-slot-fetch",
        dest="enable_opening_slot_fetch",
        action="store_true",
        help="Process 09:15 slot using opening snapshot mode.",
    )
    ap.add_argument(
        "--disable-opening-slot-fetch",
        dest="enable_opening_slot_fetch",
        action="store_false",
        help="Skip 09:15 slot (old behavior).",
    )
    ap.set_defaults(enable_opening_slot_fetch=DEFAULT_ENABLE_OPENING_SLOT_FETCH)
    ap.add_argument(
        "--report-dir",
        default=str(RUNTIME_REPORTS_DIR / "stocks_missing_reports_5m"),
    )
    args = ap.parse_args()

    # Ensure core INFO logs (including timing/verification summaries) are emitted.
    try:
        core.setup_logger()
        print("[INFO] stocks_fetcher logger initialized.")
    except Exception as e:
        print(f"[WARN] Failed to initialize stocks_fetcher logger: {e}")

    print("[LIVE] EQIDV2 5m scheduler started.")
    print(f"       Using EQIDV2_DIR: {EQIDV2_DIR}")
    print(f"       Output dir (5m): {getattr(core, 'DIRS', {}).get('5min', {}).get('out', str(RUNTIME_DATA_5M_DIR))}")
    print(f"       Runs every 5 mins between {MARKET_OPEN.strftime('%H:%M')} and {MARKET_CLOSE.strftime('%H:%M')} IST (trading days).")
    print(f"       Buffer after boundary: {args.buffer_sec}s")
    print(f"       Quarter-hour buffer after boundary: {args.quarter_hour_buffer_sec}s")
    print(f"       Max workers (total budget): {args.max_workers}")
    print(f"       Max workers per app cap: {args.max_workers_per_app}")
    print(
        "       Slot SLA:",
        f"{DEFAULT_SLOT_SLA_WARN_SEC:.1f}s soft warning only; fetch continues until verification completes.",
    )
    print(
        "       Kite request timeout:",
        f"{DEFAULT_KITE_REQUEST_TIMEOUT_SEC:.1f}s",
    )
    print(
        "       Partition timeout:",
        f"{DEFAULT_PARTITION_TIMEOUT_SEC:.1f}s",
    )
    print(
        "       Adaptive throttle:",
        f"enabled={DEFAULT_ADAPTIVE_THROTTLE}, min_total={DEFAULT_ADAPTIVE_MIN_WORKERS}, "
        f"min_per_app={DEFAULT_ADAPTIVE_MIN_WORKERS_PER_APP}, step_total={DEFAULT_ADAPTIVE_TOTAL_STEP}, "
        f"step_per_app={DEFAULT_ADAPTIVE_PER_APP_STEP}",
    )
    print(
        "       Auth self-heal:",
        f"cooldown={DEFAULT_APP_SELF_HEAL_COOLDOWN_SEC}s, "
        f"max_attempts_per_day={DEFAULT_APP_SELF_HEAL_MAX_ATTEMPTS_PER_DAY}",
    )
    print(
        "       Ready marker:",
        f"enabled={args.ready_marker_enabled}, sample={args.ready_marker_sample_size}, "
        f"poll={float(args.ready_marker_poll_seconds):.1f}s, "
        f"min_ratio={float(args.ready_marker_min_fresh_ratio):.2f}",
    )
    print(f"       Refresh tokens: {args.refresh_tokens}")
    print(f"       Opening slot fetch (09:15): {args.enable_opening_slot_fetch}")
    print(f"       Process will exit at {HARD_STOP.strftime('%H:%M')} IST.")
    holidays = _read_holidays_set()
    print(f"       Holidays loaded: {len(holidays)}")

    last_run_slot: Optional[datetime] = None
    current_max_workers = int(args.max_workers)
    current_max_workers_per_app = int(args.max_workers_per_app)
    healthy_streak = 0

    while True:
        dt = now_ist()
        if dt.time() >= HARD_STOP:
            print("[DONE] Hard stop reached. Exiting.")
            return

        if not _is_trading_day(dt, holidays):
            nxt = _next_trading_day_open(dt + timedelta(days=1), holidays)
            sleep_s = max(30.0, (nxt - dt).total_seconds())
            print(
                f"[WAIT] Non-trading day ({dt.strftime('%Y-%m-%d')}). "
                f"Sleeping {int(min(sleep_s, 300))}s..."
            )
            time.sleep(min(sleep_s, 300))
            continue

        if not _is_trading_time(dt):
            nxt = IST.localize(datetime(dt.year, dt.month, dt.day, MARKET_OPEN.hour, MARKET_OPEN.minute, 0))
            if dt.time() > MARKET_OPEN:
                nxt = nxt + timedelta(days=1)
            sleep_s = max(30.0, (nxt - dt).total_seconds())
            print(f"[WAIT] Outside market hours. Sleeping {int(min(sleep_s, 300))}s...")
            time.sleep(min(sleep_s, 300))
            continue

        # Determine the slot we should process: last completed 5m boundary
        slot_end = _floor_to_5m(dt)

        opening_slot = slot_end.time() < FIRST_5M_CLOSE
        if opening_slot and (not args.enable_opening_slot_fetch):
            if last_run_slot != slot_end:
                print(
                    f"[SKIP] Opening slot {slot_end.strftime('%H:%M')} disabled by config. "
                    "First actionable slot is 09:20."
                )
            last_run_slot = slot_end
            nxt = _next_boundary(dt) + timedelta(seconds=int(args.buffer_sec))
            time.sleep(max(2.0, (nxt - now_ist()).total_seconds()))
            continue

        # Don't run until buffer has passed for this slot_end
        slot_buffer_sec = _slot_buffer_seconds(
            slot_end,
            int(args.buffer_sec),
            int(args.quarter_hour_buffer_sec),
        )
        if dt < (slot_end + timedelta(seconds=slot_buffer_sec)):
            wake = slot_end + timedelta(seconds=slot_buffer_sec)
            time.sleep(max(1.0, (wake - dt).total_seconds()))
            continue

        if last_run_slot == slot_end:
            # Sleep until next slot buffer
            nxt = _next_boundary(dt) + timedelta(seconds=int(args.buffer_sec))
            time.sleep(max(2.0, (nxt - now_ist()).total_seconds()))
            continue

        tag = "OPEN" if opening_slot else "RUN "
        if slot_buffer_sec != int(args.buffer_sec):
            print(
                f"[INFO] Quarter-hour slot {slot_end.strftime('%H:%M')} using staggered buffer "
                f"{slot_buffer_sec}s (base={int(args.buffer_sec)}s)."
            )
        print(f"[{tag}] Updating EQIDV2 5m for slot {slot_end.strftime('%H:%M')} at {dt.strftime('%Y-%m-%d %H:%M:%S%z')}")
        slot_summary: Optional[dict[str, object]] = None
        try:
            slot_summary = run_update_5m_once(
                max_workers=int(current_max_workers),
                max_workers_per_app=int(current_max_workers_per_app),
                report_dir=str(args.report_dir),
                buffer_sec=slot_buffer_sec,
                refresh_tokens=bool(args.refresh_tokens),
                opening_slot=bool(opening_slot),
                slot_end=slot_end,
                ready_marker_enabled=bool(args.ready_marker_enabled),
                ready_marker_sample_size=int(args.ready_marker_sample_size),
                ready_marker_poll_seconds=float(args.ready_marker_poll_seconds),
                ready_marker_min_fresh_ratio=float(args.ready_marker_min_fresh_ratio),
                partition_timeout_sec=float(DEFAULT_PARTITION_TIMEOUT_SEC),
            )
        except Exception as e:
            print(f"[ERROR] Update failed: {e}", file=sys.stderr)
            failure_summary = getattr(e, "summary", None)
            if isinstance(failure_summary, dict):
                slot_summary = dict(failure_summary)
            healthy_streak = 0

        if slot_summary is not None:
            next_max_workers, next_per_app_cap, healthy_streak, budget_reason = _adapt_worker_budget(
                configured_total=int(args.max_workers),
                configured_per_app=int(args.max_workers_per_app),
                current_total=int(current_max_workers),
                current_per_app=int(current_max_workers_per_app),
                slot_summary=slot_summary,
                healthy_streak=int(healthy_streak),
            )
            if (
                next_max_workers != current_max_workers
                or next_per_app_cap != current_max_workers_per_app
            ):
                print(
                    "[INFO] 5min adaptive throttle:",
                    f"next_total={next_max_workers},",
                    f"next_per_app={next_per_app_cap},",
                    f"prev_total={current_max_workers},",
                    f"prev_per_app={current_max_workers_per_app},",
                    f"reason={budget_reason}",
                )
            elif budget_reason.startswith("healthy_streak="):
                print(f"[INFO] 5min adaptive throttle: {budget_reason}")
            current_max_workers = int(next_max_workers)
            current_max_workers_per_app = int(next_per_app_cap)

        last_run_slot = slot_end

        # Sleep until next slot + buffer
        nxt = _next_boundary(dt) + timedelta(seconds=int(args.buffer_sec))
        print(f"[INFO] Done slot {slot_end.strftime('%H:%M')}. Next at {nxt.strftime('%H:%M:%S')}.")
        time.sleep(max(2.0, (nxt - now_ist()).total_seconds()))

if __name__ == "__main__":
    main()
