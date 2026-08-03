"""
Entry engine 1 min v7 ID.

Consumes "Signal discovery v7 5mins ID" candidate tickers for the previous
5-minute slot, fetches raw 1-minute OHLCV only for those tickers, stores the raw
data as parquet, and writes executable rows into the existing ID 5min v7 live
entry CSVs:

  live_signals/signals_<YYYY-MM-DD>_id_5min_v7_short.csv
  live_signals/signals_<YYYY-MM-DD>_id_5min_v7_long.csv

This module does not calculate indicators. It only uses raw 1-minute OHLCV for
entry price discovery.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import os
import threading
import time
import urllib.request as _urlrequest
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

import avwap_5min_ID_v6_backtesting as v6
import eqidv2_v11_live_overlay as v11_live_overlay
from eqidv2_v7_position_sizing import (
    RiskSizingConfig,
    nifty_regime_short_multiplier,
    risk_based_quantity,
)
import eqidv2_final_conf_live_bootstrap as _conf_boot
import eqidv2_eod_scheduler_for_5mins_data_live_minimal as scheduler
import eqidv2_live_combined_analyser_csv_id_5min_v7_persistent as v7_persistent
import eqidv2_live_signal_writer as signal_writer
import eqidv2_v7_signal_contract as signal_contract
import eqidv2_decision_funnel as decision_funnel
import eqidv2_pre_momentum as shared_pre_momentum
from eqidv2_runtime_manifest import freeze_runtime_manifest
from eqidv2_runtime_paths import DATA_1MIN_DIR, DATA_5M_DIR, RUNTIME_ROOT, RUNTIME_STATUS_DIR, runtime_dir


SESSION_NAME = "Entry engine 1 min v7 ID"
SESSION_SLUG = "entry_engine_1min_v5_ID"

_REPLAY_OUTPUT_ROOT_RAW = os.getenv("EQIDV2_REPLAY_OUTPUT_ROOT", "").strip()
if _REPLAY_OUTPUT_ROOT_RAW:
    _REPLAY_OUTPUT_ROOT = Path(_REPLAY_OUTPUT_ROOT_RAW)
    _IS_REPLAY_ISOLATED = True
    SESSION_ROOT = _REPLAY_OUTPUT_ROOT / SESSION_SLUG
    RAW_1MIN_DIR = _REPLAY_OUTPUT_ROOT / "stocks_raw_1min_entry_v5_id_live"
    _SIGNAL_DISCOVERY_ROOT_DEFAULT = _REPLAY_OUTPUT_ROOT / "signal_discovery_v7_5mins_ID"
else:
    _REPLAY_OUTPUT_ROOT = None
    _IS_REPLAY_ISOLATED = False
    SESSION_ROOT = runtime_dir(SESSION_SLUG)
    RAW_1MIN_DIR = runtime_dir("stocks_raw_1min_entry_v5_id_live")
    _SIGNAL_DISCOVERY_ROOT_DEFAULT = runtime_dir("signal_discovery_v7_5mins_ID")

SLOT_RAW_DIR = SESSION_ROOT / "slot_raw_1min"
AUDIT_DIR = SESSION_ROOT / "audit"
LATEST_DIR = SESSION_ROOT / "latest"
HEARTBEAT_DIR = SESSION_ROOT / "heartbeat"
SIGNAL_DISCOVERY_ROOT = Path(
    os.getenv("EQIDV2_ENTRY_ENGINE_SIGNAL_DISCOVERY_ROOT", str(_SIGNAL_DISCOVERY_ROOT_DEFAULT))
)
SIGNAL_DISCOVERY_LATEST_JSON = SIGNAL_DISCOVERY_ROOT / "latest" / "latest_candidate_tickers.json"
SIGNAL_DISCOVERY_LATEST_CSV = SIGNAL_DISCOVERY_ROOT / "latest" / "latest_candidate_tickers.csv"
USE_SLOT_CANDIDATE_JSON = str(
    os.getenv("EQIDV2_ENTRY_ENGINE_USE_SLOT_CANDIDATE_JSON", "1" if _IS_REPLAY_ISOLATED else "0")
).strip().lower() not in {"0", "false", "no", "off", "disabled"}
REQUIRE_SLOT_COMPLETE_MARKER = str(
    os.getenv(
        "EQIDV2_ENTRY_ENGINE_REQUIRE_SLOT_COMPLETE_MARKER",
        "1" if USE_SLOT_CANDIDATE_JSON else "0",
    )
).strip().lower() not in {"0", "false", "no", "off", "disabled"}

IST = pytz.timezone("Asia/Kolkata")
MARKET_OPEN = dtime(9, 15)
END_TIME = dtime(15, 1)
HARD_STOP = dtime(15, 35)
ENTRY_DELAY_SEC = int(os.getenv("EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_DELAY_SEC", "60"))
ENTRY_DUE_GRACE_CONFIG_SEC = int(os.getenv("EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_DUE_GRACE_SEC", "90"))
POLL_SEC = float(os.getenv("EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_POLL_SEC", "1.0"))
KITE_INTERVAL = "minute"
RAW_FETCH_PARALLEL_APPS_ENABLED = str(
    os.getenv("EQIDV2_ENTRY_ENGINE_RAW_FETCH_PARALLEL_APPS", "1")
).strip().lower() not in {"0", "false", "no", "off", "disabled"}
RAW_FETCH_APP_COUNT = max(1, min(8, int(os.getenv("EQIDV2_ENTRY_ENGINE_RAW_FETCH_APP_COUNT", "8"))))
REPLAY_USE_LOCAL_1MIN = str(
    os.getenv("EQIDV2_ENTRY_ENGINE_REPLAY_USE_LOCAL_1MIN", "0")
).strip().lower() in {"1", "true", "yes", "on", "enable", "enabled"}
RAW_FETCH_SESSION_START = os.getenv(
    "EQIDV2_ENTRY_ENGINE_RAW_FETCH_SESSION_START", "09:15"
).strip()

# v8 backtesting resolves exits strictly from v6.SETUP_EXIT_RULES. The live
# signal-discovery stage stays signal-only; SL/target are attached here.
#
# T+1 execution contract:
#   T = completed, end-labelled 5-minute signal candle.
#   The engine wakes one wall-clock minute later and the executor fills from
#   live LTP. Minute OHLC is used only for reference and momentum filters.
#   Freshness is measured from intended_entry_ist=T+1.  The default code
#   contract is 30 seconds; the conf launcher may grant a larger fail-closed
#   handoff window while the separately reported write SLA stays at 10 seconds.
#
# IMPORTANT: V7 is unprofitable with its full setup set after realistic costs.
# Do not expand beyond the proven profitable setups (production_core).
ENTRY_SEARCH_MAX_DELAY_MIN = int(os.getenv("EQIDV2_ENTRY_ENGINE_1MIN_V7_MAX_DELAY_MIN", "3"))
# Minutes from signal_time_ist to intended_entry_ist.
# Default = 1, so intended_entry_ist = signal_time + 1 minute.
ENTRY_SIGNAL_TO_ENTRY_LAG_MIN = int(os.getenv("EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_ENTRY_LAG_MIN", "1"))
CANDIDATE_WAIT_SEC = float(os.getenv("EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_CANDIDATE_WAIT_SEC", "30"))
CANDIDATE_WAIT_POLL_SEC = float(os.getenv("EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_CANDIDATE_WAIT_POLL_SEC", "2"))
ENTRY_WRITE_SLA_SEC = float(
    os.getenv("EQIDV2_ENTRY_ENGINE_WRITE_SLA_SEC", "10")
)
MAX_SIGNAL_HANDOFF_LAG_SEC = float(
    os.getenv(
        "EQIDV2_ID5MIN_V7_MAX_ENTRY_TO_DETECTION_LAG_SEC",
        str(v7_persistent.MAX_ENTRY_TO_DETECTION_LAG_SEC),
    )
)
ENTRY_PROCESS_RESERVE_SEC = float(
    os.getenv("EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_PROCESS_RESERVE_SEC", "2")
)
ENTRY_DUE_GRACE_SEC = (
    min(
        ENTRY_DUE_GRACE_CONFIG_SEC,
        max(0, int(MAX_SIGNAL_HANDOFF_LAG_SEC - ENTRY_PROCESS_RESERVE_SEC)),
    )
    if MAX_SIGNAL_HANDOFF_LAG_SEC > 0
    else ENTRY_DUE_GRACE_CONFIG_SEC
)
V11_BACKTESTING_OVERLAY_ENABLE = str(
    os.getenv("EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_V11_BACKTESTING_OVERLAY", "1")
).strip().lower() not in {"0", "false", "no", "off", "disabled"}
V11_BACKTESTING_OVERLAY_PROFILE = os.getenv(
    "EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_V11_BACKTESTING_PROFILE",
    v11_live_overlay.DEFAULT_SELECTED_STRATEGY_PROFILE,
).strip()
V7_SIGNAL_MARGIN_RS = 20_000.0
V7_INTRADAY_LEVERAGE = 5.0
V7_SIGNAL_NOTIONAL_RS = V7_SIGNAL_MARGIN_RS * V7_INTRADAY_LEVERAGE  # legacy fallback

# P1-7: Risk-based sizing — size each trade to a fixed % of equity at the stop.
# Defaults: 0.25% risk on Rs 2L equity, capped Rs 50k–150k notional per trade.
# Override all three via env vars; set EQIDV2_RISK_SIZING_ENABLED=0 to revert
# to fixed-notional (legacy) sizing.
RISK_SIZING_ENABLED = str(os.getenv("EQIDV2_RISK_SIZING_ENABLED", "1")).strip().lower() not in {
    "0", "false", "no", "off"
}
RISK_PCT_PER_TRADE = float(os.getenv("EQIDV2_RISK_PCT_PER_TRADE", "0.25"))
RISK_EQUITY_RS = float(os.getenv("EQIDV2_RISK_EQUITY_RS", "200000.0"))
RISK_MAX_NOTIONAL_RS = float(os.getenv("EQIDV2_RISK_MAX_NOTIONAL_RS", "150000.0"))
RISK_MIN_NOTIONAL_RS = float(os.getenv("EQIDV2_RISK_MIN_NOTIONAL_RS", "50000.0"))

# P1-7: NIFTY regime gate — halve short size when NIFTY > rising 20-day MA (daily).
NIFTY_REGIME_GATE_ENABLED = str(os.getenv("EQIDV2_NIFTY_REGIME_GATE_ENABLED", "1")).strip().lower() not in {
    "0", "false", "no", "off"
}
NIFTY_5MIN_PARQUET = os.getenv(
    "EQIDV2_NIFTY_5MIN_PARQUET",
    r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live\NIFTY_stocks_indicators_5min.parquet",
)
NIFTY_MA_DAYS = int(os.getenv("EQIDV2_NIFTY_MA_DAYS", "20"))
NIFTY_REGIME_SHORT_SIZE_MULT = float(os.getenv("EQIDV2_NIFTY_REGIME_SHORT_SIZE_MULT", "0.5"))

# P2-11: ADV liquidity cap — reject signals whose per-trade notional exceeds
# ADV_PARTICIPATION_PCT% of the ticker's 20-day average daily traded value.
# ADV_RS = mean(close * volume) over last ADV_LOOKBACK_DAYS trading days.
# Default 1% participation is conservative for Rs 50cr+ ADV stocks.
# Set EQIDV2_ADV_CAP_ENABLED=0 to disable, or raise participation for large-caps.
ADV_CAP_ENABLED = str(os.getenv("EQIDV2_ADV_CAP_ENABLED", "1")).strip().lower() not in {
    "0", "false", "no", "off"
}
ADV_PARTICIPATION_PCT = float(os.getenv("EQIDV2_ADV_PARTICIPATION_PCT", "1.0"))
ADV_LOOKBACK_DAYS = int(os.getenv("EQIDV2_ADV_LOOKBACK_DAYS", "20"))
ADV_DAILY_PARQUET_DIR = os.getenv(
    "EQIDV2_ADV_DAILY_PARQUET_DIR",
    r"C:\TradingData\eqidv2\stocks_indicators_daily_eq",
)

# P2-12: F&O ban pre-trade filter — block new SHORT entries on F&O-banned stocks.
# NSE publishes the daily securities-in-ban-period list at ~07:00 IST.
# Fetched once per trading day, cached in memory.  Fail-open: if the fetch fails
# (network down, NSE URL changes), the cache stays empty and no trades are blocked.
FNO_BAN_FILTER_ENABLED = str(os.getenv("EQIDV2_FNO_BAN_FILTER_ENABLED", "1")).strip().lower() not in {
    "0", "false", "no", "off"
}
FNO_BAN_URL = os.getenv(
    "EQIDV2_FNO_BAN_URL",
    "https://nsearchives.nseindia.com/content/fo/fo_secban.csv",
)
FNO_BAN_LOCAL_FILE = os.getenv(
    "EQIDV2_FNO_BAN_LOCAL_FILE",
    r"C:\TradingData\eqidv2\runtime_status\fo_secban_today.csv",
)
FNO_BAN_FETCH_TIMEOUT_SEC = float(os.getenv("EQIDV2_ENTRY_ENGINE_FNO_BAN_FETCH_TIMEOUT_SEC", "3"))

# Pre-entry momentum gates promoted from the 2026-05-21..2026-06-03 live paper
# replay. These run after the 1-minute entry bar is known but before signal CSVs
# are written, so rejected rows never become executable live/paper signals.
PRE_ENTRY_MOMENTUM_GATES_ENABLED = str(
    os.getenv("EQIDV2_ENTRY_ENGINE_PRE_MOMENTUM_GATES", "1")
).strip().lower() not in {"0", "false", "no", "off", "disabled"}
PRE_ENTRY_MOMENTUM_MISSING_ACTION = os.getenv(
    "EQIDV2_ENTRY_ENGINE_PRE_MOMENTUM_MISSING_ACTION",
    "block",
).strip().lower()
PRE_ENTRY_MOMENTUM_GATE_VERSION = "v7_pre_entry_momentum_2026_06_04_t_probation"
# P1.1: E_VWAP_LOSE_EARLY_SHORT must not fire before 09:45 IST — the first two
# 5-minute bars (09:30, 09:35, 09:40) do not have a meaningful intraday VWAP.
E_VWAP_EARLY_SHORT_MIN_SLOT = dtime(9, 45)
# A_MOD_BREAK_C1_HIGH: cap to top-2 per slot ranked by vwap_dist_atr descending.
# Validated over 10 sessions (32 trades, PF 3.25). Signal-time gate mirrors V11.
A_MOD_C1_HIGH_MAX_SLOT = dtime(11, 10)
A_MOD_C1_HIGH_TOP_N_PER_SLOT = int(os.getenv("EQIDV2_ENTRY_ENGINE_AMOD_C1_HIGH_TOP_N_PER_SLOT", "2"))
# C_OR_BREAKOUT: VWAP separation floor and ATR cap derived from v8 backtest bucket
# analysis (842 trades). avwap_dist>=2.0 → PF 2.46; atr_pct>=0.010 → PF 0.93.
C_OR_BREAKOUT_MIN_VWAP_DIST_ATR = float(os.getenv("EQIDV2_ENTRY_ENGINE_COR_BREAKOUT_MIN_VWAP_DIST_ATR", "2.0"))
C_OR_BREAKOUT_MAX_ATR_PCT = float(os.getenv("EQIDV2_ENTRY_ENGINE_COR_BREAKOUT_MAX_ATR_PCT", "0.010"))
# C_OR_BREAKOUT time window: bars 1-6 (09:25-09:50) WR=33.2% (early false breaks),
# bars 7-18 (09:55-10:40) WR=71.2% PF=1.71, bars 19+ PF=0.68. Enforce morning only.
C_OR_BREAKOUT_MIN_SIGNAL_TIME = dtime(9, 55)
C_OR_BREAKOUT_MAX_SIGNAL_TIME = dtime(10, 40)

PRE_ENTRY_MOMENTUM_SETUP_GATES: Dict[str, Tuple[Tuple[str, str, float], ...]] = {
    "B_AVWAP_RECLAIM_REVERSAL": (
        ("pre_entry_momentum_score", "<=", 64.7678),
    ),
    "C_OR_BREAKOUT": (
        ("sig5_adx_calc", ">=", 25.0),      # ADX<25 → WR=23.6%
        ("sig5_rsi_dir", ">=", 60.0),       # RSI<60 → WR=18.3% (for LONG: sig5_rsi_dir=RSI)
        ("sig5_vol_ratio20", ">=", 1.5),    # vol<1.5x → WR=43.4%; strong breakout bar required
        ("pre2_mom_r", ">=", -0.050),
    ),
    "D_EMA20_BOUNCE": (
        ("pre3_range_r", ">=", 0.292349),
        ("pre_entry_momentum_score", "<=", 78.3448),
    ),
    "D_EMA20_REJECTION": (
        ("pre10_mom_r", "<=", 0.156614),
        ("pre5_mom_r", ">=", 0.12493),
        ("sig5_adx_calc", ">=", 20.0),  # P1.3: require trend presence — rejects flat/chop entries (e.g. MARICO ADX=12.38)
    ),
    "E_ORB_BREAKOUT_LONG": (
        ("pre15_vol_ratio20", "<=", 1.08301),
        ("pre1_adx", ">=", 42.3138),
    ),
    "E_ORB_BREAKOUT_SHORT": (
        ("pre10_dir_count", ">=", 5.0),
        ("pre5_vol_ratio20", ">=", 1.65561),
    ),
    "E_VWAP_LOSE_EARLY_SHORT": (
        ("sig5_vol_ratio20", ">=", 1.5643),
        ("pre3_body_sum_r", "<=", 0.797498),
    ),
    "G_HIGHER_HIGH_BREAK": (
        ("pre3_close_pos", "<=", 0.985417),
        ("sig5_rsi_dir", "<=", 67.878),
    ),
    "L_TREND_PULLBACK": (
        ("pre_entry_momentum_score", ">=", 73.021),
        ("pre2_mom_r", ">=", 0.233909),
    ),
    "T_TREND_DAY_EMA_STAIR_SHORT": (
        ("pre3_close_pos", ">=", 0.662492),
        ("pre5_dir_count", "<=", 3.0),
        ("pre1_adx", "<=", 31.0),
        ("pre5_range_r", ">=", 0.35),
    ),
}
PRE_ENTRY_MOMENTUM_SHADOW_SETUPS = {
    "C_OR_BREAKDOWN",
    # A_MOD_BREAK_C1_HIGH removed 2026-06-09: new V11 gate (rs_pct/atr_pct/time)
    # replaces the old market_abs/vol_ratio blocker; slot ranking caps live exposure.
}


# --- final_setup_conf shared setup-book runtime activation ------------------
# When EQIDV2_USE_FINAL_SETUP_CONF is set, the conf becomes the single source of
# truth for this engine: the per-setup pre-momentum gates and exit LEVELS are
# replaced by the conf's, and any conf setup is un-shadowed. The exit MECHANISM
# is untouched (the executor still places real market SL/target orders; this only
# changes the SL/target *levels* written into the signal CSV). Default OFF.
# _ensure_conf_engine() is idempotent and called once at the top of run_slot.
_CONF_ENGINE_ACTIVE = False
_CONF_ENGINE_BOOTSTRAPPED = False


def _ensure_conf_engine() -> bool:
    global _CONF_ENGINE_ACTIVE, _CONF_ENGINE_BOOTSTRAPPED
    global PRE_ENTRY_MOMENTUM_SETUP_GATES, PRE_ENTRY_MOMENTUM_SHADOW_SETUPS
    global PRE_ENTRY_MOMENTUM_GATES_ENABLED, PRE_ENTRY_MOMENTUM_MISSING_ACTION
    if _CONF_ENGINE_BOOTSTRAPPED:
        return _CONF_ENGINE_ACTIVE
    _CONF_ENGINE_BOOTSTRAPPED = True
    if not _conf_boot.is_enabled():
        return False
    ukeys = set(_conf_boot.conf_keys_upper())
    PRE_ENTRY_MOMENTUM_SETUP_GATES = _conf_boot.pre_momentum_gates_from_conf()
    PRE_ENTRY_MOMENTUM_SHADOW_SETUPS = {
        s for s in PRE_ENTRY_MOMENTUM_SHADOW_SETUPS if _conf_boot._u(s) not in ukeys
    }
    PRE_ENTRY_MOMENTUM_GATES_ENABLED = True
    PRE_ENTRY_MOMENTUM_MISSING_ACTION = "block"
    # Per-setup SL/target LEVELS (executor reads stop_price/target_price from the
    # signal CSV, which this engine builds from v6.SETUP_EXIT_RULES).
    v6.SETUP_EXIT_RULES.update(_conf_boot.exit_rules_from_conf())
    _CONF_ENGINE_ACTIVE = True
    print(
        f"[entry_engine] final_setup_conf ACTIVE ({_conf_boot.GATE_VERSION}): "
        f"source={_conf_boot.conf_source()} {len(_conf_boot.conf_keys())} setups; "
        f"pre-momentum gates + exit levels from conf; "
        f"un-shadowed conf setups; exit mechanism (market SL/target) unchanged.",
        flush=True,
    )
    return True


_indicator_1m_cache: Dict[str, Tuple[float, pd.DataFrame]] = {}
_indicator_5m_cache: Dict[str, Tuple[float, pd.DataFrame]] = {}
_raw_kite_session_cache: List[Tuple[str, Any]] = []
_raw_token_cache: Dict[str, int] = {}
_raw_session_cache_lock = threading.Lock()
_last_raw_fetch_stats: Dict[str, Any] = {}
_last_candidate_load_stats: Dict[str, Any] = {}
_progress_lock = threading.Lock()


for _p in (SESSION_ROOT, RAW_1MIN_DIR, SLOT_RAW_DIR, AUDIT_DIR, LATEST_DIR, HEARTBEAT_DIR):
    _p.mkdir(parents=True, exist_ok=True)


def _logger() -> logging.Logger:
    log = logging.getLogger(SESSION_SLUG)
    log.setLevel(logging.INFO)
    if not log.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        log.addHandler(h)
    return log


def _set_status_env() -> None:
    if _IS_REPLAY_ISOLATED:
        os.environ["EQIDV2_RUNTIME_STATUS_FILE"] = str(HEARTBEAT_DIR / f"{SESSION_SLUG}.status")
        os.environ["EQIDV2_RUNTIME_HEARTBEAT_FILE"] = str(HEARTBEAT_DIR / f"{SESSION_SLUG}.heartbeat")
    else:
        os.environ["EQIDV2_RUNTIME_STATUS_FILE"] = str(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.status")
        os.environ["EQIDV2_RUNTIME_HEARTBEAT_FILE"] = str(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.heartbeat")


def _touch_status(status: str, **extra: Any) -> None:
    _set_status_env()
    v7_persistent.base_v15._touch_runtime_status(status, session=SESSION_NAME, **extra)
    payload = {
        "status": status,
        "session": SESSION_NAME,
        "updated_at_ist": _fmt_ist(pd.Timestamp.now(tz=IST)),
        **extra,
    }
    (HEARTBEAT_DIR / "entry_engine.status.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def _touch_heartbeat(status: str = "RUNNING", **extra: Any) -> None:
    _set_status_env()
    v7_persistent.base_v15._touch_runtime_heartbeat(status, session=SESSION_NAME, **extra)
    payload = {
        "status": status,
        "session": SESSION_NAME,
        "updated_at_ist": _fmt_ist(pd.Timestamp.now(tz=IST)),
        **extra,
    }
    (HEARTBEAT_DIR / "entry_engine.heartbeat.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def _touch_progress(
    phase: str,
    *,
    slot: Any = None,
    status: str = "RUNNING",
    log_line: bool = False,
    **extra: Any,
) -> None:
    payload: Dict[str, Any] = {"phase": phase}
    if slot is not None:
        try:
            payload["slot"] = _ensure_ist_ts(slot).strftime("%H:%M")
        except Exception:
            payload["slot"] = str(slot)
    payload.update(extra)
    try:
        with _progress_lock:
            _touch_status(status, **payload)
            _touch_heartbeat(status, **payload)
    except Exception as exc:
        try:
            _logger().warning("[PROGRESS] heartbeat update failed phase=%s: %s", phase, exc)
        except Exception:
            pass
    if log_line:
        detail = " ".join(f"{k}={v}" for k, v in sorted(payload.items()))
        _logger().info("[PROGRESS] %s", detail)


def _ensure_ist_ts(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tz is None:
        ts = ts.tz_localize(IST)
    else:
        ts = ts.tz_convert(IST)
    return ts


def _fmt_ist(value: Any) -> str:
    ts = _ensure_ist_ts(value)
    offset = ts.strftime("%z")
    return f"{ts.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _slot_key(slot: pd.Timestamp) -> str:
    return _ensure_ist_ts(slot).strftime("%Y%m%d_%H%M")


def _atomic_write_csv(path: "Path", df: "pd.DataFrame") -> None:
    """Write df to path atomically using a temp file + os.replace."""
    import pathlib
    p = pathlib.Path(path)
    tmp = p.with_suffix(p.suffix + ".tmp")
    try:
        df.to_csv(tmp, index=False)
        os.replace(tmp, p)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _atomic_write_json(path: "Path", payload: Any) -> None:
    """Write JSON payload to path atomically using a temp file + os.replace."""
    import pathlib
    p = pathlib.Path(path)
    tmp = p.with_suffix(p.suffix + ".tmp")
    try:
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
        os.replace(tmp, p)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _floor_5m(ts: pd.Timestamp) -> pd.Timestamp:
    ts = _ensure_ist_ts(ts)
    minute = (ts.minute // 5) * 5
    return ts.replace(minute=minute, second=0, microsecond=0)


def _next_entry_run_after(now: datetime) -> pd.Timestamp:
    now_ts = _ensure_ist_ts(now)
    base_slot = _floor_5m(now_ts)
    run_at = base_slot + pd.Timedelta(seconds=ENTRY_DELAY_SEC)
    # The loop can wake a fraction of a second after run_at. Without a grace
    # band, it skips the just-due slot and jumps five minutes ahead.
    if now_ts <= run_at + pd.Timedelta(seconds=ENTRY_DUE_GRACE_SEC):
        return run_at
    return base_slot + pd.Timedelta(minutes=5, seconds=ENTRY_DELAY_SEC)


def _startup_expired_slot_keys(startup_now: Any) -> set[str]:
    """Slots already past the entry deadline when the live loop starts."""
    startup_now_ts = _ensure_ist_ts(startup_now)
    startup_slot = _floor_5m(
        _ensure_ist_ts(pd.Timestamp(f"{startup_now_ts.date()} 09:15:00"))
    ) + pd.Timedelta(minutes=5)
    startup_floor = _floor_5m(startup_now_ts)
    expired: set[str] = set()
    while startup_slot <= startup_floor:
        run_at = startup_slot + pd.Timedelta(seconds=ENTRY_DELAY_SEC)
        if startup_now_ts > run_at + pd.Timedelta(seconds=ENTRY_DUE_GRACE_SEC):
            expired.add(_slot_key(startup_slot))
        startup_slot += pd.Timedelta(minutes=5)
    return expired


def _candidate_slot_json_path(slot: pd.Timestamp) -> Path:
    return SIGNAL_DISCOVERY_ROOT / "json" / f"candidate_tickers_{_slot_key(slot)}.json"


def _candidate_slot_complete_marker_path(slot: pd.Timestamp) -> Path:
    return SIGNAL_DISCOVERY_ROOT / "json" / f"slot_complete_{_slot_key(slot)}.json"


def _read_slot_complete_marker(slot: pd.Timestamp) -> Tuple[bool, Dict[str, Any]]:
    path = _candidate_slot_complete_marker_path(slot)
    if not path.exists():
        return False, {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        marker_slot = _ensure_ist_ts(payload.get("slot_ist")).floor("min")
    except Exception as exc:
        return False, {
            "slot_complete_marker_path": str(path),
            "slot_complete_marker_error": f"{type(exc).__name__}: {exc}",
        }
    ready = bool(payload.get("complete")) and marker_slot == _ensure_ist_ts(slot).floor("min")
    payload = dict(payload)
    payload["slot_complete_marker_path"] = str(path)
    return ready, payload


def _candidate_rows_from_json(path: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    rows = payload.get("candidates", [])
    if not isinstance(rows, list):
        rows = []
    return pd.DataFrame(rows), payload


def _load_candidates_for_slot(slot: pd.Timestamp) -> pd.DataFrame:
    global _last_candidate_load_stats
    slot = _ensure_ist_ts(slot).floor("min")
    df = pd.DataFrame()
    loaded_exact_slot_snapshot = False
    _last_candidate_load_stats = {
        "candidate_source": "none",
        "candidate_source_path": "",
        "candidate_source_slot_ist": "",
        "candidate_source_rows": 0,
        "candidate_source_replay_isolated": bool(_IS_REPLAY_ISOLATED),
        "slot_complete_marker_required": bool(REQUIRE_SLOT_COMPLETE_MARKER),
    }
    if USE_SLOT_CANDIDATE_JSON:
        slot_json = _candidate_slot_json_path(slot)
        marker_ready, marker_payload = _read_slot_complete_marker(slot)
        if REQUIRE_SLOT_COMPLETE_MARKER and not marker_ready:
            _last_candidate_load_stats.update(
                {
                    "candidate_source": "slot_marker_pending",
                    "candidate_source_path": str(slot_json),
                    "slot_complete_marker_path": str(
                        _candidate_slot_complete_marker_path(slot)
                    ),
                }
            )
            return pd.DataFrame()
        if slot_json.exists():
            try:
                df, payload = _candidate_rows_from_json(slot_json)
                loaded_exact_slot_snapshot = True
                expected_sha = str(marker_payload.get("candidate_json_sha256", "")).strip()
                actual_sha = ""
                if expected_sha:
                    import hashlib
                    actual_sha = hashlib.sha256(slot_json.read_bytes()).hexdigest()
                    if actual_sha != expected_sha:
                        _last_candidate_load_stats = {
                            "candidate_source": "slot_json_hash_mismatch",
                            "candidate_source_path": str(slot_json),
                            "candidate_source_slot_ist": str(payload.get("slot_ist", "")),
                            "candidate_source_rows": 0,
                            "candidate_source_replay_isolated": bool(_IS_REPLAY_ISOLATED),
                            "slot_complete_marker_required": bool(REQUIRE_SLOT_COMPLETE_MARKER),
                            "candidate_snapshot_expected_sha256": expected_sha,
                            "candidate_snapshot_actual_sha256": actual_sha,
                        }
                        return pd.DataFrame()
                _last_candidate_load_stats = {
                    "candidate_source": "slot_json",
                    "candidate_source_path": str(slot_json),
                    "candidate_source_slot_ist": str(payload.get("slot_ist", "")),
                    "candidate_source_rows": int(len(df)),
                    "candidate_source_replay_isolated": bool(_IS_REPLAY_ISOLATED),
                    "slot_complete_marker_required": bool(REQUIRE_SLOT_COMPLETE_MARKER),
                    "slot_complete_marker_path": str(
                        marker_payload.get("slot_complete_marker_path", "")
                    ),
                    "decision_ready_at_ist": str(
                        marker_payload.get(
                            "decision_ready_at_ist",
                            payload.get("decision_ready_at_ist", ""),
                        )
                    ),
                    "candidate_snapshot_sha256": actual_sha or expected_sha,
                    "scanner_runtime_manifest_path": str(
                        marker_payload.get("runtime_manifest_path", "")
                    ),
                }
            except Exception as exc:
                _last_candidate_load_stats = {
                    "candidate_source": "slot_json_error",
                    "candidate_source_path": str(slot_json),
                    "candidate_source_slot_ist": "",
                    "candidate_source_rows": 0,
                    "candidate_source_error": f"{type(exc).__name__}: {exc}",
                    "candidate_source_replay_isolated": bool(_IS_REPLAY_ISOLATED),
                }
                df = pd.DataFrame()
        # Exact-slot mode is fail-closed.  Never fall back to the moving latest
        # pointer or a previous slot, even when the exact snapshot is empty.
        if not loaded_exact_slot_snapshot:
            return pd.DataFrame()
    if not loaded_exact_slot_snapshot and SIGNAL_DISCOVERY_LATEST_JSON.exists():
        try:
            df, payload = _candidate_rows_from_json(SIGNAL_DISCOVERY_LATEST_JSON)
            _last_candidate_load_stats = {
                "candidate_source": "latest_json",
                "candidate_source_path": str(SIGNAL_DISCOVERY_LATEST_JSON),
                "candidate_source_slot_ist": str(payload.get("slot_ist", "")),
                "candidate_source_rows": int(len(df)),
                "candidate_source_replay_isolated": bool(_IS_REPLAY_ISOLATED),
            }
        except Exception as exc:
            _last_candidate_load_stats = {
                "candidate_source": "latest_json_error",
                "candidate_source_path": str(SIGNAL_DISCOVERY_LATEST_JSON),
                "candidate_source_slot_ist": "",
                "candidate_source_rows": 0,
                "candidate_source_error": f"{type(exc).__name__}: {exc}",
                "candidate_source_replay_isolated": bool(_IS_REPLAY_ISOLATED),
            }
            df = pd.DataFrame()
    if not loaded_exact_slot_snapshot and df.empty and SIGNAL_DISCOVERY_LATEST_CSV.exists():
        try:
            df = pd.read_csv(SIGNAL_DISCOVERY_LATEST_CSV)
            _last_candidate_load_stats = {
                "candidate_source": "latest_csv",
                "candidate_source_path": str(SIGNAL_DISCOVERY_LATEST_CSV),
                "candidate_source_slot_ist": "",
                "candidate_source_rows": int(len(df)),
                "candidate_source_replay_isolated": bool(_IS_REPLAY_ISOLATED),
            }
        except Exception:
            df = pd.DataFrame()
    if df.empty:
        return df
    if "signal_time_ist" not in df.columns:
        return pd.DataFrame()
    sig = pd.to_datetime(df["signal_time_ist"], errors="coerce")
    if getattr(sig.dt, "tz", None) is None:
        sig = sig.dt.tz_localize(IST)
    else:
        sig = sig.dt.tz_convert(IST)
    df = df.loc[sig.dt.floor("min").eq(slot)].copy()
    if df.empty:
        return df
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["setup"] = df["setup"].astype(str)
    df["signal_time_ist"] = sig.loc[df.index].map(_fmt_ist)
    df["quality_score"] = pd.to_numeric(df.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    if _last_candidate_load_stats.get("decision_ready_at_ist"):
        df["decision_ready_at_ist"] = str(
            _last_candidate_load_stats["decision_ready_at_ist"]
        )
    df["candidate_snapshot_sha256"] = str(
        _last_candidate_load_stats.get("candidate_snapshot_sha256", "")
    )
    df["runtime_manifest_path"] = str(
        _last_candidate_load_stats.get("scanner_runtime_manifest_path", "")
    )
    return (
        df.sort_values(["quality_score", "ticker", "setup"], ascending=[False, True, True])
        .drop_duplicates(subset=["candidate_id"], keep="first")
        .drop_duplicates(subset=["signal_time_ist", "ticker"], keep="first")
        .reset_index(drop=True)
    )


def _latest_candidate_snapshot_slot() -> Optional[pd.Timestamp]:
    latest_marker = SIGNAL_DISCOVERY_ROOT / "latest" / "latest_slot_complete.json"
    if REQUIRE_SLOT_COMPLETE_MARKER and latest_marker.exists():
        try:
            payload = json.loads(latest_marker.read_text(encoding="utf-8", errors="replace"))
            if payload.get("complete") and payload.get("slot_ist"):
                return _ensure_ist_ts(payload["slot_ist"]).floor("min")
        except Exception:
            return None
    source_path = Path(_last_candidate_load_stats.get("candidate_source_path", ""))
    if USE_SLOT_CANDIDATE_JSON and source_path.exists() and source_path.name.startswith("candidate_tickers_"):
        try:
            payload = json.loads(source_path.read_text(encoding="utf-8", errors="replace"))
            raw = payload.get("slot_ist")
            if raw:
                return _ensure_ist_ts(raw).floor("min")
        except Exception:
            pass
    if not SIGNAL_DISCOVERY_LATEST_JSON.exists():
        return None
    try:
        payload = json.loads(SIGNAL_DISCOVERY_LATEST_JSON.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return None
    raw = payload.get("slot_ist")
    if not raw:
        return None
    try:
        return _ensure_ist_ts(raw).floor("min")
    except Exception:
        return None


def _load_candidates_for_slot_with_wait(slot: pd.Timestamp) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    slot = _ensure_ist_ts(slot).floor("min")
    configured_wait_budget = max(0.0, float(CANDIDATE_WAIT_SEC))
    intended_entry = slot + pd.Timedelta(minutes=ENTRY_SIGNAL_TO_ENTRY_LAG_MIN)
    handoff_deadline = (
        intended_entry + pd.Timedelta(seconds=MAX_SIGNAL_HANDOFF_LAG_SEC)
        if MAX_SIGNAL_HANDOFF_LAG_SEC > 0
        else None
    )
    if handoff_deadline is None:
        wait_budget = configured_wait_budget
    else:
        remaining_for_wait = (
            handoff_deadline - _ensure_ist_ts(v7_persistent.base_v15.now_ist())
        ).total_seconds() - max(0.0, ENTRY_PROCESS_RESERVE_SEC)
        wait_budget = min(configured_wait_budget, max(0.0, remaining_for_wait))
    poll = max(0.25, float(CANDIDATE_WAIT_POLL_SEC))
    started = time.perf_counter()
    waited = False
    snapshot_slot = _latest_candidate_snapshot_slot()
    while True:
        df = _load_candidates_for_slot(slot)
        marker_ready, marker_payload = _read_slot_complete_marker(slot)
        snapshot_slot = (
            slot if marker_ready else _latest_candidate_snapshot_slot()
        )
        snapshot_ready = (
            marker_ready
            if REQUIRE_SLOT_COMPLETE_MARKER
            else snapshot_slot is not None and snapshot_slot >= slot
        )
        if not df.empty or snapshot_ready or time.perf_counter() - started >= wait_budget:
            wait_stats = {
                "candidate_wait_enabled": bool(wait_budget > 0),
                "candidate_wait_sec": round(time.perf_counter() - started, 3),
                "candidate_wait_budget_sec": float(wait_budget),
                "candidate_wait_configured_budget_sec": float(configured_wait_budget),
                "candidate_wait_poll_sec": float(poll),
                "candidate_wait_used": bool(waited),
                "candidate_snapshot_slot_ist": _fmt_ist(snapshot_slot) if snapshot_slot is not None else "",
                "candidate_snapshot_ready": bool(snapshot_ready),
                "signal_handoff_deadline_ist": (
                    _fmt_ist(handoff_deadline) if handoff_deadline is not None else ""
                ),
                "max_signal_handoff_lag_sec": float(MAX_SIGNAL_HANDOFF_LAG_SEC),
                "entry_process_reserve_sec": float(ENTRY_PROCESS_RESERVE_SEC),
                "slot_complete_marker_required": bool(REQUIRE_SLOT_COMPLETE_MARKER),
                "slot_complete_marker_ready": bool(marker_ready),
                "slot_complete_marker_path": str(
                    marker_payload.get(
                        "slot_complete_marker_path",
                        _candidate_slot_complete_marker_path(slot),
                    )
                ),
                "decision_ready_at_ist": str(
                    marker_payload.get("decision_ready_at_ist", "")
                ),
                "candidate_snapshot_sha256": str(
                    marker_payload.get("candidate_json_sha256", "")
                ),
            }
            return df, wait_stats
        waited = True
        time.sleep(poll)


def _setup_kite_app_pool(
    app_count: int = RAW_FETCH_APP_COUNT,
    *,
    progress_slot: Optional[pd.Timestamp] = None,
) -> List[Tuple[str, Any]]:
    requested = max(1, min(8, int(app_count)))
    with _raw_session_cache_lock:
        if _raw_kite_session_cache:
            return list(_raw_kite_session_cache[:requested])

    setup_map: Dict[str, Callable[[], Any]] = {}
    try:
        setup_map = dict(scheduler._setup_fn_map())
    except Exception:
        setup_map = {"app1": scheduler.setup_kite_session_from_eqidv2_dir}

    sessions: List[Tuple[str, Any]] = []
    for idx in range(1, requested + 1):
        app_name = f"app{idx}"
        setup_fn = setup_map.get(app_name)
        if setup_fn is None:
            continue
        try:
            _touch_progress("RAW_SESSION_SETUP", slot=progress_slot, app=app_name)
            sessions.append((app_name, setup_fn()))
        except Exception as exc:
            print(
                f"[{SESSION_NAME}] raw fetch app disabled {app_name}: {type(exc).__name__}: {exc}",
                flush=True,
            )
    if not sessions:
        sessions.append(("app1", scheduler.setup_kite_session_from_eqidv2_dir()))
    with _raw_session_cache_lock:
        if not _raw_kite_session_cache:
            _raw_kite_session_cache.extend(sessions)
        return list(_raw_kite_session_cache[:requested])


def _setup_kite_and_tokens(
    tickers: List[str],
    *,
    progress_slot: Optional[pd.Timestamp] = None,
) -> Tuple[List[Tuple[str, Any]], Dict[str, int]]:
    log = _logger()
    _touch_progress("RAW_SESSION_SETUP", slot=progress_slot, tickers=len(tickers), log_line=True)
    sessions = _setup_kite_app_pool(progress_slot=progress_slot)
    _touch_progress(
        "RAW_TOKEN_LOOKUP",
        slot=progress_slot,
        tickers=len(tickers),
        active_apps=len(sessions),
        log_line=True,
    )
    requested_tickers = sorted({str(ticker).upper() for ticker in tickers})
    missing_tickers = [
        ticker for ticker in requested_tickers if ticker not in _raw_token_cache
    ]
    if missing_tickers:
        loaded = scheduler.core.load_or_fetch_tokens(
            sessions[0][1],
            missing_tickers,
            log,
            refresh=False,
        )
        _raw_token_cache.update(
            {
                str(key).upper(): int(value)
                for key, value in dict(loaded or {}).items()
            }
        )
    tokens = {
        ticker: _raw_token_cache[ticker]
        for ticker in requested_tickers
        if ticker in _raw_token_cache
    }
    _touch_progress(
        "RAW_TOKEN_LOOKUP_DONE",
        slot=progress_slot,
        tickers=len(tickers),
        token_count=len(tokens),
        active_apps=len(sessions),
        log_line=True,
    )
    return sessions, tokens


def _normalise_raw_1m(raw: List[Dict[str, Any]], ticker: str) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw)
    if df.empty:
        return df
    if "date" not in df.columns:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize(IST)
    else:
        df["date"] = df["date"].dt.tz_convert(IST)
    keep = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[keep].dropna(subset=["date"]).copy()
    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["ticker"] = str(ticker).upper()
    return df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)


def _upsert_ticker_raw(ticker: str, df_new: pd.DataFrame) -> Path:
    out_path = RAW_1MIN_DIR / f"{str(ticker).upper()}_stocks_raw_1min.parquet"
    if df_new.empty:
        return out_path
    if out_path.exists():
        try:
            old = pd.read_parquet(out_path)
        except Exception:
            old = pd.DataFrame()
        merged = pd.concat([old, df_new], ignore_index=True, sort=False)
    else:
        merged = df_new.copy()
    merged["date"] = pd.to_datetime(merged["date"], errors="coerce")
    if getattr(merged["date"].dt, "tz", None) is None:
        merged["date"] = merged["date"].dt.tz_localize(IST)
    else:
        merged["date"] = merged["date"].dt.tz_convert(IST)
    merged = merged.dropna(subset=["date"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")
    merged.to_parquet(out_path, index=False)
    return out_path


def _slot_raw_path(slot: pd.Timestamp, ticker: str) -> Path:
    slot_dir = SLOT_RAW_DIR / _slot_key(slot)
    slot_dir.mkdir(parents=True, exist_ok=True)
    return slot_dir / f"{str(ticker).upper()}_raw_1min.parquet"


def _partition_tickers(tickers: List[str], worker_count: int) -> List[List[str]]:
    partitions = [[] for _ in range(max(1, int(worker_count)))]
    for idx, ticker in enumerate(tickers):
        partitions[idx % len(partitions)].append(ticker)
    return partitions


def _fetch_raw_partition(
    app_name: str,
    kite: Any,
    tickers: List[str],
    tokens: Dict[str, int],
    start: pd.Timestamp,
    end: pd.Timestamp,
    slot: pd.Timestamp,
) -> Tuple[str, Dict[str, pd.DataFrame], List[Dict[str, str]], float]:
    t0 = time.perf_counter()
    fetched: Dict[str, pd.DataFrame] = {}
    errors: List[Dict[str, str]] = []
    total = int(len(tickers))
    for idx, ticker in enumerate(tickers, start=1):
        symbol = str(ticker).upper()
        token = tokens.get(symbol)
        if not token:
            errors.append({"app": app_name, "ticker": symbol, "error": "missing_token"})
            continue
        try:
            _touch_progress(
                "RAW_FETCH_TICKER",
                slot=slot,
                app=app_name,
                ticker=symbol,
                ticker_idx=idx,
                ticker_total=total,
            )
            raw = kite.historical_data(int(token), start.to_pydatetime(), end.to_pydatetime(), KITE_INTERVAL)
        except Exception as exc:
            msg = f"{type(exc).__name__}: {exc}"
            print(f"[{SESSION_NAME}] fetch failed {symbol} via {app_name}: {msg}", flush=True)
            errors.append({"app": app_name, "ticker": symbol, "error": msg})
            continue
        df = _normalise_raw_1m(raw, symbol)
        if df.empty:
            errors.append({"app": app_name, "ticker": symbol, "error": "empty_raw"})
            continue
        df["timestamp_convention"] = "candle_start_1m"
        df["raw_source"] = f"kite_historical_data:{app_name}"
        df["raw_snapshot_slot_ist"] = _fmt_ist(slot)
        df["raw_snapshot_fetched_at_ist"] = _fmt_ist(pd.Timestamp.now(tz=IST))
        df.to_parquet(_slot_raw_path(slot, symbol), index=False)
        _touch_progress("RAW_UPSERT_TICKER", slot=slot, app=app_name, ticker=symbol)
        _upsert_ticker_raw(symbol, df)
        fetched[symbol] = df
        _touch_progress(
            "RAW_FETCH_TICKER_DONE",
            slot=slot,
            app=app_name,
            ticker=symbol,
            ticker_idx=idx,
            ticker_total=total,
        )
    return app_name, fetched, errors, time.perf_counter() - t0


def _raw_fetch_start_for_slot(slot: pd.Timestamp) -> pd.Timestamp:
    """Session-open lookback makes raw ADX/RSI reproducible without parquet."""

    try:
        hour_text, minute_text = RAW_FETCH_SESSION_START.split(":", 1)
        hour, minute = int(hour_text), int(minute_text)
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
    except Exception:
        hour, minute = 9, 15
    slot_ist = _ensure_ist_ts(slot)
    start = slot_ist.normalize() + pd.Timedelta(hours=hour, minutes=minute)
    return start if slot_ist >= start else slot_ist - pd.Timedelta(minutes=30)


def _fetch_raw_for_candidates(candidates: pd.DataFrame, slot: pd.Timestamp) -> Dict[str, pd.DataFrame]:
    global _last_raw_fetch_stats
    tickers = sorted(candidates["ticker"].dropna().astype(str).str.upper().unique())
    if not tickers:
        _last_raw_fetch_stats = {
            "raw_fetch_mode": "none",
            "raw_fetch_elapsed_sec": 0.0,
            "raw_fetch_active_apps": 0,
            "raw_fetch_worker_apps": 0,
            "raw_fetch_failures": 0,
            "raw_fetch_app_partitions": {},
        }
        return {}
    fetch_started = time.perf_counter()
    _touch_progress("RAW_FETCH_START", slot=slot, tickers=len(tickers), log_line=True)
    if REPLAY_USE_LOCAL_1MIN:
        if not _IS_REPLAY_ISOLATED:
            raise RuntimeError(
                "EQIDV2_ENTRY_ENGINE_REPLAY_USE_LOCAL_1MIN is allowed only "
                "with an isolated EQIDV2_REPLAY_OUTPUT_ROOT"
            )
        start = _raw_fetch_start_for_slot(slot)
        end = _ensure_ist_ts(slot) + pd.Timedelta(
            minutes=max(1, ENTRY_SEARCH_MAX_DELAY_MIN), seconds=30
        )
        fetched: Dict[str, pd.DataFrame] = {}
        failures = 0
        for ticker in tickers:
            bars = _load_indicator_bars(ticker, "1m")
            if bars is None or bars.empty:
                failures += 1
                continue
            window = bars[(bars["date"] >= start) & (bars["date"] <= end)].copy()
            if window.empty:
                failures += 1
                continue
            window["ticker"] = ticker
            window["timestamp_convention"] = "candle_start_1m"
            window["raw_source"] = "isolated_replay_local_1min"
            window["raw_snapshot_slot_ist"] = _fmt_ist(slot)
            window["raw_snapshot_fetched_at_ist"] = _fmt_ist(pd.Timestamp.now(tz=IST))
            fetched[ticker] = window
            window.to_parquet(_slot_raw_path(slot, ticker), index=False)
        _last_raw_fetch_stats = {
            "raw_fetch_mode": "isolated_replay_local_1min",
            "raw_fetch_elapsed_sec": round(time.perf_counter() - fetch_started, 3),
            "raw_fetch_active_apps": 0,
            "raw_fetch_worker_apps": 0,
            "raw_fetch_failures": int(failures),
            "raw_fetch_missing_tokens": 0,
            "raw_fetch_app_partitions": {"local_1min": int(len(tickers))},
            "raw_fetch_app_elapsed_sec": {},
        }
        _touch_progress(
            "RAW_FETCH_DONE",
            slot=slot,
            tickers=len(tickers),
            fetched=len(fetched),
            failures=failures,
            elapsed_sec=round(time.perf_counter() - fetch_started, 3),
            log_line=True,
        )
        return fetched
    sessions, tokens = _setup_kite_and_tokens(tickers, progress_slot=slot)
    worker_sessions = sessions if RAW_FETCH_PARALLEL_APPS_ENABLED else sessions[:1]
    if not worker_sessions:
        worker_sessions = sessions[:1]
    # Fetch from session open so ADX/RSI and every momentum window are
    # reproducible from the immutable slot snapshot alone.
    start = _raw_fetch_start_for_slot(slot)
    end = _ensure_ist_ts(slot) + pd.Timedelta(minutes=max(1, ENTRY_SEARCH_MAX_DELAY_MIN), seconds=30)
    fetched: Dict[str, pd.DataFrame] = {}
    errors: List[Dict[str, str]] = []
    partition_map: Dict[str, int] = {}
    per_app_elapsed: Dict[str, float] = {}
    partitions = _partition_tickers(tickers, len(worker_sessions))
    submitted = []
    with ThreadPoolExecutor(max_workers=max(1, len(worker_sessions)), thread_name_prefix="entry-raw-fetch") as executor:
        for (app_name, kite), part in zip(worker_sessions, partitions):
            partition_map[app_name] = int(len(part))
            if not part:
                per_app_elapsed[app_name] = 0.0
                continue
            submitted.append(
                executor.submit(_fetch_raw_partition, app_name, kite, part, tokens, start, end, slot)
            )
        for fut in as_completed(submitted):
            try:
                app_name, app_fetched, app_errors, elapsed = fut.result()
            except Exception as exc:
                errors.append({"app": "unknown", "ticker": "", "error": f"{type(exc).__name__}: {exc}"})
                continue
            fetched.update(app_fetched)
            errors.extend(app_errors)
            per_app_elapsed[app_name] = round(float(elapsed), 3)
            _touch_progress(
                "RAW_FETCH_APP_DONE",
                slot=slot,
                app=app_name,
                fetched=len(app_fetched),
                errors=len(app_errors),
                elapsed_sec=round(float(elapsed), 3),
                log_line=True,
            )

    _last_raw_fetch_stats = {
        "raw_fetch_mode": "parallel_app_pool" if RAW_FETCH_PARALLEL_APPS_ENABLED else "single_app",
        "raw_fetch_elapsed_sec": round(time.perf_counter() - fetch_started, 3),
        "raw_fetch_active_apps": int(len(sessions)),
        "raw_fetch_worker_apps": int(len(worker_sessions)),
        "raw_fetch_app_names": ",".join(app_name for app_name, _ in sessions),
        "raw_fetch_worker_app_names": ",".join(app_name for app_name, _ in worker_sessions),
        "raw_fetch_app_partitions": partition_map,
        "raw_fetch_app_elapsed_sec": per_app_elapsed,
        "raw_fetch_failures": int(len(errors)),
        "raw_fetch_missing_tokens": int(sum(1 for e in errors if e.get("error") == "missing_token")),
    }
    _touch_progress(
        "RAW_FETCH_DONE",
        slot=slot,
        tickers=len(tickers),
        fetched=len(fetched),
        failures=len(errors),
        elapsed_sec=round(time.perf_counter() - fetch_started, 3),
        log_line=True,
    )
    return fetched


def _entry_bar_for_candidate(
    raw_by_ticker: Dict[str, pd.DataFrame],
    cand: pd.Series,
    intended_entry_ist: Optional[pd.Timestamp] = None,
) -> Optional[pd.Series]:
    """Return the first 1-minute reference bar at or after intended_entry_ist.

    intended_entry_ist is the executable T+1 wall-clock instant.
    If not supplied it is computed from signal_time_ist + ENTRY_SIGNAL_TO_ENTRY_LAG_MIN.
    The paper executor uses live LTP for the actual fill.
    """
    ticker = str(cand.get("ticker", "")).upper()
    df = raw_by_ticker.get(ticker)
    if df is None or df.empty:
        return None

    if intended_entry_ist is None or pd.isna(intended_entry_ist):
        sig = _ensure_ist_ts(cand.get("signal_time_ist"))
        intended_entry_ist = sig + pd.Timedelta(minutes=ENTRY_SIGNAL_TO_ENTRY_LAG_MIN)

    dates = pd.to_datetime(df["date"], errors="coerce")
    if getattr(dates.dt, "tz", None) is None:
        dates = dates.dt.tz_localize(IST)
    else:
        dates = dates.dt.tz_convert(IST)
    work = df.copy()
    work["date"] = dates
    sub = work[
        (work["date"] >= intended_entry_ist)
        & (work["date"] <= intended_entry_ist + pd.Timedelta(minutes=ENTRY_SEARCH_MAX_DELAY_MIN))
    ].sort_values("date")
    if sub.empty:
        return None
    return sub.iloc[0]


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
        return out if np.isfinite(out) else float(default)
    except Exception:
        return float(default)


def _pre_momentum_side_dir(side: str) -> float:
    return 1.0 if str(side).strip().upper() == "LONG" else -1.0


def _indicator_path(ticker: str, timeframe: str) -> Optional[Path]:
    symbol = str(ticker or "").strip().upper()
    if not symbol:
        return None
    if timeframe == "1m":
        path = DATA_1MIN_DIR / f"{symbol}_stocks_indicators_1min.parquet"
        return path if path.exists() else None
    path = DATA_5M_DIR / f"{symbol}_stocks_indicators_5min.parquet"
    fallback = runtime_dir("stocks_indicators_5min_eq_live") / f"{symbol}_stocks_indicators_5min.parquet"
    if os.getenv("EQIDV2_DATA_5M_DIR"):
        if path.exists():
            return path
        return fallback if fallback.exists() else None

    candidates = [candidate for candidate in (path, fallback) if candidate.exists()]
    if not candidates:
        return None
    return max(candidates, key=lambda candidate: candidate.stat().st_mtime)


def _load_indicator_bars(ticker: str, timeframe: str) -> Optional[pd.DataFrame]:
    path = _indicator_path(ticker, timeframe)
    if path is None:
        return None
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    cache = _indicator_1m_cache if timeframe == "1m" else _indicator_5m_cache
    key = str(path)
    cached = cache.get(key)
    if cached is not None and cached[0] == mtime:
        return cached[1]
    columns = [
        "date", "open", "high", "low", "close", "volume",
        "RSI", "ATR", "EMA_20", "EMA_50", "VWAP", "MACD_Hist", "ADX",
    ]
    try:
        try:
            df = pd.read_parquet(path, columns=columns)
        except Exception:
            df = pd.read_parquet(path)
        if "date" not in df.columns:
            return None
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if getattr(df["date"].dt, "tz", None) is None:
            df["date"] = df["date"].dt.tz_localize(IST)
        else:
            df["date"] = df["date"].dt.tz_convert(IST)
        df = df[df["date"].notna()].sort_values("date").reset_index(drop=True)
    except Exception as exc:
        _logger().warning(f"[PRE.MOMENTUM] indicator read failed {ticker} {timeframe}: {exc}")
        return None
    cache[key] = (mtime, df)
    return df


def _close_back(bars: pd.DataFrame, n: int) -> float:
    if len(bars) <= n:
        return float("nan")
    return _safe_float(bars.iloc[-(n + 1)].get("close"), float("nan"))


def _add_window_features(out: Dict[str, float], bars: pd.DataFrame, side: str, risk: float, n: int) -> None:
    if len(bars) < n or risk <= 0:
        return
    d = _pre_momentum_side_dir(side)
    window = bars.tail(n)
    last = bars.iloc[-1]
    high = _safe_float(pd.to_numeric(window["high"], errors="coerce").max(), float("nan"))
    low = _safe_float(pd.to_numeric(window["low"], errors="coerce").min(), float("nan"))
    close = _safe_float(last.get("close"), float("nan"))
    rng = max(high - low, 1e-9)
    out[f"pre{n}_close_pos"] = float((close - low) / rng if side == "LONG" else (high - close) / rng)
    open_s = pd.to_numeric(window["open"], errors="coerce")
    close_s = pd.to_numeric(window["close"], errors="coerce")
    out[f"pre{n}_dir_count"] = float((d * (close_s - open_s) > 0).sum())
    out[f"pre{n}_body_sum_r"] = float(d * (close_s - open_s).sum() / risk)
    out[f"pre{n}_range_r"] = float(rng / risk)
    prior = bars.iloc[:-1].tail(20)
    vol_base = pd.to_numeric(prior["volume"], errors="coerce").mean() if len(prior) else float("nan")
    out[f"pre{n}_vol_ratio20"] = (
        float(pd.to_numeric(window["volume"], errors="coerce").mean() / vol_base)
        if vol_base and np.isfinite(vol_base)
        else float("nan")
    )


def _last_finite(series: pd.Series) -> float:
    vals = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    return float(vals.iloc[-1]) if len(vals) else float("nan")


def _calc_rsi_last(bars: pd.DataFrame, period: int = 14) -> float:
    if "close" not in bars.columns or len(bars) < period + 1:
        return float("nan")
    close = pd.to_numeric(bars["close"], errors="coerce")
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    rsi = rsi.mask((avg_loss == 0.0) & (avg_gain > 0.0), 100.0)
    rsi = rsi.mask((avg_loss == 0.0) & (avg_gain <= 0.0), 50.0)
    return _last_finite(rsi)


def _calc_adx_last(bars: pd.DataFrame, period: int = 14) -> float:
    needed = {"high", "low", "close"}
    if not needed.issubset(set(bars.columns)) or len(bars) < period + 2:
        return float("nan")
    high = pd.to_numeric(bars["high"], errors="coerce")
    low = pd.to_numeric(bars["low"], errors="coerce")
    close = pd.to_numeric(bars["close"], errors="coerce")

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=bars.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=bars.index)
    tr = pd.concat(
        [
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean().replace(0.0, np.nan)
    plus_di = 100.0 * plus_dm.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean() / atr
    minus_di = 100.0 * minus_dm.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean() / atr
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    adx = dx.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    return _last_finite(adx)


def _pre_entry_momentum_features(entry_row: Dict[str, Any], raw_by_ticker: Dict[str, pd.DataFrame]) -> Tuple[Dict[str, float], str]:
    ticker = str(entry_row.get("ticker", "")).upper().strip()
    entry_price = _safe_float(entry_row.get("entry_price"), float("nan"))
    stop_price = _safe_float(entry_row.get("sl_price"), float("nan"))
    raw = raw_by_ticker.get(ticker)
    if raw is None or raw.empty:
        return {}, "missing immutable raw 1m snapshot"
    cutoff = entry_row.get("pre_momentum_cutoff_ist") or entry_row.get("entry_time_ist")
    return shared_pre_momentum.calculate_features(
        raw_1m=raw,
        candidate=entry_row,
        entry_price=entry_price,
        stop_price=stop_price,
        cutoff_ist=cutoff,
    )


def _eval_pre_momentum_terms(features: Dict[str, float], terms: Tuple[Tuple[str, str, float], ...]) -> Tuple[bool, str]:
    return shared_pre_momentum.evaluate_terms(features, terms)


def _apply_pre_entry_momentum_gate(
    entry_df: pd.DataFrame,
    raw_by_ticker: Dict[str, pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    if entry_df is None or entry_df.empty:
        return entry_df, pd.DataFrame(), {
            "pre_momentum_gate_enabled": bool(PRE_ENTRY_MOMENTUM_GATES_ENABLED),
            "pre_momentum_rejected_rows": 0,
        }
    if not PRE_ENTRY_MOMENTUM_GATES_ENABLED:
        return entry_df, pd.DataFrame(), {
            "pre_momentum_gate_enabled": False,
            "pre_momentum_rejected_rows": 0,
        }

    kept_rows: List[Dict[str, Any]] = []
    rejected_rows: List[Dict[str, Any]] = []
    for _, row in entry_df.iterrows():
        data = row.to_dict()
        setup = str(data.get("setup", "")).upper().strip()
        if setup in PRE_ENTRY_MOMENTUM_SHADOW_SETUPS:
            data["reject_reason"] = "pre_entry_momentum_shadow_setup"
            data["pre_momentum_gate_version"] = PRE_ENTRY_MOMENTUM_GATE_VERSION
            rejected_rows.append(data)
            continue
        terms = PRE_ENTRY_MOMENTUM_SETUP_GATES.get(setup)
        if not terms:
            kept_rows.append(data)
            continue
        features, missing_reason = _pre_entry_momentum_features(data, raw_by_ticker)
        data.update(features)
        data["pre_momentum_feature_version"] = shared_pre_momentum.FEATURE_VERSION
        data["pre_momentum_1m_source"] = "immutable_slot_raw_1min"
        data["pre_momentum_5m_source"] = "frozen_candidate_signal_fields"
        data["pre_momentum_gate_version"] = PRE_ENTRY_MOMENTUM_GATE_VERSION
        if missing_reason:
            data["pre_momentum_missing_reason"] = missing_reason
            if PRE_ENTRY_MOMENTUM_MISSING_ACTION in {"block", "skip", "reject"}:
                data["reject_reason"] = "pre_entry_momentum_missing_features"
                rejected_rows.append(data)
                continue
            kept_rows.append(data)
            continue
        passed, reason = _eval_pre_momentum_terms(features, terms)
        data["pre_momentum_gate_rule"] = " AND ".join(f"{c} {op} {t:.6g}" for c, op, t in terms)
        data["pre_momentum_gate_pass"] = bool(passed)
        data["pre_momentum_gate_reason"] = reason
        if passed:
            kept_rows.append(data)
        else:
            data["reject_reason"] = "pre_entry_momentum_gate"
            rejected_rows.append(data)

    kept = pd.DataFrame(kept_rows)
    rejected = pd.DataFrame(rejected_rows)
    stats = {
        "pre_momentum_gate_enabled": True,
        "pre_momentum_version": PRE_ENTRY_MOMENTUM_GATE_VERSION,
        "pre_momentum_rejected_rows": int(len(rejected)),
        "pre_momentum_input_rows": int(len(entry_df)),
        "pre_momentum_output_rows": int(len(kept)),
    }
    return kept, rejected, stats


def _build_entry_rows(
    candidates: pd.DataFrame,
    raw_by_ticker: Dict[str, pd.DataFrame],
    *,
    slot: Optional[pd.Timestamp] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # A_MOD_BREAK_C1_HIGH slot pre-filter: keep top-N per slot by vwap_dist_atr
    # desc so the strongest-momentum breakouts are selected when multiple fire
    # simultaneously. Operates on the full batch before the per-row loop below.
    if candidates is not None and not candidates.empty and "setup" in candidates.columns:
        _amod_mask = candidates["setup"].astype(str).str.upper().eq("A_MOD_BREAK_C1_HIGH")
        if _amod_mask.any() and A_MOD_C1_HIGH_TOP_N_PER_SLOT > 0:
            _amod = candidates[_amod_mask].copy()
            _other = candidates[~_amod_mask].copy()
            _amod["_slot"] = _amod["signal_time_ist"].apply(
                lambda x: _ensure_ist_ts(x).floor("5min") if pd.notna(x) else pd.NaT
            )
            _amod["_vdatr"] = pd.to_numeric(_amod.get("vwap_dist_atr", 0), errors="coerce").fillna(0.0)
            _amod = (
                _amod.sort_values("_vdatr", ascending=False)
                .groupby("_slot", sort=False, dropna=True)
                .head(A_MOD_C1_HIGH_TOP_N_PER_SLOT)
                .drop(columns=["_slot", "_vdatr"])
            )
            candidates = pd.concat([_other, _amod], ignore_index=True)

    rows: List[Dict[str, Any]] = []
    adv_cap_rejected_rows: List[Dict[str, Any]] = []  # P2-15: funnel audit
    fno_ban_rejected_rows: List[Dict[str, Any]] = []  # P2-15: funnel audit
    for _, cand in candidates.iterrows():
        setup = str(cand.get("setup", ""))
        side = str(cand.get("side", "")).upper()
        ticker = str(cand.get("ticker", "")).upper().strip()
        cand_id = str(cand.get("candidate_id", ""))
        # P1.1: block E_VWAP_LOSE_EARLY_SHORT before the 09:45 bar (first two
        # slots lack a reliable intraday VWAP, producing immediate adverse moves).
        if setup.upper() == "E_VWAP_LOSE_EARLY_SHORT":
            _sig_ts = _ensure_ist_ts(cand.get("signal_time_ist"))
            if not pd.isna(_sig_ts) and _sig_ts.time() < E_VWAP_EARLY_SHORT_MIN_SLOT:
                continue
        # A_MOD_BREAK_C1_HIGH: enforce the 11:10 IST signal-time ceiling here too
        # (mirrors the V11 overlay signal_minute <= 670 condition).
        if setup.upper() == "A_MOD_BREAK_C1_HIGH":
            _sig_ts = _ensure_ist_ts(cand.get("signal_time_ist"))
            if not pd.isna(_sig_ts) and _sig_ts.time() > A_MOD_C1_HIGH_MAX_SLOT:
                continue
        # C_OR_BREAKOUT: VWAP separation, volatility cap, and morning-only time window.
        # vwap_dist>=2.0 → PF 2.46; atr_pct>=0.010 → PF 0.93.
        # Time window 09:55-10:40: bars 1-6 WR=33.2%, bars 7-18 WR=71.2%, bars 19+ PF=0.68.
        if setup.upper() == "C_OR_BREAKOUT":
            _cor_vwap = _safe_float(cand.get("vwap_dist_atr"), float("nan"))
            _cor_atr = _safe_float(cand.get("atr_pct"), float("nan"))
            if not (np.isfinite(_cor_vwap) and _cor_vwap >= C_OR_BREAKOUT_MIN_VWAP_DIST_ATR):
                continue
            if np.isfinite(_cor_atr) and _cor_atr >= C_OR_BREAKOUT_MAX_ATR_PCT:
                continue
            _cor_sig_ts = _ensure_ist_ts(cand.get("signal_time_ist"))
            if not pd.isna(_cor_sig_ts):
                _cor_t = _cor_sig_ts.time()
                if _cor_t < C_OR_BREAKOUT_MIN_SIGNAL_TIME or _cor_t > C_OR_BREAKOUT_MAX_SIGNAL_TIME:
                    continue
        exit_override = (
            v11_live_overlay.selected_exit_override(setup, V11_BACKTESTING_OVERLAY_PROFILE)
            if V11_BACKTESTING_OVERLAY_ENABLE and not _CONF_ENGINE_ACTIVE
            else None
        )
        if exit_override is not None:
            rule_source = "v11_backtesting_exit_override"
            rule = exit_override
        else:
            rule_source = "v6"
            rule = v6.SETUP_EXIT_RULES.get(setup)
        if rule is None:
            continue
        _sig_ts_for_lag = _ensure_ist_ts(cand.get("signal_time_ist"))
        _intended_entry_ist = (
            (_sig_ts_for_lag + pd.Timedelta(minutes=ENTRY_SIGNAL_TO_ENTRY_LAG_MIN)).floor("min")
            if not pd.isna(_sig_ts_for_lag) else pd.NaT
        )
        entry_policy = _conf_boot.entry_policy_for_setup(setup) if _CONF_ENGINE_ACTIVE else {}
        exit_policy = _conf_boot.exit_policy_for_setup(setup) if _CONF_ENGINE_ACTIVE else {}
        entry_bar = _entry_bar_for_candidate(raw_by_ticker, cand, _intended_entry_ist)
        if entry_policy.get("model") == "high_break_trigger":
            entry_price = _safe_float(cand.get("entry_trigger_price"), np.nan)
            _entry_bar_source = "configured_high_break_trigger"
        elif entry_bar is not None:
            entry_price = float(entry_bar.get("open", np.nan))
            _entry_bar_source = "forming_t1_open_reference"
        else:
            # T+1 bar unavailable at fetch time (forming or not yet returned by Kite).
            # Use signal-bar close as SL/TGT reference only; actual fill is live LTP.
            # Executor rebases stop/target distances around the real fill price.
            entry_price = _safe_float(cand.get("signal_close"), np.nan)
            _entry_bar_source = "signal_bar_close_reference"
        if not np.isfinite(entry_price) or entry_price <= 0:
            continue
        sl_pct, tgt_pct = rule
        if side == "LONG":
            sl_price = entry_price * (1.0 - sl_pct / 100.0)
            target_price = entry_price * (1.0 + tgt_pct / 100.0)
        elif side == "SHORT":
            sl_price = entry_price * (1.0 + sl_pct / 100.0)
            target_price = entry_price * (1.0 - tgt_pct / 100.0)
        else:
            continue
        # P2-12: F&O ban filter — block new SHORT entries on banned securities
        if side == "SHORT" and FNO_BAN_FILTER_ENABLED:
            if slot is not None:
                _touch_progress("FNO_BAN_CHECK", slot=slot, ticker=ticker)
            banned = _get_fno_banned_tickers()
            if ticker.upper() in banned:
                _logger().info(
                    f"[FNO.BAN] skip {ticker} SHORT — in F&O ban period today | cand={cand_id[:12]}"
                )
                fno_ban_rejected_rows.append({**cand.to_dict(), "reject_reason": "fno_ban_short"})
                continue
        quantity = _risk_based_qty(entry_price, sl_price)
        # P1-7: halve short size on bullish NIFTY regime
        if side == "SHORT":
            nifty_mult = _nifty_regime_short_mult()
            if nifty_mult < 1.0:
                quantity = max(1, int(quantity * nifty_mult))
        # P2-11: ADV liquidity cap — skip if notional > participation% of ADV
        if ADV_CAP_ENABLED:
            if slot is not None:
                _touch_progress("ADV_CAP_CHECK", slot=slot, ticker=ticker)
            adv_rs = _get_adv_rs(ticker)
            if adv_rs > 0:
                adv_cap_rs = adv_rs * ADV_PARTICIPATION_PCT / 100.0
                proposed_notional = float(entry_price * quantity)
                if proposed_notional > adv_cap_rs:
                    _logger().info(
                        f"[ADV.CAP] skip {ticker} {side} notional=Rs{proposed_notional:,.0f} "
                        f"> {ADV_PARTICIPATION_PCT}%_ADV=Rs{adv_cap_rs:,.0f} "
                        f"(ADV=Rs{adv_rs:,.0f}) | cand={cand_id[:12]}"
                    )
                    adv_cap_rejected_rows.append({
                        **cand.to_dict(),
                        "reject_reason": "adv_cap",
                        "proposed_notional_rs": proposed_notional,
                        "adv_cap_rs": adv_cap_rs,
                        "adv_rs": adv_rs,
                    })
                    continue
        v7_signal_notional_rs = float(entry_price * quantity)
        _entry_time_ist = (
            _fmt_ist(_intended_entry_ist)
            if entry_policy.get("model") == "high_break_trigger" and not pd.isna(_intended_entry_ist)
            else _fmt_ist(entry_bar.get("date")) if entry_bar is not None else ""
        )
        diag = {
            "source_session": SESSION_NAME,
            "signal_discovery_session": str(cand.get("scan_session", "")),
            "selection_mode": str(cand.get("selection_mode", "")),
            "signal_time_ist": str(cand.get("signal_time_ist", "")),
            "entry_time_ist": _entry_time_ist,
            "entry_bar_source": _entry_bar_source,
            "sl_pct": sl_pct,
            "target_pct": tgt_pct,
            "exit_rule_source": rule_source,
            "entry_policy": entry_policy,
            "exit_policy": exit_policy,
            "v11_backtesting_overlay_enabled": bool(V11_BACKTESTING_OVERLAY_ENABLE),
            "v11_selected_strategy_profile": str(V11_BACKTESTING_OVERLAY_PROFILE),
            "reason": str(cand.get("reason", "")),
            "rs_pct": cand.get("rs_pct", ""),
            "market_ret_pct": cand.get("market_ret_pct", ""),
            "ranker_score": cand.get("ranker_score", ""),
            "vol_ratio": cand.get("vol_ratio", ""),
            "atr_pct": cand.get("atr_pct", ""),
            "body_pct": cand.get("body_pct", ""),
            "close_loc": cand.get("close_loc", ""),
            "vwap_dist_atr": cand.get("vwap_dist_atr", ""),
            "v7_signal_notional_rs": v7_signal_notional_rs,
            # P2-14: schema version of the candidate row this entry was built from
            "candidate_schema_version": str(cand.get("candidate_schema_version", "")),
        }
        rows.append({
            "ticker": str(cand.get("ticker", "")).upper(),
            "side": side,
            "setup": setup,
            "bar_time_ist": str(cand.get("signal_time_ist", "")),
            "signal_time_ist": str(cand.get("signal_time_ist", "")),
            "intended_entry_ist": _fmt_ist(_intended_entry_ist) if not pd.isna(_intended_entry_ist) else "",
            "signal_open": cand.get("signal_open", ""),
            "signal_high": cand.get("signal_high", ""),
            "signal_low": cand.get("signal_low", ""),
            "signal_close": cand.get("signal_close", ""),
            "signal_volume": cand.get("signal_volume", ""),
            "signal_adx": cand.get("signal_adx", ""),
            "signal_rsi": cand.get("signal_rsi", ""),
            "signal_vol_ratio20": cand.get(
                "signal_vol_ratio20", cand.get("vol_ratio", "")
            ),
            "entry_price": entry_price,
            "sl_price": sl_price,
            "target_price": target_price,
            "score": cand.get("quality_score", 0.0),
            "quality_score": cand.get("quality_score", 0.0),
            "ranker_score": cand.get("ranker_score", ""),
            "rs_pct": cand.get("rs_pct", ""),
            "market_ret_pct": cand.get("market_ret_pct", ""),
            "regime": cand.get("regime", ""),
            "vol_ratio": cand.get("vol_ratio", ""),
            "atr_pct": cand.get("atr_pct", ""),
            "body_pct": cand.get("body_pct", ""),
            "close_loc": cand.get("close_loc", ""),
            "vwap_dist_atr": cand.get("vwap_dist_atr", ""),
            "diagnostics_json": json.dumps(diag, default=str),
            "entry_time_ist": _entry_time_ist,
            "v7_signal_entry_time_ist": _entry_time_ist,
            "entry_bar_source": _entry_bar_source,
            "v7_signal_entry_price": entry_price,
            "v7_signal_stop_price": sl_price,
            "v7_signal_target_price": target_price,
            "quantity": int(quantity),
            "v7_signal_notional_rs": v7_signal_notional_rs,
            "candidate_id": str(cand.get("candidate_id", "")),
            "bar_closed_at_ist": str(
                cand.get("bar_closed_at_ist", cand.get("signal_time_ist", ""))
            ),
            "decision_ready_at_ist": str(cand.get("decision_ready_at_ist", "")),
            "candidate_snapshot_sha256": str(cand.get("candidate_snapshot_sha256", "")),
            "runtime_manifest_path": str(cand.get("runtime_manifest_path", "")),
            "selection_mode": str(cand.get("selection_mode", "")),
            "exit_rule_source": rule_source,
            "sl_pct": sl_pct,
            "target_pct": tgt_pct,
            "v11_selected_strategy_profile": str(V11_BACKTESTING_OVERLAY_PROFILE),
            "v11_exit_override_applied": bool(exit_override is not None),
            "entry_policy_model": str(entry_policy.get("model", "")),
            "entry_trigger_price": cand.get("entry_trigger_price", ""),
            "entry_cancel_price": cand.get("entry_cancel_price", ""),
            "entry_valid_minutes": entry_policy.get(
                "valid_minutes", cand.get("entry_valid_minutes", "")
            ),
            "entry_max_gap_pct": entry_policy.get(
                "max_gap_pct", cand.get("entry_max_gap_pct", "")
            ),
            "max_hold_minutes": exit_policy.get("max_hold_minutes", ""),
            "forced_exit_time": exit_policy.get("forced_exit_time", ""),
            "stop_gap_mode": exit_policy.get("stop_gap_mode", ""),
        })
    return (
        pd.DataFrame(rows),
        pd.DataFrame(adv_cap_rejected_rows) if adv_cap_rejected_rows else pd.DataFrame(),
        pd.DataFrame(fno_ban_rejected_rows) if fno_ban_rejected_rows else pd.DataFrame(),
    )


def _entry_reject_audit(candidates: pd.DataFrame, raw_by_ticker: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    if candidates is None or candidates.empty:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    for _, cand in candidates.iterrows():
        setup = str(cand.get("setup", ""))
        ticker = str(cand.get("ticker", "")).upper().strip()
        reason = ""
        rule = (
            v11_live_overlay.exit_rule_for_setup(setup, v6.SETUP_EXIT_RULES, V11_BACKTESTING_OVERLAY_PROFILE)
            if V11_BACKTESTING_OVERLAY_ENABLE and not _CONF_ENGINE_ACTIVE
            else v6.SETUP_EXIT_RULES.get(setup)
        )
        if rule is None:
            reason = "missing_v8_setup_exit_rule"
        elif ticker not in raw_by_ticker:
            reason = "missing_1min_fetch"
        elif (
            not (
                _CONF_ENGINE_ACTIVE
                and _conf_boot.entry_policy_for_setup(setup).get("model")
                == "high_break_trigger"
            )
            and _entry_bar_for_candidate(raw_by_ticker, cand) is None
        ):
            reason = "missing_1min_entry_bar"
        if reason:
            rows.append({
                "ticker": ticker,
                "side": str(cand.get("side", "")).upper(),
                "setup": setup,
                "signal_time_ist": str(cand.get("signal_time_ist", "")),
                "candidate_id": str(cand.get("candidate_id", "")),
                "selection_mode": str(cand.get("selection_mode", "")),
                "reject_reason": reason,
            })
    return pd.DataFrame(rows)


def _apply_v11_entry_overlay(entry_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    stats: Dict[str, Any] = {
        "v11_entry_overlay_enabled": bool(V11_BACKTESTING_OVERLAY_ENABLE),
        "v11_entry_overlay_profile": str(V11_BACKTESTING_OVERLAY_PROFILE),
        "v11_entry_overlay_input_rows": int(0 if entry_df is None else len(entry_df)),
        "v11_entry_overlay_v11_setup_rows": 0,
        "v11_entry_overlay_passed": 0,
        "v11_entry_overlay_rejected": 0,
        "v11_entry_overlay_v7_only_rows": 0,
    }
    if entry_df is None or entry_df.empty:
        return pd.DataFrame(), pd.DataFrame(), stats
    if not V11_BACKTESTING_OVERLAY_ENABLE or _CONF_ENGINE_ACTIVE:
        # Conf mode: the conf mask (scanner) + conf pre-momentum gate are the gate
        # of record. The legacy tier123_balanced overlay must NOT re-filter conf
        # entries (it rejected the 6 conf setups that overlap its universe, e.g.
        # A_MOD_BREAK_C1_LOW, B_AVWAP_RECLAIM_REVERSAL). Pass entries through.
        out = entry_df.copy()
        out["v11_live_entry_overlay_status"] = "CONF_BYPASS" if _CONF_ENGINE_ACTIVE else "DISABLED"
        stats["v11_entry_overlay_passed"] = int(len(out))
        stats["v11_entry_overlay_v7_only_rows"] = int(len(out))
        return out, out.iloc[0:0].copy(), stats

    universe = v11_live_overlay.v11_override_setup_universe(V11_BACKTESTING_OVERLAY_PROFILE)
    work = entry_df.copy()
    work["_entry_order"] = np.arange(len(work))
    setup = work.get("setup", pd.Series("", index=work.index)).astype(str)
    v11_pool = work.loc[setup.isin(universe)].copy()
    v7_only = work.loc[~setup.isin(universe)].copy()
    stats["v11_entry_overlay_v11_setup_rows"] = int(len(v11_pool))
    stats["v11_entry_overlay_v7_only_rows"] = int(len(v7_only))

    if v11_pool.empty:
        v7_only["v11_live_entry_overlay_status"] = "NOT_V11_SETUP"
        return v7_only.drop(columns=["_entry_order"], errors="ignore").reset_index(drop=True), v7_only.iloc[0:0].copy(), stats

    accepted, rejected, profile_stats = v11_live_overlay.apply_selected_strategy_profile(
        v11_pool,
        V11_BACKTESTING_OVERLAY_PROFILE,
        estimate_missing_notional=False,
    )
    if not accepted.empty:
        accepted["v11_live_entry_overlay_status"] = "PASSED"
        accepted["v11_live_entry_overlay_version"] = v11_live_overlay.V11_LIVE_OVERLAY_VERSION
    if not rejected.empty:
        rejected["v11_live_entry_overlay_status"] = "REJECTED"
        rejected["v11_live_entry_overlay_version"] = v11_live_overlay.V11_LIVE_OVERLAY_VERSION
        rejected["reject_reason"] = "v11_selected_strategy_profile_failed_after_exact_1min_entry"
    if not v7_only.empty:
        v7_only["v11_live_entry_overlay_status"] = "NOT_V11_SETUP"

    out = pd.concat([accepted, v7_only], ignore_index=True, sort=False)
    if not out.empty:
        out = (
            out.sort_values("_entry_order")
            .drop(columns=["_entry_order"], errors="ignore")
            .reset_index(drop=True)
        )
    if not rejected.empty:
        rejected = (
            rejected.sort_values("_entry_order")
            .drop(columns=["_entry_order"], errors="ignore")
            .reset_index(drop=True)
        )
    stats.update(
        {
            "v11_entry_overlay_passed": int(len(accepted)),
            "v11_entry_overlay_rejected": int(len(rejected)),
            **{f"v11_entry_overlay_{k}": v for k, v in profile_stats.items()},
        }
    )
    return out, rejected, stats


def _select_executable_entries(entry_df: pd.DataFrame) -> pd.DataFrame:
    """Keep one executable row per signal candle/ticker.

    Candidate discovery intentionally stores all setup hits. Execution should
    not place multiple orders for the same ticker/slot just because two setup
    labels or opposite-side labels fired on the same signal candle.
    """
    if entry_df is None or entry_df.empty:
        return pd.DataFrame()
    df = entry_df.copy()
    df["_score_num"] = pd.to_numeric(df.get("score", 0.0), errors="coerce").fillna(0.0)
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["bar_time_ist"] = df["bar_time_ist"].astype(str)
    df = (
        df.sort_values(["_score_num", "setup"], ascending=[False, True])
        .drop_duplicates(subset=["bar_time_ist", "ticker"], keep="first")
        .drop(columns=["_score_num"], errors="ignore")
        .reset_index(drop=True)
    )
    return df


def _live_signal_csv_path(day: str, side: str) -> Path:
    return v7_persistent._signal_csv_path(day, side)


def _load_live_written_tickers(day: str) -> set[str]:
    tickers: set[str] = set()
    for side in ("SHORT", "LONG"):
        path = _live_signal_csv_path(day, side)
        if not path.exists() or path.stat().st_size <= 0:
            continue
        try:
            existing = pd.read_csv(path, usecols=["ticker"])
        except Exception:
            continue
        tickers.update(existing["ticker"].dropna().astype(str).str.upper().str.strip())
    return {t for t in tickers if t}


def _filter_new_intraday_tickers(entry_df: pd.DataFrame, slot: pd.Timestamp) -> pd.DataFrame:
    """Reject tickers already written today in either side live CSV."""
    if entry_df is None or entry_df.empty:
        return pd.DataFrame()
    day = _ensure_ist_ts(slot).strftime("%Y-%m-%d")
    if _IS_REPLAY_ISOLATED:
        used: set[str] = set()
        current_key = _slot_key(slot)
        for path in AUDIT_DIR.glob(f"entry_rows_{day.replace('-', '')}_*.csv"):
            prior_key = path.stem.removeprefix("entry_rows_")
            if prior_key >= current_key or path.stat().st_size <= 0:
                continue
            try:
                prior = pd.read_csv(path, usecols=["ticker"])
            except Exception:
                continue
            used.update(
                prior["ticker"].dropna().astype(str).str.upper().str.strip()
            )
    else:
        used = _load_live_written_tickers(day)
    df = entry_df.copy()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["_score_num"] = pd.to_numeric(df.get("score", 0.0), errors="coerce").fillna(0.0)
    df = df.sort_values(["_score_num", "ticker"], ascending=[False, True])
    keep = []
    batch: set[str] = set()
    for _, row in df.iterrows():
        ticker = str(row.get("ticker", "")).upper().strip()
        if not ticker or ticker in used or ticker in batch:
            continue
        keep.append(row.to_dict())
        batch.add(ticker)
    if not keep:
        return pd.DataFrame(columns=[c for c in df.columns if c != "_score_num"])
    return pd.DataFrame(keep).drop(columns=["_score_num"], errors="ignore").reset_index(drop=True)


def _write_live_entry_csvs(
    entry_df: pd.DataFrame,
    slot: pd.Timestamp,
) -> Tuple[int, int, float, float, float, Any, Any]:
    if entry_df.empty:
        return 0, 0, 0.0, 0.0, 0.0, None, None
    write_started = time.perf_counter()
    day = _ensure_ist_ts(slot).strftime("%Y-%m-%d")
    live_signals_dir = v7_persistent.Path(v7_persistent.LIVE_SIGNALS_DIR)

    short_started = time.perf_counter()
    short_result = signal_writer.write_side_signals(
        entry_df,
        side="SHORT",
        signal_day_str=day,
        live_signals_dir=live_signals_dir,
        writer_name="entry_engine_1min_v5_id",
        writer_pid=os.getpid(),
        source_session=SESSION_NAME,
        entry_lag_min=ENTRY_SIGNAL_TO_ENTRY_LAG_MIN,
        max_lag_sec=float(MAX_SIGNAL_HANDOFF_LAG_SEC) if MAX_SIGNAL_HANDOFF_LAG_SEC > 0 else 30.0,
        entry_window_start=v7_persistent.ENTRY_WINDOW_START_RAW,
        entry_window_end=v7_persistent.ENTRY_WINDOW_END_RAW,
    )
    short_elapsed = time.perf_counter() - short_started

    long_started = time.perf_counter()
    long_result = signal_writer.write_side_signals(
        entry_df,
        side="LONG",
        signal_day_str=day,
        live_signals_dir=live_signals_dir,
        writer_name="entry_engine_1min_v5_id",
        writer_pid=os.getpid(),
        source_session=SESSION_NAME,
        entry_lag_min=ENTRY_SIGNAL_TO_ENTRY_LAG_MIN,
        max_lag_sec=float(MAX_SIGNAL_HANDOFF_LAG_SEC) if MAX_SIGNAL_HANDOFF_LAG_SEC > 0 else 30.0,
        entry_window_start=v7_persistent.ENTRY_WINDOW_START_RAW,
        entry_window_end=v7_persistent.ENTRY_WINDOW_END_RAW,
    )
    long_elapsed = time.perf_counter() - long_started

    return (
        int(short_result.written),
        int(long_result.written),
        round(short_elapsed, 3),
        round(long_elapsed, 3),
        round(time.perf_counter() - write_started, 3),
        short_result,
        long_result,
    )


_nifty_regime_cache: dict = {}  # date_str -> multiplier


def _nifty_regime_short_mult() -> float:
    """Return 1.0 (full size) or NIFTY_REGIME_SHORT_SIZE_MULT (halved) for the
    current trading day.  Bullish regime = NIFTY daily close > rising 20-day MA.
    Result cached once per trading day; falls back to 1.0 on any data error.
    """
    if not NIFTY_REGIME_GATE_ENABLED:
        return 1.0
    import datetime as _dt
    today_str = _dt.date.today().isoformat()
    if today_str in _nifty_regime_cache:
        return _nifty_regime_cache[today_str]
    mult = nifty_regime_short_multiplier(
        NIFTY_5MIN_PARQUET,
        trade_day=today_str,
        enabled=NIFTY_REGIME_GATE_ENABLED,
        ma_days=NIFTY_MA_DAYS,
        bullish_multiplier=NIFTY_REGIME_SHORT_SIZE_MULT,
    )
    _nifty_regime_cache[today_str] = mult
    return mult

_fno_ban_cache: dict = {}  # date_str -> frozenset[str] of banned tickers
_fno_ban_cache_lock = threading.Lock()


def _get_fno_banned_tickers() -> frozenset:
    """Return the set of F&O-banned tickers for today, fetched once and cached.

    Tries a local file written today first (cached by an earlier call), then
    falls back to a live HTTP fetch.  Returns empty frozenset on failure so that
    bans never block trades when data is unavailable (fail-open).
    """
    if not FNO_BAN_FILTER_ENABLED:
        return frozenset()
    today_str = _dt.date.today().isoformat()
    with _fno_ban_cache_lock:
        if today_str in _fno_ban_cache:
            return _fno_ban_cache[today_str]
    banned: frozenset = frozenset()
    try:
        local = FNO_BAN_LOCAL_FILE
        loaded_from = None
        text = None
        # Prefer a local file written today (avoids repeated HTTP in the same day)
        if os.path.exists(local):
            import datetime as _datetime_mod
            mtime = _datetime_mod.date.fromtimestamp(os.path.getmtime(local))
            if mtime.isoformat() == today_str:
                with open(local, "r", encoding="utf-8", errors="replace") as fh:
                    text = fh.read()
                loaded_from = "local"
        if loaded_from is None:
            # NSE archives stalls bare requests; send a browser User-Agent so the
            # fetch actually returns (it 200s in ~0.2s) instead of timing out.
            req = _urlrequest.Request(FNO_BAN_URL, headers={"User-Agent": "Mozilla/5.0"})
            with _urlrequest.urlopen(req, timeout=max(0.5, FNO_BAN_FETCH_TIMEOUT_SEC)) as resp:
                text = resp.read().decode("utf-8", errors="replace")
            loaded_from = "http"
            try:
                with open(local, "w", encoding="utf-8") as fh:
                    fh.write(text)
            except Exception:
                pass
        # NSE fo_secban.csv is a title line then "serial,SYMBOL" rows (ragged, so it
        # is parsed as raw text rather than with pd.read_csv); a cached or pre-written
        # file may be one symbol per line. Take the last comma field of each line so we
        # get the symbol regardless of layout, then drop the title line, the "SYMBOL"
        # header, and bare serial numbers.
        banned_syms = set()
        for line in (text or "").splitlines():
            sym = line.split(",")[-1].strip().upper()
            if (sym and sym != "SYMBOL" and not sym.replace(".", "").isdigit()
                    and not sym.startswith("SECURITIES IN BAN")):
                banned_syms.add(sym)
        banned = frozenset(banned_syms)
        _logger().info(f"[FNO.BAN] loaded {len(banned)} banned tickers from {loaded_from}")
    except Exception as exc:
        _logger().warning(f"[FNO.BAN] fetch failed (fail-open, no trades blocked): {exc}")
    with _fno_ban_cache_lock:
        _fno_ban_cache[today_str] = banned
    return banned


_adv_cache: dict = {}  # (ticker, date_str) -> adv_rs  (0.0 = data unavailable)


def _get_adv_rs(ticker: str) -> float:
    """Return 20-day average daily traded value (Rs) for ticker, cached per day.

    Returns 0.0 if the daily parquet is missing or has insufficient rows —
    caller treats 0.0 as "no cap" (fail-open) so bad data never blocks a trade.
    """
    if not ADV_CAP_ENABLED:
        return 0.0
    import datetime as _dt
    today_str = _dt.date.today().isoformat()
    key = (ticker.upper(), today_str)
    if key in _adv_cache:
        return _adv_cache[key]
    adv = 0.0
    try:
        path = os.path.join(ADV_DAILY_PARQUET_DIR, f"{ticker.upper()}_stocks_indicators_daily.parquet")
        df = pd.read_parquet(path, columns=["close", "volume"])
        df = df.dropna(subset=["close", "volume"]).tail(ADV_LOOKBACK_DAYS)
        if len(df) >= max(1, ADV_LOOKBACK_DAYS // 2):
            adv = float((df["close"] * df["volume"]).mean())
    except Exception:
        pass
    _adv_cache[key] = adv
    return adv


def _risk_based_qty(entry_price: float, sl_price: float) -> int:
    """Compute risk-based position size.  Falls back to fixed-notional on bad inputs."""
    return risk_based_quantity(
        entry_price,
        sl_price,
        RiskSizingConfig(
            enabled=RISK_SIZING_ENABLED,
            fallback_notional_rs=V7_SIGNAL_NOTIONAL_RS,
            equity_rs=RISK_EQUITY_RS,
            risk_pct_per_trade=RISK_PCT_PER_TRADE,
            min_notional_rs=RISK_MIN_NOTIONAL_RS,
            max_notional_rs=RISK_MAX_NOTIONAL_RS,
        ),
    )


def run_slot(slot_ts: Any, *, write_live_entries: bool = True) -> Dict[str, Any]:
    global _last_raw_fetch_stats
    _ensure_conf_engine()
    slot = _ensure_ist_ts(slot_ts).floor("min")
    t0 = time.perf_counter()
    _touch_progress("SLOT_START", slot=slot, log_line=True)
    stage_started = time.perf_counter()
    candidates, candidate_wait_stats = _load_candidates_for_slot_with_wait(slot)
    candidate_load_elapsed_sec = round(time.perf_counter() - stage_started, 3)
    _touch_progress(
        "CANDIDATES_LOADED",
        slot=slot,
        candidates=len(candidates),
        elapsed_sec=candidate_load_elapsed_sec,
        log_line=True,
    )
    raw_fetch_wrapper_started = time.perf_counter()
    if not candidates.empty:
        raw_by_ticker = _fetch_raw_for_candidates(candidates, slot)
    else:
        _last_raw_fetch_stats = {
            "raw_fetch_mode": "none",
            "raw_fetch_elapsed_sec": 0.0,
            "raw_fetch_active_apps": 0,
            "raw_fetch_worker_apps": 0,
            "raw_fetch_failures": 0,
            "raw_fetch_app_partitions": {},
            "raw_fetch_app_elapsed_sec": {},
        }
        raw_by_ticker = {}
    raw_fetch_wrapper_elapsed_sec = round(time.perf_counter() - raw_fetch_wrapper_started, 3)
    stage_started = time.perf_counter()
    _touch_progress(
        "ENTRY_ROWS_BUILD",
        slot=slot,
        candidates=len(candidates),
        tickers_fetched=len(raw_by_ticker),
        log_line=True,
    )
    if raw_by_ticker:
        raw_entries, adv_cap_rejected, fno_ban_rejected = _build_entry_rows(candidates, raw_by_ticker, slot=slot)
    else:
        raw_entries, adv_cap_rejected, fno_ban_rejected = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    entry_scan_elapsed_sec = round(time.perf_counter() - stage_started, 3)
    _touch_progress(
        "ENTRY_ROWS_BUILT",
        slot=slot,
        raw_entry_rows=len(raw_entries),
        adv_rejected=len(adv_cap_rejected),
        fno_rejected=len(fno_ban_rejected),
        elapsed_sec=entry_scan_elapsed_sec,
        log_line=True,
    )
    stage_started = time.perf_counter()
    _touch_progress("ENTRY_REJECT_AUDIT", slot=slot)
    entry_fetch_rejected = _entry_reject_audit(candidates, raw_by_ticker)
    entry_reject_audit_elapsed_sec = round(time.perf_counter() - stage_started, 3)
    stage_started = time.perf_counter()
    _touch_progress("V11_ENTRY_OVERLAY", slot=slot, input_rows=len(raw_entries), log_line=True)
    v11_filtered_entries, v11_entry_rejected, v11_entry_stats = _apply_v11_entry_overlay(raw_entries)
    v11_entry_overlay_elapsed_sec = round(time.perf_counter() - stage_started, 3)
    if not v11_filtered_entries.empty:
        v11_filtered_entries = v11_filtered_entries.copy()
        v11_filtered_entries["pre_momentum_cutoff_ist"] = _fmt_ist(
            slot + pd.Timedelta(seconds=ENTRY_DELAY_SEC)
        )
    stage_started = time.perf_counter()
    _touch_progress("PRE_MOMENTUM_GATE", slot=slot, input_rows=len(v11_filtered_entries), log_line=True)
    pre_momentum_entries, pre_momentum_rejected, pre_momentum_stats = _apply_pre_entry_momentum_gate(
        v11_filtered_entries,
        raw_by_ticker,
    )
    pre_momentum_gate_elapsed_sec = round(time.perf_counter() - stage_started, 3)
    rejected_frames = [
        df for df in (entry_fetch_rejected, v11_entry_rejected, pre_momentum_rejected)
        if df is not None and not df.empty
    ]
    rejected_entries = pd.concat(rejected_frames, ignore_index=True, sort=False) if rejected_frames else pd.DataFrame()
    stage_started = time.perf_counter()
    _touch_progress("ENTRY_SELECT", slot=slot, input_rows=len(pre_momentum_entries), log_line=True)
    selected_entries = _select_executable_entries(pre_momentum_entries)
    entries = _filter_new_intraday_tickers(selected_entries, slot)
    entry_select_elapsed_sec = round(time.perf_counter() - stage_started, 3)

    # Capture stage2_detected_at_ist immediately after all mandatory filters accept
    # the candidate. This is the wall-clock time the entry engine approved it for
    # signal writing — distinct from detected_time_ist (the later CSV-write time).
    _stage2_ts = ""
    if not entries.empty:
        _stage2_ts = _fmt_ist(pd.Timestamp.now(tz=IST))
        entries = entries.copy()
        entries["stage2_detected_at_ist"] = _stage2_ts
        entries["entry_engine_ready_at_ist"] = _stage2_ts

    csv_write_started = time.perf_counter()
    _touch_progress("AUDIT_CSV_WRITE", slot=slot, entry_rows=len(entries), log_line=True)
    latest_entries_csv = LATEST_DIR / "latest_entry_engine_rows.csv"
    _atomic_write_csv(latest_entries_csv, entries)
    slot_entries_csv = AUDIT_DIR / f"entry_rows_{_slot_key(slot)}.csv"
    _atomic_write_csv(slot_entries_csv, entries)
    raw_slot_entries_csv = AUDIT_DIR / f"entry_rows_raw_candidates_{_slot_key(slot)}.csv"
    _atomic_write_csv(raw_slot_entries_csv, raw_entries)
    rejected_entries_csv = AUDIT_DIR / f"entry_rejected_candidates_{_slot_key(slot)}.csv"
    _atomic_write_csv(rejected_entries_csv, rejected_entries)
    v11_entry_rejected_csv = AUDIT_DIR / f"entry_rejected_v11_overlay_{_slot_key(slot)}.csv"
    _atomic_write_csv(v11_entry_rejected_csv, v11_entry_rejected)
    pre_momentum_rejected_csv = AUDIT_DIR / f"entry_rejected_pre_momentum_{_slot_key(slot)}.csv"
    _atomic_write_csv(pre_momentum_rejected_csv, pre_momentum_rejected)
    # P2-15: funnel card — ADV cap and F&O ban reject audit CSVs
    adv_cap_rejected_csv = AUDIT_DIR / f"entry_rejected_adv_cap_{_slot_key(slot)}.csv"
    _atomic_write_csv(adv_cap_rejected_csv, adv_cap_rejected)
    fno_ban_rejected_csv = AUDIT_DIR / f"entry_rejected_fno_ban_{_slot_key(slot)}.csv"
    _atomic_write_csv(fno_ban_rejected_csv, fno_ban_rejected)
    audit_csv_write_elapsed_sec = round(time.perf_counter() - csv_write_started, 3)
    rules_write_started = time.perf_counter()
    setup_exit_rules = (
        v11_live_overlay.live_exit_rules(v6.SETUP_EXIT_RULES, V11_BACKTESTING_OVERLAY_PROFILE)
        if V11_BACKTESTING_OVERLAY_ENABLE
        else dict(v6.SETUP_EXIT_RULES)
    )
    _atomic_write_csv(
        LATEST_DIR / "setup_exit_rules_v8.csv",
        pd.DataFrame([{"setup": k, "sl_pct": v[0], "target_pct": v[1]} for k, v in sorted(setup_exit_rules.items())]),
    )
    setup_exit_rules_write_elapsed_sec = round(time.perf_counter() - rules_write_started, 3)

    # Pre-write freshness gate: recheck timing contract at the moment of writing.
    # detected_time_ist is intentionally set to NOW (not scanner creation time)
    # so the lag reflects the actual wall-clock delay at the write boundary.
    freshness_rejected_rows: list = []
    freshness_passed_rows: list = []
    _touch_progress("FRESHNESS_GATE", slot=slot, entry_rows=len(entries), log_line=True)
    if write_live_entries and not entries.empty:
        _max_lag = float(MAX_SIGNAL_HANDOFF_LAG_SEC) if MAX_SIGNAL_HANDOFF_LAG_SEC > 0 else 30.0
        for _, _row in entries.iterrows():
            _ok, _reason = signal_contract.validate_signal_timing_row(
                _row.to_dict(),
                max_lag_sec=_max_lag,
                detected_time=v7_persistent.base_v15.now_ist(),
            )
            if _ok:
                freshness_passed_rows.append(_row.to_dict())
            else:
                _rej = _row.to_dict()
                _rej["reject_reason"] = f"pre_write_freshness: {_reason}"
                freshness_rejected_rows.append(_rej)
        entries = pd.DataFrame(freshness_passed_rows) if freshness_passed_rows else pd.DataFrame()
    freshness_rejected_df = pd.DataFrame(freshness_rejected_rows)
    freshness_rejected_csv = AUDIT_DIR / f"entry_rejected_freshness_{_slot_key(slot)}.csv"
    _atomic_write_csv(freshness_rejected_csv, freshness_rejected_df)

    short_written = long_written = 0
    short_signal_write_elapsed_sec = 0.0
    long_signal_write_elapsed_sec = 0.0
    live_signal_csv_write_elapsed_sec = 0.0
    short_write_result = long_write_result = None
    writer_input_entries = entries.copy()
    if write_live_entries and not entries.empty:
        entries = entries.copy()
        entries["signal_write_requested_at_ist"] = _fmt_ist(pd.Timestamp.now(tz=IST))
        _touch_progress("LIVE_SIGNAL_CSV_WRITE", slot=slot, entry_rows=len(entries), log_line=True)
        (
            short_written,
            long_written,
            short_signal_write_elapsed_sec,
            long_signal_write_elapsed_sec,
            live_signal_csv_write_elapsed_sec,
            short_write_result,
            long_write_result,
        ) = _write_live_entry_csvs(entries, slot)
    written_candidate_ids = {
        str(candidate_id)
        for result in (short_write_result, long_write_result)
        if result is not None
        for candidate_id in result.written_candidate_ids
        if str(candidate_id)
    }
    if write_live_entries:
        writer_passed_entries = (
            writer_input_entries.loc[
                writer_input_entries.get(
                    "candidate_id", pd.Series("", index=writer_input_entries.index)
                ).astype(str).isin(written_candidate_ids)
            ].copy()
            if not writer_input_entries.empty
            else pd.DataFrame()
        )
        writer_rejected_entries = decision_funnel.difference_frame(
            writer_input_entries,
            writer_passed_entries,
        )
    else:
        writer_passed_entries = writer_input_entries
        writer_rejected_entries = pd.DataFrame()
    signal_write_completed_at_ist = _fmt_ist(pd.Timestamp.now(tz=IST))

    decision_ready_ts = _ensure_ist_ts(
        candidate_wait_stats.get("decision_ready_at_ist")
    ) if candidate_wait_stats.get("decision_ready_at_ist") else pd.NaT
    stage2_ts = _ensure_ist_ts(_stage2_ts) if _stage2_ts else pd.NaT
    signal_write_ts = _ensure_ist_ts(signal_write_completed_at_ist)
    bar_close_to_candidate_ready_sec = (
        float((decision_ready_ts - slot).total_seconds())
        if pd.notna(decision_ready_ts)
        else np.nan
    )
    candidate_ready_to_entry_engine_ready_sec = (
        float((stage2_ts - decision_ready_ts).total_seconds())
        if pd.notna(stage2_ts) and pd.notna(decision_ready_ts)
        else np.nan
    )
    candidate_ready_to_signal_write_sec = (
        float((signal_write_ts - decision_ready_ts).total_seconds())
        if pd.notna(decision_ready_ts) and (short_written + long_written) > 0
        else np.nan
    )
    intended_entry_ts = slot + pd.Timedelta(
        minutes=ENTRY_SIGNAL_TO_ENTRY_LAG_MIN
    )
    intended_entry_to_signal_write_sec = (
        float((signal_write_ts - intended_entry_ts).total_seconds())
        if (short_written + long_written) > 0
        else np.nan
    )
    entry_write_sla_state = (
        "PASS"
        if np.isfinite(intended_entry_to_signal_write_sec)
        and intended_entry_to_signal_write_sec <= ENTRY_WRITE_SLA_SEC
        else "MISS"
        if np.isfinite(intended_entry_to_signal_write_sec)
        else "NO_SIGNAL"
    )

    entry_funnel_recorded_at = _fmt_ist(pd.Timestamp.now(tz=IST))
    entry_funnel_records: List[Dict[str, Any]] = []
    entry_funnel_stages = (
        (candidates, "candidate_snapshot", "PASS", ""),
        (raw_entries, "entry_row_build", "PASS", ""),
        (entry_fetch_rejected, "entry_row_build", "REJECT", "entry_row_build_rejected"),
        (adv_cap_rejected, "adv_cap", "REJECT", "adv_cap"),
        (fno_ban_rejected, "fno_ban", "REJECT", "fno_ban_short"),
        (v11_filtered_entries, "entry_overlay", "PASS", ""),
        (v11_entry_rejected, "entry_overlay", "REJECT", "entry_overlay_rejected"),
        (pre_momentum_entries, "pre_momentum", "PASS", ""),
        (pre_momentum_rejected, "pre_momentum", "REJECT", "pre_momentum_rejected"),
        (selected_entries, "slot_dedupe", "PASS", ""),
        (
            decision_funnel.difference_frame(pre_momentum_entries, selected_entries),
            "slot_dedupe",
            "REJECT",
            "lower_ranked_same_ticker_slot",
        ),
        (
            writer_passed_entries,
            "signal_writer",
            "PASS" if write_live_entries else "SHADOW",
            "",
        ),
        (
            writer_rejected_entries,
            "signal_writer",
            "REJECT",
            "signal_writer_rejected",
        ),
        (freshness_rejected_df, "freshness", "REJECT", "pre_write_freshness"),
    )
    for frame, stage, outcome, reason in entry_funnel_stages:
        entry_funnel_records.extend(
            decision_funnel.records_for_stage(
                frame,
                stage=stage,
                outcome=outcome,
                recorded_at_ist=entry_funnel_recorded_at,
                default_reason=reason,
            )
        )
    entry_funnel_csv = AUDIT_DIR / f"entry_decision_funnel_{_slot_key(slot)}.csv"
    entry_funnel_jsonl = AUDIT_DIR / f"entry_decision_funnel_{_slot_key(slot)}.jsonl"
    entry_funnel_rows = decision_funnel.write_slot_funnel(
        records=entry_funnel_records,
        csv_path=entry_funnel_csv,
        jsonl_path=entry_funnel_jsonl,
    )

    raw_fetch_stats = dict(_last_raw_fetch_stats)
    raw_fetch_stats["raw_fetch_app_partitions_json"] = json.dumps(
        raw_fetch_stats.get("raw_fetch_app_partitions", {}),
        sort_keys=True,
    )
    raw_fetch_stats["raw_fetch_app_elapsed_sec_json"] = json.dumps(
        raw_fetch_stats.get("raw_fetch_app_elapsed_sec", {}),
        sort_keys=True,
    )

    summary = {
        "slot_ist": _fmt_ist(slot),
        "candidate_count": int(len(candidates)),
        "tickers_requested": int(candidates["ticker"].nunique()) if not candidates.empty and "ticker" in candidates.columns else 0,
        "tickers_fetched": int(len(raw_by_ticker)),
        "raw_entry_rows": int(len(raw_entries)),
        "v11_filtered_entry_rows": int(len(v11_filtered_entries)),
        "v11_entry_rejected_rows": int(len(v11_entry_rejected)),
        "pre_momentum_filtered_entry_rows": int(len(pre_momentum_entries)),
        "pre_momentum_rejected_csv": str(pre_momentum_rejected_csv),
        "rejected_entry_rows": int(len(rejected_entries)),
        "selected_entry_rows": int(len(selected_entries)),
        "entry_rows": int(len(entries)),
        "deduped_entry_rows": int(max(0, len(pre_momentum_entries) - len(selected_entries))),
        "intraday_duplicate_rows": int(max(0, len(selected_entries) - len(entries))),
        "short_written": int(short_written),
        "long_written": int(long_written),
        "signal_writer_input_rows": int(len(writer_input_entries)),
        "signal_writer_rejected_rows": int(len(writer_rejected_entries)),
        "signal_writer_short_result_json": json.dumps(
            short_write_result.as_dict() if short_write_result is not None else {},
            sort_keys=True,
            default=str,
        ),
        "signal_writer_long_result_json": json.dumps(
            long_write_result.as_dict() if long_write_result is not None else {},
            sort_keys=True,
            default=str,
        ),
        "entry_decision_funnel_rows": int(entry_funnel_rows),
        "entry_decision_funnel_csv": str(entry_funnel_csv),
        "signal_write_completed_at_ist": signal_write_completed_at_ist,
        "bar_close_to_candidate_ready_sec": bar_close_to_candidate_ready_sec,
        "candidate_ready_to_entry_engine_ready_sec": candidate_ready_to_entry_engine_ready_sec,
        "candidate_ready_to_signal_write_sec": candidate_ready_to_signal_write_sec,
        "intended_entry_to_signal_write_sec": intended_entry_to_signal_write_sec,
        "entry_write_sla_sec": float(ENTRY_WRITE_SLA_SEC),
        "entry_write_sla_state": entry_write_sla_state,
        "raw_dir": str(RAW_1MIN_DIR),
        "latest_entries_csv": str(latest_entries_csv),
        "setup_exit_rules_csv": str(LATEST_DIR / "setup_exit_rules_v8.csv"),
        "v11_entry_rejected_csv": str(v11_entry_rejected_csv),
        "entry_search_max_delay_min": int(ENTRY_SEARCH_MAX_DELAY_MIN),
        "elapsed_sec": round(time.perf_counter() - t0, 3),
        "candidate_load_elapsed_sec": candidate_load_elapsed_sec,
        "raw_fetch_wrapper_elapsed_sec": raw_fetch_wrapper_elapsed_sec,
        "entry_scan_elapsed_sec": entry_scan_elapsed_sec,
        "entry_reject_audit_elapsed_sec": entry_reject_audit_elapsed_sec,
        "v11_entry_overlay_elapsed_sec": v11_entry_overlay_elapsed_sec,
        "pre_momentum_gate_elapsed_sec": pre_momentum_gate_elapsed_sec,
        "entry_select_elapsed_sec": entry_select_elapsed_sec,
        "audit_csv_write_elapsed_sec": audit_csv_write_elapsed_sec,
        "setup_exit_rules_write_elapsed_sec": setup_exit_rules_write_elapsed_sec,
        "live_signal_csv_write_elapsed_sec": live_signal_csv_write_elapsed_sec,
        "short_signal_write_elapsed_sec": short_signal_write_elapsed_sec,
        "long_signal_write_elapsed_sec": long_signal_write_elapsed_sec,
        **candidate_wait_stats,
        **_last_candidate_load_stats,
        **raw_fetch_stats,
        **v11_entry_stats,
        **pre_momentum_stats,
    }
    _atomic_write_json(LATEST_DIR / "latest_summary.json", summary)
    audit_path = AUDIT_DIR / f"entry_engine_audit_{slot.strftime('%Y-%m-%d')}.jsonl"
    with open(audit_path, "a", encoding="utf-8") as f:
        f.write(json.dumps({"session": SESSION_NAME, **summary}, sort_keys=True) + "\n")
    _touch_status("RUNNING", phase="SCAN_DONE", slot=slot.strftime("%H:%M"), **summary)
    _touch_heartbeat("RUNNING", phase="SCAN_DONE", slot=slot.strftime("%H:%M"))
    return {"session": SESSION_NAME, **summary}


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=SESSION_NAME)
    ap.add_argument("--replay-slot", default="", help="Run one slot, e.g. 2026-05-21 11:10:00+05:30")
    ap.add_argument("--no-write-live-entries", action="store_true")
    ap.add_argument(
        "--production-replay",
        action="store_true",
        help="Allow --replay-slot to write to production paths (default: REFUSED "
             "unless EQIDV2_REPLAY_OUTPUT_ROOT is set)",
    )
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    manifest_path, _ = freeze_runtime_manifest(
        SESSION_SLUG,
        runtime_root=RUNTIME_ROOT,
        source_files=(
            Path(__file__),
            Path(v11_live_overlay.__file__),
            Path(_conf_boot.__file__),
            Path(signal_writer.__file__),
            Path(signal_contract.__file__),
        ),
        resolved_config={
            "session_root": str(SESSION_ROOT),
            "signal_discovery_root": str(SIGNAL_DISCOVERY_ROOT),
            "use_slot_candidate_json": bool(USE_SLOT_CANDIDATE_JSON),
            "require_slot_complete_marker": bool(REQUIRE_SLOT_COMPLETE_MARKER),
            "candidate_wait_sec": float(CANDIDATE_WAIT_SEC),
            "max_signal_handoff_lag_sec": float(MAX_SIGNAL_HANDOFF_LAG_SEC),
            "entry_due_grace_sec": int(ENTRY_DUE_GRACE_SEC),
            "entry_signal_to_entry_lag_min": int(ENTRY_SIGNAL_TO_ENTRY_LAG_MIN),
            "raw_fetch_parallel_apps": bool(RAW_FETCH_PARALLEL_APPS_ENABLED),
            "raw_fetch_app_count": int(RAW_FETCH_APP_COUNT),
            "raw_session_reuse_enabled": True,
            "entry_write_sla_sec": float(ENTRY_WRITE_SLA_SEC),
            "pre_momentum_gates_enabled": bool(PRE_ENTRY_MOMENTUM_GATES_ENABLED),
            "adv_cap_enabled": bool(ADV_CAP_ENABLED),
            "fno_ban_filter_enabled": bool(FNO_BAN_FILTER_ENABLED),
        },
    )
    print(f"[LIVE] {SESSION_NAME}", flush=True)
    print(f"[INFO] raw_1min_dir={RAW_1MIN_DIR}", flush=True)
    print(f"[INFO] frozen_runtime_manifest={manifest_path}", flush=True)

    if args.replay_slot:
        if not _IS_REPLAY_ISOLATED and not args.production_replay:
            print(
                "[ERROR] --replay-slot refused: would overwrite production outputs.\n"
                "  Option A (recommended): set EQIDV2_REPLAY_OUTPUT_ROOT=<replay_dir>\n"
                "  Option B (dangerous):   pass --production-replay to confirm.",
                flush=True,
            )
            import sys; sys.exit(1)
        if not _IS_REPLAY_ISOLATED:
            print("[REPLAY][WARN] --production-replay set: writing to PRODUCTION paths.", flush=True)
        summary = run_slot(args.replay_slot, write_live_entries=not args.no_write_live_entries)
        print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
        return

    holidays = v7_persistent.base_v15._read_holidays_safe()
    # Pre-mark all expired past slots so the monitor does not show BLOCKED for
    # slots the engine could never have fulfilled at this startup time.
    processed: set[str] = _startup_expired_slot_keys(v7_persistent.base_v15.now_ist())
    raw_sessions_prewarmed = False
    while True:
        now = v7_persistent.base_v15.now_ist()
        _touch_status("RUNNING", phase="LOOP")
        _touch_heartbeat("RUNNING", phase="LOOP")

        if now.time() >= HARD_STOP:
            _touch_status("STOPPED_AFTER_CUTOFF", phase="HARD_STOP")
            _touch_heartbeat("STOPPED", phase="HARD_STOP")
            return
        if not v7_persistent.base_v15.is_trading_day_safe(now.date(), holidays):
            nxt = v7_persistent.base_v15._next_trading_day_start(now, holidays)
            v7_persistent.base_v15._sleep_until(nxt)
            holidays = v7_persistent.base_v15._read_holidays_safe()
            processed.clear()
            continue
        if now.time() < MARKET_OPEN or now.time() > END_TIME:
            time.sleep(5.0)
            continue
        if not raw_sessions_prewarmed:
            try:
                sessions = _setup_kite_app_pool()
                print(
                    f"[{SESSION_NAME}] prewarmed {len(sessions)} Kite raw-fetch sessions",
                    flush=True,
                )
                raw_sessions_prewarmed = True
            except Exception as exc:
                print(
                    f"[{SESSION_NAME}] raw-session prewarm failed; will retry: "
                    f"{type(exc).__name__}: {exc}",
                    flush=True,
                )
                time.sleep(POLL_SEC)
                continue

        run_at = _next_entry_run_after(now)
        slot = run_at - pd.Timedelta(seconds=ENTRY_DELAY_SEC)
        key = _slot_key(slot)
        if key in processed:
            time.sleep(POLL_SEC)
            continue
        now_ts = _ensure_ist_ts(v7_persistent.base_v15.now_ist())
        if now_ts < run_at:
            time.sleep(min(POLL_SEC, max(0.0, (run_at - now_ts).total_seconds())))
            continue

        _touch_status("RUNNING", phase="ENTRY_SCAN", slot=slot.strftime("%H:%M"))
        _touch_heartbeat("RUNNING", phase="ENTRY_SCAN", slot=slot.strftime("%H:%M"))
        summary = run_slot(slot, write_live_entries=True)
        print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
        processed.add(key)
        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
