# -*- coding: utf-8 -*-
# Backup reference (2026-02-26):
# - c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\backups_codex\20260226_180142\eqidv2_eod_scheduler_for_15mins_data.py
# - c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\backups_codex\20260226_180142\run_eqidv2_eod_scheduler_for_15mins_data.bat
"""
EQIDV2 LIVE Scanner V15 SHORT (short-only split pipeline)
=========================================================

Wrapper over:
    eqidv2_live_combined_analyser_csv_v15.py

Goals:
1. Keep shared base files untouched.
2. Emit only SHORT signals.
3. Preserve immediate CSV flush behavior for SHORT.
4. Isolate all outputs/state with `v15_short` suffix.
"""

from __future__ import annotations

import contextlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import eqidv2_live_combined_analyser_csv_v15 as v2
import avwap_combined_runner_v15 as v15_runner
from eqidv2_runtime_paths import DATA_15M_DIR as RUNTIME_DATA_15M_DIR
from eqidv2_runtime_paths import LIVE_SIGNALS_DIR as RUNTIME_LIVE_SIGNALS_DIR
from eqidv2_runtime_paths import report_subdir, runtime_dir
import live_v15_shared_scan_cache as shared_scan_cache
from avwap_v11_refactored.avwap_common_v11_v15 import (
    default_short_config as v15_default_short_config,
    in_session as v15_in_session,
    prepare_indicators as v15_prepare_indicators,
    compute_day_avwap as v15_compute_day_avwap,
)
from avwap_v11_refactored.avwap_common_v7_sweep_v15 import (
    default_long_config as v15_default_long_config,
)
from avwap_v11_refactored.avwap_short_strategy_v11 import (
    scan_one_day as v15_scan_short_one_day,
)
from avwap_v11_refactored.avwap_long_strategy_v9_sweep import (
    scan_one_day as v15_scan_long_one_day,
)


ROOT = Path(__file__).resolve().parent


# Keep original functions so wrapper can delegate safely.
_ORIG_WRITE_SIGNALS_CSV = v2._write_signals_csv
_ORIG_LATEST_ENTRY_SIGNALS_FOR_TICKER = v2._latest_entry_signals_for_ticker
_ORIG_RUN_ONE_SCAN = v2.run_one_scan
_ORIG_RUN_REPLAY_FOR_DATE = v2.run_replay_for_date
_ORIG_SCAN_LONG_ONE_DAY = v2.scan_long_one_day


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int, min_value: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        value = int(default)
    else:
        try:
            value = int(str(raw).strip())
        except Exception:
            value = int(default)
    return max(int(min_value), value)


def _env_float(name: str, default: float, min_value: float = 0.0) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        value = float(default)
    else:
        try:
            value = float(str(raw).strip())
        except Exception:
            value = float(default)
    return max(float(min_value), value)


def _build_effective_v15_short_cfg():
    short_cfg = v15_default_short_config()
    long_cfg = v15_default_long_config()
    apply_profile = getattr(v15_runner, "apply_live_parity_profile", None)
    if callable(apply_profile):
        short_cfg, _ = apply_profile(short_cfg, long_cfg)
    market_regime_tickers = tuple(
        getattr(
            v15_runner,
            "NIFTY_CONTEXT_TICKERS",
            getattr(short_cfg, "market_regime_tickers", ()),
        ) or ()
    )
    if market_regime_tickers:
        short_cfg.market_regime_tickers = market_regime_tickers
    short_cfg.dir_15m = str(v15_runner._resolve_15m_dir())
    return short_cfg


STALE_ONLY_RETRY_ENABLED = _env_bool("EQIDV15_STALE_ONLY_RETRY", True)
STALE_RETRY_MAX_TICKERS = _env_int("EQIDV15_STALE_RETRY_MAX_TICKERS", 6, min_value=0)
STALE_RETRY_MAX_RATIO = _env_float("EQIDV15_STALE_RETRY_MAX_RATIO", 0.08, min_value=0.0)
_DEFAULT_V15_SHORT_CFG = _build_effective_v15_short_cfg()
SHORT_STOP_PCT = float(
    os.getenv(
        "EQIDV15_SHORT_STOP_PCT",
        str(float(getattr(_DEFAULT_V15_SHORT_CFG, "stop_pct", 0.0075))),
    )
)
SHORT_TARGET_PCT = float(
    os.getenv(
        "EQIDV15_SHORT_TARGET_PCT",
        str(float(getattr(_DEFAULT_V15_SHORT_CFG, "target_pct", 0.0095))),
    )
)
SHARED_SCAN_WAIT_SECONDS = _env_int("EQIDV15_SHARED_SCAN_WAIT_SECONDS", 90, min_value=5)
SHARED_SCAN_WAIT_POLL_SECONDS = _env_float("EQIDV15_SHARED_SCAN_WAIT_POLL_SECONDS", 0.5, min_value=0.05)
FAST_REUSE_PREVIOUS_SCAN = _env_bool("EQIDV15_FAST_REUSE_PREVIOUS_SCAN", True)
SHARD_PAIR_ID = (
    os.getenv("EQIDV15_SHORT_SHARD_ID")
    or os.getenv("EQIDV15_LONG_SHARD_ID")
    or "00"
).strip() or "00"
PAIR_STATE_FILE = ROOT / "logs" / f"eqidv2_avwap_live_state_v11_v15_pair_s{SHARD_PAIR_ID}_of_10.json"

_LAST_RAW_SLOT_KEY = ""
_LAST_RAW_CHECKS_DF = pd.DataFrame()
_LAST_RAW_SIGNALS_DF = pd.DataFrame()

_NIFTY_CONTEXT_MODE_MAP: Dict[str, str] = {}
_NIFTY_CONTEXT_RET_MAP: Dict[str, float] = {}
_NIFTY_CONTEXT_SOURCE = ""
_NIFTY_CONTEXT_CFG = None
_NIFTY_STOCK_RET_CACHE: Dict[str, Dict[str, float]] = {}


def _refresh_v15_nifty_context() -> None:
    global _NIFTY_CONTEXT_MODE_MAP, _NIFTY_CONTEXT_RET_MAP, _NIFTY_CONTEXT_SOURCE
    global _NIFTY_CONTEXT_CFG, _NIFTY_STOCK_RET_CACHE

    if not bool(getattr(v15_runner, "NIFTY_CONTEXT_ENABLED", False)):
        _NIFTY_CONTEXT_MODE_MAP = {}
        _NIFTY_CONTEXT_RET_MAP = {}
        _NIFTY_CONTEXT_SOURCE = ""
        _NIFTY_CONTEXT_CFG = None
        _NIFTY_STOCK_RET_CACHE = {}
        return

    cfg = _build_effective_v15_short_cfg()
    mode_map, ret_map, src, _counts = v15_runner._build_nifty_intraday_context(cfg)
    _NIFTY_CONTEXT_MODE_MAP = dict(mode_map or {})
    _NIFTY_CONTEXT_RET_MAP = dict(ret_map or {})
    _NIFTY_CONTEXT_SOURCE = str(src or "")
    _NIFTY_CONTEXT_CFG = cfg
    _NIFTY_STOCK_RET_CACHE = {}


def _passes_v15_nifty_context(signal: Any) -> bool:
    if not _NIFTY_CONTEXT_MODE_MAP or _NIFTY_CONTEXT_CFG is None:
        return True

    ts = pd.to_datetime(getattr(signal, "bar_time_ist", None), errors="coerce")
    if pd.isna(ts):
        return True
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize(v15_runner.IST)
    else:
        ts = ts.tz_convert(v15_runner.IST)

    ts_key = v15_runner._ts_to_key_local(ts)
    mode = str(_NIFTY_CONTEXT_MODE_MAP.get(ts_key, "BOTH")).upper()
    if mode == "LONG_ONLY":
        return False

    if not bool(getattr(v15_runner, "NIFTY_RS_FILTER_ENABLED", False)):
        return True

    if mode != "BOTH":
        rs_thresh = float(v15_runner.NIFTY_RS_THRESHOLD_PCT)
        apply_rs = True
    elif bool(getattr(v15_runner, "NIFTY_RS_BOTH_MODE_ENABLED", False)):
        rs_thresh = float(
            getattr(
                v15_runner,
                "NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT",
                getattr(v15_runner, "NIFTY_RS_BOTH_MODE_THRESHOLD_PCT", 0.0),
            )
        )
        apply_rs = True
    else:
        rs_thresh = 0.0
        apply_rs = False

    if not apply_rs:
        return True

    stock_ret_map = v15_runner._build_stock_return_map(
        getattr(signal, "ticker", ""),
        _NIFTY_CONTEXT_CFG,
        _NIFTY_STOCK_RET_CACHE,
    )
    stock_ret = stock_ret_map.get(ts_key, np.nan)
    nifty_ret = _NIFTY_CONTEXT_RET_MAP.get(ts_key, np.nan)
    if not (np.isfinite(stock_ret) and np.isfinite(nifty_ret)):
        return True
    rel_val = float(stock_ret - nifty_ret)
    return rel_val <= -rs_thresh


def _is_short_side(side_value: Any) -> bool:
    return str(side_value or "").strip().upper() == "SHORT"


def _filter_short_signals_df(signals_df: pd.DataFrame) -> pd.DataFrame:
    if signals_df is None or signals_df.empty:
        return pd.DataFrame()
    if "side" not in signals_df.columns:
        return pd.DataFrame()
    mask = signals_df["side"].astype(str).str.upper().eq("SHORT")
    return signals_df.loc[mask].copy()


def _filter_short_checks_df(checks_df: pd.DataFrame) -> pd.DataFrame:
    if checks_df is None or checks_df.empty:
        return pd.DataFrame()
    if "side" not in checks_df.columns:
        return checks_df.copy()
    mask = checks_df["side"].astype(str).str.upper().eq("SHORT")
    return checks_df.loc[mask].copy()


def _signal_obj_from_row(row_like: Any) -> Any:
    if hasattr(row_like, "to_dict"):
        payload = row_like.to_dict()
    elif isinstance(row_like, dict):
        payload = dict(row_like)
    else:
        payload = dict(getattr(row_like, "_asdict", lambda: {})())

    diagnostics = {}
    diag_raw = payload.get("diagnostics_json", "")
    if diag_raw:
        try:
            diagnostics = json.loads(diag_raw) if isinstance(diag_raw, str) else dict(diag_raw)
        except Exception:
            diagnostics = {}

    return SimpleNamespace(
        ticker=str(payload.get("ticker", "")).upper(),
        side=str(payload.get("side", "")).upper(),
        bar_time_ist=payload.get("bar_time_ist"),
        score=float(payload.get("score", 0.0) or 0.0),
        diagnostics=diagnostics,
    )


def _filter_short_signals_post(signals_df: pd.DataFrame) -> pd.DataFrame:
    df_short = _filter_short_signals_df(signals_df)
    if df_short.empty:
        return df_short

    keep_mask = []
    for _, row in df_short.iterrows():
        keep_mask.append(bool(_passes_v15_nifty_context(_signal_obj_from_row(row))))
    return df_short.loc[pd.Series(keep_mask, index=df_short.index)].copy()


def _merge_retry_rows(
    base_df: pd.DataFrame,
    retry_df: pd.DataFrame,
    stale_tickers: List[str],
) -> pd.DataFrame:
    if not stale_tickers:
        return retry_df.copy() if base_df is None or base_df.empty else base_df.copy()

    stale_set = {str(t).strip().upper() for t in stale_tickers if str(t).strip()}
    frames: List[pd.DataFrame] = []
    if base_df is not None and not base_df.empty:
        if "ticker" in base_df.columns:
            keep_mask = ~base_df["ticker"].astype(str).str.upper().isin(stale_set)
            frames.append(base_df.loc[keep_mask].copy())
        else:
            frames.append(base_df.copy())
    if retry_df is not None and not retry_df.empty:
        frames.append(retry_df.copy())
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


@contextlib.contextmanager
def _raw_full_scan_context() -> Any:
    saved = {
        "scan_short_one_day": v2.scan_short_one_day,
        "scan_long_one_day": v2.scan_long_one_day,
        "_latest_entry_signals_for_ticker": v2._latest_entry_signals_for_ticker,
        "_write_signals_csv": v2._write_signals_csv,
        "IMMEDIATE_SIGNAL_CSV_FLUSH": v2.IMMEDIATE_SIGNAL_CSV_FLUSH,
        "WRITE_RUN_PARQUETS": getattr(v2, "WRITE_RUN_PARQUETS", True),
        "STATE_FILE": v2.STATE_FILE,
    }
    try:
        v2.scan_short_one_day = v15_scan_short_one_day
        v2.scan_long_one_day = v15_scan_long_one_day
        v2._latest_entry_signals_for_ticker = _ORIG_LATEST_ENTRY_SIGNALS_FOR_TICKER
        v2._write_signals_csv = lambda _df: 0
        v2.IMMEDIATE_SIGNAL_CSV_FLUSH = False
        v2.WRITE_RUN_PARQUETS = False
        v2.STATE_FILE = PAIR_STATE_FILE
        yield
    finally:
        v2.scan_short_one_day = saved["scan_short_one_day"]
        v2.scan_long_one_day = saved["scan_long_one_day"]
        v2._latest_entry_signals_for_ticker = saved["_latest_entry_signals_for_ticker"]
        v2._write_signals_csv = saved["_write_signals_csv"]
        v2.IMMEDIATE_SIGNAL_CSV_FLUSH = saved["IMMEDIATE_SIGNAL_CSV_FLUSH"]
        v2.WRITE_RUN_PARQUETS = saved["WRITE_RUN_PARQUETS"]
        v2.STATE_FILE = saved["STATE_FILE"]


def _compute_raw_full_scan(run_tag: str, tickers_override: Optional[List[str]] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    with _raw_full_scan_context():
        orig_list_tickers = v2.list_tickers_15m
        try:
            if tickers_override is not None:
                tickers_copy = list(tickers_override)
                v2.list_tickers_15m = lambda: tickers_copy
            checks_df, signals_df = _ORIG_RUN_ONE_SCAN(run_tag)
        finally:
            v2.list_tickers_15m = orig_list_tickers
    return checks_df, signals_df


def _save_short_outputs(run_tag: str, checks_df: pd.DataFrame, signals_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    checks_short = _filter_short_checks_df(checks_df)
    signals_short = _filter_short_signals_post(signals_df)
    v2._save_run_parquets(
        checks_short,
        signals_short,
        run_tag,
        checks_dir=v2.OUT_CHECKS_DIR,
        signals_dir=v2.OUT_SIGNALS_DIR,
        suffix="v15_short",
    )
    _write_signals_csv_v15_short(signals_short)
    return checks_short, signals_short


def _slot_key(slot_ts: Any) -> str:
    ts = pd.Timestamp(slot_ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize(v2.IST)
    else:
        ts = ts.tz_convert(v2.IST)
    return ts.floor("min").strftime("%Y%m%d_%H%M")


def _latest_entry_signals_for_ticker_v15_short(
    ticker: str,
    df_raw: pd.DataFrame,
    state: Dict[str, Any],
    target_slot_ist: pd.Timestamp,
) -> Tuple[List[Any], List[Dict[str, Any]]]:
    """
    Delegate to base detector, then keep only SHORT outputs.
    """
    signals, checks = _ORIG_LATEST_ENTRY_SIGNALS_FOR_TICKER(
        ticker=ticker,
        df_raw=df_raw,
        state=state,
        target_slot_ist=target_slot_ist,
    )

    signals_short = [
        s for s in (signals or [])
        if _is_short_side(getattr(s, "side", "")) and _passes_v15_nifty_context(s)
    ]
    checks_short = [c for c in (checks or []) if _is_short_side(c.get("side", ""))]
    return signals_short, checks_short


def _write_signals_csv_v15_short(signals_df: pd.DataFrame) -> int:
    """
    CSV bridge for v15_short:
    - Keep only SHORT rows.
    - Reuse base v2 writer.
    """
    df_short = _filter_short_signals_df(signals_df)
    if df_short.empty:
        print("[V15_SHORT CSV] scanned=0 short_written=0", flush=True)
        return 0

    written = int(_ORIG_WRITE_SIGNALS_CSV(df_short))
    print(
        f"[V15_SHORT CSV] scanned={0 if signals_df is None else len(signals_df)} "
        f"| short_rows={len(df_short)} | short_written={written}",
        flush=True,
    )
    return written


def _scan_long_disabled(*_args, **_kwargs):
    """v15_short must not compute LONG side internals."""
    return []


def _extract_stale_tickers(checks_df: pd.DataFrame) -> List[str]:
    if checks_df is None or checks_df.empty or "ticker" not in checks_df.columns:
        return []

    mask = pd.Series(False, index=checks_df.index)
    for col in ("stale_data", "no_target_day_data"):
        if col in checks_df.columns:
            mask = mask | checks_df[col].astype(str).str.strip().str.lower().isin(
                {"1", "true", "yes", "y", "on"}
            )

    if not bool(mask.any()):
        return []

    tickers = (
        checks_df.loc[mask, "ticker"]
        .astype(str)
        .str.strip()
        .str.upper()
    )
    tickers = tickers[tickers != ""]
    return sorted(set(tickers.tolist()))


def _should_retry_stale_subset(stale_tickers: List[str]) -> bool:
    if not STALE_ONLY_RETRY_ENABLED or not stale_tickers:
        return False

    try:
        total_tickers = max(1, len(list(v2.list_tickers_15m())))
    except Exception:
        total_tickers = max(1, len(stale_tickers))
    stale_count = len(stale_tickers)
    stale_ratio = stale_count / float(total_tickers)
    allow = stale_count <= int(STALE_RETRY_MAX_TICKERS) and stale_ratio <= float(STALE_RETRY_MAX_RATIO)
    if not allow:
        print(
            f"[V15_SHORT RETRY] skip | stale_tickers={stale_count}/{total_tickers} "
            f"(ratio={stale_ratio:.2f}) exceeds caps "
            f"max_tickers={STALE_RETRY_MAX_TICKERS}, max_ratio={STALE_RETRY_MAX_RATIO:.2f}",
            flush=True,
        )
    return allow


def _apply_v15_strategy_engine_overrides() -> None:
    """Force v2 scanner core to use v15 strategy modules and runner config."""
    v2.default_short_config = v15_default_short_config
    v2.default_long_config = v15_default_long_config
    v2.ref_in_session = v15_in_session
    v2.ref_prepare_indicators = v15_prepare_indicators
    v2.ref_compute_day_avwap = v15_compute_day_avwap
    v2.scan_short_one_day = v15_scan_short_one_day
    v2.scan_long_one_day = v15_scan_long_one_day

    sys.modules["avwap_combined_runner"] = v15_runner
    v2.SHORT_USE_TIME_WINDOWS = bool(
        getattr(v15_runner, "FINAL_SHORT_USE_TIME_WINDOWS", v2.SHORT_USE_TIME_WINDOWS)
    )
    v2.SHORT_SIGNAL_WINDOWS = list(
        getattr(v15_runner, "FINAL_SHORT_SIGNAL_WINDOWS", v2.SHORT_SIGNAL_WINDOWS)
    )
    v2.LONG_USE_TIME_WINDOWS = bool(
        getattr(v15_runner, "FINAL_LONG_USE_TIME_WINDOWS", v2.LONG_USE_TIME_WINDOWS)
    )
    v2.LONG_SIGNAL_WINDOWS = list(
        getattr(v15_runner, "FINAL_LONG_SIGNAL_WINDOWS", v2.LONG_SIGNAL_WINDOWS)
    )
    v2.FORCE_LIVE_PARITY_MIN_BARS_LEFT = bool(
        getattr(v15_runner, "FORCE_LIVE_PARITY_MIN_BARS_LEFT", v2.FORCE_LIVE_PARITY_MIN_BARS_LEFT)
    )
    v2.FORCE_LIVE_PARITY_DISABLE_TOPN = bool(
        getattr(v15_runner, "FORCE_LIVE_PARITY_DISABLE_TOPN", v2.FORCE_LIVE_PARITY_DISABLE_TOPN)
    )


def _apply_v15_short_overrides() -> None:
    """Patch v2 module-level config/functions to isolate v15_short behavior."""
    _apply_v15_strategy_engine_overrides()

    v2.DIR_15M = str(RUNTIME_DATA_15M_DIR)
    v2.LIVE_SIGNAL_DIR = RUNTIME_LIVE_SIGNALS_DIR
    v2.LIVE_SIGNAL_DIR.mkdir(parents=True, exist_ok=True)

    v2.REPORTS_DIR = report_subdir("eqidv2_reports_v15_short")
    v2.REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    v2.OUT_CHECKS_DIR = runtime_dir("out_eqidv2_live_checks_15m_v15_short")
    v2.OUT_SIGNALS_DIR = runtime_dir("out_eqidv2_live_signals_15m_v15_short")
    v2.OUT_CHECKS_DIR.mkdir(parents=True, exist_ok=True)
    v2.OUT_SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

    v2.STATE_FILE = ROOT / "logs" / "eqidv2_avwap_live_state_v11_v15_short.json"
    v2.SIGNAL_CSV_PATTERN = "signals_{}_v15_short.csv"
    v2.END_TIME = v2.dtime(15, 0)
    v2.SESSION_END = v2.dtime(15, 0, 0)

    # Keep v2-style per-ticker immediate flush for SHORT.
    v2.IMMEDIATE_SIGNAL_CSV_FLUSH = True

    # Keep model entry/SL/TGT intact in the signal CSV for backtest/live parity.
    v2.USE_KITE_LTP_FOR_SIGNAL_CSV = False

    # V15 short stop/target override for live V15 sessions.
    v2.SHORT_STOP_PCT = float(SHORT_STOP_PCT)
    v2.SHORT_TARGET_PCT = float(SHORT_TARGET_PCT)

    # Compute only SHORT strategy internals in this process.
    v2.scan_short_one_day = v15_scan_short_one_day
    v2.scan_long_one_day = _scan_long_disabled

    # Install side filters.
    v2._latest_entry_signals_for_ticker = _latest_entry_signals_for_ticker_v15_short
    v2._write_signals_csv = _write_signals_csv_v15_short

    def _run_one_scan_v15_short(run_tag: str = "A"):
        global _LAST_RAW_SLOT_KEY, _LAST_RAW_CHECKS_DF, _LAST_RAW_SIGNALS_DF

        _refresh_v15_nifty_context()
        slot_ts = v2._current_15m_slot_start_ist()
        slot_key = _slot_key(slot_ts)
        use_cached = (
            bool(FAST_REUSE_PREVIOUS_SCAN)
            and str(run_tag).upper() != "A"
            and slot_key == _LAST_RAW_SLOT_KEY
            and _LAST_RAW_CHECKS_DF is not None
            and not _LAST_RAW_CHECKS_DF.empty
        )

        if use_cached:
            raw_checks_df = _LAST_RAW_CHECKS_DF.copy()
            raw_signals_df = _LAST_RAW_SIGNALS_DF.copy()
            print(
                f"[V15_SHORT FAST] reusing prior full scan for slot={slot_key} scan={run_tag}",
                flush=True,
            )
        else:
            raw_checks_df, raw_signals_df = _compute_raw_full_scan(run_tag)

        stale_tickers = _extract_stale_tickers(raw_checks_df)
        if _should_retry_stale_subset(stale_tickers):
            retry_tag = f"{run_tag}R"
            print(
                f"[V15_SHORT RETRY] stale_tickers={len(stale_tickers)} | rerun_subset_tag={retry_tag}",
                flush=True,
            )
            print(
                f"[V15_SHORT RETRY] stale_ticker_names={','.join(stale_tickers)}",
                flush=True,
            )
            checks_retry, signals_retry = _compute_raw_full_scan(retry_tag, tickers_override=stale_tickers)
            raw_checks_df = _merge_retry_rows(raw_checks_df, checks_retry, stale_tickers)
            raw_signals_df = _merge_retry_rows(raw_signals_df, signals_retry, stale_tickers)
            print(
                f"[V15_SHORT RETRY] done | extra_checks={0 if checks_retry is None else len(checks_retry)} "
                f"| extra_signals={0 if signals_retry is None else len(signals_retry)}",
                flush=True,
            )

        shared_scan_cache.write_bundle(
            shard_id=SHARD_PAIR_ID,
            slot_ts=slot_ts,
            run_tag=run_tag,
            checks_df=raw_checks_df,
            signals_df=raw_signals_df,
            producer="short",
        )

        _LAST_RAW_SLOT_KEY = slot_key
        _LAST_RAW_CHECKS_DF = raw_checks_df.copy()
        _LAST_RAW_SIGNALS_DF = raw_signals_df.copy()
        return _save_short_outputs(run_tag, raw_checks_df, raw_signals_df)

    v2.run_one_scan = _run_one_scan_v15_short

    # Ensure replay default output filename carries `_v15_short`.
    def _run_replay_for_date_v15_short(date_str: str, out_csv: Optional[str] = None) -> pd.DataFrame:
        if out_csv is None:
            out_csv = str(v2.OUT_SIGNALS_DIR / f"replay_signals_{date_str}_v15_short.csv")
        return _ORIG_RUN_REPLAY_FOR_DATE(date_str, out_csv=out_csv)

    v2.run_replay_for_date = _run_replay_for_date_v15_short


def main() -> None:
    _apply_v15_short_overrides()
    _refresh_v15_nifty_context()
    print(
        "[V15_SHORT] SHORT-only split enabled | immediate_flush=True | "
        f"short_stop={SHORT_STOP_PCT*100:.2f}% | "
        f"short_target={SHORT_TARGET_PCT*100:.2f}% | "
        f"stale_only_retry={STALE_ONLY_RETRY_ENABLED} | "
        f"nifty_context={'on' if bool(getattr(v15_runner, 'NIFTY_CONTEXT_ENABLED', False)) else 'off'} | "
        f"nifty_source={_NIFTY_CONTEXT_SOURCE or 'NA'} | "
        "signal_csv=signals_YYYY-MM-DD_v15_short.csv",
        flush=True,
    )
    v2.main()


if __name__ == "__main__":
    main()


