# -*- coding: utf-8 -*-
# Backup reference (2026-02-26):
# - c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\backups_codex\20260226_180142\eqidv2_eod_scheduler_for_15mins_data.py
# - c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\backups_codex\20260226_180142\run_eqidv2_eod_scheduler_for_15mins_data.bat
"""
EQIDV2 LIVE Scanner V15 LONG (long-only aligned split pipeline)
===============================================================

Wrapper over:
    eqidv2_live_combined_analyser_csv_v15.py

Goals:
1. Keep shared base files untouched.
2. Emit only LONG signals.
3. Preserve immediate CSV flush behavior so live timing tracks v15 backtest logic.
4. Isolate all outputs/state with `v15_long` suffix.
"""

from __future__ import annotations

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
)
from avwap_v11_refactored.avwap_common_v7_sweep_v15 import (
    default_long_config as v15_default_long_config,
    in_session as v15_in_session,
    prepare_indicators as v15_prepare_indicators,
    compute_day_avwap as v15_compute_day_avwap,
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


STALE_ONLY_RETRY_ENABLED = _env_bool("EQIDV15_STALE_ONLY_RETRY", True)
STALE_RETRY_MAX_TICKERS = _env_int("EQIDV15_STALE_RETRY_MAX_TICKERS", 6, min_value=0)
STALE_RETRY_MAX_RATIO = _env_float("EQIDV15_STALE_RETRY_MAX_RATIO", 0.08, min_value=0.0)
LONG_STOP_PCT = float(os.getenv("EQIDV15_LONG_STOP_PCT", "0.0075"))
LONG_TARGET_PCT = float(os.getenv("EQIDV15_LONG_TARGET_PCT", "0.0090"))

LONG_RSI_CAP_RAW = os.getenv("EQIDV15_LONG_RSI_CAP", "").strip()
LONG_ADX_MIN_RAW = os.getenv("EQIDV15_LONG_ADX_MIN", "").strip()
LONG_QUALITY_MIN_RAW = os.getenv("EQIDV15_LONG_QUALITY_MIN", "").strip()

LONG_RSI_CAP: Optional[float] = float(LONG_RSI_CAP_RAW) if LONG_RSI_CAP_RAW else None
LONG_ADX_MIN: Optional[float] = float(LONG_ADX_MIN_RAW) if LONG_ADX_MIN_RAW else None
LONG_QUALITY_MIN: Optional[float] = float(LONG_QUALITY_MIN_RAW) if LONG_QUALITY_MIN_RAW else None
SHARED_SCAN_WAIT_SECONDS = _env_int("EQIDV15_SHARED_SCAN_WAIT_SECONDS", 90, min_value=5)
SHARED_SCAN_WAIT_POLL_SECONDS = _env_float("EQIDV15_SHARED_SCAN_WAIT_POLL_SECONDS", 0.5, min_value=0.05)
SHARD_PAIR_ID = (
    os.getenv("EQIDV15_LONG_SHARD_ID")
    or os.getenv("EQIDV15_SHORT_SHARD_ID")
    or "00"
).strip() or "00"
LONG_SHARED_FALLBACK_LOCAL_SCAN = _env_bool("EQIDV15_LONG_SHARED_FALLBACK_LOCAL_SCAN", True)

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

    cfg = v15_default_short_config()
    cfg.dir_15m = str(v15_runner._resolve_15m_dir())
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
    if mode == "SHORT_ONLY":
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
                "NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT",
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
    return rel_val >= rs_thresh


def _safe_float(x: Any, default: float = np.nan) -> float:
    try:
        out = float(x)
        if np.isfinite(out):
            return out
        return float(default)
    except Exception:
        return float(default)


def _passes_v15_long_signal_quality(signal: Any) -> bool:
    diag = dict(getattr(signal, "diagnostics", {}) or {})
    rsi = _safe_float(diag.get("rsi", np.nan))
    adx = _safe_float(diag.get("adx", np.nan))
    score = _safe_float(getattr(signal, "score", diag.get("quality_score", np.nan)))

    if LONG_RSI_CAP is not None and np.isfinite(rsi) and rsi > LONG_RSI_CAP:
        return False
    if LONG_ADX_MIN is not None and np.isfinite(adx) and adx < LONG_ADX_MIN:
        return False
    if LONG_QUALITY_MIN is not None and np.isfinite(score) and score < LONG_QUALITY_MIN:
        return False
    return True


def _is_long_side(side_value: Any) -> bool:
    return str(side_value or "").strip().upper() == "LONG"


def _filter_long_signals_df(signals_df: pd.DataFrame) -> pd.DataFrame:
    if signals_df is None or signals_df.empty:
        return pd.DataFrame()
    if "side" not in signals_df.columns:
        return pd.DataFrame()
    mask = signals_df["side"].astype(str).str.upper().eq("LONG")
    return signals_df.loc[mask].copy()


def _filter_long_checks_df(checks_df: pd.DataFrame) -> pd.DataFrame:
    if checks_df is None or checks_df.empty:
        return pd.DataFrame()
    if "side" not in checks_df.columns:
        return checks_df.copy()
    mask = checks_df["side"].astype(str).str.upper().eq("LONG")
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


def _filter_long_signals_post(signals_df: pd.DataFrame) -> pd.DataFrame:
    df_long = _filter_long_signals_df(signals_df)
    if df_long.empty:
        return df_long

    keep_mask = []
    for _, row in df_long.iterrows():
        signal_obj = _signal_obj_from_row(row)
        keep_mask.append(
            bool(_passes_v15_nifty_context(signal_obj) and _passes_v15_long_signal_quality(signal_obj))
        )
    return df_long.loc[pd.Series(keep_mask, index=df_long.index)].copy()


def _save_long_outputs(run_tag: str, checks_df: pd.DataFrame, signals_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    checks_long = _filter_long_checks_df(checks_df)
    signals_long = _filter_long_signals_post(signals_df)
    v2._save_run_parquets(
        checks_long,
        signals_long,
        run_tag,
        checks_dir=v2.OUT_CHECKS_DIR,
        signals_dir=v2.OUT_SIGNALS_DIR,
        suffix="v15_long",
    )
    _write_signals_csv_v15_long(signals_long)
    return checks_long, signals_long


def _latest_entry_signals_for_ticker_v15_long(
    ticker: str,
    df_raw: pd.DataFrame,
    state: Dict[str, Any],
    target_slot_ist: pd.Timestamp,
) -> Tuple[List[Any], List[Dict[str, Any]]]:
    """
    Delegate to base detector, then keep only LONG outputs.
    """
    signals, checks = _ORIG_LATEST_ENTRY_SIGNALS_FOR_TICKER(
        ticker=ticker,
        df_raw=df_raw,
        state=state,
        target_slot_ist=target_slot_ist,
    )

    signals_long = [
        s for s in (signals or [])
        if _is_long_side(getattr(s, "side", ""))
        and _passes_v15_nifty_context(s)
        and _passes_v15_long_signal_quality(s)
    ]
    checks_long = [c for c in (checks or []) if _is_long_side(c.get("side", ""))]
    return signals_long, checks_long


def _write_signals_csv_v15_long(signals_df: pd.DataFrame) -> int:
    """
    CSV bridge for v15_long:
    - Keep only LONG rows.
    - Reuse base v2 writer with Kite rebase disabled for model-price parity.
    """
    df_long = _filter_long_signals_df(signals_df)
    scanned = 0 if signals_df is None else len(signals_df)
    if df_long.empty:
        print(f"[V15_LONG CSV] scanned={scanned} | long_rows=0 | long_written=0", flush=True)
        return 0

    written = int(_ORIG_WRITE_SIGNALS_CSV(df_long))
    print(
        f"[V15_LONG CSV] scanned={scanned} | long_rows={len(df_long)} | long_written={written}",
        flush=True,
    )
    return written


def _scan_short_disabled(*_args, **_kwargs):
    """v15_long must not compute SHORT side internals."""
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
            f"[V15_LONG RETRY] skip | stale_tickers={stale_count}/{total_tickers} "
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


def _apply_v15_long_overrides() -> None:
    """Patch v2 module-level config/functions to isolate v15_long behavior."""
    _apply_v15_strategy_engine_overrides()

    v2.DIR_15M = str(RUNTIME_DATA_15M_DIR)
    v2.LIVE_SIGNAL_DIR = RUNTIME_LIVE_SIGNALS_DIR
    v2.LIVE_SIGNAL_DIR.mkdir(parents=True, exist_ok=True)

    v2.REPORTS_DIR = report_subdir("eqidv2_reports_v15_long")
    v2.REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    v2.OUT_CHECKS_DIR = runtime_dir("out_eqidv2_live_checks_15m_v15_long")
    v2.OUT_SIGNALS_DIR = runtime_dir("out_eqidv2_live_signals_15m_v15_long")
    v2.OUT_CHECKS_DIR.mkdir(parents=True, exist_ok=True)
    v2.OUT_SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

    v2.STATE_FILE = ROOT / "logs" / "eqidv2_avwap_live_state_v11_v15_long.json"
    v2.SIGNAL_CSV_PATTERN = "signals_{}_v15_long.csv"
    # Keep scanning through the 14:45 slot, but let the strategy's own
    # long signal windows suppress any new long entries after 14:30.
    v2.END_TIME = v2.dtime(15, 0)
    v2.SESSION_END = v2.dtime(15, 30, 0)

    # Emit v15 LONG signals immediately instead of parking them in a pending queue.
    v2.IMMEDIATE_SIGNAL_CSV_FLUSH = True

    # Keep model entry/SL/TGT intact in the signal CSV for backtest/live parity.
    v2.USE_KITE_LTP_FOR_SIGNAL_CSV = False
    v2.LONG_STOP_PCT = float(LONG_STOP_PCT)
    v2.LONG_TARGET_PCT = float(LONG_TARGET_PCT)

    # Compute only LONG strategy internals in this process.
    v2.scan_short_one_day = _scan_short_disabled
    v2.scan_long_one_day = v15_scan_long_one_day

    v2._latest_entry_signals_for_ticker = _latest_entry_signals_for_ticker_v15_long
    v2._write_signals_csv = _write_signals_csv_v15_long

    def _run_one_scan_v15_long_local(run_tag: str = "A"):
        orig_write_run_parquets = getattr(v2, "WRITE_RUN_PARQUETS", True)
        try:
            v2.WRITE_RUN_PARQUETS = False
            checks_df, signals_df = _ORIG_RUN_ONE_SCAN(run_tag)
        finally:
            v2.WRITE_RUN_PARQUETS = orig_write_run_parquets

        if STALE_ONLY_RETRY_ENABLED:
            stale_tickers = _extract_stale_tickers(checks_df)
            if _should_retry_stale_subset(stale_tickers):
                retry_tag = f"{run_tag}R"
                print(
                    f"[V15_LONG RETRY] stale_tickers={len(stale_tickers)} | rerun_subset_tag={retry_tag}",
                    flush=True,
                )
                print(
                    f"[V15_LONG RETRY] stale_ticker_names={','.join(stale_tickers)}",
                    flush=True,
                )
                orig_list_tickers = v2.list_tickers_15m
                try:
                    v2.list_tickers_15m = lambda: stale_tickers
                    v2.WRITE_RUN_PARQUETS = False
                    checks_retry, signals_retry = _ORIG_RUN_ONE_SCAN(retry_tag)
                finally:
                    v2.list_tickers_15m = orig_list_tickers
                    v2.WRITE_RUN_PARQUETS = orig_write_run_parquets

                if checks_retry is not None and (not checks_retry.empty):
                    checks_df = pd.concat([checks_df, checks_retry], ignore_index=True)
                if signals_retry is not None and (not signals_retry.empty):
                    signals_df = pd.concat([signals_df, signals_retry], ignore_index=True)
                print(
                    f"[V15_LONG RETRY] done | extra_checks={0 if checks_retry is None else len(checks_retry)} "
                    f"| extra_signals={0 if signals_retry is None else len(signals_retry)}",
                    flush=True,
                )

        return _save_long_outputs(run_tag, checks_df, signals_df)

    def _run_one_scan_v15_long(run_tag: str = "A"):
        _refresh_v15_nifty_context()
        slot_ts = v2._current_15m_slot_start_ist()
        bundle = shared_scan_cache.wait_for_bundle(
            shard_id=SHARD_PAIR_ID,
            slot_ts=slot_ts,
            run_tag=run_tag,
            timeout_sec=SHARED_SCAN_WAIT_SECONDS,
            poll_sec=SHARED_SCAN_WAIT_POLL_SECONDS,
        )
        if bundle is None:
            print(
                f"[V15_LONG SHARED] cache_miss slot={pd.Timestamp(slot_ts).strftime('%Y%m%d_%H%M')} "
                f"scan={run_tag} | fallback_local={LONG_SHARED_FALLBACK_LOCAL_SCAN}",
                flush=True,
            )
            if LONG_SHARED_FALLBACK_LOCAL_SCAN:
                return _run_one_scan_v15_long_local(run_tag)
            return _save_long_outputs(run_tag, pd.DataFrame(), pd.DataFrame())

        raw_checks_df, raw_signals_df, meta = bundle
        print(
            f"[V15_LONG SHARED] consumed slot={meta.get('slot', '')} scan={run_tag} "
            f"producer={meta.get('producer', '')} raw_checks={len(raw_checks_df)} raw_signals={len(raw_signals_df)}",
            flush=True,
        )
        return _save_long_outputs(run_tag, raw_checks_df, raw_signals_df)

    v2.run_one_scan = _run_one_scan_v15_long

    def _run_replay_for_date_v15_long(date_str: str, out_csv: Optional[str] = None) -> pd.DataFrame:
        if out_csv is None:
            out_csv = str(v2.OUT_SIGNALS_DIR / f"replay_signals_{date_str}_v15_long.csv")
        return _ORIG_RUN_REPLAY_FOR_DATE(date_str, out_csv=out_csv)

    v2.run_replay_for_date = _run_replay_for_date_v15_long


def main() -> None:
    _apply_v15_long_overrides()
    _refresh_v15_nifty_context()
    print(
        "[V15_LONG] LONG-only aligned mode | immediate_flush=True | "
        f"long_stop={LONG_STOP_PCT*100:.2f}% | "
        f"long_target={LONG_TARGET_PCT*100:.2f}% | "
        f"stale_only_retry={STALE_ONLY_RETRY_ENABLED} | "
        f"nifty_context={'on' if bool(getattr(v15_runner, 'NIFTY_CONTEXT_ENABLED', False)) else 'off'} | "
        f"nifty_source={_NIFTY_CONTEXT_SOURCE or 'NA'} | "
        "signal_csv=signals_YYYY-MM-DD_v15_long.csv",
        flush=True,
    )
    if LONG_RSI_CAP is not None:
        print(f"[V15_LONG] LONG_RSI_CAP={LONG_RSI_CAP}", flush=True)
    if LONG_ADX_MIN is not None:
        print(f"[V15_LONG] LONG_ADX_MIN={LONG_ADX_MIN}", flush=True)
    if LONG_QUALITY_MIN is not None:
        print(f"[V15_LONG] LONG_QUALITY_MIN={LONG_QUALITY_MIN}", flush=True)
    v2.main()


if __name__ == "__main__":
    main()
