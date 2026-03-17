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

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import eqidv2_live_combined_analyser_csv_v15 as v2
import avwap_combined_runner_v15 as v15_runner
from eqidv2_runtime_paths import DATA_15M_DIR as RUNTIME_DATA_15M_DIR
from eqidv2_runtime_paths import LIVE_SIGNALS_DIR as RUNTIME_LIVE_SIGNALS_DIR
from eqidv2_runtime_paths import report_subdir, runtime_dir
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


STALE_ONLY_RETRY_ENABLED = _env_bool("EQIDV15_STALE_ONLY_RETRY", True)
SHORT_STOP_PCT = float(os.getenv("EQIDV15_SHORT_STOP_PCT", "0.0066"))      # 0.66%
SHORT_TARGET_PCT = float(os.getenv("EQIDV15_SHORT_TARGET_PCT", "0.011"))  # 1.10%

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
    if mode == "LONG_ONLY":
        return False

    if not bool(getattr(v15_runner, "NIFTY_RS_FILTER_ENABLED", False)):
        return True

    if mode != "BOTH":
        rs_thresh = float(v15_runner.NIFTY_RS_THRESHOLD_PCT)
        apply_rs = True
    elif bool(getattr(v15_runner, "NIFTY_RS_BOTH_MODE_ENABLED", False)):
        rs_thresh = float(v15_runner.NIFTY_RS_BOTH_MODE_THRESHOLD_PCT)
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

    # Ensure per-scan parquet outputs get explicit `_v15_short` filename suffix.
    def _run_one_scan_v15_short(run_tag: str = "A"):
        _refresh_v15_nifty_context()
        checks_df, signals_df = _ORIG_RUN_ONE_SCAN(run_tag)

        def _rename_latest(folder: Path, prefix: str, tag: str) -> None:
            candidates = sorted(
                folder.glob(f"{prefix}_*_{tag}.parquet"),
                key=lambda p: p.stat().st_mtime,
            )
            if not candidates:
                return
            src = candidates[-1]
            if src.stem.endswith("_v15_short"):
                return
            dst = src.with_name(src.stem + "_v15_short" + src.suffix)
            try:
                if dst.exists():
                    dst.unlink()
                src.rename(dst)
            except Exception:
                pass

        def _rename_for_tag(tag: str) -> None:
            day_dir = datetime.now(v2.IST).strftime("%Y%m%d")
            _rename_latest(v2.OUT_CHECKS_DIR / day_dir, "checks", tag)
            _rename_latest(v2.OUT_SIGNALS_DIR / day_dir, "signals", tag)

        _rename_for_tag(run_tag)

        if STALE_ONLY_RETRY_ENABLED:
            stale_tickers = _extract_stale_tickers(checks_df)
            if stale_tickers:
                retry_tag = f"{run_tag}R"
                print(
                    f"[V15_SHORT RETRY] stale_tickers={len(stale_tickers)} | "
                    f"rerun_subset_tag={retry_tag}",
                    flush=True,
                )
                print(
                    f"[V15_SHORT RETRY] stale_ticker_names={','.join(stale_tickers)}",
                    flush=True,
                )
                orig_list_tickers = v2.list_tickers_15m
                try:
                    v2.list_tickers_15m = lambda: stale_tickers
                    checks_retry, signals_retry = _ORIG_RUN_ONE_SCAN(retry_tag)
                finally:
                    v2.list_tickers_15m = orig_list_tickers

                _rename_for_tag(retry_tag)
                if checks_retry is not None and (not checks_retry.empty):
                    checks_df = pd.concat([checks_df, checks_retry], ignore_index=True)
                if signals_retry is not None and (not signals_retry.empty):
                    signals_df = pd.concat([signals_df, signals_retry], ignore_index=True)
                print(
                    f"[V15_SHORT RETRY] done | extra_checks={0 if checks_retry is None else len(checks_retry)} "
                    f"| extra_signals={0 if signals_retry is None else len(signals_retry)}",
                    flush=True,
                )

        return checks_df, signals_df

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


