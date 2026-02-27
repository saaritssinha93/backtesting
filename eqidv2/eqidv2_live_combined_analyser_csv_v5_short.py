# -*- coding: utf-8 -*-
# Backup reference (2026-02-26):
# - c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\backups_codex\20260226_180142\eqidv2_eod_scheduler_for_15mins_data.py
# - c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\backups_codex\20260226_180142\run_eqidv2_eod_scheduler_for_15mins_data.bat
"""
EQIDV2 LIVE Scanner V5 SHORT (short-only split pipeline)
=========================================================

Wrapper over:
    eqidv2_live_combined_analyser_csv_v2.py

Goals:
1. Keep base v2 untouched.
2. Emit only SHORT signals.
3. Preserve v2-style immediate CSV flush behavior for SHORT.
4. Isolate all outputs/state with `v5_short` suffix.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

import eqidv2_live_combined_analyser_csv_v2 as v2


ROOT = Path(__file__).resolve().parent


# Keep original functions so wrapper can delegate safely.
_ORIG_WRITE_SIGNALS_CSV = v2._write_signals_csv
_ORIG_LATEST_ENTRY_SIGNALS_FOR_TICKER = v2._latest_entry_signals_for_ticker
_ORIG_RUN_ONE_SCAN = v2.run_one_scan
_ORIG_RUN_REPLAY_FOR_DATE = v2.run_replay_for_date
_ORIG_SCAN_SHORT_ONE_DAY = v2.scan_short_one_day
_ORIG_SCAN_LONG_ONE_DAY = v2.scan_long_one_day


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


STALE_ONLY_RETRY_ENABLED = _env_bool("EQIDV5_STALE_ONLY_RETRY", True)


def _is_short_side(side_value: Any) -> bool:
    return str(side_value or "").strip().upper() == "SHORT"


def _filter_short_signals_df(signals_df: pd.DataFrame) -> pd.DataFrame:
    if signals_df is None or signals_df.empty:
        return pd.DataFrame()
    if "side" not in signals_df.columns:
        return pd.DataFrame()
    mask = signals_df["side"].astype(str).str.upper().eq("SHORT")
    return signals_df.loc[mask].copy()


def _latest_entry_signals_for_ticker_v5_short(
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

    signals_short = [s for s in (signals or []) if _is_short_side(getattr(s, "side", ""))]
    checks_short = [c for c in (checks or []) if _is_short_side(c.get("side", ""))]
    return signals_short, checks_short


def _write_signals_csv_v5_short(signals_df: pd.DataFrame) -> int:
    """
    CSV bridge for v5_short:
    - Keep only SHORT rows.
    - Reuse base v2 writer.
    """
    df_short = _filter_short_signals_df(signals_df)
    if df_short.empty:
        print("[V5_SHORT CSV] scanned=0 short_written=0", flush=True)
        return 0

    written = int(_ORIG_WRITE_SIGNALS_CSV(df_short))
    print(
        f"[V5_SHORT CSV] scanned={0 if signals_df is None else len(signals_df)} "
        f"| short_rows={len(df_short)} | short_written={written}",
        flush=True,
    )
    return written


def _scan_long_disabled(*_args, **_kwargs):
    """v5_short must not compute LONG side internals."""
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


def _apply_v5_short_overrides() -> None:
    """Patch v2 module-level config/functions to isolate v5_short behavior."""
    v2.REPORTS_DIR = ROOT / "reports" / "eqidv2_reports_v5_short"
    v2.REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    v2.OUT_CHECKS_DIR = ROOT / "out_eqidv2_live_checks_15m_v5_short"
    v2.OUT_SIGNALS_DIR = ROOT / "out_eqidv2_live_signals_15m_v5_short"
    v2.OUT_CHECKS_DIR.mkdir(parents=True, exist_ok=True)
    v2.OUT_SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

    v2.STATE_FILE = ROOT / "logs" / "eqidv2_avwap_live_state_v11_v5_short.json"
    v2.SIGNAL_CSV_PATTERN = "signals_{}_v5_short.csv"
    v2.END_TIME = v2.dtime(14, 40)
    v2.SESSION_END = v2.dtime(14, 40, 0)

    # Keep v2-style per-ticker immediate flush for SHORT.
    v2.IMMEDIATE_SIGNAL_CSV_FLUSH = True

    # Keep Kite entry rebase logic available as in v2 defaults.
    v2.USE_KITE_LTP_FOR_SIGNAL_CSV = True

    # Compute only SHORT strategy internals in this process.
    v2.scan_short_one_day = _ORIG_SCAN_SHORT_ONE_DAY
    v2.scan_long_one_day = _scan_long_disabled

    # Install side filters.
    v2._latest_entry_signals_for_ticker = _latest_entry_signals_for_ticker_v5_short
    v2._write_signals_csv = _write_signals_csv_v5_short

    # Ensure per-scan parquet outputs get explicit `_v5_short` filename suffix.
    def _run_one_scan_v5_short(run_tag: str = "A"):
        checks_df, signals_df = _ORIG_RUN_ONE_SCAN(run_tag)

        def _rename_latest(folder: Path, prefix: str, tag: str) -> None:
            candidates = sorted(
                folder.glob(f"{prefix}_*_{tag}.parquet"),
                key=lambda p: p.stat().st_mtime,
            )
            if not candidates:
                return
            src = candidates[-1]
            if src.stem.endswith("_v5_short"):
                return
            dst = src.with_name(src.stem + "_v5_short" + src.suffix)
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
                    f"[V5_SHORT RETRY] stale_tickers={len(stale_tickers)} | "
                    f"rerun_subset_tag={retry_tag}",
                    flush=True,
                )
                print(
                    f"[V5_SHORT RETRY] stale_ticker_names={','.join(stale_tickers)}",
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
                    f"[V5_SHORT RETRY] done | extra_checks={0 if checks_retry is None else len(checks_retry)} "
                    f"| extra_signals={0 if signals_retry is None else len(signals_retry)}",
                    flush=True,
                )

        return checks_df, signals_df

    v2.run_one_scan = _run_one_scan_v5_short

    # Ensure replay default output filename carries `_v5_short`.
    def _run_replay_for_date_v5_short(date_str: str, out_csv: Optional[str] = None) -> pd.DataFrame:
        if out_csv is None:
            out_csv = str(v2.OUT_SIGNALS_DIR / f"replay_signals_{date_str}_v5_short.csv")
        return _ORIG_RUN_REPLAY_FOR_DATE(date_str, out_csv=out_csv)

    v2.run_replay_for_date = _run_replay_for_date_v5_short


def main() -> None:
    _apply_v5_short_overrides()
    print(
        "[V5_SHORT] SHORT-only split enabled | immediate_flush=True | "
        f"stale_only_retry={STALE_ONLY_RETRY_ENABLED} | "
        "signal_csv=signals_YYYY-MM-DD_v5_short.csv",
        flush=True,
    )
    v2.main()


if __name__ == "__main__":
    main()

