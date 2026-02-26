# -*- coding: utf-8 -*-
"""
EQIDV2 LIVE Scanner V4 SHORT (short-only split pipeline)
=========================================================

Wrapper over:
    eqidv2_live_combined_analyser_csv_v2.py

Goals:
1. Keep base v2 untouched.
2. Emit only SHORT signals.
3. Preserve v2-style immediate CSV flush behavior for SHORT.
4. Isolate all outputs/state with `v4_short` suffix.
"""

from __future__ import annotations

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


def _is_short_side(side_value: Any) -> bool:
    return str(side_value or "").strip().upper() == "SHORT"


def _filter_short_signals_df(signals_df: pd.DataFrame) -> pd.DataFrame:
    if signals_df is None or signals_df.empty:
        return pd.DataFrame()
    if "side" not in signals_df.columns:
        return pd.DataFrame()
    mask = signals_df["side"].astype(str).str.upper().eq("SHORT")
    return signals_df.loc[mask].copy()


def _latest_entry_signals_for_ticker_v4_short(
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


def _write_signals_csv_v4_short(signals_df: pd.DataFrame) -> int:
    """
    CSV bridge for v4_short:
    - Keep only SHORT rows.
    - Reuse base v2 writer.
    """
    df_short = _filter_short_signals_df(signals_df)
    if df_short.empty:
        print("[V4_SHORT CSV] scanned=0 short_written=0", flush=True)
        return 0

    written = int(_ORIG_WRITE_SIGNALS_CSV(df_short))
    print(
        f"[V4_SHORT CSV] scanned={0 if signals_df is None else len(signals_df)} "
        f"| short_rows={len(df_short)} | short_written={written}",
        flush=True,
    )
    return written


def _apply_v4_short_overrides() -> None:
    """Patch v2 module-level config/functions to isolate v4_short behavior."""
    v2.REPORTS_DIR = ROOT / "reports" / "eqidv2_reports_v4_short"
    v2.REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    v2.OUT_CHECKS_DIR = ROOT / "out_eqidv2_live_checks_15m_v4_short"
    v2.OUT_SIGNALS_DIR = ROOT / "out_eqidv2_live_signals_15m_v4_short"
    v2.OUT_CHECKS_DIR.mkdir(parents=True, exist_ok=True)
    v2.OUT_SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

    v2.STATE_FILE = ROOT / "logs" / "eqidv2_avwap_live_state_v11_v4_short.json"
    v2.SIGNAL_CSV_PATTERN = "signals_{}_v4_short.csv"
    v2.END_TIME = v2.dtime(14, 40)
    v2.SESSION_END = v2.dtime(14, 40, 0)

    # Keep v2-style per-ticker immediate flush for SHORT.
    v2.IMMEDIATE_SIGNAL_CSV_FLUSH = True

    # Keep Kite entry rebase logic available as in v2 defaults.
    v2.USE_KITE_LTP_FOR_SIGNAL_CSV = True

    # Install side filters.
    v2._latest_entry_signals_for_ticker = _latest_entry_signals_for_ticker_v4_short
    v2._write_signals_csv = _write_signals_csv_v4_short

    # Ensure per-scan parquet outputs get explicit `_v4_short` filename suffix.
    def _run_one_scan_v4_short(run_tag: str = "A"):
        checks_df, signals_df = _ORIG_RUN_ONE_SCAN(run_tag)

        def _rename_latest(folder: Path, prefix: str) -> None:
            candidates = sorted(
                folder.glob(f"{prefix}_*_{run_tag}.parquet"),
                key=lambda p: p.stat().st_mtime,
            )
            if not candidates:
                return
            src = candidates[-1]
            if src.stem.endswith("_v4_short"):
                return
            dst = src.with_name(src.stem + "_v4_short" + src.suffix)
            try:
                if dst.exists():
                    dst.unlink()
                src.rename(dst)
            except Exception:
                pass

        day_dir = datetime.now(v2.IST).strftime("%Y%m%d")
        _rename_latest(v2.OUT_CHECKS_DIR / day_dir, "checks")
        _rename_latest(v2.OUT_SIGNALS_DIR / day_dir, "signals")
        return checks_df, signals_df

    v2.run_one_scan = _run_one_scan_v4_short

    # Ensure replay default output filename carries `_v4_short`.
    def _run_replay_for_date_v4_short(date_str: str, out_csv: Optional[str] = None) -> pd.DataFrame:
        if out_csv is None:
            out_csv = str(v2.OUT_SIGNALS_DIR / f"replay_signals_{date_str}_v4_short.csv")
        return _ORIG_RUN_REPLAY_FOR_DATE(date_str, out_csv=out_csv)

    v2.run_replay_for_date = _run_replay_for_date_v4_short


def main() -> None:
    _apply_v4_short_overrides()
    print(
        "[V4_SHORT] SHORT-only split enabled | immediate_flush=True | "
        "signal_csv=signals_YYYY-MM-DD_v4_short.csv",
        flush=True,
    )
    v2.main()


if __name__ == "__main__":
    main()
