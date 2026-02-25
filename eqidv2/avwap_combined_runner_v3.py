# -*- coding: utf-8 -*-
"""
avwap_combined_runner_v3.py
===========================

Read-only-safe, new runner that keeps existing runners untouched and applies a
LONG anti-chase entry model for backtesting:

- LONG entry model: limit retrace from signal price
- Entry can be skipped if retrace limit is not hit within a wait window
- LONG stop/target are rebuilt from executed entry price

Default mode is LONG-focused (`RUN_SHORT_SIDE=False`) to evaluate long logic
without changing any existing live/backtest scripts.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Ensure project root importability when run directly
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

from avwap_v11_refactored.avwap_common import (  # noqa: E402
    IST,
    compute_backtest_metrics,
    default_long_config,
    default_short_config,
    generate_backtest_charts,
    now_ist,
    print_metrics,
)
from avwap_combined_runner import (  # noqa: E402
    _Tee,
    _add_notional_pnl,
    _print_day_side_mix,
    _print_notional_pnl,
    _print_signal_entry_lag_summary,
    _resolve_5min_dir,
    _resolve_exits_5min,
    _run_side_parallel,
    _sort_trades_for_output,
    generate_enhanced_charts,
    read_5m_parquet,
)


# ===========================================================================
# v3 RUN CONFIG (new file only; does not affect existing runners)
# ===========================================================================
RUN_SHORT_SIDE = False
RUN_LONG_SIDE = True
MAX_WORKERS = 4

# LONG anti-chase configuration (from latest analysis sweep)
LONG_ENTRY_MODEL = "limit_retrace"  # options: limit_retrace, next_open_guard, signal_entry
LONG_LIMIT_WAIT_MIN = 60
LONG_LIMIT_OFFSET_PCT = -0.005      # buy at signal_entry * (1 - 0.5%)
LONG_CHASE_CAP_PCT = 0.003          # used only for next_open_guard

# LONG risk model (rebuilt from executed entry)
LONG_STOP_PCT_V3 = 0.006            # 0.60%
LONG_TARGET_PCT_V3 = 0.018          # 1.80%

# Optional LONG signal filters (None means disabled)
LONG_RSI_CAP: Optional[float] = None
LONG_ADX_MIN: Optional[float] = None
LONG_QUALITY_MIN: Optional[float] = None


def _to_ist_series(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    try:
        if getattr(dt.dt, "tz", None) is None:
            return dt.dt.tz_localize(IST)
        return dt.dt.tz_convert(IST)
    except Exception:
        return dt


def _to_ist_scalar(value: Any) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tzinfo is None:
        return ts.tz_localize(IST)
    return ts.tz_convert(IST)


def _pick_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _load_5m_for_ticker(
    ticker: str,
    dir_5m: Path,
    suffix_5m: str,
    engine: str,
    cache: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    if ticker in cache:
        return cache[ticker]

    found = None
    for pattern in [
        f"{ticker}{suffix_5m}",
        f"{ticker}.parquet",
        f"{ticker}_5min.parquet",
        f"{ticker}_stocks_indicators_5min.parquet",
    ]:
        p = dir_5m / pattern
        if p.exists():
            found = p
            break

    if found is None:
        cache[ticker] = pd.DataFrame()
        return cache[ticker]

    df_5m = read_5m_parquet(str(found), engine=engine)
    if df_5m.empty:
        cache[ticker] = df_5m
        return df_5m

    dt_col = _pick_col(df_5m, "datetime", "date", "DateTime", "timestamp", "Timestamp")
    if dt_col is None:
        cache[ticker] = pd.DataFrame()
        return cache[ticker]

    if dt_col != "datetime":
        df_5m = df_5m.rename(columns={dt_col: "datetime"})

    df_5m["datetime"] = _to_ist_series(df_5m["datetime"])
    df_5m = df_5m.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
    cache[ticker] = df_5m
    return df_5m


def _apply_long_entry_model_v3(
    long_df: pd.DataFrame,
    dir_5m: Path,
    suffix_5m: str = ".parquet",
    engine: str = "pyarrow",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Rebuild LONG entries using anti-chase logic.

    Returns:
      - adjusted long trades (only filled trades)
      - skipped rows report with reasons
    """
    if long_df.empty:
        return long_df, pd.DataFrame()

    if not dir_5m.is_dir():
        print(f"[V3][WARN] 5-min directory missing: {dir_5m}. LONG anti-chase skipped.")
        return long_df, pd.DataFrame()

    df = long_df.copy()
    if "stop_price" not in df.columns and "sl_price" in df.columns:
        df["stop_price"] = df["sl_price"]

    for c in ["signal_time_ist", "entry_time_ist", "exit_time_ist"]:
        if c in df.columns:
            df[c] = _to_ist_series(df[c])

    out_rows: List[Dict[str, Any]] = []
    skip_rows: List[Dict[str, Any]] = []
    cache_5m: Dict[str, pd.DataFrame] = {}

    for row in df.to_dict("records"):
        ticker = str(row.get("ticker", "")).upper()
        side = str(row.get("side", "")).upper()
        if side != "LONG":
            out_rows.append(row)
            continue

        signal_time = _to_ist_scalar(row.get("signal_time_ist"))
        signal_entry = float(pd.to_numeric(row.get("entry_price", np.nan), errors="coerce"))
        rsi_val = float(pd.to_numeric(row.get("rsi_signal", np.nan), errors="coerce"))
        adx_val = float(pd.to_numeric(row.get("adx_signal", np.nan), errors="coerce"))
        q_val = float(pd.to_numeric(row.get("quality_score", np.nan), errors="coerce"))

        if pd.isna(signal_time) or not np.isfinite(signal_entry) or signal_entry <= 0:
            skip_rows.append(
                {
                    "ticker": ticker,
                    "side": side,
                    "signal_time_ist": signal_time,
                    "signal_entry_price": signal_entry,
                    "reason": "invalid_signal_time_or_entry",
                }
            )
            continue

        if LONG_RSI_CAP is not None and np.isfinite(rsi_val) and rsi_val > LONG_RSI_CAP:
            skip_rows.append(
                {
                    "ticker": ticker,
                    "side": side,
                    "signal_time_ist": signal_time,
                    "signal_entry_price": signal_entry,
                    "rsi_signal": rsi_val,
                    "reason": f"rsi_cap_exceeded>{LONG_RSI_CAP}",
                }
            )
            continue

        if LONG_ADX_MIN is not None and np.isfinite(adx_val) and adx_val < LONG_ADX_MIN:
            skip_rows.append(
                {
                    "ticker": ticker,
                    "side": side,
                    "signal_time_ist": signal_time,
                    "signal_entry_price": signal_entry,
                    "adx_signal": adx_val,
                    "reason": f"adx_below_min<{LONG_ADX_MIN}",
                }
            )
            continue

        if LONG_QUALITY_MIN is not None and np.isfinite(q_val) and q_val < LONG_QUALITY_MIN:
            skip_rows.append(
                {
                    "ticker": ticker,
                    "side": side,
                    "signal_time_ist": signal_time,
                    "signal_entry_price": signal_entry,
                    "quality_score": q_val,
                    "reason": f"quality_below_min<{LONG_QUALITY_MIN}",
                }
            )
            continue

        bars = _load_5m_for_ticker(ticker, dir_5m, suffix_5m, engine, cache_5m)
        if bars.empty:
            skip_rows.append(
                {
                    "ticker": ticker,
                    "side": side,
                    "signal_time_ist": signal_time,
                    "signal_entry_price": signal_entry,
                    "reason": "no_5m_data",
                }
            )
            continue

        high_col = _pick_col(bars, "high", "High")
        low_col = _pick_col(bars, "low", "Low")
        open_col = _pick_col(bars, "open", "Open")
        if low_col is None or high_col is None or open_col is None:
            skip_rows.append(
                {
                    "ticker": ticker,
                    "side": side,
                    "signal_time_ist": signal_time,
                    "signal_entry_price": signal_entry,
                    "reason": "missing_ohlc_cols_5m",
                }
            )
            continue

        day_norm = signal_time.normalize()
        bars_after = bars[
            (bars["datetime"] > signal_time)
            & (bars["datetime"].dt.normalize() == day_norm)
        ].copy()
        if bars_after.empty:
            skip_rows.append(
                {
                    "ticker": ticker,
                    "side": side,
                    "signal_time_ist": signal_time,
                    "signal_entry_price": signal_entry,
                    "reason": "no_5m_bars_after_signal",
                }
            )
            continue

        entry_time = pd.NaT
        entry_price = np.nan
        reason = ""

        if LONG_ENTRY_MODEL == "signal_entry":
            entry_time = signal_time
            entry_price = round(signal_entry, 2)
            reason = "signal_entry"
        elif LONG_ENTRY_MODEL == "next_open_guard":
            first_bar = bars_after.iloc[0]
            next_open = float(first_bar[open_col])
            if next_open > signal_entry * (1 + float(LONG_CHASE_CAP_PCT)):
                skip_rows.append(
                    {
                        "ticker": ticker,
                        "side": side,
                        "signal_time_ist": signal_time,
                        "signal_entry_price": signal_entry,
                        "next_open": next_open,
                        "reason": f"chase_cap_exceeded>{LONG_CHASE_CAP_PCT:.4f}",
                    }
                )
                continue
            entry_time = first_bar["datetime"]
            entry_price = round(next_open, 2)
            reason = "next_open_guard"
        elif LONG_ENTRY_MODEL == "limit_retrace":
            limit_px = round(signal_entry * (1 + float(LONG_LIMIT_OFFSET_PCT)), 2)
            cutoff = signal_time + pd.Timedelta(minutes=int(LONG_LIMIT_WAIT_MIN))
            cand = bars_after[bars_after["datetime"] <= cutoff]
            hit = cand[pd.to_numeric(cand[low_col], errors="coerce") <= limit_px]
            if hit.empty:
                skip_rows.append(
                    {
                        "ticker": ticker,
                        "side": side,
                        "signal_time_ist": signal_time,
                        "signal_entry_price": signal_entry,
                        "limit_price": limit_px,
                        "wait_min": LONG_LIMIT_WAIT_MIN,
                        "reason": "limit_not_hit_in_window",
                    }
                )
                continue
            first_hit = hit.iloc[0]
            entry_time = first_hit["datetime"]
            entry_price = float(limit_px)
            reason = "limit_retrace"
        else:
            raise ValueError(f"Unsupported LONG_ENTRY_MODEL={LONG_ENTRY_MODEL}")

        if pd.isna(entry_time) or not np.isfinite(entry_price) or entry_price <= 0:
            skip_rows.append(
                {
                    "ticker": ticker,
                    "side": side,
                    "signal_time_ist": signal_time,
                    "signal_entry_price": signal_entry,
                    "reason": "entry_not_resolved",
                }
            )
            continue

        stop_price = round(entry_price * (1 - float(LONG_STOP_PCT_V3)), 2)
        target_price = round(entry_price * (1 + float(LONG_TARGET_PCT_V3)), 2)

        row["entry_time_ist"] = entry_time
        row["entry_price"] = float(entry_price)
        row["stop_price"] = float(stop_price)
        row["sl_price"] = float(stop_price)
        row["target_price"] = float(target_price)
        row["entry_model_v3"] = reason
        row["entry_vs_signal_pct_v3"] = float((entry_price / signal_entry - 1.0) * 100.0)
        out_rows.append(row)

    out_df = pd.DataFrame(out_rows)
    skip_df = pd.DataFrame(skip_rows)
    print(
        f"[V3][LONG ENTRY] model={LONG_ENTRY_MODEL} | input={len(df)} | "
        f"kept={len(out_df)} | skipped={len(skip_df)}"
    )
    return out_df, skip_df


def main() -> None:
    _outputs_dir = _THIS_DIR / "outputs_v3"
    _outputs_dir.mkdir(parents=True, exist_ok=True)
    _logs_dir = _THIS_DIR / "logs"
    _logs_dir.mkdir(parents=True, exist_ok=True)

    ts = now_ist().strftime("%Y%m%d_%H%M%S")
    log_path = _outputs_dir / f"avwap_combined_runner_v3_{ts}.txt"

    _orig_stdout, _orig_stderr = sys.stdout, sys.stderr
    with open(log_path, "w", encoding="utf-8") as _log_fh:
        sys.stdout = _Tee(_orig_stdout, _log_fh)
        sys.stderr = _Tee(_orig_stderr, _log_fh)

        try:
            print("=" * 72)
            print("AVWAP COMBINED RUNNER V3 (new file)")
            print(f"[INFO] RUN_SHORT_SIDE={RUN_SHORT_SIDE} | RUN_LONG_SIDE={RUN_LONG_SIDE}")
            print(
                "[INFO] LONG anti-chase: "
                f"model={LONG_ENTRY_MODEL}, wait={LONG_LIMIT_WAIT_MIN}m, "
                f"offset={LONG_LIMIT_OFFSET_PCT*100:.2f}%, chase_cap={LONG_CHASE_CAP_PCT*100:.2f}%"
            )
            print(
                "[INFO] LONG risk: "
                f"SL={LONG_STOP_PCT_V3*100:.2f}% | TGT={LONG_TARGET_PCT_V3*100:.2f}%"
            )
            print(f"[INFO] Output directory: {_outputs_dir}")
            print("=" * 72)

            dir_5m = _resolve_5min_dir()
            print(f"[INFO] 5-min data directory: {dir_5m}")
            if dir_5m.is_dir():
                print(f"[INFO] 5-min parquet files found: {len(list(dir_5m.glob('*.parquet')))}")
            else:
                print("[WARN] 5-min directory missing; exits remain at 15-min resolution.")

            short_cfg = default_short_config(reports_dir=_outputs_dir)
            long_cfg = default_long_config(
                reports_dir=_outputs_dir,
                stop_pct=LONG_STOP_PCT_V3,
                target_pct=LONG_TARGET_PCT_V3,
            )
            short_cfg.enable_topn_per_day = False
            long_cfg.enable_topn_per_day = False

            short_df = pd.DataFrame()
            long_df = pd.DataFrame()
            skip_df = pd.DataFrame()

            if RUN_SHORT_SIDE:
                print("\n[PHASE 1] Scanning SHORT signals...")
                short_df = _run_side_parallel("SHORT", short_cfg, max_workers=MAX_WORKERS)
            else:
                print("\n[PHASE 1] SHORT scan disabled.")

            if RUN_LONG_SIDE:
                print("\n[PHASE 1] Scanning LONG signals...")
                long_df = _run_side_parallel("LONG", long_cfg, max_workers=MAX_WORKERS)
            else:
                print("\n[PHASE 1] LONG scan disabled.")

            if short_df.empty and long_df.empty:
                print("[DONE] No trades found from scans.")
                return

            suffix_5m = ".parquet"

            if RUN_LONG_SIDE and not long_df.empty:
                print("\n[PHASE 2] Applying LONG anti-chase entry model...")
                long_df, skip_df = _apply_long_entry_model_v3(
                    long_df=long_df,
                    dir_5m=dir_5m,
                    suffix_5m=suffix_5m,
                    engine=long_cfg.parquet_engine,
                )

            print("\n[PHASE 3] Re-resolving exits on 5-min bars...")
            if not short_df.empty:
                short_df = _resolve_exits_5min(
                    short_df, dir_5m, suffix_5m=suffix_5m, engine=short_cfg.parquet_engine
                )
            if not long_df.empty:
                long_df = _resolve_exits_5min(
                    long_df, dir_5m, suffix_5m=suffix_5m, engine=long_cfg.parquet_engine
                )

            if not short_df.empty:
                short_df = _add_notional_pnl(short_df)
                short_df = _sort_trades_for_output(short_df)
            if not long_df.empty:
                long_df = _add_notional_pnl(long_df)
                long_df = _sort_trades_for_output(long_df)

            combined = pd.concat([short_df, long_df], ignore_index=True)
            if combined.empty:
                print("[DONE] No trades after LONG anti-chase filtering.")
                if not skip_df.empty:
                    skip_csv = _outputs_dir / f"avwap_long_entry_skips_v3_{ts}.csv"
                    skip_df.to_csv(skip_csv, index=False)
                    print(f"[FILE SAVED] {skip_csv}")
                return

            combined = _add_notional_pnl(combined)
            combined = _sort_trades_for_output(combined)
            _print_day_side_mix(combined)
            _print_signal_entry_lag_summary(combined)

            if not short_df.empty:
                print_metrics("SHORT (5-min exits)", compute_backtest_metrics(short_df))
            else:
                print("[INFO] SHORT metrics skipped (no short trades).")

            if not long_df.empty:
                print_metrics("LONG (5-min exits, anti-chase v3)", compute_backtest_metrics(long_df))
            else:
                print("[INFO] LONG metrics skipped (no long trades).")

            print_metrics("COMBINED (5-min exits)", compute_backtest_metrics(combined))
            _print_notional_pnl(combined)

            out_csv = _outputs_dir / f"avwap_longshort_trades_ALL_DAYS_v3_{ts}.csv"
            combined.to_csv(out_csv, index=False)
            print(f"[FILE SAVED] {out_csv}")

            if not long_df.empty:
                out_long = _outputs_dir / f"avwap_long_trades_only_v3_{ts}.csv"
                long_df.to_csv(out_long, index=False)
                print(f"[FILE SAVED] {out_long}")

            if not skip_df.empty:
                skip_csv = _outputs_dir / f"avwap_long_entry_skips_v3_{ts}.csv"
                skip_df.to_csv(skip_csv, index=False)
                print(f"[FILE SAVED] {skip_csv}")

            print("\n[INFO] Generating charts...")
            chart_dir_legacy = _outputs_dir / "charts_v3" / "legacy"
            chart_dir_enhanced = _outputs_dir / "charts_v3" / "enhanced"
            chart_files_legacy = generate_backtest_charts(
                combined,
                short_df,
                long_df,
                save_dir=chart_dir_legacy,
                ts_label=f"{ts}_v3",
            )
            chart_files_enhanced = generate_enhanced_charts(
                combined,
                short_df,
                long_df,
                save_dir=chart_dir_enhanced,
                ts_label=f"{ts}_v3",
            )
            print(
                f"[INFO] Charts generated: legacy={len(chart_files_legacy or [])}, "
                f"enhanced={len(chart_files_enhanced or [])}"
            )
            print("[DONE]")

        finally:
            sys.stdout = _orig_stdout
            sys.stderr = _orig_stderr

    print(f"[LOG SAVED] {log_path}")


if __name__ == "__main__":
    main()

