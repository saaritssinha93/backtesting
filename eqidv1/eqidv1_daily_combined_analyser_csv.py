# -*- coding: utf-8 -*-
"""
eqidv1_daily_combined_analyser_csv.py
===================================

Daily variant of eqidv1_live_combined_analyser_csv.py:
- scans ALL 15-minute candles of TODAY (not only latest candle)
- writes CSV to: daily_signals/signals_YYYY-MM-DD.csv
- keeps signal logic identical by reusing helpers from the live analyser
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

import eqidv1_live_combined_analyser_csv as live

ROOT = Path(__file__).resolve().parent
DAILY_SIGNAL_DIR = ROOT / "daily_signals"
DAILY_SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
DAILY_REWRITE_FILE_EACH_RUN = True

DAILY_SIGNAL_COLUMNS = [
    "signal_id",
    "logtime_ist",
    "signal_entry_datetime_ist",
    "signal_bar_time_ist",
    "ticker",
    "side",
    "entry_price",
    "sl_price",
    "target_price",
    "quality_score",
    "atr_pct_signal",
    "rsi_signal",
    "adx_signal",
    "p_win",
    "ml_threshold",
    "confidence_multiplier",
    "quantity",
    "notes",
]


def _make_signal_id(strategy: str, ticker: str, side: str, bar_time: str, setup: str = "") -> str:
    raw = f"{strategy}|{ticker.upper()}|{side.upper()}|{bar_time}|{setup}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _ensure_daily_csv_schema(csv_path: str) -> None:
    """Best-effort in-place schema migration for today's daily signal CSV."""
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return
    try:
        df_existing = pd.read_csv(
            csv_path,
            engine="python",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            on_bad_lines="warn",
        )
    except Exception:
        return

    missing = [c for c in DAILY_SIGNAL_COLUMNS if c not in df_existing.columns]
    if not missing:
        return

    for c in missing:
        df_existing[c] = ""

    df_existing = df_existing[DAILY_SIGNAL_COLUMNS]
    df_existing.to_csv(csv_path, index=False, quoting=csv.QUOTE_ALL)
    print(f"[CSV][DAILY] migrated schema for {csv_path} | added={missing}", flush=True)


def _write_daily_signals_csv(signals_df: pd.DataFrame, *, strategy: str = "EQIDV1_DAILY") -> int:
    """Append deduplicated signals into daily_signals/signals_YYYY-MM-DD.csv."""
    today_str = live.now_ist().strftime("%Y-%m-%d")
    csv_path = str(DAILY_SIGNAL_DIR / live.SIGNAL_CSV_PATTERN.format(today_str))
    DAILY_SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
    logtime = live.now_ist().strftime("%Y-%m-%d %H:%M:%S%z")

    rows_to_write = []
    existing_ids = set()
    if (not DAILY_REWRITE_FILE_EACH_RUN) and os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        _ensure_daily_csv_schema(csv_path)
        existing_ids = live._load_existing_ids(csv_path)

    if signals_df is not None and (not signals_df.empty):
        for _, row in signals_df.iterrows():
            ticker = str(row.get("ticker", "")).upper()
            side = str(row.get("side", "")).upper()
            bar_time = str(row.get("bar_time_ist", ""))
            setup = str(row.get("setup", ""))

            signal_id = _make_signal_id(strategy, ticker, side, bar_time, setup)
            if signal_id in existing_ids:
                continue

            entry = float(row.get("entry_price", 0.0) or 0.0)
            slp = float(row.get("sl_price", 0.0) or 0.0)
            tgt = float(row.get("target_price", 0.0) or 0.0)
            score = float(row.get("score", 0.0) or 0.0)

            atr_pct = 0.0
            rsi = 0.0
            adx = 0.0
            impulse = ""

            diag_str = row.get("diagnostics_json", "")
            if isinstance(diag_str, str) and diag_str:
                try:
                    diag = json.loads(diag_str)
                    atr_val = live._safe_float(diag.get("atr", diag.get("ATR15", 0)))
                    close_val = live._safe_float(diag.get("close", entry))
                    if pd.notna(atr_val) and pd.notna(close_val) and close_val > 0:
                        atr_pct = float(atr_val) / float(close_val)
                    rsi = live._safe_float(diag.get("rsi", diag.get("RSI15", 0)))
                    adx = live._safe_float(diag.get("adx", diag.get("ADX15", 0)))
                    impulse = str(diag.get("impulse_type", ""))
                except Exception:
                    pass

            qty = int(round((live.DEFAULT_POSITION_SIZE_RS * live.INTRADAY_LEVERAGE) / entry)) if entry > 0 else 0
            notes = f"eqidv1_daily|setup={setup}|impulse={impulse}"

            rows_to_write.append({
                "signal_id": signal_id,
                "logtime_ist": logtime,
                "signal_entry_datetime_ist": bar_time,
                "signal_bar_time_ist": bar_time,
                "ticker": ticker,
                "side": side,
                "entry_price": round(entry, 4),
                "sl_price": round(slp, 4),
                "target_price": round(tgt, 4),
                "quality_score": round(score, 6),
                "atr_pct_signal": round(float(atr_pct), 6),
                "rsi_signal": round(float(rsi), 2),
                "adx_signal": round(float(adx), 2),
                "p_win": "",
                "ml_threshold": "",
                "confidence_multiplier": "",
                "quantity": qty,
                "notes": notes,
            })
            existing_ids.add(signal_id)

    mode = "w" if DAILY_REWRITE_FILE_EACH_RUN else "a"
    with open(csv_path, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=DAILY_SIGNAL_COLUMNS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for r in rows_to_write:
            writer.writerow(r)

    print(
        f"[CSV][DAILY] path={csv_path} | written={len(rows_to_write)} | rewrite_mode={DAILY_REWRITE_FILE_EACH_RUN}",
        flush=True,
    )
    return len(rows_to_write)


def _scan_today_all_slots(verbose: bool = False) -> pd.DataFrame:
    tickers = live.list_tickers_15m()
    if not tickers:
        print("[WARN] No tickers found.", flush=True)
        return pd.DataFrame()

    all_signals: List[Dict[str, Any]] = []
    scanned = 0

    for t in tickers:
        path = os.path.join(live.DIR_15M, f"{t}{live.END_15M}")
        try:
            df_raw = live.read_parquet_tail(path, n=live.TAIL_ROWS)
            sigs = live._all_day_runner_parity_signals_for_ticker(t, df_raw)
            for s in sigs:
                all_signals.append({
                    "ticker": s.ticker,
                    "side": s.side,
                    "bar_time_ist": s.bar_time_ist,
                    "setup": s.setup,
                    "entry_price": s.entry_price,
                    "sl_price": s.sl_price,
                    "target_price": s.target_price,
                    "score": s.score,
                    "diagnostics_json": json.dumps(s.diagnostics, default=str),
                })

            scanned += 1
            if verbose and scanned % 25 == 0:
                print(f"  scanned {scanned}/{len(tickers)} tickers | signals={len(all_signals)}", flush=True)

        except Exception as e:
            if verbose:
                print(f"  [ERR] {t} {e}", flush=True)
            continue

    signals_df = pd.DataFrame(all_signals)
    written = _write_daily_signals_csv(signals_df, strategy="EQIDV1_DAILY")

    print(f"[DONE][DAILY] scanned={scanned} signals={len(signals_df)} written={written}", flush=True)
    return signals_df


def main() -> None:
    global DAILY_SIGNAL_DIR

    default_data_dir = Path(live.DIR_15M)
    if not default_data_dir.is_absolute():
        default_data_dir = ROOT / default_data_dir

    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=str(default_data_dir))
    ap.add_argument("--signals-dir", default=str(DAILY_SIGNAL_DIR))
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    live.DIR_15M = str(Path(args.data_dir))

    DAILY_SIGNAL_DIR = Path(args.signals_dir)
    DAILY_SIGNAL_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[START][DAILY] data_dir={live.DIR_15M} out={DAILY_SIGNAL_DIR}", flush=True)
    print(
        f"[INFO][DAILY] signal window source={getattr(live, 'WINDOW_OVERRIDE_SOURCE', 'live_defaults')}",
        flush=True,
    )
    print(
        "[INFO][DAILY] SHORT windows: "
        f"use_time_windows={live.SHORT_USE_TIME_WINDOWS} | "
        + ", ".join([f"{a.strftime('%H:%M')}-{b.strftime('%H:%M')}" for a, b in live.SHORT_SIGNAL_WINDOWS]),
        flush=True,
    )
    print(
        "[INFO][DAILY] LONG  windows: "
        f"use_time_windows={live.LONG_USE_TIME_WINDOWS} | "
        + ", ".join([f"{a.strftime('%H:%M')}-{b.strftime('%H:%M')}" for a, b in live.LONG_SIGNAL_WINDOWS]),
        flush=True,
    )
    _scan_today_all_slots(verbose=bool(args.verbose))


if __name__ == "__main__":
    main()

