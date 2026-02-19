# -*- coding: utf-8 -*-
"""
eqidv2_live_combined_analyser_parquet.py
========================================

Live parquet variant aligned with `eqidv2_live_combined_analyser_csv.py`:
- same scheduler + latest-slot scan behavior
- same core signal logic via the CSV analyser module
- additional parquet bridge output with dedupe by `signal_id`
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import numpy as np
import pandas as pd

import eqidv2_live_combined_analyser_csv as live

ROOT = Path(__file__).resolve().parent
LIVE_PQ_DIR = ROOT / "daily_signals_parquet"
LIVE_PQ_DIR.mkdir(parents=True, exist_ok=True)

SIGNAL_PQ_COLUMNS = [
    "signal_id",
    "signal_datetime",
    "signal_entry_datetime_ist",
    "signal_bar_time_ist",
    "received_time",
    "ticker",
    "side",
    "setup",
    "impulse_type",
    "entry_price",
    "stop_price",
    "target_price",
    "quality_score",
    "atr_pct",
    "rsi",
    "adx",
    "quantity",
]


def _read_existing_ids(pq_path: Path) -> Set[str]:
    if not pq_path.exists() or pq_path.stat().st_size == 0:
        return set()
    try:
        live._require_pyarrow()
        old = pd.read_parquet(pq_path, engine=live.PARQUET_ENGINE)
        if "signal_id" in old.columns:
            return set(old["signal_id"].astype(str).tolist())
    except Exception:
        pass
    return set()


def _rows_from_signals(signals_df: pd.DataFrame) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if signals_df is None or signals_df.empty:
        return out

    received_time = live.now_ist().strftime("%Y-%m-%d %H:%M:%S%z")

    for _, row in signals_df.iterrows():
        ticker = str(row.get("ticker", "")).upper()
        side = str(row.get("side", "")).upper()
        bar_time = str(row.get("bar_time_ist", ""))
        setup = str(row.get("setup", ""))
        signal_id = live._generate_signal_id(ticker, side, bar_time, setup)

        entry_price = float(row.get("entry_price", 0) or 0)
        notional = live.DEFAULT_POSITION_SIZE_RS * live.INTRADAY_LEVERAGE
        qty = max(1, int(notional / entry_price)) if entry_price > 0 else 1

        atr_pct = 0.0
        rsi_val = 0.0
        adx_val = 0.0
        impulse_type = ""
        diag_str = row.get("diagnostics_json", "")
        if diag_str:
            try:
                diag = json.loads(diag_str) if isinstance(diag_str, str) else {}
                atr_val = live._safe_float(diag.get("atr", 0))
                close_val = live._safe_float(diag.get("close", entry_price))
                if np.isfinite(atr_val) and np.isfinite(close_val) and close_val > 0:
                    atr_pct = atr_val / close_val
                rsi_val = live._safe_float(diag.get("rsi", 0))
                adx_val = live._safe_float(diag.get("adx", 0))
                impulse_type = str(diag.get("impulse_type", ""))
            except (json.JSONDecodeError, TypeError):
                pass

        out.append(
            {
                "signal_id": signal_id,
                "signal_datetime": bar_time,
                "signal_entry_datetime_ist": bar_time,
                "signal_bar_time_ist": bar_time,
                "received_time": received_time,
                "ticker": ticker,
                "side": side,
                "setup": setup,
                "impulse_type": impulse_type,
                "entry_price": round(entry_price, 2),
                "stop_price": round(float(row.get("sl_price", 0) or 0), 2),
                "target_price": round(float(row.get("target_price", 0) or 0), 2),
                "quality_score": round(float(row.get("score", 0) or 0), 4),
                "atr_pct": round(float(atr_pct), 6),
                "rsi": round(float(rsi_val), 2),
                "adx": round(float(adx_val), 2),
                "quantity": int(qty),
            }
        )

    return out


def _write_signals_parquet(signals_df: pd.DataFrame) -> int:
    today_str = live.now_ist().strftime("%Y-%m-%d")
    pq_path = LIVE_PQ_DIR / f"signals_{today_str}.parquet"

    live._require_pyarrow()
    existing_ids = _read_existing_ids(pq_path)
    rows = [r for r in _rows_from_signals(signals_df) if r["signal_id"] not in existing_ids]

    if not rows:
        print(f"[PQ  ] scanned={0 if signals_df is None else len(signals_df)} written=0 path={pq_path}", flush=True)
        return 0

    new_df = pd.DataFrame(rows, columns=SIGNAL_PQ_COLUMNS)
    if pq_path.exists() and pq_path.stat().st_size > 0:
        try:
            old_df = pd.read_parquet(pq_path, engine=live.PARQUET_ENGINE)
            out_df = pd.concat([old_df, new_df], ignore_index=True)
            out_df = out_df.drop_duplicates(subset=["signal_id"], keep="first").reset_index(drop=True)
        except Exception:
            out_df = new_df
    else:
        out_df = new_df

    out_df.to_parquet(pq_path, index=False, engine=live.PARQUET_ENGINE, compression="snappy")
    print(f"[PQ  ] scanned={len(signals_df)} written={len(new_df)} path={pq_path}", flush=True)
    return int(len(new_df))


def run_one_scan(run_tag: str = "A") -> Tuple[pd.DataFrame, pd.DataFrame]:
    checks_df, signals_df = live.run_one_scan(run_tag=run_tag)
    pq_written = _write_signals_parquet(signals_df)
    print(f"[RUN ][PQ ] tag={run_tag} signals={len(signals_df)} pq_written={pq_written}", flush=True)
    return checks_df, signals_df


def main() -> None:
    global LIVE_PQ_DIR

    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=live.DIR_15M)
    ap.add_argument("--out-dir", default=str(LIVE_PQ_DIR))
    args = ap.parse_args()

    live.DIR_15M = str(args.data_dir)
    LIVE_PQ_DIR = Path(args.out_dir)
    LIVE_PQ_DIR.mkdir(parents=True, exist_ok=True)

    print("[LIVE] EQIDV2 PARQUET v2 (15m) | strict current 15m slot only")
    print(f"[INFO] DIR_15M={live.DIR_15M} | tickers={len(live.list_tickers_15m())}")
    print(f"[INFO] OUT_PQ={LIVE_PQ_DIR}")
    print(f"[INFO] SHORT: SL={live.SHORT_STOP_PCT*100:.2f}%, TGT={live.SHORT_TARGET_PCT*100:.2f}%, ADX>={live.SHORT_ADX_MIN}, RSI<={live.SHORT_RSI_MAX}, StochK<={live.SHORT_STOCHK_MAX}")
    print(f"[INFO] LONG : SL={live.LONG_STOP_PCT*100:.2f}%, TGT={live.LONG_TARGET_PCT*100:.2f}%, ADX>={live.LONG_ADX_MIN}, RSI>={live.LONG_RSI_MIN}, StochK>={live.LONG_STOCHK_MIN}")
    print(f"[INFO] Volume filter: {live.USE_VOLUME_FILTER} (ratio>={live.VOLUME_MIN_RATIO}, SMA={live.VOLUME_SMA_PERIOD})")
    print(f"[INFO] ATR% filter: {live.USE_ATR_PCT_FILTER} (min={live.ATR_PCT_MIN*100:.2f}%)")
    print(f"[INFO] Close-confirm: {live.REQUIRE_CLOSE_CONFIRM}")
    print(f"[INFO] UPDATE_15M_BEFORE_CHECK={live.UPDATE_15M_BEFORE_CHECK}")
    print(f"[INFO] Timing: initial_delay={live.INITIAL_DELAY_SECONDS}s, scans_per_slot={live.NUM_SCANS_PER_SLOT}, interval={live.SCAN_INTERVAL_SECONDS}s")

    holidays = live._read_holidays_safe()

    while True:
        now = live.now_ist()
        if now.time() >= live.HARD_STOP_TIME:
            print("[STOP] Hard-stop reached for today. Exiting.")
            return

        if not live.is_trading_day_safe(now.date(), holidays):
            nxt = live._next_slot_after(now + timedelta(days=1))
            print(f"[SKIP] Not a trading day ({now.date()}). Sleeping until {nxt}.")
            live._sleep_until(nxt)
            continue

        slot = live._next_slot_after(now)
        if slot.date() != now.date():
            print(f"[WAIT] Next slot is tomorrow {slot}. Sleeping.")
            live._sleep_until(slot)
            continue

        if now < slot:
            print(f"[WAIT] Sleeping until slot {slot.strftime('%Y-%m-%d %H:%M:%S%z')}")
            live._sleep_until(slot)

        now = live.now_ist()
        if now.time() > live.END_TIME:
            nxt = live._next_slot_after(now + timedelta(days=1))
            print(f"[DONE] Past END_TIME. Sleeping until {nxt}.")
            live._sleep_until(nxt)
            continue

        if live.UPDATE_15M_BEFORE_CHECK:
            try:
                print(f"[UPD ] Running eqidv2 core 15m update at {live.now_ist().strftime('%Y-%m-%d %H:%M:%S%z')}")
                live.run_update_15m_once(holidays)
            except Exception as e:
                print(f"[WARN] Update failed: {e!r}")

        delay_target = slot + timedelta(seconds=live.INITIAL_DELAY_SECONDS)
        now = live.now_ist()
        if now < delay_target:
            wait_secs = (delay_target - now).total_seconds()
            print(f"[WAIT] Delaying {wait_secs:.0f}s for data fetch to complete (until {delay_target.strftime('%H:%M:%S')})")
            time.sleep(max(0.0, wait_secs))

        scan_labels = [chr(ord("A") + i) for i in range(live.NUM_SCANS_PER_SLOT)]
        for scan_idx, label in enumerate(scan_labels):
            now = live.now_ist()
            if now.time() >= live.HARD_STOP_TIME:
                print("[STOP] Hard-stop reached mid-slot. Exiting.")
                return

            print(f"[RUN ] Slot {slot.strftime('%H:%M')} | scan {label} ({scan_idx+1}/{live.NUM_SCANS_PER_SLOT}) at {now.strftime('%H:%M:%S')}")
            run_one_scan(run_tag=label)

            if scan_idx < len(scan_labels) - 1:
                time.sleep(float(live.SCAN_INTERVAL_SECONDS))

        time.sleep(1.0)


if __name__ == "__main__":
    main()
