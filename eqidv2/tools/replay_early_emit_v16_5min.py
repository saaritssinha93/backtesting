"""
Standalone replay driver for eqidv2_early_emit_v16_5min.

Iterates every 5-min slot from 09:20 to session end for a given date,
runs `evaluate_slot_candidates` across all tickers, and writes the
result to `candidate_signals_<date>_v16_5min.json`.

Does NOT touch live SE or executor. Safe to run anytime.

Usage:
  py -3.12 tools/replay_early_emit_v16_5min.py                    # defaults to today
  py -3.12 tools/replay_early_emit_v16_5min.py --date 2026-04-22
  py -3.12 tools/replay_early_emit_v16_5min.py --tickers RELIANCE,TCS
"""
from __future__ import annotations

import argparse
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytz

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import eqidv2_live_combined_analyser_csv_v16_5min as analyser
from avwap_v11_refactored.avwap_common_v11 import prepare_session_bars_for_scan
from eqidv2_early_emit_v16_5min import (
    evaluate_slot_candidates,
    write_candidate_pool,
)

IST = pytz.timezone("Asia/Kolkata")
DATA_DIR = Path("C:/TradingData/eqidv2/stocks_indicators_5min_eq_live")
POOL_DIR = Path("C:/TradingData/eqidv2/live_signals")


def _session_slots_for_date(date_str: str) -> list:
    day = datetime.strptime(date_str, "%Y-%m-%d").date()
    start = IST.localize(datetime(day.year, day.month, day.day, 9, 20, 0))
    end = IST.localize(datetime(day.year, day.month, day.day, 15, 30, 0))
    slots = []
    t = start
    while t <= end:
        slots.append(t)
        t += timedelta(minutes=5)
    return slots


def _load_ticker_parquet(ticker: str) -> pd.DataFrame | None:
    path = DATA_DIR / f"{ticker}_stocks_indicators_5min.parquet"
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def _list_tickers() -> list[str]:
    suffix = "_stocks_indicators_5min.parquet"
    return sorted([p.name.replace(suffix, "") for p in DATA_DIR.glob(f"*{suffix}")])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=datetime.now(IST).strftime("%Y-%m-%d"))
    ap.add_argument("--tickers", default="", help="Comma-separated; empty = all")
    ap.add_argument("--slots", default="", help="Comma-separated HH:MM slots; empty = all")
    ap.add_argument("--pool-dir", default=str(POOL_DIR))
    args = ap.parse_args()

    date_str = args.date
    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()] or _list_tickers()
    if not tickers:
        print(f"[FATAL] no tickers found under {DATA_DIR}", file=sys.stderr)
        return 2

    short_cfg = analyser._build_live_v16_short_cfg()
    long_cfg = analyser._build_live_v16_long_cfg()
    try:
        short_cfg, long_cfg = analyser.apply_live_parity_profile(short_cfg, long_cfg)
    except Exception:
        pass
    short_cfg.allow_incomplete_tail = True
    long_cfg.allow_incomplete_tail = True

    all_slots = _session_slots_for_date(date_str)
    if args.slots:
        wanted = set(s.strip() for s in args.slots.split(",") if s.strip())
        all_slots = [s for s in all_slots if s.strftime("%H:%M") in wanted]

    print(f"[INFO] date={date_str} tickers={len(tickers)} slots={len(all_slots)} pool_dir={args.pool_dir}")

    target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    all_candidates = []
    total_evaluated = 0
    total_emitted = 0
    started = time.perf_counter()

    for slot_ts in all_slots:
        slot_started = time.perf_counter()
        slot_emitted = 0
        for ticker in tickers:
            df = _load_ticker_parquet(ticker)
            if df is None or df.empty:
                continue
            try:
                df_prepared = prepare_session_bars_for_scan(df, short_cfg)
                if df_prepared is None or df_prepared.empty:
                    continue
                if "date" in df_prepared.columns:
                    df_prepared = df_prepared[
                        pd.to_datetime(df_prepared["date"]).dt.date == target_date
                    ]
                if df_prepared.empty:
                    continue

                dates = pd.to_datetime(df_prepared["date"])
                if dates.dt.tz is None:
                    dates = dates.dt.tz_localize(IST)
                else:
                    dates = dates.dt.tz_convert(IST)
                df_prepared = df_prepared[dates <= slot_ts]
                if df_prepared.empty:
                    continue

                cands = evaluate_slot_candidates(
                    ticker, df_prepared, slot_ts, short_cfg, long_cfg
                )
                if cands:
                    all_candidates.extend(cands)
                    slot_emitted += len(cands)
                total_evaluated += 1
            except Exception:
                traceback.print_exc(limit=2)
                continue
        elapsed = time.perf_counter() - slot_started
        total_emitted += slot_emitted
        print(
            f"  [SLOT] {slot_ts.strftime('%H:%M')}  candidates_new={slot_emitted}  elapsed={elapsed:.1f}s",
            flush=True,
        )

    out_path = write_candidate_pool(all_candidates, date_str, Path(args.pool_dir))
    print(
        f"[DONE] evaluated_ticker_slot_pairs={total_evaluated} "
        f"emitted_rows={total_emitted} elapsed={time.perf_counter()-started:.1f}s"
    )
    print(f"[OUT] {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
