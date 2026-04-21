# -*- coding: utf-8 -*-
"""
EQIDV2 — V16 5min "Option A" Intraday Replay Driver
====================================================
Replays today (or a given date) slot-by-slot using the SAME live-parity
entry points the Detection Engine calls internally:

    live_v16._scan_slot(...)
    live_v16._apply_backtest_parity_filters_to_dicts(...)

What it does per 5-min slot (09:15 -> 15:30):
  1. Build NIFTY backtest-intraday context (allow_long / allow_short, mode_map).
  2. Run the sharded per-ticker scan across the full 1044-ticker universe
     (uses the SAME code path DE's live-parity cycle uses).
  3. Apply parity filters (_finalize_side_scan_df + v17b hybrid context +
     OR-gate enrichment + entry-vol enrichment + V16 post-scan filters).
  4. Capture RAW signals (pre-filter) AND FINAL signals (post-filter).
  5. Emit timing.

What it does NOT exercise (by design - this is Option A):
  - Pending-pool JSON write / PF retry / PF ready-marker mechanics.
  - Pending-fetcher Kite re-fetch (today's parquet is already populated from
    the real live run - we read it directly).
  - Lag carry-forward of stale-but-valid signals (no pool => no stale state).
  - Trigger-bar OHLC drift check (that's a DE-side guard against Kite back-fill;
    no Kite in replay).

For those dimensions, use the full-pipeline driver (Option B).

Outputs (under eqidv2/replay_out/<date>/):
  replay_raw_signals.csv        - every raw signal a scan produced per slot
  replay_final_signals.csv      - every parity-survivor signal per slot
  replay_slot_timeline.csv      - one row per slot with counts & timing
  replay_summary.txt            - totals, top filter_reasons, long/short split
  replay_driver.log             - full stdout capture

Environment knobs (set BEFORE import):
  EQIDV16_5MIN_SCAN_SHARDS          default 16 in this driver
  EQIDV16_5MIN_SCAN_MAX_WORKERS     default 16 in this driver
  EQIDV16_5MIN_USE_SLOT_SNAPSHOTS   inherited (default 1)
  EQIDV16_5MIN_LIVE_MIN_BARS_FOR_SCAN  inherited
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# IMPORTANT: set scan worker / shard env vars BEFORE importing live_v16 so
# the module-level constants SCAN_SHARDS / SCAN_MAX_WORKERS pick them up.
# ---------------------------------------------------------------------------
import os

os.environ.setdefault("EQIDV16_5MIN_SCAN_SHARDS", "16")
os.environ.setdefault("EQIDV16_5MIN_SCAN_MAX_WORKERS", "16")
# The replay wants the full today's data parity scan - force the same live-parity
# behavior the DE uses.
os.environ.setdefault("EQIDV16_5MIN_USE_SLOT_SNAPSHOTS", "1")

import argparse
import csv
import json
import multiprocessing as mp
import sys
import time as _time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pytz

# ---------------------------------------------------------------------------
# Live engines - imported after env is configured
# ---------------------------------------------------------------------------
import eqidv2_live_combined_analyser_csv_v16_5min as live_v16
import avwap_combined_runner_v17f_5min as _v17f_runner  # noqa: F401  side-effect patch
import avwap_combined_runner_v16_5min as v16_runner
from avwap_v11_refactored.avwap_common_v11_v15 import (
    StrategyConfig,
    default_short_config,
    default_long_config as default_long_config_v11,
)
from avwap_combined_runner_v16_5min import (
    apply_live_parity_profile,
    TEST_SHORT_TARGET_PCT,
    TEST_LONG_TARGET_PCT,
)
from eqidv2_runtime_paths import DATA_5M_DIR as RUNTIME_DATA_5M_DIR

IST = pytz.timezone("Asia/Kolkata")

SCRIPT_DIR = Path(__file__).resolve().parent
END_5M = "_stocks_indicators_5min.parquet"


# ---------------------------------------------------------------------------
# CONFIG HELPERS (mirror DE._build_v16_cfgs so strategy semantics match live)
# ---------------------------------------------------------------------------
def build_v16_cfgs(live_min_bars_for_scan: int = 4) -> Tuple[StrategyConfig, StrategyConfig]:
    v16_runner.NIFTY_CONTEXT_OR_END_TIME = dtime(9, 20)
    v16_runner.NIFTY_CONTEXT_CONFIRM_TIME = dtime(9, 20)

    short_builder = getattr(v16_runner, "default_short_config", None)
    short_cfg = short_builder() if callable(short_builder) else default_short_config()
    long_builder = getattr(v16_runner, "default_long_config_v9", None)
    long_cfg = long_builder() if callable(long_builder) else default_long_config_v11()
    short_cfg, long_cfg = apply_live_parity_profile(short_cfg, long_cfg)
    short_cfg.stop_pct = 0.0075
    long_cfg.stop_pct = 0.0075
    short_cfg.target_pct = float(TEST_SHORT_TARGET_PCT)
    long_cfg.target_pct = float(TEST_LONG_TARGET_PCT)
    short_cfg.dir_15m = str(RUNTIME_DATA_5M_DIR)
    short_cfg.end_15m = END_5M
    long_cfg.dir_15m = str(RUNTIME_DATA_5M_DIR)
    long_cfg.end_15m = END_5M
    short_cfg.allow_incomplete_tail = True
    long_cfg.allow_incomplete_tail = True
    short_cfg.min_bars_for_scan = live_min_bars_for_scan
    long_cfg.min_bars_for_scan = live_min_bars_for_scan
    return short_cfg, long_cfg


# ---------------------------------------------------------------------------
# TEE stdout to a log file under the replay output dir
# ---------------------------------------------------------------------------
class _Tee:
    def __init__(self, *streams):
        self._streams = streams

    def write(self, data):
        for s in self._streams:
            try:
                s.write(data)
                s.flush()
            except Exception:
                pass

    def flush(self):
        for s in self._streams:
            try:
                s.flush()
            except Exception:
                pass


def _start_tee(log_path: Path) -> None:
    try:
        for name in ("stdout", "stderr", "__stdout__", "__stderr__"):
            stream = getattr(sys, name, None)
            if stream and hasattr(stream, "reconfigure"):
                try:
                    stream.reconfigure(encoding="utf-8", errors="replace")
                except Exception:
                    pass
        fh = open(log_path, "w", encoding="utf-8", buffering=1)
        base_stdout = getattr(sys, "__stdout__", None) or sys.stdout
        base_stderr = getattr(sys, "__stderr__", None) or sys.stderr
        sys.stdout = _Tee(base_stdout, fh)  # type: ignore[assignment]
        sys.stderr = _Tee(base_stderr, fh)  # type: ignore[assignment]
    except Exception:
        pass


# ---------------------------------------------------------------------------
# SLOT RANGE
# ---------------------------------------------------------------------------
def _build_slot_range(
    target_date: datetime.date,
    start_time: dtime = dtime(9, 15),
    end_time: dtime = dtime(15, 30),
    step_minutes: int = 5,
) -> List[datetime]:
    """Return an inclusive list of slot-open timestamps from start to end."""
    start = IST.localize(datetime.combine(target_date, start_time))
    end = IST.localize(datetime.combine(target_date, end_time))
    slots: List[datetime] = []
    cur = start
    while cur <= end:
        slots.append(cur)
        cur += timedelta(minutes=step_minutes)
    return slots


# ---------------------------------------------------------------------------
# CSV WRITERS
# ---------------------------------------------------------------------------
RAW_COLUMNS = [
    "slot", "side", "ticker", "setup",
    "signal_time_ist", "entry_time_ist",
    "entry_price", "stop_price", "target_price",
    "quality_score", "avwap_dist_atr_signal", "rsi_signal",
    "adx_signal", "rs_pct",
]

FINAL_COLUMNS = RAW_COLUMNS + ["filter_stage"]  # filter_stage always "final"

SLOT_TIMELINE_COLUMNS = [
    "slot", "allow_long", "allow_short", "nifty_mode", "nifty_rs_pct",
    "raw_short", "raw_long",
    "post_context_short", "post_context_long",
    "final_short", "final_long",
    "dropped_short", "dropped_long",
    "scan_elapsed_sec", "slot_total_sec",
]


def _row_from_signal(slot_iso: str, side: str, row: Dict[str, Any], filter_stage: Optional[str] = None) -> Dict[str, Any]:
    out = {
        "slot": slot_iso,
        "side": side,
        "ticker": str(row.get("ticker", "")),
        "setup": str(row.get("setup", "")),
        "signal_time_ist": str(row.get("signal_time_ist", row.get("bar_time_ist", ""))),
        "entry_time_ist": str(row.get("entry_time_ist", row.get("signal_entry_datetime_ist", ""))),
        "entry_price": row.get("entry_price", ""),
        "stop_price": row.get("stop_price", row.get("sl_price", "")),
        "target_price": row.get("target_price", ""),
        "quality_score": row.get("quality_score", ""),
        "avwap_dist_atr_signal": row.get("avwap_dist_atr_signal", row.get("avwap_dist_atr", "")),
        "rsi_signal": row.get("rsi_signal", ""),
        "adx_signal": row.get("adx_signal", row.get("adx", "")),
        "rs_pct": row.get("rs_pct", ""),
    }
    if filter_stage is not None:
        out["filter_stage"] = filter_stage
    return out


# ---------------------------------------------------------------------------
# PER-SLOT REPLAY
# ---------------------------------------------------------------------------
def replay_one_slot(
    slot_ist: datetime,
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
    tickers: Optional[List[str]] = None,
) -> Dict[str, Any]:
    slot_started = _time.perf_counter()
    slot_iso = slot_ist.strftime("%Y-%m-%dT%H:%M:%S%z")
    slot_hhmm = slot_ist.strftime("%H:%M")
    print("\n" + "=" * 74, flush=True)
    print(f"[REPLAY_SLOT] {slot_iso}  ({slot_hhmm})", flush=True)
    print("=" * 74, flush=True)

    # Build NIFTY backtest-intraday context (same as DE's parity cycle).
    context_state = live_v16._build_backtest_context_state(slot_ist, short_cfg)
    allow_long = bool(context_state["allow_long"])
    allow_short = bool(context_state["allow_short"])
    mode_map = context_state.get("mode_map", {})
    nifty_ret_map = context_state.get("nifty_ret_map", {})

    nifty_payload = (context_state.get("payload") or {}).get("nifty_context") or {}
    mode = str(nifty_payload.get("mode", "BOTH"))
    rs_pct_ctx = float(nifty_payload.get("rs_pct", 0.0) or 0.0)

    # Run the sharded scan - SAME code path as DE live parity.
    short_rows, long_rows, scan_meta = live_v16._scan_slot(
        slot_ist,
        short_cfg,
        long_cfg,
        allow_long,
        allow_short,
        tickers=tickers,
    )
    raw_short = list(short_rows)
    raw_long = list(long_rows)

    # Apply parity filters (same call as DE live parity).
    final_short, final_long, filter_meta = live_v16._apply_backtest_parity_filters_to_dicts(
        list(raw_short),
        list(raw_long),
        short_cfg,
        long_cfg,
        mode_map,
        nifty_ret_map,
    )

    slot_total_sec = _time.perf_counter() - slot_started
    counts = {
        "raw_short": int(len(raw_short)),
        "raw_long": int(len(raw_long)),
        "post_context_short": int(filter_meta.get("post_context_short", len(raw_short))),
        "post_context_long": int(filter_meta.get("post_context_long", len(raw_long))),
        "final_short": int(filter_meta.get("final_short", len(final_short))),
        "final_long": int(filter_meta.get("final_long", len(final_long))),
    }
    counts["dropped_short"] = counts["raw_short"] - counts["final_short"]
    counts["dropped_long"] = counts["raw_long"] - counts["final_long"]

    print(
        f"[REPLAY_SLOT] {slot_hhmm} mode={mode} nifty_rs={rs_pct_ctx:+.2f}% "
        f"allow_long={allow_long} allow_short={allow_short} | "
        f"raw short={counts['raw_short']} long={counts['raw_long']} | "
        f"final short={counts['final_short']} long={counts['final_long']} | "
        f"dropped short={counts['dropped_short']} long={counts['dropped_long']} | "
        f"scan={scan_meta.get('scan_elapsed_sec', 0.0):.1f}s "
        f"total={slot_total_sec:.1f}s",
        flush=True,
    )

    return {
        "slot_ist": slot_ist,
        "slot_iso": slot_iso,
        "allow_long": allow_long,
        "allow_short": allow_short,
        "mode": mode,
        "nifty_rs_pct": rs_pct_ctx,
        "raw_short": raw_short,
        "raw_long": raw_long,
        "final_short": final_short,
        "final_long": final_long,
        "scan_meta": scan_meta,
        "filter_meta": filter_meta,
        "counts": counts,
        "slot_total_sec": slot_total_sec,
    }


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> int:
    if mp.current_process().name != "MainProcess":
        return 0

    parser = argparse.ArgumentParser(description="V16 5min intraday replay (Option A)")
    parser.add_argument(
        "--date",
        default=None,
        help="Replay date YYYY-MM-DD (default: today in IST)",
    )
    parser.add_argument(
        "--start", default="09:15", help="Slot start HH:MM (default 09:15)"
    )
    parser.add_argument(
        "--end", default="15:30", help="Slot end HH:MM inclusive (default 15:30)"
    )
    parser.add_argument(
        "--out-dir", default=None, help="Replay output directory"
    )
    parser.add_argument(
        "--tickers",
        default=None,
        help="Optional comma-separated ticker subset (default: full 1044 universe)",
    )
    args = parser.parse_args()

    # ---- Resolve target date & output dir --------------------------------
    now_ist = datetime.now(IST)
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = now_ist.date()
    date_str = target_date.strftime("%Y-%m-%d")

    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        out_dir = SCRIPT_DIR / "replay_out" / date_str
    out_dir.mkdir(parents=True, exist_ok=True)

    log_path = out_dir / "replay_driver.log"
    _start_tee(log_path)

    # ---- Slot range ------------------------------------------------------
    def _parse_hhmm(text: str) -> dtime:
        hh, mm = text.strip().split(":")
        return dtime(int(hh), int(mm))

    start_time = _parse_hhmm(args.start)
    end_time = _parse_hhmm(args.end)
    slots = _build_slot_range(target_date, start_time, end_time, step_minutes=5)

    ticker_subset: Optional[List[str]] = None
    if args.tickers:
        ticker_subset = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    # ---- Banner ----------------------------------------------------------
    print("=" * 74, flush=True)
    print("EQIDV2 V16 5min INTRADAY REPLAY (OPTION A - signals-only)", flush=True)
    print(f"  date           : {date_str}", flush=True)
    print(f"  slots          : {len(slots)} slots from {args.start} to {args.end}", flush=True)
    print(f"  data dir       : {RUNTIME_DATA_5M_DIR}", flush=True)
    print(f"  out dir        : {out_dir}", flush=True)
    print(f"  scan shards    : {os.environ.get('EQIDV16_5MIN_SCAN_SHARDS')}", flush=True)
    print(f"  scan workers   : {os.environ.get('EQIDV16_5MIN_SCAN_MAX_WORKERS')}", flush=True)
    print(f"  snapshot mode  : {os.environ.get('EQIDV16_5MIN_USE_SLOT_SNAPSHOTS')}", flush=True)
    print(f"  ticker filter  : {'all' if not ticker_subset else f'{len(ticker_subset)} subset'}", flush=True)
    print("=" * 74, flush=True)

    # ---- Configs (mirror DE's live-parity config) ------------------------
    short_cfg, long_cfg = build_v16_cfgs()

    # ---- Outputs ---------------------------------------------------------
    raw_csv_path = out_dir / "replay_raw_signals.csv"
    final_csv_path = out_dir / "replay_final_signals.csv"
    slot_csv_path = out_dir / "replay_slot_timeline.csv"
    summary_path = out_dir / "replay_summary.txt"

    raw_rows_out: List[Dict[str, Any]] = []
    final_rows_out: List[Dict[str, Any]] = []
    slot_rows_out: List[Dict[str, Any]] = []

    totals: Dict[str, int] = defaultdict(int)
    per_setup_final: Counter = Counter()
    per_ticker_final: Counter = Counter()
    per_side_final: Counter = Counter()

    overall_started = _time.perf_counter()
    for slot_ist in slots:
        try:
            result = replay_one_slot(
                slot_ist, short_cfg, long_cfg, tickers=ticker_subset
            )
        except Exception as exc:
            print(f"[REPLAY_ERROR] slot={slot_ist.strftime('%H:%M')} {exc!r}", flush=True)
            continue

        slot_iso = result["slot_iso"]
        counts = result["counts"]
        for key in ("raw_short", "raw_long", "final_short", "final_long",
                    "post_context_short", "post_context_long",
                    "dropped_short", "dropped_long"):
            totals[key] += int(counts.get(key, 0))

        for row in result["raw_short"]:
            raw_rows_out.append(_row_from_signal(slot_iso, "SHORT", row))
        for row in result["raw_long"]:
            raw_rows_out.append(_row_from_signal(slot_iso, "LONG", row))
        for row in result["final_short"]:
            final_rows_out.append(_row_from_signal(slot_iso, "SHORT", row, filter_stage="final"))
            per_setup_final[f"SHORT:{row.get('setup', '')}"] += 1
            per_ticker_final[str(row.get("ticker", ""))] += 1
            per_side_final["SHORT"] += 1
        for row in result["final_long"]:
            final_rows_out.append(_row_from_signal(slot_iso, "LONG", row, filter_stage="final"))
            per_setup_final[f"LONG:{row.get('setup', '')}"] += 1
            per_ticker_final[str(row.get("ticker", ""))] += 1
            per_side_final["LONG"] += 1

        slot_rows_out.append({
            "slot": slot_iso,
            "allow_long": result["allow_long"],
            "allow_short": result["allow_short"],
            "nifty_mode": result["mode"],
            "nifty_rs_pct": round(result["nifty_rs_pct"], 4),
            "raw_short": counts["raw_short"],
            "raw_long": counts["raw_long"],
            "post_context_short": counts["post_context_short"],
            "post_context_long": counts["post_context_long"],
            "final_short": counts["final_short"],
            "final_long": counts["final_long"],
            "dropped_short": counts["dropped_short"],
            "dropped_long": counts["dropped_long"],
            "scan_elapsed_sec": round(float(result["scan_meta"].get("scan_elapsed_sec", 0.0)), 2),
            "slot_total_sec": round(float(result["slot_total_sec"]), 2),
        })

    overall_elapsed = _time.perf_counter() - overall_started

    # ---- Write CSVs ------------------------------------------------------
    def _write_csv(path: Path, columns: List[str], rows: List[Dict[str, Any]]) -> None:
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=columns, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for row in rows:
                writer.writerow({col: row.get(col, "") for col in columns})

    _write_csv(raw_csv_path, RAW_COLUMNS, raw_rows_out)
    _write_csv(final_csv_path, FINAL_COLUMNS, final_rows_out)
    _write_csv(slot_csv_path, SLOT_TIMELINE_COLUMNS, slot_rows_out)

    # ---- Summary ---------------------------------------------------------
    lines: List[str] = []
    lines.append("=" * 74)
    lines.append(f"V16 5MIN REPLAY SUMMARY - {date_str}")
    lines.append("=" * 74)
    lines.append(f"slots replayed       : {len(slot_rows_out)} / {len(slots)}")
    lines.append(f"total elapsed        : {overall_elapsed:.1f}s ({overall_elapsed / 60.0:.1f}m)")
    lines.append("")
    lines.append("-- TOTALS -------------------------------------------")
    lines.append(f"raw_short            : {totals['raw_short']}")
    lines.append(f"raw_long             : {totals['raw_long']}")
    lines.append(f"post_context_short   : {totals['post_context_short']}")
    lines.append(f"post_context_long    : {totals['post_context_long']}")
    lines.append(f"final_short          : {totals['final_short']}")
    lines.append(f"final_long           : {totals['final_long']}")
    lines.append(f"dropped_short        : {totals['dropped_short']}")
    lines.append(f"dropped_long         : {totals['dropped_long']}")
    lines.append(f"total_raw            : {totals['raw_short'] + totals['raw_long']}")
    lines.append(f"total_final          : {totals['final_short'] + totals['final_long']}")
    lines.append("")
    lines.append("-- FINAL BY SIDE ------------------------------------")
    for side, count in sorted(per_side_final.items()):
        lines.append(f"{side:<6}: {count}")
    lines.append("")
    lines.append("-- FINAL TOP 20 (side:setup) ------------------------")
    for key, count in per_setup_final.most_common(20):
        lines.append(f"{count:>5}  {key}")
    lines.append("")
    lines.append("-- FINAL TOP 20 TICKERS -----------------------------")
    for ticker, count in per_ticker_final.most_common(20):
        lines.append(f"{count:>5}  {ticker}")
    lines.append("")
    lines.append("-- SLOT-WISE TIMELINE -------------------------------")
    lines.append(f"{'slot':<26}{'mode':<6}{'rs%':>8}  {'rs':>4} {'rl':>4}  {'fs':>4} {'fl':>4}  {'t':>6}")
    for row in slot_rows_out:
        slot_short = row["slot"].split("T")[1][:5]
        lines.append(
            f"{row['slot']:<26}"
            f"{str(row['nifty_mode'])[:4]:<6}"
            f"{row['nifty_rs_pct']:>7.2f}  "
            f"{row['raw_short']:>4} {row['raw_long']:>4}  "
            f"{row['final_short']:>4} {row['final_long']:>4}  "
            f"{row['slot_total_sec']:>6.1f}"
        )
    lines.append("")
    lines.append("-- FILES --------------------------------------------")
    lines.append(f"raw           : {raw_csv_path}")
    lines.append(f"final         : {final_csv_path}")
    lines.append(f"slot_timeline : {slot_csv_path}")
    lines.append(f"log           : {log_path}")
    lines.append("=" * 74)

    report = "\n".join(lines)
    summary_path.write_text(report, encoding="utf-8")
    print("\n" + report, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
