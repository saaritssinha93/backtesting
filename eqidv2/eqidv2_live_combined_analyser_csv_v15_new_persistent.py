from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import eqidv2_live_combined_analyser_csv_v15 as base_v15
import eqidv2_live_combined_analyser_csv_v15_new as v15_new
from eqidv2_runtime_paths import LIVE_SIGNALS_DIR, RUNTIME_ROOT, runtime_dir
from live_v15_slot_snapshot_v15_new import (
    build_slot_snapshots,
    clear_rolling_cache,
    DEFAULT_SNAPSHOT_MAX_WORKERS,
    normalize_slot_ist,
    slot_key,
)


SHORT_SIGNAL_CSV_PATTERN = "signals_{}_v15_new_short.csv"
LONG_SIGNAL_CSV_PATTERN = "signals_{}_v15_new_long.csv"
DEFAULT_SNAPSHOT_WORKERS = int(DEFAULT_SNAPSHOT_MAX_WORKERS)
DEFAULT_SCAN_WORKERS = int(v15_new.DEFAULT_SCAN_WORKERS)
DEFAULT_SHARD_COUNT = 10
V15_NEW_DEFAULT_POSITION_SIZE_RS = float(
    __import__("os").getenv("EQIDV15_NEW_DEFAULT_POSITION_SIZE_RS", "20000")
)
START_TIME = base_v15.START_TIME
END_TIME = base_v15.dtime(15, 0)
HARD_STOP_TIME = base_v15.dtime(15, 30)


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
        if np.isfinite(out):
            return out
        return float(default)
    except Exception:
        return float(default)


def _session_summary_dir(runtime_root: Optional[Path | str] = None) -> Path:
    if runtime_root is None:
        out_dir = runtime_dir("live_v15_new_persistent")
    else:
        out_dir = Path(runtime_root) / "live_v15_new_persistent"
        out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _signal_csv_path(signal_day_str: str, side: str) -> Path:
    pattern = SHORT_SIGNAL_CSV_PATTERN if str(side).upper() == "SHORT" else LONG_SIGNAL_CSV_PATTERN
    return Path(LIVE_SIGNALS_DIR) / pattern.format(signal_day_str)


def _next_slot_after_v15_new(now: datetime) -> datetime:
    now = now.astimezone(base_v15.IST)
    today = now.date()
    start_dt = base_v15.IST.localize(datetime.combine(today, START_TIME))
    end_dt = base_v15.IST.localize(datetime.combine(today, END_TIME))

    if now <= start_dt:
        return start_dt
    if now > end_dt:
        tomorrow = today + timedelta(days=1)
        return base_v15.IST.localize(datetime.combine(tomorrow, START_TIME))

    minute = (now.minute // 15) * 15
    slot = now.replace(minute=minute, second=0, microsecond=0)
    if slot < now:
        slot += timedelta(minutes=15)
    if slot < start_dt:
        slot = start_dt
    if slot > end_dt:
        tomorrow = today + timedelta(days=1)
        slot = base_v15.IST.localize(datetime.combine(tomorrow, START_TIME))
    return slot


def _load_signals_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _load_slot_signal_frames(
    slot_ts: Any,
    *,
    runtime_root: Optional[Path | str],
    shard_count: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    slot_ist = normalize_slot_ist(slot_ts)
    short_frames: List[pd.DataFrame] = []
    long_frames: List[pd.DataFrame] = []
    for shard_id in range(1, int(shard_count) + 1):
        shard_dir = v15_new.shard_output_dir(slot_ist, shard_id, runtime_root)
        short_df = _load_signals_parquet(shard_dir / "short_signals.parquet")
        long_df = _load_signals_parquet(shard_dir / "long_signals.parquet")
        if not short_df.empty:
            short_frames.append(short_df)
        if not long_df.empty:
            long_frames.append(long_df)
    short_all = pd.concat(short_frames, ignore_index=True) if short_frames else pd.DataFrame()
    long_all = pd.concat(long_frames, ignore_index=True) if long_frames else pd.DataFrame()
    return short_all, long_all


def _write_side_signals_csv(
    signals_df: pd.DataFrame,
    *,
    side: str,
    signal_day_str: str,
) -> int:
    side_upper = str(side).upper()
    if signals_df is None or signals_df.empty:
        print(f"[V15_NEW {side_upper} CSV] scanned=0 written=0", flush=True)
        return 0

    if "side" in signals_df.columns:
        df_side = signals_df.loc[
            signals_df["side"].astype(str).str.upper().eq(side_upper)
        ].copy()
    else:
        df_side = signals_df.copy()
        df_side["side"] = side_upper
    if df_side.empty:
        print(f"[V15_NEW {side_upper} CSV] scanned={len(signals_df)} written=0", flush=True)
        return 0

    csv_path = _signal_csv_path(signal_day_str, side_upper)
    received_time = base_v15.now_ist().strftime("%Y-%m-%d %H:%M:%S%z")
    written = 0
    skipped_duplicate_key = 0
    skipped_duplicate_id = 0
    skipped_missing_time = 0

    with base_v15._locked_signal_csv(str(csv_path)):
        base_v15._ensure_signal_csv_schema(str(csv_path))
        existing_ids = base_v15._load_existing_ids(str(csv_path))
        existing_keys = base_v15._load_existing_signal_keys(str(csv_path))
        file_exists = csv_path.exists() and csv_path.stat().st_size > 0
        run_keys: set = set()

        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=base_v15.SIGNAL_CSV_COLUMNS,
                quoting=csv.QUOTE_ALL,
            )
            if not file_exists:
                writer.writeheader()

            for _, row in df_side.iterrows():
                ticker = str(row.get("ticker", "")).upper().strip()
                setup = str(row.get("setup", ""))
                bar_time_raw = row.get("bar_time_ist", "")
                bar_time_ts = base_v15._parse_ist_timestamp(str(bar_time_raw))
                if not ticker or bar_time_ts is None:
                    skipped_missing_time += 1
                    continue
                bar_time = str(bar_time_ts)
                dedupe_key = base_v15._signal_dedupe_key(ticker, side_upper, bar_time, setup)
                if dedupe_key in existing_keys or dedupe_key in run_keys:
                    skipped_duplicate_key += 1
                    continue
                signal_id = base_v15._generate_signal_id(ticker, side_upper, bar_time, setup)
                if signal_id in existing_ids:
                    skipped_duplicate_id += 1
                    continue

                entry_price = float(_safe_float(row.get("entry_price", np.nan), 0.0))
                stop_price = float(_safe_float(row.get("sl_price", np.nan), 0.0))
                target_price = float(_safe_float(row.get("target_price", np.nan), 0.0))
                notional = float(V15_NEW_DEFAULT_POSITION_SIZE_RS) * float(base_v15.INTRADAY_LEVERAGE)
                qty = max(1, int(notional / entry_price)) if entry_price > 0 else 1

                atr_pct = 0.0
                rsi_val = 0.0
                adx_val = 0.0
                impulse_type = ""
                diag: Dict[str, Any] = {}
                diag_raw = row.get("diagnostics_json", "")
                if isinstance(diag_raw, dict):
                    diag = dict(diag_raw)
                elif isinstance(diag_raw, str) and diag_raw.strip():
                    try:
                        diag = json.loads(diag_raw)
                    except Exception:
                        diag = {}
                if diag:
                    atr_pct_val = _safe_float(diag.get("atr_pct", np.nan), np.nan)
                    if np.isfinite(atr_pct_val):
                        atr_pct = float(atr_pct_val)
                    else:
                        atr_val = _safe_float(diag.get("atr", np.nan), np.nan)
                        close_val = _safe_float(diag.get("close", entry_price), np.nan)
                        if np.isfinite(atr_val) and np.isfinite(close_val) and close_val > 0:
                            atr_pct = float(atr_val / close_val)
                    rsi_val = float(_safe_float(diag.get("rsi", 0.0), 0.0))
                    adx_val = float(_safe_float(diag.get("adx", 0.0), 0.0))
                    impulse_type = str(diag.get("impulse_type", ""))

                out_row = {
                    "signal_id": signal_id,
                    "signal_datetime": bar_time,
                    "received_time": received_time,
                    "detected_time_ist": received_time,
                    "logtime_ist": received_time,
                    "ticker": ticker,
                    "side": side_upper,
                    "setup": setup,
                    "impulse_type": impulse_type,
                    "entry_price": round(entry_price, 2),
                    "stop_price": round(stop_price, 2),
                    "target_price": round(target_price, 2),
                    "quality_score": round(float(_safe_float(row.get("score", 0.0), 0.0)), 4),
                    "atr_pct": round(atr_pct, 6),
                    "rsi": round(rsi_val, 2),
                    "adx": round(adx_val, 2),
                    "quantity": int(qty),
                    "signal_entry_datetime_ist": bar_time,
                    "signal_bar_time_ist": bar_time,
                }
                writer.writerow(out_row)
                existing_ids.add(signal_id)
                existing_keys.add(dedupe_key)
                run_keys.add(dedupe_key)
                written += 1

    print(
        f"[V15_NEW {side_upper} CSV] scanned={len(df_side)} written={written} "
        f"skipped_dup_key={skipped_duplicate_key} skipped_dup_id={skipped_duplicate_id} "
        f"skipped_missing_time={skipped_missing_time} path={csv_path}",
        flush=True,
    )
    return written


def _run_slot_scan(
    slot_ts: Any,
    *,
    runtime_root: Optional[Path | str],
    snapshot_workers: int,
    scan_workers: int,
    shard_count: int,
    slot_bundle_root: Optional[Path | str] = None,
    prefer_slot_bundle: bool = False,
    write_live_csvs: bool = True,
) -> Dict[str, Any]:
    slot_ist = normalize_slot_ist(slot_ts)
    slot_runtime_root = Path(runtime_root) if runtime_root is not None else Path(RUNTIME_ROOT)
    slot_runtime_root.mkdir(parents=True, exist_ok=True)

    snapshot_meta = build_slot_snapshots(
        slot_ist,
        runtime_root=slot_runtime_root,
        shard_count=int(shard_count),
        max_workers=int(snapshot_workers),
        use_rolling_cache=True,
        slot_bundle_root_path=slot_bundle_root,
        prefer_slot_bundle=bool(prefer_slot_bundle),
        build_slot_context=True,
    )

    runner = Path(v15_new.__file__).resolve()
    python_exe = Path(sys.executable)
    scan_started = time.perf_counter()
    proc_entries: List[Tuple[subprocess.Popen, Any, str, Path]] = []
    logs_dir = v15_new.output_slot_dir(slot_ist, slot_runtime_root) / "process_logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    for shard_id in range(1, int(shard_count) + 1):
        shard_label = f"{shard_id:02d}"
        log_path = logs_dir / f"shard_{shard_label}.log"
        fh = open(log_path, "w", encoding="utf-8")
        cmd = [
            str(python_exe),
            str(runner),
            "--slot",
            slot_ist.isoformat(),
            "--shard-id",
            str(shard_id),
            "--runtime-root",
            str(slot_runtime_root),
            "--scan-workers",
            str(int(scan_workers)),
        ]
        proc = subprocess.Popen(
            cmd,
            stdout=fh,
            stderr=subprocess.STDOUT,
            cwd=str(runner.parent.parent),
        )
        proc_entries.append((proc, fh, shard_label, log_path))

    shard_results: List[Dict[str, Any]] = []
    for proc, fh, shard_label, log_path in proc_entries:
        rc = proc.wait()
        fh.close()
        summary_path = (
            v15_new.shard_output_dir(slot_ist, shard_label, slot_runtime_root) / "summary.json"
        )
        if summary_path.exists():
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        else:
            payload = {
                "shard_id": shard_label,
                "error": f"missing summary, rc={rc}",
                "log_path": str(log_path),
            }
        payload["returncode"] = int(rc)
        shard_results.append(payload)

    scan_elapsed = time.perf_counter() - scan_started
    short_df, long_df = _load_slot_signal_frames(
        slot_ist,
        runtime_root=slot_runtime_root,
        shard_count=int(shard_count),
    )
    signal_day_str = slot_ist.strftime("%Y-%m-%d")
    if bool(write_live_csvs):
        short_written = _write_side_signals_csv(short_df, side="SHORT", signal_day_str=signal_day_str)
        long_written = _write_side_signals_csv(long_df, side="LONG", signal_day_str=signal_day_str)
    else:
        short_written = 0
        long_written = 0

    shard_elapsed = [
        float(item.get("elapsed_sec", 0.0))
        for item in shard_results
        if "elapsed_sec" in item
    ]
    failed = [item for item in shard_results if int(item.get("returncode", 0)) != 0]
    summary = {
        "slot_ist": slot_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
        "slot_key": slot_key(slot_ist),
        "runtime_root": str(slot_runtime_root),
        "snapshot_total_elapsed_sec": float(snapshot_meta.get("total_elapsed_sec", 0.0)),
        "snapshot_cache": dict(snapshot_meta.get("snapshot_cache", {}) or {}),
        "scan_batch_elapsed_sec": round(scan_elapsed, 3),
        "scan_shard_avg_elapsed_sec": round(sum(shard_elapsed) / len(shard_elapsed), 3) if shard_elapsed else 0.0,
        "scan_shard_max_elapsed_sec": round(max(shard_elapsed), 3) if shard_elapsed else 0.0,
        "short_rows": int(0 if short_df is None else len(short_df)),
        "long_rows": int(0 if long_df is None else len(long_df)),
        "short_written": int(short_written),
        "long_written": int(long_written),
        "failed_shards": failed,
        "shard_results": shard_results,
        "end_to_end_elapsed_sec": round(float(snapshot_meta.get("total_elapsed_sec", 0.0)) + scan_elapsed, 3),
    }
    summary_path = _session_summary_dir(slot_runtime_root) / f"slot_summary_{slot_key(slot_ist)}.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def _run_replay_slots(
    slots: List[str],
    *,
    runtime_root: Optional[Path | str],
    snapshot_workers: int,
    scan_workers: int,
    shard_count: int,
    slot_bundle_root: Optional[Path | str] = None,
    prefer_slot_bundle: bool = False,
    write_live_csvs: bool = False,
) -> Dict[str, Any]:
    clear_rolling_cache()
    summaries = []
    for slot in slots:
        slot_ist = normalize_slot_ist(slot)
        print(f"[REPLAY] slot={slot_ist.strftime('%Y-%m-%d %H:%M:%S%z')}", flush=True)
        summary = _run_slot_scan(
            slot_ist,
            runtime_root=runtime_root,
            snapshot_workers=int(snapshot_workers),
            scan_workers=int(scan_workers),
            shard_count=int(shard_count),
            slot_bundle_root=slot_bundle_root,
            prefer_slot_bundle=bool(prefer_slot_bundle),
            write_live_csvs=bool(write_live_csvs),
        )
        summaries.append(summary)
        print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
    final_summary = {
        "runtime_root": str(Path(runtime_root) if runtime_root is not None else Path(RUNTIME_ROOT)),
        "slots": summaries,
    }
    out_path = _session_summary_dir(runtime_root) / "replay_summary_v15_new_persistent.json"
    out_path.write_text(json.dumps(final_summary, indent=2, sort_keys=True), encoding="utf-8")
    return final_summary


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Persistent v15_new live scanner that keeps the rolling snapshot cache warm across slots."
    )
    ap.add_argument("--runtime-root", default=str(RUNTIME_ROOT))
    ap.add_argument("--snapshot-workers", type=int, default=DEFAULT_SNAPSHOT_WORKERS)
    ap.add_argument("--scan-workers", type=int, default=DEFAULT_SCAN_WORKERS)
    ap.add_argument("--shard-count", type=int, default=DEFAULT_SHARD_COUNT)
    ap.add_argument("--replay-slots", nargs="*", default=None)
    ap.add_argument("--slot-bundle-root", default=None)
    ap.add_argument("--prefer-slot-bundle", action="store_true")
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    runtime_root = Path(args.runtime_root)
    runtime_root.mkdir(parents=True, exist_ok=True)

    if args.replay_slots:
        final_summary = _run_replay_slots(
            [str(v) for v in args.replay_slots],
            runtime_root=runtime_root,
            snapshot_workers=int(args.snapshot_workers),
            scan_workers=int(args.scan_workers),
            shard_count=int(args.shard_count),
            slot_bundle_root=args.slot_bundle_root,
            prefer_slot_bundle=bool(args.prefer_slot_bundle),
        )
        print(json.dumps(final_summary, indent=2, sort_keys=True), flush=True)
        return

    clear_rolling_cache()
    tickers = list(base_v15.list_tickers_15m())
    holidays = base_v15._read_holidays_safe()
    print("[LIVE] EQIDV2 v15_new persistent scanner", flush=True)
    print(f"[INFO] runtime_root={runtime_root}", flush=True)
    print(
        f"[INFO] snapshot_workers={int(args.snapshot_workers)} scan_workers={int(args.scan_workers)} "
        f"shard_count={int(args.shard_count)}",
        flush=True,
    )

    while True:
        now = base_v15.now_ist()
        base_v15._touch_runtime_status("RUNNING", phase="LOOP_V15_NEW")
        base_v15._touch_runtime_heartbeat("RUNNING", phase="LOOP_V15_NEW")

        if now.time() >= HARD_STOP_TIME:
            base_v15._touch_runtime_status("STOPPED_AFTER_CUTOFF", phase="HARD_STOP_V15_NEW")
            base_v15._touch_runtime_heartbeat("STOPPED", phase="HARD_STOP_V15_NEW")
            print("[STOP] Hard-stop reached for today. Exiting.", flush=True)
            return

        if not base_v15.is_trading_day_safe(now.date(), holidays):
            nxt = base_v15._next_trading_day_start(now, holidays)
            clear_rolling_cache()
            print(f"[SKIP] Not a trading day ({now.date()}). Sleeping until {base_v15._fmt_ist_dt(nxt)}.", flush=True)
            base_v15._sleep_until(nxt)
            holidays = base_v15._read_holidays_safe()
            continue

        slot = _next_slot_after_v15_new(now)
        if slot.date() != now.date():
            clear_rolling_cache()
            print(f"[WAIT] Next slot is tomorrow {slot}. Sleeping.", flush=True)
            base_v15._sleep_until(slot)
            holidays = base_v15._read_holidays_safe()
            continue

        if now < slot:
            print(f"[WAIT] Sleeping until slot {slot.strftime('%Y-%m-%d %H:%M:%S%z')}", flush=True)
            base_v15._sleep_until(slot)

        now = base_v15.now_ist()
        if now.time() > END_TIME:
            nxt = base_v15._next_trading_day_start(now, holidays)
            clear_rolling_cache()
            print(f"[DONE] Past END_TIME. Sleeping until {base_v15._fmt_ist_dt(nxt)}.", flush=True)
            base_v15._sleep_until(nxt)
            holidays = base_v15._read_holidays_safe()
            continue

        ready, ratio, waited, checked = base_v15._wait_for_slot_data_ready(slot, tickers)
        print(
            f"[WAIT] slot={slot.strftime('%H:%M')} ready={ready} fresh_ratio={ratio:.2f} "
            f"waited={waited:.1f}s checked={checked}",
            flush=True,
        )

        slot_started = time.perf_counter()
        base_v15._touch_runtime_status("RUNNING", phase="SCAN_V15_NEW", slot=slot.strftime("%H:%M"))
        base_v15._touch_runtime_heartbeat("RUNNING", phase="SCAN_V15_NEW", slot=slot.strftime("%H:%M"))
        summary = _run_slot_scan(
            slot,
            runtime_root=runtime_root,
            snapshot_workers=int(args.snapshot_workers),
            scan_workers=int(args.scan_workers),
            shard_count=int(args.shard_count),
        )
        slot_elapsed = time.perf_counter() - slot_started
        base_v15._touch_runtime_status(
            "RUNNING",
            phase="SCAN_DONE_V15_NEW",
            slot=slot.strftime("%H:%M"),
            elapsed_sec=f"{slot_elapsed:.3f}",
        )
        base_v15._touch_runtime_heartbeat(
            "RUNNING",
            phase="SCAN_DONE_V15_NEW",
            slot=slot.strftime("%H:%M"),
            elapsed_sec=f"{slot_elapsed:.3f}",
        )
        print(json.dumps(summary, indent=2, sort_keys=True), flush=True)

        next_slot = slot + timedelta(minutes=15)
        now_after = base_v15.now_ist()
        if now_after < next_slot:
            time.sleep(1.0)


if __name__ == "__main__":
    main()
