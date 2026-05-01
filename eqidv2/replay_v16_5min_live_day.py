from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

import eqidv2_live_combined_analyser_csv_v16_5min as live_v16


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay the v16 5-minute live scanner for a full IST trading day."
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Target IST date in YYYY-MM-DD format. Default=today IST.",
    )
    parser.add_argument(
        "--write-live-signals",
        action="store_true",
        help="Write replayed signals into the normal live signal CSVs.",
    )
    parser.add_argument(
        "--replace-live-signals",
        action="store_true",
        help="Backup any existing daily v16 signal CSVs before writing replay results.",
    )
    parser.add_argument(
        "--start-slot",
        default=None,
        help="Optional inclusive start slot in HH:MM IST.",
    )
    parser.add_argument(
        "--end-slot",
        default=None,
        help="Optional inclusive end slot in HH:MM IST.",
    )
    return parser.parse_args()


def _target_day(args: argparse.Namespace) -> datetime.date:
    if args.date:
        return pd.Timestamp(args.date).date()
    return live_v16._now_ist().date()


def _iter_slots(day_value: datetime.date) -> List[datetime]:
    start = live_v16.IST.localize(datetime.combine(day_value, live_v16.START_TIME))
    end = live_v16.IST.localize(datetime.combine(day_value, live_v16.END_TIME))
    slots: List[datetime] = []
    current = start
    while current <= end:
        slots.append(current)
        current += timedelta(minutes=live_v16.SLOT_MINUTES)
    return slots


def _filter_slots(slots: List[datetime], start_slot: str | None, end_slot: str | None) -> List[datetime]:
    def _to_minutes(raw: str | None) -> int | None:
        if not raw:
            return None
        hh, mm = str(raw).strip().split(":", 1)
        return int(hh) * 60 + int(mm)

    start_m = _to_minutes(start_slot)
    end_m = _to_minutes(end_slot)
    out: List[datetime] = []
    for slot in slots:
        minute_value = slot.hour * 60 + slot.minute
        if start_m is not None and minute_value < start_m:
            continue
        if end_m is not None and minute_value > end_m:
            continue
        out.append(slot)
    return out


def _backup_existing_signal_files(signal_day_str: str) -> List[str]:
    backed_up: List[str] = []
    stamp = live_v16._now_ist().strftime("%Y%m%d_%H%M%S")
    for side in ("SHORT", "LONG"):
        path = live_v16._signal_csv_path(signal_day_str, side)
        if not path.exists():
            continue
        backup = path.with_name(f"{path.stem}.bak_replay_{stamp}{path.suffix}")
        path.replace(backup)
        backed_up.append(str(backup))
    return backed_up


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, pd.Timestamp)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def _write_summary(signal_day_str: str, payload: Dict[str, Any]) -> Path:
    out_path = Path(live_v16.RUNTIME_LIVE_SIGNALS_DIR) / f"replay_summary_{signal_day_str}_v16_5min.json"
    out_path.write_text(
        json.dumps(payload, indent=2, default=_json_safe),
        encoding="utf-8",
    )
    return out_path


def _write_failed_summary(
    signal_day_str: str,
    *,
    status: str,
    error: str,
    traceback_text: str,
    partial_payload: Dict[str, Any] | None = None,
) -> Path:
    payload = dict(partial_payload or {})
    payload["date"] = signal_day_str
    payload["status"] = status
    payload["error"] = error
    payload["traceback"] = traceback_text
    payload["updated_at_ist"] = live_v16._now_ist().strftime("%Y-%m-%d %H:%M:%S%z")
    return _write_summary(signal_day_str, payload)


def run_replay_for_date(
    target_day: datetime.date,
    *,
    write_live_signals: bool,
    replace_live_signals: bool,
    start_slot: str | None = None,
    end_slot: str | None = None,
) -> Dict[str, Any]:
    if write_live_signals:
        live_v16.DIRECT_SIGNAL_CSV_MODE = "direct"
    signal_day_str = target_day.strftime("%Y-%m-%d")
    slots = _filter_slots(_iter_slots(target_day), start_slot, end_slot)
    tickers = live_v16._list_tickers_5m()
    short_cfg, long_cfg = live_v16._build_v16_cfgs()

    backed_up: List[str] = []
    if write_live_signals and replace_live_signals:
        backed_up = _backup_existing_signal_files(signal_day_str)

    live_v16.clear_slot_snapshot_cache()

    slot_rows: List[Dict[str, Any]] = []
    total_short_raw = 0
    total_long_raw = 0
    total_short_written = 0
    total_long_written = 0
    skipped_slots = 0
    summary_path = Path(live_v16.RUNTIME_LIVE_SIGNALS_DIR) / f"replay_summary_{signal_day_str}_v16_5min.json"

    def _progress_payload(status: str) -> Dict[str, Any]:
        return {
            "date": signal_day_str,
            "status": status,
            "slots": len(slots),
            "tickers": len(tickers),
            "write_live_signals": bool(write_live_signals),
            "replace_live_signals": bool(replace_live_signals),
            "start_slot": start_slot,
            "end_slot": end_slot,
            "backed_up_files": backed_up,
            "total_raw_short": int(total_short_raw),
            "total_raw_long": int(total_long_raw),
            "total_written_short": int(total_short_written),
            "total_written_long": int(total_long_written),
            "skipped_slots": int(skipped_slots),
            "slot_rows": slot_rows,
            "updated_at_ist": live_v16._now_ist().strftime("%Y-%m-%d %H:%M:%S%z"),
        }

    print(
        f"[REPLAY] Starting v16 5min full-day replay for {signal_day_str} "
        f"(slots={len(slots)}, write_live_signals={write_live_signals}, replace={replace_live_signals})",
        flush=True,
    )
    _write_summary(signal_day_str, _progress_payload("running"))

    for slot in slots:
        print(f"[REPLAY_SLOT] {slot.strftime('%Y-%m-%d %H:%M:%S%z')}", flush=True)
        prebuilt_snapshot_meta: Dict[str, Any] | None = None
        slot_payload: Dict[str, Any] = {}

        if live_v16.USE_SLOT_SNAPSHOTS:
            prebuilt_snapshot_meta = live_v16.build_slot_snapshots(
                slot,
                shard_count=live_v16.SCAN_SHARDS,
                tail_rows=live_v16.TAIL_ROWS,
                max_workers=live_v16.SNAPSHOT_MAX_WORKERS,
                use_rolling_cache=live_v16.USE_SNAPSHOT_ROLLING_CACHE,
                build_slot_context=True,
            )
            allow_long, allow_short, rs_pct, slot_payload = live_v16._load_snapshot_slot_context(slot)
        else:
            allow_long, allow_short, rs_pct = live_v16._compute_nifty_rs_at_slot(slot)

        slot_info: Dict[str, Any] = {
            "slot": slot.strftime("%Y-%m-%d %H:%M:%S%z"),
            "allow_long": bool(allow_long),
            "allow_short": bool(allow_short),
            "rs_pct": float(rs_pct),
            "raw_short": 0,
            "raw_long": 0,
            "short_written": 0,
            "long_written": 0,
            "status": "scanned",
        }

        if not allow_long and not allow_short:
            skipped_slots += 1
            slot_info["status"] = "skipped_no_context"
            slot_info["reason"] = live_v16._blocked_nifty_context_message(slot, slot_payload)
            print(slot_info["reason"], flush=True)
            slot_rows.append(slot_info)
            _write_summary(signal_day_str, _progress_payload("running"))
            continue

        short_rows, long_rows, scan_meta = live_v16._scan_slot(
            slot,
            short_cfg,
            long_cfg,
            allow_long,
            allow_short,
            tickers=tickers,
            prebuilt_snapshot_meta=prebuilt_snapshot_meta,
        )
        short_rows, long_rows = live_v16._apply_rs_filter_dicts(short_rows, long_rows, allow_long, allow_short)
        short_rows, long_rows = live_v16._apply_v16_filters_to_dicts(short_rows, long_rows)

        slot_info["raw_short"] = len(short_rows)
        slot_info["raw_long"] = len(long_rows)
        slot_info["scan_meta"] = scan_meta

        total_short_raw += len(short_rows)
        total_long_raw += len(long_rows)

        if write_live_signals:
            short_written = live_v16._write_side_signals_csv(short_rows, "SHORT", signal_day_str)
            long_written = live_v16._write_side_signals_csv(long_rows, "LONG", signal_day_str)
            slot_info["short_written"] = int(short_written)
            slot_info["long_written"] = int(long_written)
            total_short_written += int(short_written)
            total_long_written += int(long_written)

        slot_rows.append(slot_info)
        _write_summary(signal_day_str, _progress_payload("running"))

    summary = _progress_payload("completed")
    summary_path = _write_summary(signal_day_str, summary)
    summary["summary_path"] = str(summary_path)
    print(
        f"[REPLAY] Done for {signal_day_str}: raw_short={total_short_raw} raw_long={total_long_raw} "
        f"written_short={total_short_written} written_long={total_long_written} "
        f"skipped_slots={skipped_slots} summary={summary_path}",
        flush=True,
    )
    return summary


def main() -> None:
    args = _parse_args()
    target_day = _target_day(args)
    signal_day_str = target_day.strftime("%Y-%m-%d")
    try:
        run_replay_for_date(
            target_day,
            write_live_signals=bool(args.write_live_signals),
            replace_live_signals=bool(args.replace_live_signals),
            start_slot=args.start_slot,
            end_slot=args.end_slot,
        )
    except Exception as exc:
        traceback_text = traceback.format_exc()
        _write_failed_summary(
            signal_day_str,
            status="failed",
            error=repr(exc),
            traceback_text=traceback_text,
            partial_payload={
                "write_live_signals": bool(args.write_live_signals),
                "replace_live_signals": bool(args.replace_live_signals),
                "start_slot": args.start_slot,
                "end_slot": args.end_slot,
            },
        )
        raise


if __name__ == "__main__":
    mp.freeze_support()
    main()
