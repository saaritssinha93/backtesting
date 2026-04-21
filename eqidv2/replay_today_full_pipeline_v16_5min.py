"""
EQIDV2 V16 5min — Full-Pipeline Intraday Replay (Option B)

Replays today's live pipeline end-to-end by calling the REAL live engine
entry points (SE.scan → SE._write_pending_pool → PF fetch/marker → DE
_run_detection_cycle_live_parity) in strict slot order, with a frozen
clock per slot and all Kite I/O stubbed (the parquet is already the
source of truth).

The goal is a 1:1 replay of live behaviour from today's parquet, not a
normal vectorized backtest. Pool monotonicity, PF_VERIFIED handshakes,
DE parity filter stack, trigger-bar drift checks and lifecycle events
are all exercised exactly as in production.

Outputs (under eqidv2/replay_out_full_pipeline/<date>/):
  pool/pending_signals_<date>_v16_5min.{json,csv}
  pool/pool_lifecycle_<date>_v16_5min.jsonl
  pool/signals_<date>_v16_5min_long.csv
  pool/signals_<date>_v16_5min_short.csv
  ready/<YYYYMMDD_HHMM>.ready
  replay_slot_timeline.csv
  replay_summary.txt
  replay_driver.log
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# CLI — we need the output dirs BEFORE any eqidv2 import so env overrides
# route RUNTIME_LIVE_SIGNALS_DIR / SLOT_READY_PENDING_DIR to replay-scoped
# paths rather than live prod.
# ---------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="V16 5min full-pipeline replay (Option B)")
    p.add_argument("--date",  default=None, help="YYYY-MM-DD (defaults to today IST)")
    p.add_argument("--start", default="09:15", help="HH:MM start slot inclusive (default 09:15)")
    p.add_argument("--end",   default="15:30", help="HH:MM end slot inclusive (default 15:30)")
    p.add_argument("--out-dir", default=None, help="Output dir (default eqidv2/replay_out_full_pipeline/<date>)")
    p.add_argument("--scan-shards",  default="16", help="Scan shards (default 16)")
    p.add_argument("--scan-workers", default="16", help="Scan max workers (default 16)")
    return p.parse_args()


_ARGS = _parse_args()

# Resolve date first (today in IST if not supplied). We avoid importing pytz at
# this point; naive local-date works since this just determines the folder name
# and the replay clock anchor (which is overwritten from a tz-aware value later).
import pytz  # lightweight, no eqidv2 side effects
_IST = pytz.timezone("Asia/Kolkata")
_TODAY_IST = datetime.now(_IST).date()
_REPLAY_DATE = (
    datetime.strptime(_ARGS.date, "%Y-%m-%d").date() if _ARGS.date else _TODAY_IST
)
_DATE_STR = _REPLAY_DATE.strftime("%Y-%m-%d")

_SCRIPT_DIR = Path(__file__).resolve().parent
_DEFAULT_OUT = _SCRIPT_DIR / "replay_out_full_pipeline" / _DATE_STR
_OUT_ROOT = Path(_ARGS.out_dir) if _ARGS.out_dir else _DEFAULT_OUT
_POOL_DIR = _OUT_ROOT / "pool"
_READY_DIR = _OUT_ROOT / "ready"
_STATUS_DIR = _OUT_ROOT / "status"
for d in (_OUT_ROOT, _POOL_DIR, _READY_DIR, _STATUS_DIR):
    d.mkdir(parents=True, exist_ok=True)

_LOG_PATH = _OUT_ROOT / "replay_driver.log"
_TIMELINE_PATH = _OUT_ROOT / "replay_slot_timeline.csv"
_SUMMARY_PATH = _OUT_ROOT / "replay_summary.txt"

# ---------------------------------------------------------------------------
# Env overrides — must happen BEFORE importing eqidv2_runtime_paths (imported
# transitively by SE/PF/DE). eqidv2_runtime_paths reads these env vars at
# import time.
# ---------------------------------------------------------------------------
# Replay output redirection — these point at our isolated replay tree.
os.environ["EQIDV2_LIVE_SIGNALS_DIR"] = str(_POOL_DIR)
os.environ["EQIDV2_SLOT_READY_PENDING_DIR"] = str(_READY_DIR)
os.environ["EQIDV2_RUNTIME_STATUS_DIR"] = str(_STATUS_DIR)

# Live-engine env vars (mirror bat/run_eqidv2_*_v16_5min.bat) — the runtime root
# is read by eqidv2_runtime_paths, and DATA_5M_DIR MUST point at the live
# intraday parquet (`*_eq_live`); the default `*_eq` is the stale EOD copy.
os.environ.setdefault("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2")
os.environ.setdefault(
    "EQIDV2_DATA_5M_DIR", r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"
)
os.environ.setdefault(
    "EQIDV2_CACHE_5MIN_DIR", r"C:\TradingData\eqidv2\stocks_cache_5min_eq_live"
)
os.environ.setdefault("EQIDV16_5MIN_SHORT_STOP_PCT", "0.0075")
os.environ.setdefault("EQIDV16_5MIN_LONG_STOP_PCT", "0.0075")
os.environ.setdefault("EQIDV16_5MIN_SHORT_TARGET_PCT", "0.0100")
os.environ.setdefault("EQIDV16_5MIN_LONG_TARGET_PCT", "0.0100")
os.environ.setdefault("EQIDV16_5MIN_DEFAULT_POSITION_SIZE_RS", "10000")
os.environ.setdefault("EQIDV16_5MIN_SLOT_START_OFFSET_SECONDS", "45")
os.environ.setdefault("EQIDV16_5MIN_SCAN_SHARDS", str(_ARGS.scan_shards))
os.environ.setdefault("EQIDV16_5MIN_SCAN_MAX_WORKERS", str(_ARGS.scan_workers))
# Keep DE parity mode on (default) — we explicitly use the parity cycle.
os.environ.setdefault("EQIDV2_DETECTION_PARITY_MODE", "1")

# ---------------------------------------------------------------------------
# Tee stdout/stderr to replay_driver.log
# ---------------------------------------------------------------------------
class _Tee:
    def __init__(self, *streams):
        self._s = streams
    def write(self, data):
        for s in self._s:
            try:
                s.write(data); s.flush()
            except Exception:
                pass
    def flush(self):
        for s in self._s:
            try: s.flush()
            except Exception: pass

_log_fh = open(_LOG_PATH, "a", encoding="utf-8", buffering=1)
sys.stdout = _Tee(sys.__stdout__, _log_fh)  # type: ignore[assignment]
sys.stderr = _Tee(sys.__stderr__, _log_fh)  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Now safe to import the live engine modules
# ---------------------------------------------------------------------------
sys.path.insert(0, str(_SCRIPT_DIR))

import eqidv2_live_combined_analyser_csv_v15 as base_v15  # noqa: E402
import eqidv2_live_combined_analyser_csv_v16_5min as live_v16  # noqa: E402
import eqidv2_signal_engine_v16_5min as SE  # noqa: E402
import eqidv2_pending_data_fetcher_v16_5min as PF  # noqa: E402
import eqidv2_detection_engine_v16_5min as DE  # noqa: E402

# ---------------------------------------------------------------------------
# Frozen-clock monkey-patch.  Every live module calls base_v15.now_ist()
# (either directly or via its local _now_ist wrapper).  Swapping the
# attribute on base_v15 flips the clock for SE / PF / DE in one shot.
# ---------------------------------------------------------------------------
_FROZEN_CLOCK: Dict[str, Optional[datetime]] = {"ts": None}
_REAL_NOW_IST = base_v15.now_ist


def _replay_now_ist() -> datetime:
    ts = _FROZEN_CLOCK["ts"]
    if ts is None:
        return _REAL_NOW_IST()
    return ts


def _set_clock(ts: datetime) -> None:
    _FROZEN_CLOCK["ts"] = ts


def _release_clock() -> None:
    _FROZEN_CLOCK["ts"] = None


base_v15.now_ist = _replay_now_ist  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Bypass Kite.  PF._fetch_pending_tickers calls scheduler.run_update_5m_once
# which does real Kite HTTP.  In replay, the parquet is already populated,
# so we stub the call to claim "all verified, zero failures".
# ---------------------------------------------------------------------------
_REAL_RUN_UPDATE_5M_ONCE = getattr(PF.scheduler, "run_update_5m_once", None)


def _stub_run_update_5m_once(*args, **kwargs) -> Dict[str, Any]:
    return {
        "verification_failed_count": 0,
        "verification_failure_sample": [],
        "fetched": 0,
    }


PF.scheduler.run_update_5m_once = _stub_run_update_5m_once  # type: ignore[attr-defined]

# Likewise, PF.time.sleep is used for the B3 backoff (exception retry) — we
# never trigger that branch with the stub, so no further patch needed.  SE
# also calls time.sleep inside _wait_for_slot_data_ready; we bypass that
# entirely by calling _scan_slot directly (the parquet is known-present).

# ---------------------------------------------------------------------------
# Windows transient-lock retry for SE._write_pending_state_atomic.
# Antivirus / file-indexer can briefly hold a handle on the freshly written
# .tmp or .json, causing os.replace to raise PermissionError [WinError 5].
# Production runs only write once per slot and tolerate the rare retry; in
# replay we hammer 76 slots back-to-back, so wrap with a short retry loop.
# ---------------------------------------------------------------------------
_REAL_WRITE_PENDING_STATE_ATOMIC = SE._write_pending_state_atomic


def _retrying_write_pending_state_atomic(state, date_str):
    last_err = None
    for attempt in range(6):
        try:
            return _REAL_WRITE_PENDING_STATE_ATOMIC(state, date_str)
        except PermissionError as e:
            last_err = e
            time.sleep(0.25 * (attempt + 1))
    raise last_err  # type: ignore[misc]


SE._write_pending_state_atomic = _retrying_write_pending_state_atomic  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _parse_hhmm(s: str) -> dtime:
    hh, mm = s.split(":")
    return dtime(int(hh), int(mm))


def _ist_dt(d, t: dtime) -> datetime:
    return _IST.localize(datetime.combine(d, t))


def _iter_slots(start_ist: datetime, end_ist: datetime) -> List[datetime]:
    out = []
    cur = start_ist
    while cur <= end_ist:
        out.append(cur)
        cur = cur + timedelta(minutes=SE.SLOT_MINUTES)
    return out


def _count_rows(path: Path) -> int:
    try:
        if not path.exists():
            return 0
        with open(path, "r", encoding="utf-8") as f:
            return max(0, sum(1 for _ in f) - 1)
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Per-slot replay
# ---------------------------------------------------------------------------
def _replay_slot(
    slot: datetime,
    short_cfg_se,
    long_cfg_se,
    short_cfg_de,
    long_cfg_de,
    tickers: List[str],
) -> Dict[str, Any]:
    date_str = slot.strftime("%Y-%m-%d")
    slot_label = slot.strftime("%H:%M")
    t_wall0 = time.perf_counter()
    result: Dict[str, Any] = {
        "slot": slot_label,
        "raw_short": 0,
        "raw_long": 0,
        "pool_added": 0,
        "pool_deduped": 0,
        "pool_total_after": 0,
        "pf_pending_tickers": 0,
        "pf_ready_tickers": 0,
        "pf_marker_written": False,
        "de_cycle_result": "",
        "de_long_csv_rows": 0,
        "de_short_csv_rows": 0,
        "elapsed_sec": 0.0,
        "error": "",
    }

    long_csv = _POOL_DIR / f"signals_{date_str}_v16_5min_long.csv"
    short_csv = _POOL_DIR / f"signals_{date_str}_v16_5min_short.csv"
    long_before = _count_rows(long_csv)
    short_before = _count_rows(short_csv)

    try:
        # ---------- Stage 1 — SE scan + pool write (clock = T + 45s) ----------
        _set_clock(slot + timedelta(seconds=SE.SLOT_START_OFFSET_SECONDS))
        SE.clear_slot_snapshot_cache()
        SE._reset_trigger_bar_cache()

        rs_pct, _allow_long, _allow_short = SE._compute_nifty_rs_at_slot(slot)
        short_rows, long_rows, _scan_meta = SE._scan_slot(
            slot,
            short_cfg_se,
            long_cfg_se,
            tickers=tickers,
            prebuilt_snapshot_meta=None,
        )
        result["raw_short"] = len(short_rows)
        result["raw_long"] = len(long_rows)

        added, deduped, total_pending = SE._write_pending_pool(
            short_rows, long_rows, slot, rs_pct, date_str
        )
        result["pool_added"] = added
        result["pool_deduped"] = deduped
        result["pool_total_after"] = total_pending

        print(
            f"[REPLAY_SE] slot={slot_label} raw_short={len(short_rows)} "
            f"raw_long={len(long_rows)} added={added} deduped={deduped} "
            f"pool_total={total_pending}",
            flush=True,
        )

        # ---------- Stage 2 — PF fetch + ready marker (clock = T + 47s) ----------
        _set_clock(slot + timedelta(seconds=SE.SLOT_START_OFFSET_SECONDS + 2))
        pending_tickers, latest_source_slot = PF._get_pending_fetch_targets()
        result["pf_pending_tickers"] = len(pending_tickers)

        if pending_tickers:
            deduplicated = list(dict.fromkeys(pending_tickers))
            full_token_cache = PF._load_token_cache()
            token_map = {t: full_token_cache[t] for t in deduplicated if t in full_token_cache}
            fetch_result = PF._fetch_pending_tickers(
                deduplicated, token_map, required_ready_slot=latest_source_slot
            )
            ready_tickers = list(fetch_result.get("ready_tickers", []) or [])
            missing_tokens = list(fetch_result.get("missing_tokens", []) or [])
            if missing_tokens:
                PF._mark_missing_token_pending_signals(missing_tokens)
            marker_ready = bool(fetch_result.get("marker_ready", False))
            ready_slot = fetch_result.get("required_ready_slot")
            if marker_ready and ready_tickers and isinstance(ready_slot, datetime):
                PF._write_ready_marker(
                    ready_slot,
                    ready_tickers,
                    verify_failed_sample=list(fetch_result.get("verify_failed_sample", []) or []),
                )
                result["pf_marker_written"] = True
            result["pf_ready_tickers"] = len(ready_tickers)

            # C3 — older-slot marker pass (matches live main loop)
            try:
                slot_map = PF._get_pending_source_slot_map()
                PF._write_older_slot_markers(slot_map, latest_source_slot)
            except Exception as exc:
                print(f"[WARN] older-slot marker pass failed: {exc}", flush=True)

            print(
                f"[REPLAY_PF] slot={slot_label} pending={len(deduplicated)} "
                f"ready={len(ready_tickers)} "
                f"marker={'written' if result['pf_marker_written'] else 'withheld'}",
                flush=True,
            )
        else:
            print(f"[REPLAY_PF] slot={slot_label} pending=0 (no pending pool yet)", flush=True)

        # ---------- Stage 3 — DE parity cycle (clock = T + 49s) ----------
        _set_clock(slot + timedelta(seconds=SE.SLOT_START_OFFSET_SECONDS + 4))
        cycle_result = DE._run_detection_cycle_live_parity(short_cfg_de, long_cfg_de)
        result["de_cycle_result"] = str(cycle_result or "")
        result["de_long_csv_rows"] = max(0, _count_rows(long_csv) - long_before)
        result["de_short_csv_rows"] = max(0, _count_rows(short_csv) - short_before)

        print(
            f"[REPLAY_DE] slot={slot_label} result={cycle_result} "
            f"new_long_csv={result['de_long_csv_rows']} "
            f"new_short_csv={result['de_short_csv_rows']}",
            flush=True,
        )

    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        print(f"[ERROR] slot={slot_label}: {exc}", flush=True)
        traceback.print_exc()

    result["elapsed_sec"] = round(time.perf_counter() - t_wall0, 2)
    return result


# ---------------------------------------------------------------------------
# Main driver
# ---------------------------------------------------------------------------
def main() -> int:
    print("=" * 72)
    print("EQIDV2 V16 5min — Full-Pipeline Intraday Replay (Option B)")
    print(f"  DATE         : {_DATE_STR}")
    print(f"  START / END  : {_ARGS.start} → {_ARGS.end}")
    print(f"  OUT_DIR      : {_OUT_ROOT}")
    print(f"  POOL_DIR     : {_POOL_DIR}")
    print(f"  READY_DIR    : {_READY_DIR}")
    print(f"  DATA_5M_DIR  : {live_v16.DIR_5M if hasattr(live_v16, 'DIR_5M') else 'n/a'}")
    print(f"  SCAN_SHARDS  : {os.environ.get('EQIDV16_5MIN_SCAN_SHARDS')}")
    print(f"  SCAN_WORKERS : {os.environ.get('EQIDV16_5MIN_SCAN_MAX_WORKERS')}")
    print("=" * 72)

    start_t = _parse_hhmm(_ARGS.start)
    end_t = _parse_hhmm(_ARGS.end)
    start_ist = _ist_dt(_REPLAY_DATE, start_t)
    end_ist = _ist_dt(_REPLAY_DATE, end_t)
    slots = _iter_slots(start_ist, end_ist)
    print(f"[INFO] Replaying {len(slots)} slots from {start_ist} to {end_ist}", flush=True)

    short_cfg_se, long_cfg_se = SE._build_v16_cfgs()
    short_cfg_de, long_cfg_de = live_v16._build_v16_cfgs()
    tickers = SE._list_tickers_5m()
    print(f"[INFO] Universe: {len(tickers)} tickers from {SE.DIR_5M}", flush=True)

    timeline_fields = [
        "slot", "raw_short", "raw_long", "pool_added", "pool_deduped",
        "pool_total_after", "pf_pending_tickers", "pf_ready_tickers",
        "pf_marker_written", "de_cycle_result", "de_long_csv_rows",
        "de_short_csv_rows", "elapsed_sec", "error",
    ]
    with open(_TIMELINE_PATH, "w", newline="", encoding="utf-8") as tf:
        writer = csv.DictWriter(tf, fieldnames=timeline_fields, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        tf.flush()

    rows: List[Dict[str, Any]] = []
    for slot in slots:
        row = _replay_slot(
            slot, short_cfg_se, long_cfg_se, short_cfg_de, long_cfg_de, tickers
        )
        rows.append(row)
        with open(_TIMELINE_PATH, "a", newline="", encoding="utf-8") as tf:
            writer = csv.DictWriter(tf, fieldnames=timeline_fields, quoting=csv.QUOTE_ALL)
            writer.writerow(row)

    _release_clock()

    # -------------------------- Summary --------------------------
    pool_json = _POOL_DIR / f"pending_signals_{_DATE_STR}_v16_5min.json"
    pool_csv = _POOL_DIR / f"pending_signals_{_DATE_STR}_v16_5min.csv"
    lifecycle_path = _POOL_DIR / f"pool_lifecycle_{_DATE_STR}_v16_5min.jsonl"
    long_csv = _POOL_DIR / f"signals_{_DATE_STR}_v16_5min_long.csv"
    short_csv = _POOL_DIR / f"signals_{_DATE_STR}_v16_5min_short.csv"

    total_raw_short = sum(r.get("raw_short", 0) for r in rows)
    total_raw_long = sum(r.get("raw_long", 0) for r in rows)
    total_added = sum(r.get("pool_added", 0) for r in rows)
    total_deduped = sum(r.get("pool_deduped", 0) for r in rows)
    final_pool_total = rows[-1]["pool_total_after"] if rows else 0
    total_long_csv = _count_rows(long_csv)
    total_short_csv = _count_rows(short_csv)

    lc_counts: Dict[str, int] = {}
    try:
        if lifecycle_path.exists():
            with open(lifecycle_path, "r", encoding="utf-8") as lf:
                for line in lf:
                    try:
                        evt = json.loads(line)
                    except Exception:
                        continue
                    k = str(evt.get("event", ""))
                    lc_counts[k] = lc_counts.get(k, 0) + 1
    except Exception as exc:
        print(f"[WARN] lifecycle read failed: {exc}", flush=True)

    pool_signals: List[Dict[str, Any]] = []
    try:
        if pool_json.exists():
            pool_signals = json.loads(pool_json.read_text(encoding="utf-8")).get("signals", [])
    except Exception as exc:
        print(f"[WARN] pool_json read failed: {exc}", flush=True)

    def _count_status(status: str) -> int:
        return sum(1 for s in pool_signals if str(s.get("status", "")).lower() == status.lower())

    with open(_SUMMARY_PATH, "w", encoding="utf-8") as sf:
        sf.write("EQIDV2 V16 5min — Full-Pipeline Replay Summary\n")
        sf.write("=" * 60 + "\n")
        sf.write(f"Date              : {_DATE_STR}\n")
        sf.write(f"Slots replayed    : {len(rows)} ({_ARGS.start} -> {_ARGS.end})\n")
        sf.write(f"Universe size     : {len(tickers)}\n")
        sf.write("\n")
        sf.write("--- Signal Engine (Stage 1) ---\n")
        sf.write(f"  raw_short_total : {total_raw_short}\n")
        sf.write(f"  raw_long_total  : {total_raw_long}\n")
        sf.write(f"  pool_added      : {total_added}\n")
        sf.write(f"  pool_deduped    : {total_deduped}\n")
        sf.write(f"  pool_final_size : {final_pool_total}\n")
        sf.write("\n")
        sf.write("--- Pool status breakdown (final JSON) ---\n")
        for st in ("pending", "detected", "filtered_missing_token",
                   "filtered_verify_failed", "filtered_trigger_drifted"):
            sf.write(f"  {st:<26s}: {_count_status(st)}\n")
        sf.write("\n")
        sf.write("--- Pool Lifecycle Events ---\n")
        for k in ("WRITTEN", "PF_VERIFIED", "DE_PASSED", "DROPPED"):
            sf.write(f"  {k:<12s}: {lc_counts.get(k, 0)}\n")
        sf.write("\n")
        sf.write("--- Detection Engine (Stage 2) ---\n")
        sf.write(f"  long_signals_csv  : {total_long_csv} rows  ({long_csv.name})\n")
        sf.write(f"  short_signals_csv : {total_short_csv} rows  ({short_csv.name})\n")
        sf.write("\n")
        sf.write("--- Output Files ---\n")
        sf.write(f"  pool_json         : {pool_json}\n")
        sf.write(f"  pool_csv          : {pool_csv}\n")
        sf.write(f"  pool_lifecycle    : {lifecycle_path}\n")
        sf.write(f"  long_csv          : {long_csv}\n")
        sf.write(f"  short_csv         : {short_csv}\n")
        sf.write(f"  ready_markers     : {_READY_DIR}\n")
        sf.write(f"  slot_timeline     : {_TIMELINE_PATH}\n")

    print("\n" + "=" * 72)
    print(_SUMMARY_PATH.read_text(encoding="utf-8"))
    print("=" * 72)
    print(f"[DONE] Replay finished. Summary: {_SUMMARY_PATH}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        rc = main()
    except Exception as exc:
        print(f"[FATAL] {type(exc).__name__}: {exc}", flush=True)
        traceback.print_exc()
        rc = 1
    sys.exit(rc)
