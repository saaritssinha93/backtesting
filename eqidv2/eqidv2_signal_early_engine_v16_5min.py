# -*- coding: utf-8 -*-
"""
EQIDV2 — V16 5min Signal Early Engine (SEE)
============================================
Drop-in replacement for `eqidv2_signal_engine_v16_5min.py` (SE).

Same INPUT: 5-min parquet tail under DATA_5M_DIR.
Same OUTPUT: `pending_signals_YYYY-MM-DD_v16_5min.json` + `.csv`
             + `pool_lifecycle_YYYY-MM-DD_v16_5min.jsonl` (under LIVE_SIGNALS_DIR).
Same dedup key (ticker, side, entry_time, setup) via base_v15._generate_signal_id.

Difference is in the scan logic:
  SE fires at entry-bar close (slot t) and emits rows for setups whose
      trigger bar is t-lag.
  SEE fires at trigger-bar close (slot t) and emits CANDIDATE rows for
      setups whose trigger bar IS t (entry bar opens at t, closes at t+lag*5m).

Net effect: executor sees each setup ~`lag*5m` earlier than SE does, giving
time to place the intrabar order before the entry bar opens.

Coverage (all currently enabled V16 5min setups):
    Phase 2a — C1-anchored (trigger_iso = C1 close):
      - A_MOD_BREAK_C1_HIGH              (LONG,  lag=1)
      - A_MOD_BREAK_C1_LOW               (SHORT, lag=1)
    Phase 2b — C1-anchored HUGE / continuation:
      - A_MOD_CLOSE_CONTINUATION_BREAK   (LONG,  lag=2)
      - B_HUGE_C1_CLOSE_RECLAIM_BREAK    (LONG,  lag=2)
    Phase 2c — C2-anchored (trigger_iso = C2 close; c1_iso = C2 − 5min):
      - A_PULLBACK_C2_THEN_BREAK_C2_HIGH (LONG,  lag=2 from C1)
      - A_PULLBACK_C2_THEN_BREAK_C2_LOW  (SHORT, lag=2 from C1)
    Phase 2d — bounce-anchored (trigger_iso = bounce_end = i+3):
      - B_HUGE_RED_FAILED_BOUNCE         (SHORT, dynamic lag)
    Skipped (dead): B_HUGE_PULLBACK_HOLD_BREAK (scanner lag=999).

Status/log file names:
    eqidv2_signal_early_engine_v16_5min.log
    eqidv2_signal_early_engine_v16_5min.status
    eqidv2_signal_early_engine_v16_5min.heartbeat

Scheduled task name:
    EQIDV2_signal_early_engine_v16_5min_0900
"""
from __future__ import annotations

import multiprocessing as mp
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

import eqidv2_signal_engine_v16_5min as se
from eqidv2_signal_engine_v16_5min import (
    IST,
    END_TIME,
    HARD_STOP_TIME,
    DIR_5M,
    SCAN_SHARDS,
    SCAN_MAX_WORKERS,
    SNAPSHOT_MAX_WORKERS,
    USE_SLOT_SNAPSHOTS,
    USE_SNAPSHOT_ROLLING_CACHE,
    SLOT_MINUTES,
    TAIL_ROWS,
    SKIP_STALE_SLOT_ON_TIMEOUT,
    SETUP_LAG_BARS,
    RUNTIME_STATUS_DIR,
    RUNTIME_LIVE_SIGNALS_DIR,
    _list_tickers_5m,
    _build_v16_cfgs,
    _next_5min_slot,
    _sleep_until_resilient,
    _now_ist,
    _wait_for_slot_data_ready,
    _compute_nifty_rs_at_slot,
    _run_startup_signal_id_self_check,
    _write_pending_pool,
    _load_5m_parquet,
    _normalize_snapshot_shard_df,
    _split_tickers_for_scan_shards,
)
from avwap_v11_refactored.avwap_common_v11_v15 import prepare_session_bars_for_scan
from live_v16_5min_slot_snapshot import (
    build_slot_snapshots,
    clear_rolling_cache as clear_slot_snapshot_cache,
    read_shard_snapshot,
)
from eqidv2_early_emit_v16_5min import evaluate_slot_candidates

import eqidv2_live_combined_analyser_csv_v15 as base_v15
import eqidv2_live_combined_analyser_csv_v16_5min as live_v16


_SCRIPT_NAME = "eqidv2_signal_early_engine_v16_5min.py"
_BASE_DIR  = Path(__file__).resolve().parent
_LOG_DIR   = _BASE_DIR / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_LOG_FILE  = _LOG_DIR / "eqidv2_signal_early_engine_v16_5min.log"


# ---------------------------------------------------------------------------
# STDOUT TEE
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


def _reconfigure_stdio() -> None:
    for name in ("stdout", "stderr", "__stdout__", "__stderr__"):
        stream = getattr(sys, name, None)
        if stream is None or not hasattr(stream, "reconfigure"):
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def _start_tee() -> None:
    try:
        _reconfigure_stdio()
        fh = open(_LOG_FILE, "a", encoding="utf-8", buffering=1)
        base_stdout = getattr(sys, "__stdout__", None) or sys.stdout
        base_stderr = getattr(sys, "__stderr__", None) or sys.stderr
        sys.stdout = _Tee(base_stdout, fh)  # type: ignore[assignment]
        sys.stderr = _Tee(base_stderr, fh)  # type: ignore[assignment]
    except Exception:
        pass


# ---------------------------------------------------------------------------
# STATUS / HEARTBEAT (SEE-specific files; do NOT reuse SE's)
# ---------------------------------------------------------------------------
def _touch_status(status: str, **extra: Any) -> None:
    os.environ["EQIDV2_RUNTIME_STATUS_FILE"]    = str(RUNTIME_STATUS_DIR / "eqidv2_signal_early_engine_v16_5min.status")
    os.environ["EQIDV2_RUNTIME_HEARTBEAT_FILE"] = str(RUNTIME_STATUS_DIR / "eqidv2_signal_early_engine_v16_5min.heartbeat")
    os.environ["EQIDV2_RUNTIME_SCRIPT_NAME"]    = _SCRIPT_NAME
    base_v15._touch_runtime_status(status, **extra)


def _touch_heartbeat(state: str = "RUNNING", **extra: Any) -> None:
    base_v15._touch_runtime_heartbeat(state, **extra)


# ---------------------------------------------------------------------------
# CANDIDATE → scanner-output-compatible row
# ---------------------------------------------------------------------------
def _candidate_to_trade_row(
    cand: Dict[str, Any],
    short_cfg,
    long_cfg,
) -> Dict[str, Any]:
    """Convert an early-emit CANDIDATE dict to a dict compatible with
    `_write_pending_pool` (i.e. the shape produced by the V11 scanner's
    TradeRecord.asdict()).

    signal_time_ist MUST equal the scanner-canonical C1 close stamp (carried
    on the candidate as `c1_iso`) so downstream F3/dedup/drift logic agrees
    with scanner output. For C1-anchored setups c1_iso == trigger_iso; for
    C2- and bounce-anchored setups they differ."""
    side = str(cand.get("side", "")).upper()
    cfg = short_cfg if side == "SHORT" else long_cfg
    trigger_iso = str(cand.get("trigger_iso", ""))
    setup = str(cand.get("setup", ""))
    c1_iso = str(cand.get("c1_iso") or trigger_iso)

    trig_ts = pd.Timestamp(trigger_iso)
    if trig_ts.tzinfo is None:
        trig_ts = trig_ts.tz_localize(IST)
    else:
        trig_ts = trig_ts.tz_convert(IST)

    entry_iso_cand = cand.get("entry_iso")
    if entry_iso_cand:
        entry_open_ts = pd.Timestamp(entry_iso_cand)
        if entry_open_ts.tzinfo is None:
            entry_open_ts = entry_open_ts.tz_localize(IST)
        else:
            entry_open_ts = entry_open_ts.tz_convert(IST)
    else:
        lag = SETUP_LAG_BARS.get(setup, 1)
        if lag is None or lag < 0:
            lag = 1
        entry_open_ts = trig_ts + pd.Timedelta(minutes=SLOT_MINUTES * lag)

    # Strategy_v2 §C2 contract: SE accepts scanner_entry_dt ∈
    # {expected_entry_open, expected_entry_close}. Emit the OPEN form, which
    # is what the v11 scanner produces for entry_time_ist on the close-stamp
    # parquets.
    entry_time_ist = entry_open_ts.isoformat()
    signal_time_ist = c1_iso

    break_level = float(cand.get("break_level", 0.0))
    stop_pct = float(getattr(cfg, "stop_pct", 0.0075))
    target_pct = float(getattr(cfg, "target_pct", 0.01))

    if side == "LONG":
        stop_price = break_level * (1.0 - stop_pct)
        target_price = break_level * (1.0 + target_pct)
    else:
        stop_price = break_level * (1.0 + stop_pct)
        target_price = break_level * (1.0 - target_pct)

    snap = cand.get("preconds_snapshot") or {}
    return {
        "ticker": cand.get("ticker", ""),
        "side": side,
        "setup": setup,
        "signal_time_ist": signal_time_ist,
        "entry_time_ist": entry_time_ist,
        "signal_price": break_level,
        "entry_price": break_level,
        "stop_price": stop_price,
        "target_price": target_price,
        "quality_score": 0.0,
        "avwap_dist_atr_signal": float(snap.get("avwap_dist_atr", 0.0) or 0.0),
        "rsi_signal": 0.0,
        "adx_signal": 0.0,
        "adx": 0.0,
        "bars_from_open": 0,
        "or_high": 0.0,
        "or_low": 0.0,
        "entry_bar_vol_ratio": 0.0,
        "impulse_type": str(snap.get("impulse", "") or ""),
    }


# ---------------------------------------------------------------------------
# PER-SHARD EARLY-EMIT WORKER
# ---------------------------------------------------------------------------
def _filter_df_prepared_to_slot(
    df_prepared: pd.DataFrame,
    slot_ts: pd.Timestamp,
) -> pd.DataFrame:
    if df_prepared is None or df_prepared.empty or "date" not in df_prepared.columns:
        return pd.DataFrame()
    today = slot_ts.date()
    dates = pd.to_datetime(df_prepared["date"])
    if getattr(dates.dt, "tz", None) is None:
        dates = dates.dt.tz_localize(IST)
    else:
        dates = dates.dt.tz_convert(IST)
    mask = (dates.dt.date == today) & (dates <= slot_ts)
    out = df_prepared[mask].copy()
    return out.reset_index(drop=True)


def _see_partition_worker(
    slot_iso: str,
    partition_name: str,
    partition_tickers: List[str],
    short_cfg,
    long_cfg,
    use_slot_snapshots: bool,
) -> Tuple[Dict[str, Any], List[dict], List[dict]]:
    """Mirror of SE's `_scan_partition_worker`, but calls
    `evaluate_slot_candidates` instead of the full v16 scanner."""
    import time as _time
    slot_ts = pd.Timestamp(slot_iso)
    if slot_ts.tzinfo is None:
        slot_ts = slot_ts.tz_localize(IST)
    else:
        slot_ts = slot_ts.tz_convert(IST)

    part_short_rows: List[dict] = []
    part_long_rows: List[dict] = []
    errors = 0
    started = _time.perf_counter()
    source_rows = 0

    if use_slot_snapshots:
        shard_df = _normalize_snapshot_shard_df(read_shard_snapshot(slot_ts, partition_name))
        source_rows = int(len(shard_df))
        grouped_frames = list(shard_df.groupby("ticker", sort=True)) if not shard_df.empty else []
    else:
        grouped_frames = [(ticker, _load_5m_parquet(ticker)) for ticker in partition_tickers]

    scanned_tickers = 0
    for ticker, df in grouped_frames:
        try:
            if df is None or df.empty:
                continue
            scanned_tickers += 1
            df_prepared = prepare_session_bars_for_scan(df, short_cfg)
            if df_prepared is None or df_prepared.empty:
                continue
            df_prepared = _filter_df_prepared_to_slot(df_prepared, slot_ts)
            if df_prepared.empty:
                continue

            cands = evaluate_slot_candidates(
                str(ticker), df_prepared, slot_ts, short_cfg, long_cfg
            )
            for c in cands:
                row = _candidate_to_trade_row(c, short_cfg, long_cfg)
                side = str(row.get("side", "")).upper()
                if side == "SHORT":
                    part_short_rows.append(row)
                else:
                    part_long_rows.append(row)
        except Exception:
            errors += 1

    elapsed = _time.perf_counter() - started
    meta = {
        "partition": partition_name,
        "tickers": int(scanned_tickers),
        "errors": int(errors),
        "short_rows": len(part_short_rows),
        "long_rows": len(part_long_rows),
        "source_rows": int(source_rows),
        "elapsed_sec": round(elapsed, 3),
    }
    return meta, part_short_rows, part_long_rows


def _see_scan_slot(
    slot_ist: datetime,
    short_cfg,
    long_cfg,
    tickers: Optional[List[str]] = None,
    prebuilt_snapshot_meta: Optional[Dict[str, Any]] = None,
) -> Tuple[List[dict], List[dict], Dict[str, Any]]:
    """Early-emit variant of SE's `_scan_slot`."""
    from concurrent.futures import ProcessPoolExecutor, as_completed

    tickers = list(tickers or _list_tickers_5m())
    partitions = [p for p in _split_tickers_for_scan_shards(tickers, SCAN_SHARDS) if p]
    snapshot_meta: Dict[str, Any] = {}
    shard_names: List[str] = []

    if USE_SLOT_SNAPSHOTS:
        snapshot_meta = dict(prebuilt_snapshot_meta or {})
        if not snapshot_meta:
            snapshot_meta = build_slot_snapshots(
                slot_ist,
                shard_count=SCAN_SHARDS,
                tail_rows=TAIL_ROWS,
                max_workers=SNAPSHOT_MAX_WORKERS,
                use_rolling_cache=USE_SNAPSHOT_ROLLING_CACHE,
                build_slot_context=True,
            )
        shard_names = sorted(str(k) for k in (snapshot_meta.get("snapshot_paths") or {}).keys())
    else:
        shard_names = [f"{idx:02d}" for idx, _p in enumerate(partitions, 1)]

    effective_workers = min(max(1, int(SCAN_MAX_WORKERS)), len(shard_names))

    short_rows: List[dict] = []
    long_rows: List[dict] = []
    partition_metas: List[Dict[str, Any]] = []

    print(
        f"[SEE_SCAN] Starting sharded early-emit scan: tickers={len(tickers)} "
        f"shards={len(shard_names)} workers={effective_workers} "
        f"mode={'snapshot' if USE_SLOT_SNAPSHOTS else 'raw'}",
        flush=True,
    )

    scan_started = time.perf_counter()
    with ProcessPoolExecutor(max_workers=effective_workers, mp_context=mp.get_context("spawn")) as executor:
        future_map = {
            executor.submit(
                _see_partition_worker,
                slot_ist.isoformat(),
                shard_names[idx - 1],
                partition_tickers,
                short_cfg,
                long_cfg,
                USE_SLOT_SNAPSHOTS,
            ): idx
            for idx, partition_tickers in enumerate(partitions, 1)
        }
        for future in as_completed(future_map):
            meta, part_short_rows, part_long_rows = future.result()
            partition_metas.append(meta)
            short_rows.extend(part_short_rows)
            long_rows.extend(part_long_rows)
            print(
                f"  [SEE_SCAN] shard={meta['partition']} tickers={meta['tickers']} "
                f"short={meta['short_rows']} long={meta['long_rows']} "
                f"errors={meta['errors']} elapsed={meta['elapsed_sec']:.1f}s",
                flush=True,
            )

    scan_elapsed = time.perf_counter() - scan_started
    total_elapsed = float(snapshot_meta.get("total_elapsed_sec", 0.0)) + scan_elapsed
    print(
        f"[SEE_SCAN] Done: cand_short={len(short_rows)} cand_long={len(long_rows)} "
        f"scan_elapsed={scan_elapsed:.1f}s total_elapsed={total_elapsed:.1f}s",
        flush=True,
    )
    meta_out = {
        "scan_elapsed_sec": round(scan_elapsed, 3),
        "total_elapsed_sec": round(total_elapsed, 3),
    }
    return short_rows, long_rows, meta_out


# ---------------------------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------------------------
def main() -> None:
    if mp.current_process().name != "MainProcess":
        return
    _start_tee()

    print(
        "=" * 70 + "\n"
        "EQIDV2 V16 5min SIGNAL EARLY ENGINE (SEE)\n"
        f"  DATA_5M_DIR   : {DIR_5M}\n"
        f"  PENDING_DIR   : {RUNTIME_LIVE_SIGNALS_DIR}\n"
        "  LOGIC          : early-emit (trigger-bar close; lag-aware entry_time)\n"
        "  OUTPUT         : pending_signals_YYYY-MM-DD_v16_5min.json / .csv\n"
        "  COVERAGE       : Phase 2a+b+c+d (all V16 5min setups except dead B_HUGE_PULLBACK_HOLD_BREAK)\n"
        "=" * 70,
        flush=True,
    )

    _touch_status("STARTING")
    try:
        _run_startup_signal_id_self_check()
    except Exception as exc:
        _touch_status("FAILED", phase="STARTUP_SELF_CHECK", reason=str(exc))
        _touch_heartbeat("FAILED", phase="STARTUP_SELF_CHECK", reason=str(exc))
        print(f"[FATAL] Startup signal_id self-check failed: {exc}", flush=True)
        raise

    short_cfg, long_cfg = _build_v16_cfgs()
    tickers = _list_tickers_5m()
    holidays = base_v15._read_holidays_safe()
    cache_day: Optional[Any] = None
    print(f"[INFO] Tickers in 5-min dir: {len(tickers)}", flush=True)

    while True:
        now = _now_ist()
        _touch_status("RUNNING", phase="LOOP")
        _touch_heartbeat("RUNNING", phase="LOOP")

        if now.time() >= HARD_STOP_TIME:
            _touch_status("STOPPED_AFTER_CUTOFF")
            _touch_heartbeat("STOPPED")
            print("[STOP] Hard-stop reached. Exiting.", flush=True)
            return

        if not base_v15.is_trading_day_safe(now.date(), holidays):
            clear_slot_snapshot_cache()
            nxt = base_v15._next_trading_day_start(now, holidays)
            print(f"[SKIP] Not a trading day. Sleeping until {base_v15._fmt_ist_dt(nxt)}.", flush=True)
            _sleep_until_resilient(nxt, phase="WAIT_NEXT_TRADING_DAY", next_wake=base_v15._fmt_ist_dt(nxt))
            holidays = base_v15._read_holidays_safe()
            continue

        slot = _next_5min_slot(now)
        if slot.date() != now.date():
            clear_slot_snapshot_cache()
            nxt = base_v15._next_trading_day_start(now, holidays)
            print(f"[DONE] Past END_TIME. Sleeping until {base_v15._fmt_ist_dt(nxt)}.", flush=True)
            _sleep_until_resilient(nxt, phase="WAIT_NEXT_TRADING_DAY", next_wake=base_v15._fmt_ist_dt(nxt))
            holidays = base_v15._read_holidays_safe()
            continue

        if now < slot:
            print(f"[WAIT] Sleeping until slot {slot.strftime('%Y-%m-%d %H:%M:%S%z')}", flush=True)
            _sleep_until_resilient(
                slot, phase="WAIT_SLOT",
                slot=slot.strftime("%H:%M"),
                next_wake=slot.strftime("%Y-%m-%d %H:%M:%S%z"),
                poll_seconds=15.0,
            )

        now = _now_ist()
        if now.time() > END_TIME:
            nxt = base_v15._next_trading_day_start(now, holidays)
            print(f"[DONE] Past END_TIME. Sleeping until {base_v15._fmt_ist_dt(nxt)}.", flush=True)
            _sleep_until_resilient(nxt, phase="WAIT_NEXT_TRADING_DAY", next_wake=base_v15._fmt_ist_dt(nxt))
            holidays = base_v15._read_holidays_safe()
            continue

        if cache_day != slot.date():
            clear_slot_snapshot_cache()
            cache_day = slot.date()

        slot_start = time.perf_counter()
        date_str   = slot.strftime("%Y-%m-%d")
        slot_str   = slot.strftime("%H:%M")
        print(f"\n[SLOT] {slot.strftime('%Y-%m-%d %H:%M:%S%z')}", flush=True)
        _touch_status("RUNNING", phase="WAIT_DATA", slot=slot_str)
        _touch_heartbeat("RUNNING", phase="WAIT_DATA", slot=slot_str)

        ready, ratio, waited, checked, gate_reason, gate_detail = _wait_for_slot_data_ready(slot, tickers)
        print(
            f"[WAIT] slot={slot_str} ready={ready} reason={gate_reason} "
            f"fresh_ratio={ratio:.2f} waited={waited:.1f}s checked={checked}",
            flush=True,
        )
        if not ready and SKIP_STALE_SLOT_ON_TIMEOUT:
            _touch_status("RUNNING", phase="SKIP_STALE", slot=slot_str)
            if gate_reason == "marker_incomplete":
                tickers_written = gate_detail.get("tickers_written", "n/a")
                tickers_expected = gate_detail.get("tickers_expected", "n/a")
                failures = gate_detail.get("partition_failures", []) or []
                verify_failed = gate_detail.get("verification_failed_count", 0)
                print(
                    f"[ABORT] LF_INCOMPLETE slot={slot_str} reason=marker_incomplete "
                    f"tickers_written={tickers_written}/{tickers_expected} "
                    f"partition_failures={len(failures)} verify_failed={verify_failed} "
                    f"duration_ms={gate_detail.get('duration_ms', 0):.0f}",
                    flush=True,
                )
            else:
                print(
                    f"[ABORT] LF_INCOMPLETE slot={slot_str} reason={gate_reason} "
                    f"ratio={ratio:.2f} waited={waited:.1f}s checked={checked}",
                    flush=True,
                )
            continue

        _touch_status("RUNNING", phase="SCAN", slot=slot_str)
        _touch_heartbeat("RUNNING", phase="SCAN", slot=slot_str)

        prebuilt_snapshot_meta: Optional[Dict[str, Any]] = None
        if USE_SLOT_SNAPSHOTS:
            prebuilt_snapshot_meta = build_slot_snapshots(
                slot,
                shard_count=SCAN_SHARDS,
                tail_rows=TAIL_ROWS,
                max_workers=SNAPSHOT_MAX_WORKERS,
                use_rolling_cache=USE_SNAPSHOT_ROLLING_CACHE,
                build_slot_context=True,
            )

        rs_pct, _allow_long, _allow_short = _compute_nifty_rs_at_slot(slot)

        short_rows, long_rows, _scan_meta = _see_scan_slot(
            slot, short_cfg, long_cfg,
            tickers=tickers,
            prebuilt_snapshot_meta=prebuilt_snapshot_meta,
        )

        raw_short = len(short_rows)
        raw_long = len(long_rows)
        raw_short_backup = list(short_rows)
        raw_long_backup = list(long_rows)
        filter_meta: Dict[str, Any] = {}
        try:
            context_state = live_v16._build_backtest_context_state(slot, short_cfg)
            short_rows, long_rows, filter_meta = live_v16._apply_backtest_parity_filters_to_dicts(
                short_rows,
                long_rows,
                short_cfg,
                long_cfg,
                context_state.get("mode_map", {}),
                context_state.get("nifty_ret_map", {}),
            )
        except Exception as exc:
            print(
                f"[SEE_PARITY_FILTER] slot={slot_str} error={exc!r} -> emitting raw candidates",
                flush=True,
            )

        # Sanitize NaN numeric fields introduced by pandas DataFrame round-trip
        # inside _apply_backtest_parity_filters_to_dicts. _write_pending_pool does
        # int(row.get("bars_from_open", 0) or 0), which raises on NaN because NaN
        # is truthy in Python. Dropping the key lets the default-0 fallback fire.
        def _strip_nan(rows: List[dict]) -> None:
            for r in rows:
                for k in ("bars_from_open",):
                    v = r.get(k, None)
                    try:
                        if v is not None and pd.isna(v):
                            r.pop(k, None)
                    except Exception:
                        pass
        _strip_nan(short_rows)
        _strip_nan(long_rows)

        # Audit: emit pool-lifecycle DROPPED events for rows the v17f filter
        # stack killed at SEE. Gives you a greppable kill log in
        # pool_lifecycle_<date>.jsonl since live-parity DE never sees these rows.
        try:
            def _audit_key(row: dict) -> Tuple[str, str, str]:
                return (
                    str(row.get("ticker", "")).upper().strip(),
                    str(row.get("setup", "")).upper().strip(),
                    str(row.get("signal_time_ist", row.get("signal_datetime", ""))).strip(),
                )
            final_short_keys = {_audit_key(r) for r in short_rows}
            final_long_keys = {_audit_key(r) for r in long_rows}
            _audit_now = _now_ist().strftime("%Y-%m-%d %H:%M:%S%z")
            _audit_slot_iso = slot.strftime("%Y-%m-%dT%H:%M:%S%z")
            from avwap_combined_runner_v16_5min import get_v16_filter_reason as _v16_filter_reason
            for _raw_rows, _final_keys, _side in (
                (raw_short_backup, final_short_keys, "SHORT"),
                (raw_long_backup, final_long_keys, "LONG"),
            ):
                for _r in _raw_rows:
                    _k = _audit_key(_r)
                    if _k in _final_keys:
                        continue
                    try:
                        _reason = _v16_filter_reason(_r, _side) or "nifty_context_or_aggregate"
                    except Exception:
                        _reason = "parity_filter_drop"
                    try:
                        se._append_pool_lifecycle_event(date_str, {
                            "event": "DROPPED",
                            "ticker": _k[0],
                            "side": _side,
                            "setup": _k[1],
                            "signal_time_ist": _k[2],
                            "source_slot": _audit_slot_iso,
                            "reason": f"see_pre_pool_filter: {_reason}",
                            "ts": _audit_now,
                        })
                    except Exception:
                        pass
        except Exception as exc:
            print(
                f"[SEE_PARITY_AUDIT] slot={slot_str} error={exc!r}",
                flush=True,
            )

        added, deduped, total_pending = _write_pending_pool(
            short_rows, long_rows, slot, rs_pct, date_str
        )

        elapsed = time.perf_counter() - slot_start
        print(
            f"[SEE] slot={slot_str} "
            f"cand_short={raw_short}->{len(short_rows)} "
            f"cand_long={raw_long}->{len(long_rows)} "
            f"(post_ctx_short={filter_meta.get('post_context_short', '?')} "
            f"final_short={filter_meta.get('final_short', '?')} "
            f"post_ctx_long={filter_meta.get('post_context_long', '?')} "
            f"final_long={filter_meta.get('final_long', '?')}) "
            f"new_pending={added} ({deduped} deduped) "
            f"total_pending={total_pending} "
            f"total_elapsed={elapsed:.1f}s",
            flush=True,
        )

        _touch_status("RUNNING", phase="SCAN_DONE", slot=slot_str)
        _touch_heartbeat("RUNNING", phase="SCAN_DONE", slot=slot_str)

        next_slot = slot + timedelta(minutes=SLOT_MINUTES)
        if _now_ist() < next_slot:
            time.sleep(2.0)


if __name__ == "__main__":
    mp.freeze_support()
    if mp.current_process().name == "MainProcess":
        main()
